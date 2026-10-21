use memmap2::{Mmap, MmapMut, MmapOptions};
use num::FromPrimitive;
use std::{
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::Path,
};
use tokio::task::JoinSet;

use crate::{
    add_result::{
        B, K, decode_positions_multiterm_multifield, decode_positions_multiterm_singlefield,
        get_next_position_multifield, get_next_position_singlefield,
    },
    compatible::{_blsr_u64, _mm_tzcnt_64},
    compress_postinglist::compress_postinglist,
    index::{
        CompressionType, DOCUMENT_LENGTH_COMPRESSION, FACET_VALUES_FILENAME, IndexArc, LevelIndex,
        NgramType, NonUniquePostingListObjectQuery, PostingListObjectQuery, ROARING_BLOCK_SIZE,
        Shard, TermObject, warmup,
    },
    utils::{block_copy, read_u8, read_u16, read_u32, read_u64, write_u16, write_u32, write_u64},
};

/// Write pending derived index components and propagate I/O failures.
///
/// This legacy mutable-file operation is not an atomic multi-file checkpoint.
/// On failure discard the affected derived instance; retrying it in place is not
/// a recovery protocol. The reliable record layer keeps authoritative operations
/// separately and rebuilds complete views before publication.
#[allow(async_fn_in_trait)]
pub trait Commit {
    /// Wait for every shard and return any write, mapping or synchronization error.
    async fn commit(&self) -> std::io::Result<()>;
}

impl Commit for IndexArc {
    async fn commit(&self) -> std::io::Result<()> {
        let index_ref = self.read().await;

        let shard_vec = index_ref.shard_vec.clone();
        drop(index_ref);

        let mut uncommitted_doc_count = 0usize;
        let mut uncommitted_vec_count = 0usize;

        let mut join_set = JoinSet::new();

        for shard in shard_vec {
            join_set.spawn(async move {
                let semaphore = { shard.read().await.semaphore.clone() };
                let _permit = semaphore.acquire_owned().await.map_err(std::io::Error::other)?;

                let shard_stats = {
                    let s = shard.read().await;
                    if !s.uncommitted {
                        None
                    } else {
                        let indexed_doc_count = s.indexed_doc_count;
                        let doc_delta = indexed_doc_count.saturating_sub(s.committed_doc_count);
                        let vec_delta = s.block_vector_buffer.len();
                        let is_vector_indexing = s.is_vector_indexing;
                        Some((indexed_doc_count, doc_delta, vec_delta, is_vector_indexing))
                    }
                };

                if let Some((indexed_doc_count, doc_delta, vec_delta, is_vector_indexing)) =
                    shard_stats
                {
                    {
                        let mut s = shard.write().await;
                        if is_vector_indexing {
                            s.commit_vector_shard().await?;
                        }
                        s.commit_lexical_shard(indexed_doc_count).await?;
                    }
                    warmup(&shard).await;
                    Ok::<_, std::io::Error>((doc_delta, vec_delta))
                } else {
                    Ok((0usize, 0usize))
                }
            });
        }

        // Drain every task even after a failure: returning early must not detach
        // other shard mutations or cancel them halfway through file publication.
        let mut first_error = None;
        while let Some(joined) = join_set.join_next().await {
            match joined {
                Ok(Ok((doc_delta, vec_delta))) => {
                    uncommitted_doc_count += doc_delta;
                    uncommitted_vec_count += vec_delta;
                }
                Ok(Err(error)) => { first_error.get_or_insert(error); }
                Err(error) => { first_error.get_or_insert_with(|| std::io::Error::other(error)); }
            }
        }
        if let Some(error) = first_error { return Err(error); }

        let index_ref = self.read().await;

        if !index_ref.mute {
            if uncommitted_doc_count == 0 && uncommitted_vec_count == 0 {
                log::info!(
                    "commit index {} level {} no uncommitted documents and vectors, skipping commit",
                    index_ref.meta.id,
                    index_ref.level_count().await,
                );
            } else {
                log::info!(
                    "commit index {} level {} committed documents {} {} committed vectors {} {}",
                    index_ref.meta.id,
                    index_ref.level_count().await,
                    uncommitted_doc_count,
                    index_ref.indexed_doc_count().await,
                    uncommitted_vec_count,
                    index_ref.indexed_vector_count().await
                );
            }
        }
        drop(index_ref);
        Ok(())
    }
}

impl Shard {
    pub(crate) async fn commit_lexical_shard(&mut self, indexed_doc_count: usize) -> std::io::Result<()> {
        self.invalidate_frequentword_results();
        let is_last_level_incomplete = self.is_last_level_incomplete;
        if is_last_level_incomplete {
            self.merge_incomplete_index_level_to_level0();

            self.index_file_mmap = MmapOptions::new().len(1).map_anon()?.make_read_only()?;

            self.index_file.set_len(self.last_level_index_file_start_pos)?;
            self.index_file.seek(SeekFrom::Start(self.last_level_index_file_start_pos))?;

            let idx = self.level_index.len() - 1;
            {
                // Keep the anonymous placeholder until the rewritten file is
                // complete. Remapping here would reopen a Windows mapping while
                // subsequent writes still need to extend the file.
                for segment in self.segments_index.iter_mut() {
                    if idx == segment.byte_array_blocks_pointer.len() - 1 {
                        segment.byte_array_blocks_pointer.remove(idx);
                    }
                }
            }
        } else {
            self.last_level_index_file_start_pos = self.index_file.stream_position()?;
            self.last_level_docstore_file_start_pos = self.docstore_file.stream_position()?;
        };

        if self.committed_doc_count / ROARING_BLOCK_SIZE == 0 {
            write_u16(
                self.longest_field_id as u16,
                &mut self.compressed_index_segment_block_buffer,
                0,
            );

            self
                .index_file
                .write_all(&self.compressed_index_segment_block_buffer[0..2])?;
        }

        let document_length_compressed_array_pointer =
            self.index_file.stream_position()? as usize;

        for document_length_compressed_array in self.document_length_compressed_array.iter_mut() {
            self.index_file.write_all(document_length_compressed_array)?;
        }

        if !self.mute {
            log::info!(
                "commit shard {} level {} indexed documents {} ({})",
                self.meta.id,
                self.level_index.len(),
                indexed_doc_count,
                (if indexed_doc_count == 0 {
                    0
                } else {
                    ((indexed_doc_count - 1) % 65_536) + 1
                })
            );
        }

        write_u64(
            indexed_doc_count as u64,
            &mut self.compressed_index_segment_block_buffer,
            0,
        );
        write_u64(
            self.positions_sum_normalized,
            &mut self.compressed_index_segment_block_buffer,
            8,
        );

        self
            .index_file
            .write_all(&self.compressed_index_segment_block_buffer[0..16])?;

        // Per-field snapshots belong to this committed level, alongside the
        // existing physical count and decoded-length sum, not a separate sidecar.
        if self.meta.field_length_normalization == crate::index::FieldLengthNormalization::PerField {
            for field in &self.indexed_field_vec {
                self.index_file.write_all(&(field.field_length_sum as u64).to_le_bytes())?;
                self.index_file.write_all(&field.field_document_count.to_le_bytes())?;
            }
        }

        let segment_head_position = self.index_file.stream_position()? as usize;
        self.index_file
            .seek(SeekFrom::Current((self.segment_number1 * 8) as i64))?;

        self.document_length_normalized_average =
            self.positions_sum_normalized as f32 / indexed_doc_count as f32;

        for (i, component) in self.bm25_component_cache.iter_mut().enumerate() {
            let document_length_quotient =
                DOCUMENT_LENGTH_COMPRESSION[i] as f32 / self.document_length_normalized_average;
            *component = K * (1.0 - B + B * document_length_quotient);
        }

        for k0 in 0..self.segment_number1 {
            let strip_compressed = self.commit_segment(k0)?;
            self.strip_compressed_sum += strip_compressed as u64;
            self.key_count_sum += self.segments_level0[k0].segment.len() as u64;
        }

        if !is_last_level_incomplete {
            self.level_index.push(LevelIndex {
                document_length_compressed_array_pointer,
                docstore_pointer_docs_pointer: 0,
            });
        }

        for document_length_compressed_array in self.document_length_compressed_array.iter_mut() {
            *document_length_compressed_array = [0; ROARING_BLOCK_SIZE];
        }

        let segment_head_position2 = self.index_file.stream_position()? as usize;
        self.index_file
            .seek(SeekFrom::Start(segment_head_position as u64))?;
        let segment_head_position3 =
            self.compressed_index_segment_block_buffer.len() - (self.segment_number1 * 8);
        self
            .index_file
            .write_all(&self.compressed_index_segment_block_buffer[segment_head_position3..])?;

        self.index_file.flush()?;

        self.index_file
            .seek(SeekFrom::Start(segment_head_position2 as u64))?;

        if !self.stored_field_names.is_empty() {
            self.commit_docstore(indexed_doc_count, is_last_level_incomplete)?;
        }

        {
            self.index_file.flush()?;

            self.index_file_mmap =
                unsafe { Mmap::map(&self.index_file)? };
        }

        if !self.facets.is_empty() {
            self.facets_file_mmap.flush()?;
            if self.facets_file.metadata()?.len()
                != (self.facets_size_sum * (self.level_index.len() + 1) * ROARING_BLOCK_SIZE) as u64
            {
                // Windows forbids SetEndOfFile while a file mapping is open.
                self.facets_file_mmap = MmapOptions::new().len(1).map_anon()?;
                self.facets_file.set_len(
                    (self.facets_size_sum * (self.level_index.len() + 1) * ROARING_BLOCK_SIZE) as u64,
                )?;

                self.facets_file_mmap =
                    unsafe { MmapMut::map_mut(&self.facets_file)? };
            }

            let index_path = Path::new(&self.index_path_string);
            let dictionary = File::create(Path::new(index_path).join(FACET_VALUES_FILENAME))?;
            serde_json::to_writer(&dictionary, &self.facets)?;
            dictionary.sync_all()?;
            self.facets_file.sync_all()?;
        }

        self.index_file.sync_all()?;
        if !self.stored_field_names.is_empty() {
            self.docstore_file.sync_all()?;
        }
        self.string_set_to_single_term_id();

        self.committed_doc_count = indexed_doc_count;
        self.is_last_level_incomplete =
            !(self.committed_doc_count).is_multiple_of(ROARING_BLOCK_SIZE);

        self.compressed_index_segment_block_buffer = vec![0; 10_000_000];
        self.postings_buffer = Vec::new();

        self.postings_buffer_pointer = 0;
        self.strip_compressed_sum = 0;

        for segment in self.segments_level0.iter_mut() {
            segment.segment.clear();
        }

        self.modified = true;

        self.uncommitted = false;
        Ok(())
    }

    /// Flush a single segment from the key hash range partitioned level to RAM and disk
    pub(crate) fn commit_segment(&mut self, key0: usize) -> std::io::Result<usize> {
        let mut key_head_pointer_w: usize = 0;
        let segment_head_position = self.compressed_index_segment_block_buffer.len()
            - (self.segment_number1 * 8)
            + (key0 * 8)
            + 4;
        write_u32(
            self.segments_level0[key0].segment.len() as u32,
            &mut self.compressed_index_segment_block_buffer,
            segment_head_position,
        );

        let mut key_body_pointer_w: usize =
            key_head_pointer_w + (self.segments_level0[key0].segment.len() * self.key_head_size);
        let key_body_pointer_wstart: usize = key_body_pointer_w;

        let mut key_list: Vec<u64> = self.segments_level0[key0].segment.keys().cloned().collect();
        key_list.sort_unstable();

        for key in key_list.iter() {
            let plo = self.segments_level0[key0].segment.get_mut(key).unwrap();

            let mut key_position_pointer_w: usize = key_body_pointer_w;
            let key_rank_position_pointer_w: usize =
                key_body_pointer_w + plo.size_compressed_positions_key;

            let posting_pointer_size_sum = plo.pointer_pivot_p_docid as usize * 2
                + if (plo.pointer_pivot_p_docid as usize) < plo.posting_count {
                    (plo.posting_count - plo.pointer_pivot_p_docid as usize) * 3
                } else {
                    0
                };

            let size_compressed_positions_key =
                plo.size_compressed_positions_key + posting_pointer_size_sum;

            let key_docid_pointer_w: usize = key_body_pointer_w + size_compressed_positions_key;

            let mut size_compressed_docid_key;

            let key_body_offset =
                key_rank_position_pointer_w as u32 - key_body_pointer_wstart as u32;

            size_compressed_docid_key = compress_postinglist(
                self,
                &mut key_head_pointer_w,
                &mut key_position_pointer_w,
                key_body_offset,
                &key0,
                key,
            );

            key_body_pointer_w = key_docid_pointer_w + size_compressed_docid_key;

            size_compressed_docid_key += self.key_head_size;

            self.size_compressed_docid_index += size_compressed_docid_key as u64;
            self.size_compressed_positions_index += size_compressed_positions_key as u64;
        }

        let compressed_segment_block_size = key_body_pointer_w;

        let segment_head_position = self.compressed_index_segment_block_buffer.len()
            - (self.segment_number1 * 8)
            + (key0 * 8);

        write_u32(
            compressed_segment_block_size as u32,
            &mut self.compressed_index_segment_block_buffer,
            segment_head_position,
        );

        let file_position = self.index_file.stream_position()? as usize;

        self
            .index_file
            .write_all(&self.compressed_index_segment_block_buffer[0..compressed_segment_block_size])?;

        {
            self.segments_index[key0].byte_array_blocks_pointer.push((
                file_position + key_body_pointer_wstart,
                (compressed_segment_block_size - key_body_pointer_wstart),
                key_list.len() as u32,
            ));
        }

        Ok(compressed_segment_block_size)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_docid(
        self: &mut Shard,
        plo: &mut PostingListObjectQuery,
        docid: usize,
        key_hash: u64,
        key0: usize,
        ngram_type: &NgramType,
        posting_count_ngram_1_compressed: u8,
        posting_count_ngram_2_compressed: u8,
        posting_count_ngram_3_compressed: u8,
    ) {
        let mut field_positions_vec: Vec<Vec<u16>> = vec![Vec::new(); self.indexed_field_vec.len()];

        if self.indexed_field_vec.len() == 1 {
            decode_positions_multiterm_singlefield(plo, true, true, false);

            let mut plo2 = NonUniquePostingListObjectQuery {
                positions_pointer: plo.positions_pointer as usize,
                is_embedded: plo.is_embedded,
                embedded_positions: plo.embedded_positions,
                field_vec: plo.field_vec.clone(),
                p_pos: 0,
                p_field: 0,
                positions_count: plo.positions_count,
                key0: key0 as u32,
                byte_array: plo.byte_array,
                term_index_unique: 0,
                term_index_nonunique: 0,
                pos: 0,
            };

            let mut prev_pos = 0;
            let mut one = 0;
            for _ in 0..plo.positions_count {
                plo2.pos = get_next_position_singlefield(&mut plo2);
                let pos = prev_pos + plo2.pos as u16 + one;
                field_positions_vec[0].push(pos);
                prev_pos = pos;
                one = 1;
                plo2.p_pos += 1;
            }
        } else {
            decode_positions_multiterm_multifield(self, plo, true, true, false);

            let mut plo2 = NonUniquePostingListObjectQuery {
                positions_pointer: plo.positions_pointer as usize,
                is_embedded: plo.is_embedded,
                embedded_positions: plo.embedded_positions,
                field_vec: plo.field_vec.clone(),
                p_pos: 0,
                p_field: 0,
                positions_count: plo.positions_count,
                key0: key0 as u32,
                byte_array: plo.byte_array,
                term_index_unique: 0,
                term_index_nonunique: 0,
                pos: 0,
            };

            for field in plo.field_vec.iter() {
                let mut prev_pos = 0;
                let mut one = 0;
                for _ in 0..field.1 {
                    plo2.pos = get_next_position_multifield(&mut plo2);
                    let pos = prev_pos + plo2.pos as u16 + one;
                    field_positions_vec[field.0 as usize].push(pos);
                    prev_pos = pos;
                    one = 1;
                    plo2.p_pos += 1;
                }
            }
        }

        let term = match ngram_type {
            NgramType::SingleTerm => TermObject {
                key_hash,
                key0: key0 as u32,
                ngram_type: ngram_type.clone(),
                term_ngram_1: String::new(),
                term_ngram_0: String::new(),
                field_positions_vec,

                ..Default::default()
            },
            NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => TermObject {
                key_hash,
                key0: key0 as u32,
                ngram_type: ngram_type.clone(),
                term_ngram_1: String::new(),
                term_ngram_0: String::new(),
                field_positions_vec,
                field_vec_ngram1: if self.indexed_field_vec.len() == 1 {
                    vec![(0, plo.tf_ngram1)]
                } else {
                    plo.field_vec_ngram1
                        .iter()
                        .map(|field| (field.0 as usize, field.1 as u32))
                        .collect()
                },

                field_vec_ngram2: if self.indexed_field_vec.len() == 1 {
                    vec![(0, plo.tf_ngram2)]
                } else {
                    plo.field_vec_ngram2
                        .iter()
                        .map(|field| (field.0 as usize, field.1 as u32))
                        .collect()
                },

                ..Default::default()
            },
            _ => TermObject {
                key_hash,
                key0: key0 as u32,
                ngram_type: ngram_type.clone(),
                term_ngram_1: String::new(),
                term_ngram_0: String::new(),

                field_positions_vec,
                field_vec_ngram1: if self.indexed_field_vec.len() == 1 {
                    vec![(0, plo.tf_ngram1)]
                } else {
                    plo.field_vec_ngram1
                        .iter()
                        .map(|field| (field.0 as usize, field.1 as u32))
                        .collect()
                },

                field_vec_ngram2: if self.indexed_field_vec.len() == 1 {
                    vec![(0, plo.tf_ngram2)]
                } else {
                    plo.field_vec_ngram2
                        .iter()
                        .map(|field| (field.0 as usize, field.1 as u32))
                        .collect()
                },

                field_vec_ngram3: if self.indexed_field_vec.len() == 1 {
                    vec![(0, plo.tf_ngram3)]
                } else {
                    plo.field_vec_ngram3
                        .iter()
                        .map(|field| (field.0 as usize, field.1 as u32))
                        .collect()
                },

                ..Default::default()
            },
        };

        self.index_posting(
            term,
            docid,
            true,
            posting_count_ngram_1_compressed,
            posting_count_ngram_2_compressed,
            posting_count_ngram_3_compressed,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn iterate_docid(
        self: &mut Shard,
        compression_type_pointer: u32,
        pointer_pivot_p_docid: u16,
        posting_count: u16,
        block_id: usize,
        key0: usize,
        key_hash: u64,
        ngram_type: NgramType,
        posting_count_ngram_1_compressed: u8,
        posting_count_ngram_2_compressed: u8,
        posting_count_ngram_3_compressed: u8,
    ) {
        let compression_type: CompressionType =
            FromPrimitive::from_u32(compression_type_pointer >> 30).unwrap();

        let rank_position_pointer_range: u32 =
            compression_type_pointer & 0b0011_1111_1111_1111_1111_1111_1111_1111;

        let posting_pointer_size_sum = pointer_pivot_p_docid as u32 * 2
            + if (pointer_pivot_p_docid as usize) <= posting_count as usize {
                ((posting_count as u32 + 1) - pointer_pivot_p_docid as u32) * 3
            } else {
                0
            };
        let compressed_doc_id_range: u32 = rank_position_pointer_range + posting_pointer_size_sum;

        let byte_array = {
            let segment = &self.segments_index[key0];
            let byte_array = &self.index_file_mmap[segment.byte_array_blocks_pointer[block_id].0
                ..segment.byte_array_blocks_pointer[block_id].0
                    + segment.byte_array_blocks_pointer[block_id].1];
            byte_array.to_owned()
        };

        let mut plo = PostingListObjectQuery {
            rank_position_pointer_range,
            pointer_pivot_p_docid,
            byte_array: &byte_array,
            p_docid: 0,
            ngram_type: ngram_type.clone(),
            ..Default::default()
        };

        match compression_type {
            CompressionType::Array => {
                for i in 0..=posting_count {
                    plo.p_docid = i as usize;

                    let docid = (block_id << 16)
                        | read_u16(
                            &byte_array[compressed_doc_id_range as usize..],
                            i as usize * 2,
                        ) as usize;

                    self.add_docid(
                        &mut plo,
                        docid,
                        key_hash,
                        key0,
                        &ngram_type,
                        posting_count_ngram_1_compressed,
                        posting_count_ngram_2_compressed,
                        posting_count_ngram_3_compressed,
                    );
                }
            }

            CompressionType::Rle => {
                let runs_count =
                    read_u16(&byte_array[compressed_doc_id_range as usize..], 0) as i32;

                plo.p_docid = 0;
                for i in (1..(runs_count << 1) + 1).step_by(2) {
                    let startdocid = read_u16(
                        &byte_array[compressed_doc_id_range as usize..],
                        i as usize * 2,
                    );
                    let runlength = read_u16(
                        &byte_array[compressed_doc_id_range as usize..],
                        (i + 1) as usize * 2,
                    );

                    for j in 0..=runlength {
                        let docid = (block_id << 16) | (startdocid + j) as usize;
                        self.add_docid(
                            &mut plo,
                            docid,
                            key_hash,
                            key0,
                            &ngram_type,
                            posting_count_ngram_1_compressed,
                            posting_count_ngram_2_compressed,
                            posting_count_ngram_3_compressed,
                        );

                        plo.p_docid += 1;
                    }
                }
            }

            CompressionType::Bitmap => {
                plo.p_docid = 0;

                for ulong_pos in 0u64..1024 {
                    let mut intersect: u64 = read_u64(
                        &byte_array[compressed_doc_id_range as usize..],
                        ulong_pos as usize * 8,
                    );

                    while intersect != 0 {
                        let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as u64;

                        intersect = unsafe { _blsr_u64(intersect) };

                        let docid = (block_id << 16) | ((ulong_pos << 6) + bit_pos) as usize;

                        self.add_docid(
                            &mut plo,
                            docid,
                            key_hash,
                            key0,
                            &ngram_type,
                            posting_count_ngram_1_compressed,
                            posting_count_ngram_2_compressed,
                            posting_count_ngram_3_compressed,
                        );

                        plo.p_docid += 1;
                    }
                }
            }

            _ => {}
        }
    }

    pub(crate) fn merge_incomplete_index_level_to_level0(self: &mut Shard) {
        let block_id = self.level_index.len() - 1;
        let committed_doc_count = ((self.committed_doc_count - 1) % ROARING_BLOCK_SIZE) + 1;

        for i in 0..self.indexed_field_vec.len() {
            {
                block_copy(
                    &self.index_file_mmap[self.level_index[block_id]
                        .document_length_compressed_array_pointer
                        + i * ROARING_BLOCK_SIZE..],
                    0,
                    &mut self.document_length_compressed_array[i],
                    0,
                    committed_doc_count,
                );
            }
        }

        for key0 in 0..self.segment_number1 {
            {
                let pointer = self.segments_index[key0].byte_array_blocks_pointer[block_id];

                let key_count = pointer.2 as usize;

                for key_index in 0..key_count {
                    let key_address;
                    let key_hash;
                    let posting_count;

                    let ngram_type;
                    let posting_count_ngram_1_compressed;
                    let posting_count_ngram_2_compressed;
                    let posting_count_ngram_3_compressed;
                    let pointer_pivot_p_docid_old;
                    let compression_type_pointer;
                    {
                        let byte_array = &self.index_file_mmap
                            [pointer.0 - (key_count * self.key_head_size)..pointer.0];
                        key_address = key_index * self.key_head_size;
                        key_hash = read_u64(byte_array, key_address);
                        posting_count = read_u16(byte_array, key_address + 8);
                        ngram_type = FromPrimitive::from_u64(key_hash & 0b111)
                            .unwrap_or(NgramType::SingleTerm);
                        match ngram_type {
                            NgramType::SingleTerm => {
                                posting_count_ngram_1_compressed = 0;
                                posting_count_ngram_2_compressed = 0;
                                posting_count_ngram_3_compressed = 0;
                            }
                            NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
                                posting_count_ngram_1_compressed =
                                    read_u8(byte_array, key_address + 14);
                                posting_count_ngram_2_compressed =
                                    read_u8(byte_array, key_address + 15);
                                posting_count_ngram_3_compressed = 0;
                            }
                            _ => {
                                posting_count_ngram_1_compressed =
                                    read_u8(byte_array, key_address + 14);
                                posting_count_ngram_2_compressed =
                                    read_u8(byte_array, key_address + 15);
                                posting_count_ngram_3_compressed =
                                    read_u8(byte_array, key_address + 16);
                            }
                        }

                        pointer_pivot_p_docid_old =
                            read_u16(byte_array, key_address + self.key_head_size - 6);
                        compression_type_pointer =
                            read_u32(byte_array, key_address + self.key_head_size - 4);
                    }

                    let mut pointer_pivot_p_docid_new = 0;
                    let mut size_compressed_positions_key_new = 0;
                    let mut pointer_first_new = 0;
                    let mut pointer_last_new = 0;
                    let mut pointer_first_old = 0;
                    let merge = match self.segments_level0[key0].segment.get_mut(&key_hash) {
                        Some(plo0) => {
                            pointer_pivot_p_docid_new = plo0.pointer_pivot_p_docid;
                            size_compressed_positions_key_new = plo0.size_compressed_positions_key;
                            plo0.pointer_pivot_p_docid = 0;
                            plo0.size_compressed_positions_key = 0;

                            pointer_first_new = plo0.pointer_first;
                            pointer_last_new = plo0.pointer_last;
                            pointer_first_old = self.postings_buffer_pointer;
                            true
                        }
                        None => false,
                    };

                    self.iterate_docid(
                        compression_type_pointer,
                        pointer_pivot_p_docid_old,
                        posting_count,
                        block_id,
                        key0,
                        key_hash,
                        ngram_type,
                        posting_count_ngram_1_compressed,
                        posting_count_ngram_2_compressed,
                        posting_count_ngram_3_compressed,
                    );

                    if merge {
                        let plo0 = self.segments_level0[key0]
                            .segment
                            .get_mut(&key_hash)
                            .unwrap();

                        plo0.pointer_pivot_p_docid = if pointer_pivot_p_docid_new == 0 {
                            pointer_pivot_p_docid_old
                        } else {
                            pointer_pivot_p_docid_old + pointer_pivot_p_docid_new
                        };

                        plo0.size_compressed_positions_key = size_compressed_positions_key_new;

                        let pointer_last_old = plo0.pointer_last;
                        plo0.pointer_first = pointer_first_old;
                        plo0.pointer_last = pointer_last_new;

                        write_u32(
                            pointer_first_new as u32,
                            &mut self.postings_buffer,
                            pointer_last_old,
                        );
                    }
                }
            }
        }
    }
}
