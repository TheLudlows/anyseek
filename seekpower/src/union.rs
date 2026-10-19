use crate::{
    add_result::{add_result_multiterm_multifield, is_facet_filter},
    compatible::{_blsr_u64, _mm_tzcnt_64},
    index::{
        CompressionType, NonUniquePostingListObjectQuery, PostingListObjectQuery, QueueObject,
        ROARING_BLOCK_SIZE, Shard,
    },
    intersection::intersection_blockid,
    search::{FilterSparse, ResultType, SearchResult},
    single::{single_blockid, single_docid},
    utils::{read_u16, read_u64, write_u64},
};

use ahash::AHashSet;
use num_traits::FromPrimitive;

use std::sync::Arc;
use std::{
    cmp,
    sync::atomic::{AtomicUsize, Ordering},
};

use async_recursion::async_recursion;

/// Union for a single block
#[allow(clippy::too_many_arguments)]
pub(crate) async fn union_docid<'a>(
    shard: &'a Shard,
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery<'a>],
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut [PostingListObjectQuery<'a>],
    block_id: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult<'_>,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
) {
    for plo in not_query_list.iter_mut() {
        let query_list_item_mut = plo;

        let result = query_list_item_mut
            .blocks
            .binary_search_by(|block| block.block_id.cmp(&(block_id as u32)));
        match result {
            Ok(p_block) => {
                query_list_item_mut.bm25_flag = true;
                query_list_item_mut.p_block = p_block as i32
            }
            Err(_) => {
                query_list_item_mut.bm25_flag = false;
                continue;
            }
        };
        let blo = &query_list_item_mut.blocks[query_list_item_mut.p_block as usize];

        query_list_item_mut.compression_type =
            FromPrimitive::from_u32(blo.compression_type_pointer >> 30).unwrap();

        query_list_item_mut.rank_position_pointer_range =
            blo.compression_type_pointer & 0b0011_1111_1111_1111_1111_1111_1111_1111;

        let posting_pointer_size_sum = blo.pointer_pivot_p_docid as usize * 2
            + if (blo.pointer_pivot_p_docid as usize) <= blo.posting_count as usize {
                ((blo.posting_count as usize + 1) - blo.pointer_pivot_p_docid as usize) * 3
            } else {
                0
            };
        query_list_item_mut.compressed_doc_id_range =
            query_list_item_mut.rank_position_pointer_range as usize + posting_pointer_size_sum;

        query_list_item_mut.p_docid = 0;
        query_list_item_mut.p_docid_count =
            query_list_item_mut.blocks[query_list_item_mut.p_block as usize].posting_count as usize
                + 1;

        query_list_item_mut.docid = 0;

        if query_list_item_mut.compression_type == CompressionType::Rle {
            query_list_item_mut.p_run_count = read_u16(
                query_list_item_mut.byte_array,
                query_list_item_mut.compressed_doc_id_range,
            ) as i32;
            let startdocid = read_u16(
                query_list_item_mut.byte_array,
                query_list_item_mut.compressed_doc_id_range + 2,
            );
            let runlength = read_u16(
                query_list_item_mut.byte_array,
                query_list_item_mut.compressed_doc_id_range + 4,
            );
            query_list_item_mut.docid = startdocid as i32;
            query_list_item_mut.run_end = (startdocid + runlength) as i32;
            query_list_item_mut.p_run_sum = runlength as i32;
            query_list_item_mut.p_run = 6;
        }
    }

    let mut valid_term_count = 0;
    let mut term_index = 0;
    let mut single_term_index = 0;

    for query_list_item_mut in query_list.iter_mut() {
        query_list_item_mut.end_flag = query_list_item_mut.end_flag_block
            || (query_list_item_mut.blocks[query_list_item_mut.p_block as usize].block_id
                != block_id as u32);

        if query_list_item_mut.end_flag {
            term_index += 1;
            continue;
        }

        valid_term_count += 1;
        single_term_index = term_index;
        term_index += 1;

        query_list_item_mut.p_docid = 0;
        query_list_item_mut.p_docid_count =
            query_list_item_mut.blocks[query_list_item_mut.p_block as usize].posting_count as usize
                + 1;

        query_list_item_mut.compression_type = FromPrimitive::from_u32(
            query_list_item_mut.blocks[query_list_item_mut.p_block as usize]
                .compression_type_pointer
                >> 30,
        )
        .unwrap();

        query_list_item_mut.rank_position_pointer_range = query_list_item_mut.blocks
            [query_list_item_mut.p_block as usize]
            .compression_type_pointer
            & 0b0011_1111_1111_1111_1111_1111_1111_1111;

        query_list_item_mut.pointer_pivot_p_docid =
            query_list_item_mut.blocks[query_list_item_mut.p_block as usize].pointer_pivot_p_docid;

        let posting_pointer_size_sum = query_list_item_mut.blocks
            [query_list_item_mut.p_block as usize]
            .pointer_pivot_p_docid as usize
            * 2
            + if (query_list_item_mut.blocks[query_list_item_mut.p_block as usize]
                .pointer_pivot_p_docid as usize)
                <= query_list_item_mut.blocks[query_list_item_mut.p_block as usize].posting_count
                    as usize
            {
                ((query_list_item_mut.blocks[query_list_item_mut.p_block as usize].posting_count
                    as usize
                    + 1)
                    - query_list_item_mut.blocks[query_list_item_mut.p_block as usize]
                        .pointer_pivot_p_docid as usize)
                    * 3
            } else {
                0
            };
        query_list_item_mut.compressed_doc_id_range =
            query_list_item_mut.rank_position_pointer_range as usize + posting_pointer_size_sum;
        query_list_item_mut.docid = 0;
        query_list_item_mut.intersect = 0;
        query_list_item_mut.ulong_pos = 0;
        query_list_item_mut.p_run = -2;
        query_list_item_mut.run_end = 0;
    }

    if valid_term_count == 0 {
        return;
    }

    if valid_term_count == 1 {
        if result_type == &ResultType::Count {
            *result_count += query_list[single_term_index].p_docid_count as i32;
        } else {
            single_docid(
                shard,
                query_list,
                not_query_list,
                &query_list[single_term_index].blocks
                    [query_list[single_term_index].p_block as usize],
                single_term_index,
                result_count,
                search_result,
                top_k,
                result_type,
                field_filter_set,
                facet_filter,
            )
            .await;
        }
        return;
    };

    if result_type == &ResultType::Count {
        union_count(
            shard,
            result_count,
            search_result,
            query_list,
            not_query_list,
            facet_filter,
            block_id,
        )
        .await;
        return;
    }

    // Ranked scans may prune candidates. Reuse the exact bitmap pass
    // for document hit counts when filters or large disjunctions require it.
    let count_before = *result_count;
    let exact_metadata_count = result_type == &ResultType::TopkCount
        && (query_list.len() > 32 || !facet_filter.is_empty());

    if query_list.len() <= 8 {
        union_scan_8(
            shard,
            non_unique_query_list,
            query_list,
            not_query_list,
            block_id,
            result_count,
            search_result,
            top_k,
            result_type,
            field_filter_set,
            facet_filter,
        )
        .await;
    } else {
        union_scan_32(
            shard,
            non_unique_query_list,
            query_list,
            not_query_list,
            block_id,
            result_count,
            search_result,
            top_k,
            result_type,
            field_filter_set,
            facet_filter,
        )
        .await;
    }
    if exact_metadata_count {
        let mut exact_count = count_before;
        union_count(
            shard,
            &mut exact_count,
            search_result,
            query_list,
            not_query_list,
            facet_filter,
            block_id,
        )
        .await;
        *result_count = exact_count;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn union_blockid<'a>(
    shard: &'a Shard,
    non_unique_query_list: &mut Vec<NonUniquePostingListObjectQuery<'a>>,
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut [PostingListObjectQuery<'a>],
    result_count_arc: &Arc<AtomicUsize>,
    search_result: &mut SearchResult<'_>,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
) {
    loop {
        let mut break_loop = true;
        let mut block_id_min = usize::MAX;

        for plo in query_list.iter_mut() {
            if !plo.end_flag_block {
                let block_id = plo.blocks[plo.p_block as usize].block_id as usize;

                if block_id < block_id_min {
                    block_id_min = block_id;
                }
            }
        }

        {
            for query_list_item_mut in query_list.iter_mut() {
                let segment = &shard.segments_index[query_list_item_mut.key0 as usize];
                query_list_item_mut.byte_array =
                    &shard.index_file_mmap[segment.byte_array_blocks_pointer[block_id_min].0
                        ..segment.byte_array_blocks_pointer[block_id_min].0
                            + segment.byte_array_blocks_pointer[block_id_min].1];
            }
            for nonunique_query_list_item_mut in non_unique_query_list.iter_mut() {
                let segment = &shard.segments_index[nonunique_query_list_item_mut.key0 as usize];
                nonunique_query_list_item_mut.byte_array =
                    &shard.index_file_mmap[segment.byte_array_blocks_pointer[block_id_min].0
                        ..segment.byte_array_blocks_pointer[block_id_min].0
                            + segment.byte_array_blocks_pointer[block_id_min].1];
            }
            for not_query_list_item_mut in not_query_list.iter_mut() {
                let segment = &shard.segments_index[not_query_list_item_mut.key0 as usize];
                not_query_list_item_mut.byte_array =
                    &shard.index_file_mmap[segment.byte_array_blocks_pointer[block_id_min].0
                        ..segment.byte_array_blocks_pointer[block_id_min].0
                            + segment.byte_array_blocks_pointer[block_id_min].1];
            }
        }

        let mut result_count_local = 0;
        union_docid(
            shard,
            non_unique_query_list,
            query_list,
            not_query_list,
            block_id_min,
            &mut result_count_local,
            search_result,
            top_k,
            result_type,
            field_filter_set,
            facet_filter,
        )
        .await;

        result_count_arc.fetch_add(result_count_local as usize, Ordering::Relaxed);

        for plo in query_list.iter_mut() {
            if !plo.end_flag_block {
                let block_id = plo.blocks[plo.p_block as usize].block_id as usize;
                if block_id == block_id_min {
                    if plo.p_block < plo.p_block_max - 1 {
                        plo.p_block += 1;
                        break_loop = false;
                    } else {
                        plo.end_flag_block = true;
                    }
                } else {
                    break_loop = false;
                }
            }
        }

        if break_loop {
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn union_scan_8<'a>(
    shard: &'a Shard,
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery<'a>],
    query_list: &mut [PostingListObjectQuery<'a>],
    not_query_list: &mut [PostingListObjectQuery<'a>],
    block_id: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult<'_>,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
) {
    let union_max = 8usize;

    let mut query_terms_bitset_table: [u8; ROARING_BLOCK_SIZE] = [0u8; ROARING_BLOCK_SIZE];
    let mut result_count_local = 0;

    query_list.sort_by(|a, b| {
        b.blocks[b.p_block as usize]
            .max_block_score
            .partial_cmp(&a.blocks[a.p_block as usize].max_block_score)
            .unwrap()
    });

    let mut max_score = 0.0;

    for (i, plo) in query_list.iter_mut().take(union_max).enumerate() {
        if plo.end_flag {
            continue;
        }

        plo.p_docid = 0;
        let mask = 1 << i;
        max_score += plo.blocks[plo.p_block as usize].max_block_score;

        if plo.compression_type == CompressionType::Bitmap {
            for ulong_pos in 0u64..1024 {
                let mut intersect: u64 = read_u64(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    ulong_pos as usize * 8,
                );

                while intersect != 0 {
                    let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as u64;
                    intersect = unsafe { _blsr_u64(intersect) };

                    let docid = ((ulong_pos << 6) + bit_pos) as usize;
                    query_terms_bitset_table[docid] |= mask;
                }
            }
        } else if plo.compression_type == CompressionType::Array {
            for i in 0..plo.p_docid_count {
                let docid =
                    read_u16(&plo.byte_array[plo.compressed_doc_id_range..], i * 2) as usize;

                query_terms_bitset_table[docid] |= mask;
            }
        } else {
            let runs_count = read_u16(&plo.byte_array[plo.compressed_doc_id_range..], 0) as i32;

            for ii in (1..(runs_count << 1) + 1).step_by(2) {
                let startdocid = read_u16(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    ii as usize * 2,
                ) as usize;
                let runlength = read_u16(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    (ii + 1) as usize * 2,
                ) as usize;

                for j in 0..=runlength {
                    let docid = startdocid + j;

                    query_terms_bitset_table[docid] |= mask;
                }
            }
        }
    }

    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        if plo.compression_type == CompressionType::Bitmap {
            for ulong_pos in 0u64..1024 {
                let mut intersect: u64 = read_u64(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    ulong_pos as usize * 8,
                );

                while intersect != 0 {
                    let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as u64;
                    intersect = unsafe { _blsr_u64(intersect) };

                    let docid = ((ulong_pos << 6) + bit_pos) as usize;
                    query_terms_bitset_table[docid] = 0;
                }
            }
        } else if plo.compression_type == CompressionType::Array {
            for i in 0..plo.p_docid_count {
                let docid =
                    read_u16(&plo.byte_array[plo.compressed_doc_id_range..], i * 2) as usize;

                query_terms_bitset_table[docid] = 0;
            }
        } else {
            let runs_count = read_u16(&plo.byte_array[plo.compressed_doc_id_range..], 0) as i32;

            for ii in (1..(runs_count << 1) + 1).step_by(2) {
                let startdocid = read_u16(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    ii as usize * 2,
                ) as usize;
                let runlength = read_u16(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    (ii + 1) as usize * 2,
                ) as usize;

                for j in 0..=runlength {
                    let docid = startdocid + j;

                    query_terms_bitset_table[docid] = 0;
                }
            }
        }
    }

    let block_skip = search_result.topk_candidates.current_heap_size >= top_k
        && search_result.topk_candidates.can_prune(max_score, block_id << 16, top_k)
        && search_result.topk_candidates.result_sort.is_empty();

    let query_list_len = cmp::min(query_list.len(), union_max);

    let query_combination_count = 1 << query_list_len;
    let mut query_terms_max_score_sum_table: Vec<f32> = vec![0.0; query_combination_count];
    for (i, max_score) in query_terms_max_score_sum_table.iter_mut().enumerate() {
        for (j, term) in query_list.iter().enumerate().take(query_list_len) {
            if ((1 << j) & i) > 0 {
                *max_score += term.blocks[term.p_block as usize].max_block_score
            }
        }
    }

    // A full block consumes 65,536 postings, including its one-past-end cursor.
    let mut p_docid_array = vec![0usize; union_max];

    let mut _result_count = 0;
    let block_id_msb = block_id << 16;

    for (i, query_terms_bitset) in query_terms_bitset_table.iter().enumerate() {
        if *query_terms_bitset > 0 {
            result_count_local += 1;

            if !block_skip
                && (search_result.topk_candidates.current_heap_size < top_k
                    || !search_result.topk_candidates.can_prune(
                        query_terms_max_score_sum_table[*query_terms_bitset as usize], block_id_msb | i, top_k))
            {
                for (j, query_term) in query_list.iter_mut().take(query_list_len).enumerate() {
                    query_term.bm25_flag = (query_terms_bitset & (1 << j)) > 0;

                    query_term.p_docid = p_docid_array[j];
                }

                add_result_multiterm_multifield(
                    shard,
                    block_id_msb | i,
                    &mut _result_count,
                    search_result,
                    top_k,
                    result_type,
                    field_filter_set,
                    facet_filter,
                    non_unique_query_list,
                    query_list,
                    not_query_list,
                    false,
                    f32::MAX,
                    false,
                );
            }

            if !block_skip {
                for (j, item) in p_docid_array.iter_mut().take(query_list_len).enumerate() {
                    *item += ((query_terms_bitset >> j) & 1) as usize
                }
            }
        }
    }

    *result_count += result_count_local;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn union_scan_32<'a>(
    shard: &'a Shard,
    non_unique_query_list: &mut [NonUniquePostingListObjectQuery<'a>],
    query_list: &mut [PostingListObjectQuery<'a>],
    not_query_list: &mut [PostingListObjectQuery<'a>],
    block_id: usize,
    result_count: &mut i32,
    search_result: &mut SearchResult<'_>,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
) {
    let union_max = 32usize;

    let mut query_terms_bitset_table: [u32; ROARING_BLOCK_SIZE] = [0u32; ROARING_BLOCK_SIZE];
    let mut result_count_local = 0;

    query_list.sort_by(|a, b| {
        b.blocks[b.p_block as usize]
            .max_block_score
            .partial_cmp(&a.blocks[a.p_block as usize].max_block_score)
            .unwrap()
    });

    let mut max_score = 0.0;
    let mut mask = u32::MAX >> (union_max - cmp::min(query_list.len(), union_max));
    // A partial heap has no valid top-k score threshold: its minimum can
    // fall as new candidates arrive. Never freeze a mask from that value.
    // Explicit sorts/browsing also cannot use this relevance-only mask.
    if top_k > 0
        && search_result.topk_candidates.current_heap_size >= top_k
        && search_result.topk_candidates.result_sort.is_empty()
        && !search_result.topk_candidates.empty_query
        && search_result.topk_candidates._elements[0].score.is_finite()
    {
        for plo in query_list.iter_mut().take(union_max).rev() {
            if plo.end_flag {
                continue;
            }
            max_score += plo.blocks[plo.p_block as usize].max_block_score;

            if max_score >= search_result.topk_candidates._elements[0].score {
                break;
            }

            mask >>= 1;
        }
    }

    let mut max_score = 0.0;

    for (i, plo) in query_list.iter_mut().take(union_max).enumerate() {
        if plo.end_flag {
            continue;
        }

        plo.p_docid = 0;
        let mask = 1 << i;
        max_score += plo.blocks[plo.p_block as usize].max_block_score;

        if plo.compression_type == CompressionType::Bitmap {
            for ulong_pos in 0u64..1024 {
                let mut intersect: u64 = read_u64(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    ulong_pos as usize * 8,
                );

                while intersect != 0 {
                    let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as u64;
                    intersect = unsafe { _blsr_u64(intersect) };

                    let docid = ((ulong_pos << 6) + bit_pos) as usize;
                    query_terms_bitset_table[docid] |= mask;
                }
            }
        } else if plo.compression_type == CompressionType::Array {
            for i in 0..plo.p_docid_count {
                let docid =
                    read_u16(&plo.byte_array[plo.compressed_doc_id_range..], i * 2) as usize;

                query_terms_bitset_table[docid] |= mask;
            }
        } else {
            let runs_count = read_u16(&plo.byte_array[plo.compressed_doc_id_range..], 0) as i32;

            for ii in (1..(runs_count << 1) + 1).step_by(2) {
                let startdocid = read_u16(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    ii as usize * 2,
                ) as usize;
                let runlength = read_u16(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    (ii + 1) as usize * 2,
                ) as usize;

                for j in 0..=runlength {
                    let docid = startdocid + j;

                    query_terms_bitset_table[docid] |= mask;
                }
            }
        }
    }

    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        if plo.compression_type == CompressionType::Bitmap {
            for ulong_pos in 0u64..1024 {
                let mut intersect: u64 = read_u64(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    ulong_pos as usize * 8,
                );

                while intersect != 0 {
                    let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as u64;
                    intersect = unsafe { _blsr_u64(intersect) };

                    let docid = ((ulong_pos << 6) + bit_pos) as usize;
                    query_terms_bitset_table[docid] = 0;
                }
            }
        } else if plo.compression_type == CompressionType::Array {
            for i in 0..plo.p_docid_count {
                let docid =
                    read_u16(&plo.byte_array[plo.compressed_doc_id_range..], i * 2) as usize;

                query_terms_bitset_table[docid] = 0;
            }
        } else {
            let runs_count = read_u16(&plo.byte_array[plo.compressed_doc_id_range..], 0) as i32;

            for ii in (1..(runs_count << 1) + 1).step_by(2) {
                let startdocid = read_u16(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    ii as usize * 2,
                ) as usize;
                let runlength = read_u16(
                    &plo.byte_array[plo.compressed_doc_id_range..],
                    (ii + 1) as usize * 2,
                ) as usize;

                for j in 0..=runlength {
                    let docid = startdocid + j;

                    query_terms_bitset_table[docid] = 0;
                }
            }
        }
    }

    let block_skip = search_result.topk_candidates.current_heap_size >= top_k
        && search_result.topk_candidates.can_prune(max_score, block_id << 16, top_k)
        && search_result.topk_candidates.result_sort.is_empty();

    let query_list_len = cmp::min(query_list.len(), union_max);

    // A full block consumes 65,536 postings, including its one-past-end cursor.
    let mut p_docid_array = vec![0usize; union_max];

    let mut _result_count = 0;
    let block_id_msb = block_id << 16;

    for (i, query_terms_bitset) in query_terms_bitset_table.iter().enumerate() {
        if *query_terms_bitset > 0 {
            result_count_local += 1;

            if !block_skip
                && (search_result.topk_candidates.current_heap_size < top_k
                    || query_terms_bitset & mask > 0)
            {
                let mut query_terms_max_score_sum = 0f32;
                for (j, plo) in query_list.iter().take(query_list_len).enumerate() {
                    if (query_terms_bitset & (1 << j)) > 0 {
                        query_terms_max_score_sum +=
                            plo.blocks[plo.p_block as usize].max_block_score;
                    }
                }
                if search_result.topk_candidates.current_heap_size < top_k
                    || !search_result.topk_candidates.can_prune(query_terms_max_score_sum, block_id_msb | i, top_k)
                {
                    for (j, query_term) in query_list.iter_mut().take(query_list_len).enumerate() {
                        query_term.bm25_flag = (query_terms_bitset & (1 << j)) > 0;

                        query_term.p_docid = p_docid_array[j];
                    }

                    add_result_multiterm_multifield(
                        shard,
                        block_id_msb | i,
                        &mut _result_count,
                        search_result,
                        top_k,
                        result_type,
                        field_filter_set,
                        facet_filter,
                        non_unique_query_list,
                        query_list,
                        not_query_list,
                        false,
                        f32::MAX,
                        false,
                    );
                }
            }

            if !block_skip {
                for (j, item) in p_docid_array.iter_mut().take(query_list_len).enumerate() {
                    *item += ((query_terms_bitset >> j) & 1) as usize
                }
            }

            continue;
        }
    }

    *result_count += result_count_local;
}

pub(crate) async fn union_count<'a>(
    shard: &'a Shard,
    result_count: &mut i32,
    _search_result: &mut SearchResult<'_>,

    query_list: &mut [PostingListObjectQuery<'a>],
    not_query_list: &mut [PostingListObjectQuery<'a>],
    facet_filter: &[FilterSparse],
    block_id: usize,
) {
    let mut result_count_local = 0u32;

    let mut bitmap_0: [u8; 8192] = [0u8; 8192];
    let mut first_valid = true;

    for plo in query_list.iter_mut() {
        if plo.end_flag {
            continue;
        }
        let is_first = first_valid;
        first_valid = false;

        if plo.compression_type == CompressionType::Bitmap {
            for i in (0..8192).step_by(8) {
                let x1 = read_u64(&bitmap_0, i);
                let x2 = read_u64(&plo.byte_array[plo.compressed_doc_id_range..], i);
                result_count_local += u64::count_ones(!x1 & x2);
                write_u64(x1 | x2, &mut bitmap_0, i);
            }
        } else if plo.compression_type == CompressionType::Array {
            for i in 0..plo.p_docid_count {
                let docid =
                    read_u16(&plo.byte_array[plo.compressed_doc_id_range..], i * 2) as usize;
                let byte_index = docid >> 3;
                let bit_index = docid & 7;

                if bitmap_0[byte_index] & (1 << bit_index) == 0 {
                    bitmap_0[byte_index] |= 1 << bit_index;
                    result_count_local += 1;
                }
            }
        } else {
            let runs_count = read_u16(&plo.byte_array[plo.compressed_doc_id_range..], 0) as i32;

            if is_first {
                for ii in (1..(runs_count << 1) + 1).step_by(2) {
                    let startdocid = read_u16(
                        &plo.byte_array[plo.compressed_doc_id_range..],
                        ii as usize * 2,
                    ) as usize;
                    let runlength = read_u16(
                        &plo.byte_array[plo.compressed_doc_id_range..],
                        (ii + 1) as usize * 2,
                    ) as usize;

                    result_count_local += runlength as u32 + 1;
                    for j in 0..=runlength {
                        let docid = startdocid + j;
                        bitmap_0[docid >> 3] |= 1 << (docid & 7);
                    }
                }
            } else {
                for ii in (1..(runs_count << 1) + 1).step_by(2) {
                    let startdocid = read_u16(
                        &plo.byte_array[plo.compressed_doc_id_range..],
                        ii as usize * 2,
                    ) as usize;
                    let runlength = read_u16(
                        &plo.byte_array[plo.compressed_doc_id_range..],
                        (ii + 1) as usize * 2,
                    ) as usize;

                    for j in 0..=runlength {
                        let docid = startdocid + j;
                        let byte_index = docid >> 3;
                        let bit_index = docid & 7;

                        if bitmap_0[byte_index] & (1 << bit_index) == 0 {
                            bitmap_0[byte_index] |= 1 << bit_index;
                            result_count_local += 1;
                        }
                    }
                }
            }
        }
    }

    for plo in not_query_list.iter_mut() {
        if !plo.bm25_flag {
            continue;
        }

        match plo.compression_type {
            CompressionType::Array => {
                for i in 0..plo.p_docid_count {
                    let docid =
                        read_u16(&plo.byte_array[plo.compressed_doc_id_range..], i * 2) as usize;
                    let byte_index = docid >> 3;
                    let bit_index = docid & 7;
                    if bitmap_0[byte_index] & (1 << bit_index) != 0 {
                        bitmap_0[byte_index] &= !(1 << bit_index);
                        result_count_local -= 1;
                    }
                }
            }

            CompressionType::Rle => {
                let runs_count = read_u16(&plo.byte_array[plo.compressed_doc_id_range..], 0) as i32;

                for i in (1..(runs_count << 1) + 1).step_by(2) {
                    let startdocid = read_u16(
                        &plo.byte_array[plo.compressed_doc_id_range..],
                        i as usize * 2,
                    );
                    let runlength = read_u16(
                        &plo.byte_array[plo.compressed_doc_id_range..],
                        (i + 1) as usize * 2,
                    );

                    for j in 0..=runlength {
                        let docid = (startdocid + j) as usize;

                        let byte_index = docid >> 3;
                        let bit_index = docid & 7;
                        if bitmap_0[byte_index] & (1 << bit_index) != 0 {
                            bitmap_0[byte_index] &= !(1 << bit_index);
                            result_count_local -= 1;
                        }
                    }
                }
            }

            CompressionType::Bitmap => {
                for i in (0..8192).step_by(8) {
                    let x1 = read_u64(&bitmap_0, i);
                    let x2 = read_u64(&plo.byte_array[plo.compressed_doc_id_range..], i);
                    result_count_local -= u64::count_ones(x1 & x2);
                    write_u64(x1 & !x2, &mut bitmap_0, i);
                }
            }

            _ => {}
        }
    }

    if !shard.delete_hashset.is_empty() {
        for docid in shard.delete_hashset.iter() {
            if block_id == docid >> 16 {
                let byte_index = (docid >> 3) & 8191;
                let bit_mask = 1 << (docid & 7);
                if bitmap_0[byte_index] & bit_mask > 0 {
                    bitmap_0[byte_index] &= !bit_mask;
                    result_count_local -= 1;
                }
            }
        }
    }

    if !facet_filter.is_empty() {
        let block_id_msb = block_id << 16;
        for ulong_pos in 0usize..1024 {
            let ulong_pos_msb = block_id_msb | ulong_pos << 6;
            let mut intersect = read_u64(&bitmap_0, ulong_pos * 8);
            'next: while intersect != 0 {
                let bit_pos = unsafe { _mm_tzcnt_64(intersect) } as usize;
                intersect = unsafe { _blsr_u64(intersect) };

                let docid = ulong_pos_msb | bit_pos;

                if !facet_filter.is_empty() && is_facet_filter(shard, facet_filter, docid) {
                    result_count_local -= 1;
                    continue 'next;
                }
            }
        }
    }

    *result_count += result_count_local as i32;
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::ptr_arg)]
pub(crate) async fn union_docid_2<'a>(
    shard: &'a Shard,
    non_unique_query_list: &mut Vec<NonUniquePostingListObjectQuery<'a>>,
    query_list: &mut Vec<PostingListObjectQuery<'a>>,
    not_query_list: &mut Vec<PostingListObjectQuery<'a>>,
    result_count_arc: &Arc<AtomicUsize>,
    search_result: &mut SearchResult<'_>,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
    matching_blocks: &mut i32,
    query_term_count: usize,
) {
    let filtered = !not_query_list.is_empty()
        || !field_filter_set.is_empty()
        || !shard.delete_hashset.is_empty();
    let mut count = 0;
    if filtered {
        single_blockid(
            shard,
            non_unique_query_list,
            &mut query_list[0..1].to_vec(),
            not_query_list,
            result_count_arc,
            search_result,
            top_k,
            &ResultType::Count,
            field_filter_set,
            facet_filter,
            matching_blocks,
        )
        .await;

        single_blockid(
            shard,
            non_unique_query_list,
            &mut query_list[1..2].to_vec(),
            not_query_list,
            result_count_arc,
            search_result,
            top_k,
            &ResultType::Count,
            field_filter_set,
            facet_filter,
            matching_blocks,
        )
        .await;

        count = result_count_arc.load(Ordering::Relaxed);
        result_count_arc.store(0, Ordering::Relaxed);
    }

    intersection_blockid(
        shard,
        non_unique_query_list,
        query_list,
        not_query_list,
        result_count_arc,
        search_result,
        top_k,
        result_type,
        field_filter_set,
        facet_filter,
        matching_blocks,
        false,
        query_term_count,
    )
    .await;

    let mut result_count_local = if filtered {
        count
    } else {
        (query_list[0].posting_count + query_list[1].posting_count) as usize
    };
    let result_count_global = result_count_arc.load(Ordering::Relaxed);
    if result_count_local > result_count_global {
        result_count_local -= result_count_global
    }

    if result_type == &ResultType::Count {
        result_count_arc.store(result_count_local, Ordering::Relaxed);
        return;
    }

    if (search_result.topk_candidates.current_heap_size < top_k)
        || (query_list[0].max_list_score >= search_result.topk_candidates._elements[0].score)
    {
        search_result.topk_candidates.pin_heap();

        single_blockid(
            shard,
            non_unique_query_list,
            &mut query_list[0..1].to_vec(),
            not_query_list,
            result_count_arc,
            search_result,
            top_k,
            &ResultType::Topk,
            field_filter_set,
            facet_filter,
            matching_blocks,
        )
        .await;
    }

    if (search_result.topk_candidates.current_heap_size < top_k)
        || (query_list[1].max_list_score >= search_result.topk_candidates._elements[0].score)
    {
        search_result.topk_candidates.pin_heap();

        single_blockid(
            shard,
            non_unique_query_list,
            &mut query_list[1..2].to_vec(),
            not_query_list,
            result_count_arc,
            search_result,
            top_k,
            &ResultType::Topk,
            field_filter_set,
            facet_filter,
            matching_blocks,
        )
        .await;
    }

    result_count_arc.store(result_count_local, Ordering::Relaxed);
}

#[allow(clippy::too_many_arguments)]
#[async_recursion]
pub(crate) async fn union_docid_3<'a>(
    shard: &'a Shard,
    non_unique_query_list: &mut Vec<NonUniquePostingListObjectQuery<'a>>,
    query_queue: &'a mut Vec<QueueObject<'a>>,
    not_query_list: &mut Vec<PostingListObjectQuery<'a>>,

    result_count_arc: &Arc<AtomicUsize>,
    search_result: &mut SearchResult,
    top_k: usize,
    result_type: &ResultType,
    field_filter_set: &AHashSet<u16>,
    facet_filter: &[FilterSparse],
    matching_blocks: &mut i32,
    recursion_count: usize,
    query_term_count: usize,
    empty_streak: usize,
) {
    let queue_object = query_queue.remove(0);

    let mut query_list = queue_object.query_list;

    if result_type == &ResultType::Topk || result_type == &ResultType::TopkCount {
        let mut streak = empty_streak;
        const EMPTY_STREAK_LIMIT: usize = 5;
        let sparse_threshold = shard.indexed_doc_count / 128;
        if recursion_count == 0
            && query_list
                .iter()
                .all(|plo| (plo.posting_count as usize) < sparse_threshold)
        {
            union_blockid(
                shard,
                non_unique_query_list,
                &mut query_list,
                not_query_list,
                result_count_arc,
                search_result,
                top_k,
                &ResultType::Topk,
                field_filter_set,
                facet_filter,
            )
            .await;
            for plo in query_list.iter_mut() {
                plo.p_block = 0;
                plo.end_flag_block = false;
            }
        } else if query_list.len() >= 3 {
            let (heap_size_before, heap_min_before) = search_result.topk_candidates.heap_progress();
            // Child query_index partitions assume the inherited term order.
            // Intersection reorders lists by per-block codec/frequency; keep
            // that execution order separate from the subset enumeration order.
            let mut intersection_query_list = query_list.clone();
            intersection_blockid(
                shard,
                non_unique_query_list,
                &mut intersection_query_list,
                not_query_list,
                result_count_arc,
                search_result,
                top_k,
                &ResultType::Topk,
                field_filter_set,
                facet_filter,
                matching_blocks,
                false,
                query_term_count,
            )
            .await;

            {
                let (heap_size_after, heap_min_after) =
                    search_result.topk_candidates.heap_progress();
                let productive = heap_size_after > heap_size_before
                    || (heap_size_after > 0 && search_result.topk_candidates.result_ordering_shard(heap_min_after, heap_min_before).is_gt());
                streak = if productive { 0 } else { empty_streak + 1 };
            }

            search_result.topk_candidates.pin_heap();

            {
                for i in queue_object.query_index..query_list.len() {
                    let ii = query_list.len() - 1 - i;

                    for plo in query_list.iter_mut() {
                        plo.p_block = 0;
                    }

                    let list = if ii == 0 {
                        query_list[1..query_list.len()].to_vec()
                    } else if ii == query_list.len() - 1 {
                        query_list[0..query_list.len() - 1].to_vec()
                    } else {
                        [&query_list[0..ii], &query_list[ii + 1..query_list.len()]].concat()
                    };

                    let mut max_score = 0.0;
                    for term in list.iter() {
                        max_score += term.max_list_score;
                    }

                    if search_result.topk_candidates.current_heap_size < top_k
                        || max_score >= search_result.topk_candidates._elements[0].score
                    {
                        if !query_queue.is_empty()
                            && max_score > query_queue[query_queue.len() - 1].max_score
                        {
                            let pos = query_queue
                                .binary_search_by(|e| {
                                    e.max_score
                                        .partial_cmp(&max_score)
                                        .expect("Couldn't compare values")
                                        .reverse()
                                })
                                .unwrap_or_else(|e| e);
                            query_queue.insert(
                                pos,
                                QueueObject {
                                    query_list: list,
                                    query_index: i,
                                    max_score,
                                },
                            );
                        } else {
                            query_queue.push(QueueObject {
                                query_list: list,
                                query_index: i,
                                max_score,
                            });
                        }
                    };
                }
            }
        } else {
            union_docid_2(
                shard,
                non_unique_query_list,
                &mut query_list,
                not_query_list,
                result_count_arc,
                search_result,
                top_k,
                &ResultType::Topk,
                field_filter_set,
                facet_filter,
                matching_blocks,
                query_term_count,
            )
            .await;
        }

        if !query_queue.is_empty()
            && (search_result.topk_candidates.current_heap_size < top_k
                || query_queue.first().unwrap().max_score
                    >= search_result.topk_candidates._elements[0].score)
        {
            search_result.topk_candidates.pin_heap();

            if recursion_count < 200 && streak < EMPTY_STREAK_LIMIT {
                union_docid_3(
                    shard,
                    non_unique_query_list,
                    query_queue,
                    not_query_list,
                    result_count_arc,
                    search_result,
                    top_k,
                    &ResultType::Topk,
                    field_filter_set,
                    facet_filter,
                    matching_blocks,
                    recursion_count + 1,
                    query_term_count,
                    streak,
                )
                .await;
            } else {
                search_result.topk_candidates.pin_heap();

                let mut merged: Vec<PostingListObjectQuery> = Vec::new();
                for pending in query_queue.iter() {
                    if search_result.topk_candidates.current_heap_size >= top_k
                        && pending.max_score < search_result.topk_candidates._elements[0].score
                    {
                        continue;
                    }
                    for term in pending.query_list.iter() {
                        if !merged
                            .iter()
                            .any(|m| m.term_index_unique == term.term_index_unique)
                        {
                            merged.push(term.clone());
                        }
                    }
                }

                if !merged.is_empty() {
                    union_blockid(
                        shard,
                        non_unique_query_list,
                        &mut merged,
                        not_query_list,
                        result_count_arc,
                        search_result,
                        top_k,
                        &ResultType::Topk,
                        field_filter_set,
                        facet_filter,
                    )
                    .await;
                }
            }
        }
    }

    if result_type == &ResultType::Count || result_type == &ResultType::TopkCount {
        for plo in query_list.iter_mut() {
            plo.p_block = 0;
        }

        result_count_arc.store(0, Ordering::Relaxed);

        union_blockid(
            shard,
            non_unique_query_list,
            &mut query_list,
            not_query_list,
            result_count_arc,
            search_result,
            top_k,
            &ResultType::Count,
            field_filter_set,
            facet_filter,
        )
        .await;
    }
}
