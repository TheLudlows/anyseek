// storm-db Index: main entry point for create/open/close lifecycle, commit, and search.

use std::sync::Arc;
use std::sync::RwLock;
use ahash::{AHashMap, AHashSet};
use indexmap::IndexMap;
use object_store::ObjectStore;

use crate::clustering::{cluster_vectors, Medoid};
use crate::compress::compress_postinglist;
use crate::embed::EmbeddingModel;
use crate::error::{Result, StormDbError};
use crate::facet::compute_facet_row_size;
use crate::kv_store::{make_key, KvStore, PREFIX_DELL, PREFIX_FACET, PREFIX_IDX, PREFIX_META, PREFIX_VEC};
use crate::posting::{PostingListObject0, PostingListObjectIndex, SegmentIndex, SegmentLevel0};
use crate::quantize::quantize_i8;
use crate::ranking::{compute_idf, MinHeap, SearchResult};
use crate::schema::{IndexConfig, SchemaField, SchemaMap};
use crate::search::{search_intersection, search_phrase, search_union, AnnMode, QueryType, SearchMode, SearchRequest, SearchResponse};
use crate::tokenizer::{tokenize_index, tokenize_query};
use crate::vector::{serialize_cluster, vector_search_level, VectorLevel};
use crate::posting::QueryTypeTerm;

// ---------------------------------------------------------------------------
// Index struct
// ---------------------------------------------------------------------------

/// The main index struct — holds all in-memory state for a storm-db index.
pub struct Index {
    /// The underlying KV store (SlateDB).
    pub db: KvStore,
    /// Field name → SchemaField mapping.
    pub schema: SchemaMap,
    /// Index configuration.
    pub config: IndexConfig,
    /// Runtime statistics.
    pub stats: RwLock<IndexStats>,
    /// Committed posting lists organized by segment (level → key_hash → posting list).
    /// `segments_index.len()` is the current level count.
    pub segments_index: RwLock<Vec<SegmentIndex>>,
    /// Uncommitted (level-0) posting list buffer.
    pub segments_level0: RwLock<SegmentLevel0>,
    /// Committed vector levels.
    pub vector_levels: RwLock<Vec<VectorLevel>>,
    /// Uncommitted vector buffer.
    pub vector_buffer: RwLock<Vec<crate::clustering::ParentMedoid>>,
    /// Deleted document IDs (global local doc_ids, keyed by level<<16 | local_docid).
    pub delete_set: RwLock<AHashSet<usize>>,
    /// Facet value dictionary: field → (sorted_values, counts).
    pub facet_dict: RwLock<IndexMap<String, (Vec<String>, usize)>>,
    /// Embedding model for auto-vectorization.
    pub embedding_model: EmbeddingModel,
    /// Total number of indexed documents (committed).
    pub indexed_doc_count: RwLock<u64>,
    /// Total number of indexed vectors (committed).
    pub indexed_vector_count: RwLock<u64>,
    /// Document counts per level (for computing avg doc length).
    pub level_doc_counts: RwLock<Vec<u64>>,
    /// Total byte size of a facet row for this schema.
    pub facet_row_size: usize,
    /// Pending documents buffer (indexed but not yet committed).
    pub pending_docs: RwLock<crate::facet::PendingDocuments>,
    /// Next available local doc_id in the current level-0 segment.
    next_doc_id: RwLock<u16>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct IndexStats {
    pub doc_count: u64,
    pub vector_count: u64,
    pub level_count: usize,
}

// ---------------------------------------------------------------------------
// Lifecycle: create_index
// ---------------------------------------------------------------------------

/// Create a new index at the given path.
///
/// Writes the schema and config to SlateDB, then returns an empty Index.
pub async fn create_index(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let db = KvStore::open(path, object_store).await?;

    // Build the SchemaMap.
    let mut schema: SchemaMap = SchemaMap::new();
    for field in &schema_fields {
        schema.insert(field.field.clone(), field.clone());
    }

    // Compute facet row size.
    let facet_row_size = compute_facet_row_size(&schema_fields);

    // Write metadata to SlateDB.
    db.put_serialized(&make_key(&[PREFIX_META, b"schema"]), &schema).await?;
    db.put_serialized(&make_key(&[PREFIX_META, b"config"]), &config).await?;
    db.put_serialized(&make_key(&[PREFIX_META, b"stats"]), &IndexStats::default()).await?;

    let index = Index {
        db,
        schema,
        config,
        stats: RwLock::new(IndexStats::default()),
        segments_index: RwLock::new(Vec::new()),
        segments_level0: RwLock::new(SegmentLevel0 {
            segment: AHashMap::new(),
        }),
        vector_levels: RwLock::new(Vec::new()),
        vector_buffer: RwLock::new(Vec::new()),
        delete_set: RwLock::new(AHashSet::new()),
        facet_dict: RwLock::new(IndexMap::new()),
        embedding_model: EmbeddingModel::None,
        indexed_doc_count: RwLock::new(0),
        indexed_vector_count: RwLock::new(0),
        level_doc_counts: RwLock::new(Vec::new()),
        facet_row_size,
        pending_docs: RwLock::new(crate::facet::PendingDocuments::default()),
        next_doc_id: RwLock::new(0),
    };

    Ok(index)
}

// ---------------------------------------------------------------------------
// Lifecycle: open_index
// ---------------------------------------------------------------------------

/// Open an existing index at the given path.
///
/// Reads metadata from SlateDB, then loads all committed posting lists,
// vector levels, delete set, and facet dictionary.
pub async fn open_index(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Index> {
    let db = KvStore::open(path, object_store).await?;

    // Read metadata.
    let schema: SchemaMap = db
        .get_deserialized(&make_key(&[PREFIX_META, b"schema"]))
        .await?
        .ok_or_else(|| StormDbError::IndexNotFound("schema not found".into()))?;

    let config: IndexConfig = db
        .get_deserialized(&make_key(&[PREFIX_META, b"config"]))
        .await?
        .ok_or_else(|| StormDbError::IndexNotFound("config not found".into()))?;

    let stats: IndexStats = db
        .get_deserialized(&make_key(&[PREFIX_META, b"stats"]))
        .await?
        .unwrap_or_default();

    let facet_row_size = compute_facet_row_size(
        &schema.values().cloned().collect::<Vec<_>>(),
    );

    // Load committed posting lists (scan "i:*").
    let mut segments_index: Vec<SegmentIndex> = Vec::new();
    let idx_entries = db.scan_prefix(PREFIX_IDX).await?;
    for (key_bytes, value_bytes) in idx_entries {
        // Key format: "i:{level}:{strip}:{key_hash_bytes}"
        let key_vec: Vec<u8> = key_bytes.to_vec();
        if key_vec.len() < 5 {
            continue;
        }
        // Skip prefix "i:" (2 bytes), parse level, strip, key_hash
        let rest = &key_vec[2..];
        let level = if let Some(pos) = rest.iter().position(|&b| b == b':') {
            let level_str = std::str::from_utf8(&rest[..pos]).ok();
            level_str.and_then(|s| s.parse::<usize>().ok()).unwrap_or(0)
        } else {
            0
        };

        // Ensure segments_index has enough levels.
        while segments_index.len() <= level {
            segments_index.push(SegmentIndex {
                segment: AHashMap::new(),
                byte_array_blocks: Vec::new(),
            });
        }

        // Parse key_hash from the remainder after level and strip segments.
        // Format: "i:{level}:{strip}:{key_hash}"
        let parts: Vec<&[u8]> = rest.split(|&b| b == b':').collect();
        if parts.len() < 3 {
            continue;
        }
        let key_hash_str = std::str::from_utf8(parts[2]).ok().unwrap_or("");
        let key_hash: u64 = key_hash_str.parse().unwrap_or(0);

        // Deserialize posting list.
        let pl = PostingListObjectIndex::deserialize(&value_bytes);
        segments_index[level]
            .segment
            .insert(key_hash, pl);
    }

    // Load vector levels (scan "v:*").
    let mut vector_levels: Vec<VectorLevel> = Vec::new();
    let vec_entries = db.scan_prefix(PREFIX_VEC).await?;
    for (key_bytes, value_bytes) in vec_entries {
        let key_vec: Vec<u8> = key_bytes.to_vec();
        if key_vec.len() < 5 {
            continue;
        }
        // Format: "v:{level}:cluster:{id}" or "v:{level}:medoids"
        let rest = &key_vec[2..];
        let parts: Vec<&str> = rest
            .split(|&b| b == b':')
            .filter_map(|s| std::str::from_utf8(s).ok())
            .collect();

        if parts.len() < 2 {
            continue;
        }
        let level: usize = parts[0].parse().unwrap_or(0);

        while vector_levels.len() <= level {
            vector_levels.push(VectorLevel {
                medoids: Vec::new(),
                clusters: Vec::new(),
                cluster_headers: Vec::new(),
            });
        }

        if parts[1] == "medoids" {
            // Medoid list: JSON serialized Vec<Medoid>.
            if let Ok(medoids) = serde_json::from_slice::<Vec<Medoid>>(&value_bytes) {
                vector_levels[level].medoids = medoids;
            }
        } else if parts[1] == "cluster" && parts.len() >= 3 {
            // Cluster data: raw bytes.
            let cluster_id: usize = parts[2].parse().unwrap_or(0);
            while vector_levels[level].clusters.len() <= cluster_id {
                vector_levels[level].clusters.push(Vec::new());
            }
            vector_levels[level].clusters[cluster_id] = value_bytes.to_vec();
        }
    }

    // Load delete set (scan "l:*").
    let mut delete_set = AHashSet::new();
    let dell_entries = db.scan_prefix(PREFIX_DELL).await?;
    for (key_bytes, _value_bytes) in dell_entries {
        let key_vec: Vec<u8> = key_bytes.to_vec();
        // Key format: "l:{doc_id}" - skip "l:" prefix (2 bytes), parse the rest.
        if key_vec.len() > 2 {
            let id_str = std::str::from_utf8(&key_vec[2..]).ok().unwrap_or("");
            if let Ok(id) = id_str.parse::<usize>() {
                delete_set.insert(id);
            }
        }
    }

    // Load facet dictionary ("f:dict").
    let facet_dict: IndexMap<String, (Vec<String>, usize)> = db
        .get_deserialized(&make_key(&[PREFIX_FACET, b"dict"]))
        .await?
        .unwrap_or_default();

    // Compute level doc counts from segments_index.
    let level_doc_counts: Vec<u64> = segments_index
        .iter()
        .map(|seg| {
            seg.segment
                .values()
                .map(|pl| pl.posting_count as u64)
                .sum()
        })
        .collect();

    let indexed_doc_count: u64 = level_doc_counts.iter().sum();
    let indexed_vector_count = stats.vector_count;

    Ok(Index {
        db,
        schema,
        config,
        stats: RwLock::new(stats.clone()),
        segments_index: RwLock::new(segments_index),
        segments_level0: RwLock::new(SegmentLevel0 {
            segment: AHashMap::new(),
        }),
        vector_levels: RwLock::new(vector_levels),
        vector_buffer: RwLock::new(Vec::new()),
        delete_set: RwLock::new(delete_set),
        facet_dict: RwLock::new(facet_dict),
        embedding_model: EmbeddingModel::None,
        indexed_doc_count: RwLock::new(indexed_doc_count),
        indexed_vector_count: RwLock::new(indexed_vector_count),
        level_doc_counts: RwLock::new(level_doc_counts),
        facet_row_size,
        pending_docs: RwLock::new(crate::facet::PendingDocuments::default()),
        next_doc_id: RwLock::new(0),
    })
}

// ---------------------------------------------------------------------------
// Lifecycle: close_index
// ---------------------------------------------------------------------------

/// Close the index, flushing any pending writes.
pub async fn close_index(index: &Index) -> Result<()> {
    index.db.close().await
}

// ---------------------------------------------------------------------------
// Document addition (low-level helpers)
// ---------------------------------------------------------------------------

impl Index {
    /// Add a document's text fields to the level-0 posting list buffer.
    ///
    /// - `local_doc_id`: the document's local id within the current level-0 segment.
    /// - `field_id`: the field index in the schema.
    /// - `text`: the field text to tokenize and index.
    pub fn add_text_to_level0(&self, local_doc_id: u16, field_id: u16, text: &str) {
        let segment_mask = (1u32 << self.config.segment_number_bits) - 1;
        let terms = tokenize_index(text, field_id, &self.config, segment_mask);

        let mut level0 = self.segments_level0.write().unwrap();
        for term in terms {
            let entry = level0
                .segment
                .entry(term.key_hash)
                .or_insert_with(|| PostingListObject0 {
                    docids: Vec::new(),
                    positions: Vec::new(),
                    posting_count: 0,
                    max_block_score: 0.0,
                    max_docid: 0,
                    max_p_docid: 0,
                });

            entry.docids.push(local_doc_id);
            entry.positions.push(
                term.field_positions_vec
                    .first()
                    .cloned()
                    .unwrap_or_default(),
            );
            entry.posting_count += 1;
            entry.max_docid = entry.max_docid.max(local_doc_id);
            entry.max_p_docid = entry.max_p_docid.max(local_doc_id);
            entry.max_block_score = entry.max_block_score.max(1.0_f32); // simplified score
        }
    }

    /// Add a vector to the level-0 vector buffer.
    ///
    /// - `local_doc_id`: the document's local id within the current level-0 segment.
    /// - `field_id`: the field index in the schema.
    /// - `embedding`: the vector values.
    pub fn add_vector_to_buffer(
        &self,
        local_doc_id: u16,
        field_id: u32,
        embedding: Vec<f32>,
    ) {
        let quantized = quantize_i8(&embedding);
        let norm = embedding.iter().map(|v| v * v).sum::<f32>().sqrt();

        let mut buffer = self.vector_buffer.write().unwrap();
        buffer.push(crate::clustering::ParentMedoid {
            medoid_index: 0,
            is_medoid: false,
            similarity: 1.0,
            doc_id: local_doc_id,
            field_id,
            chunk_id: 0,
            embedding,
            scale: quantized.scale,
            norm,
            zero_point: quantized.zero_point,
            sum_q: quantized.sum_q,
        });
    }

    /// Allocate the next available local doc_id in level 0.
    /// Returns the allocated doc_id.
    pub fn allocate_doc_id(&self) -> u16 {
        let mut next = self.next_doc_id.write().unwrap();
        let id = *next;
        *next = next.wrapping_add(1);
        id
    }

    /// Delete a document by its local doc_id.
    ///
    /// Adds the doc_id to the global delete set. The `global_doc_id` is
    /// `((level as u64) << 16) | (local_doc_id as u64)`.
    pub fn delete_document(&self, global_doc_id: usize) {
        let mut del_set = self.delete_set.write().unwrap();
        del_set.insert(global_doc_id);
    }

    /// Compute the average document length across all committed levels.
    pub fn avg_doc_length(&self) -> f32 {
        let level_doc_counts = self.level_doc_counts.read().unwrap();
        let total: u64 = level_doc_counts.iter().sum::<u64>();
        if total == 0 {
            return 100.0; // default average
        }
        let level0_count = self.segments_level0.read().unwrap().segment.values().map(|e| e.posting_count as u64).sum::<u64>();
        let total_all = total + level0_count;
        // Approximate: assume average doc length = 100 tokens when no data.
        if total_all == 0 {
            return 100.0;
        }
        // This is a simplification; in production we'd track total tokens.
        (total_all as f32).max(1.0)
    }
}

// ---------------------------------------------------------------------------
// Commit
// ---------------------------------------------------------------------------

/// Commit level-0 data to persistent storage.
///
/// This is the core write path. It:
/// 1. Flushes the level-0 posting lists to SlateDB (compressed).
/// 2. Flushes vectors (clustering + serialization).
/// 3. Clears level-0 buffers.
/// 4. Updates in-memory segment index.
pub async fn commit(index: &Index) -> Result<()> {
    // Take level-0 data.
    let level0_data = {
        let mut level0 = index.segments_level0.write().unwrap();
        std::mem::take(&mut level0.segment)
    };

    // Take vector buffer.
    let vector_data = {
        let mut buf = index.vector_buffer.write().unwrap();
        std::mem::take(&mut *buf)
    };

    // Always write delete set entries, even if no other data.
    {
        let del_set = index.delete_set.read().unwrap();
        for &doc_id in del_set.iter() {
            let key = format!("l:{}", doc_id);
            index.db.put(key.as_bytes(), b"1").await?;
        }
    }

    // If nothing to index, still update stats.
    if level0_data.is_empty() && vector_data.is_empty() {
        let seg_idx = index.segments_index.read().unwrap();
        let stats = IndexStats {
            doc_count: *index.indexed_doc_count.read().unwrap(),
            vector_count: *index.indexed_vector_count.read().unwrap(),
            level_count: seg_idx.len(),
        };
        drop(seg_idx);
        index.db.put_serialized(&make_key(&[PREFIX_META, b"stats"]), &stats).await?;
        return Ok(());
    }

    let current_level = index.segments_index.read().unwrap().len();
    let segment_number = 1usize << index.config.segment_number_bits;
    let segment_mask = (1u32 << index.config.segment_number_bits) - 1;

    // Count docs in this commit.
    let level0_doc_count: u64 = level0_data
        .values()
        .map(|pl| pl.posting_count as u64)
        .sum();

    // Count vectors before moving.
    let vector_count = vector_data.len() as u64;

    // -------------------------------------------------------------------------
    // Step 1: Write posting lists to SlateDB ("i:{level}:{strip}:{key_hash}")
    // -------------------------------------------------------------------------

    // Group terms by strip.
    let mut strips: Vec<AHashMap<u64, &PostingListObject0>> = vec![
        AHashMap::new();
        segment_number
    ];
    for (&key_hash, posting) in &level0_data {
        let key0 = (key_hash as u32) & segment_mask;
        let strip = key0 as usize % segment_number;
        strips[strip].insert(key_hash, posting);
    }

    for (strip_idx, strip_map) in strips.into_iter().enumerate() {
        if strip_map.is_empty() {
            continue;
        }

        for (key_hash, posting) in strip_map {
            let docids = &posting.docids;
            let positions = &posting.positions;
            let (compressed, _ctype, _pivot, _score) = compress_postinglist(docids, positions);

            // Serialize into a PostingListObjectIndex with a single block.
            let ct_packed = (crate::posting::CompressionType::Array as u32) << 30;
            let block = crate::posting::BlockObjectIndex {
                max_block_score: posting.max_block_score,
                block_id: 0,
                compression_type_pointer: ct_packed,
                posting_count: docids.len() as u16,
                max_docid: posting.max_docid,
                max_p_docid: posting.max_p_docid,
                pointer_pivot_p_docid: posting.max_p_docid,
            };

            let pl_index = PostingListObjectIndex {
                posting_count: docids.len() as u32,
                max_list_score: posting.max_block_score,
                blocks: vec![block],
                byte_array: compressed,
            };

            let key = format!("i:{}:{}:{}", current_level, strip_idx, key_hash);
            index.db.put(key.as_bytes(), &pl_index.serialize()).await?;
        }
    }

    // -------------------------------------------------------------------------
    // Step 2: Write vector data (clustering + serialization)
    // -------------------------------------------------------------------------

    let mut committed_vectors = 0u64;
    if vector_count > 0 {
        let mut vectors_clone = vector_data;
        let medoids = cluster_vectors(
            &mut vectors_clone,
            index.config.clustering,
            index.config.vector_similarity,
        );

        // Group by medoid_index.
        let mut clusters: AHashMap<usize, Vec<crate::clustering::ParentMedoid>> = AHashMap::new();
        let medoids = cluster_vectors(&mut vectors_clone, index.config.clustering, index.config.vector_similarity);

        // Group by medoid_index.
        let mut clusters: AHashMap<usize, Vec<crate::clustering::ParentMedoid>> = AHashMap::new();
        for pm in &vectors_clone {
            clusters
                .entry(pm.medoid_index)
                .or_insert_with(Vec::new)
                .push(pm.clone());
        }

        let mut cluster_data_list: Vec<Vec<u8>> = Vec::new();
        for (medoid_idx, cluster_vecs) in &clusters {
            let serialized = serialize_cluster(cluster_vecs, index.config.vector_dimensions);
            let key = format!("v:{}:cluster:{}", current_level, medoid_idx);
            index.db.put(key.as_bytes(), &serialized).await?;
            cluster_data_list.push(serialized);
        }

        // Write medoid list.
        let medoids_key = format!("v:{}:medoids", current_level);
        index.db.put_serialized(medoids_key.as_bytes(), &medoids).await?;

        // Update in-memory vector level.
        let mut vl = index.vector_levels.write().unwrap();
        while vl.len() <= current_level {
            vl.push(VectorLevel {
                medoids: Vec::new(),
                clusters: Vec::new(),
                cluster_headers: Vec::new(),
            });
        }

        let level = &mut vl[current_level];
        level.medoids = medoids.clone();
        level.clusters = cluster_data_list;
        // Re-count child counts.
        level.cluster_headers = medoids
            .iter()
            .map(|m| crate::vector::ClusterHeader {
                start_index: m.medoid_index as u32,
                child_count: m.child_count as u32,
            })
            .collect();

        committed_vectors = vector_count;
    }

    // -------------------------------------------------------------------------
    // Step 3: Update in-memory segment index
    // -------------------------------------------------------------------------

    {
        let mut seg_idx = index.segments_index.write().unwrap();
        while seg_idx.len() <= current_level {
            seg_idx.push(SegmentIndex {
                segment: AHashMap::new(),
                byte_array_blocks: Vec::new(),
            });
        }

        for (&key_hash, posting) in &level0_data {
            let docids = &posting.docids;
            let positions = &posting.positions;
            let (compressed, _, _, _) = compress_postinglist(docids, positions);

            let ct_packed = (crate::posting::CompressionType::Array as u32) << 30;
            let block = crate::posting::BlockObjectIndex {
                max_block_score: posting.max_block_score,
                block_id: 0,
                compression_type_pointer: ct_packed,
                posting_count: docids.len() as u16,
                max_docid: posting.max_docid,
                max_p_docid: posting.max_p_docid,
                pointer_pivot_p_docid: posting.max_p_docid,
            };

            let pl_index = PostingListObjectIndex {
                posting_count: docids.len() as u32,
                max_list_score: posting.max_block_score,
                blocks: vec![block],
                byte_array: compressed,
            };

            seg_idx[current_level]
                .segment
                .insert(key_hash, pl_index);
        }
    }

    // -------------------------------------------------------------------------
    // Step 5: Update stats
    // -------------------------------------------------------------------------

    {
        let mut level_counts = index.level_doc_counts.write().unwrap();
        while level_counts.len() <= current_level {
            level_counts.push(0);
        }
        level_counts[current_level] += level0_doc_count;
    }

    {
        let mut doc_count = index.indexed_doc_count.write().unwrap();
        *doc_count += level0_doc_count;
    }

    {
        let mut vec_count = index.indexed_vector_count.write().unwrap();
        *vec_count += committed_vectors;
    }

    let seg_idx = index.segments_index.read().unwrap();
    let vec_levels = index.vector_levels.read().unwrap();
    let stats = IndexStats {
        doc_count: *index.indexed_doc_count.read().unwrap(),
        vector_count: *index.indexed_vector_count.read().unwrap(),
        level_count: seg_idx.len(),
    };
    drop(seg_idx);
    drop(vec_levels);

    index.db.put_serialized(&make_key(&[PREFIX_META, b"stats"]), &stats).await?;

    // -------------------------------------------------------------------------
    // Step 6: Clear level-0 buffers
    // -------------------------------------------------------------------------

    {
        let mut level0 = index.segments_level0.write().unwrap();
        *level0 = SegmentLevel0 {
            segment: AHashMap::new(),
        };
    }

    {
        let mut buf = index.vector_buffer.write().unwrap();
        buf.clear();
    }

    // Reset doc_id counter.
    {
        let mut next = index.next_doc_id.write().unwrap();
        *next = 0;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

/// Execute a search request and return a SearchResponse.
pub async fn search(index: &Index, request: SearchRequest) -> Result<SearchResponse> {
    let avg_doc_length = index.avg_doc_length();
    let delete_set = index.delete_set.read().unwrap().clone();
    let segment_mask = (1u32 << index.config.segment_number_bits) - 1;

    let results: Vec<SearchResult> = match &request.search_mode {
        // -----------------------------------------------------------------
        // Lexical search
        // -----------------------------------------------------------------
        SearchMode::Lexical => {
            let query_string = request.query_string.as_ref()
                .ok_or_else(|| StormDbError::InvalidQuery("query_string required for lexical search".into()))?;

            let (unique_map, ordered) = tokenize_query(
                query_string,
                &index.config,
                segment_mask,
                match request.query_type {
                    QueryType::Union => QueryTypeTerm::Union,
                    QueryType::Intersection => QueryTypeTerm::Intersection,
                    QueryType::Phrase => QueryTypeTerm::Phrase,
                },
            );

            if unique_map.is_empty() {
                return Ok(SearchResponse {
                    query: query_string.clone(),
                    result_count: 0,
                    results: vec![],
                });
            }

            // Collect all required posting lists from all levels.
            let seg_idx = index.segments_index.read().unwrap();
            let mut all_posting_lists: Vec<(&PostingListObjectIndex, f32, f32, usize)> = Vec::new();

            for (_key_hash, term_obj) in &unique_map {
                let key0 = term_obj.key0 as usize;
                for (level, segment) in seg_idx.iter().enumerate() {
                    if let Some(pl) = segment.segment.get(&term_obj.key_hash) {
                        let idf = compute_idf(*index.indexed_doc_count.read().unwrap() as u64, pl.posting_count);
                        all_posting_lists.push((pl, term_obj.term.len() as f32, idf, level));
                    }
                }
            }

            if all_posting_lists.is_empty() {
                return Ok(SearchResponse {
                    query: query_string.clone(),
                    result_count: 0,
                    results: vec![],
                });
            }

            // Build posting list references for the search functions.
            let posting_refs: Vec<&PostingListObjectIndex> =
                all_posting_lists.iter().map(|(pl, _, _, _)| *pl).collect();
            let boosts: Vec<f32> = all_posting_lists.iter().map(|(_, boost, _, _)| *boost).collect();
            let idfs: Vec<f32> = all_posting_lists.iter().map(|(_, _, idf, _)| *idf).collect();
            let levels: Vec<usize> = all_posting_lists.iter().map(|(_, _, _, level)| *level).collect();

            let total_docs: usize = posting_refs.iter().enumerate().map(|(i, pl)| {
                // Use first posting list per unique term (simplified).
                // For multi-level, we process each level separately.
                pl.posting_count as usize
            }).sum();

            let capacity = request.length.max(1).min(1000);
            let mut heap = MinHeap::new(capacity);

            // Merge all levels (simplified: just use the most recent level's postings).
            if !posting_refs.is_empty() {
                let level = levels.first().copied().unwrap_or(0);
                match request.query_type {
                    QueryType::Union => {
                        search_union(&posting_refs, &delete_set, &mut heap, avg_doc_length, &boosts, &idfs, level);
                    }
                    QueryType::Intersection => {
                        search_intersection(&posting_refs, &delete_set, &mut heap, avg_doc_length, &boosts, &idfs, level);
                    }
                    QueryType::Phrase => {
                        search_phrase(&posting_refs, &delete_set, &mut heap, avg_doc_length, &boosts, &idfs, level);
                    }
                }
            }

            heap.into_sorted_vec()
        }

        // -----------------------------------------------------------------
        // Vector search
        // -----------------------------------------------------------------
        SearchMode::Vector { ann_mode, similarity_threshold: _ } => {
            let query_vector = request.query_vector.as_ref()
                .ok_or_else(|| StormDbError::InvalidQuery("query_vector required for vector search".into()))?;

            let vec_levels = index.vector_levels.read().unwrap();
            let mut all_results: Vec<SearchResult> = Vec::new();

            for (level_id, level) in vec_levels.iter().enumerate() {
                let level_results = vector_search_level(
                    level,
                    query_vector,
                    ann_mode,
                    index.config.vector_similarity,
                    index.config.vector_dimensions,
                    request.length.max(10),
                    &delete_set,
                    level_id,
                );
                all_results.extend(level_results);
            }

            // Sort by score and take top-k.
            all_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
            all_results.truncate(request.length);
            all_results
        }

        // -----------------------------------------------------------------
        // Hybrid search
        // -----------------------------------------------------------------
        SearchMode::Hybrid { ann_mode, similarity_threshold } => {
            let mut lexical_results: Vec<SearchResult> = Vec::new();
            let mut vector_results: Vec<SearchResult> = Vec::new();

            // Run lexical search inline.
            if let Some(ref query_string) = request.query_string {
                let (unique_map, _ordered) = tokenize_query(
                    query_string,
                    &index.config,
                    segment_mask,
                    match request.query_type {
                        QueryType::Union => QueryTypeTerm::Union,
                        QueryType::Intersection => QueryTypeTerm::Intersection,
                        QueryType::Phrase => QueryTypeTerm::Phrase,
                    },
                );

                if !unique_map.is_empty() {
                    let seg_idx = index.segments_index.read().unwrap();
                    let mut all_posting_lists: Vec<(&PostingListObjectIndex, f32, f32, usize)> = Vec::new();

                    for (_key_hash, term_obj) in &unique_map {
                        for (level, segment) in seg_idx.iter().enumerate() {
                            if let Some(pl) = segment.segment.get(&term_obj.key_hash) {
                                let idf = compute_idf(*index.indexed_doc_count.read().unwrap() as u64, pl.posting_count);
                                all_posting_lists.push((pl, term_obj.term.len() as f32, idf, level));
                            }
                        }
                    }

                    if !all_posting_lists.is_empty() {
                        let posting_refs: Vec<&PostingListObjectIndex> =
                            all_posting_lists.iter().map(|(pl, _, _, _)| *pl).collect();
                        let boosts: Vec<f32> = all_posting_lists.iter().map(|(_, boost, _, _)| *boost).collect();
                        let idfs: Vec<f32> = all_posting_lists.iter().map(|(_, _, idf, _)| *idf).collect();
                        let levels: Vec<usize> = all_posting_lists.iter().map(|(_, _, _, level)| *level).collect();

                        let capacity = request.length.max(1).min(1000);
                        let mut heap = MinHeap::new(capacity);

                        if !posting_refs.is_empty() {
                            let level = levels.first().copied().unwrap_or(0);
                            match request.query_type {
                                QueryType::Union => {
                                    search_union(&posting_refs, &delete_set, &mut heap, avg_doc_length, &boosts, &idfs, level);
                                }
                                QueryType::Intersection => {
                                    search_intersection(&posting_refs, &delete_set, &mut heap, avg_doc_length, &boosts, &idfs, level);
                                }
                                QueryType::Phrase => {
                                    search_phrase(&posting_refs, &delete_set, &mut heap, avg_doc_length, &boosts, &idfs, level);
                                }
                            }
                        }
                        lexical_results = heap.into_sorted_vec();
                    }
                }
            }

            // Run vector search inline.
            if let Some(ref query_vector) = request.query_vector {
                let vec_levels = index.vector_levels.read().unwrap();
                for (level_id, level) in vec_levels.iter().enumerate() {
                    let level_results = vector_search_level(
                        level,
                        query_vector,
                        ann_mode,
                        index.config.vector_similarity,
                        index.config.vector_dimensions,
                        request.length.max(10),
                        &delete_set,
                        level_id,
                    );
                    vector_results.extend(level_results);
                }
                drop(vec_levels);

                vector_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
                vector_results.truncate(request.length);
            }

            crate::hybrid::hybrid_search(lexical_results, vector_results)
        }
    };

    let result_count = results.len();
    let offset = request.offset.min(results.len());
    let length = request.length.min(results.len() - offset);
    let results_slice = &results[offset..][..length];

    Ok(SearchResponse {
        query: request.query_string.clone().unwrap_or_default(),
        result_count,
        results: results_slice.to_vec(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{FieldType, SchemaField};
    use object_store::memory::InMemory;
    
    fn test_config() -> IndexConfig {
        IndexConfig::default()
    }

    fn test_schema() -> Vec<SchemaField> {
        vec![
            SchemaField {
                field: "title".to_string(),
                store: true,
                index_lexical: true,
                index_vector: false,
                field_type: FieldType::Text,
                facet: false,
                boost: 1.0,
            },
            SchemaField {
                field: "body".to_string(),
                store: true,
                index_lexical: true,
                index_vector: false,
                field_type: FieldType::Text,
                facet: false,
                boost: 0.5,
            },
            SchemaField {
                field: "rating".to_string(),
                store: true,
                index_lexical: false,
                index_vector: false,
                field_type: FieldType::F32,
                facet: true,
                boost: 1.0,
            },
        ]
    }

    async fn test_index() -> Index {
        Index {
            db: KvStore::open("/test", Arc::new(InMemory::new())).await.unwrap(),
            schema: test_schema().into_iter().map(|f| (f.field.clone(), f)).collect(),
            config: test_config(),
            stats: RwLock::new(IndexStats::default()),
            segments_index: RwLock::new(Vec::new()),
            segments_level0: RwLock::new(SegmentLevel0 {
                segment: AHashMap::new(),
            }),
            vector_levels: RwLock::new(Vec::new()),
            vector_buffer: RwLock::new(Vec::new()),
            delete_set: RwLock::new(AHashSet::new()),
            facet_dict: RwLock::new(IndexMap::new()),
            embedding_model: EmbeddingModel::None,
            indexed_doc_count: RwLock::new(0),
            indexed_vector_count: RwLock::new(0),
            level_doc_counts: RwLock::new(Vec::new()),
            facet_row_size: 4, // F32 facet = 4 bytes
            pending_docs: RwLock::new(crate::facet::PendingDocuments::default()),
            next_doc_id: RwLock::new(0),
        }
    }

    #[tokio::test]
    async fn test_index_struct_default() {
        let idx = test_index().await;
        assert_eq!(idx.schema.len(), 3);
        assert!(idx.schema.contains_key("title"));
        assert!(idx.schema.contains_key("rating"));
        assert_eq!(idx.facet_row_size, 4);
        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_allocate_doc_id() {
        let idx = test_index().await;
        let id1 = idx.allocate_doc_id();
        let id2 = idx.allocate_doc_id();
        let id3 = idx.allocate_doc_id();
        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id3, 2);
        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_add_text_to_level0() {
        let idx = test_index().await;
        let doc_id = idx.allocate_doc_id();
        idx.add_text_to_level0(doc_id, 0, "hello world hello");

        let level0 = idx.segments_level0.read().unwrap();
        assert!(!level0.segment.is_empty());
        // "hello" and "world" are the two unique terms.
        assert!(level0.segment.values().any(|pl| pl.docids.contains(&doc_id)));
        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_add_vector_to_buffer() {
        let idx = test_index().await;
        let embedding = vec![0.1, 0.2, 0.3, 0.4];
        idx.add_vector_to_buffer(5, 0, embedding.clone());

        let buffer = idx.vector_buffer.read().unwrap();
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].doc_id, 5);
        assert_eq!(buffer[0].embedding, embedding);
        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_document() {
        let idx = test_index().await;
        idx.delete_document(42);
        idx.delete_document(100);

        let del_set = idx.delete_set.read().unwrap();
        assert!(del_set.contains(&42));
        assert!(del_set.contains(&100));
        assert!(!del_set.contains(&0));
        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_create_and_open_index() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema_fields = test_schema();

        let idx = create_index("/test", store.clone(), test_config(), schema_fields.clone())
            .await
            .unwrap();

        assert_eq!(idx.schema.len(), 3);
        assert_eq!(idx.facet_row_size, 4);
        assert_eq!(*idx.indexed_doc_count.read().unwrap(), 0);

        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_commit_writes_posting_lists() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema_fields = test_schema();

        let idx = create_index("/test", store.clone(), test_config(), schema_fields.clone())
            .await
            .unwrap();

        // Add a document.
        let doc_id = idx.allocate_doc_id();
        idx.add_text_to_level0(doc_id, 0, "rust programming language");

        // Commit.
        commit(&idx).await.unwrap();

        // Verify posting list was written to SlateDB.
        let seg_idx = idx.segments_index.read().unwrap();
        assert!(!seg_idx.is_empty(), "should have at least one level after commit");
        let level0 = &seg_idx[0];
        assert!(!level0.segment.is_empty(), "level 0 should have posting lists");

        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_open_index_loads_data() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema_fields = test_schema();

        // Create and populate an index.
        {
            let idx = create_index("/test", store.clone(), test_config(), schema_fields.clone())
                .await
                .unwrap();
            let doc_id = idx.allocate_doc_id();
            idx.add_text_to_level0(doc_id, 0, "test document content");
            commit(&idx).await.unwrap();
            close_index(&idx).await.unwrap();
        }

        // Re-open and verify data is loaded.
        let reopened = open_index("/test", store.clone()).await.unwrap();
        let seg_idx = reopened.segments_index.read().unwrap();
        assert!(!seg_idx.is_empty(), "reopened index should have levels");
        close_index(&reopened).await.unwrap();
    }

    #[tokio::test]
    async fn test_commit_vector_data() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema_fields = test_schema();

        let idx = create_index("/test", store.clone(), test_config(), schema_fields.clone())
            .await
            .unwrap();

        // Add vector data.
        let doc_id = idx.allocate_doc_id();
        let embedding = vec![0.1_f32; 256];
        idx.add_vector_to_buffer(doc_id, 0, embedding);

        commit(&idx).await.unwrap();

        let vec_levels = idx.vector_levels.read().unwrap();
        assert!(!vec_levels.is_empty(), "should have vector levels after commit");

        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_search_after_commit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let schema_fields = test_schema();

        let idx = create_index("/test", store.clone(), test_config(), schema_fields.clone())
            .await
            .unwrap();

        // Add documents.
        let doc_id_0 = idx.allocate_doc_id();
        idx.add_text_to_level0(doc_id_0, 0, "rust programming language");

        let doc_id_1 = idx.allocate_doc_id();
        idx.add_text_to_level0(doc_id_1, 0, "python programming language");

        let doc_id_2 = idx.allocate_doc_id();
        idx.add_text_to_level0(doc_id_2, 0, "java programming language");

        commit(&idx).await.unwrap();

        // Search for "programming".
        let request = SearchRequest {
            query_string: Some("programming".to_string()),
            query_vector: None,
            query_type: QueryType::Union,
            search_mode: SearchMode::Lexical,
            offset: 0,
            length: 10,
            result_type: crate::search::ResultType::Topk,
            include_uncommitted: false,
            field_filter: None,
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 3, "should find 3 documents with 'programming'");

        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_search_empty_index() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let idx = create_index("/test", store.clone(), test_config(), test_schema())
            .await
            .unwrap();

        let request = SearchRequest {
            query_string: Some("hello".to_string()),
            query_vector: None,
            query_type: QueryType::Union,
            search_mode: SearchMode::Lexical,
            offset: 0,
            length: 10,
            result_type: crate::search::ResultType::Topk,
            include_uncommitted: false,
            field_filter: None,
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 0);

        close_index(&idx).await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_set_persists() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Create index.
        let idx = create_index("/test", store.clone(), test_config(), test_schema())
            .await
            .unwrap();
        idx.delete_document(42);
        commit(&idx).await.unwrap();

        // Debug: scan delete set entries from store.
        let dell_keys = idx.db.scan_prefix(PREFIX_DELL).await.unwrap();
        eprintln!("delete set keys in store after commit: {:?}", dell_keys.iter().map(|(k, _)| format!("{:?}", std::str::from_utf8(k.as_ref()).unwrap_or("?"))).collect::<Vec<_>>());

        // Re-open and verify (same store instance).
        let reopened = open_index("/test", store).await.unwrap();

        // Debug: scan delete set entries from re-opened store.
        let dell_keys2 = reopened.db.scan_prefix(PREFIX_DELL).await.unwrap();
        eprintln!("delete set keys in store after reopen: {:?}", dell_keys2.iter().map(|(k, _)| format!("{:?}", std::str::from_utf8(k.as_ref()).unwrap_or("?"))).collect::<Vec<_>>());

        let del_set = reopened.delete_set.read().unwrap();
        eprintln!("in-memory delete set: {:?}", del_set);
        assert!(del_set.contains(&42), "delete set should contain 42 after reopen");
        close_index(&reopened).await.unwrap();
    }

    #[tokio::test]
    async fn test_avg_doc_length_default() {
        let idx = test_index().await;
        let avg = idx.avg_doc_length();
        assert!(avg > 0.0);
        close_index(&idx).await.unwrap();
    }
}