//! SeekPower core: Index struct, lifecycle, document operations, commit, and search.

use std::collections::HashMap;
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use object_store::ObjectStore;
use seekpower_error::{Result, SeekPowerError};
use seekpower_posting::PostingCodec;
use seekpower_ranking::{compute_idf, Bm25fScorer, MinHeap, ResultType, SearchResult};
use seekpower_schema::{DocCompression, IndexConfig, IndexStats, SchemaField, SchemaMap};
use seekpower_storage::{make_key, validate_path, KvStore, ReadCache, PREFIX_DELL, PREFIX_DOC, PREFIX_IDX, PREFIX_LEN};
use seekpower_tokenizer::TokenizerRegistry;
use seekpower_vector::IvfIndex;

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

pub use seekpower_error::Result as SeekResult;
pub use seekpower_posting::{PostingEntry, PostingListBlock};
pub use seekpower_ranking::{compress_doc_length, decompress_doc_length};
pub use seekpower_schema::*;
pub use seekpower_search::*;
pub use seekpower_vector::{AnnMode, IvfMeta, MedoidData, VectorIndex, VectorSearchResult};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// ---------------------------------------------------------------------------
// WriteBuffer
// ---------------------------------------------------------------------------

pub struct WriteBuffer {
    pub postings: AHashMap<u64, Vec<PostingEntry>>,
    pub documents: AHashMap<u64, Vec<u8>>,
    pub doc_lengths: AHashMap<u64, Vec<(u16, u16)>>,
    pub deletions: AHashSet<u64>,
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self {
            postings: AHashMap::new(),
            documents: AHashMap::new(),
            doc_lengths: AHashMap::new(),
            deletions: AHashSet::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Index
// ---------------------------------------------------------------------------

pub struct Index {
    pub db: KvStore,
    pub path: String,
    pub schema: SchemaMap,
    pub config: IndexConfig,
    pub stats: tokio::sync::RwLock<IndexStats>,
    pub next_doc_id: std::sync::atomic::AtomicU64,
    pub buffer: tokio::sync::RwLock<WriteBuffer>,
    pub vector_index: IvfIndex,
    pub query_engine: QueryEngine,
    pub cache: ReadCache,
    pub delete_set: tokio::sync::RwLock<AHashSet<u64>>,
}

// ---------------------------------------------------------------------------
// Compression helpers
// ---------------------------------------------------------------------------

fn compress_document(json: &[u8], compression: &DocCompression) -> Vec<u8> {
    match compression {
        DocCompression::None => json.to_vec(),
        DocCompression::Snappy => snap::raw::Encoder::new().compress_vec(json).unwrap_or_else(|_| json.to_vec()),
        DocCompression::Lz4 => lz4_flex::compress(json),
        DocCompression::Zstd => zstd::bulk::compress(json, 3).unwrap_or_else(|_| json.to_vec()),
    }
}

fn decompress_document(data: &[u8], compression: &DocCompression) -> Vec<u8> {
    match compression {
        DocCompression::None => data.to_vec(),
        DocCompression::Snappy => snap::raw::Decoder::new().decompress_vec(data).unwrap_or_default(),
        DocCompression::Lz4 => lz4_flex::decompress(data, data.len() * 4).unwrap_or_default(),
        DocCompression::Zstd => zstd::bulk::decompress(data, data.len() * 4).unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

pub async fn create_index(
    shared_db: &KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let db = shared_db.for_index(path);
    create_index_with_db(db, path, config, schema_fields).await
}

pub async fn create_index_with_db(
    db: KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let mut schema: SchemaMap = SchemaMap::new();
    for field in &schema_fields {
        schema.insert(field.field.clone(), field.clone());
    }

    db.put_serialized(b"m:schema", &schema).await?;
    db.put_serialized(b"m:config", &config).await?;
    db.put_serialized(b"m:stats", &IndexStats::default()).await?;

    let cache = ReadCache::new(config.posting_cache_size_mb, config.cluster_cache_size_mb);
    let vector_index = IvfIndex::new(db.clone(), config.clone());

    Ok(Index {
        db,
        path: path.to_string(),
        schema,
        config,
        stats: tokio::sync::RwLock::new(IndexStats::default()),
        next_doc_id: std::sync::atomic::AtomicU64::new(0),
        buffer: tokio::sync::RwLock::new(WriteBuffer::default()),
        vector_index,
        query_engine: QueryEngine::new(),
        cache,
        delete_set: tokio::sync::RwLock::new(AHashSet::new()),
    })
}

pub async fn open_index(
    shared_db: &KvStore,
    path: &str,
) -> Result<Index> {
    let db = shared_db.for_index(path);
    open_index_with_db(db, path).await
}

pub async fn open_index_with_db(db: KvStore, path: &str) -> Result<Index> {
    let schema: SchemaMap = db
        .get_deserialized(b"m:schema")
        .await?
        .ok_or_else(|| SeekPowerError::IndexNotFound(format!("schema not found for index '{}'", path)))?;

    let config: IndexConfig = db
        .get_deserialized(b"m:config")
        .await?
        .ok_or_else(|| SeekPowerError::IndexNotFound(format!("config not found for index '{}'", path)))?;

    let stats: IndexStats = db
        .get_deserialized(b"m:stats")
        .await?
        .unwrap_or_default();

    let mut delete_set = AHashSet::new();
    let dell_entries = db.scan_prefix(b"l:").await?;
    for (key_bytes, _) in dell_entries {
        // key format after strip: "l:" + doc_id_le_bytes
        let suffix = &key_bytes[2..];
        if suffix.len() == 8 {
            let bytes: [u8; 8] = suffix.try_into().unwrap();
            delete_set.insert(u64::from_le_bytes(bytes));
        }
    }

    let cache = ReadCache::new(config.posting_cache_size_mb, config.cluster_cache_size_mb);
    let vector_index = IvfIndex::new(db.clone(), config.clone());
    let next_doc_id = stats.doc_count;

    Ok(Index {
        db,
        path: path.to_string(),
        schema,
        config,
        stats: tokio::sync::RwLock::new(stats),
        next_doc_id: std::sync::atomic::AtomicU64::new(next_doc_id),
        buffer: tokio::sync::RwLock::new(WriteBuffer::default()),
        vector_index,
        query_engine: QueryEngine::new(),
        cache,
        delete_set: tokio::sync::RwLock::new(delete_set),
    })
}

pub async fn close_index(index: &Index) -> Result<()> {
    index.db.close().await
}

/// Physically delete an index: scan and remove all keys under the index's
/// view, then remove the entry from the `__indexes__` directory.
pub async fn delete_index(shared_db: &KvStore, path: &str) -> Result<()> {
    validate_path(path)?;
    let view = shared_db.for_index(path);
    let entries = view.scan_all().await?;
    for (key, _) in entries {
        view.delete(&key).await?;
    }
    shared_db
        .for_index("__indexes__")
        .delete(path.as_bytes())
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Document operations
// ---------------------------------------------------------------------------

pub async fn index_document(
    index: &Index,
    document: HashMap<String, serde_json::Value>,
) -> Result<u64> {
    let doc_id = index.next_doc_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let registry = TokenizerRegistry::global();
    let registry_guard = registry.read().unwrap();

    let mut buffer = index.buffer.write().await;
    let mut total_tokens: u16 = 0;

    for (field_name, value) in &document {
        let field_schema = match index.schema.get(field_name) {
            Some(s) => s,
            None => continue,
        };

        if field_schema.index_lexical && field_schema.field_type == FieldType::Text {
            if let Some(text) = value.as_str() {
                let tokenizer_name = match &index.config.tokenizer {
                    seekpower_schema::TokenizerType::Simple => "simple",
                    seekpower_schema::TokenizerType::Unicode => "unicode",
                    seekpower_schema::TokenizerType::Jieba => "jieba",
                    seekpower_schema::TokenizerType::Custom(name) => name.as_str(),
                };

                if let Some(tokenizer) = registry_guard.get(tokenizer_name) {
                    let terms = seekpower_tokenizer::tokenize_index(
                        text, 0, &index.config, &registry_guard,
                    );
                    for term in terms {
                        let entry = buffer.postings.entry(term.key_hash).or_default();
                        entry.push(PostingEntry {
                            doc_id,
                            field_id: 0,
                            positions: term.positions,
                        });
                        total_tokens += 1;
                    }
                }
            }
        }

        if field_schema.index_vector && field_schema.field_type == FieldType::Vector {
            if let Some(arr) = value.as_array() {
                let embedding: Vec<f32> = arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect();
                if !embedding.is_empty() {
                    index.vector_index.add_vector(doc_id, 0, embedding);
                }
            }
        }
    }

    let should_store = index.schema.values().any(|f| f.store);
    if should_store {
        let json = serde_json::to_vec(&document).map_err(|e| SeekPowerError::EncodingError(e.to_string()))?;
        let compressed = compress_document(&json, &index.config.doc_compression);
        buffer.documents.insert(doc_id, compressed);
    }

    if total_tokens > 0 {
        buffer.doc_lengths.insert(doc_id, vec![(0, total_tokens)]);
    }

    Ok(doc_id)
}

pub async fn delete_document(index: &Index, doc_id: u64) -> Result<()> {
    index.buffer.write().await.deletions.insert(doc_id);
    index.delete_set.write().await.insert(doc_id);
    Ok(())
}

pub async fn get_document(index: &Index, doc_id: u64) -> Result<Option<HashMap<String, serde_json::Value>>> {
    // Check buffer first.
    {
        let buffer = index.buffer.read().await;
        if let Some(data) = buffer.documents.get(&doc_id) {
            let json = decompress_document(data, &index.config.doc_compression);
            let doc: HashMap<String, serde_json::Value> =
                serde_json::from_slice(&json).map_err(|e| SeekPowerError::EncodingError(e.to_string()))?;
            return Ok(Some(doc));
        }
    }

    // Check SlateDB.
    let key = make_key(&[PREFIX_DOC, b":", &doc_id.to_le_bytes()]);
    match index.db.get(&key).await? {
        Some(data) => {
            let json = decompress_document(&data, &index.config.doc_compression);
            let doc: HashMap<String, serde_json::Value> =
                serde_json::from_slice(&json).map_err(|e| SeekPowerError::EncodingError(e.to_string()))?;
            Ok(Some(doc))
        }
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Commit
// ---------------------------------------------------------------------------

pub async fn commit(index: &Index) -> Result<()> {
    let old_buffer = {
        let mut buf = index.buffer.write().await;
        std::mem::take(&mut *buf)
    };

    // Write deletion markers.
    for &doc_id in &old_buffer.deletions {
        let key = make_key(&[PREFIX_DELL, b":", &doc_id.to_le_bytes()]);
        index.db.put(&key, b"1").await?;
    }

    // Write posting lists: read-modify-write.
    for (&term_hash, new_entries) in &old_buffer.postings {
        let key = make_key(&[PREFIX_IDX, b":", &term_hash.to_le_bytes()]);

        let existing_block = match index.cache.get_posting(&key) {
            Some(data) => Some(PostingCodec::deserialize(&data)?),
            None => match index.db.get(&key).await? {
                Some(data) => {
                    let block = PostingCodec::deserialize(&data)?;
                    index.cache.insert_posting(&key, data);
                    Some(block)
                }
                None => None,
            }
        };

        let block = match existing_block {
            Some(existing) => seekpower_posting::merge_postings(&existing, new_entries),
            None => PostingCodec::encode(new_entries),
        };

        let serialized = PostingCodec::serialize(&block);
        index.cache.insert_posting(&key, serialized.clone());
        index.db.put(&key, &serialized).await?;
    }

    // Write document lengths.
    for (&doc_id, lengths) in &old_buffer.doc_lengths {
        for &(field_id, len) in lengths {
            let key = make_key(&[PREFIX_LEN, b":doc_len:", &doc_id.to_le_bytes(), b":", &field_id.to_le_bytes()]);
            index.db.put(&key, &[len as u8]).await?;
        }
    }

    // Write documents.
    for (&doc_id, data) in &old_buffer.documents {
        let key = make_key(&[PREFIX_DOC, b":", &doc_id.to_le_bytes()]);
        index.db.put(&key, data).await?;
    }

    // Flush vector index.
    index.vector_index.flush().await?;

    // Update stats.
    let new_doc_count = index.next_doc_id.load(std::sync::atomic::Ordering::SeqCst);
    {
        let mut stats = index.stats.write().await;
        stats.doc_count = new_doc_count;
        index.db.put_serialized(b"m:stats".as_slice(), &*stats).await?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

pub async fn search(index: &Index, request: SearchRequest) -> Result<SearchResponse> {
    let delete_set = index.delete_set.read().await.clone();
    let delete_std: std::collections::HashSet<u64> = delete_set.iter().copied().collect();
    let total_docs = index.next_doc_id.load(std::sync::atomic::Ordering::SeqCst);
    let avg_doc_length = 100.0f32;
    let scorer = Bm25fScorer::new();

    let results: Vec<SearchResult> = match &request.search_mode {
        SearchMode::Lexical => {
            let query_string = request.query_string.as_ref()
                .ok_or_else(|| SeekPowerError::InvalidQuery("query_string required for lexical search".into()))?;

            let registry = TokenizerRegistry::global();
            let registry_guard = registry.read().unwrap();
            let terms = seekpower_tokenizer::tokenize_query(query_string, &index.config, &registry_guard);

            if terms.is_empty() {
                return Ok(SearchResponse { query: query_string.clone(), result_count: 0, results: vec![] });
            }

            let mut heap = MinHeap::new(request.length.max(1).min(1000));

            for term in &terms {
                let key = make_key(&[PREFIX_IDX, b":", &term.key_hash.to_le_bytes()]);
                if let Some(data) = index.cache.get_posting(&key) {
                    if let Ok(block) = PostingCodec::deserialize(&data) {
                        let idf = compute_idf(total_docs, block.posting_count);
                        for entry in PostingCodec::decode(&block) {
                            if delete_set.contains(&entry.doc_id) { continue; }
                            let tf = entry.positions.len() as u32;
                            let score = scorer.score_field(tf, avg_doc_length, avg_doc_length, 1.0, idf);
                            heap.push(SearchResult { doc_id: entry.doc_id, score, result_type: ResultType::Lexical });
                        }
                    }
                }
            }

            // Also search buffer.
            {
                let buffer = index.buffer.read().await;
                for entries in buffer.postings.values() {
                    for entry in entries {
                        if delete_set.contains(&entry.doc_id) { continue; }
                        let tf = entry.positions.len() as u32;
                        let score = scorer.score_field(tf, avg_doc_length, avg_doc_length, 1.0, 1.0);
                        heap.push(SearchResult { doc_id: entry.doc_id, score, result_type: ResultType::Lexical });
                    }
                }
            }

            heap.into_sorted_vec()
        }

        SearchMode::Vector { ann_mode } => {
            let query_vector = request.query_vector.as_ref()
                .ok_or_else(|| SeekPowerError::InvalidQuery("query_vector required for vector search".into()))?;
            index.vector_index.search(query_vector, request.length.max(10), ann_mode, &delete_std)
                .into_iter()
                .map(|r| SearchResult { doc_id: r.doc_id, score: r.score, result_type: ResultType::Vector })
                .collect()
        }

        SearchMode::Hybrid { ann_mode, w_lexical, w_vector } => {
            let mut lexical_results: Vec<SearchResult> = Vec::new();
            let mut vector_results: Vec<SearchResult> = Vec::new();

            if let Some(ref query_string) = request.query_string {
                let registry = TokenizerRegistry::global();
                let registry_guard = registry.read().unwrap();
                let terms = seekpower_tokenizer::tokenize_query(query_string, &index.config, &registry_guard);

                let mut heap = MinHeap::new(request.length.max(1).min(1000));

                for term in &terms {
                    let key = make_key(&[PREFIX_IDX, b":", &term.key_hash.to_le_bytes()]);
                    if let Some(data) = index.cache.get_posting(&key) {
                        if let Ok(block) = PostingCodec::deserialize(&data) {
                            let idf = compute_idf(total_docs, block.posting_count);
                            for entry in PostingCodec::decode(&block) {
                                if delete_set.contains(&entry.doc_id) { continue; }
                                let tf = entry.positions.len() as u32;
                                let score = scorer.score_field(tf, avg_doc_length, avg_doc_length, 1.0, idf);
                                heap.push(SearchResult { doc_id: entry.doc_id, score, result_type: ResultType::Lexical });
                            }
                        }
                    }
                }

                {
                    let buffer = index.buffer.read().await;
                    for entries in buffer.postings.values() {
                        for entry in entries {
                            if delete_set.contains(&entry.doc_id) { continue; }
                            let tf = entry.positions.len() as u32;
                            let score = scorer.score_field(tf, avg_doc_length, avg_doc_length, 1.0, 1.0);
                            heap.push(SearchResult { doc_id: entry.doc_id, score, result_type: ResultType::Lexical });
                        }
                    }
                }

                lexical_results = heap.into_sorted_vec();
            }

            if let Some(ref query_vector) = request.query_vector {
                vector_results = index.vector_index.search(query_vector, request.length.max(10), ann_mode, &delete_std)
                    .into_iter()
                    .map(|r| SearchResult { doc_id: r.doc_id, score: r.score, result_type: ResultType::Vector })
                    .collect();
            }

            let fusion = seekpower_search::RrfFusion { k: 60, w_lexical: *w_lexical, w_vector: *w_vector };
            fusion.fuse(lexical_results, vector_results)
        }
    };

    let result_count = results.len();
    let offset = request.offset.min(results.len());
    let length = request.length.min(results.len() - offset);
    let results_slice = &results[offset..][..length];

    Ok(SearchResponse {
        query: request.query_string.unwrap_or_default(),
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
    use object_store::memory::InMemory;

    fn test_schema() -> Vec<SchemaField> {
        vec![
            SchemaField::text("title"),
            SchemaField::text("body").with_boost(0.5).with_store(true),
            SchemaField::vector("embedding"),
        ]
    }

    fn test_config() -> IndexConfig {
        IndexConfig { vector_dimensions: 4, ..IndexConfig::default() }
    }

    #[tokio::test]
    async fn test_create_and_close_index() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-create-close", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();
        assert_eq!(idx.schema.len(), 3);
        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_index_document_returns_doc_id() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-index-doc", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("hello world"));
        let doc_id = index_document(&idx, doc).await.unwrap();
        assert_eq!(doc_id, 0);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_commit_persists_data() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-commit", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("rust programming"));
        doc.insert("body".to_string(), serde_json::json!("systems language"));
        index_document(&idx, doc).await.unwrap();

        commit(&idx).await.unwrap();

        let stats = idx.stats.read().await;
        assert!(stats.doc_count >= 1);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_document_after_commit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-get-doc", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("test title"));
        doc.insert("body".to_string(), serde_json::json!("test body"));
        let doc_id = index_document(&idx, doc.clone()).await.unwrap();

        let retrieved = get_document(&idx, doc_id).await.unwrap();
        assert!(retrieved.is_some());

        commit(&idx).await.unwrap();

        let retrieved = get_document(&idx, doc_id).await.unwrap();
        assert!(retrieved.is_some());

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_document() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-delete", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        delete_document(&idx, 42).await.unwrap();
        assert!(idx.delete_set.read().await.contains(&42));

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_search_empty_index() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-search", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();

        let request = SearchRequest {
            query_string: Some("hello".to_string()),
            search_mode: SearchMode::Lexical,
            ..Default::default()
        };

        let response = search(&idx, request).await.unwrap();
        assert_eq!(response.result_count, 0);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_compress_decompress_roundtrip() {
        let json = br#"{"title":"hello","body":"world"}"#;
        for compression in &[DocCompression::None, DocCompression::Snappy, DocCompression::Lz4, DocCompression::Zstd] {
            let compressed = compress_document(json, compression);
            let decompressed = decompress_document(&compressed, compression);
            assert_eq!(decompressed, json.to_vec(), "roundtrip failed for {:?}", compression);
        }
    }

    #[tokio::test]
    async fn test_two_indexes_share_db() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-two-idx", store).await.unwrap();

        let a = create_index(&shared_db, "a", test_config(), test_schema()).await.unwrap();
        let b = create_index(&shared_db, "b", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("hello"));
        index_document(&a, doc.clone()).await.unwrap();
        index_document(&b, doc).await.unwrap();
        commit(&a).await.unwrap();
        commit(&b).await.unwrap();

        let raw_a = shared_db.for_index("a").scan_prefix(b"m:").await.unwrap();
        let raw_b = shared_db.for_index("b").scan_prefix(b"m:").await.unwrap();
        assert!(!raw_a.is_empty());
        assert!(!raw_b.is_empty());

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_index_removes_all_keys() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-delete-idx", store).await.unwrap();
        let idx = create_index(&shared_db, "doomed", test_config(), test_schema()).await.unwrap();
        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("x"));
        index_document(&idx, doc).await.unwrap();
        commit(&idx).await.unwrap();

        assert!(!shared_db.for_index("doomed").scan_all().await.unwrap().is_empty());
        delete_index(&shared_db, "doomed").await.unwrap();
        assert!(shared_db.for_index("doomed").scan_all().await.unwrap().is_empty());
        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_index_fails_for_missing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-missing", store).await.unwrap();
        let res = open_index(&shared_db, "nope").await;
        assert!(res.is_err());
        match res {
            Err(SeekPowerError::IndexNotFound(_)) => {}
            Err(_) => panic!("expected IndexNotFound"),
            Ok(_) => panic!("expected error"),
        }
        shared_db.close().await.unwrap();
    }
}
