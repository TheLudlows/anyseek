// storm-db module: document indexing, deletion, and retrieval

use ahash::AHashSet;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::clustering::ParentMedoid;
use crate::embed::EmbeddingModel;
use crate::error::{Result, StormDbError};
use crate::facet::{PendingDocuments, StringDictionary};
use crate::index::Index;
use crate::kv_store::{make_key, KvStore, PREFIX_DELL, PREFIX_DOC};
use crate::posting::{PostingListObject0, SegmentLevel0};
use crate::schema::{Compression, FieldType, IndexConfig, SchemaField, SchemaMap};
use crate::tokenizer::tokenize_index;

// ---------------------------------------------------------------------------
// Pending document storage
// ---------------------------------------------------------------------------

/// Buffer for documents indexed but not yet committed.
#[derive(Default)]
pub struct PendingDocuments {
    /// (doc_id, compressed document bytes)
    pub docs: Vec<(u64, Vec<u8>)>,
    /// (doc_id, facet row bytes)
    pub facets: Vec<(u64, Vec<u8>)>,
}

// ---------------------------------------------------------------------------
// Document compression helpers
// ---------------------------------------------------------------------------

/// Compress document bytes using the specified compression algorithm.
pub fn compress_document(data: &[u8], compression: Compression) -> Vec<u8> {
    match compression {
        Compression::None => data.to_vec(),
        Compression::Snappy => {
            let mut encoder = snap::write::FrameEncoder::new(Vec::new());
            std::io::Write::write_all(&mut encoder, data).unwrap();
            encoder.into_inner().unwrap()
        }
        Compression::Lz4 => lz4_flex::block::compress_prepend_size(data),
        Compression::Zstd => zstd::encode_all(data, 3).unwrap_or_else(|_| data.to_vec()),
    }
}

/// Decompress document bytes using the specified compression algorithm.
pub fn decompress_document(data: &[u8], compression: Compression) -> Vec<u8> {
    match compression {
        Compression::None => data.to_vec(),
        Compression::Snappy => {
            let mut decoder = snap::read::FrameDecoder::new(data);
            let mut result = Vec::new();
            std::io::Read::read_to_end(&mut decoder, &mut result).unwrap();
            result
        }
        Compression::Lz4 => lz4_flex::block::decompress_size_prepended(data)
            .unwrap_or_else(|_| data.to_vec()),
        Compression::Zstd => zstd::decode_all(data).unwrap_or_else(|_| data.to_vec()),
    }
}

// ---------------------------------------------------------------------------
// Document operations
// ---------------------------------------------------------------------------

/// Index a single document into the index.
///
/// Returns the global document ID assigned to this document.
pub fn index_document(
    index: &Index,
    document: HashMap<String, serde_json::Value>,
) -> Result<u64> {
    // 1. Determine level and local doc_id
    let level_doc_counts = index.level_doc_counts.read().unwrap();
    let num_levels = level_doc_counts.len();
    let level = if num_levels == 0 { 0 } else { num_levels - 1 };

    // Drop read lock before acquiring write lock
    drop(level_doc_counts);

    let local_doc_id = {
        let mut counts = index.level_doc_counts.write().unwrap();
        if counts.is_empty() {
            counts.push(0);
        }
        let id = counts[level];
        if id >= 65536 {
            counts.push(0);
        }
        id
    };

    // Re-check level (might have changed)
    let counts = index.level_doc_counts.read().unwrap();
    let final_level = if counts.len() == 1 { 0 } else { counts.len() - 1 };
    drop(counts);

    let local_doc_id = {
        let mut counts = index.level_doc_counts.write().unwrap();
        let lvl = if counts.len() == 1 { 0 } else { counts.len() - 1 };
        let id = counts[lvl];
        counts[lvl] += 1;
        id
    };

    let global_doc_id = (final_level as u64) << 16 | local_doc_id as u64;

    // 2. Process text fields (index_lexical)
    let segment_mask = (1u32 << index.config.segment_number_bits) - 1;

    {
        let mut seg = index.segments_level0.write().unwrap();
        for (field_name, schema_field) in &index.schema {
            if schema_field.field_type == FieldType::Text && schema_field.index_lexical {
                if let Some(value) = document.get(field_name) {
                    let text = match value {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    if !text.is_empty() {
                        let terms = tokenize_index(&text, 0, &index.config, segment_mask);
                        for term in terms {
                            let posting = seg
                                .segment
                                .entry(term.key_hash)
                                .or_insert_with(|| PostingListObject0 {
                                    docids: Vec::new(),
                                    positions: Vec::new(),
                                    posting_count: 0,
                                    max_block_score: 0.0,
                                    max_docid: local_doc_id as u16,
                                    max_p_docid: 0,
                                });
                            posting.docids.push(local_doc_id as u16);
                            // Flatten positions from all field_positions_vec
                            let pos_vec: Vec<u16> = term
                                .field_positions_vec
                                .iter()
                                .flat_map(|p| p.iter().copied())
                                .collect();
                            posting.positions.push(pos_vec);
                            posting.posting_count += 1;
                            if local_doc_id as u16 > posting.max_docid {
                                posting.max_docid = local_doc_id as u16;
                            }
                        }
                    }
                }
            }
        }
    }

    // 3. Process vector fields (index_vector)
    for (field_name, schema_field) in &index.schema {
        if schema_field.index_vector {
            match schema_field.field_type {
                FieldType::Vector => {
                    if let Some(value) = document.get(field_name) {
                        if let Some(arr) = value.as_array() {
                            let embedding: Vec<f32> = arr
                                .iter()
                                .filter_map(|v| v.as_f64().map(|f| f as f32))
                                .collect();
                            if !embedding.is_empty() {
                                let pm = crate::clustering::ParentMedoid {
                                    medoid_index: 0,
                                    is_medoid: false,
                                    similarity: 0.0,
                                    doc_id: local_doc_id as u16,
                                    field_id: 0,
                                    chunk_id: 0,
                                    embedding,
                                    scale: 1.0,
                                    norm: 0.0,
                                    zero_point: 0,
                                    sum_q: 0,
                                };
                                index.vector_buffer.write().unwrap().push(pm);
                            }
                        }
                    }
                }
                FieldType::Text => {
                    if index.embedding_model.is_loaded() {
                        if let Some(value) = document.get(field_name) {
                            let text = match value {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            if let Some(embedding) = index.embedding_model.embed(&text) {
                                let pm = crate::clustering::ParentMedoid {
                                    medoid_index: 0,
                                    is_medoid: false,
                                    similarity: 0.0,
                                    doc_id: local_doc_id as u16,
                                    field_id: 0,
                                    chunk_id: 0,
                                    embedding,
                                    scale: 1.0,
                                    norm: 0.0,
                                    zero_point: 0,
                                    sum_q: 0,
                                };
                                index.vector_buffer.write().unwrap().push(pm);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // 4. Encode facet values
    let mut dict = StringDictionary::new();
    let mut facet_row_bytes = Vec::new();
    for (field_name, schema_field) in &index.schema {
        if schema_field.facet {
            if let Some(value) = document.get(field_name) {
                let encoded = crate::facet::encode_facet_value(
                    value,
                    schema_field.field_type,
                    &mut dict,
                );
                facet_row_bytes.extend(encoded);
            }
        }
    }

    // 5. Store in pending buffer and increment counter
    let doc_bytes = serde_json::to_vec(&document).unwrap_or_default();
    index.pending_docs.write().unwrap().push(
        global_doc_id,
        doc_bytes,
        facet_row_bytes,
    );

    *index.indexed_doc_count.write().unwrap() += 1;

    Ok(global_doc_id)
}

/// Batch index multiple documents.
pub fn index_documents(
    index: &Index,
    documents: Vec<HashMap<String, serde_json::Value>>,
) -> Result<Vec<u64>> {
    documents
        .into_iter()
        .map(|doc| index_document(index, doc))
        .collect()
}

/// Delete a document by its global document ID.
pub async fn delete_document(index: &Index, doc_id: u64) -> Result<()> {
    index
        .delete_set
        .write()
        .unwrap()
        .insert(doc_id as usize);
    let key = make_key(&[PREFIX_DELL, &doc_id.to_le_bytes()]);
    index.db.put(&key, &[]).await?;
    Ok(())
}

/// Get a document by its global document ID from the KV store.
pub async fn get_document(
    index: &Index,
    doc_id: u64,
) -> Result<HashMap<String, serde_json::Value>> {
    let key = make_key(&[PREFIX_DOC, &doc_id.to_le_bytes()]);
    match index.db.get(&key).await? {
        Some(bytes) => {
            let decompressed = decompress_document(&bytes, index.config.compression);
            let doc: HashMap<String, serde_json::Value> = serde_json::from_slice(&decompressed)
                .map_err(|e| StormDbError::StorageError(format!("deserialize document: {e}")))?;
            Ok(doc)
        }
        None => Err(StormDbError::DocumentNotFound(doc_id)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashMap;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    /// Build a minimal Index with in-memory SlateDB for testing.
    async fn build_test_index() -> Index {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let db = KvStore::open("/test", store)
            .await
            .expect("failed to open test store");

        let mut schema = SchemaMap::new();
        schema.insert(
            "title".to_string(),
            SchemaField {
                field: "title".to_string(),
                store: true,
                index_lexical: true,
                index_vector: false,
                field_type: FieldType::Text,
                facet: false,
                boost: 1.0,
            },
        );
        schema.insert(
            "body".to_string(),
            SchemaField {
                field: "body".to_string(),
                store: true,
                index_lexical: true,
                index_vector: false,
                field_type: FieldType::Text,
                facet: false,
                boost: 1.0,
            },
        );
        schema.insert(
            "embedding".to_string(),
            SchemaField {
                field: "embedding".to_string(),
                store: false,
                index_lexical: false,
                index_vector: true,
                field_type: FieldType::Vector,
                facet: false,
                boost: 1.0,
            },
        );
        schema.insert(
            "category".to_string(),
            SchemaField {
                field: "category".to_string(),
                store: true,
                index_lexical: false,
                index_vector: false,
                field_type: FieldType::U8,
                facet: true,
                boost: 1.0,
            },
        );

        let config = IndexConfig::default();

        Index {
            db,
            schema,
            config,
            segments_level0: RwLock::new(SegmentLevel0 {
                segment: AHashMap::new(),
            }),
            vector_buffer: RwLock::new(Vec::new()),
            delete_set: RwLock::new(AHashSet::new()),
            indexed_doc_count: RwLock::new(0),
            facet_row_size: 1, // U8 facet field = 1 byte
            facet_dict: RwLock::new(IndexMap::new()),
            embedding_model: EmbeddingModel::None,
            level_doc_counts: RwLock::new(Vec::new()),
            pending_docs: RwLock::new(PendingDocuments::default()),
        }
    }

    #[tokio::test]
    async fn test_index_document_text_fields_populate_segments() {
        let index = build_test_index().await;

        let mut doc = HashMap::new();
        doc.insert(
            "title".to_string(),
            serde_json::Value::String("hello world".to_string()),
        );
        doc.insert(
            "body".to_string(),
            serde_json::Value::String("rust programming language".to_string()),
        );
        doc.insert(
            "embedding".to_string(),
            serde_json::json!([0.1, 0.2, 0.3, 0.4]),
        );
        doc.insert(
            "category".to_string(),
            serde_json::Value::Number(serde_json::Number::from(1u8)),
        );

        let doc_id = index_document(&index, doc).unwrap();
        assert_eq!(doc_id, 0);

        // Verify segments_level0 has entries from text fields
        let seg = index.segments_level0.read().unwrap();
        assert!(
            seg.segment.len() > 0,
            "segments should have entries after indexing text"
        );

        // Verify level_doc_counts incremented
        let counts = index.level_doc_counts.read().unwrap();
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0], 1);

        // Verify indexed_doc_count
        assert_eq!(*index.indexed_doc_count.read().unwrap(), 1);

        // Verify pending_docs has entry
        let pending = index.pending_docs.read().unwrap();
        assert_eq!(pending.docs.len(), 1);
        assert_eq!(pending.docs[0].0, 0);

        index.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_index_document_vector_fields_populate_buffer() {
        let index = build_test_index().await;

        let mut doc = HashMap::new();
        doc.insert(
            "embedding".to_string(),
            serde_json::json!([1.0, 2.0, 3.0, 4.0]),
        );

        let doc_id = index_document(&index, doc).unwrap();
        assert_eq!(doc_id, 0);

        // Verify vector_buffer has entry
        let buf = index.vector_buffer.read().unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0].doc_id, 0);
        assert_eq!(buf[0].embedding.len(), 4);
        assert!((buf[0].embedding[0] - 1.0).abs() < 1e-6);
        assert!((buf[0].embedding[1] - 2.0).abs() < 1e-6);

        index.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_index_document_facet_data_stored() {
        let index = build_test_index().await;

        let mut doc = HashMap::new();
        doc.insert(
            "title".to_string(),
            serde_json::Value::String("test".to_string()),
        );
        doc.insert(
            "category".to_string(),
            serde_json::Value::Number(serde_json::Number::from(5u8)),
        );

        let doc_id = index_document(&index, doc).unwrap();
        assert_eq!(doc_id, 0);

        // Verify facet data stored in pending
        let pending = index.pending_docs.read().unwrap();
        assert_eq!(pending.facets.len(), 1);
        assert_eq!(pending.facets[0].0, 0);
        // U8 facet value = 1 byte
        assert_eq!(pending.facets[0].1.len(), 1);
        assert_eq!(pending.facets[0].1[0], 5u8);

        index.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_document() {
        let index = build_test_index().await;

        delete_document(&index, 42).await.unwrap();

        // Verify doc_id in delete_set
        let ds = index.delete_set.read().unwrap();
        assert!(ds.contains(&42usize));

        // Verify the index doc_count is still 0 (we only deleted, didn't index)
        assert_eq!(*index.indexed_doc_count.read().unwrap(), 0);

        index.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_index_documents_batch() {
        let index = build_test_index().await;

        let mut docs = Vec::new();
        for i in 0..5 {
            let mut doc = HashMap::new();
            doc.insert(
                "title".to_string(),
                serde_json::Value::String(format!("doc {}", i)),
            );
            doc.insert(
                "category".to_string(),
                serde_json::Value::Number(serde_json::Number::from(i as u8)),
            );
            docs.push(doc);
        }

        let doc_ids = index_documents(&index, docs).unwrap();
        assert_eq!(doc_ids.len(), 5);

        // Verify sequential doc_ids: 0, 1, 2, 3, 4
        for (i, &id) in doc_ids.iter().enumerate() {
            assert_eq!(id, i as u64);
        }

        // Verify indexed_doc_count
        assert_eq!(*index.indexed_doc_count.read().unwrap(), 5);

        // Verify segments populated
        let seg = index.segments_level0.read().unwrap();
        assert!(seg.segment.len() > 0);

        // Verify pending_docs
        let pending = index.pending_docs.read().unwrap();
        assert_eq!(pending.docs.len(), 5);

        index.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_document_from_store() {
        let index = build_test_index().await;

        // Create and store a document directly
        let mut doc = HashMap::new();
        doc.insert(
            "title".to_string(),
            serde_json::Value::String("stored doc".to_string()),
        );
        let doc_bytes = serde_json::to_vec(&doc).unwrap();
        let compressed = compress_document(&doc_bytes, index.config.compression);

        let key = make_key(&[PREFIX_DOC, &0u64.to_le_bytes()]);
        index.db.put(&key, &compressed).await.unwrap();

        // Retrieve it
        let retrieved = get_document(&index, 0).await.unwrap();
        assert_eq!(
            retrieved.get("title").unwrap().as_str().unwrap(),
            "stored doc"
        );

        // Non-existent doc
        let result = get_document(&index, 999).await;
        assert!(result.is_err());

        index.db.close().await.unwrap();
    }

    #[test]
    fn test_compress_decompress_none() {
        let data = br#"{"title":"test","body":"hello"}"#;
        let compressed = compress_document(data, Compression::None);
        assert_eq!(&compressed[..], &data[..]);
        let decompressed = decompress_document(&compressed, Compression::None);
        assert_eq!(&decompressed[..], &data[..]);
    }

    #[test]
    fn test_compress_decompress_snappy() {
        let data = br#"{"title":"hello world","body":"rust programming language is great"}"#;
        let compressed = compress_document(data, Compression::Snappy);
        let decompressed = decompress_document(&compressed, Compression::Snappy);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decompressed).unwrap(),
            serde_json::from_slice::<serde_json::Value>(data).unwrap()
        );
    }

    #[test]
    fn test_compress_decompress_lz4() {
        let data = br#"{"title":"hello world","body":"rust programming language is great"}"#;
        let compressed = compress_document(data, Compression::Lz4);
        let decompressed = decompress_document(&compressed, Compression::Lz4);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decompressed).unwrap(),
            serde_json::from_slice::<serde_json::Value>(data).unwrap()
        );
    }

    #[test]
    fn test_compress_decompress_zstd() {
        let data = br#"{"title":"hello world","body":"rust programming language is great"}"#;
        let compressed = compress_document(data, Compression::Zstd);
        let decompressed = decompress_document(&compressed, Compression::Zstd);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decompressed).unwrap(),
            serde_json::from_slice::<serde_json::Value>(data).unwrap()
        );
    }

    #[test]
    fn test_document_roundtrip_all_compression_types() {
        let original = serde_json::json!({
            "title": "test document",
            "body": "some content here for testing",
            "score": 42,
            "tags": ["rust", "search"]
        });
        let data = serde_json::to_vec(&original).unwrap();

        for compression in [
            Compression::None,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let compressed = compress_document(&data, compression);
            let decompressed = decompress_document(&compressed, compression);
            let recovered: serde_json::Value = serde_json::from_slice(&decompressed).unwrap();
            assert_eq!(
                recovered, original,
                "roundtrip failed for compression {:?}",
                compression
            );
        }
    }

    #[tokio::test]
    async fn test_index_document_global_doc_id() {
        let index = build_test_index().await;

        // First document: level 0, local_id 0 => global 0
        let mut doc1 = HashMap::new();
        doc1.insert(
            "title".to_string(),
            serde_json::Value::String("doc1".to_string()),
        );
        let id1 = index_document(&index, doc1).unwrap();
        assert_eq!(id1, 0);

        // Second document: level 0, local_id 1 => global 1
        let mut doc2 = HashMap::new();
        doc2.insert(
            "title".to_string(),
            serde_json::Value::String("doc2".to_string()),
        );
        let id2 = index_document(&index, doc2).unwrap();
        assert_eq!(id2, 1);

        index.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_index_document_pending_buffer() {
        let index = build_test_index().await;

        let mut doc = HashMap::new();
        doc.insert(
            "title".to_string(),
            serde_json::Value::String("pending test".to_string()),
        );

        let doc_id = index_document(&index, doc.clone()).unwrap();

        let pending = index.pending_docs.read().unwrap();
        assert_eq!(pending.docs.len(), 1);
        assert_eq!(pending.docs[0].0, doc_id);

        // Verify document content in pending
        let stored_doc: HashMap<String, serde_json::Value> =
            serde_json::from_slice(&pending.docs[0].1).unwrap();
        assert_eq!(
            stored_doc.get("title").unwrap().as_str().unwrap(),
            "pending test"
        );

        index.db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_documents_segments_accumulate() {
        let index = build_test_index().await;

        let texts = vec!["hello world", "hello rust", "rust search", "programming rust"];

        for text in &texts {
            let mut doc = HashMap::new();
            doc.insert(
                "title".to_string(),
                serde_json::Value::String(text.to_string()),
            );
            index_document(&index, doc).unwrap();
        }

        let seg = index.segments_level0.read().unwrap();
        // Should have entries for: hello, world, rust, search, programming
        let posting_count: usize = seg.segment.values().map(|p| p.posting_count).sum();
        assert!(
            posting_count > 0,
            "segments should accumulate postings from multiple documents"
        );

        // Verify total doc count
        assert_eq!(*index.indexed_doc_count.read().unwrap(), 4);

        index.db.close().await.unwrap();
    }
}
