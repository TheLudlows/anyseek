use std::collections::HashMap;

use seekpower_error::{Result, SeekPowerError};
use seekpower_schema::{FieldType, TYPE_DOC};
use seekpower_vector::VectorIndex;

use crate::index::Index;

pub async fn index_document(
    index: &Index,
    document: HashMap<String, serde_json::Value>,
) -> Result<u64> {
    let doc_id = index.next_doc_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let mut buffer = index.lexical_index.buffer_write().await;
    let mut field_lengths: Vec<(u16, u16)> = Vec::new();

    for (field_name, value) in &document {
        let field_schema = match index.schema.get(field_name) {
            Some(s) => s,
            None => continue,
        };

        let field_id = index.field_names.iter().position(|n| n == field_name).unwrap_or(0) as u16;

        if field_schema.index_lexical && field_schema.field_type == FieldType::Text {
            if let Some(text) = value.as_str() {
                let token_count = index.lexical_index.index_text_field(
                    text, field_id, doc_id, &mut buffer,
                ).await;
                if let Ok(count) = token_count {
                    if count > 0 {
                        field_lengths.push((field_id, count));
                    }
                }
            }
        }

        if field_schema.index_vector {
            if let FieldType::Vector { dimensions } = field_schema.field_type {
                if let Some(arr) = value.as_array() {
                    let embedding: Vec<f32> = arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect();
                    if embedding.len() as u32 == dimensions && !embedding.is_empty() {
                        index.vector_index.add_vector(doc_id, field_id as u32, embedding);
                    } else if !embedding.is_empty() {
                        // Vector dimension mismatch - skip but could log warning
                        continue;
                    }
                }
            }
        }
    }

    let should_store = index.schema.values().any(|f| f.store);
    if should_store {
        let json = serde_json::to_vec(&document).map_err(|e| SeekPowerError::EncodingError(e.to_string()))?;
        buffer.documents.insert(doc_id, json);
    }

    if !field_lengths.is_empty() {
        buffer.doc_lengths.insert(doc_id, field_lengths);
    }

    Ok(doc_id)
}

pub async fn delete_document(index: &Index, doc_id: u64) -> Result<()> {
    index.lexical_index.delete_document(doc_id).await;
    Ok(())
}

pub async fn get_document(index: &Index, doc_id: u64) -> Result<Option<HashMap<String, serde_json::Value>>> {
    {
        let buffer = index.lexical_index.buffer_postings().await;
        if let Some(data) = buffer.documents.get(&doc_id) {
            let doc: HashMap<String, serde_json::Value> =
                serde_json::from_slice(data).map_err(|e| SeekPowerError::EncodingError(e.to_string()))?;
            return Ok(Some(doc));
        }
    }

    match index.db().prefixed_get(index.index_id, TYPE_DOC, &doc_id.to_le_bytes()).await? {
        Some(data) => {
            let doc: HashMap<String, serde_json::Value> =
                serde_json::from_slice(&data).map_err(|e| SeekPowerError::EncodingError(e.to_string()))?;
            Ok(Some(doc))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use crate::create_index_with_id;
use super::*;
    use object_store::memory::InMemory;
    use seekpower_schema::{IndexConfig, SchemaField};
    use seekpower_storage::KvStore;
    use std::sync::Arc;

    fn test_schema() -> Vec<SchemaField> {
        vec![
            SchemaField::text("title"),
            SchemaField::text("body").with_boost(0.5).with_store(true),
            SchemaField::vector("embedding", 4),
        ]
    }

    fn test_config() -> IndexConfig {
        IndexConfig { vector_dimensions: 4, ..IndexConfig::default() }
    }

    #[tokio::test]
    async fn test_index_document_returns_doc_id() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-index-doc", store).await.unwrap();
        let idx = create_index_with_id(shared_db.clone(), 1, "test", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("hello world"));
        let doc_id = index_document(&idx, doc).await.unwrap();
        assert_eq!(doc_id, 0);

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_document_after_commit() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-get-doc", store).await.unwrap();
        let idx = create_index_with_id(shared_db.clone(), 1, "test", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("test title"));
        doc.insert("body".to_string(), serde_json::json!("test body"));
        let doc_id = index_document(&idx, doc.clone()).await.unwrap();

        let retrieved = get_document(&idx, doc_id).await.unwrap();
        assert!(retrieved.is_some());

        crate::commit::commit(&idx).await.unwrap();

        let retrieved = get_document(&idx, doc_id).await.unwrap();
        assert!(retrieved.is_some());

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_document() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-delete", store).await.unwrap();
        let idx = create_index_with_id(shared_db.clone(), 1, "test", test_config(), test_schema()).await.unwrap();

        delete_document(&idx, 42).await.unwrap();
        let delete_set = idx.lexical_index.delete_set().await;
        assert!(delete_set.contains(&42));

        shared_db.close().await.unwrap();
    }
}
