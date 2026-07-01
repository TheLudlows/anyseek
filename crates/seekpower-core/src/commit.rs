use seekpower_error::Result;
use seekpower_schema::{TYPE_META, TYPE_DOC};
use seekpower_vector::VectorIndex;

use crate::index::Index;

pub async fn commit(index: &Index) -> Result<()> {
    let old_buffer = index.lexical_index.take_buffer().await;

    let new_doc_count = index.next_doc_id.load(std::sync::atomic::Ordering::SeqCst);
    {
        let mut stats = index.stats.write().await;
        stats.doc_count = new_doc_count;

        index.lexical_index.flush(&old_buffer, &mut stats).await?;

        index.db().prefixed_put_serialized(index.index_id, TYPE_META, b"stats", &*stats).await?;
    }

    // Write documents (stored outside lexical index since they're raw data, not postings).
    for (&doc_id, data) in &old_buffer.documents {
        index.db().prefixed_put(index.index_id, TYPE_DOC, &doc_id.to_le_bytes(), data).await?;
    }

    // Flush vector index.
    index.vector_index.flush().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::create_index_with_id;
use super::*;
    use object_store::memory::InMemory;
    use seekpower_schema::{IndexConfig, SchemaField};
    use seekpower_storage::KvStore;
    use std::sync::Arc;
    use std::collections::HashMap;

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
    async fn test_commit_persists_data() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-commit", store).await.unwrap();
        let idx = create_index_with_id(shared_db.clone(), 1, "test", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("rust programming"));
        doc.insert("body".to_string(), serde_json::json!("systems language"));
        crate::document::index_document(&idx, doc).await.unwrap();

        commit(&idx).await.unwrap();

        let stats = idx.stats.read().await;
        assert!(stats.doc_count >= 1);

        shared_db.close().await.unwrap();
    }
}
