use ahash::AHashSet;
use seekpower_schema::{IndexConfig, IndexStats, SchemaField, SchemaMap, TYPE_META, TYPE_DELL};
use seekpower_error::{Result, SeekPowerError};
use seekpower_storage::KvStore;
use seekpower_vector::IvfIndex;

use crate::lexical_index::LexicalIndex;
use crate::search::QueryEngine;

pub struct Index {
    db: KvStore,
    pub index_id: u32,
    pub name: String,
    pub schema: SchemaMap,
    pub field_names: Vec<String>,
    pub config: IndexConfig,
    pub stats: tokio::sync::RwLock<IndexStats>,
    pub next_doc_id: std::sync::atomic::AtomicU64,
    pub lexical_index: LexicalIndex,
    pub vector_index: IvfIndex,
    pub query_engine: QueryEngine,
    pub delete_set: tokio::sync::RwLock<AHashSet<u64>>,
}

impl Index {
    /// Access the underlying KvStore for this index.
    pub fn db(&self) -> &KvStore {
        &self.db
    }
}

// ---------------------------------------------------------------------------
// Index lifecycle management
// ---------------------------------------------------------------------------

/// Create a new index with the specified globally unique `index_id`.
pub async fn create_index_with_id(
    db: KvStore,
    index_id: u32,
    name: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let mut schema: SchemaMap = SchemaMap::new();
    let mut field_names = Vec::new();
    for field in &schema_fields {
        schema.insert(field.field.clone(), field.clone());
        field_names.push(field.field.clone());
    }

    db.prefixed_batch_put(index_id, TYPE_META, &[
        (b"schema", serde_json::to_vec(&schema).map_err(|e| SeekPowerError::StorageError(e.to_string()))?),
        (b"config", serde_json::to_vec(&config).map_err(|e| SeekPowerError::StorageError(e.to_string()))?),
        (b"stats", serde_json::to_vec(&IndexStats::default()).map_err(|e| SeekPowerError::StorageError(e.to_string()))?),
        (b"fields", serde_json::to_vec(&field_names).map_err(|e| SeekPowerError::StorageError(e.to_string()))?),
    ]).await?;
    let vector_index = IvfIndex::new(db.clone(), index_id, config.clone());
    let lexical_index = LexicalIndex::new(
        db.clone(), index_id, config.clone(), schema.clone(), field_names.clone(),
    );

    Ok(Index {
        db,
        index_id,
        name: name.to_string(),
        schema,
        field_names,
        config,
        stats: tokio::sync::RwLock::new(IndexStats::default()),
        next_doc_id: std::sync::atomic::AtomicU64::new(0),
        lexical_index,
        vector_index,
        query_engine: QueryEngine::new(),
        delete_set: tokio::sync::RwLock::new(AHashSet::new()),
    })
}

/// Open an existing index by its globally unique `index_id`.
pub async fn open_index_with_id(db: KvStore, index_id: u32, name: &str) -> Result<Index> {
    let schema: SchemaMap = db
        .prefixed_get_deserialized(index_id, TYPE_META, b"schema")
        .await?
        .ok_or_else(|| SeekPowerError::IndexNotFound(format!("schema not found for index '{}'", name)))?;

    let config: IndexConfig = db
        .prefixed_get_deserialized(index_id, TYPE_META, b"config")
        .await?
        .ok_or_else(|| SeekPowerError::IndexNotFound(format!("config not found for index '{}'", name)))?;

    let stats: IndexStats = db
        .prefixed_get_deserialized(index_id, TYPE_META, b"stats")
        .await?
        .unwrap_or_default();

    let field_names: Vec<String> = db
        .prefixed_get_deserialized(index_id, TYPE_META, b"fields")
        .await?
        .unwrap_or_else(|| schema.keys().cloned().collect());

    let mut delete_set = AHashSet::new();
    let dell_entries = db.prefixed_scan_prefix(index_id, TYPE_DELL, b"").await?;
    for (key_bytes, _) in dell_entries {
        if key_bytes.len() == 8 {
            let bytes: [u8; 8] = key_bytes.try_into().unwrap();
            delete_set.insert(u64::from_le_bytes(bytes));
        }
    }

    let vector_index = IvfIndex::new(db.clone(), index_id, config.clone());
    let lexical_index = LexicalIndex::open(
        db.clone(), index_id, config.clone(), schema.clone(), field_names.clone(),
    ).await?;
    let next_doc_id = stats.doc_count;

    Ok(Index {
        db,
        index_id,
        name: name.to_string(),
        schema,
        field_names,
        config,
        stats: tokio::sync::RwLock::new(stats),
        next_doc_id: std::sync::atomic::AtomicU64::new(next_doc_id),
        lexical_index,
        vector_index,
        query_engine: QueryEngine::new(),
        delete_set: tokio::sync::RwLock::new(delete_set),
    })
}

pub async fn close_index(index: &Index) -> Result<()> {
    index.db().close().await
}

/// Delete an index by its globally unique `index_id`.
/// Does NOT remove the registry entry - that's handled by SeekPower.
pub async fn delete_index_with_id(shared_db: &KvStore, index_id: u32) -> Result<()> {
    shared_db.prefixed_delete_all(index_id).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn test_schema() -> Vec<SchemaField> {
        vec![
            SchemaField::text("title"),
            SchemaField::text("body").with_boost(0.5).with_store(true),
        ]
    }

    #[tokio::test]
    async fn test_create_and_close_index() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shared_db = KvStore::open("test-create-close", store).await.unwrap();
        let idx = create_index_with_id(shared_db.clone(), 1, "test", IndexConfig::default(), test_schema()).await.unwrap();
        assert_eq!(idx.schema.len(), 2);
        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_two_indexes_share_db() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shared_db = KvStore::open("test-two-idx", store).await.unwrap();

        let a = create_index_with_id(shared_db.clone(), 1, "a", IndexConfig::default(), test_schema()).await.unwrap();
        let b = create_index_with_id(shared_db.clone(), 2, "b", IndexConfig::default(), test_schema()).await.unwrap();

        let mut doc = std::collections::HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("hello"));
        crate::document::index_document(&a, doc.clone()).await.unwrap();
        crate::document::index_document(&b, doc).await.unwrap();
        crate::commit::commit(&a).await.unwrap();
        crate::commit::commit(&b).await.unwrap();

        // Verify indexes are isolated
        let a_entries = shared_db.prefixed_scan_all(a.index_id, TYPE_META).await.unwrap();
        let b_entries = shared_db.prefixed_scan_all(b.index_id, TYPE_META).await.unwrap();
        assert!(!a_entries.is_empty());
        assert!(!b_entries.is_empty());

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_index_removes_all_keys() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shared_db = KvStore::open("test-delete-idx", store).await.unwrap();
        let idx = create_index_with_id(shared_db.clone(), 1, "doomed", IndexConfig::default(), test_schema()).await.unwrap();
        let mut doc = std::collections::HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("x"));
        crate::document::index_document(&idx, doc).await.unwrap();
        crate::commit::commit(&idx).await.unwrap();

        // Before: check index has data (now need to scan each data_type separately)
        use seekpower_storage::{encode_prefix, TYPE_DOC, TYPE_IDX, TYPE_META, TYPE_VEC};
        let mut has_data = false;
        for data_type in [TYPE_META, TYPE_DOC, TYPE_IDX, TYPE_VEC] {
            let prefix = encode_prefix(idx.index_id, data_type);
            if !shared_db.scan_prefix(&prefix).await.unwrap().is_empty() {
                has_data = true;
                break;
            }
        }
        assert!(has_data);

        delete_index_with_id(&shared_db, idx.index_id).await.unwrap();

        // After: verify all data is deleted
        for data_type in [TYPE_META, TYPE_DOC, TYPE_IDX, TYPE_VEC] {
            let prefix = encode_prefix(idx.index_id, data_type);
            assert!(shared_db.scan_prefix(&prefix).await.unwrap().is_empty());
        }
        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_index_fails_for_missing() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shared_db = KvStore::open("test-missing", store).await.unwrap();
        let res = open_index_with_id(shared_db.clone(), 999, "nope").await;
        assert!(res.is_err());
        match res {
            Err(SeekPowerError::IndexNotFound(_)) => {}
            Err(_) => panic!("expected IndexNotFound"),
            Ok(_) => panic!("expected error"),
        }
        shared_db.close().await.unwrap();
    }
}
