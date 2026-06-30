use ahash::AHashSet;
use seekpower_schema::{IndexConfig, IndexStats, SchemaField, SchemaMap, validate_path};
use seekpower_error::{Result, SeekPowerError};
use seekpower_storage::{Cache, KvStore};
use seekpower_vector::IvfIndex;

use crate::lexical_index::LexicalIndex;
use crate::search::QueryEngine;

type ReadCache = Cache;

/// Wrapper that prefixes all keys with an index path.
#[derive(Clone)]
pub struct PrefixedKvStore {
    db: KvStore,
    prefix: Vec<u8>,
}

impl PrefixedKvStore {
    pub fn new(db: KvStore, path: &str) -> Result<Self> {
        validate_path(path).map_err(|e| SeekPowerError::InvalidIndexName(e))?;
        let mut prefix = Vec::with_capacity(path.len() + 1);
        prefix.extend_from_slice(path.as_bytes());
        prefix.push(b':');
        Ok(Self { db, prefix })
    }

    fn prefixed_key(&self, key: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.prefix.len() + key.len());
        result.extend_from_slice(&self.prefix);
        result.extend_from_slice(key);
        result
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(&self.prefixed_key(key), value).await
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(&self.prefixed_key(key)).await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(&self.prefixed_key(key)).await
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let full_prefix = self.prefixed_key(prefix);
        let results = self.db.scan_prefix(&full_prefix).await?;
        // Strip the view prefix from returned keys
        let prefix_len = self.prefix.len();
        Ok(results.into_iter()
            .filter_map(|(k, v)| {
                if k.len() >= prefix_len {
                    Some((k[prefix_len..].to_vec(), v))
                } else {
                    None
                }
            })
            .collect())
    }

    pub async fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(b"").await
    }

    pub async fn put_serialized<T: serde::Serialize>(&self, key: &[u8], value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| SeekPowerError::StorageError(format!("Serialization failed: {}", e)))?;
        self.put(key, &bytes).await
    }

    pub async fn get_deserialized<T: serde::de::DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes).map_err(|e| {
                    SeekPowerError::StorageError(format!("Deserialization failed: {}", e))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub async fn begin_transaction(&self) -> Result<PrefixedTransaction> {
        let txn = self.db.begin_transaction().await?;
        Ok(PrefixedTransaction { txn, prefix: self.prefix.clone() })
    }

    pub fn raw_db(&self) -> KvStore {
        self.db.clone()
    }
}

/// Transaction wrapper that prefixes all keys.
pub struct PrefixedTransaction {
    txn: seekpower_storage::Transaction,
    prefix: Vec<u8>,
}

impl PrefixedTransaction {
    fn prefixed_key(&self, key: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.prefix.len() + key.len());
        result.extend_from_slice(&self.prefix);
        result.extend_from_slice(key);
        result
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn.put(&self.prefixed_key(key), value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.txn.delete(&self.prefixed_key(key))
    }

    pub async fn commit(self) -> Result<()> {
        self.txn.commit().await
    }

    pub fn rollback(self) -> Result<()> {
        self.txn.rollback()
    }
}

pub struct Index {
    pub db: PrefixedKvStore,
    pub path: String,
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

// ---------------------------------------------------------------------------
// Index lifecycle management
// ---------------------------------------------------------------------------

pub async fn create_index(
    shared_db: &KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let db = PrefixedKvStore::new(shared_db.clone(), path)?;
    create_index_with_db(db, path, config, schema_fields).await
}

pub async fn create_index_with_db(
    db: PrefixedKvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let mut schema: SchemaMap = SchemaMap::new();
    let mut field_names = Vec::new();
    for field in &schema_fields {
        schema.insert(field.field.clone(), field.clone());
        field_names.push(field.field.clone());
    }

    db.put_serialized(b"m:schema", &schema).await?;
    db.put_serialized(b"m:config", &config).await?;
    db.put_serialized(b"m:stats", &IndexStats::default()).await?;
    db.put_serialized(b"m:fields", &field_names).await?;

    let cache = ReadCache::new((config.posting_cache_size_mb + config.cluster_cache_size_mb) * 1024 * 1024);
    let raw_db = db.raw_db();
    let vector_index = IvfIndex::new(raw_db.clone(), config.clone());
    let lexical_index = LexicalIndex::new(
        db.clone(), config.clone(), schema.clone(), field_names.clone(), cache, AHashSet::new(),
    );

    Ok(Index {
        db,
        path: path.to_string(),
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

pub async fn open_index(shared_db: &KvStore, path: &str) -> Result<Index> {
    let db = PrefixedKvStore::new(shared_db.clone(), path)?;
    open_index_with_db(db, path).await
}

pub async fn open_index_with_db(db: PrefixedKvStore, path: &str) -> Result<Index> {
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

    let field_names: Vec<String> = db
        .get_deserialized(b"m:fields")
        .await?
        .unwrap_or_else(|| schema.keys().cloned().collect());

    let mut delete_set = AHashSet::new();
    let dell_entries = db.scan_prefix(b"l:").await?;
    for (key_bytes, _) in dell_entries {
        let suffix = &key_bytes[2..];
        if suffix.len() == 8 {
            let bytes: [u8; 8] = suffix.try_into().unwrap();
            delete_set.insert(u64::from_le_bytes(bytes));
        }
    }

    let cache = ReadCache::new((config.posting_cache_size_mb + config.cluster_cache_size_mb) * 1024 * 1024);
    let raw_db = db.raw_db();
    let vector_index = IvfIndex::new(raw_db.clone(), config.clone());
    let lexical_index = LexicalIndex::new(
        db.clone(), config.clone(), schema.clone(), field_names.clone(), cache, delete_set.clone(),
    );
    let next_doc_id = stats.doc_count;

    Ok(Index {
        db,
        path: path.to_string(),
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
    index.db.raw_db().close().await
}

pub async fn delete_index(shared_db: &KvStore, path: &str) -> Result<()> {
    validate_path(path).map_err(|e| SeekPowerError::InvalidIndexName(e))?;
    let prefixed = PrefixedKvStore::new(shared_db.clone(), path)?;
    let entries = prefixed.scan_all().await?;
    for (key, _) in entries {
        prefixed.delete(&key).await?;
    }
    // Remove from index registry
    shared_db.delete(format!("__indexes__:{}", path).as_bytes()).await?;
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
            SchemaField::vector("embedding"),
        ]
    }

    fn test_config() -> IndexConfig {
        IndexConfig { vector_dimensions: 4, ..IndexConfig::default() }
    }

    #[tokio::test]
    async fn test_create_and_close_index() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shared_db = KvStore::open("test-create-close", store).await.unwrap();
        let idx = create_index(&shared_db, "test", test_config(), test_schema()).await.unwrap();
        assert_eq!(idx.schema.len(), 3);
        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_two_indexes_share_db() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shared_db = KvStore::open("test-two-idx", store).await.unwrap();

        let a = create_index(&shared_db, "a", test_config(), test_schema()).await.unwrap();
        let b = create_index(&shared_db, "b", test_config(), test_schema()).await.unwrap();

        let mut doc = std::collections::HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("hello"));
        crate::document::index_document(&a, doc.clone()).await.unwrap();
        crate::document::index_document(&b, doc).await.unwrap();
        crate::commit::commit(&a).await.unwrap();
        crate::commit::commit(&b).await.unwrap();

        // Verify indexes are isolated
        let raw_a = PrefixedKvStore::new(shared_db.clone(), "a").unwrap();
        let raw_b = PrefixedKvStore::new(shared_db.clone(), "b").unwrap();
        assert!(!raw_a.scan_prefix(b"m:").await.unwrap().is_empty());
        assert!(!raw_b.scan_prefix(b"m:").await.unwrap().is_empty());

        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_index_removes_all_keys() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let shared_db = KvStore::open("test-delete-idx", store).await.unwrap();
        let idx = create_index(&shared_db, "doomed", test_config(), test_schema()).await.unwrap();
        let mut doc = std::collections::HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("x"));
        crate::document::index_document(&idx, doc).await.unwrap();
        crate::commit::commit(&idx).await.unwrap();

        assert!(!PrefixedKvStore::new(shared_db.clone(), "doomed").unwrap().scan_all().await.unwrap().is_empty());
        delete_index(&shared_db, "doomed").await.unwrap();
        assert!(PrefixedKvStore::new(shared_db.clone(), "doomed").unwrap().scan_all().await.unwrap().is_empty());
        shared_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_index_fails_for_missing() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
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