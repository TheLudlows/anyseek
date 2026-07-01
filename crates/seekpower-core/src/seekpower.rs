use std::collections::HashMap;
use std::sync::Arc;

use seekpower_error::{Result, SeekPowerError};
use seekpower_schema::{IndexConfig, SchemaField, validate_name};
use seekpower_storage::KvStore;

use crate::index::{self, Index};

const REGISTRY_PREFIX: &[u8] = b"__indexes__:";

/// Top-level entry point for the SeekPower search engine.
///
/// Owns the shared `KvStore`, a globally auto-incrementing index ID counter,
/// and a cache of open indexes. All index lifecycle operations go through this struct.
pub struct SeekPower {
    db: KvStore,
    next_index_id: std::sync::atomic::AtomicU32,
    indexes: tokio::sync::RwLock<HashMap<String, Arc<Index>>>,
}

impl SeekPower {
    /// Open or create a SeekPower engine backed by a SlateDB database.
    ///
    /// Scans existing indexes to recover the maximum `index_id` and initializes
    /// the auto-increment counter to `max_id + 1`.
    pub async fn open(path: &str, object_store: Arc<dyn object_store::ObjectStore>) -> Result<Self> {
        let db = KvStore::open(path, object_store).await?;

        // Scan registry to find the maximum existing index_id
        let entries = db.scan_prefix(REGISTRY_PREFIX).await?;
        let mut max_id: u32 = 0;
        for (_key, value) in entries {
            if value.len() == 4 {
                let id_bytes: [u8; 4] = value.try_into().unwrap();
                let id = u32::from_be_bytes(id_bytes);
                if id > max_id {
                    max_id = id;
                }
            }
        }

        Ok(Self {
            db,
            next_index_id: std::sync::atomic::AtomicU32::new(max_id + 1),
            indexes: tokio::sync::RwLock::new(HashMap::new()),
        })
    }

    /// Close the underlying database.
    pub async fn close(&self) -> Result<()> {
        self.db.close().await
    }

    /// Create a new index with the given name, config, and schema.
    ///
    /// Allocates a globally unique `index_id`, writes metadata to KV,
    /// registers the index name→id mapping, and caches it.
    /// Returns `Arc<Index>` for shared access.
    pub async fn create_index(
        &self,
        name: &str,
        config: IndexConfig,
        schema: Vec<SchemaField>,
    ) -> Result<Arc<Index>> {
        validate_name(name).map_err(|e| SeekPowerError::InvalidIndexName(e))?;

        // Check if already loaded
        {
            let guard = self.indexes.read().await;
            if guard.contains_key(name) {
                return Err(SeekPowerError::StorageError(format!("Index '{}' already exists", name)));
            }
        }

        // Check if already exists in registry
        if let Some(_) = self.db.get(&registry_key(name)).await? {
            return Err(SeekPowerError::StorageError(format!("Index '{}' already exists", name)));
        }

        // Allocate a globally unique index_id
        let index_id = self.next_index_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let idx = index::create_index_with_id(self.db.clone(), index_id, name, config, schema).await?;
        let idx = Arc::new(idx);

        // Register in KV: store the allocated index_id (u32 BE)
        self.db.put(&registry_key(name), &index_id.to_be_bytes()).await?;

        // Cache
        self.indexes.write().await.insert(name.to_string(), idx.clone());

        Ok(idx)
    }

    /// Open an existing index by name.
    ///
    /// Returns a cached `Arc<Index>` if already loaded, otherwise loads from KV
    /// by looking up the allocated `index_id` in the registry.
    pub async fn open_index(&self, name: &str) -> Result<Arc<Index>> {
        validate_name(name).map_err(|e| SeekPowerError::InvalidIndexName(e))?;

        // Check cache
        {
            let guard = self.indexes.read().await;
            if let Some(idx) = guard.get(name) {
                return Ok(idx.clone());
            }
        }

        // Read index_id from registry
        let key = registry_key(name);
        let id_bytes = self.db.get(&key).await?
            .ok_or_else(|| SeekPowerError::IndexNotFound(format!("Index '{}' not found in registry", name)))?;
        let id_bytes: [u8; 4] = id_bytes.try_into()
            .map_err(|_| SeekPowerError::StorageError(format!("Invalid index_id for '{}'", name)))?;
        let index_id = u32::from_be_bytes(id_bytes);

        let idx = index::open_index_with_id(self.db.clone(), index_id, name).await?;
        let idx = Arc::new(idx);

        self.indexes.write().await.insert(name.to_string(), idx.clone());

        Ok(idx)
    }

    /// Get a cached index by name without triggering IO.
    /// Returns `None` if the index is not currently loaded.
    pub fn get_index(&self, name: &str) -> Option<Arc<Index>> {
        // Use try_read to avoid blocking; if contended, return None.
        match self.indexes.try_read() {
            Ok(guard) => guard.get(name).cloned(),
            Err(_) => None,
        }
    }

    /// Delete an index by name.
    ///
    /// Removes it from the cache, reads the index_id from registry,
    /// deletes all KV data for that index_id, and removes the registry entry.
    pub async fn delete_index(&self, name: &str) -> Result<()> {
        validate_name(name).map_err(|e| SeekPowerError::InvalidIndexName(e))?;

        // Read index_id from registry before deleting
        let key = registry_key(name);
        let id_bytes = self.db.get(&key).await?
            .ok_or_else(|| SeekPowerError::IndexNotFound(format!("Index '{}' not found in registry", name)))?;
        let id_bytes: [u8; 4] = id_bytes.try_into()
            .map_err(|_| SeekPowerError::StorageError(format!("Invalid index_id for '{}'", name)))?;
        let index_id = u32::from_be_bytes(id_bytes);

        // Remove from cache
        self.indexes.write().await.remove(name);

        // Delete KV data for the index_id
        index::delete_index_with_id(&self.db, index_id).await?;

        // Remove registry entry
        self.db.delete(&key).await?;

        Ok(())
    }

    /// List all registered index names by scanning the KV registry.
    pub async fn index_names(&self) -> Result<Vec<String>> {
        let entries = self.db.scan_prefix(REGISTRY_PREFIX).await?;
        let mut names = Vec::new();
        for (key, _) in entries {
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if let Some(name) = key_str.strip_prefix("__indexes__:") {
                    names.push(name.to_string());
                }
            }
        }
        Ok(names)
    }

    /// Get a reference to the underlying KvStore.
    pub fn db(&self) -> &KvStore {
        &self.db
    }
}

fn registry_key(name: &str) -> Vec<u8> {
    format!("{}{}", unsafe { std::str::from_utf8_unchecked(REGISTRY_PREFIX) }, name).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use seekpower_schema::SchemaField;

    fn test_schema() -> Vec<SchemaField> {
        vec![
            SchemaField::text("title"),
            SchemaField::text("body").with_boost(0.5).with_store(true),
        ]
    }

    #[tokio::test]
    async fn test_create_and_get_index() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let engine = SeekPower::open("test-create", store).await.unwrap();

        let idx = engine.create_index("articles", IndexConfig::default(), test_schema()).await.unwrap();
        assert_eq!(idx.schema.len(), 2);

        // get_index should return the cached instance
        let cached = engine.get_index("articles").unwrap();
        assert!(Arc::ptr_eq(&idx, &cached));

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_open_index_uses_cache() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let engine = SeekPower::open("test-cache", store).await.unwrap();

        let idx1 = engine.create_index("docs", IndexConfig::default(), test_schema()).await.unwrap();
        let idx2 = engine.open_index("docs").await.unwrap();
        assert!(Arc::ptr_eq(&idx1, &idx2));

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_index() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let engine = SeekPower::open("test-delete", store).await.unwrap();

        engine.create_index("doomed", IndexConfig::default(), test_schema()).await.unwrap();
        engine.delete_index("doomed").await.unwrap();

        assert!(engine.get_index("doomed").is_none());
        assert!(engine.open_index("doomed").await.is_err());

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_index_names() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let engine = SeekPower::open("test-names", store).await.unwrap();

        engine.create_index("alpha", IndexConfig::default(), test_schema()).await.unwrap();
        engine.create_index("beta", IndexConfig::default(), test_schema()).await.unwrap();

        let mut names = engine.index_names().await.unwrap();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_create_duplicate_fails() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let engine = SeekPower::open("test-dup", store).await.unwrap();

        engine.create_index("dup", IndexConfig::default(), test_schema()).await.unwrap();
        let result = engine.create_index("dup", IndexConfig::default(), test_schema()).await;
        assert!(result.is_err());

        engine.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_two_indexes_isolated() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let engine = SeekPower::open("test-isolated", store).await.unwrap();

        let a = engine.create_index("a", IndexConfig::default(), test_schema()).await.unwrap();
        let b = engine.create_index("b", IndexConfig::default(), test_schema()).await.unwrap();

        let mut doc = std::collections::HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("hello"));
        crate::document::index_document(&a, doc.clone()).await.unwrap();
        crate::document::index_document(&b, doc).await.unwrap();
        crate::commit::commit(&a).await.unwrap();
        crate::commit::commit(&b).await.unwrap();

        engine.close().await.unwrap();
    }
}
