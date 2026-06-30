use object_store::ObjectStore;
use seekpower_error::{Result, SeekPowerError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use slatedb::{DbTransaction, IsolationLevel};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// KV store wrapper around SlateDB.
///
/// A simple key-value store with CRUD operations and batch writes.
/// All keys are stored as-is without any prefixing.
pub struct KvStore {
    db: Arc<slatedb::Db>,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self { db: self.db.clone() }
    }
}

impl KvStore {
    /// Open or create a SlateDB database at the given path.
    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        let db = slatedb::Db::open(path, object_store)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Failed to open database: {}", e)))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Put a key-value pair.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Put failed: {}", e)))?;
        Ok(())
    }

    /// Get a value by key. Returns None if key doesn't exist.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let result = self
            .db
            .get(key)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Get failed: {}", e)))?;
        Ok(result.map(|b| b.to_vec()))
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.db
            .delete(key)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Delete failed: {}", e)))?;
        Ok(())
    }

    /// Scan all entries with the given prefix.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self
            .db
            .scan_prefix(prefix)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Scan failed: {}", e)))?;

        let mut results = Vec::new();
        while let Some(entry) = iter
            .next()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Iterator failed: {}", e)))?
        {
            results.push((entry.key.to_vec(), entry.value.to_vec()));
        }
        Ok(results)
    }

    /// Scan all entries in the database.
    pub async fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(b"").await
    }

    /// Begin a transaction for batch writes.
    pub async fn begin_transaction(&self) -> Result<Transaction> {
        let txn = self
            .db
            .begin(IsolationLevel::default())
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Begin transaction failed: {}", e)))?;
        Ok(Transaction { txn })
    }

    /// Put a serialized value.
    pub async fn put_serialized<T: Serialize>(&self, key: &[u8], value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| SeekPowerError::StorageError(format!("Serialization failed: {}", e)))?;
        self.put(key, &bytes).await
    }

    /// Get and deserialize a value.
    pub async fn get_deserialized<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
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

    /// Close the database.
    pub async fn close(&self) -> Result<()> {
        self.db
            .close()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Close failed: {}", e)))
    }
}

/// A transaction for atomic batch writes.
pub struct Transaction {
    txn: DbTransaction,
}

impl Transaction {
    /// Put a key-value pair in the transaction.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn
            .put(key, value)
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction put failed: {}", e)))
    }

    /// Delete a key in the transaction.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.txn
            .delete(key)
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction delete failed: {}", e)))
    }

    /// Commit the transaction.
    pub async fn commit(self) -> Result<()> {
        self.txn
            .commit()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction commit failed: {}", e)))?;
        Ok(())
    }

    /// Rollback the transaction.
    pub fn rollback(self) -> Result<()> {
        self.txn.rollback();
        Ok(())
    }
}

/// Generic LRU cache for key-value data.
///
/// Re-exported as `ReadCache` for backward compatibility.
pub struct Cache {
    inner: RwLock<CacheInner>,
}

/// Alias for `Cache` for backward compatibility.
pub type ReadCache = Cache;

struct CacheInner {
    data: HashMap<Vec<u8>, Vec<u8>>,
    size: usize,
    max_bytes: usize,
}

impl Cache {
    /// Create a new cache with the given size limit (in bytes).
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: RwLock::new(CacheInner {
                data: HashMap::new(),
                size: 0,
                max_bytes,
            }),
        }
    }

    /// Get a value from cache.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let inner = self.inner.read().ok()?;
        inner.data.get(key).cloned()
    }

    /// Insert a value into cache.
    pub fn insert(&self, key: &[u8], value: Vec<u8>) {
        let mut inner = match self.inner.write() {
            Ok(i) => i,
            Err(_) => return,
        };

        // Remove old entry if exists
        if let Some(old_value) = inner.data.remove(key) {
            inner.size -= old_value.len();
        }

        // Evict if necessary (evict half the entries)
        if inner.size + value.len() > inner.max_bytes {
            let keys_to_remove: Vec<Vec<u8>> = inner.data.keys()
                .take(inner.data.len() / 2)
                .cloned()
                .collect();
            for k in keys_to_remove {
                if let Some(v) = inner.data.remove(&k) {
                    inner.size -= v.len();
                }
            }
        }

        inner.size += value.len();
        inner.data.insert(key.to_vec(), value);
    }

    /// Invalidate a key from cache.
    pub fn invalidate(&self, key: &[u8]) {
        if let Ok(mut inner) = self.inner.write() {
            if let Some(value) = inner.data.remove(key) {
                inner.size -= value.len();
            }
        }
    }

    /// Clear all cache entries.
    pub fn clear(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.data.clear();
            inner.size = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u64,
        name: String,
    }

    #[tokio::test]
    async fn test_kvstore_put_get_delete() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();

        // Test put and get
        store.put(b"key1", b"value1").await.unwrap();
        let result = store.get(b"key1").await.unwrap();
        assert_eq!(result, Some(b"value1".to_vec()));

        // Test delete
        store.delete(b"key1").await.unwrap();
        let result = store.get(b"key1").await.unwrap();
        assert_eq!(result, None);

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_scan_prefix() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();

        // Insert multiple entries with same prefix
        store.put(b"prefix:key1", b"value1").await.unwrap();
        store.put(b"prefix:key2", b"value2").await.unwrap();
        store.put(b"prefix:key3", b"value3").await.unwrap();
        store.put(b"other:key4", b"value4").await.unwrap();

        // Scan with prefix
        let results = store.scan_prefix(b"prefix:").await.unwrap();
        assert_eq!(results.len(), 3);

        // Verify all results have the correct prefix
        for (key, _) in &results {
            assert!(key.starts_with(b"prefix:"));
        }

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_transaction() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();

        // Start a transaction
        let txn = store.begin_transaction().await.unwrap();

        // Do multiple operations
        txn.put(b"key1", b"value1").unwrap();
        txn.put(b"key2", b"value2").unwrap();
        txn.put(b"key3", b"value3").unwrap();
        txn.delete(b"key2").unwrap();

        // Commit
        txn.commit().await.unwrap();

        // Verify results
        assert_eq!(store.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").await.unwrap(), None);
        assert_eq!(store.get(b"key3").await.unwrap(), Some(b"value3".to_vec()));

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_transaction_rollback() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();

        // Add initial data
        store.put(b"key1", b"old_value").await.unwrap();

        // Start a transaction
        let txn = store.begin_transaction().await.unwrap();
        txn.put(b"key1", b"new_value").unwrap();
        txn.put(b"key2", b"value2").unwrap();

        // Rollback
        txn.rollback().unwrap();

        // Verify data unchanged
        assert_eq!(store.get(b"key1").await.unwrap(), Some(b"old_value".to_vec()));
        assert_eq!(store.get(b"key2").await.unwrap(), None);

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_serialization() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();

        let data = TestData {
            id: 42,
            name: "test".to_string(),
        };

        // Test put_serialized and get_deserialized
        store.put_serialized(b"test_key", &data).await.unwrap();
        let result: Option<TestData> = store.get_deserialized(b"test_key").await.unwrap();
        assert_eq!(result, Some(data));

        // Test non-existent key
        let result: Option<TestData> = store.get_deserialized(b"nonexistent").await.unwrap();
        assert_eq!(result, None);

        store.close().await.unwrap();
    }

    #[test]
    fn test_cache_insert_get() {
        let cache = Cache::new(1024);

        cache.insert(b"key1", b"value1".to_vec());
        assert_eq!(cache.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(cache.get(b"nonexistent"), None);
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = Cache::new(1024);

        cache.insert(b"key1", b"value1".to_vec());
        cache.invalidate(b"key1");
        assert_eq!(cache.get(b"key1"), None);
    }

    #[test]
    fn test_cache_eviction() {
        let cache = Cache::new(1000);

        // Insert data that will trigger eviction
        cache.insert(b"key1", vec![0u8; 600]);
        cache.insert(b"key2", vec![0u8; 600]);

        // Insert more to trigger eviction
        cache.insert(b"key3", vec![0u8; 600]);

        // At least one of key1 or key2 should be evicted
        let key1_present = cache.get(b"key1").is_some();
        let key2_present = cache.get(b"key2").is_some();
        let key3_present = cache.get(b"key3").is_some();

        assert!(key3_present);
        assert!(!key1_present || !key2_present);
    }

    #[test]
    fn test_cache_clear() {
        let cache = Cache::new(1024);

        cache.insert(b"key1", b"value1".to_vec());
        cache.insert(b"key2", b"value2".to_vec());

        cache.clear();

        assert_eq!(cache.get(b"key1"), None);
        assert_eq!(cache.get(b"key2"), None);
    }

    #[tokio::test]
    async fn test_kvstore_clone_shares_underlying() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        let store_clone = store.clone();

        store.put(b"x", b"1").await.unwrap();
        assert_eq!(store_clone.get(b"x").await.unwrap(), Some(b"1".to_vec()));

        store.close().await.unwrap();
    }
}