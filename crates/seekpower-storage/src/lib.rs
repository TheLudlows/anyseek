use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use object_store::ObjectStore;
use seekpower_error::{Result, SeekPowerError};
use serde::de::DeserializeOwned;
use serde::Serialize;

// Key prefix constants
pub const PREFIX_META: &[u8] = b"m:";
pub const PREFIX_IDX: &[u8] = b"i:";
pub const PREFIX_VEC: &[u8] = b"v:";
pub const PREFIX_DOC: &[u8] = b"d:";
pub const PREFIX_DELL: &[u8] = b"l:";
pub const PREFIX_LEN: &[u8] = b"s:";

/// Construct a key from multiple parts
pub fn make_key(parts: &[&[u8]]) -> Vec<u8> {
    let mut key = Vec::new();
    for part in parts {
        key.extend_from_slice(part);
    }
    key
}

/// KV store wrapper around SlateDB
pub struct KvStore {
    db: slatedb::Db,
}

impl KvStore {
    /// Open or create a SlateDB database at the given path
    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        let db = slatedb::Db::open(path, object_store)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Failed to open database: {}", e)))?;

        Ok(Self { db })
    }

    /// Put a key-value pair
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Put failed: {}", e)))?;
        Ok(())
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let result = self
            .db
            .get(key)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Get failed: {}", e)))?;
        Ok(result.map(|b| b.to_vec()))
    }

    /// Delete a key
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.db
            .delete(key)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Delete failed: {}", e)))?;
        Ok(())
    }

    /// Scan all entries with the given prefix
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results = Vec::new();
        let mut iter = self
            .db
            .scan_prefix(prefix)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Scan failed: {}", e)))?;

        while let Some(entry) = iter
            .next()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Iterator failed: {}", e)))?
        {
            results.push((entry.key.to_vec(), entry.value.to_vec()));
        }

        Ok(results)
    }

    /// Put a serialized value
    pub async fn put_serialized<T: Serialize>(&self, key: &[u8], value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| SeekPowerError::StorageError(format!("Serialization failed: {}", e)))?;
        self.put(key, &bytes).await
    }

    /// Get and deserialize a value
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

    /// Close the database
    pub async fn close(&self) -> Result<()> {
        self.db
            .close()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Close failed: {}", e)))
    }
}

/// Read-through LRU cache for posting lists and cluster data
pub struct ReadCache {
    posting_cache: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    cluster_cache: RwLock<HashMap<Vec<u8>, Vec<u8>>>,
    posting_cache_size: RwLock<usize>,
    cluster_cache_size: RwLock<usize>,
    posting_max_bytes: usize,
    cluster_max_bytes: usize,
}

impl ReadCache {
    /// Create a new read cache with the given size limits (in MB)
    pub fn new(posting_max_mb: usize, cluster_max_mb: usize) -> Self {
        Self {
            posting_cache: RwLock::new(HashMap::new()),
            cluster_cache: RwLock::new(HashMap::new()),
            posting_cache_size: RwLock::new(0),
            cluster_cache_size: RwLock::new(0),
            posting_max_bytes: posting_max_mb * 1024 * 1024,
            cluster_max_bytes: cluster_max_mb * 1024 * 1024,
        }
    }

    /// Get a posting list from cache
    pub fn get_posting(&self, key: &[u8]) -> Option<Vec<u8>> {
        let cache = self.posting_cache.read().ok()?;
        cache.get(key).cloned()
    }

    /// Insert a posting list into cache
    pub fn insert_posting(&self, key: &[u8], value: Vec<u8>) {
        let mut cache = match self.posting_cache.write() {
            Ok(c) => c,
            Err(_) => return,
        };
        let mut size = match self.posting_cache_size.write() {
            Ok(s) => s,
            Err(_) => return,
        };

        // Remove old entry if exists
        if let Some(old_value) = cache.remove(key) {
            *size -= old_value.len();
        }

        // Check if we need to evict
        if *size + value.len() > self.posting_max_bytes {
            // Evict half the entries
            let keys_to_remove: Vec<Vec<u8>> = cache.keys().take(cache.len() / 2).cloned().collect();
            for k in keys_to_remove {
                if let Some(v) = cache.remove(&k) {
                    *size -= v.len();
                }
            }
        }

        // Insert new entry
        *size += value.len();
        cache.insert(key.to_vec(), value);
    }

    /// Get cluster data from cache
    pub fn get_cluster(&self, key: &[u8]) -> Option<Vec<u8>> {
        let cache = self.cluster_cache.read().ok()?;
        cache.get(key).cloned()
    }

    /// Insert cluster data into cache
    pub fn insert_cluster(&self, key: &[u8], value: Vec<u8>) {
        let mut cache = match self.cluster_cache.write() {
            Ok(c) => c,
            Err(_) => return,
        };
        let mut size = match self.cluster_cache_size.write() {
            Ok(s) => s,
            Err(_) => return,
        };

        // Remove old entry if exists
        if let Some(old_value) = cache.remove(key) {
            *size -= old_value.len();
        }

        // Check if we need to evict
        if *size + value.len() > self.cluster_max_bytes {
            // Evict half the entries
            let keys_to_remove: Vec<Vec<u8>> = cache.keys().take(cache.len() / 2).cloned().collect();
            for k in keys_to_remove {
                if let Some(v) = cache.remove(&k) {
                    *size -= v.len();
                }
            }
        }

        // Insert new entry
        *size += value.len();
        cache.insert(key.to_vec(), value);
    }

    /// Invalidate a key from all caches
    pub fn invalidate(&self, key: &[u8]) {
        if let Ok(mut cache) = self.posting_cache.write() {
            if let Some(value) = cache.remove(key) {
                if let Ok(mut size) = self.posting_cache_size.write() {
                    *size -= value.len();
                }
            }
        }
        if let Ok(mut cache) = self.cluster_cache.write() {
            if let Some(value) = cache.remove(key) {
                if let Ok(mut size) = self.cluster_cache_size.write() {
                    *size -= value.len();
                }
            }
        }
    }

    /// Clear all caches
    pub fn clear(&self) {
        if let Ok(mut cache) = self.posting_cache.write() {
            cache.clear();
        }
        if let Ok(mut size) = self.posting_cache_size.write() {
            *size = 0;
        }
        if let Ok(mut cache) = self.cluster_cache.write() {
            cache.clear();
        }
        if let Ok(mut size) = self.cluster_cache_size.write() {
            *size = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u64,
        name: String,
    }

    #[tokio::test]
    async fn test_kvstore_put_get_delete() {
        let object_store = Arc::new(InMemory::new());
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
        let object_store = Arc::new(InMemory::new());
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
    async fn test_kvstore_serialization() {
        let object_store = Arc::new(InMemory::new());
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
    fn test_readcache_posting() {
        let cache = ReadCache::new(1, 1); // 1 MB each

        // Test insert and get
        cache.insert_posting(b"key1", b"value1".to_vec());
        let result = cache.get_posting(b"key1");
        assert_eq!(result, Some(b"value1".to_vec()));

        // Test non-existent key
        let result = cache.get_posting(b"nonexistent");
        assert_eq!(result, None);

        // Test invalidate
        cache.invalidate(b"key1");
        let result = cache.get_posting(b"key1");
        assert_eq!(result, None);
    }

    #[test]
    fn test_readcache_cluster() {
        let cache = ReadCache::new(1, 1);

        // Test insert and get
        cache.insert_cluster(b"key1", b"value1".to_vec());
        let result = cache.get_cluster(b"key1");
        assert_eq!(result, Some(b"value1".to_vec()));

        // Test invalidate
        cache.invalidate(b"key1");
        let result = cache.get_cluster(b"key1");
        assert_eq!(result, None);
    }

    #[test]
    fn test_readcache_eviction() {
        let cache = ReadCache::new(1, 1); // 1 MB limit

        // Insert enough data to trigger eviction
        let large_value = vec![0u8; 600 * 1024]; // 600 KB
        cache.insert_posting(b"key1", large_value.clone());
        cache.insert_posting(b"key2", large_value.clone());

        // Both should be present
        assert!(cache.get_posting(b"key1").is_some());
        assert!(cache.get_posting(b"key2").is_some());

        // Insert another large value to trigger eviction
        cache.insert_posting(b"key3", large_value.clone());

        // At least one of the first two should be evicted
        let key1_present = cache.get_posting(b"key1").is_some();
        let key2_present = cache.get_posting(b"key2").is_some();
        let key3_present = cache.get_posting(b"key3").is_some();

        // key3 should definitely be present
        assert!(key3_present);
        // At least one of key1 or key2 should be evicted
        assert!(!key1_present || !key2_present);
    }

    #[test]
    fn test_readcache_clear() {
        let cache = ReadCache::new(1, 1);

        cache.insert_posting(b"key1", b"value1".to_vec());
        cache.insert_cluster(b"key2", b"value2".to_vec());

        cache.clear();

        assert_eq!(cache.get_posting(b"key1"), None);
        assert_eq!(cache.get_cluster(b"key2"), None);
    }

    #[test]
    fn test_make_key() {
        let key = make_key(&[PREFIX_META, b"index1", b":config"]);
        assert_eq!(key, b"m:index1:config");

        let key = make_key(&[PREFIX_VEC, b"123"]);
        assert_eq!(key, b"v:123");
    }
}
