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

const PATH_MAX_LEN: usize = 128;

pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(SeekPowerError::InvalidIndexName(
            "index name must not be empty".into(),
        ));
    }
    if path.len() > PATH_MAX_LEN {
        return Err(SeekPowerError::InvalidIndexName(format!(
            "index name too long: {} > {} chars",
            path.len(),
            PATH_MAX_LEN
        )));
    }
    if !path.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
        return Err(SeekPowerError::InvalidIndexName(format!(
            "index name must match [a-zA-Z0-9_-]+: {:?}",
            path
        )));
    }
    Ok(())
}

/// Construct a key from multiple parts
pub fn make_key(parts: &[&[u8]]) -> Vec<u8> {
    let mut key = Vec::new();
    for part in parts {
        key.extend_from_slice(part);
    }
    key
}

/// KV store wrapper around SlateDB.
///
/// A `KvStore` is either a "raw" view over a freshly opened `slatedb::Db`, or
/// a derived view with a path prefix. Derived views share the underlying
/// `slatedb::Db` via `Arc` — cloning a derived view is cheap.
pub struct KvStore {
    inner: Arc<KvStoreInner>,
}

struct KvStoreInner {
    db: slatedb::Db,
    prefix: Vec<u8>,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

fn join_prefix(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + key.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(key);
    out
}

impl KvStore {
    /// Open or create a SlateDB database at the given path. The returned
    /// `KvStore` is the "raw" view with no prefix.
    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        let db = slatedb::Db::open(path, object_store)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Failed to open database: {}", e)))?;
        Ok(Self {
            inner: Arc::new(KvStoreInner { db, prefix: Vec::new() }),
        })
    }

    /// Derive a new view that auto-prepends `path:` to every key, sharing
    /// the same underlying `slatedb::Db` as `self`. Panics if `path` is not
    /// a valid index name (see `validate_path`).
    pub fn for_index(&self, path: &str) -> Self {
        validate_path(path).expect("invalid index name");
        let mut prefix = Vec::with_capacity(path.len() + 1);
        prefix.extend_from_slice(path.as_bytes());
        prefix.push(b':');
        Self {
            inner: Arc::new(KvStoreInner {
                db: self.inner.db.clone(),
                prefix,
            }),
        }
    }

    /// Returns true if this view has no prefix (the original opened view).
    pub fn is_raw(&self) -> bool {
        self.inner.prefix.is_empty()
    }

    /// Put a key-value pair
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let full = join_prefix(&self.inner.prefix, key);
        self.inner
            .db
            .put(&full, value)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Put failed: {}", e)))?;
        Ok(())
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let full = join_prefix(&self.inner.prefix, key);
        let result = self
            .inner
            .db
            .get(&full)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Get failed: {}", e)))?;
        Ok(result.map(|b| b.to_vec()))
    }

    /// Delete a key
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let full = join_prefix(&self.inner.prefix, key);
        self.inner
            .db
            .delete(&full)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Delete failed: {}", e)))?;
        Ok(())
    }

    /// Scan all entries with the given prefix. The returned keys have the
    /// view's own prefix stripped, so callers see the same logical keys
    /// regardless of which view they used.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let full_prefix = join_prefix(&self.inner.prefix, prefix);
        let mut iter = self
            .inner
            .db
            .scan_prefix(&full_prefix)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Scan failed: {}", e)))?;

        let strip_len = self.inner.prefix.len();
        let mut results = Vec::new();
        while let Some(entry) = iter
            .next()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Iterator failed: {}", e)))?
        {
            let raw_key = entry.key.to_vec();
            let stripped = if strip_len <= raw_key.len() {
                raw_key[strip_len..].to_vec()
            } else {
                raw_key
            };
            results.push((stripped, entry.value.to_vec()));
        }
        Ok(results)
    }

    /// List all entries under this view (equivalent to `scan_prefix(b"")`).
    pub async fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(b"").await
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

    /// Close the database. Only the raw (un-prefixed) view may close the
    /// underlying SlateDB instance.
    pub async fn close(&self) -> Result<()> {
        if !self.inner.prefix.is_empty() {
            return Err(SeekPowerError::StorageError(
                "close() must be called on the raw KvStore".into(),
            ));
        }
        self.inner
            .db
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

    #[test]
    fn test_validate_path_accepts_valid() {
        assert!(validate_path("blog").is_ok());
        assert!(validate_path("shop").is_ok());
        assert!(validate_path("a").is_ok());
        assert!(validate_path("foo_bar-baz123").is_ok());
        assert!(validate_path("__indexes__").is_ok());
        assert!(validate_path(&"a".repeat(128)).is_ok());
    }

    #[test]
    fn test_validate_path_rejects_empty() {
        assert!(validate_path("").is_err());
    }

    #[test]
    fn test_validate_path_rejects_too_long() {
        assert!(validate_path(&"a".repeat(129)).is_err());
    }

    #[test]
    fn test_validate_path_rejects_bad_chars() {
        assert!(validate_path("foo/bar").is_err());
        assert!(validate_path("foo:bar").is_err());
        assert!(validate_path("foo bar").is_err());
        assert!(validate_path("foo.bar").is_err());
        assert!(validate_path("foo@bar").is_err());
        assert!(validate_path("föo").is_err());
    }

    #[tokio::test]
    async fn test_kvstore_for_index_isolates_keys() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let a = db.for_index("a");
        let b = db.for_index("b");

        a.put(b"m:schema", b"schema-a").await.unwrap();
        b.put(b"m:schema", b"schema-b").await.unwrap();

        assert_eq!(a.get(b"m:schema").await.unwrap(), Some(b"schema-a".to_vec()));
        assert_eq!(b.get(b"m:schema").await.unwrap(), Some(b"schema-b".to_vec()));
        // raw view sees neither
        assert!(db.get(b"m:schema").await.unwrap().is_none());

        a.close().await.unwrap_err(); // derived view cannot close
    }

    #[tokio::test]
    async fn test_kvstore_scan_prefix_strips_view_prefix() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let view = db.for_index("blog");
        view.put(b"m:schema", b"s").await.unwrap();
        view.put(b"m:config", b"c").await.unwrap();
        view.put(b"i:1", b"posting").await.unwrap();

        let mut results = view.scan_prefix(b"m:").await.unwrap();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(results, vec![
            (b"m:config".to_vec(), b"c".to_vec()),
            (b"m:schema".to_vec(), b"s".to_vec()),
        ]);
    }

    #[tokio::test]
    async fn test_kvstore_scan_all_lists_index_data() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let view = db.for_index("blog");
        view.put(b"m:schema", b"s").await.unwrap();
        view.put(b"i:1", b"posting").await.unwrap();

        let all = view.scan_all().await.unwrap();
        assert_eq!(all.len(), 2);
        let keys: Vec<&Vec<u8>> = all.iter().map(|(k, _)| k).collect();
        assert!(keys.contains(&&b"m:schema".to_vec()));
        assert!(keys.contains(&&b"i:1".to_vec()));
    }

    #[tokio::test]
    async fn test_kvstore_clone_shares_underlying() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let a = db.for_index("a");
        let a_clone = a.clone();
        a.put(b"x", b"1").await.unwrap();
        assert_eq!(a_clone.get(b"x").await.unwrap(), Some(b"1".to_vec()));
    }

    #[tokio::test]
    async fn test_kvstore_close_only_on_raw() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let view = db.for_index("a");
        assert!(!view.is_raw());
        assert!(db.is_raw());
        assert!(view.close().await.is_err());
        db.close().await.unwrap();
    }
}
