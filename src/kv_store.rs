use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use slatedb::{self, bytes::Bytes};
use std::sync::Arc;

use crate::error::{Result, StormDbError};

// Key prefix constants for namespacing different data types in the KV store.
pub const PREFIX_META: &[u8] = b"m:";
pub const PREFIX_IDX: &[u8] = b"i:";
pub const PREFIX_VEC: &[u8] = b"v:";
pub const PREFIX_DOC: &[u8] = b"d:";
pub const PREFIX_FACET: &[u8] = b"f:";
pub const PREFIX_DELL: &[u8] = b"l:";
pub const PREFIX_LEN: usize = 2;

/// A key-value store backed by SlateDB.
///
/// Wraps `slatedb::Db` and provides higher-level async methods
/// for the storm-db project.
pub struct KvStore {
    db: Arc<slatedb::Db>,
}

impl KvStore {
    /// Open a new KvStore at the given path using the provided object store.
    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        let db = slatedb::Db::open(path, object_store)
            .await
            .map_err(|e| StormDbError::StorageError(format!("failed to open SlateDB: {e}")))?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Put a key-value pair into the store.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .await
            .map_err(|e| StormDbError::StorageError(format!("SlateDB put failed: {e}")))?;
        Ok(())
    }

    /// Get a value by key. Returns `Ok(None)` if the key does not exist.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.db
            .get(key)
            .await
            .map_err(|e| StormDbError::StorageError(format!("SlateDB get failed: {e}")))
    }

    /// Delete a key from the store.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.db
            .delete(key)
            .await
            .map_err(|e| StormDbError::StorageError(format!("SlateDB delete failed: {e}")))?;
        Ok(())
    }

    /// Scan all keys with the given prefix and return the full list of key-value pairs.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Bytes, Bytes)>> {
        let mut iter = self
            .db
            .scan_prefix(prefix)
            .await
            .map_err(|e| StormDbError::StorageError(format!("SlateDB scan_prefix failed: {e}")))?;
        let mut results = Vec::new();
        while let Some(kv) = iter
            .next()
            .await
            .map_err(|e| StormDbError::StorageError(format!("SlateDB iterator next failed: {e}")))?
        {
            results.push((kv.key, kv.value));
        }
        Ok(results)
    }

    /// Serialize a value and put it into the store.
    pub async fn put_serialized(&self, key: &[u8], value: &impl Serialize) -> Result<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| StormDbError::StorageError(format!("serialization failed: {e}")))?;
        self.put(key, &bytes).await
    }

    /// Get a value by key and deserialize it. Returns `Ok(None)` if the key does not exist.
    pub async fn get_deserialized<T: for<'de> Deserialize<'de>>(&self, key: &[u8]) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(b) => {
                let value: T = serde_json::from_slice(&b)
                    .map_err(|e| StormDbError::StorageError(format!("deserialization failed: {e}")))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Close the database, flushing all pending writes.
    pub async fn close(&self) -> Result<()> {
        self.db
            .close()
            .await
            .map_err(|e| StormDbError::StorageError(format!("SlateDB close failed: {e}")))?;
        Ok(())
    }
}

/// Build a composite key by concatenating byte-slice parts.
///
/// This is a convenience helper to assemble storage keys from multiple
/// segments, for example `make_key(PREFIX_IDX, b"my_index", b"posting_list_1")`.
pub fn make_key(parts: &[&[u8]]) -> Vec<u8> {
    let total_len: usize = parts.iter().map(|p| p.len()).sum();
    let mut key = Vec::with_capacity(total_len);
    for part in parts {
        key.extend_from_slice(part);
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::collections::HashMap;

    async fn new_test_store() -> KvStore {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        KvStore::open("/test", store).await.expect("failed to open test store")
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let kv = new_test_store().await;
        kv.put(b"key1", b"value1").await.unwrap();
        let val: Option<Bytes> = kv.get(b"key1").await.unwrap();
        assert_eq!(val, Some(Bytes::from("value1")));
        kv.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_nonexistent() {
        let kv = new_test_store().await;
        let val: Option<Bytes> = kv.get(b"nope").await.unwrap();
        assert!(val.is_none());
        kv.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete() {
        let kv = new_test_store().await;
        kv.put(b"delkey", b"delval").await.unwrap();
        kv.delete(b"delkey").await.unwrap();
        let val: Option<Bytes> = kv.get(b"delkey").await.unwrap();
        assert!(val.is_none());
        kv.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_scan_prefix() {
        let kv = new_test_store().await;
        kv.put(b"prefix:a", b"va").await.unwrap();
        kv.put(b"prefix:b", b"vb").await.unwrap();
        kv.put(b"other:c", b"vc").await.unwrap();

        let results = kv.scan_prefix(b"prefix:").await.unwrap();
        assert_eq!(results.len(), 2);

        let map: HashMap<_, _> = results.into_iter().collect();
        assert_eq!(map.get(&Bytes::from("prefix:a")), Some(&Bytes::from("va")));
        assert_eq!(map.get(&Bytes::from("prefix:b")), Some(&Bytes::from("vb")));
        assert!(!map.contains_key(&Bytes::from("other:c")));
        kv.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_put_serialized_and_get_deserialized() {
        let kv = new_test_store().await;
        let mut data: HashMap<String, String> = HashMap::new();
        data.insert("name".to_string(), "test".to_string());
        data.insert("kind".to_string(), "kv".to_string());
        kv.put_serialized(b"meta:cfg", &data).await.unwrap();

        let retrieved: Option<HashMap<String, String>> =
            kv.get_deserialized(b"meta:cfg").await.unwrap();
        assert_eq!(retrieved, Some(data));

        let missing: Option<HashMap<String, String>> =
            kv.get_deserialized(b"meta:missing").await.unwrap();
        assert!(missing.is_none());
        kv.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_make_key() {
        let key = make_key(&[PREFIX_IDX, b"idx1", b"posting"]);
        assert_eq!(&key, b"i:idx1posting");
    }

    #[tokio::test]
    async fn test_overwrite() {
        let kv = new_test_store().await;
        kv.put(b"key", b"old").await.unwrap();
        kv.put(b"key", b"new").await.unwrap();
        let val: Option<Bytes> = kv.get(b"key").await.unwrap();
        assert_eq!(val, Some(Bytes::from("new")));
        kv.close().await.unwrap();
    }
}
