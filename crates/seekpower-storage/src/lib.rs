//! seekpower-storage: SlateDB KV wrapper with prefix-based key isolation.

mod key;

pub use key::{encode_key, encode_prefix, encode_index_prefix, name_to_id};

use object_store::ObjectStore;
use seekpower_error::{Result, SeekPowerError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use slatedb::IsolationLevel;
use std::sync::Arc;

/// Thin wrapper around SlateDB providing CRUD + scan + transactions.
///
/// Index isolation is achieved by prefixing keys with `index_id` (u32 BE)
/// and `data_type` (u8). Physical key format:
///
/// ```text
/// index_id (4B BE) ‖ data_type (1B) ‖ business_key
/// ```
pub struct KvStore {
    db: Arc<slatedb::Db>,
}

/// Length of the fixed prefix: index_id (4B) + data_type (1B) = 5 bytes.
pub const PREFIX_LEN: usize = 5;

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

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Put failed: {}", e)))?;
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let result = self
            .db
            .get(key)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Get failed: {}", e)))?;
        Ok(result.map(|b| b.to_vec()))
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.db
            .delete(key)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Delete failed: {}", e)))?;
        Ok(())
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self
            .db
            .scan_prefix(prefix, ..)
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

    pub async fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(b"").await
    }

    pub async fn begin_transaction(&self) -> Result<Transaction> {
        let txn = self
            .db
            .begin(IsolationLevel::default())
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Begin transaction failed: {}", e)))?;
        Ok(Transaction { txn })
    }

    pub async fn put_serialized<T: Serialize>(&self, key: &[u8], value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| SeekPowerError::StorageError(format!("Serialization failed: {}", e)))?;
        self.put(key, &bytes).await
    }

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

    pub async fn close(&self) -> Result<()> {
        self.db
            .close()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Close failed: {}", e)))
    }

    // -----------------------------------------------------------------------
    // Prefixed convenience methods
    // Physical key: index_id (u32 BE) ‖ data_type (u8) ‖ business_key
    // -----------------------------------------------------------------------

    /// Put with index_id + data_type prefix.
    pub async fn prefixed_put(
        &self,
        index_id: u32,
        data_type: u8,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let full_key = encode_key(index_id, data_type, key);
        self.put(&full_key, value).await
    }

    /// Get with index_id + data_type prefix.
    pub async fn prefixed_get(
        &self,
        index_id: u32,
        data_type: u8,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let full_key = encode_key(index_id, data_type, key);
        self.get(&full_key).await
    }

    /// Delete with index_id + data_type prefix.
    pub async fn prefixed_delete(
        &self,
        index_id: u32,
        data_type: u8,
        key: &[u8],
    ) -> Result<()> {
        let full_key = encode_key(index_id, data_type, key);
        self.delete(&full_key).await
    }

    /// Scan within a given index_id + data_type, with an optional business_key prefix.
    /// Returned keys have the 5-byte fixed prefix (index_id + data_type) stripped.
    pub async fn prefixed_scan_prefix(
        &self,
        index_id: u32,
        data_type: u8,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let full_prefix = encode_prefix(index_id, data_type)
            .into_iter()
            .chain(prefix.iter().copied())
            .collect::<Vec<_>>();
        let results = self.scan_prefix(&full_prefix).await?;
        Ok(results
            .into_iter()
            .map(|(key, value)| (key[PREFIX_LEN..].to_vec(), value))
            .collect())
    }

    /// Scan all keys for a given index_id + data_type.
    /// Returned keys have the 5-byte fixed prefix stripped.
    pub async fn prefixed_scan_all(
        &self,
        index_id: u32,
        data_type: u8,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.prefixed_scan_prefix(index_id, data_type, b"").await
    }

    /// Put a serialized value with index_id + data_type prefix.
    pub async fn prefixed_put_serialized<T: Serialize>(
        &self,
        index_id: u32,
        data_type: u8,
        key: &[u8],
        value: &T,
    ) -> Result<()> {
        let full_key = encode_key(index_id, data_type, key);
        self.put_serialized(&full_key, value).await
    }

    /// Get and deserialize a value with index_id + data_type prefix.
    pub async fn prefixed_get_deserialized<T: DeserializeOwned>(
        &self,
        index_id: u32,
        data_type: u8,
        key: &[u8],
    ) -> Result<Option<T>> {
        let full_key = encode_key(index_id, data_type, key);
        self.get_deserialized(&full_key).await
    }

    /// Delete all keys for a given index_id (across all data_types).
    pub async fn prefixed_delete_all(&self, index_id: u32) -> Result<()> {
        let index_prefix = encode_index_prefix(index_id);
        let results = self.scan_prefix(&index_prefix).await?;
        for (key, _) in results {
            self.delete(&key).await?;
        }
        Ok(())
    }
}

/// A transaction for atomic batch writes.
pub struct Transaction {
    txn: slatedb::DbTransaction,
}

impl Transaction {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn
            .put(key, value)
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction put failed: {}", e)))
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.txn
            .delete(key)
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction delete failed: {}", e)))
    }

    pub async fn commit(self) -> Result<()> {
        self.txn
            .commit()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction commit failed: {}", e)))?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        self.txn.rollback();
        Ok(())
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
        store.put(b"key1", b"value1").await.unwrap();
        assert_eq!(store.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        store.delete(b"key1").await.unwrap();
        assert_eq!(store.get(b"key1").await.unwrap(), None);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_scan_prefix() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        store.put(b"prefix:key1", b"value1").await.unwrap();
        store.put(b"prefix:key2", b"value2").await.unwrap();
        store.put(b"prefix:key3", b"value3").await.unwrap();
        store.put(b"other:key4", b"value4").await.unwrap();
        let results = store.scan_prefix(b"prefix:").await.unwrap();
        assert_eq!(results.len(), 3);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_transaction() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        let txn = store.begin_transaction().await.unwrap();
        txn.put(b"key1", b"value1").unwrap();
        txn.put(b"key2", b"value2").unwrap();
        txn.delete(b"key2").unwrap();
        txn.commit().await.unwrap();
        assert_eq!(store.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2").await.unwrap(), None);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_transaction_rollback() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        store.put(b"key1", b"old_value").await.unwrap();
        let txn = store.begin_transaction().await.unwrap();
        txn.put(b"key1", b"new_value").unwrap();
        txn.rollback().unwrap();
        assert_eq!(store.get(b"key1").await.unwrap(), Some(b"old_value".to_vec()));
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_serialization() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        let data = TestData { id: 42, name: "test".to_string() };
        store.put_serialized(b"test_key", &data).await.unwrap();
        let result: Option<TestData> = store.get_deserialized(b"test_key").await.unwrap();
        assert_eq!(result, Some(data));
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_clone_shares_underlying() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        let clone = store.clone();
        store.put(b"x", b"1").await.unwrap();
        assert_eq!(clone.get(b"x").await.unwrap(), Some(b"1".to_vec()));
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_prefixed_data_type_isolation() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        let index_id = name_to_id("test");

        // Same business key, different data_type → different physical key
        store.prefixed_put(index_id, 0, b"key", b"meta_value").await.unwrap();
        store.prefixed_put(index_id, 1, b"key", b"doc_value").await.unwrap();

        assert_eq!(
            store.prefixed_get(index_id, 0, b"key").await.unwrap(),
            Some(b"meta_value".to_vec())
        );
        assert_eq!(
            store.prefixed_get(index_id, 1, b"key").await.unwrap(),
            Some(b"doc_value".to_vec())
        );

        // Scan per data_type
        let meta_entries = store.prefixed_scan_all(index_id, 0).await.unwrap();
        let doc_entries = store.prefixed_scan_all(index_id, 1).await.unwrap();
        assert_eq!(meta_entries.len(), 1);
        assert_eq!(doc_entries.len(), 1);

        store.close().await.unwrap();
    }
}
