//! seekpower-storage: SlateDB KV wrapper with prefix-based key isolation.

mod key;

pub use key::{encode_key, encode_prefix, encode_index_prefix, name_to_id};

use object_store::ObjectStore;
use seekpower_error::{Result, SeekPowerError};
use serde::de::DeserializeOwned;
use serde::Serialize;
use slatedb::{bytes::Bytes, config::WriteOptions, IsolationLevel, WriteBatch};

pub use seekpower_schema::{TYPE_DELL, TYPE_DOC, TYPE_DICT, TYPE_IDX, TYPE_LEN, TYPE_META, TYPE_VEC};
use std::sync::Arc;

/// Term identifier type (u32 can store ~4.3 billion unique terms)
pub type TermId = u32;

/// Maximum number of unique terms allowed per index
pub const MAX_TERM_ID: TermId = TermId::MAX;

/// Term dictionary for bidirectional term <-> term_id mapping.
/// Used to replace term hash with a compact 32-bit identifier while maintaining
/// the ability to recover the original term for highlighting, verification, etc.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TermDictionary {
    /// term -> term_id mapping
    pub term_to_id: ahash::AHashMap<Vec<u8>, TermId>,
    /// term_id -> term mapping
    pub id_to_term: ahash::AHashMap<TermId, Vec<u8>>,
    /// Next term_id to allocate
    pub next_id: TermId,
}

impl Default for TermDictionary {
    fn default() -> Self {
        Self {
            term_to_id: ahash::AHashMap::new(),
            id_to_term: ahash::AHashMap::new(),
            next_id: 0,
        }
    }
}

impl TermDictionary {
    /// Get term_id for a term, creating a new mapping if not exists.
    pub fn get_or_create(&mut self, term: &[u8]) -> Result<TermId> {
        if let Some(&id) = self.term_to_id.get(term) {
            return Ok(id);
        }

        if self.next_id >= MAX_TERM_ID {
            return Err(SeekPowerError::StorageError(
                "Term dictionary full: maximum number of unique terms reached".to_string(),
            ));
        }

        let id = self.next_id;
        self.next_id += 1;
        self.term_to_id.insert(term.to_vec(), id);
        self.id_to_term.insert(id, term.to_vec());
        Ok(id)
    }

    /// Get term_id for a term if exists, returns None otherwise.
    pub fn get_id(&self, term: &[u8]) -> Option<TermId> {
        self.term_to_id.get(term).copied()
    }

    /// Get term for a term_id if exists, returns None otherwise.
    pub fn get_term(&self, term_id: TermId) -> Option<&[u8]> {
        self.id_to_term.get(&term_id).map(|v| v.as_slice())
    }

    /// Return the number of unique terms in the dictionary.
    pub fn len(&self) -> usize {
        self.term_to_id.len()
    }

    /// Check if the dictionary is empty.
    pub fn is_empty(&self) -> bool {
        self.term_to_id.is_empty()
    }
}

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

    /// Batch put multiple key-value pairs with index_id + data_type prefix atomically.
    /// All writes in the batch are applied atomically to the database.
    pub async fn prefixed_batch_put(
        &self,
        index_id: u32,
        data_type: u8,
        writes: &[(&[u8], Vec<u8>)],
    ) -> Result<()> {
        let mut batch = WriteBatch::new();
        for (key, value) in writes {
            let full_key = encode_key(index_id, data_type, key);
            batch.put_bytes(Bytes::from(full_key), Bytes::from(value.clone()));
        }
        self.db
            .write_with_options(batch, &WriteOptions::default())
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Batch write failed: {}", e)))?;
        Ok(())
    }

    /// Delete all keys for a given index_id (across all data_types).
    /// Since data_type comes first, we need to scan each data_type separately.
    pub async fn prefixed_delete_all(&self, index_id: u32) -> Result<()> {
        let data_types = [TYPE_META, TYPE_DOC, TYPE_IDX, TYPE_VEC, TYPE_DELL, TYPE_LEN];
        for data_type in data_types {
            let prefix = encode_prefix(index_id, data_type);
            let results = self.scan_prefix(&prefix).await?;
            for (key, _) in results {
                self.delete(&key).await?;
            }
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
    async fn test_prefixed_batch_put() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_batch_db", object_store).await.unwrap();
        let index_id = 123;

        // Batch write multiple keys
        store.prefixed_batch_put(index_id, 0, &[
            (b"key1", b"value1".to_vec()),
            (b"key2", b"value2".to_vec()),
            (b"key3", b"value3".to_vec()),
        ]).await.unwrap();

        // Verify all writes succeeded
        assert_eq!(store.prefixed_get(index_id, 0, b"key1").await.unwrap(), Some(b"value1".to_vec()));
        assert_eq!(store.prefixed_get(index_id, 0, b"key2").await.unwrap(), Some(b"value2".to_vec()));
        assert_eq!(store.prefixed_get(index_id, 0, b"key3").await.unwrap(), Some(b"value3".to_vec()));
        assert_eq!(store.prefixed_get(index_id, 0, b"key4").await.unwrap(), None);

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
