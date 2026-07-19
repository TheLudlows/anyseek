use roaring::RoaringBitmap;
use seekpower_storage::ObjectStore;
use std::sync::Arc;

/// 删除集
///
/// 使用 RoaringBitmap 高效存储已删除的 doc_id
pub struct DeleteSet {
    deleted: RoaringBitmap,
    store: Arc<dyn ObjectStore>,
    path: String,
}

impl DeleteSet {
    /// 创建新的删除集
    pub fn new(store: Arc<dyn ObjectStore>, path: String) -> Self {
        Self {
            deleted: RoaringBitmap::new(),
            store,
            path,
        }
    }

    /// 添加到删除集
    pub fn insert(&mut self, doc_id: u64) {
        self.deleted.insert(doc_id as u32);
    }

    /// 批量添加
    pub fn insert_batch(&mut self, doc_ids: &[u64]) {
        for &doc_id in doc_ids {
            self.deleted.insert(doc_id as u32);
        }
    }

    /// 检查是否已删除
    pub fn contains(&self, doc_id: u64) -> bool {
        self.deleted.contains(doc_id as u32)
    }

    /// 移除（取消删除）
    pub fn remove(&mut self, doc_id: u64) {
        self.deleted.remove(doc_id as u32);
    }

    /// 获取删除数量
    pub fn len(&self) -> u64 {
        self.deleted.len() as u64
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.deleted.is_empty()
    }

    /// 获取所有已删除的 doc_id
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.deleted.iter().map(|x| x as u64)
    }

    /// 持久化到对象存储
    pub async fn persist(&self) -> Result<(), DeleteSetError> {
        let mut bytes = Vec::new();
        self.deleted.serialize_into(&mut bytes).map_err(|e| {
            DeleteSetError::InvalidFormat(format!("Serialization error: {}", e))
        })?;
        self.store.put(&self.path, bytes.into()).await
            .map_err(|e| DeleteSetError::StorageError(e.to_string()))?;
        Ok(())
    }

    /// 从对象存储加载
    pub async fn load(store: Arc<dyn ObjectStore>, path: &str) -> Result<Self, DeleteSetError> {
        match store.get(path).await {
            Ok(data) => {
                let bitmap = RoaringBitmap::deserialize_from(&data.to_vec()[..])
                    .map_err(|e| DeleteSetError::InvalidFormat(format!("{}", e)))?;
                Ok(Self {
                    deleted: bitmap,
                    store,
                    path: path.to_string(),
                })
            }
            Err(e) if e.to_string().contains("not found") => {
                // 文件不存在，返回空删除集
                Ok(Self {
                    deleted: RoaringBitmap::new(),
                    store,
                    path: path.to_string(),
                })
            }
            Err(e) => Err(DeleteSetError::StorageError(e.to_string())),
        }
    }

    /// 清空删除集
    pub fn clear(&mut self) {
        self.deleted.clear();
    }

    /// 获取统计信息
    pub fn stats(&self) -> DeleteSetStats {
        DeleteSetStats {
            count: self.deleted.len() as u64,
            serialized_size: self.deleted.serialized_size() as u64,
        }
    }
}

/// 删除集统计信息
#[derive(Debug, Clone)]
pub struct DeleteSetStats {
    /// 删除的文档数量
    pub count: u64,
    /// 序列化后的大小（字节）
    pub serialized_size: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum DeleteSetError {
    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock ObjectStore for testing
    struct MockStore {
        data: Arc<RwLock<Vec<u8>>>,
    }

    #[async_trait::async_trait]
    impl ObjectStore for MockStore {
        async fn put(&self, _location: &str, bytes: bytes::Bytes) -> Result<(), seekpower_storage::StorageError> {
            *self.data.write().await = bytes.to_vec();
            Ok(())
        }

        async fn get(&self, _location: &str) -> Result<bytes::Bytes, seekpower_storage::StorageError> {
            Ok(self.data.read().await.clone().into())
        }

        async fn delete(&self, _location: &str) -> Result<(), seekpower_storage::StorageError> {
            *self.data.write().await = Vec::new();
            Ok(())
        }

        async fn list(&self, _prefix: Option<&str>) -> Result<Vec<String>, seekpower_storage::StorageError> {
            Ok(vec![])
        }

        async fn range(
            &self,
            _location: &str,
            _offset: u64,
            _len: Option<u64>,
        ) -> Result<bytes::Bytes, seekpower_storage::StorageError> {
            Ok(self.data.read().await.clone().into())
        }

        async fn put_if_absent(
            &self,
            _location: &str,
            _bytes: bytes::Bytes,
        ) -> Result<bool, seekpower_storage::StorageError> {
            Ok(true)
        }

        async fn put_if_match(
            &self,
            _location: &str,
            bytes: bytes::Bytes,
            _etag: &str,
        ) -> Result<bool, seekpower_storage::StorageError> {
            self.put(_location, bytes).await?;
            Ok(true)
        }

        async fn metadata(&self, _location: &str) -> Result<seekpower_storage::ObjectMetadata, seekpower_storage::StorageError> {
            Ok(seekpower_storage::ObjectMetadata {
                size: self.data.read().await.len() as u64,
                last_modified: std::time::SystemTime::now(),
                etag: Some("mock".to_string()),
            })
        }
    }

    #[tokio::test]
    async fn test_delete_set_insert() {
        let store = Arc::new(MockStore {
            data: Arc::new(RwLock::new(Vec::new())),
        });

        let mut delete_set = DeleteSet::new(store.clone(), "delete_set.json".to_string());
        assert!(!delete_set.contains(1));

        delete_set.insert(1);
        assert!(delete_set.contains(1));
        assert!(!delete_set.contains(2));

        delete_set.insert(2);
        assert!(delete_set.contains(2));
        assert_eq!(delete_set.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_set_persistence() {
        let store = Arc::new(MockStore {
            data: Arc::new(RwLock::new(Vec::new())),
        });

        let delete_set = DeleteSet::new(store.clone(), "delete_set.json".to_string());
        delete_set.insert(1);
        delete_set.insert(100);
        delete_set.insert(1000);

        // 持久化
        delete_set.persist().await.unwrap();

        // 加载
        let loaded = DeleteSet::load(store.clone(), "delete_set.json".await.unwrap();
        assert!(loaded.contains(1));
        assert!(loaded.contains(100));
        assert!(loaded.contains(1000));
        assert!(!loaded.contains(2));
        assert_eq!(loaded.len(), 3);
    }

    #[tokio::test]
    async fn test_delete_set_remove() {
        let mut delete_set = DeleteSet::new(Arc::new(MockStore {
            data: Arc::new(RwLock::new(Vec::new())),
        }), "delete_set.json".to_string());

        delete_set.insert(1);
        assert!(delete_set.contains(1));

        delete_set.remove(1);
        assert!(!delete_set.contains(1));
    }

    #[tokio::test]
    async fn test_delete_set_batch() {
        let mut delete_set = DeleteSet::new(Arc::new(MockStore {
            data: Arc::new(RwLock::new(Vec::new())),
        }), "delete_set.json".to_string());

        delete_set.insert_batch(&[1, 2, 3, 4, 5]);
        assert_eq!(delete_set.len(), 5);
        for i in 1..=5 {
            assert!(delete_set.contains(i));
        }
    }

    #[tokio::test]
    async fn test_delete_set_stats() {
        let mut delete_set = DeleteSet::new(Arc::new(MockStore {
            data: Arc::new(RwLock::new(Vec::new())),
        }), "delete_set.json".to_string());

        for i in 1..=1000 {
            delete_set.insert(i);
        }

        let stats = delete_set.stats();
        assert_eq!(stats.count, 1000);
        assert!(stats.serialized_size > 0);
    }
}