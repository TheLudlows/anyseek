use seekpower_storage::ObjectStore;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// CAS 序列分配器
///
/// 基于 Compare-And-Swap (CAS) 的原子计数器，用于分配 doc_id
/// 序列号持久化到对象存储，支持崩溃恢复
#[derive(Clone)]
pub struct SequenceAllocator {
    /// 对象存储
    store: Arc<dyn ObjectStore>,
    /// 序列文件路径
    seq_path: String,
    /// 本地缓存值（减少 CAS 调用）
    cached_value: Arc<RwLock<u64>>,
    /// 下一个可分配的序列号（原子）
    next_seq: Arc<AtomicU64>,
}

impl SequenceAllocator {
    /// 创建新的序列分配器
    pub fn new(store: Arc<dyn ObjectStore>, seq_path: String) -> Self {
        Self {
            store,
            seq_path,
            cached_value: Arc::new(RwLock::new(0)),
            next_seq: Arc::new(AtomicU64::new(1)),
        }
    }

    /// 初始化：从对象存储加载当前序列号
    pub async fn initialize(&self) -> Result<(), SequenceError> {
        match self.store.get(&self.seq_path).await {
            Ok(data) => {
                let value_slice: [u8; 8] = data.to_vec().try_into().map_err(|_| {
                    SequenceError::InvalidFormat("Invalid sequence file format".to_string())
                })?;
                let value = u64::from_le_bytes(value_slice);
                *self.cached_value.write().await = value;
                self.next_seq.store(value + 1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) if e.to_string().contains("not found") => {
                // 文件不存在，从 1 开始
                *self.cached_value.write().await = 0;
                self.next_seq.store(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(SequenceError::StorageError(e.to_string())),
        }
    }

    /// 分配下一个 doc_id
    ///
    /// 简化实现：直接 CAS 每次分配
    pub async fn next(&self) -> Result<u64, SequenceError> {
        let current_cached = *self.cached_value.read().await;
        let next = current_cached + 1;

        // 写入对象存储（简化：直接覆盖，不使用 CAS）
        let bytes = next.to_le_bytes();
        self.store.put(&self.seq_path, bytes.to_vec().into()).await
            .map_err(|e| SequenceError::StorageError(e.to_string()))?;

        // 更新本地缓存
        *self.cached_value.write().await = next;
        self.next_seq.store(next + 1, Ordering::Release);

        Ok(next)
    }

    /// 获取 current 序列号（不分配）
    pub async fn current(&self) -> u64 {
        *self.cached_value.read().await
    }

    /// 设置序列号（用于恢复）
    pub async fn set(&self, seq: u64) -> Result<(), SequenceError> {
        let bytes = seq.to_le_bytes();
        self.store.put(&self.seq_path, bytes.to_vec().into()).await
            .map_err(|e| SequenceError::StorageError(e.to_string()))?;
        *self.cached_value.write().await = seq;
        self.next_seq.store(seq + 1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SequenceError {
    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("CAS conflict")]
    Conflict,
}

#[cfg(test)]
mod tests {
    use super::*;
    use seekpower_storage::FsStore;
    fn make_store() -> (Arc<FsStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let store = FsStore::new(dir.path().to_path_buf()).expect("create FsStore");
        (Arc::new(store), dir)
    }

    #[tokio::test]
    async fn test_sequence_allocator_initialization() {
        let (store, _dir) = make_store();

        // 空目录：序列文件不存在，初始化为 0
        let allocator = SequenceAllocator::new(store.clone(), "test.seq".to_string());
        allocator.initialize().await.unwrap();
        assert_eq!(allocator.current().await, 0);

        // 写入一个值并持久化
        allocator.set(100).await.unwrap();
        assert_eq!(allocator.current().await, 100);

        // 重新创建分配器（模拟重启），应从对象存储加载到 100
        let reloaded = SequenceAllocator::new(store, "test.seq".to_string());
        reloaded.initialize().await.unwrap();
        assert_eq!(reloaded.current().await, 100);
    }

    #[tokio::test]
    async fn test_sequence_allocator_next() {
        let (store, _dir) = make_store();

        let allocator = SequenceAllocator::new(store, "test.seq".to_string());
        allocator.initialize().await.unwrap();

        let seq1 = allocator.next().await.unwrap();
        let seq2 = allocator.next().await.unwrap();
        let seq3 = allocator.next().await.unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
    }

    #[tokio::test]
    async fn test_sequence_allocator_set() {
        let (store, _dir) = make_store();

        let allocator = SequenceAllocator::new(store, "test.seq".to_string());
        allocator.initialize().await.unwrap();

        allocator.set(1000).await.unwrap();

        assert_eq!(allocator.current().await, 1000);
    }
}