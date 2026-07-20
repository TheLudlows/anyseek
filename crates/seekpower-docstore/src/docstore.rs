use seekpower_storage::ObjectStore;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::config::DocStoreConfig;
use crate::delete_set::{DeleteSet, DeleteSetError};
use crate::document::Document;
use crate::error::{DocStoreError, Result};
use crate::sequence::{SequenceAllocator, SequenceError};
use crate::sst::{SegmentMeta, SSTReader};
use crate::wal_store::{WALRecord, WALRecordType, WALStore};

impl From<SequenceError> for DocStoreError {
    fn from(e: SequenceError) -> Self {
        match e {
            SequenceError::StorageError(msg) => DocStoreError::StorageError(seekpower_error::Error::Internal(msg)),
            SequenceError::InvalidFormat(msg) => DocStoreError::SerializationError(msg),
            SequenceError::Conflict => DocStoreError::CasConflict,
        }
    }
}

impl From<DeleteSetError> for DocStoreError {
    fn from(e: DeleteSetError) -> Self {
        match e {
            DeleteSetError::StorageError(msg) => DocStoreError::StorageError(seekpower_error::Error::Internal(msg)),
            DeleteSetError::InvalidFormat(msg) => DocStoreError::SerializationError(msg),
        }
    }
}

/// 文档存储 - WAL-First 架构
///
/// 核心特性：
/// - 无 MemTable，直接写 WAL 到对象存储
/// - 写入后立即可查（从 WAL）
/// - 后台异步构建 SST
/// - 三级缓存查询路径
pub struct DocStore {
    /// 对象存储
    store: Arc<dyn ObjectStore>,
    /// 配置
    config: DocStoreConfig,
    /// WAL Store
    wal: Arc<WALStore>,
    /// 序列分配器
    sequence: Arc<SequenceAllocator>,
    /// 删除集
    delete_set: Arc<RwLock<DeleteSet>>,
    /// SST 段列表
    segments: Arc<RwLock<Vec<SegmentMeta>>>,
}

impl DocStore {
    /// 创建新的 DocStore
    pub async fn new(store: Arc<dyn ObjectStore>, config: DocStoreConfig) -> Result<Self> {
        let wal_prefix = format!("{}/wal", config.index_prefix);
        let delete_set_path = format!("{}/delete_set.json", config.index_prefix);
        let sequence_path = format!("{}/sequence", config.index_prefix);

        // 初始化序列分配器
        let sequence = Arc::new(SequenceAllocator::new(store.clone(), sequence_path));
        sequence.initialize().await?;

        // 加载删除集
        let delete_set = Arc::new(RwLock::new(
            DeleteSet::load(store.clone(), &delete_set_path)
                .await
                .unwrap_or_else(|_| DeleteSet::new(store.clone(), delete_set_path.clone())),
        ));

        // 初始化 WAL Store
        let wal = Arc::new(WALStore::new(store.clone(), config.index_prefix.clone()));

        // TODO: 加载 SST 段列表

        Ok(Self {
            store,
            config,
            wal,
            sequence,
            delete_set,
            segments: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// 插入文档
    ///
    /// 流程：
    /// 1. 分配 doc_id（CAS 序列分配器）
    /// 2. 写入 WAL 到对象存储（同步）
    /// 3. 检查反压
    /// 4. 返回成功（数据可从 WAL 查询）
    pub async fn insert(&self, mut doc: Document) -> Result<u64> {
        // 1. 分配 doc_id
        let doc_id = self.sequence.next().await?;

        // 2. 写入 WAL
        doc.doc_id = Some(doc_id);
        let record = WALRecord {
            record_type: WALRecordType::Add,
            doc_id,
            payload: doc.clone(),
        };

        self.wal.append(&record).await?;

        // 3. 检查反压
        let wal_size = self.wal.size_bytes().await? as usize;
        if wal_size > self.config.max_wal_size {
            return Err(DocStoreError::Backpressure);
        }

        // 4. 返回 doc_id
        Ok(doc_id)
    }

    /// 批量插入文档
    pub async fn insert_batch(&self, docs: Vec<Document>) -> Result<Vec<u64>> {
        let mut doc_ids = Vec::with_capacity(docs.len());

        for mut doc in docs {
            let doc_id = self.insert(doc).await?;
            doc_ids.push(doc_id);
        }

        Ok(doc_ids)
    }

    /// 获取文档
    ///
    /// 查询路径：
    /// 1. 检查删除集
    /// 2. 查询 WAL
    /// 3. 查询 SST 段（按 doc_id 二分查找）
    /// 4. 返回文档或 NotFound
    pub async fn get(&self, doc_id: u64) -> Result<Document> {
        // 1. 检查删除集
        if self.delete_set.read().await.contains(doc_id) {
            return Err(DocStoreError::DocumentNotFound { doc_id });
        }

        // 2. 查询 WAL
        if let Some(doc) = self.wal.get(doc_id).await? {
            return Ok(doc);
        }

        // 3. 查询 SST 段
        let segments = self.segments.read().await;
        for segment in segments.iter() {
            // 检查 doc_id 是否在段范围内
            if doc_id >= segment.min_doc_id && doc_id <= segment.max_doc_id {
                if let Some(doc) = self.read_segment(segment, doc_id).await? {
                    return Ok(doc);
                }
            }
        }

        Err(DocStoreError::DocumentNotFound { doc_id })
    }

    /// 批量获取文档
    pub async fn get_batch(&self, doc_ids: Vec<u64>) -> Result<Vec<Option<Document>>> {
        let mut results = Vec::with_capacity(doc_ids.len());

        for doc_id in doc_ids {
            match self.get(doc_id).await {
                Ok(doc) => results.push(Some(doc)),
                Err(DocStoreError::DocumentNotFound { .. }) => results.push(None),
                Err(e) => return Err(e),
            }
        }

        Ok(results)
    }

    /// 更新文档
    ///
    /// 更新 = 删除旧文档 + 插入新文档
    pub async fn update(&self, doc_id: u64, doc: Document) -> Result<()> {
        // 1. 先删除
        self.delete(doc_id).await?;

        // 2. 插入新文档（保持相同 doc_id）
        // TODO: 实现指定 doc_id 的插入
        Err(DocStoreError::NotImplemented("update with specific doc_id".to_string()))
    }

    /// 删除文档（软删除）
    pub async fn delete(&self, doc_id: u64) -> Result<()> {
        // 1. 添加到删除集
        self.delete_set.write().await.insert(doc_id);

        // 2. 持久化删除集
        self.delete_set.read().await.persist().await?;

        // 3. 写入 DELETE 记录到 WAL
        let record = WALRecord {
            record_type: WALRecordType::Delete,
            doc_id,
            payload: Document::new(),
        };

        self.wal.append(&record).await?;

        Ok(())
    }

    /// 批量删除
    pub async fn delete_batch(&self, doc_ids: Vec<u64>) -> Result<()> {
        for doc_id in doc_ids {
            self.delete(doc_id).await?;
        }
        Ok(())
    }

    /// 检查文档是否存在
    pub async fn exists(&self, doc_id: u64) -> Result<bool> {
        self.get(doc_id).await.map(|_| true).or_else(|e| {
            if matches!(e, DocStoreError::DocumentNotFound { .. }) {
                Ok(false)
            } else {
                Err(e)
            }
        })
    }

    /// 获取统计信息
    pub async fn stats(&self) -> Result<DocStoreStats> {
        let delete_set_stats = self.delete_set.read().await.stats();
        let wal_size = self.wal.size_bytes().await?;
        let segments = self.segments.read().await;

        let total_docs = segments.iter().map(|s| s.doc_count).sum();
        let total_bytes = segments.iter().map(|s| s.size_bytes).sum::<u64>() + wal_size;

        Ok(DocStoreStats {
            total_docs,
            total_deleted: delete_set_stats.count,
            total_bytes,
            wal_size,
            segment_count: segments.len(),
            doc_id_range: segments
                .first()
                .map(|s| s.min_doc_id)
                .unwrap_or(0)
                ..segments.last().map(|s| s.max_doc_id).unwrap_or(0),
        })
    }

    /// 从 SST 段读取文档
    async fn read_segment(&self, segment: &SegmentMeta, doc_id: u64) -> Result<Option<Document>> {
        let path = format!("{}/segments/{}.sst", self.config.index_prefix, segment.segment_id);
        let data = self.store.get(&path).await?;

        let reader = SSTReader::from_bytes(data)?;
        reader.get(doc_id)
    }

    /// 触发索引构建（手动）
    pub async fn trigger_index_build(&self) -> Result<u64> {
        // TODO: 实现索引构建逻辑
        Err(DocStoreError::NotImplemented("index_build".to_string()))
    }
}

/// 文档存储统计信息
#[derive(Debug, Clone)]
pub struct DocStoreStats {
    /// 总文档数
    pub total_docs: u64,
    /// 已删除文档数
    pub total_deleted: u64,
    /// 总存储字节数
    pub total_bytes: u64,
    /// WAL 大小（字节）
    pub wal_size: u64,
    /// SST 段数量
    pub segment_count: usize,
    /// doc_id 范围
    pub doc_id_range: std::ops::Range<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use seekpower_storage::{BackendType, FsBackendBuilder, FsStore, ObjectStoreBuilder};

    async fn  make_store() -> (Arc<dyn ObjectStore>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("create tempdir");
        let store = ObjectStoreBuilder::new(BackendType::fs_builder().build()).build().await.unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn test_docstore_insert() {
        let (store, _dir) = make_store().await;

        let config = DocStoreConfig::new().with_index_prefix("test".to_string());
        let docstore = DocStore::new(store, config).await.unwrap();

        let mut doc = Document::new();
        doc.insert("key".to_string(), crate::document::DocumentValue::String("value".to_string()));

        let doc_id = docstore.insert(doc).await.unwrap();
        assert_eq!(doc_id, 1);
    }

    #[tokio::test]
    async fn test_docstore_insert_and_get() {
        let (store, _dir) = make_store().await;

        let config = DocStoreConfig::new().with_index_prefix("test".to_string());
        let docstore = DocStore::new(store, config).await.unwrap();

        let mut doc = Document::new();
        doc.insert("key".to_string(), crate::document::DocumentValue::String("value".to_string()));

        let doc_id = docstore.insert(doc.clone()).await.unwrap();
        let retrieved = docstore.get(doc_id).await.unwrap();

        assert_eq!(retrieved.doc_id, Some(doc_id));
        assert_eq!(retrieved.get("key"), Some(&crate::document::DocumentValue::String("value".to_string())));
    }

    #[tokio::test]
    async fn test_docstore_delete() {
        let (store, _dir) = make_store().await;

        let config = DocStoreConfig::new().with_index_prefix("test".to_string());
        let docstore = DocStore::new(store, config).await.unwrap();

        let mut doc = Document::new();
        doc.insert("key".to_string(), crate::document::DocumentValue::String("value".to_string()));

        let doc_id = docstore.insert(doc).await.unwrap();
        docstore.delete(doc_id).await.unwrap();

        let result = docstore.get(doc_id).await;
        assert!(matches!(result, Err(DocStoreError::DocumentNotFound { .. })));
    }
}