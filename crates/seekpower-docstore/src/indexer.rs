use crate::config::DocStoreConfig;
use crate::error::{DocStoreError, Result};
use crate::sst::{SSTWriter, SegmentMeta};
use crate::wal_store::{WALRecord, WALRecordType};
use seekpower_storage::ObjectStore;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 后台索引器配置
#[derive(Debug, Clone)]
pub struct IndexerConfig {
    /// 索引间隔（毫秒）
    pub interval_ms: u64,
    /// 最小 WAL 段大小才触发索引
    pub min_wal_size: usize,
    /// 最小文档数才触发索引
    pub min_doc_count: usize,
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            interval_ms: 5000,
            min_wal_size: 10 * 1024 * 1024,  // 10MB
            min_doc_count: 100,
        }
    }
}

/// 后台索引器
///
/// 职责：
/// - 扫描 WAL 段
/// - 构建 SST 段
/// - 更新 Manifest
/// - 截断 WAL
pub struct Indexer {
    store: Arc<dyn ObjectStore>,
    config: DocStoreConfig,
    indexer_config: IndexerConfig,
    shutdown: AtomicBool,
    last_processed_seq: AtomicU64,
}

impl Indexer {
    /// 创建新的索引器
    pub fn new(
        store: Arc<dyn ObjectStore>,
        config: DocStoreConfig,
        indexer_config: IndexerConfig,
    ) -> Self {
        Self {
            store,
            config,
            indexer_config,
            shutdown: AtomicBool::new(false),
            last_processed_seq: AtomicU64::new(0),
        }
    }

    /// 启动后台索引任务
    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            while !self.shutdown.load(Ordering::Relaxed) {
                // 运行一次索引循环
                if let Err(e) = self.run_once().await {
                    eprintln!("Indexer error: {}", e);
                }

                // 等待下一次
                tokio::time::sleep(Duration::from_millis(self.indexer_config.interval_ms)).await;
            }
        })
    }

    /// 停止后台索引任务
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// 运行一次索引循环
    async fn run_once(&self) -> Result<()> {
        // TODO: 扫描未处理的 WAL 段
        // 暂时跳过
        Ok(())
    }

    /// 构建 SST
    async fn build_sst(&self, records: Vec<WALRecord>) -> Result<SegmentMeta> {
        // 1. 过滤删除记录
        let mut documents = BTreeMap::new();

        for record in records {
            match record.record_type {
                WALRecordType::Add => {
                    documents.insert(record.doc_id, record.payload);
                }
                WALRecordType::Delete => {
                    documents.remove(&record.doc_id);
                }
            }
        }

        if documents.is_empty() {
            return Err(DocStoreError::NoDocuments);
        }

        // 2. 排序并构建 SST
        let segment_id = self.generate_segment_id()?;
        let mut writer = SSTWriter::new(segment_id, 0);

        for (doc_id, doc) in documents {
            writer.add(doc);
        }

        let sst_bytes = writer.to_bytes()?;
        let size_bytes = sst_bytes.len() as u64;

        // 3. 写入对象存储
        let path = format!("{}/segments/{}.sst", self.config.index_prefix, segment_id);
        self.store.put(&path, sst_bytes.into()).await?;

        // 4. 构建段元数据
        let min_doc_id = *documents.keys().next().unwrap();
        let max_doc_id = *documents.keys().next_back().unwrap();

        Ok(SegmentMeta {
            segment_id,
            level: 0,
            doc_count: documents.len() as u64,
            min_doc_id,
            max_doc_id,
            size_bytes,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        })
    }

    /// 更新 Manifest
    async fn update_manifest(&self, segment_meta: SegmentMeta) -> Result<()> {
        // TODO: 实现实际的 Manifest 更新
        // 这里使用简单的文件追加
        let manifest_path = format!("{}/manifest.json", self.config.index_prefix);

        // 读取现有 Manifest
        let mut segments = if let Ok(data) = self.store.get(&manifest_path).await {
            serde_json::from_slice::<Vec<SegmentMeta>>(&data).unwrap_or_default()
        } else {
            Vec::new()
        };

        // 添加新段
        segments.push(segment_meta.clone());

        // 写回
        let manifest_json = serde_json::to_string_pretty(&segments).unwrap();
        self.store.put(&manifest_path, manifest_json.into()).await?;

        Ok(())
    }

    /// 截断 WAL
    async fn truncate_wal(&self, _seq: u64) -> Result<()> {
        // TODO: 实现实际的 WAL 截断
        Ok(())
    }

    /// 生成段 ID
    fn generate_segment_id(&self) -> Result<u64> {
        // TODO: 实现从 Manifest 获取下一个段 ID
        Ok(1)
    }

    /// 手动触发索引构建
    pub async fn build_index(&self, _min_seq: u64, _max_seq: u64) -> Result<u64> {
        // TODO: 实现手动索引构建
        Ok(0)
    }

    /// 获取索引器状态
    pub fn stats(&self) -> IndexerStats {
        IndexerStats {
            last_processed_seq: self.last_processed_seq.load(Ordering::Relaxed),
            is_running: !self.shutdown.load(Ordering::Relaxed),
        }
    }
}

/// 索引器统计信息
#[derive(Debug, Clone)]
pub struct IndexerStats {
    /// 最后处理的 WAL 序列号
    pub last_processed_seq: u64,
    /// 是否正在运行
    pub is_running: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indexer_config_default() {
        let config = IndexerConfig::default();
        assert_eq!(config.interval_ms, 5000);
        assert_eq!(config.min_wal_size, 10 * 1024 * 1024);
        assert_eq!(config.min_doc_count, 100);
    }

    #[test]
    fn test_indexer_stats() {
        let indexer = Indexer {
            store: Arc::new(MockStore::new()),
            config: DocStoreConfig::default(),
            indexer_config: IndexerConfig::default(),
            shutdown: AtomicBool::new(false),
            last_processed_seq: AtomicU64::new(100),
        };

        let stats = indexer.stats();
        assert_eq!(stats.last_processed_seq, 100);
        assert!(stats.is_running);
    }

    // Mock ObjectStore
    struct MockStore {
        data: Arc<RwLock<Vec<u8>>>,
    }

    impl MockStore {
        fn new() -> Self {
            Self {
                data: Arc::new(RwLock::new(Vec::new())),
            }
        }
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
}