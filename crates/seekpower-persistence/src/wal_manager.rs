//! WAL 管理器
//!
//! 管理双层 WAL（本地 + S3），实现异步上传和灾难恢复

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::time::{sleep, timeout};

use crate::local_wal::LocalWAL;
use crate::wal::WALRecord;
use seekpower_error::PersistenceError;
use seekpower_storage::ObjectStore;

/// WAL 管理器配置
#[derive(Debug, Clone)]
pub struct WALConfig {
    /// 保留的本地 WAL 文件数量
    pub local_keep: usize,
    /// 异步上传超时（秒）
    pub upload_timeout_secs: u64,
    /// 上传重试次数
    pub upload_retries: usize,
    /// S3 WAL 保留天数
    pub s3_retention_days: u32,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            local_keep: 2,
            upload_timeout_secs: 30,
            upload_retries: 3,
            s3_retention_days: 7,
        }
    }
}

/// WAL 管理器
///
/// 管理本地 WAL 和 S3 WAL，实现异步上传和灾难恢复
pub struct WALManager {
    /// 本地 WAL
    local_wal: Arc<LocalWAL>,
    /// S3 存储（可选）
    s3_store: Option<Arc<dyn ObjectStore>>,
    /// 配置
    config: WALConfig,
    /// 上传任务句柄
    upload_handle: Arc<tokio::sync::RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

impl WALManager {
    /// 创建新的 WAL 管理器
    ///
    /// # 参数
    ///
    /// * `local_wal` - 本地 WAL 实例
    /// * `s3_store` - 可选的 S3 存储，用于备份
    /// * `config` - WAL 配置
    pub fn new(
        local_wal: Arc<LocalWAL>,
        s3_store: Option<Arc<dyn ObjectStore>>,
        config: WALConfig,
    ) -> Self {
        Self {
            local_wal,
            s3_store,
            config,
            upload_handle: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    /// 追加记录到 WAL
    ///
    /// 同步写入本地 WAL，异步上传到 S3
    pub async fn append(&self, record: &WALRecord) -> Result<(), PersistenceError> {
        // 同步写入本地 WAL
        self.local_wal.append(record).await?;

        // 触发异步上传
        self.trigger_upload().await;

        Ok(())
    }

    /// 读取 WAL 记录
    pub async fn read(&self, seq: u64) -> Result<Vec<WALRecord>, PersistenceError> {
        self.local_wal.read(seq).await
    }

    /// 截断 WAL
    pub async fn truncate(&self, seq: u64) -> Result<(), PersistenceError> {
        self.local_wal.truncate(seq).await?;

        // 同时删除 S3 上的 WAL 文件
        if let Some(ref s3_store) = self.s3_store {
            let key = format!("wal/{:08}.wal", seq);
            let _ = s3_store.delete(&key).await;
        }

        Ok(())
    }

    /// 清理过期的 WAL 文件
    pub async fn cleanup(&self) -> Result<(), PersistenceError> {
        // 清理本地 WAL
        self.local_wal
            .cleanup(self.config.local_keep)
            .await?;

        // 清理 S3 WAL
        if let Some(ref s3_store) = self.s3_store {
            self.cleanup_s3_wal(s3_store).await?;
        }

        Ok(())
    }

    /// 从 S3 恢复 WAL
    ///
    /// 当本地 WAL 丢失时，从 S3 下载恢复
    pub async fn recover_from_s3(&self) -> Result<(), PersistenceError> {
        let s3_store = self
            .s3_store
            .as_ref()
            .ok_or_else(|| PersistenceError::Internal("S3 store not configured".to_string()))?;

        // 列举 S3 上的 WAL 文件
        let keys = s3_store
            .list(Some("wal/"))
            .await
            .map_err(|e| PersistenceError::Internal(format!("Failed to list S3 WAL files: {}", e)))?;

        for key in keys {
            if !key.ends_with(".wal") {
                continue;
            }

            // 从 S3 下载 WAL 文件
            let data = s3_store
                .get(&key)
                .await
                .map_err(|e| PersistenceError::Internal(format!("Failed to download WAL file: {}", e)))?;

            // 写入本地
            let seq = Self::extract_seq(&key)?;
            self.local_wal.write_raw(seq, data.to_vec()).await?;
        }

        Ok(())
    }

    /// 重放 WAL 记录
    ///
    /// 从指定序列号开始重放所有未提交的记录
    pub async fn replay<F>(&self, start_seq: u64, mut callback: F) -> Result<(), PersistenceError>
    where
        F: FnMut(&WALRecord) -> Result<(), PersistenceError>,
    {
        let seqs = self.local_wal.list_seqs().await?;

        for seq in seqs {
            if seq < start_seq {
                continue;
            }

            let records = self.local_wal.read(seq).await?;
            for record in records {
                callback(&record)?;
            }
        }

        Ok(())
    }

    /// 触发异步上传
    async fn trigger_upload(&self) {
        let local_wal = self.local_wal.clone();
        let s3_store = self.s3_store.clone();
        let config = self.config.clone();
        let upload_handle = self.upload_handle.clone();

        // 检查是否已经有上传任务在运行
        let mut handle_opt = upload_handle.write().await;
        if let Some(handle) = handle_opt.as_ref() {
            if !handle.is_finished() {
                return; // 已有任务在运行
            }
        }

        // 启动新的上传任务
        let handle = tokio::spawn(async move {
            if let Some(ref s3) = s3_store {
                if let Err(e) = Self::upload_to_s3(&local_wal, s3, &config).await {
                    eprintln!("Failed to upload WAL to S3: {}", e);
                }
            }
        });

        *handle_opt = Some(handle);
    }

    /// 上传 WAL 到 S3
    async fn upload_to_s3(
        local_wal: &LocalWAL,
        s3_store: &Arc<dyn ObjectStore>,
        config: &WALConfig,
    ) -> Result<(), PersistenceError> {
        let seqs = local_wal.list_seqs().await?;

        for seq in seqs {
            // 跳过已经上传的文件（通过检查 S3 是否存在）
            let key = format!("wal/{:08}.wal", seq);
            let exists = s3_store.metadata(&key).await.is_ok();
            if exists {
                continue;
            }

            // 读取本地 WAL 文件
            let data = local_wal.read_raw(seq).await.map_err(|e| {
                PersistenceError::Internal(format!("Failed to read WAL file {}: {}", seq, e))
            })?;

            // 带重试的上传
            let mut retries = 0;
            loop {
                let upload_result = timeout(
                    Duration::from_secs(config.upload_timeout_secs),
                    s3_store.put(&key, data.clone().into()),
                )
                .await;

                match upload_result {
                    Ok(Ok(_)) => break, // 上传成功
                    Ok(Err(e)) => {
                        retries += 1;
                        if retries >= config.upload_retries {
                            return Err(PersistenceError::Internal(format!(
                                "Failed to upload WAL {} after {} retries: {}",
                                seq, config.upload_retries, e
                            )));
                        }
                        sleep(Duration::from_millis(100 * 2_u64.pow(retries as u32))).await;
                    }
                    Err(_) => {
                        retries += 1;
                        if retries >= config.upload_retries {
                            return Err(PersistenceError::Timeout(format!(
                                "Upload timeout for WAL {}",
                                seq
                            )));
                        }
                        sleep(Duration::from_millis(100 * 2_u64.pow(retries as u32))).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// 清理 S3 上的过期的 WAL 文件
    async fn cleanup_s3_wal(&self, s3_store: &Arc<dyn ObjectStore>) -> Result<(), PersistenceError> {
        let keys = s3_store
            .list(Some("wal/"))
            .await
            .map_err(|e| PersistenceError::Internal(format!("Failed to list S3 WAL files: {}", e)))?;

        let retention_duration = Duration::from_secs(86400 * self.config.s3_retention_days as u64);

        for key in keys {
            if !key.ends_with(".wal") {
                continue;
            }

            // 检查文件年龄
            if let Ok(meta) = s3_store.metadata(&key).await {
                if let Ok(age) = SystemTime::now().duration_since(meta.last_modified) {
                    if age > retention_duration {
                        let _ = s3_store.delete(&key).await;
                    }
                }
            }
        }

        Ok(())
    }

    /// 从 WAL 键提取序列号
    fn extract_seq(key: &str) -> Result<u64, PersistenceError> {
        let file_name = key
            .strip_prefix("wal/")
            .ok_or_else(|| PersistenceError::InvalidWalFormat("Invalid WAL key".to_string()))?;

        let seq_str = file_name
            .strip_suffix(".wal")
            .ok_or_else(|| PersistenceError::InvalidWalFormat("Invalid WAL file name".to_string()))?;

        seq_str.parse::<u64>().map_err(|_| {
            PersistenceError::InvalidWalFormat(format!("Invalid WAL sequence: {}", seq_str))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WALRecordType;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let local_wal = Arc::new(LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap());

        let manager = WALManager::new(
            local_wal.clone(),
            None,
            WALConfig::default(),
        );

        let record = WALRecord::new(WALRecordType::Add, 1, b"data".to_vec());
        manager.append(&record).await.unwrap();

        let records = manager.read(local_wal.current_seq().await).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].doc_id, 1);
    }

    #[tokio::test]
    async fn test_replay() {
        let temp_dir = TempDir::new().unwrap();
        let local_wal = Arc::new(LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap());

        let manager = WALManager::new(
            local_wal.clone(),
            None,
            WALConfig::default(),
        );

        let record1 = WALRecord::new(WALRecordType::Add, 1, b"data1".to_vec());
        let record2 = WALRecord::new(WALRecordType::Delete, 2, vec![]);

        manager.append(&record1).await.unwrap();
        manager.append(&record2).await.unwrap();

        let mut replayed = Vec::new();
        manager.replay(local_wal.current_seq().await, |record| {
            replayed.push(record.clone());
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].doc_id, 1);
        assert_eq!(replayed[1].doc_id, 2);
    }

    #[tokio::test]
    async fn test_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let local_wal = Arc::new(LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap());

        let manager = WALManager::new(
            local_wal.clone(),
            None,
            WALConfig::default(),
        );

        let record = WALRecord::new(WALRecordType::Add, 1, b"data".to_vec());
        manager.append(&record).await.unwrap();

        manager.truncate(1).await.unwrap();

        let seqs = local_wal.list_seqs().await.unwrap();
        assert!(!seqs.contains(&1));
    }

    #[tokio::test]
    async fn test_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let local_wal = Arc::new(LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap());

        let manager = WALManager::new(
            local_wal.clone(),
            None,
            WALConfig {
                local_keep: 1,
                ..Default::default()
            },
        );

        // 创建多个 WAL 文件
        for i in 0..1000 {
            let record = WALRecord::new(
                WALRecordType::Add,
                i,
                vec![0u8; 100_000],
            );
            manager.append(&record).await.unwrap();
        }

        manager.cleanup().await.unwrap();

        let seqs = local_wal.list_seqs().await.unwrap();
        assert!(seqs.len() <= 1);
    }
}