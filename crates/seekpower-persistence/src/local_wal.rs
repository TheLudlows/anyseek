//! 本地 WAL 实现
//!
//! 实现本地文件系统的 WAL，使用 OpenDAL FS Service 进行快速路径的数据持久化

use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};

use bytes::Bytes;
use futures_util::stream::StreamExt;
use opendal::{ErrorKind, Operator};
use tokio::sync::RwLock;

use crate::wal::{WALRecord, WALRecordType};
use seekpower_error::PersistenceError;

/// WAL 文件头魔数
const WAL_MAGIC: &[u8; 4] = b"WLOG";
/// WAL 格式版本
const WAL_VERSION: u32 = 1;
/// 单个 WAL 文件的最大大小（100MB）
const WAL_MAX_SIZE: u64 = 100 * 1024 * 1024;

/// 本地 WAL 实现
///
/// 使用 OpenDAL FS Service 实现
pub struct LocalWAL {
    /// OpenDAL Operator for FS
    op: Operator,
    /// 数据目录
    data_dir: PathBuf,
    /// 当前 WAL 文件序列号
    current_seq: RwLock<u64>,
    /// 当前 WAL 文件大小
    current_size: RwLock<u64>,
}

impl LocalWAL {
    /// 创建新的本地 WAL
    ///
    /// # 参数
    ///
    /// * `data_dir` - 数据目录，WAL 文件将存储在 {data_dir}/wal/ 下
    pub async fn new(data_dir: PathBuf) -> Result<Self, PersistenceError> {
        let root_str = data_dir
            .to_str()
            .ok_or_else(|| PersistenceError::InvalidWalFormat("Invalid data dir path".to_string()))?;

        // 使用 scheme 创建 FS operator
        let mut map = std::collections::HashMap::new();
        map.insert("root".to_string(), root_str.to_string());

        let op = Operator::via_iter(opendal::Scheme::Fs, map)
            .map_err(|e| PersistenceError::Internal(format!("Failed to create FS operator: {}", e)))?;

        // 创建 WAL 目录
        let wal_dir = "wal";

        // 找到最大的序列号
        let current_seq = Self::find_max_seq(&op).await?;

        // 计算当前文件大小
        let current_file = format!("{}/{:08}.wal", wal_dir, current_seq);
        let current_size = if op.exists(&current_file).await.unwrap_or(false) {
            let meta = op.stat(&current_file).await.map_err(|e| match e.kind() {
                ErrorKind::NotFound => PersistenceError::InvalidWalFormat(format!(
                    "WAL file {} does not exist",
                    current_seq
                )),
                _ => PersistenceError::Internal(format!("Stat failed: {}", e)),
            })?;
            meta.content_length() as u64
        } else {
            // 创建新文件并写入头
            let header = Self::create_header()?;
            op.write(&current_file, header)
                .await
                .map_err(|e| PersistenceError::Internal(format!("Failed to create WAL file: {}", e)))?;
            WAL_HEADER_SIZE as u64
        };

        Ok(Self {
            op,
            data_dir,
            current_seq: RwLock::new(current_seq),
            current_size: RwLock::new(current_size),
        })
    }

    /// 写入 WAL 记录
    pub async fn append(&self, record: &WALRecord) -> Result<(), PersistenceError> {
        let serialized = record.serialize()?;

        // 检查是否需要轮换
        let need_rotate = {
            let mut size = self.current_size.write().await;
            if *size + serialized.len() as u64 > WAL_MAX_SIZE {
                true
            } else {
                *size += serialized.len() as u64;
                false
            }
        };

        if need_rotate {
            self.rotate().await?;
        }

        // 读取当前文件内容
        let current_file = format!("{}/{:08}.wal", "wal", *self.current_seq.read().await);
        let current_buffer = self.op.read(&current_file).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => PersistenceError::InvalidWalFormat("WAL file does not exist".to_string()),
            _ => PersistenceError::Internal(format!("Read failed: {}", e)),
        })?;

        // 写入新内容
        let new_data = {
            let mut data = current_buffer.to_vec();
            data.extend_from_slice(&serialized);
            Bytes::from(data)
        };

        self.op
            .write(&current_file, new_data)
            .await
            .map_err(|e| PersistenceError::Internal(format!("Write failed: {}", e)))?;

        Ok(())
    }

    /// 读取指定序列号的 WAL 文件
    pub async fn read(&self, seq: u64) -> Result<Vec<WALRecord>, PersistenceError> {
        let file_path = format!("{}/{:08}.wal", "wal", seq);

        let buffer = self.op.read(&file_path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => PersistenceError::InvalidWalFormat(format!(
                "WAL file {} does not exist",
                seq
            )),
            _ => PersistenceError::Internal(format!("Read failed: {}", e)),
        })?;

        let data = buffer.to_vec();

        // 跳过头
        if data.len() < WAL_HEADER_SIZE {
            return Err(PersistenceError::InvalidWalFormat(
                "WAL file too short".to_string(),
            ));
        }

        let mut records = Vec::new();
        let mut cursor = Cursor::new(&data[WAL_HEADER_SIZE..]);

        while cursor.position() < data.len() as u64 {
            // 计算剩余数据长度
            let remaining = (data.len() - cursor.position() as usize) as i64;
            if remaining < 13 {
                // 至少需要 13 字节（1 + 8 + 4）
                break;
            }

            // 读取记录类型
            let mut record_type_byte = [0u8; 1];
            cursor.read_exact(&mut record_type_byte)?;
            let record_type = WALRecordType::try_from(record_type_byte[0])
                .map_err(|e| PersistenceError::InvalidWalFormat(e.to_string()))?;

            // 读取文档 ID
            let mut doc_id_bytes = [0u8; 8];
            cursor.read_exact(&mut doc_id_bytes)?;
            let doc_id = u64::from_le_bytes(doc_id_bytes);

            // 读取负载数据长度
            let mut payload_len_bytes = [0u8; 4];
            cursor.read_exact(&mut payload_len_bytes)?;
            let payload_len = u32::from_le_bytes(payload_len_bytes) as usize;

            // 读取负载数据
            let mut payload = vec![0u8; payload_len];
            cursor.read_exact(&mut payload)?;

            // 读取校验和
            let mut checksum_bytes = [0u8; 4];
            cursor.read_exact(&mut checksum_bytes)?;
            let stored_checksum = u32::from_le_bytes(checksum_bytes);

            // 验证校验和
            use crc32fast::Hasher;
            let record_start = (cursor.position() as usize - 13 - payload_len) + WAL_HEADER_SIZE;
            let record_end = cursor.position() as usize + WAL_HEADER_SIZE;
            let mut hasher = Hasher::new();
            hasher.update(&data[record_start..record_end - 4]);
            let calculated_checksum = hasher.finalize();

            if calculated_checksum != stored_checksum {
                return Err(PersistenceError::ChecksumMismatch(stored_checksum, calculated_checksum));
            }

            records.push(WALRecord {
                record_type,
                doc_id,
                payload: payload.into(),
            });
        }

        Ok(records)
    }

    /// 轮换到新的 WAL 文件
    async fn rotate(&self) -> Result<(), PersistenceError> {
        *self.current_seq.write().await += 1;

        // 创建新文件并写入头
        let current_file = format!("{}/{:08}.wal", "wal", *self.current_seq.read().await);
        let header = Self::create_header()?;

        self.op
            .write(&current_file, header)
            .await
            .map_err(|e| PersistenceError::Internal(format!("Failed to create new WAL file: {}", e)))?;

        Ok(())
    }

    /// 截断 WAL（删除指定序列号及之前的所有 WAL 文件）
    pub async fn truncate(&self, seq: u64) -> Result<(), PersistenceError> {
        let prefix = "wal/";
        let mut stream = self
            .op
            .lister(prefix)
            .await
            .map_err(|e| PersistenceError::Internal(format!("List failed: {}", e)))?;

        while let Some(entry_result) = stream.next().await {
            let entry = entry_result
                .map_err(|e| PersistenceError::Internal(format!("List stream error: {}", e)))?;
            let path = entry.path();

            if path.ends_with(".wal") {
                if let Ok(file_seq) = Self::parse_seq(path) {
                    if file_seq <= seq {
                        self.op
                            .delete(path)
                            .await
                            .map_err(|e| PersistenceError::Internal(format!("Delete failed: {}", e)))?;
                    }
                }
            }
        }

        Ok(())
    }

    /// 清理过期的 WAL 文件
    ///
    /// 保留最近 N 个 WAL 文件
    pub async fn cleanup(&self, keep: usize) -> Result<(), PersistenceError> {
        let mut seqs: Vec<u64> = Vec::new();
        let prefix = "wal/";

        let mut stream = self
            .op
            .lister(prefix)
            .await
            .map_err(|e| PersistenceError::Internal(format!("List failed: {}", e)))?;

        while let Some(entry_result) = stream.next().await {
            let entry = entry_result
                .map_err(|e| PersistenceError::Internal(format!("List stream error: {}", e)))?;
            let path = entry.path();

            if path.ends_with(".wal") {
                if let Ok(seq) = Self::parse_seq(path) {
                    seqs.push(seq);
                }
            }
        }

        seqs.sort();

        // 删除除最近 N 个外的所有文件
        for seq in seqs.iter().take(seqs.len().saturating_sub(keep)) {
            let file_path = format!("{}/{:08}.wal", "wal", seq);
            let _ = self.op.delete(&file_path).await;
        }

        Ok(())
    }

    /// 获取所有 WAL 文件的序列号
    pub async fn list_seqs(&self) -> Result<Vec<u64>, PersistenceError> {
        let mut seqs = Vec::new();
        let prefix = "wal/";

        let mut stream = self
            .op
            .lister(prefix)
            .await
            .map_err(|e| PersistenceError::Internal(format!("List failed: {}", e)))?;

        while let Some(entry_result) = stream.next().await {
            let entry = entry_result
                .map_err(|e| PersistenceError::Internal(format!("List stream error: {}", e)))?;
            let path = entry.path();

            if path.ends_with(".wal") {
                if let Ok(seq) = Self::parse_seq(path) {
                    seqs.push(seq);
                }
            }
        }

        seqs.sort();
        Ok(seqs)
    }

    /// 查找最大的序列号
    async fn find_max_seq(op: &Operator) -> Result<u64, PersistenceError> {
        let mut max_seq = 1u64;

        let prefix = "wal/";

        // 检查目录是否存在
        if !op.exists(prefix).await.unwrap_or(false) {
            return Ok(max_seq);
        }

        let mut stream = op
            .lister(prefix)
            .await
            .map_err(|e| PersistenceError::Internal(format!("List failed: {}", e)))?;

        while let Some(entry_result) = stream.next().await {
            let entry = entry_result
                .map_err(|e| PersistenceError::Internal(format!("List stream error: {}", e)))?;
            let path = entry.path();

            if path.ends_with(".wal") {
                if let Ok(seq) = Self::parse_seq(path) {
                    max_seq = max_seq.max(seq);
                }
            }
        }

        Ok(max_seq)
    }

    /// 从文件名解析序列号
    fn parse_seq(path: &str) -> Result<u64, PersistenceError> {
        let file_name = path
            .strip_prefix("wal/")
            .and_then(|s| s.strip_suffix(".wal"))
            .ok_or_else(|| PersistenceError::InvalidWalFormat(format!("Invalid WAL path: {}", path)))?;

        file_name.parse::<u64>().map_err(|_| {
            PersistenceError::InvalidWalFormat(format!(
                "Invalid WAL sequence number: {}",
                file_name
            ))
        })
    }

    /// 创建 WAL 文件头
    fn create_header() -> Result<Vec<u8>, PersistenceError> {
        let mut header = Vec::with_capacity(WAL_HEADER_SIZE);
        header.extend_from_slice(WAL_MAGIC);
        header.extend_from_slice(&WAL_VERSION.to_le_bytes());

        // 计算校验和
        use crc32fast::Hasher;
        let mut hasher = Hasher::new();
        hasher.update(WAL_MAGIC);
        hasher.update(&WAL_VERSION.to_le_bytes());
        let checksum = hasher.finalize();

        header.extend_from_slice(&checksum.to_le_bytes());

        Ok(header)
    }

    /// 获取当前序列号 (async)
    pub async fn current_seq(&self) -> u64 {
        *self.current_seq.read().await
    }

    /// 获取数据目录
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// 读取原始 WAL 文件字节数据（用于上传）
    pub async fn read_raw(&self, seq: u64) -> Result<Vec<u8>, PersistenceError> {
        let file_path = format!("{}/{:08}.wal", "wal", seq);
        let buffer = self.op.read(&file_path).await.map_err(|e| match e.kind() {
            ErrorKind::NotFound => PersistenceError::InvalidWalFormat(format!(
                "WAL file {} does not exist",
                seq
            )),
            _ => PersistenceError::Internal(format!("Read failed: {}", e)),
        })?;
        Ok(buffer.to_vec())
    }

    /// 写入原始 WAL 文件字节数据（用于恢复）
    pub async fn write_raw(&self, seq: u64, data: Vec<u8>) -> Result<(), PersistenceError> {
        let file_path = format!("{}/{:08}.wal", "wal", seq);
        self.op
            .write(&file_path, data)
            .await
            .map_err(|e| PersistenceError::Internal(format!("Write failed: {}", e)))
    }
}

/// WAL 头大小
const WAL_HEADER_SIZE: usize = 12; // 4 (magic) + 4 (version) + 4 (checksum)

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let wal = LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap();

        let record1 = WALRecord::new(WALRecordType::Add, 1, b"data1".to_vec());
        let record2 = WALRecord::new(WALRecordType::Delete, 2, vec![]);

        wal.append(&record1).await.unwrap();
        wal.append(&record2).await.unwrap();

        let records = wal.read(wal.current_seq().await).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].doc_id, 1);
        assert_eq!(records[1].doc_id, 2);
    }

    #[tokio::test]
    async fn test_rotate() {
        let temp_dir = TempDir::new().unwrap();
        let wal = LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap();

        // 创建多个记录以触发轮换
        for i in 0..1000 {
            let record = WALRecord::new(
                WALRecordType::Add,
                i,
                vec![0u8; 100_000], // 大记录
            );
            wal.append(&record).await.unwrap();
        }

        // 应该有多个 WAL 文件
        let seqs = wal.list_seqs().await.unwrap();
        assert!(seqs.len() > 1);
    }

    #[tokio::test]
    async fn test_truncate() {
        let temp_dir = TempDir::new().unwrap();
        let wal = LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap();

        let record = WALRecord::new(WALRecordType::Add, 1, b"data".to_vec());
        wal.append(&record).await.unwrap();

        // 轮换到新文件
        wal.rotate().await.unwrap();

        // 截断到序列号 1
        wal.truncate(1).await.unwrap();

        // 序列号 1 的文件应该被删除
        let seqs = wal.list_seqs().await.unwrap();
        assert!(!seqs.contains(&1));
    }

    #[tokio::test]
    async fn test_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let wal = LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap();

        // 创建多个记录以触发轮换
        for i in 0..1000 {
            let record = WALRecord::new(
                WALRecordType::Add,
                i,
                vec![0u8; 100_000],
            );
            wal.append(&record).await.unwrap();
        }

        let seqs_before = wal.list_seqs().await.unwrap();

        // 清理，只保留 2 个文件
        wal.cleanup(2).await.unwrap();

        let seqs_after = wal.list_seqs().await.unwrap();
        assert!(seqs_after.len() <= 2);
        assert!(seqs_after.len() < seqs_before.len());
    }

    #[tokio::test]
    async fn test_checksum_validation() {
        let temp_dir = TempDir::new().unwrap();
        let wal = LocalWAL::new(temp_dir.path().to_path_buf()).await.unwrap();

        let record = WALRecord::new(WALRecordType::Add, 1, b"data".to_vec());
        wal.append(&record).await.unwrap();

        // 手动破坏文件
        let seq = wal.current_seq().await;
        let mut data = wal.read_raw(seq).await.unwrap();

        // 破坏某个字节
        data[WAL_HEADER_SIZE + 10] = data[WAL_HEADER_SIZE + 10].wrapping_add(1);

        // 重新写入
        wal.write_raw(seq, data).await.unwrap();

        // 读取应该失败
        let result = wal.read(seq).await;
        assert!(matches!(result, Err(PersistenceError::ChecksumMismatch(_, _))));
    }
}