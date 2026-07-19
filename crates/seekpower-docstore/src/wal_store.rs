use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::document::Document;
use crate::error::{DocStoreError, Result};

/// WAL 魔术值
const WAL_MAGIC: &[u8; 4] = b"WLOG";
const WAL_VERSION: u32 = 1;
const WAL_MAX_SEGMENT_SIZE: u64 = 100 * 1024 * 1024;  // 100MB per segment

/// WAL 记录类型
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WALRecordType {
    Add = 1,
    Delete = 2,
}

/// WAL 记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALRecord {
    pub record_type: WALRecordType,
    pub doc_id: u64,
    pub payload: Document,
}

impl WALRecord {
    /// 序列化为字节
    pub fn to_bytes(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        // record_type: 1 byte
        buf.put_u8(self.record_type as u8);

        // doc_id: 8 bytes
        buf.put_u64_le(self.doc_id);

        // payload: JSON 序列化
        let payload_json = serde_json::to_vec(&self.payload)
            .map_err(|e| DocStoreError::SerializationError(format!("JSON error: {}", e)))?;

        // payload_len: 4 bytes
        buf.put_u32_le(payload_json.len() as u32);

        // payload
        buf.extend_from_slice(&payload_json);

        // checksum: 4 bytes (CRC32)
        let checksum = crc32fast::hash(&buf);
        buf.put_u32_le(checksum);

        Ok(buf.freeze())
    }

    /// 从字节反序列化
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut buf = data;

        if buf.len() < 14 {
            return Err(DocStoreError::InvalidWalFormat("Record too short".to_string()));
        }

        // record_type
        let record_type_byte = buf.get_u8();
        let record_type = match record_type_byte {
            1 => WALRecordType::Add,
            2 => WALRecordType::Delete,
            _ => {
                return Err(DocStoreError::InvalidWalFormat(format!(
                    "Unknown record type: {}",
                    record_type_byte
                )))
            }
        };

        // doc_id
        let doc_id = buf.get_u64_le();

        // payload_len
        let payload_len = buf.get_u32_le() as usize;

        if buf.len() < 13 + payload_len + 4 {
            return Err(DocStoreError::InvalidWalFormat(
                "Payload length mismatch".to_string(),
            ));
        }

        // payload
        let payload_bytes = &buf[..13 + payload_len];
        let payload: Document = serde_json::from_slice(payload_bytes)
            .map_err(|e| DocStoreError::SerializationError(format!("JSON error: {}", e)))?;

        // 验证 checksum
        let expected_checksum = buf[13 + payload_len..17]
            .try_into()
            .map(u32::from_le_bytes)
            .unwrap();
        let actual_checksum = crc32fast::hash(&data[..13 + payload_len]);

        if expected_checksum != actual_checksum {
            return Err(DocStoreError::InvalidWalFormat("Checksum mismatch".to_string()));
        }

        Ok(WALRecord {
            record_type,
            doc_id,
            payload,
        })
    }
}

/// WAL 头部
#[derive(Debug, Clone)]
struct WALHeader {
    magic: [u8; 4],
    version: u32,
    sequence: u64,
    checksum: u32,
}

impl WALHeader {
    fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(20);
        buf.extend_from_slice(&self.magic);
        buf.put_u32_le(self.version);
        buf.put_u64_le(self.sequence);
        buf.put_u32_le(self.checksum);
        buf.freeze()
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 20 {
            return Err(DocStoreError::InvalidWalFormat("Header too short".to_string()));
        }

        let mut buf = data;
        let mut magic = [0u8; 4];
        buf.copy_to_slice(&magic);

        if &magic != WAL_MAGIC {
            return Err(DocStoreError::InvalidWalFormat(format!(
                "Invalid magic: {:?}",
                magic
            )));
        }

        let version = buf.get_u32_le();
        if version != WAL_VERSION {
            return Err(DocStoreError::InvalidWalFormat(format!(
                "Unsupported version: {}",
                version
            )));
        }

        let sequence = buf.get_u64_le();
        let checksum = buf.get_u32_le();

        Ok(WALHeader {
            magic,
            version,
            sequence,
            checksum,
        })
    }
}

/// WAL Store - 直接写对象存储
pub struct WALStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    current_sequence: Arc<AtomicU64>,
    current_size: Arc<AtomicU64>,
    current_offset: Arc<AtomicU64>,
}

impl WALStore {
    /// 创建新的 WAL Store
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            current_sequence: Arc::new(AtomicU64::new(1)),
            current_size: Arc::new(AtomicU64::new(0)),
            current_offset: Arc::new(AtomicU64::new(20)),  // header size
        }
    }

    /// 追加记录到 WAL
    pub async fn append(&self, record: &WALRecord) -> Result<u64> {
        let record_bytes = record.to_bytes()?;
        let record_size = record_bytes.len() as u64;

        // 检查是否需要轮换
        if self.current_size.load(Ordering::Acquire) + record_size > WAL_MAX_SEGMENT_SIZE {
            self.rotate().await?;
        }

        let current_seq = self.current_sequence.load(Ordering::Acquire);
        let offset = self.current_offset.load(Ordering::Acquire);

        // 追加到当前 WAL 段（简化实现：直接覆盖）
        let path = format!("{}/wal/{}.wal", self.prefix, current_seq);

        // 读取现有数据
        let existing = self.store.get(&path).await.ok();
        let mut data = existing.map(|b| b.to_vec()).unwrap_or_default();

        // 确保足够空间
        let required_size = (offset + record_bytes.len()) as usize;
        if data.len() < required_size {
            data.resize(required_size, 0);
        }

        // 写入数据
        let offset = offset as usize;
        data[offset..required_size].copy_from_slice(&record_bytes);

        // 写回
        self.store.put(&path, data.into()).await?;

        // 更新状态
        self.current_offset.fetch_add(record_size, Ordering::Release);
        self.current_size.fetch_add(record_size, Ordering::Release);

        Ok(record.doc_id)
    }

    /// 轮换 WAL 段
    async fn rotate(&self) -> Result<()> {
        let new_seq = self.current_sequence.fetch_add(1, Ordering::AcqRel) + 1;

        // 创建新的 WAL 段
        let path = format!("{}/wal/{}.wal", self.prefix, new_seq);
        let header = WALHeader {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            sequence: new_seq,
            checksum: 0,
        };

        self.store.put(&path, header.to_bytes()).await?;

        // 重置状态
        self.current_offset.store(20, Ordering::Release);  // header size
        self.current_size.store(20, Ordering::Release);

        Ok(())
    }

    /// 从 WAL 获取文档
    pub async fn get(&self, doc_id: u64) -> Result<Option<Document>> {
        // TODO: 实现 WAL 查找
        // 暂时返回 None
        Ok(None)
    }

    /// 获取 WAL 总大小
    pub async fn size_bytes(&self) -> Result<u64> {
        let current_seq = self.current_sequence.load(Ordering::Acquire);
        let mut total_size = 0u64;

        for seq in 1..current_seq {
            let path = format!("{}/wal/{}.wal", self.prefix, seq);
            if let Ok(metadata) = self.store.metadata(&path).await {
                total_size += metadata.size;
            }
        }

        // 加上当前段的大小
        total_size += self.current_size.load(Ordering::Acquire);

        Ok(total_size)
    }

    /// 截断 WAL 到指定序列
    pub async fn truncate(&self, up_to_seq: u64) -> Result<()> {
        let current_seq = self.current_sequence.load(Ordering::Acquire);

        for seq in 1..=up_to_seq.min(current_seq) {
            let path = format!("{}/wal/{}.wal", self.prefix, seq);
            let _ = self.store.delete(&path).await?;

            // 删除对应的索引文件
            let idx_path = format!("{}/wal/{}.idx", self.prefix, seq);
            let _ = self.store.delete(&idx_path).await?;
        }

        Ok(())
    }

    /// 构建 WAL 索引（异步调用）
    pub async fn build_index(&self, seq: u64) -> Result<()> {
        // TODO: 实现实际的 WAL 索引构建
        Ok(())
    }
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

    #[test]
    fn test_wal_record_serialization() {
        let mut doc = crate::document::Document::new();
        doc.insert("test".to_string(), crate::document::DocumentValue::String("value".to_string()));

        let record = WALRecord {
            record_type: WALRecordType::Add,
            doc_id: 123,
            payload: doc,
        };

        let bytes = record.to_bytes().unwrap();
        let parsed = WALRecord::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.record_type, WALRecordType::Add);
        assert_eq!(parsed.doc_id, 123);
        assert_eq!(parsed.payload.get("test"), Some(&crate::document::DocumentValue::String("value".to_string())));
    }
}