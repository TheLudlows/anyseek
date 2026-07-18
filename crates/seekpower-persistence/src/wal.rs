//! WAL 类型定义
//!
//! 定义 WAL 记录的类型和结构

use serde::{Deserialize, Serialize};
use std::io::{self, Cursor, Read, Write};
use std::ops::Deref;

/// WAL 记录类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WALRecordType {
    /// 添加文档
    Add = 1,
    /// 删除文档
    Delete = 2,
}

impl TryFrom<u8> for WALRecordType {
    type Error = io::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(WALRecordType::Add),
            2 => Ok(WALRecordType::Delete),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid WAL record type: {}", value),
            )),
        }
    }
}

/// 字节数据包装器
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrappedBytes {
    data: Vec<u8>,
}

impl Deref for WrappedBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl From<Vec<u8>> for WrappedBytes {
    fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl From<WrappedBytes> for Vec<u8> {
    fn from(wrapped: WrappedBytes) -> Self {
        wrapped.data
    }
}

impl Serialize for WrappedBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_bytes(&self.data)
    }
}

impl<'de> Deserialize<'de> for WrappedBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct BytesVisitor;

        impl<'de> serde::de::Visitor<'de> for BytesVisitor {
            type Value = WrappedBytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("byte array")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(WrappedBytes { data: v.to_vec() })
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(WrappedBytes { data: v })
            }
        }

        deserializer.deserialize_bytes(BytesVisitor)
    }
}

/// WAL 记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALRecord {
    /// 记录类型
    pub record_type: WALRecordType,
    /// 文档 ID
    pub doc_id: u64,
    /// 负载数据
    pub payload: WrappedBytes,
}

impl WALRecord {
    /// 创建新的 WAL 记录
    pub fn new(record_type: WALRecordType, doc_id: u64, payload: Vec<u8>) -> Self {
        Self {
            record_type,
            doc_id,
            payload: payload.into(),
        }
    }

    /// 序列化为字节数组
    pub fn serialize(&self) -> Result<Vec<u8>, io::Error> {
        let mut buffer = Vec::new();

        // 写入记录类型
        buffer.write_all(&[self.record_type as u8])?;

        // 写入文档 ID
        buffer.write_all(&self.doc_id.to_le_bytes())?;

        // 写入负载数据长度
        let payload_len = self.payload.len() as u32;
        buffer.write_all(&payload_len.to_le_bytes())?;

        // 写入负载数据
        buffer.write_all(&self.payload)?;

        // 计算并写入校验和（CRC32）
        use crc32fast::Hasher;
        let mut hasher = Hasher::new();
        hasher.update(&buffer);
        let checksum = hasher.finalize();
        buffer.write_all(&checksum.to_le_bytes())?;

        Ok(buffer)
    }

    /// 从字节数组反序列化
    pub fn deserialize(data: &[u8]) -> Result<Self, io::Error> {
        if data.len() < 13 {
            // 1 (record_type) + 8 (doc_id) + 4 (payload_len) = 13
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Data too short for WAL record",
            ));
        }

        let mut cursor = Cursor::new(data);

        // 读取记录类型
        let mut record_type_byte = [0u8; 1];
        cursor.read_exact(&mut record_type_byte)?;
        let record_type = WALRecordType::try_from(record_type_byte[0])?;

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
        let payload = payload.into();

        // 读取校验和
        let mut checksum_bytes = [0u8; 4];
        cursor.read_exact(&mut checksum_bytes)?;
        let stored_checksum = u32::from_le_bytes(checksum_bytes);

        // 验证校验和
        use crc32fast::Hasher;
        let record_start = (cursor.position() as usize - 13 - payload_len);
        let record_end = cursor.position() as usize + 4;
        let mut hasher = Hasher::new();
        hasher.update(&data[record_start..record_end - 4]);
        let calculated_checksum = hasher.finalize();

        if calculated_checksum != stored_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Checksum mismatch: expected {}, got {}",
                    stored_checksum, calculated_checksum
                ),
            ));
        }

        Ok(Self {
            record_type,
            doc_id,
            payload,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_deserialize() {
        let record = WALRecord::new(
            WALRecordType::Add,
            42,
            b"test payload data".to_vec(),
        );

        let serialized = record.serialize().unwrap();
        let deserialized = WALRecord::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.record_type, WALRecordType::Add);
        assert_eq!(deserialized.doc_id, 42);
        assert_eq!(deserialized.payload.as_ref(), b"test payload data");
    }

    #[test]
    fn test_record_type_conversion() {
        assert_eq!(
            WALRecordType::try_from(1).unwrap(),
            WALRecordType::Add
        );
        assert_eq!(
            WALRecordType::try_from(2).unwrap(),
            WALRecordType::Delete
        );
        assert!(WALRecordType::try_from(3).is_err());
    }

    #[test]
    fn test_checksum_validation() {
        let record = WALRecord::new(
            WALRecordType::Add,
            42,
            b"test payload data".to_vec(),
        );

        let mut serialized = record.serialize().unwrap();

        // 破坏校验和
        let len = serialized.len();
        serialized[len - 1] = serialized[len - 1].wrapping_add(1);

        let result = WALRecord::deserialize(&serialized);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_record() {
        let record = WALRecord::new(
            WALRecordType::Delete,
            123,
            vec![],
        );

        let serialized = record.serialize().unwrap();
        let deserialized = WALRecord::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.record_type, WALRecordType::Delete);
        assert_eq!(deserialized.doc_id, 123);
        assert!(deserialized.payload.is_empty());
    }
}