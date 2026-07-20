use crate::document::Document;
use crate::error::{DocStoreError, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// SST 魔术值
const SST_MAGIC: &[u8; 4] = b"SST ";
const SST_VERSION: u32 = 1;

/// 段元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// 段 ID
    pub segment_id: u64,
    /// 层级
    pub level: u32,
    /// 文档数量
    pub doc_count: u64,
    /// 最小 doc_id
    pub min_doc_id: u64,
    /// 最大 doc_id
    pub max_doc_id: u64,
    /// 大小（字节）
    pub size_bytes: u64,
    /// 创建时间（Unix 毫秒）
    pub created_at: u64,
}

/// SST 索引条目
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexEntry {
    doc_id: u64,
    offset: u64,
}

/// SST 读取器
pub struct SSTReader {
    /// 段元数据
    meta: SegmentMeta,
    /// 索引（doc_id → offset）
    index: BTreeMap<u64, u64>,
    /// 数据缓存（doc_id → Document）
    data_cache: BTreeMap<u64, Document>,
}

impl SSTReader {
    /// 从字节数据创建 SST 读取器
    pub fn from_bytes(data: Bytes) -> Result<Self> {
        let data_len = data.len();

        // 读取 Header
        let magic = &data[0..4];
        if magic != SST_MAGIC {
            return Err(DocStoreError::InvalidSstFormat(
                format!("Invalid magic: {:?}", magic)
            ));
        }

        let version_slice: [u8; 4] = data[4..8].try_into().unwrap();
        let version = u32::from_le_bytes(version_slice);
        if version != SST_VERSION {
            return Err(DocStoreError::InvalidSstFormat(
                format!("Unsupported version: {}", version)
            ));
        }

        let count_slice: [u8; 8] = data[8..16].try_into().unwrap();
        let doc_count = u64::from_le_bytes(count_slice);

        // 读取索引
        let index_size = (doc_count as usize) * 16; // doc_id(8) + offset(8)
        let index_data = &data[20..20 + index_size];
        let index = Self::read_index(index_data, doc_count as usize)?;

        // 读取数据块
        let data_start = 20 + index_size; // header(20) + index
        let data_size = data_len - data_start - 4; // 减去 checksum
        let data_bytes = &data[data_start..data_start + data_size];

        // 读取 checksum
        let checksum_slice: [u8; 4] = data[data_start + data_size..data_start + data_size + 4].try_into().unwrap();
        let checksum = u32::from_le_bytes(checksum_slice);
        let actual_checksum = crc32fast::hash(data_bytes);
        if checksum != actual_checksum {
            return Err(DocStoreError::SstError(
                format!("Checksum mismatch: expected {}, got {}", checksum, actual_checksum)
            ));
        }

        // 读取文档数据
        let mut docs = BTreeMap::new();
        let mut offset = 0;

        for _ in 0..doc_count {
            let len_slice: [u8; 4] = data_bytes[offset..offset + 4].try_into().unwrap();
            let doc_len = u32::from_le_bytes(len_slice) as usize;
            let doc_data = &data_bytes[offset + 4..offset + 4 + doc_len];
            let doc: Document = bincode::deserialize(doc_data)
                .map_err(|e| DocStoreError::SerializationError(format!("bincode error: {}", e)))?;
            if let Some(doc_id) = doc.doc_id {
                docs.insert(doc_id, doc);
            }
            offset += 4 + doc_len;
        }

        // 构建段元数据
        let min_doc_id = docs.keys().next().copied().unwrap_or(0);
        let max_doc_id = docs.keys().next_back().copied().unwrap_or(0);

        let meta = SegmentMeta {
            segment_id: 0, // 从文件名推断
            level: 0,
            doc_count,
            min_doc_id,
            max_doc_id,
            size_bytes: data.len() as u64,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        Ok(Self {
            meta,
            index,
            data_cache: docs,
        })
    }

    /// 读取索引
    fn read_index(data: &[u8], count: usize) -> Result<BTreeMap<u64, u64>> {
        let mut index = BTreeMap::new();
        let mut buf = data;

        for _ in 0..count {
            let doc_id = buf.get_u64_le();
            let offset = buf.get_u64_le();
            index.insert(doc_id, offset);
        }

        Ok(index)
    }

    /// 获取段元数据
    pub fn meta(&self) -> &SegmentMeta {
        &self.meta
    }

    /// 查询文档
    pub fn get(&self, doc_id: u64) -> Result<Option<Document>> {
        Ok(self.data_cache.get(&doc_id).cloned())
    }

    /// 检查是否包含 doc_id
    pub fn contains(&self, doc_id: u64) -> bool {
        self.index.contains_key(&doc_id) || self.data_cache.contains_key(&doc_id)
    }

    /// 获取所有 doc_id
    pub fn doc_ids(&self) -> Vec<u64> {
        self.data_cache.keys().copied().collect()
    }

    /// 迭代所有文档
    pub fn iter(&self) -> Vec<(u64, Document)> {
        self.data_cache.iter().map(|(k, v)| (*k, v.clone())).collect()
    }
}

/// SST 写入器
pub struct SSTWriter {
    /// 缓冲的文档（按 doc_id 排序）
    docs: BTreeMap<u64, Document>,
    /// 段 ID
    segment_id: u64,
    /// 层级
    level: u32,
}

impl SSTWriter {
    /// 创建新的 SST 写入器
    pub fn new(segment_id: u64, level: u32) -> Self {
        Self {
            docs: BTreeMap::new(),
            segment_id,
            level,
        }
    }

    /// 添加文档
    pub fn add(&mut self, doc: Document) {
        if let Some(doc_id) = doc.doc_id {
            self.docs.insert(doc_id, doc);
        }
    }

    /// 批量添加文档
    pub fn add_all(&mut self, docs: Vec<(u64, Document)>) {
        for (doc_id, doc) in docs {
            self.docs.insert(doc_id, doc);
        }
    }

    /// 获取文档数量
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// 写入到字节
    pub fn to_bytes(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        // 写入 Header
        buf.extend_from_slice(SST_MAGIC);
        buf.put_u32_le(SST_VERSION);
        buf.put_u64_le(self.docs.len() as u64);
        // TODO: 计算 header checksum
        buf.put_u32_le(0);

        // 索引起始位置（数据开始）
        let index_start_pos = buf.len();

        // 预留索引空间
        let index_size = self.docs.len() * 16; // doc_id(8) + offset(8)
        for _ in 0..index_size {
            buf.put_u8(0);
        }

        // 数据起始位置
        let data_start_pos = buf.len();

        // 写入数据
        let mut index_entries = Vec::new();
        for (&doc_id, doc) in self.docs.iter() {
            let offset = (buf.len() - data_start_pos) as u64;
            index_entries.push(IndexEntry { doc_id, offset });

            let doc_data = bincode::serialize(doc)
                .map_err(|e| DocStoreError::SerializationError(format!("bincode error: {}", e)))?;
            buf.put_u32_le(doc_data.len() as u32);
            buf.extend_from_slice(&doc_data);
        }

        // 写入索引（回填）
        let mut index_buf = Vec::new();
        for entry in index_entries {
            index_buf.put_u64_le(entry.doc_id);
            index_buf.put_u64_le(entry.offset);
        }

        // 回填索引
        let end_pos = buf.len();
        let mut buf = buf.to_vec();
        buf.splice(index_start_pos..index_start_pos + index_size, index_buf);
        buf = buf.to_vec();

        // 写入 checksum
        let data_only = &buf[data_start_pos..end_pos];
        let checksum = crc32fast::hash(data_only);
        buf.extend_from_slice(&checksum.to_le_bytes());

        Ok(buf.into())
    }

    /// 构建段元数据
    pub fn build_meta(&self) -> SegmentMeta {
        let doc_count = self.docs.len() as u64;
        let min_doc_id = self.docs.keys().next().copied().unwrap_or(0);
        let max_doc_id = self.docs.keys().next_back().copied().unwrap_or(0);

        SegmentMeta {
            segment_id: self.segment_id,
            level: self.level,
            doc_count,
            min_doc_id,
            max_doc_id,
            size_bytes: 0, // 序列化后计算
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocumentValue;

    #[test]
    fn test_sst_writer_creation() {
        let writer = SSTWriter::new(1, 0);
        assert!(writer.is_empty());
        assert_eq!(writer.len(), 0);
    }

    #[test]
    fn test_sst_writer_add() {
        let mut writer = SSTWriter::new(1, 0);
        let mut doc = Document::new();
        doc.insert("key".to_string(), DocumentValue::String("value".to_string()));
        doc.doc_id = Some(1);

        writer.add(doc);
        assert_eq!(writer.len(), 1);
        assert!(!writer.is_empty());
    }

    #[test]
    fn test_sst_writer_to_bytes() {
        let mut writer = SSTWriter::new(1, 0);
        let mut doc = Document::new();
        doc.insert("key".to_string(), DocumentValue::String("value".to_string()));
        doc.doc_id = Some(1);

        writer.add(doc);

        let bytes = writer.to_bytes().unwrap();
        assert!(!bytes.is_empty());

        // 验证 magic
        assert_eq!(&bytes[0..4], SST_MAGIC);
        assert_eq!(bytes[4..8].to_vec(), (1u32).to_le_bytes().to_vec());
    }

    #[test]
    fn test_sst_reader_from_bytes() {
        let mut writer = SSTWriter::new(1, 0);

        for i in 1..=3 {
            let mut doc = Document::new();
            doc.insert(format!("key_{}", i), DocumentValue::String(format!("value_{}", i)));
            doc.doc_id = Some(i);
            writer.add(doc);
        }

        let bytes = writer.to_bytes().unwrap();
        let reader = SSTReader::from_bytes(bytes).unwrap();

        assert_eq!(reader.meta().doc_count, 3);
        assert_eq!(reader.get(1).unwrap().unwrap().doc_id, Some(1));
        assert_eq!(reader.get(2).unwrap().unwrap().doc_id, Some(2));
        assert_eq!(reader.get(3).unwrap().unwrap().doc_id, Some(3));
    }

    #[test]
    fn test_sst_reader_get_not_found() {
        let mut writer = SSTWriter::new(1, 0);
        let mut doc = Document::new();
        doc.doc_id = Some(1);
        writer.add(doc);

        let bytes = writer.to_bytes().unwrap();
        let reader = SSTReader::from_bytes(bytes).unwrap();

        assert!(reader.get(999).unwrap().is_none());
    }

    #[test]
    fn test_sst_reader_contains() {
        let mut writer = SSTWriter::new(1, 0);
        let mut doc = Document::new();
        doc.doc_id = Some(1);
        writer.add(doc);

        let bytes = writer.to_bytes().unwrap();
        let reader = SSTReader::from_bytes(bytes).unwrap();

        assert!(reader.contains(1));
        assert!(!reader.contains(999));
    }

    #[test]
    fn test_sst_writer_add_all() {
        let mut writer = SSTWriter::new(1, 0);

        let docs = vec![
            (1, {
                let mut d = Document::new();
                d.doc_id = Some(1);
                d.insert("k".to_string(), DocumentValue::String("v".to_string()));
                d
            }),
            (2, {
                let mut d = Document::new();
                d.doc_id = Some(2);
                d.insert("k".to_string(), DocumentValue::String("v".to_string()));
                d
            }),
        ];

        writer.add_all(docs);
        assert_eq!(writer.len(), 2);
    }

    #[test]
    fn test_sst_invalid_magic() {
        let mut data = Vec::new();
        data.extend_from_slice(b"BAD ");
        data.extend_from_slice(&(1u32).to_le_bytes());
        data.extend_from_slice(&(0u64).to_le_bytes());
        data.extend_from_slice(&(0u32).to_le_bytes());

        let result = SSTReader::from_bytes(data.into());
        assert!(matches!(result, Err(DocStoreError::InvalidSstFormat(_))));
    }
}