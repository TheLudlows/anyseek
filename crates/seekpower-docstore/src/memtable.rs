use crate::document::Document;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// 内存表（MemTable）
///
/// 基于 DashMap 的并发安全内存缓冲，支持高并发写入
pub struct MemTable {
    /// 文档缓冲（doc_id → Document）
    documents: DashMap<u64, Document>,
    /// 下一个 doc_id（原子计数器）
    next_doc_id: AtomicU64,
    /// 当前大小（字节）
    size_bytes: AtomicU64,
}

impl MemTable {
    /// 创建新的 MemTable
    pub fn new() -> Self {
        Self {
            documents: DashMap::new(),
            next_doc_id: AtomicU64::new(1),
            size_bytes: AtomicU64::new(0),
        }
    }

    /// 创建从指定 doc_id 开始的 MemTable
    pub fn with_start_doc_id(start_doc_id: u64) -> Self {
        Self {
            documents: DashMap::new(),
            next_doc_id: AtomicU64::new(start_doc_id),
            size_bytes: AtomicU64::new(0),
        }
    }

    /// 插入文档，返回 doc_id
    pub fn insert(&self, doc: Document) -> u64 {
        let doc_id = self.next_doc_id.fetch_add(1, Ordering::SeqCst);
        let mut doc_with_id = doc;
        doc_with_id.doc_id = Some(doc_id);
        let size = doc_with_id.size_bytes();
        self.documents.insert(doc_id, doc_with_id);
        self.size_bytes.fetch_add(size as u64, Ordering::SeqCst);
        doc_id
    }

    /// 获取文档
    pub fn get(&self, doc_id: u64) -> Option<Document> {
        self.documents.get(&doc_id).map(|v| v.value().clone())
    }

    /// 检查是否包含文档
    pub fn contains(&self, doc_id: u64) -> bool {
        self.documents.contains_key(&doc_id)
    }

    /// 检查是否需要 flush
    pub fn needs_flush(&self, threshold: usize) -> bool {
        self.size_bytes.load(Ordering::SeqCst) >= threshold as u64
    }

    /// 清空并返回所有文档（用于 flush）
    pub fn drain(&self) -> Vec<(u64, Document)> {
        let mut docs = Vec::new();
        let keys: Vec<u64> = self.documents.iter().map(|entry| *entry.key()).collect();
        for key in keys {
            if let Some((id, doc)) = self.documents.remove(&key) {
                docs.push((id, doc));
            }
        }
        self.size_bytes.store(0, Ordering::SeqCst);
        docs
    }

    /// 获取文档数量
    pub fn len(&self) -> usize {
        self.documents.len()
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    /// 获取当前大小（字节）
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes.load(Ordering::SeqCst)
    }

    /// 获取下一个将要分配的 doc_id（不分配）
    pub fn peek_next_doc_id(&self) -> u64 {
        self.next_doc_id.load(Ordering::SeqCst)
    }

    /// 重置 doc_id 分配器（用于恢复）
    pub fn reset_doc_id(&self, new_doc_id: u64) {
        self.next_doc_id.store(new_doc_id, Ordering::SeqCst);
    }

    /// 更新文档
    pub fn update(&self, doc_id: u64, doc: Document) -> Option<Document> {
        let mut doc_with_id = doc;
        doc_with_id.doc_id = Some(doc_id);

        // 计算大小变化
        let old_size = self.documents.get(&doc_id)
            .map(|v| v.value().size_bytes() as u64)
            .unwrap_or(0);
        let new_size = doc_with_id.size_bytes() as u64;
        let delta = new_size as i64 - old_size as i64;

        let old = self.documents.insert(doc_id, doc_with_id);

        if delta > 0 {
            self.size_bytes.fetch_add(delta as u64, Ordering::SeqCst);
        } else if delta < 0 {
            self.size_bytes.fetch_sub(delta.unsigned_abs(), Ordering::SeqCst);
        }

        old
    }

    /// 删除文档
    pub fn remove(&self, doc_id: u64) -> Option<Document> {
        if let Some((_, old)) = self.documents.remove(&doc_id) {
            self.size_bytes.fetch_sub(old.size_bytes() as u64, Ordering::SeqCst);
            Some(old)
        } else {
            None
        }
    }

    /// 获取所有文档 ID
    pub fn doc_ids(&self) -> Vec<u64> {
        self.documents.iter().map(|entry| *entry.key()).collect()
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::DocumentValue;

    #[test]
    fn test_memtable_creation() {
        let memtable = MemTable::new();
        assert!(memtable.is_empty());
        assert_eq!(memtable.len(), 0);
        assert_eq!(memtable.size_bytes(), 0);
        assert_eq!(memtable.peek_next_doc_id(), 1);
    }

    #[test]
    fn test_memtable_with_start_doc_id() {
        let memtable = MemTable::with_start_doc_id(100);
        assert_eq!(memtable.peek_next_doc_id(), 100);
    }

    #[test]
    fn test_memtable_insert_and_get() {
        let memtable = MemTable::new();
        let mut doc = Document::new();
        doc.insert("key".to_string(), DocumentValue::String("value".to_string()));

        let doc_id = memtable.insert(doc);
        assert_eq!(doc_id, 1);
        assert_eq!(memtable.len(), 1);
        assert!(memtable.contains(doc_id));

        let retrieved = memtable.get(doc_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().doc_id, Some(1));
    }

    #[test]
    fn test_memtable_doc_id_sequential() {
        let memtable = MemTable::new();

        let id1 = memtable.insert(Document::new());
        let id2 = memtable.insert(Document::new());
        let id3 = memtable.insert(Document::new());

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_memtable_size_tracking() {
        let memtable = MemTable::new();
        let mut doc = Document::new();
        doc.insert("key".to_string(), DocumentValue::String("value".to_string()));

        memtable.insert(doc);
        assert!(memtable.size_bytes() > 0);
    }

    #[test]
    fn test_memtable_needs_flush() {
        let memtable = MemTable::new();
        assert!(!memtable.needs_flush(100));

        let mut doc = Document::new();
        doc.insert("data".to_string(), DocumentValue::String("a".repeat(200)));
        memtable.insert(doc);

        assert!(memtable.needs_flush(100));
    }

    #[test]
    fn test_memtable_drain() {
        let memtable = MemTable::new();

        memtable.insert(Document::new());
        memtable.insert(Document::new());
        memtable.insert(Document::new());

        assert_eq!(memtable.len(), 3);

        let docs = memtable.drain();
        assert_eq!(docs.len(), 3);
        assert!(memtable.is_empty());
        assert_eq!(memtable.size_bytes(), 0);
    }

    #[test]
    fn test_memtable_update() {
        let memtable = MemTable::new();
        let mut doc = Document::new();
        doc.insert("key".to_string(), DocumentValue::String("value".to_string()));

        let doc_id = memtable.insert(doc.clone());
        let old = memtable.update(doc_id, doc);
        assert!(old.is_some());
        assert_eq!(memtable.len(), 1);
    }

    #[test]
    fn test_memtable_remove() {
        let memtable = MemTable::new();
        let doc = Document::new();

        let doc_id = memtable.insert(doc);
        assert!(memtable.contains(doc_id));

        let removed = memtable.remove(doc_id);
        assert!(removed.is_some());
        assert!(!memtable.contains(doc_id));
        assert_eq!(memtable.len(), 0);
    }

    #[test]
    fn test_memtable_doc_ids() {
        let memtable = MemTable::new();

        memtable.insert(Document::new());
        memtable.insert(Document::new());
        memtable.insert(Document::new());

        let ids = memtable.doc_ids();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_memtable_reset_doc_id() {
        let memtable = MemTable::new();
        assert_eq!(memtable.peek_next_doc_id(), 1);

        memtable.reset_doc_id(100);
        assert_eq!(memtable.peek_next_doc_id(), 100);

        let doc_id = memtable.insert(Document::new());
        assert_eq!(doc_id, 100);
    }
}