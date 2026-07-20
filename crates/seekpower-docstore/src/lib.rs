// Phase 3: Document Store - 文档存储（WAL-First 架构）
//
// 核心功能：
// - Document 和 DocumentValue 定义
// - FieldType 自动推断（包括 F32、Vector）
// - CAS 序列分配器（无状态 doc_id 分配）
// - WAL Store（直接写对象存储）
// - SST 段文件管理
// - DocStore 主逻辑（插入、查询、删除、更新）
// - DeleteSet（持久化删除集）
// - 后台索引器（异步构建 SST）

mod config;
mod delete_set;
mod docstore;
mod document;
mod error;
mod indexer;
mod sequence;
mod sst;
mod wal_store;

pub use config::DocStoreConfig;
pub use delete_set::{DeleteSet, DeleteSetStats};
pub use docstore::{DocStore, DocStoreStats};
pub use document::{Document, DocumentValue};
pub use error::{DocStoreError, Result};
pub use indexer::{Indexer, IndexerConfig, IndexerStats};
pub use sequence::SequenceAllocator;
pub use sst::{SSTReader, SSTWriter, SegmentMeta};
pub use wal_store::{WALRecord, WALRecordType, WALStore};