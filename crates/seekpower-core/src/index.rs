use ahash::AHashSet;
use seekpower_schema::{IndexConfig, IndexStats, SchemaMap};
use seekpower_storage::KvStore;
use seekpower_vector::IvfIndex;

use crate::lexical_index::LexicalIndex;
use crate::search::QueryEngine;

pub struct Index {
    pub db: KvStore,
    pub path: String,
    pub schema: SchemaMap,
    pub field_names: Vec<String>,
    pub config: IndexConfig,
    pub stats: tokio::sync::RwLock<IndexStats>,
    pub next_doc_id: std::sync::atomic::AtomicU64,
    pub lexical_index: LexicalIndex,
    pub vector_index: IvfIndex,
    pub query_engine: QueryEngine,
    pub delete_set: tokio::sync::RwLock<AHashSet<u64>>,
}
