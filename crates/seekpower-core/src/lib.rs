pub mod commit;
pub mod compression;
pub mod document;
pub mod index;
pub mod lexical_index;
pub mod lifecycle;
pub mod search;
pub mod write_buffer;

// Re-exports for backward compatibility
pub use commit::commit;
pub use compression::{compress_document, decompress_document};
pub use document::{delete_document, get_document, index_document};
pub use index::Index;
pub use lexical_index::LexicalIndex;
pub use lifecycle::{close_index, create_index, create_index_with_db, delete_index, open_index, open_index_with_db};
pub use search::search;
pub use write_buffer::WriteBuffer;

// Re-exports from dependencies
pub use seekpower_error::Result as SeekResult;
pub use seekpower_posting::{PostingEntry, PostingListBlock};
pub use seekpower_ranking::{compress_doc_length, decompress_doc_length};
pub use seekpower_schema::{
    ClusteringConfig, DocCompression, FieldType, IndexConfig, IndexStats, QuantType,
    SchemaField, SchemaMap, Similarity, TokenizerType,
};
pub use seekpower_search::*;
pub use seekpower_vector::{AnnMode, IvfMeta, MedoidData, VectorIndex, VectorSearchResult};

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
