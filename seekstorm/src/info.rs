//! Typed index statistics, independent of HTTP and terminal presentation.
use crate::{
    index::{IndexArc, IndexMetaObject, META_FILENAME},
    utils::dir_size,
    vector::{Precision, Quantization},
    vector_similarity::VectorSimilarity,
};
use std::{fs::metadata, path::Path, time::SystemTime};

/// A snapshot of index configuration and counters. Collection does not commit or sync files.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index identity and configuration.
    pub meta: IndexMetaObject,
    /// Stored major and minor format versions.
    pub format_version: (u16, u16),
    /// Indexed document count.
    pub indexed_documents: usize,
    /// Deleted document count.
    pub deleted_documents: usize,
    /// Schema field count.
    pub fields: usize,
    /// Indexed field count.
    pub indexed_fields: usize,
    /// Stored field count.
    pub stored_fields: usize,
    /// Internal shard count.
    pub shards: usize,
    /// Index level count.
    pub levels: usize,
    /// Size of the index directory in bytes.
    pub index_size_bytes: u64,
    /// Filesystem creation time, if supported.
    pub creation_time: Option<SystemTime>,
    /// Whether lexical indexing is enabled.
    pub lexical: bool,
    /// Whether vector indexing is enabled.
    pub vector: bool,
    /// Lexical segment count.
    pub segments: usize,
    /// Filter/sort metadata field count.
    pub metadata_fields: usize,
    /// Stop-word count.
    pub stop_words: usize,
    /// Frequent-word count.
    pub frequent_words: usize,
    /// Synonym map entry count.
    pub synonyms: usize,
    /// Indexed vector count.
    pub indexed_vectors: usize,
    /// Indexed cluster count.
    pub indexed_clusters: usize,
    /// Original vector dimensionality.
    pub vector_dimensions: usize,
    /// Internal vector dimensionality, including padding.
    pub padded_dimensions: usize,
    /// Vector precision.
    pub precision: Precision,
    /// Vector quantization method.
    pub quantization: Quantization,
    /// Whether Euclidean scalar quantization uses an affine mapping; otherwise None.
    pub scalar_affine: Option<bool>,
    /// Vector similarity metric.
    pub vector_similarity: VectorSimilarity,
}

/// Collect typed statistics without formatting, printing, committing or syncing index files.
pub async fn collect_index_info(index: &IndexArc) -> IndexInfo {
    let index = index.read().await;
    let indexed_documents = index.indexed_doc_count().await;
    let indexed_vectors = index.indexed_vector_count().await;
    let indexed_clusters = index.indexed_cluster_count().await;
    let levels = index.level_count().await;
    let shards = index.shard_count().await;
    let shard = index.shard_vec[0].read().await;
    let scalar_affine = (index.quantization == Quantization::ScalarQuantizationI8
        && index.vector_similarity == VectorSimilarity::Euclidean)
        .then_some(shard.max_vector_value != f32::MIN);
    IndexInfo {
        meta: index.meta.clone(),
        format_version: (
            index.index_format_version_major,
            index.index_format_version_minor,
        ),
        indexed_documents,
        deleted_documents: index.deleted_doc_count,
        fields: index.schema_map.len(),
        indexed_fields: index.indexed_field_vec.len(),
        stored_fields: index.stored_field_names.len(),
        shards,
        levels,
        index_size_bytes: dir_size(Path::new(&index.index_path_string)).unwrap_or(0),
        creation_time: metadata(Path::new(&index.index_path_string).join(META_FILENAME))
            .ok()
            .and_then(|metadata| metadata.created().ok()),
        lexical: index.is_lexical_indexing,
        vector: index.is_vector_indexing,
        segments: index.segment_number1,
        metadata_fields: index.facets.len(),
        stop_words: shard.stop_words.len(),
        frequent_words: shard.frequent_words.len(),
        synonyms: index.synonyms_map.len(),
        indexed_vectors,
        indexed_clusters,
        vector_dimensions: index.vector_dimensions_original,
        padded_dimensions: index.vector_dimensions,
        precision: index.vector_precision,
        quantization: index.quantization,
        scalar_affine,
        vector_similarity: index.vector_similarity,
    }
}
