//! Public data types for the vector index crate.

use seekpower_schema::{QuantType, Similarity};
use std::collections::HashSet;

/// A single result from a vector search.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub doc_id: u64,
    pub score: f32,
    pub field_id: u32,
    pub chunk_id: u32,
}

/// Controls how many clusters are probed during an ANN search.
#[derive(Debug, Clone)]
pub enum AnnMode {
    /// Search every cluster (exact k-NN).
    All,
    /// Search only the top-n most similar clusters.
    Nprobe(usize),
    /// Search every cluster whose medoid similarity >= threshold.
    SimilarityThreshold(f32),
}

/// Persisted metadata for an IVF index (stored at `v:{path}:meta`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IvfMeta {
    pub dim: u32,
    pub n_clusters: u32,
    pub similarity: Similarity,
    pub quantization: QuantType,
    pub total_vectors: u64,
}

/// Persisted medoid (cluster centre) stored at `v:{path}:medoid:{id}`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MedoidData {
    pub medoid_id: u32,
    pub vector: Vec<f32>,
    pub child_count: u32,
}

/// On-wire header for a cluster blob.
#[derive(Debug, Clone)]
pub struct ClusterHeader {
    pub cluster_id: u32,
    pub vector_count: u32,
    pub byte_size: u32,
}

/// On-wire header for each quantized vector inside a cluster blob.
#[derive(Debug, Clone)]
pub struct QuantizedVectorHeader {
    pub doc_id: u64,
    pub field_id: u32,
    pub chunk_id: u32,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
}

/// Trait implemented by all vector index backends.
#[allow(async_fn_in_trait)]
pub trait VectorIndex: Send + Sync {
    async fn search(
        &self,
        query: &[f32],
        k: usize,
        ann_mode: &AnnMode,
        delete_set: &HashSet<u64>,
    ) -> Vec<VectorSearchResult>;

    fn add_vector(&self, doc_id: u64, field_id: u32, vector: Vec<f32>);

    async fn flush(&self) -> seekpower_error::Result<()>;

    fn n_clusters(&self) -> usize;
}
