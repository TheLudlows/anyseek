//! Vector index crate: IVF index with KV-based storage, scalar quantization,
//! and pluggable clustering.

mod clustering;
mod codec;
mod ivf;
mod types;

// Re-export public API
pub use clustering::{ClusterCount, ClusterVector, Clusterer, KMedoidClusterer};
pub use codec::{
    decode_cluster_header, decode_qv_header, deserialize_cluster, encode_cluster_header,
    encode_qv_header, serialize_cluster,
};
pub use ivf::IvfIndex;
pub use types::{
    AnnMode, ClusterHeader, IvfMeta, MedoidData, QuantizedVectorHeader, VectorIndex,
    VectorSearchResult,
};

// Re-exports from seekpower-schema for convenience
pub use seekpower_schema::{make_key, QuantType, Similarity, TYPE_VEC};
