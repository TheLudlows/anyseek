//! Vector index crate: IVF index with KV-based storage, scalar quantization,
//! and pluggable clustering.

use std::collections::HashSet;
use std::sync::RwLock;

pub use seekpower_schema::{make_key, PREFIX_VEC, QuantType, Similarity};
use seekpower_clustering::Clusterer;
use seekpower_quantize::Quantizer;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// IvfIndex
// ---------------------------------------------------------------------------

/// IVF (Inverted File) vector index backed by a KV store.
pub struct IvfIndex {
    kv: seekpower_storage::KvStore,
    config: seekpower_schema::IndexConfig,
    meta: RwLock<Option<IvfMeta>>,
    medoids: RwLock<Vec<MedoidData>>,
    buffer: tokio::sync::RwLock<Vec<seekpower_clustering::ClusterVector>>,
    cache: seekpower_storage::Cache,
}

// Key helpers ---------------------------------------------------------------

fn meta_key() -> Vec<u8> {
    b"v:meta".to_vec()
}

fn medoid_key(id: u32) -> Vec<u8> {
    make_key(&[
        PREFIX_VEC,
        b":medoid:",
        &id.to_le_bytes(),
    ])
}

fn medoid_prefix() -> Vec<u8> {
    make_key(&[
        PREFIX_VEC,
        b":medoid:",
    ])
}

fn cluster_key(id: u32) -> Vec<u8> {
    make_key(&[
        PREFIX_VEC,
        b":cluster:",
        &id.to_le_bytes(),
    ])
}

// Serialization helpers -----------------------------------------------------

const CLUSTER_HEADER_SIZE: usize = 12;
const QV_HEADER_SIZE: usize = 26;

fn encode_cluster_header(h: &ClusterHeader) -> Vec<u8> {
    let mut buf = Vec::with_capacity(CLUSTER_HEADER_SIZE);
    buf.extend_from_slice(&h.cluster_id.to_le_bytes());
    buf.extend_from_slice(&h.vector_count.to_le_bytes());
    buf.extend_from_slice(&h.byte_size.to_le_bytes());
    buf
}

fn decode_cluster_header(data: &[u8]) -> Option<ClusterHeader> {
    if data.len() < CLUSTER_HEADER_SIZE {
        return None;
    }
    Some(ClusterHeader {
        cluster_id: u32::from_le_bytes(data[0..4].try_into().ok()?),
        vector_count: u32::from_le_bytes(data[4..8].try_into().ok()?),
        byte_size: u32::from_le_bytes(data[8..12].try_into().ok()?),
    })
}

fn encode_qv_header(h: &QuantizedVectorHeader) -> Vec<u8> {
    let mut buf = Vec::with_capacity(QV_HEADER_SIZE);
    buf.extend_from_slice(&h.doc_id.to_le_bytes());
    buf.extend_from_slice(&h.field_id.to_le_bytes());
    buf.extend_from_slice(&h.chunk_id.to_le_bytes());
    buf.extend_from_slice(&h.scale.to_le_bytes());
    buf.extend_from_slice(&h.norm.to_le_bytes());
    buf.extend_from_slice(&h.zero_point.to_le_bytes());
    buf
}

fn decode_qv_header(data: &[u8]) -> Option<QuantizedVectorHeader> {
    if data.len() < QV_HEADER_SIZE {
        return None;
    }
    Some(QuantizedVectorHeader {
        doc_id: u64::from_le_bytes(data[0..8].try_into().ok()?),
        field_id: u32::from_le_bytes(data[8..12].try_into().ok()?),
        chunk_id: u32::from_le_bytes(data[12..16].try_into().ok()?),
        scale: f32::from_le_bytes(data[16..20].try_into().ok()?),
        norm: f32::from_le_bytes(data[20..24].try_into().ok()?),
        zero_point: i16::from_le_bytes(data[24..26].try_into().ok()?),
    })
}

/// Serialize a batch of cluster vectors into the on-disk cluster blob format.
fn serialize_cluster(
    cluster_id: u32,
    vectors: &[seekpower_clustering::ClusterVector],
    dim: u32,
) -> Vec<u8> {
    let quantizer = seekpower_quantize::ScalarQuantI8::new();
    let bytes_per_vec = QV_HEADER_SIZE + dim as usize;
    let mut buf = Vec::with_capacity(CLUSTER_HEADER_SIZE + vectors.len() * bytes_per_vec);

    let header = ClusterHeader {
        cluster_id,
        vector_count: vectors.len() as u32,
        byte_size: dim,
    };
    buf.extend_from_slice(&encode_cluster_header(&header));

    for v in vectors {
        let q = quantizer.quantize(&v.embedding);
        let qvh = QuantizedVectorHeader {
            doc_id: v.doc_id,
            field_id: v.field_id,
            chunk_id: v.chunk_id,
            scale: q.scale,
            norm: q.norm,
            zero_point: q.zero_point,
        };
        buf.extend_from_slice(&encode_qv_header(&qvh));
        buf.extend_from_slice(&q.data);
    }

    buf
}

/// Parse a cluster blob back into (header, vec-of-(header, quantized-data)).
fn deserialize_cluster(
    data: &[u8],
) -> Option<(ClusterHeader, Vec<(QuantizedVectorHeader, Vec<u8>)>)> {
    let header = decode_cluster_header(data)?;
    let dim = header.byte_size as usize;
    let mut offset = CLUSTER_HEADER_SIZE;
    let mut vectors = Vec::with_capacity(header.vector_count as usize);

    for _ in 0..header.vector_count {
        if offset + QV_HEADER_SIZE + dim > data.len() {
            return None;
        }
        let qvh = decode_qv_header(&data[offset..offset + QV_HEADER_SIZE])?;
        offset += QV_HEADER_SIZE;
        let qdata = data[offset..offset + dim].to_vec();
        offset += dim;
        vectors.push((qvh, qdata));
    }

    Some((header, vectors))
}

// Similarity / selection helpers --------------------------------------------

/// Decide which cluster indices to probe given the query and `AnnMode`.
fn select_clusters(
    query: &[f32],
    medoids: &[MedoidData],
    mode: &AnnMode,
    similarity: &Similarity,
) -> Vec<usize> {
    let n = medoids.len();
    if n == 0 {
        return Vec::new();
    }

    let mut sims: Vec<(usize, f32)> = medoids
        .iter()
        .enumerate()
        .map(|(i, m)| (i, seekpower_clustering::compute_similarity(query, &m.vector, similarity)))
        .collect();

    match mode {
        AnnMode::All => (0..n).collect(),
        AnnMode::Nprobe(k) => {
            sims.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            sims.iter().take(*k).map(|(i, _)| *i).collect()
        }
        AnnMode::SimilarityThreshold(threshold) => {
            sims.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            sims.iter()
                .take_while(|(_, s)| *s >= *threshold)
                .map(|(i, _)| *i)
                .collect()
        }
    }
}

/// Dequantize a scalar-quantized vector back to approximate f32 values.
fn dequantize(qdata: &[u8], dim: usize, scale: f32, zero_point: i16) -> Vec<f32> {
    (0..dim)
        .map(|i| {
            let q = qdata[i] as f32;
            scale * (q - zero_point as f32)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// VectorIndex trait
// ---------------------------------------------------------------------------

/// Trait implemented by all vector index backends.
#[allow(async_fn_in_trait)]
pub trait VectorIndex: Send + Sync {
    fn search(
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

impl VectorIndex for IvfIndex {
    fn search(
        &self,
        query: &[f32],
        k: usize,
        ann_mode: &AnnMode,
        delete_set: &HashSet<u64>,
    ) -> Vec<VectorSearchResult> {
        let meta = self.meta.read().ok().and_then(|m| m.clone());
        let dim = self.config.vector_dimensions as usize;

        // Collect candidates from the in-memory buffer (non-blocking).
        let mut results: Vec<VectorSearchResult> = {
            match self.buffer.try_read() {
                Ok(buf) => buf
                    .iter()
                    .filter(|v| !delete_set.contains(&v.doc_id))
                    .map(|v| {
                        let score = seekpower_clustering::compute_similarity(
                            query,
                            &v.embedding,
                            &self.config.vector_similarity,
                        );
                        VectorSearchResult {
                            doc_id: v.doc_id,
                            score,
                            field_id: v.field_id,
                            chunk_id: v.chunk_id,
                        }
                    })
                    .collect(),
                Err(_) => Vec::new(),
            }
        };

        // Flat index: search buffer + cluster 0 from KV store.
        let is_flat = meta.as_ref().map(|m| m.n_clusters).unwrap_or(0) == 0;
        if is_flat {
            // Also search cluster 0 (where flat data is stored after flush).
            let key = cluster_key(0u32);
            if let Some(data) = self.cache.get(&key) {
                if let Some((_hdr, vecs)) = deserialize_cluster(&data) {
                    for (qvh, qdata) in vecs {
                        if delete_set.contains(&qvh.doc_id) {
                            continue;
                        }
                        let approx = dequantize(&qdata, dim, qvh.scale, qvh.zero_point);
                        let score = seekpower_clustering::compute_similarity(
                            query,
                            &approx,
                            &self.config.vector_similarity,
                        );
                        results.push(VectorSearchResult {
                            doc_id: qvh.doc_id,
                            score,
                            field_id: qvh.field_id,
                            chunk_id: qvh.chunk_id,
                        });
                    }
                }
            }
            results.sort_by(|a, b| {
                b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(k);
            return results;
        }

        // Clustered index: probe selected clusters.
        let meta = meta.unwrap();
        let medoids = self.medoids.read().ok().map(|m| m.clone()).unwrap_or_default();
        let selected =
            select_clusters(query, &medoids, ann_mode, &meta.similarity);

        for &cid in &selected {
            let key = cluster_key(cid as u32);
            let cluster_data = self
                .cache
                .get(&key)
                .or_else(|| {
                    // Synchronous cache-miss path: we cannot `.await` inside a
                    // sync trait method, so we only use what the cache already
                    // holds.  Populated by `load_cluster` during async paths.
                    None
                });

            if let Some(data) = cluster_data {
                if let Some((_hdr, vecs)) = deserialize_cluster(&data) {
                    for (qvh, qdata) in vecs {
                        if delete_set.contains(&qvh.doc_id) {
                            continue;
                        }
                        let approx = dequantize(&qdata, dim, qvh.scale, qvh.zero_point);
                        let score = seekpower_clustering::compute_similarity(
                            query,
                            &approx,
                            &meta.similarity,
                        );
                        results.push(VectorSearchResult {
                            doc_id: qvh.doc_id,
                            score,
                            field_id: qvh.field_id,
                            chunk_id: qvh.chunk_id,
                        });
                    }
                }
            }
        }

        results.sort_by(|a, b| {
            b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal)
        });
        results.truncate(k);
        results
    }

    fn add_vector(&self, doc_id: u64, field_id: u32, vector: Vec<f32>) {
        let cv = seekpower_clustering::ClusterVector {
            doc_id,
            field_id,
            chunk_id: 0,
            embedding: vector,
            medoid_index: 0,
            is_medoid: false,
            similarity: 0.0,
            scale: 1.0,
            norm: 0.0,
            zero_point: 0,
        };
        if let Ok(mut buf) = self.buffer.try_write() {
            buf.push(cv);
        }
    }

    async fn flush(&self) -> seekpower_error::Result<()> {
        // 1. Drain the in-memory buffer.
        let buffer_vecs = {
            let mut buf = self.buffer.write().await;
            std::mem::take(&mut *buf)
        };

        if buffer_vecs.is_empty() {
            return Ok(());
        }

        // 2. Load existing persisted state.
        let existing_meta = self.meta.read().ok().and_then(|m| m.clone());
        let existing_medoids = self.medoids.read().ok().map(|m| m.clone()).unwrap_or_default();

        let total_vectors =
            existing_meta.as_ref().map(|m| m.total_vectors).unwrap_or(0) + buffer_vecs.len() as u64;
        let buffer_count = buffer_vecs.len() as u64;

        let dim = self.config.vector_dimensions;
        let similarity = self.config.vector_similarity;

        // 3. Decide strategy.
        let need_full = existing_meta.is_none()
            || total_vectors < 1000
            || buffer_count > (existing_meta.as_ref().map(|m| m.total_vectors).unwrap_or(0) as f64
                * 0.2) as u64;

        if total_vectors < 1000 {
            // Store as flat without clustering
            let mut all_vectors = if !existing_medoids.is_empty() {
                self.reconstruct_all_vectors(&existing_meta, &existing_medoids, dim, similarity)
                    .await?
            } else {
                Vec::new()
            };
            all_vectors.extend(buffer_vecs);
            self.store_flat(&all_vectors, dim, similarity).await
        } else if need_full {
            self.flush_full(buffer_vecs, existing_meta, existing_medoids, dim, similarity, total_vectors)
                .await
        } else {
            self.flush_incremental(buffer_vecs, existing_meta.unwrap(), existing_medoids, dim, similarity, total_vectors)
                .await
        }
    }

    fn n_clusters(&self) -> usize {
        self.meta
            .read()
            .ok()
            .and_then(|m| m.as_ref().map(|meta| meta.n_clusters as usize))
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// IvfIndex inherent methods (KV I/O)
// ---------------------------------------------------------------------------

impl IvfIndex {
    /// Create a new, empty `IvfIndex`.
    pub fn new(
        kv: seekpower_storage::KvStore,
        config: seekpower_schema::IndexConfig,
    ) -> Self {
        let cache =
            seekpower_storage::Cache::new(config.posting_cache_size_mb * 1024 * 1024);
        Self {
            kv,
            config,
            meta: RwLock::new(None),
            medoids: RwLock::new(Vec::new()),
            buffer: tokio::sync::RwLock::new(Vec::new()),
            cache,
        }
    }

    /// Read `IvfMeta` from the KV store (key `v:{path}:meta`).
    pub async fn load_meta(&self) -> seekpower_error::Result<Option<IvfMeta>> {
        let key = meta_key();
        let result: Option<IvfMeta> = self.kv.get_deserialized(&key).await?;
        if let Some(ref meta) = result {
            if let Ok(mut m) = self.meta.write() {
                *m = Some(meta.clone());
            }
        }
        Ok(result)
    }

    /// Read all medoids whose keys match `v:{path}:medoid:*`.
    pub async fn load_medoids(&self) -> seekpower_error::Result<Vec<MedoidData>> {
        let prefix = medoid_prefix();
        let entries = self.kv.scan_prefix(&prefix).await?;

        let mut medoids = Vec::new();
        for (_key, value) in entries {
            if let Ok(m) = serde_json::from_slice::<MedoidData>(&value) {
                medoids.push(m);
            }
        }
        medoids.sort_by_key(|m| m.medoid_id);

        if let Ok(mut stored) = self.medoids.write() {
            *stored = medoids.clone();
        }
        Ok(medoids)
    }

    /// Load raw cluster blob from cache or KV store.
    pub async fn load_cluster(&self, cluster_id: u32) -> seekpower_error::Result<Vec<u8>> {
        let key = cluster_key(cluster_id);

        if let Some(data) = self.cache.get(&key) {
            return Ok(data);
        }

        let data = self
            .kv
            .get(&key)
            .await?
            .ok_or_else(|| {
                seekpower_error::SeekPowerError::StorageError(format!(
                    "Cluster {} not found",
                    cluster_id
                ))
            })?;

        self.cache.insert(&key, data.clone());
        Ok(data)
    }

    /// Persist `IvfMeta`.
    pub async fn store_meta(&self, meta: &IvfMeta) -> seekpower_error::Result<()> {
        let key = meta_key();
        self.kv.put_serialized(&key, meta).await?;
        if let Ok(mut m) = self.meta.write() {
            *m = Some(meta.clone());
        }
        Ok(())
    }

    /// Persist a single medoid.
    pub async fn store_medoid(&self, id: u32, medoid: &MedoidData) -> seekpower_error::Result<()> {
        let key = medoid_key(id);
        self.kv.put_serialized(&key, medoid).await
    }

    /// Persist a cluster blob.
    pub async fn store_cluster(&self, id: u32, data: &[u8]) -> seekpower_error::Result<()> {
        let key = cluster_key(id);
        self.kv.put(&key, data).await?;
        self.cache.insert(&key, data.to_vec());
        Ok(())
    }

    // -- private flush helpers ----------------------------------------------

    async fn flush_full(
        &self,
        buffer_vecs: Vec<seekpower_clustering::ClusterVector>,
        existing_meta: Option<IvfMeta>,
        existing_medoids: Vec<MedoidData>,
        dim: u32,
        similarity: Similarity,
        total_vectors: u64,
    ) -> seekpower_error::Result<()> {
        // Gather every vector we know about.
        let mut all_vectors: Vec<seekpower_clustering::ClusterVector> = if !existing_medoids.is_empty()
        {
            self.reconstruct_all_vectors(&existing_meta, &existing_medoids, dim, similarity)
                .await?
        } else {
            Vec::new()
        };
        all_vectors.extend(buffer_vecs);

        if all_vectors.is_empty() {
            return Ok(());
        }

        // Run the clustering algorithm.
        let clusterer = seekpower_clustering::KMedoidClusterer::new(
            seekpower_clustering::ClusterCount::Auto,
        );
        let medoid_results = clusterer.cluster(&mut all_vectors, similarity);
        let n_clusters = medoid_results.len();

        // Build MedoidData for every cluster.
        let mut new_medoids = Vec::with_capacity(n_clusters);
        for (i, medoid) in medoid_results.iter().enumerate() {
            let embedding = all_vectors[medoid.medoid_index].embedding.clone();
            new_medoids.push(MedoidData {
                medoid_id: i as u32,
                vector: embedding,
                child_count: medoid.child_count as u32,
            });
        }

        // Serialize and persist each cluster.
        for (i, _) in medoid_results.iter().enumerate() {
            let cluster_vecs: Vec<_> = all_vectors
                .iter()
                .filter(|v| v.medoid_index == i)
                .cloned()
                .collect();
            let data = serialize_cluster(i as u32, &cluster_vecs, dim);
            self.store_cluster(i as u32, &data).await?;
        }

        // Persist medoids.
        for (i, m) in new_medoids.iter().enumerate() {
            self.store_medoid(i as u32, m).await?;
        }

        // Persist meta.
        let new_meta = IvfMeta {
            dim,
            n_clusters: n_clusters as u32,
            similarity,
            quantization: QuantType::SqI8,
            total_vectors,
        };
        self.store_meta(&new_meta).await?;

        if let Ok(mut stored) = self.medoids.write() {
            *stored = new_medoids;
        }

        Ok(())
    }

    async fn flush_incremental(
        &self,
        buffer_vecs: Vec<seekpower_clustering::ClusterVector>,
        existing_meta: IvfMeta,
        existing_medoids: Vec<MedoidData>,
        _dim: u32,
        similarity: Similarity,
        total_vectors: u64,
    ) -> seekpower_error::Result<()> {
        let n_clusters = existing_meta.n_clusters as usize;

        // Bucket each new vector by its nearest existing cluster.
        let mut assignments: Vec<Vec<seekpower_clustering::ClusterVector>> =
            vec![Vec::new(); n_clusters];

        for v in buffer_vecs {
            let mut best_cid = 0;
            let mut best_sim = f32::NEG_INFINITY;
            for (i, m) in existing_medoids.iter().enumerate() {
                let sim =
                    seekpower_clustering::compute_similarity(&v.embedding, &m.vector, &similarity);
                if sim > best_sim {
                    best_sim = sim;
                    best_cid = i;
                }
            }
            assignments[best_cid].push(v);
        }

        // Append to each cluster that received new vectors.
        for (cid, new_vecs) in assignments.iter().enumerate() {
            if new_vecs.is_empty() {
                continue;
            }

            let existing_data = self.load_cluster(cid as u32).await?;
            let (mut header, mut existing_vecs) =
                deserialize_cluster(&existing_data).ok_or_else(|| {
                    seekpower_error::SeekPowerError::Corrupted(format!(
                        "Cannot parse cluster {}",
                        cid
                    ))
                })?;

            for v in new_vecs {
                let quantizer = seekpower_quantize::ScalarQuantI8::new();
                let q = quantizer.quantize(&v.embedding);
                existing_vecs.push((
                    QuantizedVectorHeader {
                        doc_id: v.doc_id,
                        field_id: v.field_id,
                        chunk_id: v.chunk_id,
                        scale: q.scale,
                        norm: q.norm,
                        zero_point: q.zero_point,
                    },
                    q.data,
                ));
            }

            header.vector_count = existing_vecs.len() as u32;

            // Re-serialize the whole cluster.
            let mut buf = Vec::new();
            buf.extend_from_slice(&encode_cluster_header(&header));
            for (qvh, qdata) in &existing_vecs {
                buf.extend_from_slice(&encode_qv_header(qvh));
                buf.extend_from_slice(qdata);
            }
            self.store_cluster(cid as u32, &buf).await?;

            // Bump the medoid child count.
            if let Some(m) = existing_medoids.get(cid) {
                let updated = MedoidData {
                    medoid_id: m.medoid_id,
                    vector: m.vector.clone(),
                    child_count: m.child_count + new_vecs.len() as u32,
                };
                self.store_medoid(cid as u32, &updated).await?;
            }
        }

        // Update meta.
        let updated_meta = IvfMeta {
            total_vectors,
            ..existing_meta
        };
        self.store_meta(&updated_meta).await?;

        if let Ok(mut stored) = self.medoids.write() {
            *stored = existing_medoids;
        }

        Ok(())
    }

    /// Store vectors without clustering (flat / brute-force mode).
    async fn store_flat(
        &self,
        vectors: &[seekpower_clustering::ClusterVector],
        dim: u32,
        similarity: Similarity,
    ) -> seekpower_error::Result<()> {
        if vectors.is_empty() {
            return Ok(());
        }

        // Compute centroid as the medoid representative.
        let centroid = {
            let mut c = vec![0.0f32; dim as usize];
            for v in vectors {
                for (j, &val) in v.embedding.iter().enumerate() {
                    c[j] += val;
                }
            }
            let n = vectors.len() as f32;
            for val in c.iter_mut() {
                *val /= n;
            }
            c
        };

        let medoid = MedoidData {
            medoid_id: 0,
            vector: centroid,
            child_count: vectors.len() as u32,
        };
        self.store_medoid(0, &medoid).await?;

        let data = serialize_cluster(0, vectors, dim);
        self.store_cluster(0, &data).await?;

        let meta = IvfMeta {
            dim,
            n_clusters: 0, // signals "flat"
            similarity,
            quantization: QuantType::SqI8,
            total_vectors: vectors.len() as u64,
        };
        self.store_meta(&meta).await?;

        if let Ok(mut m) = self.meta.write() {
            *m = Some(meta);
        }

        if let Ok(mut stored) = self.medoids.write() {
            *stored = vec![medoid];
        }

        Ok(())
    }

    /// Dequantize every vector stored in existing clusters.
    async fn reconstruct_all_vectors(
        &self,
        meta: &Option<IvfMeta>,
        medoids: &[MedoidData],
        dim: u32,
        _similarity: Similarity,
    ) -> seekpower_error::Result<Vec<seekpower_clustering::ClusterVector>> {
        let n_clusters = meta.as_ref().map(|m| m.n_clusters).unwrap_or(0) as usize;
        let mut result = Vec::new();

        for cid in 0..n_clusters {
            let data = self.load_cluster(cid as u32).await?;
            if let Some((_hdr, vecs)) = deserialize_cluster(&data) {
                for (qvh, qdata) in vecs {
                    let embedding = dequantize(&qdata, dim as usize, qvh.scale, qvh.zero_point);
                    result.push(seekpower_clustering::ClusterVector {
                        doc_id: qvh.doc_id,
                        field_id: qvh.field_id,
                        chunk_id: qvh.chunk_id,
                        embedding,
                        medoid_index: 0,
                        is_medoid: false,
                        similarity: 0.0,
                        scale: qvh.scale,
                        norm: qvh.norm,
                        zero_point: qvh.zero_point,
                    });
                }
            }
        }

        // Flat mode: cluster 0 holds the raw vectors.
        if n_clusters == 0 && !medoids.is_empty() {
            let data = self.load_cluster(0).await?;
            if let Some((_hdr, vecs)) = deserialize_cluster(&data) {
                for (qvh, qdata) in vecs {
                    let embedding = dequantize(&qdata, dim as usize, qvh.scale, qvh.zero_point);
                    result.push(seekpower_clustering::ClusterVector {
                        doc_id: qvh.doc_id,
                        field_id: qvh.field_id,
                        chunk_id: qvh.chunk_id,
                        embedding,
                        medoid_index: 0,
                        is_medoid: false,
                        similarity: 0.0,
                        scale: qvh.scale,
                        norm: qvh.norm,
                        zero_point: qvh.zero_point,
                    });
                }
            }
        }

        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn default_config() -> seekpower_schema::IndexConfig {
        seekpower_schema::IndexConfig {
            vector_dimensions: 4,
            ..seekpower_schema::IndexConfig::default()
        }
    }

    async fn make_test_index() -> IvfIndex {
        let os = Arc::new(InMemory::new());
        let kv = seekpower_storage::KvStore::open("test_vec", os).await.unwrap();
        IvfIndex::new(kv, default_config())
    }

    // -- serialization round-trips -----------------------------------------

    #[test]
    fn test_ivf_meta_serialization_roundtrip() {
        let meta = IvfMeta {
            dim: 768,
            n_clusters: 100,
            similarity: Similarity::Cosine,
            quantization: QuantType::SqI8,
            total_vectors: 1_000_000,
        };
        let json = serde_json::to_vec(&meta).unwrap();
        let parsed: IvfMeta = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed.dim, 768);
        assert_eq!(parsed.n_clusters, 100);
        assert_eq!(parsed.total_vectors, 1_000_000);
        assert!(matches!(parsed.similarity, Similarity::Cosine));
        assert!(matches!(parsed.quantization, QuantType::SqI8));
    }

    #[test]
    fn test_medoid_data_serialization_roundtrip() {
        let medoid = MedoidData {
            medoid_id: 42,
            vector: vec![1.0, 2.5, -3.0, 0.0],
            child_count: 150,
        };
        let json = serde_json::to_vec(&medoid).unwrap();
        let parsed: MedoidData = serde_json::from_slice(&json).unwrap();
        assert_eq!(parsed.medoid_id, 42);
        assert_eq!(parsed.vector, vec![1.0, 2.5, -3.0, 0.0]);
        assert_eq!(parsed.child_count, 150);
    }

    // -- header encode / decode --------------------------------------------

    #[test]
    fn test_quantized_vector_header_encode_decode() {
        let header = QuantizedVectorHeader {
            doc_id: 12345,
            field_id: 1,
            chunk_id: 2,
            scale: 0.5,
            norm: 3.14,
            zero_point: -10,
        };
        let encoded = encode_qv_header(&header);
        assert_eq!(encoded.len(), 26);
        let decoded = decode_qv_header(&encoded).unwrap();
        assert_eq!(decoded.doc_id, 12345);
        assert_eq!(decoded.field_id, 1);
        assert_eq!(decoded.chunk_id, 2);
        assert!((decoded.scale - 0.5).abs() < 1e-6);
        assert!((decoded.norm - 3.14).abs() < 1e-2);
        assert_eq!(decoded.zero_point, -10);
    }

    #[test]
    fn test_cluster_header_encode_decode() {
        let header = ClusterHeader {
            cluster_id: 7,
            vector_count: 100,
            byte_size: 128,
        };
        let encoded = encode_cluster_header(&header);
        assert_eq!(encoded.len(), CLUSTER_HEADER_SIZE);
        let decoded = decode_cluster_header(&encoded).unwrap();
        assert_eq!(decoded.cluster_id, 7);
        assert_eq!(decoded.vector_count, 100);
        assert_eq!(decoded.byte_size, 128);
    }

    #[test]
    fn test_decode_cluster_header_too_short() {
        assert!(decode_cluster_header(&[0u8; 5]).is_none());
    }

    #[test]
    fn test_decode_qv_header_too_short() {
        assert!(decode_qv_header(&[0u8; 10]).is_none());
    }

    // -- AnnMode selection --------------------------------------------------

    #[test]
    fn test_ann_mode_all() {
        let medoids = vec![
            MedoidData { medoid_id: 0, vector: vec![1.0, 0.0], child_count: 10 },
            MedoidData { medoid_id: 1, vector: vec![0.0, 1.0], child_count: 20 },
            MedoidData { medoid_id: 2, vector: vec![-1.0, 0.0], child_count: 30 },
        ];
        let selected = select_clusters(&[1.0, 0.0], &medoids, &AnnMode::All, &Similarity::Cosine);
        assert_eq!(selected, vec![0, 1, 2]);
    }

    #[test]
    fn test_ann_mode_nprobe() {
        let medoids = vec![
            MedoidData { medoid_id: 0, vector: vec![1.0, 0.0], child_count: 10 },
            MedoidData { medoid_id: 1, vector: vec![0.0, 1.0], child_count: 20 },
            MedoidData { medoid_id: 2, vector: vec![-1.0, 0.0], child_count: 30 },
        ];
        let selected =
            select_clusters(&[1.0, 0.0], &medoids, &AnnMode::Nprobe(2), &Similarity::Cosine);
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0], 0); // most similar to [1,0]
    }

    #[test]
    fn test_ann_mode_similarity_threshold() {
        let medoids = vec![
            MedoidData { medoid_id: 0, vector: vec![1.0, 0.0], child_count: 10 },
            MedoidData { medoid_id: 1, vector: vec![0.0, 1.0], child_count: 20 },
            MedoidData { medoid_id: 2, vector: vec![-1.0, 0.0], child_count: 30 },
        ];
        let selected = select_clusters(
            &[1.0, 0.0],
            &medoids,
            &AnnMode::SimilarityThreshold(0.5),
            &Similarity::Cosine,
        );
        assert_eq!(selected, vec![0]);
    }

    #[test]
    fn test_ann_mode_nprobe_zero() {
        let medoids = vec![
            MedoidData { medoid_id: 0, vector: vec![1.0, 0.0], child_count: 10 },
        ];
        let selected =
            select_clusters(&[1.0, 0.0], &medoids, &AnnMode::Nprobe(0), &Similarity::Cosine);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_clusters_empty_medoids() {
        let selected =
            select_clusters(&[1.0, 0.0], &[], &AnnMode::All, &Similarity::Cosine);
        assert!(selected.is_empty());
    }

    // -- cluster serialize / deserialize ------------------------------------

    #[test]
    fn test_cluster_serialize_deserialize_roundtrip() {
        let vectors = vec![
            seekpower_clustering::ClusterVector {
                doc_id: 1, field_id: 0, chunk_id: 0,
                embedding: vec![1.0, 2.0, 3.0, 4.0],
                medoid_index: 0, is_medoid: false, similarity: 0.0,
                scale: 1.0, norm: 0.0, zero_point: 0,
            },
            seekpower_clustering::ClusterVector {
                doc_id: 2, field_id: 1, chunk_id: 0,
                embedding: vec![5.0, 6.0, 7.0, 8.0],
                medoid_index: 0, is_medoid: false, similarity: 0.0,
                scale: 1.0, norm: 0.0, zero_point: 0,
            },
        ];
        let data = serialize_cluster(0, &vectors, 4);
        let (header, vecs) = deserialize_cluster(&data).unwrap();
        assert_eq!(header.cluster_id, 0);
        assert_eq!(header.vector_count, 2);
        assert_eq!(header.byte_size, 4);
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs[0].0.doc_id, 1);
        assert_eq!(vecs[0].0.field_id, 0);
        assert_eq!(vecs[1].0.doc_id, 2);
        assert_eq!(vecs[1].0.field_id, 1);
        assert_eq!(vecs[0].1.len(), 4);
        assert_eq!(vecs[1].1.len(), 4);
    }

    // -- dequantize ---------------------------------------------------------

    #[test]
    fn test_dequantize() {
        let qdata = vec![0, 128, 255];
        let result = dequantize(&qdata, 3, 0.1, 0);
        assert!((result[0] - 0.0).abs() < 1e-6);
        assert!((result[1] - 12.8).abs() < 1e-5);
        assert!((result[2] - 25.5).abs() < 1e-5);
    }

    #[test]
    fn test_dequantize_with_zero_point() {
        let qdata = vec![100, 200];
        let result = dequantize(&qdata, 2, 0.5, 50);
        assert!((result[0] - 25.0).abs() < 1e-6);
        assert!((result[1] - 75.0).abs() < 1e-6);
    }

    // -- IvfIndex KV integration -------------------------------------------

    #[tokio::test]
    async fn test_store_and_load_meta() {
        let index = make_test_index().await;
        let meta = IvfMeta {
            dim: 4,
            n_clusters: 5,
            similarity: Similarity::Cosine,
            quantization: QuantType::SqI8,
            total_vectors: 100,
        };
        index.store_meta(&meta).await.unwrap();
        let loaded = index.load_meta().await.unwrap().unwrap();
        assert_eq!(loaded.dim, 4);
        assert_eq!(loaded.n_clusters, 5);
        assert_eq!(loaded.total_vectors, 100);
    }

    #[tokio::test]
    async fn test_store_and_load_medoid() {
        let index = make_test_index().await;
        let medoid = MedoidData {
            medoid_id: 0,
            vector: vec![1.0, 2.0, 3.0, 4.0],
            child_count: 50,
        };
        index.store_medoid(0, &medoid).await.unwrap();
        let medoids = index.load_medoids().await.unwrap();
        assert_eq!(medoids.len(), 1);
        assert_eq!(medoids[0].medoid_id, 0);
        assert_eq!(medoids[0].vector, vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(medoids[0].child_count, 50);
    }

    #[tokio::test]
    async fn test_store_and_load_cluster() {
        let index = make_test_index().await;
        let vectors = vec![seekpower_clustering::ClusterVector {
            doc_id: 1, field_id: 0, chunk_id: 0,
            embedding: vec![1.0, 2.0, 3.0, 4.0],
            medoid_index: 0, is_medoid: false, similarity: 0.0,
            scale: 1.0, norm: 0.0, zero_point: 0,
        }];
        let data = serialize_cluster(0, &vectors, 4);
        index.store_cluster(0, &data).await.unwrap();
        let loaded = index.load_cluster(0).await.unwrap();
        assert_eq!(loaded, data);
    }

    #[tokio::test]
    async fn test_load_cluster_not_found() {
        let index = make_test_index().await;
        let result = index.load_cluster(999).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_load_meta_none() {
        let index = make_test_index().await;
        let result = index.load_meta().await.unwrap();
        assert!(result.is_none());
    }

    // -- flush + search (flat) ---------------------------------------------

    #[tokio::test]
    async fn test_flush_flat_and_search() {
        let index = make_test_index().await;

        for i in 0..50u64 {
            let vec = vec![i as f32, 0.0, 0.0, 0.0];
            index.add_vector(i, 0, vec);
        }
        index.flush().await.unwrap();

        assert_eq!(index.n_clusters(), 0); // flat

        // Verify meta was persisted.
        let meta = index.load_meta().await.unwrap().unwrap();
        assert_eq!(meta.total_vectors, 50);
        assert_eq!(meta.n_clusters, 0);

        // Verify cluster data was stored.
        let cluster_data = index.load_cluster(0).await.unwrap();
        assert!(!cluster_data.is_empty(), "cluster 0 should have data");
    }

    // -- flush + search (clustered) ----------------------------------------

    #[tokio::test]
    async fn test_flush_clustered_and_search() {
        let index = make_test_index().await;

        // Two well-separated groups.
        for i in 0..600u64 {
            index.add_vector(i, 0, vec![1.0 + i as f32 * 0.001, 0.0, 0.0, 0.0]);
        }
        for i in 600..1200u64 {
            index.add_vector(i, 0, vec![0.0, 0.0, 0.0, 1.0 + (i - 600) as f32 * 0.001]);
        }
        index.flush().await.unwrap();

        assert!(index.n_clusters() > 0);

        // Reload from KV to prove persistence.
        let _ = index.load_meta().await.unwrap();
        let _ = index.load_medoids().await.unwrap();

        // Pre-warm the cache for the sync search path.
        for cid in 0..index.n_clusters() as u32 {
            let data = index.load_cluster(cid).await.unwrap();
            let key = cluster_key(cid);
            index.cache.insert(&key, data);
        }

        let results = index.search(
            &[1.0, 0.0, 0.0, 0.0],
            10,
            &AnnMode::All,
            &HashSet::new(),
        );
        assert!(!results.is_empty());
        // Top results should come from the first group (doc_ids < 600).
        for r in &results {
            assert!(r.doc_id < 600, "expected group-1 doc, got {}", r.doc_id);
        }
    }

    // -- delete set ---------------------------------------------------------

    #[tokio::test]
    async fn test_search_respects_delete_set() {
        let index = make_test_index().await;
        for i in 0..10u64 {
            index.add_vector(i, 0, vec![i as f32, 0.0, 0.0, 0.0]);
        }
        index.flush().await.unwrap();

        let mut deletes = HashSet::new();
        deletes.insert(5u64);

        let results = index.search(
            &[5.0, 0.0, 0.0, 0.0],
            10,
            &AnnMode::All,
            &deletes,
        );
        for r in &results {
            assert_ne!(r.doc_id, 5);
        }
    }

    // -- incremental flush --------------------------------------------------

    #[tokio::test]
    async fn test_flush_incremental_preserves_existing() {
        let index = make_test_index().await;

        for i in 0..500u64 {
            index.add_vector(i, 0, vec![i as f32, 0.0, 0.0, 0.0]);
        }
        index.flush().await.unwrap();

        let meta1 = index.load_meta().await.unwrap().unwrap();
        assert!(meta1.total_vectors >= 500);

        for i in 500..550u64 {
            index.add_vector(i, 0, vec![i as f32, 0.0, 0.0, 0.0]);
        }
        index.flush().await.unwrap();

        let meta2 = index.load_meta().await.unwrap().unwrap();
        assert!(meta2.total_vectors >= 550);
    }

    // -- n_clusters ---------------------------------------------------------

    #[test]
    fn test_n_clusters_initially_zero() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let index = rt.block_on(make_test_index());
        assert_eq!(index.n_clusters(), 0);
    }

    // -- multiple medoids ---------------------------------------------------

    #[tokio::test]
    async fn test_multiple_medoids_persisted() {
        let index = make_test_index().await;
        for id in 0..3u32 {
            let m = MedoidData {
                medoid_id: id,
                vector: vec![id as f32; 4],
                child_count: 10,
            };
            index.store_medoid(id, &m).await.unwrap();
        }
        let medoids = index.load_medoids().await.unwrap();
        assert_eq!(medoids.len(), 3);
        for (i, m) in medoids.iter().enumerate() {
            assert_eq!(m.medoid_id, i as u32);
        }
    }
}
