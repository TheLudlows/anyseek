// storm-db module: IVF index, TopK, vector search, and serialization

use crate::clustering::{Medoid, ParentMedoid};
use crate::quantize::compute_similarity;
use crate::ranking::{ResultSource, SearchResult};
use crate::schema::VectorSimilarity;
use crate::search::AnnMode;
use ahash::AHashSet;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Header for a single quantized vector stored in a cluster.
/// Layout (little-endian, 24 bytes): doc_id(2) + field_id(4) + chunk_id(4)
/// + scale(4) + norm(4) + zero_point(2) + sum_q(4)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct VectorHeader {
    pub doc_id: u16,
    pub field_id: u32,
    pub chunk_id: u32,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
    pub sum_q: i32,
}

const VECTOR_HEADER_SIZE: usize = 24;

/// Points to the start and size of a cluster's serialized blob within a level.
#[derive(Debug, Clone)]
pub struct ClusterHeader {
    pub start_index: u32,
    pub child_count: u32,
}

/// One level of the IVF hierarchy: medoid pointers + serialized cluster data.
pub struct VectorLevel {
    pub medoids: Vec<Medoid>,
    pub clusters: Vec<Vec<u8>>,  // serialized cluster data
    pub cluster_headers: Vec<ClusterHeader>,
}

/// Fixed-size top-k accumulator that tracks minimum similarity for pruning.
pub struct TopK {
    results: Vec<(usize, f32)>, // (global doc_id, similarity)
    capacity: usize,
    min_similarity: f32,
}

// ---------------------------------------------------------------------------
// TopK
// ---------------------------------------------------------------------------

impl TopK {
    pub fn new(capacity: usize) -> Self {
        Self {
            results: Vec::with_capacity(capacity),
            capacity,
            min_similarity: f32::NEG_INFINITY,
        }
    }

    /// Insert (index, similarity) keeping only the top-k by similarity.
    /// If the same index already exists, keep the higher score.
    pub fn push(&mut self, index: usize, similarity: f32) {
        if self.capacity == 0 {
            return;
        }

        // Dedup: if we already have this index with a higher or equal score, skip.
        if let Some(pos) = self.results.iter().position(|(i, _)| *i == index) {
            if self.results[pos].1 >= similarity {
                return;
            }
            // Replace with higher score.
            self.results[pos].1 = similarity;
            // Re-establish min_similarity after replacement.
            self.min_similarity = self.results.iter().map(|(_, s)| *s).fold(f32::INFINITY, f32::min);
            return;
        }

        if !self.is_full() {
            self.results.push((index, similarity));
            // Recompute min from all entries (handles first-push NEG_INFINITY case).
            self.min_similarity = self
                .results
                .iter()
                .map(|(_, s)| *s)
                .fold(f32::INFINITY, f32::min);
            return;
        }

        // Full: only accept if strictly better than current minimum.
        if similarity <= self.min_similarity {
            return;
        }

        // Find and replace the entry with the minimum similarity.
        let min_pos = self
            .results
            .iter()
            .position(|(_, s)| *s == self.min_similarity)
            .unwrap();
        self.results[min_pos] = (index, similarity);

        // Recompute the new minimum.
        self.min_similarity = self.results.iter().map(|(_, s)| *s).fold(f32::INFINITY, f32::min);
    }

    /// Consume and return results sorted descending by similarity.
    pub fn into_sorted(mut self) -> Vec<(usize, f32)> {
        self.results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        self.results
    }

    pub fn is_full(&self) -> bool {
        self.results.len() >= self.capacity
    }

    pub fn len(&self) -> usize {
        self.results.len()
    }
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

fn write_le_u16(buf: &mut [u8], val: u16) {
    buf[0..2].copy_from_slice(&val.to_le_bytes());
}

fn write_le_u32(buf: &mut [u8], val: u32) {
    buf[0..4].copy_from_slice(&val.to_le_bytes());
}

fn write_le_f32(buf: &mut [u8], val: f32) {
    buf[0..4].copy_from_slice(&val.to_le_bytes());
}

fn write_le_i16(buf: &mut [u8], val: i16) {
    buf[0..2].copy_from_slice(&val.to_le_bytes());
}

fn write_le_i32(buf: &mut [u8], val: i32) {
    buf[0..4].copy_from_slice(&val.to_le_bytes());
}

fn read_le_u16(buf: &[u8]) -> u16 {
    u16::from_le_bytes([buf[0], buf[1]])
}

fn read_le_u32(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

fn read_le_f32(buf: &[u8]) -> f32 {
    f32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

fn read_le_i16(buf: &[u8]) -> i16 {
    i16::from_le_bytes([buf[0], buf[1]])
}

fn read_le_i32(buf: &[u8]) -> i32 {
    i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

/// Serialize a cluster of parent medoids into a flat byte buffer.
///
/// For each vector: write a 24-byte `VectorHeader` followed by the raw f32
/// embedding bytes (`dimensions * 4`).
pub fn serialize_cluster(
    vectors: &[ParentMedoid],
    dimensions: usize,
) -> Vec<u8> {
    let vector_size = VECTOR_HEADER_SIZE + dimensions * 4;
    let mut buf = Vec::with_capacity(vectors.len() * vector_size);

    for pm in vectors {
        // Write header (24 bytes)
        let mut header_buf = [0u8; VECTOR_HEADER_SIZE];
        write_le_u16(&mut header_buf[0..2], pm.doc_id);
        write_le_u32(&mut header_buf[2..6], pm.field_id);
        write_le_u32(&mut header_buf[6..10], pm.chunk_id);
        write_le_f32(&mut header_buf[10..14], pm.scale);
        write_le_f32(&mut header_buf[14..18], pm.norm);
        write_le_i16(&mut header_buf[18..20], pm.zero_point);
        write_le_i32(&mut header_buf[20..24], pm.sum_q);
        buf.extend_from_slice(&header_buf);

        // Write embedding as raw little-endian f32 bytes
        for &v in &pm.embedding {
            buf.extend_from_slice(&v.to_le_bytes());
        }
    }

    buf
}

/// Deserialize a cluster blob back into `(VectorHeader, embedding)` pairs.
pub fn deserialize_cluster(
    data: &[u8],
    dimensions: usize,
) -> Vec<(VectorHeader, Vec<f32>)> {
    let vector_size = VECTOR_HEADER_SIZE + dimensions * 4;
    if vector_size == 0 || data.is_empty() {
        return vec![];
    }
    let count = data.len() / vector_size;
    let mut result = Vec::with_capacity(count);

    for i in 0..count {
        let offset = i * vector_size;
        let hdr_data = &data[offset..offset + VECTOR_HEADER_SIZE];
        let vec_data = &data[offset + VECTOR_HEADER_SIZE..offset + vector_size];

        let header = VectorHeader {
            doc_id: read_le_u16(&hdr_data[0..2]),
            field_id: read_le_u32(&hdr_data[2..6]),
            chunk_id: read_le_u32(&hdr_data[6..10]),
            scale: read_le_f32(&hdr_data[10..14]),
            norm: read_le_f32(&hdr_data[14..18]),
            zero_point: read_le_i16(&hdr_data[18..20]),
            sum_q: read_le_i32(&hdr_data[20..24]),
        };

        let embedding: Vec<f32> = (0..dimensions)
            .map(|j| read_le_f32(&vec_data[j * 4..(j + 1) * 4]))
            .collect();

        result.push((header, embedding));
    }

    result
}

// ---------------------------------------------------------------------------
// Vector search
// ---------------------------------------------------------------------------

/// Search a single IVF level for the top-k most similar vectors to `query`.
///
/// 1. Medoid search: deserialize the first vector of each cluster (the medoid)
///    and compute similarity with the query.
/// 2. Select clusters per `AnnMode`.
/// 3. Cluster search: for each selected cluster, deserialize all vectors,
///    compute similarity, push to TopK. Skip doc_ids in `delete_set`.
/// 4. Return sorted `Vec<SearchResult>` with `source = ResultSource::Vector`.
pub fn vector_search_level(
    level: &VectorLevel,
    query: &[f32],
    ann_mode: &AnnMode,
    similarity: VectorSimilarity,
    dimensions: usize,
    top_k: usize,
    delete_set: &AHashSet<usize>,
    level_id: usize,
) -> Vec<SearchResult> {
    let num_clusters = level.medoids.len();
    if num_clusters == 0 {
        return vec![];
    }

    // --- Step 1: Medoid search ---
    // Deserialize the first vector of each cluster as the medoid representative.
    let mut medoid_similarities: Vec<(usize, f32)> = Vec::with_capacity(num_clusters);
    for (cluster_idx, _medoid) in level.medoids.iter().enumerate() {
        let cluster_data = &level.clusters[cluster_idx];
        let vectors = deserialize_cluster(cluster_data, dimensions);
        if let Some((header, embedding)) = vectors.first() {
            let sim = compute_similarity(query, &embedding, similarity);
            medoid_similarities.push((cluster_idx, sim));
        } else {
            medoid_similarities.push((cluster_idx, f32::NEG_INFINITY));
        }
    }

    // --- Step 2: Select clusters per AnnMode ---
    let selected_clusters: Vec<usize> = match ann_mode {
        AnnMode::All => {
            medoid_similarities.iter().map(|(i, _)| *i).collect()
        }
        AnnMode::Nprobe(n) => {
            medoid_similarities.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            medoid_similarities.iter().take(*n).map(|(i, _)| *i).collect()
        }
        AnnMode::SimilarityThreshold(t) => {
            medoid_similarities.iter()
                .filter(|(_, sim)| sim >= t)
                .map(|(i, _)| *i)
                .collect()
        }
    };

    // --- Step 3: Cluster search ---
    let mut topk = TopK::new(top_k);
    for &cluster_idx in &selected_clusters {
        let cluster_data = &level.clusters[cluster_idx];
        let vectors = deserialize_cluster(cluster_data, dimensions);

        for (local_idx, (header, embedding)) in vectors.iter().enumerate() {
            let global_doc_id: usize = (level_id << 16) | (header.doc_id as usize);
            if delete_set.contains(&global_doc_id) {
                continue;
            }
            let sim = compute_similarity(query, &embedding, similarity);
            topk.push(global_doc_id, sim);
        }
    }

    // --- Step 4: Return sorted results ---
    topk.into_sorted()
        .into_iter()
        .map(|(doc_id, score)| SearchResult {
            doc_id: doc_id as u64,
            score,
            source: ResultSource::Vector,
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a mock VectorLevel from (doc_id, embedding) pairs split
    // into `num_clusters` clusters.
    fn make_mock_level(
        pairs: &[(u16, Vec<f32>)],
        num_clusters: usize,
    ) -> (VectorLevel, usize) {
        let dimensions = if pairs.is_empty() {
            0
        } else {
            pairs[0].1.len()
        };

        let mut medoids: Vec<Medoid> = Vec::with_capacity(num_clusters);
        let mut clusters: Vec<Vec<u8>> = Vec::with_capacity(num_clusters);
        let mut cluster_headers: Vec<ClusterHeader> = Vec::with_capacity(num_clusters);

        let chunk_size = if num_clusters > 0 {
            (pairs.len() + num_clusters - 1) / num_clusters
        } else {
            0
        };

        for c in 0..num_clusters {
            let start = c * chunk_size;
            let end = std::cmp::min(start + chunk_size, pairs.len());
            let chunk = &pairs[start..end];

            // Build ParentMedoid list for this cluster
            let parent_medoids: Vec<ParentMedoid> = chunk
                .iter()
                .enumerate()
                .map(|(i, &(doc_id, ref emb))| ParentMedoid {
                    medoid_index: c,
                    is_medoid: i == 0,
                    similarity: 1.0,
                    doc_id,
                    field_id: 0,
                    chunk_id: i as u32,
                    embedding: emb.clone(),
                    scale: 1.0,
                    norm: emb.iter().map(|v| v * v).sum::<f32>().sqrt(),
                    zero_point: 0,
                    sum_q: 0,
                })
                .collect();

            let serialized = serialize_cluster(&parent_medoids, dimensions);
            medoids.push(Medoid { medoid_index: c, child_count: chunk.len() });
            cluster_headers.push(ClusterHeader {
                start_index: c as u32,
                child_count: chunk.len() as u32,
            });
            clusters.push(serialized);
        }

        let level = VectorLevel {
            medoids,
            clusters,
            cluster_headers,
        };

        (level, dimensions)
    }

    // -----------------------------------------------------------------------
    // TopK tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_topk_push_below_capacity() {
        let mut topk = TopK::new(5);
        topk.push(0, 0.5);
        topk.push(1, 0.8);
        topk.push(2, 0.3);
        assert_eq!(topk.len(), 3);
        assert!(!topk.is_full());
    }

    #[test]
    fn test_topk_push_above_capacity_evicts() {
        let mut topk = TopK::new(3);
        topk.push(0, 0.5);
        topk.push(1, 0.8);
        topk.push(2, 0.3);
        assert!(topk.is_full());
        // Push a higher score than the minimum (0.3) → should evict index 2
        topk.push(3, 0.6);
        assert_eq!(topk.len(), 3);
        let sorted = topk.into_sorted();
        // Should contain indices 0(0.5), 1(0.8), 3(0.6), not index 2(0.3)
        assert_eq!(sorted[0].1, 0.8);
        assert_eq!(sorted[1].1, 0.6);
        assert_eq!(sorted[2].1, 0.5);
    }

    #[test]
    fn test_topk_dedup_keep_higher_score() {
        let mut topk = TopK::new(5);
        topk.push(10, 0.4);
        topk.push(10, 0.7);  // same index, higher score
        topk.push(10, 0.5);  // same index, lower score → ignored
        assert_eq!(topk.len(), 1);
        let sorted = topk.into_sorted();
        assert_eq!(sorted[0].0, 10);
        assert!((sorted[0].1 - 0.7).abs() < 1e-6);
    }

    #[test]
    fn test_topk_into_sorted_ordering() {
        let mut topk = TopK::new(10);
        for (i, sim) in [0.2, 0.9, 0.5, 0.1, 0.8, 0.3].iter().enumerate() {
            topk.push(i, *sim);
        }
        let sorted = topk.into_sorted();
        assert_eq!(sorted.len(), 6);
        // Verify strictly descending
        for i in 1..sorted.len() {
            assert!(
                sorted[i - 1].1 >= sorted[i].1,
                "not descending at position {}: {} < {}",
                i,
                sorted[i - 1].1,
                sorted[i].1
            );
        }
        assert!((sorted[0].1 - 0.9).abs() < 1e-6);
        assert!((sorted[5].1 - 0.1).abs() < 1e-6);
    }

    #[test]
    fn test_topk_capacity_zero() {
        let mut topk = TopK::new(0);
        topk.push(0, 0.5);
        assert_eq!(topk.len(), 0);
    }

    // -----------------------------------------------------------------------
    // Serialization roundtrip tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let dimensions = 4;
        let parent_medoids = vec![
            ParentMedoid {
                medoid_index: 0,
                is_medoid: true,
                similarity: 1.0,
                doc_id: 10,
                field_id: 1,
                chunk_id: 0,
                embedding: vec![1.0, 2.0, 3.0, 4.0],
                scale: 0.5,
                norm: 5.477,
                zero_point: 5,
                sum_q: 42,
            },
            ParentMedoid {
                medoid_index: 0,
                is_medoid: false,
                similarity: 0.8,
                doc_id: 20,
                field_id: 2,
                chunk_id: 1,
                embedding: vec![0.1, -0.2, 0.3, -0.4],
                scale: 1.0,
                norm: 0.5477,
                zero_point: -3,
                sum_q: -10,
            },
        ];

        let data = serialize_cluster(&parent_medoids, dimensions);
        let parsed = deserialize_cluster(&data, dimensions);

        assert_eq!(parsed.len(), 2);

        // Verify first vector
        let (h0, e0) = &parsed[0];
        assert_eq!(h0.doc_id, 10);
        assert_eq!(h0.field_id, 1);
        assert_eq!(h0.chunk_id, 0);
        assert!((h0.scale - 0.5).abs() < 1e-6);
        assert!((h0.norm - 5.477).abs() < 1e-3);
        assert_eq!(h0.zero_point, 5);
        assert_eq!(h0.sum_q, 42);
        for (i, &v) in e0.iter().enumerate() {
            assert!((v - parent_medoids[0].embedding[i]).abs() < 1e-6);
        }

        // Verify second vector
        let (h1, e1) = &parsed[1];
        assert_eq!(h1.doc_id, 20);
        assert_eq!(h1.field_id, 2);
        assert_eq!(h1.chunk_id, 1);
        assert!((h1.scale - 1.0).abs() < 1e-6);
        assert!((h1.norm - 0.5477).abs() < 1e-3);
        assert_eq!(h1.zero_point, -3);
        assert_eq!(h1.sum_q, -10);
        for (i, &v) in e1.iter().enumerate() {
            assert!((v - parent_medoids[1].embedding[i]).abs() < 1e-6);
        }
    }

    #[test]
    fn test_serialize_deserialize_empty() {
        let dimensions = 3;
        let parent_medoids: Vec<ParentMedoid> = vec![];
        let data = serialize_cluster(&parent_medoids, dimensions);
        let parsed = deserialize_cluster(&data, dimensions);
        assert!(parsed.is_empty());
    }

    // -----------------------------------------------------------------------
    // vector_search_level tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_vector_search_level_small() {
        // 2 clusters, 3 vectors each, 4 dimensions
        let dimensions = 4;
        let pairs = vec![
            // Cluster 0
            (1u16, vec![1.0, 0.0, 0.0, 0.0]),
            (2u16, vec![0.9, 0.1, 0.0, 0.0]),
            (3u16, vec![0.8, 0.2, 0.0, 0.0]),
            // Cluster 1
            (4u16, vec![0.0, 1.0, 0.0, 0.0]),
            (5u16, vec![0.1, 0.9, 0.0, 0.0]),
            (6u16, vec![0.2, 0.8, 0.0, 0.0]),
        ];

        let (level, dim) = make_mock_level(&pairs, 2);

        // Query close to cluster 0
        let query = vec![1.0, 0.0, 0.0, 0.0];
        let delete_set = AHashSet::new();

        // --- AnnMode::All ---
        let results = vector_search_level(
            &level, &query, &AnnMode::All,
            VectorSimilarity::Cosine, dim, 10, &delete_set, 0,
        );
        assert!(!results.is_empty());
        assert!(results.len() <= 6);
        // Verify descending order
        for i in 1..results.len() {
            assert!(results[i - 1].score >= results[i].score);
        }
        // Doc 1 should be the top result (exact match)
        assert_eq!(results[0].doc_id, 1);
        assert!((results[0].score - 1.0).abs() < 1e-6);
        // All sources should be Vector
        for r in &results {
            assert_eq!(r.source, ResultSource::Vector);
        }

        // --- AnnMode::Nprobe(1) ---
        let results_nprobe = vector_search_level(
            &level, &query, &AnnMode::Nprobe(1),
            VectorSimilarity::Cosine, dim, 10, &delete_set, 0,
        );
        // Should only search cluster 0 (3 vectors), cluster 1 should be excluded
        assert_eq!(results_nprobe.len(), 3);
        // All results should have doc_ids from cluster 0 (1, 2, 3)
        for r in &results_nprobe {
            assert!((1..=3).contains(&(r.doc_id as u16)));
        }

        // --- AnnMode::SimilarityThreshold ---
        let results_threshold = vector_search_level(
            &level, &query,
            &AnnMode::SimilarityThreshold(0.5),
            VectorSimilarity::Cosine, dim, 10, &delete_set, 0,
        );
        // At least the first cluster's medoid (very high cosine similarity) should pass.
        // The second cluster's medoid (cosine near 0) should not.
        assert!(!results_threshold.is_empty());
    }

    #[test]
    fn test_vector_search_level_delete_set() {
        let dimensions = 4;
        let pairs = vec![
            (1u16, vec![1.0, 0.0, 0.0, 0.0]),
            (2u16, vec![0.9, 0.1, 0.0, 0.0]),
            (3u16, vec![0.8, 0.2, 0.0, 0.0]),
        ];

        let (level, dim) = make_mock_level(&pairs, 1);

        let mut delete_set = AHashSet::new();
        delete_set.insert(1); // delete doc_id 1 (global: 0<<16 | 1 = 1)

        let results = vector_search_level(
            &level,
            &[1.0, 0.0, 0.0, 0.0],
            &AnnMode::All,
            VectorSimilarity::Cosine,
            dim,
            10,
            &delete_set,
            0,
        );

        assert_eq!(results.len(), 2);
        // Doc 1 should not be in results
        for r in &results {
            assert_ne!(r.doc_id, 1);
        }
    }

    #[test]
    fn test_vector_search_level_level_id_encoding() {
        let dimensions = 3;
        let pairs = vec![
            (5u16, vec![1.0, 0.0, 0.0]),
        ];
        let (level, dim) = make_mock_level(&pairs, 1);

        let delete_set = AHashSet::new();
        let results = vector_search_level(
            &level,
            &[1.0, 0.0, 0.0],
            &AnnMode::All,
            VectorSimilarity::Cosine,
            dim,
            10,
            &delete_set,
            3, // level_id = 3
        );

        assert_eq!(results.len(), 1);
        // global_doc_id = (3 << 16) | 5 = 196608 + 5 = 196613
        assert_eq!(results[0].doc_id, (3u64 << 16) | 5);
    }

    #[test]
    fn test_vector_search_level_empty() {
        let dimensions = 3;
        let pairs: Vec<(u16, Vec<f32>)> = vec![];
        let (level, dim) = make_mock_level(&pairs, 0);

        let delete_set = AHashSet::new();
        let results = vector_search_level(
            &level,
            &[1.0, 0.0, 0.0],
            &AnnMode::All,
            VectorSimilarity::Cosine,
            dim,
            10,
            &delete_set,
            0,
        );
        assert!(results.is_empty());
    }

    #[test]
    fn test_vector_search_level_dot_similarity() {
        let dimensions = 3;
        let pairs = vec![
            (1u16, vec![1.0, 0.0, 0.0]),
            (2u16, vec![0.0, 1.0, 0.0]),
            (3u16, vec![0.0, 0.0, 1.0]),
        ];
        let (level, dim) = make_mock_level(&pairs, 1);

        let query = vec![1.0, 2.0, 3.0];
        let delete_set = AHashSet::new();

        let results = vector_search_level(
            &level, &query,
            &AnnMode::All,
            VectorSimilarity::Dot,
            dim, 10, &delete_set, 0,
        );

        assert_eq!(results.len(), 3);
        // Highest dot product: doc 3 (3.0), then doc 2 (2.0), then doc 1 (1.0)
        assert_eq!(results[0].doc_id, 3);
        assert_eq!(results[1].doc_id, 2);
        assert_eq!(results[2].doc_id, 1);
    }
}
