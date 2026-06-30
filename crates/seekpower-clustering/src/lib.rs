use seekpower_schema::Similarity;

/// Trait for clustering algorithms.
pub trait Clusterer: Send + Sync {
    fn cluster(&self, vectors: &mut [ClusterVector], similarity: Similarity) -> Vec<Medoid>;
}

/// A vector used in clustering, carrying document metadata and embedding data.
#[derive(Debug, Clone)]
pub struct ClusterVector {
    pub doc_id: u64,
    pub field_id: u32,
    pub chunk_id: u32,
    pub embedding: Vec<f32>,
    pub medoid_index: usize,
    pub is_medoid: bool,
    pub similarity: f32,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
}

/// A medoid (cluster center) produced by clustering.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Medoid {
    pub medoid_index: usize,
    pub child_count: usize,
}

/// Controls how the number of clusters is determined.
pub enum ClusterCount {
    Auto,
    Fixed(usize),
    None,
}

pub struct KMedoidClusterer {
    pub n_clusters: ClusterCount,
}

/// Compute similarity between two vectors.
pub fn compute_similarity(a: &[f32], b: &[f32], sim: &Similarity) -> f32 {
    match sim {
        Similarity::Dot => a.iter().zip(b.iter()).map(|(x, y)| x * y).sum(),
        Similarity::Cosine => {
            let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                0.0
            } else {
                dot / (norm_a * norm_b)
            }
        }
        Similarity::Euclidean => {
            let dist: f32 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum::<f32>().sqrt();
            1.0 / (1.0 + dist)
        }
    }
}

/// Find the nearest medoid to a given embedding.
/// Returns (positional_medoid_index, similarity).
fn find_nearest_medoid(
    embedding: &[f32],
    medoid_indices: &[usize],
    vectors: &[ClusterVector],
    similarity: &Similarity,
) -> (usize, f32) {
    let mut best_sim = f32::NEG_INFINITY;
    let mut best_pos = 0;
    for (pos, &mi) in medoid_indices.iter().enumerate() {
        let sim = compute_similarity(embedding, &vectors[mi].embedding, similarity);
        if sim > best_sim {
            best_sim = sim;
            best_pos = pos;
        }
    }
    (best_pos, best_sim)
}

impl KMedoidClusterer {
    pub fn new(n_clusters: ClusterCount) -> Self {
        Self { n_clusters }
    }
}

impl Clusterer for KMedoidClusterer {
    fn cluster(&self, vectors: &mut [ClusterVector], similarity: Similarity) -> Vec<Medoid> {
        let sim = &similarity;
        let n = vectors.len();
        if n == 0 {
            return Vec::new();
        }

        let k = match self.n_clusters {
            ClusterCount::Auto => ((n as f64).sqrt() * 2.0).ceil() as usize,
            ClusterCount::Fixed(k) => k,
            ClusterCount::None => 1,
        };

        if n <= k {
            for (i, v) in vectors.iter_mut().enumerate() {
                v.medoid_index = i;
                v.is_medoid = true;
                v.similarity = compute_similarity(&v.embedding, &v.embedding, sim);
            }
            return (0..n).map(|i| Medoid { medoid_index: i, child_count: 1 }).collect();
        }

        let dim = vectors[0].embedding.len();
        let mut medoid_indices: Vec<usize> = Vec::with_capacity(k);

        // First medoid: closest to centroid
        let mut centroid = vec![0.0f32; dim];
        for v in vectors.iter() {
            for (j, &val) in v.embedding.iter().enumerate() {
                centroid[j] += val;
            }
        }
        for c in centroid.iter_mut() {
            *c /= n as f32;
        }

        let mut best_sim = f32::NEG_INFINITY;
        let mut best_idx = 0;
        for (i, v) in vectors.iter().enumerate() {
            let s = compute_similarity(&v.embedding, &centroid, sim);
            if s > best_sim {
                best_sim = s;
                best_idx = i;
            }
        }
        medoid_indices.push(best_idx);

        // Subsequent medoids: greedy selection
        let step = std::cmp::max(n / 100, 1);
        for _ in 1..k {
            let mut best_gain = f32::NEG_INFINITY;
            let mut best_candidate = 0;
            let mut i = 0;
            while i < n {
                if !medoid_indices.contains(&i) {
                    let mut total_gain = 0.0f32;
                    for v in vectors.iter() {
                        let sim_candidate = compute_similarity(&v.embedding, &vectors[i].embedding, sim);
                        let current_best = medoid_indices
                            .iter()
                            .map(|&mi| compute_similarity(&v.embedding, &vectors[mi].embedding, sim))
                            .fold(f32::NEG_INFINITY, f32::max);
                        let gain = sim_candidate - current_best;
                        if gain > 0.0 {
                            total_gain += gain;
                        }
                    }
                    if total_gain > best_gain {
                        best_gain = total_gain;
                        best_candidate = i;
                    }
                }
                i += step;
            }
            medoid_indices.push(best_candidate);
        }

        // Assign each vector to nearest medoid
        let assignments: Vec<(usize, f32)> = vectors
            .iter()
            .map(|v| find_nearest_medoid(&v.embedding, &medoid_indices, vectors, sim))
            .collect();
        for (v, (mi, s)) in vectors.iter_mut().zip(assignments) {
            v.medoid_index = mi;
            v.similarity = s;
        }

        // Iterative optimization (max 20 rounds)
        for _round in 0..20 {
            let mut new_medoids = Vec::with_capacity(k);
            for cluster_id in 0..k {
                let mut cluster_centroid = vec![0.0f32; dim];
                let mut count = 0usize;
                for v in vectors.iter() {
                    if v.medoid_index == cluster_id {
                        for (j, &val) in v.embedding.iter().enumerate() {
                            cluster_centroid[j] += val;
                        }
                        count += 1;
                    }
                }
                if count > 0 {
                    for c in cluster_centroid.iter_mut() {
                        *c /= count as f32;
                    }
                }
                let mut best_vec_idx = medoid_indices[cluster_id];
                let mut best_vec_sim = f32::NEG_INFINITY;
                for (idx, v) in vectors.iter().enumerate() {
                    if v.medoid_index == cluster_id {
                        let s = compute_similarity(&v.embedding, &cluster_centroid, sim);
                        if s > best_vec_sim {
                            best_vec_sim = s;
                            best_vec_idx = idx;
                        }
                    }
                }
                new_medoids.push(best_vec_idx);
            }
            medoid_indices = new_medoids;

            let assignments: Vec<(usize, f32)> = vectors
                .iter()
                .map(|v| find_nearest_medoid(&v.embedding, &medoid_indices, vectors, sim))
                .collect();

            let mut reassigned = 0usize;
            for (v, (new_mi, new_sim)) in vectors.iter_mut().zip(assignments) {
                if v.medoid_index != new_mi {
                    reassigned += 1;
                }
                v.medoid_index = new_mi;
                v.similarity = new_sim;
            }

            if reassigned < n / 100 {
                break;
            }
        }

        // Mark medoid vectors and build result
        for v in vectors.iter_mut() {
            v.is_medoid = false;
        }
        let mut result = Vec::with_capacity(k);
        for (pos, &mi) in medoid_indices.iter().enumerate() {
            vectors[mi].is_medoid = true;
            vectors[mi].medoid_index = pos;
        }
        // Count children per cluster (after medoid assignment)
        let child_counts: Vec<usize> = (0..k)
            .map(|pos| vectors.iter().filter(|v| v.medoid_index == pos).count())
            .collect();
        for (pos, _) in medoid_indices.iter().enumerate() {
            result.push(Medoid { medoid_index: pos, child_count: child_counts[pos] });
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vector(doc_id: u64, embedding: Vec<f32>) -> ClusterVector {
        ClusterVector {
            doc_id, field_id: 0, chunk_id: 0, embedding,
            medoid_index: 0, is_medoid: false, similarity: 0.0,
            scale: 1.0, norm: 0.0, zero_point: 0,
        }
    }

    #[test]
    fn test_dot_similarity() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![4.0, 5.0, 6.0];
        let sim = compute_similarity(&a, &b, &Similarity::Dot);
        assert!((sim - 32.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let sim = compute_similarity(&a, &a, &Similarity::Cosine);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = compute_similarity(&a, &b, &Similarity::Cosine);
        assert!(sim.abs() < 1e-6);
    }

    #[test]
    fn test_euclidean_similarity_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let sim = compute_similarity(&a, &a, &Similarity::Euclidean);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_fixed_2_clusters() {
        let mut vectors: Vec<ClusterVector> = Vec::new();
        for i in 0..5 { vectors.push(make_vector(i as u64, vec![0.0 + i as f32 * 0.1, 0.0, 0.0])); }
        for i in 0..5 { vectors.push(make_vector((i + 5) as u64, vec![10.0 + i as f32 * 0.1, 10.0, 10.0])); }
        let clusterer = KMedoidClusterer::new(ClusterCount::Fixed(2));
        let medoids = clusterer.cluster(&mut vectors, Similarity::Euclidean);
        assert_eq!(medoids.len(), 2);
        let total: usize = medoids.iter().map(|m| m.child_count).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_none_produces_1_cluster() {
        let mut vectors: Vec<ClusterVector> = Vec::new();
        for i in 0..5 { vectors.push(make_vector(i as u64, vec![i as f32, 0.0, 0.0])); }
        let clusterer = KMedoidClusterer::new(ClusterCount::None);
        let medoids = clusterer.cluster(&mut vectors, Similarity::Euclidean);
        assert_eq!(medoids.len(), 1);
        assert_eq!(medoids[0].child_count, 5);
    }

    #[test]
    fn test_empty_vectors() {
        let mut vectors: Vec<ClusterVector> = Vec::new();
        let clusterer = KMedoidClusterer::new(ClusterCount::Fixed(3));
        let medoids = clusterer.cluster(&mut vectors, Similarity::Cosine);
        assert!(medoids.is_empty());
    }

    #[test]
    fn test_identical_vectors() {
        let mut vectors: Vec<ClusterVector> = Vec::new();
        for i in 0..5 { vectors.push(make_vector(i as u64, vec![1.0, 1.0, 1.0])); }
        let clusterer = KMedoidClusterer::new(ClusterCount::Fixed(2));
        let medoids = clusterer.cluster(&mut vectors, Similarity::Cosine);
        assert_eq!(medoids.len(), 2);
        let total: usize = medoids.iter().map(|m| m.child_count).sum();
        assert_eq!(total, 5, "all vectors should be assigned to a cluster");
    }

    #[test]
    fn test_auto_cluster_count() {
        let mut vectors: Vec<ClusterVector> = Vec::new();
        for i in 0..100 { vectors.push(make_vector(i as u64, vec![i as f32 * 0.1, (100 - i) as f32 * 0.1])); }
        let clusterer = KMedoidClusterer::new(ClusterCount::Auto);
        let medoids = clusterer.cluster(&mut vectors, Similarity::Euclidean);
        assert_eq!(medoids.len(), 20);
        let total: usize = medoids.iter().map(|m| m.child_count).sum();
        assert_eq!(total, 100);
    }
}
