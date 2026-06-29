// storm-db module

use crate::schema::{Clustering, VectorSimilarity};
use serde::{Deserialize, Serialize};
use std::f32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Medoid {
    pub medoid_index: usize,
    pub child_count: usize,
}

// Legacy alias for backward compatibility with serialization code.
impl Medoid {
    pub fn cluster_index(&self) -> usize {
        self.medoid_index
    }
}

#[derive(Debug, Clone)]
pub struct ParentMedoid {
    pub medoid_index: usize,
    pub is_medoid: bool,
    pub similarity: f32,
    pub doc_id: u16,
    pub field_id: u32,
    pub chunk_id: u32,
    pub embedding: Vec<f32>,
    // Legacy fields kept for backward compatibility with serialization.
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
    pub sum_q: i32,
}

fn compute_similarity_dispatch(a: &[f32], b: &[f32], similarity: VectorSimilarity) -> f32 {
    match similarity {
        VectorSimilarity::Dot => {
            a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
        }
        VectorSimilarity::Cosine => {
            let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm_a == 0.0 || norm_b == 0.0 {
                0.0
            } else {
                dot / (norm_a * norm_b)
            }
        }
        VectorSimilarity::Euclidean => {
            let dist_sq: f32 = a
                .iter()
                .zip(b.iter())
                .map(|(x, y)| {
                    let d = x - y;
                    d * d
                })
                .sum();
            let dist = dist_sq.sqrt();
            1.0 / (1.0 + dist)
        }
    }
}

/// Assign each vector to the medoid it is most similar to.
/// Returns the total similarity sum across all vectors.
fn assign_to_medoids(
    vectors: &mut [ParentMedoid],
    medoid_indices: &[usize],
    similarity: VectorSimilarity,
) -> f32 {
    // Clone medoid embeddings first to avoid borrow issues.
    let medoid_embeddings: Vec<Vec<f32>> = medoid_indices
        .iter()
        .map(|&mid| vectors[mid].embedding.clone())
        .collect();
    let mut total_similarity = 0.0;
    for (i, vec) in vectors.iter_mut().enumerate() {
        let mut best_sim = f32::NEG_INFINITY;
        let mut best_medoid_idx = 0;
        for (j, medoid_emb) in medoid_embeddings.iter().enumerate() {
            let sim = compute_similarity_dispatch(medoid_emb, &vec.embedding, similarity);
            if sim > best_sim {
                best_sim = sim;
                best_medoid_idx = medoid_indices[j];
            }
        }
        vec.similarity = best_sim;
        vec.medoid_index = best_medoid_idx;
        vec.is_medoid = false;
        total_similarity += best_sim;
    }
    // Re-mark medoids
    for &mid in medoid_indices {
        vectors[mid].is_medoid = true;
        vectors[mid].similarity = 1.0;
    }
    total_similarity
}

pub fn cluster_vectors(
    vectors: &mut Vec<ParentMedoid>,
    clustering: Clustering,
    similarity: VectorSimilarity,
) -> Vec<Medoid> {
    let n = vectors.len();
    if n == 0 {
        return vec![];
    }

    // 1. Compute cluster_count
    let cluster_count = match clustering {
        Clustering::Auto => {
            let count = (n as f64).sqrt() as usize * 2;
            count.max(1)
        }
        Clustering::Fixed(count) => count.max(1),
        Clustering::None => 1,
    };
    let cluster_count = cluster_count.min(n);

    // 2. First medoid selection: find vector closest to centroid
    let dim = vectors[0].embedding.len();
    let mut centroid = vec![0.0_f32; dim];
    for vec in &*vectors {
        for (j, &val) in vec.embedding.iter().enumerate() {
            centroid[j] += val;
        }
    }
    for val in &mut centroid {
        *val /= n as f32;
    }

    // Find the vector most similar to the centroid
    let mut best_idx = 0;
    let mut best_sim = f32::NEG_INFINITY;
    for (i, vec) in vectors.iter().enumerate() {
        let sim = compute_similarity_dispatch(&vec.embedding, &centroid, similarity);
        if sim > best_sim {
            best_sim = sim;
            best_idx = i;
        }
    }

    // Track medoid indices
    let mut medoid_indices = vec![best_idx];

    // Assign all vectors to the first medoid
    {
        let medoid_embedding = vectors[best_idx].embedding.clone();
        for (i, vec) in vectors.iter_mut().enumerate() {
            let sim = if i == best_idx {
                1.0
            } else {
                compute_similarity_dispatch(&medoid_embedding, &vec.embedding, similarity)
            };
            vec.similarity = sim;
            vec.medoid_index = best_idx;
            vec.is_medoid = i == best_idx;
        }
    }

    // 3. Subsequent medoid selection (for cluster_id 1..cluster_count)
    let sample_step = (n / 100).max(1);

    for _ in 1..cluster_count {
        let mut best_candidate: Option<usize> = None;
        let mut best_total_gain = f32::NEG_INFINITY;

        // Sample non-medoid candidates
        for i in (0..n).step_by(sample_step) {
            if medoid_indices.contains(&i) {
                continue;
            }

            // Compute total similarity gain if this candidate were a new medoid
            let mut total_gain = 0.0;
            for vec in vectors.iter() {
                if medoid_indices.contains(&vec.medoid_index) && vec.is_medoid {
                    continue; // already a medoid
                }
                let new_sim =
                    compute_similarity_dispatch(&vectors[i].embedding, &vec.embedding, similarity);
                let gain = (new_sim - vec.similarity).max(0.0);
                total_gain += gain;
            }

            if total_gain > best_total_gain {
                best_total_gain = total_gain;
                best_candidate = Some(i);
            }
        }

        if let Some(new_medoid) = best_candidate {
            medoid_indices.push(new_medoid);
            vectors[new_medoid].is_medoid = true;
            vectors[new_medoid].similarity = 1.0;
            vectors[new_medoid].medoid_index = new_medoid;
            // Reassign vectors to new medoid if similarity improves
            let new_medoid_embedding = vectors[new_medoid].embedding.clone();
            for vec in vectors.iter_mut() {
                if vec.is_medoid {
                    continue;
                }
                let new_sim = compute_similarity_dispatch(
                    &new_medoid_embedding,
                    &vec.embedding,
                    similarity,
                );
                if new_sim > vec.similarity {
                    vec.similarity = new_sim;
                    vec.medoid_index = new_medoid;
                }
            }
        } else {
            // No suitable candidate found; break early
            break;
        }
    }

    // 4. Refinement loop (max 20 iterations)
    for _iter in 0..20 {
        // Compute per-cluster centroids
        let num_medoids = medoid_indices.len();
        let mut cluster_sums: Vec<(Vec<f32>, usize)> =
            vec![(vec![0.0_f32; dim], 0); num_medoids];

        for vec in &*vectors {
            let cluster_idx = medoid_indices
                .iter()
                .position(|&m| m == vec.medoid_index)
                .unwrap();
            let (sum, count) = &mut cluster_sums[cluster_idx];
            for (j, &val) in vec.embedding.iter().enumerate() {
                sum[j] += val;
            }
            *count += 1;
        }

        for (sum, count) in &mut cluster_sums {
            if *count > 0 {
                for val in sum.iter_mut() {
                    *val /= *count as f32;
                }
            }
        }

        // Find the member vector closest to each centroid
        let mut new_medoid_indices = Vec::with_capacity(num_medoids);
        for (cluster_idx, (ref centroid_vec, count)) in cluster_sums.iter().enumerate() {
            if *count == 0 {
                continue;
            }

            let old_medoid = medoid_indices[cluster_idx];
            let mut best_member = old_medoid;
            let mut best_sim = f32::NEG_INFINITY;

            for (i, vec) in vectors.iter().enumerate() {
                if vec.medoid_index != old_medoid {
                    continue;
                }
                let sim = compute_similarity_dispatch(&vec.embedding, centroid_vec, similarity);
                if sim > best_sim {
                    best_sim = sim;
                    best_member = i;
                }
            }
            new_medoid_indices.push(best_member);
        }

        // Check if medoids changed
        let medoids_changed = new_medoid_indices != medoid_indices;
        medoid_indices = new_medoid_indices;

        if !medoids_changed {
            break;
        }

        // Reassign all vectors to their best medoid
        assign_to_medoids(vectors, &medoid_indices, similarity);
    }

    // 5. Sort: by medoid_index, medoids first within each cluster
    vectors.sort_by(|a, b| {
        // Primary: medoid_index
        let cmp = a.medoid_index.cmp(&b.medoid_index);
        if cmp != std::cmp::Ordering::Equal {
            return cmp;
        }
        // Secondary: medoids first
        b.is_medoid.cmp(&a.is_medoid)
    });

    // 6. Build result
    let mut result = Vec::new();
    let mut current_medoid: Option<usize> = None;
    let mut current_count = 0;

    for vec in &*vectors {
        if current_medoid != Some(vec.medoid_index) {
            if let Some(m) = current_medoid {
                result.push(Medoid {
                    medoid_index: m,
                    child_count: current_count,
                });
            }
            current_medoid = Some(vec.medoid_index);
            current_count = 1;
        } else {
            current_count += 1;
        }
    }
    if let Some(m) = current_medoid {
        result.push(Medoid {
            medoid_index: m,
            child_count: current_count,
        });
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_vector(
        embedding: Vec<f32>,
        doc_id: u16,
        field_id: u32,
        chunk_id: u32,
    ) -> ParentMedoid {
        ParentMedoid {
            medoid_index: 0,
            is_medoid: false,
            similarity: 0.0,
            doc_id,
            field_id,
            chunk_id,
            embedding,
            scale: 1.0,
            norm: 0.0,
            zero_point: 0,
            sum_q: 0,
        }
    }

    /// Generate 20 vectors in 4 dimensions, forming 3 natural clusters.
    fn make_test_vectors() -> Vec<ParentMedoid> {
        let mut vectors = Vec::new();
        // Cluster 0 center: [1, 0, 0, 0]
        for i in 0..7 {
            let offset = (i as f32) * 0.05;
            vectors.push(make_vector(
                vec![1.0 + offset, offset * 0.5, 0.0, offset * 0.3],
                i as u16,
                0,
                0,
            ));
        }
        // Cluster 1 center: [0, 1, 0, 0]
        for i in 0..7 {
            let offset = (i as f32) * 0.05;
            vectors.push(make_vector(
                vec![offset * 0.3, 1.0 + offset, offset * 0.5, 0.0],
                (7 + i) as u16,
                0,
                0,
            ));
        }
        // Cluster 2 center: [0, 0, 1, 1]
        for i in 0..6 {
            let offset = (i as f32) * 0.05;
            vectors.push(make_vector(
                vec![0.0, offset * 0.3, 1.0 + offset, 1.0 + offset * 0.5],
                (14 + i) as u16,
                0,
                0,
            ));
        }
        vectors
    }

    #[test]
    fn test_k_medoid_three_clusters() {
        let mut vectors = make_test_vectors();
        assert_eq!(vectors.len(), 20);

        let medoids = cluster_vectors(
            &mut vectors,
            Clustering::Fixed(3),
            VectorSimilarity::Cosine,
        );

        // Verify 3 medoids returned
        assert_eq!(medoids.len(), 3);

        // All vectors should be assigned to a cluster
        let total_children: usize = medoids.iter().map(|m| m.child_count).sum();
        assert_eq!(total_children, 20);
    }

    #[test]
    fn test_clustering_none() {
        let mut vectors = make_test_vectors();
        let medoids = cluster_vectors(&mut vectors, Clustering::None, VectorSimilarity::Cosine);

        // Should produce exactly 1 cluster with all vectors
        assert_eq!(medoids.len(), 1);
        assert_eq!(medoids[0].child_count, 20);
    }

    #[test]
    fn test_clustering_fixed_one() {
        let mut vectors = make_test_vectors();
        let medoids =
            cluster_vectors(&mut vectors, Clustering::Fixed(1), VectorSimilarity::Cosine);

        // Fixed(1) should behave same as None
        assert_eq!(medoids.len(), 1);
        assert_eq!(medoids[0].child_count, 20);
    }

    #[test]
    fn test_medoid_is_medoid_flag() {
        let mut vectors = make_test_vectors();
        cluster_vectors(&mut vectors, Clustering::Fixed(3), VectorSimilarity::Cosine);

        // Count how many vectors have is_medoid = true
        let medoid_count = vectors.iter().filter(|v| v.is_medoid).count();
        assert_eq!(medoid_count, 3);

        // Verify each unique medoid_index group has exactly one medoid
        use std::collections::HashMap;
        let mut medoids_per_cluster: HashMap<usize, usize> = HashMap::new();
        for vec in &vectors {
            if vec.is_medoid {
                *medoids_per_cluster.entry(vec.medoid_index).or_insert(0) += 1;
            }
        }
        for (cluster_idx, count) in &medoids_per_cluster {
            assert_eq!(
                *count, 1,
                "Cluster {} should have exactly 1 medoid, got {}",
                cluster_idx, count
            );
        }
    }

    #[test]
    fn test_medoid_vectors_are_cluster_members() {
        let mut vectors = make_test_vectors();
        cluster_vectors(&mut vectors, Clustering::Fixed(3), VectorSimilarity::Cosine);

        // For each medoid (is_medoid == true), verify it is assigned to its own cluster.
        // The medoid_index should point to a vector that is itself a medoid.
        for vec in &vectors {
            if vec.is_medoid {
                // Find the medoid in its cluster
                let medoid_in_cluster = vectors
                    .iter()
                    .find(|v| v.medoid_index == vec.medoid_index && v.is_medoid)
                    .expect("Each cluster should have a medoid");
                // The medoid should be self-referential
                assert_eq!(
                    medoid_in_cluster.doc_id, vec.doc_id,
                    "Medoid with doc_id {} should be assigned to itself (medoid_index points to doc_id {})",
                    vec.doc_id, medoid_in_cluster.doc_id
                );
            }
        }
    }

    #[test]
    fn test_compute_similarity_cosine() {
        let a = vec![1.0, 0.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0, 0.0];
        let sim = compute_similarity_dispatch(&a, &b, VectorSimilarity::Cosine);
        assert!((sim - 1.0).abs() < 1e-6);

        let c = vec![0.0, 1.0, 0.0, 0.0];
        let sim_ortho = compute_similarity_dispatch(&a, &c, VectorSimilarity::Cosine);
        assert!(sim_ortho.abs() < 1e-6);
    }

    #[test]
    fn test_compute_similarity_dot() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![4.0, 3.0, 2.0, 1.0];
        let sim = compute_similarity_dispatch(&a, &b, VectorSimilarity::Dot);
        assert!((sim - 20.0).abs() < 1e-6);
    }

    #[test]
    fn test_compute_similarity_euclidean() {
        let a = vec![0.0, 0.0, 0.0, 0.0];
        let b = vec![0.0, 0.0, 0.0, 0.0];
        let sim = compute_similarity_dispatch(&a, &b, VectorSimilarity::Euclidean);
        assert!((sim - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_auto_clustering() {
        let mut vectors = make_test_vectors();
        let medoids = cluster_vectors(&mut vectors, Clustering::Auto, VectorSimilarity::Cosine);

        // sqrt(20) * 2 = 8, so should have 8 clusters
        assert_eq!(medoids.len(), 8);

        let total_children: usize = medoids.iter().map(|m| m.child_count).sum();
        assert_eq!(total_children, 20);
    }

    #[test]
    fn test_single_vector() {
        let mut vectors = vec![make_vector(vec![1.0, 0.0, 0.0, 0.0], 0, 0, 0)];
        let medoids =
            cluster_vectors(&mut vectors, Clustering::Fixed(5), VectorSimilarity::Cosine);

        // cluster_count clamped to len = 1
        assert_eq!(medoids.len(), 1);
        assert_eq!(medoids[0].child_count, 1);
    }
}
