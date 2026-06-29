// storm-db module

use crate::schema::VectorSimilarity;

#[derive(Debug, Clone)]
pub struct QuantizedVector {
    pub data: Vec<i8>,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
    pub sum_q: i32,
}

/// Affine scalar quantization f32 -> i8
pub fn quantize_i8(vector: &[f32]) -> QuantizedVector {
    let min = vector
        .iter()
        .cloned()
        .reduce(f32::min)
        .unwrap_or(0.0);
    let max = vector
        .iter()
        .cloned()
        .reduce(f32::max)
        .unwrap_or(0.0);

    let scale = if (max - min).abs() < f32::EPSILON {
        1.0
    } else {
        (max - min) / 255.0
    };

    let zero_point = ((-min / scale).round() as i16).clamp(-128, 127);

    let data: Vec<i8> = vector
        .iter()
        .map(|&v| {
            let q = ((v - min) / scale).round() as i16;
            // Reinterpret u8 [0..255] as i8 for storage (wraparound)
            (q.clamp(0, 255) as u8) as i8
        })
        .collect();

    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();

    let sum_q: i32 = data.iter().map(|&q| (q as u8) as i32).sum();

    QuantizedVector {
        data,
        scale,
        norm,
        zero_point,
        sum_q,
    }
}

/// Inverse quantization
pub fn dequantize_i8(qv: &QuantizedVector) -> Vec<f32> {
    qv.data
        .iter()
        .map(|&q| (q as f32 - qv.zero_point as f32) * qv.scale)
        .collect()
}

/// Dot product similarity
pub fn compute_similarity_dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(&x, &y)| x * y).sum()
}

/// Cosine similarity: dot product on L2-normalized vectors
pub fn compute_similarity_cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut a_norm = a.to_vec();
    normalize(&mut a_norm);
    let mut b_norm = b.to_vec();
    normalize(&mut b_norm);
    compute_similarity_dot(&a_norm, &b_norm)
}

/// Euclidean similarity: negative squared distance
pub fn compute_similarity_euclidean(a: &[f32], b: &[f32]) -> f32 {
    let neg_sq_dist: f32 = a
        .iter()
        .zip(b.iter())
        .map(|(&x, &y)| {
            let d = x - y;
            -(d * d)
        })
        .sum();
    neg_sq_dist
}

/// Dispatch similarity based on enum
pub fn compute_similarity(
    query: &[f32],
    vector: &[f32],
    similarity: VectorSimilarity,
) -> f32 {
    match similarity {
        VectorSimilarity::Dot => compute_similarity_dot(query, vector),
        VectorSimilarity::Cosine => compute_similarity_cosine(query, vector),
        VectorSimilarity::Euclidean => compute_similarity_euclidean(query, vector),
    }
}

/// L2 normalize a vector in-place
pub fn normalize(vector: &mut [f32]) {
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in vector.iter_mut() {
            *v /= norm;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantize_dequantize_roundtrip() {
        let original = vec![1.0, 2.5, -3.0, 0.5, 7.2];
        let qv = quantize_i8(&original);
        let reconstructed = dequantize_i8(&qv);

        for (orig, recon) in original.iter().zip(reconstructed.iter()) {
            assert!(
                (orig - recon).abs() < qv.scale,
                "orig={}, recon={}, scale={}",
                orig,
                recon,
                qv.scale
            );
        }
    }

    #[test]
    fn test_dot_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        assert!((compute_similarity_dot(&a, &b) - 0.0).abs() < 1e-6);
    }

    #[test]
    fn test_dot_identical() {
        let a = vec![1.0, 0.0, 0.0];
        assert!((compute_similarity_dot(&a, &a) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_cosine_parallel() {
        let a = vec![3.0, 4.0, 0.0];
        let b = vec![6.0, 8.0, 0.0];
        assert!(
            (compute_similarity_cosine(&a, &b) - 1.0).abs() < 1e-6,
            "cosine = {}",
            compute_similarity_cosine(&a, &b)
        );
    }

    #[test]
    fn test_cosine_orthogonal() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        assert!(
            (compute_similarity_cosine(&a, &b) - 0.0).abs() < 1e-6,
            "cosine = {}",
            compute_similarity_cosine(&a, &b)
        );
    }

    #[test]
    fn test_euclidean_identical() {
        let a = vec![1.0, 2.0, 3.0];
        assert!(
            (compute_similarity_euclidean(&a, &a) - 0.0).abs() < 1e-6,
            "euclidean = {}",
            compute_similarity_euclidean(&a, &a)
        );
    }

    #[test]
    fn test_euclidean_different() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 1.0, 1.0];
        let sim = compute_similarity_euclidean(&a, &b);
        assert!(sim < 0.0, "euclidean similarity for different vectors should be negative, got {}", sim);
    }

    #[test]
    fn test_compute_similarity_dispatch_dot() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        assert_eq!(
            compute_similarity(&a, &b, VectorSimilarity::Dot),
            compute_similarity_dot(&a, &b)
        );
    }

    #[test]
    fn test_compute_similarity_dispatch_cosine() {
        let a = vec![1.0, 2.0];
        let b = vec![3.0, 4.0];
        assert_eq!(
            compute_similarity(&a, &b, VectorSimilarity::Cosine),
            compute_similarity_cosine(&a, &b)
        );
    }

    #[test]
    fn test_compute_similarity_dispatch_euclidean() {
        let a = vec![1.0, 2.0];
        let b = vec![3.0, 4.0];
        assert_eq!(
            compute_similarity(&a, &b, VectorSimilarity::Euclidean),
            compute_similarity_euclidean(&a, &b)
        );
    }

    #[test]
    fn test_normalize() {
        let mut v = vec![3.0, 4.0];
        normalize(&mut v);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6, "norm = {}", norm);
    }

    #[test]
    fn test_constant_vector() {
        let original = vec![5.0, 5.0, 5.0, 5.0];
        let qv = quantize_i8(&original);
        let reconstructed = dequantize_i8(&qv);

        for (orig, recon) in original.iter().zip(reconstructed.iter()) {
            assert!(
                (orig - recon).abs() < qv.scale,
                "orig={}, recon={}, scale={}",
                orig,
                recon,
                qv.scale
            );
        }
    }
}
