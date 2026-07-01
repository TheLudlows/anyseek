//! Binary serialization/deserialization for cluster data and quantized vectors.

use crate::types::{ClusterHeader, QuantizedVectorHeader};
use crate::clustering::ClusterVector;
use seekpower_quantize::Quantizer;

const CLUSTER_HEADER_SIZE: usize = 12;
const QV_HEADER_SIZE: usize = 26;

pub fn encode_cluster_header(h: &ClusterHeader) -> Vec<u8> {
    let mut buf = Vec::with_capacity(CLUSTER_HEADER_SIZE);
    buf.extend_from_slice(&h.cluster_id.to_le_bytes());
    buf.extend_from_slice(&h.vector_count.to_le_bytes());
    buf.extend_from_slice(&h.byte_size.to_le_bytes());
    buf
}

pub fn decode_cluster_header(data: &[u8]) -> Option<ClusterHeader> {
    if data.len() < CLUSTER_HEADER_SIZE {
        return None;
    }
    Some(ClusterHeader {
        cluster_id: u32::from_le_bytes(data[0..4].try_into().ok()?),
        vector_count: u32::from_le_bytes(data[4..8].try_into().ok()?),
        byte_size: u32::from_le_bytes(data[8..12].try_into().ok()?),
    })
}

pub fn encode_qv_header(h: &QuantizedVectorHeader) -> Vec<u8> {
    let mut buf = Vec::with_capacity(QV_HEADER_SIZE);
    buf.extend_from_slice(&h.doc_id.to_le_bytes());
    buf.extend_from_slice(&h.field_id.to_le_bytes());
    buf.extend_from_slice(&h.chunk_id.to_le_bytes());
    buf.extend_from_slice(&h.scale.to_le_bytes());
    buf.extend_from_slice(&h.norm.to_le_bytes());
    buf.extend_from_slice(&h.zero_point.to_le_bytes());
    buf
}

pub fn decode_qv_header(data: &[u8]) -> Option<QuantizedVectorHeader> {
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
pub fn serialize_cluster(
    cluster_id: u32,
    vectors: &[ClusterVector],
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
pub fn deserialize_cluster(
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

/// Dequantize a scalar-quantized vector back to approximate f32 values.
pub fn dequantize(qdata: &[u8], dim: usize, scale: f32, zero_point: i16) -> Vec<f32> {
    (0..dim)
        .map(|i| {
            let q = qdata[i] as f32;
            scale * (q - zero_point as f32)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clustering::ClusterVector;

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

    #[test]
    fn test_cluster_serialize_deserialize_roundtrip() {
        let vectors = vec![
            ClusterVector {
                doc_id: 1, field_id: 0, chunk_id: 0,
                embedding: vec![1.0, 2.0, 3.0, 4.0],
                medoid_index: 0, is_medoid: false, similarity: 0.0,
                scale: 1.0, norm: 0.0, zero_point: 0,
            },
            ClusterVector {
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
}
