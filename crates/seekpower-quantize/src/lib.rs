/// Trait for vector quantization.
pub trait Quantizer: Send + Sync {
    /// Quantize a f32 vector, returning the quantized data and metadata.
    fn quantize(&self, vector: &[f32]) -> QuantizedVector;

    /// The number of bytes per quantized dimension.
    fn bytes_per_dim(&self) -> usize;
}

/// Result of quantizing a vector.
#[derive(Debug, Clone)]
pub struct QuantizedVector {
    /// Quantized data (u8 bytes, range 0-255).
    pub data: Vec<u8>,
    /// Scale factor: original_value ≈ scale * quantized_value + zero_point
    pub scale: f32,
    /// Zero point offset.
    pub zero_point: i16,
    /// L2 norm of the original vector.
    pub norm: f32,
    /// Sum of quantized values.
    pub sum_q: i32,
}

/// Scalar Quantization to i8.
pub struct ScalarQuantI8;

impl ScalarQuantI8 {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ScalarQuantI8 {
    fn default() -> Self {
        Self::new()
    }
}

impl Quantizer for ScalarQuantI8 {
    fn quantize(&self, vector: &[f32]) -> QuantizedVector {
        if vector.is_empty() {
            return QuantizedVector {
                data: Vec::new(),
                scale: 1.0,
                zero_point: 0,
                norm: 0.0,
                sum_q: 0,
            };
        }

        let min = vector.iter().cloned().fold(f32::INFINITY, f32::min);
        let max = vector.iter().cloned().fold(f32::NEG_INFINITY, f32::max);

        let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();

        let scale = if max > min {
            (max - min) / 255.0
        } else {
            1.0
        };

        let zero_point = if max > min {
            ((-min) / scale).round() as i16
        } else {
            128i16
        };

        let data: Vec<u8> = vector
            .iter()
            .map(|&v| {
                let q = ((v - min) / scale).round() as i32;
                q.clamp(0, 255) as u8
            })
            .collect();

        let sum_q: i32 = data.iter().map(|&q| q as i32).sum();

        QuantizedVector {
            data,
            scale,
            zero_point,
            norm,
            sum_q,
        }
    }

    fn bytes_per_dim(&self) -> usize {
        1 // i8 = 1 byte
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantize_simple() {
        let q = ScalarQuantI8::new();
        let v = vec![0.0, 1.0, 2.0, 3.0];
        let result = q.quantize(&v);
        assert_eq!(result.data.len(), 4);
        assert!(result.scale > 0.0);
        assert!(result.norm > 0.0);
        // min maps to 0, max maps to ~255
        assert_eq!(result.data[0], 0);
        assert!(result.data[3] > 250);
    }

    #[test]
    fn test_quantize_constant_vector() {
        let q = ScalarQuantI8::new();
        let v = vec![5.0; 4];
        let result = q.quantize(&v);
        assert_eq!(result.data.len(), 4);
        // Constant vector: scale = 1.0 fallback
        assert_eq!(result.scale, 1.0);
    }

    #[test]
    fn test_quantize_empty() {
        let q = ScalarQuantI8::new();
        let v: Vec<f32> = vec![];
        let result = q.quantize(&v);
        assert!(result.data.is_empty());
        assert_eq!(result.norm, 0.0);
    }

    #[test]
    fn test_bytes_per_dim() {
        let q = ScalarQuantI8::new();
        assert_eq!(q.bytes_per_dim(), 1);
    }
}
