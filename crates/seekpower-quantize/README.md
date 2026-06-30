# seekpower-quantize

Scalar quantization of `f32` embedding vectors into compact integer form for
~4x storage savings with controllable precision loss. Used by the IVF vector
index to keep dense embeddings in SlateDB without paying the full `f32`
storage cost.

See design spec section 5.5 for context.

## The `Quantizer` trait

```rust
pub trait Quantizer: Send + Sync {
    /// Quantize an f32 vector, returning the quantized data and metadata.
    fn quantize(&self, vector: &[f32]) -> QuantizedVector;

    /// Number of bytes per quantized dimension.
    fn bytes_per_dim(&self) -> usize;
}
```

Distance / similarity computation lives on the quantizer's output type or in
the index layer that consumes it; the trait itself only owns the
quantize-and-store surface.

## `QuantizedVector`

The output of every `quantize` call.

| Field | Type | Meaning |
| --- | --- | --- |
| `data` | `Vec<u8>` | Quantized values in the range 0..=255, one byte per dim |
| `scale` | `f32` | Dequantization slope: `original ≈ scale * q + zero_point` |
| `zero_point` | `i16` | Dequantization offset |
| `norm` | `f32` | L2 norm of the original `f32` vector (needed for cosine) |
| `sum_q` | `i32` | Sum of quantized values (used by efficient dot-product math) |

## `ScalarQuantI8`

The only implementation today. One byte per dimension, asymmetric
quantization that learns its own `(scale, zero_point)` per vector.

```text
min  = min(v)
max  = max(v)
norm = ||v||_2

scale      = (max - min) / 255          (fallback 1.0 if max == min)
zero_point = round(-min / scale)        (fallback 128 if max == min)
q[i]       = clamp(round((v[i] - min) / scale), 0, 255)  as u8
```

Dequantization recovers an approximation:

```text
v[i] ≈ scale * (q[i] as f32) + zero_point
```

Edge cases the implementation handles:

| Input | Behavior |
| --- | --- |
| Empty vector | Returns empty `data`, `scale = 1.0`, `zero_point = 0`, `norm = 0.0`, `sum_q = 0` |
| Constant vector (`max == min`) | `scale = 1.0`, `zero_point = 128`, `data` filled with 128 |

### Quantized-space inner product

The index can score query against quantized vectors without ever
dequantizing them. With `q` the stored bytes and `u` the f32 query:

```text
dot_q      = Σ_i q[i] as i32 * u[i]   (u8 × f32, SIMD-friendly)
score      ≈ scale * dot_q
            + zero_point * Σ_i u[i] * scale
            (full expansion using sum_q and Σu)
```

The retained `sum_q` field lets the index fold the `zero_point` term out
of the inner loop, which keeps the hot path on widened integer ops and
avoids a per-dim dequantize.

## Storage savings

| Representation | Bytes per dim | For 1024-dim vector |
| --- | --- | --- |
| `f32` | 4 | 4096 B |
| Quantized `u8` + per-vector metadata | 1 | ~1024 B + ~24 B |

Roughly **75% reduction** in raw embedding footprint. Per-vector metadata
(`scale`, `zero_point`, `norm`, `sum_q`) is amortized across all
dimensions, so the win grows with dimension count.

## Usage pattern

1. At index time, call `ScalarQuantI8::new().quantize(&embedding)` for each
   stored vector and write the resulting `QuantizedVector` blob (plus its
   `medoid_index` / `doc_id` keys) into SlateDB.
2. At search time, the IVF index loads only the medoids and their quantized
   blobs for the probed clusters, then scores the (full-precision) query
   against the stored `u8` bytes using the formula above. There is no need
   to dequantize each stored vector on the hot path.

## Trade-offs

- **Bounded error.** Per-dimension error is at most `scale / 2`. Vectors
  with a wide dynamic range pay a larger `scale` and lose more precision.
- **Cosine requires the original norm.** Dot-product scoring in quantized
  space is cheap; cosine needs the stored `norm` to renormalize.
- **Asymmetric is heavier than symmetric.** Storing `zero_point` plus
  `sum_q` adds metadata per vector but lets the query stay in `f32` and
  keeps the inner loop free of branches.