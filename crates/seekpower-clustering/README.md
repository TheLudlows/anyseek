# seekpower-clustering

Vector clustering for the IVF (Inverted File) index. Partitions the
embedding space into clusters whose medoids drive coarse search at query
time.

See design spec section 5.4 for the algorithm and 5.3 for the cluster-count
heuristic.

## The `Clusterer` trait

```rust
pub trait Clusterer: Send + Sync {
    fn cluster(
        &self,
        vectors: &mut [ClusterVector],
        similarity: Similarity,
    ) -> Vec<Medoid>;
}
```

The trait mutates the input slice in place to fill each vector's assigned
cluster, and returns one `Medoid` per resulting cluster.

## Input: `ClusterVector`

The shape passed in for every embedding being clustered.

```rust
pub struct ClusterVector {
    pub doc_id: u64,
    pub field_id: u32,
    pub chunk_id: u32,
    pub embedding: Vec<f32>,
    pub medoid_index: usize, // filled in by the clusterer
    pub is_medoid: bool,     // filled in by the clusterer
    pub similarity: f32,     // filled in by the clusterer
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
}
```

The clusterer fills in `medoid_index`, `is_medoid`, and `similarity`. The
remaining fields are inputs (or unused by the clusterer, but present so a
`ClusterVector` can flow through the index pipeline untouched).

## Output: `Medoid`

```rust
pub struct Medoid {
    pub medoid_index: usize, // position of this medoid within the cluster list
    pub child_count: usize,  // number of vectors assigned to this cluster
}
```

Corresponds to the spec's `ClusterResult`. Each `Medoid` describes one
cluster; its `medoid_index` is a 0-based position, not an offset into the
input `vectors` slice.

## `KMedoidClusterer`

The only implementation today.

```rust
pub struct KMedoidClusterer {
    pub n_clusters: ClusterCount,
}

impl KMedoidClusterer {
    pub fn new(n_clusters: ClusterCount) -> Self;
}
```

### Algorithm (spec §5.4)

1. **First medoid.** Compute the dataset centroid (mean of all embeddings)
   and pick the vector with the highest similarity to that centroid.
2. **Greedy medoid seeding.** For each remaining medoid slot, scan
   candidates at stride `max(n / 100, 1)` (skipping already-chosen
   indices). For every candidate compute the total similarity gain
   (`sim(candidate, v) − max sim(medoids, v)`) across all vectors, and
   pick the candidate with the highest positive gain.
3. **Initial assignment.** Each vector is assigned to its nearest medoid.
4. **Iterative refinement.** Up to 20 rounds:
   - Recompute each cluster's centroid.
   - Pick the new medoid as the cluster member closest to that centroid
     (always an actual input vector — never a synthetic mean).
   - Reassign every vector to its nearest medoid.
5. **Stopping rule.** Stop when fewer than `n / 100` vectors (1%) are
   reassigned in a round, or after 20 rounds.

### Edge behavior

| Condition | Behavior |
| --- | --- |
| Empty input slice | Returns an empty `Vec<Medoid>` |
| `n <= k` | Every vector becomes its own single-member cluster |
| `ClusterCount::None` | `k = 1`; all vectors land in one cluster |

After the loop, the clusterer marks each medoid's `is_medoid = true` and
rewrites its `medoid_index` to be the cluster's position (0..k).

## `ClusterCount`

```rust
pub enum ClusterCount {
    Auto,
    Fixed(usize),
    None,
}
```

| Variant | Effect |
| --- | --- |
| `Auto` | `k = ceil(sqrt(n) * 2)` — derives a cluster count from the input size |
| `Fixed(k)` | Uses the user-supplied `k` |
| `None` | Forces `k = 1` (one cluster containing everything) |

The spec's `Auto` heuristic only kicks in once the corpus has at least
1000 vectors total; below that threshold the index falls back to Flat
search with `n_clusters = 0` at a higher layer. The clusterer itself
always computes a `k` from `Auto` regardless of corpus size — callers
that need the Flat-search fallback gate it before invoking the
clusterer.

## `compute_similarity`

```rust
pub fn compute_similarity(a: &[f32], b: &[f32], sim: &Similarity) -> f32;
```

| `Similarity` | Returns |
| --- | --- |
| `Dot` | `Σ a[i] * b[i]` |
| `Cosine` | `dot(a, b) / (||a|| * ||b||)`, or `0.0` if either norm is 0 |
| `Euclidean` | `1 / (1 + ||a − b||)` (converted to a similarity in `[0, 1]`) |

The medoid-selection loop uses this directly to score and assign vectors.

## Why K-Medoid (not K-Means)

Medoids are actual members of the input dataset. Consequences:

- **No float drift.** A medoid is one of the `Vec<f32>` embeddings the
  caller already has, not a synthetic mean that has to be reconstructed
  later.
- **Cheap to store.** IVF persists medoids as `(doc_id, vector)` pairs in
  `MedoidData` — pointing at a real vector means the index can read the
  medoid bytes straight out of the same quantized-vector store it uses
  for every other embedding.
- **Stable under quantization.** When medoids are later quantized, the
  quantized bytes correspond to a real distribution the quantizer was
  built to handle, rather than to a centroid that may sit outside the
  convex hull of the data.