# seekpower-vector

Vector index abstraction plus the production IVF (Inverted File) implementation
backed by SlateDB. Encapsulates the "聚类即键值" (clustering-as-key-value) design
from spec §5.1: medoids are first-class KV rows, each cluster is a serialized
blob, and the whole layout rides on the storage layer's KV API.

## Public API surface

### `VectorIndex` trait

The single integration point a query engine talks to.

```rust
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
```

| Method | Effect |
|---|---|
| `search` | Synchronous top-k search; consults in-memory buffer and KvStore for cluster data. |
| `add_vector` | Pushes a `ClusterVector` into the in-memory write buffer (non-blocking). |
| `flush` | Async; commits the buffer to SlateDB via `store_meta` / `store_medoid` / `store_cluster`. |
| `n_clusters` | Returns `0` for flat indexes, otherwise the persisted `IvfMeta::n_clusters`. |

### `AnnMode`

```rust
pub enum AnnMode {
    All,                       // exact k-NN over every cluster
    Nprobe(usize),             // top-n clusters by medoid similarity
    SimilarityThreshold(f32),  // every cluster whose medoid sim >= threshold
}
```

### `VectorSearchResult`

```rust
pub struct VectorSearchResult {
    pub doc_id: u64,
    pub score: f32,
    pub field_id: u32,
    pub chunk_id: u32,
}
```

## Persisted types

All three live under the `TYPE_VEC` data-type namespace within the index's
keyspace (isolated by `index_id` + `data_type` prefix in KvStore). A single
SlateDB instance can host many indexes without key collisions.

| Type | Storage key | Purpose |
|---|---|---|
| `IvfMeta` | `v:{path}:meta` | `dim`, `n_clusters`, `similarity`, `quantization`, `total_vectors`. |
| `MedoidData` | `v:{path}:medoid:{id}` | `medoid_id`, full-precision `vector`, `child_count`. |
| Cluster blob | `v:{path}:cluster:{id}` | Header + array of quantized vectors. |

`n_clusters == 0` in `IvfMeta` is the sentinel for **flat** (un-clustered)
mode and triggers a different search/serialize path.

### `IvfMeta`

```rust
pub struct IvfMeta {
    pub dim: u32,
    pub n_clusters: u32,
    pub similarity: Similarity,
    pub quantization: QuantType,
    pub total_vectors: u64,
}
```

### `MedoidData`

```rust
pub struct MedoidData {
    pub medoid_id: u32,
    pub vector: Vec<f32>,
    pub child_count: u32,
}
```

### Cluster blob on-wire layout

A cluster blob is a packed `ClusterHeader` followed by one
`QuantizedVectorHeader` plus a `dim`-byte quantized payload per vector.

```rust
// Total header size: 12 bytes.
pub struct ClusterHeader {
    pub cluster_id: u32,
    pub vector_count: u32,
    pub byte_size: u32,   // actually the per-vector byte length (== dim)
}

// Total header size: 26 bytes.
pub struct QuantizedVectorHeader {
    pub doc_id: u64,
    pub field_id: u32,
    pub chunk_id: u32,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
}
```

`CLUSTER_HEADER_SIZE = 12`, `QV_HEADER_SIZE = 26`. Quantization currently uses
`ScalarQuantI8` from `seekpower-quantize`.

## `IvfIndex` (concrete implementation)

### Construction

```rust
pub fn new(
    kv: seekpower_storage::KvStore,
    index_id: u32,
    config: seekpower_schema::IndexConfig,
) -> Self;
```

The constructor takes a shared `KvStore` and the index's `index_id` (derived
from the index name via `name_to_id`). All KV operations use `KvStore`'s
`prefixed_*` methods for automatic key isolation.

### KV I/O

| Method | Reads / writes |
|---|---|
| `load_meta()` | Reads `v:meta` via `prefixed_get_deserialized`, refreshes the cached `meta` lock. |
| `load_medoids()` | Scans `v:medoid:*` via `prefixed_scan_prefix`, sorts by `medoid_id`, refreshes the medoids cache. |
| `load_cluster(id)` | Reads `v:cluster:{id}` via `prefixed_get`. |
| `store_meta(&meta)` | Writes the key via `prefixed_put_serialized` and updates the cached `meta`. |
| `store_medoid(id, &m)` | Writes one medoid row via `prefixed_put_serialized`. |
| `store_cluster(id, &[u8])` | Writes the cluster blob via `prefixed_put`. |

### In-memory state

| Field | Lock | Notes |
|---|---|---|
| `kv` | — | Shared `KvStore` (clone is cheap, backed by `Arc`). |
| `index_id` | — | u32 prefix for key isolation. |
| `meta` | `std::sync::RwLock<Option<IvfMeta>>` | Hydrated lazily by `load_meta`. |
| `medoids` | `std::sync::RwLock<Vec<MedoidData>>` | Hydrated lazily by `load_medoids`. |
| `buffer` | `tokio::sync::RwLock<Vec<ClusterVector>>` | Drain target of `add_vector`. |

### `flush` strategy

`flush()` drains the buffer, then picks one of three paths based on the
current state (spec §5.3):

1. **`total_vectors < 1000`** — flat storage in cluster `0` (a single
   `MedoidData` whose vector is the centroid). No K-Medoid is run.
2. **`buffer_count > 20% of total_vectors`** (or no prior meta exists) — full
   K-Medoid re-clustering: load every persisted cluster, dequantize, merge in
   the buffer, re-run the clusterer, and rewrite all clusters, medoids, and
   meta.
3. Otherwise — **incremental**: assign each buffered vector to its nearest
   existing medoid, load the target cluster, append quantized vectors, rewrite
   the cluster, and bump that medoid's `child_count`. Meta gains only the new
   `total_vectors`.

`n_clusters()` simply reads from the cached `IvfMeta`.

## Search flow

1. Read `meta` + `medoids` from the cached locks (cheap).
2. Score the query against each medoid; pick cluster indices per `AnnMode`.
   `All` returns every index; `Nprobe(n)` returns the top-`n`; the threshold
   variant walks the sorted list and stops once similarity drops below the
   threshold.
3. For each selected cluster id, load via `self.kv.prefixed_get(self.index_id, &key)`.
4. Dequantize every vector in the cluster and compute the configured
   `Similarity` against the query.
5. Also score every vector still in the in-memory buffer (using
   `try_read` so a writer never blocks a search).
6. Sort all candidates by score descending, drop anything in `delete_set`,
   and truncate to `k`.

The flat mode (no clusters) collapses to: scan the buffer + scan cluster `0`'s
blob, then truncate to `k`.

## Verification

All KV operations use `KvStore::prefixed_*` methods with `self.index_id`,
ensuring automatic key isolation. Integration tests create `IvfIndex` via:

```rust
let kv = KvStore::open("test_vec", os).await.unwrap();
let index_id = name_to_id("test");
IvfIndex::new(kv, index_id, default_config())
```