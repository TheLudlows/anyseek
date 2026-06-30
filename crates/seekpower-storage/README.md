# seekpower-storage

Wraps [SlateDB](https://github.com/slatedb/slatedb) (an LSM-tree embedded KV
store) and provides a thread-safe in-memory cache for hot posting lists and
IVF cluster blobs. Every higher crate (`seekpower-core`, `seekpower-vector`,
`seekpower-search`, ...) routes all persistent IO through this layer.

The design rationale is laid out in the project spec at
`docs/superpowers/specs/2026-06-30-hybrid-search-design.md` (section 3 in
particular). Read that first if you need the big picture.

## Public surface at a glance

| Item | Kind | Purpose |
|---|---|---|
| `PREFIX_META` / `PREFIX_IDX` / `PREFIX_VEC` / `PREFIX_DOC` / `PREFIX_DELL` / `PREFIX_LEN` | constants | SlateDB key namespaces |
| `make_key(&[&[u8]]) -> Vec<u8>` | fn | Concatenate byte slices into a SlateDB key |
| `KvStore` | struct | Thin async wrapper around `slatedb::Db` |
| `ReadCache` | struct | Size-budgeted, in-memory cache for postings and clusters |

## Key prefix constants

Per the spec (section 3.1), SlateDB is partitioned by single-character byte
prefixes so that ordered prefix scans can enumerate a namespace in one pass.

| Constant | Bytes | Namespace |
|---|---|---|
| `PREFIX_META` | `b"m:"` | Schema, IndexConfig, stats JSON |
| `PREFIX_IDX` | `b"i:"` | Inverted-index posting list blocks |
| `PREFIX_VEC` | `b"v:"` | IVF meta, medoid vectors, cluster blobs |
| `PREFIX_DOC` | `b"d:"` | Compressed stored document bodies |
| `PREFIX_DELL` | `b"l:"` | Deletion tombstones (lower-case L) |
| `PREFIX_LEN` | `b"s:"` | Compressed document lengths |

## `make_key`

```rust
pub fn make_key(parts: &[&[u8]]) -> Vec<u8>
```

Concatenates byte slices in order. The only place this lives is the storage
crate so every consumer builds keys identically.

```rust
use seekpower_storage::{make_key, PREFIX_VEC};

let key = make_key(&[PREFIX_VEC, b"index-a:medoid:7"]);
// -> b"v:index-a:medoid:7"
```

## `KvStore`

Async handle to a SlateDB instance. `KvStore` is `Clone` -- the inner SlateDB
handle is reference-counted, so cloning is cheap and clones talk to the same
database.

### Opening

```rust
pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self>
```

`path` is the SlateDB object-store prefix. `object_store` may be any
`Arc<dyn object_store::ObjectStore>`:

- `object_store::memory::InMemory` -- tests
- `object_store::local::LocalFileSystem` -- single-node dev
- `object_store::aws::AmazonS3` / `gcp::GoogleCloudStorage` / `azure::AzureBlobStorage` -- production

### Basic ops

```rust
pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>
pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>
pub async fn delete(&self, key: &[u8]) -> Result<()>
pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
pub async fn close(&self) -> Result<()>
```

`scan_prefix` returns a fully materialised `Vec` of `(key, value)` pairs --
fine for the small namespace scans SeekPower does (meta, length, deltas) but
not intended for streaming the whole index.

### JSON helpers

```rust
pub async fn put_serialized<T: Serialize>(&self, key: &[u8], value: &T) -> Result<()>
pub async fn get_deserialized<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>>
```

The raw `put`/`get` take byte slices. For anything structured (schema, stats,
`IndexConfig`, `IvfMeta`) use the `_serialized` / `_deserialized` pair --
they round-trip through `serde_json`.

## `ReadCache`

Two independent, size-budgeted maps living behind `RwLock`s, with byte-size
tracking.

```rust
pub fn new(posting_max_mb: usize, cluster_max_mb: usize) -> Self
```

Sizes are in MiB; the constructor multiplies by `1024 * 1024` internally.

### Posting-list cache (key is the `key_hash`, value is a `PostingListBlock`)

```rust
pub fn get_posting(&self, key: &[u8]) -> Option<Vec<u8>>
pub fn insert_posting(&self, key: &[u8], value: Vec<u8>)
```

### Cluster-blob cache (key is the cluster id, value is `ClusterData`)

```rust
pub fn get_cluster(&self, key: &[u8]) -> Option<Vec<u8>>
pub fn insert_cluster(&self, key: &[u8], value: Vec<u8>)
```

### Maintenance

```rust
pub fn invalidate(&self, key: &[u8])   // drop a single entry from BOTH caches
pub fn clear(&self)                     // drop everything
```

### Eviction

Eviction is intentionally simple and deterministic: when an `insert_*` would
push the cache above its byte budget, half of the current entries (the first
half of the `HashMap` iteration order) are dropped. It is **not** a true LRU
-- the only freshness signal is that re-inserting an existing key overwrites
its value but does not reorder the map.

## Typical usage

```rust
use std::sync::Arc;
use object_store::memory::InMemory;
use seekpower_storage::{KvStore, ReadCache, make_key, PREFIX_META};

let store = KvStore::open("idx", Arc::new(InMemory::new())).await?;
let cache = ReadCache::new(256, 512); // 256 MiB postings, 512 MiB clusters

let key = make_key(&[PREFIX_META, b"idx:config"]);
store.put_serialized(&key, &my_index_config).await?;

if let Some(bytes) = cache.get_posting(&key) {
    // hit
} else if let Some(bytes) = store.get(&key).await? {
    cache.insert_posting(&key, bytes.clone());
    // ...
}

store.close().await?;
```

## Verifying against source

Everything above is the public API of
`crates/seekpower-storage/src/lib.rs`. Re-read that file (it is short) when
the spec and the source disagree -- the source wins.
