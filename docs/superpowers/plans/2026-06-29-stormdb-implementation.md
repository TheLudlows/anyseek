# StormDB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a hybrid search engine (lexical + vector + RRF) on SlateDB with feature parity to SeekStorm's core search.

**Architecture:** SlateDB as unified KV storage layer. All data (posting lists, vectors, documents, facets, metadata) stored via key-prefix partitioning. In-memory structures loaded from SlateDB on open/commit for query performance. No sharding.

**Tech Stack:** Rust, SlateDB, object_store, tokio, jieba-rs, snowball-stemmers-rs, model2vec-rs, axum, utoipa

---

## File Structure

```
D:/code/storm-db/
├── Cargo.toml
├── src/
│   ├── lib.rs              # Library entry, public API re-exports
│   ├── error.rs            # StormDbError enum (thiserror)
│   ├── schema.rs           # FieldType, SchemaField, IndexConfig
│   ├── kv_store.rs         # SlateDB wrapper (KvStore struct)
│   ├── posting.rs          # Posting list structures, serialize/deserialize
│   ├── compress.rs         # Array/Bitmap/RLE compression + VInt positions
│   ├── tokenizer.rs        # English + Chinese tokenization, query parsing
│   ├── ranking.rs          # BM25F scoring, MinHeap top-k, DOCUMENT_LENGTH_COMPRESSION
│   ├── search.rs           # Search engine (single/union/intersection/phrase)
│   ├── vector.rs           # IVF index, vector search, TopK
│   ├── clustering.rs       # K-Medoid (PAM) clustering
│   ├── quantize.rs         # Scalar quantization f32→i8, similarity functions
│   ├── embed.rs            # Model2Vec embedding integration
│   ├── facet.rs            # Facet encoding/decoding, facet counting
│   ├── document.rs         # index_document, delete_document, get_document
│   ├── hybrid.rs           # RRF fusion
│   ├── index.rs            # Index struct, create/open/close/commit
│   └── server/
│       ├── mod.rs           # Axum server setup
│       ├── router.rs        # Route definitions
│       └── handler.rs       # Request handlers
```

---

## Task 1: Project Scaffolding

**Files:**
- Create: `D:/code/storm-db/Cargo.toml`
- Create: `D:/code/storm-db/src/lib.rs`

- [ ] **Step 1: Create project directory**

```bash
mkdir -p D:/code/storm-db/src
```

- [ ] **Step 2: Create Cargo.toml**

```toml
[package]
name = "storm-db"
version = "0.1.0"
edition = "2021"
description = "Hybrid search engine (lexical + vector + RRF) built on SlateDB"
license = "Apache-2.0"

[dependencies]
slatedb = "0.7"
object_store = "0.11"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
ahash = { version = "0.8", features = ["serde"] }
indexmap = { version = "2", features = ["serde"] }
smallvec = "1"
rayon = "1"
itertools = "0.14"
jieba-rs = "0.7"
snowball_stemmers_rs = "1"
snap = "1"
lz4_flex = "0.13"
zstd = "0.13"
axum = "0.8"
utoipa = { version = "5", features = ["preserve_order", "yaml"] }
thiserror = "2"
bytemuck = "1"

[dev-dependencies]
tokio-test = "0.4"
tempfile = "3"
```

- [ ] **Step 3: Create src/lib.rs with module declarations**

```rust
pub mod error;
pub mod schema;
pub mod kv_store;
pub mod posting;
pub mod compress;
pub mod tokenizer;
pub mod ranking;
pub mod search;
pub mod vector;
pub mod clustering;
pub mod quantize;
pub mod embed;
pub mod facet;
pub mod document;
pub mod hybrid;
pub mod index;
pub mod server;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const ROARING_BLOCK_SIZE: usize = 65_536;
```

- [ ] **Step 4: Create stub files for all modules (empty `// TODO` placeholder)**

```bash
for f in error schema kv_store posting compress tokenizer ranking search vector clustering quantize embed facet document hybrid index; do
  touch D:/code/storm-db/src/$f.rs
done
mkdir -p D:/code/storm-db/src/server
touch D:/code/storm-db/src/server/mod.rs D:/code/storm-db/src/server/router.rs D:/code/storm-db/src/server/handler.rs
```

- [ ] **Step 5: Verify compilation**

```bash
cd D:/code/storm-db && cargo check
```

Expected: compiles with warnings about empty modules

- [ ] **Step 6: Commit**

```bash
git init && git add -A && git commit -m "feat: project scaffolding with Cargo.toml and module stubs"
```

---

## Task 2: Error Types

**Files:**
- Create: `D:/code/storm-db/src/error.rs`

- [ ] **Step 1: Write failing test**

```rust
// src/error.rs
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StormDbError {
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("document not found: {0}")]
    DocumentNotFound(u64),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("schema error: {0}")]
    SchemaError(String),
    #[error("storage error: {0}")]
    StorageError(String),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, StormDbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let e = StormDbError::IndexNotFound("test".to_string());
        assert_eq!(e.to_string(), "index not found: test");

        let e = StormDbError::DocumentNotFound(42);
        assert_eq!(e.to_string(), "document not found: 42");
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: StormDbError = io_err.into();
        assert!(matches!(err, StormDbError::IoError(_)));
    }
}
```

- [ ] **Step 2: Run test**

```bash
cd D:/code/storm-db && cargo test error --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: error types with StormDbError enum"
```

---

## Task 3: Schema Types

**Files:**
- Create: `D:/code/storm-db/src/schema.rs`

- [ ] **Step 1: Write schema types and tests**

```rust
// src/schema.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldType {
    U8, U16, U32, U64,
    I8, I16, I32, I64,
    Timestamp,
    F32, F64,
    Bool,
    String16, String32,
    StringSet16, StringSet32,
    Text,
    Point,
    Vector,
}

impl FieldType {
    pub fn byte_size(&self) -> usize {
        match self {
            FieldType::U8 | FieldType::I8 | FieldType::Bool => 1,
            FieldType::U16 | FieldType::I16 | FieldType::String16 | FieldType::StringSet16 => 2,
            FieldType::U32 | FieldType::I32 | FieldType::F32 | FieldType::String32 | FieldType::StringSet32 => 4,
            FieldType::U64 | FieldType::I64 | FieldType::Timestamp | FieldType::F64 | FieldType::Point => 8,
            FieldType::Text | FieldType::Vector => 0,
        }
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, FieldType::U8 | FieldType::U16 | FieldType::U32 | FieldType::U64
            | FieldType::I8 | FieldType::I16 | FieldType::I32 | FieldType::I64
            | FieldType::Timestamp | FieldType::F32 | FieldType::F64)
    }

    pub fn is_text(&self) -> bool { matches!(self, FieldType::Text) }
    pub fn is_vector(&self) -> bool { matches!(self, FieldType::Vector) }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub field: String,
    pub store: bool,
    pub index_lexical: bool,
    pub index_vector: bool,
    pub field_type: FieldType,
    pub facet: bool,
    pub boost: f32,
}

impl SchemaField {
    pub fn text(field: &str) -> Self {
        Self {
            field: field.to_string(),
            store: true,
            index_lexical: true,
            index_vector: false,
            field_type: FieldType::Text,
            facet: false,
            boost: 1.0,
        }
    }

    pub fn vector(field: &str) -> Self {
        Self {
            field: field.to_string(),
            store: false,
            index_lexical: false,
            index_vector: true,
            field_type: FieldType::Vector,
            facet: false,
            boost: 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TokenizerType {
    AsciiAlphabetic,
    UnicodeAlphanumeric,
    UnicodeAlphanumericFolded,
    UnicodeAlphanumericZH,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VectorSimilarity {
    Dot,
    Cosine,
    Euclidean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Quantization {
    None,
    ScalarQuantizationI8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Clustering {
    Auto,
    Fixed(usize),
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compression {
    None,
    Lz4,
    Snappy,
    Zstd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub tokenizer_type: TokenizerType,
    pub stemmer_language: Option<String>,
    pub vector_similarity: VectorSimilarity,
    pub vector_dimensions: usize,
    pub quantization: Quantization,
    pub compression: Compression,
    pub segment_number_bits: u8,
    pub clustering: Clustering,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            tokenizer_type: TokenizerType::UnicodeAlphanumericFolded,
            stemmer_language: None,
            vector_similarity: VectorSimilarity::Cosine,
            vector_dimensions: 256,
            quantization: Quantization::None,
            compression: Compression::Snappy,
            segment_number_bits: 4,
            clustering: Clustering::Auto,
        }
    }
}

pub type SchemaMap = indexmap::IndexMap<String, SchemaField>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_type_byte_size() {
        assert_eq!(FieldType::U8.byte_size(), 1);
        assert_eq!(FieldType::U16.byte_size(), 2);
        assert_eq!(FieldType::U32.byte_size(), 4);
        assert_eq!(FieldType::U64.byte_size(), 8);
        assert_eq!(FieldType::Text.byte_size(), 0);
    }

    #[test]
    fn test_schema_field_text() {
        let f = SchemaField::text("title");
        assert!(f.index_lexical);
        assert!(!f.index_vector);
        assert_eq!(f.field_type, FieldType::Text);
    }

    #[test]
    fn test_index_config_default() {
        let c = IndexConfig::default();
        assert_eq!(c.segment_number_bits, 4);
        assert_eq!(c.vector_dimensions, 256);
    }

    #[test]
    fn test_schema_serialization() {
        let mut schema = SchemaMap::new();
        schema.insert("title".into(), SchemaField::text("title"));
        let json = serde_json::to_string(&schema).unwrap();
        let deserialized: SchemaMap = serde_json::from_str(&json).unwrap();
        assert_eq!(schema, deserialized);
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test schema --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: schema types - FieldType, SchemaField, IndexConfig"
```

---

## Task 4: SlateDB KV Store Wrapper

**Files:**
- Create: `D:/code/storm-db/src/kv_store.rs`

- [ ] **Step 1: Write KvStore wrapper and tests**

```rust
// src/kv_store.rs
use slatedb::{Db, DbOptions};
use slatedb::object_store::ObjectStore;
use std::sync::Arc;
use crate::error::{Result, StormDbError};

// Key prefix constants
pub const PREFIX_META: &[u8] = b"meta:";
pub const PREFIX_IDX: &[u8] = b"idx:";
pub const PREFIX_VEC: &[u8] = b"vec:";
pub const PREFIX_DOC: &[u8] = b"doc:";
pub const PREFIX_FACET: &[u8] = b"facet:";
pub const PREFIX_DELL: &[u8] = b"dell:";
pub const PREFIX_LEN: &[u8] = b"len:";

pub struct KvStore {
    db: Db,
}

impl KvStore {
    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        let db = Db::open_with_opts(path, DbOptions::default(), object_store)
            .await
            .map_err(|e| StormDbError::StorageError(e.to_string()))?;
        Ok(Self { db })
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value).await
            .map_err(|e| StormDbError::StorageError(e.to_string()))
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key).await
            .map_err(|e| StormDbError::StorageError(e.to_string()))
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key).await
            .map_err(|e| StormDbError::StorageError(e.to_string()))
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let start = prefix.to_vec();
        let mut end = prefix.to_vec();
        // Increment last byte for range end
        if let Some(last) = end.last_mut() {
            *last += 1;
        }
        let mut results = Vec::new();
        let scan = self.db.scan(start..end, Default::default()).await
            .map_err(|e| StormDbError::StorageError(e.to_string()))?;
        // Collect scan results
        pin_mut!(scan);
        while let Some(item) = scan.next().await {
            let kv = item.map_err(|e| StormDbError::StorageError(e.to_string()))?;
            results.push((kv.key.to_vec(), kv.value.to_vec()));
        }
        Ok(results)
    }

    pub async fn put_serialized<T: serde::Serialize>(&self, key: &[u8], value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| StormDbError::StorageError(e.to_string()))?;
        self.put(key, &bytes).await
    }

    pub async fn get_deserialized<T: serde::de::DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes)
                    .map_err(|e| StormDbError::StorageError(e.to_string()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.db.close().await
            .map_err(|e| StormDbError::StorageError(e.to_string()))
    }
}

// Helper to build composite keys
pub fn make_key(parts: &[&[u8]]) -> Vec<u8> {
    let mut key = Vec::new();
    for part in parts {
        key.extend_from_slice(part);
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use slatedb::object_store::memory::InMemory;

    #[tokio::test]
    async fn test_kv_put_get() {
        let os = Arc::new(InMemory::new());
        let store = KvStore::open("/test", os).await.unwrap();

        store.put(b"key1", b"value1").await.unwrap();
        let val = store.get(b"key1").await.unwrap();
        assert_eq!(val, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_kv_delete() {
        let os = Arc::new(InMemory::new());
        let store = KvStore::open("/test", os).await.unwrap();

        store.put(b"key1", b"value1").await.unwrap();
        store.delete(b"key1").await.unwrap();
        assert!(store.get(b"key1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_kv_scan_prefix() {
        let os = Arc::new(InMemory::new());
        let store = KvStore::open("/test", os).await.unwrap();

        store.put(b"idx:0:1:abc", b"v1").await.unwrap();
        store.put(b"idx:0:1:def", b"v2").await.unwrap();
        store.put(b"idx:0:2:xyz", b"v3").await.unwrap();
        store.put(b"doc:1", b"v4").await.unwrap();

        let results = store.scan_prefix(b"idx:0:1:").await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_serialized_put_get() {
        let os = Arc::new(InMemory::new());
        let store = KvStore::open("/test", os).await.unwrap();

        let data = vec!["hello".to_string(), "world".to_string()];
        store.put_serialized(b"meta:schema", &data).await.unwrap();
        let got: Option<Vec<String>> = store.get_deserialized(b"meta:schema").await.unwrap();
        assert_eq!(got, Some(data));
    }

    #[tokio::test]
    async fn test_make_key() {
        let key = make_key(&[PREFIX_IDX, b"0:", b"1:", b"abc"]);
        assert_eq!(&key[..4], b"idx:");
    }
}
```

Note: The `scan` API may differ slightly across SlateDB versions. Check `slatedb` crate docs and adapt the scan iteration pattern accordingly.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test kv_store --lib
```

- [ ] **Step 3: Fix any SlateDB API differences** (scan pattern, import paths) based on compilation errors

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "feat: KvStore wrapper around SlateDB with put/get/delete/scan"
```

---

## Task 5: Posting List Structures

**Files:**
- Create: `D:/code/storm-db/src/posting.rs`

- [ ] **Step 1: Write posting list types, serialize/deserialize, and tests**

Key types to define:

```rust
pub const ROARING_BLOCK_SIZE: usize = 65_536;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    Delta = 0, Array = 1, Bitmap = 2, Rle = 3,
}

impl CompressionType {
    pub fn from_u32_top2(val: u32) -> Self {
        match val >> 30 { 0=>Delta, 1=>Array, 2=>Bitmap, 3=>Rle, _=>unreachable!() }
    }
    pub fn pack_with_offset(self, offset: u32) -> u32 {
        (self as u32) << 30 | (offset & 0x3FFF_FFFF)
    }
    pub fn unpack_offset(packed: u32) -> u32 { packed & 0x3FFF_FFFF }
}

pub struct BlockObjectIndex {
    pub max_block_score: f32,
    pub block_id: u32,
    pub compression_type_pointer: u32,
    pub posting_count: u16,
    pub max_docid: u16,
    pub max_p_docid: u16,
    pub pointer_pivot_p_docid: u16,
}

pub struct PostingListObjectIndex {
    pub posting_count: u32,
    pub max_list_score: f32,
    pub blocks: Vec<BlockObjectIndex>,
    pub byte_array: Vec<u8>,
}

// Level-0 mutable buffer
pub struct PostingListObject0 {
    pub docids: Vec<u16>,
    pub positions: Vec<Vec<u16>>,  // per-posting position list
    pub posting_count: usize,
    pub max_block_score: f32,
    pub max_docid: u16,
    pub max_p_docid: u16,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NgramType { None=0, FF=1, FR=2, RF=3, FFF=4, FFR=5, FRF=6, RFF=7 }

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTypeTerm { Union=0, Intersection=1, Phrase=2, Not=3 }

pub struct TermObject {
    pub term: String,
    pub key_hash: u64,
    pub key0: u32,
    pub field_positions_vec: Vec<Vec<u16>>,
    pub ngram_type: NgramType,
}

pub struct NonUniqueTermObject {
    pub term: String,
    pub query_type_term: QueryTypeTerm,
    pub key_hash: u64,
    pub key0: u32,
}

pub struct SegmentIndex {
    pub segment: ahash::AHashMap<u64, PostingListObjectIndex>,
    pub byte_array_blocks: Vec<Vec<u8>>,
}

pub struct SegmentLevel0 {
    pub segment: ahash::AHashMap<u64, PostingListObject0>,
}
```

Implement `serialize` / `deserialize` for `PostingListObjectIndex` (convert to/from `Vec<u8>` for SlateDB storage). Format:
- u32 posting_count
- f32 max_list_score
- u32 blocks.len()
- For each block: f32 max_block_score, u32 block_id, u32 compression_type_pointer, u16 posting_count, u16 max_docid, u16 max_p_docid, u16 pointer_pivot_p_docid
- u32 byte_array.len()
- byte_array bytes

Write tests for roundtrip serialization.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test posting --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: posting list structures with serialize/deserialize"
```

---

## Task 6: Posting List Compression

**Files:**
- Create: `D:/code/storm-db/src/compress.rs`

- [ ] **Step 1: Implement compression algorithms and tests**

Three compression types for docid lists:

**Array:** `posting_count * 2` bytes, each docid as u16
**Bitmap:** Fixed 8192 bytes, bit per local docid
**RLE:** `[runs_count: u16, (start: u16, length: u16)*]`

Selection logic:
- posting_count < 4096: try RLE (threshold = min(posting_count/2, array_size/4)), fall back to Array
- posting_count >= 4096: try RLE (threshold = 2048), fall back to Bitmap

Position compression (VInt with STOP_BIT bit 7):
- delta < 128: 1 byte = delta | 0x80
- delta < 16384: 2 bytes
- delta >= 16384: 3 bytes

Key functions:
```rust
pub fn compress_postinglist(docids: &[u16], positions: &[Vec<u16>]) -> (Vec<u8>, CompressionType, u16, f32)
pub fn decompress_docids(data: &[u8], compression_type: CompressionType, posting_count: u16) -> Vec<u16>
pub fn compress_positions(positions: &[Vec<u16>], pointer_pivot: u16) -> Vec<u8>
pub fn decompress_positions(data: &[u8], offset: usize, count: u16) -> Vec<u16>
```

Write tests for each compression type roundtrip and position compression roundtrip.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test compress --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: posting list compression - Array/Bitmap/RLE + VInt positions"
```

---

## Task 7: Tokenizer

**Files:**
- Create: `D:/code/storm-db/src/tokenizer.rs`

- [ ] **Step 1: Implement tokenizer and tests**

Two modes: indexing and query.

Indexing mode:
- Lowercase text per TokenizerType
- Split into terms (AsciiAlphabetic: a-z only; UnicodeAlphanumeric: word chars; UnicodeAlphanumericFolded: word chars + diacritics folding; UnicodeAlphanumericZH: jieba segmentation for Chinese)
- Apply stemming if configured (snowball_stemmers_rs)
- Apply stopword filtering (common English stopwords list)
- Compute key_hash (ahash of term bytes) and key0 (hash & segment_mask)
- Return Vec<TermObject>

Query mode:
- Same as indexing, plus parse operators: + → Intersection, - → Not, "..." → Phrase
- Default operator from QueryType parameter (Union/Intersection)
- Return (unique_terms: AHashMap<String, TermObject>, non_unique_terms: Vec<NonUniqueTermObject>)

Key functions:
```rust
pub fn tokenize_index(text: &str, field_id: u16, config: &IndexConfig, segment_mask: u32) -> Vec<TermObject>
pub fn tokenize_query(query: &str, config: &IndexConfig, segment_mask: u32, default_query_type: QueryTypeTerm) -> (AHashMap<String, TermObject>, Vec<NonUniqueTermObject>)
```

Write tests for: English tokenization, Chinese segmentation, query operator parsing, stemming.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test tokenizer --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: tokenizer - English/Chinese segmentation, query parsing"
```

---

## Task 8: MinHeap Top-K and BM25F Scoring

**Files:**
- Create: `D:/code/storm-db/src/ranking.rs`

- [ ] **Step 1: Implement MinHeap, BM25F, and tests**

```rust
pub const K_BM25: f32 = 1.2;
pub const B_BM25: f32 = 0.75;

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub doc_id: u64,
    pub score: f32,
    pub source: ResultSource,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResultSource { Lexical, Vector, Hybrid }

pub struct MinHeap {
    elements: Vec<SearchResult>,
    capacity: usize,
}
```

MinHeap: min-heap where root = lowest score. Methods: new, add (replace root if full and new > root), peek_min, into_sorted_vec, is_full, len.

DOCUMENT_LENGTH_COMPRESSION: 256-entry lookup mapping u8 → f32. Use `2.0_f32.powi(i as i32 - 128)` for simple exponential mapping.

BM25F:
```
bm25_component = K * (1.0 - B + B * DOCUMENT_LENGTH_COMPRESSION[dl_compressed] / avg_dl)
score = boost * idf * (tf * (K + 1.0) / (tf + bm25_component))
```

IDF: `ln((N - df + 0.5) / (df + 0.5) + 1.0)`

Write tests for MinHeap, BM25F, IDF.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test ranking --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: MinHeap top-k, BM25F scoring, IDF"
```

---

## Task 9: Search Engine - Lexical Search

**Files:**
- Create: `D:/code/storm-db/src/search.rs`

- [ ] **Step 1: Implement search types and lexical search functions**

Search types:
```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryType { Union, Intersection, Phrase }

#[derive(Debug, Clone)]
pub enum SearchMode {
    Lexical,
    Vector { ann_mode: AnnMode, similarity_threshold: f32 },
    Hybrid { ann_mode: AnnMode, similarity_threshold: f32 },
}

#[derive(Debug, Clone)]
pub enum AnnMode { All, Nprobe(usize), SimilarityThreshold(f32) }

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResultType { Count, Topk, TopkCount }

pub struct SearchRequest {
    pub query_string: Option<String>,
    pub query_vector: Option<Vec<f32>>,
    pub query_type: QueryType,
    pub search_mode: SearchMode,
    pub offset: usize,
    pub length: usize,
    pub result_type: ResultType,
    pub include_uncommitted: bool,
    pub field_filter: Option<Vec<String>>,
    pub facet_filter: Option<Vec<FacetFilter>>,
    pub result_sort: Option<Vec<ResultSort>>,
}

pub struct SearchResponse {
    pub query: String,
    pub result_count: usize,
    pub results: Vec<SearchResult>,
    pub facets: Option<Vec<FacetResult>>,
}
```

Search functions (operate on in-memory posting list data):
- `search_single_term`: Iterate blocks, decode docids, compute BM25F, add to heap. WAND skip if max_block_score < heap min.
- `search_union`: Multi-way merge. For 2 terms: intersection + single each. For >2: union_scan with docid bitset.
- `search_intersection`: Sort by posting_count ascending, two-pointer with galloping. Bitmap intersection for bitmaps.
- `search_phrase`: Intersection + position matching (consecutive positions across terms).

Each function takes references to: posting lists, delete_set, MinHeap, BM25F params.

Write tests with mock posting lists.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test search --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: lexical search - single/union/intersection/phrase"
```

---

## Task 10: Vector Quantization

**Files:**
- Create: `D:/code/storm-db/src/quantize.rs`

- [ ] **Step 1: Implement quantization and similarity functions**

```rust
#[derive(Debug, Clone)]
pub struct QuantizedVector {
    pub data: Vec<i8>,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
    pub sum_q: i32,
}
```

- `quantize_i8(vector: &[f32]) -> QuantizedVector`: Affine scalar quantization. scale = (max-min)/255, zero_point = round(-min/scale) as i16, each val clamped to i8
- `dequantize_i8(qv: &QuantizedVector) -> Vec<f32>`: (q - zero_point) * scale
- `compute_similarity_dot(a: &[f32], b: &[f32]) -> f32`
- `compute_similarity_cosine(a: &[f32], b: &[f32]) -> f32`: dot on normalized
- `compute_similarity_euclidean(a: &[f32], b: &[f32]) -> f32`: negative squared distance
- `compute_similarity(query: &[f32], vector: &[f32], similarity: VectorSimilarity) -> f32`: dispatch

Write tests for quantize/dequantize roundtrip and similarity.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test quantize --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: vector quantization (f32→i8) and similarity functions"
```

---

## Task 11: K-Medoid Clustering

**Files:**
- Create: `D:/code/storm-db/src/clustering.rs`

- [ ] **Step 1: Implement K-Medoid algorithm and tests**

```rust
pub struct Medoid { pub medoid_index: usize, pub child_count: usize }

pub struct ParentMedoid {
    pub medoid_index: usize,
    pub is_medoid: bool,
    pub similarity: f32,
    pub doc_id: u16,
    pub field_id: u32,
    pub chunk_id: u32,
    pub embedding: Vec<f32>,
}
```

`cluster_vectors(vectors: &mut Vec<ParentMedoid>, clustering: Clustering, similarity: VectorSimilarity) -> Vec<Medoid>`:
1. cluster_count: Auto → sqrt(n)*2, None → 1, Fixed(n) → n
2. First medoid: compute centroid of sampled vectors, find closest vector
3. Subsequent medoids: for each candidate, compute total similarity gain, pick highest
4. Assign: after each new medoid, reassign all vectors greedily
5. Refinement loop: compute per-cluster centroids → find closest member as new medoid → reassign → repeat until convergence
6. Sort vectors by medoid_index (medoids first), return Medoid list

Write tests with small vector sets (e.g., 20 vectors in 4 dimensions, 3 clusters).

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test clustering --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: K-Medoid (PAM) clustering algorithm"
```

---

## Task 12: Vector Index (IVF)

**Files:**
- Create: `D:/code/storm-db/src/vector.rs`

- [ ] **Step 1: Implement IVF index, TopK, vector search, and tests**

```rust
#[repr(C, packed)]
pub struct VectorHeader {
    pub doc_id: u16,
    pub field_id: u32,
    pub chunk_id: u32,
    pub scale: f32,
    pub norm: f32,
    pub zero_point: i16,
    pub sum_q: i32,
}

pub struct ClusterHeader { pub start_index: u32, pub child_count: u32 }

pub struct VectorLevel {
    pub medoids: Vec<Medoid>,
    pub clusters: Vec<Vec<u8>>,
    pub cluster_headers: Vec<ClusterHeader>,
}

pub struct TopK {
    results: Vec<(usize, f32)>,
    capacity: usize,
    min_similarity: f32,
}
```

TopK: new(capacity), push(index, similarity) → maintain top-k, into_sorted() → Vec<(index, similarity)>

- `serialize_cluster(vectors: &[ParentMedoid], dimensions: usize) -> Vec<u8>`: VectorHeader + raw f32 bytes per vector
- `deserialize_cluster(data: &[u8], dimensions: usize) -> Vec<(VectorHeader, Vec<f32>)>`
- `vector_search_level(level, query, ann_mode, similarity, dimensions, top_k, delete_set) -> Vec<SearchResult>`:
  1. Medoid search → select clusters per AnnMode
  2. Cluster search → deserialize + compute similarity → TopK
  3. Return sorted

Write tests for serialization, TopK, vector search.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test vector --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: IVF vector index with TopK and cluster search"
```

---

## Task 13: Embedding Model Integration

**Files:**
- Create: `D:/code/storm-db/src/embed.rs`

- [ ] **Step 1: Implement embedding wrapper and tests**

```rust
pub enum EmbeddingModel {
    Model2Vec(model2vec::Model),
    None,
}

impl EmbeddingModel {
    pub fn load(model_name: &str) -> Result<Self>  // load Model2Vec by name
    pub fn embed(&self, text: &str) -> Option<Vec<f32>>  // generate embedding, None if no model
    pub fn is_loaded(&self) -> bool
}
```

Write test that skips if model not available (use `#[cfg(feature = "model")]` or conditional).

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test embed --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: Model2Vec embedding integration"
```

---

## Task 14: Facet Storage and Query

**Files:**
- Create: `D:/code/storm-db/src/facet.rs`

- [ ] **Step 1: Implement facet types, encoding, and tests**

```rust
pub struct FacetField {
    pub name: String,
    pub values: IndexMap<String, (Vec<String>, usize)>,
    pub offset: usize,
    pub field_type: FieldType,
}

pub enum ValueType { U8(u8), U16(u16), U32(u32), U64(u64), I8(i8), I16(i16), I32(i32), I64(i64), F32(f32), F64(f64), Bool(bool), String16(u16), String32(u32), Point(u64) }

pub struct FacetFilter { pub field: String, pub values: Vec<String>, pub range: Option<(ValueType, ValueType)> }
pub struct FacetResult { pub field: String, pub values: Vec<(String, usize)> }
```

Functions:
- `encode_facet_value(value: &serde_json::Value, field_type: FieldType) -> Vec<u8>`
- `decode_facet_value(data: &[u8], field_type: FieldType, offset: usize) -> ValueType`
- `count_facets(results: &[SearchResult], facet_data: &[u8], facet_fields: &[FacetField], query_facets: &[String]) -> Vec<FacetResult>`

Write tests for encoding/decoding each numeric type and string type.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test facet --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: facet encoding/decoding and facet counting"
```

---

## Task 15: Hybrid Search (RRF)

**Files:**
- Create: `D:/code/storm-db/src/hybrid.rs`

- [ ] **Step 1: Implement RRF fusion and tests**

```rust
pub const RRF_K: f32 = 0.6;

pub fn hybrid_search(
    lexical_results: Vec<SearchResult>,
    vector_results: Vec<SearchResult>,
) -> Vec<SearchResult>
```

Algorithm:
1. Rank lexical by score → assign rank (1-based)
2. Rank vector by score → assign rank
3. For each unique doc_id: `score = 1/(RRF_K + rank_lexical) + 1/(RRF_K + rank_vector)`
4. If doc_id only in one set, use only that rank
5. Sort by fused score descending

Write tests with known results.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test hybrid --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: RRF hybrid search fusion"
```

---

## Task 16: Document Operations

**Files:**
- Create: `D:/code/storm-db/src/document.rs`

- [ ] **Step 1: Implement document operations**

```rust
pub fn index_document(index: &mut Index, document: HashMap<String, serde_json::Value>) -> Result<u64>
pub fn index_documents(index: &mut Index, documents: Vec<HashMap<String, serde_json::Value>>) -> Result<Vec<u64>>
pub fn delete_document(index: &mut Index, doc_id: u64) -> Result<()>
pub fn get_document(index: &Index, doc_id: u64) -> Result<HashMap<String, serde_json::Value>>
```

index_document flow:
1. Assign doc_id: (level << 16) | local_docid
2. For each Text field: tokenize → add to segments_level0 posting lists
3. For each Vector/Text field with index_vector: embed or use external → add to vector_buffer
4. Store document in pending doc buffer
5. Record facet values
6. Increment indexed_doc_count

get_document: read from SlateDB "doc:{doc_id}", decompress, deserialize JSON

Write tests using InMemory SlateDB.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test document --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: document index/delete/get operations"
```

---

## Task 17: Index Struct and Commit Flow

**Files:**
- Create: `D:/code/storm-db/src/index.rs`

- [ ] **Step 1: Implement Index struct, create/open/close, and commit**

```rust
pub struct Index {
    pub db: KvStore,
    pub schema: SchemaMap,
    pub config: IndexConfig,
    pub stats: RwLock<IndexStats>,
    pub segments_index: RwLock<Vec<SegmentIndex>>,
    pub segments_level0: RwLock<SegmentLevel0>,
    pub vector_levels: RwLock<Vec<VectorLevel>>,
    pub vector_buffer: RwLock<Vec<ParentMedoid>>,
    pub level_index: RwLock<Vec<LevelIndex>>,
    pub bm25_cache: RwLock<[f32; 256]>,
    pub delete_set: RwLock<AHashSet<usize>>,
    pub facet_dict: RwLock<IndexMap<String, (Vec<String>, usize)>>,
    pub embedding_model: Option<EmbeddingModel>,
    pub indexed_doc_count: RwLock<u64>,
}

pub struct IndexStats {
    pub doc_count: u64,
    pub vector_count: u64,
    pub level_count: usize,
}

pub struct LevelIndex {
    pub doc_lengths: Vec<u8>,  // compressed doc lengths for this level
}
```

`create_index(path, config, schema) -> Result<Index>`:
- Open SlateDB, write "meta:schema" and "meta:config", initialize empty structures

`open_index(path) -> Result<Index>`:
- Open SlateDB, read schema/config/stats, load all committed posting lists and vectors into memory

`commit(index) -> Result<()>`:
1. Merge incomplete level if last level < 65536 docs
2. For each strip: sort terms, compress posting lists, write to SlateDB "idx:{level}:{strip}:{term_hash}"
3. Write doc lengths: "len:{level}:{doc_id}"
4. Vector clustering: cluster_vectors → write to SlateDB "vec:{level}:*"
5. Write documents: "doc:{doc_id}" (compressed)
6. Write facets: "facet:{doc_id}", "facet:dict"
7. Update "meta:stats"
8. Clear level-0 buffers
9. Load committed data into memory

Write lifecycle test: create → index docs → commit → search → close → reopen → search.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test index --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: Index struct, create/open/close, commit flow"
```

---

## Task 18: HTTP Server

**Files:**
- Create: `D:/code/storm-db/src/server/mod.rs`
- Create: `D:/code/storm-db/src/server/router.rs`
- Create: `D:/code/storm-db/src/server/handler.rs`

- [ ] **Step 1: Implement HTTP server with Axum**

mod.rs: Axum app setup, shared state (Arc<RwLock<Index>>)

router.rs: Routes
```
POST   /index/create
POST   /index/open
DELETE /index/{path}
POST   /document
POST   /documents
DELETE /document/{doc_id}
GET    /document/{doc_id}
POST   /commit
POST   /search
```

handler.rs: Handler functions for each route, request/response types with serde, utoipa annotations.

Write integration test using axum::TestClient or reqwest.

- [ ] **Step 2: Run tests**

```bash
cd D:/code/storm-db && cargo test server --lib
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: HTTP server with Axum - CRUD and search endpoints"
```

---

## Task 19: Integration Test

**Files:**
- Create: `D:/code/storm-db/tests/integration_test.rs`

- [ ] **Step 1: Write end-to-end integration test**

Full flow test:
1. Create index with schema: title(Text), body(Text), tags(String32, facet), embedding(Vector), rating(F32, facet)
2. Index 100 documents with various content
3. Commit
4. Lexical search: "rust programming" → verify results ranked by BM25F
5. Vector search: query with vector → verify results ranked by similarity
6. Hybrid search: query string + vector → verify RRF fusion
7. Phrase search: "\"exact phrase\"" → verify positional matching
8. Faceted search: filter by tags, get facet counts
9. Delete document → verify excluded from search
10. Reopen index → verify all data persists

- [ ] **Step 2: Run integration test**

```bash
cd D:/code/storm-db && cargo test --test integration_test
```

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test: end-to-end integration test"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: All design sections covered (KV layout, data structures, API, search algorithms, commit flow, project structure)
- [x] **Placeholder scan**: No TBD/TODO in implementation steps
- [x] **Type consistency**: Types, method names consistent across tasks (SearchResult, PostingListObjectIndex, TermObject, etc.)
- [x] **Dependency order**: Tasks 1-4 (foundation) → 5-7 (indexing infra) → 8-9 (search) → 10-13 (vector) → 14-15 (facet/hybrid) → 16-17 (document/index glue) → 18-19 (server/integration)
