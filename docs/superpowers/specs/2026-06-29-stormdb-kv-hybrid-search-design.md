# StormDB Design Spec

A hybrid search engine (lexical + vector + RRF) built on SlateDB as the unified storage layer, with feature parity to SeekStorm's core search capabilities.

## 1. Architecture Overview

**Approach**: SlateDB as unified storage (方案A). All data — inverted index, vector index, documents, facets, metadata — stored in a single SlateDB instance via key-prefix partitioning. No shards. Query-time data structures (posting list HashMaps, IVF clusters) loaded into memory from SlateDB on open/commit.

**Why SlateDB**: LSM-tree with object storage backend (S3/GCS/local_fs), ACID transactions, Rust-native. Replaces SeekStorm's mmap file system with a cloud-native KV store while preserving the same in-memory query performance model.

## 2. KV Key Layout

```
Prefix     Key Format                                Value
─────────────────────────────────────────────────────────────────────
meta:      "meta:schema"                             Schema JSON
           "meta:config"                             IndexConfig JSON
           "meta:stats"                              Stats (doc_count, vector_count)

idx:       "idx:{level}:{strip}:{term_hash}"         PostingListBlock (binary)
           - key_head + compressed docid list + position data

vec:       "vec:{level}:medoids"                     Medoid list (binary)
           "vec:{level}:cluster:{id}"                ClusterHeader + vectors (binary)
           "vec:{level}:headers"                     Cluster count + child_counts (binary)

doc:       "doc:{doc_id}"                            Compressed document JSON (Snappy/Lz4/Zstd)

facet:     "facet:{doc_id}"                          Fixed-row facet values (binary)
           "facet:dict"                              String dictionary JSON

dell:      "dell:{doc_id}"                           Deletion marker (empty value = deleted)

len:       "len:{level}:{doc_id}"                    Compressed document length (BM25)
```

- `level`: Each commit of up to 65,536 documents creates a new level. Global doc_id = `(level << 16) | local_docid`.
- `strip`: Term hash partition (segment_number1 partitions), same as SeekStorm.
- `term_hash`: u64 hash of the indexed term.

## 3. Core Data Structures

### Index (top-level)

```rust
struct Index {
    db: Db,                                        // SlateDB instance
    schema: SchemaMap,                             // field_name -> SchemaField
    config: IndexConfig,                           // index configuration
    stats: RwLock<IndexStats>,                     // doc_count, vector_count, etc.

    // Lexical index - in-memory query acceleration
    segments_index: RwLock<Vec<SegmentIndex>>,     // committed posting list HashMaps per level
    segments_level0: RwLock<SegmentLevel0>,        // uncommitted level-0 write buffer

    // Vector index
    vector_levels: RwLock<Vec<VectorLevel>>,       // committed vector data per level
    vector_buffer: RwLock<Vec<ParentMedoid>>,      // uncommitted vector buffer

    // BM25 statistics
    level_index: RwLock<Vec<LevelIndex>>,          // per-level doc length arrays
    bm25_cache: RwLock<Bm25ComponentCache>,

    // Deletion
    delete_set: RwLock<AHashSet<usize>>,

    // Facets
    facet_dict: RwLock<IndexMap<String, (Vec<String>, usize)>>,

    // Embedding model
    embedding_model: Option<EmbeddingModel>,
}
```

### SchemaField

```rust
struct SchemaField {
    field: String,
    store: bool,            // store original text in document store
    index_lexical: bool,    // index in inverted index
    index_vector: bool,     // generate/index vector embeddings
    field_type: FieldType,  // U8/U16/U32/U64/I8/I16/I32/I64/Timestamp/F32/F64/Bool/String16/String32/Text/Point/Vector
    facet: bool,            // enable faceting
    boost: f32,             // BM25 field weight
}
```

### SegmentIndex / SegmentLevel0

```rust
struct SegmentIndex {
    segment: AHashMap<u64, PostingListObjectIndex>,  // key_hash -> posting list
    byte_array_blocks: Vec<Vec<u8>>,                  // compressed block data (loaded from SlateDB)
}

struct SegmentLevel0 {
    segment: AHashMap<u64, PostingListObject0>,      // key_hash -> level-0 buffer
    postings_buffer: Vec<u8>,                         // positions RAM buffer
}
```

### PostingListBlock Value Encoding (reuses SeekStorm's format)

- **key_head**: key_hash(u64) + posting_count(u16) + max_docid(u16) + max_p_docid(u16) + ngram_counts(0-3B) + pointer_pivot(u16) + compression_type_pointer(u32)
- **docid compressed data**: Array / Bitmap / RLE
- **position compressed data**: delta-encoded variable-length integers

### CompressionType

```rust
enum CompressionType {
    Delta = 0,    // bit-packed delta (reserved, currently disabled)
    Array = 1,    // sorted u16 array: posting_count * 2 bytes
    Bitmap = 2,   // 8192-byte (65536-bit) roaring bitmap
    Rle = 3,      // u16 runs_count, then (u16 start, u16 length) pairs
}
```

Compression decision: posting_count < 4096 → try RLE then Array; posting_count >= 4096 → try RLE then Bitmap.

### Vector Structures

```rust
#[repr(C, packed)]
struct VectorHeader {
    doc_id: u16,
    field_id: u32,
    chunk_id: u32,
    scale: f32,
    norm: f32,
    zero_point: i16,
    sum_q: i32,
}

struct ParentMedoid {
    medoid_index: usize,
    is_medoid: bool,
    similarity: f32,
    doc_id: u16,
    field_id: u32,
    chunk_id: u32,
    embedding: Embedding,
    scale: f32, norm: f32, zero_point: i16, sum_q: i32,
}

struct ClusterHeader {
    start_index: u32,
    child_count: u32,
}
```

## 4. API Design

### Library API

```rust
// Index management
fn create_index(path: &str, config: IndexConfig, schema: Vec<SchemaField>) -> Result<Index>;
fn open_index(path: &str) -> Result<Index>;
fn close_index(&mut Index) -> Result<()>;
fn delete_index(path: &str) -> Result<()>;

// Document operations
fn index_document(&mut Index, document: HashMap<String, Value>) -> Result<u64>;
fn index_documents(&mut Index, documents: Vec<HashMap<String, Value>>) -> Result<Vec<u64>>;
fn delete_document(&mut Index, doc_id: u64) -> Result<()>;
fn delete_documents_by_query(&mut Index, query: &str, query_type: QueryType) -> Result<usize>;
fn get_document(&Index, doc_id: u64) -> Result<HashMap<String, Value>>;
fn commit(&mut Index) -> Result<()>;

// Search
fn search(&Index, query: SearchRequest) -> Result<SearchResponse>;

// Iterator
fn get_iterator(&Index, skip: usize, take: usize) -> Result<Vec<HashMap<String, Value>>>;
```

### Search Types

```rust
struct SearchRequest {
    query_string: Option<String>,
    query_vector: Option<Vec<f32>>,
    query_type: QueryType,          // Union / Intersection / Phrase
    search_mode: SearchMode,        // Lexical / Vector / Hybrid
    offset: usize,
    length: usize,                  // top-k
    result_type: ResultType,        // Count / Topk / TopkCount
    include_uncommitted: bool,
    field_filter: Option<Vec<String>>,
    facet_filter: Option<Vec<FacetFilter>>,
    result_sort: Option<Vec<ResultSort>>,
}

struct SearchResponse {
    query: String,
    result_count: usize,
    results: Vec<SearchResult>,
    facets: Option<Vec<FacetResult>>,
}

struct SearchResult {
    doc_id: u64,
    score: f32,
    document: Option<HashMap<String, Value>>,
    source: ResultSource,           // Lexical / Vector / Hybrid
}
```

### HTTP API

```
POST   /index/create          Create index
POST   /index/open            Open index
DELETE /index/{name}          Delete index

POST   /document              Index single document
POST   /documents             Batch index documents
DELETE /document/{doc_id}     Delete document
GET    /document/{doc_id}     Get document
POST   /commit                Commit

POST   /search                Search
```

## 5. Search Algorithm Flow

### Lexical Search (BM25F)

1. **Query parsing**: query_string → tokenizer → unique_terms[] + non_unique_terms[]. Supports +, -, "" operators.
2. **Posting list resolution**: For each term, compute key_hash → locate strip → find PostingListObject in segments_level0 and segments_index → decode compressed data (Array/Bitmap/RLE) to get docid list.
3. **Query execution**:
   - Single term: direct iteration
   - Union (OR): multi-way merge, union
   - Intersection (AND): multi-way merge, intersection
   - Phrase: intersection + positional matching (delta-decoded positions)
4. **BM25F scoring**: `score = Σ_f weight_f * IDF * (tf_f * (K+1)) / (tf_f + K*(1-B+B*dl_f/avgdl_f))`. IDF = `ln((N - df + 0.5) / (df + 0.5) + 1)`. K=1.2, B=0.75.
5. **Top-K collection**: MinHeap with max_block_score WAND-style block skipping.
6. **Filtering**: delete_set exclusion, facet_filter exclusion.

### Vector Search (IVF + K-Medoid)

1. **Query vector preparation**: normalize (cosine) or use raw; quantize (scalar i8) if index uses quantization.
2. **Medoid search**: iterate all cluster medoids, compute similarity with query, select top-nprobe clusters per ANN mode (All/Nprobe(n)/SimilarityThreshold(t)).
3. **Cluster search**: for each selected cluster, iterate all vectors, compute similarity, maintain TopK.
4. **Uncommitted search**: scan vector_buffer.
5. **Result sort**: TopK by score descending → SearchResult.

### Hybrid Search (RRF)

1. Execute lexical and vector search in parallel.
2. RRF fusion: `score = 1/(0.6 + rank_lexical) + 1/(0.6 + rank_vector)`.
3. Sort by fusion score, take top-k.
4. Facet statistics from lexical search only.

### ANN Modes

```rust
enum AnnMode {
    All,                      // exhaustive search all clusters
    Nprobe(usize),            // top-n clusters by medoid similarity
    SimilarityThreshold(f32), // clusters above similarity threshold
}
```

### Vector Similarity

```rust
enum VectorSimilarity {
    Dot,        // dot product
    Cosine,     // dot product on normalized vectors
    Euclidean,  // negative squared Euclidean distance
}
```

## 6. Commit Flow

1. **Merge incomplete level**: if previous level had < 65,536 docs, read its posting lists from SlateDB, decode back to level-0 format, merge with current level-0.
2. **For each strip**: sort level-0 terms by key_hash, compress each term's posting list (Array/Bitmap/RLE), write to SlateDB: `idx:{level}:{strip}:{term_hash}`.
3. **Write document lengths**: `len:{level}:{doc_id}`.
4. **Vector clustering**: K-Medoid clustering, write to SlateDB: `vec:{level}:medoids`, `vec:{level}:cluster:{id}`, `vec:{level}:headers`.
5. **Write documents**: `doc:{doc_id}` (compressed).
6. **Write facets**: `facet:{doc_id}`.
7. **Update meta**: `meta:stats`.
8. **Clear level-0 buffers**.

## 7. K-Medoid Clustering Algorithm

1. **Cluster count**: `sqrt(vector_count) * 2` (auto), or user-specified.
2. **First medoid selection**: compute centroid of all vectors (sampled), find closest vector.
3. **Subsequent medoid selection**: for each candidate (sampled), compute total similarity gain, pick highest.
4. **Assignment**: after each new medoid, greedily reassign all vectors to best medoid.
5. **Refinement loop**: compute per-cluster centroids → find closest vector as new medoid → reassign → repeat until convergence.

## 8. Project Structure

```
storm-db/
├── Cargo.toml
├── src/
│   ├── lib.rs                  // library entry, public API
│   ├── index.rs                // Index struct, create/open/close/commit
│   ├── schema.rs               // SchemaField, FieldType, SchemaMap
│   ├── document.rs             // document index/delete/update/get
│   ├── search.rs               // search main flow, query parsing, result assembly
│   ├── ranking.rs              // BM25F scoring, MinHeap top-k
│   ├── posting.rs              // PostingList structures, encode/decode
│   ├── compress.rs             // posting list compression (Array/Bitmap/RLE)
│   ├── tokenizer.rs            // tokenizer (English + jieba Chinese)
│   ├── vector.rs               // vector search, IVF index, TopK
│   ├── clustering.rs           // K-Medoid clustering algorithm
│   ├── quantize.rs             // vector quantization (ScalarQuant I8)
│   ├── embed.rs                // Model2Vec embedding inference
│   ├── facet.rs                // facet storage and query
│   ├── hybrid.rs               // RRF hybrid fusion
│   ├── kv_store.rs             // SlateDB wrapper (KV read/write helpers)
│   ├── error.rs                // unified error type
│   └── server/
│       ├── mod.rs              // HTTP server entry
│       ├── router.rs           // route definitions
│       └── handler.rs          // request handlers
```

## 9. Dependencies

```toml
[dependencies]
slatedb = "..."                  # KV storage
object_store = "..."             # SlateDB backend (local_fs / s3)
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
ahash = "0.8"
indexmap = "2"
smallvec = "1"
rayon = "1"
itertools = "0.13"
jieba-rs = "0.7"                 # Chinese segmentation
snowball-stemmers-rs = "0.2"     # English stemming
model2vec-rs = "..."             # embedding inference
snap = "1"                       # Snappy compression
lz4_flex = "0.11"                # LZ4 compression
zstd = "0.13"                    # Zstd compression
axum = "0.7"                     # HTTP framework
utoipa = { version = "4", features = ["axum_extras"] }  # OpenAPI
thiserror = "2"                  # error derivation
```

## 10. Error Handling

```rust
#[derive(Debug, thiserror::Error)]
enum StormDbError {
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
```

## 11. Key Differences from SeekStorm

| Aspect | SeekStorm | StormDB |
|--------|-----------|---------|
| Storage | mmap files (index.bin, docstore.bin, facet.bin, vector.bin) | SlateDB unified KV |
| Sharding | Multi-shard parallel | No sharding |
| Persistence | File system write + mmap | SlateDB put/get/scan |
| Index open | mmap files, decode on demand | Load from SlateDB into memory |
| Cloud-native | No (local filesystem only) | Yes (SlateDB + object_store) |
| ACID | No (manual commit) | Yes (SlateDB transactions) |
| Real-time search | Yes (level-0 buffer) | Yes (same model) |
| Chinese support | Feature-gated | Built-in (jieba-rs) |
| Embedding | Model2Vec (optional) | Model2Vec (optional) |
