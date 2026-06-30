# seekpower-schema

Defines the static data model for a SeekPower index: how a field is described,
what types are allowed, what an index is configured with, and what stats are
tracked. This crate is `Serialize`/`Deserialize` end to end so the schema and
config can round-trip through JSON for persistence and IPC.

The whole module lives in `crates/seekpower-schema/src/lib.rs`.

## SchemaField

A `SchemaField` describes one named field in a document.

```rust
pub struct SchemaField {
    pub field: String,
    pub store: bool,
    pub index_lexical: bool,
    pub index_vector: bool,
    pub field_type: FieldType,
    pub boost: f32,
}
```

| Field | Meaning |
| --- | --- |
| `field` | Field name. |
| `store` | If `true`, the original value is persisted for retrieval. |
| `index_lexical` | If `true`, the value is tokenized and indexed for text search. |
| `index_vector` | If `true`, the value is indexed as a dense vector. |
| `field_type` | The on-disk and in-memory type of the value. |
| `boost` | Per-field score multiplier applied at query time. |

### Constructors and builders

```rust
let title   = SchemaField::text("title");        // store=true, index_lexical=true
let embed   = SchemaField::vector("embedding");   // index_vector=true

let boosted = SchemaField::text("title").with_boost(2.5);
let stored  = SchemaField::vector("embedding").with_store(true);
```

`text(name)` produces a field with `store=true`, `index_lexical=true`,
`index_vector=false`, `field_type=Text`, `boost=1.0`.

`vector(name)` produces a field with `store=false`, `index_lexical=false`,
`index_vector=true`, `field_type=Vector`, `boost=1.0`.

`with_boost(f32)` and `with_store(bool)` return `self` and are chainable.

## FieldType

The set of types a `SchemaField` can carry. There are 17 variants:

| Category | Variants |
| --- | --- |
| Unsigned integers | `U8`, `U16`, `U32`, `U64` |
| Signed integers | `I8`, `I16`, `I32`, `I64` |
| Floats | `F32`, `F64` |
| Other scalars | `Bool`, `Timestamp`, `Text` |
| Strings | `String16`, `String32` |
| String sets | `StringSet16`, `StringSet32` |
| Vectors | `Vector` |

The numeric variants set the on-disk width; the `16` / `32` suffix on the
string and string-set variants sets the maximum byte length of the
short-string optimization.

## IndexConfig

Top-level configuration for a single index. `IndexConfig::default()` returns
the values below.

| Field | Default | Notes |
| --- | --- | --- |
| `tokenizer` | `TokenizerType::Unicode` | Tokenizer to apply to textual fields. |
| `stemmer_language` | `None` | Optional stemmer language tag. |
| `stop_words` | `None` | Optional list of stop words. |
| `lowercase` | `true` | Lowercase terms during tokenization. |
| `vector_dimensions` | `768` | Dimensionality of indexed vectors. |
| `vector_similarity` | `Similarity::Cosine` | Distance function for vector search. |
| `clustering` | `ClusteringConfig::Auto` | IVF cluster count strategy. |
| `quantization` | `QuantType::SqI8` | Vector quantization mode. |
| `doc_compression` | `DocCompression::Snappy` | Codec for stored documents. |
| `posting_cache_size_mb` | `256` | Posting-list block cache budget. |
| `cluster_cache_size_mb` | `512` | IVF cluster cache budget. |

## IndexStats

Counters persisted alongside the index and updated as documents are
ingested.

```rust
pub struct IndexStats {
    pub doc_count: u64,
    pub vector_count: u64,
    pub total_doc_length: u64,
    pub avg_doc_length: f32,
}
```

`IndexStats::default()` zero-initializes all four fields. `avg_doc_length`
is the mean of per-document token counts across the index.

## SchemaMap

```rust
pub type SchemaMap = ahash::AHashMap<String, SchemaField>;
```

The schema is a flat map from field name to its `SchemaField`. `ahash` is
used to keep hashing cheap on the hot ingest path.

## Enums

A short summary of the configuration enums; the full variant lists live in
the source.

- `TokenizerType` — `Simple`, `Unicode`, `Jieba`, or `Custom(String)` for
  user-supplied tokenizers.
- `Similarity` — `Dot`, `Cosine`, `Euclidean`. Distance function used by
  the vector index.
- `ClusteringConfig` — `Auto` (let the engine pick), `Fixed(usize)`
  (explicit cluster count), or `None` (disable IVF clustering).
- `QuantType` — `None` (full precision) or `SqI8` (scalar 8-bit
  quantization, the default).
- `DocCompression` — `None`, `Snappy`, `Lz4`, or `Zstd` codec for stored
  document payloads.

## Source of truth

Everything in this crate is defined in one file:

- `crates/seekpower-schema/src/lib.rs`
