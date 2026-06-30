# seekpower-search

Search execution engine and hybrid fusion. The orchestration layer that turns
a `SearchRequest` into a ranked `SearchResponse`. Lexical scoring lives in
`seekpower-ranking`; this crate is the glue that walks posting lists, runs
BM25F through a `MinHeap`, and (for hybrid queries) fuses lexical and vector
results with an injectable `FusionStrategy`.

## Public types

### `SearchRequest`

```rust
pub struct SearchRequest {
    pub query_string: Option<String>,
    pub query_vector: Option<Vec<f32>>,
    pub search_mode: SearchMode,
    pub query_type: QueryType,
    pub ann_mode: seekpower_vector::AnnMode,
    pub offset: usize,
    pub length: usize,
    pub field_filter: Option<FieldFilter>,
}
```

`Default` produces a pure lexical union search: `query_string = None`,
`search_mode = Lexical`, `query_type = Union`, `ann_mode = AnnMode::All`,
`offset = 0`, `length = 10`, `field_filter = None`.

### `SearchMode`

```rust
pub enum SearchMode {
    Lexical,
    Vector { ann_mode: seekpower_vector::AnnMode },
    Hybrid { ann_mode: seekpower_vector::AnnMode, w_lexical: f32, w_vector: f32 },
}
```

### `QueryType`

```rust
pub enum QueryType {
    Union,
    Intersection,
    Phrase,
}
```

### `FieldFilter`

```rust
pub struct FieldFilter {
    pub field: String,
    pub condition: FilterCondition,
}

pub enum FilterCondition {
    Equals(serde_json::Value),
    Range { min: Option<serde_json::Value>, max: Option<serde_json::Value> },
    In(Vec<serde_json::Value>),
}
```

### `SearchResponse`

```rust
pub struct SearchResponse {
    pub query: String,
    pub result_count: usize,
    pub results: Vec<seekpower_ranking::SearchResult>,
}
```

## `FusionStrategy`

Pluggable combiner; required to be `Send + Sync` so it can sit behind an
`Arc<dyn FusionStrategy>` if needed.

```rust
pub trait FusionStrategy: Send + Sync {
    fn fuse(
        &self,
        lexical_results: Vec<seekpower_ranking::SearchResult>,
        vector_results: Vec<seekpower_ranking::SearchResult>,
    ) -> Vec<seekpower_ranking::SearchResult>;
}
```

### `RrfFusion`

Weighted Reciprocal Rank Fusion. The score for a document is the sum of its
per-list contributions:

```text
score = w_lexical / (k + rank_lexical) + w_vector / (k + rank_vector)
```

where rank is 1-based (the implementation uses `rank as f32 + 1.0` since
`enumerate()` is 0-based). Default constants are
`k = 60`, `w_lexical = 1.0`, `w_vector = 1.0`. All three are public fields, so
`RrfFusion { k: 60, w_lexical: 2.0, w_vector: 1.0 }` is a valid override.

```rust
pub struct RrfFusion {
    pub k: u32,
    pub w_lexical: f32,
    pub w_vector: f32,
}

impl RrfFusion {
    pub fn new() -> Self;   // k=60, w_lexical=1.0, w_vector=1.0
}
```

`fuse` accumulates scores in a `HashMap<doc_id, f32>`, then emits one
`SearchResult` per doc with `result_type = Hybrid`, sorted by score
descending. Documents that appear in only one of the two inputs still
contribute their single-list score.

## `QueryEngine`

```rust
pub struct QueryEngine {
    pub fusion: Box<dyn FusionStrategy>,
}

impl QueryEngine {
    pub fn new() -> Self;                              // RrfFusion::new()
    pub fn with_fusion(self, fusion: Box<dyn FusionStrategy>) -> Self;
    pub fn search_lexical(...) -> Vec<SearchResult>;
    pub fn search_vector<V: VectorIndex>(...) -> Vec<SearchResult>;
    pub async fn search_hybrid(...) -> Vec<SearchResult>;
}
```

### Lexical search

```rust
pub fn search_lexical(
    &self,
    posting_refs: &[&seekpower_posting::PostingListBlock],
    delete_set: &HashSet<u64>,
    query_type: QueryType,
    avg_doc_length: f32,
    boosts: &[f32],
    idfs: &[f32],
    k: usize,
) -> Vec<seekpower_ranking::SearchResult>
```

Each block is decoded via `PostingCodec::decode`, then scored with
`Bm25fScorer::score_field` (term frequency = `positions.len()`,
`dl = avg_doc_length`, IDF from `idfs[idx]`, boost from `boosts[idx]`).
A `seekpower_ranking::MinHeap` of capacity `k` keeps the running top-k; the
result is returned as `into_sorted_vec()`.

| `QueryType` | Behavior |
|---|---|
| `Union` | Score every entry from every block; drop `delete_set` docs. |
| `Intersection` | Decode each block's doc_id set, intersect, then sum the per-block BM25F contributions for the surviving candidates. |
| `Phrase` | Delegates to `search_lexical` with `QueryType::Intersection` (position verification is intended per spec §4.4 — see below). |

### Vector search

```rust
pub fn search_vector<V: seekpower_vector::VectorIndex>(
    &self,
    vector_index: &V,
    query: &[f32],
    k: usize,
    ann_mode: &seekpower_vector::AnnMode,
    delete_set: &HashSet<u64>,
) -> Vec<seekpower_ranking::SearchResult>
```

Pass-through to `VectorIndex::search`, re-tagged with `result_type = Vector`.

### Hybrid search

```rust
pub async fn search_hybrid(
    &self,
    lexical_results: Vec<seekpower_ranking::SearchResult>,
    vector_results: Vec<seekpower_ranking::SearchResult>,
) -> Vec<seekpower_ranking::SearchResult>
```

Delegates to the configured `FusionStrategy` (default `RrfFusion`).

## Phrase search — position verification (spec §4.4)

Phrase semantics are intentionally narrow at this layer: `QueryType::Phrase`
falls back to `Intersection`. The full spec requires that, for each
intersection candidate, the per-term position lists be checked for
consecutive positions (`term_0 at pos`, `term_1 at pos+1`, ..., `term_n at
pos+n`) before the document is admitted. That verification lives in the
caller of `search_lexical` (typically `seekpower-core`); this crate only
provides the candidate set.

## Field filter

The crate does not yet evaluate `FieldFilter` itself — `SearchRequest` carries
it but the lexical and vector paths above do not consume it. The intended
post-fusion contract is: drop docs whose stored field value fails the
condition, then take top-k. Filter application lives one layer up
(`seekpower-core`), where the document store is reachable.

## Offset / length

`SearchRequest.offset` and `length` are pagination over the final fused list
after field filtering. The lexical path's `k` is the per-call top-k that feeds
fusion, not the user's page size.

## Verification

`D:\code\seekpower\crates\seekpower-search\src\lib.rs` has the canonical
tests:

- `SearchRequest::default` — defaults to lexical union, `offset = 0`,
  `length = 10`, `AnnMode::All`, no filter.
- `RrfFusion::new` basic fusion — doc `2` (present in both lists) tops a
  three-doc fused output with `ResultType::Hybrid`.
- `RrfFusion` weights — with `w_lexical = 2.0`, `w_vector = 1.0`,
  `k = 60`, the lone overlapping doc scores `2.0/61.0 + 1.0/61.0`.
- `QueryEngine::search_lexical` — encodes two `PostingEntry`s and confirms
  BM25F returns positive scores.
- `SearchResponse` construction — verifies `query`, `result_count`, and the
  `results` vec round-trip.