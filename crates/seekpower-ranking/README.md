# seekpower-ranking

Scoring primitives and result-aggregation containers for SeekPower. This crate
sits between the posting list decoder and the top-level query engine: given
per-field term frequencies, document lengths, and a corpus IDF, it produces a
BM25F score; given a stream of scored hits, it keeps the top-k via a min-heap.

The BM25F formula is defined in design spec §4.3. This crate implements the
per-field scoring kernel — higher layers are responsible for summing across
fields and supplying the corpus-level statistics (`avgdl`, `N`).

## Document-length compression

Document lengths are stored as a single byte in log2 space. A 256-entry
lookup table maps `u8 -> f32` so decompression is a constant-time table
lookup and compression is a `log2` plus a bias.

```rust
pub fn document_length_compression() -> &'static [f32; 256];
pub fn compress_doc_length(length: f32) -> u8;
pub fn decompress_doc_length(compressed: u8) -> f32;
```

`document_length_compression()` returns a `&'static [f32; 256]` where
`table[i] = 2^(i - 128)`. Index `128` decodes to `1.0` (the neutral value).
`compress_doc_length` clamps the log2-domain result into `0..=255`; values
`<= 0.0` round to `0`. Round-trip error on the included test cases is under
~2%.

## IDF

```rust
pub fn compute_idf(total_docs: u64, doc_freq: u32) -> f32;
```

Robertson–Sparck Jones IDF:

```
IDF = ln((N - df + 0.5) / (df + 0.5) + 1)
```

The `+ 1` keeps the value strictly positive for very common terms.

## BM25F

```rust
pub struct Bm25fScorer {
    pub k: f32,
    pub b: f32,
}

impl Bm25fScorer {
    pub fn new() -> Self;                              // k = 1.2, b = 0.75
    pub fn score_field(
        &self, tf: u32, dl: f32, avgdl: f32,
        boost: f32, idf: f32,
    ) -> f32;
    pub fn score_multi_field(
        &self, fields: &[FieldScoreInput], idf: f32,
    ) -> f32;
}

pub struct FieldScoreInput {
    pub tf: u32,
    pub dl: f32,
    pub avgdl: f32,
    pub boost: f32,
}
```

`Bm25fScorer::new()` returns the spec defaults (`k = 1.2`, `b = 0.75`). The
`Default` impl delegates to `new()`. The per-field formula is:

```
score_field = boost * idf * (tf * (k + 1)) / (tf + k * (1 - b + b * dl / avgdl))
```

`score_multi_field` is a straight `sum` of `score_field` over the supplied
fields, with the caller passing a single `idf` value (the same term IDF
applies to every field contribution).

## Top-k: `MinHeap`

```rust
pub struct MinHeap { /* heap: Vec<SearchResult>, capacity: usize */ }

impl MinHeap {
    pub fn new(capacity: usize) -> Self;          // capacity clamped to >= 1
    pub fn push(&mut self, result: SearchResult); // drops lowest score when full
    pub fn is_full(&self) -> bool;
    pub fn min_score(&self) -> f32;               // f32::MAX when empty
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn into_sorted_vec(self) -> Vec<SearchResult>; // descending by score
}
```

`MinHeap` always retains the highest-scoring `capacity` items seen so far.
When full, `push` compares the new item against `min_score()` and discards
the new item if it cannot beat the current minimum. `into_sorted_vec` consumes
the heap and returns results sorted by score descending, which is what the
search API hands back to callers.

`SearchResult` and `ResultType`:

```rust
pub struct SearchResult {
    pub doc_id: u64,
    pub score: f32,
    pub result_type: ResultType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultType { Lexical, Vector, Hybrid }
```

`ResultType` tags the origin of the result so hybrid fusion (§6) can tell
where each hit came from.

## WAND integration

`MinHeap::min_score()` is the WAND gate. A block whose `max_block_score` (see
`seekpower-posting`) is `<=` the heap's current minimum can be skipped without
decoding — spec §4.5. This crate owns the heap; `seekpower-posting` owns the
per-block upper bounds; the query engine brings them together.

## Tests

Unit tests in `src/lib.rs` cover: IDF for typical and saturated cases,
document-length compress/decompress round-trip and the `128 -> 1.0` neutral
case, BM25F for higher-tf / longer-doc / zero-tf, and `MinHeap` insertion
order, capacity enforcement, and `into_sorted_vec` ordering.

## See also

- Design spec: `docs/superpowers/specs/2026-06-30-hybrid-search-design.md`
  (§4.3, §4.5)
- Source: `crates/seekpower-ranking/src/lib.rs`
