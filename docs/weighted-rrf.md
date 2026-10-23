# Configurable weighted reciprocal rank fusion

[文档索引](README.md) · [当前功能与限制](../FEATURES.md) · [开发与验证](development.md)


Existing `SearchMode::Hybrid` and HTTP requests without `rrf` retain zero-based
constant 0.6, equal unit weights, and the existing per-shard candidate window
`offset + length`. These defaults have **not** been retuned.

Rust callers opt into `SearchMode::WeightedHybrid { similarity_threshold,
ann_mode, rrf: RrfOptions { ..Default::default() } }`. Adding a variant preserves
existing Hybrid struct literals but downstream exhaustive SearchMode matches
must handle the new variant. Storage/query access uses `RecordPartition`; the
retired low-level IndexArc/Search/Commit API is not available. The current
Tantivy package format is for new databases; it does not read legacy indexes.
The HTTP API accepts an optional `rrf` object only with nonempty query text and a
query vector:

```json
{
  "query": "example",
  "query_vector": [1, 0, 0],
  "length": 10,
  "rrf": {
    "constant": 5,
    "keyword_weight": 0.5,
    "vector_weight": 1,
    "keyword_candidates": 100,
    "vector_candidates": 200
  }
}
```

The vector dimension must match the index. The example parameters are exploratory,
not independently validated recommendations.

For each document, the score is the sum over participating channels of
`weight / (constant + rank)`. Rank starts at **zero**, after each channel is merged
across shards and sorted by descending score, then ascending global document ID.
Missing hits contribute zero. Final relevance ties retain ascending global ID.
Zero-based constant 5 equals one-based constant 4; the conventional one-based
constant 60 comparison requires `constant: 61` here.

The constant must be finite and positive. Weights must be finite and nonnegative;
at least one must be positive. Combinations whose maximum fused f32 score would
overflow are rejected. A zero weight excludes that channel's hits from fusion,
including hits found only in that channel. Retrieval still runs both channels,
so zero weights do not imply cheaper queries or relaxed vector validation.
Raw-score diagnostics and source labels describe contributing channels.

Each explicit candidate budget must be a positive integer. That channel retrieves
up to the budget **per shard**, merges and ranks the candidates, then truncates to
the budget **globally before fusion**. These values bound retained TopK
candidates, not documents examined, elapsed time or vector probes. The two budgets are independent of each
other, the output window, and `nprobe`/`all` (which retain their vector probing
semantics). A cap below the output window can yield fewer results. Omitted/null
candidate caps retain the legacy per-shard window, with no extra global cap;
this distinction preserves multi-shard default scores. Other numeric fields and
the HTTP `rrf` object reject null. Unknown RRF fields are rejected.

Explicit budgets held fixed across pages keep the fusion candidate pool fixed.
With omitted budgets it can change with `offset + length`, as before. Count-only
core requests retain existing count semantics; hybrid totals remain the existing
per-shard maximum of channel counts, not an exact union or fused-candidate count.
HTTP vector/hybrid requests still require positive output length. Filters,
realtime visibility, explicit final sort and vector thresholds retain existing
behavior. Explicit field sorts retain existing lexical candidate-selection
semantics; fusion ranks selected candidates by score, not by facet value.

Larger budgets increase per-shard TopK work, merge memory and sorting work; a
rough merge bound is shards times the sum of budgets. More vector candidates do
not replace a sufficient probe budget. No latency, resource or relevance gain is
claimed without measurement on the same frozen corpus and vectors.

## Evaluation boundary

Current defaults are not a measured quality or latency guarantee. Parameter tuning requires a frozen corpus, query/qrel splits, vectors and raw rankings, with independent held-out data and paired resource measurements. Previously inspected dev queries cannot establish blind quality. No model runtime or embedding generation is included. Historical experiment inventories and machine-specific results remain in Git history.
