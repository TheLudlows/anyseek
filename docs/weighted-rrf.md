# Configurable weighted reciprocal rank fusion

Existing `SearchMode::Hybrid` and HTTP requests without `rrf` retain zero-based
constant 0.6, equal unit weights, and the existing per-shard candidate window
`offset + length`. These defaults have **not** been retuned.

Rust callers opt into `SearchMode::WeightedHybrid { similarity_threshold,
ann_mode, rrf: RrfOptions { ..Default::default() } }`. Adding a variant preserves
existing Hybrid struct literals but downstream exhaustive SearchMode matches
must handle the new variant. Search signatures and index persistence are unchanged.
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

## Evaluation status and required protocol

The 393 MIRACL Chinese dev queries have already influenced hypotheses and are
not blind data. The supplied prior scores are historical context, not results
from this change. Constant 5 with keyword/vector weights 0.5/1 is a hypothesis.
Do not change defaults based on it.

The local inventory in `validation/weighted-rrf/inventory.json` records available
corpus hashes and missing benchmark dependencies. Frozen document/query vectors,
subset membership, query/qrel splits and raw rankings are unavailable here.
No embedding regeneration, model introduction, large download or replacement
benchmark was performed. Train selection, split leakage checking, independent
held-out scoring and paired latency/resource measurement remain blocked.

Source status update: historical assets were reported as verified in the
original benchmark environment, separate from this implementation workspace. The inventory records the supplied SHA-256 hashes
for the 300k subset and frozen document, train and dev vectors. Raw old train
rankings and the plan are in that environment's
`/workspace/quality-diagnostic-20261019`; current master dev rankings and scripts
are in `/tmp/benchmark-main-20261020`. These paths are not local assets in this
workspace, and nothing has been materialized here.

The historical 1,312 train queries were split into 1,056 fit and 256 validation
queries. The 256 validation queries were already used for fixed-candidate
evaluation on October 19. All 393 dev queries were repeatedly benchmarked and
diagnosed. No independent test vectors or qrels exist in the original benchmark
directories, and no genuinely unseen held-out set has been verified. Recovering
historical regression assets does not resolve the independent-evaluation blocker.
No independent quality result, selected parameter change or default change is
claimed.

The source workspace has only 5.011 GiB free, 11.39 MiB above its guard floor.
Do not attempt jobs there. Any subsequent recovery must preserve historical data
and establish sufficient resources in the destination before running jobs.

When the original artifacts are restored:

1. Verify content hashes and provenance of corpus subset, frozen vectors, query
   text, qrels, preprocessing and original rankings. Check query ID and normalized
   text overlaps across train/dev/held-out; report qrel/document overlap separately
   since sharing a retrieval corpus is expected. Exclude previously inspected dev
   queries from any claim of blind evaluation.
2. Predeclare a train-only parameter grid, candidate/probe budgets, objective and
   deterministic selection/tie rule. Select only on train; persist the exact
   selected configuration and hashes before opening truly held-out labels.
3. Compare vector-only, keyword-only, legacy fusion, equal-weight conventional
   RRF (`constant=61`) and the frozen selected weighted RRF on identical candidates,
   plus explicitly labeled candidate-budget sensitivity runs. Reuse frozen vectors.
4. Report nDCG@10 and recall with per-query results, paired uncertainty and failure
   counts. Measure warmup and paired C1/C4 latency percentiles, throughput, peak RSS,
   disk, candidate/probe counts and build/query costs separately. Check available
   disk and the cgroup memory limit before any large job. Preserve historical data.
