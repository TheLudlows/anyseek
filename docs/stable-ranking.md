# Stable ranking

[文档索引](README.md) · [当前功能与限制](../FEATURES.md) · [开发与验证](development.md)


For a fixed index and candidate set, relevance ranking uses descending numeric
score, then ascending internal document `_id`. This rule is applied in lexical
heaps, vector TopK (including equal-score admission), the two hybrid branch
rankings, shard aggregation, and final output before pagination. The stored
user field named `docid` is not the internal ID. Reindexing with different ID
assignment is not covered by this guarantee.

Explicit sort fields retain their declared order and direction. A tied `_score`
continues to subsequent fields instead of returning immediately. Existing score
fallback remains after the explicit fields, followed by ascending ID. Empty
browsing without explicit sorting retains its existing descending-ID behavior;
explicit `_score` browsing has zero scores and therefore uses ascending ID.

Signed zero scores compare equal. Infinities keep numeric order. The comparator
orders NaN below all numeric scores and treats NaNs as tied, so comparison stays
transitive; external non-finite vectors remain rejected, and vector TopK rejects
an internally produced NaN rather than retaining an unordered candidate.

A score-only upper bound cannot prune equality: an equal-score candidate may
have a better ID. Default relevance pruning uses the complete bound (score upper
bound, minimum possible internal ID); equality is rejected only if that ID
cannot improve the full heap's worst rank. Explicit field sorts, browsing and
unknown NaN bounds disable this pruning. Blocks visited by descending upper
score are secondarily ordered by ascending block ID, making an early break safe.
The former block-count-only cutoff remains removed.

Vector TopK caches a conservative full-rank lower bound for the worst retained
item. Retained ranks only improve, so a stale bound can admit extra work but
cannot reject a better candidate. Candidate/count thresholds are unchanged.
The dense-tie unit test verifies one full K-item scan after filling K=100 from
70,000 ascending equal-score candidates; this is not a general latency claim.
Union posting cursors use usize so a complete 65,536-posting block can represent
its one-past-end position without u16 overflow.

The core scoring expressions, native hybrid fusion constant/formula, ANN probe
budgets, index format and parallel architecture are unchanged. Tie-breaking
within hybrid branch ranks can change fused scores, because the unchanged
rank-based formula now receives deterministic ranks. ANN still selects a
bounded candidate set: deterministic tie-breaking is not an exact-recall
promise. Existing candidate-window semantics for explicit sorting in vector
and hybrid retrieval are not expanded into a full-corpus field-sort scan.

Regression coverage includes more tied candidates than K, forward/reverse/
permuted input orders, signed zeros, duplicate IDs, threshold equality, zero K,
single/three shards, realtime lexical results, pagination, explicit multi-field
sorting and repeated C1/C4 lexical/vector/hybrid requests (core and HTTP).
