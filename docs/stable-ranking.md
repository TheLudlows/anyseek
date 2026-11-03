# Stable ranking

[文档索引](README.md) · [当前功能与限制](../FEATURES.md) · [开发与验证](development.md)

For a fixed logical dataset, scoring configuration and candidate set, Record
relevance ranking uses descending numeric score, then ascending stable
`record_id` (string order). The rule participates in candidate admission and
final ordering. Physical component ordinals are addresses, not public identities;
rotation or compaction must not redefine the tie-break by insertion order. An
ordinary document field named `docid` does not replace `record_id`.

Explicit sort fields retain their declared order and direction. A tied `_score`
continues to subsequent fields; after tied explicit fields, the comparator falls
back to descending score and ascending stable identity. `_id` sorts by stable
Record identity. Empty Record browsing without an explicit sort, and browsing
with explicit `_score` when every score is zero, both use ascending stable ID.
Metadata null/missing ordering is described in [FEATURES](../FEATURES.md#字段数值与空值约定).

Signed zero scores compare equal. Infinities keep numeric order. The internal
comparator orders NaN below numeric scores and treats NaNs as tied; external
non-finite vectors are rejected. This fallback is not permission to submit NaN.

Pure lexical candidate collection uses a bounded heap sized to the requested
window (`offset + length`, or the Hybrid keyword candidate budget). The same
requested ordering selects lexical candidates and orders them. Pure lexical
Count skips scoring and candidate collection. Hybrid ranks each selected channel
globally by relevance before one weighted RRF; its budgets and count limitations
are described in [weighted RRF](weighted-rrf.md).

ANN and bounded fusion can omit candidates. Stable tie-breaking does not promise
exact recall, and Vector/Hybrid field sorting does not become a full-corpus
field-sort scan. Concurrent writes or TTL expiry can change the selected corpus
or remove candidates during final revalidation; deterministic ranking does not
provide a persistent pagination snapshot.

Implementation: `record/lexical/runtime_query.rs`, `min_heap.rs` and the vector
candidate path. Existing `tests/stable_ranking.rs`, `tests/weighted_rrf.rs`, the
bounded collector regression and captured/guarded comparator parity cover ties,
explicit sorting, pagination, numeric edge cases and component layouts. The old
posting-block pruning, union cursor and realtime flag descriptions do not apply
to the current Tantivy-backed Record path. Execution evidence belongs to the
exact tested revision in its PR; this document makes no performance claim.
