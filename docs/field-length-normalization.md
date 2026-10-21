# Field length normalization

[文档索引](README.md) · [当前功能与限制](../FEATURES.md) · [开发与验证](development.md)


Index creation accepts `field_length_normalization: "Shared" | "PerField"`. Omission keeps Shared. The setting is immutable for an index; create a new index to change it. Rust callers set `IndexMetaObject::field_length_normalization`. The HTTP index response reports the setting.

PerField keeps the sum of field contributions, global-across-fields term DF, boosts, decoded document lengths, k1 and b. It changes each field denominator to its own mean decoded length. Each shard maintains the sum and the number of physical documents with a nonzero decoded length for each indexed field. Missing/empty fields contribute neither count nor sum; an empty field population uses mean 1. Tombstones remain in length statistics and physical DF, but deleted documents are excluded from hits and exact counts. Duplicate deletion does not alter statistics. Clear resets sums, counts and lookup caches.

Completed appends update statistics and a 256-entry denominator cache under the shard write lock. Statistics include completed uncommitted writes even when a query excludes realtime candidates. Commit writes a pair of u64 values (sum, count) per field with each level snapshot. Reopen restores the latest snapshot directly. Neither queries nor reopening reconstruct statistics by scanning documents. Statistics and DF retain existing shard-local semantics; no cross-shard DF aggregation is introduced.

Shared retains the prior disk layout and opens existing indexes whose metadata omits the setting. PerField uses a distinct metadata-selected layout and requires this implementation to read it; older binaries must not open PerField indexes. There is no in-place migration. A snapshot adds 16 bytes per indexed field per level; each field also has a 256-f32 lookup table and count in memory. The cache is currently allocated for Shared fields too, although Shared scoring uses its existing shared table. PerField refresh work is O(256) per updated field per document; query lookup is O(1).

The statistical definition matches the earlier experiment: decoded stored lengths, nonempty physical field count, tombstones retained, per-shard means, float32 division. The mechanism differs: the experiment reconstructed statistics at open, while this implementation maintains and persists them. Fresh correctness and paired measurements are required; earlier experiment metrics are not substituted.

## Scope and known limitations

Shared remains the default. Performance acceptance budget #35 remains unresolved, and this draft does not enable PerField by default. Independent correctness fixes #40 (realtime boost), #41 (mixed scoring DF), #43 (wide OR membership), #44 (deleted exact counts), and #45 (clear/reindex field identity) apply to both configurations. Controls must use the same corrected binary.

Existing issue #42 (realtime ngram behavior under Bm25fProximity) remains open; this change does not claim to repair all ngram modes. The synthetic component oracle covers Bm25f ngram masks 0/9/127. The fixed Chinese retrieval workload and results must be reported with their actual tokenizer/configuration rather than generalized to the affected proximity case.
