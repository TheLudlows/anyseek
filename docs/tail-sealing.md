# Tail sealing work

The first slice introduces explicit native writer sealing and stable candidate
package paths. It does **not** yet reuse the live tail or decouple checkpoint.
Frozen candidates still rebuild from authoritative records.

A candidate is built at its final segment identity, drains native commit/reload,
joins writer workers, and only then computes the immutable inventory. Existing
fixed Searchers and their pins survive. Sealing is idempotent; sealed or failed
writers reject further appends. A failed commit retains queued-input admission
until the owning workers/readers disappear. Directory presence is not authority:
the existing authenticated Reference and Record certificate still select it.
Partition ownership excludes concurrent retirement during preparation; subsequent
anchor bundle publication supplies parent-directory durability.

## Validation of this slice

Linux compilation and five isolated regressions passed, with no ignored cases.
The largest fixture contains 129 records:

- `sealed_writer_keeps_fixed_readers_and_rejects_future_writes`
- `failed_seal_fences_writer_without_releasing_worker_admission`
- `automatic_tail_rotation_preserves_durable_retry_and_fixed_searcher`
- `rejected_prepared_anchor_reclaims_only_unselected_objects`
- `default_anchor_loads_index_and_replays_only_tail_with_pinned_quantization`

The injected failure is a real directory write/commit failure, not a separately
injected worker-join failure. Windows execution of this new slice is pending.
No original capacity database or stopped load was resumed.

## Next independently verifiable slice

Admit and create a successor before sealing the only mutable tail. Retain the
sealed predecessor and its complete pin in the live query view until authority
installation succeeds. A definitely rejected certificate must leave that view
queryable and the successor writable; uncertain publication must keep the
existing recovery fence. Failed sealing requires authoritative reconstruction or
an explicit recovery fence, never reuse of a failed writer. Candidate GC must
exclude every still-owned predecessor, including unpublished ones.

Only independently complete packages can take the reuse path. Replacements,
retired versions, generation changes and compaction need explicit membership and
statistics checks before enabling it. Preserve full byte authentication and its
current detection timing. Index-seal/checkpoint separation remains subsequent
work; these changes alone make no throughput or 100k-capacity claim.
