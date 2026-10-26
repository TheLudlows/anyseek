# Tail sealing work

The first slice introduces explicit native writer sealing and stable candidate
package paths. The second slice directly reuses stable live tails during ordinary
anchor publication. Initial cache layouts, independent snapshots and bounded
compaction retain authoritative rebuilds. Local index sealing is now separate
from State checkpoint publication. Remote/S3 still performs full checkpoint
publication, but shares eligible direct-tail package reuse in that common path.

A candidate is built at its final segment identity, drains native commit/reload,
joins writer workers, and only then computes the immutable inventory. Existing
fixed Searchers and their pins survive. Sealing is idempotent; sealed or failed
writers reject further appends. A failed commit retains queued-input admission
until the owning workers/readers disappear. Directory presence is not authority:
the existing authenticated Reference and Record certificate still select it.
Partition ownership excludes concurrent retirement during preparation; subsequent
anchor bundle publication supplies parent-directory durability.

## Historical foundation validation

Linux compilation and five isolated regressions passed, with no ignored cases.
The largest fixture contains 129 records:

- `sealed_writer_keeps_fixed_readers_and_rejects_future_writes`
- `failed_seal_fences_writer_without_releasing_worker_admission`
- `automatic_tail_rotation_preserves_durable_retry_and_fixed_searcher`
- `rejected_prepared_anchor_reclaims_only_unselected_objects`
- `default_anchor_loads_index_and_replays_only_tail_with_pinned_quantization`

The injected failure is a real directory write/commit failure, not a separately
injected worker-join failure. These are the initial local results; subsequent
dual-platform validation is tracked by exact commit in PR #85.
No original capacity database or stopped load was resumed.

## Direct reuse and failed publication

An empty successor is built and its native writer is actually admitted and
initialized before sealing the predecessor. The predecessor flushes vectors and
metadata, drains its native writer, and retains its fixed Searcher and pin under
the live view. The candidate Reference selects its original package identity and
file inventory instead of rereading bodies to build another index.

A definitely rejected preparation leaves both predecessor and writable successor
owned by the live query view. Repeated attempts carry ordered pending packages;
retirement statistics exclude updated/deleted versions before Top-K. Inventory
bytes are reauthenticated on selection. Failed sealing/handoff and uncertain
certificate publication keep the recovery fence; reopen follows journal authority.
Normal document admission still applies to future writes. Initial layouts without
stable identity, independent snapshots and package-count compaction use rebuilds.

Seven targeted Linux cases passed: the three new direct-reuse tests below plus
prepared rejection/GC, incremental closure recovery, pinned vector quantization
and the 129-record automatic rotation regression. The new cases verify actual
package identity and identical inventory, rather than only successful queries:

- `direct_tail_seal_reuses_bytes_and_rejection_keeps_successor_writable`
- `direct_tail_seal_unknown_certificate_fences_and_reopens`
- `pending_seals_keep_retired_versions_out_of_vector_and_field_queries`

A further two-record admission regression passed: a full background quota rejects
successor writer admission while the predecessor remains writable, then releasing
the pressure permits admission. This reserves logical quota, not GiB allocations.

Quick CI includes these four and the first slice's two writer-owner cases; the
original 24-case core run passed, followed by this additional exact case.
The successor-admission and direct-reuse cases subsequently passed on Linux and
Windows at `5505926` (Actions run `40334585300`). The largest targeted fixture
is 129 records. No large experiment or original data writes were resumed.

## Local index seal versus checkpoint

A normal local native-tail rotation first reauthenticates the selected State
closure, all Payload objects and selected/pending packages. It parses and replays
the entire committed WAL suffix, including same-sequence maintenance events, from
the immutable captured State previously validated against the full manifest SHA.
The replay/live-State equivalence proof below is required before handoff. Only
then does it seal the index into an admitted successor. It does not replace the
journal certificate, truncate history or make an unselected package authoritative.

Explicit checkpoint, recoverable WAL space pressure and the package-count bound
still use full publication. Initial/nonstable layouts, private cleanup and S3
fall back to the full publication path. S3 can reuse an eligible stable tail through
`prepare_and_publish_anchor`, then still performs State authentication, remote
authority publication and local journal checkpoint; it is not seal-only. TTL/lease appends also preserve reserved WAL
capacity; invalid frames/recovery-required errors do not request checkpointing,
and OutcomeUnknown propagates unchanged. Subsequent admission is checked again.

Recovery authenticates the selected closure, retires only unselected/unpinned
derived segments before allocating another tail, then replays the complete WAL.
Replay can seal derived components without publishing a new certificate. Failed
recovery cannot accumulate an unbounded sequence of derived directories.

Local verification: 30 exact cases passed on the production change, followed by
a ninth-record package-bound case (31 total, no ignored cases). Largest fixture:
130 records; the test confirms unchanged checkpoint identity across automatic
sealing, three interrupted recoveries with stable leftover counts, final 130
records and identical retries for the 129 appended operations. Other cases cover
old Payload/State/pending-package corruption, semantically invalid WAL maintenance,
same-sequence FeedTrim, space-triggered checkpoint and uncertain certificate
recovery. The package-bound case verifies nine records and nine history entries,
not individual receipt retries. Server/S3 feature compilation and four bounded
Linux SQLite/in-memory object-store regressions passed: refused admission/receipts,
checkpoint/GC/cold-tail queries, interrupted publication, and cache corruption/loss.
These are fixtures, not real S3 or the full remote matrix. Exact-commit platform
results are maintained in [PR #85](https://github.com/TheLudlows/anyseek/pull/85).

This is not a throughput result. WAL suffix traversal and State metadata/history
validation remain. The WAL visitor's frame scratch and decoder residency overlap
and are both charged; neither 32x owner was removed. No original load was resumed.

## Equivalent replay proof foundation

The local proof compares every State metadata field, complete ordered record keys,
source revisions and operation history. Production Record/Applied values are
immutable: identical shared owners prove identical content; different owners
retain canonical per-value SHA comparison. Lookup associations and lookup byte
accounting are independently checked. Metadata projection exhaustively destructures
State and is tested against the original serde representation. This does not derive
a full SHA by combining hashes, nor cache a proof by business sequence.

Incremental anchor verification still replays its delta and authenticates all disk
closures. It now proves replay-State equality without reserializing every unchanged
body; the existing live State full SHA, byte bound and validators still authenticate
the persistent manifest. Clone admission uses an exact length assembled from
immutable value lengths plus escaped keys, delimiters and all metadata, preserving
32× accounting. Format and corruption detection timing are unchanged.

Two small proof fixtures plus the 25 existing exact core cases passed locally
(27/27). They include independent decode, same-length content replacement, ordered
history, nonempty revision values, low epoch, same-sequence metadata, lookup and
full JSON length/projection oracles. The local separation above uses this proof;
remote checkpoint separation remains unimplemented.

## Remaining cost and measurement scope

Direct sealing removes rebuilding an eligible native tail and avoids publishing a
State checkpoint on each local index rotation. It does not make rotation O(1):

- `Reference::verify_live_suffix` reauthenticates the selected disk closure,
  Payload objects and selected/pending packages, then parses/replays the entire
  committed suffix since the checkpoint.
- `state_proof::verify` walks complete ordered records/history and validates
  metadata and lookup associations. Shared immutable values avoid re-encoding
  unchanged bodies, but container traversal and independently decoded value hashes
  remain. State clones retain key/pointer and metadata allocation charges.
- Explicit, WAL-pressure and package-bound checkpoints still encode full State
  and authenticate the complete closure. S3 still uses full publication.
- Ordinary writes still pay their existing durable journal and native
  commit/reload costs. No fsync-only cost or new throughput gain has been measured.

Next measurement should use fresh isolated roots, fixed builds, identical budgets
and corpus, and separate ordinary batches, index seals, checkpoints and reopen
latencies. Check correctness/errors alongside timings. Restarting large workloads
requires renewed authorization; the original 63,325-record experiment remains
stopped and untouched. Full recovery/browser stability, real S3, hard power loss
and cross-AES acceptance are not established by the small regressions.
