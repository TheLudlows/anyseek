# Tail sealing work

The first slice introduces explicit native writer sealing and stable candidate
package paths. The second slice directly reuses stable live tails during ordinary
anchor publication. Initial cache layouts, independent snapshots and bounded
compaction retain authoritative rebuilds. Checkpoint is **not yet decoupled**.

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
Windows validation for direct reuse remains pending. The largest targeted fixture
is 129 records. No large experiment or original data writes were resumed.

## Remaining checkpoint separation

Ordinary publication still computes a full State encoding and authenticates the
recovery closure. Direct tail reuse removes index reconstruction on the eligible
path; it does not eliminate full-State/history traversal or demonstrate a measured
throughput gain. Separate index-seal and checkpoint watermarks must retain one
business authority, complete WAL recovery, unchanged corruption detection timing,
owned pins and bounded pending packages. That subsequent slice is not implemented.

The decoupling review confirmed a further distinction: full-file authentication
and WAL checksums alone do not replace the existing delta replay/result-vs-State
self-check. The first decoupled implementation must retain that proof, or supply
an equivalent incremental proof. It must not silently defer the check merely to
remove traversal cost. Payload whole-object SHA is checked on open, while each
payload range SHA is also checked when read. Same-sequence maintenance events and
remote journal bounds remain in scope.

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
full JSON length/projection oracles. Full checkpoint separation remains pending.
