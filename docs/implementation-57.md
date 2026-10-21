# Issue #57 implementation record

Status: **in progress — P1-A is not complete**. Baseline:
`fc4fcfe64fdf86d03c07f82cf1d4889deb896801` (#56, #58, #59 included; #60 excluded).
Working branch: `feat/57-reliable-record-engine`. No PR or issue publication is authorized yet.

This is the implementation/validation entry point, not a replacement roadmap.
The product contract remains [issue #57](https://github.com/TheLudlows/seekpower/issues/57).
Current capabilities remain in [FEATURES](../FEATURES.md). Historical validation
remains in [VALIDATION](../VALIDATION.md).

## Latest checkpoint — default Record HTTP/UI integration (in progress)

Parent checkpoint: `40d5ee35b0453e8e4ff58d84706000e9a0bc4eb5`.
Default startup now opens the durable catalog and recovers its registered Record
partitions before serving requests. It never mounts the legacy per-key index
router. Prepared creation intents are completed before activation; missing active
partitions fail startup. The initial data root must be absent, or already a valid
catalog; existing empty/legacy directories are not silently imported.

Actual changes and decisions:

- `record_service.rs` authenticates API keys, orchestrates collection creation and
  recovery, and serves original records, conditional writes, query, operation
  status and online checkpoint. Only the bootstrap master identity may register
  or revoke keys, manage grants or inspect catalog operation status. An ordinary
  key may create a new collection but cannot enlarge access to existing data.
  Revocation preserves collections and other authorized keys.
- The catalog output permit now captures an active identity plus all grant
  revisions represented by a response. HTTP checks it at final application
  status/header handoff and again when handing the bounded body frame to Hyper.
  Success, conflict, not-found, serialization and oversized-response errors use
  the same barrier. Bytes already handed to the network cannot be retracted.
- Writes and control work survive requester cancellation. Sixteen control tasks
  and 64 admitted requests bound service concurrency; request admission remains
  held through body consumption/disconnect. A 30-second deadline reports
  `outcome_unknown`; clients must retry their original operation identity.
- Internal activation identities use a reserved `sys:` namespace, rejected for
  external commands, and generated directory IDs rather than arbitrary-length
  collection IDs. Master creation is part of the initial durable snapshot.
- `RecordQuery` carries existing AND/OR, filters, sorting, pagination, count,
  vector budget and RRF semantics into one Record read view. Query hits carry
  stable Record envelopes. Direct reads retain complete original documents;
  search projection follows schema storage and requested fields.
- The existing Web UI uses string collection IDs, an explicit embedding space
  for vector collections and per-record conditional writes. Pending creation and
  write requests are saved before sending and replayed unchanged after response
  loss/reload. Multi-document input is a sequence of independent writes, not an
  atomic batch. No raw API key is duplicated in pending request storage.
- API reference generation describes the new mounted routes. Historical legacy
  tests remain library regression tests, not proof of default HTTP integration.

Independent review fixes: reserved activation identity collisions/length,
protected error responses, bounded control admission, response-body admission
lifetime, and serialization/size-error output barriers. Regression coverage uses
real TCP revocation interleavings plus bounded-body unit tests. No new graph,
account platform, migration or performance investigation was added.

Validation: final-source commands and results are recorded below and in
[the validation ledger](validation/issue57-p1-checkpoint.json). Native CI now runs
service barrier tests and HTTP/restart tests on Linux and Windows; Linux also
runs Chromium. These workflow changes have not been published or executed on
Windows. Process kills are not system-crash or power-loss certification.

Remaining acceptance gates (P1-A is **not complete**): iterator, bulk/ID-list/query
and all-record deletion, collection deletion lifecycle, independent HTTP backup/
restore, comprehensive failure injection through actual HTTP, further legacy
panic/decoder audit, and Windows native/system-fault certification. These routes
are currently unmounted rather than delegated to an unauthorized legacy path.
The UI still needs an explicit resolution/discard flow for permanently rejected
pending requests; automatic identity regeneration would be unsafe. Cross-origin
CORS/trailing-slash compatibility is not yet restored. Typed source dependencies
and immediate derived-data hiding remain P2; arbitrary JSON metadata is not an
implemented dependency graph.

Final-source Linux validation for this checkpoint:

- `cargo test --workspace --all-targets --quiet`: **158 passed, 0 failed,
  10 ignored, 35 suites**. Log `/tmp/seekpower-service-final-tests.log`.
- `cargo build --workspace --quiet`: passed. Existing dead-code warnings from
  unmounted legacy service modules remain.
- `SEEKPOWER_TEST_ROOT=/workspace/seekpower/target/p1-native-data
  PLAYWRIGHT_BROWSERS_PATH=/workspace/playwright-browsers python3
  scripts/record_service_smoke.py --server target/debug/seekpower_server --browser`:
  passed, real HTTP and Chromium, with data on overlayfs. Log
  `/tmp/seekpower-service-final-e2e.log`.
- `python3 scripts/check_core_boundary.py` (with the configured Cargo PATH):
  passed, including isolated core builds. The initial invocation lacked Cargo
  on PATH and was rerun with the executor toolchain environment.
- `target/debug/seekpower_server generate_openapi=true`: passed; 10 mounted API
  paths, no legacy bulk/iterator routes advertised.
- Linux native process/restart tests: passed. Linux host/system-crash: **not run**.
  Windows native tests and Windows system-crash: **not run**. To execute the new
  Windows CI path requires authorization to publish the branch and invoke the
  workflow (or a separately provided Windows runner); neither was done here.

## Previous checkpoint — durable catalog and authorization barriers

Parent checkpoint: `837bac35d7c629ab3ca1ff6ed62658038668333b`.
This is the executable catalog dependency for service integration. **The default
HTTP/UI path has not been switched. There is no new HTTP end-to-end acceptance
claim, and P1-A remains incomplete.** Existing HTTP key deletion still follows the
legacy ownership model until that service replacement is completed; this branch
must not be represented as a shared-collection service release.

Actual changes:

- `catalog.rs` stores independent keys, collection creation intents, complete
  collection configuration, grant revisions and idempotent control-plane results.
  It reuses `PartitionJournal`, including its owner lock, witnesses, publication
  certificates, online checkpoint and old-WAL retirement. It does not duplicate
  filesystem synchronization code or store authoritative grants in plain JSON.
- `PrepareCollection` persists a generated relative directory and complete config
  before data creation. It is invisible to authorized data operations until
  `ActivateCollection`. The service must create/recover the actual RecordPartition
  and verify it before issuing Activate; this orchestration is still pending.
  Recovery exposes prepared intents, never registers arbitrary directories.
- Key and collection identities are never reused. Revoking a key removes its
  grants and leaves collections and other keys intact. A creator revoked during
  initialization cannot strand data: privileged activation can complete without
  restoring that key's grant, and a valid key can be granted access separately.
- `authorized` runs owned work under a catalog publication barrier, including
  nested Record writes that survive cancellation. Dropping the requester does
  not release the barrier prematurely. A panicking work task fences authority.
- `OutputPermit::deliver` checks the captured grant revision under a synchronous
  read guard held through the actual bounded output handoff. Revoke takes the
  corresponding write barrier. Revoke/regrant cannot revive a previously captured
  permit. Dropping the catalog invalidates outstanding permits. Wiring this into
  the HTTP body handoff remains necessary; checking before returning JSON alone
  is insufficient. Already handed-off network bytes cannot be retracted.
- Catalog commands and operation lookup are privileged library interfaces. The
  service must authenticate control-plane callers; these are not unauthenticated
  public HTTP routes. No account platform or read/write role split was added.
- `record.rs` shares configuration validation with Prepare, including finite
  boosts, vector/schema rules and checkpoint admission. Catalog entries also
  undergo recovery-decoder roundtrip before durability. Future-epoch Record
  operation lookups now reject consistently with writes.

Retention and resource policy:

- Both data and catalog operation IDs accept the current and previous UTC day.
  `expires_at_unix_seconds` exposes the start of epoch + 2; an ID issued in its
  own day has at least 24 hours. Future and expired IDs reject explicitly.
- Catalog retains up to 10,000 results and a 16 MiB journal generation. Online
  checkpoint removes only expired receipts, persists the epoch watermark, and
  preserves all identity tombstones, collection intents/config and grant revisions.
  Clock rollback cannot resurrect removed retry identities. WAL admission retries
  after safe compaction; unexpired ledger exhaustion applies backpressure.
- If capacity prevents a revoke, no success is returned and authorization is
  fenced. Recovery preserves the actual durable state; the caller must retry
  the original revoke ID. Uncertain persistence also fences existing outputs.
- Record checkpoint still retains its bounded operation ledger rather than
  reclaiming it. Data-operation retention/reclamation and physical-version bounds
  must be finalized together; this change does not silently remove retry results
  or claim unbounded continuous operation.
- Catalog mutations currently clone bounded control-plane state. Record writes
  remain incremental. The global catalog barrier serializes authorized work;
  per-collection concurrency and HTTP response-size/deadline enforcement remain
  explicit service integration work, not performance tuning claims.

Regression coverage includes real Record writes/get/operation lookup through
catalog authorization, shared-key revoke and reopen, durable data preservation,
prepared intent replay, creator revocation during creation, illegal configuration
rejection with unchanged WAL, cancellation barriers, grant ABA, panic fencing,
output handoff ordering, capacity-failed revoke, uncertain revoke replay, online
checkpoint/retry preservation, expired receipt reclamation and clock rollback.
Eight subprocess exit boundaries cover revoke WAL/witness publication; these
are process-failure tests, not system/power-loss certification.

Final-code Linux validation (overlayfs, Rust 1.99.1):

- `SEEKPOWER_TEST_ROOT=/workspace/seekpower/target/p1-native-data cargo test --workspace --all-targets --quiet`:
  **154 passed, 0 failed, 10 ignored**, across 35 suites. Nine ignored subprocess
  workers are invoked by parent tests; the existing opt-in AES test remains
  ignored. Final log: `/tmp/seekpower-catalog-final2.log`.
- `cargo build --workspace --quiet`: passed on final source;
  `/tmp/seekpower-catalog-build-final.log`.
- New catalog tests: 12 passed plus one subprocess worker ignored by default.
  Initial fixture failures (missing schema `store`) were fixed before full runs.
- Independent read-only review identified invalid-config recovery, revoked
  creator activation, and revoke-at-capacity defects; all fixed with regression
  coverage. Followup static review found no new blocking persistence/revision
  defect in its scope. This is not a service integration review.
- HTTP/UI smoke was not rerun for this dependency-only change. The prior legacy
  smoke cannot establish new service integration; new HTTP end-to-end is pending.

Native Windows compilation/tests and both platforms' system crash verification
remain **not run**. The native CI matrix includes catalog tests only as a local
change; it has not been published or dispatched.

Next required work: default server startup and HTTP/UI use of this authority;
prepared collection creation/recovery orchestration; public key management and
shared grants; operation-status authorization; body delivery integration; real
service crash/restart/revocation end-to-end tests; full platform acceptance. No
permission denial is blocking local work. This checkpoint is a dependency slice,
not completion of those service tasks.

## Historical checkpoint — incremental publication and online WAL generations (validated locally)

Parent checkpoint: `7fa0fd3f4b60bad336674b54fbbb49013aedc956`. This stage replaces
the per-write reference rebuild with the actual existing-engine append path.
**The library path is incremental; the HTTP/UI default path is still legacy.**
P1-A is not complete and there is no reliable-service release claim.

Actual module changes:

- `active.rs`, `index.rs`, `field_length.rs`, `realtime_search.rs`, `search.rs`:
  maintain active-document footprints, exact active term df and compressed field
  lengths separately from physical posting/encoding counters. Committed,
  realtime, ngram and normalization paths use the same active statistics.
  Active pruning uses conservative finite bounds; equal-score pruning cannot
  assume a physical ID lower bound is a logical record-ID bound.
- `min_heap.rs`, `vector.rs`, `search.rs`: record-ID ordering enters candidate
  heaps, vector selection, fusion and empty-query selection before truncation.
  Physical IDs remain internal addresses. Longest-field encoding is selected
  from the schema (honoring an explicit choice; otherwise first lexical field)
  and persisted, so recovery cannot change the encoding decision.
- `record.rs`: writes append one physical version, retire its predecessor before
  publication, and publish authoritative data, lookup positions and statistics
  under the same guard. Index failures after WAL durability still fence every
  read/write view. Writes no longer clone/serialize the whole state or rebuild
  the index. Exact checkpoint-byte accounting changes by this operation/record;
  an indexed operation ledger replaces linear dedup scans. Full snapshots and
  startup reconstruction remain intentionally proportional to retained state.
- `RecordSearch.statistics` and `statistics()` report the same published sequence,
  active record count, retained identities/operations, physical versions and
  complete-checkpoint bytes. ANN/fusion hit counts are not promoted to exact
  corpus counts. Physical history remains bounded by retained operation/journal
  limits; long-running physical index compaction remains P3 work.
- `storage/partition.rs`: a stable owner lock spans local WAL generations.
  `compact_journal()` publishes a complete checkpoint online, preserves the
  serving index, then retires the old WAL generation. Independent `checkpoint`
  destinations retain their backup/restore semantics. No cloud protocol or
  format migration was added.
- `commit.rs`, `vector.rs`: release mmap before complete-block file extension;
  address realtime vectors correctly after a partially committed block.
  The native CI matrix now also includes partition-generation tests.

Local publication protocol and resource boundary:

1. Build the complete new generation in a unique prepared directory. Sync its
   initial snapshot and WAL witnesses, then publish the generation directory.
2. Only after directory publication succeeds, append a publication certificate
   using the same three-witness journal. A valid certificate implies the earlier
   namespace durability step completed; merely seeing a directory is insufficient.
   Initial root creation writes its certificate only after final root publication,
   while continuously holding the owner lock through return to the creator.
3. Recovery chooses the highest certified generation. Journal corruption fails
   closed; it never falls back on corruption. Only a complete single-frame
   generation without a certificate is known not to have served writes and may
   be discarded. A root without any publication certificate is refused: it did
   not finish creation and cannot have acknowledged record writes.
4. After a successful switch, close old journal handles before removal. Before
   allocating another generation, unfinished reclamation must succeed. There
   can be current plus one prepared/old generation, each bounded by the journal
   budget (plus fixed metadata); cleanup errors cannot permit unbounded rotation.
5. A non-certified higher generation is durably renamed out of the selection
   namespace before recursive deletion. Recovery also re-renames leftover
   `.prepare` directories durably before deleting their contents, because a
   prior retirement may have failed after rename but before parent sync.
6. Unknown publication outcomes fence the instance. Known pre-publication
   failures preserve the existing view. These are local filesystem guarantees
   under the stated fault model, still awaiting native system-crash validation.
7. Recovery may use a larger external read budget to load a checkpoint, but
   restores the persisted journal write limit before accepting new operations.
   Reopening cannot silently expand the configured resource boundary.

Regression coverage added:

- Stable ties beyond K, including replacement with a later physical ID; lexical,
  vector, hybrid, weighted fusion and empty query; commit/reopen/append.
- Shared and per-field statistics, ngrams, empty fields, deletion/replacement,
  compared with a clean effective-record index before/after commit and reopen.
- Physical 65535/65536 crossing, automatic flush, facet growth, partial commit
  then vector append. The fixture seeds retired history through the real engine;
  the final boundary crossing uses the authoritative Record write path. It is
  not 65K separately durable operations or a capacity benchmark.
- Failure after index append and after retirement fences get/search/statistics;
  recovery replays the durable replacement and preserves retry/ABA contracts.
- Online checkpoint preserves originals, tombstones, retries and logical seq;
  process exits cover preparation, un-certified publication, certification,
  switch, partial old-generation deletion and completed reclamation.
- Repeated interruption while discarding un-certified generations; injected
  post-rename sync failure and repeated recovery cleanup sync failures; corrupt
  certified generation refuses fallback; creator retains owner lock through return.

Final-code Linux validation (overlayfs; Rust 1.99.1):

- `SEEKPOWER_TEST_ROOT=/workspace/seekpower/target/p1-native-data cargo test --workspace --all-targets --quiet`:
  **142 passed, 0 failed, 9 ignored**, across 35 suites. Eight ignored subprocess
  helpers are invoked by their parent tests; one existing opt-in AES test remains
  ignored. Log: `/tmp/seekpower-incremental-final4.log`.
- `cargo build --workspace --quiet`: passed after the final persisted-budget fix.
- `python3 scripts/http_smoke.py --server target/debug/seekpower_server`: passed
  after the final build; Chromium not run. This covers the existing service, not
  integration of the new Record path. Log: `/tmp/seekpower-incremental-http-final.log`.
- Independent read-only reviews found and drove corrections to equal-score
  pruning, partial-block vector addresses, mmap extension, publication witnesses,
  owner-lock lifetime and interrupted reclamation. Regression tests cover these.

Windows native compilation/tests and both platforms' system/power-loss tests
remain **not run**. The local CI job has not been published or dispatched;
process exits and simulated I/O failures do not constitute system-crash certification.

Remaining P1-A work: default HTTP/UI integration, independent collection/grant
catalog, shared-key authorization plus revocation/publication/output barriers,
configured retention/deadline policy, and complete dual-platform acceptance.
Startup and explicit checkpoints may reconstruct/serialize retained state;
per-write rebuilding is removed. Abandoned **root-level** initialization siblings
outside a partition still require catalog-level safe reclamation. Source-derived
visibility is still pending the typed P2 interfaces, not implicit JSON metadata.

## Historical checkpoint — independent review corrections (completed locally)

Parent checkpoint: `6fd41b2c9210df59ff539e9c6cef240959352124`. This follow-up
addresses the independent persistence review before incremental publication.
It does **not** complete P1-A or enable the reliable path in HTTP/UI.

Actual changes:

- Authoritative records reject JSON nesting above 64 before serialization or
  WAL writes. Each encoded entry is checked with the recovery decoder before
  admission, including complete checkpoints. Accepted depth is tested through
  reopen and independent snapshot restoration; rejected writes leave WAL bytes
  and visible sequence unchanged.
- Creation and independent checkpoints prepare their complete initial frame and
  synchronized witnesses before publishing the directory. Budget errors and
  pre-publication failures are definite rejections; failures after attempting
  directory publication remain `outcome_unknown`. No empty authority is
  published merely to reserve a destination.
- Recovery holds the exclusive journal lock while removing only recognized
  `cache-<32 hex digits>` directories before reconstruction. Unknown files and
  directory names are retained; a cache-name symlink fails closed. Repeated
  recovery, interrupted-build debris and competing opens are covered.
- Owned create/open/search tasks retain the authority and admission guards until
  their indexing/search tasks finish, even after caller cancellation. The
  journal field drops after index handles. Independent follow-up review found
  that field order alone was insufficient for detached hybrid search tasks;
  a controlled shard gate now verifies cancellation plus dropping the last
  public partition owner does not release the journal lock prematurely.
- Initialization process-exit tests cover interrupted log sync, torn first
  witness, fully prepared-but-unpublished state, and published state. These are
  process-exit tests, not filesystem/system-crash certification.

Validation on Linux/overlayfs, with `SEEKPOWER_TEST_ROOT` set to
`/workspace/seekpower/target/p1-native-data`:

- `cargo test --workspace --all-targets --quiet`: **131 passed, 0 failed,
  7 ignored**, 35 suites. Six ignored tests are subprocess workers explicitly
  invoked by parent tests; the seventh is the existing opt-in AES cross-build
  test. Log: `/tmp/seekpower-p1-review-final.log` in this executor.
- After the final publication-error classification refinement,
  `cargo test -p seekpower --lib journal_ --quiet`: **6 passed, 0 failed,
  3 ignored** (subprocess workers invoked by parent tests).
- Controlled cancellation tests: **2 passed**. An earlier test harness used the
  single-shard lexical shortcut and never entered the asynchronous shard gate;
  it was corrected to use hybrid search. The corrected targeted and full suite
  both pass; there are no remaining failing tests in this checkpoint.
- `cargo check --workspace --quiet`: passed after the final error-classification change.
- `git diff --check`: passed. Remote master was read-only rechecked at
  `fc4fcfe64fdf86d03c07f82cf1d4889deb896801`; no upstream baseline drift.
- Independent read-only review covered the original checkpoint, corrections,
  cancellation follow-up, and final publication error stages. No new blocking
  defect was found within this correction scope; this is not full P1 acceptance.

Windows native compilation, native tests, and system-crash tests remain **not
run**; Linux system-crash tests also remain **not run**. The existing local Windows CI job is
still unpublished; no runner was dispatched or user machine accessed.

Review constraints for the next incremental change:

- Keep physical posting/doc counters for decoding; introduce separate active
  document count, term df and field-length sums. All committed, realtime, ngram,
  upper-bound and cache paths must read the same published statistics.
- Logical record-ID tie ordering must enter lexical/vector candidate heaps and
  fusion before truncation. Final output sorting cannot repair lost candidates.
- Freeze and persist the longest-field encoding decision across replacement,
  deletion and recovery. Do not reinterpret existing postings by changing it.
- Invalidate warmup caches at the same publication boundary and bound physical
  history, tombstones and dedup state as well as live records.

Remaining: incremental publication/resource accounting, default HTTP/UI path,
shared-key collection registry and revocation/output barriers, live checkpoint
rollover/WAL reclamation, native Windows validation and both platforms' system
fault validation. Prepared directories from failed initialization remain
identifiable but require future safe parent/catalog-level reclamation. The
current per-write rebuild and full-state serialization remain a bounded
reference, **not a production implementation**.

## Confirmed scope

Linux **and Windows** require native reliability acceptance, not merely builds.
API keys can share a collection with the same access capability; no account
platform or granular roles. Revocation does not delete the collection. Source
deletion hides derived content immediately; revocation blocks access immediately.
Cross-collection evidence dependencies are unsupported in the initial scope.
No legacy migration, old-format reader, cloud protocol or performance study.

P1-A must integrate stable IDs, conditional replacement/delete, durable operations,
idempotency, a common read view and active scoring statistics with the existing
lexical/vector/filter engine, complete checkpoints and recovery. It is not done
when types, a journal, or a separate API exist. P2 supplies source generations,
evidence and optional vectors. P3 supplies long-running compaction/reclamation.

## Historical stage 1 — persistence prerequisites

Actual changes:

- Core commit/docstore/vector/segment file errors propagate as `io::Result`.
  Short-write-sensitive calls use `write_all`; file sync errors propagate.
- Explicit commit drains all shard tasks, including after one fails.
- Document indexing waits for the spawned indexing task and propagates its
  result, including automatic block commit errors.
- HTTP commit propagates storage failure as HTTP 500; shutdown logs close failures and exits unsuccessfully.
- Local journal prototype uses checksummed frames and a synchronized commit
  witness. Bytes beyond the witness are uncommitted; corruption inside its
  extent is an error. A second writer is refused using a file lock.

### Local journal decision and limitations

The log is synchronized before publishing its end/sequence in three separate
witness files. Each commit overwrites the oldest two witnesses one at a time,
syncing each; success requires two durable copies. Recovery selects the highest
valid position, validates its complete log extent, and repairs redundancy before
accepting another write. One torn witness cannot discard an acknowledged commit;
two invalid witnesses or inconsistent positions fail closed. This replaced an
initial two-slot fail-closed prototype during self-review, before any publication.
Recovery repairs invalid witnesses before touching older valid copies; a child
process regression covers interruption during that repair. The fault model
assumes a write to one witness file does not corrupt another;
native filesystem/system-crash certification is still required.

After write/sync failure the live journal refuses further writes until reopen.
Capacity/size rejection happens before entering the uncertain write phase.
File locking protects against concurrent cooperating writers, not external file
modification. No WAL truncation/rotation or checkpoint reclamation exists yet.

### Linux/Windows platform review

- Linux: sync files, sync the prepared directory, rename within the same parent,
  then sync the parent directory. `fsync(file)` alone does not persist its parent
  entry ([Linux fsync documentation](https://man7.org/linux/man-pages/man2/fsync.2.html)).
- Windows: sync file handles before closing; publish a unique prepared directory
  using `MoveFileExW(MOVEFILE_WRITE_THROUGH)` without replacement or cross-volume
  copy. No live mmap generation is replaced. This is an implementation candidate,
  **not a certified atomic/durable directory protocol**. Its crash behavior must
  be tested on native Windows; the documented write-through behavior alone is
  insufficient evidence for all required guarantees.
  [FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers),
  [MoveFileExW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw).
- Fixed journal/witness handles use `File::sync_all`; locks stay held for the
  writer lifetime. Fault tests must kill a process while handles remain open.
- Existing index code still mutates mapped files in place. Result propagation
  alone does not make those files atomic checkpoints or a common read view.
- All Windows file deletion tests must release handles/mappings first; Linux
  unlink-while-open behavior must not be assumed portable.

### Validation ledger

| Check | Linux | Windows native |
| --- | --- | --- |
| Workspace compilation after Result plumbing | Passed `cargo check --workspace --quiet` | Not run |
| Journal recovery, corruption, writer lock, capacity | Passed (5 tests, including child exits and repair interruption) | Not run |
| Existing core/service regressions | Passed: 125 tests, 0 failed, 4 ignored across 35 suites | Not run |
| Child-process crash recovery | Passed: 8 journal write boundaries, 1 repair interruption and 5 record/response-loss boundaries | Not run |
| System/power-loss recovery | Not run | Not run |

Commands use installed Rust with `PATH=/workspace/.cargo/bin:$PATH`,
`RUSTUP_HOME=/workspace/.rustup`, `CARGO_HOME=/workspace/.cargo` on this executor.
A test build initially found a match arm expecting `()` after the Commit result
change; test callers are being updated to assert successful results.

### Gates recorded at the original checkpoint

- Core journal/Record/version/dedup now use existing-engine publication (Stage 2);
  HTTP/UI do not yet use that path, so no durable service guarantee is added.
- Fence reads/writes after partial application; distinguish rejected, durable
  pending, and unknown outcomes. Add cancellation and response-loss tests.
- Independent complete snapshot/recovery and configuration persistence now exist;
  live checkpoint-root publication/manifest and WAL rollover remain outstanding.
- Common scoring context/visibility, version ABA, bounded operation retention.
- Shared-collection authorization and response/commit revocation barriers.
- Native Windows CI job has been prepared locally; it has not run.
  Workflow edits can be prepared locally, but triggering a remote run would
  require publication outside the current authorized scope.

## Historical stage 2 — initial record/reference-view integration

`record.rs` uses the existing retrieval engine, not a replacement scorer. It
persists original Documents (including fields excluded by search projections and
unnormalized vector values), immutable routing keys, revisions/tombstones,
operation results, configuration and logical sequence. A bounded serial guard
covers ranking and authoritative record lookup. Accepted writes continue in an
owned task even if the caller disconnects. Successful writes are witnessed and
published; post-durability application failure fences the instance until reopen.

Implementation decisions awaiting full P1 acceptance:

- Current/previous UTC-day operation epochs provide at least 24 hours of retry
  retention. The full `(epoch, nonce)` is the identity; expired epochs explicitly
  fail. Observed clock epoch is persisted with writes. Operation count and WAL
  bytes are bounded; there is no silent result eviction or infinite retention
  guarantee. Retention configurability/clock fault coverage remain outstanding.
- The bounded correctness reference rebuilds the effective record set into one
  existing-engine shard after each write, then publishes it. This establishes
  active BM25 statistics/pruning and excludes obsolete vector candidates without
  maintaining two independently visible states. It is **not a production write
  strategy**: incremental publication, fair scheduling and recovery resource
  measurement are still P1 gates. Limits currently cap identities at 10,000,
  schema fields at 64 and dimensions at 4,096; these are prototype bounds, not
  measured service capacity promises.
- `checkpoint(new_path)` writes a complete independent snapshot including
  versions and dedup results. Restore uses the same `open` path. No live WAL
  checkpoint-root rollover, incremental checkpoint reuse or log reclamation is
  implemented. The initial checkpoint and whole journal remain authoritative.
- Search returns original records with versions and a common position. It does
  not label hybrid/ANN window counts as exact totals. Full diagnostic/count,
  iterator, projection and deadline contracts are not complete.
- The module is **not yet wired into the HTTP/UI default path**. Shared-key
  grants, revocation/send barriers, service integration and UI retry handling
  remain unfinished; P1-A must not be marked complete on these core tests alone.

Tests currently cover all three retrieval modes, raw input retention, replacement,
delete/recreate ABA, conflicting/repeated operation IDs, concurrent version
conditions, active-score equality to a clean effective-data index, complete
snapshot restore and post-durability application failure/replay. Record-level
native child-process and response-loss tests pass on Linux (5 boundaries).

Validation fixture fixes: vector schema must explicitly declare `store` and
`index_lexical`. Reopen fixtures must use the existing 11-bit segment layout
(`open_index` currently hardcodes that layout); the initial 4-bit fixture failed
on reopen and was corrected, not suppressed. P1 record checkpoints persist
configuration and rebuild derived indexes, rather than trusting such bare files.

No applicable files were found in repository `.agents/skills` or workspace
`.agents` during this stage. No historical performance investigation was resumed.

## 2026-10-21 local checkpoint

Status: **tested core checkpoint; P1-A still in progress**. No external publication.
The source baseline remains `fc4fcfe64fdf86d03c07f82cf1d4889deb896801`.

Commands and actual results:

- `cargo check --workspace --quiet`: passed.
- `cargo test --workspace --all-targets --quiet`: **125 passed, 0 failed,
  4 ignored, 35 test suites**. Three ignored tests are child-process workers that
  the parent tests execute explicitly; the other is the existing opt-in
  cross-build term-hash fixture. No ignored test was counted as a standalone pass.
- `cargo build --workspace --quiet`: passed.
- `python3 scripts/http_smoke.py --server target/debug/seekpower_server`: passed,
  including embedded UI/static checks. Browser/Chromium interaction was **not**
  enabled for this invocation. This validates the retained HTTP path, not the
  unfinished reliable-record HTTP integration.
- `SEEKPOWER_TEST_ROOT=/workspace/seekpower/target/p1-native-data cargo test -p
  seekpower --lib --quiet`: passed: 32 tests, 0 failed, 2 ignored before the additional repair test.
  Final full-workspace run with the same environment: 125 passed, 0 failed,
  4 ignored; new journal/record/commit fixtures used overlayfs.
- `git diff --check`: passed before local commit.

Executor: Linux 6.18.55 x86_64; Rust 1.99.1. `/tmp` is **tmpfs** and `/workspace`
is **overlayfs**. New storage/record tests now accept `SEEKPOWER_TEST_ROOT`; the
filesystem rerun places them on overlayfs. Neither tmpfs nor a container process
exit proves host power-loss durability. Linux native process tests are distinct
from Linux filesystem/system-crash certification, which remains **not run**.
Windows compilation, native integration/process crashes, and system-crash tests
are all **not run**. The local CI matrix explicitly includes `windows-latest`
and `ubuntu-latest` and runs the existing-index I/O/mapping tests, journal crash
boundaries and record restore/response-loss tests. It has not been published or
triggered; doing so is outside this checkpoint's authorization. No user's machine
was accessed. The platform directory-publication candidate still needs native
filesystem certification, including initialization failure boundaries.

Self-review findings retained as open gates:

1. HTTP/UI must use this same Record path; key grants and revocation barriers
   still need implementation. Existing key revocation remains legacy behavior
   until that integration, so this branch is not a shared-collection release.
2. Full effective-set rebuilding is a bounded correctness reference only.
   Incremental updates, complete query diagnostics, iterator/projection,
   request deadlines and fair resource accounting remain to be finished.
3. Index initialization/decoding still contains legacy panic/error paths outside
   the converted commit chain. They require further audit before P1 acceptance.
4. Independent snapshots do not replace live checkpoint-root rollover or WAL
   retirement. Recovery still reads the bounded complete journal. Recovery now
   reclaims recognized derived caches under its lock; general physical-history
   and abandoned preparation reclamation remain unfinished.
5. Operation retention currently uses fixed day epochs with a persisted observed
   epoch watermark and count backpressure; configurable policy and clock-fault
   validation are not complete.
6. Source/evidence semantics and cross-collection rejection belong to the pending
   typed P2 interfaces. Arbitrary Document metadata is not an implemented evidence
   graph and has no source-deletion guarantee at this checkpoint.
