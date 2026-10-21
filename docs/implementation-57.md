# Issue #57 implementation record

Status: **in progress — P1-A is not complete**. Baseline:
`fc4fcfe64fdf86d03c07f82cf1d4889deb896801` (#56, #58, #59 included; #60 excluded).
Working branch: `feat/57-reliable-record-engine`. No PR or issue publication is authorized yet.

This is the implementation/validation entry point, not a replacement roadmap.
The product contract remains [issue #57](https://github.com/TheLudlows/seekpower/issues/57).
Current capabilities remain in [FEATURES](../FEATURES.md). Historical validation
remains in [VALIDATION](../VALIDATION.md).

## Latest local checkpoint — independent review corrections (completed locally)

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

## Stage 1 — persistence prerequisites (in progress)

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

### Remaining work / gates

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

## Stage 2 — record/reference-view integration (in progress)

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
