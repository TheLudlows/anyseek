# Move maintenance off the foreground critical path

Implementation in progress; no new throughput result. The previous 71,900-record
run hit the 30 s deadline while full checkpoint/native rebuilding ran inside the
partition lock. Exact fallback counters were absent; the 16-package trigger is
plausible, not proven. Two independent decisions must be diagnosed: full State
base construction and whole-native-index rebuilding.

## Triggers and observations

- Tail admission: more than 128 native updates, or insufficient tail build quota.
  Direct seal is unavailable for private cleanup, absent certified State capture,
  selected + pending + successor >= package bound, or an unstable tail directory.
- WAL: recoverable physical room pressure, including reserved generation bytes;
  malformed/oversized frames must remain rejection, not perpetual maintenance.
- S3: retained WAL block count/bytes/object bounds. Private lease cleanup and
  same-sequence FeedTrim can independently enter complete publication through
  time advancement, including read APIs. Explicit checkpoint/snapshot/takeover
  are distinct operations, not hidden write fallbacks.
- State base: explicit rewrite, payload pool ceiling, private cleanup, absent
  predecessor/journal/captured proof, 32 deltas, empty suffix, or manifest byte
  fallback. Native reuse has its own selected + pending bound.

The first slice logs fixed reason names, sequence/count/byte counters and scoped
phase times. No document, query, operation identifier, object path or credential
is logged. Exit timing includes failures; nested timings overlap and are not
summable. Logging is observability only, not a performance fix.

## Required implementation boundaries

1. Merge maintenance requests per partition. Admit a bounded owned worker, not
   an unbounded task queue. At hard pressure, reject *before accepting a new WAL
   operation* with retryable Busy; never synchronously rebuild to admit it.
   Existing operation receipts remain retryable. Oversized operations stay
   permanent Rejected. Worker admission/deadline failure must release ownership.
2. Capture an immutable committed cut and protect its files/State owners. Do not
   start by recursively running time maintenance. Foreground reads/writes use
   their live view while the job builds and uploads under separate leases.
3. Extract a private replay context containing only State/view/addresses/graph/
   configuration. It must not own the live journal, remote authority, output
   barrier or background slot. Replay and complete State proof run off-lock.
4. Capture the complete ordered suffix and a mutation token under lock; include
   same-sequence events, byte/count bounds, job identity and anchor identity.
   Check the token again before install. If newer writes exist, catch up in
   bounded rounds. At hard pressure, backpressure permits the worker to catch up.
   Raw buffers, decoded entries, retained candidates and shared State leases must
   all remain accounted for until their last owner drops.
5. Install only an authenticated candidate matching the current cut. Source file
   authentication cannot be replaced by a seq token, filename or mtime. Preserve
   old-view/payload pins, owner epoch/head CAS, WAL/certificate ordering and
   output fences on uncertain publication. Explicitly short installation still
   contains durable certificate I/O / remote CAS; it is not zero-I/O.
6. Destroy failed candidate handles before GC. Retain selected files and old
   readers through failures/cancellation. Reopen selects certified authority and
   replays all committed suffix entries. No history truncation or limit increase.

Implemented locally: a private replay context excludes publication authority.
Candidate replay, full State comparison and feed-cache construction now run
outside the partition guard; installation rechecks the anchor and a suffix
revision that also counts same-sequence events. Catch-up is bounded to eight
rounds. Automatic pressure coalesces one owned job per partition, with two workers
process-wide and no queued tasks. Hard-pressure maintenance pauses **new accepted
writes** with Busy; it does not promise concurrent writes into a successor tail.
Normal reads continue through the old view. A test barrier inside native package
construction (after building the private index, before sealing) proves that old
searches and a Busy write return within two seconds while construction is paused.

Private erasure and remote FeedTrim use an authenticated same-sequence Intent:
verify the original cut, transform only the private candidate, prove the expected
transition, then install the certificate and feed cache together. Time advancement
is blocked while such an Intent is pending, so reads or original-operation retries
requiring time maintenance can also return Busy. No TTL/authorization check is
skipped to keep a read available. GC failure retains a separate cleanup obligation;
it does not mutate the already certified State. On reopen, old generation and
object retirement inventories remain available to normal GC.

Automatic S3 maintenance authenticates the captured old descriptor, C/I file
closure and ordered C-to-D WAL using uncached reads on an independent connection.
The final owner/CAS check remains mandatory. Resource Busy does not fence the
partition; detected remote corruption fences it and never advances authority.
This is protocol-fixture coverage, not a real cloud S3 validation.

Remaining lock work: snapshot capture still copies keys/container pointers O(N);
capturing committed raw frames, initial/final GC and anchor encoding remain under
the guard. Direct tail sealing still authenticates its source synchronously.
Durable certificate I/O and remote CAS also remain guarded. Therefore this slice
removes full rebuild/replay from the guard but does not claim every foreground
maintenance interval is short or that the previous 30-second issue is solved.

## Verification plan

Pure trigger/overflow tests first, then deterministic isolated fixtures <=257
records: blocked worker plus concurrent write/read; same-sequence FeedTrim;
private cleanup; raw/decoded quota refusal before commit; original-operation
retry; candidate failure and cancellation; old reader pin; corrupt old closure;
CAS rejection/unknown; certificate failure; cold reopen and complete receipts.
Linux and Windows quick CI follow the code review. No stopped benchmark is
resumed. Larger measurement requires a fresh directory and a separate safe-space
preflight after correctness verification.

## Current isolated evidence

- Trigger/admission, actual-build barrier, pre-acceptance Busy, receipt retry,
  no-progress WAL rejection, private erasure and injected GC retry: four tests
  passed; largest fixture 129 records. No large load was restarted.
- Same-sequence S3 FeedTrim and cold reopen: one test passed.
- Corrupted remote selected manifest, payload chunk and WAL: one test with three
  independent two-record fixtures passed; authority remained unchanged and output
  was fenced.
- These are local Linux results. Windows and the complete regression matrix have
  not validated this slice. Existing callers/tests that assume maintenance never
  returns Busy require bounded retry handling, without weakening their assertions.

Follow-up compatibility checks retain existing assertions with bounded retries of
unaccepted Busy using the same operation ID: all six staging/lease tests passed;
WAL unknown-publication, fixed Searcher at update 129, remote byte pressure,
64-to-65 remote tail pressure and backup/GC/same-sequence preservation all passed.
The 256-record frequent-term integration fixture passed. Test-only certificate and
post-certificate GC fault points now cover the background publication route too.
The quick CI list includes seven new background/Intent/authentication regressions;
no previous case was removed. Busy retry is never used for OutcomeUnknown or
DurablePending. These results do not substitute for Windows or full service tests.

The low-level journal regression explicitly asserts `PublicationUnknown` after
writing the new certificate with its suffix, then verifies the complete selected
suffix after reopening. This complements the worker-level unhealthy/output-fence
assertions. `server,fault-injection` type checking passed before the final early
Busy fast path; the final S3-feature library build and seven focused tests passed
after that path was added. Early Busy can precede validation of an unknown invalid
or expired operation during pressure; known IDs still receive normal replay and
time checks. No command is accepted by that fast path.

## Entry audit and remaining work

The bottom-level `maintain_index_tail` fallback itself now requests background
maintenance and returns Busy; correctness does not depend on a second precheck in
a write-only wrapper. Implicit native/package, local WAL and remote WAL pressure,
private erasure and S3 same-sequence FeedTrim are covered. Explicit
`compact_journal`, snapshot/takeover and S3 restore/activation still use complete
publication as management operations; they are not ordinary write fallbacks.

New timing names separate `background_capture_locked`,
`source_frames_capture_locked`, `snapshot_clone_locked`,
`catchup_capture_locked`, `candidate_recheck_locked`,
`candidate_install_locked`, `background_journal_install_locked` and
`background_remote_cas_locked`. They start **after acquiring** the guard and do
not measure queueing. `background_source_proof` and
`background_remote_source_authentication` run off-lock. `gc_reclaim` can run during
open or skip work under a build pin; the name alone does not prove a locked scan.
Nested phases overlap; errors also emit exit timings. The fixed worker-finished
outcome distinguishes installed, busy, recovery_required and other failures.

Small Linux debug probes (not production performance):

| Phase | Local 128→129 fixture (ms) | One-write S3 memory fixture (ms) |
| --- | ---: | ---: |
| Initial locked capture | 2.828 | 0.585 |
| WAL frame capture (nested) | 0.264 | 0.019 |
| State clone (nested) | 0.091 | 0.020 |
| Source proof off-lock | 22.943 | — |
| Remote source authentication off-lock | — | 0.779 |
| Candidate authentication/load off-lock | 122.967 | 79.202 |
| Catch-up locked capture | 0.103 | 0.060 |
| Final locked recheck | not separately instrumented in this probe | 0.001 |
| Locked installation | 1.010 | 1.275 |
| Journal install (nested) | 0.531 | 0.381 |
| Remote CAS (nested) | — | 0.404 |

The local native construction phase includes an intentional concurrency barrier;
it cannot be used as a throughput benchmark. S3 here is an in-memory protocol
fixture with SQLite, not a network/cloud latency measurement. Neither probe
restarts the retained experiments or establishes large-scale throughput.

Minimal follow-up order, not implemented promises:

1. **Capture:** immutable container roots and an owned sealed WAL extent could
   eliminate O(N) key copying under the guard. They must retain per-value and
   container leases, a full same-sequence revision and Windows cursor ownership;
   a second handle reading a live owner-locked WAL is not an acceptable shortcut.
2. **GC:** prepare an authenticated retirement inventory, retain reader/build and
   backup pins, and delete only admitted retired identities off-lock. Keep a
   durable cleanup obligation across failure/reopen. Never delete a path merely
   because it was absent from a stale snapshot or trust filenames/mtime.
3. **Direct-seal authentication:** authenticate a pinned cut off-lock, then
   recheck cut/revision/owner before handoff. Every publication must still perform
   the required fresh local/remote checks; this is not permission to defer damage
   detection until a later read or replace it with cached hashes.
4. **Install I/O:** prepare and fsync an undiscoverable generation outside the
   guard, then validate the exact suffix and perform the authority transition.
   Do not expose a PUBLISHED marker before the current cut is certified. Local
   publication sync and SQLite CAS remain durability costs; preserve unknown
   outcome fences instead of acknowledging early or increasing timeouts.

Each step needs its own bounded fault/concurrency checks before measuring a new
large fixture. This staged PR does not claim those remaining steps are complete.
