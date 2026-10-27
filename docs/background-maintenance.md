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

Known work: existing `indexer` prepares off-lock, but final replay/full digest is
inside the live partition guard and must be replaced. Its snapshot clone still
copies keys/container pointers O(N); capture/GC/anchor encoding must be measured
and bounded separately. Moving an async function while retaining the lock does
not satisfy this change.

## Verification plan

Pure trigger/overflow tests first, then deterministic isolated fixtures <=257
records: blocked worker plus concurrent write/read; same-sequence FeedTrim;
private cleanup; raw/decoded quota refusal before commit; original-operation
retry; candidate failure and cancellation; old reader pin; corrupt old closure;
CAS rejection/unknown; certificate failure; cold reopen and complete receipts.
Linux and Windows quick CI follow the code review. No stopped benchmark is
resumed. Larger measurement requires a fresh directory and a separate safe-space
preflight after correctness verification.
