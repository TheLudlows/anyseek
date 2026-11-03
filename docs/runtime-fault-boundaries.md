# Runtime fault boundaries

Production uses the workspace release profile, `panic = "abort"`. A Rust panic
terminates the entire process. A supervisor must restart it; opening a partition
replays its durable authority before serving it. `catch_unwind` remains defensive
for unwind builds; it is **not** a production isolation guarantee. Allocation
abort, native faults and process termination likewise cannot be caught this way.

An acknowledged committed receipt survives restart. A disconnected request has
an unknown outcome: retry the identical command and operation ID, or look up its
receipt. In-memory admission is not durability. Unpublished queued commands do
not survive process exit; clients retry them. Never assign a new operation ID to
resolve an unknown result. A failed recovery must keep the partition unavailable.

Run the real release subprocess check (not `cargo test --release`):

```sh
cargo build -p anyseek --locked --release --features fault-injection --example runtime_panic_probe
python3 scripts/release_panic_smoke.py --probe target/release/examples/runtime_panic_probe
```

The vector case acknowledges a write while its consumer is paused, panics after
mutating the vector component, requires SIGABRT, reopens, and checks receipt
replay. The queue case panics before publication, checks both submitted commands
were absent, and retries the original ID. A third case panics after publication
but before delivering receipts, then resolves the unknown outcome using the same
operation IDs. These small local tests do not prove
S3 crash recovery or every possible publication boundary. Fault markers exist
only in the explicit fault-injection build; no remote activation API is added.

## Current failure matrix

| Failure | Search channels | Canonical records / receipts | Watermark and recovery |
| --- | --- | --- | --- |
| Lexical consumer | All reject with RecoveryRequired | Existing committed authority remains valid while healthy | Failed lexical cursor does not advance; reopen rebuilds derived caches |
| Vector consumer | All reject with RecoveryRequired | Existing committed authority remains valid while healthy | Failed vector cursor does not advance; reopen rebuilds derived caches |
| Public identity / metadata mutation | All channels depend on identity, live membership and filters; failed component is fenced | Authority is separate; do not read failed component as truth | Vector and metadata share a component; no supported independent metadata repair |
| Authority publication unknown / unhealthy | Reject | Reads/writes fail closed; do not infer failure from missing response | Recover durable authority, then rebuild caches; no manual cursor advancement |
| Process panic | Every API disconnects | Only durable authority survives | Restart and reopen; same-ID retries resolve unknown outcomes |

This retains fail-closed search. Independent consumption is not independent
failure isolation: lexical search joins vector-owned identities and facet state.
There is no single-component online recovery contract. Do not clear the shared
error slot to enable partial results, delete live cache files, or reuse a failed
writer. Reopen owns the recovery lifecycle; package pins continue to protect old
readers and delayed retirement. New writes after an index failure must not be
advertised as searchable merely because their canonical commit succeeded.

Regression coverage includes consumer errors, real metadata mmap growth failure,
partial vector commit failure, and existing authority partial-frame/reopen tests.
The new consumer matrix checks receipts, canonical reads, all search modes, the
failed cursor, reopen and idempotent retry. Execution results belong in the PR;
this document does not claim unexecuted tests passed.

## Blocking execution

Each query moves its search execution to one blocking worker. The existing
service governor (global and per-collection limits) and partition admission bound
accepted requests; no second fixed scoring semaphore is introduced. The owned
query retains admission, resource scope, query handles and package pins until the
worker joins, even after client cancellation. Result identity and TTL
checks still run against current authority after scoring.

Checkpoint journal installation moves its owned partition/writer guards and I/O
loan to a blocking worker under the existing two-job maintenance admission. It
still holds the publication lock: executor offloading does not remove that lock
pause. Publication errors retain their existing recovery handling.

Small regressions check scoring responsiveness on a single-thread runtime and
query/checkpoint ownership after cancellation. The checkpoint test verifies locks
and I/O ownership only; it does not measure executor scheduling or throughput.
Release panic subprocesses run in the existing manual full-acceptance workflow,
not an additional PR workflow. No latency or throughput improvement is claimed.
