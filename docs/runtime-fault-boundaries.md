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

## Service shutdown

SIGINT/SIGTERM establishes the stop boundary before HTTP graceful shutdown.
Requests admitted before that boundary may finish, including required child
tasks started after the signal. New requests reaching admission receive HTTP
503 with `code: shutting_down`, before their bodies are read. HTTP/1 responses
close the connection; HTTP/2 does not receive a `Connection` header.

Shutdown then joins the periodic maintenance coordinator and drains accepted
activities across the existing HTTP, index and blocking executors. An activity
outlives a cancelled request, response frames retained after their body is
dropped, and an undelivered task result that still owns resources.
Completed consumer/maintenance handles retain only status values;
their existence is not outstanding work. New automatic checkpoint/cache-maintenance
scheduling is suppressed, and deferred admission retries stop without cancelling
an admitted job. Accepted consumers finish their existing bounded flush/merge
sequence. There is
no forced checkpoint, TTL sweep, GC pass or extra durability promise: accepted
work still uses the existing committed-receipt and unknown-outcome rules above.

After producers drain, shutdown joins retained index consumers and native writer
threads, then drops partitions, readers, package pins, mappings and catalog
resources. The service root lock is released last. Only this completed sequence
reports `Shutdown complete`; the same root can then be opened immediately.
Remaining transport owners keep shutdown pending until they are released;
repeated shutdown calls share one completion. Consumer health errors remain
recoverable through reopen; a failed worker join, native writer termination or
untracked partition owner instead reports incomplete
shutdown and keeps the root locked until process exit. The executable exits
nonzero on these errors and relies on existing recovery.

Cancelling or timing out an internal shutdown waiter does not abort the
coordinator, release ownership or imply closure. The same completion handle can
be awaited again. The executable has no forced drain deadline; an external hard
kill uses crash recovery, not the graceful-close contract.

This is the service's single-owner lifecycle, not a new embedded multi-instance
API. Process-wide executors and native memory limits remain shared and fixed by
their existing initialization. Per-service admission stays separate from those
budgets; zero governor slots alone does not prove that all owners have exited.

The HTTP Tokio runtime must stay alive until `Shutdown::wait()` returns `Ok(())`.
The coordinator is spawned on that runtime; dropping the runtime while close is
pending is outside graceful shutdown, even if a waiter was previously cancelled
or timed out. Dropping every external completion handle does not cancel the
coordinator while the runtime remains alive. A stop/drain notification, HTTP
server completion, or an `Err` result alone does not permit immediate reopen or
directory removal. The partition's `finish_service_shutdown()` is an internal
post-drain barrier: its caller must already have stopped producers and released
partition clones; it does not itself stop admission or close an arbitrary library
instance. The service root barrier is the final successful completion above.

### Resource ownership and configuration

| Resource | Scope and configuration lifetime | Effect of service close |
| --- | --- | --- |
| HTTP Tokio executor | Owned by the executable; kept alive through shutdown | No runtime destruction before successful close |
| `INDEX_RUNTIME`, maintenance `WORKERS` | Process-wide lazy runtime and two-worker semaphore | Join this service's tracked work; do not destroy/reset shared executors or wait for unrelated owners |
| Build `MEMORY` / `MEMORY_LIMITS` | Process-wide accounting; `ANYSEEK_BUILD_MEMORY_BYTES` and `ANYSEEK_BUILD_SHARED_MEMORY_BYTES` parsed once by `LazyLock`, including invalid configuration | Release actual leases; reopening does not reread these variables or reset limits/accounting |
| Service `Governor` | One governor shared by this service's collections; `Governor::from_env()` snapshots lane settings on each service open | Release retained task/buffer/I/O leases; this is separate from process-wide build limits |

Set environment configuration before starting the service. Runtime environment
mutation is not a supported reconfiguration mechanism. A new governor can read
new lane settings on a later open; this does not reconfigure the process-wide
build budget. These limits account for explicit reservations, not all allocations
or a process RSS cap. Independent embedded instances with isolated runtimes or
budgets remain outside the supported service contract; no `EngineContext` or
multi-instance isolation claim follows from this shutdown barrier.

### Issue #167 acceptance boundary

| Acceptance item | Existing implementation / regression and remaining scope |
| --- | --- |
| Shared versus isolated instances; stable budget configuration | Shared process model above is intentional for the single-owner service. Build limits are parsed once; governor settings are snapshots. Configurable embedded isolation is deferred, not implemented or tested by service close. |
| Stop, drain, timeout, unresolved persistence | `shutdown_refuses_new_roots_but_finishes_admitted_ingest_and_consumers` and `shutdown_keeps_cancelled_owned_write_and_allows_wait_retry` cover refusal before body reads, retained accepted work, timeout/retry and same-ID receipt recovery. Timeout never means rollback. |
| Reopen and safe reclamation | `shutdown_waits_for_transport_owners_and_reuses_completion`, `shutdown_waits_for_transport_frames_after_body_drop` and `shutdown_failed_owner_keeps_root_until_process_exit` cover completion ownership, immediate reopen/removal and terminal failure retention. Crash recovery remains a separate contract. |
| Minimal lifecycle interface | `stop_accepting()` establishes admission stop; `shutdown()` transfers the owner and returns repeatable completion; only successful `wait()` permits root reuse. #189 already implements this sequence. |

Under this service boundary, #167 can remain concluded after review of these
clarifications. Its original configurable embedded isolation item is explicitly
deferred, not an additional capability delivered by #189. POSIX signal execution,
Windows ownership tests and historical CI results remain distinct evidence;
the table names regressions, not a claim that every platform was rerun here.

The bounded shutdown tests run in the existing core CI lane. Linux additionally
runs `scripts/record_shutdown_smoke.py` against a fault-injection executable: an
accepted write loses its HTTP caller, SIGTERM arrives while it is paused, both
index consumers must finish, idle keepalive connections close, and immediate
reopen preserves the original receipt. Windows runs the in-process ownership,
wait-retry and immediate-directory-removal regressions, not POSIX signal tests.
