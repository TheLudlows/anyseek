# Current architecture

**English** · [简体中文](architecture.md)

This page checks module and runtime boundaries against main `3e0a9a4` (#203). See the [Record contract](record-model.en.md) for business invariants, the [storage protocol](storage.en.md) for durable formats and recovery, and the [roadmap](roadmap.en.md) for acceptance gaps and performance evidence.

AnySeek is a Rust storage and retrieval engine with a standalone HTTP service and an in-process Rust SDK. The web console uses the standalone HTTP service. The SDK runs on local disk and the host manages its lifecycle. The resource model is Tenant → Collection → Partition → Segment. Each collection has one durable partition and RecordPartition handles all data requests.

## Module boundaries

Paths below are relative to `anyseek/src/`. The standalone UI is at `console/` in the repository root.

| Layer | Code | Responsibility |
| --- | --- | --- |
| HTTP service | `server/record_service.rs`, `server/server.rs`, `server/main.rs` | Authentication, admission, requests and responses, startup recovery, and shutdown |
| Control authority | `catalog.rs`, `catalog/sqlite.rs`, `catalog/service_snapshot.rs` | Collection generations within tenant namespaces, backup ownership, control receipts, and service snapshots |
| Data authority | `record.rs`, `record/committed.rs`, `record/write.rs` | Original text and vectors, stable identities, versions, tombstones, idempotency, and durable batch commits |
| Coordination metadata contract | `storage/authority.rs` | Shared object identities, partition watermarks, build tokens, and publication outcomes. Contains no SQL or database connections. |
| Persistence | `storage/journal.rs`, `storage/partition.rs`, `storage/sqlite_authority.rs`, `storage/s3_compatible.rs` | Local WAL or SQLite coordination authority and immutable objects. Owner lock and recovery. |
| Certified layout | `record/anchor.rs`, `record/segment.rs`, `record/remote_checkpoint.rs` | Payloads, frozen Segments, manifests, checkpoints, and GC |
| Full-text derivatives | `record/lexical/runtime.rs`, `runtime/live.rs`, `memory.rs` | Pinned Tantivy Searchers, queryable tails, memory reservations, and background materialization |
| Vector/metadata derivatives | `component.rs`, `component/memory.rs`, `record/index_consumers.rs` | Component files, active identities, consumer progress, and memory lease transfers |
| Retrieval | `record/query.rs`, `record/lexical/runtime_query.rs`, `vector.rs`, `min_heap.rs` | Three query modes, global candidate budgets, ordering, and identity revalidation |
| Filtering and analysis | `active.rs`, `metadata_filter.rs`, `tokenizer.rs` | Active identities, shared metadata predicates, tokenization, and normalization |

`server/api_types.rs` and `server/api_schema.rs` define HTTP DTOs and OpenAPI. The Rust service does not embed frontend assets. The default build includes HTTP and local/S3 storage.

The Catalog uses four tables in `metadata.sqlite` for collections, backup ownership, and control receipts. Both local and S3 modes retain them. S3 also stores Record commit and reclamation coordination state in the same database. Local Records recover from native WAL and certificates. Full-text and vector queries run against Records and derived indexes. The storage contract owns shared coordination types. Records, object logs, object storage, and verification caches use these types. The SQLite adapter manages connections and transactions. Uploads and background builds validate format and table structure when opening an existing metadata database. Initialization occurs only when creating the coordination database. An upload reservation also returns confirmed object state. Backup reservation and pinning are atomic, and retirement state is rechecked after object I/O.

## Writes and queries

`/batch` handles synchronous HTTP writes. Single and multiple items use `write_batch` without separate paths. Rust `write` submits one request through the same batch executor. Each success means durable and visible, and the batch is not transactional. `/ingest` maps to `commit_writes`, which confirms durable business outcomes only, with independent full-text and vector consumers. There is one Record WAL. See the [Record contract](record-model.en.md#identity-versions-and-retries) for outcomes and retries.

Internally, delivery is synchronous or asynchronous. Single and multiple synchronous writes use the same batch publication path. Synchronous delivery waits for both consumers to publish. Asynchronous delivery returns after submitting consumer work. The batch still shares a WAL commit.

Ordinary writes reserve capacity for canonical values, containers, full-text publication workspace, and vector/metadata components before WAL. PreparedBatch binds to the actual writer/Inner lock identity. Planning retains only this batch's successful prefix and target records. Recovery and maintenance still validate fully. See the [ordinary-write commit anchor](record-model.en.md#commit-anchor-for-ordinary-writes) for the complete proof.

A single `resources::MemoryReservation` obtains both process memory and collection capacity for an explicit buffer. The same object follows the data. Growth takes effect only if both admissions succeed. Failure releases the new reservation automatically. Publication inputs, queues, full-text analysis and publication workspaces, writers, directories, and pinned Searchers use it, preserving foreground/background categories and existing caps. Canonical State, component memory, and request concurrency retain their own limits according to actual ownership. They do not transfer a common reservation across lifetimes.

Both local and S3 backends remain. The executor orchestrates planning, persistence, and publication. `record/write/persistence.rs` hides backend selection. Local commits WAL synchronously. S3 uploads immutable logs outside the state lock, commits the SQLite authority, and then updates local WAL cache. Unknown remote commits and post-durability publication failures still fence the partition for recovery.

### WAL-backed queryable full-text tail

The full-text read view combines a pinned native Searcher and bounded immutable analyzed deltas. Eligible writes publish those deltas after authoritative persistence, so synchronous writes can participate in queries before native materialization. Deltas retain term positions, field length, and exact record identity, and search with the native base under the same scoring, filtering, and ordering rules. They establish no independent business commit authority.

Background materialization owns the native writer, input, and reservations. It writes Tantivy files outside the foreground publication lock, then atomically hands off the native snapshot and remaining deltas. Pinned readers retain the old snapshot and leases. The tail is bounded to 128 rows and an explicit 64 MiB buffer budget. Deltas pinned by readers still count, and pressure creates backpressure before WAL. Records too large for queryable deltas use the native publication path with the same reserved workspace. Writes can still require file I/O.

The full-text consumer publishes through a queryable delta or native fallback and does not flush a second time at batch end. A receipt-only replay within the published watermark creates no empty consumer task. Commits with no new index rows, such as deletion, still advance progress.

Deltas from the asynchronous consumer can be queried before native materialization. A response from `/ingest` does not guarantee consumption through that commit. A materialization failure fences search. Durable receipts recover by reopening and replaying WAL. Checkpoint, segmentation, and shutdown wait for materialization when needed. Tail sealing does not replace an authoritative checkpoint.

### Query views and locks

Under a short Record guard, queries capture derived topology and evaluation_time. They score outside the guard and revalidate current Record versions, fingerprints, deletion, and TTL. Component ordinal is an address only. Consumer progress can differ. Ordinary queries do not present one immutable business snapshot across lanes, record reads, and statistics. Updates or expiry can shorten pages.

Pure full-text Count, default ordering, and queries sorted only by `_id`/`_score` release vector and metadata component read locks after capturing identities and the lexical snapshot. Hybrid and field sorting retain the relevant component locks. Query projection copies text after scoring and identity revalidation. Each query uses one blocking worker. Final identity and TTL checks still use current authority.

Lexical, Vector, and Hybrid share stable Record identity. Hybrid applies one RRF after global per-lane ranking across components. Candidate-window counts and field ordering inside ANN/Hybrid candidates do not guarantee exact corpus-wide results. Vector-only collections support metadata browsing. See the [search contract](search.en.md) for settings, scoring, and ordering.

## Segment ownership and file protection

A logical VectorIndex owns a bounded `segments` topology with one mutable tail and frozen Segments. Each RetrievalSegment pairs a lexical runtime with a vector/metadata component. There is no separate logical shard count or persisted positional ID.

| Owner | Holds | Lifecycle |
| --- | --- | --- |
| RecordPartition | Canonical data, version/TTL, receipts, and ordinal-to-stable-identity mapping | Authoritative commit and recovery |
| VectorIndex / RetrievalView | Topology, global Top-K and candidate budget, reader pins | Capture views and replace topology on rotation or merge |
| VectorSegment | Explicit range reads and writes, ANN clusters and quantization, physical row identity, active mask, and component admission | Build, freeze, merge, and eventually release one component |
| Tantivy runtime | Native Searcher, queryable deltas, native files, writer, directory owner | Full-text publication, materialization, and pinned readers |

VectorIndex schema, distance, and quantization are logical query configuration. Component configuration interprets local files. `indexed_*`, `committed_doc_count`, `uncommitted`, and `failed` describe progress. `active`, `delete_hashset`, and retirement tokens describe derived active state. Component admission accounts for held and pending-transfer leases. Rotation closes old-generation admission, while work already admitted can finish under its original binding.

A physical address uses `local_ordinal * segments.len() + slot` within the captured topology only. Rotation, merge, and recovery can remap the current layout. Pinned readers retain the original topology and revalidate by stable identity, revision/fingerprint, and retirement token. A topology position is not written to a shared Segment's `meta.id`.

Canonical payloads store original text and vectors. A full-text package records exact Tantivy version, schema, and analysis contract without storing a separate authoritative copy of text. S3 files are materialized and verified in full before use. There is no direct random remote Directory read. Certified packages verify the full immutable closure. Readers, native workers, builds, and backups protect files and partition ownership until release. See the [storage protocol](storage.en.md#local-segments-and-combined-views) for format tags, checkpoints, and reclamation.

### Native-directory I/O isolation

`record/lexical/directory_gate.rs` shares one I/O mutex per physical index directory. Different directories no longer share a process-wide file lock. Unix uses the canonical path's device/inode as the key. Other platforms use the canonical path. The global registry stores weak references and is locked briefly on open, without file I/O or capacity waits inside the lock.

Directory/Writer clones and pinned readers retain the directory lock via the existing owner. Memory, service and collection I/O limits, OS writer/owner locks, cancellation, and package pins still apply. External replacement of a directory in use is unsupported. This change removes mutex waits between unrelated directories. End-to-end throughput and Windows aliases require separate acceptance. [Native I/O evidence](../benchmarks/native-io/README.md)

A standalone snapshot borrows one I/O reservation for the file phase, shared by internal file operations. If materialization competes, it waits for existing capacity without extra reservation or relaxed limits. Native build and network operations run outside this file transaction. A fully certified standalone output does not cache a State container for live incremental proofs.

Under single-writer protection, S3 authoritative backup waits for accepted index I/O to finish, then uploads backup objects. The wait establishes I/O quiescence only and retains a separate derived-index failure mark. Backup still reads healthy SQLite/S3 authority.

## Authentication and lifecycle

Origence or the gateway handles user authentication and permissions. AnySeek checks the service credential derived from process configuration and uses trusted `X-AnySeek-Tenant` as the namespace. It stores no tenant registry or key verification material. Owned work and responses retain the original namespace. DataOutputPermit checks the data-failure fence on response and body-frame delivery.

Lock order is global snapshot barrier → collection lifecycle barrier → short Catalog metadata lock. Record futures run outside the metadata lock. Create, activate, and delete take the exclusive collection barrier, and full-service snapshots take the exclusive global barrier. Accepted owned tasks, output buffers, and subtasks retain capacity until they actually finish. Caller cancellation does not release ownership early.

Creation uses durable Prepare → partition initialization → Activate. Reusing a deleted name allocates a fresh internal generation. Old handles, receipts, uploads, and GC stay bound to the old generation. See the [Record contract](record-model.en.md#authorization-and-recovery) for business rules on names, authentication, and restore scope, and the [storage protocol](storage.en.md#final-reclamation-of-deleted-collection-generations) for physical reclamation.

## Failure boundaries

Production release uses `panic = "abort"`. A Rust panic, native crash, or allocation abort terminates the process. The supervisor restarts it to replay durable authority. `catch_unwind` only protects unwind builds and does not isolate production tasks. In-memory queue admission is not durable. A disconnected request still needs the original operation identity and full request to resolve an unknown result.

| Failure | Query and authority boundary | Recovery |
| --- | --- | --- |
| Full-text consumer or materialization fails | All search modes return RecoveryRequired. Records and committed receipts remain valid on healthy authority. Failing consumer progress does not advance. | Reopen and rebuild derived caches |
| Vector consumer or metadata component fails after partial modification | All search depends on active identity and filters. Failed components stay fenced and cannot serve canonical reads. | Reopen. Independent online metadata repair is unavailable. |
| Authoritative publication unknown or unhealthy | Reads and writes are fenced. A missing response does not prove no commit. | Recover authority, then rebuild caches |
| Process panic | API disconnect. Only durable authority is recoverable. | Restart, reopen, look up or retry with the original identity |

Independent consumption does not imply independent failure isolation. Do not advance cursors manually, clear shared errors to serve partial results, remove caches in use, or reuse failed writers. Failed recovery keeps the partition unavailable. Regressions cover consumer errors, failed metadata writes, partial vector commits, partial authoritative frames, and reopen. Execution results belong to the matching source revision.

The blocking worker holds query admission, resource scope, handles, and package pins until join. Client cancellation does not revoke these resources. Checkpoint journal install also transfers partition/writer guards and I/O leases to a blocking worker while still holding the publication lock. Moving work off an async executor does not prove the publishing pause disappeared or throughput improved.

## Service shutdown

SIGINT/SIGTERM first closes admission and then starts HTTP graceful shutdown. Accepted requests and necessary subtasks can finish. Later requests receive HTTP 503 and `code: shutting_down` before body read. HTTP/1 closes the connection and HTTP/2 does not send a Connection header.

Shutdown order follows.

1. Join the periodic maintenance coordinator. Stop new automatic checkpoint/cache maintenance and retries without admission. Already accepted consumers finish existing bounded flush and merge.
2. Drain owned HTTP, indexing, and blocking executor activity, including cancelled tasks, frames held after a body is dropped, and undelivered task results. Completed handles storing only status do not count as in flight.
3. Join index consumers, lexical materialization, and native writers. Release partitions, readers, package pins, index file handles, and Catalog.
4. Release the service root lock last. Report `Shutdown complete` only after full success. The directory can then be reopened or reclaimed immediately.

Shutdown does not force a checkpoint, TTL sweep, or GC and makes no stronger durability guarantee. It waits for outstanding transport owners. Repeated calls share a completion state. Reopening can recover consumer health errors. Worker join failure, native writer termination failure, or remaining partition owners report incomplete shutdown, retain the root lock until process exit, and produce a nonzero executable exit.

Cancelling or timing out a waiter does not cancel the coordinator or indicate completion. Wait again on the same handle. The executable has no forced drain deadline. External hard kill follows crash recovery. Keep the HTTP Tokio runtime running until `Shutdown::wait()` returns `Ok(())`. A stop notification, return from the HTTP server, or `Err` does not permit early reopen. Dropping the external completion handle does not cancel the running coordinator.

`finish_service_shutdown()` is an internal post-drain barrier. The caller must have stopped producers and released partition clones. It cannot shut down arbitrary library instances. The service uses a single owner and a shared process executor.

### Resource ownership and configuration

| Resource | Scope and configuration timing | Shutdown behavior |
| --- | --- | --- |
| HTTP Tokio executor | Owned by the executable and runs until shutdown success | Keep the runtime until success |
| INDEX_RUNTIME and maintenance WORKERS | Process-wide LazyLock runtime and two-worker semaphore | Join this service's work without resetting the shared executor or waiting for unrelated owners |
| Build MEMORY / MEMORY_LIMITS | LazyLock parses ANYSEEK_BUILD_MEMORY_BYTES and ANYSEEK_BUILD_SHARED_MEMORY_BYTES once, including invalid config | Release actual leases. Reopen does not reread or reset budgets. |
| Service Governor | Governor::from_env() captures lane configuration at each open | Release task, buffer, and I/O leases independently of process build budget |

Configure environment before startup. Runtime environment changes are not supported reconfiguration. Explicit reservations do not cover all allocations or hard-limit process RSS. Embedded multi-instance isolation with independent runtimes and budgets is not implemented.

Shutdown regressions retain new-admission refusal, cancellation and wait retries, frame and task ownership, immediate reopen, and retention of the root lock on failure. Linux also runs a real SIGTERM smoke test. Windows runs in-process ownership and directory reclamation regressions. Those do not establish POSIX signal or power-loss acceptance. See the [development guide](development.en.md) for commands and CI responsibilities.
