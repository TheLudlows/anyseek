# Persistence, object storage, and recovery

**English** · [简体中文](storage.md)

This page describes persistence, checkpoints, backup, and reclamation for local WAL/Segments and SQLite/S3. See the [Record contract](record-model.en.md) for request semantics, [service guide](../anyseek/src/server/README.en.md) for configuration, and [roadmap](roadmap.en.md) for implementation and remaining acceptance.

Both backends use Catalog 7 in `metadata.sqlite`. The S3 authority uses schema 3 and backup snapshot format 2. Startup validates existing roots and rejects unsupported formats.

## Local Segments and combined views

Every vector/metadata component package must contain revisions and SHA-256 fingerprints in one-to-one correspondence with keys. Load validates counts and active identities. Missing fields, empty fingerprints, and mismatched identity counts cause loading to fail. Empty components record explicit empty identity arrays.

`record/anchor.rs` certifies the immutable anchor, Segment, and payload closure. The canonical payload pool preserves complete original Documents including input vectors, shared by records and operation receipts through references. Derived Segments no longer store complete search text. Quantized vectors do not replace original vectors. Full text uses pinned native Tantivy 0.27.0 packages, while vectors and metadata use separate components. All data access uses RecordPartition. Current formats are RecordCheckpointV19, full and incremental state descriptors 6/7 (8/9 with group state), private vector and metadata descriptor 3, anchor manifest 9, and retrieval package manifest 3.

`record/view.rs` captures frozen Segment and mutable-tail topology under the partition guard, scores outside it, and revalidates version, deletion, and TTL under current authority. Full text builds cross-Segment N/df/field length context over selected active identities. Each lane ranks globally before fusion. Async consumers can progress independently. The authoritative visible_seq is not a complete index watermark. Query, record lookup, and statistics are not guaranteed to come from one immutable business snapshot. Final revalidation can shorten the page.

A certified checkpoint can reuse frozen segments. Compaction rebuilds a fully equivalent closure. Mutable-tail admission is bounded by bytes and 128 native updates. The queryable lexical delta serves queries before native materialization, with workspace reserved before WAL. Publishing visibility, handing off materialization, and sealing or rotating derived caches neither advance nor replace WAL authority. Recovery uses the complete committed suffix. See [architecture](architecture.en.md#wal-backed-queryable-full-text-tail) for tails and owner protection. An authoritative checkpoint builds, verifies, and syncs before publishing its selection certificate and installing the view. Post-publication application failure remains fenced. Retirement failure does not roll back committed state.

At the end of a private bulk build, `finish_build` reuses bounded small-Segment merging. Tantivy runs GC by active file references before sealing and collecting inventory. The 1,024-file cap remains. Never remove files still referenced by readers, builds, or backups. Insufficient budget or inability to merge further can safely reject a build. There is no unlimited build scale guarantee.

A partition guard and package pin protect local readers and builds. Full-text Searchers read ranges through pinned file handles. Files used by pinned Searchers, native workers, and vector and metadata views cannot be deleted until the last holder leaves. Backup exports independent authority without borrowing mutable paths that source GC can remove. Build and reclamation have explicit budgets. Full in-memory State and conservative admission remain, with no hard whole-process RSS cap.

Vector and metadata files keep their component format and publication order. A query or build owns an aligned vector window of at most 64 KiB and releases its reservation on completion. Filters read fixed-width fields. Custom sorting captures values for bounded candidates before comparison and stops the query on a read fault. Reads and sequential writes use file I/O admission, and buffers count toward the process and request limits. The operating system may cache ordinary file pages.

### Disk record directory component

One authenticated checkpoint descriptor selects the collection generation, primary index, record segments, payload objects, and committed cut. The primary tree uses 4 KiB ordinary-file pages with a byte-bounded page cache. Long ID suffixes live in a separate file and are verified on demand through their page references. A leaf authenticates the row at `(segment_id, doc_id)`. The doc ID is the zero-based segment ordinal. Rows carry the logical version, time metadata, a tombstone or candidate-validation fingerprint, and a payload location. The fingerprint and payload SHA-256 are independent. Point reads and pagination evaluate TTL at the caller's time. Only a body read allocates and verifies the selected payload range. Shared checkpoint owners retain the open files.

The recent delta accepts consecutive committed decisions, with newer records and tombstones overriding sealed rows. Admission and validation are atomic for each byte-bounded batch. A rejection preserves the prior cut. Compaction may change row addresses and retain payload references. The directory and segments switch together under one root. Old readers retain their original root.

New full checkpoint bases store current record metadata in the disk directory. State frames carry non-record metadata and operation history. Both reference the same payload pool and are authenticated by the same anchor. Existing full descriptors 6 and 8 can still be opened. Cold open currently decodes the selected base into live State before replaying the WAL. Readback from a pinned directory and recent-tail publication are under integration in [#224](https://github.com/TheLudlows/anyseek/issues/224).

## Authority and data ownership

In S3 mode, SQLite is the mutable commit coordination authority for Record partitions. It stores head/owner epoch, commit and index watermarks, immutable references, bounded idempotency coordination, dependencies, pins, retirement, and backup/restore coordination. Object storage holds text, original vectors, indexes, and WAL contents. Local Record logs, payloads, and indexes are rebuildable cache, while Catalog and S3 SQLite metadata are durable authority.

The root's `metadata.sqlite` has four Catalog tables, `catalog_meta`, `catalog_collections`, `catalog_backups`, and `catalog_operations`. Short transactions commit collection state, backup ownership, retirement, and control receipts. Full Catalog encoding has a 16 MiB budget and does not copy Record text, vectors, or indexes. No tenant/key registry or native Catalog journal exists. One Catalog owner file lock prevents two in-memory catalog views serving at once. Uncertain commit fences until reopen confirms state. Full-service archives pin the Catalog and collections through existing barriers. Restore excludes service credentials and preserves historical operation isolation.

S3 adds ten authority coordination tables to the same database for partition heads, idempotent commits, object registration/dependencies/pins, index builds, backup/restore, and retirement. Catalog and Record use separate short transactions outside network/object I/O. Collection lifecycle relies on intent, execution barriers, and recovery. SQLite uses WAL and `synchronous=FULL`. PUT stays outside transactions. The Record object backend holds a stable local owner file lock per partition. Two instances cannot own the same partition, while different partitions can share the database. Collection backup exports that partition's object closure and coordination records only. Restore bindings are per partition, and GC retires and deletes only its prefix. Database-wide reachability protects other partitions and pins. Local paths derive directly as `collections/{tenant_id}/{collection_id}/{generation}` with no name-to-path table. Objects use `anyseek/{tenant_id}/{collection_id}/` and `wal/records/lexical/vector/manifests` subpaths. Internal generation is in object IDs. Recreating a name allocates a new generation, and old work, receipts, and GC stay bound to the old one. Clients do not send generations. There is no automatic namespace or bucket ownership marker. Standalone services require separate buckets.

Ordinary writes register an upload identity, complete immutable PUT, verify length/bytes/SHA-256 by readback, then conditionally publish head and receipt in SQLite. Apply local authoritative state and either wait for or enqueue derived indexing. `/ingest` does not wait for both index consumers. Missing or invalid objects do not produce success from old caches.

## SQLite usage and connections

SQLite stores Catalog management metadata and S3 coordination. The default build includes `rusqlite`/bundled SQLite. Text and retrieval do not depend on SQL. `storage/authority.rs` defines object identities, partition heads, build tokens, and publication outcomes without binding them to a driver.

- Initialization creates new coordination databases. Open validates schema 3 and required tables and columns. Missing or incompatible schemas fail without changes. First S3 activation creates authority tables in the existing Catalog.
- Uploads and background builds use strict `open_existing`. They do not create missing metadata files or run table creation. Existing WAL mode is not set a second time. Connections retain `synchronous=FULL` and the existing busy timeout.
- Upload reservation returns observed durable state in the same transaction, avoiding another query. Registration reads digest, length, and state once. Backup control object reservation and pin run once and preserve fault-injection boundaries.
- Reservation observation only decides whether to reuse an immutable object. Revalidate in a short transaction after I/O. A stale observation cannot undo concurrent retirement. Head CAS, receipts, pins, and GC preserve their rules and do not hold transactions over network calls.

Catalog keeps required management transactions. S3 removes duplicated SQL, table-creation write locks, and type coupling. This does not establish throughput or P99 gains in production.

## Remote checkpoint and Segment publication

Reuse `anchor::Reference` certification closure and canonical payload pool. Segments contain native Tantivy files and separate vector/metadata components. The descriptor maps selected anchor, Segment, and payload files to bounded object blocks, retaining original lengths and SHA-256 without another text or vector copy. Immutable file identity and digest name blocks so unchanged files can be reused.

Under the partition serialization barrier, perform the following.

1. Build and verify the full local candidate anchor/index.
2. Upload and verify all members, then upload the descriptor and register its dependencies on each member block.
3. SQLite conditionally publishes manifest, indexed seq, and log base using expected head version, owner epoch, and exact committed seq. Clear covered live WAL tips.
4. Publish local recovery certificate and combined view. Local failures after SQLite publication remain fenced and reopening uses the remote selection.

Synchronous maintenance checkpoints the full current cut. The background Indexer builds a pinned committed cut and publishes conditionally while retaining new tails. Cold recovery downloads and verifies all selected files, loads existing Segments, and replays tails after the checkpoint. Even with equal local seq, the authority-selected manifest wins because same-sequence maintenance can alter history GC cut and other state.

### Bounded verified object cache

`*.object-cache` beside the SQLite authority is disposable derived state. Each database allows 256 MiB and 4096 blocks, evicting earliest insertions and not persisting access frequency. Cache identity binds endpoint/bucket/region, full object key, SHA-256 and length. Credentials are excluded. Revalidate length and hash on every hit. Delete corrupt entries as misses, and go to origin on access failure or lock contention. Buffer remote streams within certified length. Oversize, missing, or invalid objects are not cached.

Fully write and sync private temporary files, then publish atomically in the same directory. A nonblocking cross-process file lock protects scan, delete, and publish. Its file is beside the cache, so clearing cache cannot replace the lock inode. Temps and orphans are counted. Failed deletion releases no quota. Cache persistence is not required for startup. Read back from remote to confirm PUT and uncertain PUT. Cache cannot certify real durability. SQLite separately checks owner, head, and watermarks. Cache hits grant no read or output access.

Cache returns owned bytes only, never mmap. A materialized Segment has its own directory with existing certification and file protection. Eviction, corruption, or clearing the cache cannot delete Segment files held by a view. Prefetch and direct cache block mapping are unimplemented. Tiered admission and explicit buffer/I/O budgets exist, but cross-collection cache capacity and full isolation still need acceptance. Cache limits do not hard-limit service disk or RSS.

### Bounded background cut publication

`RecordPartition::start_index_committed` returns an owned job after admission. `finish` waits for its result and dropping the handle does not cancel it. `index_committed` wraps the wait. HTTP PATCH starts under the collection lifecycle barrier and waits outside. Owned work retains the original namespace without re-querying upstream permissions. Failure fencing still guards output.

At most two builds per process and one per partition. Excess explicit admission returns Busy, with bounded auto-maintenance wait below. Caller deadlines are at most five minutes and HTTP is thirty seconds. Snapshots and new tails use existing shared-memory admission separately. Tail capture is bounded by the smaller of journal budget and 64 MiB. Oversize cancels this build but preserves acknowledged writes. Build files and concurrent WAL share one disk scope. These are conservative estimates and not a hard RSS cap. Full State is resident. Full checkpoint builds a pinned cut. Time reclamation, log-pressure maintenance, and cache merging are separate and do not provide a general incremental checkpoint.

SQLite has one `index_builds` row per partition for monotonic build epoch and owner/cut/base manifest/deadline binding. Retain epoch after finish to prevent ABA without copying records or indexes. Commit registration and input closure pins together. Register and pin output before PUT atomically. Joint S3 backup returns Busy while building to exclude incomplete uploads. Object I/O uses a separate connection to the same SQLite file without holding the foreground authority mutex over the network. This creates no second head. Pins do not expire by time. Expiry only prevents publication and cleanup must match the exact epoch. Reopening under exclusive owner lock cancels unfinished builds and pins.

Build and certify outside the partition barrier. The local owned worker protects candidates and input files, explicit GC returns Busy, and synchronous maintenance cannot replace the base. At publication, replay bounded new tails under the guard, compare full logical state, and verify local base and deadline. SQLite rechecks durable build/owner epoch, base manifest and indexed seq, and deadline. Advance manifest/indexed/log base to the fixed cut only, preserving later committed seq and WAL tip without consuming a business sequence.

Sync the local certificate and full new suffix together in a private generation, then publish its directory. This keeps acknowledged later writes with the visible certificate. S3 local failures after SQLite publication remain fenced. Cold startup recovers remote manifest and full tail. Failed, expired, or panicked builds cannot serve old cuts as current. Panic closes output and requires reopening. Reclaim files after candidate file handles are released and tasks finish.

### Collection lifecycle and snapshot execution barriers

Lock order is global snapshot barrier → Tenant/Collection execution barrier → Catalog metadata. Ordinary work captures the tenant's collection then releases metadata before executing. Create, activate, and delete are serialized by tenant-scoped collection keys, and full-service snapshots and backup retirement retain their required barriers. Do not wait for an execution barrier while holding metadata.

Upstream authorizes users. AnySeek stores no tenant credential, Grant revision, or Catalog output authorization permit. Accepted work remains owned and caller cancellation does not release barriers early. Faults fence subsequent Catalog operations without creating a roles system. Record data protection remains independent.

Foreground, recovery, and background lanes use tiered admission with shared and per-collection buffer and I/O reservations. Owned work after cancellation and transport Bytes retain leases. S3 GET leases follow the returned buffer and do not end when download returns. PUT readback is remote. A proved pre-SQLite budget refusal is Busy. Other uncertain errors stay fenced. Existing reader/build/backup protection and GC closure remain. See [service guide](../anyseek/src/server/README.en.md) for limits and [roadmap](roadmap.en.md) for combined isolation acceptance.

## Log and receipt history retention

Full checkpoints contain current text, vectors, ordinary metadata, technical versions and tombstones, TTL/time watermark, recent receipts, certified GC cut, and independent recovery scope. They omit application source history. `log_base_seq` can advance to the checkpoint cut. New WAL chains start at the new base. Old immutable predecessor edges stay unchanged and protected by backups or other pins.

Internal history trim certifies GC cut and time watermarks with a **same-sequence full checkpoint**. Cold recovery gets the same boundaries without consuming a business sequence. Receipt GC checks both retention (default and minimum two days, configurable longer) and current/prior UTC retry window. Two days is earliest eligibility. SQLite publication receipts support exact tail/index cuts and recovery and cannot be cleared solely by that API window. Current format uses `RecordCheckpointV19`, `RecordAppliedV16`, `RecordAnchorV5`, `history`, and `RecordHistoryTrimV14`. Former notification quota placeholders and old trimming are removed. Old databases, WAL, and backups are refused without migration. External bootstrap and changes sync were removed.

Physical removal depends on certified checkpoint commit anchor, idempotency and receipt trimming state, current root, object reference closure, and reader/build/backup pins. Current single-host readers and builds use the partition guard. Multi-node leasing and external indexers are out of scope.

## Uploads, references, and GC

- Before PUT, persist key, length, and digest, giving GC an identity even after lost responses or exits. Registration alone does not commit a Record.
- Register dependencies for all remote checkpoint members and WAL predecessors. Current bootstrap is also a GC root.
- Traverse immutable edges from current SQLite roots and explicit pins. LIST does not decide commits, waiting a fixed time does not protect references, and no JOIN is added.
- Retirement is monotonic. A retired key cannot be registered, pinned, or published again. Certify retirement, DELETE, then record deletion completion. NotFound is idempotent delete success.
- Verified complete uploads use completion tombstones on deletion to avoid deleting all history each round. Unknown-upload inventory uses separate tombstones and repeats DELETE for late PUTs. A late object cannot become committed data.
- GC runs under the single owner's partition guard after publication of the whole view. Failed deletes remain retryable. `collect_s3_garbage` explicitly retries.
- Backup metadata objects register and pin atomically before PUT so GC cannot remove snapshots interrupted before descriptor creation.

Inventory and tombstones are coordination metadata, not payload copies. Long-term metadata size, cold recovery, and object request cost remain to be quantified. Unlimited capacity is not promised.

## Joint backup and restore

### Backup

Capture a logical SQLite snapshot and pin the complete object closure at one controlled cut. `backup_exports` stages **coordination snapshot bytes** so the original cut can resume with the same backup ID. It stores no text, vectors, or indexes. This copy is needed for recoverable export and is cleared on explicit retirement.

Metadata snapshot and descriptor use immutable objects. Backup ID derives from Tenant, collection generation, and Operation ID. Retry reuses original time, watermarks, and closure without including newer writes. Service pointers use private file + sync + rename. Unfinished pointers can resume. Pointer retirement and local PartitionJournal retirement are distinct. Startup retries certified Catalog retirements.

Retain at most 32 collection backup pointers locally. An interrupted same-ID directory occupies one slot and can be repaired at capacity. Reject new IDs before allocation. Retries remove only regular `.marker-<32hex>` temp files owned by this implementation and reuse the original remote cut.

### Restore

Verify descriptor, SQLite snapshot, and complete object closure. Interrupted import of the same snapshot can resume without overwriting another snapshot or reverting an advanced head.

Restore to a new collection does not copy an independently GC-authoritative view of source object references. Replay the source cut in a temporary import database with neither service nor GC, copy verified canonical files to **object IDs of the target collection's new generation**, and publish an independent target checkpoint. Source and target can compact, retire backups, and GC independently. Repeated restore cannot roll back later target writes.

Preserve technical versions, tombstones, original vectors, and receipts. Independent snapshots do not automatically inherit current deletion-history authority. Keep `independent_snapshot` and change historical recovery scope. Credentials are excluded.

Full-service archives reuse existing Catalog/local immutable archive protocol and add private materialization input for S3 collection backup. The archive is self-contained and independent of source bucket pins. Target process configuration supplies the service credential. On first S3 startup, publish the certified standalone archive to a separate target bucket. Collection backups inside an archive remain restorable.

Materialize private inputs one by one as certified journal paths. The Catalog opens, preflights, and exports each individually, releasing its indexes and budgets before continuing. It does not hold every retained collection or backup view at once. Clean private working directories on success or failure. Shared memory and archive size budgets remain.

Repeating a completed full-service backup verifies manifest and completion witness and returns the original receipt without rematerializing all inputs and exceeding HTTP deadlines. This retrieves a completed result. Restore still validates the full archive closure. Incomplete archives retain existing publication and repair procedures.

### Local WAL window and targeted commit restore

Ordinary HTTP restore selects the cut of the specified `backup_id`. The offline local tool captures the retained committed WAL suffix of the stopped source after its base backup. The backup must match the initial cut of the selected journal. Capture verifies source identity, base contents, group decisions, and contiguous business sequences. It returns a SHA-256 witness covering the entire window. Retain the witness independently with the paired backup. Once a checkpoint has reclaimed the old journal generation, use a window captured earlier.

```sh
cargo build --locked -p anyseek --no-default-features --example local_archive_cut
target/debug/examples/local_archive_cut capture SOURCE BACKUP ARCHIVE BUDGET_BYTES > witness.json
target/debug/examples/local_archive_cut restore BACKUP ARCHIVE NEW_ROOT BUDGET_BYTES TARGET_SEQ witness.json
```

Restore checks the entire window and base backup, replays through the selected business decision, and atomically publishes a new path. A group decision includes its members and original receipt at the same sequence. Later changes and maintenance at the target's same sequence are left outside the restored cut. The caller manages source paths, service catalog, and any paired Origence SQLite backup. Use `restore-chain BACKUP NEW_ROOT BUDGET_BYTES TARGET_SEQ parts.json` for up to 64 ordered captures. Each entry in the JSON list carries `path` and `witness`. The recovery tool verifies each matching boundary and every listed part, including parts after the target cut. Capture each generation before its live journal is reclaimed. [#224](https://github.com/TheLudlows/anyseek/issues/224)

## Maintenance publication and failure boundaries

In S3 mode, SQLite selects the remote manifest. Owner epoch and failure fencing block stale writers and untrusted output. Writes and TTL share cache append. After confirmed SQLite commit, cache failure returns `durable_pending` and keeps output fenced. Reopen recovers the original receipt. A genuinely unknown SQLite result remains `outcome_unknown` and cannot justify a new operation.

| Path | Durable publication and recovery authority | Failure/retry boundary |
| --- | --- | --- |
| Ordinary write and TTL | Immutable WAL plus SQLite head and receipt | Cache failure after commit does not roll back or expose stale output. Replay without cache. |
| flush, compaction, history trim, seal | Full object closure plus SQLite-selected manifest | Same-sequence maintenance still certifies remotely. Failure remains fenced. |
| Cold startup | Exclusive authority owner and certified bootstrap/manifest/tail | Validate bootstrap budget, configuration, and initial sequence before creating cache. Errors do not publish cache directories. |
| Backup and pointer | Original-ID SQLite snapshot/pin and immutable descriptor | Retry after interruption preserves the original cut. Pointer slot 32 can still be repaired |
| Restore, full-service archive | Verify source closure and allocate target generation (separate bucket for whole service) | Finish remote target publication before Catalog activation. Archives include retained sources. |
| GC and backup retirement | SQLite reachability, pin, monotonic retirement/delete records, and Catalog retirement intent | Retry failed deletes without reverting head or reviving identities. |

## Verification entry points and boundaries

- `cargo test --workspace --all-targets` covers Record/catalog, Segments, SQLite, upload/dependency/GC, and existing retrieval regressions.
- `scripts/record_service_smoke.py` covers the default local backend, HTTP, authorization, and recovery. See the [console guide](../console/README.en.md#verification) for the separate UI browser flow. `--cold-restarts --section …` applies the same semantics to S3, deleting collection and `*.object-cache` caches on each restart.
- `scripts/s3_storage_smoke.py --faults` connects a real process to a disposable HTTP fixture. Covers before and after remote publication, failed cache append, cache-free queries, history GC cut, four backup interruption boundaries, restore retry, independent GC, and full-service archive.
- `scripts/s3_acceptance.py` runs the storage matrix, six shared semantic groups with --faults, and immutable-object and joint-backup contracts. `--backend real` accepts only dedicated test configuration. See [development guide](development.en.md) for commands and reporting boundaries.
- The [roadmap](roadmap.en.md) indexes performance evidence and pending acceptance. See the [development guide](development.en.md#full-acceptance) for full-matrix triggers and coverage. Historical CI cannot validate current source or dependency changes.

HTTP fixture is distinct from real S3 certification. Actual endpoints, system/filesystem crashes, and hard power loss are separate acceptance items. SQLite is a single-host durable-volume authority. It provides neither cross-host HA nor zero RPO for lost authority volumes.

Catalog control performs one full budget and recoverability check before write transactions. Full archive format is 2. Restore retains only source Catalog cut/hash, source identity, and needed historical Operation ID isolation without copying full historical Catalog state. Databases and archives contain no key material. HTTP middleware authenticates once before body read, and handlers and owned work reuse that context. Collection execution still checks generation inside the lifecycle barrier.

## Final reclamation of deleted collection generations

Delete durably commits logical deletion and returns `physical_reclamation: pending`. One bounded service loop selects one old generation per second and deletes up to 64 local entries or 16 remote objects. Separately retry at most 16 monotonic object tombstones every 30 seconds. Admission refusal, owner/pinned readers, or backup protection defers work. Progress and failures are logged.

The Catalog first persists reclaiming intent. New whole-service backups exclude that generation, and existing independent backups are intact. Start only without valid collection backup associations. Internal generation mutexes do not occupy the new same-name collection's business lock. Hold the full-backup barrier until this round ends. After requests, tasks, and package readers release, atomically move the local root to stable trash. Resume from trash after interruption without reopening deleted Record journals.

S3 first revokes old-owner build state and verifies pins dropped, then retires the generation head and removes receipts, build/recovery metadata, and dependency edges of reclaimed objects. SQLite pages deletions by key cursor. Before each DELETE and completion recording, check active head and reverse-dependent head/pin reachability instead of reloading the full pending list. Each round removes at most 16 dependency edges. Keep a minimal tombstone for the old partition to prevent stale publication or same-identity recreation. A bounded service sweep retries uncertain PUT tombstones after Catalog removal to catch late objects. These are live coordination data. Batches bound file/object/dependency processing and returned candidates. Verification can still grow with active heads and dependence graphs, without a hard process CPU or RSS bound.

After physical cleanup, delete the old Catalog row and retain control receipts through their window and durable epoch rejection rules. Old jobs, GC, and delete receipts cannot change a new same-name generation. Ordinary clients use only names and must stop old requests/retries during delete/recreate. Cloud S3, system power loss, and Windows acceptance are separate from a local fixture.

### Automatic maintenance admission waits

The process still runs at most two maintenance workers concurrently. When initial automatic maintenance is refused by worker or collection admission, retain at most 64 pending items deduplicated by collection instance. The existing worker semaphore handles FIFO waits and wakeup on release without a new client request. If collection admission or an existing same-collection build prevents progress, release the worker and back off for 100ms. A pending item waits up to 300 seconds and holds only a weak reference, with no owner or reader/build pin. It never reopens a deleted or replaced collection by path. Accepted builds retain existing owned-task and publication protections.

`record_maintenance` logs `admission_deferred`, `admission_backoff`, `retry_started`, and `worker_finished`. Retries include `age_ms` and `attempts`. Completion distinguishes installed, execution Busy, recovery required, and other failure. Capacity or deadline exhaustions log `retry_capacity_exhausted` or `retry_stopped`. Keep maintenance intent and stop automatic waiting. Started execution failures do not loop automatically.

For stops or failures, verify capacity has recovered and diagnose storage/recovery. When required, use the normal recovery procedure before retrying authorized `PATCH /api/v1/index/{id}` checkpoint (library `index_committed`). A successful explicit checkpoint triggers unfinished maintenance again. Workloads beyond capacity 64, permanently unfinished jobs, and repeated refusals have no automatic fairness or completion-time guarantee. FIFO applies only to admitted, unexpired worker waiters.
