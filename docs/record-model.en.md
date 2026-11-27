# Record semantics and consistency

**English** · [简体中文](record-model.md)

The [service guide](../anyseek/src/server/README.en.md) defines HTTP requests, fields, quotas, and examples. This page records invariants implementations must preserve. See the [roadmap and acceptance evidence](roadmap.en.md) for delivery status and the [storage protocol](storage.en.md) for persistence.

## Identity, versions, and retries

Locate collections using the trusted tenant header after service authentication and the validated collection name. Local directories derive directly from name and internal generation. Reusing a deleted name assigns a new generation. Receipts are scoped by Tenant, generation, and Operation ID regardless of the credential value. Old job handles can operate only on the old generation. Clients must stop old requests and retries before deletion and recreation, since names alone cannot distinguish old requests arriving after recreation. Collection restore takes an unused name and fresh generation and retains versions and receipts from the backup.

`IndexMetaObject` stores retrieval settings and display name without a numeric index ID for discovery. The Tenant, collection_id, and generation locate a collection.

`record_id` is stable inside a collection. Internal DocID is an index address. Deletes retain tombstones. Recreation continues version increments so reused identities cannot make a stale condition match new content. `document` is a complete replacement and explicit null deletes. `schema.store` controls search projection without affecting durability of original data.

Before sending, save the complete `operation_id` and semantic request. An identical identity and request return the original receipt without advancing versions. Different contents under the same identity conflict. The supported window is the current and previous UTC day. The durable epoch watermark prevents a clock rollback from reviving requests. Timeout, missing receipts, expiry, or later refusal cannot prove an earlier request did not commit. Do not guess by issuing a new ID.

| Result | Semantics and action |
| --- | --- |
| Synchronous `/batch` per-item applied receipt, or successful internal `write` | The authority is durable and synchronous derived application is complete |
| Committed item from `/ingest` or Rust `commit_writes` | The authoritative result is durable. Full-text and vector consumption is asynchronous and the sequence may not yet be searchable. [Progress and retries](../anyseek/src/server/README.en.md#writes-with-asynchronous-indexing) |
| rejected / conflict / busy | Validation, precondition, or admission for this attempt. It leaves any earlier unknown outcome unresolved. |
| outcome_unknown | Persistence or response outcome is unknown. Look up or retry with the original identity after recovery. |
| durable_pending | Authority is durable and derived application is incomplete. Fence until recovery. |
| recovery_required / storage_error | Cannot continue trusted service. Do not fall back to stale cache or reset to empty storage. |
| operation_expired | Outside the supported window. A new identity cannot resolve the old outcome. |

Receipts become GC-eligible after at least two days by default, outside the retry window and covered by the certified history floor. This is not a cleanup deadline. Explicitly longer Rust settings persist, and omitted values use the two-day default. HTTP creates use the default without a retention option. SQLite publication receipts serve recovery and other purposes and are not deleted directly by this window.

An independently owned task takes over durable work. Cancelling HTTP does not revoke accepted work. Quotas cover original records, the full recovery closure, operation receipts, and background builds as well as visible text.

### Batch write orchestration

HTTP `/batch` validates the request and encodes per-item responses. Core `write_independent_batch` takes over ordered, non-atomic execution of at most 100 items. One detached owner holds the request, resource context, and work after cancellation, using existing engine windows of up to 64 items and 4 MiB. `write_batch` and asynchronous `commit_writes` still admit up to 64 items each.

Advance after definitive refusal. For Busy, only items known not attempted use scalar admission. After scalar success, regroup the unattempted suffix. Do not retry unknown items in the batch. Fatal interruption preserves known receipts and stops subsequent attempts. Progress uses input slices and offsets without copying the remaining suffix. Measure each engine window once and subtract consumed bytes as the prefix advances. Clone only commands admitted to the planner inside its reserved window. Synchronous delivery still waits for both consumers, and asynchronous receipts confirm only the durable authoritative outcome.

### Atomic group replacement

`replace_group` reads the target head, the union of its old and requested members, and the exact receipt for the operation ID. It prepares the member values, ownership and receipt updates, checkpoint byte delta, TTL reserve, and derived index admission from those changes. An empty group retains its version and retired ownership.

One `GroupApplied` WAL decision precedes whole-group publication. The partition writer and publication guard protect member installation and both index channels. A successful return confirms the commit and both visible watermarks. A failure after durability fences the partition for recovery. Replay reads the same decision, and the ordered receipt array and checkpoint format are unchanged. ID and expiry indexes are rebuilt from retained receipts on open and after maintenance collection.

## Search engine and application boundary

Source versions, chunking, sections, evidence dependencies, and relationships belong to upstream applications and can be stored as ordinary document fields. AnySeek provides no source/derived write, historical text excerpt, locator verification, graph traversal, or graph fusion endpoints. General full-text/vector hybrid retrieval, conditional updates, and late vectors for exact text versions remain available.

See the [ordinary-write commit anchor](#commit-anchor-for-ordinary-writes) for commit ownership and batch-local incremental planning. See the [roadmap and acceptance evidence](roadmap.en.md) for implementation and verification scope.

## Time, TTL, and query views

Queries apply TTL at capture, then recheck final candidates at the latest process evaluation time. Expiry, update, or deletion may shorten pages. `count_total` is a mode-specific observation of the captured index, not a precise final count. BM25 corpus statistics may still include expired rows until retirement. A change in seq or crossing the view's earliest TTL invalidates the cursor and requires restarting iteration.

A read only advances in-process evaluation_time. It neither persists expiry/history trimming nor takes the partition writer to mutate authority. Background TTL reclamation uses the existing WAL or remote protocol. IDs can be reused with increasing technical versions. Old vector patches cannot apply to new text. The service selects at most one loaded partition per second for time maintenance. Busy, budgets, and I/O can defer reclamation without a fixed physical-deletion deadline.

evaluation_time within one process is monotonic and bounded by persisted write/maintenance time. Restart retains only that persisted lower bound. Unreclaimed text could reappear after wall clock rolls back across its deadline. Large clock errors return `clock_uncertain`. Durably reclaimed versions cannot reappear spontaneously. WAL, backups, and old snapshots can still hold text. TTL does not guarantee privacy erasure.

An opt-in event-time window starts at the current certified local base. It pins the canonical row overlay, which includes every selected manifest delta and the published WAL suffix. The query does not replay the log. A canonical identity shadows its old base key, including updates that move outside the window and tombstones. Pure time requires the base and canonical rows at the same cut, with a selected-root and TTL check before delivery. Exceeding the candidate or suffix budget rejects the entire window. See the [service guide](../anyseek/src/server/README.en.md#opt-in-event-time-windows) for HTTP and SDK entry points.

## Authorization and recovery

Origence or the gateway handles user authentication and authorization. AnySeek checks only the derived service credential in `apikey`. Data requests also need trusted upstream `X-AnySeek-Tenant`. Tenants are namespaces, with no registration or tenant-key storage, issuance, or revocation. The service credential has service-level scope. Before forwarding, the gateway verifies authorization for the selected tenant and administrative action. The tenant header is not a user credential.

SQLite Catalog saves collections, backup ownership, and control receipts. Collection changes and receipts commit in one short transaction. Namespaces isolate collections, backups, and receipts, so names and IDs may be reused across tenants. There is no User, Project, Collection Grant, or role model.

Accepted work and outputs retain their original tenant scope and lifecycle barriers. Record validity is still checked at delivery. Full service restore preserves namespace, collection and backup ownership and separates historical management operation IDs. Credentials enter neither database nor backup. The target reads its own service credential configuration. Standalone collection backups retain versions and receipts.

Standalone restore only recovers saved backup data. It does not catch up on later writes or deletes or provide cross-service active-active operation.

Each collection has one partition. Cross-collection transactions, source dependencies, SQL/JOIN, and full graph language are unavailable. Linux/Windows process-fault regressions do not replace crash or power-loss tests.

## Commit anchor for ordinary writes

This section specifies commit ownership and incremental planning for ordinary writes. Performance gains need production-path measurement at the matching revision. Recovery, reconstruction, maintenance, and diagnostics retain full State/SourceProof validation. Ordinary writes neither cache a verified flag nor infer unchanged state from equal sequence alone.

### Exclusive scope

An ordinary write takes the partition's owned writer guard before capturing State and retains it until durable publication, index batch follow-up, and error/unwind fencing are complete. PreparedBatch borrows that actual guard and cannot commit after release. Prepare verifies the guard's mutex. Commit verifies both writer and Inner guards are from the target partition. base_seq checks continuity only and is not proof of the baseline.

An instance accessible to callers can modify canonical State only through the following paths.

| Entry | Mutation ownership and verification |
| --- | --- |
| Ordinary Applied publication | Same writer→Inner. Plan and admit, persist, then apply items from the same immutable Prepared values. |
| Automatic TTL / local history trim | writer→Inner during capture. Use the existing WAL/remote commit and failure fencing. |
| Maintenance GC / remote trim | Mutate a private candidate, then install under writer→Inner after complete SourceProof and State comparison. |
| Background candidate State install | Retake writer→Inner, compare anchor and tail revision/count/bytes, then publish certificate and replace. A sequence-only comparison is insufficient. |

Some create/open/recovery and S3 reconcile/restore constructors have no writer guard while the object remains unpublished to callers. Export modifies only private clones. These do not provide concurrent mutation paths for exposed instances.

query/get/page/statistics observe_time only changes transient evaluation_time and never State. Consumers update only derived state. Health, index failure, output fence, maintenance requests, and reservations can still change and remain commit checks.

### Limits on the writer itself

The exclusive lock does not stop its owner mutating State. Helpers between prepare and commit must obey the following contract.

- maintain_index_tail only drains, seals, or rotates derived indexes. It cannot trim/GC or install canonical State.
- ensure_journal_room and remote tail room validate or request a checkpoint before returning Busy. They do not checkpoint synchronously.
- prepare_remote_batch and uploads outside locks do not mutate canonical State. Remote object upload has side effects and keeps head/owner checks and unknown outcome handling.
- admit_containers can increase live State reservations but cannot change serialized content.

If synchronous maintenance is added to these helpers, reestablish the commit anchor. The current proof would no longer apply.

### Preservation of structural validity

The lock only proves the captured anchor stays unchanged. The induction below preserves validity without rescanning all records and receipts for each ordinary write.

1. create/open validates settings, numeric vectors, identity/lookup, capacity, TTL and commit time, history, and recovery sources before exposure. Untrusted WAL, anchors, and payload are still fully validated.
2. mutation::prepare_input validates current record and final vector-patch document fields, size, numeric vectors, local conditions, time, operation window, version overflow, and full-checkpoint byte budget. Recent-operation retries compare full semantic requests and do not make a new decision.
3. state_memory::Prepared obtains an independent value lease before allocation. PlanningView reserves space for this batch's maps, and the caller reserves canonical container high-water capacity. Production Shared values are immutable. Commit reuses planned Record/Applied values and does not recreate decisions.
4. apply_prepared checks consecutive seq, monotonic commit time, unique Operation ID, and document version. Late vectors preserve document version. The record key comes from the same record_id. lookup points to the exact appended operation position. lookup_bytes is updated with checked_add.
5. Record replacement maintains value bytes and the deadline index for current live TTL records incrementally. Operation append maintains byte counts. Recovery rebuilds the index from verified row metadata, and ordinary and group writes admit new deadline entries before WAL publication. Final `active_records` reads only the deadline prefix due at its evaluation time and checks current versions and published locations before subtracting. Statistics and background retirement use the same index. Checkpoints and WAL still come from the original rows and commit decisions.
6. history trim, expiry, GC, and candidate installation still validate and install at their maintenance boundaries. GC rebuilds lookup. Full proofs still validate same-seq metadata and container consistency.

Repeated updates to one ID use the same ordered successful-prefix view. Each successful Append adds prior and next TTLs, including intermediate versions, to the commit deadline. Unrelated expired text does not block the batch. Observe time again if it advances during uploads or planning. An expired candidate returns Busy with known replay and unattempted scope retained.

Durability, partial failure, and cancellation retain existing semantics. Commit to WAL or remote authority before in-memory and derived publication. An unknown outcome is not a rejection. Failure keeps fencing, and independently owned work completes after acceptance. Ordinary writes no longer clone the full base or shadow. Full-state clones and checks in recovery and maintenance retain their boundaries.

### Reserve derived workspaces before WAL

Before persistence, ordinary writes also reserve vector/metadata input, retained values, and identity capacity through `RetrievalView::reserve_component`. Full text selects queryable delta or native publication by applicable size and reserves analysis, identity, manifest/snapshot, and writer workspace. Missing empty native bases are created before WAL. Recoverable pressure returns Busy. Configuration overflow, corruption, and real I/O failures retain their specific errors and cannot all become retryable.

Component credit binds the actual admission generation and includes accepted pending work. Application transfers leases without reacquiring the same explicit memory after confirmation. Cancellation before publication releases the complete reservation. In-flight native work retains ownership until it exits. Deltas held by pinned older snapshots stay charged and cannot be released when materialization starts. This protocol bounds explicit reservations and does not guarantee every allocation succeeds.

Handoff between a queryable delta and native materialization only changes the derived view. It does not advance the business sequence. Materialization failure cannot undo a durable decision and requires fencing, reopen, and replay. See the [queryable full-text tail](architecture.en.md#wal-backed-queryable-full-text-tail) for views and file ownership.

### Bounded planning read set

Planning retains the successful Append's shared Record/Applied values and two small maps. For each item, look up the successful prefix first, including tombstone values, then the target canonical Record/operation under a short Inner lock. The same actual writer protects State from GC/TTL/maintenance installation. Reads can advance transient evaluation_time, and consumers can advance derived state, but cannot rewrite the planning baseline. Later IDs are neither collected nor validated early.

A single mutation planner takes explicit Input for the target record, the complete existing decision for that Operation ID, and the candidate's seq/epoch/time plus record/operation/TTL counts. Late vectors, TTL inheritance, and document projection reuse existing helpers. Replay neither advances the prefix, adds an operation, nor reserves a current large document. Full-state recovery and test callers use the same helpers, without divergent mutation rules.

Each successful Append updates global bytes as follows.

```text
values_next = values_before - prior_record_wire + next_record_wire + applied_wire
lookup_next = lookup_before + operation_id_wire + epoch_index_allowance
containers_next = snapshot_next - values_next + lookup_next
```

snapshot_next retains the original incremental encoding formula for comma changes in globally empty/nonempty state, escaped keys, and changes in sequence/epoch/time digit lengths. Reserve each successful prefix's high-water containers in canonical State before persistence, rather than just final state or small-map sizes. Each map key/node obtains its own lease before insertion. Shared values retain Prepared ownership. All checks and reservations that can fail precede prefix mutation. Lower copying changes available headroom and does not guarantee the former refusal points under memory pressure.

Receipt GC checks are still against the fixed canonical prefix. New batch operations have seq above fixed history.floor and cannot be collected in this round. Use the greater of current UTC epoch and the successful candidate epoch watermark, preserving a candidate's higher observed epoch near midnight. Per receipt stream, the prefix minimum operation epoch at or below the floor determines whether collection is due. Only maintenance selects and prunes individual receipts. Lookups retain tree-index cost, and persistence and async indexing have their own costs. This does not establish O(batch) for the complete write or guaranteed tail-latency or RSS gains.
