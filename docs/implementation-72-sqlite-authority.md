# SQLite metadata authority — implementation slice

Status: **implementation foundation in Draft PR #72; not yet accepted or wired into the record write path**.

## Scope implemented

- Added `rusqlite` with bundled SQLite and registered the internal `storage::sqlite_authority` module.
- Added the `object_store` S3 backend (`S3CompatibleStore`) with configurable endpoint/bucket/region/credentials and HTTP opt-in for explicitly configured development endpoints.
- Immutable S3 writes use conditional create (`PutMode::Create`), then read back and compare exact bytes, length and SHA-256. If a PUT response is ambiguous, the adapter reads the existing key and treats it as success only when the bytes match exactly; conflicting bytes are never overwritten.
- `put_and_register` completes object upload and verification before recording the object as durable in SQLite; it deliberately does not advance `committed_seq`.
- `get_verified` checks each read against the SQLite-published object reference.
- Added bounded object-log blocks and backward chain replay from the SQLite committed tip to the indexed watermark.
- Added `append_and_commit`: resolve an existing operation receipt first, upload and verify a deterministic immutable log object, register it as durable, then publish the new sequential committed watermark/reference and idempotency receipt in a single SQLite transaction.
- Added a separate immutable bootstrap checkpoint reference in the SQLite partition head, plus `publish_bootstrap` to upload/verify/register and publish the initial Record configuration/checkpoint before the first data commit. Bootstrap roots are included in backup and GC reachability. Stale owner epochs return conflict; response-loss retries resolve the same operation ID and digest without allocating a second sequence.
- Added a focused test for object-first publication, unindexed-tail visibility, duplicate operation retry, and stale owner epoch rejection. Partition mismatch, sequence gaps, checksum failures and block/byte budget exhaustion fail closed rather than returning a partial tail.
- Added a logical metadata backup snapshot containing partition heads, operation receipts, object registry, pins and retired-key tombstones. Backup creation pins the caller-supplied complete object closure in the same SQLite transaction that captures the snapshot.
- Added a backup descriptor that references both the metadata snapshot and the object closure, plus restore that verifies the descriptor, snapshot and each object before importing metadata into an empty SQLite authority.
- Added an authority-driven backup entry point that derives the transitive object closure from registered object dependency edges. Backup descriptor and metadata snapshot roots are pinned by backup ID, and restore recreates those pins so the restore entry point is protected from GC. Closure discovery now includes bootstrap roots and existing pinned roots before recursively following dependency edges.
- Added authority-mediated retirement, pin/unpin, and collectible-object enumeration. An object is only retired when it is not referenced by any current partition head and has no live reader/indexer/backup/staging pin. Retirement is monotonic; a retired key cannot be reactivated.
- `delete_retired` only deletes keys that the authority currently reports collectible. There is intentionally no unconditional object delete API.
- Opening the metadata database enables SQLite WAL, sets `synchronous=FULL`, verifies the setting, enables foreign keys, and configures a bounded busy timeout.
- Metadata tables represent the schema version, partition heads, operation receipts, durable object registration, reader/indexer/backup/staging pins, and retired-object candidates.
- `PartitionHead` holds the single mutable authority's head version, owner epoch, committed sequence, WAL tip, manifest, indexed sequence, log base and recovery anchor.
- Commit publication uses a short `BEGIN IMMEDIATE` transaction with expected-head and owner-epoch checks, requires exactly the next committed sequence, and writes the head and operation receipt atomically.
- An immutable object reference can only be registered as durable after the external object adapter has uploaded and verified its length and SHA-256. Publication refuses unregistered/unverified references and key reuse with different bytes.
- Index publication is conditional on the current head/owner and constrains `indexed_seq` to the interval from the existing indexed watermark through `committed_seq`.
- Same-operation retry checks the request digest and returns the original recorded head, independent of the current head; reusing the ID with a different digest is rejected.

## Explicit limitations

This slice is **not** the end-to-end SQLite + object-storage backend. The S3-compatible adapter, object-log tail reader, logical metadata backup/restore bundle and basic pin-aware GC eligibility exist as isolated primitives, but they are not yet connected to the current RecordService authoritative write path or search/index view. Tail replay currently returns validated operation envelopes; applying them to the live Record/search state remains outstanding. The authority-driven backup derives closure through registered object dependency edges. Every writer/indexer must register all manifest/segment and WAL-chain dependencies; opaque manifest internals cannot be inferred automatically if those edges are omitted. Backup root pins are recreated on restore. GC reachability traverses dependency edges from both partition heads and pinned roots, so a pinned descriptor protects its transitive child objects. A failed backup may leave pins; release them only after confirming no valid descriptor was published. S3 conditional-create, visibility, and delete semantics must be tested against a real target service before certification.

The SQLite database is a single local authority. Do not put its WAL database on a network filesystem or allow independent nodes to maintain writable copies. Cross-host failover/HA is out of scope for this first slice.

## Tests added

- Commit and operation retry, including original receipt persistence after reopening.
- Reject an index watermark beyond the committed watermark.
- Reject publication before an object has been registered as durable.
- Reject reusing an immutable object key for different digest/length.
- Reopen the database and verify the indexed watermark persists.
- Verify upload/register does not itself commit a record.
- Verify S3 adapter create/read-back, exact-content retry resolution, size limit, digest mismatch and conflicting-key rejection using the in-memory ObjectStore test double.
- Verify active backup pins block retirement, retired objects are deleted only when collectible, repeated delete is safe, and retired keys cannot be registered again.
- Verify committed log tail replay from the authoritative tip, replay only above the indexed watermark, and fail closed on partition mismatch or exhausted budgets.
- Verify logical metadata backup/restore preserves committed head and idempotency receipt after validating the object closure.
- Verify a pinned backup descriptor protects transitive child objects from retirement until the root pin is explicitly released.

## Required next work

1. Run CI on the exact latest PR head and fix compile/test failures.
2. Add a real immutable object-store adapter and unique object naming/content verification.
3. Implement the object-first, SQLite-transaction-second commit flow and response-loss resolution.
4. Add tail replay/query integration so committed-but-unindexed records remain searchable.
5. Integrate backup snapshot roots, restore validation and reference/pin-based GC.
6. Exercise fault injection and native Linux/Windows recovery before removing Draft status.

## Progress status — 2026-10-22

Overall: **in progress; not ready to merge**.

### Delivered
- SQLite metadata authority with sequential conditional commit publication, owner-epoch fencing and operation receipts.
- S3-compatible immutable object adapter with conditional create, read-back verification and ambiguous-PUT retry handling.
- `append_and_commit` primitive: resolve the original operation receipt, upload/register the immutable operation log, then publish the committed head in SQLite.
- Bounded committed-tail replay with chain, sequence, partition, digest and byte-budget validation.
- Logical metadata backup/restore, object dependency edges, transitive closure discovery, backup root pins and GC deletion tombstones.

### Validation
- CI Run #264 passed on commit `78549bef`: core boundary checks, retained workspace build, HTTP/UI smoke, existing core/server tests, and Linux/Windows persistence/service fault tests.
- **That green run predates the subsequent append-and-commit and backup-root changes.** Those newer changes need a fresh current-head CI run before they are considered validated.
- No real S3-compatible endpoint has yet been exercised; in-memory object-store tests do not prove service-specific conditional PUT or visibility semantics.

### Remaining work
1. Integrate `publish_bootstrap` and `append_and_commit` into actual RecordPartition/RecordService create/open/write paths. Object-backed recovery must load the bootstrap checkpoint and replay committed log entries; do not dual-commit local WAL and S3 without an explicit recovery protocol.
2. Apply committed-but-unindexed entries in the same authoritative Record query view, including update/delete/version semantics.
3. Register complete manifest/segment/WAL dependency edges in all writers/indexers and integrate backup/restore into service lifecycle.
4. Validate conditional create, ambiguous PUT, read-after-write and delete behavior against a real S3-compatible service.
5. Complete object-backend process-failure/restart tests and only then remove Draft status.

## Latest progress — 2026-10-22

- Added the object-first `append_and_commit` primitive and a focused idempotency/tail test.
- Fixed GC reachability to treat pinned roots as roots of the transitive object graph, and added a regression test that a pinned backup descriptor protects its child objects.
- CI Run #264 passed before these latest changes. Run #265 was started on the preceding progress-document commit; its result must not be treated as validation of the later GC reachability changes.
- Current state remains **not ready to merge** and RecordPartition/RecordService integration remains outstanding.

