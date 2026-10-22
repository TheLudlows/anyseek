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
- Added a separate immutable bootstrap checkpoint reference in the SQLite partition head, plus `publish_bootstrap` to upload/verify/register and publish the initial Record configuration/checkpoint before the first data commit. Bootstrap roots are included in backup and GC reachability.
- Added the feature-gated `ObjectBackedPartition` storage coordinator. It creates/opens a SQLite authority, publishes/loads the immutable bootstrap, appends canonical operation payloads through the object-first commit protocol, and returns a single-head snapshot with a verified committed-but-unindexed tail. A regression test covers bootstrap, append, tail read, duplicate retry, and same operation ID with different payload. Stale owner epochs return conflict; response-loss retries resolve the same operation ID and digest without allocating a second sequence.
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

The Record layer now exposes feature-gated `create_s3` and `open_s3` paths. Accepted `Applied` operations are committed to S3 + SQLite before local journal/index publication; recovery validates the immutable bootstrap, appends missing committed tail entries to the local recovery journal, then reopens through the ordinary Record replay/index path. The local journal is a rebuildable cache in this mode. Initial checkpoint publication is retryable while the remote head remains at sequence zero. WAL blocks register predecessor dependency edges so backup closure discovery traverses the full committed log chain. Remaining limitations: real S3-compatible endpoint semantics are not yet tested; index/segment dependencies must be registered by every writer; and service-level backup/restore still needs fault testing. The feed-retention trim is a same-sequence metadata transition that the remote log protocol does not yet represent, so S3 mode deliberately skips feed trimming rather than allowing a local-only floor advance that would be lost on cache rebuild. This trades retention reclamation for correctness/backpressure until a remote metadata transition is designed.

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

1. Complete current-head CI after the Record bootstrap/recovery changes.
2. Audit all state-changing maintenance events and compaction/anchor paths so none bypasses remote commit.
3. Record-level missing-cache recovery now verifies a create/update/delete sequence and confirms the rebuilt query view contains the updated record while hiding the deleted one. Interrupted bootstrap retry and provider-specific S3 semantics still need dedicated tests.
4. Register complete manifest/segment/index dependency edges and verify service-level backup/restore closure.
5. Run the ignored S3 contract test against a dedicated real endpoint to validate conditional create, ambiguous/duplicate PUT behavior, read-after-write visibility and delete semantics.
6. Complete object-backend fault-injection and restart acceptance before removing Draft status.

## Progress status — 2026-10-22

Overall: **Record-level S3 integration is implemented as an opt-in path. Build/regression CI is green on the latest validated head, but this feature is not ready to merge until the remaining service-specific and retention/backup checks pass.**

### Delivered
- SQLite metadata authority with sequential conditional commit publication, owner-epoch fencing and operation receipts.
- S3-compatible immutable object adapter with conditional create, read-back verification and ambiguous-PUT retry handling.
- Record `create_s3/open_s3`: remote bootstrap publication/recovery, object-first commit before local journal/index publication, and committed-tail reconciliation into the ordinary Record replay path.
- Bounded tail validation and incremental replay after a recovered local sequence; immutable WAL predecessor dependency edges.
- Logical metadata backup/restore, transitive closure discovery, backup root pins and GC deletion tombstones.
- Record-level cache-loss regression test covers create/update/delete and confirms final visible state after rebuilding from remote bootstrap + committed log.
- S3 mode deliberately disables local-only feed trimming until a remote-authoritative feed-floor transition is implemented.

### Validation
- **CI Run #305 completed successfully**: Linux persistence/fault tests passed; workspace build and smoke tests passed; Windows persistence and native HTTP process fault/recovery matrix passed.
- This is the latest confirmed green run at the time of this update.
- The ignored real S3 contract test has not been run against a real endpoint. In-memory tests do not prove provider-specific conditional-create, read-after-write, or delete behavior.

### Remaining work
1. Implement a remote-authoritative feed-floor/FeedTrim transition so feed retention can safely resume in S3 mode.
2. Verify all manifest/index/segment writers register complete dependency edges and test service-level backup/restore across interrupted recovery.
3. Run the ignored contract test against the intended S3-compatible endpoint/bucket.
4. Run a final CI after any further code changes and keep the PR Draft until all remaining acceptance criteria pass.
5. Review/document operational constraints: SQLite authority is single-host local metadata; this slice does not provide cross-host HA.

## Latest progress — 2026-10-22

- CI Run #305 is green across Linux persistence, Windows persistence/native HTTP fault recovery, workspace build and smoke.
- The Record S3 path is integrated for bootstrap, Applied writes and cache-loss replay, with create/update/delete recovery coverage.
- Added incremental tail replay so recovery resumes after the locally recovered sequence rather than rereading the entire history.
- Added WAL predecessor edges for backup closure discovery.
- TTL and staging-lease expiration already publish sequenced maintenance entries remotely before local journal append.
- FeedTrim is intentionally a no-op in S3 mode until its retention-floor update is represented by the remote commit protocol.
- Still outstanding: remote-authoritative feed retention, real S3 endpoint contract run, and service-level backup/restore/GC acceptance. PR #72 remains Draft.
