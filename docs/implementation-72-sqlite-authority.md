# SQLite metadata authority — implementation slice

Status: **implementation foundation in Draft PR #72; not yet accepted or wired into the record write path**.

## Scope implemented

- Added `rusqlite` with bundled SQLite and registered the internal `storage::sqlite_authority` module.
- Added the `object_store` S3 backend (`S3CompatibleStore`) with configurable endpoint/bucket/region/credentials and HTTP opt-in for explicitly configured development endpoints.
- Immutable S3 writes use conditional create (`PutMode::Create`), then read back and compare exact bytes, length and SHA-256. If a PUT response is ambiguous, the adapter reads the existing key and treats it as success only when the bytes match exactly; conflicting bytes are never overwritten.
- `put_and_register` completes object upload and verification before recording the object as durable in SQLite; it deliberately does not advance `committed_seq`.
- `get_verified` checks each read against the SQLite-published object reference.
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

This slice is **not** the end-to-end SQLite + object-storage backend. The S3-compatible adapter and basic pin-aware GC eligibility are implemented as isolated primitives, but they are not yet connected to the current RecordService authoritative write path. The adapter's conditional-create behavior must be tested against each selected S3-compatible service; services that do not honor conditional create and read-after-write visibility must not be certified. Query-time tail replay, indexer execution, coherent backup/restore closure, and integration fault tests remain outstanding.

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

## Required next work

1. Run CI on the exact latest PR head and fix compile/test failures.
2. Add a real immutable object-store adapter and unique object naming/content verification.
3. Implement the object-first, SQLite-transaction-second commit flow and response-loss resolution.
4. Add tail replay/query integration so committed-but-unindexed records remain searchable.
5. Integrate backup snapshot roots, restore validation and reference/pin-based GC.
6. Exercise fault injection and native Linux/Windows recovery before removing Draft status.
