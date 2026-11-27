# AnySeek Record HTTP service

**English** · [简体中文](README.md)

Manage collections and records by tenant namespace through the HTTP API. The service provides full-text, vector, and hybrid retrieval. Successful items in synchronous `/batch` are durable and visible. `/ingest` returns durable business receipts and advances indexes asynchronously. See [writes and retries](#stable-identities-writes-and-retries). The web console is a separate process.

The [Record contract](../../../docs/record-model.en.md) defines responsibilities and consistency boundaries. See the [roadmap and acceptance evidence](../../../docs/roadmap.en.md) for pending work and verification scope.

## Build and start

Run from the repository root. Linux / macOS commands follow.

```sh
cargo build
export MASTER_KEY_SECRET='replace-with-your-private-secret'
target/debug/anyseek_server local_ip=127.0.0.1 local_port=8080
```

Windows PowerShell：

```powershell
cargo build
$env:MASTER_KEY_SECRET = 'replace-with-your-private-secret'
.\target\debug\anyseek_server.exe local_ip=127.0.0.1 local_port=8080
```

Set `MASTER_KEY_SECRET` explicitly to a value that is not all whitespace. An absent or invalid value exits before creating the data directory or opening a port. For a new deployment, generate and store a private random value, for example with `python3 -c "import secrets; print(secrets.token_hex(32))"`. Replace the example placeholder. Normal startup logs omit both the secret and Master Key. When initializing the credential, run the following command separately in the same environment.

```sh
target/debug/anyseek_server print_master_key=true
```

In Windows PowerShell, use `.\target\debug\anyseek_server.exe print_master_key=true`. This command prints `Base64(SHA-256(secret 的 UTF-8 字节))`, which hashes the secret’s bytes and encodes the digest in Base64, to stdout and exits. It opens no directory, starts no service, and neither generates nor rotates the key. The output is itself the administrative credential. Keep it out of shared logs.

Send the service credential as the `apikey` header to all protected AnySeek endpoints from Origence or the gateway. Data endpoints also require `X-AnySeek-Tenant`. `/api/v1/live` remains public and strictly validates the service credential if `apikey` is present. This lets a host check its connection without a tenant or collection. Upstream handles user authentication and authorization. The gateway verifies permissions for the selected tenant and administrative operations. AnySeek does not register tenants or issue, store, or revoke tenant keys. List collections with GET `/api/v1/index`. A tenant namespace must match `^[a-z0-9][a-z0-9_-]{0,62}$` (including UUIDs starting with digits), and a collection name must match `^[a-z][a-z0-9_-]{0,62}$`. Case is preserved. Origence sends its application tenant UUID and uses `ws_<workspace UUID 无连字符>`, the workspace UUID without hyphens, as the collection name. The default data directory is `anyseek_index` next to the executable. On first startup, `index_path` must not exist and its parent must exist. On restart, it must point to a recoverable Catalog root. See the [storage protocol](../../../docs/storage.en.md) for supported formats.

On startup the standalone HTTP service checks the Catalog and finishes pending creates and restores. It opens existing active collections on their first authorized access. Concurrent requests for the same tenant, collection, and generation share one open in the Recovery lane. Cancellation of one request leaves other waiters in place. GET `/api/v1/index` returns live statistics and also opens any cold collections for that tenant. A request to a cold collection reports an open failure and later requests can retry.

`ANYSEEK_RESIDENT_MEMORY_BYTES` sets the eviction threshold for the standalone HTTP service. It defaults to 75% of `ANYSEEK_BUILD_SHARED_MEMORY_BYTES` and must be positive and at most that shared limit. The threshold uses bytes actually reserved in the process. Under pressure the service closes the least recently used idle generation, then checks the budget after closure and resource release. Active requests, accepted writes, background work, backups, and output still in transit hold their generation. A memory admission phase waits for up to two seconds before returning `busy`. The Catalog and durable records survive eviction. The next authorized access can reopen the same generation.

| Parameter | Default and constraints |
| --- | --- |
| `index_path` | `anyseek_index`. Relative paths are resolved from the executable's directory. |
| `local_ip` | `0.0.0.0` |
| `local_port` | `8080` |
| `default_nprobe` | `200`. Positive integer, overridable per request. |
| `print_master_key` | `false`. `true` explicitly prints the Master Key and exits. Requires the secret and cannot be combined with `generate_openapi=true`. |
| `generate_openapi` | `false`. `true` writes JSON/YAML for the current routes and exits. |

Only `name=value` is accepted. Unknown, duplicate, or invalid arguments exit nonzero. The service uses `log`, configured by `RUST_LOG`. Ctrl-C/SIGINT/SIGTERM stops accepting requests and waits for connections. Successful writes are already durable and do not depend on committing derived indexes at shutdown. The service credential is read only from process configuration and is never stored in the Catalog. Update the gateway's service credential when rotating the configuration. Whitespace in the secret is preserved and the derivation algorithm is unchanged.

## Start the web UI

Start the API and UI separately. The console uses an authenticated gateway to select a tenant namespace. See the [console guide](../../../console/README.en.md#run-the-ui-locally). The browser neither stores nor sends the master key. The API root path `/` does not serve a page.

## S3 backend (explicit opt-in)

S3 support is always compiled into the service. The runtime defaults to the `local` backend. This example enables S3 for a new deployment.

```sh
export ANYSEEK_STORAGE_BACKEND=s3
export ANYSEEK_S3_ENDPOINT='https://your-s3-endpoint'
export ANYSEEK_S3_BUCKET='your-bucket'
export ANYSEEK_S3_REGION='your-region'
export ANYSEEK_S3_ACCESS_KEY_ID='your-access-key'
export ANYSEEK_S3_SECRET_ACCESS_KEY='your-secret-key'
```

Set `ANYSEEK_S3_ALLOW_HTTP=1` only for an explicitly configured development HTTP endpoint. Optional `ANYSEEK_S3_MAX_OBJECT_BYTES` defaults to 64 MiB and `ANYSEEK_S3_MAX_TAIL_BLOCKS` to 1,000,000. A single tail's byte budget is bounded by both the collection log budget and the 1 GiB cap. Credentials are used only at runtime.

The Catalog stores collections, backup ownership, and control receipts in `metadata.sqlite` at the root. Both local and S3 modes retain this database, SQLite WAL, and owner lock. Catalog 7 has no tenant registration or key table and does not store the service credential. In S3 mode, the same database also holds Record coordination state. Standalone services use different buckets. Configure a separate target bucket before starting a restored full service. Objects live in `anyseek/{tenant_id}/{collection_id}/`. No automatic namespace or bucket ownership marker is created. SQLite metadata is authoritative, so use the backup API and never copy live database files. The collection's local S3 cache can be reconstructed from authoritative checkpoints and tails. See [storage design](../../../docs/storage.en.md) for object cache and lock protection.

PATCH collection publishes a full remote checkpoint before reclaiming covered, unpinned objects. Retrying the same collection backup after interruption resumes the original cut. Restore creates a new collection and copies to the target collection's new generation without service credentials. A full service backup uses the existing standalone local archive format, with the catalog and full data. In S3 mode, starting the restore publishes into the separate target bucket.

The S3 backend has merged. Conditional writes and read-after-write visibility against the target service, and auditing remaining maintenance paths, still require acceptance. See the [storage protocol](../../../docs/storage.en.md). The initial SQLite implementation coordinates one host and provides no cross-host HA.

## Field types and numeric values

Field types are String, StringSet, Int, Float, Text, Timestamp, Json, and Binary. Available index methods vary by type. Vectors use Json/Binary with index_vector. A configured vector field may be absent. Such records cannot be retrieved through that field. Later vector patches require exact version conditions. Filterable fields require explicit filterable:true. Facet aggregations, histograms, and field extrema are unavailable.

Int is a signed 64-bit integer (-9223372036854775808 to 9223372036854775807). Float is a finite 64-bit floating-point value. Int requires an integer within range. Timestamp uses signed 64-bit Unix seconds. Json fields support boolean values. Vector precision can be configured as F32 or I8.

String matches the entire string exactly. StringSet matches any member exactly, deduplicates, and stores 32-bit dictionary IDs. Text supports tokenized full-text search. Int/Float/Timestamp range filters include the lower bound and exclude the upper bound. Nulls and absent fields retain their original payload representation, do not match metadata filters, and appear after fields with values in both sort directions.

StringSet sorts by the first member after sorting and deduplicating the set. Empty sets precede nonempty sets in ascending order and follow them in descending order. Null or absent fields always follow fields with values, including empty sets. Ties use later sort keys or the stable record identity.

The web document editor preserves the input JSON's original numeric text. Inserts, conditional-update envelopes, and persisted resends avoid re-encoding through JavaScript Number. Nested Json, integers, and fractions retain original text. Requests already stored and possibly sent by older clients keep their existing content and operation identity. Previously lost precision cannot be restored.

The web inspector and expanded Document JSON in search results preserve the raw server response. Search field displays also preserve numeric text so copying back to the editor is lossless. Business logic still uses ordinary JavaScript objects. The raw text is for display and copying.

## Stable identities, writes, and retries

Collection ID and record ID are string application identities. Internal DocID is not a valid HTTP identity. All data requests use the `apikey` header.

- Create with `{operation_id,collection_id,config,embedding_space_id?}`. `config` includes name, schema, and optional vector configuration. Collection info exposes read-only `vector_dimensions` (null without vectors) for client validation. The caller generates vectors. `embedding_space_id` is optional descriptive metadata and is not used for model calls or vector provenance validation.
- Write with `{operation_id,record_id,expected,document}`. Supply `document` explicitly. An object fully replaces the record and `null` deletes it.
- `expected` is `"Absent"`, `{"Match":N}`, or `"Any"`. Re-creating a deleted record advances its version so old conditions cannot match new content.
- An operation ID is `{epoch,nonce}`. The epoch is UTC days since the Unix epoch. Retries are allowed in the current and preceding day. Issuing on the current day normally provides at least a 24-hour window. Control operations reserve the `sys:` nonce prefix.
- A successful synchronous `/batch` item is durable and visible without a subsequent PATCH. Committed items from `/ingest` guarantee a durable business outcome and index asynchronously. PATCH collection is an online full checkpoint independent of write confirmation. Retention and safe GC determine operation history, and a manual checkpoint cannot clear receipts still within the window.

Save the complete request before sending it. Retry with the original ID, conditions, and content. An identical request returns its original receipt, while changed content conflicts. Missing or expired receipts do not justify resubmitting a write with a new ID.

| Result | Meaning and action |
| --- | --- |
| `rejected` / `conflict` | This attempt was rejected or hit an identity or precondition conflict. A later rejection cannot establish that an earlier unknown attempt did not execute. |
| `busy`, HTTP 429 | Admission refused the request, possibly before checking an earlier operation receipt. Preserve and retry the original request. |
| `outcome_unknown` | Result could not be confirmed. Retry the original request or look up the operation after recovery. |
| `durable_pending` | Durable, with derived application still pending. Recovery is required. |
| `recovery_required` / `storage_error` | Stop relying on the current instance for further writes and follow recovery procedures. |
| `operation_expired`, HTTP 410 | Outside the supported window. This does not establish that the operation did not execute earlier. |

The UI saves the original request and unconfirmed status in sessionStorage before sending. A lost response, reload, or later rejection does not unlock an unknown write. Only an explicit rejection without a previous unknown result can release the rejected item and remaining unsent items. Clients are responsible for backing up pending requests.

Use `statistics.active_records` for the active record count. Query indexing progress through `/ingest`.

## Synchronous writes

The synchronous write endpoint is `POST /api/v1/index/{id}/batch`. Even a single item uses `{"writes":[RecordWrite]}`. The `/doc` POST/PATCH/DELETE aliases are removed. Reads through `/doc/{record_id}` and status remain available. Supports inserts, conditional updates with expected, explicit `document:null` deletion, and late vectors with `vector_patch`.

Each batch has 1..100 items executed independently and in order. It is not a transaction, and successful items are not rolled back. A non-2xx outer status is a request-level failure. With HTTP 200, check each item in `results`. Only `code=applied` with a `receipt` confirms durability and visibility. After `conflict` or `rejected`, continue. On `outcome_unknown`, `durable_pending`, required recovery, or storage failure, stop later items and return `not_attempted` for the tail. Unknown is distinct from failure. Retry with the original Operation ID and complete RecordWrite. For requests the browser already has pending, only the batch wrapper is added, preserving original numbers and identities.

## Atomic group replacement

`POST /api/v1/index/{id}/group/replace` accepts the complete new version of a group of Records in one collection.

```json
{
  "operation_id": {"epoch": 123, "nonce": "replace-guide-v1"},
  "group_id": "guide",
  "expected_group_version": 0,
  "records": [
    {"record_id": "guide-1", "document": {"body": "第一段"}},
    {"record_id": "guide-2", "document": {"body": "第二段"}}
  ]
}
```

Generate `operation_id` using the same current epoch rules as ordinary Record writes. The example epoch is a placeholder. First creation uses `expected_group_version: 0`. Subsequent replacements submit the current group version. `records` contains the complete membership. An empty array clears the group while keeping a monotonically increasing group version. Each member must supply a non-null `document` object. Member-level operation IDs and vector patches are rejected. The application handles chunking and vectorization.

Success returns `atomic: true`, the original operation ID, and a `receipt` with `seq`, `group_id`, `group_version`, and `record_count`. For timeouts or unknown results, repeat the complete original request and operation ID to get its original receipt. Do not use a new ID to guess an earlier result. `GET /api/v1/index/{id}/group?group_id=guide` returns the current group version. A group that has never existed returns 404. The group ID is an opaque application value. URL-encode it as a query parameter. A current version lookup is distinct from an operation outcome lookup.

Both endpoints use the `apikey` service credential and the `X-AnySeek-Tenant` namespace. The request applies to that tenant's current collection generation. Ordinary `/batch` and `/ingest` still write independent Records with `atomic: false` and cannot bypass existing group member ownership.

## Writes with asynchronous indexing

`POST /api/v1/index/{id}/ingest` accepts `{"records":[RecordWrite,...]}` with 1..64 items. Each carries its stable Record ID, version condition, and Operation ID. Explicitly provide an object or null as the document, or submit vector_patch. The HTTP body limit remains 2 MiB. The partition queue bounds admission to 64 total in-flight items and 4 MiB of serialized requests and refuses excess work.

Returns `atomic:false`, `searchability:"asynchronous"`, and `results` aligned to input positions. HTTP 200 indicates determinate per-item business results and does not mean all items succeeded.

| results[].code | Meaning and action |
| --- | --- |
| committed | Includes a durable Record receipt, operation_id, and expires_at. Full-text and vector consumers may not yet have reached receipt.seq. |
| rejected | Definitive rejection for this item, with the reason in error. Later items can proceed. |
| outcome_unknown | Publication outcome is unknown. Save the original request and look up after recovery or retry with the same identity. |
| not_attempted | No authoritative publication was attempted during this request. A prior attempt with the same identity may have committed. |

Interruption can produce 429/503 and retain known per-item receipts. Save and inspect the response body first. Do not rebuild the entire request or assign new Operation IDs based only on HTTP status. HTTP cancellation does not cancel durable tasks whose ownership has already transferred.

Reuse the `requests/write.json` saved in the next section to generate one batch request file.

```sh
python3 - <<'PYCODE'
import json
from pathlib import Path
write = json.loads(Path('requests/write.json').read_text())
with Path('requests/ingest.json').open('x') as output:
    json.dump({'records': [write]}, output)
PYCODE
curl -X POST "$BASE/api/v1/index/demo/ingest" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT" -H 'Content-Type: application/json' --data-binary @requests/ingest.json
curl "$BASE/api/v1/index/demo/ingest" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT"
```

The example reuses the original operation. A request already written synchronously returns the same receipt without creating another record. Resend the saved file directly on an unknown result.

`GET /api/v1/index/{id}/ingest` returns committed_seq, lexical_seq, vector_seq, index_error, and recovery_required. To search through a particular commit, check that required consumers have reached at least that receipt.seq and handle index_error/recovery_required. Consumer watermarks can differ and need not be globally equal. committed_seq is null when recovery is required.

Use `GET /api/v1/index/{id}/operation/{epoch}/{nonce}` to look up the Record receipt for synchronous and asynchronous writes. Absence or expiry does not prove that an operation never committed. See `commit_writes` and `committed_progress` in [service internals](../../../docs/development.en.md#service-internals).

## Runnable HTTP examples

The trusted service caller supplies the `API_KEY` service credential and sets the `TENANT` namespace. Browsers use a gateway without a service credential. Prepare request files once in a new directory. On timeout, resend the file without repeating ID generation.

```sh
BASE='http://127.0.0.1:8080'
TENANT='my_workspace'
API_KEY='paste-service-key'
mkdir requests
python3 - <<'PY'
import json, time, uuid
from pathlib import Path
op = lambda: {'epoch':int(time.time()//86400), 'nonce':uuid.uuid4().hex}
requests = {
    'create': {'operation_id':op(), 'collection_id':'demo', 'config':{
        'index_name':'Demo', 'schema':[{'field':'body','field_type':'Text'}]}},
    'write': {'writes':[{'operation_id':op(), 'record_id':'welcome',
              'expected':'Absent', 'document':{'body':'hello search'}}]}
}
for name, request in requests.items():
    with (Path('requests')/(name+'.json')).open('x') as output:
        json.dump(request, output)
PY
curl -X POST "$BASE/api/v1/index" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT" -H 'Content-Type: application/json' --data-binary @requests/create.json
curl -X POST "$BASE/api/v1/index/demo/batch" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT" -H 'Content-Type: application/json' --data-binary @requests/write.json
curl -X POST "$BASE/api/v1/index/demo/query" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT" -H 'Content-Type: application/json' -d '{"query":"hello"}'
curl "$BASE/api/v1/index/demo/doc/welcome" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT"
```

A write returns `receipt`, `operation_id`, and `expires_at`. A search returns `hits[].record` with record_id, version, and document, plus score, visible_seq, statistics, and count_total. Read by ID returns a Record envelope. See [test_api.rest](test_api.rest) for conditional updates, deletes, authorization, vectors, and backups.

## Configuration and query diagnostics

Collection info returns `analysis` with a versioned format, full configuration fingerprint, tokenizer and implementation/dictionary identity, and stop_words mode and digest. The fingerprint is the SHA-256 of the index/schema/tokenizer identity tuple used to verify native packages. Custom vocabulary contents are not returned. Callers use it to verify and pin settings. It is not proof of retrieval quality. Existing version, limits, and embedding_space_id remain available.

Every HTTP response carries the server-generated `x-anyseek-request-id`. The service does not reuse a caller-provided value. Errors also have a safe `x-anyseek-error-code`. Successful queries add format v2 `diagnostics` with the same ID, engine version, browse/lexical/vector/hybrid mode, elapsed_us, analysis digest, embedding_space_sha256, and index_progress. The space digest is SHA256 of the collection's embedding_space_id UTF-8 bytes and is null if unconfigured. Callers can check it on each request without revealing the raw space identifier. The analysis digest describes index/schema/tokenizer only and cannot replace checking space identity or prove which model generated vectors. Progress reuses committed/lexical/vector sequences and recovery state. index_failed only indicates that an error exists and omits raw diagnostics. Progress is sampled separately after querying, with sampling set to after_query_not_atomic_with_result. It is not an atomic result snapshot, a cross-system transaction, or proof of delivery. Existing score/channels semantics remain.

Server elapsed time covers query preparation, execution, and progress sampling, excluding outer admission and complete response delivery. Measure caller round-trip time separately. The response_prepared debug log correlates status and timing with the ID. body_delivery is always unconfirmed. Diagnostics omit queries, vectors, filter values, text, and credentials. Error and cancellation paths may have no query diagnostics body. Callers must preserve the gap without inventing progress.

## Queries and iteration

Text selects Lexical, a vector selects Vector, and both select Hybrid. `rrf` supports weighted fusion and independent candidate budgets. An empty object `{}` browses active records, including vector-only collections. `length:0` can return just counts for lexical queries or browsing. `count:false` returns `count_total:null`. The default result length is 10. Offset, stored-field projection, filters, and sorting are supported. Search projections include only fields with `store:true` in the schema. Reads by ID and iterator can return the full authoritative original data and can project explicitly.

Vector/Hybrid probes up to `default_nprobe` clusters per level by default. `nprobe` overrides it, while `all:true` scans all clusters. `all:true` and `nprobe` are mutually exclusive. Both require query_vector. ANN/Hybrid count_total is not an exact full-corpus union, and field reranking within the candidate window does not guarantee full-corpus field Top-K. Scanning all clusters still uses the configured storage precision and quantization. Defaults are not performance or exact recall guarantees.

POST `iterator` accepts `{cursor?,length?,fields?}` for forward pagination by stable ID. A write changing visible_seq invalidates the old cursor, returning 409. The old bidirectional skip/take HTTP protocol is unsupported. The low-level Rust iterator has its own API.

## Tenant namespaces, deletion, and bulk operations

Tenants serve only as namespaces passed by trusted upstream through `X-AnySeek-Tenant`. No preregistration is required. All non-public endpoints validate the same service credential. Data routes also require a present, valid, nonduplicate tenant header. Upstream authorization determines collection access within the namespace. AnySeek supplies no User, Project, Collection Grant, roles, or shared authorization.

Accepted work retains the tenant context and resource ownership from admission without rechecking upstream user permissions. The data-failure fence still applies to the response and body frame delivery.

A trusted service caller can supply the tenant header and DELETE `/api/v1/index/{id}` with `{operation_id}` to delete a collection in the namespace. The name can be reused. The new collection gets a new internal generation with no client-supplied generation required. Old background work, receipts, backup identities, and GC cannot affect the new generation. Stop old requests and retries before deletion and recreation. Old requests carrying only the name are indistinguishable from requests for the new collection. Physical reclamation still runs later.

`delete-plan` accepts `{select:"all"}`, `{select:"ids",record_ids:[...]}`, or `{select:"query",query:{query:"text"}}`. Save the returned targets, assign and save an operation_id for each, and submit `{writes:[...]}` to `/batch`. Each batch has 1..100 independent operations with per-item results and **no atomic transaction**. Unknown or recovery-required results stop the unattempted tail. Retry without regenerating deletion selection conditions.

## Backup and restore


Collection backup uses POST `/api/v1/index/{id}/backup` with `{operation_id}`. Retrying the original ID keeps the same snapshot. POST `/api/v1/restore` using the same tenant header and `{operation_id,collection_id,backup_id}` to restore under a new identity in that tenant namespace. Backup IDs from another tenant are inaccessible. Trusted services can GET `/api/v1/backups` or durably retire a backup via DELETE `/api/v1/backups/{id}` and `{operation_id}`. An incomplete restore pins its source. `cleanup_pending:true` indicates logical retirement completed and physical cleanup needs a retry.

Master POST `/api/v1/service-backups` with `{operation_id}` freezes the catalog, all retained collection data, and nonretired collection backups. The manifest verifies authoritative object content. Outer publication has a completion witness and does not recursively include old service archives.

Trusted service POST `/api/v1/service-restores` with `{operation_id,backup_id}` returns a **new, unstarted target_root** without overwriting the running root. Start it separately with the target process's `MASTER_KEY_SECRET`. Namespace and backup ownership are retained. Historic management requests conflict explicitly, while Record versions and receipts are preserved. Use the returned lowercase hex for source backup and restore IDs.

## Current resource and acceptance boundaries

Collections use the same resource budgets. Collection statistics report retained identity and receipt counts. The `limits` object reports journal byte and query candidate limits.

| Boundary | Current behavior |
| --- | --- |
| Record identities including tombstones, retained operations | Governed by resource budgets for memory, journal and checkpoint bytes. Receipts are eligible for reclamation after at least two days, and only after the current and prior UTC day retry window and when the certified history cut and protection requirements are met. Maintenance can be delayed and does not guarantee cleanup within two days. |
| Query candidates | 10,000 |
| Operation / HTTP body / response | 1 MiB / 2 MiB / 16 MiB |
| Record WAL / full checkpoint | 256 MiB generation budget. Individual WAL and checkpoint frames are limited to 16 MiB. A complete checkpoint can span multiple frames. |
| Catalog | 16 MiB of metadata, 10,000 operations, safe reclamation for expired control receipts only |
| admission | Ordinary 32 / 16 per collection, recovery 2/1, background 2/1, trusted control 8/2, public/liveness 8/8, internal partition 16, and a 30-second request deadline. |
| Scheduling | Different collections can run concurrently. Shared snapshots retain a global barrier. Accepted work retains its namespace. Insufficient resources return Busy immediately. |
| collection backups | 32, with durable retirement |
| service archives / new restore roots | At most 4 archives or preparations per source root, 4 generated roots per managed parent directory, and 1 GiB of preflight budget for authoritative backup payload |

Resource lanes are selected by route and the decoded registered collection identity before a large body is read. Unknown identities share service capacity. All lanes first check the service credential, and data routes also validate the tenant header. Lane classification does not replace upstream authorization. Management receipt lookup has its own control lane and does not wait for a long backup or restore workflow lock. Deletion preserves existing lifecycle and recovery ordering. Restore/create use the recovery lane. PATCH checkpoint, collection backup, and service backup use the background lane.

| Class | Service-wide / per-collection reserved bytes | Service-wide / per-collection I/O concurrency |
| --- | --- | --- |
| FOREGROUND | 2048 / 512 MiB | 8 / 2 |
| RECOVERY | 512 / 256 MiB | 2 / 1 |
| BACKGROUND | 4096 / 2048 MiB | 2 / 1 |
| CONTROL | 144 / 36 MiB | 2 / 1 |
| PUBLIC | 144 / 144 MiB | 1 / 1 |

Deployments can set `ANYSEEK_{类别}_SLOTS`, `_PER_COLLECTION`, `_BYTES`, `_PER_COLLECTION_BYTES`, `_IO`, and `_PER_COLLECTION_IO`. Substitute the uppercase class above. Byte values use decimal bytes, for example `ANYSEEK_FOREGROUND_PER_COLLECTION=2`. Values must be positive integers, local limits must not exceed global limits, and bytes per collection must be at least 18 MiB. Each class has at most 64 slots/I/O. The first three classes have at most 48 slots in total, and CONTROL has at most 8 slots to reserve catalog control headroom. Invalid configuration prevents startup.

The 30-second deadline covers authentication, identity lookup, and the handler. A timeout before the handler starts returns Busy, while a timeout in owned work remains outcome_unknown. Point GET and query wait for publication within the original request deadline, including at response handoff. A failed publication rejects data delivery with `recovery_required`. Each HTTP request conservatively reserves 18 MiB for body and output workspaces. Building snapshots/tails, object downloads, and PUT copies are charged separately. S3 buffers and accumulated decoded tail capacity follow the actual owner, including iterators and extracted entries. Owned tasks and delivered transport frames retain the original admission reservation. Cancelling a wait does not release it early. The JSON response size is checked before growth. I/O has no additional waiting queue. Resource refusals known to precede publication can return busy. Unknown outcomes follow outcome_unknown and recovery, and retries reuse the original identity. This accounting does not cover all runtime objects, allocators, mmap/OS cache, or SDK memory. It does not promise hard whole-process RSS, system I/O bandwidth, or disk caps. Each authority object cache allows 256 MiB / 4096 blocks. The Catalog always uses `metadata.sqlite`. S3 collections share its coordination tables and adjacent object cache. Total use across materialized indexes, backups, and multiple authorities still needs evaluation.

Service archive/root cleanup is an explicit offline operation. No API deletes a running root. System and filesystem crashes and power loss have not passed the acceptance matrix. Native process kill/exit, fault injection, and calling fsync are separate evidence. Safe history-receipt GC and background reclamation of deleted collection generations exist. Sustained capacity, combined isolation acceptance, cross-host coordination, and distributed operation remain future work. Optional vectors are implemented.

## Development verification

The [development guide](../../../docs/development.en.md) covers builds, unit tests, real HTTP and fault processes, S3 acceptance, and the full CI matrix. Run browser tests per the [console guide](../../../console/README.en.md#verification). Fault injection is off by default in normal builds.

The small caller example uses `scripts/anyseek_client.py`. Set `ANYSEEK_API_KEY` and run `python3 scripts/index_write_query_smoke.py --port 8080 --requests requests.json`. Rerun the same command to retry saved requests without generating new identities.

`generate_openapi=true` writes `openapi.json` and `openapi.yml` next to the executable. Generated files describe current routes and request schemas. Public request types are in `api_types.rs` and the default service implementation is `record_service.rs`.

## Ordinary source metadata

`document_id`, `chunk_index`, section, `source_id`, `source_version`, and similar metadata can be stored as ordinary `document` fields and filtered or searched according to the schema. The engine does not interpret their application meaning or manage dependencies, stale state, or cascading visibility.

The top-level `system` write, `/evidence`, `/entities`, `/neighbors`, and query `relation_fusion` have been removed. Applications handle historical byte and page excerpts, source versions, and derived recomputation. General full-text/vector `rrf` fusion remains available.

## Optional and late vectors

Write text first without a zero vector placeholder. Submit `operation_id,record_id,expected:{"Match":当前正文版本},vector_patch:{"vector":[2,0]}` with the current text version to the write endpoint, omitting document. It only fills previously absent schema vector fields. The text version remains unchanged and the commit sequence advances. Retries retain operation_id. Deleted records cannot be patched, and a changed text version conflicts with old requests.

The status endpoint returns `pending_vector_fields`. The real web UI supports this request in envelope mode. See [Record semantics](../../../docs/record-model.en.md) for the optional vector contract.


## Temporal predicates and TTL

System temporal metadata uses UTC Unix **milliseconds** and is separate from the existing user schema Timestamp seconds field. A complete Record write may include the following.

```json
{"temporal":{"event_time_ms":1735689600000,"valid_from_ms":1735689600000,"valid_to_ms":1767225600000,"expire_at_ms":1893456000000}}
```

Business validity is the half-open interval [valid_from_ms,valid_to_ms) and does not implicitly create TTL. After expire_at_ms, ID reads, pagination, and retrieval exclude the text. Reads do not persist expiry or history pruning. Bounded background maintenance writes expiry tombstones and reclaims index entries without a fixed completion deadline. An expired identity can be reused with a complete document and incremented version. Absent matches the absence of currently valid text. Match compares the technical version. Old vector patches cannot match newly written text. Omitting temporal inherits metadata only from currently valid text. Reusing an expired identity clears old TTL by default.

Observed time within a process is monotonic. Restarts preserve only persisted write and maintenance watermarks. An expired record not yet reclaimed could become visible after a wall-clock rollback past its deadline. Large clock deviations return clock_uncertain. Text can still exist in the WAL and backups. TTL does not guarantee privacy erasure.

Search filters TTL at capture time and revalidates candidates using the latest time after scoring, so expiration can shorten a page. count_total is the observed count from the captured index and not an exact count at final time. Expired index rows can affect BM25 corpus statistics until background reclamation. Crossing the earliest TTL during pagination conflicts on continuation and requires restarting iteration.

`/doc/{record_id}/status` retains the current technical version, visible/deleted/expired state, and pending_vector_fields. See the [Record contract](../../../docs/record-model.en.md) for all boundaries and the [roadmap and acceptance evidence](../../../docs/roadmap.en.md) for verification status.

### Opt-in event-time windows

Service administrators build a projection for a local collection with `POST /api/v1/event-time-checkpoints`. Use the service credential, omit `X-AnySeek-Tenant`, and supply `tenant_id` and `collection_id` in JSON. The gateway must restrict this operation to administrators. Existing background and partition admission limit construction. Success returns `indexed_seq`. The host authorizes the local SDK's `build_event_time_projection(tenant, collection)`. This build enables the current certified base. Incremental checkpoints can reuse it, and an ordinary full checkpoint (including the ordinary PATCH) selects a new base without the projection. The same administrative operation can rebuild it after reopening. Remote and archive collections do not support this option.

Tenants read a window with `POST /api/v1/index/{id}/event-time/query` or SDK `query_event_time`. See the [HTTP examples](test_api.rest). `temporal.event` requires both UTC millisecond endpoints of a nonempty half-open interval. Combine `temporal.ingested`, `temporal.valid_at_ms`, and ordinary `filter` predicates as needed. The response includes `visible_seq`, `evaluation_time_ms`, and `hits`, ordered by descending event time and then ascending record ID. `length` accepts 1 through 128 and defaults to 10. `fields` selects stored fields only. Count, pagination, text, vector, hybrid, and custom-sort parameters return 400.

The query pins one published canonical cut and applies TTL. An absent projection returns 400 `event_time_unavailable`. A suffix over 128 changed IDs or 2 MiB, a gap from the base over 4096 sequences, or an exhausted candidate or body-read budget returns 400 `event_time_budget` without a partial window. Use the same administrative operation to refresh a missing projection or oversized suffix to a new certified base. Narrow the query when the window itself exceeds its work budget. The base scan visits at most 4096 candidates and respects the collection candidate limit. Base bodies are limited to 16 MiB per query. A changed selected root or TTL crossing before delivery can return 429 `busy` within the original request deadline. Missing or damaged certified projection files cause the collection to fail to open with 503 `storage_error`. The SDK exposes the same codes in `LocalError::code()`. Ordinary `/query` continues to accept existing time predicates.

### Receipt retention boundaries

Rust RecordConfig.history.retention_ms defaults to a minimum of 172800000 (2×24 hours). Rust configuration can expressly keep a longer value. Existing explicit seven-day values are untouched. HTTP collection creation uses the engine default and no HTTP retention parameter was added. Deserializing Rust config without retention_ms uses the current default. The API supports idempotency and outcome lookup in the current and previous UTC day. Reclamation requires certified history floor coverage and an operation epoch outside that window. Wall-clock expiry alone does not delete the WAL.

Recent idempotency retains the complete semantic request to detect conflicts. Existing GC summaries can replace old operations covered by a certified checkpoint. Internal SQLite publication receipts are also used for remote WAL accounting, index cuts, recovery, and snapshots. They do not duplicate text and are retained. A two-day window cannot be used alone to delete these rows. Safe WAL, checkpoint, and backup reclamation boundaries are independent of the API lookup window.
