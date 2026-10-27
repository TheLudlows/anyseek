# Build admission ownership audit

Parent code: `34dc46f`. This repair changes ownership accounting, not configured
limits, scoring, tokenization, or business publication.

| Reservation | Actual owner / lifetime | Decision |
| --- | --- | --- |
| JSON bytes × 32 | Component metadata/vector storage and conservative transient copies | Retain coefficient unchanged |
| Schema fields × 65536 × 32 | Component facet/map capacity allowance | Retain unchanged |
| Lexical terms × 3 × (256 + fields × 96) | Analyzer output and pre-tokenized document, not vector component postings | Remove from retained component sum; retain per-document native admission |
| Native writer 20,000,000 bytes | Tantivy worker buffers, held by Directory clones | Retain through successful join |
| Identity/manifest/Searcher charges | Retained identity arrays and fixed readers, temporary manifest serialization | Retain unchanged |
| Complete write/generation estimate | Pre-WAL admission of the whole business operation | Retain unchanged |

`retrieval_schema` disables lexical storage and payload storage. Its companion
`component::append` already estimates only the vector schema. Bulk admission had
instead accumulated the full lexical schema for every document indefinitely.
For the preserved 5,248-document fixture, that wrongly retained lexical portion
is 1,074,119,424 bytes; the component allowance is still 99,138,048 bytes.

Removing that charge alone is unsafe: Tantivy 0.27's `add_document` enqueues
pre-tokenized documents (upstream queue capacity 10,000) and returns before the
worker consumes them. Previously the native per-document analysis lease expired
at this return. The repair transfers it to shared Directory ownership before
queueing. Successful commit joins the preceding indexing workers, allowing those
leases to be refunded. Failed commit/join retains them with Directory clones.

Private bulk queues drain before exceeding 128 documents or 32 MiB of analysis
estimates. A single larger document is still admitted using the existing single
and shared limits and drained immediately. This bounds pending analysis without
rejecting previously legal individual documents or raising any budget. These
private commits do not reload/publish a Searcher, write the Record certificate,
or select a business cut. Additional small native segments are a maintenance
cost; this is not a throughput optimization.

Validation must include queue admission/refund, unpublished candidate drop,
failed low-resource admission followed by retry, worker-clone ownership, existing
pre-WAL oversized generation/document rejection, and the original HTTP fixture
with unchanged input/defaults. Results belong to the exact repair commit/binary.
The subsequent 16 MiB complete-state and 10,000 identity/operation boundaries are
still present and are not claimed fixed here.

Local validation at the initial repair checkpoint: all 40 lexical tests pass,
including queue ownership, low-admission retry, candidate drop, fixed Searcher,
rotation/generation recovery and native package lifecycle; the separate oversized
pre-WAL admission/retry regression passes (1/1). Logs are
`/tmp/anyseek-capacity-native-tests.log` and
`/tmp/anyseek-capacity-admission-test.log`. Full-target/HTTP capacity validation
is still pending; Windows has not run on this repair.

## Payload ceiling follow-up

At 64 selected Payload objects, incremental reuse has no free slot for the next
object. `4451c9c` rebuilds all referenced bodies/operation history into a private
closure at exactly that ceiling. It retains the 64-object cap and byte/count/body
budgets. Old files are not removed to make room; the existing DiskScope includes
both generations. The authenticated anchor selects the new closure only after
preparation/loading succeeds; the old anchor stays authoritative on pre-publication
failure. This is compaction, not shortening idempotency history.

Local production readers are protected by the owned partition guard; an active
background build suppresses `reclaim_anchor_objects` entirely. The existing
`retire_if_unpinned` gate additionally protects held package paths. No new deletion
path or changed remote publication/backup protocol is introduced. Review and
regressions must cover a reader/pin held across compaction, old authority after
injected preparation failure, and old-object reclamation only after release.

## Extended failure-boundary validation

On `43d789c`, full local all-targets completed with **351 passed, 0 failed,
13 ignored** (35 test groups). The first attempt failed during linking because
workspace disk was exhausted; a second attempt reached integration fixtures but
those relative paths still used the full workspace. Both infrastructure-failure
logs remain preserved. The successful run used a separate `/tmp` target and
fixture working directories, one compile job and disabled debug symbols; test
selection, assertions and debug assertions were unchanged. Full log:
`/tmp/anyseek-capacity-final-alltargets-isolated.log`.

Subsequent changes in this validation stage are test-only:

- Actual conservative estimates cross 32MiB with three documents while count is
  below128; the third drains the preceding two. A legal single larger document
  drains immediately. Exact Governor headroom is checked, with no Searcher
  publication until explicit flush.
- A per-Directory cfg(test) write-error gate is exercised by real Tantivy worker
  writes. Failed commit retains analysis; a failed join retains it with surviving
  Directory clones. Last worker/reader release refunds admission. This verifies
  actual error returns, not real hardware failure or OS power loss.
- The actual64-object closure is rejected after preparation and after candidate
  loading. Test-only unknown-outcome injection immediately before and after the
  journal certificate checks both recovery selections: old certificate plus
  durable tail, or the newly durable certificate. Both fence live access, retain
  all65 original receipts, and clean unselected orphans after recovery. Old
  Payload readers/pins remain valid; reclamation happens only after release.

Extended focused results: Runtime14/14 pass; anchor8/8 pass; partition5 pass,
1 existing child-worker entry ignored (invoked by its parent test). Logs:
`/tmp/anyseek-capacity-extended-runtime.log`,
`/tmp/anyseek-capacity-extended-anchor.log`,
`/tmp/anyseek-capacity-partition-tests.log`. These focused runs do not turn the
previous full-target SHA into a full-target result for a later SHA. Windows and
exact-head CI remain pending because draft-PR creation approval is outstanding.

To restore source-write headroom without discarding data, one failed-link
`.tmpf724695` artifact was preserved byte-for-byte under
`/tmp/anyseek-capacity-preserved-build-artifacts/`, verified by SHA256 before
removing its original cache path. Its manifest records both paths and the hash.
No source, raw experiment evidence, database or other cache was removed.
