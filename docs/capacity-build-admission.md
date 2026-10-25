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
