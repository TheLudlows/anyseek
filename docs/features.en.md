# Current features and limits

**English** · [简体中文](features.md)

HTTP and web UI data requests share the service's Record/catalog implementation. The old low-level Rust full-text API has been removed. The [Record contract](record-model.en.md) defines responsibilities. System power loss and long-running sustained capacity are still awaiting acceptance. [Service usage](../anyseek/src/server/README.en.md) · [Architecture](architecture.en.md) · [Roadmap and acceptance evidence](roadmap.en.md)

## Search and data

| Area | Current features and limits |
| --- | --- |
| Full text | BM25F/proximity, Boolean and phrase queries, multiple fields, Chinese segmentation, stop words, and N-grams |
| Vector | External JSON/base64, multiple vector fields, F32/I8, three distance metrics, quantization, and IVF ANN. The caller provides model inference. |
| Hybrid | RRF over a common full-text/vector view, with weights, candidate budgets, and per-hit ranks, raw scores, and contributions. The candidate window does not represent an exact corpus-wide union. |
| Time and lifecycle | UTC millisecond filters, captured evaluation_time, final candidate revalidation, and background TTL reclamation. The [Record contract](record-model.en.md#time-ttl-and-query-views) defines the restart clock-rollback boundary. |
| Filtering and ordering | Shared metadata predicates, field ordering, pagination, and projection. Reranking ANN/Hybrid candidates does not guarantee corpus-wide field Top-K. |
| Identity and writes | Stable Record IDs, version conditions, tombstones, ABA prevention, and retries using the original operation_id |
| Original data | The authoritative state retains the complete Document and original vectors. schema.store controls query projection. Persistence of the original data is independent of this setting. |
| Browsing | An empty query browses active records, including vector-only collections. The iterator pages by stable ID. A change to visible_seq or crossing the view's earliest TTL invalidates its cursor. |
| Persistence | Local Segment plus tail, or SQLite/S3 object commits with remote checkpoints and GC. Synchronous writes are durable and visible. The asynchronous indexing endpoint returns a durable business result. Real S3 and system power loss still require acceptance. |
| Service authentication and tenant namespaces | The upstream gateway handles user permissions. AnySeek validates the service credential and isolates collections, backups, and receipts using the trusted tenant header. It does not register or issue tenant keys. |
| Backups | Independent collection snapshots, backup retirement, full service/catalog backup, and restoration to a new root that has never been started |

The derived full-text index uses Tantivy and WAL-backed queryable deltas. Background materialization does not create a second commit authority. Vectors and metadata use separate components. Payload stores canonical original text and original vectors. The [architecture](architecture.en.md) covers queryable tails, budget reservations, and file ownership.

## Integration and limits

- [Service guide](../anyseek/src/server/README.en.md) covers startup, authentication, HTTP examples, queries, backup, and restoration.
- [Console guide](../console/README.en.md) covers tenant connections, import, embedding services, search, and deployment.
- [Resource and acceptance boundaries](../anyseek/src/server/README.en.md#current-resource-and-acceptance-boundaries) covers requests and responses, concurrency, bytes, and I/O budgets.
- [Roadmap and acceptance evidence](roadmap.en.md) covers pending work, measurement scope, and unverified environments.

Collections admit writes according to resource budgets. The query candidate cap is 10,000. The complete logical State is resident in memory. Unlimited writes, stable disk use, and a hard cap on whole-process RSS are beyond the guarantee. Receipts become eligible for reclamation after at least two days, subject to the safe cut and protection conditions.

Each collection has one partition. Cross-collection transactions, arbitrary historical reads, SQL/JOIN, a complete graph language, and distributed HA are unavailable. Upstream applications handle sources, relationships, parsing and chunking, Embedding, and Reflect. Saving URLs neither hosts nor backs up external files.

See [field types and numeric values](../anyseek/src/server/README.en.md#field-types-and-numeric-values) for supported fields, numerical precision, and null handling.
