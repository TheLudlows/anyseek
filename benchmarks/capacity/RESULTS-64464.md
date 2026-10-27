# MIRACL partial-capacity comparison: 64,464 documents

This is the first real resource boundary of the explicit100k trial, not a100k pass. Both engines use the same fixed IDs/title/text, no normalization,393 original dev questions, and all994 positive qrel pairs. The40 previously used queries are nonblind;353 are held out. No query, label or production strategy was tuned for scores.

| Engine / strategy | Recall@10 | Recall@100 | nDCG@10 | MRR@10 | Empty /393 | Errors |
|---|---:|---:|---:|---:|---:|---:|
| anyseek and | 0.003817 | 0.003817 | 0.004105 | 0.005089 | 388 | 0 |
| anyseek or | 0.537068 | 0.805259 | 0.377901 | 0.385216 | 0 | 0 |
| Meilisearch strict_all | 0.003817 | 0.003817 | 0.004105 | 0.005089 | 385 | 0 |
| Meilisearch strict_last | 0.477829 | 0.728205 | 0.342304 | 0.353504 | 0 | 0 |
| Meilisearch default_last | 0.477829 | 0.728205 | 0.342304 | 0.353504 | 0 | 0 |

Metrics are macro means over all393 queries; positive-qrel denominator is the full query set, not only returned documents. nDCG uses graded2^grade−1 gains. Errors are separate and score zero. Per-query results permit independent recomputation; Recall@100 was independently recalculated.

## Scope and settings

- anyseek production af45faf; release SHA72bf6da6828bd6a73661b60584d5639711685f69c7c4cfb0aaaf2e9f09502ab1. Default JiebaMixedV1, title/text, top100. Explicit bounded_100k and3/6GiB budgets, not defaults.
- Meilisearch1.55.0 SHA7e33f5889235f14290074a19a6d3574bd9b10fb2ee8731eabb7dafd0714b0513. strict modes verified typo=false,prefix=disabled; default_last verified typo=true,prefix=indexingTime. maxTotalHits1000;top100 requested. Its word-limit/tokenization/last relaxation differ from anyseek OR; these are not matching-equivalent quality comparisons.
- Same4CPU/16GiB cgroup; each service CPU0/1, driver2/3; services run separately. Meili explicit6GiB indexing budget is not a hard RSS cap or identical ownership policy. No QPS or latency ranking is inferred.
- anyseek all393OR results exactly reproduce the pre-reopen quality aggregates. Both services recovered64464 documents. Meili indexing44.208s is not compared to anyseek sequential durable25-item batches: batching, acknowledgement and persistence differ.

## Capacity failure remains

anyseek accepted64464, then ordinal64465 failed a single3GiB admission. Explicit checkpoint subsequently refused the additional full-State clone under shared6GiB. Existing authority+tail reopened in4.059s;18 original receipts and5 exact query probes matched. This preserves failure evidence, not a claim that the100k configuration is operational at its ceiling. No coefficient, budget or timeout was relaxed.

## Reproduction and raw evidence

Use [quality.py](quality.py) and [protocol](QUALITY-PROTOCOL.md). Corpus SHA2d00fa102eda7bc56e651f9b21eb9a639cf75799b259e41c070583425ff58d86; accepted-ID SHA695c5c675b1ff34da71cd93dfe51737a0ea0131d5e515e60a02584622fad41f1. Raw per-query JSONL, actual settings, resource samples and split40/353 metrics remain in `/tmp/anyseek-capacity-phase25-{anyseek,meili}-64464`; capacity evidence in `/tmp/anyseek-capacity-phase24-release-100k`. These are environment paths, not downloadable attachments. RealS3, hard power loss and manual cross-AES remain untested.
