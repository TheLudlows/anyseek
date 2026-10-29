# MIRACL capacity deadline and same-corpus comparison: 68,825 documents

The fixed 100k-target run stopped at its first HTTP deadline failure. **100k has
not passed.** The previous 64,465th-write quota boundary was crossed without
increasing the 3/6 GiB budgets. Original failure evidence remains unchanged.

## Capacity and recovery

- Tested commit: `331a511ca95cc27ed871bff634ce4d04a039c2ef`, release profile,
  binary SHA256 `b34a3b901d96645fef37ed89d25c8e465a5e8451a2bc4c4a2402efb4da41c26b`.
  Explicit `bounded_100k`, 3 GiB single / 6 GiB shared; default settings unchanged.
- First-pass acknowledged writes: 68,800. The next 25-item batch returned HTTP503
  `outcome_unknown` at 30,001ms, matching the unchanged 30s service deadline.
  No write was blindly resubmitted. Read-only original-operation lookup found all
  25 receipts (seq68801–68825), with 68,825 visible records and retained operations.
  The first failed ordinal denotes the batch response, not a proven rejected write.
- The original summary/accepted-ID list still says 68,800. A separate observed
  manifest adds only the 25 verified identities for this 68,825-document comparison.
- 10k/20k/50k checkpoint and reopen passed. At50k: checkpoint19.13s, reopen2.64s.
  After the deadline boundary, one new explicit checkpoint returned200 in26.74s;
  reopen4.12s,29 original receipts and5 query ID/score/count probes matched.
  Retrying one already-confirmed operation after reopen returned its exact receipt
  and did not increase the record count. This does not erase the original503.
- Load-to-boundary5268.22s; sampled service peak RSS711532KiB. Logical leases are
  not RSS limits. The data root is local journal/Payload, not SQLite/S3.

## Why writes slow down

These intervals sum batch-request latency only: explicit checkpoint/reopen pauses
are excluded. The driver issues one HTTP request at a time; each request contains
25 independent writes. This is the actual capacity protocol, not a tuned QPS test.

| Input interval | Documents / request-second |
|---|---:|
| 0–10k | 74.88 |
| 10–20k | 32.40 |
| 20–30k | 20.22 |
| 30–40k | 13.34 |
| 40–50k | 9.71 |
| 50–60k | 8.36 |
| 60k–67,425 (diagnostic capture) | 6.89 |

The source executes each batch item through `p.write().await`
([HTTP handler](../../anyseek/src/server/record_service.rs)). Each operation synchronizes
its journal and witnesses, then publishes a Tantivy commit/reload. This is not a
shared25-operation durable commit. The mutable tail rotates after128 published
updates ([runtime](../../anyseek/src/record/lexical/runtime.rs)); the foreground
write waits for certified anchor publication. Anchor preparation still walks,
serializes and authenticates the full State/history; native segments are reused
when possible and rebuilt when their finite package limit requires compaction
([anchor](../../anyseek/src/record/anchor.rs),
[framed State](../../anyseek/src/record/state_chunks.rs)).

After50k, the diagnostic capture found138 batches crossing a128-update rotation
boundary versus573 ordinary batches. Rotation-crossing batches occupied2041.48s
of2315.83s request time: **19.4% of batches,88.2% of elapsed request time**.
Their median was14.17s, versus0.463s for ordinary batches; all ten slowest batches
crossed rotation boundaries. This localizes the growing cost to the synchronous
rotation path; it does not split serialization, hashing, native build and fsync
into independently measured percentages. A5s sample consumed4.55 user CPU seconds
and0.46 system CPU seconds, showing active processing. `/proc/PID/io` was denied
and wait-channel readings were masked; no separate fsync-time claim is made.

The next repair should reuse authenticated immutable State blocks/deltas instead
of rewriting/reauthenticating all history every128 updates. Preserve complete
history, certification, durable+visible receipts and bounded ownership. Merely
raising128, weakening durability, or extending the deadline is not the proposed
fix. Per-operation native publication is a separate remaining cost; group commit
needs its own semantic design and is not implemented here.

## Quality at the actual observed population

Same68,825 IDs and original title/text, no normalization,393 formal MIRACL dev
questions, all994 positive qrel pairs.40 previously used/nonblind queries and353
heldout queries remain separate; inputs and qrels were not tuned.

| Engine / strategy | Recall@10 | Recall@100 | nDCG@10 | MRR@10 | Empty /393 | HTTP errors |
|---|---:|---:|---:|---:|---:|---:|
| anyseek AND | .003817 | .003817 | .004105 | .005089 | 388 | 0 |
| anyseek OR | .529901 | .803478 | .372463 | .381923 | 0 | 0 |
| Meili strict all | .003817 | .003817 | .004105 | .005089 | 385 | 0 |
| Meili strict last | .476133 | .723879 | .340825 | .352642 | 0 | 0 |
| Meili default last | .476133 | .723879 | .340825 | .352642 | 0 | 0 |

Recall@100: anyseek OR .807917(nonblind40)/.802975(heldout353);
Meili last .682083/.728615. Both strict AND/all are0/.004249. Metrics are macro
means over the complete query group. Positive denominators retain all qrels;
nDCG uses2^grade−1, MRR truncates at10, errors score zero and remain separately
counted. All5 modes ×3 groups were independently recomputed to1e-12 tolerance.

anyseek uses default JiebaMixedV1; Meilisearch1.55.0 binary SHA256
`7e33f5889235f14290074a19a6d3574bd9b10fb2ee8731eabb7dafd0714b0513`.
Both search title/text and request top100. Meili strict groups read back
`typoTolerance.enabled=false`, `prefixSearch=disabled`; default last reads back
true/indexingTime. Meili word limits, tokenization and last-word relaxation differ
from anyseek OR. Empty strict-AND questions do not mean Chinese is unsearchable.
This table is not a matching-equivalent universal engine ranking.

Services ran separately on the same4CPU/16GiB cgroup, service CPUs0/1 and client2/3.
Meili indexing used an explicit6GiB budget (not equivalent to anyseek ownership
accounting or a hard RSS cap),1000-document tasks, and completed in48.60s. Peak
sampled Meili RSS1589020KiB; reopen52.80ms preserved68,825 documents. These timings
use different batching/acknowledgement/durability paths and are not an equal
contract throughput ratio. Disk monitoring stopped neither run; Meili's lowest
observed free space was3129049088 bytes, above the2GiB floor. No data was removed.

Compared with [64,464 results](RESULTS-64464.md), query/qrels/preprocessing/fields
are unchanged and all positives were already present. The new corpus contains4361
additional distractors and uses the ownership repair; it is not an identical
candidate-set regression experiment. OR Recall@100 .805259→.803478 and Meili last
.728205→.723879 must not be attributed solely to engine changes.

## Reproduction and evidence

Use [baseline.py](baseline.py), [quality.py](quality.py) and the
[quality protocol](QUALITY-PROTOCOL.md). Corpus SHA256
`2d00fa102eda7bc56e651f9b21eb9a639cf75799b259e41c070583425ff58d86`;
queries SHA256 `ebb564fd7b99fb02cdad3d9e9809bd62f71ed4237edcb10cc473c979b42a6447`;
observed accepted-ID SHA256 `b719b28d05ac9a468ef50ce22a01cbc1eaf355135ae84fb963d79459959b3a4b`.

Raw evidence remains under `/tmp/anyseek-capacity-phase30-release-100k` (original
responses and unknown-operation observation), `/tmp/anyseek-capacity-phase31-observed-68825`
(derived manifest), `/tmp/anyseek-capacity-phase31-{anyseek,meili}-68825`
(settings/per-query results/resources), and `/tmp/anyseek-capacity-phase31-recovery-68825`
(checkpoint, receipts and query stability). Diagnostic/independent recomputation
records are `/tmp/anyseek-capacity-phase30-write-diagnosis.json` and
`/tmp/anyseek-capacity-phase31-independent-metrics.json`. The recovery driver is
`/tmp/anyseek-capacity-phase31-recovery.py`; no failed writes are resubmitted before
their outcomes are resolved. These are environment paths, not downloadable links.
Real S3, hard power loss and manual cross-AES remain unverified.
