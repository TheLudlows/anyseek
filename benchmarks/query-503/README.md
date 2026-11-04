# Concurrent-query publication fence

Baseline: `5976daa1efb932043503298fce941a5dec03dc05` (latest master at task start, includes #198).
No open PR duplicated this work at inspection time. This is an isolated local-storage
experiment, separate from the #162 capacity investigation.

## Cause and change

Normal synchronous batch publication fences output while the durable changes and
both index consumers are being published. The writer owns the partition mutex
through this interval. Query scoring already acquires that mutex at capture and
revalidation, but the HTTP handler's initial `output_permit`, response construction,
header middleware and body-frame checks previously checked the fence without waiting.
Thus ordinary concurrent writes could produce `503 recovery_required`, or a body
transport error if publication started after the response headers were handed off.

The query transport permit now waits for publication under the existing partition
mutex, then applies the **unchanged** output fence/revision check. The header and
body handoff do the same for these permits. Encoding occurs outside that mutex;
the synchronous handoff releases all guards before waiting on network I/O. Pending
bodies retain their response bytes; cancellation aborts their independently polled
publication waiter. The waiter inherits the request budget and shutdown activity;
its completion never substitutes for the actual frame-handoff fence check. Saved permits
use weak partition owners, so an unpolled response cannot preserve an old owner
across reopen. Internal callers and other endpoint permits retain their existing
fail-fast checks. No WAL, indexing, write ownership or consistency barrier was removed.

[Independent review](REVIEW.md) found an unbounded publication-wait path in the
initial candidate. The final build places header waiting inside the original request
deadline and captures that same deadline for all body waits/retries. Late first polls
do not get a fresh budget. Expired publication waits discard undelivered bytes and
return TimedOut; normal unconsumed responses retain their existing transport-owner
quota/lifecycle semantics. The production request budget remains 30 seconds.

Every load-probe HTTP error body is retained. All measured 503s were exactly
`{"code":"recovery_required","message":"RecoveryRequired"}`. There were no measured
429s, storage errors, unknown outcomes or connection errors. All final-matrix write receipts,
final indexed counts and terminal health checks succeeded. Deterministic paused
publication tests exercise the old fast rejection and the new wait, while consumer
failure tests retain rejection and replay recovery. There is no per-request internal
trace splitting the baseline 503s among capture, response construction and header
handoff; those sites share the error code. Do not claim that site-level breakdown.

## Fixed-total results

One baseline checkpoint: 1,000 records, 32-dimensional vectors, standard_10k profile.
Each independent process gets an identical checked copy. Four reader threads make
80 requests each; one writer submits 40 batches of 16 new records. No retries.
Each version runs twice, in baseline/candidate then candidate/baseline order, with
lexical + batch, hybrid + batch, and lexical + ingest. The shared performance lock
and visible-process check exclude simultaneous builds/servers. Server CPUs 0/1,
client CPUs 2/3; Tokio/Rayon each 2 workers. Linux overlay filesystem, 4-CPU cgroup
quota, 16 GiB memory limit; release/default features, LTO, target-cpu=native.
No query warmup is performed. All final numbers below use the deadline-fixed build.

Each count below covers two runs (640 queries/version/workload). Timeout and
transport columns show baseline / candidate; body-stage timeout root causes cannot
always be identified from a connection failure, so both categories are retained.

| Workload | Baseline success | Candidate success | 503 baseline / candidate | Identified timeouts | Transport failures |
| --- | ---: | ---: | ---: | ---: | ---: |
| batch + lexical | 54/640 | 640/640 | 586 / 0 | 0 / 0 | 0 / 0 |
| batch + hybrid | 77/640 | 640/640 | 563 / 0 | 0 / 0 | 0 / 0 |
| ingest + lexical | 159/640 | 640/640 | 481 / 0 | 0 / 0 | 0 / 0 |
| Total | 290/1,920 | 1,920/1,920 | 1,630 / 0 | 0 / 0 | 0 / 0 |

Latency is in milliseconds. Each cell is **run 0 / run 1**; quantiles are not
pooled. Each run has only 40 writes, so write P99 is its maximum. Sparse successful
baseline query samples are not stable tail estimates.

**Successful query requests only:**

| Workload | Baseline P95 | Baseline P99 | Candidate P95 | Candidate P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 68.05 / 40.38 | 68.05 / 51.31 | 51.89 / 56.38 | 64.94 / 81.63 |
| batch + hybrid | 45.73 / 44.94 | 53.79 / 53.11 | 58.13 / 48.28 | 83.50 / 62.35 |
| ingest + lexical | 166.90 / 130.64 | 174.82 / 137.06 | 31.39 / 27.36 | 161.68 / 168.17 |

**All query requests, including fast failures:**

| Workload | Baseline P95 | Baseline P99 | Candidate P95 | Candidate P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 4.19 / 27.03 | 34.82 / 40.31 | 51.89 / 56.38 | 64.94 / 81.63 |
| batch + hybrid | 27.96 / 20.78 | 45.21 / 39.09 | 58.13 / 48.28 | 83.50 / 62.35 |
| ingest + lexical | 25.67 / 21.12 | 166.90 / 135.03 | 31.39 / 27.36 | 161.68 / 168.17 |

**Write HTTP latency (all requests succeeded):**

| Workload | Baseline P95 | Baseline P99 | Candidate P95 | Candidate P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 44.32 / 40.24 | 53.67 / 43.18 | 47.29 / 54.75 | 57.43 / 61.55 |
| batch + hybrid | 43.16 / 38.17 | 58.75 / 45.50 | 48.79 / 46.72 | 88.18 / 59.42 |
| ingest + lexical | 144.09 / 123.37 | 169.06 / 134.21 | 145.21 / 147.85 | 162.56 / 174.64 |

All 480 write HTTP requests (240/version) succeeded, with all 16 items acknowledged.
There were zero write timeouts, transport errors or failed items. Every process ended
at committed/lexical/vector sequence 1,640, with 640 new records, no index error and
no recovery requirement. CPU/RSS and post-load catch-up observations remain in the
per-process summaries.

Queries starting before the final write finished: baseline 316–320,
candidate 107–161 per process. This closed-loop fixed-total
comparison has unequal temporal overlap, motivating the repeated-interleaving matrix.
It does not establish a sustained-QPS SLA.

## Repeated interleaving results

The same binaries, seed, counts, CPU placement and run order are used. Each of
40 phases starts one writer with four readers making two queries each, then joins
them before the next phase. This limits the idle-query tail; it is not an open-loop
saturation workload. These results are kept separate from the fixed-total matrix.

Each count below covers two runs (640 queries/version/workload). Timeout and
transport columns show baseline / candidate; body-stage timeout root causes cannot
always be identified from a connection failure, so both categories are retained.

| Workload | Baseline success | Candidate success | 503 baseline / candidate | Identified timeouts | Transport failures |
| --- | ---: | ---: | ---: | ---: | ---: |
| batch + lexical | 467/640 | 640/640 | 173 / 0 | 0 / 0 | 0 / 0 |
| batch + hybrid | 522/640 | 640/640 | 118 / 0 | 0 / 0 | 0 / 0 |
| ingest + lexical | 543/640 | 640/640 | 97 / 0 | 0 / 0 | 0 / 0 |
| Total | 1,532/1,920 | 1,920/1,920 | 388 / 0 | 0 / 0 | 0 / 0 |

Latency is in milliseconds. Each cell is **run 0 / run 1**; quantiles are not
pooled. Each run has only 40 writes, so write P99 is its maximum. Sparse successful
baseline query samples are not stable tail estimates.

**Successful query requests only:**

| Workload | Baseline P95 | Baseline P99 | Candidate P95 | Candidate P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 40.23 / 43.93 | 44.46 / 51.84 | 44.05 / 48.10 | 60.07 / 54.02 |
| batch + hybrid | 42.69 / 48.27 | 45.49 / 70.66 | 51.01 / 43.46 | 93.15 / 53.08 |
| ingest + lexical | 91.31 / 20.93 | 132.94 / 106.33 | 29.62 / 29.68 | 101.68 / 112.32 |

**All query requests, including fast failures:**

| Workload | Baseline P95 | Baseline P99 | Candidate P95 | Candidate P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 35.95 / 41.84 | 44.16 / 51.82 | 44.05 / 48.10 | 60.07 / 54.02 |
| batch + hybrid | 41.54 / 47.46 | 44.80 / 60.95 | 51.01 / 43.46 | 93.15 / 53.08 |
| ingest + lexical | 25.76 / 23.72 | 132.71 / 106.94 | 29.62 / 29.68 | 101.68 / 112.32 |

**Write HTTP latency (all requests succeeded):**

| Workload | Baseline P95 | Baseline P99 | Candidate P95 | Candidate P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 43.13 / 51.87 | 45.01 / 57.11 | 57.52 / 53.47 | 72.68 / 56.85 |
| batch + hybrid | 45.93 / 60.05 | 54.13 / 71.17 | 62.27 / 48.13 | 94.21 / 57.50 |
| ingest + lexical | 103.49 / 106.19 | 138.17 / 109.75 | 100.54 / 106.92 | 102.16 / 115.48 |

All 480 write HTTP requests (240/version) succeeded, with all 16 items acknowledged.
There were zero write timeouts, transport errors or failed items. Every process ended
at committed/lexical/vector sequence 1,640, with 640 new records, no index error and
no recovery requirement. CPU/RSS and post-load catch-up observations remain in the
per-process summaries.

Across both final matrices, the candidate completed 3,840/3,840 queries and 480/480
write requests. Some write tails increased: successful searches perform work that
the baseline rejected. This is an availability fix, not a claim of uniformly lower
latency or improved saturated throughput. Do not pool the two workload modes.

## Reproduce and inspect

Build exact baseline and candidate release binaries separately and copy each to an
immutable path. Do not measure while compiling. `--seed` accepts the baseline's
`seed-1000` directory together with its sibling `seed-1000.json`; their hashes and
operation-ID epoch are checked. Output directories must not already exist.

```bash
python benchmarks/query-503/measure.py --binary /absolute/baseline --out /absolute/seed-run
python benchmarks/query-503/measure.py --binary /absolute/candidate \
  --out /absolute/candidate-run --seed /absolute/seed-run/seed-1000 \
  --endpoint batch --mode lexical
# Add --phased to synchronize every batch with eight reader requests.
python benchmarks/query-503/summarize.py /absolute/evidence --prefix deadline-paired
```

The summarizer requires the complete 12-process matrix named
`PREFIX-REP-VERSION-ENDPOINT-MODE`, recomputes latency/error counts from raw bodies,
checks all per-item write receipts and final watermarks, and verifies the common seed
and binary fingerprints. Assertions must be enabled. It does not silently omit failures.

Baseline binary SHA256: `114e91fc77f883c74b94f9a064e0a6cefe4d6b56cca71f03f9f20e3abc9a6093`.
Candidate binary SHA256: `50a08e5825c926f9910d17ecda38e2e741d41a69e877719c23e2aa2a83b9c593`.
Seed file-manifest digest: `6f51b24898946f8cf86fc5f6ec337732e2efb69e7feab61174af0af614b5a82d`.

Preliminary `before-*` runs are separate from the paired matrix. In particular,
`before-ingest-lexical` failed a **driver** assertion by checking count before the
asynchronous index caught up (608 indexed, lexical_seq 1608, committed_seq 1640).
Its raw data and failure are preserved. The corrected driver waits for both consumers
after the timed window; no measurements or failures were rewritten.

This does not establish real S3/physical-disk performance, large-corpus capacity,
long-running saturation, Windows behavior, or performance for updates/deletes.
Other HTTP read endpoints still use fail-fast permits and are outside this query fix.

## Correctness and retained failures

60 relevant release/fault-injection tests passed: 2 new output-wait tests, 6 committed
batch tests, 8 independent-query tests, 8 write tests, 3 publication/checkpoint recovery
tests, and all 33 server tests. The latter include the new actual-router body/header
wait and cancellation test, the three-case shared-deadline test, persistence-error
body rejection, admission retention,
collection generation behavior and shutdown. The full repository/CI suite was not run.

The first candidate passed fixed-total HTTP load but retained a queued mutex waiter
in an unpolled response body. A deliberate backpressure test timed out after 15 seconds.
That implementation was discarded: final body waiters run independently, release the
mutex without requiring another body poll, abort on body drop, and retain shutdown
activity through cleanup. The same regression test now passes and verifies the writer
finishes while the old body remains unpolled. `first-paired-*`, its measured harness,
first-candidate binary fingerprint and timeout log are retained separately and never
pooled with the final matrices.

`evidence.tar.gz` contains raw request bodies, per-process summaries/logs, recomputed
matrices, environment and verification metadata, measured harnesses, source diff and
SHA256 manifest. Databases and binaries remain in `/workspace/query503-evidence` on
the task environment; they are not included in the repository archive. The preliminary
ingest driver failure and discarded-candidate timeout database are retained there too.

The reviewed `bba809f` candidate remains in `paired-*` / `phased-*`, separate from
the final `deadline-paired-*` / `deadline-phased-*` data. Its original report and
binary fingerprint are archived. The independent finding, follow-up review, scope
limits and timeout test outcome are documented in [REVIEW.md](REVIEW.md).
