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

Every observed HTTP error body is retained. All measured 503s were exactly
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

| Workload | Baseline success / 640 | Candidate success / 640 | Baseline 503 | Candidate 503 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 33 | 640 | 607 | 0 |
| batch + hybrid | 30 | 640 | 610 | 0 |
| ingest + lexical | 193 | 640 | 447 | 0 |
| Total | 256 / 1,920 | 1,920 / 1,920 | 1,664 | 0 |

Latency in milliseconds; each cell lists **run 0 / run 1**, not pooled quantiles.
Query tails below include successful requests only. The sparse successful baseline
batch samples are not stable tail estimates. Fast failures remain in the raw data
and `all` quantiles. Each process has 40 write samples, so write P99 is the maximum.

| Workload | Baseline query P95 | Baseline query P99 | Candidate query P95 | Candidate query P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 94.08 / 45.18 | 94.08 / 45.18 | 88.02 / 63.66 | 111.97 / 95.38 |
| batch + hybrid | 67.07 / 53.85 | 67.07 / 57.98 | 66.10 / 63.14 | 96.86 / 79.88 |
| ingest + lexical | 31.19 / 30.60 | 180.64 / 177.25 | 34.22 / 36.68 | 175.80 / 196.12 |

| Workload | Baseline write P95 | Baseline write P99 | Candidate write P95 | Candidate write P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 82.66 / 43.64 | 112.05 / 62.74 | 85.95 / 60.47 | 91.22 / 93.73 |
| batch + hybrid | 52.81 / 46.31 | 62.03 / 50.22 | 63.48 / 60.44 | 76.89 / 63.53 |
| ingest + lexical | 179.81 / 140.40 | 228.14 / 167.39 | 163.75 / 163.17 | 178.00 / 202.61 |

All 480 write HTTP requests (240/version) succeeded, with all 16 items acknowledged.
Every process ended at committed/lexical/vector sequence 1,640, with 640 new records,
no index error and no recovery requirement. CPU/RSS and post-load catch-up observations
are retained per process. Increased server work and some write-tail cost are expected:
the candidate actually executes the queries previously rejected. This is an
availability correction, not evidence of improved throughput or lower latency.

The fixed-total closed-loop runs have unequal temporal overlap. Baseline 297–320
queries/process started before the last write finished; candidate 95–163 did.
Success rates therefore describe the fixed workload, not a sustained-QPS production
SLA. The phased variant synchronizes each batch with eight queries to check repeated
interleaving separately; it is not pooled with the fixed-total matrix.

## Repeated interleaving results

The `phased-*` matrix uses the same binaries, seed, counts, order and CPU placement.
Each of 40 phases starts one batch writer with four readers making two queries each,
then joins them before starting the next phase. This limits the idle-query tail of
the fixed-total run; it is not a sustained open-loop workload. There is no query warmup
in either matrix.

| Workload | Baseline success / 640 | Candidate success / 640 | Baseline 503 | Candidate 503 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 491 | 640 | 149 | 0 |
| batch + hybrid | 529 | 640 | 111 | 0 |
| ingest + lexical | 534 | 640 | 106 | 0 |
| Total | 1,554 / 1,920 | 1,920 / 1,920 | 366 | 0 |

| Workload | Baseline query P95 | Baseline query P99 | Candidate query P95 | Candidate query P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 41.38 / 51.93 | 49.34 / 167.99 | 39.08 / 72.01 | 45.12 / 130.81 |
| batch + hybrid | 49.34 / 49.61 | 52.58 / 60.49 | 41.67 / 50.12 | 56.21 / 58.67 |
| ingest + lexical | 17.31 / 21.15 | 107.02 / 95.69 | 32.14 / 32.44 | 118.76 / 157.37 |

| Workload | Baseline write P95 | Baseline write P99 | Candidate write P95 | Candidate write P99 |
| --- | ---: | ---: | ---: | ---: |
| batch + lexical | 47.26 / 57.03 | 48.87 / 167.99 | 45.59 / 97.49 | 57.28 / 131.48 |
| batch + hybrid | 50.77 / 59.99 | 55.05 / 66.55 | 42.66 / 52.37 | 56.74 / 60.64 |
| ingest + lexical | 93.76 / 93.32 | 110.67 / 123.43 | 93.03 / 122.11 | 121.61 / 160.66 |

All 480 additional write requests and final counts/watermarks passed. Across both
final matrices the candidate had 3,840 successful queries, no 503, no transport error
and no budget refusal. Keep per-process tails and the two workload modes separate.

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
python benchmarks/query-503/summarize.py /absolute/evidence --prefix paired
```

The summarizer requires the complete 12-process matrix named
`PREFIX-REP-VERSION-ENDPOINT-MODE`, recomputes latency/error counts from raw bodies,
checks all per-item write receipts and final watermarks, and verifies the common seed
and binary fingerprints. Assertions must be enabled. It does not silently omit failures.

Baseline binary SHA256: `114e91fc77f883c74b94f9a064e0a6cefe4d6b56cca71f03f9f20e3abc9a6093`.
Candidate binary SHA256: `895264b25288db193f02f88147322860b57aba56a50b8f61407c6a287b983a2c`.
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

59 relevant release/fault-injection tests passed: 2 new output-wait tests, 6 committed
batch tests, 8 independent-query tests, 8 write tests, 3 publication/checkpoint recovery
tests, and all 32 server tests. The latter include the new actual-router body/header
wait and cancellation test, persistence-error body rejection, admission retention,
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
