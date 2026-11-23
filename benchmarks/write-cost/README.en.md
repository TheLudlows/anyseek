# Write cost comparison (#191)

**English** · [简体中文](README.md)

This directory contains scripts for manual measurements and compressed evidence. It leaves production algorithms and the default CI performance gates unchanged. See the [report](REPORT.en.md) for findings. **The complete HTTP workload matrix ran on tmpfs on the same host. Shared disk exhaustion prevented completion of the physical disk measurements.**

## Reproduction

Pin three separate checkouts and use binaries built from the specified commits:

| Label | Exact commit | Purpose |
| --- | --- | --- |
| before | `32225e5e656926736e81517837be65ebce3c88ca` | First parent of the #191 merge |
| after | `c01e2e172e9a3747da07fb008d2bf02ae6d1dea9` | #191 merge commit, testing the entire PR |
| latest | `768a18eb0ccb25d23d1f2b5bf9a09ec071afc857` | Tip of the main branch when this task began, with documentation-only changes since after |

Use a separate Cargo target directory for each checkout. If reusing cached dependencies, run `cargo clean -p anyseek --release` first to prevent incorrect reuse across checkouts based on old timestamps. Run `cargo build --offline --locked --release -p anyseek --bin anyseek_server -j 2` sequentially and immediately copy each server to its own binary path. Prepare any missing dependency caches before measuring. Complete all builds before starting the benchmark. Use the repository's release/default features, LTO, `panic=abort` and `target-cpu=native`, with fault injection disabled.

```bash
python benchmarks/write-cost/measure.py \
  --binary before=/absolute/binaries/before \
  --binary after=/absolute/binaries/after \
  --binary latest=/absolute/binaries/latest \
  --out /dev/shm/write-cost-timing
python benchmarks/write-cost/summarize.py /dev/shm/write-cost-timing

gcc -Wall -Wextra -Werror -O2 -shared -fPIC \
  benchmarks/write-cost/alloc.c -o benchmarks/write-cost/alloc.so
python benchmarks/write-cost/measure.py \
  --binary before=/absolute/binaries/before \
  --binary after=/absolute/binaries/after \
  --binary latest=/absolute/binaries/latest \
  --out /dev/shm/write-cost-allocation --batches 16 --allocation
python benchmarks/write-cost/summarize.py /dev/shm/write-cost-allocation
```

Use an output directory that does not exist. `--short --sizes 256 --batches 1` validates the driver only. The measurement, recovery and summary entry points reject `python -O` to keep their correctness assertions enabled. The benchmark uses Linux/glibc, defaults to server CPUs 0/1 and client CPUs 2/3, and allows both assignments to be overridden. Tokio and Rayon each use 2 workers. The script acquires `/tmp/anyseek-performance.lock` and stops if it detects interfering build or server processes. Finish other work using the same resources before starting. The lock only governs processes that honor it and cannot establish that the unseen host is idle.

## Measurement and validation

### Small-scale check of the write publication path

`publication_probe.py` uses the Python standard library and runs on Windows and Linux against two fixed server binaries. Each run creates a fresh local root, warms up three times, records per-request HTTP latency and Busy retries of the original request, and verifies query counts and receipt replay. By default each batch size runs in three separate processes. Execution stops on failure. Only `all_requested_runs_completed=true` in the report establishes completion of all requested scenarios.

```sh
python benchmarks/write-cost/publication_probe.py --binary /absolute/before --out target/publication-before.json
python benchmarks/write-cost/publication_probe.py --binary /absolute/after --out target/publication-after.json
```

Complete builds first, run the probe sequentially and use a new output path. The report captures each executable's SHA-256 hash. This probe measures sequential synchronous inserts in a fresh database without vectors. Historical scales, mixed traffic, saturation throughput and production tail latency require other workloads. Debug results do not replace release measurements on representative durable storage. Interpret it separately from the historical #191 matrix below.

### Historical #191 matrix

- Use the standard 10k tier, 1k or 5k initial records, identical 249–251B bodies and pregenerated 32-dimensional vectors. Create the initial data only with before, checkpoint it and close it normally. Each run starts from a copy with identical per-file SHA-256 hashes. The default two-day record and receipt retention applies. No retention-period GC or external embedding service cost is in the workload.
- Insert without vectors → Match(1) vector patch (body version stays at 1) → Match(1) replace body and vector (version 2) → Match(2) delete (version 3). Fix IDs, Operation IDs and batch order. Warm up with 5 requests per operation, then measure 128 single-item requests or 40 batch16 requests. The seed records stay untouched. The same set of temporary records undergoes all four operations.
- For each capacity, batch size and endpoint, run before/after/latest and latest/after/before, yielding two independent processes per version. Calculate percentiles within **one process and one scenario** and report the median of the two process metrics. With 40 batch16 requests, p99 is the maximum and cannot estimate stable production tails.
- Response time includes HTTP, deserialization and explicit Busy retries of the original request. Successful single-attempt HTTP latency is also retained in attempts. Check Busy both at the HTTP level and in per-item results under HTTP 200. Replay the entire original batch, verify previously successful receipts item by item, and count Busy responses and Busy items separately. Retry only explicit Busy. Unknown, transport error and unsuccessful per-item results do not count as successes.
- HTTP `/batch` calls `p.write` for each element in `writes` instead of one kernel `write_batch` call. `/ingest` batches commits through the committed queue and `commit_writes`. The endpoint gap for batch16 therefore includes differences in durable publications and native refresh counts.
- On receipt, poll both consumer watermarks every 2ms. The observed **additional catch-up time** includes observation overhead and sampling error. The same probe runs after synchronous batch calls, which can show nonzero catch-up time even when indexes are current. Subtracting ingest from batch does not isolate indexing cost.
- `wall_records_s` includes driver, resource and directory probes and raw evidence writes. `completed_records_s` divides record count by cumulative response plus catch-up time. Both report closed-loop C1 throughput with catch-up after each batch. Saturated queue throughput was not measured. Server CPU includes background work, indexing, logging and progress requests during that window. RSS and HWM are process metrics and HWM is the high-water mark since startup.
- Inspect new cache directories before and after each request and catch-up. This identifies windows that overlap cache creation, without measuring exact `rotate_tail` duration or excluding concurrent background builds. Keep ordinary windows and cache-creation windows, including regressions, Busy and outliers.
- Reuse the glibc counter from [#190](https://github.com/TheLudlows/anyseek/pull/190) at `b531aa60f770ffe1155a6dcb5f6f96eaaacb2c25` verbatim. HTTP and proc helpers come from the same script. Maintenance phase parsing reuses `capacity/async_compare.py`. Measure allocation in separate runs. Count successful malloc/calloc/realloc requests and cumulative requested bytes. Live or peak bytes, mmap and separate aligned allocation APIs are outside scope. Keep instrumented latency out of the normal timing results.
- For every run, verify full-text and vector results for the complete temporary ID set, removal of old terms, totals, versions and pending vectors. Outside the timed window keep three live records, one awaiting a vector, one with a patched vector and one updated. Sample receipts, close normally, reopen and replay **all** operations in their original order. Compare the original receipts item by item, check that committed_seq did not increase, and verify full contents and search results for all three live records. This normal reopen procedure does not establish power-loss or unknown-outcome recovery.

## Phase observability and design constraints

| Phase | Evidence in this task | Limits of attribution |
| --- | --- | --- |
| Planning | Exact before/after sources and end-to-end CPU and allocation evidence. #191 replaces the full shadow with the successful prefix of the current batch | No dedicated production planner timer, so the full delta cannot be attributed to planning |
| Receipt checks | Lookup and full comparison of requests with the same ID remain. Every new operation calls `has_expired_operations`, which can scan the retained prefix within the certified floor | No dedicated CPU stack sampling establishes this as the largest measured bottleneck |
| Persistence | Successful HTTP items and their receipts, plus replay after reopen. The local WAL path includes admission and append | No exact split for fsync, SQLite and I/O, or performance certification for real S3 or physical disk |
| Index consumption | Lexical and vector watermarks, visible search result checks and extra catch-up time | Both consumers overlap with foreground and background work, so their costs cannot simply be added or subtracted |
| Segment rotation | 128 mutable native update limit, cache-creation windows and latency evidence | Directory probes cannot establish exact rotation counts or exclusive rotation time |
| Background maintenance | Existing named production `record_maintenance` intervals and whole-process CPU | Intervals can nest and overlap. An exit can also indicate error or cancellation, so summing intervals does not give total cost |

A lightweight commit must make successful writes searchable while preserving the single WAL and durable authority, idempotency under the original ID, and unknown/fence recovery. The smallest searchable view published before success must simultaneously expose body identity, update and delete status, late vector patches and filter metadata. Full-text search must honor active identity on the vector and metadata side. Reuse the tail, bound synchronous publication CPU, allocations and rotation work, and leave expensive builds and merges in the background. Define Busy and backpressure. Keep complete rebuilds, global document frequency maintenance and secondary commit logs out of the response path. This PR does not implement this feature.


## Recomputing the evidence

`evidence.tar.gz` (approximately 7 MiB) contains only JSON and CSV, per-request gzip files, filtered maintenance logs, source and environment fingerprints, and failure evidence. It has no databases or build outputs. `timing/` and `allocation/` contain the final tmpfs matrix. `excluded-driver-v1/` and `excluded-overlay/` contain earlier rounds kept separate from the comparison. Each directory's `summary.json` corresponds to its per-request records. The archive's `SHA256SUMS.json` verifies files individually. See `recovery.json` for recovery results.

The summarizer reconstructs the full plan from workload sizes, batch sizes, versions and forward and reverse process order in the run's `environment.json`. It verifies that every process and all four operations occur exactly once, with the expected sample counts and binary fingerprints. It rejects missing processes or operations, duplicate identities, failed results or damaged percentiles, and never reports a partial matrix as the complete comparison.

```bash
mkdir /tmp/write-cost-evidence
tar -xzf benchmarks/write-cost/evidence.tar.gz -C /tmp/write-cost-evidence
python benchmarks/write-cost/summarize.py /tmp/write-cost-evidence/timing
python benchmarks/write-cost/summarize.py /tmp/write-cost-evidence/allocation
```

Do not point the all-success summarizer at excluded directories. `measured-harness.py` in the archive is the driver actually used for the final measurements. `post-measurement-resume-guard.patch` records the previously added `--resume` check for the completed directory state. This update adds full matrix validation and rejection of `python -O` in all three tools. It leaves the archived evidence, CSV, performance measurements and default measurement algorithm unchanged. `driver-correction.patch` records the initial per-item Busy fix. After the official measurement ended, the delivered recovery helper separately verified all 720 records on a copy of the original failed database. If new runs encounter similar durable_pending results during a vector patch, preserve the database and run:

```bash
python benchmarks/write-cost/recover_failure.py   --source /absolute/failed-run --out /dev/shm/recovery-check   --binary /absolute/binaries/before
```

`--resume` starts only planned processes that have not run and preserves existing passed/failed results. Keep the original snapshot for the unfinished capacity and the same epoch, binaries and parameters. A failed write leaves a failed result and its database for inspection and is not automatically retried into a success sample. Resource conflicts and driver errors stop the entire driver. Retain at least 512MiB of free output filesystem space before each request. TMPDIR is within each run directory. The allocation library must reside on a filesystem that permits executable mappings. In this run the `.so` on tmpfs links to the original build location.
