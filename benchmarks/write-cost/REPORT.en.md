# Bounded write comparison of the entire #191 PR

**English** · [简体中文](REPORT.md)

**These conclusions apply to this same-host tmpfs run. #191 shows clear CPU and throughput gains, with remaining tail latency, Busy results and large index allocations. Full physical-disk costs remain unverified.** Production code, algorithms, capacity limits and CI stayed unchanged. See the [README](README.en.md) for request semantics, raw evidence and reproduction commands.

## Scope and evidence

- before=`32225e5e656926736e81517837be65ebce3c88ca` (first parent of #191), after=`c01e2e172e9a3747da07fb008d2bf02ae6d1dea9`, latest=`768a18eb0ccb25d23d1f2b5bf9a09ec071afc857`. Run latest separately. The after and latest binaries have the same SHA-256 with documentation-only differences, so variation between their runs does not establish a new code gain. The baseline includes #187 and does not remeasure it. #191's original PR numbers isolate address movement and differ from this full-PR comparison. #192 addressed documentation and #190 covers query acceptance. Both are outside this measurement.
- Linux with an 8 CPU quota and 8 GiB, server CPUs 0/1, client CPUs 2/3, and 2 workers each for Tokio and Rayon. Use Rust 1.99.1, release/default features, LTO, panic=abort and target-cpu=native. Test 1k and 5k in the same standard_10k tier using 249–251B texts and fixed 32-dimensional vectors. All versions use seeded snapshots with identical per-file hashes. ABC/CBA gives two processes per version, with 5 warmups per scenario. Builds and performance tasks do not overlap. Check visible build and server processes before the run and every request. This cannot establish that the unseen host is idle. Official samples were written to tmpfs with at least 4,407,959,552 bytes free per request.
- Normal timing covered **48/48 processes, 192 windows, 16,128 measured requests and 73,728 measured mutations**. Separate allocation runs covered **24/24 processes, 96 windows and 3,840 measured requests**. Workloads included insert, vector patch, replace and delete, synchronous batch and asynchronous ingest, and batch1/16 (batch16 only for allocation). All completed processes had no unresolved write failure, including Busy retries under the original ID.
- Verify the full temporary ID set against full-text and vector results, old-term removal, totals, technical versions and pending vectors. Every process closes normally and reopens. Check full contents and query visibility of the retained records with a pending vector, completed vector patch or update. Replay all original operations and verify unchanged receipts. Timing runs replayed 82,128 original receipts, and allocation runs 69,240. Counts include warmups and 5 extra recovery checks. Crash and power-loss recovery, recall rates and floating-point scores were outside the test scope.
- Measured calls created a new cache in 144 observed windows per version, 48 each for inserts, vector patches and replacements. Deletes did not create a tail and were checked across existing segments. Directory observations do not isolate rotation timing.

## Historical change impact: before → after

Total throughput improved in all 32 paired scenarios (+3.97% to +681.94%, scenario median +27.13%). Window CPU per item fell in all scenarios (−3.23% to −92.90%, scenario median −27.91%). These figures weight the measured scenarios equally and do not estimate a production traffic mix. Server process CPU for the full matrix (startup, warmup, verification and shutdown included, replay after reopen excluded) fell from 78.887 to 58.785 seconds, a 25.48% reduction. The separate latest run used 60.817 seconds.

The table uses **5k, batch16**, and each cell is the median of two independent process metrics. Throughput includes driver and probe gaps. p95 and p99 reflect logical request latency including Busy retries, excluding subsequent catch-up. With only 40 measured requests per process, p99 is the process maximum and cannot serve as a stable production SLA.

| Endpoint / operation | Throughput items/s | Response p95 ms | Response p99 ms | CPU ms/item |
| --- | ---: | ---: | ---: | ---: |
| /batch / insert | 179.13→279.46 | 109.65→68.62 | 122.42→72.08 | 5.43→3.44 |
| /batch / vector patch | 146.43→210.40 | 198.49→124.38 | 264.87→158.24 | 6.84→4.77 |
| /batch / replace | 151.39→238.42 | 173.31→102.75 | 208.24→131.00 | 6.56→4.13 |
| /batch / delete | 641.82→5018.69 | 27.27→1.85 | 44.17→2.12 | 1.43→0.10 |
| /ingest / insert | 1374.49→1849.61 | 12.67→7.56 | 15.81→8.84 | 0.59→0.42 |
| /ingest / vector patch | 954.01→1296.28 | 32.13→24.57 | 46.88→30.91 | 1.04→0.77 |
| /ingest / replace | 854.71→1214.82 | 34.30→24.61 | 43.16→29.50 | 1.18→0.83 |
| /ingest / delete | 3900.49→6358.75 | 3.35→1.07 | 5.30→1.21 | 0.15→0.04 |

**Retained regressions:** p95 increased +9.29% for 1k/ingest/batch1/vector patch. p99 increased for 1k/batch/batch1/replace (8.635→17.365ms, +101.09%), 5k/batch/batch1/insert (9.527→13.256ms, +39.14%) and 5k/batch/batch1/vector patch (11.464→12.670ms, +10.52%). These samples are retained. The maximum response among all measured requests was 265.402/181.831/207.185ms for before/after/latest. A lower maximum does not establish a gain in every p99.

Before/after/latest recorded **50/51/47** Busy responses, including **10/11/7** per-item Busy results in HTTP 200 responses. The rest were outer Busy responses. Every affected request preserved its original request and receipts and ultimately succeeded. Backpressure remains after #191. Scenario median RSS changed between −18.55% and +20.49%. Maximum process HWM rose from 265.70 to 271.69MiB, with latest at 269.97MiB. RSS did not consistently fall.

## Independent verification of the current main branch

This table also uses 5k/batch16. The binary matches after, but the measurements come from separate processes. `results/timing/` contains all results for the 32 scenarios and per-process data. The historical gains are calculated separately.

| Operation | /batch throughput / p95 / p99 | /ingest throughput / p95 / p99 |
| --- | ---: | ---: |
| Insert | 257.38 / 76.69 / 81.85 | 1858.66 / 7.76 / 11.81 |
| Vector patch | 195.95 / 150.84 / 205.45 | 1141.78 / 26.52 / 28.07 |
| Replace | 212.21 / 129.00 / 153.69 | 977.43 / 26.33 / 33.22 |
| Delete | 5145.92 / 1.75 / 1.92 | 5874.17 / 1.35 / 1.48 |

Throughput is in items/s and latency in ms. The maximum observed extra time for both indexes to catch up after asynchronous commit was 43.667/26.370/44.345ms for before/after/latest. This includes 2ms polling uncertainty and query overhead. A nonzero synchronous batch probe does not imply missing visibility. Caught-up watermarks do not establish that all background work is idle.

## Allocation and remaining bottlenecks in priority order

In 16 separately instrumented paired scenarios, malloc/calloc/realloc call counts fell 18.04% to 99.09% (scenario median 47.05%). Cumulative requested bytes fell 1.12% to 97.06% (scenario median **only 5.04%**). Allocation count improvements do not imply proportional byte or RSS savings. Cumulative requested KiB per item for 5k/batch16:

| Operation | /batch before→after | /ingest before→after |
| --- | ---: | ---: |
| Insert | 19353.88→18408.51 | 1313.72→1244.28 |
| Vector patch | 19968.47→18889.37 | 1916.91→1831.18 |
| Replace | 19963.58→18804.02 | 1872.41→1775.04 |
| Delete | 1231.29→36.21 | 91.38→16.73 |

Cumulative allocation requests include indexing, background work, logging and progress reads. They do not establish live or peak memory or leaks and exclude separate aligned allocation and mmap APIs.

| Priority | Assessment and evidence | Next step and limits |
| --- | --- | --- |
| 1 | `/batch` calls `p.write` per item. It commits and waits for the native index separately for each item. `/ingest` uses the committed queue and `commit_writes`. Synchronous body writes still request about 18MiB per item cumulatively, versus about 36KiB for deletes | Check durable publications and native flush counts for synchronous batching first, then attribute allocation with call stacks. The endpoint gap combines batching and waiting, so it cannot all be assigned to indexing. This PR leaves the protocol and algorithms unchanged |
| 2 | Rotation and maintenance still cause Busy responses and tail latency. Latest observed interval maxima were capture_locked 46.149ms, build_unlocked 129.964ms, install_locked 10.103ms and cleanup_locked 18.998ms | First locate snapshot capture and cleanup under lock to reduce expensive work before successful responses. Intervals nest or overlap and can exit on error or cancellation, so they cannot be summed into total maintenance time. Directory probes do not isolate rotation time |
| 3 | Every new item still calls `has_expired_operations`, which scans the retained receipt prefix inside the certified floor. PlanningView already removed the full shadow, so do not duplicate that work | Measure the CPU share of the scan with focused sampling before evaluating a bounded cursor or incremental check. This scale risk is visible in source, but there is no separate phase CPU measurement establishing it as the largest bottleneck |
| Pending measurement | The local WAL still persists the log and witness synchronously. Full State, receipts and derived components remain resident or continue to accumulate | tmpfs cannot establish physical-disk fsync, S3, long retention windows, or high-dimensional vectors with large bodies. Prioritize the same protocol on a dedicated durable disk before estimating physical-disk throughput |

Planning, receipt checks and persistence lack separate production timers. This task reports request/window CPU, allocation and static call relationships without a production instrumentation patch. Maintenance intervals come from existing logs and do not provide a complete phase profile.

## Constraints for lightweight commits with immediate search visibility

Before reporting success, publish only a bounded searchable view using the existing tail. Keep full-text search consistent with active vector and metadata identities, updates and deletes, late vector patches and filters. A caught-up full-text consumer alone does not prove full visibility. Do not move a full index drain, complete rebuild, global document frequency maintenance or per-record native refresh into the new success path. Keep expensive maintenance in the background and define admission and Busy behavior. If physical batching changes, preserve the already-successful per-item prefix, original-ID receipts, and unknown/durable_pending/fence recovery with one commit authority. Cross-segment scoring and actual concurrent reads and writes still need verification. This task did not implement immediate visibility.

## Failed attempts and unfinished work

Initial overlay run 1 hit per-item Busy in HTTP 200 and the driver stopped. After correcting whole-batch retry the benchmark started another run. In round 2, before/1k/batch16/r1 returned durable_pending at vector-patch seq=2108 and left 12 items unattempted. The shared disk then hit ENOSPC and filled again after regenerable task files were cleared. **The exact syscall error was not captured, so this does not establish an engine defect or an improvement due to #191.** All raw samples generated in both runs are retained separately, excluded from the tmpfs comparison and not discarded as noise.

The failed database was copied to tmpfs and reopened with the same before binary. The commit and both indexes were at 2108 and the durable_pending receipt was present. All 1,104 preceding receipts replayed unchanged. Completing the 12 remaining items advanced the watermark to 2120. Checks passed for 720 full-text hits, 400 vector hits and all 720 records. See `recovery.json` in the archive. This verifies restart recovery after an actual process run failed, without establishing power-loss recovery. The database itself is not committed to the repository.

The environment blocked the complete physical-disk comparison. S3, Windows, power loss, long-running GC, large bodies/high-dimensional vectors, concurrent queries, saturated queue throughput and allocator live/lease peaks were not measured. 5k is a workload point within standard_10k and does not establish a 100k capacity. Follow-up order is **dedicated disk measurement → attribution of synchronous per-item commits, flushes and allocations → locked segment rotation work → lightweight searchable publication within those constraints**.
