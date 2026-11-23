# Query identity alignment: bounded validation

**English** · [简体中文](README.zh-CN.md)

Baseline `fbdb904e83461cc5fccde0bb7a2953340959adec`. Production/test tree before adding these reproducibility files: `45a5c1bf9f7ed74e10b15642e6555d10a31c2842`.

The change removes repeated native identity selection and owned identity copies. It preserves live-vector/native intersection (asynchronous consumers may lag), full-corpus BM25 statistics, filters, ordering, Count, Hybrid/RRF, admission and fixed snapshot/pin lifetimes. It does not change persistent storage or add a global cache.

## Checks on the unchanged production/test source
Core 207/207; isolated write path 62/62; query 11/11; lexical units 59 passed and one unrelated I/O diagnostic ignored; lexical correctness 6/6 small-scale; eight additional integration targets 15/15; default and no-default feature checks, source boundaries and whitespace passed. Independent source review found no blocker. These exact-source checks are reused; adding benchmark scripts/data does not imply a second Rust regression run. Existing compiler warnings remain; strict Clippy, full scale, real S3, native Windows locally and power-loss validation were not run. Windows must be assessed using this PR head CI. The separate #209 historical Windows recovery failure remains unexplained; its temporary diagnostics are not included.

## Core API baseline
Three ABBA groups per population, 1000 TTL / 8000 no TTL, two-token text, 3D exact ANN. Each process runs 10 warmups and 60 samples per case. Exact IDs, score bits, channel ranks and counts matched. Lexical medians of run medians were 2.652 → 1.749 ms and 23.154 → 14.770 ms. Hybrid medians were 2.909 → 1.998 ms and 23.399 → 15.448 ms. Original samples are retained in core-api-samples.json.gz. These small-text timings alone do not establish HTTP or production capacity.

## Actual Rust allocator requests
Separate diagnostic GlobalAlloc wrapper around System; all threads, alloc/alloc_zeroed/realloc call count and requested sizes. Realloc counts full new size, not net growth. Counter reads are separate atomics, so per-query boundary/background noise is possible. C allocations, memory maps, deallocation and resident/peak bytes are not counted. Admission estimates are unchanged and not presented as allocation savings. Do not use this instrumented binary for clean latency claims. Same core fixtures, three ABBA groups,360 samples per version/population/case; parity passed.

Values below are medians per measured query.

|N|case|base calls|candidate calls|base requested bytes|candidate requested bytes|
|---:|---|---:|---:|---:|---:|
|1000|lexical|7020|2425|819225|448065|
|1000|count|6464|1869|670043|298883|
|1000|hybrid|7129|2534|789869|418709|
|8000|lexical|53872|17142|6270406|3307702|
|8000|count|50644|13914|5206310|2243606|
|8000|hybrid|53336|16606|5720885|2758181|

## HTTP queries with continuous asynchronous updates
N=2048, with 768-dimensional seeded F32 vectors (seed20261109). The body includes common, tag and unique markers plus 24 generated tokens, with integer facets. Use ExactANNAll, a common query, Top10 and the body/serial/group projection. One collection. Four serial query workers each issue 120 queries at 30ms intervals. One writer issues 72 batches of 4 conditional updates at 50ms intervals. This is a fixed-workload paced closed loop, not a guaranteed open arrival rate. Identical update payloads are precomputed and each run starts from an identical immutable seed copy. Three ABBA groups per lexical/hybrid/facet-sort mode yield 36 processes, 17,280 timed queries and 10,368 updated records. Ten query warmups per run. Initial and final ID, version, document, score and channel parity passed, along with new- and old-term checks after updates. Runtime clocks are excluded from signatures. Counts can temporarily decrease during asynchronous consumption. Exact N is enforced before and after drain. Keep all attempts and failures. Retry only explicit Busy with original IDs, never unknown transport results.

Linux, Rust1.99.1, release opt3, target-cpu=native, LTO=false and default features. Server affinity is CPUs 0, 1 and client affinity is CPUs 2, 3. Use 2 workers each for Tokio and Rayon, a 4 CPU cgroup and a 16 GiB limit. No parallel compilation, tests or benchmarks. Some cgroup throttling occurred (0.302 seconds in aggregate for the lexical baseline and 0.183 seconds for the hybrid candidate) and is recorded in SUMMARY.json. This shared local environment does not establish production capacity.

The following are medians of six per-process latency percentiles, in milliseconds. SUMMARY.json also reports pooled percentiles, scheduled-to-completion, ACK/public-cursor bounds, CPU/RSS, counts and throttling. Requests include Busy retry time (none occurred). Scheduled completion includes accumulated pacing lag. Each version and mode has 2880 queries, 432 write requests and 1728 updated records.

|mode|version|P50|P95|P99|max across runs|
|---|---|---:|---:|---:|---:|
|lexical|base|29.67|67.06|146.38|473.80|
|lexical|candidate|19.17|50.91|166.72|429.49|
|hybrid|base|30.35|59.63|128.44|259.25|
|hybrid|candidate|19.99|41.85|96.64|725.80|
|facet|base|27.26|51.71|111.44|245.05|
|facet|candidate|15.12|29.63|77.64|300.66|

**Adverse samples are retained:** lexical P99 of run percentiles rose from 146.38 to 166.72 ms. Hybrid maximum rose from 259.25 to 725.80 ms and facet maximum from 245.05 to 300.66 ms. Lexical pooled write ACK P99 rose from 153.93 to 195.65 ms and facet ACK P95 from 46.93 to 56.63 ms. Facet pooled scheduled-to-completion P99 rose from 655.72 to 862.39 ms. This change does not resolve all tail latency or read/write interference. Query P50 and CPU improved on these fixtures, with no broader latency guarantee.

Busy and errors were 0 for both versions across all 36 runs. Public cursor catch-up uses sparse observations after ACK and therefore gives an upper bound rather than private native materialization timing. Final drain begins after readers finish and is reported separately (<=2.41ms). It does not measure ACK-to-searchable delay. Final queries verify visibility after drain. Sampled peak RSS ranges from approximately 358 to 395 MiB. These process-level values include server and caches.

## Reproduce
Use a disposable checkout and the existing toolchain. Never point the harness at a user service/data directory. Build baseline and candidate sequentially with identical release flags. Save the two server binaries as `$ANYSEEK_QUERY_BENCH_ROOT/bin/base-server` and `candidate-server`. For allocator probes, copy allocation_probe.rs to each temporary checkout as anyseek/examples/query_allocation_probe.rs before building; it is diagnostic-only, not production code.

```sh
export ANYSEEK_QUERY_BENCH_ROOT=/absolute/new/disposable-directory
mkdir -p "$ANYSEEK_QUERY_BENCH_ROOT/bin"
export CARGO_PROFILE_RELEASE_LTO=false CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
# In each temporary checkout, first copy allocation_probe.rs into anyseek/examples/.
cargo build --locked -p anyseek --release --bin anyseek_server --example query_allocation_probe
# In the baseline checkout (use candidate-server/candidate-alloc for the candidate):
cp target/release/anyseek_server "$ANYSEEK_QUERY_BENCH_ROOT/bin/base-server"
cp target/release/examples/query_allocation_probe "$ANYSEEK_QUERY_BENCH_ROOT/bin/base-alloc"
# Adapt target paths if CARGO_TARGET_DIR is set; save both versions before proceeding.
# Ensure no compilation/tests run concurrently.
python3 benchmarks/query-alignment/http_compare.py 3
python3 benchmarks/query-alignment/allocation_compare.py --before /absolute/base-alloc --after /absolute/candidate-alloc --output /absolute/new/alloc --cpus 0,1 --workloads 1000:true,8000:false
```

Adjust CPU IDs for the host, keeping both versions identical. The HTTP driver here requires CPUs0–3. It isolates data paths and deletes successful data only after parity, keeping failed data and logs. It refuses to reuse run output directories. Seed metadata verifies copied file hashes. A separate warm/cold correctness pilot used 12 queries per worker and 8 write batches before the fixed matrix. The public scripts take the output root from the environment. This path-only option was added after measurement without changing load or semantics.

http-samples.json.gz contains all timed sample latencies,attempt statuses,progress and resources without bulky response bodies; allocation-samples.json.gz contains every diagnostic sample and signatures. Full original response logs remain in the delivered evidence archive. SUMMARY.json records executable SHA256s. All local regression commands are as in docs/development.md and the task evidence.

## Remaining scope
No approximate-ANN recall study,real S3,hard power loss,long-run stability or arbitrary high-concurrency capacity claims. Only one high dimension/population was measured. Global allocator request-byte reductions are not resident-memory reductions. Hybrid/facet component-guard ownership and slow-query/consumer interference are a separate diagnostic/design task; no production restructuring of that path is included.

## Shared runner maintenance
The compatibility allocator entry now uses `../shared_query_compare.py` (three
ABBA groups and resource sidecars remain its defaults). HTTP lifecycle/transport
and explicit Busy retry helpers are shared with write-cost via
`../shared_record_http.py`; workloads, thread-local status and sampling stay local.
This consolidation does not replace the recorded raw samples or change their
provenance. Runner regression checks are listed in `../README.md`.
