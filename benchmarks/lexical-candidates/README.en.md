# Bounded lexical candidate collection: #153 acceptance comparison

**English** · [简体中文](README.md)

On 2026-11-03 we compared real release HTTP servers before and after [#178](https://github.com/TheLudlows/anyseek/pull/178) merged. The implementation is already merged. This directory adds manual measurement tools and acceptance evidence and leaves production code, #152 write planning and CI unchanged.

Together with the existing semantic coverage in #178, this report provides the fixed-corpus allocation, CPU, and tail-latency comparisons for [#153](https://github.com/TheLudlows/anyseek/issues/153). See below for the granularity of allocation evidence and closure recommendation. Implementation merge alone does not establish all acceptance. Across 16 scale/scenario/concurrency combinations, the four-round median CPU/request fell 36.2%–61.7%, p95 fell 38.7%–63.2%, and p99 fell 31.3%–62.5%. No improvement gate was set. Individual rounds still varied. Round four of 10,000-record ties/C1 had a 6.4% p99 increase, so the result establishes neither improvement in every round nor a production SLA.

## Compared versions and method

| Role | Commit |
| --- | --- |
| before: first parent of #178 | `3c9db3e66665c40938fb2b387531fdb07694a05c` |
| after: #178 merge commit | `a047b55e0a108adf027ba0ba31b0b02686f42d7c` |

The revisions differ by only six #178 files. This report does not measure later master changes. See [build-provenance.json](results/2026-11-03/build-provenance.json) for revisions, binary and Cargo.lock SHA-256, compiler, and CPUs. Rust 1.99.1 / LLVM 23.1.1, default features, release opt-level=3, LTO=true, target-cpu=native. Linux x86_64 / AMD EPYC 9V74, 8 CPU quota and 8 GiB memory cap. The server is pinned to two logical CPUs, Tokio/Rayon use 2 threads each, and the client uses other CPUs.

The corpus is in reverse order of business IDs, with the text fixed to 9 tokens: common repeated 1–8 times, padding completes the first 8 tokens, tied exactly once; priority is the inserted sequence number, the vector is uniformly `[1,0]`, and the tokenizer is TantivySimpleV1. 10,000 items use standard_10k, 20,000 items use bounded_100k; the configuration is the same before and after the same scale, and the comparison across scales also changes the capacity level. Through the existing /ingest, up to 64 items are prepared per batch, and the explicit Busy retry retains the operation ID; the checkpoint confirms that indexed_seq aligns visible_seq. Each process copies the same baseline snapshot, and no writing occurs during the timing period.

| Scenario | Query Window and Sorting |
| --- | --- |
| score | common, Top-32 by relevance, count=true |
| ties | tied, Top-32 business IDs with the same score, count=true |
| field_page | common, priority descending, offset=16, length=16 |
| hybrid | common + `[1,0]`, precise vector retrieval, keyword/vector candidates 32/48, RRF constant=60, return 16 |

Four rounds run in AB/BA/AB/BA order, with 24 warmups then 200 samples per phase. Four scenarios, two sizes, and C1/C4 closed-loop concurrency yield **25,600 measured requests**. CPU is service process `/proc` user+system seconds divided by requests, with 10ms resolution. HTTP latency ends after reading the response and excludes client JSON parsing and verification. Client checks still affect closed-loop spacing, so this is not a fixed-arrival-rate load test. Table values are medians across rounds, not pooled sample percentiles.

## CPU and Tail Latency

| Records | Scenario | Concurrency | CPU ms/request (before → after) | p95 ms (before → after) | p99 ms (before → after) |
| ---: | --- | ---: | ---: | ---: | ---: |
| 10,000 | score | 1 | 40.725 → 21.175 | 53.097 → 26.648 | 69.796 → 34.921 |
| 10,000 | score | 4 | 39.275 → 21.275 | 150.173 → 78.363 | 187.721 → 97.605 |
| 10,000 | ties | 1 | 34.075 → 20.850 | 40.983 → 24.630 | 52.082 → 35.791 |
| 10,000 | ties | 4 | 33.600 → 21.425 | 127.641 → 78.210 | 140.965 → 95.797 |
| 10,000 | field_page | 1 | 38.900 → 19.650 | 43.729 → 22.145 | 62.314 → 26.400 |
| 10,000 | field_page | 4 | 38.925 → 20.375 | 147.656 → 74.236 | 170.181 → 81.221 |
| 10,000 | hybrid | 1 | 40.200 → 21.850 | 46.789 → 25.561 | 54.929 → 29.936 |
| 10,000 | hybrid | 4 | 39.025 → 22.475 | 112.681 → 65.108 | 121.905 → 76.861 |
| 20,000 | score | 1 | 113.450 → 47.150 | 136.545 → 59.752 | 153.439 → 75.128 |
| 20,000 | score | 4 | 116.425 → 47.900 | 417.704 → 191.861 | 474.005 → 219.370 |
| 20,000 | ties | 1 | 76.500 → 46.850 | 92.305 → 55.843 | 108.405 → 69.243 |
| 20,000 | ties | 4 | 77.250 → 46.400 | 288.709 → 171.340 | 314.016 → 213.622 |
| 20,000 | field_page | 1 | 114.200 → 43.775 | 135.822 → 49.947 | 154.077 → 57.722 |
| 20,000 | field_page | 4 | 111.900 → 44.075 | 416.932 → 158.624 | 447.985 → 182.876 |
| 20,000 | hybrid | 1 | 111.625 → 45.575 | 130.636 → 51.740 | 142.903 → 60.471 |
| 20,000 | hybrid | 4 | 111.225 → 48.450 | 317.616 → 137.202 | 342.244 → 148.194 |

The changes in each round of pairing are shown in [comparison.csv](results/2026-11-03/comparison.csv). The original results for each stage and the latency per request are available in [measured](results/2026-11-03/measured); the JSONL per request and the expected results are compressed using gzip for lossless compression.

## Allocation requests and correctness

Independent instrumentation was performed for two rounds of AB/BA, C1, and 30 times per stage, totaling **960 formal requests**. `alloc.c` uses LD_PRELOAD to count the successful calls to glibc malloc/calloc/realloc and the requested sizes, including activities in the service background; other allocation entries were not covered. It measures the cumulative requested bytes, not live/peak memory or RSS; the instrumentation delay was not used in the above table.

| Score Scenario | Cumulative Requested Bytes/Request (Before → After, Median of Two Rounds) | Change |
| --- | ---: | ---: |
| 10,000 records | 10,019,148.8 → 6,899,260.5 | −31.1% |
| 20,000 records | 19,816,818.4 → 13,573,672.9 | −31.5% |

See [allocation-comparison.csv](results/2026-11-03/allocation-comparison.csv) and [allocation](results/2026-11-03/allocation) for all scenarios. Neither phase had request errors or before/after output differences. Per-request comparison checks full hits, scores, lane explanations, count/count_total, visible_seq, query_terms, and fusion. An independent corpus oracle checks score/ties/field-pagination IDs and calculates Hybrid membership, scores, and lane ranks using f32 RRF. It does not independently reimplement BM25. Broader semantic coverage comes from #178.

Historical allocation summaries record calls/request and requested bytes/request for each stage **without before-and-after counter snapshots**. Two-run medians and change rates can be recomputed, but the original cumulative counters cannot be reconstructed independently. CPU is also recomputed from the stored `cpu_s`, without `/proc` start and end snapshots. The per-request archive stores latency and omits the complete responses. Zero differences come from passing per-request assertions during the measurement, not replaying every response afterward. Expected responses and the corpus oracle can be reviewed offline. Preserve these evidence limits without inventing historical raw data.

## #153 acceptance evidence

| Acceptance Items | Evidence and Boundaries |
| --- | --- |
| Collection Space Growth with Window Size | The capacity test of #178, as well as its collector-only measurements: K=32, 10k/20k hits both have a peak of 2,048 bytes; this peak was not re-measured, so it cannot be inferred that the entire query has an O(K) memory complexity. |
| Independent oracle for membership, sort order, offset and explanations | Compare the existing exact assertions in #178 against this corpus oracle and complete per-request business results. |
| Field sorting, Hybrid budgets, multiple segments, updates, deletes and counts | This run covers field pagination, independent 32/48 budgets, channel explanations and counts. Cross-segment behavior, updates and deletes use existing cases from #178 and its [regular CI](https://github.com/TheLudlows/anyseek/actions/runs/41266182689), without counting them as reruns. |
| Allocation, CPU and tail latency on a fixed corpus | This directory retains 25,600 timed requests, 960 separate allocation requests and reproduction scripts. No required improvement threshold is set. |

The six-file #178 diff contains the merged implementation and historical semantic tests. `bounded_candidates_match_full_sort` compares all candidates against full sorting and checks capacity after every insertion. `union_topk_fill_ranking` checks exact prefixes of the complete ranking and business ID tie-breaks. Cross-component tests in `segment.rs` cover updates, deletes, sorting and pagination. `weighted_rrf` covers independent channel candidate budgets after field sorting, global rankings and explanations. The full-sort oracle is independent of heap collection but shares scoring code, so it is not an independent BM25 implementation. Historical results are excluded from this run's test count.

**Closure recommendation: the #153 implementation, semantic coverage and fixed-load performance comparison have evidence available for review. The issue can close once the precision of the above phase allocation summaries is accepted.** If rebuilding results from the original allocation counters is required, that item remains incomplete. Rerun allocation measurements with the corrected tool. The current archive is not a substitute. The PR retains `Refs #153` and does not auto-close the issue. Real S3, cold cache, concurrent updates and overall current main branch performance are outside this historical comparison.

Only verifying the deterministic synthetic read-only load after preheating; does not cover real S3, Chinese relevance, cold cache, Windows performance, update concurrency tail latency, or #152/#155 acceptance criteria. The original measurement did not rerun the Rust semantics test; this conclusion was reached by rebuilding the Linux default feature test binary on the production code with the main branch `768a18e`, reusing 8 existing targeted cases, all of which passed: bounded capacity/fully sorted oracle, cross-component update and deletion statistics, as well as 4 items for `union_topk_fill_ranking` and 2 items for `weighted_rrf`. They verified the current source code semantics, were not new performance measurements, and were not equivalent to a complete CI. Existing build warnings are not included in this scope. Early trials, shared targets that mistakenly reused the same binary, and trials that were not completed with replication have been discarded; after final cleanup, the binaries were recompiled and verified with different hashes.

## Reproduction

Requires Linux x86_64, glibc, Python 3.12+, Rust 1.99.1, C compiler, taskset, at least three available logical CPUs, and a readable cgroup v2 with cpu.max/memory.max. No third-party Python dependencies required. Do not run in parallel with builds or other stress tests; do not use `python -O`, as the correctness check uses assert.

Build two fixed versions from the root directory of the repository, using separate target directories to avoid reusing incorrect products (retain the two versions' built-in release profiles and .cargo/config.toml):

```sh
repo=$(git rev-parse --show-toplevel)
run=$(mktemp -d /tmp/anyseek-153.XXXXXX)
git worktree add --detach "$run/before-src" 3c9db3e66665c40938fb2b387531fdb07694a05c
git worktree add --detach "$run/after-src" a047b55e0a108adf027ba0ba31b0b02686f42d7c
(cd "$run/before-src" && CARGO_TARGET_DIR="$run/before-target" cargo build --release --locked -p anyseek --bin anyseek_server -j 4)
(cd "$run/after-src" && CARGO_TARGET_DIR="$run/after-target" cargo build --release --locked -p anyseek --bin anyseek_server -j 4)
cp "$run/before-target/release/anyseek_server" "$run/before"
cp "$run/after-target/release/anyseek_server" "$run/after"
cd "$repo/benchmarks/lexical-candidates"
cc -O2 -shared -fPIC -o alloc.so alloc.c
python3 -u measure.py --before "$run/before" --after "$run/after" --out "$run/measured" --sizes 10000 20000 --rounds 4 --requests 200
python3 -u measure.py --before "$run/before" --after "$run/after" --out "$run/allocation" --seed-root "$run/measured" --sizes 10000 20000 --rounds 2 --requests 30 --allocation
python3 summarize.py "$run" --out "$run/summary" > "$run/summary.json"
```

Continue after successful verification of each step. The script only starts the local temporary services, automatically stops the services, and deletes the copies for each run, retaining the seed for reuse during the allocation phase; `--out` must use a new directory, not the historical results of this directory. Archived data does not contain database snapshots and cannot be used directly as `--seed-root`; Prepare again according to the fixed rules. The operation UUID/times will change, and the content SHA-256 should be consistent with the seed list. Small-scale trials can reduce the number of rounds/requests, but the summary script corresponds to the complete four rounds of timing and two rounds of allocation matrices in this report.

Only recalculate the archived results, no need to start the service:

```sh
python3 summarize.py results/2026-11-03 --out /tmp/anyseek-153-summary > /tmp/anyseek-153-summary.json
```

This cleanup preserves the original archive and summary figures. Corrections verify the SHA-256 inventory and complete matrix before summarizing, request counts for each percentile, CPU/QPS arithmetic, executable fingerprints, corpus fingerprints and watermarks reconstructed from the rules, expected-response oracles and consistency across both stages. Missing historical allocation snapshots are reported explicitly. Output must be outside the input evidence directory. The measurement entry point rejects existing directories, invalid matrices and `python -O`. New allocation runs also save initial and final counters. `alloc.c` is unchanged. The original measurement script is available at PR #190's initial commit `b531aa6`. The corrected script is distinct from the version used for historical measurements.

`results/2026-11-03/SHA256SUMS.json` covers the archived evidence, including environment, corpus fingerprints, watermarks, expected business results, per-stage summaries and compressed request latencies. It excludes service binaries, databases, redundant HTML, and build and service logs. The list detects corrupt files without authenticating signatures. The manual performance tools are outside CI and introduce no timing thresholds.
