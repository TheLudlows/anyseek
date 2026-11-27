# Vector Segment ownership consolidation before and after

**English** · [简体中文](README.md)

2026-10-30. Baseline is PR #108 commit `4c795bfdbf8a8f8840250f91f8b3e2a44a5632b4`. Candidate is this PR's production code. Retain raw measurements without a performance-improvement gate.

## Query behavior and cost boundaries

The change removes duplicate `shard_number`, constructor arguments fixed at one, unused positional IDs, and writes to shared `meta.id` during tail rotation. VectorIndex owns logical query settings. Each Segment owns its files, ANN levels, quantization, build leases, and derived visibility. See [current architecture](../../docs/architecture.en.md#segment-ownership-and-file-protection) for ownership and addressing.

A query still captures S active Segments and starts at most S vector subtasks, bounded by the existing component limit. Each Segment keeps at most K candidates, global merge handles at most S×K candidates, and then selects K. Sorting is bounded by O(S×K×log(S×K)). Hybrid applies one RRF over global lane ranks and separate keyword/vector candidate caps. Visibility filtering, stable record-ID tie breaking, ANN probes, and quantization stay unchanged. There is no new scheduling layer or candidate budget.

The manual case writes 120 three-dimensional vectors in four batches and checkpoints after the first three, so frozen components and the tail both participate. Each mode warms up 20 times, takes 100 samples, and returns Top-10. Vector uses exact `AnnMode::All` and Hybrid uses default RRF. Stable IDs, hit order, score bits, and lane explanations matched across 20 independent processes. This small set does not establish large-scale ANN recall. 13 integration cases in `server_shared_filters`, `external_vectors`, `stable_ranking`, `weighted_rrf`, and `server_ann_contract` cover ANN, quantization, and filters, and all passed.

## Measurements

Build used Rust 1.99.1, Release opt-level=3, LTO=false, default zh/vb, and target-cpu=native. The Linux container had an 8 CPU quota and 8 GiB memory limit. No concurrent service requests. Each group alternated five before/after processes and reversed order in even rounds. In the second group, both revisions ran on CPU 1 and 2. Reported P50 is the median of the five process P50s in µs. P95 likewise is the median of per-process nearest-rank P95s, independent of production P99.

| CPU conditions | Mode | Before P50 | After P50 | Before P50 range | After P50 range | Before P95 | After P95 |
| --- | --- | ---: | ---: | --- | --- | ---: | ---: |
| Unpinned | Lexical | 666 | 766.5 | 569–854 | 615–942 | 859 | 889 |
| Unpinned | Vector | 58 | 67 | 42–129.5 | 62–85 | 77 | 105 |
| Unpinned | Hybrid | 722.5 | 705 | 644–884.5 | 627–803 | 986 | 1011 |
| CPU 1、2 | Lexical | 595.5 | 608.5 | 579.5–619 | 568–646.5 | 837 | 770 |
| CPU 1、2 | Vector | 49 | 51 | 35–82 | 43–52 | 117 | 77 |
| CPU 1、2 | Hybrid | 680 | 689 | 637–694.5 | 657–775 | 898 | 1015 |

Unpinned lexical/vector medians rose about 15%, and Hybrid fell about 2%. With CPU pinning, the medians rose roughly 2.2%, 4.1%, and 1.3% respectively. Process ranges and tail latency vary enough that this structural consolidation does not establish a performance gain or rule out production regressions.

Linux process VmHWM (kB) was 54388–59904 before and 53936–59796 after without pinning, and 54500–60856 before and 62096–65432 after with pinning. Record the higher peak for the pinned candidate. No allocation breakdown or steady-state RSS was measured, so it cannot be attributed to index growth or used to rule out memory regressions. Median process CPU seconds were 1.421 before and 1.570 after unpinned, and 1.393 before and 1.436 after pinned. These include setup and all queries and are not per-query CPU cost.

Correctness outputs match. Removed management state and shared metadata writes were demonstrably redundant. No reliable performance gain was measured. Candidate, scheduling, and ANN algorithms are unchanged. Any claim of latency or memory improvement needs longer sampling and independent, pinned-hardware measurement at realistic scale and concurrency. See raw [unpinned](unpinned-results.json) and [pinned](pinned-results.json) samples. Initial Debug trials were affected by concurrent builds and scheduling and are diagnostic only.

## Reproduction

Copy `anyseek/tests/segment_query_profile.rs` to the baseline checkout as well. Use the same Cargo.lock, compiler, features, and flags in both checkouts and run the following in each.

```sh
CARGO_TARGET_DIR=/tmp/segment-target CARGO_BUILD_JOBS=2 \
CARGO_PROFILE_RELEASE_LTO=false CARGO_INCREMENTAL=0 \
cargo test --offline --release -p anyseek --test segment_query_profile --no-run
```

Copy the test binaries shown in the Cargo output to `/tmp/segment-before` and `/tmp/segment-after`. If reusing a target directory, run `cargo clean --offline --target-dir /tmp/segment-target --profile release -p anyseek` before switching checkouts to avoid reusing cache for an identical crate path. Stop builds and other loads before running.

```sh
python3 benchmarks/vector-segments/compare.py \
  --before /tmp/segment-before --after /tmp/segment-after --output /tmp/segment-unpinned
python3 benchmarks/vector-segments/compare.py \
  --before /tmp/segment-before --after /tmp/segment-after --output /tmp/segment-pinned --cpus 1,2
```

The manual test is ignored by default, so normal CI does not time it. It adds no dependencies or flaky pass threshold. Windows does not support its CPU affinity or resource collection. Cross-platform CI verifies correctness.

## #227 filtered I/O comparison

Use the build procedure above and copy the same probe into both checkouts. The matrix uses independent temporary databases, 2,048 synthetic records, and four batches. It covers vector and hybrid queries, no filter and about 5% or 90% matching, and one or four concurrent requests. Each condition records a first round after reopen, then warms up and repeats the query. Reopen itself can warm OS pages, so this first round measures cold application readers. JSON records individual successful query and admission durations alongside the number of Busy responses.

```sh
ANYSEEK_FOREGROUND_PER_COLLECTION_IO=8 python3 benchmarks/vector-segments/compare.py \
  --before /tmp/segment-before --after /tmp/segment-after --cpus 1,2 \
  --matrix --pairs 5 --trace --output /tmp/segment-filter-io
```

The example uses the same per-collection I/O limit of 8 to measure successful queries at concurrency four. Remove the environment override to observe default admission. The optional `--trace` pass runs separate processes under `strace` to count calls, requested and returned bytes, and file mappings for the two component files. Noncontiguous ranges are counted in call order within each file. Concurrent rounds include repeat ranges across requests. Trace collection and latency sampling run separately.
