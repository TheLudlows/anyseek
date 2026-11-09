# Lexical token reuse: local evidence

Baseline production source: `ed3052f2afdbcf18392a76a4bd3816e4e96275a1`.
Candidate: the lexical reuse implementation accompanying this report. Binary SHA256 values are in every row of [results.csv](results.csv).

The deterministic result is one field analysis per visible row: the baseline once-only regression observed 9 calls before handoff and 18 afterward; the candidate stays at 9. Each measured 24-record workload observed 48 baseline calls and 24 candidate calls. Query compilation does not increment this instance-local counter.

## Measurement and limits

This is an internal `RecordPartition` write-to-native-handoff microbenchmark, including WAL/index work. It is **neither isolated analyzer timing nor end-to-end HTTP throughput**. It does not measure production capacity.

- Linux x86-64/glibc, Rust 1.99.1, cgroup quota 4 CPUs and 16 GiB; process affinity CPUs 0–1; Tokio/Rayon 2 threads. Shared host, local filesystem. See [environment.json](environment.json).
- Optimized **test** binaries: opt-level 3, LTO disabled, 16 codegen units, default features plus fault-injection, test unwind behavior. This differs from production release/LTO/panic-abort. No active Cargo/rustc/AnySeek process was allowed alongside measurements; the harness held `/tmp/anyseek-performance.lock`. Measured cgroup throttling delta was zero.
- Each fresh partition has one warm record, then 24 unique single-record inserts. Identical fixed text (`ALPHA beta gamma delta 臺灣搜索 中文分詞 ` repeated 128 times), two-dimensional auxiliary vector, profile, normalization and durability configuration on both versions. Ngram mask 127 uses explicit frequent words alpha/beta/gamma; a regression verifies actual three-position grams.
- One caller; native handoff is awaited after every four inserts and at the end. `records/s` includes those waits and excludes the subsequent validation query. Single-record requests/s equals records/s. ACK latency is each `write()` duration. CPU ticks cover writes, handoff **and one subsequent validation query**; resolution is 10 ms. RSS is sampled after that query; process HWM and logical reservations are also retained in CSV.
- Every run verifies all 25 records are searchable after handoff and the expected field-call count. Any write error fails the run. No records/errors are filtered out.
- Five initial paired rounds alternate baseline/candidate order. A mixed direction in Chinese/unigram motivated a separate 20-pair confirmation batch with unchanged binaries/workload and alternating order. Both batches are retained separately, not pooled. There were 200 successful scenario runs / 4,800 timed inserts, plus warmups; the initial baseline pilot is excluded.
- Individual timed windows are very short. P95/P99 below are medians of per-run quantiles, not pooled tails; each has only 24 samples and P99 equals that run's maximum. There is no statistical-significance or universal latency-improvement claim.

## Initial five pairs

| Profile / mask | baseline records/s | candidate records/s | change | faster pairs |
|---|---:|---:|---:|---:|
| TantivySimpleV1 / 0 | 180.96 | 198.25 | +9.56% | 5/5 |
| TantivySimpleV1 / 127 | 145.98 | 160.92 | +10.23% | 4/5 |
| JiebaMixedV1 / 0 | 188.49 | 170.48 | -9.55% | 2/5 |
| JiebaMixedV1 / 127 | 168.62 | 189.48 | +12.37% | 4/5 |

## Separate 20-pair confirmation

Medians; B/C means baseline/candidate. CPU seconds have the slightly broader interval explained above.

| Profile / mask | records/s B/C | change | faster pairs | CPU s B/C | ACK P95 ms B/C | ACK P99 ms B/C | RSS MiB B/C |
|---|---:|---:|---:|---:|---:|---:|---:|
| TantivySimpleV1 / 0 | 189.44/195.46 | +3.18% | 12/20 | 0.090/0.080 | 1.395/1.370 | 1.854/1.859 | 43.05/42.31 |
| TantivySimpleV1 / 127 | 175.12/183.78 | +4.95% | 14/20 | 0.105/0.100 | 1.512/1.667 | 1.828/2.035 | 45.37/46.10 |
| JiebaMixedV1 / 0 | 185.07/202.42 | +9.38% | 15/20 | 0.100/0.080 | 1.622/1.560 | 1.890/1.931 | 82.72/81.05 |
| JiebaMixedV1 / 127 | 170.11/186.01 | +9.34% | 14/20 | 0.110/0.095 | 1.727/1.729 | 1.873/2.004 | 93.76/81.58 |

The confirmation throughput medians are higher, but only 12–15 of 20 pairs improve. Initial Chinese/unigram throughput regressed 9.55%; confirmation reverses that direction. ACK tails do not uniformly improve: standard/Ngram P95 is 1.512→1.667 ms and P99 1.828→2.035 ms. The patch moves lexical-only estimate calculation into visible-row construction; this report does not isolate that work's contribution to latency. Native byte-drain thresholds themselves retain their original values.

Logical post-drain reservations are identical between versions in each profile: 35,050,830 / 35,050,894 / 118,936,814 / 118,936,878 bytes (table order). RSS is allocator/process behavior, not logical admission. These post-handoff samples do not bound physical peak memory when old snapshots retain full tokens. No claim of lower memory consumption or long-term steady state is made.

## Correctness and lifecycle validation

Before production edits: `python3 scripts/ci_write_path_cases.py` 54/54; `cargo test --locked -p anyseek --test lexical_correctness` 6/6.

Final production source:

- `cargo test --locked -p anyseek --lib --features fault-injection record::lexical::runtime::live::reuse_tests -- --test-threads=1 --nocapture`: 2 passed, manual benchmark ignored.
- `python3 scripts/ci_write_path_cases.py`: 56/56 exact isolated cases. This includes pre-WAL cancellation/partial scope refunds, native-task cancellation, full-memory progress with old snapshots pinned, materialization failure fencing/replay, shutdown/reopen and remote-checkpoint cancellation cases. The inventory only appends two lexical cases.
- `python3 scripts/ci_core_cases.py`: 203/203 exact cases.
- `cargo test --locked -p anyseek --lib record::lexical::tests:: -- --test-threads=1`: 19/19, including Ngram/field-length/query/scoring contracts, fixed readers, punctuation and reopen.
- `cargo test --locked -p anyseek --test lexical_correctness`: 6/6.
- `cargo check --locked -p anyseek --no-default-features`: passed.
- Source boundary, retired dependency and lexical partition checks passed. `python3 scripts/test_s3_acceptance.py` passed its **runner unit test**, not a live S3 acceptance suite.

New tests compare full Token equality (offset_from, offset_to, position, position_length, normalized text), field lengths, JSON string flattening/missing fields, and exact scores/order before and after handoff/reopen. Native estimate parity is checked against the old lexical-only projection with a large non-lexical/vector input. The original analyzed row stays with pinned snapshots; the owned native clone occurs after existing pre-WAL workspace checks and uses that reservation until the native queue drains. An analyzed input with no workspace is rejected rather than requesting a new reservation.

No batch entrypoint, consumer watermark contract, pre-WAL budget algorithm, storage format, synchronous/async API semantics or task ownership was changed. Not run here: Windows execution, real S3 endpoints, full HTTP acceptance, production release panic probes, strict Clippy/full acceptance, hard power loss or long-duration capacity tests. Existing compiler warnings were not treated as a clean lint result.

## Reproduce

Use a fresh scratch directory on a compatible Linux host; keep historical results intact. Obtain the candidate checkout containing this report and save its SHA. Copy this directory's `measure.py` and `baseline-instrumentation.patch` into scratch. The patch adds only test instrumentation/harness to the pinned baseline; its blank context whitespace is normalized without changing the applied source (both hashes are recorded in environment.json).

```sh
# Run from the candidate repository; choose a new scratch directory.
SCRATCH=/tmp/anyseek-token-reuse-reproduction
mkdir -p "$SCRATCH"
CANDIDATE=$(git rev-parse HEAD)
cp benchmarks/lexical-token-reuse/{measure.py,baseline-instrumentation.patch} "$SCRATCH/"
git worktree add --detach "$SCRATCH/baseline" ed3052f2afdbcf18392a76a4bd3816e4e96275a1
git worktree add --detach "$SCRATCH/candidate" "$CANDIDATE"
git -C "$SCRATCH/baseline" apply "$SCRATCH/baseline-instrumentation.patch"
export CARGO_TARGET_DIR="$SCRATCH/target" CARGO_BUILD_JOBS=2
export CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
for label in baseline candidate; do
  (cd "$SCRATCH/$label" && cargo test --locked -p anyseek --lib     --features fault-injection --release --no-run --message-format=json) > "$SCRATCH/$label-build.jsonl"
  python3 - "$SCRATCH/$label-build.jsonl" "$SCRATCH/$label-bench" <<'COPY'
import json, shutil, sys
items = [json.loads(line) for line in open(sys.argv[1])]
paths = [x['executable'] for x in items if x.get('reason') == 'compiler-artifact'
         and x.get('executable') and x.get('target', {}).get('name') == 'anyseek'
         and x.get('profile', {}).get('test')]
assert len(paths) == 1
shutil.copy2(paths[0], sys.argv[2])
COPY
done
python3 "$SCRATCH/measure.py" pilot
python3 "$SCRATCH/measure.py" paired
python3 "$SCRATCH/measure.py" confirm
```

CPU affinity 0–1 and cgroup v2 `/proc` measurements are explicit harness requirements; adapting them changes the environment. Each JSON retains raw ACK samples, status, binary digest and cgroup counters. `results.csv` contains every initial/confirmation sample and raw ACK array; no failed attempt is silently replaced.
