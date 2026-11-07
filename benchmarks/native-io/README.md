# Native directory I/O isolation

PR #203 baseline: `014e66a8570685667d60197f543845a399186915` (remote head re-fetched before work). Master was `3815f230bee864feedd53ccbed9e6ecc4a4326a3`. The starting checkout was clean. Candidate source digests and toolchain are in [results/provenance.json](results/provenance.json); the containing commit identifies the final candidate.

## Decision and scope

The baseline holds one process-wide mutex across native directory reads, writes, atomic publication, file termination and directory synchronization. A slow sync in one collection can therefore block an unrelated index even when resource admission allows it to proceed. Baseline measurements below confirmed this mechanism before the production change.

The candidate shares a mutex between handles to the **same physical index directory**, rather than between every index in the process. A short global weak-reference registry is accessed only when opening a directory; filesystem operations and capacity waits never run under the registry mutex. Directory/Writer clones and fixed-reader owner tokens retain the gate through their existing `Arc<Budget>`. Dead entries are pruned on subsequent opens. Memory reservations, cancellation, WAL order, OS metadata/writer locks, owner files and package pins retain their existing behavior.

Unix keys use `(device, inode)` after resolving the path, covering relative/dot paths, symlinks and alternate names for the same directory. Other platforms use the canonical path. Lifecycle ownership still forbids externally replacing a live index directory; this is not a new external-filesystem-mutation guarantee. Windows alias behavior and Windows native tests remain unverified locally. Unix rename identity is tested directly; a live MmapDirectory is not promised to support external renaming.

## Experiment

This is a **diagnostic filesystem workload, not an end-to-end collection benchmark**. One or four independent native directories each perform 200 operations: charge 4 KiB, acquire the real Budget I/O gate, call Tantivy's atomic write (temporary file, write, `fdatasync`, rename), then sync the directory. Every payload is read back and checked after timing. No artificial sleep is inserted. Capacity admission is deliberately absent to isolate the gate. Production normally has separate service-wide/subject I/O limits; these are unchanged.

The diagnostic is `cfg(test)` and ignored by default. The baseline executable has the diagnostic module added but no production changes. Both builds use the same unoptimized test profile, default features, Rust 1.99.1 and repository CPU flags. Linux container: 4 CPU quota, 16 GiB memory limit, workspace overlayfs filesystem. Temporary data is explicitly placed under the evidence directory on `/workspace`, not a memory filesystem. Binaries were built before the experiment. Five pairs alternate baseline/candidate then candidate/baseline, for both one and four directories. No concurrent build or test workload was launched during measurement. Host-level interference cannot be excluded and no production stability acceptance is claimed.

The table reports the **median of five process-level statistics**, in milliseconds. These are not pooled percentiles; component medians need not sum. Hold and I/O totals overlap across worker threads in the candidate.

| 4 directories, 800 operations | Global baseline | Per-directory candidate |
| --- | ---: | ---: |
| Wall time | 1305.828 | 595.509 |
| Wall time range | 1137.280–1819.164 | 566.863–660.417 |
| Total gate wait | 2998.076 | 1.476 |
| Gate wait P99 | 98.677147 | 0.022718 |
| Total gate hold | 1280.306 | 2247.914 |
| Atomic write including file sync, total | 877.823 | 1501.940 |
| Directory sync including open/close, total | 402.483 | 758.092 |

Single-directory wall medians: 284.957 ms baseline, 286.235 ms candidate (ranges 259.858–378.104 and 262.560–366.700 ms). No single-directory speedup is established.

Pure sync syscall elapsed time was measured separately using `strace -f -w -c -e trace=fsync,fdatasync`. Both executions made 1,600 successful `fdatasync` calls, zero errors. Summed syscall wall time was 0.980276 s baseline and 1.637560 s candidate. `-w` matters: ordinary strace summary CPU time is not sync wall time. Tracing perturbs scheduling, so these runs are **excluded** from the untraced comparison. The directory-sync timer includes open/close overhead, while atomic-write timing includes publication work; neither is mislabeled pure file-sync cost.

The result supports removing unrelated-directory mutex waits. It also shows increased per-operation storage contention under concurrency. It does **not** establish HTTP throughput, Busy rate, index catch-up or service P99 improvement, nor resolve the historical four-collection C8 regression. No prior unstable incremental-ledger result is used as evidence for this candidate. Release builds, realistic concurrent collection workloads, Windows and representative persistent devices still need separate performance acceptance.

## Reproduction

Use independent source/build directories and preserve existing work. At baseline SHA, copy only `directory_diagnostic.rs` from this candidate into `anyseek/src/record/lexical/`, and append the following to baseline `directory.rs`:

```rust
#[cfg(test)]
#[path = "directory_diagnostic.rs"]
mod diagnostic;
```

Build each checkout, then copy the executable path printed by Cargo into a separate baseline/candidate path before building anything else:

```sh
CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test --locked -p anyseek --lib --no-run -j 4
python3 benchmarks/native-io/compare.py /absolute/baseline-test /absolute/candidate-test /workspace/new-results
```

The output directory must not already exist. Stop other builds/services/performance experiments first. The runner saves all 20 untraced runs, two independently traced runs and binary hashes. [results](results/) contains the recorded logs (trailing blank lines removed), sync summaries, binary hashes and aggregate JSON from this run. The portable runner implements the same alternating command sequence used locally; absolute executor paths and build/test logs are recorded under `/workspace/io-evidence`.

## Local validation

Validation commands, exit codes and durations are recorded in `results/validation.json`. Raw logs are retained in `/workspace/io-evidence/validation-*.log`. The new cross-directory progress/same-directory sharing case is included in `scripts/ci_core_cases.json` for both CI operating systems. The Unix alias/identity test is also run by the local directory test selection.

Final local results (Linux, small fixtures):

- Directory tests: **9 passed**; core aggregate: **203 passed**; fault-enabled write-path aggregate: **46 passed**. These selections overlap and are not a count of distinct tests.
- Six deletion/lexical integration targets: **15 passed**, zero failed or ignored.
- Default and no-default-features `cargo check --all-targets`: passed.
- Console boundary, retired dependencies, lexical partition and S3 acceptance-driver unit checks: passed. The last is a driver test, **not** a real S3 acceptance run.
- Default service build and full normal `record_service_smoke.py`: passed, including lifecycle, backup/restore, restart, ingest, temporal and retry checks.
- Unmodified baseline and final candidate both fail default `cargo clippy --all-targets` with the **same two pre-existing** `clippy::async_yields_async` errors at `anyseek/src/resources/lifecycle.rs:146,156`. That source is byte-identical. Error excerpts are retained here; full logs are in `/workspace/io-evidence/{baseline-clippy,validation-clippy}.log`.
- `cargo clippy --locked -p anyseek --all-targets -- -A clippy::async_yields_async`: passed, with existing warnings. This narrow lint exception is explicit; the default lint result remains failed. Baseline and candidate library test builds each emitted 246 compiler warnings.
- Benchmark runner syntax and `git diff --check`: passed.

Not run: full manual Linux/Windows acceptance matrix, Windows native alias tests, browser checks, real S3, hard power-loss recovery, or end-to-end release performance acceptance. This local change is not a claim that those merge/deployment gates are satisfied. No remote push, PR update, merge or deployment is part of this work.
