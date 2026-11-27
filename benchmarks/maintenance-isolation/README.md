# Online maintenance isolation (#237)

**English** · [简体中文](README.zh-CN.md)

Run the same local HTTP read and append plan against two synthetic collections on the
fixed Release engine `9861140f5ee66459ffce31908a5aa9f02eada5b9`. Each case
starts a fresh loopback process and uses a new data root. The fixture starts
with 1,024 live hot records, 64 neighbor records and a backup-pinned, deleted
512-record hot incarnation. Two read lanes per collection alternate point reads
and lexical queries. A separate lane per collection appends records. Requests
are offered at fixed offsets, with no timed retries. The report records launch
lateness when a preceding request delays its lane.

## Run

Use Linux with pidfd support, Python 3.11+, `taskset`, and `strace` with
`--kill-on-exit` and permission to trace its own child. Choose two available CPUs and a new
output path with at least 1 GiB free. The traced run keeps its disposable data
roots and raw syscall logs. Build the default-feature Release binary from the
pinned engine checkout and run the following command from the harness checkout.

```bash
cargo build --locked --release -p anyseek --bin anyseek_server
python3 benchmarks/maintenance-isolation/measure.py \
  --server /path/to/pinned-engine/target/release/anyseek_server \
  --engine-sha 9861140f5ee66459ffce31908a5aa9f02eada5b9 \
  --cpus 0,1 --out /tmp/anyseek-237-run1 --with-crash
```

Use `--case` only for exploratory runs. The full comparison runs all four
cases with the same planned slots and authoritative starting records. Setup
checkpoints the hot derived view in the control, index and reclamation cases.
The compaction case retains mutable components so the common appends can
trigger the production merge. Compare runs using the recorded harness and
binary hashes, engine SHA, plan hash, CPU affinity, filesystem and tracer version.

Run the focused shutdown regression against the same Release binary.

```bash
python3 benchmarks/maintenance-isolation/check_shutdown.py \
  --server /path/to/pinned-engine/target/release/anyseek_server --cpus 0,1
```

## Required evidence

| Case | Evidence required for coverage |
| --- | --- |
| Control | No observed target build, merge build or old-root deletion during the measurement. The backup protects the retired incarnation. |
| Index build | The real hot `PATCH /api/v1/index/hot` returns an indexed sequence with a matching `anchor_prepare` phase inside the request. |
| Cache compaction | Native merge capture, build and install are followed by an installed worker outcome. Successful writes inside the build target a new native cache within the current hot incarnation. |
| Reclamation | Deleting the old backup releases its pin. A rename and successful file or directory deletions target exactly the old incarnation, which eventually disappears. |
| Optional process failure | After an acknowledged neighbor write, kill the service while the old hot incarnation is still backup-pinned. Reopening preserves both current collections and the pin. The old delete and neighbor write replay their original receipts. Unpinning then physically reclaims the old hot incarnation. |

Each active maintenance case requires a measured read from **both** collections
to overlap a confirmed window. Stage start and end timestamps come from traced
server log writes. Reclamation uses the duration of each successful
`unlink`, `unlinkat` or `rmdir`, without counting rename time or gaps
between deletion calls. Requests count toward overlap only when the
conservative HTTP in-flight interval intersects a window. Clock sampling
rejects coverage when the sampled wall/monotonic offset changes by more than
2 ms. Point reads, lexical hits and successful append receipts carry identity,
version and document or body assertions through the live service.

## Read the result

- Start with the top-level `report.json` and each case's `report.json`.
  Compare `all_requests`, `overlapped_requests` and the control subset
  with the same planned slots. By collection and operation, these contain
  attempts, successful fraction, nearest-rank successful p99, HTTP 429,
  per-item Busy and HTTP 503 codes. An HTTP 200 append is successful only
  when every item returns an applied receipt. Successful p99 excludes failed
  attempts, and 429 and 503 are recorded separately.
- The case report records confirmed window duration, index request-to-phase,
  merge worker-to-capture and unpin-to-first-removal waits when observed.
  A missing server schedule event leaves schedule-to-capture blank. Raw
  evidence lives in `requests.jsonl.gz`, `events.json`, `server.log` and
  `syscalls.*`. Reasons come only from explicit server evidence. A generic
  Busy response alone leaves budget, file I/O and publication revalidation
  undetermined. The [#237 diagnosis and proposed fixes](https://github.com/TheLudlows/anyseek/issues/237#issuecomment-7223083173)
  cover the observed file I/O and publication revalidation cases.
- Read `resources.json` for sampled process RSS/high-water RSS and allocated
  blocks under the live root and `TMPDIR`. These are periodic samples
  with per-path disk accounting. They can miss short disk peaks.
- Exit zero means the traced scenario coverage, fixture checks and shutdown
  succeeded. A successful run can include HTTP 429 and 503. Check their
  counters and the sample sizes when interpreting latency and availability. This is a small, traced regression fixture. Payload-journal
  repacking, untraced throughput, sustained saturation, production SLA and
  the dedicated-NVMe matrix remain outside its evidence.

The fixture reuses the shared HTTP client and complements the existing
[reclamation smoke](../../scripts/record_reclamation_smoke.py) with concurrent
hot and neighbor traffic and an optional crash before unpinning.
