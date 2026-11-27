# Single-node performance and recovery acceptance · #229

**English** · [简体中文](README.zh-CN.md)

This covers [#229](https://github.com/TheLudlows/anyseek/issues/229) under [#224](https://github.com/TheLudlows/anyseek/issues/224). The fixed baseline is `74d48d92db27d4fa155ce38004ac079f3bf92835`, which includes the capacity-limit removal in #220, the lexical visibility work in #221, and the retired upgrade-path cleanup in #223. Remote `master` pointed at this commit when inspected.

## Workload

- A fresh standalone service loads 64 Records per request through HTTP `/ingest`. The hot collection has 100,000, 1,000,000, or 3,000,000 Records, with a second active collection of 4,096 and a cold collection of 64. The loader checks every `committed` receipt and Record ID, then waits separately for lexical and vector watermarks. It generates one deterministic batch at a time and hashes the content by batch.
- Synthetic workload `anyseek-229-v1` cycles through 128, 128, 256 and 512 body bytes, 32 lexical topics and eight integer filter buckets. Vectors have 128 dimensions, Top-K is 16, and filters use half-open integer ranges. Readers rotate point reads, lexical queries, vector queries and filtered hybrid queries.
- After warmup, one closed-loop reader measures the hot collection before the peer and cold collections are created. A writer-only phase precedes the mixed phase with eight readers (six hot and two peer), one hot-collection writer and one maintenance client. The writer creates a two-member group, replaces it with one member, clears it, then makes an asynchronous write and an independent delete. Maintenance issues two actual PATCH index/checkpoint requests. Baseline PATCH has no explicit Compact parameter. The report captures observed `state_image` events, payload compaction flags and overlapping query intervals. Explicit Compact and unobserved physical rewriting remain separate acceptance items.
- Use the repository Release profile, default features, native CPU flags and the same two service CPUs. Build each engine version before measurement. The report saves cgroup limits, memory, media, CPU IDs, source revision and binary SHA-256. Use the same machine, devices, CPUs and workload for comparisons. Each candidate and scale gets a fresh root. Failed runs retain their reports.

## Run

Run with Linux, Python 3.11 or later, and `findmnt` and `lsblk` from util-linux. Set absolute paths for `BASE_CHECKOUT`, `LOCAL_NVME` and `BACKUP_DEVICE` before running from the harness checkout. The first is a separate AnySeek checkout. Use reserved mounts for the other two.

```bash
git -C "$BASE_CHECKOUT" checkout --detach 74d48d92db27d4fa155ce38004ac079f3bf92835
(cd "$BASE_CHECKOUT" && cargo +1.98.0 build --locked --release -p anyseek --bin anyseek_server -j 4)
baseline="$BASE_CHECKOUT/target/release/anyseek_server"
sha256sum "$baseline"

python3 benchmarks/single-node/measure.py --binary "$baseline" \
  --revision 74d48d92db27d4fa155ce38004ac079f3bf92835 --out /tmp/anyseek-229-smoke

for scale in 100000 1000000 3000000; do
  python3 benchmarks/single-node/measure.py --binary "$baseline" \
    --revision 74d48d92db27d4fa155ce38004ac079f3bf92835 \
    --mode dedicated --scale "$scale" --exclusive-nvme --cpus 0,1 \
    --out "$LOCAL_NVME/baseline-$scale" --backup-root "$BACKUP_DEVICE/baseline-$scale"
done
```

Rerun each point with the candidate binary, its full SHA and distinct output directories. Run one measurement at a time. Dedicated mode requires operator attestation, an identifiable backing NVMe data device and a separate backing block device for the backup. Overlay and shared runners use the small fixture. If loading stops, record the first affected batch and its exact item outcomes. Each later point can still be run independently.

Record the environment and free bytes before scheduling either binary on the reserved host.

```bash
uname -srvmo
lscpu
taskset -pc "$$"
cat /proc/meminfo /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max
findmnt -T "$LOCAL_NVME" -o SOURCE,TARGET,FSTYPE,OPTIONS
findmnt -T "$BACKUP_DEVICE" -o SOURCE,TARGET,FSTYPE,OPTIONS
lsblk -b -o NAME,TYPE,TRAN,MODEL,SIZE,ROTA,MOUNTPOINTS
df -B1 --output=source,avail,target "$LOCAL_NVME" "$BACKUP_DEVICE"
```

The canonical document JSON has an upper bound of 1,441 bytes per Record averaged over each four-Record body cycle. The three scales, including the 4,096 and 64 auxiliary Records, have respective document JSON bounds of 150,094,560, 1,446,994,560, and 4,328,994,560 bytes. The bounds exclude the request envelope and on-disk data structures. Use `S` for measured steady physical engine bytes before backup, `A` for measured physical bytes of a complete archive copy, and `T` for additional peak bytes for maintenance, staging and temporary files above the reserved copies. Use physical `du -s -B1` and the report's allocation samples on the reserved filesystem. For planning, reserve at least `1.2 × (2S + 4A + T)` on the data device and `1.2 × A` on the separate backup device per run. Allow for the original and restored roots, the published archive, two restore-control copies and one transient archive-sized allowance. Sum retained runs and include the integrated replay window's measured log bytes before scheduling PITR. The 1 GiB admission estimate uses different inputs from archive disk usage. Record the first observed budget rejection separately.

Successful-request p99 in `report.json` uses nearest rank. HTTP latency includes the round trip and reading the response body. Workload generation and JSON decoding are excluded from HTTP latency and included in end-to-end throughput alongside validation, reporting and catch-up. Report Busy and other failures separately and preserve unknown outcomes. Sample process `VmHWM` and physical allocations in the data root and temporary directory every second. Sampled storage peaks can miss short-lived allocations. Reopen timings are process-cold with the OS cache intact. Device-cold measurements require separate cache evidence on the reserved machine.

For collection-level mixed results, set `trace` to the run's `requests.jsonl.gz` and group the actual requests by collection and operation.

```bash
gzip -dc "$trace" | jq -c 'select(.phase == "mixed" and
  (.operation | IN("point", "lexical", "vector", "hybrid_filtered"))) |
  . + {collection: (.path | split("/")[4])}' |
jq -s 'group_by([.collection, .operation]) | map(
    (map(select(.success).http_ms) | sort) as $latency |
    {collection: .[0].collection, operation: .[0].operation, requests: length,
     busy: map(select(.busy)) | length,
     other_failures: map(select((.success or .busy) | not)) | length,
     p99_ms: (if ($latency | length) == 0 then null
              else $latency[(($latency | length) * 0.99 | ceil) - 1] end)})'
```

## Recovery and faults

### Point reads during publication

Run the same small-store probe for the frozen Release and each nominated #224 candidate. Each round uses four fresh roots with the same corpus and eight reader lanes. The four windows cover readers alone, concurrent writes, HTTP checkpoint, and writes plus HTTP checkpoint. Every initial and follow-up request is retained with its path and interval. An observer retries 503 reads during the mixed window and the main probe checks the same Record version and body after it ends. The progress route takes the partition lock, so a `false` response can follow the clearance of a short output fence. If reads or health still fail after the quiet interval, the probe attempts a same-version reopen and records its outcome.

```bash
python3 benchmarks/single-node/measure.py --binary "$baseline" \
  --revision 74d48d92db27d4fa155ce38004ac079f3bf92835 \
  --point-fence-probe --probe-rounds 3 --cpus 0,1 --out /tmp/anyseek-229-fence-baseline
python3 benchmarks/single-node/measure.py --binary "$CANDIDATE_BINARY" \
  --revision "$CANDIDATE_REVISION" \
  --point-fence-probe --probe-rounds 3 --cpus 0,1 --out /tmp/anyseek-229-fence-candidate
```

Exit 1 records an availability failure even when a retry gets the original data. HTTP 503 and HTTP 429 remain distinct. Exit 2 signals incomplete evidence, including a checkpoint scenario without a successful hot checkpoint overlapping point reads. Exit 0 covers the measured smoke windows. The command saves `report.json`, per-case compressed request traces, and service logs regardless of the verdict.

The frozen baseline failed the three-round overlay probe. The earlier five failures during a hot checkpoint remain in the original mixed trace. The controlled rerun reproduces them with concurrent writes both with and without checkpoint.

| Frozen baseline window | Initial point 503 / requests | All mixed HTTP 429 | Verified original version and body on the same process |
| --- | ---: | ---: | ---: |
| Readers only | 0 / 96 | 0 | 0 retries needed |
| Writes without checkpoint | 9 / 96 | 18 | 9 / 9 |
| Checkpoint without writes | 0 / 96 | 0 | 0 retries needed |
| Checkpoint and writes | 14 / 96 | 20 | 14 / 14 |

All 23 initial `recovery_required` responses overlapped a normal `async_write` or `record_delete` request. Six first retries also returned 503. Near-failure and settled progress samples reported `recovery_required: false`, and all failed reads recovered on the original process. The diagnostic Release emitted 14 `stage=capture ... fenced=true` events for eight initial failures and six first-retry failures. It emitted no rejection at the Record health check or output handoff. The diagnostic build adds logging only. [Probe reports](results/point-fence-report.json) · [Per-request traces](results/point-fence-requests.jsonl.gz) · [Fence and native regression output](results/point-fence-diagnostic.txt)

Integration Release `d8e19e18ac4166a5ffa71f41fe79888566496a89` also failed this probe. Initial point 503 counts were 7 of 96 with writes alone and 3 of 96 with writes and checkpoint. Both windows logged 23 mixed 429 responses each. The read-only and checkpoint-only controls had zero point 503s with four and one mixed 429 responses respectively. All ten failed reads recovered the original version and body on the same process. Five first retries still returned 503, the progress samples stayed healthy, and no reopen was required. [Integration PR #235](https://github.com/TheLudlows/anyseek/pull/235)

The point route captures a synchronous `output_permit()` before looking up the Record. A set `OutputState.fenced` returns `RecordError::RecoveryRequired`, which becomes HTTP 503. An ordinary batch sets the flag while publishing and clears it on success. The progress call acquires `Inner` and can therefore wait out the transient fence. Preserve rejection for an actual failed publication. For #224, the point route's capture and handoff can use the existing wait-for-publication path used by query. Apply the probe again after the transport change, and preserve the paused-writer and failed-publication regressions. [Point handler](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/server/record_service.rs#L970-L995) · [Capture check](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/record.rs#L998-L1005) · [Writer publication](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/record/write.rs#L250-L281) · [Query handoff](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/server/record_service.rs#L1687-L1705)

To reproduce diagnostic output, apply the [logging patch](results/point-fence-diagnostic.patch.gz) only in a disposable `74d48d9` checkout, then build the same Release profile and run the same probe. Keep the binary separate from the frozen Release.

```bash
gzip -dc benchmarks/single-node/results/point-fence-diagnostic.patch.gz | git -C "$DIAGNOSTIC_CHECKOUT" apply
(cd "$DIAGNOSTIC_CHECKOUT" && cargo +1.98.0 build --locked --release -p anyseek --bin anyseek_server -j 4)
```

### Recovery matrix

| Case | Execution and checks |
| --- | --- |
| Same-version reopen | Confirm and verify C, then SIGKILL the owned process. Reopen the same root with the same binary. Check point versions, the group tombstone, original B/C receipts and that replaying B does not resurrect C's deletion. Read the untouched collection after reopening. |
| Independent backup | Take a native full-service backup at A and verify a copied archive file by file with SHA-256. Give only that archive to a fresh control root, call native `service-restores`, then boot the returned root. Check A's records and receipt and absence of B/C. Flip a byte in a second copy and require rejection while preserving the original archive. |
| PITR | `pitr-fixture.json` fixes a base backup at A, an atomic two-Record turn at B, and a retraction plus queryable operation witness at C. Recovery must replay continuous certified archive data from A to B. Check both B members, group version 1, the original B receipt, absence of the C witness and its receipt, and Origence's paired `directory_sha256` at B. Validate absence of the group retraction receipt through the integration's receipt authority. The baseline HTTP operation lookup only exposes ordinary Record receipts. Run retention-edge and gap cases against the configured archive window. Current HTTP restore takes a backup ID. Archive window, cut selection, and verified replay await #224 integration. |
| Targeted process faults | Use the existing actual HTTP fault suite, the Release panic probe and the group WAL, checkpoint selection, reclamation and damaged-file regressions below. Each case uses fresh disposable data. |

The full-service snapshot checks a 1 GiB preflight before staging. Its estimate is serialized Catalog bytes plus each retained collection or backup's `checkpoint_bytes` and 4096 bytes per item. Record this estimate, the limit, HTTP outcome, physical archive bytes and query capacity separately. The stock baseline does not emit the estimated number of bytes, so that field is null for the pinned executable. Builds with diagnostics emit `service snapshot preflight payload_bytes=… limit_bytes=…`. [Baseline source](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/catalog/service_snapshot.rs#L339-L385)

```bash
CARGO_TARGET_DIR=target/fault-release cargo +1.98.0 build --locked --release -p anyseek \
  --features fault-injection --bin anyseek_server --example runtime_panic_probe
python3 scripts/record_service_smoke.py --server target/fault-release/release/anyseek_server --faults
python3 scripts/record_shutdown_smoke.py --server target/fault-release/release/anyseek_server
python3 scripts/record_reclamation_smoke.py --server target/fault-release/release/anyseek_server
python3 scripts/release_panic_smoke.py --probe target/fault-release/release/examples/runtime_panic_probe
for case in \
  record::group_write::tests::group_wal_process_exit_recovers_single_decision \
  record::anchor::tests::incremental_state_closure_survives_rejection_gc_and_reopen \
  record::anchor::tests::rejected_prepared_anchor_reclaims_only_unselected_objects \
  record::anchor::tests::multisegment_delete_and_corrupt_selected_object_fail_closed \
  record::output::tests::transport_output_waits_for_publication_and_cancellation_does_not_cancel_writer \
  record::output::tests::transport_output_rejects_failed_publication_and_old_permit_after_recovery; do
  cargo +1.98.0 test --locked -p anyseek --lib "$case" -- --exact
done
```

## Origence paired recovery

Use an isolated Origence checkout and binary. Synthetic vectors are served locally. Origence and AnySeek run as separate loopback HTTP processes. The probe first verifies private archival and offline `archive-verify`, then checks the restored HTTP list, readback and withdrawal. It attempts ordinary asset publication in a second fresh pair. On success it checks the published Job and source search. On failure it records the actual AnySeek Info response and Origence process log. [Origence paired recovery procedure](https://github.com/TheLudlows/Origence/blob/d34804c52560936e0c654ef40a0440fba3a23ca8/docs/OPERATIONS.en.md#private-turn-archive-and-paired-recovery)

```bash
(cd "$ORIGENCE_CHECKOUT" && cargo +1.98.0 build --locked --bin origence)
python3 benchmarks/single-node/origence_pair.py --origence-repo "$ORIGENCE_CHECKOUT" \
  --origence-binary "$ORIGENCE_CHECKOUT/target/debug/origence" \
  --anyseek-binary "$baseline" --out /tmp/anyseek-229-origence
```

## Observed results

The shared Linux x86-64 overlay runner had a 16 GiB cgroup memory limit, a 4 vCPU quota, and server affinity on CPUs 0 and 1. The [dedicated-mode guard](results/media-guard.txt) rejected its storage. The [frozen Release run](results/overlay-report.json) confirmed 512 + 512 + 64 Records through actual HTTP. The Release binary SHA-256 is `b09aa6a5ef012a3be8221c8ed9d6546c3843e0995c5b5eeb7af205d6c0bdebfc`. The 100,000, 1,000,000 and 3,000,000 points, device-cold cache behavior, hard power loss and archive-window replay remain pending.

| Check | Shared-runner observation |
| --- | --- |
| Read and maintenance | 128 queries overlapped the hot checkpoint and none overlapped the subsequent peer checkpoint. Across 128 mixed read requests, five point reads failed with 503 `recovery_required`. Thirteen search requests returned 429 Busy. See the [per-request trace](results/overlay-requests.jsonl.gz) and the controlled publication probe above. |
| Same-version reopen | Process-cold SIGKILL recovery checked C in 782 ms. |
| Independent backup | Backup A returned 200, and recovery from the copied archive into a fresh root verified A in 4,234 ms. Both roots used the same backing filesystem. The corrupt copy returned 503 `storage_error` with `unsupported payload segment`. |
| Snapshot budget | The frozen binary published a 17,454,518-byte archive. The [separate fault/diagnostic Release build](results/preflight-report.json) logged a 3,496,639-byte preflight estimate against a 1,073,741,824-byte limit, returned 200, and published 17,454,510 archive bytes. No preflight rejection or large-scale query capacity was measured. |
| PITR | The [A/B/C fixture](results/pitr-fixture.json) records A's archive, B's two verified members and original receipt, and C's withdrawal and operation witness. Archive replay and the shared cut with Origence still require integration. |
| Targeted faults | The full HTTP fault suite, SIGTERM and reclamation smokes, three Release SIGABRT probes and four targeted library cases passed. [Command output](results/fault-cases.txt) |

[Origence `d34804c` evidence](results/origence-pinned.json) shows two archived turns and equal `directory_sha256` values before and after stopped paired restore. HTTP list, exact body and withdrawal passed on the recovered pair. Ordinary assets initially returned 200, then the worker logged `VERSION_OR_IDEMPOTENCY_CONFLICT` and the process exited with code 1 and `incompatible AnySeek retrieval collection`. Actual AnySeek Info exposes `max_candidates` and `max_journal_bytes` without the removed `max_records` field. The ordinary asset compatibility change uses its own Origence branch.
