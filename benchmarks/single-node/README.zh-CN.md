# 单机性能与恢复验收 · #229

**简体中文** · [English](README.md)

以 [#229](https://github.com/TheLudlows/anyseek/issues/229) 和 [#224](https://github.com/TheLudlows/anyseek/issues/224) 为准。基线固定在 `74d48d92db27d4fa155ce38004ac079f3bf92835`。它已包含 #220 的容量限制移除、#221 的全文可见性优化和 #223 的退役兼容路径清理。源码核对时远端 `master` 位于该提交。

## 负载

- 通过实际 standalone HTTP `/ingest` 分批写入独立新库，每批 64 项。热集合测试点为 100,000、1,000,000、3,000,000 条，另有 4,096 条活跃集合与 64 条冷集合。每项核对 `committed` 回执和 Record ID，独立等词法及向量水位追平。脚本每次生成一批，按批记录内容哈希。
- 合成负载版本 `anyseek-229-v1`，正文字节数以 128、128、256、512 循环，32 个词法主题，8 个整型过滤桶。向量固定为 128 维，Top-K 为 16，过滤使用整型半开区间。读者执行点读、全文、向量和过滤后的混合检索，轮换请求顺序。
- 创建第二个活跃集合和冷集合前，先对热集合预热，再以一个闭环读者测量。随后运行单写者，再运行八读者（热集合六个、第二集合两个）、一个热集合写者和一个维护客户端。写者依次提交两成员组、替换为一个成员、清空，再执行单项异步写和独立删除。维护发送两个真实 PATCH 索引及 checkpoint 请求。基线 PATCH 没有显式 Compact 参数。报告记录实际 `state_image`、payload 压实及查询时间相交情况。显式 Compact 和未观察到的物理重写另列待验。
- Release 使用仓库默认特性、原生 CPU 配置和相同两颗服务 CPU，每个引擎版本单独编译。执行器记录 cgroup、内存、设备、CPU 序号、源码和二进制哈希。比较同机、同设备、相同 CPU 分配及数据集的结果。各版本各规模用全新数据目录并保留失败报告。

## 运行

正式测量在操作者预留的本地 Linux NVMe 主机上运行，需要 Python 3.11 或更新版本，以及 util-linux 的 `findmnt` 和 `lsblk`。开跑前将干净的 harness checkout 固定在一个完整 Git SHA，两个引擎始终用这同一个 checkout 的脚本。`BASE_CHECKOUT` 和 `CANDIDATE_CHECKOUT` 是独立的引擎 checkout。基线固定为 `74d48d92db27d4fa155ce38004ac079f3bf92835`。等 [#235](https://github.com/TheLudlows/anyseek/pull/235) 生产路径就绪并选定候选，再设置 `CANDIDATE_REVISION`。三个 checkout、证据目录 `META_DIR`、预留数据盘 `LOCAL_NVME` 及独立备份盘 `BACKUP_DEVICE` 均用绝对路径。

在 harness 根目录确认工作树干净，再把提交和文件哈希保存到 checkout 之外的 `META_DIR`。清单覆盖共享 HTTP transport、执行器和本地辅助脚本，与同组报告一起保留。

```bash
git status --short
test -z "$(git status --porcelain)" || exit 1
mkdir -p "$META_DIR"
git rev-parse --verify HEAD > "$META_DIR/harness.commit"
sha256sum benchmarks/single-node/{measure,recovery,point_fence,origence_pair}.py \
  benchmarks/shared_record_http.py > "$META_DIR/harness.sha256"
```

正式运行前验证同一套 Rust 工具链可编译两边。先用固定基线已验证的 Rust 1.98.0，保持 Release profile、默认特性、目标 CPU 配置及编译器覆写一致。如果两边无法使用同一工具链，保留编译失败证据，随各自结果记录实际编译器、flags 和 Release 设置，并注明这项对照差异。

```bash
toolchain=1.98.0
base_revision=74d48d92db27d4fa155ce38004ac079f3bf92835
git -C "$BASE_CHECKOUT" checkout --detach "$base_revision" || exit 1
git -C "$CANDIDATE_CHECKOUT" checkout --detach "$CANDIDATE_REVISION" || exit 1
rustc "+$toolchain" --version --verbose > "$META_DIR/rustc.txt"
cargo "+$toolchain" --version --verbose > "$META_DIR/cargo.txt"
(cd "$BASE_CHECKOUT" && CARGO_TARGET_DIR="$BASE_CHECKOUT/target" cargo "+$toolchain" \
  build --locked --release -p anyseek --bin anyseek_server -j 4) > "$META_DIR/baseline-build.log" 2>&1 || exit 1
(cd "$CANDIDATE_CHECKOUT" && CARGO_TARGET_DIR="$CANDIDATE_CHECKOUT/target" cargo "+$toolchain" \
  build --locked --release -p anyseek --bin anyseek_server -j 4) > "$META_DIR/candidate-build.log" 2>&1 || exit 1
baseline="$BASE_CHECKOUT/target/release/anyseek_server"
CANDIDATE_BINARY="$CANDIDATE_CHECKOUT/target/release/anyseek_server"
sha256sum "$baseline" "$CANDIDATE_BINARY" > "$META_DIR/binaries.sha256"

python3 benchmarks/single-node/measure.py --binary "$baseline" \
  --revision 74d48d92db27d4fa155ce38004ac079f3bf92835 --out /tmp/anyseek-229-smoke

for scale in 100000 1000000 3000000; do
  test "$(git rev-parse --verify HEAD)" = "$(cat "$META_DIR/harness.commit")" || exit 1
  sha256sum -c "$META_DIR/harness.sha256" || exit 1
  python3 benchmarks/single-node/measure.py --binary "$baseline" \
    --revision "$base_revision" \
    --mode dedicated --scale "$scale" --exclusive-nvme --cpus 0,1 \
    --out "$LOCAL_NVME/baseline-$scale" --backup-root "$BACKUP_DEVICE/baseline-$scale"
  python3 benchmarks/single-node/measure.py --binary "$CANDIDATE_BINARY" \
    --revision "$CANDIDATE_REVISION" \
    --mode dedicated --scale "$scale" --exclusive-nvme --cpus 0,1 \
    --out "$LOCAL_NVME/candidate-$scale" --backup-root "$BACKUP_DEVICE/candidate-$scale"
done
```

同一时刻运行一组，每轮保留输出目录的 `report.json` 和 `requests.jsonl.gz`。专用档要求操作者确认独占，数据路径必须有可识别的 NVMe 底层块设备，备份根必须在不同的块设备。共享/overlay 仅运行固定小夹具。装载达到预算时保留首个停止批及其逐项结果，后续规模仍可单独尝试。

在预留主机上记录设备、进程 CPU 分配和实际可用字节，再安排两个版本的运行。

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

按四条正文循环平均，确定性文档 JSON 每条上界为 1,441 字节。计入 4,096 和 64 条辅助集合，三档文档 JSON 上界分别为 150,094,560、1,446,994,560、4,328,994,560 字节。请求封套和实际磁盘结构须另计。将备份前引擎稳态物理字节记为 `S`，完整归档复制件物理字节记为 `A`，额外的维护、staging 和临时空间峰值记为 `T`，在预留文件系统通过 `du -s -B1` 和报告的分配量采样测算。每轮数据盘先按 `1.2 × (2S + 4A + T)`、独立备份盘按 `1.2 × A` 预留，覆盖源库、恢复库、原归档、两个恢复控制目录复制件及一个归档大小的暂用空间。保留多轮输出时累加。安排 PITR 前按实际归档窗口补算日志字节。1 GiB preflight 采用不同于归档占盘的估算口径，独立记录首次预算拒绝。

`report.json` 的成功请求 p99 使用最近秩。延迟包括 HTTP 往返和读取整个响应，排除生成请求及 JSON 响应解码。吞吐含生成负载、结果检验、记录及等待水位。Busy 和其它失败分别统计，未知结果保留原样。进程内存用 `VmHWM`，数据根、临时目录物理分配量每秒采样。采样峰值不覆盖两个采样点间的瞬时用量。重开是进程冷启动，操作系统页缓存保留。测设备冷读需专用机器另行记录缓存处理和操作。

`status: complete` 只表示 runner 执行到末尾。分别给出 HTTP 503、HTTP 429 和逐项 Busy、快照发布及预算、独立备份恢复、同版本重开、PITR 的结果。验证基础备份恢复只覆盖该归档的切点，PITR 需要单独的目标切点证据。下列提取保留每次失败或 Busy 请求的阶段，可识别故意损坏归档的拒绝。将 `run` 设为任一引擎的输出目录。

```bash
trace="$run/requests.jsonl.gz"
gzip -dc "$trace" | jq -c 'select(.status == 503 or .status == 429 or (.busy_items // 0) > 0)
  | {phase, operation, path, status, code: (.response.code // .response.error.code), busy_items}'
jq '{runner_status: .status,
     backup: (.backup | {status, http_status, preflight_payload_bytes, preflight_limit_bytes, archive_bytes: .copy.bytes}),
     independent_backup_restore, damaged_archive, same_version_reopen, pitr}' "$run/report.json"
```

单写与混合阶段的真实索引追平使用轨迹中 `sample == "index_catch_up"` 的 `wait_ms`，即收到回执后轮询到全文、向量两路水位都达到目标切点的等待。`index_progress` HTTP p99 衡量轮询往返。批量装载的同类采样在 `report.json` 各集合的 `load.*.catch_up`。

```bash
gzip -dc "$trace" | jq -c 'select(.sample == "index_catch_up")
  | {phase: .sample_phase, cut, wait_ms, lexical_seq: .progress.lexical_seq, vector_seq: .progress.vector_seq}'
jq -c '.load | to_entries[] | .key as $collection | .value.catch_up[]?
  | {collection: $collection, cut, wait_ms, lexical_seq: .progress.lexical_seq, vector_seq: .progress.vector_seq}' "$run/report.json"
```

以集合和操作为单位复核混合负载时，将 `trace` 指向本次运行的 `requests.jsonl.gz`。

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

## 恢复与故障矩阵

### 发布期间点读

以固定 Release 和指定的 #224 候选分别运行。每轮为四个独立小库复用同一语料与八路读者，覆盖仅读、并发写入、HTTP checkpoint、写入与 checkpoint 同时运行。记录每次初始请求和复核的路径及时间区间。观察线程在混合窗口内重试 503，窗口结束后核对同一 Record 的版本和原文。进度接口会等待分区锁，返回 `false` 时短时输出 fence 可能已经清除。静默等待后若读或健康仍失败，探针尝试同版本重开并记录结果。

```bash
python3 benchmarks/single-node/measure.py --binary "$baseline" \
  --revision 74d48d92db27d4fa155ce38004ac079f3bf92835 \
  --point-fence-probe --probe-rounds 3 --cpus 0,1 --out /tmp/anyseek-229-fence-baseline
python3 benchmarks/single-node/measure.py --binary "$CANDIDATE_BINARY" \
  --revision "$CANDIDATE_REVISION" \
  --point-fence-probe --probe-rounds 3 --cpus 0,1 --out /tmp/anyseek-229-fence-candidate
```

退出 1 表示可用性失败，重试读到原文仍记失败。HTTP 503 与 HTTP 429 分开统计。退出 2 表示证据不完整，例如 checkpoint 对照缺少与点读重叠且成功的热集合 checkpoint。退出 0 适用于本次已测小库窗口。报告、逐场景请求轨迹及服务日志保留在输出目录。

固定基线在 overlay 上的三轮探针判定为可用性失败。初次混合冒烟在热集合 checkpoint 期间的 5 次失败保留在原始轨迹。控制变量复测在有无 checkpoint 的并发写入中均复现。

| 固定基线窗口 | 首次点读 503 / 请求数 | 混合窗口全部 HTTP 429 | 在同一进程复核原版本及原文 |
| --- | ---: | ---: | ---: |
| 仅读 | 0 / 96 | 0 | 无需重试 |
| 写入，无 checkpoint | 9 / 96 | 18 | 9 / 9 |
| Checkpoint，无写入 | 0 / 96 | 0 | 无需重试 |
| Checkpoint 与写入 | 14 / 96 | 20 | 14 / 14 |

23 次初始 `recovery_required` 都与普通 `async_write` 或 `record_delete` 请求重叠。6 次首次重试仍为 503。近端及静默后的进度采样均显示 `recovery_required: false`。所有失败点读均在原进程读回同一版本和原文。仅加日志的诊断 Release 对 8 次初始和 6 次首次重试 503 输出了 14 条 `stage=capture ... fenced=true`，未命中 Record 健康检查或响应交付拒绝分支。 [探针报告](results/point-fence-report.json) · [逐请求轨迹](results/point-fence-requests.jsonl.gz) · [诊断和原生用例输出](results/point-fence-diagnostic.txt)

集成 Release `d8e19e18ac4166a5ffa71f41fe79888566496a89` 也未通过探针。并发写入无 checkpoint 时首次点读 503 为 7 / 96，加 checkpoint 为 3 / 96。两个窗口各有 23 次混合 HTTP 429。仅读及仅 checkpoint 均无点读 503，混合 429 分别为 4 次和 1 次。10 次失败都在原进程读回同一版本和原文，5 次首次重试仍为 503，进度采样均正常，无需重开。 [集成 PR #235](https://github.com/TheLudlows/anyseek/pull/235)

集成 Release `2811966` 和 `f068270` 在同一小库探针中各有 384 次初始点读，均返回 HTTP 200。每版的两个 checkpoint 场景各有 72 次同集合点读重叠。其他混合请求仍有 HTTP 429，另行统计。两版均用 Rust 1.98.0 构建，操作者本地 NVMe 正式运行的候选尚待选定。 [#233 补充证据](https://github.com/TheLudlows/anyseek/pull/233)

点读路由在查询 Record 前同步取得 `output_permit()`。读到 `OutputState.fenced` 即返回 `RecordError::RecoveryRequired`，HTTP 映射为 503。普通批写在发布阶段置位，成功后清除。进度接口获取 `Inner`，因此会在短时 fence 清除后才采样。#224 可复用查询路由已用的等待发布机制，涵盖点读捕获和交付，并保留真正发布失败时的拒绝语义。修改后复跑本探针，以及暂停健康写者和实际发布失败两项回归。 [点读入口](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/server/record_service.rs#L970-L995) · [捕获检查](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/record.rs#L998-L1005) · [写入发布](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/record/write.rs#L250-L281) · [查询交付](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/server/record_service.rs#L1687-L1705)

复核诊断日志时，在一次性 `74d48d9` checkout 应用[日志补丁](results/point-fence-diagnostic.patch.gz)，按同一 Release profile 编译并用相同探针运行。保留独立诊断二进制和原固定 Release。

```bash
gzip -dc benchmarks/single-node/results/point-fence-diagnostic.patch.gz | git -C "$DIAGNOSTIC_CHECKOUT" apply
(cd "$DIAGNOSTIC_CHECKOUT" && cargo +1.98.0 build --locked --release -p anyseek --bin anyseek_server -j 4)
```

### 恢复矩阵

| 用例 | 执行与核对 |
| --- | --- |
| 同版本重开 | 写入、确认并核对 C 后 SIGKILL 本次服务。用同一二进制、同一根重开，核对点读版本、组 tombstone、原 B/C 回执及原 B 重试不会复活。冷集合做首次点读。 |
| 独立备份 | 在 A 阶段调用完整服务备份并复制已发布归档，逐文件验证 SHA-256。只把归档交给新控制根，使用原生 `service-restores` 恢复，并启动返回的新根。核对 A 及原 A 回执，B/C 均缺失。在另一份复制件翻动一个 payload 字节，恢复必须失败且保留原备份。 |
| PITR | `pitr-fixture.json` 固定 A 基础备份、B 原子写入两条轮次 Record、C 撤回和可查账见证。真实恢复应从 A 加连续的认证归档重放到 B。检查 B 双成员、组版本 1、B 原回执、C 见证与回执缺失，并与 B 时点 Origence 配套 `directory_sha256` 比对。用集成后的权威回执查询验证 C 组撤回回执缺失。基线 HTTP 回执接口只提供普通 Record 回执。归档窗口边缘及断档使用集成配置验证。当前 HTTP restore 只接受 backup ID，配置归档窗口、目标切点及无断档重放依赖 #224。 |
| 精确进程故障 | 复用下列真实 HTTP 故障、Release panic 探针以及已存在的组 WAL、checkpoint 选择、回收和损坏用例。整个矩阵只操作新库。 |

完整服务快照的 1 GiB preflight 为 Catalog 的序列化字节数，加保留的集合及备份 `checkpoint_bytes`，每项另加 4096 字节。估算发生在创建 staging 之前。测量分别保存预估、限额、HTTP 结果、归档实际字节和查询容量。固定基线未输出 preflight 精确字节，报告留空。支持诊断输出的构建通过 `service snapshot preflight payload_bytes=… limit_bytes=…` 记录实际估算。归档落盘大小是独立值。 [基线实现](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/catalog/service_snapshot.rs#L339-L385)

```bash
# 使用独立的目标目录保存两种构建的二进制。
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

## Origence 配套验证

使用隔离的 Origence checkout 及构建。模型返回合成向量，Origence 和 AnySeek 均运行独立 loopback HTTP。脚本先验证私有归档及离线配套 `archive-verify`，恢复后重新通过 HTTP 核验列表、回读和撤回。普通资产发布在另一套新库单独测试。成功时验证发布 Job 和原文搜索。失败时保存真实 AnySeek Info 和 Origence 进程日志。 [Origence 配套恢复说明](https://github.com/TheLudlows/Origence/blob/d34804c52560936e0c654ef40a0440fba3a23ca8/docs/OPERATIONS.md#私有问答归档与配套恢复)

```bash
(cd "$ORIGENCE_CHECKOUT" && cargo +1.98.0 build --locked --bin origence)
python3 benchmarks/single-node/origence_pair.py --origence-repo "$ORIGENCE_CHECKOUT" \
  --origence-binary "$ORIGENCE_CHECKOUT/target/debug/origence" \
  --anyseek-binary "$baseline" --out /tmp/anyseek-229-origence
```

## 已得结果

共享 Linux x86-64 overlay，16 GiB cgroup 上限、4 vCPU 配额，服务固定 CPU 0、1。[专用档检查](results/media-guard.txt) 拒绝该存储。[固定 Release 冒烟](results/overlay-report.json) 通过真实 API 确认 512 + 512 + 64 条。Release 二进制 SHA-256 为 `b09aa6a5ef012a3be8221c8ed9d6546c3843e0995c5b5eeb7af205d6c0bdebfc`。待在合约设备上执行 100,000、1,000,000、3,000,000 三档，验证设备冷读、硬断电和归档窗口重放。

| 检查 | 共享执行器观察 |
| --- | --- |
| 查询和维护 | 热集合 checkpoint 与 128 次查询重叠，随后运行的第二个集合 checkpoint 无重叠。混合负载共 128 次读，其中 5 次点读失败并返回 503 `recovery_required`，13 次搜索返回 429 Busy。见[逐请求记录](results/overlay-requests.jsonl.gz)及上面的对照探针。 |
| 同版本重开 | SIGKILL 后进程冷启动，在 782 ms 内验证 C。 |
| 独立备份 | 备份 A 返回 200。从复制的归档恢复到新根并验证 A 共 4,234 ms。归档与源库仍在同一底层文件系统。损坏复制件返回 503 `storage_error`，原因是 `unsupported payload segment`。 |
| 快照预算 | 固定二进制发布了 17,454,518 字节归档。[独立故障／诊断 Release 构建](results/preflight-report.json) 实测 preflight 为 3,496,639 字节，限额 1,073,741,824 字节。返回 200，归档实际为 17,454,510 字节。本轮未触及 preflight 拒绝或测量大规模查询容量。 |
| PITR | [A/B/C 夹具](results/pitr-fixture.json) 保存 A 的归档、B 两个已核对成员及原回执，还有 C 的撤回与操作见证。归档重放和 Origence 配套切点待集成。 |
| 精确故障 | 完整 HTTP 故障套件、SIGTERM 和回收冒烟、三个 Release SIGABRT 探针和四项库级定点用例均通过。见[命令输出](results/fault-cases.txt)。 |

[Origence `d34804c` 实测](results/origence-pinned.json) 保存两轮，停机配套恢复前后 `directory_sha256` 一致，恢复后 HTTP 列表、原文回读、撤回通过。普通知识发布先返回 200，随后 Worker 记录 `VERSION_OR_IDEMPOTENCY_CONFLICT`，服务以 1 退出并记录 `incompatible AnySeek retrieval collection`。实际 AnySeek Info 提供 `max_candidates` 和 `max_journal_bytes`，已移除 `max_records`。普通资产兼容修复另走 Origence 分支。
