# 单机性能与恢复验收 · #229

**简体中文** · [English](README.md)

以 [#229](https://github.com/TheLudlows/anyseek/issues/229) 和 [#224](https://github.com/TheLudlows/anyseek/issues/224) 为准。基线固定在 `74d48d92db27d4fa155ce38004ac079f3bf92835`。它已包含 #220 的容量限制移除、#221 的全文可见性优化和 #223 的退役兼容路径清理。源码核对时远端 `master` 位于该提交。

## 负载

- 通过实际 standalone HTTP `/ingest` 分批写入独立新库，每批 64 项。热集合测试点为 100,000、1,000,000、3,000,000 条，另有 4,096 条活跃集合与 64 条冷集合。每项核对 `committed` 回执和 Record ID，独立等词法及向量水位追平。脚本每次生成一批，按批记录内容哈希。
- 合成负载版本 `anyseek-229-v1`，正文字节数以 128、128、256、512 循环，32 个词法主题，8 个整型过滤桶。向量固定为 128 维，Top-K 为 16，过滤使用整型半开区间。读者执行点读、全文、向量和过滤后的混合检索，轮换请求顺序。
- 单集合热读先预热，再以一个闭环读者测量。随后运行单写者，再运行八读者（热集合六个、第二集合两个）、一个热集合写者和一个维护客户端。写者依次提交两成员组、替换为一个成员、清空，再执行单项异步写和独立删除。维护发送两个真实 PATCH 索引及 checkpoint 请求。基线 PATCH 没有显式 Compact 参数。报告记录实际 `state_image`、payload 压实及查询时间相交情况。显式 Compact 和未观察到的物理重写另列待验。
- Release 使用仓库默认特性、原生 CPU 配置和相同两颗服务 CPU，每个引擎版本单独编译。执行器记录 cgroup、内存、设备、CPU 序号、源码和二进制哈希。比较同机、同设备、相同 CPU 分配及数据集的结果。各版本各规模用全新数据目录并保留失败报告。

## 运行

先准备固定基线 checkout，以下命令在基准工具所在仓库根执行。使用绝对路径设置 `BASE_CHECKOUT`、`LOCAL_NVME`、`BACKUP_DEVICE`。前者应为已签出精确基线的独立 checkout。后两者应是本轮专用挂载路径。

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

用不同输出目录与候选 SHA 对候选二进制复跑，同一时刻运行一组。专用档要求操作员确认独占，数据路径必须有可识别的 NVMe 底层块设备，备份根必须在不同的块设备。共享/overlay 仅运行固定小夹具。装载达到预算时保留首个停止批及其逐项结果，后续规模仍可单独尝试。

`report.json` 的成功请求 p99 使用最近秩。延迟包括 HTTP 往返和读取整个响应，排除生成请求及 JSON 响应解码。吞吐含生成负载、结果检验、记录及等待水位。Busy 和其它失败分别统计，未知结果保留原样。进程内存用 `VmHWM`，数据根、临时目录物理分配量每秒采样。采样峰值不覆盖两个采样点间的瞬时用量。重开是进程冷启动，操作系统页缓存保留。测设备冷读需专用机器另行记录缓存处理和操作。

## 恢复与故障矩阵

| 用例 | 执行与核对 |
| --- | --- |
| 同版本重开 | 写入、确认并核对 C 后 SIGKILL 本次服务。用同一二进制、同一根重开，核对点读版本、组 tombstone、原 B/C 回执及原 B 重试不会复活。冷集合做首次点读。 |
| 独立备份 | 在 A 阶段调用完整服务备份并复制已发布归档，逐文件验证 SHA-256。只把归档交给新控制根，使用原生 `service-restores` 恢复，并启动返回的新根。核对 A 及原 A 回执，B/C 均缺失。在另一份复制件翻动一个 payload 字节，恢复必须失败且保留原备份。 |
| PITR | `pitr-fixture.json` 固定 A 基础备份、B 原子写入两条轮次 Record、C 撤回和可查账见证。真实恢复应从 A 加连续的认证归档重放到 B。检查 B 双成员、组版本 1、B 原回执、C 见证与回执缺失，并与 B 时点 Origence 配套 `directory_sha256` 比对。归档窗口边缘及断档使用集成配置验证。当前 HTTP restore 只接受 backup ID，配置归档窗口、目标切点及无断档重放依赖 #224。 |
| 精确进程故障 | 复用下列真实 HTTP 故障、Release panic 探针以及已存在的组 WAL、checkpoint 选择、回收和损坏用例。整个矩阵只操作新库。 |

完整服务快照的 1 GiB preflight 为 Catalog 的序列化字节数，加保留的集合及备份 `checkpoint_bytes`，每项另加 4096 字节。估算发生在创建 staging 之前。测量分别保存预估、限额、HTTP 结果、归档实际字节和查询容量。固定基线未输出 preflight 精确字节，报告留空。支持诊断输出的构建通过 `service snapshot preflight payload_bytes=… limit_bytes=…` 记录实际估算。归档落盘大小是独立值。 [基线实现](https://github.com/TheLudlows/anyseek/blob/74d48d92db27d4fa155ce38004ac079f3bf92835/anyseek/src/catalog/service_snapshot.rs#L339-L385)

```bash
# 使用独立的目标目录保存两种构建的二进制。
CARGO_TARGET_DIR=target/fault-release cargo +1.98.0 build --locked --release -p anyseek \
  --features fault-injection --bin anyseek_server --example runtime_panic_probe
python3 scripts/record_service_smoke.py --server target/fault-release/release/anyseek_server --faults
python3 scripts/record_shutdown_smoke.py --server target/fault-release/release/anyseek_server
python3 scripts/release_panic_smoke.py --probe target/fault-release/release/examples/runtime_panic_probe
for case in \
  record::group_write::tests::group_wal_process_exit_recovers_single_decision \
  record::anchor::tests::incremental_state_closure_survives_rejection_gc_and_reopen \
  record::anchor::tests::rejected_prepared_anchor_reclaims_only_unselected_objects \
  record::anchor::tests::multisegment_delete_and_corrupt_selected_object_fail_closed; do
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

结果见 [固定基线冒烟](results/overlay-report.json)、[PITR 夹具](results/pitr-fixture.json) 和 [Origence 固定组合](results/origence-pinned.json)。Linux x86-64 overlay，16 GiB cgroup 上限及 4 vCPU 配额，服务固定 CPU 0、1。未发现符合合约的本地独占 NVMe。只用实际 API 写入 512 + 512 + 64 条，适用范围为冒烟。未跑 100,000、1,000,000、3,000,000 三档正式测试，也未验证设备冷读、硬断电或归档窗口重放。

同版本重开验证 C。备份 A 返回 200，并从独立目录恢复。损坏复制件返回 503 `storage_error`。混合负载的 503 `recovery_required` 与 429 Busy 分开记录。该结果用于选择后续追踪点，样本规模不足以给出部署阈值。

Origence `d34804c` 独立启动并保存两轮，停机配套复核的 `directory_sha256` 一致，恢复后 HTTP 列表、回读、撤回通过。普通知识发布先返回 200，随后 Worker 记录 `VERSION_OR_IDEMPOTENCY_CONFLICT`，服务以 1 退出并记录 `incompatible AnySeek retrieval collection`。实际 AnySeek Info 提供 `max_candidates` 和 `max_journal_bytes`，已移除 `max_records`。普通资产兼容修复另走 Origence 分支。
