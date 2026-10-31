# Vector Segment 所有权收敛：前后对照

2026-10-30，基线为 PR #108 的 `4c795bfdbf8a8f8840250f91f8b3e2a44a5632b4`，对照为本 PR 的生产代码。测量保留原始结果，不设性能提升门槛。

## 查询行为和成本边界

本次移除重复的 `shard_number`、固定为一的构造参数、未使用的位置 ID，以及 tail 旋转时对共享 `meta.id` 的改写。逻辑查询配置由 VectorIndex 保存；文件、ANN 层级、量化、构建租约与派生可见性由各 Segment 保存。字段归属及地址规则见 [ARCHITECTURE](../../ARCHITECTURE.md#向量索引与-segment-所有权)。

查询仍捕获 S 个有效 Segment，最多启动 S 个向量子任务，S 受现有组件数上限约束。每段最多保留 K 个候选，全局合并至多 S×K 个候选后截取 K 个，排序上界仍为 O(S×K×log(S×K))。Hybrid 仍按全局通道名次执行一次 RRF，并分别应用 keyword/vector candidate cap。可见性过滤、稳定记录 ID tie-break、ANN 探测与量化算法不变，没有增加调度层或候选预算。

手工用例写入 120 条三维向量，分四批写入，前三批后执行 checkpoint，使冻结组件与 tail 同时参与检索。每种模式预热 20 次、采样 100 次，返回 Top-10；Vector 使用精确 `AnnMode::All`，Hybrid 使用默认 RRF。20 个独立进程的稳定 ID、命中顺序、分数位模式和各通道解释完全一致。该小数据集不代表大规模 ANN 召回率；ANN/量化/过滤边界另由现有 `server_shared_filters`、`external_vectors`、`stable_ranking`、`weighted_rrf` 和 `server_ann_contract` 的 13 个集成用例覆盖，均已通过。

## 实测

构建：Rust 1.99.1，Release opt-level=3，LTO=false，默认特性 zh/vb，target-cpu=native。环境为 Linux 容器，8 CPU quota、8 GiB 内存上限。没有服务并发请求。每组五次前后进程交替运行，偶数轮交换顺序；第二组把两版本进程均绑定到 CPU 1、2。报告值为五个进程各自 P50 的中位数，单位 µs；P95 同样是各进程最近秩 P95 的中位数，不是生产服务 P99。

| CPU 条件 | 模式 | 前 P50 | 后 P50 | 前 P50 范围 | 后 P50 范围 | 前 P95 | 后 P95 |
| --- | --- | ---: | ---: | --- | --- | ---: | ---: |
| 未绑定 | Lexical | 666 | 766.5 | 569–854 | 615–942 | 859 | 889 |
| 未绑定 | Vector | 58 | 67 | 42–129.5 | 62–85 | 77 | 105 |
| 未绑定 | Hybrid | 722.5 | 705 | 644–884.5 | 627–803 | 986 | 1011 |
| CPU 1、2 | Lexical | 595.5 | 608.5 | 579.5–619 | 568–646.5 | 837 | 770 |
| CPU 1、2 | Vector | 49 | 51 | 35–82 | 43–52 | 117 | 77 |
| CPU 1、2 | Hybrid | 680 | 689 | 637–694.5 | 657–775 | 898 | 1015 |

未绑定组的 Lexical/Vector 中位数约增加 15%，Hybrid 约减少 2%；绑定组分别约增加 2.2%、4.1%、1.3%。跨进程范围与尾延迟有明显波动，不能把这次结构收敛宣传为性能提升，也不能据此排除生产场景的回归。

Linux 进程 VmHWM（kB）未绑定组前 54388–59904、后 53936–59796；绑定组前 54500–60856、后 62096–65432。绑定组后版本峰值较高，记录该现象；未测逐项分配或稳定驻留集，不能归因为索引结构增长或证明不存在内存回归。进程 CPU 秒中位数分别为未绑定前 1.421/后 1.570，绑定前 1.393/后 1.436，包含数据构建与全部查询，不等于单查询 CPU 成本。

结论：正确性对照一致，删去的管理状态和共享元数据写入有明确冗余证据；没有可靠性能收益证据。保持现有候选、调度和 ANN 算法。若要宣称延迟或内存改善，需要更长采样、固定硬件和实际规模/并发下的独立测量。原始逐样本结果见 [未绑定组](unpinned-results.json) 与 [绑定组](pinned-results.json)。早期 Debug 试跑受同时编译和任务调度影响，只作排查，不用作收益证据。

## 复现

将 `anyseek/tests/segment_query_profile.rs` 同样复制到基线 checkout。两版本使用相同 Cargo.lock、编译器、特性和标志；在各自目录执行：

```sh
CARGO_TARGET_DIR=/tmp/segment-target CARGO_BUILD_JOBS=2 \
CARGO_PROFILE_RELEASE_LTO=false CARGO_INCREMENTAL=0 \
cargo test --offline --release -p anyseek --test segment_query_profile --no-run
```

从 Cargo 输出取得测试二进制路径，分别复制为 `/tmp/segment-before`、`/tmp/segment-after`。若复用同一 target 目录，切换 checkout 前必须执行 `cargo clean --offline --target-dir /tmp/segment-target --profile release -p anyseek`，避免相同 crate 路径的缓存误用。停止编译和其他负载后运行：

```sh
python3 benchmarks/vector-segments/compare.py \
  --before /tmp/segment-before --after /tmp/segment-after --output /tmp/segment-unpinned
python3 benchmarks/vector-segments/compare.py \
  --before /tmp/segment-before --after /tmp/segment-after --output /tmp/segment-pinned --cpus 1,2
```

手工测试默认 ignored，正常 CI 不执行计时；无新依赖、无易波动的通过阈值。Windows 不支持本工具的 CPU affinity/resource 采集，正确性由跨平台 CI 验证。
