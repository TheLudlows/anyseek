# 全文候选有界收集：#153 验收对照

2026-11-03，对比 [#178](https://github.com/TheLudlows/anyseek/pull/178) 合入前后的真实 Release HTTP 服务。实现已经合入；本目录只补手工测量工具与验收证据，没有修改生产代码、#152 写入规划或 CI。

结论：结合 #178 已有语义覆盖，本次提供 [#153](https://github.com/TheLudlows/anyseek/issues/153) 的固定语料分配、CPU 与尾延迟对照。分配证据的原始程度及结案建议见下文，不把实现合入等同于全部验收通过。16 个规模/场景/并发组合的四轮 CPU/请求中位数下降 36.2%–61.7%，p95 下降 38.7%–63.2%，p99 下降 31.3%–62.5%。未预设收益门槛。单轮仍有波动：10,000 条 ties/C1 第四轮 p99 上升 6.4%，不能宣称每轮均改善或把结果当作生产 SLA。

## 对照版本与方法

| 角色 | Commit |
| --- | --- |
| before：#178 第一父提交 | `3c9db3e66665c40938fb2b387531fdb07694a05c` |
| after：#178 合并提交 | `a047b55e0a108adf027ba0ba31b0b02686f42d7c` |

两版本仅相差 #178 的六个文件；本报告没有测量当前 master 的所有后续改动。版本、二进制和 Cargo.lock 的 SHA-256、编译器与 CPU 信息见 [build-provenance.json](results/2026-11-03/build-provenance.json)。Rust 1.99.1 / LLVM 23.1.1，默认 features，release opt-level=3、LTO=true、target-cpu=native。Linux x86_64 / AMD EPYC 9V74，容器 CPU quota 8 核、内存上限 8 GiB；服务固定在两个逻辑 CPU，Tokio/Rayon 各 2 线程，客户端使用其他 CPU。

语料为逆序业务 ID，正文固定 9 token：common 重复 1–8 次、padding 补足前 8 token、tied 恰好一次；priority 为插入序号，向量统一 `[1,0]`，tokenizer 为 TantivySimpleV1。10,000 条使用 standard_10k，20,000 条使用 bounded_100k；同一规模前后配置相同，跨规模比较同时改变了容量档。通过现有 /ingest 每批最多 64 条准备数据，显式 Busy 重试保留操作 ID；checkpoint 确认 indexed_seq 追平 visible_seq。每个进程复制同一基准快照，计时期间不写入。

| 场景 | 查询窗口及排序 |
| --- | --- |
| score | common，相关性 Top-32，count=true |
| ties | tied，同分业务 ID Top-32，count=true |
| field_page | common，priority 降序，offset=16、length=16 |
| hybrid | common + `[1,0]`，精确向量检索，keyword/vector 候选 32/48，RRF constant=60，返回 16 |

四轮顺序 AB/BA/AB/BA，每阶段预热 24 次，再采样 200 次；四场景、两个规模、C1/C4 闭环并发，共 **25,600 次正式请求**。CPU 为服务进程 `/proc` user+system 秒数除以请求数，粒度 10ms；HTTP 延迟计至读完响应，不含客户端 JSON 解析与核对。客户端核对仍影响闭环请求间隔，因此这不是固定到达率压测。表中是每轮指标的中位数，不是混合所有样本的分位数。

## CPU 与尾延迟

| 记录数 | 场景 | 并发 | CPU ms/请求（前 → 后） | p95 ms（前 → 后） | p99 ms（前 → 后） |
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

每轮配对变化见 [comparison.csv](results/2026-11-03/comparison.csv)。原始每阶段结果及逐请求延迟在 [measured](results/2026-11-03/measured)；逐请求 JSONL 与期望结果 JSON 使用 gzip 无损压缩。

## 分配请求与正确性

独立插桩两轮 AB/BA、C1、每阶段 30 次，合计 **960 次正式请求**。`alloc.c` 通过 LD_PRELOAD 统计 glibc malloc/calloc/realloc 成功调用及申请大小，包含服务后台活动；其他分配入口未覆盖。它测的是累计申请字节，不是 live/peak 内存或 RSS；插桩延迟不用于上表。

| score 场景 | 累计申请字节/请求（前 → 后，两轮中位数） | 变化 |
| --- | ---: | ---: |
| 10,000 条 | 10,019,148.8 → 6,899,260.5 | −31.1% |
| 20,000 条 | 19,816,818.4 → 13,573,672.9 | −31.5% |

全部场景见 [allocation-comparison.csv](results/2026-11-03/allocation-comparison.csv) 与 [allocation](results/2026-11-03/allocation)。两个阶段均零请求错误、零前后业务结果差异。逐请求比较完整 hits、分数、通道解释、count/count_total、visible_seq、query_terms、fusion；独立语料 oracle 核对 score/ties/字段分页的 ID，Hybrid 按 f32 RRF 独立计算成员、分数及通道名次。这不是独立重实现 BM25 的 oracle；更广语义覆盖来自 #178。

历史分配摘要保存了每阶段的 calls/request 和 requested bytes/request，**没有保存计数器前后快照**。可重算两轮中位数、变化率，但无法独立还原当时读取的累计计数器；CPU 也从已存 `cpu_s` 重算，没有保留 `/proc` 起止快照。逐请求归档保存延迟，不保存每次完整响应；零差异来自测量时逐次 assert 成功，不是事后重放全部响应所得。期望响应和语料 oracle 可以离线复核。保留这些证据边界，不补造历史原始数据。

## #153 验收证据对应

| 验收项 | 证据及边界 |
| --- | --- |
| 收集空间随窗口增长 | #178 的容量单测，以及其 collector-only 测量：K=32、10k/20k 命中均为 2,048 字节峰值；本次没有重测该峰值，不能推断整条查询 O(K) 内存。 |
| 独立 oracle 比较成员、排序、offset、解释 | #178 的既有精确断言与本次语料 oracle、逐请求完整业务结果对照。 |
| 字段排序、Hybrid 预算、多 Segment、更新删除、计数 | 本次覆盖字段分页、独立 32/48 预算、通道解释和计数；跨 Segment、更新删除等沿用 #178 的用例及其 [常规 CI](https://github.com/TheLudlows/anyseek/actions/runs/41266182689)，没有算作本次重新执行。 |
| 固定语料分配、CPU、尾延迟 | 本目录保存的 25,600 次计时请求、960 次独立分配请求及复现脚本；不预设收益阈值。 |

已合入实现和历史语义测试分别见 #178 的六文件 diff：`bounded_candidates_match_full_sort` 对照全候选排序并检查每次插入后的容量；`union_topk_fill_ranking` 对照完整排名的精确前缀及同分业务 ID；`segment.rs` 的跨组件测试覆盖更新、删除、排序和分页；`weighted_rrf` 覆盖字段排序后独立通道候选预算、全局排名与解释。完整排序 oracle 与堆收集独立，但共享评分代码，不是独立 BM25 实现。这些历史结果不计入本次重新执行的测试数量。

**结案建议：#153 的实现、语义覆盖及固定负载性能对照已具备审阅依据；接受上述阶段分配摘要的证据精度后可以结案。** 若要求从原始分配计数器完全重放，则该项仍未完成，须用本次修正后的工具另做分配复测，不能用当前归档冒充。PR 保留 `Refs #153`，不自动关闭。真实 S3、冷缓存、更新并发及当前主干整体性能不是本次历史算法对照的完成项。

仅验证预热后的确定性合成只读负载；未覆盖真实 S3、中文相关性、冷缓存、Windows 性能、更新并发尾延迟或 #152/#155 验收。原始测量整理未重跑 Rust 语义测试；本次收尾在主干 `768a18e` 的生产源码上重新构建 Linux 默认特性测试二进制，复用 8 个现有定向用例全部通过：有界容量/全排序 oracle、跨组件更新删除统计，以及 `union_topk_fill_ranking` 的 4 项和 `weighted_rrf` 的 2 项。它们验证当前源码语义，不是新的性能测量，也不等同完整 CI。已有构建警告不在本次范围。早期试跑、共享 target 误复用相同二进制的结果及未完成复制的试跑均已丢弃；最终清理后分别重编并验证二进制哈希不同。

## 复现

需要 Linux x86_64、glibc、Python 3.12+、Rust 1.99.1、C 编译器、taskset，至少三个可用逻辑 CPU，以及可读的 cgroup v2 cpu.max/memory.max。无 Python 第三方依赖。不要与构建或其他压测并行运行；不要用 `python -O`，正确性检查使用 assert。

从仓库根目录构建两个固定版本，使用独立 target 目录避免复用错误产物（保留两个版本自带 release profile 和 .cargo/config.toml）：

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

检查每一步成功后继续。脚本只启动本机临时服务，自动停止服务并删除每轮运行副本，保留 seed 供分配阶段复用；`--out` 必须使用新的目录，不要指向本目录的历史 results。已归档数据不含数据库快照，不能直接作为 `--seed-root`；从固定规则重新准备即可。操作 UUID/时间会变化，语料内容 SHA-256 应与 seed 清单一致。小规模试跑可减少 rounds/requests，但汇总脚本对应本报告的完整四轮计时与两轮分配矩阵。

仅重算已归档结果，无需启动服务：

```sh
python3 summarize.py results/2026-11-03 --out /tmp/anyseek-153-summary > /tmp/anyseek-153-summary.json
```

本次收尾保留原始归档与汇总数值，修正工具：汇总前检查 SHA-256 清单及完整矩阵、逐请求数量/分位数、CPU/QPS 算术、二进制指纹、按规则重建的语料指纹/水位、期望响应 oracle 和两阶段一致性；明确报告缺失的历史分配快照。输出必须位于输入证据目录之外。测量入口拒绝已有输出目录、无效矩阵和 `python -O`；新分配运行额外保存计数器起止值。`alloc.c` 未改动，历史测量脚本可从 PR #190 初始提交 `b531aa6` 获取，新脚本没有冒充历史测量时的原样版本。

`results/2026-11-03/SHA256SUMS.json` 覆盖归档证据；包含环境、语料指纹、水位、期望业务结果、逐阶段摘要和压缩逐请求延迟，不包含服务二进制、数据库、冗余 HTML 或编译/服务日志。清单检测文件损坏，不提供签名认证。手工性能工具不接入 CI，也不增加计时通过阈值。
