# 写入成本对照（#191）

本目录只含手工测量脚本和压缩证据，不改变生产算法，不接入默认 CI 性能门禁。结论见 [REPORT](REPORT.md)。**已完成同机 tmpfs 的完整 HTTP 场景矩阵；真实磁盘成本因共享磁盘耗尽而未完成。**

## 复现

固定三个独立 checkout（不要混用分支上已有的二进制）：

| 标签 | 精确提交 | 用途 |
| --- | --- | --- |
| before | `32225e5e656926736e81517837be65ebce3c88ca` | #191 合并的第一父提交 |
| after | `c01e2e172e9a3747da07fb008d2bf02ae6d1dea9` | #191 合并提交，测完整 PR |
| latest | `768a18eb0ccb25d23d1f2b5bf9a09ec071afc857` | 本任务开始时最新主干；与 after 仅文档差异 |

每个 checkout 使用独立 Cargo target 目录；如复用依赖目录，先运行 `cargo clean -p anyseek --release`，以免旧时间戳导致跨 checkout 错误复用。随后串行运行 `cargo build --offline --locked --release -p anyseek --bin anyseek_server -j 2`，立即将服务复制到独立二进制路径。缺少依赖缓存时先准备依赖，再开始测量；所有构建完成后才运行基准。沿用仓库 release/default features、LTO、`panic=abort` 和 `target-cpu=native`，不启用 fault-injection。

```bash
python benchmarks/write-cost/measure.py \
  --binary before=/absolute/binaries/before \
  --binary after=/absolute/binaries/after \
  --binary latest=/absolute/binaries/latest \
  --out /dev/shm/write-cost-timing
python benchmarks/write-cost/summarize.py /dev/shm/write-cost-timing

gcc -Wall -Wextra -Werror -O2 -shared -fPIC \
  benchmarks/write-cost/alloc.c -o benchmarks/write-cost/alloc.so
python benchmarks/write-cost/measure.py \
  --binary before=/absolute/binaries/before \
  --binary after=/absolute/binaries/after \
  --binary latest=/absolute/binaries/latest \
  --out /dev/shm/write-cost-allocation --batches 16 --allocation
python benchmarks/write-cost/summarize.py /dev/shm/write-cost-allocation
```

输出目录必须不存在。`--short --sizes 256 --batches 1` 仅验证驱动，不能作为性能证据。测量、恢复和汇总入口均拒绝 `python -O`，不能禁用其正确性断言。Linux/glibc；server CPU 0/1、client CPU 2/3，均可显式改参；Tokio/Rayon 各 2 worker。脚本取得 `/tmp/anyseek-performance.lock` 并检查可见的构建/服务进程，发现干扰立即终止；同资源上的其他任务须先结束，锁不能约束不遵守锁的任务，也不能证明不可见宿主机绝对空闲。

## 测量与验证口径

### 写入发布路径的小规模检查

`publication_probe.py` 使用 Python 标准库，可在 Windows/Linux 对固定的两个服务二进制分别执行。每组新建临时 local 根，三次预热后记录逐次 HTTP 延迟及 Busy 原请求重试，并验证查询计数和回执重放；每个批量默认三个独立进程。失败即停止，只有报告的 `all_requested_runs_completed=true` 才表示全部请求场景完成。

```sh
python benchmarks/write-cost/publication_probe.py --binary /absolute/before --out target/publication-before.json
python benchmarks/write-cost/publication_probe.py --binary /absolute/after --out target/publication-after.json
```

先完成构建，再串行运行，输出路径须不存在；报告记录二进制 SHA-256。这个入口仅测新库、无向量的顺序同步新增，不代表历史规模、混合负载、饱和吞吐或生产尾延迟；debug 测量不能代替 release 与代表性持久设备验收。它与下述 #191 历史矩阵分别解释。

### #191 历史矩阵

- 标准 10k 档、1k/5k 初始记录、相同 249–251B 正文和 32 维预生成向量。初始数据只用 before 创建、checkpoint 并正常关闭；每次从逐文件 SHA-256 相同的副本开始。记录/回执保留使用默认两日，没有跨保留期 GC 或外部 embedding 服务成本。
- 新增无向量正文 → Match(1) 向量补写（正文版本保持 1）→ Match(1) 替换正文与向量（版本 2）→ Match(2) 删除（版本 3）。固定 ID/Operation ID/批顺序；每种操作 5 个预热请求，单条 128 个正式请求、batch16 40 个正式请求。初始记录不变，同一临时记录集合完成四步。
- 每个容量、批量、端点按 before/after/latest、latest/after/before 运行两个独立进程/版本。分位数在**单进程单场景**内计算，再报告两个进程指标的中位数；40 个 batch16 请求的 p99 实际就是最大值，不能当稳定生产尾部估计。
- 响应耗时包含 HTTP/反序列化及显式 Busy 原请求重试；成功 HTTP 单次耗时另保留在 attempts。Busy 同时检查 HTTP 外层与 HTTP 200 的逐项结果；重放完整原批并逐项核对已成功回执不变，分别保留 Busy 响应次数和 Busy 项次数。仅重试明确 Busy，不把 unknown、transport error 或非成功逐项结果计为成功。
- HTTP `/batch` 的 `writes` 是逐条 `p.write`，不等于一次内核 `write_batch`；`/ingest` 才经 committed queue / `commit_writes` 批量提交。因此 batch16 的端点差距还包括持久发布与 native 刷新次数差异。
- 返回回执后轮询两消费者水位，每 2ms 一次，测量的是**观察到追平的额外时间**，含观测请求与采样误差。同步 batch 也执行同一探针，因此其非零 catchup 不代表索引落后。不用 batch−ingest 相减推算索引独占成本。
- `wall_records_s` 包含驱动、资源/目录探针和原始证据写入；`completed_records_s` 是记录数除以累计响应+追平时间。两者均为 C1 闭环、逐批追平吞吐，不代表饱和队列吞吐。server CPU 包括该窗口内后台/索引/日志/进度接口；RSS/HWM 是进程指标，HWM 是进程启动以来峰值。
- 新 cache 目录在请求及追平前后观察，表示窗口与 cache 建立重合；不等于精确 `rotate_tail` 时长，也不能排除后台并发构建。普通与 cache 创建窗口单独保留，不删除退化、Busy 或极端样本。
- glibc 计数器原样复用 [#190](https://github.com/TheLudlows/anyseek/pull/190) 的 `b531aa60f770ffe1155a6dcb5f6f96eaaacb2c25`，HTTP/proc 辅助代码来自同一脚本，维护阶段解析复用现有 `capacity/async_compare.py`。分配用独立轮次，计成功 malloc/calloc/realloc 请求次数与累计请求字节，不计 live/peak bytes、mmap 或独立 aligned allocation 接口；不将插桩轮次延迟混入正常结果。
- 每组独立检查完整临时 ID 集的全文/向量结果、旧词清除、总数、版本及待补向量；计时外另保留待补向量/已补向量/已更新的三条活记录；回执抽样读取后正常关闭、重开，再按原顺序重放**全部**操作并逐项比较原回执，确认 committed_seq 未增加，并核对三条活记录的完整内容与检索结果。正常重开不是断电或 unknown 故障认证。

## 阶段可观测性与设计约束

| 阶段 | 本任务能证明什么 | 不能拆分什么 |
| --- | --- | --- |
| 规划 | 精确前后源码、端到端 CPU/分配对照；#191 用本批成功前缀替代 full shadow | 生产服务无独立 planner timer，不能把全部差值归给 planner |
| 回执检查 | lookup 与完整同 ID 请求比较保留；`has_expired_operations` 可扫描已认证 floor 内的保留前缀，每个新操作调用 | 无独立 CPU 栈采样，不能宣称是实测第一瓶颈 |
| 持久化 | HTTP 成功项及回执、重开重放验证；本地 WAL 路径包含准入/append | 无 fsync/SQLite/I/O 精确分项；非真实 S3、非物理盘性能认证 |
| 索引消费 | lexical/vector 水位、可见查询验证、额外追平时间 | 两消费者与前台/后台重叠，不可简单相加或相减 |
| 切段 | 128 次 mutable native update 限制、cache 创建窗口和延迟证据 | 目录探针不提供精确旋转次数与独占成本 |
| 后台维护 | 复用生产 `record_maintenance` 命名区间及全进程 CPU | 区间嵌套可重叠；exit 也可能来自错误/取消，不能求和作总成本 |

“轻量提交成功即可搜索”必须保留唯一 WAL/持久权威、原 ID 幂等及 unknown/fence 恢复；成功前发布的最小可查视图必须同时包含正文身份、更新/删除状态、向量后补与过滤元数据。全文不能绕过向量/元数据侧的活动身份。应复用 tail，并约束同步发布的 CPU、分配及切段工作；重型构建/合并继续后台执行，明确 Busy 和背压。不要把完整重建、全局 DF 维护或第二提交日志加入成功响应路径。本 PR 不实现该能力。


## 证据复算

`evidence.tar.gz`（约 7 MiB）仅包含 JSON/CSV、逐请求 gzip、过滤后的维护日志、源码/环境指纹及失败证据，没有数据库或构建产物。`timing/`、`allocation/` 是最终 tmpfs 矩阵；`excluded-driver-v1/`、`excluded-overlay/` 是未混算的早期轮次。每目录的 `summary.json` 与逐请求记录对应，归档 `SHA256SUMS.json` 可逐文件核对。恢复结果见 `recovery.json`。

汇总器从该轮 `environment.json` 的规模、批量、版本和正反进程顺序还原完整计划，检查每个进程及四种操作恰好出现一次、样本数和二进制指纹与配置一致；缺少进程/操作、重复身份、未通过的结果或损坏分位数均拒绝，不输出部分矩阵冒充完整对照。

```bash
mkdir /tmp/write-cost-evidence
tar -xzf benchmarks/write-cost/evidence.tar.gz -C /tmp/write-cost-evidence
python benchmarks/write-cost/summarize.py /tmp/write-cost-evidence/timing
python benchmarks/write-cost/summarize.py /tmp/write-cost-evidence/allocation
```

不要对 excluded 目录调用要求全部成功的汇总器。归档 `measured-harness.py` 是实际最终测量代码；`post-measurement-resume-guard.patch` 记录此前收尾增加的 `--resume` 完整目录终态检查。本次另补完整矩阵校验及三个工具的 `python -O` 拒绝保护；没有重新测量或改写归档、CSV 及性能数字，默认测量算法未变。`driver-correction.patch` 记录最初逐项 Busy 修正。正式测量结束后，交付的恢复助手在原失败库副本上另行通过全部 720 条记录检查；若新测量发生同类补向量 durable_pending，保留 db 后可运行：

```bash
python benchmarks/write-cost/recover_failure.py   --source /absolute/failed-run --out /dev/shm/recovery-check   --binary /absolute/binaries/before
```

`--resume` 只继续未开始的计划进程，不覆盖已有 passed/failed；必须保留未完成容量的原快照及同一 epoch、二进制/参数。发现非成功写入会留存 failed 结果和数据库，不自动把它重试成成功样本。资源冲突/驱动错误停止整个驱动。每个请求前至少保留 512MiB 输出文件系统空间；TMPDIR 位于各运行目录。分配库需位于允许映射可执行段的文件系统（本次 tmpfs 上的 `.so` 链接到原构建路径）。
