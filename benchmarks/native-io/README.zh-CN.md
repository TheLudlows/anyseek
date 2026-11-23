# 原生目录 I/O 隔离

**简体中文** · [English](README.md)

PR #203 基线为 `014e66a8570685667d60197f543845a399186915`，工作前重新拉取远端 head。Master 为 `3815f230bee864feedd53ccbed9e6ecc4a4326a3`。初始工作区干净。候选源码摘要及工具链见 [results/provenance.json](results/provenance.json)，收录该文件的提交标识最终候选。

## 决策与范围

基线在原生目录读取、写入、原子发布、文件终止和目录同步期间持有进程级 mutex。因此即便资源额度允许，一个集合的慢同步仍可阻塞与之无关的索引。生产改动前的基线测量证实了这个机制。

候选仅在同一**物理索引目录**的句柄间共享 mutex，各索引不再共用进程锁。全局弱引用注册表只在打开目录时短时间访问，文件系统操作和容量等待均不在注册表 mutex 内执行。Directory/Writer 克隆和固定读者的所有者令牌通过已有 `Arc<Budget>` 保有门控。后续打开会清理失效项。内存预留、取消、WAL 顺序、OS 元数据与写者锁、owner 文件以及包 pin 维持原行为。

Unix 解析路径后按 `(device, inode)` 标识目录，覆盖相对路径、点路径、符号链接和同目录别名。其他平台使用 canonical 路径。已有生命周期所有权禁止从外部替换使用中的目录，这不增加对外部文件系统改写的保证。Windows 别名和原生测试未在本地验证。Unix 直接测试重命名后的身份，使用中的 MmapDirectory 未承诺支持外部重命名。

## 实验

**用于诊断文件系统 I/O，未测端到端集合性能。** 一个或四个独立目录各执行 200 次操作。每次计入 4 KiB，取得真实 Budget I/O 门控，执行 Tantivy 原子写（临时文件、写入、`fdatasync`、重命名），再同步目录。计时后逐条读回并验证正文。无人工 sleep。为隔离门控，诊断不使用容量准入，生产中的全服务及单主体 I/O 上限不变。

诊断为 `cfg(test)` 且默认 ignored。基线二进制新增诊断模块，生产代码不变。均使用相同未优化 test profile、默认特性、Rust 1.99.1 和仓库 CPU flags。Linux 容器配额 4 CPU 和 16 GiB 内存，工作区使用 overlayfs。临时数据放于 `/workspace` 的证据目录，未使用内存文件系统。先构建二进制，再运行实验。单目录和四目录各执行五对，交替基线/候选和候选/基线。测量期间没有启动并行构建或测试。无法排除宿主级干扰，本结果不作生产稳定性验收。

单位为毫秒，取**五个进程各自指标的中位数**。这里不混合百分位，各组成项中位数未必可相加。候选的持锁和 I/O 总量跨线程有重叠。

| 4 个目录，800 次操作 | 进程级基线 | 按目录候选 |
| --- | ---: | ---: |
| 墙钟时间 | 1305.828 | 595.509 |
| 墙钟范围 | 1137.280–1819.164 | 566.863–660.417 |
| 门控等待总时间 | 2998.076 | 1.476 |
| 门控等待 P99 | 98.677147 | 0.022718 |
| 持有门控总时间 | 1280.306 | 2247.914 |
| 原子写（含文件同步）总时间 | 877.823 | 1501.940 |
| 目录同步（含打开关闭）总时间 | 402.483 | 758.092 |

单目录墙钟中位数为基线 284.957 ms、候选 286.235 ms（范围分别为 259.858–378.104 和 262.560–366.700 ms）。未取得单目录提速证据。

用 `strace -f -w -c -e trace=fsync,fdatasync` 单独测纯同步系统调用耗时。两个进程各执行 1,600 次成功 `fdatasync`，零错误。累计墙钟基线 0.980276 s、候选 1.637560 s。`-w` 确保统计墙钟时间，普通 strace 汇总是 CPU 时间。跟踪扰动调度，所以**不纳入**无跟踪对照。目录同步含打开与关闭，原子写计时含发布，二者都不是纯文件同步开销。

结果支持移除无关目录之间的 mutex 等待，也表明并发下每次操作面临更高的存储竞争。它未证明 HTTP 吞吐、Busy 率、索引追平或服务 P99 改善，也未解决历史四集合 C8 退化。此前不稳定的增量账本结果不作本候选证据。Release、真实并发集合负载、Windows 与代表性持久盘仍需各自性能验收。

## 复现

使用独立源码和构建目录，保留现有工作。基线 SHA 仅从候选复制 `directory_diagnostic.rs` 到 `anyseek/src/record/lexical/`，然后向基线 `directory.rs` 追加以下内容。

```rust
#[cfg(test)]
#[path = "directory_diagnostic.rs"]
mod diagnostic;
```

各自构建。在下一次构建前，按 Cargo 输出复制可执行文件到独立的基线或候选路径。

```sh
CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
  cargo test --locked -p anyseek --lib --no-run -j 4
python3 benchmarks/native-io/compare.py /absolute/baseline-test /absolute/candidate-test /workspace/new-results
```

输出目录不能已存在。先停止其他构建、服务或性能实验。工具保存 20 轮无跟踪、两轮独立跟踪及二进制哈希。[results](results/) 保留去掉尾部空行的日志、同步摘要、哈希与聚合 JSON。可移植脚本使用与本地相同的交替命令顺序。绝对运行路径和构建、测试日志在 `/workspace/io-evidence`。

## 本地验证

`results/validation.json` 记录命令、退出码和耗时。原始日志保留在 `/workspace/io-evidence/validation-*.log`。`scripts/ci_core_cases.json` 涵盖两个 CI 操作系统上的跨目录推进及同目录共享新用例。Unix 别名/身份测试亦纳入本地目录选择。

最终本地结果（Linux，小规模夹具）。

- 目录测试 **9 项通过**，core 聚合 **203 项通过**，故障注入写入聚合 **46 项通过**。选集重叠，不能相加为唯一测试数。
- 六个删除/词法集成目标 **15 项通过**，零失败或忽略。
- 默认和无默认特性的 `cargo check --all-targets` 通过。
- 控制台边界、退役依赖、词法分区及 S3 验收运行器单测通过。最后一项是运行器测试，不是真实 S3 验收。
- 默认服务构建与完整常规 `record_service_smoke.py` 通过，包含生命周期、备份、恢复、重启、ingest、时间条件与重试。
- 未修改的基线和最终候选默认 `cargo clippy --all-targets` 都因 `anyseek/src/resources/lifecycle.rs:146,156` 既有的**相同两处** `clippy::async_yields_async` 报错失败。该源码字节一致。保留错误摘录，完整日志在 `/workspace/io-evidence/{baseline-clippy,validation-clippy}.log`。
- `cargo clippy --locked -p anyseek --all-targets -- -A clippy::async_yields_async` 通过并保留原警告。例外仅适用该 lint，默认 lint 仍失败。两版本的库测试构建各产生 246 条编译警告。
- 基准脚本语法和 `git diff --check` 通过。

未执行手工 Linux/Windows 完整矩阵、Windows 原生别名、浏览器、真实 S3、硬断电恢复和端到端 Release 性能验收。本地变更不证明这些合入与部署条件满足。当次工作不包含远端推送、PR 更新、合入或部署。
