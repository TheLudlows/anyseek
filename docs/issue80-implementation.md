# Issue #80：实现与验收

生产全文使用 Tantivy 0.27.0，锁定版本与新格式；公开入口统一为 `RecordPartition`。仅面向新库，不提供升级迁移、旧格式读取、双写或多引擎切换。本分支尚未合入，issue 保持开放。架构取舍见[Tantivy 设计](tantivy-evaluation.md)，当前进度见 [STATUS](../STATUS.md)。

## 实现与代码入口

| 责任 | 实现与验收关注点 |
| --- | --- |
| 全文构建、加载、执行 | [lexical](../anyseek/src/record/lexical.rs) 及其 runtime/runtime_query：原生 postings，固定 Searcher，活动统计；Record 写入→即时查询→checkpoint→重开回归证明无旧全文执行器回退 |
| 统一业务视图 | [view](../anyseek/src/record/view.rs)、[segment](../anyseek/src/record/segment.rs)：全文、向量和关系固定同一 cut/evaluation_time；更新、删除、来源/stale、TTL、generation 在 Top-K 前过滤 |
| 认证发布与恢复 | [anchor](../anyseek/src/record/anchor.rs)、[remote_checkpoint](../anyseek/src/record/remote_checkpoint.rs)：完整包闭包认证、唯一权威选择、完整 tail、发布失败封锁 |
| 独立向量/元数据 | [component](../anyseek/src/component.rs)：独立文件，不借用旧全文格式；保留量化、ANN 和过滤语义 |
| 预算及资源所有权 | [native](../anyseek/src/record/lexical/directory.rs)、[package pins](../anyseek/src/record/package_pins.rs)：writer/读取/构建准入，owned worker 与固定 Searcher 保护文件，取消后实际工作结束才退额 |
| 业务与输出安全 | [record](../anyseek/src/record.rs)、[catalog](../anyseek/src/catalog.rs)：durable + visible、版本/幂等、原 operation_id 重试、即时撤权和输出屏障 |

新格式为 RecordCheckpointV14 / manifest 3 / native package 1。不可变包包含原生 Tantivy 文件及独立 vectors.bin、metadata.bin、component.json、schema.json；原始正文与原始向量唯一归规范 Payload。SQLite 保存必要协调元数据，不保存第二份正文、向量或倒排。Tantivy commit 只产生候选快照，不能成为第二业务 head。

原生写入使用 NoMergePolicy；writer/worker、下载、目录读写与输出有显式准入。tail 在 128 次原生更新边界前轮换，generation 按整个业务操作预留，不拆分业务提交；冻结组件上限触发认证 compaction。固定 Searcher、mmap、后台构建与备份持有包保护，最后持有者退出前不得回收。预算是逻辑预留，不是总 RSS 硬上限。

## 查询契约与已批准修正

保留实际启用的 BM25F、Bm25fProximity、N-gram（含 mask 9）、中文/归一化/短语/字段长度契约。评分使用跨冻结段和 tail 的活动 N、df、字段长度，不切换到默认字段加权 BM25。

限定 body 的 OR 查询已统一：body="alpha"、title="beta"，只搜 body 的 "alpha beta" 在提交前后均命中；原即时路径漏召回已修复，评分公式未变。回归包含计数和分页。

Record 默认候选窗口为每通道全局 `offset + length`，显式候选 cap 也在全局生效，再做一次 RRF。全局排名后才做候选内字段排序和分页，稳定 ID 处理平分。混合计数保持既有逐组件 `max(lexical_count, vector_count)` 求和契约，不称为精确去重总数。ANN 和有限候选窗口不保证全库精确融合召回；`relation_complete` 只描述邻接遍历。

实施中曾把已删除低层 API 的窗口行为误用于 Record（`79017aa`）；已在 `3f09fe76` 恢复主干契约。原布局不变性断言未修改，一/三组件独立 RRF oracle、显式 cap、同窗口分页均通过。该错误版本的服务结果不作为最终依据。

## 清理范围

已删除旧全文倒排读写、docstore、执行器、公开低层 Index/Search/Commit 入口与废弃直接依赖；有效外部测试迁到 Record。保留向量、分词、过滤和数据安全功能。

发布前复核进一步删除私有 Search trait 的纯转发、未使用的 query_type/empty 参数、不可达的 Hybrid/RRF 分支和 RetrievalView 的两个无效布尔参数。查询语法的 7 个 doctest 移到公开 QueryType 文档，未减少该覆盖。未注册的 posting_buffer_tests.rs 引用已删除旧 API，只验证废弃缓冲格式，已删除；新引擎的增长/轮换/恢复覆盖保留。还删除无调用的旧 facet 取值入口/FacetValue、旧游标二进制读写与复制工具、read_min_max、validate_document_vectors 转发及旧位置上限常量；Record 使用的过滤、向量校验和独立组件读取继续保留。没有删除预算、pin、授权或认证检查。

阶段流水账合并为当前文档，设计、使用、状态和路线各有单一入口；旧过程保留在 Git 历史。

## 验证版本与结果

### 清理前完整验收（历史证据）

生产及测试代码 `3f09fe76fb7b9fd1eb17ae3381facf66a27bf234`；`6bbc244` 仅更新 Markdown。原代码基线 master `3893cd54`，发布前并入 master `31a4d059` 的文档更新。

| 检查 | 结果 |
| --- | --- |
| all-targets，server + fault-injection | 34 binaries，338 passed / 0 failed / 13 ignored，exit 0，3443.81 s |
| no-default library | 197 passed / 0 failed / 10 ignored，exit 0 |
| default / no-default doctests | 各 9 passed / 0 failed，exit 0 |
| 原 3000 条写入及大词法夹具 | 分别 133.53 s、2250.63 s；规模和两种长度策略保持 |
| HTTP faults / Indexer / authorization | 均 exit 0；健康恢复默认仍为 15 s，未放宽超时 |
| S3 storage fixture | exit 0：checkpoint、保留、备份、接管及故障边界 |
| S3 无缓存语义 | base、locator、graph、temporal、upload、lease、feed 七组全部 exit 0 |
| S3 object contract fixture | 1 passed；未使用真实 endpoint |
| 独立 core 默认/无默认依赖边界 | 通过 |

证据为工作环境 `/tmp/anyseek80-phase18-results.json` 和对应 phase18 日志、S3 JSON 报告。服务二进制 SHA-256：`f43acdcd2ff2fdeff1bf3653dd67ad8b83f6752ad510b38c1afbb823382e6c51`；三个 S3 报告均明确 backend=fixture、real_endpoint_exercised=false。这些结果不冒充本轮私有查询清理后的验证。

### 本轮发布前清理

本轮最终代码的全目标编译通过；核心 + stable_ranking / weighted_rrf / test / server_query_contract / server_shared_filters / server_ann_contract 共 265 passed / 0 failed / 12 ignored；no-default lib 为 197 passed / 0 failed / 10 ignored；默认和无默认 doctest 各 9 passed。所有命令 exit 0。文档链接检查为 22 Markdown / 103 surfaces 通过，git diff 空白检查通过；全仓 rustfmt 仍 exit 1，沿用下述基线问题。证据：工作环境 `/tmp/anyseek80-phase20-final-results.json` 及各步骤日志。完整 all-targets 执行、HTTP/S3 和 Linux/Windows 平台矩阵由本轮最终提交的 PR CI 重新验收，不借用旧版本通过替代。

### 平台 CI 与 Windows 任务边界

[`199cdd0` 的 CI](https://github.com/TheLudlows/anyseek/actions/runs/40054343256) 为 4 个 job 成功、Windows Persistence 因 120 分钟上限取消。Linux 全目标 339 passed / 0 failed / 13 ignored，no-default 198 passed / 10 ignored，两组 doctest 各 9 passed，HTTP 与存储故障矩阵通过；两平台 S3 fixture 通过。Windows 已通过原 3000 条写入（920.99 s）和跨平台预算回归，但后续 feature、doctest、HTTP/存储矩阵未执行，不算 Windows 完整通过。

超时时词法大用例仍未返回，旧日志没有内部水位，不能据此断言挂死。Linux 两个大用例各需约 22.5 分钟，均在一个测试内串行执行 Shared/PerField。现在将它们拆成四个独立 Windows job，每项仍写入 8194 条并保留全部原断言；两个小用例仍各自遍历两种策略。常规 Windows job 仅排除这四项，继续执行其他所有验收；Linux 仍跑完整集合。每个 job 保持 120 分钟。`check_lexical_partition.py` 对比实际编译清单、排除集合和 matrix，防止漏测或名称错误导致零测试成功。

测试新增每 256 条写入、checkpoint、重开和查询前后的耗时水位；独立 job 使用 `--nocapture`。这是验收任务拆分与诊断，没有改变生产执行、语料、评分、断言或超时。最新提交的结果以 [PR 检查](https://github.com/TheLudlows/anyseek/pull/81/checks) 为准；上述通过项绑定旧 SHA，不能替代新 SHA 实测。

### 13 个忽略项

11 个进程 worker 由父测试显式启动，不是遗漏执行：

- catalog_revoke_crash_worker
- background_index_crash_worker
- record_cancelled_query_worker、record_cancelled_recovery_worker
- record_checkpoint_crash_worker、record_crash_worker
- journal_crash_worker、journal_initialization_worker、journal_recovery_worker
- partition_creation_owner_worker
- cache_crash_worker

另外两个是 `real_s3_compatible_contract`（由 fixture 合同工具显式运行，不能视为真实 S3）和 `external_persistent_term_hash::cross_build_same_index`（需 TERM_HASH_FIXTURE/MODE 与相反 AES CPU 配置的手工交叉验证，尚未运行）。

### 已知边界

全仓 `cargo fmt --all -- --check` 在清理前和独立原始主干均 exit 1；现有紧凑风格造成大范围格式差异，未为本任务重排全仓，不能报告为通过。编译 warnings 仍需区分遗留与本轮新增。

2026-10-23 调试词法测试进程样本：RSS 503048 KiB、峰值 503360 KiB、10 线程、27 文件描述符（phase18-resource-sample.json）。不是 benchmark 或 RSS 限额。每次即时写入的 commit/reload、tail 轮换、包 hash/物化、活动 postings 统计均有维护成本；完整 Record State/回执账本仍驻内存，不声称速度或内存收益。

当前分支 Windows 待 CI；真实 S3、系统/文件系统崩溃、硬断电和手工跨 AES 尚未验收。进程退出与 fixture 不替代这些验证。核心剩余及建议顺序见[路线](roadmap.md)，本轮不扩展实现下一阶段。
