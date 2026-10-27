# 轻量写入：持久接收、批量物化与独立全文消费

2026-10-27 确认的新目标是轻量追加写入、异步索引与独立合并，不再要求跨全文、向量和原文的严格共同视图。

## 本批范围

打通持久化接收、独立后台物化和终态查询。Rust 入口是 `IngestPipeline`，HTTP 入口是 `/ingest`。现有 RecordPartition 暂作为物化器；`/doc`、`/batch` 和 `RecordPartition::write` 仍提供同步结果，没有悄悄改变旧接口的成功含义。

后台按最多 64 条、累计编码请求 4 MiB 聚合物化。Record 决策与向量/元数据由物化任务推进；全文由单独的有界消费任务推进，复用长期 Tantivy writer，每批一次提交、reload 与清单发布。条件拒绝先持久记录，再处理后续命令。Record 的业务 WAL 暂仍逐条提交，不将整批变成业务事务。

全文处理不持 Record/index 互斥锁。全文暂停时，原文和向量可以继续物化；Record/向量等待时，已经入队的全文批次仍可发布。全文查询使用已发布 Searcher 的有效前缀，允许暂缺新记录，旧版本、删除和失效证据仍在 TopK 前排除。全文快照的读锁与 writer 锁分开，查询不等待全文提交锁；查询本身仍与 Record/向量共享分区锁。

接收日志不取得该锁，物化等待期间可以继续接收，直到接收预算耗尽。向量尚未从 Record 决策中拆离，仍保留物理行号映射和全量 State/checkpoint 协议；轻量合并尚未交付。

## API 契约

`POST /api/v1/index/{id}/ingest` 接收 `{"records":[RecordWrite,...]}`，每批 1–64 条，累计编码请求最多 4 MiB，同时受 HTTP 2 MiB body 上限和 collection 单记录限制约束。每条必须显式提供 document（null 删除）、vector_patch 或 generation。

202 返回 `state=durable`、`searchability=asynchronous` 及每条命令的 `durable_seq/operation_id`。它只表示命令已持久接收；条件版本、来源和 generation 等业务检查由后台按序执行。不能将 202 解释为条件成功或立即可搜索，接收批次也不是业务事务。

`GET /api/v1/index/{id}/ingest/operation/{epoch}/{nonce}` 返回 status：

| 字段 | 含义 |
| --- | --- |
| accepted 非空，receipt/error 为空 | 已接收，仍待处理或需要恢复 |
| receipt 非空 | Record 与向量/元数据物化成功，含原 Record 回执；全文可能滞后 |
| error 非空 | 确定的条件冲突、无效内容或过期拒绝；终态也已持久化 |
| status 为 null | 当前流未查到，不能证明未知提交没有发生 |

发送前保存整个请求。原身份及相同内容重试返回原接收回执，不再次入队；同一身份修改内容返回 409。进度/终态查询不取得 Record/index 互斥锁。所有接口保留 collection 授权及交付时撤权检查。已授权持久接收的命令继续处理，撤权不撤销已接收命令。

`GET /api/v1/index/{id}/ingest` 返回以下进度，不取得 Record/index 锁：

| 字段 | 序号空间与含义 |
| --- | --- |
| durable_seq / completed_seq | 接收流序号；完成前缀包含确定拒绝 |
| materialized_seq | 最近成功命令的 Record 回执序号 |
| vector_seq | Record/向量/元数据已处理的 Record 序号，包含无索引变化的命令 |
| lexical_seq | 全文消费任务已经发布的 Record 序号，可小于 vector_seq |
| pending / retained_bytes / blocked | 待物化命令数、接收 WAL 占用、物化或持久化阻塞 |
| index_error | 派生索引消费失败，需要重新打开以重建；不撤销已持久的命令和 Record 回执 |

lexical_seq、vector_seq 是可重建缓存的内存消费水位，不是另一个持久提交头。打开时从权威 Record 日志/证书恢复索引后初始化；后台故障不能把未处理批次跳过去。搜索响应的 visible_seq 表示原文状态位置，不保证每个索引已追到该位置。

Python 客户端新增 `ingest(collection_id, requests)`、`ingest_status(collection_id, operation_id)` 和 `ingest_progress(collection_id)`；客户端不隐式重试或分配新身份。

## 日志、恢复与管理

本地接收流位于 collection 的 `intake/`。一批共享一次底层 WAL 同步提交，文件 I/O 在 blocking worker 上执行。默认段大小为 8 MiB，不小于最大批次所需空间；总预算为 collection journal 预算的八倍，稳定 control journal 预算为 1 MiB。段满后先持久创建下一段，再向 control journal 追加段引用，随后切换写入；不构建 Record checkpoint 或索引。恢复只读 control 选择的有序段链，缺段报错，未选准备段不作为最新数据。

S3 每批先上传并校验不可变命令对象，再由 intake SQLite 发布引用和提交水位。SQLite 不保存正文、向量或命令正文。intake authority 为原 authority 同级的 `*.intake.sqlite`，稳定锁为 `*.intake.owner`，对象 namespace 为原 partition namespace 加 `-intake`。这些是持久权威元数据，不能当作 collection 缓存删除。恢复从 SQLite 所选完整远端事件链读取命令与终态，不依赖本地缓存。

每个流至多一个活跃物化任务，每个可变尾段至多一个活跃全文消费任务；按批投递，不为每条命令派生 detached task。全文临时输入只复制索引字段，持有独立内存/资源额度；可变尾段的 128 次更新上限在入队时计算，队列另有 128 批上限。物化不持接收日志锁。Busy 在有界时间内重试原身份；未知结果或持久后应用失败保持 pending/blocked，重新打开后解析原回执。接收前预留终态日志空间，避免接收后无法记录结果。

备份、恢复、删除和接管等串行管理流程持接收管理屏障；有待处理命令或全文未追上向量时返回 Busy，有 blocked/index_error 返回 RecoveryRequired。当前保守检查全部已打开接收流，避免成功备份遗漏接收数据。guard 由 owned task 持有，不随 HTTP 等待取消提前释放。同步旧写入、尾段封存、checkpoint 和后台重建安装先排空旧全文任务；普通异步物化不等待全文批次完成。Rust 的 finish() 显式等待物化和索引消费，普通提交不调用它。

成功物化后的备份保留 Record 数据和成功回执；恢复到新 collection/服务权威时建立新 intake 流，不继承原接收序号或独立拒绝账本。本批不提供接收流迁移。

## 验证与后续

测试覆盖物化锁被持有时接收、原身份重试、确定拒绝、未知结果后恢复、段滚动、缺段/准备段、预算拒绝、S3 缓存丢失后的 pending/终态恢复、HTTP 202/409/400 和撤权输出。

后续依次交付：向量从 Record 决策进一步拆离并解除物理行号绑定；查询读句柄与 Record/向量写锁分离；移除全量 State 证明及 checkpoint 绑定；接收日志/幂等账本持续回收；独立增量合并与文件生命周期。当前接收日志与 Record 恢复日志仍有过渡重叠。

本批仍受 max_operations、最多 1024 条待处理命令及日志总预算限制，未实现接收日志持续 GC。真实 S3 和硬断电需单独验收，没有运行大型容量基准，不宣称无限持续写入或合并零成本。

## 接收首批的历史验证记录（2026-10-27）

Linux，Rust stable 1.99.1：

| 检查 | 结果 |
| --- | --- |
| `cargo test -p anyseek --locked --features server --lib record::ingest` | 8 passed，0 failed；包含 SQLite/内存对象存储的冷恢复 |
| `cargo test -p anyseek --locked --features server --bin anyseek_server` | 19 passed，0 failed；包含两项新 HTTP/授权测试 |
| 认证归档损坏专项 | 1 passed；改为破坏证书实际选中的段，避免随机目录顺序使夹具失效 |
| `scripts/check_core_boundary.py` | 独立消费者默认、无默认特性依赖图和构建通过 |
| 文档/品牌、词法覆盖、Python 语法、diff | 通过；未移除旧测试 |

早期代码检查点的 `cargo test --workspace --locked --features server --all-targets` 在 lib 阶段停止：318 passed、12 failed、12 ignored，未完成后续全部 target。12 项中 11 项在未修改的 `b215458` 上逐项复现，涉及旧维护路径的 Busy、段身份及 generation/恢复断言；另一项归档测试原先任意选择 segment 目录，可能破坏未选中的恢复 tail，已修正夹具并专项通过。不能把该完整运行记为全绿，也不能用新增专项代替完整平台验收。

接收首批八项核心测试及新增四项独立消费/批处理测试已纳入 58 项 quick CI 清单，两项 HTTP 测试加入 Linux/Windows core lane。本轮 HEAD 的 Windows、真实云 S3 和断电结果需单独核验。

## 独立消费与批处理验证

新增专项验证：全文暂停时 64 条命令仍能完成 Record/向量物化、接收与向量查询继续；持有 Record 锁时全文仍发布；64 条记录生成一个原生索引段；更新/删除/条件拒绝保持顺序；同步旧接口继续即时可搜；批中跨过 128 次更新边界先排空再封存；全文任务失败后回执保留，重新打开从权威日志恢复索引。Linux / Rust 1.99.1 本轮结果：

| 检查 | 结果 |
| --- | --- |
| 接收日志、物化和独立消费专项 | 12 passed，0 failed |
| `scripts/ci_core_cases.py` | 58 项精确核心回归通过 |
| 全部服务端单元测试 | 19 passed，0 failed；最后的管理排空/任务退出改动后复验通过 |
| `record::lexical` 测试 | 44 passed，0 failed；显式跳过 1 项已在基线复现的 generation 失败用例 |
| 独立核心默认/无默认特性 | 依赖图与构建均通过 |
| 文档链接/品牌、词法覆盖、diff | 通过 |

跳过的测试为 `record::lexical::runtime::tests::complete_generation_is_admitted_before_rotation_and_recovers_atomically`，测试仍保留；完整 workspace 的历史失败尚未全部收尾。没有吞吐/延迟基准，不把一个批次生成一个全文段解释为已测得的吞吐提升。这些检查不替代完整平台与真实 S3/断电验收。
