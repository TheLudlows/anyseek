# 轻量写入：持久接收、独立索引消费与查询解锁

2026-10-27 确认的新目标是轻量追加写入、异步索引与独立合并，不再要求跨全文、向量和原文的严格共同视图。

## 本批范围

打通持久化接收、独立后台物化和终态查询。Rust 入口是 `IngestPipeline`，HTTP 入口是 `/ingest`。现有 RecordPartition 暂作为物化器；`/doc`、`/batch` 和 `RecordPartition::write` 仍提供同步结果，没有悄悄改变旧接口的成功含义。

后台按最多 64 条、累计编码请求 4 MiB 聚合物化。Record 决策由物化任务推进；全文与向量/元数据分别由独立的有界消费任务推进。全文复用长期 Tantivy writer，每批一次提交、reload 与清单发布。条件拒绝先持久记录，再处理后续命令。Record 的业务 WAL 暂仍逐条提交，不将整批变成业务事务。

两个索引消费任务都不取得 Record 状态锁，水位可以互相领先。Record 物化先分配稳定的组件内行号并投递输入；向量慢时，Record 与全文仍可推进。退休掩码独立于组件读写锁，旧行和尚未消费的旧版本在查询 TopK 前被排除；序列化及封存会把掩码固化成有效成员集合。

普通全文、向量、混合查询及关系融合在检索和元数据过滤期间释放 Record 锁。查询只固定组件拓扑与文件 pin，不克隆完整 State 或所有原文；取得有界候选后短暂重新进入 Record 锁，核对当前可见位置并读取原文。并发更新、删除、checkpoint 替换可让候选被丢弃，结果可能不足请求长度；count_total 描述检索时观察的候选，visible_seq 描述返回前的原文状态，不构成严格共同视图。来源删除、撤权和故障输出屏障继续生效。

接收日志不取得 Record 锁，物化等待期间可以继续接收，直到接收预算耗尽。全文与向量/元数据通过 `(record_id, version, revision)` 关联，不再要求相同行号、组件数量或排列。引擎内部仍使用物理地址。lexical_seq 领先 vector_seq 不保证相应记录已能按元数据过滤检索；向量写入与向量查询仍共享组件锁。候选跨缓存重建时可用版本及完整索引输入指纹复验，同版本向量补丁、删除、来源失效和组件故障仍被拦截。


## API 契约

`POST /api/v1/index/{id}/ingest` 接收 `{"records":[RecordWrite,...]}`，每批 1–64 条，累计编码请求最多 4 MiB，同时受 HTTP 2 MiB body 上限和 collection 单记录限制约束。每条必须显式提供 document（null 删除）、vector_patch 或 generation。

202 返回 `state=durable`、`searchability=asynchronous` 及每条命令的 `durable_seq/operation_id`。它只表示命令已持久接收；条件版本、来源和 generation 等业务检查由后台按序执行。不能将 202 解释为条件成功或立即可搜索，接收批次也不是业务事务。

`GET /api/v1/index/{id}/ingest/operation/{epoch}/{nonce}` 返回 status：

| 字段 | 含义 |
| --- | --- |
| accepted 非空，receipt/error 为空 | 已接收，仍待处理或需要恢复 |
| receipt 非空 | Record 物化成功，含原 Record 回执；全文与向量/元数据都可能滞后 |
| error 非空 | 确定的条件冲突、无效内容或过期拒绝；终态也已持久化 |
| status 为 null | 当前流未查到，不能证明未知提交没有发生 |

发送前保存整个请求。原身份及相同内容重试返回原接收回执，不再次入队；同一身份修改内容返回 409。进度/终态查询不取得 Record/index 互斥锁。所有接口保留 collection 授权及交付时撤权检查。已授权持久接收的命令继续处理，撤权不撤销已接收命令。

`GET /api/v1/index/{id}/ingest` 返回以下进度，不取得 Record/index 锁：

| 字段 | 序号空间与含义 |
| --- | --- |
| durable_seq / completed_seq | 接收流序号；完成前缀包含确定拒绝 |
| materialized_seq | 最近成功命令的 Record 回执序号 |
| vector_seq | 向量/元数据消费任务已处理的 Record 序号，包含无索引变化的命令 |
| lexical_seq | 全文消费任务已经发布的 Record 序号，可领先或滞后 vector_seq |
| pending / retained_bytes / blocked | 待物化命令数、接收 WAL 占用、物化或持久化阻塞 |
| index_error | 派生索引消费失败，需要重新打开以重建；不撤销已持久的命令和 Record 回执 |

lexical_seq、vector_seq 是可重建缓存的内存消费水位，不是另一个持久提交头。打开时从权威 Record 日志/证书恢复索引后初始化；后台故障不能把未处理批次跳过去。搜索响应的 visible_seq 表示原文状态位置，不保证每个索引已追到该位置。

Python 客户端新增 `ingest(collection_id, requests)`、`ingest_status(collection_id, operation_id)` 和 `ingest_progress(collection_id)`；客户端不隐式重试或分配新身份。

## 日志、恢复与管理

本地接收流位于 collection 的 `intake/`。一批共享一次底层 WAL 同步提交，文件 I/O 在 blocking worker 上执行。默认段大小为 8 MiB，不小于最大批次所需空间；总预算为 collection journal 预算的八倍，稳定 control journal 预算为 1 MiB。段满后先持久创建下一段，再向 control journal 追加段引用，随后切换写入；不构建 Record checkpoint 或索引。恢复只读 control 选择的有序段链，缺段报错，未选准备段不作为最新数据。

S3 每批先上传并校验不可变命令对象，再由 intake SQLite 发布引用和提交水位。SQLite 不保存正文、向量或命令正文。intake authority 为原 authority 同级的 `*.intake.sqlite`，稳定锁为 `*.intake.owner`，对象 namespace 为原 partition namespace 加 `-intake`。这些是持久权威元数据，不能当作 collection 缓存删除。恢复从 SQLite 所选完整远端事件链读取命令与终态，不依赖本地缓存。

每个流至多一个活跃 Record 物化任务，每个可变尾段各至多一个活跃全文和向量消费任务；按批投递，不为每条命令派生 detached task。两类临时输入只复制各自所需的全文或向量/元数据字段，持有独立内存/资源额度；可变尾段的 128 次更新上限在入队时计算，两条队列各有 128 批上限。物化不持接收日志锁。Busy 在有界时间内重试原身份；未知结果或持久后应用失败保持 pending/blocked，重新打开后解析原回执。接收前预留终态日志空间，避免接收后无法记录结果。

备份、恢复、删除和接管等串行管理流程持接收管理屏障；有待处理命令或任一索引未追上 materialized_seq 时返回 Busy，有 blocked/index_error 返回 RecoveryRequired。当前保守检查全部已打开接收流，避免成功备份遗漏接收数据。guard 由 owned task 持有，不随 HTTP 等待取消提前释放。同步旧写入、尾段封存、checkpoint 和后台重建安装先排空两条索引队列；普通异步物化不等待索引批次完成。两个消费任务持有源 owner 到结束，关闭公开句柄不能提前释放 journal 锁或映射文件。向量任务在组件写锁内标记失败，正在执行及后续查询拒绝访问失败的组件；重新打开从权威日志重建。Rust 的 finish() 显式等待物化和索引消费，普通提交不调用它。

成功物化后的备份保留 Record 数据和成功回执；恢复到新 collection/服务权威时建立新 intake 流，不继承原接收序号或独立拒绝账本。本批不提供接收流迁移。

## 验证与后续

测试覆盖物化锁被持有时接收、原身份重试、确定拒绝、未知结果后恢复、段滚动、缺段/准备段、预算拒绝、S3 缓存丢失后的 pending/终态恢复、HTTP 202/409/400 和撤权输出。

本轮实施与验收状态见[实施清单](async-completion-checklist.md)。已加入稳定索引身份、有界全文段合并、接收日志快照回收和认证的过期操作账本裁剪。State/checkpoint 长度计量复用不可变共享值，退休历史用认证时间前缀表示；完整 State 驻内存和维护来源重放证明仍保留，不能把局部简化称为该协议已移除。接收日志与 Record 恢复日志仍有过渡重叠。

本批仍受 max_operations、最多 1024 条待处理命令及日志总预算限制。回收只释放已过重试/Feed 保留范围且不承担业务证据的操作；generation 身份精确拒绝复用集合仍受 max_records 限制。真实 S3 和硬断电需单独验收，不宣称无限容量或合并零成本。

## 回收与独立合并契约

接收终态持久化后，内存只保留规范化请求摘要和状态，不再持有命令正文。规范化保留原 JSON 相等比较语义。重试窗口内仍返回原接收回执；已被回收的旧身份受到持久 epoch floor 拒绝，不能在时钟回退后重新执行。durable_seq/completed_seq 是绝对序号，不因裁剪归零。状态查询为空仍不能证明旧提交未发生。

本地回收先完整写入并同步新 generation，再原子选择它，最后清理旧代；原 control 所有权锁贯穿切换。选择结果未知时阻塞接收，重开只使用持久选择的完整链。远端先写入校验过的快照分块，再由 SQLite CAS 选择，随后退休旧对象和内部事件回执。已确认删除对象的元数据可由持久 WAL/快照代次水位保护后裁剪，旧对象注册和发布不能让水位回退。未知 PUT 不能以一次 DELETE 成功证明不会晚到，因此保留其防复活和重复删除记录。

Record 维护捕获固定的退休集合，先验证来源，再把时间前缀和 epoch 水位随 checkpoint 一起认证发布。后台 catch-up 不能因后来 Feed/epoch 前进而扩大该集合。当前私有上传的来源、最新续租、到期前置回执和仍需证明的撤销保留；已关闭世代的过期控制可回收。旧 FeedTrim 事件按原规则恢复，新事件带显式策略标记，以完整提交时间证明推进保留范围，包含没有公开 Feed 点的私有命令。

独立全文合并最多选择 8 个小段、合计不超过 128 文档，按实际不可变输入大小预留资源。它不取得 Record 锁、不构建 State checkpoint，也不改变逻辑消费水位；旧 Searcher 保留原视图。失败或调用取消隔离不确定 writer，后台工作保有资源直到结束。另外，两份冻结全文/向量组件可独立合并为派生缓存：最多捕获 16384 条记录，输入预算 32 MiB、可选构建分析预算 256 MiB；锁外构建后短锁复验来源并安装。共享失效 token 保证更新、删除和来源级联继续影响旧读句柄，reader pin 保护仍在使用的文件。普通 tail 轮转不再每 128 次更新生成权威 checkpoint；合并不推进业务或索引消费水位，也不修改权威 WAL/certificate。冷恢复遇到可选合并预算不足时从已验证 State 重建私有缓存，不拒绝已提交日志。完整权威 checkpoint 仍验证来源证明。并发磁盘扫描保留旧 ledger 的逐路径高水位，避免遗漏正在写入的预收费；串行恢复/权威构建可重新扫描回收保守额度。

写入预留复用 Records 的 live TTL/Source/typed 增量计数与 generation staged 统计缓存。非持久化最早 TTL 下界跳过尚未到期的 Records 扫描；下界到达时仍完整检查，恢复重新构建并验证缓存，不能以缓存放松租约或撤权规则。

后台 checkpoint 固定物理 WAL cut 与 generation，以每段不超过 64 KiB 的持锁读取保留原句柄所有权和 cursor；帧组装、校验与来源重放在锁外执行。显式 Compact 使用 owned 后台任务，保留 State delta/payload 复用，并在冻结 membership 后锁外复用不可变原生包。完整 State 仍驻内存，初始 State/索引元数据捕获仍与规模相关；有界 mutable tail handoff、最终权威安装的部分 I/O 仍在 Record 锁内。读取字节上限不保证设备等待时长上限。

接收快照按 Spool revision 捕获 Arc<Row> 与固定 cut，编码和本地新 generation 写入在锁外进行，短锁复验后选择 CURRENT。远端在短锁内原子预登记候选全部对象及无过期 staging pins，PUT 在锁外执行，只有原 cut/head 未变才 CAS 选择；迟到候选弃选重试。未知选择保留 pins 并隔离写入，重新取得稳定 owner 且解析选中 head 后才取消遗留 build pins。旧行释放、对象退休、DELETE 和回执裁剪在锁外进行。pending 终态字节与对象预留仍需复验；未知 PUT 登记不会按时间遗忘。finish/admin 等待 owned GC，慢上传或删除不再占用 Spool 锁。

远端首次乐观候选预留下一完整快照的对象名额，该预留持续计入 Accept/Complete 的准入；若不能预留，直接进入排空模式而不先注册候选。候选首次失效后，新身份返回 Busy，原身份重试仍返回已有回执，既有 pending 可继续完成；排空后构建稳定快照，成功选择即解除门控。资源 Busy 会清除临时门控/备用名额，保留后续重试途径；unknown 仍隔离。资源可用且既有物化最终完成时，此路径避免持续新写入造成无限上传弃选；不保证外部资源或持久化故障下仍能推进。

捕获仍遍历保留行，远端预登记事务和选择时的可达性扫描仍在 Spool 锁内。本地快照仍可能反复弃选，但弃选会清理自身准备文件。普通远端 append 网络操作仍持 Spool 锁，因此期间进度查询和终态持久化需要等待；向量发布的组件写锁也保留。相关维护临界区和接收长尾必须实测，不把“异步”解释为无阻塞。

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

## 向量独立消费与查询解锁

接收与独立全文消费已通过 PR #86 合入主干 `f072eb6`。PR #87 已完成并随本批合入：向量/元数据消费任务、两索引水位互相领先、锁外退休掩码、查询拓扑 pin 与返回前校验、关系融合锁外检索，以及消费失败和取消时的 owner/文件生命周期。

专项覆盖向量暂停期间 Record/全文继续、旧版本排队后删除、消费前/消费后故障与重建、两类消费任务在关闭后的所有权保留，以及查询期间写入、候选期间更新删除、来源删除屏障。七项新纳入或补入的精确回归使 quick CI 达到 65 项。执行结果以本批 PR 记录为准；历史完整 workspace 失败仍需另行收尾。


本轮 Linux 复验：65 项精确核心回归、19 项服务端测试、3 项查询并发专项通过；接收/消费组共 15 项通过，覆盖消费前失败、追加后失败与 panic；关系融合 2 项、关系检索 4 项、全文 44 项和活动成员 1 项通过。独立核心默认/无默认特性构建、文档链接/品牌、词法覆盖和 diff 检查通过。全文组仍显式跳过历史 generation 基线失败。一次广泛 Record 回归中途终止，没有形成完整结果，不计为通过；完整 workspace 的已知失败仍未全部修复。没有真实 S3、断电及性能验收结果。


2026-10-27 远端复核：代码 `bfd81facdec73cbad073aed63f7e1a3798fd605a` 的 [PR #87 quick CI](https://github.com/TheLudlows/anyseek/actions/runs/40474609077) 已完成。static、Linux/Windows core、Linux/Windows lexical 共五项任务全部成功；full-acceptance 跳过。以上是双平台定向回归结果，不代表完整 workspace 或真实 S3/断电验收；后续状态提交只更新文档。
