# 当前架构

本文按当前主干 `3e0a9a4`（#203）核对模块和运行边界。业务不变量见[Record 契约](record-model.md)，持久格式与恢复见[存储协议](storage.md)，验收缺口和性能证据见[路线](roadmap.md)。

AnySeek 是通过 HTTP API 和独立 Web 控制台使用的 Rust 存储与检索服务。资源模型为 Tenant → Collection → Partition → Segment；每个 collection 一个持久 partition，数据请求统一交给 RecordPartition。

## 模块边界

以下路径均相对 `anyseek/src/`，独立 UI 位于仓库根的 `console/`。

| 层 | 代码 | 职责 |
| --- | --- | --- |
| HTTP 服务 | `server/record_service.rs`、`server/server.rs`、`server/main.rs` | 认证、准入、请求与响应、启动恢复和关闭 |
| 控制权威 | `catalog.rs`、`catalog/sqlite.rs`、`catalog/service_snapshot.rs` | Tenant、Key verifier、集合代次、管理回执和服务快照 |
| 数据权威 | `record.rs`、`record/committed.rs`、`record/write.rs` | 原文/原始向量、稳定身份、版本、墓碑、幂等和持久批提交 |
| 持久化 | `storage/journal.rs`、`storage/partition.rs`、`storage/sqlite_authority.rs`、`storage/s3_compatible.rs` | 本地 WAL，或 SQLite 协调权威与不可变对象；owner lock 与恢复 |
| 认证布局 | `record/anchor.rs`、`record/segment.rs`、`record/remote_checkpoint.rs` | payload、冻结 Segment、manifest、checkpoint 与 GC |
| 全文派生 | `record/lexical/runtime.rs`、`runtime/live.rs`、`memory.rs` | Tantivy 固定 Searcher、可查尾段、内存预留与后台物化 |
| 向量/元数据派生 | `component.rs`、`component/memory.rs`、`record/index_consumers.rs` | 组件文件、活动身份、消费进度与内存租约转移 |
| 检索 | `record/query.rs`、`record/lexical/runtime_query.rs`、`vector.rs`、`min_heap.rs` | 三路查询、全局候选预算、排序与身份重验 |
| 过滤与分析 | `active.rs`、`metadata_filter.rs`、`tokenizer.rs` | 活动身份、共享元数据谓词、分词与归一化 |

`server/api_types.rs` 与 `server/api_schema.rs` 定义 HTTP DTO 和 OpenAPI。Rust 服务不嵌入前端资源；默认构建包含 HTTP 与本地/S3 存储。

## 写入与查询

`/batch` 是唯一同步 HTTP 写入入口，单条内部使用 `write`，多条使用 `write_batch` 共享持久提交与派生发布；成功项表示 durable + visible，批次不是事务。`/ingest` 对应 `commit_writes`，只确认持久业务结果，全文和向量消费者独立推进。只有一层 Record WAL，具体结果与重试规则见[Record 契约](record-model.md#身份版本与重试)。

普通写在 WAL 前预留规范值、容器、全文发布工作区和向量/元数据组件额度。PreparedBatch 绑定实际 writer/Inner 锁身份；规划只保留本批成功前缀和目标记录，恢复与维护继续完整校验。完整证明见[普通写入的提交基准](record-model.md#普通写入的提交基准)。

### WAL 支撑的可查全文尾段

全文读取视图由原生固定 Searcher 与有界、不可变的已分析增量组成。满足尾段准入的写入在权威持久化后发布增量，使同步写可在原生文件物化完成前参与查询。增量保留词项位置、字段长度和精确记录身份，按同一评分、过滤和排序规则与原生基底一起检索；它没有独立的业务提交权威。

后台物化持有原生 writer、输入和预留额度，在前台发布锁外写入 Tantivy 文件，再原子交接原生快照与剩余增量。固定读者继续持有旧快照和对应租约。尾段受 128 行与 64 MiB 显式缓冲预算约束，已被读者固定的增量仍占额度；压力在 WAL 前形成背压。超过可查增量路径适用大小的记录使用同样预留工作区的原生发布路径，不承诺所有写入都绕过文件 I/O。

异步消费者发布的增量也可早于原生物化被查询，但 `/ingest` 返回本身仍不保证索引已消费到该提交。物化失败保持搜索封锁，已持久回执通过重开与 WAL 重放恢复。checkpoint、切段及关闭在需要时等待物化，不以尾段封存替换权威 checkpoint。

### 查询视图与锁范围

查询在短持 Record guard 时捕获派生拓扑与 evaluation_time，在 guard 外评分，再重验当前 Record 的版本、fingerprint、删除及 TTL。组件 ordinal 仅是地址。消费者进度可不同，普通查询不提供跨通道、回表和统计的全局不可变业务快照；更新或到期可能缩短页。

纯全文 Count、默认排序及只按 `_id`/`_score` 排序的路径在捕获身份与全文快照后释放向量/元数据组件读锁；Hybrid 和字段排序仍保留相应组件锁。正文在评分与身份重验后按输出字段复制。每次查询将搜索执行交给一个阻塞 worker，最终身份与 TTL 检查仍面对当前权威状态。

Lexical、Vector、Hybrid 共用稳定 Record 身份。Hybrid 在各路跨组件形成全局排名后执行一次 RRF；候选窗口计数与 ANN/Hybrid 候选内字段排序不保证全库精确结果。纯向量 collection 也支持 metadata 浏览。配置、评分和排序细节见[检索契约](search.md)。

## Segment 所有权与文件保护

一个逻辑 VectorIndex 持有有界 `segments` 拓扑：一个可变 tail 与若干冻结 Segment。每个 RetrievalSegment 配对持有全文 runtime 和向量/元数据组件，不另设逻辑 shard 数量或持久位置 ID。

| 归属 | 保存内容 | 生命周期 |
| --- | --- | --- |
| RecordPartition | 规范数据、版本/TTL、回执、ordinal 到稳定身份映射 | 权威提交与恢复 |
| VectorIndex / RetrievalView | 拓扑、全局 Top-K/候选预算、读取 pin | 捕获视图，旋转/合并时替换拓扑 |
| VectorSegment | 文件/mmap、ANN 聚类与量化、物理行身份、活动 mask、组件 admission | 单组件构建、冻结、合并及最终释放 |
| Tantivy runtime | 原生 Searcher、可查增量、原生文件、writer 和目录 owner | 全文发布、物化与固定读者 |

VectorIndex 的 schema、距离和量化配置是逻辑查询配置；组件中的配置用于解释本地文件。`indexed_*`、`committed_doc_count`、`uncommitted`、`failed` 描述组件进度，`active`、`delete_hashset` 与退休 token 表达派生活动状态。组件 admission 汇总已持有和待转移的租约；切段关闭旧代次的新准入，已获额度的工作可按原绑定完成。

物理地址仅在捕获拓扑内表示 `local_ordinal * segments.len() + slot`。旋转、合并或恢复会重排当前映射；固定读者使用原拓扑，并以稳定身份、revision/fingerprint 和退休 token 重验。拓扑位置不写入共享 Segment 的 `meta.id`。

原文和原始向量由规范 payload 保存；全文包记录精确 Tantivy 版本、schema 与分析契约，不另存权威正文。S3 文件先完整物化并校验，没有直接远端随机读 Directory。认证包验证完整不可变闭包，读者、原生 worker、构建和备份在释放前保护文件及 partition owner。格式标签、checkpoint 和回收协议统一见[存储协议](storage.md#本地-segment-与共同视图)。

### 原生目录 I/O 隔离

`record/lexical/directory_gate.rs` 按同一物理索引目录共享 I/O mutex，不再让不同目录共用进程级文件操作锁。Unix 以 canonical 路径对应的 device/inode 为键；其他平台使用 canonical 路径。全局 registry 只存弱引用，在打开目录时短持锁，不在该锁内做文件 I/O 或等待容量。

Directory/Writer 克隆与固定读者通过原有 owner 保留目录锁。内存、服务级/集合级 I/O 额度、OS writer/owner lock、取消和包 pin 继续生效；不允许外部替换仍在使用的目录。该改动消除了无关目录间的 mutex 等待，端到端吞吐与 Windows 别名行为仍需独立验收，见[原生 I/O 证据](../benchmarks/native-io/README.md)。

## 认证与生命周期

普通 API Key 认证得到 Tenant，同租户有效 Key 的集合访问范围一致；master 管理 Tenant、凭据和服务备份恢复。撤销只拒绝后续认证，已认证工作及准备好的响应可完成。DataOutputPermit 在响应和 body frame 交付时检查数据失败 fence。

锁顺序为全局快照屏障 → 集合生命周期屏障 → 短持 Catalog 元数据锁，Record future 在元数据锁外执行。创建、激活、删除使用集合独占屏障，完整服务快照使用全局独占屏障。已接收的 owned task、输出缓冲与子任务持有额度至实际结束，取消调用者不会提前释放所有权。

创建遵循持久 Prepare → 初始化 partition → Activate。删除后同名重建分配新内部代次；旧句柄、回执、上传与 GC 绑定旧代次。名称、认证及恢复 scope 的业务边界见[Record 契约](record-model.md#授权与恢复)，物理回收见[存储协议](storage.md#已删除集合代次的最终回收)。

## 故障边界

生产 release 使用 `panic = "abort"`，Rust panic、原生崩溃或 allocation abort 会终止进程，由 supervisor 重启并重放持久权威。`catch_unwind` 仅防御 unwind 构建，不提供生产任务隔离。内存队列准入不等于持久化；断连请求仍须使用原操作身份与完整请求解决未知结果。

| 故障 | 查询与权威边界 | 恢复 |
| --- | --- | --- |
| 全文消费者或物化失败 | 所有搜索通道返回 RecoveryRequired；健康权威中的记录与已提交回执仍有效，失败消费水位不推进 | 重开并重建派生缓存 |
| 向量消费者或 metadata 组件部分修改失败 | 所有搜索依赖活动身份和过滤，失败组件保持封锁；不能作为规范数据读取 | 重开；不支持独立 metadata 在线修复 |
| 权威发布未知或不健康 | 读写失败封锁，响应缺席不证明未提交 | 恢复权威后再重建缓存 |
| 进程 panic | API 断连，仅持久权威可恢复 | 重启、重开、原身份查账或重试 |

独立消费不等于独立故障隔离。不得手工推进 cursor、清空共享错误槽以开放部分结果、删除在用缓存或复用失败 writer；恢复失败的 partition 必须保持不可用。回归覆盖消费者错误、metadata mmap 增长失败、部分向量提交、权威部分帧及重开，执行结果以对应源码的验收记录为准。

阻塞 worker 持有查询 admission、资源 scope、句柄和 package pin，直到工作 join，客户端取消不撤回这些资源。checkpoint 的 journal 安装也把 partition/writer guard 与 I/O 租约移交给阻塞 worker，仍持发布锁；移出异步 executor 不代表消除发布暂停或已证明吞吐提升。

## 服务关闭

SIGINT/SIGTERM 先建立停止准入边界，再启动 HTTP graceful shutdown。此前接收的请求及必要子任务可完成；后续请求在读 body 前收到 HTTP 503、`code: shutting_down`。HTTP/1 关闭连接，HTTP/2 不发送 Connection 头。

关闭顺序如下：

1. join 周期维护协调器，停止新的自动 checkpoint/cache 维护与未获准入的重试；已接受消费者完成原有有界 flush/merge。
2. 排空 HTTP、索引和阻塞 executor 中的 owned activity，包括取消后的任务、body 丢弃后仍持有的 frame，以及尚未交付的任务结果。仅保存状态的已完成 handle 不算在途工作。
3. join 索引消费者、全文物化和原生 writer，释放 partition、读者、package pin、mmap 与 Catalog。
4. 最后释放服务根锁，仅完整成功后报告 `Shutdown complete`，此时才可立即重开或回收目录。

关闭不强制 checkpoint、TTL sweep 或 GC，也不增加持久性承诺。传输 owner 未释放会让关闭继续等待；重复调用共用同一完成状态。消费者健康错误可通过重开恢复；worker join、原生 writer 终止失败或遗留 partition owner 会报告关闭未完成并保留根锁至进程退出，可执行程序返回非零。

取消或超时等待者不会取消协调器，也不表示已关闭；可再次等待同一完成 handle。可执行程序没有强制排空期限，外部 hard kill 按崩溃恢复处理。HTTP Tokio runtime 必须保持运行，直到 `Shutdown::wait()` 返回 `Ok(())`；停止通知、HTTP server 返回或 `Err` 都不允许提前重开。丢弃外部完成 handle 不会取消仍在运行的协调器。

`finish_service_shutdown()` 是内部排空后屏障，调用者须已停止生产者并释放 partition 克隆，不能用它关闭任意库实例。服务仍采用单 owner、共享进程 executor 的模型。

### 资源所有权与配置

| 资源 | 作用域与配置时机 | 关闭行为 |
| --- | --- | --- |
| HTTP Tokio executor | 可执行程序所有，运行至关闭成功 | 成功前不能销毁 runtime |
| INDEX_RUNTIME、维护 WORKERS | 进程级 LazyLock runtime 与双 worker semaphore | join 本服务工作，不重置共享 executor 或等待无关 owner |
| 构建 MEMORY / MEMORY_LIMITS | ANYSEEK_BUILD_MEMORY_BYTES、ANYSEEK_BUILD_SHARED_MEMORY_BYTES 由 LazyLock 解析一次，错误配置也保留 | 释放实际租约；重开不重读或重置预算 |
| 服务 Governor | 每次 open 由 Governor::from_env() 捕获 lane 配置 | 释放任务、缓冲和 I/O 租约；独立于进程构建预算 |

环境配置在启动前设置，运行时修改环境不属于支持的重配置。显式额度不覆盖全部分配，也不提供进程 RSS 硬上限；可独立配置 runtime/预算的嵌入式多实例隔离仍未实现。

关闭回归保留拒绝新准入、取消与等待重试、frame/任务所有权、立即重开和失败保留根锁等场景。Linux 另跑真实 SIGTERM 冒烟；Windows 运行进程内所有权与目录回收回归，不能据此推定 POSIX 信号或系统断电通过。运行命令和 CI 分工见[开发指南](development.md)。
