# 当前架构

anyseek 由 Rust 核心库、HTTP API 服务和独立 Web 控制台组成。资源模型为 Tenant → Collection → Partition → Segment；每个 collection 一个持久 partition。公开数据入口统一为 RecordPartition；内部向量/元数据组件包含可变 tail 与冻结 Segment，重复逻辑 shard 状态已由 #111 移除。

## 模块边界

| 层 | 代码 | 职责 |
| --- | --- | --- |
| 独立 UI | `console/` | 创建/写入原请求保存与重试、搜索、分页、预览、日期过滤、服务备份恢复 |
| 默认服务 | `record_service.rs`、`server.rs`、`main.rs` | HTTP 契约、身份入口、admission、响应数据有效性保护、启动恢复 |
| 控制权威 | `catalog.rs`、`catalog/sqlite.rs`、`catalog/service_snapshot.rs` | Tenant、Key verifier、集合代次与生命周期、控制回执和完整服务快照 |
| 数据权威 | `record.rs`、`record/committed.rs` | 原始 Document/向量、稳定 ID、版本、墓碑、幂等与持久批提交 |
| 持久化 | `storage/journal.rs`、`partition.rs`、`sqlite_authority.rs`、`s3_compatible.rs` | 本地 WAL/见证，或 SQLite 唯一协调权威 + 不可变对象；owner lock、共同恢复 |
| 不可变布局 | `record/anchor.rs`、`segment.rs`、`remote_checkpoint.rs` | 规范 payload、冻结 Segment + tail、远端 manifest 发布与 GC |
| 派生索引 | `record/lexical/runtime.rs`、`component.rs`、`record/index_consumers.rs` | Tantivy 全文与独立向量/元数据消费、构建和派生包提交 |
| 检索 | `record/query.rs`、`record/lexical/runtime_query.rs`、`vector.rs`、`min_heap.rs` | 三路检索、候选、排序、遍历；Record 的稳定身份在截断前参与平分排序 |
| 活动身份 | `active.rs` | 向量/元数据记录身份、fingerprint 和活动 ordinal；全文统计由 Tantivy 固定 Searcher 管理 |
| 过滤/分词 | `metadata_filter.rs`、`tokenizer.rs` | 共享元数据判定与文本处理 |

路径相对于核心或服务的 src。`api_types.rs` 和 `api_schema.rs` 保留服务公共配置/查询 DTO 及 OpenAPI 描述；核心不依赖 HTTP schema。默认启动仅挂载 Record 服务，不保留独立的旧 HTTP/key/index 管理实现。

## 写入与查询

`commit_writes` 与 HTTP `/ingest` 在业务判定和权威持久提交后逐项返回 committed、rejected、outcome_unknown 或 not_attempted；只有 committed 带业务回执。全文与向量消费者异步推进，持久提交不保证对应索引已可查。`/doc`、`/batch` 与 Rust `write` 继续提供同步写入入口。只有一层 Record WAL，不再维护独立 Intake 日志。

查询短暂持有 Record guard 捕获派生拓扑，在 guard 外评分，完成后重验当前记录版本、fingerprint、删除及来源可见性。组件变化不依靠物理 ordinal 充当稳定身份。纯向量 collection 也能按 metadata 浏览；普通查询允许观察不同消费者的进度，提交与各索引水位分别表达。

服务根据文本/向量选择 Lexical、Vector、Hybrid。Hybrid 两路候选通过 RRF 融合，支持权重和各路预算；ANN 可能因簇探测返回不足 K。候选窗口计数和候选内字段重排不等于全库精确结果。查询原始数据经 schema.store 和 fields 投影后输出；直接读取和 iterator 可返回完整权威 Record。

## 来源和派生可见性

`record/` 中的 mutation/publication/recovery 复用同一权威提交；evidence、temporal、history、graph 模块分别处理来源依赖、时间逻辑、回执历史保留和显式一跳关系。HTTP/UI 只适配请求，不维护第二套业务权威。

来源以精确版本引用。来源删除、stale 或 TTL 使派生内容隐藏，查询、回表和输出共用可见性屏障。片段通过普通写入逐批持久化，不提供整文原子发布或整代切换；可选向量后补使用条件版本。

时间过滤固定一次 evaluation_time；不可逆过期通过权威提交形成墓碑。关系双端精确版本及一跳候选受同一来源、TTL 和当前 Tenant 集合边界约束；融合预算耗尽不伪称全量结果。

## 认证与生命周期

普通 API Key 认证后确定 Tenant，同一 Tenant 的有效 Key 可访问其全部集合。master 创建/查询 Tenant、签发/撤销 Key 和管理服务备份恢复，不进入集合数据接口。撤销拒绝后续认证，已认证工作和准备好的响应允许完成；不维护 Grant、grant revision 或 Catalog 输出授权许可。DataOutputPermit 在响应和 body frame 交付时检查来源数据有效性。

普通工作按全局快照共享屏障 → 集合共享生命周期屏障 → 短持 Catalog 元数据锁的顺序取得当前代次，在元数据锁外执行 Record future。删除/创建等生命周期变更使用对应集合独占屏障；完整服务快照保留全局独占屏障，Key 撤销不排空数据工作。分级资源准入在读 body 前计费，已接收 owned task 和传输缓冲保留额度至实际结束。

创建为持久 Prepare → 初始化 partition → Activate。删除后名称可复用，新建集合分配新内部代次；旧句柄、缓存、回执、上传、后台索引和 GC 绑定旧代次，物理回收后置。本地路径由 `collections/{tenant_id}/{collection_id}/{generation}` 直接派生，对象使用 `anyseek/{tenant_id}/{collection_id}/`，代次编码在对象 ID 中。客户端仅使用名称，删除/重建前须停止旧请求及重试。

集合恢复只占用空闲名称并分配新代次。完整服务恢复发布到新未启动根，使用独立 bucket，禁用历史普通 Key，由目标 master 为已有 Tenant 重新签发。独立备份不能证明后续没有删除，因此 typed evidence 保持隔离；无解除隔离或跨服务自动灾备入口。

## 权威与派生文件

服务根由外部稳定 owner lock 保护。Catalog 和 S3 authority 共用 `metadata.sqlite`，只存协调元数据，不存完整 Record State、正文、原始向量或索引；Catalog 不保留独立持久化日志或名称到生成目录的映射。SQLite 事务不跨对象上传或索引构建。Record checkpoint 保留配置、当前记录、墓碑、逻辑水位和操作账本。backup-*、service-backup-* 是独立快照，完整服务 manifest 和外层完成见证认证目录发布。不同独立服务使用不同 bucket，不创建自动服务 namespace，也不检查 Bucket 归属。

派生 cache-* 保存 Tantivy 原生文件与向量/元数据组件文件（component.json、vectors.bin、metadata.bin、schema.json 等）。启动加载认证 Segment 并重放权威 tail，缺失的派生缓存可重建，不把这组可变文件当作多文件原子事务。原始 Document 和未经归一化的输入向量独立保留，搜索 docstore 的投影不能替代它们。持久词项哈希使用固定 XXH64，当前低层索引格式为 8。

Linux 同步文件和父目录；Windows 以同卷无覆盖目录移动完成发布并在移动前关闭内部句柄。恢复对已认证的新状态损坏报错，不静默回退旧代或重置为空数据库。低层 commit/close 传播 I/O Result，但仍不是权威事务协议。

## 当前边界

这是有界单机实现：Record 累计账本默认 10,000 项、checkpoint/状态容量等限制见 [FEATURES](FEATURES.md)。history trim 按保留期限回收操作历史，checkpoint/对象 GC 负责各自闭包；已删除集合代次的最终物理回收也已由 #109 合入，仍受在途任务、读者和备份保护约束。P2 的 source/evidence、TTL、显式关系和同视图融合已实现。不可变段、compaction、SQLite/S3 checkpoint、安全回收、维护修复、后台 Indexer、校验缓存和分级资源准入已合入。真实 S3、系统断电、整体磁盘/缓存容量和组合资源隔离验收仍未完成；分布式未实现。原生进程故障测试不能替代主机掉电验收。整体能力与优先级建议见[项目全貌](docs/project-overview.md)，全文与认证包边界见[下节契约](#全文与认证包契约)。

[Record 语义](docs/record-model.md) · [存储协议](docs/storage.md) · [服务契约](anyseek/src/server/README.md) · [许可](LICENSE)

## 向量索引与 Segment 所有权

一个逻辑 `VectorIndex` 持有当前有界 `segments` 拓扑。可变 tail 独立持有一个可写 Segment；冻结 Segment 与新 tail 一起参与查询。没有独立逻辑 shard 数量配置或计数，组件数量直接来自固定读取拓扑的数组长度。

| 归属 | 保存内容 | 生命周期 |
| --- | --- | --- |
| RecordPartition | 规范数据、版本/删除/来源可见性、操作回执、当前 ordinal 到稳定记录身份的映射 | 权威提交与恢复 |
| VectorIndex / RetrievalView | 查询拓扑、全局候选预算与 Top-K、伴随 Tantivy runtime 的读取 pin | 捕获读取视图，旋转/合并时替换拓扑 |
| VectorSegment | 自己的向量/metadata 文件与 mmap、ANN 聚类/量化、物理行身份和活动 mask、构建内存租约 | 单组件构建、冻结、合并及最终释放 |
| Tantivy runtime | 全文 postings、固定 Searcher、原生统计及文件引用 | 全文消费与固定读者 |

`VectorIndex` 的 `meta`、schema、量化/距离/SIMD 参数是逻辑查询配置，`record_lexical` 与 `segments` 在同一读取拓扑中一一对应。Segment 中的同类配置用于本地文件解释与构建，不是独立逻辑索引的可变配置。其 `indexed_*`、`committed_doc_count`、`uncommitted`、`failed` 描述单组件构建进度；`active`、`delete_hashset` 和退休 token 是 Record 权威可见性的派生读取状态。`path`、向量及 metadata 文件/mmap、facet 字典、`block_vector_buffer`、层级偏移、维度/精度/量化范围属于物理 Segment；`build_reservation` 与 `build_input_covered` 随组件构建及引用释放。冻结后文件不可变，派生可见性 mask 仍可因 Record 更新而退休旧行。

物理地址只在捕获的拓扑内表示 `local_ordinal * segments.len() + slot`。tail 旋转、合并和恢复对当前映射整体重排；固定读者继续使用原拓扑，并通过稳定记录身份、revision/fingerprint 和退休 token 重验。拓扑位置不写入共享 Segment 的 `meta.id`，也不充当持久记录身份。

查询仍有界遍历 tail 和有效 Segment，按统一距离、稳定业务 ID 和候选预算做一次全局合并；各 Segment 内保留 ANN 聚类、SIMD 和量化。移除重复数量和位置 ID 不意味着把数据合为单个大文件，也不改变 ANN 的近似召回边界。

## 全文与认证包契约

Tantivy 使用原生词典、postings、位置和固定 Searcher，不另存完整正文，也不决定业务 head。Payload 唯一保存规范原文和原始向量；伴随向量/元数据组件只保存派生文件。S3 对象先完整物化并校验再检索，当前没有直接远端随机读 Directory。

分析身份保留 `xxh64-utf8-v1` helper；全文包记录精确 Tantivy 版本、schema 和分析契约。RecordCheckpointV16、检索 package manifest 3 与私有向量/metadata descriptor 1 是不同结构标签，不是多个提交权威。认证包选择验证完整不可变文件闭包；固定 Searcher、构建、备份和查询 pin 保护仍在使用的文件。

同步写入等待必要派生应用；异步索引写入单独报告 committed/lexical/vector 水位。查询固定派生拓扑与 evaluation_time，再重验当前 Record 身份、fingerprint 和来源可见性，不要求各异步消费者同时推进。保留 BM25F/邻近度、中文与 N-gram 适配及全局通道排名后的单次 RRF；ANN 和有界候选不保证全库精确召回。

只支持新库，无旧倒排格式读取、双写或迁移。跨 AES 手工 fixture、Windows、真实 S3 和断电证据分别以实际执行为准；重新构建派生缓存不等于修改权威格式，也不能据此推断性能提升。
