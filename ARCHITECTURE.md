# 当前架构

anyseek 由 Rust 核心库和 HTTP/Web UI 服务组成。默认服务已接入 Record/catalog，每个 collection 一个持久 partition 和一个内部搜索 shard。低层索引库仍保留自身 API 和本机多 shard 实现，不代表服务已支持分布式分片。

## 模块边界

| 层 | 代码 | 职责 |
| --- | --- | --- |
| UI | `anyseek/web/` | 创建/写入原请求保存与重试、搜索、分页、预览、日期过滤、服务备份恢复 |
| 默认服务 | `record_service.rs`、`server.rs`、`main.rs` | HTTP 契约、身份入口、admission、响应数据有效性保护、启动恢复 |
| 控制权威 | `catalog.rs`、`catalog/sqlite.rs`、`catalog/service_snapshot.rs` | Tenant、Key verifier、集合代次与生命周期、控制回执和完整服务快照 |
| 数据权威 | `record.rs` | 原始 Document/向量、稳定 ID、版本、墓碑、幂等、共同可见视图 |
| 持久化 | `storage/journal.rs`、`partition.rs`、`sqlite_authority.rs`、`s3_compatible.rs` | 本地 WAL/见证，或 SQLite 唯一协调权威 + 不可变对象；owner lock、共同恢复 |
| 不可变布局 | `record/anchor.rs`、`segment.rs`、`remote_checkpoint.rs` | 规范 payload、冻结 Segment + tail、远端 manifest 发布与 GC |
| 派生索引 | `index.rs`、`commit.rs`、`doc_store.rs` | 现有倒排、向量、文档投影和 metadata 存储的构建/提交 |
| 检索 | `search.rs`、`vector.rs`、`iterator.rs`、`min_heap.rs` | 三路检索、候选、排序、遍历；Record 的稳定身份在截断前参与平分排序 |
| 活动统计 | `active.rs`、`field_length.rs` | 活动版本 N/df/字段长度；与物理编码计数分离 |
| 过滤/分词 | `metadata_filter.rs`、`tokenizer.rs` | 共享元数据判定与文本处理 |

路径相对于核心或服务的 src。`api_types.rs` 和 `api_schema.rs` 保留服务公共配置/查询 DTO 及 OpenAPI 描述；核心不依赖 HTTP schema。默认启动仅挂载 Record 服务，不保留独立的旧 HTTP/key/index 管理实现。

## 写入与查询

写入先校验身份、条件版本、向量、记录和完整快照容量，再同步权威操作日志；随后增量应用搜索索引、撤销旧活动统计、发布共同序列，最后返回成功。索引应用失败返回 durable_pending 并封锁该实例；日志结果不确定返回 outcome_unknown，需要恢复和原 ID 重试。已接管工作不随 HTTP 请求取消而中断。

Record partition 的 mutex 覆盖候选检索和原始数据回表。同一个查询只看到一个发布位置；替换/删除在 Top-K 前过滤旧版本。空查询使用同一删除检查、metadata 过滤和候选堆，纯向量 collection 不依赖词法字段也能浏览。

服务根据文本/向量选择 Lexical、Vector、Hybrid。Hybrid 两路候选通过 RRF 融合，支持权重和各路预算；ANN 可能因簇探测返回不足 K。候选窗口计数和候选内字段重排不等于全库精确结果。查询原始数据经 schema.store 和 fields 投影后输出；直接读取和 iterator 可返回完整权威 Record。

## 来源、整文和派生可见性

`record/` 中的 mutation/publication/recovery 复用同一权威提交；evidence、generation、temporal、history、graph 模块分别处理来源依赖、整代生命周期、时间逻辑、回执历史保留和显式一跳关系。HTTP/UI 只适配请求，不维护第二套业务权威。

来源以精确版本引用。来源删除、stale 或 TTL 使派生内容隐藏，查询、回表和输出共用可见性屏障。generation Begin/StageBatch 仅持久私有成员，Seal 验证完整清单，Publish 原子切换整代及索引活动统计；可选向量后补使用条件版本。租约到期清理私有正文但保留幂等摘要回执。

时间过滤固定一次 evaluation_time；不可逆过期通过权威提交形成墓碑。关系双端精确版本及一跳候选受同一来源、generation、TTL 和当前 Tenant 集合边界约束；融合预算耗尽不伪称全量结果。

## 认证与生命周期

普通 API Key 认证后确定 Tenant，同一 Tenant 的有效 Key 可访问其全部集合。master 创建/查询 Tenant、签发/撤销 Key 和管理服务备份恢复，不进入集合数据接口。撤销拒绝后续认证，已认证工作和准备好的响应允许完成；不维护 Grant、grant revision 或 Catalog 输出授权许可。DataOutputPermit 在响应和 body frame 交付时检查来源数据有效性。

普通工作按全局快照共享屏障 → 集合共享生命周期屏障 → 短持 Catalog 元数据锁的顺序取得当前代次，在元数据锁外执行 Record future。删除/创建等生命周期变更使用对应集合独占屏障；完整服务快照保留全局独占屏障，Key 撤销不排空数据工作。分级资源准入在读 body 前计费，已接收 owned task 和传输缓冲保留额度至实际结束。

创建为持久 Prepare → 初始化 partition → Activate。删除后名称可复用，新建集合分配新内部代次；旧句柄、缓存、回执、上传、后台索引和 GC 绑定旧代次，物理回收后置。本地路径由 `collections/{tenant_id}/{collection_id}/{generation}` 直接派生，对象使用 `anyseek/{tenant_id}/{collection_id}/`，代次编码在对象 ID 中。客户端仅使用名称，删除/重建前须停止旧请求及重试。

集合恢复只占用空闲名称并分配新代次。完整服务恢复发布到新未启动根，使用独立 bucket，禁用历史普通 Key，由目标 master 为已有 Tenant 重新签发。独立备份不能证明后续没有删除，因此 typed evidence 保持隔离；无解除隔离或跨服务自动灾备入口。

## 权威与派生文件

服务根由外部稳定 owner lock 保护。Catalog 和 S3 authority 共用 `metadata.sqlite`，只存协调元数据，不存完整 Record State、正文、原始向量或索引；Catalog 不保留独立持久化日志或名称到生成目录的映射。SQLite 事务不跨对象上传或索引构建。Record checkpoint 保留配置、当前记录、墓碑、逻辑水位和操作账本。backup-*、service-backup-* 是独立快照，完整服务 manifest 和外层完成见证认证目录发布。不同独立服务使用不同 bucket，不创建自动服务 namespace，也不检查 Bucket 归属。

派生 cache-* 中继续使用 schema/index 配置、index.bin、vector.bin、docstore.bin、facet 文件等。启动加载认证 Segment 并重放权威 tail，缺失的派生缓存可重建，不把这组可变文件当作多文件原子事务。原始 Document 和未经归一化的输入向量独立保留，搜索 docstore 的投影不能替代它们。持久词项哈希使用固定 XXH64，当前低层索引格式为 8。

Linux 同步文件和父目录；Windows 以同卷无覆盖目录移动完成发布并在移动前关闭内部句柄。恢复对已认证的新状态损坏报错，不静默回退旧代或重置为空数据库。低层 commit/close 传播 I/O Result，但仍不是权威事务协议。

## 当前边界

这是有界单机实现：Record 累计账本默认 10,000 项、完整 checkpoint 16 MiB 等限制见 [FEATURES](FEATURES.md)。checkpoint 回收旧 WAL 代，不等于持续账本/物理版本 GC。P2 的 source/evidence、generation、TTL、显式关系和同视图融合已实现。不可变段、compaction、SQLite/S3 checkpoint、安全回收、维护修复、后台 Indexer、校验缓存和分级资源准入已合入。真实 S3、系统断电、服务总缓存预算和完整资源隔离验收仍未完成；分布式未实现。原生进程故障测试不能替代主机掉电验收。整体能力与优先级建议见[项目全貌](docs/project-overview.md)，全文替换的复用边界见 [Tantivy 评估](docs/tantivy-evaluation.md)。

[Record 语义](docs/record-model.md) · [存储协议](docs/storage.md) · [服务契约](anyseek/src/server/README.md) · [许可](LICENSE)
