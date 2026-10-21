# 当前架构

Seekpower 由 Rust 核心库和 HTTP/Web UI 服务组成。默认服务已接入 Record/catalog，每个 collection 一个持久 partition 和一个内部搜索 shard。低层索引库仍保留自身 API 和本机多 shard 实现，不代表服务已支持分布式分片。

## 模块边界

| 层 | 代码 | 职责 |
| --- | --- | --- |
| UI | `seekpower_server/web/` | 创建/写入原请求保存与重试、搜索、分页、预览、日期过滤、服务备份恢复 |
| 默认服务 | `record_service.rs`、`server.rs`、`main.rs` | HTTP 契约、身份入口、admission、响应授权屏障、启动恢复 |
| 控制权威 | `catalog.rs`、`catalog/service_snapshot.rs` | key verifier、collection 生命周期、grant revision、控制回执和完整服务快照 |
| 数据权威 | `record.rs` | 原始 Document/向量、稳定 ID、版本、墓碑、幂等、共同可见视图 |
| 持久化 | `storage/journal.rs`、`partition.rs`、`platform.rs` | WAL、见证、代切换、目录同步/发布、owner lock |
| 派生索引 | `index.rs`、`commit.rs`、`doc_store.rs` | 现有倒排、向量、文档投影和 metadata 存储的构建/提交 |
| 检索 | `search.rs`、`vector.rs`、`iterator.rs`、`min_heap.rs` | 三路检索、候选、排序、遍历；Record 的稳定身份在截断前参与平分排序 |
| 活动统计 | `active.rs`、`field_length.rs` | 活动版本 N/df/字段长度；与物理编码计数分离 |
| 过滤/分词 | `metadata_filter.rs`、`tokenizer.rs` | 共享元数据判定与文本处理 |

路径相对于核心或服务的 src。`api_types.rs` 和 `api_schema.rs` 保留服务公共配置/查询 DTO 及 OpenAPI 描述；核心不依赖 HTTP schema。`http_server.rs`、`keys.rs`、`operations.rs` 是尚未清理的旧服务模块，默认启动不挂载它们。它们不提供受支持的兼容服务。

## 写入与查询

写入先校验身份、条件版本、向量、记录和完整快照容量，再同步权威操作日志；随后增量应用搜索索引、撤销旧活动统计、发布共同序列，最后返回成功。索引应用失败返回 durable_pending 并封锁该实例；日志结果不确定返回 outcome_unknown，需要恢复和原 ID 重试。已接管工作不随 HTTP 请求取消而中断。

Record partition 的 mutex 覆盖候选检索和原始数据回表。同一个查询只看到一个发布位置；替换/删除在 Top-K 前过滤旧版本。空查询使用同一删除检查、metadata 过滤和候选堆，纯向量 collection 不依赖词法字段也能浏览。

服务根据文本/向量选择 Lexical、Vector、Hybrid。Hybrid 两路候选通过 RRF 融合，支持权重和各路预算；ANN 可能因簇探测返回不足 K。候选窗口计数和候选内字段重排不等于全库精确结果。查询原始数据经 schema.store 和 fields 投影后输出；直接读取和 iterator 可返回完整权威 Record。

## 授权与生命周期

API key 身份独立于 collection 数据目录。master 管理身份和 grants，普通 key 可访问其授权集合；撤销不删除数据。catalog 当前持有全局 mutex 执行完整授权工作，确定撤销和写入提交先后；代价是跨 collection 的查询/写入也会等待。OutputPermit 捕获 grant revision，在最终响应和 body frame 交付时复核，撤销再授权不能复活旧响应。

创建为持久 Prepare → 初始化 partition → Activate。删除 collection 保留墓碑并撤销 grants，物理回收后置。全服务备份在 catalog 屏障下取得一致切面。恢复发布到新未启动根，历史 key 禁用和 grants 清空，当前 master 获取初始访问。

## 权威与派生文件

服务根由外部稳定 owner lock 保护，内部 catalog 使用 generation-* 下的 WAL 和 commit 见证；collection 数据位于 catalog 生成的 collection-* 路径。Record checkpoint 保留配置、当前记录、墓碑、逻辑水位和操作账本。backup-*、service-backup-* 是独立快照，完整服务 manifest 和外层完成见证认证目录发布。

派生 cache-* 中继续使用 schema/index 配置、index.bin、vector.bin、docstore.bin、facet 文件等。启动从权威日志重建派生索引，不把这组可变文件当作多文件原子事务。原始 Document 和未经归一化的输入向量独立保留，搜索 docstore 的投影不能替代它们。持久词项哈希使用固定 XXH64，当前低层索引格式为 8。

Linux 同步文件和父目录；Windows 以同卷无覆盖目录移动完成发布并在移动前关闭内部句柄。恢复对已认证的新状态损坏报错，不静默回退旧代或重置为空数据库。低层 commit/close 传播 I/O Result，但仍不是权威事务协议。

## 当前边界

这是有界 P1-A 功能闭环：Record 累计账本 10,000 项、完整 checkpoint 16 MiB 等限制见 [FEATURES](FEATURES.md)。checkpoint 回收旧 WAL 代，不等于持续账本/物理版本 GC。系统断电、P2 来源语义、P3 不可变段和持续回收、对象存储与分布式尚未完成。原生进程故障测试不能替代主机掉电验收。

[实施和平台验证](docs/implementation-57.md) · [服务契约](seekpower_server/README.md) · [上游来源](UPSTREAM.md) · [许可](LICENSE)
