# Tantivy 全文引擎复用评估

日期：2026-10-23。项目基线：master `3893cd54`。状态：设计评估，未引入 Tantivy 依赖、未修改全文引擎、未承诺实施或格式兼容。项目整体优先级见[全貌](project-overview.md)。

## 已确认的替换约束

用户已确认不需要考虑迁移和升级，可以完全替换。后续方案直接面向新建数据库和全新存储格式，不要求读取旧数据库、导入旧备份、原地升级、双写或长期保留旧全文引擎。既有 Payload、WAL、Anchor 和向量文件格式的复用是工程选择，不是兼容性约束；可以按新架构重新设计。

若采用 Tantivy，按单一全文引擎规划，删除被替代的旧倒排读写、查询执行和兼容路径。不为多引擎共存建设通用框架。格式标识仍用于校验当前文件、拒绝错误或不支持的格式，不意味着提供升级链。

同一新格式内的崩溃恢复、日志重放、检查点、备份恢复，以及 durable + visible、版本/幂等、来源隐藏、TTL 和授权屏障仍是必需能力。允许完全替换不等于删除已有数据的执行指令，也不自动改变这些数据正确性契约。Tantivy 是否正式采用及新的评分规则仍由设计与验证结果确定。

## 结论

可以保留现有数据库语义和存储协议，把全文检索交给 Tantivy，但它不是直接可替换的纯计算模块。Tantivy 的索引写入、文件编码、读取和查询执行相互配合；它不能直接读取 anyseek 当前 format 8 的倒排文件。

如果“复用存储格式”指 Record/WAL、规范 Payload、SQLite/S3 协调和对象分块传输，原则上可以保留并做适配。如果指现有倒排索引字节不变，只把查询计算替换成 Tantivy，则不具备直接可行的接入路径。实现自定义 Directory 只改变文件读写位置，不改变 Tantivy 对文件内部编码的预期。

## 可复用与需改造的边界

| 层 | 复用判断 | 必须处理的适配 |
| --- | --- | --- |
| Record、版本、幂等、来源、TTL、generation、Feed | 保留现有业务权威和语义 | Tantivy 只是派生全文索引，不决定提交或访问权限 |
| 原始 Document / 原始向量 Payload | 保留规范数据唯一归属；编码与引用池可复用或重设计 | 不在 Tantivy stored fields 再存一份完整正文；允许必要索引、位置、fast fields 和身份映射 |
| 本地 WAL、SQLite head/owner、S3 不可变对象与 pin/GC | 保留唯一权威与恢复协议原则；业务编码可重设计 | 新索引文件完整同步、校验、保护后，才由权威选择；Tantivy commit 不成为第二套业务 head |
| Anchor、Segment inventory、远端 descriptor | 文件清单/hash/分块传输机制可复用，布局可替换 | 替换硬编码旧引擎的 loader，定义新格式与完整文件闭包验证；不要求旧 descriptor 可读 |
| 全文倒排、词典、词频、位置、fieldnorm 等 | 采用 Tantivy 自有格式，重新构建 | 当前 `index.bin` 不是 Tantivy 索引；不能只改文件名或 Directory 来兼容 |
| 向量索引 | 可继续使用现有向量通道 | 现有 IndexArc 同时承载全文/向量/过滤等状态，需拆开职责并建立稳定 Record/版本映射 |
| 全文/向量/关系融合 | 保留 RRF 及候选预算的产品语义 | 各通道固定同一视图、过滤边界和稳定平分排序，再融合一次 |
| 过滤、排序、分词、解释 | 逐项适配 | schema、中文分词、归一化、短语/邻近度、字段长度、查询语法和计数语义不能默认一致 |

## 代码中的实际耦合

- [RetrievalView](../anyseek/src/record/view.rs) 持有同一套 IndexArc，负责 tail 追加、旧位置退休、多 Segment 组合及查询，尚无独立的全文后端接口。
- [ActiveStatistics / Corpus](../anyseek/src/active.rs) 按活动版本维护跨冻结段和 tail 的 N、df、字段长度；删除与更新同时改变统计。
- [Anchor](../anyseek/src/record/anchor.rs) 的加载过程调用现有索引 loader，并验证 schema、活动统计和内部拓扑；通用文件清单不等于通用查询引擎。
- [远端 checkpoint](../anyseek/src/record/remote_checkpoint.rs) 按文件路径、长度和 SHA-256 分块存储，适合作为新派生文件的传输机制，但需要扩展认证闭包。
- [融合](../anyseek/src/record/fusion.rs) 复用当前查询路径，不能仅替换一个最终结果列表就保证原有过滤、统计和预算语义。

## 四个必须先证明的问题

### 1. durable + visible 与最新 tail

当前普通写入成功后立即可查。Tantivy 的常规发布路径涉及 writer commit、reader reload 和获取新 Searcher；不能把“已提交”改成“等后台刷新后可查”。

优先评估同一 Tantivy 引擎内同步发布候选 Searcher，并使全文、向量和权威记录在同一发布屏障内可见。若需要独立的有界 tail 检索，应按新引擎的分词、统计与评分设计；不因兼容要求保留旧 tail 全文引擎。具体方案须验证资源和延迟，不能以异步刷新替代即时可见契约。

### 2. Top-K 前的可见性和过滤

Record 的旧版本、删除、来源隐藏、stale、TTL 和 generation 状态必须在候选截断前处理。不能先取 Tantivy Top-K 再过滤：前 K 条全部失效时，真实有效结果可能永远未被取出。

应在固定视图中将有效记录映射为执行期过滤集合，或实现可证明完整、受预算限制且明确错误边界的收集方案。Tantivy DocAddress 是搜索快照内地址，不能充当持久 Record 身份；merge/reload 后必须重建或认证映射。最终回表与输出授权屏障仍保留。

### 3. 活动统计与排序语义

当前评分基于跨冻结段和 tail 的活动版本 N、df 和字段长度，BM25F/邻近度也不是把 Tantivy 默认多字段 BM25 加权后自然得到的同一算法。

Tantivy 提供 BM25 statistics provider 等扩展点，可作为适配基础，但仍须验证 Query/Weight/Scorer 路径、字段长度及 pruning 上界，不能只换统计数值就宣称排名等价。其默认统计也不能直接假设为本项目的活动版本统计。

需要选择：保留现有排名契约并承担适配成本，或明确发布新的全文排名语义。RRF 降低了不同通道原始分数尺度的耦合，但全文内部排名改变仍会改变混合结果。后者是产品决策，本文未作决定。

### 4. 发布、合并、回收与预算

Tantivy 的内部 metadata/commit 只能表示一个候选全文快照；本地证书或 SQLite 才能把它与原始数据、向量、tail 和业务水位一起选为当前视图。需固定 Searcher，避免自动 reload 悄悄切换到未认证快照。

索引 merge 和内部 GC 不能删除当前读者、构建、备份所需文件。候选目录应独立构建、稳定化和验证，发布后保持不可变；首次方案优先本地完整物化，再复用现有 S3 对象协议，不直接增加远端 Directory 的同步随机读。writer 内存、merge 线程、下载和缓冲也须接入现有资源预算。

## 建议方案与决策条件

若决定推进，建议让 Tantivy 完整负责全文索引与执行，anyseek 管理原始数据、业务状态、授权、向量/关系通道及统一发布。原始数据唯一归属、本地证书或 SQLite 唯一权威、不可变对象闭包和安全回收原则继续保留；具体代码、文件编码和目录布局按新架构取舍。初期采用私有候选目录构建与本地物化，不实现旧格式 codec、迁移器、双写或旧引擎运行时切换，也不依赖 bucket 可变 root 选择提交。

先做隔离的、有范围的语义与可行性验证，再决定替换；无需验证旧磁盘格式兼容或升级。验证至少覆盖中文/多字段/短语、缺词与空查询、更新删除后的 Top-K 和统计、新写入即时可见、generation/来源/TTL、混合排名和平分、候选预算、进程退出、清空缓存恢复、同一新格式的备份恢复和授权输出。应比较实际语义、资源、依赖与维护成本，不预设 Tantivy 更快或更省内存；这不是重启全项目性能调优专项。

潜在收益是复用成熟全文引擎和生态，减少自维护倒排与执行算法的范围；代价是派生格式重建、接口拆分、评分/分词适配及新的发布/预算集成。它不会直接解决 Record 账本 10,000 操作上限、完整 State 驻内存、总缓存预算或跨主机 HA。

当前建议先完成长期运行与资源闭环。如果团队明确希望长期外包全文引擎维护，再在扩展大容量索引和分片之前决策；无需为未来可能的切换先建设庞大的通用后端框架。

## 上游参考

本评估核对了 Tantivy 上游接口与说明，尚未选定依赖版本；正式验证前必须锁定版本并重新核对 API。

- [Tantivy README](https://github.com/quickwit-oss/tantivy/blob/main/README.md)：定位、BM25、分词及 commit/reload 可见性。
- [Directory](https://github.com/quickwit-oss/tantivy/blob/main/src/directory/directory.rs)：文件存取抽象及文件切片生命周期，不是索引编码转换接口。
- [BM25 statistics provider](https://github.com/quickwit-oss/tantivy/blob/main/src/query/bm25.rs)：统计扩展点与默认计算。
- [Index、Writer、Searcher 与格式说明](https://github.com/quickwit-oss/tantivy/blob/main/src/lib.rs)：索引生命周期、内部文档地址和独立索引格式。
