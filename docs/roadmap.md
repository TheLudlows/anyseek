# 后续核心路线

已合入能力见 [STATUS](../STATUS.md)，当前存储实现见[存储协议](storage.md)。路线不代替验收结果。

## B 收尾：SQLite + S3

PR #72/#73/#74 已合入，当前源码基线为 `afbe323d`；第 1–6 项实现及第 7 项工具/用例已交付。PR #74 的 [Linux/Windows CI 五项全部通过](https://github.com/TheLudlows/anyseek/actions/runs/39814329888)，合入后主干 `a36e71e0` 的 [CI 五项通过](https://github.com/TheLudlows/anyseek/actions/runs/39884779455)。真实 S3 与系统故障/断电验收继续开放。

1. 维护路径审计已落实到远端 seal/接管/归档、提交后的缓存失败分类、bootstrap 发布前校验和备份指针中断修复；对应协议与失败行为见[存储协议](storage.md)。后续修改继续维持唯一权威、失败 fenced 和原意图重试。
2. 无缓存检索与失效验收复用七组现有 HTTP 用例，覆盖 generation、TTL、staging 清理、来源/关系、过滤/统计和撤权输出；七组 fixture 验收及 PR 原生 Linux/Windows CI 均已通过。
3. 实际 endpoint 合同工具和用例已提供，按本轮约定先完成工具，不连接外部服务。仍需在专用真实 bucket 跑完整合同与故障矩阵；HTTP fixture 只覆盖协议回归，不能证明云服务兼容性。
4. PR 最终源码的 Linux/Windows CI、维护协议及共同恢复审查已完成；主干 `a36e71e0` CI 已核对通过；后续源码单独验收。系统/文件系统崩溃、硬断电需要隔离环境和外部确认见证，单独记录。

## C：Indexer、缓存与 collection 服务化

- Indexer 消费已提交连续范围，带 pin、owner/build epoch、期限与有界队列；发布检查基础，保留构建期间新增 tail，不提交业务操作。
- 缓存仅保存有命名空间、hash/长度认证的不可变块，下载验证后原子入缓存。淘汰不能删除正在使用的 mmap；丢失缓存仍可恢复。先完整小段下载，块级预取按实际需求推进。
- 一次查询固定 manifest、indexed/committed 水位、完整 tail、活动统计和 evaluation_time；必要 tail 缺失时失败。先各通道全局排名，再融合一次。
- 多 collection 的前台、下载与后台任务各有并发/队列/内存/I/O 预算，避免热点耗尽全部资源。
- owner 路由不能替代权威 fence。SQLite 首版仍为唯一持久卷权威；跨主机接管须先定义外部隔离和可用性协议。失联不能回退旧授权/缓存；多节点输出必须与撤权/隐藏屏障有序。

### C1：有界后台 Indexer（PR #75 已合入）

目标是在构建索引时继续接收写入，缩短 partition 串行屏障占用，保持既有查询、删除与撤权契约。

1. 建立有界任务队列及并发、内存、I/O 准入；固定构建输入范围。
2. 构建绑定 owner/build epoch、期限及完整依赖 pin，SQLite 继续承担唯一发布权威。
3. 在串行屏障外构建并验证候选文件，在短发布阶段条件检查基础 manifest/水位。
4. 保留构建期间新增的完整提交 tail；过期、旧 owner 或基础已变化的任务不得覆盖新状态。
5. 验收持续写入、索引暂停、重复任务、过期发布、退出恢复、删除与撤权输出；失败不得丢已提交数据。

[PR #75](https://github.com/TheLudlows/anyseek/pull/75) 已合入为 `09d30ae4`，最终提交 `96306e95` 的 [CI 五项全部成功](https://github.com/TheLudlows/anyseek/actions/runs/39891546462)。已交付 `start_index_committed` / `index_committed`、HTTP PATCH 后台构建、SQLite 构建 epoch/期限/pin、旧 cut 发布与完整新增 tail，以及并发/取消/过期/进程退出回归。合入后主干 CI 单独跟踪。

### C2：有界校验缓存（开发分支实现，尚未合入）

远端读取按 endpoint/bucket/region、完整对象 key、SHA-256 与精确长度隔离对象块；每次命中重新认证，损坏删除并回源。每 authority 库旁的 `*.object-cache` 目录限 256 MiB / 4096 块，按最早写入淘汰；先清理中断临时文件，删除失败不退额度，非阻塞文件锁协调进程。缓存错误不改变提交结果。下载验证完整后同步私有文件、同目录原子发布；PUT 的读后验证始终访问远端，不能由缓存证明远端持久化。

缓存返回独立字节，不暴露文件句柄/mmap；Segment 在另一路径完整校验并映射，因此淘汰不触碰查询中的 mmap。冷恢复仍由当前 SQLite head 选择 manifest + 完整 tail，授权/来源隐藏屏障不从缓存推导。无缓存验收删除 collection 和对象块两类目录。预取与直接映射缓存块后置；本批验证见 PR Checks，不据分支实现标主线完成。

C2 后继续推进 collection 的前台、下载、后台并发/队列/内存/I/O 公平准入。当前完整 State 驻内存及同步 compaction 串行行为仍是实际限制。

## D：超大 collection，按容量需求后置

先稳定路由与静态分片、显式分片视图及有界扇出，再持久化 Prepare → Build → Catch up → Fence → Publish → Cleanup，最后自动调度。不能用随 partition 数变化的取模路由。来源/关系跨 partition 语义未定义前，不自动分裂。

跨分片先合并各通道候选和活动统计，再做全局融合；必要分片失败默认整次查询失败。候选不完整不能豁免版本、删除与授权检查。

## 范围

保留 Web UI、现有 HTTP API 和 Record 契约。模型推理、解析切块、embedding/reflect、托管大文件、自动语义关系、SQL/JOIN、完整图语言、旧格式升级迁移均不进入当前主线。监控先覆盖验收必需水位差、tail 字节、段数、pin/退休阻塞、unknown、恢复时间和资源拒绝，不先扩展产品看板。
