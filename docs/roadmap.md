# 后续核心路线

已合入能力见 [STATUS](../STATUS.md)，当前存储实现见[存储协议](storage.md)。路线不代替验收结果。

## B 收尾：SQLite + S3

1. 维护路径审计已落实到远端 seal/接管/归档、提交后的缓存失败分类、bootstrap 发布前校验和备份指针中断修复；对应协议与失败行为见[存储协议](storage.md)。后续修改继续维持唯一权威、失败 fenced 和原意图重试。
2. 无缓存检索与失效验收复用七组现有 HTTP 用例，覆盖 generation、TTL、staging 清理、来源/关系、过滤/统计和撤权输出；fixture 原生 Linux/Windows CI 入口已加入，按最终提交核对结果。
3. 实际 endpoint 合同工具和用例已提供，按本轮约定先完成工具，不连接外部服务。仍需在专用真实 bucket 跑完整合同与故障矩阵；HTTP fixture 只覆盖协议回归，不能证明云服务兼容性。
4. 对最终源码完成 Linux/Windows CI、文档及共同恢复审查。系统/文件系统崩溃、硬断电需要隔离环境和外部确认见证，单独记录。

## C：Indexer、缓存与 collection 服务化

- Indexer 消费已提交连续范围，带 pin、owner/build epoch、期限与有界队列；发布检查基础，保留构建期间新增 tail，不提交业务操作。
- 缓存仅保存有命名空间、hash/长度认证的不可变块，下载验证后原子入缓存。淘汰不能删除正在使用的 mmap；丢失缓存仍可恢复。先完整小段下载，块级预取按实际需求推进。
- 一次查询固定 manifest、indexed/committed 水位、完整 tail、活动统计和 evaluation_time；必要 tail 缺失时失败。先各通道全局排名，再融合一次。
- 多 collection 的前台、下载与后台任务各有并发/队列/内存/I/O 预算，避免热点耗尽全部资源。
- owner 路由不能替代权威 fence。SQLite 首版仍为唯一持久卷权威；跨主机接管须先定义外部隔离和可用性协议。失联不能回退旧授权/缓存；多节点输出必须与撤权/隐藏屏障有序。

## D：超大 collection，按容量需求后置

先稳定路由与静态分片、显式分片视图及有界扇出，再持久化 Prepare → Build → Catch up → Fence → Publish → Cleanup，最后自动调度。不能用随 partition 数变化的取模路由。来源/关系跨 partition 语义未定义前，不自动分裂。

跨分片先合并各通道候选和活动统计，再做全局融合；必要分片失败默认整次查询失败。候选不完整不能豁免版本、删除与授权检查。

## 范围

保留 Web UI、现有 HTTP API 和 Record 契约。模型推理、解析切块、embedding/reflect、托管大文件、自动语义关系、SQL/JOIN、完整图语言、旧格式升级迁移均不进入当前主线。监控先覆盖验收必需水位差、tail 字节、段数、pin/退休阻塞、unknown、恢复时间和资源拒绝，不先扩展产品看板。
