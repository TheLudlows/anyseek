# Record 语义与一致性

HTTP 请求、字段、配额和操作示例以[服务指南](../anyseek/src/server/README.md)为准；本文保留实现必须维持的不变量。当前交付状态见 [STATUS](../STATUS.md)，持久化见[存储协议](storage.md)。

## 身份、版本与重试

集合由认证得到的 Tenant 和验证后的集合名定位；本地目录直接由名称和内部代次派生。删除后同名重建使用新代次，写入回执按 Tenant、集合代次和 Operation ID 隔离，与具体 API Key 无关。旧任务持有的句柄只能操作原代次。客户端必须在删除/重建前停止旧请求及重试；服务端无法凭名称识别重建后到达的旧客户端请求。集合备份恢复仅占用空闲名称并分配新代次，恢复保留归档内的版本和回执。

`record_id` 是 collection 内稳定身份，内部 DocID 仅为索引地址。删除保留墓碑，重建继续递增版本，旧条件不能因 ID 复用而接受新内容。`document` 是完整替换，显式 null 表示删除；`schema.store` 只控制搜索投影，不影响原始数据持久保存。

客户端在发送前保存完整 `operation_id` 与语义请求。同一身份和相同请求返回原回执，不重复推进版本；同一身份改变内容冲突。支持窗口为当前及前一 UTC 日，持久 epoch 水位阻止时钟回退复活请求。超时、回执缺席、过期及后续拒绝都不能证明早先请求未提交，不应换 ID 猜测补写。

| 结果 | 语义与处理 |
| --- | --- |
| 同步 `/doc`、`/batch`、Rust `write` 成功 | 权威已持久且同步派生应用完成；staging 成功只保证私有成员持久，须显式 Publish |
| `/ingest`、Rust `commit_writes` 的 committed 项 | 权威业务结果已持久，全文/向量异步消费；不保证该序号已经可搜索，见[进度与重试](../anyseek/src/server/README.md#异步索引写入) |
| rejected / conflict / busy | 描述本次校验、条件或准入结果；不解除早先未知状态 |
| outcome_unknown | 持久化或响应结果不明确；恢复后以原身份查账/重试 |
| durable_pending | 权威已持久、派生应用未完成；实例封锁并等待恢复 |
| recovery_required / storage_error | 不能继续可信服务，不回退旧缓存或重置空库 |
| operation_expired | 已超支持窗口，不能用新操作身份推断补写 |

持久工作由独立持有所有权的任务接管；HTTP 取消不撤销已接受工作。配额覆盖原始记录、完整恢复闭包、操作回执及后台构建，不只是当前可见正文。

## 来源、generation 与时间

来源和关系引用精确版本。来源删除、stale、generation 切换与 TTL 共用可见性规则，查询、回表、解释和最终输出不能绕过隐藏。来源旧修订仍可能按既有证据协议读取，不能仅因没有当前引用就回收。

Begin/StageBatch 只写私有成员；Seal 认证完整清单；Publish 原子切换整代成员与活动统计。Abort/租约到期清理私有正文，但保留精确重试需要的摘要回执。向量后补以精确版本为前提，物理 payload 身份不能只用正文版本表示。

一次查询固定 `evaluation_time`，TTL 过期通过权威决定形成不可逆墓碑。响应期间单纯经过时间期限不要求改变固定视图；后续请求观察已提交的过期结果。

## 授权与恢复

Catalog 在 SQLite 保存 Tenant、租户 Key、Collection、备份归属与管理回执。普通 Key 只认证 Tenant；Tenant 内集合访问范围一致。master 管理 Tenant/凭据，不读取集合数据。没有 User、Project、Collection Grant 或角色模型。

凭据撤销只影响后续认证；已认证工作和输出允许完成。保留集合生命周期与完整服务快照的执行屏障，以及来源/数据有效性的输出检查。租户和集合名称采用统一正则，服务端不信任客户端声明的 Tenant。

独立备份恢复保留版本与回执、更新 generation 恢复隔离身份，并维持证据隔离；普通集合备份不包含凭据。完整服务恢复保留 Tenant，禁用历史普通 Key，由目标 master 重新签发租户凭据。

独立 restore 只恢复备份时保存的数据，不自动追平备份后的写入或删除。typed evidence 持续隔离，没有强制解锁或跨服务双活入口；live WAL/在线 checkpoint 重启恢复不新增该隔离。

每个 collection 一个 partition。没有跨 collection 事务、来源依赖、SQL/JOIN、完整图语言或旧格式迁移。Linux/Windows 进程故障回归不替代系统崩溃与断电验收。
