# Record 语义与一致性

HTTP 请求、字段、配额和操作示例以[服务指南](../anyseek/src/server/README.md)为准；本文保留实现必须维持的不变量。当前交付状态见 [STATUS](../STATUS.md)，持久化见[存储协议](storage.md)。

## 身份、版本与重试

集合由认证得到的 Tenant 和验证后的集合名定位；本地目录直接由名称和内部代次派生。删除后同名重建使用新代次，写入回执按 Tenant、集合代次和 Operation ID 隔离，与具体 API Key 无关。旧任务持有的句柄只能操作原代次。客户端必须在删除/重建前停止旧请求及重试；服务端无法凭名称识别重建后到达的旧客户端请求。集合备份恢复仅占用空闲名称并分配新代次，恢复保留归档内的版本和回执。

`IndexMetaObject` 只保存检索配置和展示名称，不包含集合定位用的数字索引 ID。集合定位由 Tenant、collection_id 和集合代次负责。

`record_id` 是 collection 内稳定身份，内部 DocID 仅为索引地址。删除保留墓碑，重建继续递增版本，旧条件不能因 ID 复用而接受新内容。`document` 是完整替换，显式 null 表示删除；`schema.store` 只控制搜索投影，不影响原始数据持久保存。

客户端在发送前保存完整 `operation_id` 与语义请求。同一身份和相同请求返回原回执，不重复推进版本；同一身份改变内容冲突。支持窗口为当前及前一 UTC 日，持久 epoch 水位阻止时钟回退复活请求。超时、回执缺席、过期及后续拒绝都不能证明早先请求未提交，不应换 ID 猜测补写。

| 结果 | 语义与处理 |
| --- | --- |
| 同步 `/batch` 逐项 applied 回执、内部 `write` 成功 | 权威已持久且同步派生应用完成 |
| `/ingest`、Rust `commit_writes` 的 committed 项 | 权威业务结果已持久，全文/向量异步消费；不保证该序号已经可搜索，见[进度与重试](../anyseek/src/server/README.md#异步索引写入) |
| rejected / conflict / busy | 描述本次校验、条件或准入结果；不解除早先未知状态 |
| outcome_unknown | 持久化或响应结果不明确；恢复后以原身份查账/重试 |
| durable_pending | 权威已持久、派生应用未完成；实例封锁并等待恢复 |
| recovery_required / storage_error | 不能继续可信服务，不回退旧缓存或重置空库 |
| operation_expired | 已超支持窗口，不能用新操作身份推断补写 |

回执默认最早在两天后可进入 GC，且必须同时超过操作重试窗口并被认证 history floor 覆盖；这不是固定清理期限。显式较长 Rust 配置保留，缺省字段使用当前两日默认；HTTP 创建使用默认值，没有保留期参数。SQLite publication receipts 与恢复日志的职责不同，不按这个窗口直接删除。

持久工作由独立持有所有权的任务接管；HTTP 取消不撤销已接受工作。配额覆盖原始记录、完整恢复闭包、操作回执及后台构建，不只是当前可见正文。

## 检索引擎与应用边界

来源版本、分块、章节、证据依赖和关系属于上层应用；可将这些信息作为普通 document 字段保存。AnySeek 不提供 source/derived 写入、旧版本正文摘录、定位符验证、关系遍历或关系融合接口。全文/向量通用混合检索、条件更新和精确正文版本的向量后补继续保留。

本地分支已收敛内核、TTL、回执政策和普通提交校验。[实施说明](engine-scope.md)列明实现与验证边界；未发布的本地改动不代表主干或已发布版本行为。

## 授权与恢复

Catalog 在 SQLite 保存 Tenant、租户 Key、Collection、备份归属与管理回执。普通 Key 只认证 Tenant；Tenant 内集合访问范围一致。master 管理 Tenant/凭据，不读取集合数据。没有 User、Project、Collection Grant 或角色模型。

凭据撤销只影响后续认证；已认证工作和输出允许完成。保留集合生命周期与完整服务快照的执行屏障，以及数据有效性的输出检查。租户和集合名称采用统一正则，服务端不信任客户端声明的 Tenant。

独立备份恢复保留版本与回执、分配独立恢复 scope；普通集合备份不包含凭据。完整服务恢复保留 Tenant，禁用历史普通 Key，由目标 master 重新签发租户凭据。

独立 restore 只恢复备份时保存的数据，不自动追平备份后的写入或删除，也不提供跨服务双活。

每个 collection 一个 partition。没有跨 collection 事务、来源依赖、SQL/JOIN、完整图语言或旧格式迁移。Linux/Windows 进程故障回归不替代系统崩溃与断电验收。
