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

普通提交的所有权与本批增量规划见[写入证明](record-write-proof.md)；合入记录及验证范围集中在 [STATUS](../STATUS.md)，主干合入不代表已发布版本行为。

## 时间、TTL 与查询视图

查询在捕获时刻应用 TTL 过滤，最终候选按最新进程评估时间重验，期间到期、更新或删除可能造成短页。`count_total` 是捕获索引的模式相关观测数，不是最终时刻精确总数。BM25 语料统计在后台退休过期索引行前仍可能包含这些记录。分页游标在 seq 改变或跨越捕获视图最早 TTL 时冲突，需要重新遍历。

读取只推进进程内 evaluation_time，不持久执行 expiry/history trim，也不等待 partition writer 来修改权威状态。后台通过原 WAL/远端提交协议回收 TTL，ID 可复用且技术版本递增，旧向量不能补到新正文。服务每秒最多选择一个已加载 partition 调度时间维护，繁忙、资源预算或 I/O 会推迟回收，不承诺固定物理删除时限。

同一进程的 evaluation_time 不回退，并受持久化写入/维护时间下界约束。重启只保留该持久下界：未回收正文可能因墙钟回退跨过 deadline 而重新可见；大幅时钟偏差返回 `clock_uncertain`。已经持久回收的旧版本不会自行复活，但 WAL、备份和旧快照仍可保存正文，TTL 不是隐私删除承诺。

## 授权与恢复

Catalog 在 SQLite 保存 Tenant、租户 Key、Collection、备份归属与管理回执。普通 Key 只认证 Tenant；Tenant 内集合访问范围一致。master 管理 Tenant/凭据，不读取集合数据。没有 User、Project、Collection Grant 或角色模型。

凭据撤销只影响后续认证；已认证工作和输出允许完成。保留集合生命周期与完整服务快照的执行屏障，以及数据有效性的输出检查。租户和集合名称采用统一正则，服务端不信任客户端声明的 Tenant。

独立备份恢复保留版本与回执、分配独立恢复 scope；普通集合备份不包含凭据。完整服务恢复保留 Tenant，禁用历史普通 Key，由目标 master 重新签发租户凭据。

独立 restore 只恢复备份时保存的数据，不自动追平备份后的写入或删除，也不提供跨服务双活。

每个 collection 一个 partition。没有跨 collection 事务、来源依赖、SQL/JOIN、完整图语言或旧格式迁移。Linux/Windows 进程故障回归不替代系统崩溃与断电验收。
