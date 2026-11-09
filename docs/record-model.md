# Record 语义与一致性

HTTP 请求、字段、配额和操作示例以[服务指南](../anyseek/src/server/README.md)为准；本文保留实现必须维持的不变量。当前交付状态见 [路线与验收证据](roadmap.md)，持久化见[存储协议](storage.md)。

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

### 批量写入的编排边界

HTTP `/batch` 只负责请求校验和逐项响应编码；核心 `write_independent_batch` 接管最多 100 项的有序、非原子执行。单个 detached owner 保留请求、资源上下文和取消后的工作，按原有最多 64 项、4 MiB 的内核窗口运行。`write_batch` 和异步 `commit_writes` 的 64 项入口边界保持不变。

确定拒绝之后推进到下一项，Busy 只允许已知未尝试项走标量准入；标量成功后可重新组合未尝试后缀。未知项不重试，致命中断保留已确认回执并停止后续尝试。所有推进使用输入切片和位置，不复制剩余请求后缀；每个内核窗口只计长一次，前缀推进直接扣除累计字节；仅实际进入 planner 的命令在预留的规划窗口内克隆。同步回执仍等两个消费者发布，异步回执仍仅确认权威持久结果。

## 检索引擎与应用边界

来源版本、分块、章节、证据依赖和关系属于上层应用；可将这些信息作为普通 document 字段保存。AnySeek 不提供 source/derived 写入、旧版本正文摘录、定位符验证、关系遍历或关系融合接口。全文/向量通用混合检索、条件更新和精确正文版本的向量后补继续保留。

普通提交的所有权与本批增量规划见下文[普通写入的提交基准](#普通写入的提交基准)；实现与验证范围集中在[路线与验收证据](roadmap.md)。

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

## 普通写入的提交基准

本节描述普通写入的所有权和增量规划约束；性能收益需由对应版本的生产路径测量验证。恢复、重建、维护和诊断继续使用完整 State/SourceProof 校验；普通写不缓存“已验证”标志，也不根据 seq 相等推断状态未变。

### 排他范围

普通写在任何 State 捕获之前取得该 partition 的 owned writer guard，直到全部持久发布、索引批次收尾及错误/unwind 封锁完成才释放。PreparedBatch 借用这个实际 guard，不能在释放它后提交。prepare 检查 guard 所属 mutex；commit 同时检查 writer 和 Inner guard 均属于目标 partition。base_seq 只检查序号连续性，不充当基准证明。

对外可用实例的规范 State 修改入口只有以下几类：

| 入口 | 修改权限与验证 |
| --- | --- |
| 普通 Applied 发布 | 同一 writer→Inner；先规划和准入，持久后用同一份不可变 Prepared 值逐项应用 |
| 自动维护 TTL / 本地 history trim | capture 时 writer→Inner，仍使用原 WAL/远端提交和失败封锁 |
| 维护 GC / remote trim | 修改私有候选，完整 SourceProof 和 State 比对后，在 writer→Inner 下安装 |
| 后台候选 State 安装 | 重新取得 writer→Inner，核对 anchor 与 tail revision/数量/字节，再发布证书并替换；不能简化为 seq 比较 |

create/open/recovery、S3 reconcile/restore 有不持 writer 的构造，但对象尚未向调用者公开；导出只修改私有克隆。它们不是可并发修改已公开实例的例外。

query/get/page/statistics 的 observe_time 仅修改 transient evaluation_time，不修改 State。索引消费者只修改派生状态。健康、索引失败、输出 fence、维护请求和资源预留仍可变化，提交必须继续检查这些边界。

### 同一 writer 自身的限制

排他锁不阻止其持有者自行修改状态。prepare→commit 的辅助函数必须保持如下契约：

- maintain_index_tail 只 drain/seal/rotate 派生索引；不得 trim/GC 或安装规范 State。
- ensure_journal_room / remote tail room 只校验或请求 checkpoint 后返回 Busy，不同步执行 checkpoint。
- prepare_remote_batch 与锁外 upload 不修改规范 State；远端对象上传有副作用，仍保留 head/owner 检查与 unknown 处理。
- admit_containers 可以增加 live State 的资源预留，不改变其序列化内容。

若未来将同步维护加入这些函数，必须重新建立提交基准；当前证明不适用。

### 结构合法性为何保持

锁只证明捕获基准未变。其合法性另由以下归纳链保持，而非每次普通写重复扫描全部记录和回执：

1. create/open 在公开实例前校验配置、数值向量、身份/lookup、容量、TTL/提交时间、history 和恢复来源；不可信 WAL、anchor 和 payload 仍完整验证。
2. mutation::prepare_input 对本次记录和最终 vector patch 文档验证字段、大小、向量数值、局部条件、时间、操作窗口、版本溢出及完整 checkpoint 字节预算。近期重试仍比较完整语义请求，不产生新决定。
3. state_memory::Prepared 在分配前取得独立值租约；PlanningView 为本批 map、调用方为 canonical 容器高水位分别预留租约。生产 Shared 值不可变；提交复用已规划的 Record/Applied，不重新生成另一套决定。
4. apply_prepared 检查连续 seq、非倒退提交时间、operation ID 唯一性及正文版本（向量后补保持正文版本）。记录 key 来自同一个 record_id；lookup 指向新追加 operation 的精确位置，lookup_bytes 用 checked_add 更新。
5. Records::insert_shared 增量维护值字节、TTL 数量和保守期限下界；operation 追加增量维护字节。snapshot_after 的精确长度增量算法及 reserve/admit 检查保留。它们不是未经验证的外部缓存。
6. history trim / expiry / GC / candidate install 继续在维护边界验证和安装；GC 重建 lookup，完整 proof 仍核对同 seq 元数据及容器一致性。

同批对同 ID 的多次更新按顺序作用于唯一成功前缀视图。每个成功 Append 的 prior/next TTL（包括中间版本）都加入提交期限；无关过期正文不阻断本批。上传或规划期间时钟推进时重新观察时间，过期候选返回 Busy、保留已知重放和未尝试范围。

持久性、部分失败和取消语义不变：先 WAL/远端权威，后内存/派生发布；未知结果不当作拒绝，失败保持封锁，已接受任务由独立 owner 完成。普通写不再复制 full base 或 full shadow；恢复和维护的完整状态克隆/校验保持原有边界。

### WAL 前的派生工作区预留

普通写在持久化前还通过 `RetrievalView::reserve_component` 预留向量/元数据输入、保留值与身份额度；全文根据适用大小选择可查增量或原生发布，并预留分析、身份、manifest/快照和 writer 工作区。缺失的空原生基底也在 WAL 前建立。可恢复的现有占用压力返回 Busy，配置超限、损坏和真实 I/O 错误仍按各自错误处理，不能一律降级为重试。

组件 credit 绑定实际 admission 代次，包含已接受但未应用的 pending 额度；应用时转移租约，不在确认后重新竞争同一份显式内存。取消尚未发布的计划退还完整预留，已启动的原生任务保留所有权至退出。固定旧快照持有的增量继续计费，不能在后台开始物化时提前退款。该协议约束显式预留，不保证所有分配都不会失败。

可查增量和原生物化的交接只改变派生视图，不推进业务序号；物化失败不能撤销已持久决定，仍须封锁并重开重放。具体视图与文件所有权见[可查全文尾段](architecture.md#wal-支撑的可查全文尾段)。

### 有界规划读取闭包

规划只保留本批成功 Append 的 Record/Applied 共享值与两个小 map。每项先查询成功前缀（墓碑也是值），再在 Inner 短锁下回读 canonical State 的目标 Record/operation。该 State 由同一个实际 writer 保护，期间不能安装 GC/TTL/维护结果。只读请求可以推进 transient evaluation_time、索引消费者可以推进派生状态，但不能改写回读基准。不会提前收集后项 ID 或提前校验后项。

唯一 mutation planner 接收明确的 Input：目标记录、相同 operation ID 的完整保留决定，以及整个候选的 seq/epoch/time、记录/操作/TTL 数量。向量补丁、TTL 继承和正文投影复用原 helper；Replay 不推进前缀、不增加 operation 或预留当前大正文。完整 State 的恢复/测试调用使用相同局部 helper，未引入另一套业务决定规则。

每个成功 Append 的全局字节更新为：

```text
values_next = values_before - prior_record_wire + next_record_wire + applied_wire
lookup_next = lookup_before + operation_id_wire
containers_next = snapshot_next - values_next + lookup_next
```

snapshot_next 继续使用原增量编码公式，包括全局空/非空的逗号、转义 key、序号/epoch/time 位数变化。每个成功前缀的 container 高水位都在持久化前为 canonical State 预留；不是只计算最终状态或小 map 大小。map 的 key/node 在插入前逐项取得独立内存租约，Shared 值保持原 Prepared 所有权。所有可失败检查、预算申请均在局部前缀修改之前完成。减少复制会改变可用资源余量，不能承诺低内存情况下与旧实现有相同的拒绝点。

收据 GC 的检查仍针对固定 canonical 前缀：本批新 operation 的 seq 大于固定 history.floor，不可能成为本轮 GC 对象。检查 epoch 取当前 UTC epoch 与成功候选 epoch watermark 的较大者，不能在午夜附近丢弃候选已观察的更高 epoch。这个 GC 前缀扫描仍可能与历史规模有关；查找仍有树索引成本，持久化和异步索引也有各自成本。因此不宣称整个写入已成为 O(batch)，或尾延迟、RSS 必然改善。
