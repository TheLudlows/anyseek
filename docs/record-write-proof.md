# 普通写入的提交基准

本文描述普通写入的所有权和增量规划约束；性能收益需由对应版本的生产路径测量验证。恢复、重建、维护和诊断继续使用完整 State/SourceProof 校验；普通写不缓存“已验证”标志，也不根据 seq 相等推断状态未变。

## 排他范围

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

## 同一 writer 自身的限制

排他锁不阻止其持有者自行修改状态。prepare→commit 的辅助函数必须保持如下契约：

- maintain_index_tail 只 drain/seal/rotate 派生索引；不得 trim/GC 或安装规范 State。
- ensure_journal_room / remote tail room 只校验或请求 checkpoint 后返回 Busy，不同步执行 checkpoint。
- prepare_remote_batch 与锁外 upload 不修改规范 State；远端对象上传有副作用，仍保留 head/owner 检查与 unknown 处理。
- admit_containers 可以增加 live State 的资源预留，不改变其序列化内容。

若未来将同步维护加入这些函数，必须重新建立提交基准；当前证明不适用。

## 结构合法性为何保持

锁只证明捕获基准未变。其合法性另由以下归纳链保持，而非每次普通写重复扫描全部记录和回执：

1. create/open 在公开实例前校验配置、数值向量、身份/lookup、容量、TTL/提交时间、history 和恢复来源；不可信 WAL、anchor 和 payload 仍完整验证。
2. mutation::prepare_input 对本次记录和最终 vector patch 文档验证字段、大小、向量数值、局部条件、时间、操作窗口、版本溢出及完整 checkpoint 字节预算。近期重试仍比较完整语义请求，不产生新决定。
3. state_memory::Prepared 在分配前取得独立值租约；PlanningView 为本批 map、调用方为 canonical 容器高水位分别预留租约。生产 Shared 值不可变；提交复用已规划的 Record/Applied，不重新生成另一套决定。
4. apply_prepared 检查连续 seq、非倒退提交时间、operation ID 唯一性及正文版本（向量后补保持正文版本）。记录 key 来自同一个 record_id；lookup 指向新追加 operation 的精确位置，lookup_bytes 用 checked_add 更新。
5. Records::insert_shared 增量维护值字节、TTL 数量和保守期限下界；operation 追加增量维护字节。snapshot_after 的精确长度增量算法及 reserve/admit 检查保留。它们不是未经验证的外部缓存。
6. history trim / expiry / GC / candidate install 继续在维护边界验证和安装；GC 重建 lookup，完整 proof 仍核对同 seq 元数据及容器一致性。

同批对同 ID 的多次更新按顺序作用于唯一成功前缀视图。每个成功 Append 的 prior/next TTL（包括中间版本）都加入提交期限；无关过期正文不阻断本批。上传或规划期间时钟推进时重新观察时间，过期候选返回 Busy、保留已知重放和未尝试范围。

持久性、部分失败和取消语义不变：先 WAL/远端权威，后内存/派生发布；未知结果不当作拒绝，失败保持封锁，已接受任务由独立 owner 完成。普通写不再复制 full base 或 full shadow；恢复和维护的完整状态克隆/校验保持原有边界。

## 有界规划读取闭包

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
