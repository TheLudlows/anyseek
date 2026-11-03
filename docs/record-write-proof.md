# 普通写入的提交基准

本文描述职责收敛后本地实现的约束，不宣称已测得性能收益。恢复、重建、维护和诊断继续使用完整 State/SourceProof 校验；普通写不缓存“已验证”标志，也不根据 seq 相等推断状态未变。

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
2. mutation::prepare_at 对本次记录和最终 vector patch 文档验证字段、大小、向量数值、局部条件、时间、操作窗口、版本溢出及完整 checkpoint 字节预算。近期重试仍比较完整语义请求，不产生新决定。
3. state_memory::Prepared 在分配前取得独立值与容器租约。生产 Shared 值不可变；提交复用已规划的 Record/Applied，不重新生成另一套决定。
4. apply_prepared 检查连续 seq、非倒退提交时间、operation ID 唯一性及正文版本（向量后补保持正文版本）。记录 key 来自同一个 record_id；lookup 指向新追加 operation 的精确位置，lookup_bytes 用 checked_add 更新。
5. Records::insert_shared 增量维护值字节、TTL 数量和保守期限下界；operation 追加增量维护字节。snapshot_after 的精确长度增量算法及 reserve/admit 检查保留。它们不是未经验证的外部缓存。
6. history trim / expiry / GC / candidate install 继续在维护边界验证和安装；GC 重建 lookup，完整 proof 仍核对同 seq 元数据及容器一致性。

同批对同 ID 的多次更新仍按顺序作用于唯一 shadow。每个成功 Append 的 prior/next TTL（包括中间版本）都加入提交期限；无关过期正文不阻断本批。上传或规划期间时钟推进时重新观察时间，过期候选返回 Busy、保留已知重放和未尝试范围。

持久性、部分失败和取消语义不变：先 WAL/远端权威，后内存/派生发布；未知结果不当作拒绝，失败保持封锁，已接受任务由独立 owner 完成。该改动仅移除普通写的冗余 full base clone/比较；规划 shadow 仍是完整容器克隆，其成本仍随状态规模增长。
