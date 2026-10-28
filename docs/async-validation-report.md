# 异步架构实施与验证汇总

本轮基线为 master `50f88396af2e92fda391965353d5763462394ea5`，包含 #86/#87。工作区原先干净；实现、测试与基准均在新分支进行。只发布代码、测试和汇总，不发布原始实验数据、请求明细或本地日志。

## 实现

| 阶段 | 本轮实现 |
| --- | --- |
| 索引身份 | 全文与向量/元数据按稳定逻辑身份关联，允许物理行及组件拓扑不同。候选按版本及完整索引输入指纹复验；同版本向量补丁、删除、来源级联及组件故障仍隔离 |
| State/checkpoint | 长度与 lookup 计量复用已计量的共享值；过期回执由认证时间前缀替代，保留 epoch 拒绝水位；合并只引用选中记录，不克隆完整 State/操作账本 |
| 接收日志 | 终态释放命令正文；本地先选择完整新 generation 再删除旧代；远端先发布分块快照 CAS 再退休对象；保留绝对序号和旧身份拒绝水位 |
| Record 账本 | 固定维护捕获的删除集合，保留重试窗口、Feed 保留范围及必要租约证据；新 FeedTrim 可由完整提交时间推进私有控制流，旧事件保持原恢复规则 |
| 元数据容量 | 已确认删除的 intake 对象登记由持久水位保护后裁剪；未知 PUT 记录不遗忘。intake 命名空间的全部 inventory 状态受 `8 × max_operations + 256` 上限约束，并为 pending 终态预留对象名额 |
| 独立合并 | 全文内部小段合并；两份冻结全文/向量组件的有界派生缓存合并，锁外构建、短锁重映射安装、共享失效 token 与 reader pin 保护。权威 WAL/certificate 不因派生合并改变 |

202 仅代表命令已持久接收。业务成功回执、全文水位和向量水位分别核验，不把 202 当作立即可搜。回收预算耗尽时反压，不遗忘未知结果以恢复写入。

完整 State 驻内存、权威 checkpoint 的来源证明与认证发布仍保留。可选派生合并有输入、构建及组件预算；超限不能转化为已提交 WAL 的冷恢复失败，恢复使用私有缓存重建回退。generation ID 的永久精确拒绝复用集合仍受 `max_records` 硬上限约束。以上是明确的容量契约，不是无限容量承诺。

## 验证状态

2026-10-28 已按用户授权保存并发布 Draft PR #88，后续按独立小步持续提交。以下区分中间版本诊断与最终验收；专项通过不代表最新源码全量通过。原接管前 `/tmp` 日志丢失的运行不计入结果。

| 检查 | 当前结果 |
| --- | --- |
| Linux 完整 lib（固定测试二进制） | `08f485d`：417 通过、0 失败、12 忽略，1549.27 秒 |
| 服务端及全部其他 targets | `08f485d` 全部 34 个非 lib targets 通过；总计 35 targets、524 通过、0 失败、13 忽略 |
| 明确核心回归清单 | 最终二进制包含全部 146 个清单用例；Linux CI 清单已通过；Windows 无断言失败，但远端长测运行时触及 30 分钟 job 上限被取消 |
| 独立 core 默认/无默认特性 | `08f485d` 两种独立依赖图与编译均通过 |
| 文档示例 | `08f485d` doctest 9 项通过 |
| 静态文档、词法覆盖、Python、diff | `53749ee` 文档链接/品牌、完整词法矩阵源检查、Python 语法和 diff 通过；汇总更新后再检查 |
| 同数据同资源 Meilisearch 对比 | `08f485d` 基准暴露后台合并/native 写入 I/O 配额争用，尚未完成成对对比；修复后重新冻结 release |
| 远端最终 SHA 与 CI | `08f485d` static、双平台 lexical、Ubuntu core 通过；Windows core 被 job 时限取消，full-acceptance skipped |

最初诊断回归发现历史维护边界测试不符合异步 Busy、可变 tail 和合法时钟契约，均保留测试并修正其设置/断言；也发现运行中测试二进制被并行 cargo 替换会使子进程用例 ENOENT。最终运行使用固定副本，避免这类验证干扰。

## 真实环境与资源

本地初始约 30 GiB 可写磁盘、16 GiB 可用内存、4 CPU 配额。编译限制两个任务；基准顺序运行，服务绑 CPU 0–1、客户端绑 CPU 2–3，保持至少 4 GiB 空闲。下载、样本、日志及新建基准数据库预算为 2 GiB。没有创建凭据或计费资源，也没有清理现存实验数据。

真实 S3 阻塞：专用端点、桶、region、访问标识和密钥配置均缺失，变量名与最小隔离权限见[实施清单](async-completion-checklist.md#真实-s3-验收前提)。SQLite＋内存对象存储／本地 HTTP fixture 只提供协议与故障验证，不能算作真实云 S3 验收。

进程故障注入、退出与强杀不能替代系统/文件系统崩溃或硬断电。真实云与硬断电仍需独立环境验收，不阻止本轮代码以 Draft PR 供审查；不自动合并或部署。

## 本次发布的限制

Draft PR 包含代码、故障及并发测试和基准脚本，本次提交不是完整验收声明。完整诊断回归中的夹具及断言问题、checkpoint 对现有远端权威损坏的认证问题已分别修复并通过专项。小步改造中的中间失败会继续明确记录；只有最终固定二进制全量结果才能更新验收结论。性能、维护持锁长尾和 Meilisearch 对比尚无最终数据，不能声称达到性能目标。

## 尚未闭环的长期元数据保留

本轮持久水位裁剪只适用于显式登记的 intake 命名空间。通用 Record checkpoint、内容寻址对象和备份的 SQLite `retired_objects`／`object_uploads` 墓碑仍保留，尚未实现持续有界裁剪。这些键没有统一的单调分配序号；删除墓碑会使迟到上传或旧引用重新注册已退休键，破坏“退休后永不重新发布或 pin”的不变量。闭环需要为通用对象引入可认证的分配 epoch/水位，并在上传登记、所有发布入口、pin、备份与恢复中同时拒绝旧 epoch；还须证明迟到未知 PUT 不会在删除后复活。不能仅按删除成功或时间过期清表。本轮未改变这套通用协议，因此此项明确未完成。

generation 的可重建操作/租约证据已可回收；精确拒绝历史 generation ID 复用的 `used` 集合仍永久保留，并由已有全局 `max_records` 总数上限拒绝继续增长。此处实现的是有限容量下的安全反压，不是无限创建 generation 的有界滚动历史。若要永久运行且支持任意旧字符串 ID，必须改变身份契约（例如受认证 epoch 的 generation ID）或保留精确历史；单个普通哈希摘要无法支持任意 ID 的精确成员查询。

## 小步收尾进度

第一步：Records 容器增量维护 live TTL、retained Source 和 live typed 三项派生计数，去掉普通写入/预留中的对应 Record 全扫。持久化格式不变，恢复时重建并全扫校验；更新失败不修改计数。两项新容器测试与五项 TTL、generation、Source/abort/feed 预算回归共 7 项通过。generation staged reserve 和到期候选扫描仍待后续步骤，不宣称整个写路径为常数时间。

第二步：非持久化最早 TTL 下界避免未到期时遍历 Records；延期/删除允许保守早值，到达下界后仍完整扫描候选并刷新。更早更新即时下调，恢复重建，全扫 oracle 拒绝会漏过期限的缓存。9 项专项通过，含精确“未扫描”断言；到期扫描和 generation staging 扫描仍保留。

接收回收安全修复：替换快照选择前同时验证 pending 完成记录的字节预留（本地及远端各自编码系数），不足仅反压且保留旧链。两项新增精确容量边界测试和 ingest 全组 26 项通过。

后台 checkpoint 来源捕获：固定物理提交 cut 和 generation，每次借原句柄最多读取 64 KiB，恢复当次 cursor 后释放 Record 锁；帧拼装、校验和与分配在锁外执行。保持完整发布标记、序号、损坏及任务归属校验。新增 4 项、原 committed-stream 7 项、GC catch-up 1 项、indexer 8 项通过（另 1 个子进程辅助测试忽略）。64 KiB 限制 I/O 量而非设备等待时间；State 容器 clone、初始目录预算扫描仍在锁内，显式 compact 尚待下一步。

generation 预留统计：私有容器的 insert/get_mut 自动失效 OnceLock 三统计缓存，普通写复用 staged members/control/TTL 数量；旧 map wire 格式不变、固定缓存内存计量、恢复全扫 oracle 保留。11 项专项通过。身份占用检查和 staging 到期搜索仍扫描，不宣称全部 generation 路径常数时间。

远端候选保护基础：SQLite 一次事务预登记完整候选并创建无过期 staging pins，校验 owner/head、水位、全库存与 pending 名额；支持精确 build 释放。3 项专项通过，覆盖未知上传保护及备份恢复。此提交只是远端锁外回收的前置 API，尚未接入，不代表远端回收已解锁。

显式 Compact 中间步骤：迁移到 owned 后台候选路径，保留不推进时间/不额外 GC 的语义，保留 State delta/payload 复用及未知发布隔离，300 秒构建期限。10 项关键专项通过；修复了资源拒绝误 fence 和压缩后遗漏远端 GC。当前此中间版本重建 native 索引，3 项既有 native 包复用断言仍失败，未修改断言；下一独立步骤恢复安全不可变包复用，不能把本步称为完整验收。

本地 Spool 快照：持锁只捕获 Arc<Row> 和完整 revision/cut，锁外编码并同步新 generation，短锁在 cut 未变时选择 CURRENT 并整体换表，旧行与目录锁外释放。并发 Accept/Complete 使候选弃选重试，pending 完成预留在选择前复验；finish/admin 等待 owned GC。28 项 ingest 回归通过，包括压力下 pending 完成、stale cut、owner、未知选择恢复。持续写入仍可能反复弃选，不能宣称无饥饿；远端此时仍为串行压缩，后续接入独立预登记/pin。

显式 Compact 原生复用：固定 cut 时捕获不带共享失效 token 的 membership，锁外复用 immutable native 包并仅构建 tail；同版本向量补丁、并发删除由 catch-up 和安装复验处理。detached cache 或包数压力仍私有全重建，预算拒绝发生在 tail handoff 前。新增 3 项、既有 3 项原生复用原断言及 Compact/delta/远端专项通过；上一中间步骤的三项复用失败已解决。可复用 mutable tail 的有界 handoff 仍在 Record 锁内执行 I/O，且 metadata 捕获仍遍历索引成员，已单独计时，不声称所有工作均已移出锁。

远端 Spool 快照：固定 cut 编码与 PUT、候选释放/退休/DELETE/回执裁剪移到锁外，短锁内原子预登记完整候选与 pending 名额并复验 CAS。unknown publication 保留 pins，稳定 owner 恢复后才取消遗留 build pins；未知 PUT inventory 不遗忘。最终 ingestion 26 项和 SQLite staging 4 项通过，覆盖慢上传/删除下 Accept/Complete/status 推进、stale cut、finish 归属、未知选择及选中 manifest closure 保护。这是 SQLite/内存对象存储的协议验证，不是真实 S3。捕获 Arc 行、原子预登记及选择时可达性扫描仍在 Spool 锁内，append 的网络锁语义未改变。

远端维护进展：首次候选保留下一完整快照的对象名额，并持续计入 append 准入；首次 stale 或不足以保留第二候选时转为排空模式，新身份 Busy、旧身份回执照常、pending 继续完成，随后稳定构建。已知资源 Busy 解除临时门控与备用名额；未知结果仍隔离。30 项 ingestion 专项通过，包含 16 次并发新提交被背压后精确一次 stale/一次成功、低库存无首轮上传、pending 终态名额、精确分块边界无额外空块和 Busy 后 finish 可结束。首轮低库存测试失败是门控文件路径错误，修正路径后通过，未放宽超时。普通远端 append 的锁内网络仍会阻塞 status/progress 和 Complete，后续解耦需要单独的追加串行器及提交中状态，未混入此回收协议切片。

跨平台 CI 修复：Windows 串行磁盘重扫的既有断言得到 7 而非 8，目录枚举未反映仍打开文件的增长。Windows 分支改用 metadata-only 文件句柄读取真实长度，非 Windows 路径不变；不修改原断言或在测试里强制 flush。Linux build_budget 9 项通过，Windows 结果等待新 SHA CI。此差异对应 [Microsoft 对 NTFS 目录枚举属性时效的说明](https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-findfirstfilea)。`53749ee` 固定 Linux 全量运行继续收集；不会将旧二进制称作更新后 SHA 的重新执行。

完整回归终态与修复：`53749ee` 库测试完成 413 通过、4 失败、12 忽略（1533.54 秒），失败集中于三个 checkpoint 子进程边界/未认证清理夹具，以及 sealed owner 的 Compact。新 `checkpoint_with_tail` 在目录选择前已写完整 certificate/suffix，因此未认证清理夹具改为调用仍存在的旧发布路径制造真实未认证状态；新路径补齐 certified/switched/reclaimed 的真实测试中断点。Compact 恢复对 sealed cut 的合法维护，保留 seal 和永久输出屏障；普通 Index/Automatic 仍拒绝 sealed。四项原失败及 takeover、seal、indexer、checkpoint-with-tail 相邻组均专项通过，不放宽超时、不删除正确性断言。修复后重跑最终完整固定二进制回归，不能把 413/4 当作通过。

最终库回归证据：`08f485d` 的固定二进制完整运行再次通过三项原生复用原断言（direct tail seal、pending seals 隔离、successive flush）及四项 checkpoint/takeover 修复。12 个 ignored 中，11 个是由已执行父测试启动的子进程入口，另 1 个是真实 S3 条件测试；不能把忽略的真实云测试记为云验收。所有长测保留原数据规模。

基准发现的待修复问题：`08f485d` 在 1040 条持久接收、1024 条完成回执及索引核验后，后台合并占用 Background I/O 配额导致 native Directory 写入遇到 Busy，索引进入失败状态。该运行未完成查询、重启或 Meilisearch 对比，不构成性能验收。需在实际写入的资源准入边界处理争用，不能直接重试已部分执行的索引批次或隐藏真实 I/O 错误。

CI 时限诊断：Windows core 的本地 long-suffix 用例通过（734.37 秒），远端 long-suffix 仍在运行时整个 job 达到 30 分钟上限；GitHub annotation 明确报告 job 超时，没有用例断言失败。仅该矩阵 job 的预算调整为 45 分钟，保留测试数据规模、用例超时和 runner 数量；调整后仍须验证新的终态。
