# A 批实施设计：本地不可变 Segment、发布与安全回收

状态：A 核心本地实现及 Linux 回归完成；Windows 原生与系统崩溃/断电仍未验收。实际检查点见 §11–20。基线为已合入 PR #70 的 `f73d3eb27ff4bf97149031069f49cac3e01966ab`。承接[核心路线 §3–5](implementation-57-core-roadmap.md#5-a本地-segmentmanifest-与回收)，本文件细化 A，不代替既有 P1/P2 契约。用户已确认优先核心能力、避免关系库和 Segment 冗余；以下物理布局与接口是工程方案，不能表述为已完成能力。

## 1. 交付和不做的事

交付：真实默认 Record 服务使用多不可变段与有界 tail 查询；flush/manifest 发布、恢复、等价 compaction、备份 pin 和安全 GC 可运行。成功仍是 durable + visible，staging 成功仍仅持久私有状态。不是把 checkpoint 改名，也不是新建旁路 API。

A 不引入 SQLite、对象存储、独立远程 Indexer/cache、多节点、自动 split/merge、模型或性能专项；不支持旧格式升级迁移。保留现有 HTTP/UI、授权和恢复隔离。先保留当前容量限制，替换完整快照限制时须给出独立的操作/字节/段数预算和验收，不仅提高常数。

## 2. 实施基线差距（后续落地见阶段记录）

- `record.rs::State` 同时保存 records、source_revisions、generations、operations；`Applied.request` 内又可能保存完整原请求正文。当前全量 JSON checkpoint 会携带这些内容，不能直接封装为每个 Segment 的元数据。
- `record.rs::build` 每次恢复创建 cache 并重新索引活动记录；单 `Inner.index`、ordinals/locations 绑定一个可变索引。A 需真正加载已认证段，仅重放 anchor 后的尾部，不能全量重建才可服务。
- `doc_store.rs::store_document` 只保存 schema.store 字段；它既可能重复权威正文，又不能替代完整原文。`vector.rs` 对检索向量归一化/量化，不能反推出原始输入。`active.rs` 的活动统计现绑定 Shard，跨段不能直接合并各段局部 BM25 分数。
- 当前 operation 重试有完整请求比较语义；不能为了去重存储，直接把所有请求都改成“短 hash 相等即相同”。复用完整 canonical 请求字节引用，保留现有已采用摘要清理路径的限定语义。

## 3. 唯一归属与副本预算

“唯一”是唯一业务权威及一份规范 payload；备份、WAL 恢复重叠、量化索引与短暂发布双版本各有必要理由，不承诺无物理副本。

| 数据 | A 的规范持久归属 | 可保留派生/短暂副本及回收条件 | B 的 SQLite 是否保存 |
|---|---|---|---|
| 完整 Document，含原始向量字段 | payload 区的一份 canonical 对象；Record、Source 修订、请求回执以引用共享 | tail 尚未落段时 WAL 携带；落段后只有恢复/回执/备份 pin 需要才保留旧字节 | 不保存 |
| 原始向量 | 上述完整 Document 内原始值；不另写 raw-vectors 全量镜像 | 检索向量是可重建的解码/归一化/量化表示；相同字节布局可直接引用同区，不能以量化副本替代原值 | 不保存 |
| docstore 投影 | 查询回表从规范 payload 读取，沿用 schema.store/fields 搜索投影；原始 Record/iterator 只按显式 fields 投影，Source 读取不受 store:false 阻断 | 新段不再持久写一份完整 JSON 投影；复用旧索引编码时关闭其正文存储，必要 offset 表可保留。低层独立 Index API 不被改变 | 不保存 |
| postings、词典、facet 值、ANN、邻接 | Segment 检索区，可由规范状态重建 | 必要访问路径，不是第二业务权威；复用现有 codec，不先造第二套完整格式 | 不保存 |
| Record 身份/路由、版本及墓碑、当前 payload 引用 | 不可变状态块 + tail 的有序决定 | 内存目录可重建；索引 ordinal→逻辑身份映射只存最少定位项 | 不逐 Record 建表 |
| Source 历史、证据、关系、generation 控制与私有成员 | 状态块引用 payload；重复出现的同一对象只引用，不复制整文 | 反向依赖/邻接索引可重建；私有正文受租约清理与回执保留约束 | 不建第二套状态机 |
| operation 请求/回执 | 去重键、canonical 请求引用、结果、epoch；共享已有 payload | 请求被替换仍可能因精确重试而需保留；只能按既有到期/清理契约退役 | B 仅为事务发布保留必要去重身份、摘要和结果引用，不复制业务请求全文 |
| feed | 有序内容无关事件、scope/floor、保留水位 | 事件不携正文；至少七天，容量不足背压。不可用索引水位替代 feed 保留证明 | 必要提交/保留协调位置或引用，不再复制全事件表 |
| 时间高水位、TTL/lease 终结、封存/恢复隔离 | 权威状态块与 tail | 内存缓存不能自行推导为未过期/未封存 | 仅必要 owner/恢复协调，不复制业务状态 |
| 活动 N/df/长度和安全上界 | 与同一可见集合认证的可重建统计块 | 每段保留增量贡献/定位，不给每段复制一份全局统计；视图共享统计上下文 | 不保存 |
| 段目录、长度/hash、格式与覆盖范围 | immutable manifest/descriptor | head 只存 manifest 引用，不重复完整成员清单 | manifest_ref；不再建可推导的文件/字段/段成员关系表 |
| head、owner/fence、提交及索引水位 | A 唯一本地发布协议 | 可认证恢复见证不是另一个可写 head | B 迁为唯一事务协调权威；不同时保留可独立发布的本地 root |
| 读/构建/备份 pin、退休任务 | A 本地生命周期协议；持久备份/构建保护可恢复 | 内存读 pin 在 owner 存活期有效；重启后先获独占锁，无旧 reader 才清理 | B 只存跨操作/节点竞争必须事务化的 pin/intent/退休身份 |
| key/grant | 当前 catalog 权威，不复制进各段 | 输出许可仍复核撤权与隐藏水位 | B 若迁入协调事务，替代原 catalog 权威，不双写授权数据库 |

PayloadRef 必须区分 body version 与物理提交修订：vector_patch 可不增加正文 version，因此不能仅用 `(record_id,version)` 作为唯一内容地址。采用受校验的对象/块 ID + offset/length，定位记录绑定业务版本和提交序号；内容 hash 用于校验而非授权。相同 hash 不自动跨租户共享；A 不引入全局内容去重服务。

## 4. 最少布局和模块

物理文件数由 codec 决定，不强制每个概念一份文件。最少布局：`head`/认证发布记录、`manifests/<id>`、`segments/<id>`（payload/state/search 区）、现有认证 WAL tail、临时构建区及退休登记。配置、编码版本、schema/embedding space 必须在恢复闭包可认证，不能只靠进程默认值。

- `segment/{writer,reader}`：有界编码、校验、正文引用、候选迭代；复用既有 postings/vector/facet 编码。只有完整验证后才生成 PreparedSegment。不能分配业务版本。
- `storage/manifest`：描述符、覆盖区间、组件认证与恢复 anchor；基于现有 partition/platform 同步及见证实现本地条件发布。不可用普通 rename 调用代替整个跨平台协议。
- `record/view`：一个 manifest + indexed I + committed C + tail ≤ C + 固定 evaluation_time + 统计/隐藏上下文。HTTP 不新增平行规则。
- `record` 原 mutation/recovery/publication：继续生成/重放业务决定。状态访问逐步改为 metadata+PayloadRef，不能在长期运行路径继续保留一份全量 State JSON 镜像。
- `storage/pins` 与 `segment/compaction`：局部、可恢复的保护/退休；先不增加通用任务框架或远程调度服务。

内部接口只覆盖 `prepare_segment(view, range, budget)`、`publish_manifest(expected_basis, prepared)`、`acquire_view()`、`retire/reclaim()` 四类职责。错误复用 rejected/conflict/outcome_unknown/durable_pending；接口名称可调整。当前 partition 串行提交与 catalog 输出屏障先保留，不把并发扩张作为 A 前置。

## 5. 关键流程和原子边界

**写入与读取**：先校验当前授权/owner/版本/operation 和容量，持久化唯一业务决定；完整更新 tail 正文、候选、来源/generation/TTL、活动统计后发布 C 才成功。持久见证已确认但 tail 应用失败，必须 fence 整个实例读写并返回 durable_pending，恢复完成前不能继续服务旧 C；持久见证未知则 outcome_unknown。取消不撤销已接管提交。查询固定视图后按稳定逻辑身份合并段与 tail；新版本、墓碑、失效屏障先于 Top-K 选择。回表与过滤不跨视图，响应仍复核当前隐藏/授权屏障。

各段与 tail 使用同一活动 N/df/长度，通道先全局排序后 RRF，不把局部融合结果再融合。优先将现有评分访问适配为共享只读上下文；无法证明安全的上界使用保守枚举，维持有界预算并显式 incomplete/失败，不假装精确。ANN 维持现有近似声明和预算；分段改变候选布局时必须说明，不承诺逐项 ANN 同序。精确模式 oracle 必须等价。

**Flush**：捕获受 pin 保护的 `(H,I,C)`，构建覆盖连续范围的段；同步文件/目录，校验引用和全组件统计后发布 manifest。发布校验基础/owner，将 I 推进至构建范围末尾；不得覆盖构建期间新增 C/tail。generation 成员完整集合仍由一个业务 Publish 决定，不按段逐块可见。未认证构建结果只能作为孤儿，恢复不能扫描到文件便自动采纳。

**恢复**：独占 owner → 验证 head/见证与 manifest → 验证引用闭包 → 加载状态/索引段 → 重放必要 tail → 恢复统计与输出屏障后服务。认证对象损坏失败关闭，不回退后丢已确认写入。允许受控重建派生索引，但正常恢复不能必须重新分词全库；缺失权威 payload 不可重建。未知发布以既有持久见证解析，不猜结果。

**Compaction**：受保护输入→等价输出→一次 manifest 发布→旧对象 retired。它不产生新业务版本/operation/feed 事件。只针对被选段与必要覆盖目录，不每次重写所有 payload。原文/向量重编码仅为压缩或布局必要，不改变语义。旧视图文件仍可读但不能绕过最新隐藏屏障。

**GC**：以可达引用和分语义水位证明，而非单一 min(indexed_seq)。读/构建 pin 先登记再验证发布世代；备份完成前保留临时保护，完成后收敛到准确引用闭包。过期任务必须失去发布权。旧 WAL 只有在新 anchor 完整认证业务状态、精确重试及 feed 等所需信息转存后才可截断；源历史仍可读，不能因暂无证据引用就删除。删除必须可重试，失败不撤销当前 head；Windows 映射及文件句柄全部释放后才物理回收。磁盘满先停止构建/背压，不能删除仍需恢复的对象腾空间。

## 6. 资源与防冗余验收

资源验收采用用户已确认的方案 1（详见 §20）：保守输入准入、显式缓冲限制和写入前共享逻辑磁盘扣账，不承诺全部分配或 RSS 硬上限。预算分别覆盖 tail 字节/操作、段数、构建输入输出、内存引用缓存、查询候选、未退休对象和备份保留；不让“分批”最终再聚合成无限大单 WAL/单内存对象。达到阈值先触发有限 flush/compaction，无法腾出正确性允许的空间才背压。

测试报告拆分：唯一 payload、检索派生、业务历史/回执/feed、备份 pin、临时构建、retired 未清理。不存在第二套持久全量 JSON docstore；规范原始向量只保存在 payload 中，索引表示差异单独计量。重复同一物理内容只引用，不做无依据跨版本去重。固定活跃数据循环更新后，无引用的索引和过期私有数据应收敛；有效 Source 历史/精确重试/备份本身可增长，必须解释并背压，不宣称恒定空间。

## 7. 分批实现与退出条件

| 步骤 | 改动落点与可运行交付 | 必须通过后才继续 |
|---|---|---|
| A1 段和规范正文访问 | segment codec/reader、PayloadRef、record 访问适配；真实 Record 数据→构建 PreparedSegment→校验读取，关闭重复 docstore；此步不推进生产 head/I、不截断 WAL | 全字段/缺 store/原始向量/补向量同 version、Source 历史/私有 generation/精确重试无损；格式/校验/长度/边界错误拒绝；不可只交类型壳 |
| A2 多段+tail 默认查询 | 先接通最小安全 manifest 发布/恢复骨架，再启用 view、shared scoring context、候选/回表/输出屏障 | 两段+tail 替换删除、stale/TTL/generation/关系融合、统计及稳定排序与有效集合重建 oracle 一致；正常 reopen 不全量重建索引 |
| A3 发布/恢复闭环 | 在 A2 发布/恢复骨架上补全故障矩阵，默认 checkpoint/备份/接管使用新闭包 | 构建期间新写不丢；同步/见证/发布各点故障与重试、取消、unknown、磁盘满、损坏、恢复隔离/接管均通过 |
| A4 compaction/GC | pin/退休、WAL anchor、备份/feed/回执保留 | 长读/旧 mmap、并发备份/构建取消、重复清理/重启、保留窗口边界和资源分项测量通过；无引用可回收项收敛 |

每步有本地代码、真实接口验证与阶段文档；阶段未集成不得称完整 A。A1 可用受测试内部入口验证物理路径，但 A2/A3 必须贯通默认服务，交付时不保留旁路权威。依赖共享契约稳定后才并行 codec、恢复协议、oracle、pin；不为拆 PR 再造数据副本。

## 8. 最低故障与测试矩阵

- 每个平台：文件写/同步/目录发布/见证前后退出，认证损坏，短写/磁盘满，重开与旧 owner；Windows mmap 释放与删除竞争必须原生执行。
- 业务：响应丢失原 ID 重试、版本/ABA、vector patch 不增正文版本、generation 批次不可半发布、来源删除/stale/TTL、多级证据、关系双端、撤权再授权不复活输出。
- 视图：flush/compaction 与新提交交错；活动统计、精确全文/向量/关系、过滤/iterator/count 共同 oracle；预算耗尽真实标记，缺必要 tail 失败关闭。
- 保留：七天 feed/过期 cursor、幂等保留、Source 历史读取、有效备份与受控接管、私有 staging 过期、pin 获取/退休交错。恢复不复活旧 grants。
- 默认/no-default core 与 server/server+fault-injection、完整 Rust/doctest、OpenAPI、HTTP/Chromium；不删除既有覆盖。最终 SHA Linux/Windows CI 分开记录。
- 系统崩溃/断电仍需可销毁隔离 VM/卷、硬重启控制及外部确认见证。无环境标未验收，不能用 kill 或 CI 代替；不得操作真实用户数据。

## 9. 关键决定与开始门槛

已确认：用户业务/可靠性契约、优先核心和避免冗余、SQLite 在 B 的唯一协调角色。工程决定：A 继续本地唯一权威，payload 共享引用、无重复搜索正文，保持同一服务与预算；这些不改变业务保留语义，无需新增产品确认。

A 可开始设计内分批实现，但不得绕过当前主干 CI 失败的诊断。若发现“减少副本”必须改变精确重试、来源历史可读性或已承诺向量/查询语义，应停止该取舍并报告；不能私自改产品合同。具体编码阈值和库版本是技术选择，按可测上限记录。B 的对象后端/部署条件仍待定，不阻塞 A；SQLite HA/跨主机接管不在 A 范围。

## 10. 设计检查点

独立只读审查后明确：搜索与原始读取投影分离；持久后应用失败封锁读写；默认多段查询启用必须晚于最小发布/恢复骨架。文档链接与 `git diff --check` 通过，仅设计和导航变更。PR #70 Windows run 39623397174 已观察到多批 generation 故障重启时内存分配失败，两个 Linux job 通过；新 master run 39623936785 尚在运行，需独立诊断，不把本设计当作该问题修复。

## 11. A 前置实现检查点：移除检索 JSON 副本

本地 `feat/57-a-segments`，设计提交 `780372fb6de71fd3c38bfc601ccc063ef42520fd` 后开始代码。`record.rs::retrieval_schema` 仅关闭派生索引的 store，原始配置保持不变；默认 Record 查询从权威 State 回表，HTTP 仍用原 schema.store/fields 投影。独立低层 Index API 不改。不存在存储格式迁移，恢复仍按既有权威日志重建当前派生索引。

新增真实 Record 测试覆盖 store:true 的正文/向量、额外未索引字段、全文/向量检索、checkpoint/reopen、原操作回执；确认原始向量不被归一化副本替代，派生索引 stored_field_names 为空。独立审查通过。最终代码 Linux：`cargo test --workspace --features server --all-targets` 为 34 suites、227 passed、0 failed、10 ignored；`cargo build --features server --bin anyseek_server` 成功；`record_service_smoke.py --browser` 26 个 PASS 检查点；文档链接/diff 检查通过。十个 ignored 仍为九个父测试进程 worker 和一个异构 AES fixture，并未视作全部运行。

仅为去冗余前置，尚未实现 Segment codec/PayloadRef、manifest、多段 tail、compaction 或 GC，不宣称 A1/A 完成。此次未重跑 fault-injection HTTP，Windows/容器/系统断电未运行；后续实际持久化改动必须完整故障矩阵。本次修改不修复或宣称消除下述 Windows 失败。

### 独立的合入主干 CI 结果

PR #70 的 head `2b69cb83fa344ebefaa151d6f993ef87b672a0df` / run [39623397174](https://github.com/TheLudlows/anyseek/actions/runs/39623397174)，及 master `f73d3eb27ff4bf97149031069f49cac3e01966ab` / run [39623936785](https://github.com/TheLudlows/anyseek/actions/runs/39623936785)，均为 build-and-smoke、Ubuntu 原生成功，Windows 原生失败。两次均在真实 HTTP 故障矩阵重新启动服务时报告 `memory allocation of 329744 bytes failed`；PR 是 `record_upload_smoke.py:53`，master 是 `record_lease_smoke.py:59`，调用 `record_service_smoke.py:25` 检测进程退出。说明实际分配失败，尚未证明根因；需分别测量多 collection 恢复的资源峰值和 runner 提交内存，不能无证据归为偶发。没有重跑、改主干或降低覆盖。

## 12. A1 进行中：payload 与 prepared 检索闭包

新增 `storage/segment.rs` 的有界 payload 编码、引用、校验与逐字节去重；`record/segment_state.rs` 只替换引擎拥有的正文槽位，不解释用户 JSON 中的同名字段。保留正文顺序、原始向量、同正文 version 的补向量修订、来源历史、私有 generation 与精确操作请求。读取受调用方预算约束，分别检查元数据、单体、引用数量及累计展开字节；任何追加/比较 I/O 失败后 writer 必须废弃。

`record/segment.rs` 已能从真实 Record 状态构建包含 payload、引用状态、既有 postings/vector 文件和活动统计的 prepared 闭包，校验文件清单/hash/schema/文档映射后直接打开既有索引，不重新分词。活动统计由 footprint 校验自洽；状态恢复复用幂等查找表重建和业务结构校验。独立审查发现的累计展开预算、I/O poison、恢复后 operation_lookup 丢失、目录遍历无界问题已修复。

**仍为内部 prepared 路径，不是已上线的不可变多段存储。** 当前预算参数只保证最终闭包接受上限，尚不是检索索引构建全过程的硬资源配额；检索打开仍复用可写 IndexArc，生产接入前必须限制 mutator。当前生产 State 仍持有原始对象，尚未切换为规范 payload 按需访问；默认多段 + tail、manifest 原子发布恢复、compaction/GC 均未完成。不得将本检查点称为完整 A1 或 A。

恢复后最终源码执行 `cargo test --workspace --features server --all-targets`：34 suites、234 passed、0 failed、10 ignored，退出码 0。七个新增专项覆盖精确请求/原始向量/来源历史/私有成员、引用损坏与放大预算、writer poison、真实全文和向量索引重开与重试冲突。忽略项沿用现有父进程 worker/AES fixture；未减少覆盖。本轮尚未重跑 HTTP/Chromium/fault-injection，Windows 原生、容器、系统断电均未运行。

本轮环境中断后，真实命令确认六个修改/新增源码文件和分支仍在；旧测试 session 40364 及 `/tmp/a1-full.log` 无法恢复，不计通过。已确认旧进程不在后重跑 session 21151，最终日志保存在工作区 `/workspace/anyseek-validation/a1-full.log`，结果如上。

Windows CI 的独立只读诊断：`index.rs` 的 2048 个空词典各预分配 1000 项，隔离分配实验复现与两次失败日志一致的 329744 字节分配，约 644 MiB/collection；多 collection 恢复是明确峰值来源候选。未获得 Windows 提交内存/pagefile 遥测，不能认定唯一根因。建议独立评估按需空词典分配，不改路由或降低覆盖；本检查点未修改该分配策略、master 或重新触发 CI。

## 13. A2 进行中：共享统计与跨段候选 oracle

`active.rs` 新增单份运行期 Corpus 聚合，段仍保留删除/恢复需要的本地 footprints。增删只调整对应贡献，不每次重建整个检索索引；Corpus 不序列化到每个段。`clone_local()` 明确复制本地数据而不共享可变 Corpus，attach/detach 显式登记贡献，不在 Drop 中改变其他视图。当前尚未接入默认生产多段生命周期，整次查询/发布仍须统一屏障；共享锁不能代替 ReadView。

真实两物理 shard 的候选合并与单一有效集合 oracle 覆盖替换、删除、新增、全文/精确向量/Hybrid、升降序及小分页窗口。修复了 oracle 暴露的三个布局相关问题：向量先确定全局候选窗口再展示排序；默认 RRF 候选数量按全局窗口而非 shard 数放大；带字段排序的词法通道先确定全局候选集合再按得分计算 RRF。默认权重/评分公式不变。

active Record 路径的显式 `_id` 排序现在在局部 Top-K 和 root 都使用逻辑 record_id，避免物理 ordinal 因更新或分段变化而改变选中对象；没有 active 的独立 Index API 继续使用物理 DocID。逻辑 ID 平分规则同时用于跨 shard 融合，早于候选截断。prepared 校验的字段编号范围使用 `index_lexical || index_vector`，增加 vector-first schema 覆盖，避免误拒合法向量先行 schema。

独立只读复审通过本轮候选/排序修复；仍明确要求生产接入时处理共享上下文错误的失败关闭、整次查询屏障、段替换与贡献退役原子性。最终源码 `cargo test --workspace --features server --all-targets` 退出 0：34 suites、236 passed、0 failed、10 ignored；日志 `/workspace/anyseek-validation/a2-full.log`。此次没有借用 A1 结果，也未运行 Windows、HTTP/Chromium、系统断电验证。该检查点是实际检索路径的前置正确性实现，尚非默认多段 + tail、manifest 或安全 GC 闭环。

## 14. A2 进行中：默认检索能力隔离与内存轮换

默认 Record 的 `Inner.index` 现由私有 `record/view.rs::RetrievalView` 管理。生产调用者不能取得原始 IndexArc；增量写入仅进入 tail，全文/向量查询和关系 facet 过滤走共同视图，删除/TTL 只更新对应分段的活动覆盖层。低层独立 Index API 不改，HTTP/UI 协议不变。物理地址保留 shard 映射的空槽，但物理版本统计不再把空槽计作记录。

内部轮换先验证独立单 shard tail、schema/向量配置、段数与地址预算，完整构造新映射与独立 Corpus；获取全部锁后一次切换，无异步取消点或可失败转换。独立审查发现并修复了逐项 attach/地址修改导致的失败半切换，以及冻结段旧频词缓存使用过期全局统计的问题。活动 Record 查询绕过该旧缓存，独立 Index 缓存行为保留。

新增真实 Record 写入 oracle 覆盖两次轮换后替换、删除、来源依赖隐藏、全文/精确向量/Hybrid 得分和统计；另覆盖预算拒绝后重试、锁等待期间取消后重试及旧频词缓存绕过。最终源码 `cargo test --workspace --features server --all-targets` 退出 0：34 suites、240 passed、0 failed、10 ignored。`cargo build --features server,fault-injection --bin anyseek_server` 成功，`record_service_smoke.py --server target/debug/anyseek_server --browser --faults` 退出 0，34 个 PASS 检查点。分别记录于工作区 `a-view-final-full.log`、`a-view-final-build.log`、`a-view-http-browser-faults.log`。未减少既有测试覆盖；十个忽略项仍是九个父测试调用的进程 worker 和一个异构 AES fixture。

**明确未完成**：轮换仍只有内部测试调用，默认持久化尚未启用 manifest/多段恢复；当前 checkpoint 仍是原完整状态协议。上述能力隔离及进程内切换不代表文件只读打开、持久发布或安全 GC 已完成。构建全过程硬资源限制、规范正文按需访问、默认 manifest/tail 恢复、compaction/GC 仍需贯通。没有推送、创建 PR、改 master 或运行 Windows/系统断电验证。

## 15. A 持久化前置：跨对象规范正文引用

`storage/payload_pool.rs` 为后续认证 manifest 准备明确的对象闭包，引用采用受限对象 ID 与对象内位置，不依赖 record_id/version 作为内容地址。连续构建先对旧对象进行长度/hash/完整字节核对，复用已有正文，仅把新字节追加到新对象；不扫描目录推断权威，不覆盖或删除旧对象。一个旧对象内尚有引用时保留整个对象，后续物理压缩属于 compaction。

`record/segment_state.rs::ReferencedState` 复用受控正文槽位遍历、operation_lookup 重建和完整业务恢复校验。真实两次 Source 修订测试证明当前正文、原请求回执与来源历史共享旧对象，新对象只有新修订正文；恢复结果与真实 State 完全一致。用户正文里的 `$payload` 仍只是普通数据。

预算在打开闭包前累计检查对象/正文数量及字节；单体读取在分配前取 pool、state 配置和本次恢复预算最小值；元数据在反序列化前检查长度。比较读取、创建或追加 I/O 失败后构建器必须废弃；预算预检拒绝不擅自删除旧数据腾空间。独立审查提出的创建失败 poison、较小状态预算前置检查已修复并补测试。

**范围限制**：这是 prepared 引用层，尚未替换默认持久化入口。候选闭包预算不等于整个目录预算，不包括尚未回收的孤儿、历史 manifest 和备份固定对象；State 在内存中仍包含完整正文。生产 manifest 必须原子认证 collection/谱系、状态及索引水位、对象闭包，并贯通 catalog 隔离与输出屏障；这些以及备份固定、compaction/GC 尚未完成。本层不自动删除任何旧对象，也未推送或发布。

最终源码 `cargo test --workspace --features server --all-targets`：34 suites、245 passed、0 failed、10 ignored，退出 0；带 `server,fault-injection` 的服务重新构建成功，真实 `record_service_smoke.py --server target/debug/anyseek_server --browser --faults` 重新运行退出 0，34 个 PASS 检查点。日志为工作区 `a-pool-final-full.log`、`a-pool-final-build.log`、`a-pool-http-browser-faults.log`，不是沿用上一提交结果。Windows 与系统断电仍未运行。

## 16. A 默认认证 anchor 与索引恢复（本地验证完成）

默认 `compact_journal` 已接入 `record/anchor.rs`：先持久化不可变正文对象、引用状态、现有检索索引及活动统计，完整校验后再由 `PartitionJournal` 发布唯一证书。候选目录不具备发布权；不扫描目录选取较新版本，不引入第二个 root。恢复核验认证闭包后直接打开索引，只将证书之后的日志决定重放到新的可写 tail。索引正文 JSON 仍关闭，原始正文及操作回执的正文槽位共享 payload 引用。

证书发布前准备新的完整查询视图；已知未发布保留原视图，不确定发布保持 fenced，恢复前不能输出。在线 checkpoint 使用已有 owned actor 保持取消安全。普通写入仅追加 tail，不修改已认证 anchor 的文件。Record 欧氏距离标量量化固定使用非仿射编码，避免首条向量决定不同分段的编码方式；独立 Index 接口未改，量化仍有原有精度限制。

回收在 partition 独占执行边界进行，先清理旧 journal，再删除当前证书和活动查询路径未引用的严格命名对象；恢复返回前也必须成功执行回收。独立备份仍导出完整 inline 权威状态，不借用运行中的对象路径。私有 staging 逻辑删除后强制重写正文闭包，避免旧混合对象保留已取消正文；证书成功但物理回收失败保留 pending，重启即使看到已清除 pending 的证书，也必须先成功回收才能开放。

故障注入明确区分未选中候选对象和日志权威切换；原 checkpoint 同步失败及恢复退休失败测试仍覆盖后者。新增测试覆盖认证索引与 tail 恢复、冻结文件不变、量化分数一致，以及私有清理证书成功后 GC 失败、重启关闭、恢复删除能力后重试回执不变。独立审查未发现新的明确阻塞，提出的 GC 重启交错已补测试。

**尚未完成**：当前 checkpoint 仍全量构建有效检索段，不能视为多轮增量冻结或最终 compaction。对象闭包上限是验收预算，不是全过程硬内存/磁盘预算；内存 State 仍保留完整正文。多段增量发布、等价 compaction 与构建硬限制继续实施。Windows 原生及系统崩溃/断电未运行，Linux 进程故障测试不能替代。

最终源码 Linux 验证：`cargo test --workspace --features server --all-targets` 为 34 suites、247 passed、0 failed、10 ignored；`cargo test --doc` 为 44 passed；`cargo build --features server,fault-injection --bin anyseek_server` 成功；真实 `record_service_smoke.py --server target/debug/anyseek_server --browser --faults` 退出 0、34 PASS。十个忽略项仍是九个父测试调用的进程 worker 和一个异构 AES fixture。日志为工作区 `a-anchor-full.log`、`a-anchor-doc.log`、`a-anchor-build.log`、`a-anchor-http-browser-faults.log`，不是沿用此前提交结果。

## 17. A 默认多轮冻结与有界段数 compaction（本地验证完成）

认证 anchor 选择独立 `segment-<id>` 检索闭包；状态/payload 引用与各段活动统计在同一证书下发布。后续 flush 只取当前 tail 的有效记录构建新段，旧段文件不复制、不重写；索引内旧物理版本通过 manifest 活动 footprint 和恢复时重建的删除集合排除。加载显式区分全部物理 ordinal 与活跃 locations，防止同一 ID 的旧版本被最后一次 map 插入错误选中。所有段加上新 tail 重新附着共同 Corpus，沿用既有稳定排序。

最多选择八个冻结段；到达上限后的下一次 flush 构建等价有效集合索引，原子替换所选段，不推进业务 seq/version/feed。全无效段可从下一证书移除；空集合保留一个空索引承载 schema。GC 保护证书列出的所有段，不能只按查询根和 tail 路径推断可达性。候选构建/校验失败不推进证书，旧段仅在新证书成功后退休。私有 staging 清理仍强制重写闭包，不复用含已取消正文的对象。

构建空索引不再为 2048 个路由桶各预分配 1000 个词项容量，按首次写入分配。该变更不改变 hash/评分/格式；它消除一项明确的空桶预分配，但不据此宣称旧 Windows 分配失败已经解决。新增空 tail 字典容量回归。

验证包括十轮新增/修订/flush/reopen、跨轮旧文件校验和不变、超过段数阈值 compaction、全文/向量/混合评分与共同 oracle、tail 重试、选中段损坏关闭服务；另覆盖旧段与 tail 的同 version 向量 patch 后来源级联失效、flush/reopen 不复活及 feed 不增加业务事件。

**仍需完成**：当前全量 compaction 在持有 partition 执行屏障时进行，不宣称后台无停顿；逐段文件验收预算不是整个构建过程硬内存/磁盘限额。原文 State 仍驻内存，备份保持独立 inline 权威状态。全过程资源预算与失败回归仍是 A 的未完成验收项；Windows 原生、系统断电未运行。最终源码 Linux：`cargo test --workspace --features server --all-targets` 34 suites、250 passed、0 failed、10 ignored；`cargo test --doc` 44 passed；带 `server,fault-injection` 构建成功；真实 HTTP/Chromium `--browser --faults` 退出 0、34 PASS。十个忽略项归属不变。日志为 `a-multisegment-final-full.log`、`a-multisegment-doc.log`、`a-multisegment-build.log`、`a-multisegment-http-browser-faults.log`。独立审查未发现新阻塞，建议的同 version 向量补写/来源失效交错已补入最终回归。

## 18. A 构建资源失败前置修复（本地验证完成）

`create_index_root/create_shard` 的目录、docstore/delete/facet/vector 文件打开、metadata、mmap 及 schema/meta 序列化失败改为 `Result`；索引头使用 `write_all` 并传播失败，不再忽略短写。并行 shard 构建失败后收集错误并等待全部已接受任务结束，避免调用方清理候选目录时仍有兄弟任务写入。

临时数据测试分别用目录占据 docstore、vector、schema 文件路径，验证错误正常返回而非 panic。第一 shard 失败时通过测试专用 gate 阻塞第二 shard，父调用必须等待 gate 释放、第二 shard 完成后才返回；关闭后删除目录验证句柄生命周期。该 gate 不进入生产构建。

本步骤仅修复错误传播，**不是全过程资源预算已完成**。内存分配 abort、tokenizer/term/posting 的增长预留、文件扩展前统一扣账及发布峰值仍需接通。受控分配请求与逻辑文件长度预算也不等于进程 RSS 或文件系统实际占用上限。最终源码 Linux 验证：`cargo test --workspace --features server --all-targets` 为 34 suites、251 passed、0 failed、10 ignored；`cargo test --doc` 为 44 passed；`cargo build --features server,fault-injection --bin anyseek_server` 成功；真实 HTTP/Chromium `--browser --faults` 退出 0、34 PASS。十个忽略项归属与前述一致。独立审查提出的兄弟任务等待验证已用 gate 回归补齐。日志：`a-resource-io-final-full.log`、`a-resource-io-doc.log`、`a-resource-io-build.log`、`a-resource-io-http-browser-faults.log`。Windows 原生及系统崩溃/断电仍未运行。

## 19. 未发布候选失败清理（本地验证完成）

候选构建/加载/新 tail 准备放在独立 future 作用域；错误返回后先释放全部局部索引和映射，再在 `healthy` 仍为真时回收当前认证引用之外的对象。清理失败保留原始错误并记录待清理状态，不谎报已释放容量。提交结果未知时 `healthy=false`，禁止按旧内存引用清理可能已经认证的新对象，必须由恢复判定权威。不会为腾出空间删除当前选中段、payload、tail 或仍需保留的日志。

回归覆盖候选文件已发布但未加载、candidate mmap 与新 tail 已准备两处拒绝，各重复三次：旧认证引用不变、孤儿文件不累计、旧副本可继续写入、再发布/重启与重试仍成功。独立审查未发现新的认证边界问题，并按建议补充了后一处句柄释放测试。最终源码 Linux：`cargo test --workspace --features server --all-targets` 为 34 suites、252 passed、0 failed、10 ignored；`cargo test --doc` 44 passed；故障注入服务构建成功；真实 HTTP/Chromium `--browser --faults` 退出 0、34 PASS。日志为 `a-candidate-cleanup-full.log`、`a-candidate-cleanup-doc.log`、`a-candidate-cleanup-build.log`、`a-candidate-cleanup-http-browser-faults.log`。

A 清单当前状态：默认多段+tail、认证发布及只重放 tail 恢复、多轮冻结与等价 compaction、选中引用安全 GC 已接入真实路径并有 Linux 回归；全过程共享内存/逻辑磁盘限额仍未接通，不用末尾文件大小检查冒充硬限额。Windows 原生及系统断电未验收；此阶段不推送或触发 CI。

## 20. 方案 1 资源准入与显式缓冲（本地验证完成）

用户于 2026-10-22 确认采用“保守准入 + 显式缓冲预算”。本节替代此前把 A 资源门槛笼统称作“全过程硬内存限制”的表述；历史检查点的测试结果不改写。**预算不是全部分配请求或进程 RSS 上限**，不精确计量 HashMap 桶、容器扩容临时内存、分配器开销、系统文件缓存及已有业务 State/查询缓存。

### 准入与内存生命周期

当前采用内部固定限额，不增加账户、调度或通用资源平台：单租约估算上限 256 MiB，共享估算额度 4 GiB，Record shard 的 posting/position scratch/压缩缓冲及 root 压缩工作区合计显式长度上限 64 MiB。4 GiB 是逻辑估算额度，不表示机器需要或必然只使用 4 GiB RAM。

构建在创建索引之前遍历选中的输入、统计序列化字节和候选词数量；估算包含 `32 × 正文字节` 的复制/规范化/向量工作量余量，以及 unigram/bigram/trigram、字段位置与词项描述符余量。每字段另预留数组/facet 工作量；ZH 另有 64 MiB 字典余量。非 ASCII 字符按逐字符候选计数，同时覆盖空白分词。它们是保守工程估算与背压规则，不是标准库 malloc 布局证明。恢复已认证段在打开索引之前预留字段/ZH 基础余量和两倍索引文件字节估算。

租约附着 Shard，旧视图及其映射仍存活时不提前释放。新 tail 的后续输入累积计入该租约；分词前另有暂态分析租约。已预审整个输入的冻结构建不重复累加相同输入。生产写入在 WAL 前检查单 tail 准入；空间不足先尝试一次已认证 flush，再检查并拒绝，不静默丢弃内容。全局额度可能被其他 collection 并发占用，因此不能声称所有资源失败都发生在 WAL 前：持久化后应用失败仍返回 DurablePending 并保持输出封锁，按原协议恢复。

posting 的全部增长在编码前计算并用 `try_reserve_exact` 预留；每位置最多三字节的 scratch 与 round-up posting capacity 一并检查。压缩前按已编码位置字节、posting 指针/docid 和 key 头部推导工作区上界；增长前检查合计额度，完成后复用缓冲，不再无条件重新分配固定工作区。构建拒绝不改变已认证视图；不是通过截断字段、词项或向量来满足预算。

### 磁盘、发布与回收

每个 partition 的逻辑目录预算沿用 `64 × max_journal_bytes + 32 MiB`，现在限制共享总量，而非只验收单段最终文件。构建在 partition 串行屏障下扫描真实目录：当前选中对象、缓存/tail、候选、WAL/见证及未删孤儿都计入；不按“计划删除”扣减。索引、payload、manifest/schema/statistics、WAL 与见证文件使用同一检查入口，在 write 或 set_len 扩展之前扣账；mmap 的文件长度先由该入口预留。

跨轮 flush 复用仍存活句柄持有的同根账本，下一次安全扫描才用实际文件重新核对。失败写入、truncate、unlink、rename 都不会立即退款；删除失败仍占额。打开文件随后 rename 可能产生保守重复计费，允许提前背压，但绝不能少计。registry 锁异常明确返回错误，不能退回未受限写入。目录扫描遵循持有 partition 屏障的前置条件，不支持外部进程任意修改存储目录。

只有明确未发布的候选在句柄释放后清理；未知发布继续保留恢复证据。文件系统实际占用、日志以外的服务备份目录不属于这个 partition 逻辑长度指标；独立备份仍按已有完整性与保留契约管理。该限制不能保证底层磁盘永远不满，真实 I/O 失败仍走既有错误/恢复协议。

### 验收映射与限制

| A 项目 | 当前实际落点 | 验收证据/边界 |
|---|---|---|
| A1 数据唯一归属 | payload pool + 引用闭包；Record 检索关闭 JSON docstore | 原正文/原始向量/版本与回执恢复已有回归；运行态 State 仍驻内存，不宣称全量按需加载 |
| A2 多段与 tail | 默认 RetrievalView、共同 Corpus、活动映射 | 多轮 oracle、旧版本过滤、稳定排序；本轮重跑全套 |
| A3 原子发布/恢复 | PartitionJournal 唯一证书，认证索引 + tail 重放 | 发布故障/损坏关闭、HTTP 进程故障；系统断电未验收 |
| A4 compaction/GC | 八段上限后等价合并、当前引用保护、失败重试 | 保持串行屏障，不宣称后台无停顿；预算失败不删除有效历史 |
| 方案 1 资源 | 构建准入、显式缓冲、共享逻辑文件检查 | 超限前检查、兄弟任务竞争、旧句柄跨轮/rename、保留文件占额、失败重试；不是完整 allocator 限额 |

低层 64K 物理块回归继续保留：夹具原本就直接灌入历史、跳过 WAL；现在明确将这部分作为预构造输入，避免生产准入提前 flush 使其失去跨块覆盖。最终写入和查询仍经过真实发布路径。该夹具不是生产可保留 65K tail 变更的容量证明；资源拒绝另有正常 Record 路径测试。

最终源码 Linux 验证：`cargo test --workspace --features server --all-targets` 退出 0，34 suites、257 passed、0 failed、10 ignored（九个由父测试调用的进程 worker、一个异构 AES fixture）；`cargo test --doc` 44 passed；`cargo check --no-default-features` 成功；`python3 scripts/check_core_boundary.py` 的独立外部消费者默认及无默认特性构建和依赖检查均通过；`cargo build --features server,fault-injection --bin anyseek_server` 成功；`PLAYWRIGHT_BROWSERS_PATH=/workspace/playwright-browsers /workspace/seekpower-tools/bin/python scripts/record_service_smoke.py --server target/debug/anyseek_server --browser --faults` 退出 0、34 PASS。日志位于工作区 `/workspace/anyseek-validation/`：`a-budget-verified-full.log`、`a-budget-verified-doc.log`、`a-budget-core-check.log`、`a-budget-core-boundary.log`、`a-budget-verified-build.log`、`a-budget-verified-http-browser-faults.log`。独立审查指出的 WAL 计费遗漏、旧句柄账本跨轮复用、registry 锁异常失败关闭和测试共享计数器干扰均已修复并回归；最终复审无新的实质阻塞。A 核心实现按本节限定范围完成，不代表全部平台可靠性验收完成。Windows 原生与隔离系统崩溃/断电仍未运行；旧 Windows CI 分配失败的候选原因不能因本轮改动或 Linux 通过而宣布已修复。无推送、PR 或主干变更。
