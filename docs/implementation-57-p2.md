# Issue #57 P2 实施设计：来源、证据与统一可见性

状态：**P2 设计草案；基础拆分进行中，来源语义未实现，P2 未验收**。本文第 1–10 节保留设计交付时的事实，当前实施进度见第 11 节。D1–D4 尚未答复，建议未转为已批准默认。

- 产品依据：[issue #57](https://github.com/TheLudlows/seekpower/issues/57)，包含顶部 2026-10-21 确认。
- 实施入口：[P1-A 与阶段记录](implementation-57.md)。本设计不替换总体路线。
- 代码基线：`9cb4be40b5e5b3cbca63a438f49df8482c90cbdb`，PR [#61](https://github.com/TheLudlows/seekpower/pull/61)；2026-10-21 只读核验为 **open / draft / 未合并**。
- 主干：`fc4fcfe64fdf86d03c07f82cf1d4889deb896801`；#56/#58/#59 已包括，#60 未包括。开始编码前重新核验，不能把本地文档分支当成已发布能力。
- 标签约定：**[确认]** 为 issue 或用户已确定要求；**[现状]** 为基线代码事实；**[建议]** 为待实施评审的工程方案；**[待决]** 为真正需要产品选择的语义。所有新类型名、字段、错误名和路径均为设计，不是现有 API。

## 1. 目标、非目标与前置门槛

[确认] P2 将通用可靠 Record 衔接为可以引用来源、整文替换、保存证据、按时间与显式关系检索、逻辑遗忘的单机底座。模型、解析、切块、embedding、实体消歧、归纳及重算由上层完成。保留 Web UI。多个 API key 共享 collection，访问能力统一；管理授权仍只由可信管理入口执行。

[确认] 来源删除确认前直接和间接派生内容立即隐藏；授权撤销阻断未交付输出和后续提交。范围为同 collection、同持久化 partition；跨 collection 来源/边请求明确拒绝。旧读视图也不能穿过删除或授权屏障。普通 generation 更新允许已经取得一致旧视图的查询完成，不能把这一规则扩大到删除和撤权。

非目标：SQL/JOIN、完整图语言、跨 collection 依赖事务、跨 partition 发布、账户平台、细粒度读写角色、模型运行时、自动语义连边、P3 Segment/长期 Compaction、P4 云 root 协议、升级迁移/旧格式兼容、性能专项研究。必要正确性、内存/磁盘/队列上限与资源失败测试仍必须执行。

P1 + P2 是首个功能版本目标；长期持续更新删除的生产验收另依赖 P3。P2 不得自行解除 P1-A 的 10,000 身份等资源限制来掩盖历史增长。

### 1.1 已有与缺口（代码依据）

| 领域 | P1-A 实际已有 | P2 必补，不应假定已有 |
| --- | --- | --- |
| Record | `Record{record_id,routing_key,version,document}`；完整替换，墓碑保留版本/路由；`ExpectedVersion::{Absent,Match,Any}` | 受引擎校验的系统元数据、来源精确版本、generation 成员、可引用历史版本 |
| 幂等 | `OperationId{epoch,nonce}`；当前/前一 UTC 日；同请求回执，不同请求冲突；超期显式拒绝 | 新命令共用同一 operation 命名空间与恢复结果，发布/向量/删除事件不能各自再造账本 |
| 持久化 | 完整 `Applied`/checkpoint，WAL 见证，持久后应用失败 fencing，取消安全 | 新状态全量纳入同一日志/checkpoint，分批 staging 不可成为第二权威库 |
| 可见性 | `RecordPartition` 锁内查询/回表/统计；`RecordCursor{visible_seq,after}` 变化即冲突 | 明确的 `ReadView` 边界及来源/generation/TTL/关系一致性；不是已有 MVCC 多版本快照服务 |
| 检索 | `active.rs` 增量活动统计、逻辑 ID 稳定排序、全文/向量/RRF | 多记录原子发布、TTL/来源变化统计差量、共同剪枝上界、可选向量、时间/实体/邻接访问 |
| 授权 | `Catalog::authorized` 覆盖完整 owned 工作；`OutputPermit::deliver` 在交付锁内复核 grant revision | 可与授权许可组合的数据隐藏许可，覆盖引用/边/计数/缓存/错误响应 |
| 恢复 | collection 与完整 service/catalog 备份，新位置恢复，历史 key 禁用/清 grants | 新来源版本、证据、generation、事件保留水位、Blob 依赖纳入同一恢复切面 |
| schema | 创建 collection 时冻结；自定义 JSON document，schema 控制索引/投影 | 系统字段独立命名空间；非向量普通字段当前不是严格通用 schema 校验器 |

源码：[record.rs](../seekpower/src/record.rs)、[active.rs](../seekpower/src/active.rs)、[catalog.rs](../seekpower/src/catalog.rs)、[journal.rs](../seekpower/src/storage/journal.rs)、[partition.rs](../seekpower/src/storage/partition.rs)、[vector.rs](../seekpower/src/vector.rs)、[record_service.rs](../seekpower_server/src/record_service.rs)、[api_types.rs](../seekpower_server/src/api_types.rs)、[service_snapshot.rs](../seekpower/src/catalog/service_snapshot.rs)。`record.rs` 顶部“尚未接入 HTTP”是历史注释，实际默认路由已接入；本轮不改功能文件。

### 1.2 可靠性前置项

[现状] 基线 [CI 39545934556](https://github.com/TheLudlows/seekpower/actions/runs/39545934556) 的 Linux 主任务及 Linux/Windows 原生任务成功；全套 163 passed / 0 failed / 10 ignored（说明见 [验证账本](validation/issue57-p1-checkpoint.json)）。真实 HTTP 两平台、Chromium Linux。它们是 P1-A 证据，不能作为 P2 证据。

[确认] Linux 与 Windows 都是验收平台。系统/文件系统崩溃与断电仍**未运行/未验收**；需要独立可丢弃 VM/机器、文件系统盘、外部强制复位/断电及外部已确认写入/撤销见证。不得重启用户或共享环境。P2 可先做设计和开发，但功能发布须继承未验收声明；整体可靠性门槛未过不能宣称 P1/P2 完整可靠性验收。草稿 PR 合入与前置接口冻结亦需独立协调，本文不授权合入。

## 2. 分层与依赖方向

[建议] 继续一个核心 crate、一个服务，不为了设计拆服务。先在现有 RecordPartition 内建立单一命令解释/提交/恢复入口，随后按职责抽模块；不在 HTTP 旁新增独立记忆存储。

```text
HTTP / Web UI / 上层任务（解析、embedding、重算）
                  ↓ 可信身份 + 命令/查询
Catalog 授权/管理 ─→ partition executor（唯一提交序）
                         ↓
source / generation / evidence / temporal / entity / relation
                         ↓ 纯验证、状态差量、事件描述
record + version directory + published ReadView + operation ledger
             ↓                          ↓
PartitionJournal / checkpoint       可重建检索适配
             ↓                   lexical/vector/time/entity/adjacency
现有平台同步与所有权适配
```

- `record` 拥有身份、版本、操作结果和通用提交协议；不依赖 HTTP、模型或账号。语义模块用纯状态转移计算已验证 `MutationPlan`，由 executor 在一个串行边界执行。恢复应用已确认决策，不重新检查当时权限或调用外部服务。
- `source` 管来源身份和不可变 revision；`generation` 管 staging/完整性/发布状态；`evidence` 管精确依赖、环检测与失效传播；`temporal` 管时间解释；`entity/relation` 管声明与一跳访问；`change_feed` 是同一提交历史的持久投影，不是独立提交序。
- `ReadView` 固定数据/配置/统计水位；输出屏障独立于数据快照。Catalog 仍是唯一授权权威，Record 不复制 key/grant 表。内核直接调用者必须持有其环境提供的可信权限上下文，不能把 payload 的 `tenant_id` 当授权。
- 检索组件只吃已验证 Record 及 `VisibilityContext`，返回稳定 ID/版本和通道分数。引擎按视图回表，不允许组件自行读“最新”正文。活动统计和索引缓存可重建，权威 Record/版本/证据/状态不得只存 posting。
- HTTP 只做解码、可信授权、预算、命令映射及受保护输出。UI 负责显示状态、保存完整重试请求，不在 JS 重演发布状态机；旧普通 Record 写入口也必须走统一验证器，不能绕过系统对象约束。

### 2.1 必要替换点与接口（设计签名）

| 契约 | 输入/输出与所有权 | 后续替换边界 |
| --- | --- | --- |
| `validate(command, state) -> MutationPlan` | 串行版本条件；完整原始输入、版本结果、依赖差量、事件、预算预留 | 语义模块无文件/网络操作；验证失败未提交 |
| `commit(plan) -> OperationReceipt` | 唯一 WAL durable → 全组件准备 → 单次发布；成功携带 seq/对象版本/可见性状态 | 继续封装现有 PartitionJournal，不先造 S3 通用事务 trait |
| `acquire_view(query) -> ReadViewGuard` | 固定 visible_seq、schema、generation map、版本目录、统计版本、evaluation_time；有界生命周期 | 第一实现沿用锁；P3 可改 Arc/pin，不承诺无限历史视图 |
| `candidate_channel(view, budget)` | 同一可见集合/评分上下文；有限枚举与 refill；返回完整性原因 | 包住现有 lexical/vector，P3 换 SegmentReader；不提前定义全套 Segment 格式 |
| `resolve_revision(ref, view)` | 精确 ID/incarnation/version；不可见、缺失、损坏分开 | P2 单 partition 精确目录；P6 路由目录/epoch 另协议 |
| `output_permit(view) + Catalog OutputPermit` | 输出时持锁校验并立即交付，不能先校验后异步写 | 只读派生状态，不成为独立持久权威 |
| `BlobStore`（仅管理型 Blob 需要） | `put_immutable/hash/len`、受保护读取、备份枚举、pin/retire；先 durable 再引用 | 首个实现本地，外部 URL 是另一种引用；P4 再认证对象存储 |

不把所有模块都做 trait；只有真实存在可替换实现和生命周期约束的边界才抽接口。P2 新权威状态进入同一个 checkpoint 状态树；可以拆代码，不能拆成各自成功的多个 WAL。

## 3. 数据模型与 API 草案

以下示例是**设计草案，当前服务不能接收**。沿用现有 `version` 与 `expected` 名称，不把 issue 的概念名 `record_version/expected_version` 误当现有 HTTP 字段。路径先按现有 `/api/v1/index/{collection_id}` 命名组织，正式实现时更新 OpenAPI 和 UI；不同时维护旁路 `/memory` 权威存储。

### 3.1 通用 Record 与系统元数据

[建议] Record 外壳扩为 `record_id,routing_key,version,document,system`。`document` 保留任意业务 JSON；schema 仍控制索引，`store:false` 不影响完整原文持久保存。`system` 为受引擎验证的 typed metadata，不能由业务 `document.system` 或任意标签替代。

| 系统属性 | 写入者/语义 |
| --- | --- |
| `kind` | 调用方声明可扩展业务类型；受限保留值用于 source/entity/relation 等系统对象，不据 kind 自动建立索引组 |
| `source_ref` / `evidence_refs` | 调用方提交精确引用，引擎验证同 collection、存在、版本、范围、依赖预算 |
| `generation_ref` | staging 命令赋值，普通 upsert 禁止修改成员归属 |
| `temporal` | 调用方提交 event_time/valid_from/valid_to/expire_at；ingested_at 由提交器生成并写 WAL |
| `vector_state` | 引擎派生 absent/pending/ready，失败原因由显式失败报告命令记录；不能仅相信客户端写 ready |
| `stale` | 引擎根据依赖版本派生，不能由调用方清除；重算需新证据快照和条件写 |

系统字段未知/矛盾/非法组合拒绝；普通 document 不升级为全套 JSON Schema 验证平台。reserved system object 只能通过语义命令变更。历史身份墓碑与版本不可由普通替换绕过。系统元数据的 author/tenant 等业务值不能赋予权限；source 管理和内容写入使用同一已授权共享访问能力，只有 key/grant/恢复控制面仍限 master。

### 3.2 来源、原始文件、证据和精确定位

[建议] Source 是同 partition 权威 typed object，共用通用版本/墓碑/命令序；采用显式对象种类标签防止与普通 record 同名歧义，不靠字符串前缀猜类型。Source 有稳定 `source_id`、不可变 `routing_key`、递增 `version`、`incarnation`、当前 revision、删除状态。删除后重建分配新 incarnation 且 version 继续增大；旧引用永不自动绑定新来源。

SourceRevision 是不可变内容版本，含 `source_version`、content_hash、media_type、原始内容或 BlobRef、定位编码。修正来源创建新 revision，不覆盖旧版本。Document 是来源的分块发布控制对象（`document_id`、source_ref、active_generation）；一个来源可有多个处理结果，先限制每个 document 的一个 active_generation。chunks 与 document 使用相同 routing_key，调用方不得迁移已有 ID。任意 ID 查询依赖精确目录，不采用 hash(id)%partition_count。

`EvidenceRef` 指向 SourceRevision 或精确 RecordRevision，包含目标种类、ID、incarnation（适用时）、version、可选 locator。直接引用 source 是直接证据；引用另一个派生 RecordRevision 是间接证据。`source_ref`、generation 的 source_ref 和 evidence_refs 一起进入规范化依赖集合，generation 成员隐式继承其来源依赖；不能只遍历 evidence_refs 而漏掉文档来源。每次更新保留被引用的不可变修订，不能仅靠 P1 当前值 BTreeMap 或会回收的 operation ledger 充当永久证据历史。版本目录与历史修订均为权威 checkpoint 内容；倒排依赖索引可重建。

[建议] locator 首版支持 UTF-8 字节半开区间 `[start,end)`（以被哈希的原文为准）、一基页码和可选 chunk_id；不能混用字符/字节偏移。页码由上层解析报告，底座只校验非负范围/已知长度等可验证约束，不承诺理解 PDF。外部 URL 不保证源文件仍存在；API 返回 `external_reference` 和可用性未知。管理型 Blob 的完整内容/hash 必须先同步，发布只引用认证完成对象；不得在回放时下载 URL 或解析原文。

[建议] source 删除是源身份当前 incarnation 的持久屏障，对其所有 revision 的直接/间接派生生效；保留精确版本供受控审计/备份不等于允许普通查询返回。普通 RecordRevision 被显式删除时也作为证据屏障根；其间接依赖同样按统一闭包隐藏，不能因为对象不是 Source 类型而绕过。保留的历史版本只通过受保护的精确证据读取访问；其 source 已删除或祖先被隐藏时也拒绝，不能以“审计”名义增加普通读取后门。跨 collection/partition 引用在写前返回明确 `unsupported_scope`，不得降级成未验证引用。

### 3.3 写入与条件补向量示例

设计请求（`epoch` 示例仅作结构说明，实际需客户端当前 UTC 日）：

```json
{
  "operation_id": {"epoch": 20747, "nonce": "save-fact-01"},
  "record_id": "fact-01",
  "routing_key": "meeting-01",
  "expected": "Absent",
  "document": {"text": "项目计划于十二月发布", "tags": ["计划"]},
  "system": {
    "kind": "fact",
    "evidence_refs": [{"target_type": "source", "source_id": "meeting-01", "incarnation": 1, "source_version": 3, "locator": {"utf8_start": 0, "utf8_end": 36}}],
    "temporal": {"event_time": "2026-10-21T08:00:00Z"}
  }
}
```

[建议] 可选向量需改造 `vector::validate_document_vectors` 和索引 ordinal 映射：缺失就不入向量通道，绝不能填零向量；全文仍可检索。配置的向量字段存在时继续严格验证类型、维度、精度、有限数值/非零 Cosine、embedding_space_id。每个字段记录状态，整体 ready 由所需字段集合决定。

后补向量用设计命令 `AttachVector{operation_id,record_id,routing_key,expected:{Match:4},embedding_space_id,field,values,input_content_hash}`。提交前验证版本/空间/原文 hash，并将原始向量、版本推进及 ANN 发布一次提交。旧上层任务不能把旧原文的 embedding 写到新原文；冲突后重新读并重算。错误分类和取消安全沿用 P1。通用更新修改 embedding 输入却沿用旧向量时拒绝或明确清空为 pending，不自动声称 ready；具体受哪些字段影响由 collection 向量输入声明冻结。

Generation 成员补向量是唯一受控的成员增补命令，不等于开放普通成员 upsert。建议状态规则：staging 时可 AttachVector，更新 staging 清单版本；sealed 未发布时禁止修改（需要新 generation，或在 seal 前补齐）；published 的 lexical-ready generation 允许 AttachVector，但必须同时 Match 当前成员版本、active_generation 和 enrichment_revision。sealed 清单永远描述初次发布输入，不改其 hash；单一权威 generation 状态另持久保存 enrichment_revision 及成员当前 revision 映射，作为完整 ReadView 的组成部分。补写一次推进成员 version/enrichment_revision，发布向量、当前正文与计数，不改变原始 chunk 内容/来源/顺序；checkpoint、备份及恢复包含 sealed manifest 加全部仍必要的 enrichment 状态。旧视图读旧映射；已退休 generation 拒绝补写。纯向量增补事件标明 content_changed:false，不把证据原文未变误当事实失效；精确历史引用仍指向原 revision，不漂移。

### 3.4 Generation 状态与发布示例

[建议] `BeginGeneration → StageBatch* → Seal → Publish`；终态为 published 或 aborted/expired。generation_id 由调用方稳定提供，含 document/source 精确版本、expected_generation、清单摘要、chunk 数量、就绪策略、截止时间/lease revision。StageBatch 每批独立 operation_id；重试不能增加重复成员。活动发布历史永不重用 generation ID。

```json
{
  "command": "PublishGeneration",
  "operation_id": {"epoch": 20747, "nonce": "publish-guide-g2"},
  "document_id": "guide",
  "routing_key": "guide",
  "generation_id": "g2",
  "expected_generation": "g1",
  "source_ref": {"source_id": "guide-source", "incarnation": 1, "source_version": 2},
  "manifest_hash": "sha256:<sealed-manifest>",
  "expected_document_version": 7
}
```

设计回执：`{"seq":42,"document_version":8,"active_generation":"g2","visibility":"published","operation_id":{...}}`。StageBatch 回执必须写 `visibility:"staged"`，其 durable seq 不代表可搜索。seal 后清单不可改，原文/成员/来源修正需新 generation；发布后的受控向量增补按 §3.3 的 enrichment 协议，不改 sealed 清单。检查 chunk 集合/顺序/内容 hash/数量，不能只看数量。使用新 chunk ID 可简化历史引用；若复用业务 ID，必须按 generation+version 保存修订，不能覆盖 active 内容。普通写入口对 generation 成员拒绝绕过 Publish 的单条变更。

### 3.5 时间、实体、关系及变更流

- [建议] 时间采用 UTC 整数微秒内部表示、API RFC3339（拒绝不带时区/溢出），`[valid_from,valid_to)`；`event_time` 是业务发生时间，不是序号。无 expire_at 默认永不过期；event_time 不触发隐式 TTL。event_time 范围作为硬过滤，近期偏好仅显式请求；首片先实现范围过滤，再实现有预算的时间候选与命中通道解释，不擅自修改默认排序。
- [建议] Entity 是稳定声明（entity_id、版本、业务 document），引用实体不自动合并名字。Relation 是权威 typed Record（edge_id、版本、from/to 类型和 ID、relation_type、可选有限 weight、evidence_refs、时间属性）。首版端点同 collection/partition，可选 expected endpoint version 在写时防止竞态；边本身不默认为内容证据，需 evidence_ref 才表示可追溯依赖。查询同时检查边及两端可见性，禁止隐藏端点的名称/计数泄漏。
- [建议] 一跳从已授权可见 seeds 扩展，支持 direction/type，固定种子顺序、邻居/边稳定 ID 排序与总预算；不是任意遍历语言。关系循环合法（普通图），证据依赖循环拒绝，两者不能混淆。实体倒排与正反邻接只是可重建索引。
- [建议] feed 事件含 partition_id、lineage、seq、event_index、event_type、对象 ID/版本、operation_id、affected generation、cause；同一命令可生成多个事件，用 `(lineage,seq,event_index)` 去重。SourceUpdated/Deleted、GenerationPublished、RecordChanged、VectorReady、TTLExpired、RelationChanged 由同一命令决定并持久保存。至少一次分页，客户端持久游标后自行处理重复；无 exactly-once 承诺。
- 设计 feed 请求 `{"after":"opaque-token","limit":100}`；响应含 `events,next,retention_floor,visible_seq`。feed 默认只返回变更标识/版本/类型，不携带旧正文、证据片段或任意业务 metadata；消费者用当前受保护读取获得内容，删除事件不能成为读取已隐藏内容的通道。token 绑定 collection/partition lineage/格式，不是授权凭证；每页重新授权和受保护输出。超期返回 `cursor_expired` 及重建所需快照入口，不能悄悄跳到现有尾部。当前 `RecordCursor` 是“位置变化即冲突”的迭代器，不能直接充当 feed cursor。

## 4. 一致性、发布与隐藏协议

### 4.1 同一提交协议

[建议] 新命令仍先授权，再在 partition 串行执行：校验语义/版本/期限 → 有界预留资源与准备差量 → WAL 持久见证 → 完成全部组件 → 原子替换 published state → 回执。持久后失败返回 `DurablePending` 并 fence；同步结果未知返回 `OutcomeUnknown`，绝不伪装未写。恢复重放已记录计划，不重算旧 expected 条件。

新 command operation_id 与原 Record 共用 collection 级账本：跨命令复用同一 ID 属于冲突。沿用当前/前一 UTC 日、持久 epoch watermark、明确过期；generation 可长期存在，但各次续租/发布用各自保存的操作 ID。操作查询不存在不证明未提交；超时/取消 UI 保留原请求，known rejected 才可安全编辑重发。读超时不让后台任务持无限 pin；写接受后的 owned 工作直到安全终态，不能释放授权串行边界后继续偷偷提交。

### 4.2 Generation 发布共同集合

发布前在资源预算内准备新 generation 的全文/向量/时间/实体/邻接与活动统计差量；staged chunks 永不进入普通候选。Publish 重新验证授权、document/version、expected_generation、source/evidence 当前屏障、sealed 清单、向量就绪和租约 fencing。

单个发布点同时切换：active_generation、有效版本目录、原始正文映射、各通道可见位图、N/df/字段长度和均值、结果计数口径、评分缓存与剪枝上界。旧上下界无法证明安全时禁用对应剪枝走有界精确路径，不能沿用过小上界漏结果。不得“先切正文，再后台更新 BM25”。

[建议] 首版沿用锁保护查询完成与发布，减少 MVCC 并行复杂度；准备工作可以在锁外基于固定 sealed 输入，但最终验证与发布必须串行。P1 发布后立即改同一 mutable 索引的接口需收拢成内部 multi-record plan；不能调用 N 次公开 write 冒充原子发布。多批 staging 的 payload 已 durable，Publish 日志只引用已认证清单与确定差量，不超过 16 MiB 帧上限；每批也有上限。durable_seq/visible_seq 表示已完整应用命令前缀，staging 的“可见”仅在控制状态中，不进入检索有效集合。

### 4.3 来源删除与输出屏障

[确认] 删除要求比普通 generation 更新更强。source tombstone 与隐藏屏障同一 durable 命令；删除确认前所有受影响派生记录/边在新查询不可见，旧视图不得继续输出相关结果。

[建议] 有界闭包遍历直接/间接精确 evidence 引用，计算隐藏记录集合及统计退役差量；缺失不可验证依赖、损坏或超预算一律失败关闭，不能少遍历几层当作完成。插入证据时限制最大深度/边数/总节点，使 collection 级删除闭包始终有硬上限。达到预留容量时拒绝新增依赖；合法已有来源的删除不得因普通写配额耗尽而无法执行，预留 tombstone/屏障空间和应急 checkpoint 预算。

查询取得数据 `ReadView` 与 collection 数据屏障 revision；输出时同时持 Catalog 输出读许可和数据屏障读许可，在同一同步 handoff 中核验并交付。推荐初版任何 source 删除使该 collection 所有旧响应许可失效（保守取消，避免在输出处重新算统计/重排）。删除在持久化/应用过程中把相关输出状态设为 pending/fenced，未知或 durable_pending 时继续阻断直到恢复确定结果，不能在返回错误后泄漏旧数据。持久成功更新屏障 revision，再放行新视图。删除成功回执使用发布后的数据屏障 revision 签发许可，不能复用入口旧许可而取消自己的回执；unknown/durable_pending 仅允许受限无内容错误状态输出，不能借错误带出旧正文。

锁顺序固定：catalog 操作所有者 → partition writer → 短生命周期输出锁；组合输出只取 catalog output → data output 的读锁，持锁期间不得 await、回调写入或取 partition mutex。删除/撤权不能持输出写锁等待被它阻塞的响应完成。测试穷举该顺序交错。已经交给网络的字节不能收回；每个后续 frame、错误正文/头、证据预览、导出、feed、计数与缓存同样走许可。旧响应被取消，不能删掉几个 hits 却继续返回旧 count/score。

[建议] 结果缓存键含 collection、授权 revision、数据屏障 revision、view/统计版本与 evaluation_time；缓存命中仍要输出许可。大 Blob 下载每块检查同样许可，直接返回绕过服务的永久公开 URL 不构成受保护读取。

### 4.4 来源更新、重算与循环

[确认] 更新不是删除；证据永远引用原版本，不能把旧引用偷偷指向最新来源。上层消费 feed 重算；底座不执行 reflect。

[建议，待 D1 确认] 来源更新保留旧 revision，将依赖旧内容的派生记录标 stale，默认普通 recall 隐藏 stale；可选显式 include_stale 读取仍有效且未删除的历史证据。这里的隐藏策略未获用户批准。独立 source 更新先令旧派生 stale，再发布新 generation；若要零空窗整文替换，建议 `PublishGeneration` 可同时激活其预先 staged SourceRevision，作为一个 MutationPlan，不能连续两个成功 API 冒充原子。

重算用新 operation_id + Match 派生 record 版本 + 精确新 evidence 版本，提交时再次验证 source 屏障。证据环在提交前通过有界 DFS 检查，含自引用/跨多个新成员的环；批量命令对最终候选图检查，不能逐条绕过。缺失依赖不按“暂时允许”发布。source 删除和旧重算并发，按串行顺序确定；删除后重算不能恢复旧 incarnation 的内容。

### 4.5 TTL、时间与统计

[确认] 每次查询固定 evaluation_time，所有通道/回表/关系/计数使用同一值。TTL 先隐藏后回收；不能等待异步定时器才从普通查询消失。

[建议] 只允许可信服务时钟设置当前 evaluation_time，客户端业务 as_of 不能复活已删除/TTL 过期内容。获取视图时，基于截止时间有序索引处理到期集合并生成视图的有效位图/统计差量；新视图的 N/df/长度/剪枝也排除过期记录。过期系统命令的事件与不可逆隐藏水位必须在首次向外返回受该 TTL 影响的结果前 durable；查询触发推进也要通过 executor，拿锁前后遵循授权顺序，不在读锁内升级造成死锁。若同步失败，本次读取失败关闭，不能先给出“已过期”再异步落盘。后台仅负责提前推进而不是唯一正确性来源。积压超出预算时等待/背压或显式失败，不以旧统计输出。

[建议，待 D3 确认] TTL 作为不可逆逻辑遗忘：已过期身份保留墓碑；客户端不能通过晚到延长 TTL 恢复，须明确新版本/新生命周期操作。记录持久时间水位以避免重启时钟回退复活；服务发现明显时钟异常进入时间请求失败状态，运行文档声明时钟校准预算。普通未到期记录可 Match 修改 expire_at。

同一已固定查询允许按其 evaluation_time 完成（即使处理期间时间推进），但显式 source 删除/撤权仍立即取消其输出。TTL 的直接/间接派生传播需采用 D3 策略，默认建议同来源删除隐藏；不能在 count/关系上保留另一套 TTL 解释。

## 5. 恢复、备份与回收

### 5.1 完整恢复状态

[建议] checkpoint 新增 typed 元数据、SourceRevision/RecordRevision 保留目录、document generation 清单及状态、staging lease/fence、evidence 图的权威引用、时间水位、实体/关系原始记录、feed 保留事件/floor/lineage、幂等结果、BlobRef 可达清单。原始全文/向量和配置继续保留。倒排/ANN/时间索引/邻接/反向依赖可丢弃重建，但服务在完成全组件与统计重建前保持不可用。

使用有格式版本和严格拒绝的全新 checkpoint/schema；不实现 P1 文件到 P2 的升级转换。文档明确重新初始化或从相同 P2 格式恢复，不能承诺 PR #61 数据可直接加载。恢复/备份并不是历史迁移。

source/generation/实体/关系都在一个 partition 日志切面中，service snapshot 继续由 catalog barrier 冻结各 collection cut。管理 Blob 在引用发布前 durable，快照遍历可达对象并校验 hash；外部 URL 原样保存并标明不含文件。staging 若纳入 checkpoint 必须连同内容/lease一起备份，不可只备份清单。恢复到新位置后未完成上传租约失效，需新授权显式续租且改变 fence；已发布内容完整保留。

完整 service 恢复继续禁用历史 keys/清 grants，只当前可信 master 可访问；旧控制 operation 结果不得复活授权。collection 恢复也不带旧 grants。数据操作去重、版本 ABA、source 删除屏障必须保留。当前授权与历史审计分离，普通读不能通过“历史证据”绕过已撤销 key 或 source 屏障。

必须区分恢复到 snapshot cut 与恢复所有后来删除：早于删除的备份不包含未来 tombstone，不能凭空保证不复活。建议 P2 恢复根默认保持数据恢复隔离，除了当前 master 的恢复校验入口不开放普通内容读取/新 grants；只有从可信当前状态或独立保存的删除/遗忘水位导入抑制清单并验证覆盖到要求的水位，才可显式激活。清单包含 source incarnation 删除与 TTL 遗忘，和归档 hash/cut 绑定；认证/持久化后再重建有效集合。缺失必要删除历史则保持隔离并报告可恢复水位，不能把“新位置+新 key”当成恢复后隐藏正确的证明。该扩展需要 P2-7 的恢复状态机及测试；不声称 P1 已实现。数据与授权恢复均不自动撤销当前可信管理入口。

### 5.2 Feed 与日志生命周期

[建议] feed 由提交状态保存有界事件窗口，checkpoint 含窗口；不能直接暴露将被 P1 compact_journal 删除的物理 WAL offset。数据提交与事件必须一起 durable，事件不可异步补写，否则崩溃会漏通知。TTLExpired 用持久化过期命令的唯一事件身份，重放不重复逻辑过期。

默认建议配置保留 7 天且设置字节/条数硬上限（D4）；达到容量不能静默突破保证：在过期窗口释放前背压普通新变更，提供告警。source 删除/TTL 隐藏不能被普通 feed 配额堵住：预留覆盖最大有效身份数的紧急墓碑、operation receipt、闭包状态、完整可恢复 checkpoint 和固定大小聚合事件空间，cascade 不强制为每个派生输出独立事件；聚合事件携带 cause 和闭包范围/重建指示，消费者重新读取受保护状态。容量预留在创建身份/依赖时核算；恢复必须保留此预算，授权撤销继续走独立 catalog 权威；P1 当前容量耗尽会 fence 而非保证撤销 durable 成功，P2 不得弱化这个失败关闭行为。若要求满额下撤销仍可提交，须明确新增 catalog 预算预留并测试，不能声称现有保证。消费者离线超过 floor 必须重新 bootstrap；不按最慢消费者无限 pin WAL。长查询/备份/staging与合法 feed 窗口的恢复依赖分别管理，不能用单一“最小 seq”概括所有非连续引用。

设计 bootstrap 协议：在同一 barrier 获取数据快照 cut 与 feed continuation；有界 pin 保护分页快照/事件窗口，客户端完成初始枚举后从 cut 后消费，允许重复不得遗漏。P1 iterator 每次写入就失效，不能直接用于不停写的 feed 重建；首版可以在已存在 immutable checkpoint 上有界枚举，租约到期显式重试。备份恢复创建新 feed lineage（防止恢复后新事件与原时间线相同 seq 碰撞），旧 token 明确失效；持久事件仍保留用于审计和重新 bootstrap，数据 operation_id 不因此重用。

### 5.3 P2 必要回收与 P3 分界

P2 只回收可证明未发布的过期 staging、失去引用且超过 pin 的私有 Blob、过期 feed 窗口和已有安全 WAL generations；不实现通用 Segment compaction。任务领取/续租带单调 fence；GC 先持久退休标记，再等有效读者/备份/上传 pin 释放，再物理删除；旧上传进程不能在退休后发布。重启后旧进程 pin 不再有效，持久租约按协议判定，不能按文件 mtime 猜。

权威 revision/墓碑不得因为 feed 过期就删掉；还需证明无证据引用、无 generation/备份/恢复依赖及 ABA/幂等保留必要性。P2 默认不删除这些历史，容量到限背压并明确 P3 依赖。删除成功表示逻辑隐藏，不承诺备份立即物理擦除。Linux/Windows 都先释放 mmap/文件 owner 再移动/删目录，失败记录 cleanup_pending，不能改变已发布逻辑结果。

## 6. 资源与结果协议

[建议] 下表是首版实施起点/测试参数，未经容量测量不是吞吐承诺；配置记录在 collection 权威配置中。先保留 P1 约束，若真实 fixture 证明不足则评审调整而非重新开展性能调优。

| 预算 | 初始建议与行为 |
| --- | --- |
| Record/命令 | 沿用 1 MiB 单命令、16 MiB WAL frame、HTTP 2 MiB body、16 MiB response；staging 分批不能绕过总量 |
| 身份/保留修订 | 沿用 10,000 身份上限；新增修订字节/数量和 source/generation/edge 合计上限，首轮明确计入同一总预算，不无限乘出新表 |
| 依赖 | 每记录最多 32 引用，深度 16，闭包最多 10,000 节点/320,000 边；超过拒绝新建/发布，精确删除有独立预留空间 |
| Generation | 每 document 同时最多 2 个 staging；每 generation 最多 1,000 chunks、64 MiB 原始输入；每批最多 100 项且受命令字节预算约束 |
| Lease | 上传 30 分钟可续租；续租需授权+fence；总 staging 字节限制，不因续租无限增长 |
| 一跳 | seeds 32、每 seed 128 邻居、总 candidates 不超过 P1 的 10,000；记录截断位置/原因 |
| Feed | 建议 7 天、100,000 事件/64 MiB 双硬上限；先到者触发背压，不提前静默丢弃承诺窗口 |
| 全局 | 沿用 P1 队列/超时，新增后台总任务与 staging/快照内存、Blob 磁盘预留；删除/恢复应急预算独立 |

所有上限做边界值与失败原子性测试，尤其原始向量/证据 revisions/事件复制引起的内存与 checkpoint 膨胀；预估超出既有 checkpoint 协议或 1 GiB service backup 上限，在接受前拒绝。文档中的 64 MiB generation 不能覆盖当前完整 checkpoint 编码上限：实现须取各预算最小值，先支持较小可恢复 generation；更大容量必须先交付有认证的多对象 checkpoint，不能硬塞 WAL。管理型大 Blob 不放单帧，独立 payload 引用及 pin/备份先完成才开放上传。

[建议] 新查询返回 `view{visible_seq,statistics_version,evaluation_time}`、`completeness{exact,reason,budgets_used}`、`count{value,relation}`（relation 为 exact/estimated/window）、命中通道及精确引用。ANN 近似、候选窗截断、关系预算分别标明；混合各通道最大 count 不是并集精确计数。精确模式预算不够则错误；仅显式允许 partial 才返回不完整结果。禁止在最终 Top-K 才过滤隐藏记录；各通道候选前过滤、有界 refill，最终仍复核。统计口径固定为有效 collection/index_group 语料，用户 query filter 不隐式重定义 IDF；source/TTL/generation 隐藏属于有效语料变化。

## 7. 依赖拆分与退出条件

下列为**建议拆分**，不是已创建 PR/已批准实施。阶段代码和文档同提交；每步真实服务贯通后再称交付。默认 HTTP/UI 不得暴露只能绕开权限调用的库层原型。

| 步骤 | 模块落点/交付 | 验证与退出条件 |
| --- | --- | --- |
| P2-0 契约/前置核验 | 本文，P1 最终基线/可靠性缺口，D1–D4 决策；维护实施入口 | 所有系统字段与既有请求语义映射明确；审查与决策记录完成，不算功能完成 |
| P2-1 来源与证据最小闭环 | record 内统一 command/MutationPlan、source/evidence、revision archive、data output permit；catalog/server 组合许可；无向量 source+fact UI | 创建来源→派生→间接派生→删除→查询/响应阻断→重启/备份恢复不复活；有真实 HTTP/UI；新增状态 checkpoint 完整，不能仅类型壳 |
| P2-2 分批 generation 原子发布 | generation 状态机、sealed 清单、多记录索引发布、active/statistics/pruning、staging lease 清理 | 旧代→staging 不可见→publish 全切换；并发发布只有一方条件成立；每故障点恢复到完整状态；真实 UI 上传/失败重试；统计/排名与精确有效集合对照 |
| P2-3 可选向量与条件补写 | vector/index 输入和 ordinal 映射、readiness、HTTP/UI 状态 | 无向量先全文可用，向量后加；过期任务冲突；generation 两种就绪策略；原始精度恢复；稳定排序、ANN/全文无旧版本混合 |
| P2-4 时间与 TTL | temporal、有序访问/时间候选、查询 evaluation_time/活动差量、过期命令 | 范围边界、同查询一致、时钟回退/重启不复活；TTL 对派生/边/统计按 D3；独立时间通道说明；无后台 worker 仍不泄漏 |
| P2-5 实体与显式一跳 | entity/relation、双向邻接、query 有预算融合/解释、UI 证据预览 | 同 collection、两端/边有效、环只允许关系；删除与查询交错零泄漏；精确与窗口 count 标注，重建邻接同结果 |
| P2-6 可恢复 feed 与重算交接 | change_feed、事件窗口、bootstrap/pin、恢复 lineage、客户端/UI 状态 | 数据和事件同提交，重复/丢响应/过期 token/备份恢复/慢消费者；不停写 bootstrap 无遗漏；不执行模型重算 |
| P2-7 集成与验收封板 | 全 service snapshot、必要 Blob 支持、资源/故障矩阵、默认 HTTP/UI/OpenAPI、实施/验证账本 | 所有阶段最终 SHA 全量回归及双平台原生成功；主机故障另标；未决产品项已落地；只在已满足的范围标验收 |

依赖：P2-1 是首个功能切片，因为来源/版本/隐藏是 generation、时间、关系的共同安全基础。P2-2 依赖它的原子计划；P2-3 可在相同接口稳定后开发，但其可见性仍依赖 P2-2 发布协议。P2-4/5 可按模块并行设计，不并行分裂提交权威；feed 的事件结构从 P2-1 就记录，P2-6 才开放持久消费/保留协议，防止后补事件丢历史。管理 Blob 若首片需要即提前其 durable/备份依赖，否则 source 先用小内联原文或明确外部引用，不能声称备份已保存外部文件。

每个 PR 必须有：实际改动/非目标、源码落点、协议变化/不兼容说明、测试命令与准确计数、Linux/Windows 单独结果、故障点/未运行项、独立审查修复、剩余项。文档与测试不能替代真实默认服务接入；任何中间阶段接口不能允许尚未实现的语义。

## 8. 测试与故障矩阵

P2 新测试文件名以下是拟议名称，尚不存在。复用现有 `cargo test --workspace --all-targets`、`scripts/check_core_boundary.py`、`scripts/record_service_smoke.py` 及 `.github/workflows/ci.yml` 原生平台矩阵；实现时把 P2 场景并入真实服务脚本或新增 `p2_service_smoke.py`，不能让脚本只测试旧 Record。

| 边界/注入 | 预期与恢复断言 |
| --- | --- |
| 校验/预留失败（版本/跨 collection/环/预算） | 无提交、无幂等成功回执、无 staging/事件残留可见；现有已确认状态不变 |
| WAL 前/写中/见证同步前后退出、磁盘满/同步失败 | 已知失败与 unknown 分开；只忽略可证明未提交尾部；已见证完整提交重放；损坏失败关闭 |
| durable 后 source 屏障/索引/统计应用失败 | durable_pending + fence；禁止越过失败 seq 新读/写/输出；恢复后正文/统计/依赖一起生效 |
| StageBatch 丢响应/乱序重试/取消 | 相同 operation_id 一次逻辑生效；不同内容冲突；staged durable 不可检索；取消不放走后台授权工作 |
| Seal/Publish 前后每组件中断 | 只有完整旧代或完整新代；无旧正文+新向量/旧 df；丢响应查询原 op；旧 expected_generation 拒绝 |
| source 删除与准备/发布/重算/慢输出交错 | 删除先则后续发布不复活；发布先则删除隐藏新依赖；已交付 bytes 之外均阻断；多级依赖/缓存/计数/关系/feed 同测；加入仅 source_ref 及仅 generation 来源继承的 chunk |
| revoke/regrant 与排队写/查询/输出交错 | 旧 grant revision 永不复用；其他 key 正常访问数据；source 删除对所有共享 key 生效；操作查询授权不例外 |
| 缺向量/精度维度不符/旧 input hash | 缺向量可全文、无零向量；非法值拒绝；补写冲突不改原文；generation readiness 验证 |
| TTL 临界点/长查询/回拨/重启 | 固定 evaluation_time；新查询不含过期、统计/剪枝同集合；显式删除仍取消旧视图；回拨不复活 |
| feed 写入/截断/游标持久化之间退出 | 无漏事件，允许重复；已过 floor 明确过期；新恢复 lineage 拒绝旧 token；snapshot+tail 无缺口 |
| checkpoint/备份发布/Blob 同步/GC 前后退出 | 完整切面；没有缺来源的“成功”备份；新根无旧授权；pin 内文件不删、租约失效任务不可发布 |
| Windows mmap/打开句柄/长路径、Linux rename/sync | 原生测试验证错误传播；不为过测试放松持久化/锁；cleanup_pending 不逆转已确认逻辑 |
| 系统崩溃/断电 | 两平台隔离环境+外部见证校验数据/撤销/隐藏/事件前缀；没有条件则明确未验收 |

查询参考模型对小型确定性集合直接计算有效版本、依赖闭包、N/df/长度、精确词法分数/排序、时间过滤、邻居和 count；与真实内核对照。检查剪枝开启/保守关闭结果一致，generation 大小边界不丢成员。向量区分精确距离校验与 ANN 候选近似，不伪称完整召回。

测试分层：纯状态机/属性测试（重放幂等、循环、ABA）→真实 WAL 故障注入→真实进程退出恢复→共享 key HTTP 并发与流输出→Chromium 创建来源/分批发布/重试/证据预览→Linux 与 Windows 原生矩阵。请求取消和 kill worker 的忽略子程序要列清归属；最终代码修改后重跑适用全套，不能沿用修改前结果。性能只记录预算内资源观察，不重启 benchmark 调参。

## 9. 待用户决定的业务语义

以下问题不阻塞通用版本/日志/授权/备份设计，但必须在对应功能编码/验收前确定。其余正确性选择直接采用工程评审，不逐项要求用户确认。

| 编号 | 实质问题与推荐 | 影响 |
| --- | --- | --- |
| D1 | **来源更新后，旧证据派生默认还能检索吗？** 推荐标 stale 并默认隐藏，显式历史模式才返回；备选是标 stale 但继续普通 recall，直至上层重算 | 有效语料统计、召回空窗、历史模式、更新事件；删除始终隐藏已确认，不重复询问 |
| D2 | **整文发布默认是否等待全部向量？** 推荐默认允许 lexical-ready 发布、返回各通道 readiness，并提供显式 require_vectors；备选默认全部向量就绪才发布 | 导入延迟与完整通道要求；无论选择哪种都不可混合 generation |
| D3 | **TTL 到期是否作为不可逆遗忘，并连带隐藏依赖它的派生内容？** 推荐是；无 TTL 永不过期；备选仅当前查询过滤、允许延长及独立派生保留，需要另行明确语义 | 删除屏障、历史/备份说明、重算与 clock-watermark；不改变明确的 source 删除规则 |
| D4 | **change feed 的保证离线重放窗口需要多久？** 建议首版 7 天，容量不足背压而不静默提前过期；请按实际消费者最长离线时间确定 | 磁盘/事件预算、消费者重建频率；不承诺无限保留 |

管理型大文件 Blob 的必要性按首个实际输入场景决定：如果只需外部引用，不强加上传平台；若要求底座完整保存 PDF/音视频，须将 BlobStore 与备份保护纳入对应切片并确定容量。此为能力选用说明，不把未给定文件规模设成阻塞所有 P2 的问题。

## 10. 本次文档交付与审查记录

本次仅设计；所有 P2 功能、测试矩阵均**未运行/未实现**。P1 历史测试不重新命名为 P2 验收。Linux/Windows P2 原生与系统故障均为未运行。

- 仓库核验：远端主干/PR head、PR draft 状态、issue 正文、当前 Record/vector/catalog/输出路径/活动统计/CI；未修改外部状态。
- 本地交付：独立文档分支，本文与实施入口导航；不推送、不更新 PR/issue。
- 独立文档审查已完成，审查者只读核对 Record/catalog/vector/WAL/HTTP 输出。修订包括：sealed 清单与受控向量增补协议；feed 紧急删除预算；TTL 首次隐藏前 durable；source_ref/generation 隐式依赖；纯向量变化不误标语义 stale；新屏障签发删除回执。定点复核无剩余协议阻塞，并纠正 catalog 满额是 fence、不是已有预留提交保证。
- 文档检查已通过：`git diff --check`；Python 检查两个文档的本地链接存在、代码围栏成对、两个 JSON 示例可解析。文档结构/源码核对不是 P2 功能测试。
- 未运行 Rust/HTTP/Chromium 或新 CI：本轮无功能代码更改，所有 P2 测试仍为计划；无推送/PR/issue 变更。D1–D4 待产品答复，主机故障环境门槛继续保留。


## 11. 首个实施检查点：实际主干与写入职责拆分

状态：**本地基础拆分检查点完成；P2-1 来源功能未完成**。2026-10-21 重新核验 PR #61 已合并，merge 与远端 master 均为 `64b1c8f656394a55f5b8a1239754d03d2872efd5`，最终 PR head 为 `d3feff9eafab1af95dc7ea60df441cf46c16a4b1`。相较本文原设计基线还包括向量浏览、Busy/未知重试及文档整理，不能用旧 SHA 测试替代本次测试。

本地实施分支 `feat/57-p2-source-evidence` 从此 master 新建；原文档分支 `docs/57-p2-implementation-design` 与提交 `8443067b17b0a720f87b39b41ad93a27811b3897` 保留。本分支仅导入设计文件并对新主干入口增加导航，不重复合入旧 P1 提交。当前授权仅本地实施/测试/提交；未推送、创建 PR、修改 issue 或部署。

### 范围与依赖方向

首步抽出真实写入路径职责，不增加尚不能兑现的来源 API：

- `record/mutation.rs`：纯校验与计划，接收受串行 guard 保护的 State、冻结配置、已知 checkpoint 字节数和显式时钟。处理版本/路由/ABA、operation replay、原始向量与容量；只返回 Replay 或完整 Applied 决策/序列化 WAL/预计完整 checkpoint 大小，不修改权威数据或派生索引。
- `record.rs`：继续唯一 owned executor，持同一 guard 完成准备、journal admission、durable append、失败 fencing、发布与返回；不在计划与提交间释放锁。恢复仍重放已记录 Applied，不重新执行当时的重试时钟与用户条件。
- `record/publication.rs`：durable 后增量维护物理索引、当前 ordinal、删除集合、活动统计/缓存、权威状态及精确 checkpoint 账目。只有全部完成，executor 才恢复 healthy。错误保留 durable_pending；调用方取消不取消后台已接受工作。
- `vector.rs`：增加无索引状态依赖的 schema 向量校验函数，由通用 index 和 Record planner 共用；维度/有限数值/零 Cosine/原始精度规则保持同一实现，未来 optional-vector 策略不会被 HTTP 单独解释。

这是正在被默认 HTTP/库写入调用的生产路径拆分，不是新旁路接口。此检查点没有修改请求/文件格式，不实现升级兼容层，也没有新增模型/账号/性能功能。

### 首片后续边界

来源删除立即隐藏可以继续实现，且不依赖 D1–D4。后续需增添受保护系统元数据、精确 evidence 引用/修订保留、源删除闭包、数据输出许可，并将同一 multi-record 计划贯通全文/向量/活动统计/回表及备份恢复。当前尚无这些能力，不能把普通 document 中的 source_id 当成系统来源。

不新增产品问题：来源更新/stale（D1）、generation readiness（D2）、TTL（D3）、feed 保留（D4）继续等待用户答复。实现初期对需这些策略的命令明确不开放；不能用任意默认吞掉歧义。优先完成来源创建→派生→多级依赖→删除→重启/真实 HTTP/UI 不复活的已确认切片，而不是提前推进 generation/TTL。

### 验证记录

新契约测试验证计划无副作用、恢复不受重试时钟过期影响、持久时间水位抵抗回拨、已提交重试先于容量判断、不同请求冲突，以及向量校验保持原始数据。

| 平台/层次 | 本检查点结果 |
| --- | --- |
| Linux 契约/全套/真实 HTTP 与 Chromium | 通过：2 个新增契约测试；最终全套 168 passed / 0 failed / 10 ignored，35 suites；默认与 fault-injection HTTP/Chromium 均通过 |
| Windows 原生 | 未运行；新 P2 分支尚未授权推送/CI |
| Linux/Windows 系统崩溃/断电 | 未运行；仍需隔离环境和外部见证 |

独立只读代码审查已完成，无阻塞性缺陷；确认原校验顺序、重放先于容量、owned task/锁生命周期、持久后 fencing、增量统计/缓存与恢复顺序保持一致。唯一可观察的错误优先级变化是多个非法向量字段按配置 schema 顺序报告，不再依赖哈希 map 遍历。

实际命令与证据（本地 Linux 6.18.55，测试根位于 overlay 文件系统，不是断电验证环境）：

```bash
cargo test -p seekpower record_planning --lib --quiet
SEEKPOWER_TEST_ROOT=/workspace/seekpower/target/p2-native-data cargo test --workspace --all-targets --quiet
cargo build --workspace
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
python3 scripts/check_core_boundary.py
cargo build -p seekpower_server --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --faults --browser
```

以上全部退出 0。环境设置 `PATH=/workspace/.cargo/bin:$PATH`、`RUSTUP_HOME=/workspace/.rustup`、`CARGO_HOME=/workspace/.cargo`；浏览器使用 `PLAYWRIGHT_BROWSERS_PATH=/workspace/playwright-browsers`。完整日志位于 `/tmp/seekpower-p2-foundation-tests.log`、`-build.log`、`-http-browser.log`、`-boundary.log`、`-fault-build.log`、`-fault-http.log`（共同前缀 `/tmp/seekpower-p2-foundation`）。10 个 ignored 与 P1 相同：9 个由父测试启动的崩溃/取消 worker 入口，1 个需显式 fixture 的跨构建哈希辅助程序；没有新增忽略测试。无功能性测试失败，编译仍有既有 warning，未做无关清理。

本次 HTTP/UI 是已存在生产路径的重构回归，不是新来源能力端到端验收。来源创建/证据、级联删除、输出数据屏障、来源恢复验证尚未实现，下一检查点必须贯通这些功能再称 P2-1 完成。没有新推送/CI 授权，不把旧 P1 Windows 结果当成本次通过。

## 12. 来源/证据删除切片（本地实现，等待用户检视）

状态：**本地基础删除切片及 Linux 最终回归完成，供用户检视；不是完整 P2 或整个 P2-1 验收完成**。基于本地 `42f6604`，不启用 D1–D4。最终代码结果在本节末记录，所有前面草案示例不能替代本节实际接口。

### 当前实际模型与范围

复用通用 Record 的 `record_id/routing_key/version/document`，新增可选顶层 `system`，其结构为 `{kind:"source"|"derived",evidence_refs:[{record_id,routing_key,version}]}`。不存在独立 source_id 权威表；source 是受校验的 typed Record，与普通 Record 共享身份命名空间、版本、操作账本和唯一 WAL。普通 document 仍可自定义，里面的同名属性不产生系统语义。

首片只允许 source/derived 创建与删除。source 不带 evidence_refs；derived 必须有 1–32 个同 collection 精确引用，只能指向已可见 typed Record。间接依赖、路由及版本一并校验。跨 collection 字段/未知系统属性明确拒绝；自引用、重复引用、不可见证据及超深图拒绝。图最大深度 16，单次访问最多 10,000 节点；恢复共享带高度的 memo，不对每个节点重复遍历整图。

**更新、重建及转换 typed 身份明确不支持**，普通替换也不能清掉 system 绕过。删除省略 system，由引擎保留原元数据与递增墓碑版本。重复请求仍返回原 receipt；使用新 operation_id 重删墓碑返回冲突。源内容被冻结，引用始终精确，不需要在策略未定时自行选择 stale 行为。未实现 locator、历史修订读取 API、generation、向量后补、TTL、entity/relation 或 feed，不将其标为完成。

### 删除、统计与输出

- `record/evidence.rs` 只校验系统模型、计算有界依赖可见性与删除闭包，不做 I/O、不保有独立权威状态。
- planner 完成引用/版本/预算验证；executor 仍唯一 durable 提交。删除成功前将 root 及所有依赖节点从 locations、物理候选删除集和活动统计中一起退役，同时失效评分缓存。保留派生原文与元数据供恢复，不通过把所有派生 document 写 null 冒充级联语义。
- 各通道在现有引擎候选阶段排除退役 ordinal，get/page/query/count/统计使用同一 guard。恢复从 metadata+tombstone 推导有效集合，再构建可重建索引；普通写入仍增量发布，不逐次重建全文/向量索引。
- `record/output.rs` 保存进程内的派生数据输出屏障。读取前捕获许可；source/derived 删除使旧许可失效，未知/持久后失败保持 fenced。最后 owner 释放时也关闭旧许可，防止重新打开后继续输出上一 owner 的内容。
- HTTP 在同一次同步交付中先持 catalog 授权读锁，再持数据许可读锁；两者覆盖 status/header 和 body frame。collection 列表聚合全部涉及的许可，避免旧 active_records 经列表泄漏。聚合按锁身份排序去重，不递归取锁；交付期间不 await、不重入 partition。缓存不绕过该路径。
- 写成功回执/operation lookup 只返回操作 ID、版本和 seq，不含旧来源正文；删除回执不会被自己的数据屏障取消。已经交给网络的字节不能收回。

### 删除预留与格式

每个 live typed 身份预留 1 个 operation 槽和 8 KiB WAL/checkpoint 空间（即使已被间接隐藏仍保留），普通写不能占用。typed record_id/routing_key 限 256 UTF-8 字节，nonce 限 128 字节；删除必须省略 system，使最坏 JSON 转义/回执/帧开销可被固定预算覆盖。初始 identity 总上限、命令/响应预算仍有效；预留量与 16 MiB 完整 checkpoint 帧上限共同限制实际容量，不能承诺每个配置都能装满 10,000 个 typed 身份。真实磁盘满/同步失败仍可能 unknown/fenced，这不是静默成功或可用性保证。

Record 权威 entry 标签更新为 `RecordCheckpointV2` / `RecordAppliedV2`；P1 标签明确拒绝，无升级转换。上层 catalog/索引自身版本不伪装成完整 Record 协议兼容。源格式不匹配时打开失败，不重新初始化覆盖目录。

### 恢复与备份的明确限制

同一 live partition 的 WAL 重放/在线 checkpoint 切换完整恢复原来的可见性，包括已确认 source tombstone 和间接隐藏；不会任意隔离正常重启后的有效来源。

**独立备份不是“恢复后立即重新开放所有来源”的完整交付。** 任何包含 typed 身份的独立 checkpoint 均保存 `evidence_quarantined:true`；collection/service snapshot 的复制与恢复持续保留此状态。源/派生原始输入、版本、操作结果、tombstone 都保留，但 typed 内容不进入读取/检索/计数，typed 写入也拒绝。普通无依赖记录仍按原功能恢复。服务 info/restore 回应和 UI 明示隔离状态。

原因：早于删除的备份无法证明不存在快照后的删除。目前没有可信删除历史核对/解除隔离接口，**因此 typed 备份暂不能恢复为可搜索内容**。这项限制防止静默复活，不应包装成完整备份恢复能力。后续需交付设计 §5.1 的可信抑制清单/水位核对和显式激活协议后才解除；现阶段不猜测恢复时可以忽略哪些删除。历史 key/grant 的隔离继续沿用 P1，不因为来源恢复而恢复授权。

### 默认 HTTP/UI 与实测场景

原 `/api/v1/index/{id}/doc` 写入/删除入口接收 system；读/搜索/iterator 返回的 Record 外壳带该元数据。Web UI 保留普通 JSON 导入，新增显式 Record envelope 选项，可输入稳定 ID、来源引用与 document:null 删除；operation_id 仍由 UI 生成、先持久保存并在失败后原样重试。规则由核心校验，UI 不自己判断闭包/版本合法性。

核心新增测试覆盖多级删除、原始数据保留、全文/向量/hybrid、统计、操作重放、在线 checkpoint/WAL 与独立快照、旧 owner 输出、durable_pending、引用版本/路由/深度/范围、满操作槽/写入压力下删除预留、P1 格式拒绝。真实 HTTP 覆盖共享 key 删除、旧快照隔离、源删除后重启、未知写入/进程退出与原请求重试。服务交错测试暂停已准备响应，完成删除，再交付成功/错误/collection 列表；另测 status 已交付但 body 尚未 poll 的删除屏障。Chromium 实际输入 source/derived envelope、搜索与删除，核对派生内容消失。

独立只读审查发现并修复：旧备份自动复活风险、删除预算不足、共享 DAG 恢复重复遍历、collection 列表旧统计绕过输出屏障。修复后独立审查者完成定点复核，未发现本切片剩余阻塞；明确确认独立备份隔离尚不等于完整恢复激活能力。

| 验证 | 当前状态 |
| --- | --- |
| Linux 全量最终源码 | 通过：176 passed / 0 failed / 10 ignored，35 suites |
| Linux 默认及 fault-injection 真实 HTTP/Chromium、核心依赖边界 | 全部通过，退出 0 |
| Windows 原生 P2 | 未运行；本分支未授权推送或 CI |
| Linux/Windows 系统崩溃/断电 | 未运行；无隔离环境与外部见证 |

阶段结束只提交本地供用户检视，不推送、不创建 PR、不合入、不部署。P2-1 剩余恢复激活/引用定位等边界和四项业务决策继续列为未完成，不以基础删除闭环替代完整阶段。

最终源码验证命令（环境与 §11 相同；测试完成后只更新文档）：

```bash
SEEKPOWER_TEST_ROOT=/workspace/seekpower/target/p2-native-data cargo test --workspace --all-targets --quiet
cargo build --workspace
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
python3 scripts/check_core_boundary.py
cargo build -p seekpower_server --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --faults --browser
```

对应本地日志：`/tmp/p2-source-final2-tests.log`、`/tmp/p2-source-final2-build.log`、`/tmp/p2-source-final-http.log`、`/tmp/p2-source-final-boundary.log`、`/tmp/p2-source-final-fault-build.log`、`/tmp/p2-source-final-fault-http.log`。最终测试全部退出 0。10 个 ignored 沿用既有 9 个子进程 worker 入口及 1 个显式 fixture 的跨构建哈希辅助程序，无新增忽略项。故障注入包含 source 删除的 append unknown、durable pending、durable/visible 后进程退出和恢复重试；这些证据不代表系统崩溃或断电可靠性验收。文档链接/围栏/JSON 示例检查及 `git diff --check` 通过。
