# Issue #57 P2 实施设计：来源、证据与统一可见性

状态：**P2 分批实施，未整体验收**。D1–D4 已获用户确认（2026-10-21，Sentinel_d2c624cd956c81919e1aac72655b354c：“四项按建议”）。第 10–13 节保留各检查点当时的状态，最新实施及范围审计校正见第 24 节；已批准不等于已实现。

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

[确认 D1] 来源更新保留旧 revision，将依赖旧内容的派生记录标 stale，默认普通 recall 隐藏 stale；可选显式 include_stale 读取仍有效且未删除的历史证据。默认隐藏已获批准；显式历史读取是否开放仍是独立接口范围，本批不开放。独立 source 更新先令旧派生 stale，再发布新 generation；若要零空窗整文替换，建议 `PublishGeneration` 可同时激活其预先 staged SourceRevision，作为一个 MutationPlan，不能连续两个成功 API 冒充原子。

重算用新 operation_id + Match 派生 record 版本 + 精确新 evidence 版本，提交时再次验证 source 屏障。证据环在提交前通过有界 DFS 检查，含自引用/跨多个新成员的环；批量命令对最终候选图检查，不能逐条绕过。缺失依赖不按“暂时允许”发布。source 删除和旧重算并发，按串行顺序确定；删除后重算不能恢复旧 incarnation 的内容。

### 4.5 TTL、时间与统计

[确认] 每次查询固定 evaluation_time，所有通道/回表/关系/计数使用同一值。TTL 先隐藏后回收；不能等待异步定时器才从普通查询消失。

[建议] 只允许可信服务时钟设置当前 evaluation_time，客户端业务 as_of 不能复活已删除/TTL 过期内容。获取视图时，基于截止时间有序索引处理到期集合并生成视图的有效位图/统计差量；新视图的 N/df/长度/剪枝也排除过期记录。过期系统命令的事件与不可逆隐藏水位必须在首次向外返回受该 TTL 影响的结果前 durable；查询触发推进也要通过 executor，拿锁前后遵循授权顺序，不在读锁内升级造成死锁。若同步失败，本次读取失败关闭，不能先给出“已过期”再异步落盘。后台仅负责提前推进而不是唯一正确性来源。积压超出预算时等待/背压或显式失败，不以旧统计输出。

[确认 D3] TTL 作为不可逆逻辑遗忘：已过期身份保留墓碑；客户端不能通过晚到延长 TTL 恢复，须明确新版本/新生命周期操作。记录持久时间水位以避免重启时钟回退复活；服务发现明显时钟异常进入时间请求失败状态，运行文档声明时钟校准预算。普通未到期记录可 Match 修改 expire_at。

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

[确认 D4] 至少保留 7 天且设置字节/条数硬上限；达到容量不能静默突破保证：在过期窗口释放前背压普通新变更，提供告警。source 删除/TTL 隐藏不能被普通 feed 配额堵住：预留覆盖最大有效身份数的紧急墓碑、operation receipt、闭包状态、完整可恢复 checkpoint 和固定大小聚合事件空间，cascade 不强制为每个派生输出独立事件；聚合事件携带 cause 和闭包范围/重建指示，消费者重新读取受保护状态。容量预留在创建身份/依赖时核算；恢复必须保留此预算，授权撤销继续走独立 catalog 权威；P1 当前容量耗尽会 fence 而非保证撤销 durable 成功，P2 不得弱化这个失败关闭行为。若要求满额下撤销仍可提交，须明确新增 catalog 预算预留并测试，不能声称现有保证。消费者离线超过 floor 必须重新 bootstrap；不按最慢消费者无限 pin WAL。长查询/备份/staging与合法 feed 窗口的恢复依赖分别管理，不能用单一“最小 seq”概括所有非连续引用。

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

## 9. 已确认的业务语义与实施状态

用户在 Sentinel_d2c624cd956c81919e1aac72655b354c 回复“四项按建议”，批准以下语义。不是批准一次实现所有阶段，也不授权合入或部署。

| 编号 | 已批准语义 | 实施边界 |
| --- | --- | --- |
| D1 | 来源更新使旧派生 stale，默认隐藏，等待上层重算发布；来源删除始终立即隐藏 | 第 14 节先交付条件更新与隐藏；不自动重算，不开放 include_stale 绕过 |
| D2 | 正文就绪即可发布，先支持全文检索；向量后补，缺向量不参加向量召回，不填零向量 | 第 15 节实现可选向量及版本受控后补；第 16 节实现有界完整 generation staging/publish，最终验证见该节 |
| D3 | TTL 到期为不可逆逻辑删除，并隐藏依赖派生；不能仅延长 TTL 复活，物理回收异步 | 第 17 节本地 TTL/时间切片；Windows 与系统故障验收单独列示 |
| D4 | feed 至少保留 7 天；容量不足限制新写，不提前静默丢弃窗口内事件；过期消费者重建同步状态 | 第 18 节已实现持久窗口、背压与 bootstrap；当前最终堆叠的验收仍按第 22–23 节 |

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

## 13. 同服务受控接管恢复（进行中）

用户已确认首版采用接管恢复，原副本封存后才能开放目标，不允许双活；进一步确认目标追平最终封存水位，保留全部已确认写入。不是恢复到备份时点再丢弃后续更新。四项 D1–D4 决策仍待答复，本节不启用它们。

### 实际协议与职责

- `record/recovery.rs` 负责权威谱系、终止 seal、完整状态摘要和目标认证。每个新 partition 创建随机不可复用谱系，独立 checkpoint 无论是否已有来源内容都持久标记 `independent_snapshot`，不能作为当前删除权威；谱系仅证明来源，不证明跨根全局唯一 owner。
- `catalog/takeover.rs` 保存不随两日操作回执过期清理的接管意图。Prepare 绑定原/目标 collection、原 operation_id、备份 ID/摘要和谱系，同时关闭源 active/grants；Finalize 一次持久命令退休源并激活目标，只授予仍有效的当前管理身份，不复活旧 grants。
- `record_service/takeover.rs` 仅编排当前独占服务根内的流程。普通 HTTP 写仍通过整个 `Catalog::authorized` guard；Prepare 的 Record 精确容量检查也在同一 catalog guard 内，防止检查后写入耗尽空间。HTTP/UI 不自行处理删除闭包或版本规则。
- 源 seal 在 Record mutex 内取得最终水位与完整状态摘要，持久 checkpoint 后才能返回；所有旧 clone、普通读写、操作查询、统计和数据输出许可关闭。重新打开、在线 compact 或再次独立备份都不清 seal。可信恢复检查/配置读取是明确例外。
- 备份必须是同一谱系的精确摘要输入且水位不晚于源；最终目标从源封存后的完整权威状态重建，保留全部正文、配置、版本、墓碑及操作回执。目标打开完成索引构建，并核对去除恢复控制字段后的完整状态摘要，才允许 Catalog Finalize。没有只复制旧回执却丢失对应成功写入的回滚行为。
- 每个 pending 接管保留一个完成操作槽和 16 KiB WAL/checkpoint 预算；普通 catalog 操作不得占用。预算覆盖合法 owner/target/nonce 的最大 JSON 转义和完成证书/新 grant。真实磁盘满、同步失败仍可能 pending/fenced，不保证硬件故障时立即可用。

### 持久边界与失败规则

1. Prepare 之前：校验备份、源谱系与容量；失败不封存源。
2. Prepare durable：源授权关闭、目标不可访问。即使尚未执行 Record seal，服务恢复也不得普通初始化/重新激活源。
3. Seal durable：源写入/内容输出永久关闭，最终状态可供受控核验。未知结果只能重开核对同一 intent；没有 unseal 或强制解除接口。已知 checkpoint 未发布错误也保守关闭当前句柄，需要重开确认，不能把 Rejected 当作句柄继续可服务的保证。
4. 目标创建及索引认证：目标无 grants，任一步失败继续隔离；启动按同一持久 intent 续办，不按目录存在直接激活。
5. Finalize durable：原退休、目标 active 和管理 grant 在同一 Catalog 状态发布。内存 map 尚未安装时进程退出，重启按已完成目标打开，不能重新创建原。

旧 operation_id 和参数用于 HTTP 重试；目标 intent 保留完整绑定，不能换源/备份/目标复用。进行中不得删除所绑定 collection 或退休其备份，完整 service backup 也拒绝归档 pending 接管。已完成服务归档可保留历史，但独立恢复再次标记非权威并隔离 typed 内容，不允许以历史接管意图自动解除隔离。跨服务自动灾难恢复、外部所有权 fencing、缺失删除历史的强制激活都不在本节范围。

### 当前接口与 UI

新增 `POST /api/v1/takeovers`，仅现有 master 入口可调用。以下为实际请求结构，调用会关闭原 collection，不用于真实用户数据测试：

```json
{"operation_id":{"epoch":20747,"nonce":"saved-takeover-request"},"source_collection_id":"current","collection_id":"successor","backup_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
```

成功响应含 `status:"active"`、`recovery:"final_sealed_waterline"`、最终 certificate 和 `old_grants_restored:false`。UI 提供原 collection、备份和新 collection 输入，先持久保存请求；响应丢失或重载后保留相同请求重试。未确认结果时输入锁定，不将错误响应当作可以新建接管身份的依据。

Record entry 更新为 `RecordCheckpointV3` / `RecordAppliedV3`，权威状态增加谱系、独立快照标记及 seal/proof；拒绝 V2/P1 格式，不实现迁移。第 12 节 V2 是上一检查点历史格式。

### 验证记录（本地完成，等待本提交原生 CI）

新增核心契约覆盖旧 owner/许可、最终正文及回执保留、间接隐藏、seal 重开/compact/复制、空备份不可冒充权威、取消和 durable 后失败；Catalog 覆盖完成槽预留及最坏转义预算；服务覆盖连续接管和旧响应 body。真实 HTTP 加入 Prepare、sealed、target_created、certified、finalized 退出恢复，Chromium 加入接管响应丢失后重载重试。

独立审查已发现并推动修复：空快照权威绕过、sealed 统计读取、Prepare 后不可完成的容量窗口、撤销容量失败关闭回归及最大转义预留不足。最终定点复核通过，无剩余阻塞项；源码全量与真实服务回归结果如下。所有测试只用临时服务数据。

- 上一检查点 `aa168ca32874192a68d3eefb9a47d1af7dd71d9a` 双平台 CI：[39568481764](https://github.com/TheLudlows/seekpower/actions/runs/39568481764)，Linux build-and-smoke、Linux/Windows persistence-native 全部通过；不覆盖本节新增代码。
- 当前接管实现：Linux 最终源码全量 **183 passed / 0 failed / 10 ignored，35 suites**；默认及 fault-injection 真实 HTTP/Chromium 全部通过；核心依赖边界、OpenAPI 路由检查及文档链接/JSON/围栏、`git diff --check` 通过。10 个 ignored 仍为既有辅助程序，无新增。Windows 原生待推送当前源码后运行。
- 系统崩溃/断电：未运行，仍需隔离存储故障环境及外部确认见证。进程退出测试不能替代。

本阶段最终命令均退出 0：

```bash
SEEKPOWER_TEST_ROOT=/workspace/seekpower/target/p2-native-data cargo test --workspace --all-targets --quiet
cargo build --workspace
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
python3 scripts/check_core_boundary.py
cargo build -p seekpower_server --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --faults --browser
target/debug/seekpower_server generate_openapi=true
```

本地日志：`/tmp/p2-takeover-final4-tests.log`、`/tmp/p2-takeover-final2-build.log`、`/tmp/p2-takeover-final3-http.log`、`/tmp/p2-takeover-boundary.log`、`/tmp/p2-takeover-fault-build.log`、`/tmp/p2-takeover-final-fault-http.log`、`/tmp/p2-takeover-openapi.log`。中间验证曾发现 catalog 未规范化配置与已持久配置比较不一致，以及 UI 通用连接状态覆盖接管输入锁，均修复后重跑。测试夹具完成备份容量测试后显式退休其旧备份，未扩大生产上限。

剩余范围：同服务受控接管已形成可运行闭环；独立灾难恢复缺少最新删除历史时仍不可激活，未实现跨服务 fencing；完整 P2-1/P2、D1–D4 及系统故障验收均未完成。不得把当前切片当作整个 P2 已交付。PR 保持草稿供用户检视，不自动合入或部署。


## 14. 来源修订与 stale 隐藏（本地实现，等待用户检视）

2026-10-21 核验：master 仍为 `64b1c8f656394a55f5b8a1239754d03d2872efd5`，PR #62 保持 OPEN/draft、未合入，head 为 `20531fa24aaf531faa338451ed55d4d7c2e917b5`。该 head 的 [双平台 CI 39571399133](https://github.com/TheLudlows/seekpower/actions/runs/39571399133) 已全部通过。

本地分支 `feat/57-p2-source-revisions` 堆叠在上述 PR head；不重复合入已有提交，不推送、不创建新 PR、不修改 PR #62。第一批落实 D1 来源更新及直接/间接 stale 默认隐藏，随后另批实现 document generation 与 D2 可选向量。TTL/feed 再按依赖推进，不塞入本批。

### 实际实现与接口边界

- `record/evidence.rs` 负责精确版本规则、依赖可见性和历史完整性检查；`mutation.rs` 负责版本条件及持久容量预留；`publication.rs` 负责增量退役旧 ordinal/派生候选、活动统计及缓存失效。通用 journal 不解释 stale 业务。
- Source 更新沿用现有写入命令，必须 `expected:{"Match":当前版本}`。省略 system 时继承旧来源类型；不允许换类型/路由。原 operation_id 保持原回执，不重复增加历史；更新冲突不会部分发布。Derived 重算使用新 ID 和最新精确证据版本，不开放原 ID 编辑或删除后重建。
- 权威 State 保存 `source_revisions`（每个被替换或删除的来源正文、版本、路由及元数据）；与当前记录、回执一起进入 WAL/checkpoint、备份和最终封存水位接管。不把幂等回执保留期当证据保留期；历史没有第二份独立提交状态。恢复拒绝孤立、重复、越界及缺失修订。
- Source 更新使直接和间接旧证据 stale，默认正文/查询/iterator 隐藏；新 source 仍可见。增量发布同步撤销旧候选并更新活动 BM25 统计和结果计数，所有检索模式共享相同可见集合。旧数据输出许可失效；授权输出屏障保留。删除优先于 stale，隔离恢复仍不可输出 typed 正文。
- `GET /api/v1/index/{id}/doc/{record_id}/status` 是已实现的受保护元数据接口，返回 `record_id/version/visibility/visible_seq`；状态为 visible/stale/deleted/unavailable/quarantined。它不提供历史正文，也没有 include_stale 绕过。HTTP/UI 只适配核心规则，Web UI envelope 模式可以直接更新来源。
- 资源上限：历史修订数加 live Source 数不得超过 collection `max_records`，为每个 live Source 留出最终删除位置；实际 checkpoint 字节和既有删除字节预留同时检查。容量不足拒绝新修订，不丢旧历史、不阻止已预留的来源逻辑删除。未实现历史 GC，后续 P3 再处理持续回收。
- 权威 entry 使用 **V4**，旧 V3/V2/P1 明确拒绝；本批不实现升级迁移。依赖遍历仍采用既有有界、失败关闭约束，同 collection 限制不变。

实际请求结构（operation_id 必须使用调用时有效 epoch，并保存原值重试）：

```json
{
  "operation_id": {"epoch": 20747, "nonce": "source-revision-2"},
  "record_id": "source-1", "routing_key": "document-1",
  "expected": {"Match": 1}, "document": {"body": "Revised source text"}
}
```

### 验证与退出条件

独立代码审查完成，未发现剩余阻塞缺陷；审查建议的完整历史校验已实现并复核。新增核心测试覆盖条件并发冲突、重复提交、直接/间接 stale、新派生、三种检索模式、满历史预算删除、旧输出许可失效、checkpoint/reopen、旧备份接管追平及 durable-pending 恢复。

Linux 最终 Rust 源码全量：**186 passed / 0 failed / 10 ignored，35 suites**；10 个 ignored 为既有辅助程序，本批无新增忽略。默认真实 HTTP/Chromium、核心依赖边界及 OpenAPI 生成通过。HTTP 新用例覆盖共享 key 更新、版本冲突、状态无正文、计数、重启、删除优先级和撤权；Chromium 实际编辑器执行更新、搜索隐藏及删除。故障注入 HTTP/Chromium 全部通过：新增来源更新在 after_append_unknown、after_durable、crash_after_durable、crash_after_visible 四个边界的服务恢复、stale 隐藏与原请求重试；既有删除/接管/备份故障矩阵保持通过。上述命令均退出 0。

```bash
cargo test --workspace --all-targets --quiet
cargo build --workspace
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
python3 scripts/check_core_boundary.py
cargo build -p seekpower_server --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --faults --browser
target/debug/seekpower_server generate_openapi=true
```

本地证据日志：`/tmp/p2-revisions-full.log`、`/tmp/p2-revisions-build.log`、`/tmp/p2-revisions-http.log`、`/tmp/p2-revisions-boundary.log`、`/tmp/p2-revisions-fault-build.log`、`/tmp/p2-revisions-fault-http.log`、`/tmp/p2-revisions-openapi.log`。

**Windows 本批原生测试未运行**，不能沿用基线 CI 当作本批通过；须另获新分支发布/CI 授权。**Linux/Windows 系统崩溃与断电仍未验收**，需要隔离环境及外部确认见证，进程退出不能替代。范围仅 D1 有界来源修订闭环；generation、可选向量、TTL、feed、历史正文 API 及持续历史回收未实现，四策略批准不代表这些已交付。完成本地检查点后交用户检视，不自动合入或扩展 PR #62。


## 15. 可选向量及精确版本后补（本地实现，generation 的依赖切片）

2026-10-21 再次只读核验 master `64b1c8f`、PR #62 OPEN/draft/head `20531fa` 均未改变。

本地分支 `feat/57-p2-optional-vectors` 堆叠在 D1 `084320af6ba4b14c048de4b838d083a2a6b688b4`，仍依赖未合入的 PR #62 `20531fa`。不改 PR #62、不推送、不创建 PR。本批先落实实际向量依赖路径，**不声称 generation 已实现**。

### 设计与实际边界

- 缺失 schema 向量字段现在合法；显式 null、维度/精度错误、无效数值继续拒绝。底层物理索引已按字段是否存在索引向量，缺失记录不参与向量召回，绝不生成占位零向量。正文及全文立即可用；向量和 Hybrid 的向量通道只召回已具备向量的记录。
- `record/enrichment.rs` 独立负责后补命令校验与文档合成，planner、增量发布和 WAL replay 共用合成规则。HTTP/UI 只转换现有写入 envelope，不维护第二套业务状态或独立向量 journal。
- 新可选字段 `vector_patch` 仅填充当前 schema 中尚未存在的向量字段，必须指定当前正文 `expected:{"Match":version}`，省略 document/system。拒绝已填字段、非向量字段、已删除/stale/隔离 typed 目标。源正文修订后，旧版本请求失败；并发填充同字段只有一个新操作成功。
- 成功后提交 seq 推进、正文 version 不变，不添加 source revision、不让现有派生证据 stale；回执仍由同一 operation_id ledger 去重。原始向量值保留于权威 document，后补不改其他正文。正文替换时由调用方明确提交新版本完整内容，不隐式继承旧向量。
- 增量替换物理 ordinal，统一维护当前索引、活动统计、缓存和共同视图。失败时分区 fence，恢复重放完整后补决策；授权与来源删除输出屏障不变。对 source 的 vector-only 变化不误判语义失效。
- 合成后的完整正文检查字节上限及 checkpoint 预算，typed 删除槽/字节预留继续保留。所有状态随 WAL/checkpoint、collection/service 备份及最终水位接管，不新增双权威状态。
- 状态接口增加 `pending_vector_fields`，列出 live 正文尚缺的 schema 向量字段；不表示模型推理进度，不返回隐藏正文。HTTP 单写/batch 只允许显式 document 或对象 vector_patch，单独 `vector_patch:null` 不会变成隐式删除。
- 权威 entry 标签 **V5**，拒绝 V4/V3 等旧格式，不提供升级迁移。

已实现请求示例，使用 `POST /api/v1/index/{id}/doc`；epoch 应使用当前值并保存原请求重试：

```json
{
  "operation_id": {"epoch": 20747, "nonce": "fill-vector"},
  "record_id": "chunk-1", "routing_key": "document-1",
  "expected": {"Match": 1}, "vector_patch": {"vector": [2.0, 0.0]}
}
```

### 验证与剩余工作

独立代码审查未发现阻塞缺陷；检查了版本/seq、历史、重放合成、容量预留、null 防误删及输出边界。新增核心测试涵盖缺向量召回、后补保持证据有效、重复请求、已有字段冲突、并发填充、非向量字段/无条件补写拒绝、durable-pending 恢复和来源 stale 后拒绝补写。底层索引测试验证缺向量正文持久化后仍不参与向量召回，非法显式向量继续拒绝。

Linux 最终源码全量 **188 passed / 0 failed / 10 ignored，35 suites**，退出 0。10 个 ignored 为既有辅助程序，本批无新增。默认和故障注入真实 HTTP/Chromium 均退出 0；全部缺向量时 Hybrid 的全文召回亦通过。后补故障矩阵覆盖 after_append_unknown、after_durable、crash_after_durable、crash_after_visible，检查恢复后正文版本与原回执不变；既有来源修订、删除、接管、完整服务备份恢复测试保持通过。核心依赖边界、OpenAPI vector_patch schema 及 `git diff --check` 通过。

验证命令：

```bash
cargo test --workspace --all-targets --quiet
cargo build --workspace
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
python3 scripts/check_core_boundary.py
cargo build -p seekpower_server --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --faults --browser
target/debug/seekpower_server generate_openapi=true
```

日志：`/tmp/p2-vectors-final-tests.log`、`/tmp/p2-vectors-build.log`、`/tmp/p2-vectors-http.log`、`/tmp/p2-vectors-boundary.log`、`/tmp/p2-vectors-fault-build.log`、`/tmp/p2-vectors-fault-http.log`、`/tmp/p2-vectors-openapi.log`。新分支 **Windows 原生未运行**；Linux/Windows **系统崩溃/断电未验收**，仍需隔离环境及外部确认见证。未沿用旧分支原生 CI 当作本批通过。

下一批是 generation 本身：权威 staging 清单及有界完整性验证、持久但不可见的成员、精确 source revision 绑定、原子替换 active generation 与全部检索/统计集合、重试/并发发布/恢复/备份接管，复用本批的向量规则。不能用逐成员普通写入冒充原子 generation 发布。本批没有 staging/publish API、TTL/feed、关系或持续回收。

## 16. 完整 generation staging / 原子发布（本地完成，等待用户检视）

本地分支 `feat/57-p2-generations` 堆叠在 `8f5e4ab81afac233cc9d46c9c212530d65f80f5e`，经 `084320af` 依赖未合入 PR #62 `20531fa`。未推送、未修改 PR #62、未创建 PR。本批实现真实 generation 闭环，不包含 TTL/feed/关系或持续回收。

### 权威模型与契约

- `record/generation.rs` 集中处理 staging 清单、发布/取消、控制版本、资源预留及恢复校验。命令仍走现有 Record planner → owned executor → 单 WAL → 增量发布；HTTP/UI 不复制业务规则。
- `State.generations` 是文档控制状态的唯一权威。每个 document control ID 有固定 routing_key、单调 control version、最多一个 staged generation、一个 active generation 和已使用 generation ID 集合。control ID 与普通 Record、staged member ID 共用 collection 身份空间，不能互相占用。
- Stage 接收**完整** manifest：精确 Source 引用、显式 members 清单及对应 chunks；1..100 个成员，唯一且集合完全一致。所有成员自动继承 Source 依赖，可另附既有精确证据；只接受同 collection、同 Source routing group。上层负责解析/切块/模型。
- Staging 正文只存于权威 manifest，未进入 records/正文读取/全文/向量/iterator/活动统计。成功回执 `staged_only:true` 明确仅表示 durable；元数据接口可读 staged 成员数，不可读 staged 正文。单次 stage envelope 仍受 `max_record_bytes` 限制，不提供无界流式上传。
- Publish/Abort 必须 Match 文档当前 control version，并指定当前 staged generation ID；Any 不允许。首次 Stage Absent 得到 version 1，Publish Match 1 得到 version 2，下一 Stage Match 2 得到 version 3，以此递增。原 operation_id 重试返回原回执，不重新解释为另一次发布。
- Publish 在 append 前重新验证 Source/额外证据，并计算**替换后的最终可见集合**。如果新成员直接或间接依赖将被退役的旧代，拒绝并保留旧 active、seq、统计和输出许可。不会先持久提交再发现新代自失效。
- 一个 durable Applied 操作将 staged 正文移入 records，保留 `generation_member:{document_id,generation_id}` 永久归属，退役旧代；旧正文仍保留，`retired_generation` 使其及间接派生默认 stale/隐藏。新代所有正文/全文/向量可用集合、活动 BM25 统计、缓存与候选删除集在同一 partition guard 下增量切换。单查询不能跨代；旧响应经数据许可复核，不能越过来源删除/撤权屏障。
- 无全 collection 索引重建。generation 事务规划采用受 collection 上限约束的 State 候选快照与完整 checkpoint 字节核算；发布做有界全图可见性验证，再只添加新成员、退役受影响 ordinal。普通 Record 写入仍走原增量路径。这不是无界批事务或性能优化结论。
- 显式单成员删除仍是原 Record 删除语义，可减少 active generation 的 visible_members；整代**发布**保证完整，后续显式删除不会伪装成新 generation。来源更新/删除立即使关联成员 stale/隐藏，已失效 staging 不允许 Publish。Abort 可释放 staged 正文与其未发布成员 ID，generation ID 保留为 spent；**已发布成员 ID 永不复用**。
- 可选向量规则复用第 15 节：正文就绪即可发布；缺向量不召回、不补零，后补须匹配成员正文版本。后补不改变 control version、正文版本或证据关系，旧代/失效成员不允许后补。

### 持久化、恢复与容量

Stage、Publish、Abort 与其他写入共享同一 seq、operation ledger、WAL/checkpoint。取消调用不会取消已接受 owned executor；未知结果必须保留原 operation_id 重试。durable 后索引发布失败会 fence 整个分区，不返回部分成员；重启按完整权威决定重建统一视图。

身份预算包括普通/tombstone records、文档 control、staged 预留成员。每个 staged 成员保留 32 KiB 发布 envelope 开销及 8 KiB 最终 typed 删除空间，另为待完成 staging 保留提交/取消槽；普通写也尊重这些预留。来源删除、Abort 不被普通写耗尽预留空间。generation ID 保留集合有界；满额拒绝新 staging，不隐式丢历史。历史回收属于后续工作。

恢复校验控制/成员归属、active/retired 一致性、Source 身份/路由/版本范围、staged 身份唯一性及容量。完整 State 进入 collection/service 备份和最终封存水位接管；独立恢复的 typed 内容及 generation 命令保持隔离，不能凭旧备份发布。权威 entry 标签 **V6**，拒绝 V5 等旧格式，无升级迁移。

### 已实现接口与 UI

现有写入端点 `POST /api/v1/index/{id}/doc` 增加可选 `generation`，须省略 document/system/vector_patch。以下为请求结构示例，实际 epoch 使用当前值并保存原请求重试：

```json
{
  "operation_id": {"epoch": 20747, "nonce": "stage-doc"},
  "record_id": "doc-1", "routing_key": "route-1", "expected": "Absent",
  "generation": {
    "action": "stage", "generation_id": "g1",
    "source": {"record_id": "source-1", "routing_key": "route-1", "version": 1},
    "members": ["chunk-a", "chunk-b"],
    "chunks": [
      {"record_id": "chunk-a", "document": {"body": "First chunk"}},
      {"record_id": "chunk-b", "document": {"body": "Second chunk"}}
    ]
  }
}
```

```json
{
  "operation_id": {"epoch": 20747, "nonce": "publish-doc"},
  "record_id": "doc-1", "routing_key": "route-1", "expected": {"Match": 1},
  "generation": {"action": "publish", "generation_id": "g1"}
}
```

将 action 改成 abort 并匹配当前 control version 可取消 staging；控制身份和 spent generation ID 保留。`GET /api/v1/index/{id}/doc/{record_id}/generation` 返回 control version、active/staged generation、visible/staged member counts 与 visible_seq；同时执行 Catalog 授权和数据输出屏障。collection statistics 增加 staged_records，不计入 active_records。

Web UI 保留现有 editor/search；envelope 模式支持完整 stage/publish/abort 请求，并为 stage 显示“durable, not searchable”，不错误宣称已可见。重试继续保存整个原始请求与 operation_id。

### 验证与审查（本地完成）

独立审查发现并推动修复 Publish 自失效缺口；已补直接/间接旧代证据的失败前无副作用回归。另收紧非 active 成员必须 retired 的恢复验证，并澄清 Abort 释放未发布 member ID、已发布 ID 永不复用。已完成定向契约和最终源码全量：Linux **194 passed / 0 failed / 10 ignored，35 suites**，退出 0。新增取消调用回归确认已接受的 Stage/Publish 仍由 owned executor 完成，原请求重试获得相同回执。最终默认真实 HTTP/Chromium、核心依赖边界、OpenAPI generation 路由/envelope 检查均通过。审查最后两项已定点复核，无剩余阻塞。

故障注入构建及完整 HTTP/Chromium 均退出 0。接续原 session `22798` 核验终态，未重复启动或中断；日志 `/tmp/p2-generation-fault-http.log`。Stage/Publish 各覆盖 after_append_unknown、after_durable、crash_after_durable、crash_after_visible，恢复后分别验证 staging 不可见、发布成员完整可见及原请求重试。来源删除、后补向量、接管和完整服务备份恢复回归保持通过。最终测试日志 `/tmp/p2-generation-final-tests.log`；默认服务 `/tmp/p2-generation-final-http.log`；构建 `/tmp/p2-generation-final-build.log`、`/tmp/p2-generation-fault-build.log`；核心边界 `/tmp/p2-generation-boundary.log`；OpenAPI `/tmp/p2-generation-openapi.log`。本批形成有界 generation 功能闭环，随代码本地提交；不代表完整 P2 或双平台可靠性验收完成。10 个 ignored 仍为既有辅助程序，本批无新增忽略。

最终验证命令（均退出 0）：

```bash
cargo test --workspace --all-targets --quiet
cargo build --workspace
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
python3 scripts/check_core_boundary.py
cargo build -p seekpower_server --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --faults --browser
target/debug/seekpower_server generate_openapi=true
```

Windows 本批原生未运行；Linux/Windows 系统崩溃/断电未验收。需要隔离环境及外部确认见证；进程退出测试不能替代。仍仅本地实现和提交，发布/CI 待单独协调。


## 17. TTL / 时间本地功能闭环（本地验证完成，等待用户检视）

### 分支、范围与发布隔离

本批本地分支 `feat/57-p2-temporal-ttl` 从 `87ac2bb5d0f989e33a0e5bbb54b3b6cd1ef238eb` 开始。该基线已单独发布草稿 [PR #63](https://github.com/TheLudlows/seekpower/pull/63)，base 为未合入 PR #62 的 `feat/57-p2-source-evidence`，仅三个后续提交；[CI 39578995252](https://github.com/TheLudlows/seekpower/actions/runs/39578995252) 的 Linux build/HTTP/Chromium、Linux 和 Windows 原生持久化/服务故障任务全部通过，head 与远端均为上述 SHA。**这些证据不覆盖本节 TTL 修改。** 本批未获发布授权，仍仅本地实施，不混入 PR #63、不修改 PR #62、不合入。

[确认] TTL 到期不可逆逻辑删除，直接及间接依赖隐藏；仅延长 TTL 不得复活。当前检查点已接入核心转换、时间过滤、默认 HTTP/UI 和恢复结构校验；最终 Linux 本地回归通过，Windows 原生和系统故障验收尚未运行，详见本节末尾。

### 已写入代码的职责与边界

- `record/temporal.rs` 负责时间值校验、可替换时钟、owned 到期命令及持久墓碑。沿用 Record 单一 journal、partition guard 和增量活动索引；没有第二权威 TTL 数据库或独立调度服务。
- `RecordWrite.temporal` / `GenerationChunk.temporal` 保存可选 `event_time_ms`、半开有效区间 `valid_from_ms` / `valid_to_ms` 和 `expire_at_ms`；Record 保存 `ingested_at_ms`、`expired_at_ms`。默认 HTTP/UI 和 OpenAPI 已接入，实际示例见服务 README。向量后补不更改时间元数据；完整替换省略 temporal 时继承原值。到期后身份永久保留，重试原 operation_id 仍返回原回执，新操作不能复活它。
- [工程选择] 本批使用 UTC Unix **毫秒**，有效区间 `[from,to)`；业务时间可表示公元 1–9999 年，TTL 使用 1970 年起非负毫秒。原设计中微秒/RFC3339 是建议，并非用户承诺，本批不同时开放多种精度和字符串解析。event/ingested 范围与显式 valid_at_ms 条件在各检索通道候选选择前合并为 AND，不进行 TopK 后过滤；缺失 event 不匹配 event 条件。
- 到期条件为 `evaluation_time >= expire_at_ms`。新视图/写入先排空到期记录；一次 Expired 提交包含同一评估时间、水位与精确旧版本目标。持久后才删除活动 ordinal、活动 BM25 统计与缓存。失败保持关闭；原有 fixed-time 视图可按设计完成，不把 TTL 屏障冒充来源显式删除/授权撤销的即时输出屏障。
- 到期墓碑递增版本、保留来源历史与过期原因；WAL/checkpoint 均保存时间水位和过期记录；恢复校验完整连续提交 seq、时间单调性、过期原因与终态墓碑/版本的一一对应，供未来 feed 投影，但**没有 change-feed API 或 7 天保留实现**。格式标签升为 V7，拒绝旧格式，不提供迁移。
- 普通写入、staging/publish 预留到期空间；TTL Record ID/routing key 限 256 字节，避免过期时重存长 ID 超出固定预留。复杂转义长 ID 拒绝已有回归；当前不是长期容量或吞吐承诺。
- 备份不证明后续未发生删除。TTL 身份与 typed evidence 一样在独立快照中隔离；复制独立快照不推进其权威状态，避免破坏归档摘要。受控接管封存前推进原 owner 时间，最终状态由已有协议复制，目标服务前再检查时间。完整 service/catalog 恢复沿用既有安全协议；真实 HTTP 额外核验到期墓碑和撤销授权在最终状态接管后不复活。

### 时钟与资源的诚实边界

[工程选择] 进程内 wall clock 与 monotonic elapsed 的差异超过 5 分钟返回 `clock_uncertain`（HTTP 503）；小幅回退在进程内由已观察评估时间、恢复后由已持久水位钳制，不复活墓碑。启动后无法凭机器自身识别停机期间错误向前跳时，**依赖可信系统 UTC 时钟**，不承诺跨重启识别所有时钟异常。测试注入可控 Clock；显式 fault-injection 构建另支持 `SEEKPOWER_TEST_CLOCK_FILE`，普通发行构建不读取它。

当前每次推进扫描有界 Record 集合，到期批次克隆/编码完整候选状态以校验 checkpoint 预算，随后增量退役索引；不是每次写入重建全文/向量索引。全量状态大小和身份上限沿用当前 P2 有界实现，不声称具备 P3 长期回收能力。

### 独立审查与最终验证

独立审查覆盖过滤端点、缺失时间、候选过滤、向量后补时间保留、generation 与恢复、旧视图及授权边界。已修复固定预留与长 ID、备份复制误推进时钟、独立 owner 的 generation TTL 旁路。真实故障矩阵发现并修复错误分类被数据 fence 覆盖：仅无正文的 durable_pending/outcome_unknown 错误不附数据许可，但保留所有授权交付检查；成功响应和其他错误仍附数据屏障，服务单元测试覆盖最终交付和 body poll 时撤权。静态定点复核未发现剩余阻塞项。

新增确定性回归覆盖截止前 1 ms / 截止时刻、多级依赖隐藏、墓碑版本、旧操作幂等、禁止复活、时钟回退、持久后发布失败与恢复、长转义 ID 拒绝、备份隔离及复制不改变权威 cut、generation 到期拒绝发布、恢复缺失/重复过期原因、到期前条件续期、到期时竞争更新/向量补充失败、共同评估时间及各检索通道时间过滤。TTL 判定没有 sleep 依赖。既有统计不变测试新增可控时钟，避免将不同评估时间误认成状态差异。

最终源码验证（2026-10-21，本地 Linux；本节文档更新后未改功能代码）：

| 检查 | 实际结果 |
| --- | --- |
| `cargo build --workspace` | 通过 |
| `cargo test --workspace --all-targets --quiet` | **35 suites，202 passed，0 failed，10 ignored** |
| 默认真实 HTTP + Chromium | 通过；含时间窗口、缺 event、有效期端点、只计数、重启、共同时间，以及 UI envelope/时间控件 |
| 显式 fault-injection 构建 + HTTP + Chromium | 通过；含到期后追加结果未知、已持久未发布、持久后/可见后进程退出、重启幂等、不可复活、最终状态接管、旧授权拒绝与时钟大幅回退 |
| 核心边界，默认及 no-default-features 独立构建 | 通过 |
| OpenAPI 生成及 temporal schema 引用断言 | 通过 |
| 本批 Windows 原生 | **未运行**，本分支尚无推送/CI 发布授权 |
| Linux/Windows 系统崩溃、断电 | **未运行**；进程退出不等同系统故障 |

10 个 ignored 中，9 个是由父测试显式启动的子进程 worker，1 个是需要额外环境参数的既有跨构建 fixture，不单独算通过。最终汇总日志为 `/tmp/p2-ttl-acceptance.log`；各检查日志为 `/tmp/p2-ttl-acceptance-{build,http,fault-build,fault-http,tests,boundary,openapi}.log`。这些是本地执行证据，不是公开 CI。

```sh
cargo build --workspace
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
cargo build -p seekpower_server --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --faults --browser
cargo test --workspace --all-targets --quiet
python3 scripts/check_core_boundary.py
target/debug/seekpower_server generate_openapi=true
```

原生 CI 已调用的服务 smoke 入口会执行新增 `record_temporal_smoke.py`；Windows 使用 `.exe`。需要用户另行批准发布本分支并触发 Linux/Windows CI，不能用 PR #63 的结果覆盖本批。系统崩溃/断电需要隔离 VM/机器、可丢弃文件系统磁盘、外部 reset/power-cut 以及故障域之外的确认见证；不重启用户或共享环境。

本批是有界 TTL/时间功能闭环，未实现 feed 的 7 天窗口与背压、实体关系、后台物理回收或 P3 持续压缩。请求驱动过期意味着空闲时不主动写墓碑，但任何新数据视图/写入前必须执行到期转换，不能暴露已到期正文。无 TTL 独立定时服务，不承诺长期稳定磁盘占用或无界规模。保持 PR #62/#63 草稿不变，本分支只本地提交，等待用户检视与后续发布授权。

## 18. 持久 change feed（本地功能闭环，尚未发布）

独立本地分支 `feat/57-p2-change-feed` 基于 TTL 提交 `49ef3c6661298ebf35d7fa4f2a827d5eea412910`。本分支未推送，PR #62/#63 未修改；这是堆叠的本地批次，不意味着 TTL 已合入主干或完整 P2 完成。环境恢复已通过实际 shell、工作树及编译命令确认，原改动保留。

### 18.1 权威边界与模块职责

`record/change_feed.rs` 从已有 `State.operations` / `State.expirations` 唯一持久账本确定性投影事件，不另写第二份事件 WAL。`mutation` / `generation` 规划提交后应保留的失效通知配额，`RecordPartition` 在追加前做 feed 准入，并在业务发布时更新可重建的事件位置/字节缓存。TTL 用相同权威提交序列；恢复从原账本重建缓存并核验配额。HTTP/UI 只适配调用、授权和响应，不重复投影规则。

每个对外相关命令至多一个事件，以 `(scope, seq, event_index)` 去重，目前 event_index 为 0；每 partition 有序、至少一次，序列可有私有 staging/abort 导致的间隙。事件种类为 `record_changed`、`record_deleted`、`source_created`、`source_updated`、`source_deleted`、`vector_ready`、`generation_published`、`ttl_expired`。正文/向量不进入事件，隐藏内容不可通过历史事件取回；只有已提交公开身份、版本、操作 ID、已发布 generation ID 和必要失效信息。

来源修订/删除、typed 身份删除、整代发布及 TTL 聚合事件设置 `rescan_required=true`，不展开无界依赖闭包。消费者应重建有效视图；其它变更按当前授权回表取得正文，可能已不可见。缺向量补充保留原 body version，但产生独立提交序列通知。私有 staging/abort 不暴露成员身份或正文。查询授权、来源隐藏和最终输出屏障继续有效。

### 18.2 保留、背压和恢复

collection 创建配置新增 `change_feed`：`retention_ms` 默认 604800000，范围 7–365 天；`max_events` 默认 100000，范围 1–100000；`max_bytes` 默认 67108864，范围 1024–67108864。配置不可在线更改。容量包含已保留事件和未来删除/到期通知的预留；普通写入无法满足时返回 HTTP 429 `feed_full`，追加前明确拒绝，不能提前删除窗口内事件。既有 journal/operation/record 配额仍分别生效。

活跃 typed 身份、staging 控制及 TTL 保守预留通知槽和每槽 8 KiB；TTL 与 typed 可重复保留以确保安全。删除及到期消费已预留预算。保留时间使用可信提交评估时间（UTC Unix 毫秒），在七天边界及之后才可丢弃前缀；时钟异常继承 TTL 的失败关闭规则。`FeedTrim` 是同 journal 的持久元数据转换，包含旧 floor、新 floor 和评估时间，不伪造业务 seq。恢复验证裁剪是当时可到期的完整前缀。WAL 或 checkpoint 无空余预算时延后裁剪，不能占用或阻塞删除/TTL 预留。

裁剪后的 `floor` 表示已丢弃事件前缀末端，游标早于 floor 或 scope 不符返回 HTTP 410 `cursor_expired`，必须 bootstrap，不静默跳到尾部。未传游标等于从 0 开始，历史已裁剪时同样返回 410。响应 `next` 只应在幂等消费整页后持久保存。

**逻辑窗口不等于物理历史回收。** 原 operations/expirations 仍因幂等、版本、恢复而保留；checkpoint 可安全切换/回收 WAL 文件，但不提供 P3 无界持续运行。达到原账本配额仍需明确背压，不能删回执掩盖容量限制。

### 18.3 bootstrap、scope 与输出

bootstrap 在 partition guard 内原子捕获有效 Record 集合、seq、固定 evaluation_time 和对应 feed continuation。每 partition 同时一个 pin，租期五分钟；总序列化记录预算不超过 `max_journal_bytes`，分页最多 1000 条，约 4 MiB（单个合法大记录不拆分），HTTP 总响应仍受既有上限约束。只返回当前有效记录，不包括 staging、墓碑或隔离内容。

协议：

1. `GET /api/v1/index/{id}/bootstrap` 取得 `request_id_prefix`（16 hex）。客户端追加随机 16 hex，生成本次 `request_id`。
2. `POST .../bootstrap`，例如 `{"request_id":"<prefix><random16>","after":null,"length":100}`。响应包含 records、next、continuation、visible_seq、evaluation_time_ms、expires_at_ms。
3. 同 request_id 和 after 重试得到同快照页；next 非空则逐页读取。只有所有页成功应用后，才将 continuation 作为 feed cut 保存。新普通写入不会导致快照漏写：它们由 cut 之后的 feed 提供。
4. 完成后 `DELETE .../bootstrap/{request_id}` 释放 pin，再 `POST .../changes`，请求 `{"after":"<continuation>","length":100}`。
5. 重启、释放、租期到期或来源隐藏使运行期 prefix 轮换；旧请求包括首分页重试返回 410 `bootstrap_expired`。丢弃所有未完成页，重新取得 prefix，不能把旧页与新切点混合。不同请求争用有效 pin 返回 Busy。源删除/修订会使 pin 数据许可失效；TTL 保持已确认的单查询固定评估时间语义。

每页重新走 catalog 授权；HTTP 在调用前捕获数据许可，交付头部/正文时再检查授权与数据屏障。UI 提供分页面板，显示当前快照和内容无关事件，错误时清空展示，传输失败保留原快照请求供精确重试，授权/过期错误丢弃不完整状态；仅为有界人工检查工具，不是自动同步客户端，feed 面板不新增正文或 key 持久化存储（既有连接界面仍使用 sessionStorage 保存 API key）。

同 owner 重开保留 feed scope。独立备份副本换 scope；受控接管将原完整最终状态复制到新 owner，并用原 scope 与 intent 派生新 scope，因此旧游标必须重建。SealCertificate 持久绑定 source_feed_scope，目标认证仅在验证派生 scope 后归一化该字段进行原最终摘要核对，不能泛化为忽略 feed 状态。partition_id 保留来源谱系，不表示允许双写。完整 service/catalog 备份沿用实际复制对象清单和 hash 校验；恢复不复活旧 grants。当前 journal 为 V8，旧 V7 等格式明确拒绝，不实现升级迁移。

### 18.4 审查、验证和限制

独立审查修复了三项：checkpoint 预算不足的 trim 不应挡住 TTL；bootstrap 首分页响应丢失后不得静默换切点；快照已完成后的释放失败必须单独重试清理，不能误重读首页。新增 core 测试覆盖有序与幂等、恢复后稳定投影、七天裁剪/旧回执保留、scope 变更、容量及删除/TTL 预留、staging 私有、generation 发布、bootstrap 并发切点/隐藏/释放/到期/重启，以及精确 checkpoint 预算边界。HTTP 新用例位于 `scripts/record_feed_smoke.py`，由现有原生矩阵的 smoke 入口自动执行；Chromium 操作真实 bootstrap/feed 按钮。

最终默认源码验证（2026-10-21，本地 Linux）：

| 检查 | 实际结果 |
| --- | --- |
| `cargo test --workspace --all-targets --quiet` | **35 suites，208 passed，0 failed，10 ignored** |
| `cargo build --workspace` | 通过 |
| 默认真实 HTTP + Chromium | 通过；包括 feed 背压、预留删除、同切点 bootstrap、首分页响应丢失重试、释放响应丢失后保留 continuation、撤权与恢复 |
| `python3 scripts/check_core_boundary.py` | 默认及 no-default-features 独立构建通过 |
| OpenAPI 生成与新增路由/配置断言 | 通过 |
| 显式 fault-injection 构建 + HTTP + Chromium | 通过；包括七天边界、trim 结果未知/持久未发布/持久后进程退出及恢复、旧回执保留、过期游标重建与同一浏览器矩阵 |

10 个 ignored 中，9 个为父测试显式启动的子进程 worker，1 个为既有额外参数跨构建 fixture，不单独算通过。最终默认验证汇总 `/tmp/p2-feed-acceptance.log`，详细日志 `/tmp/p2-feed-acceptance-{tests,build,http,openapi}.log`；核心边界 `/tmp/p2-feed-boundary.log`。这些为本地证据，不是公开 CI。

验证过程中一次故障测试误用了被 `cargo test --all-targets` 重新生成的默认二进制，启动故障 worker 超时；已终止该临时 fixture，补充测试超时清理，并把验证顺序改为显式构建后立即测试，禁止共享 binary 的并行 feature 覆盖。未把这次失败算通过。

```sh
cargo test --workspace --all-targets --quiet
cargo build --workspace
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
python3 scripts/check_core_boundary.py
target/debug/seekpower_server generate_openapi=true
cargo build -p seekpower_server --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --faults --browser
```

故障终态汇总 `/tmp/p2-feed-fault-acceptance.log`，构建和 HTTP/Chromium 日志 `/tmp/p2-feed-fault-acceptance-{build,http}.log`。不把旧 TTL 的结果算作本批通过。Windows 原生未运行（本分支尚未获发布/CI授权）；系统崩溃/断电未运行，进程退出不是替代。系统级测试仍需隔离可丢弃 Linux/Windows VM 或机器、外部 reset/power-cut、故障域外确认见证，不重启共享环境。

本批未实现显式实体/一跳关系、P3 长期回收、跨 partition 全局排序、跨服务灾备自动激活或后台订阅服务，不扩大为模型/性能专项。本地终态通过，随代码提交供检视；推送、草稿 PR 和 Windows CI 仍需单独授权。

## 19. 实体与显式一跳关系

本地分支 `feat/57-p2-relations` 堆叠于 feed `43a898ffa330966a0d644000600ae22754f41346`，不发布本分支。#62–#65 的堆叠依赖不因本地开发自动合入。以下为本批工程契约，不把建议写成新增用户决策；验收以 19.2 的实际记录为准。

- Entity/Relation 继续是唯一 Record 权威对象，使用 `system.kind=entity|relation`；业务字段仍在自定义 document。实体没有自动名称合并/模型消歧。关系额外声明 `system.relation={from,to,relation_type,weight?}`，from/to 复用精确 `{record_id,routing_key,version}`，只引用当前 collection/partition 的已可见非 Relation 端点。未知 collection/partition 字段拒绝；不猜路由，不隐式重绑定版本。relation_type 非空、有长度上限，weight 可选且有限，不自动充当全文/向量分数。
- 边、双端及其证据/来源/generation/TTL 必须同时有效，统一决定普通 get/query/iterator、活动统计、邻接输出。端点版本变化即隐藏旧边，ABA 不能恢复旧引用。关联关系不是内容证据；需 evidence_refs 显式声明依据。普通图循环、自环合法，证据/可见性依赖环拒绝；首版不允许以关系为关系端点，不提供任意遍历语言。
- 实体/关系正文可精确 Match 条件替换，system 类型、证据和端点声明不可变；重新绑定端点或重算失效依据使用新身份，与当前派生重算边界一致。删除/TTL 不复活。该保守声明契约便于未来另增显式关系修订命令，不提前把隐式重绑定引入普通替换。
- 一跳请求使用至多 32 个按调用顺序排列且不重复的精确 seeds、in/out/both、至多 16 个 relation_type、每 seed 至多 128 条、总候选预算不超过 collection max_candidates、总响应字节预算。稳定按 seed 顺序、neighbor ID/edge ID 排序，自环在 both 仅一次。只计有效边路径，不把隐藏端点/边名字或数量返回。默认精确模式超预算拒绝；仅显式 allow_partial 才返回 complete=false 和窗口计数，不把窗口当全图精确数。
- 实体目录、正反邻接为可丢弃重建缓存，无独立 journal。增量提交更新目录，端点/来源失效复用可见性闭包和输出屏障；重开、checkpoint、备份/接管从权威 Record 重建。feed 复用原业务提交，关系变化/端点失效提供必要 rescan，不双写新流。HTTP/UI 薄适配，同查询固定 seq/evaluation_time/统计，撤权与显式隐藏在输出复核。

实施顺序：权威模型和合法性 → 可见性/增量统计/输出与恢复 → 有界实体目录和一跳 API → 默认 HTTP/UI 与故障/并发测试 → 独立审查和最终全套 → 更新完整 P2 缺口清单。Windows 原生与系统断电分别记录，不借用 TTL/feed CI。

### 19.1 实际落点与一致性

`record/graph.rs` 负责精确端点验证、实体目录和双向邻接可重建缓存、有界一跳读取；`evidence.rs` 在共同依赖闭包中检查双端及证据。普通端点更新也进入 publication 的失效集合与数据输出屏障。generation 切换和 TTL 沿用整体有效集合计算，正文、全文/向量可用集合和活动统计同时退役失效关系及其下游。

`Applied.graph_invalidation` 在规划时确定并持久化，feed 重放使用该值，不能因后来新增边改变历史事件。邻接缓存没有独立持久日志，重开/接管由 Record 重建；元数据不可重绑定使增量插入与恢复一致。邻接读取惰性归并两个有序集合，预算外不先复制整张邻接表；保留墓碑邻接以保持重建前后候选预算一致，仍受 max_records 总限额。

服务新增受保护 `POST .../entities` 与 `POST .../neighbors`，UI 只传递请求和展示结果。响应共同水位、时间、统计；重新授权与数据屏障不移到浏览器。具体 JSON 与约束见 server README。本批 journal 升为 V9；拒绝旧 V8 等格式，不提供历史迁移。

独立审查发现新关系额外依赖一层可能把深度 16 端点扩成 17，并在持久后才失败；现从 depth=1 校验端点，在追加前拒绝，已补无 seq/统计/输出许可变化及重启回归。复审未发现新的阻塞缺陷。其他覆盖包括 A→B/B→A 与自环、精确条件重试、普通端点删除/重建 ABA、generation staging 不影响旧边/发布退役旧边及间接派生、source 删除、TTL、实体/关系 system 不可变、feed 失效历史稳定、checkpoint 重建、备份隔离与受控接管。

### 19.2 验证状态

2026-10-21 最终本地 Linux 源码验证已完成，独立复审及增量复审未发现未修复的阻塞缺陷。本批新增四个核心关系测试，真实 HTTP 脚本为 `scripts/record_graph_smoke.py`，由现有 smoke 入口执行；Chromium 操作真实实体/边编辑、实体目录和一跳按钮。

| 验证命令 | Linux 实际结果 | Windows 本批 |
| --- | --- | --- |
| `cargo test --workspace --all-targets --quiet` | **35 suites，212 passed，0 failed，10 ignored** | 未运行 |
| `cargo build --workspace` | 通过 | 未运行 |
| `python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser` | 通过；包含实体/边、预算、版本变化隐藏、备份隔离/接管和撤权 | 未运行 |
| `cargo build -p seekpower_server --features fault-injection` 后立即运行上述 smoke `--faults --browser` | 通过；关系写入 append unknown、durable pending、持久后/可见后进程退出与精确重试，同时重跑 generation/source/TTL/feed/接管故障矩阵 | 未运行 |
| `python3 scripts/check_core_boundary.py` | default/no-default-features 独立构建通过 | 未运行 |
| `target/debug/seekpower_server generate_openapi=true` 及关系 schema/path 断言 | 通过 | 未运行 |
| `git diff --check` | 通过 | 不适用 |

10 个 ignored：9 个由父测试显式启动的子进程 worker，1 个既有额外参数跨构建 fixture；不单独算通过。最终全量在补完 ABA 删除/重建断言后重跑，未用修改前结果冒充最终验证。默认测试、默认服务、fault 构建/服务顺序执行，避免 feature 二进制相互覆盖。日志：`/tmp/p2-relations-final-{all,build,http,fault-build,fault-http,boundary,openapi}.log`，仅为本地证据。此检查点只本地提交，未推送或创建关系 PR。

Windows 本批未运行，尚未获本分支推送/CI授权；系统崩溃/断电未运行，需要隔离可丢弃双平台环境、外部 reset/power-cut 及故障域外确认见证。上游 TTL #64 和 feed #65 的原生 CI 已成功，但不作为本批结果；进程故障也不能替代系统断电。

### 19.3 对照原 P2 清单的真实缺口

| 原阶段 | 当前已实现 | 尚未完成或受限范围 |
| --- | --- | --- |
| P2-1 来源/证据 | 来源精确版本、直接/间接依赖、删除与 stale 隐藏、共享授权及输出屏障、同服务接管 | 内容 hash/media_type 与精确字节/页码 locator 的专用契约尚未实现；普通 document 字段不能冒充引擎校验定位。管理型大文件 Blob 未提供，是否选用按实际输入需求确定 |
| P2-2 generation | 持久 staging、完整清单、整代原子发布/abort、旧代退休、失败重放/幂等 | 没有租约/fence 和自动过期 staging 清理；当前必须显式 abort，受总容量背压，不承诺无限运行 |
| P2-3 可选向量 | 正文先发布，缺向量不召回，精确版本后补、原始向量保存 | 未提供异步任务领取或失败原因上报服务；上层负责计算与重试，不在底座执行模型 |
| P2-4 时间/TTL | event/ingested/valid 过滤、固定 evaluation_time、不可逆过期和依赖隐藏、持久可信时间与恢复 | 到期由操作入口触发，尚无独立后台调度器；无请求时不承诺及时物理回收。独立时间召回融合未新增 |
| P2-5 实体/一跳 | 本批实体声明、精确双端、邻接、有界稳定枚举及 exact/window、默认 HTTP/UI | 一跳接口与全文/向量是分开请求，尚未实现单 ReadView 的关系融合查询/评分解释；客户端串接不能宣称同视图。模型抽取、消歧、完整图语言明确非目标 |
| P2-6 feed | 同权威事件、七天窗口和背压、bootstrap 同切点/租期、scope、恢复/撤权 | UI 是有界检查器，未提供后台重算/订阅服务；历史账本并未随逻辑事件裁剪物理 GC |
| P2-7 集成 | 各切片真实服务/UI、备份接管、审查与回归逐批记录 | 本批最终平台验证、上述定位/融合/必要 staging 生命周期等仍需后续收敛；不能称完整 P2 已完成 |

P3 操作/版本历史持续回收、Segment compaction 另属后续阶段，不以本批解决；P4/P5 云对象存储/indexer/cache、P6 分区亦未实现。系统断电是验收环境缺口，不能与上述功能缺口混为一项，也不能用进程退出或原生 CI 替代。本地分批结束供用户检视，不自动扩大当前关系切片或合入主干。

## 20. P2 收尾第一批：精确内联来源定位

本地 `feat/57-p2-source-locators` 基于关系检查点 `2d985c1b5a1238af047698a80eda029c246fb069`。关系分支保持原提交，新收尾不混入关系 PR，也未获发布授权。

### 20.1 原计划与分批边界

§3.2/P2-1 要求精确 source revision/locator、原文和摘要及受保护历史证据读取；现有精确 ID/version 与 source_revisions 已具备，缺 locator 校验及读取。§3.4/§7/P2-2 要求 staging 截止/租约/fence 和安全回收，当前只有持久整份 staging 与显式 abort。P2-5 要求关系候选和全文/向量同 ReadView 融合，当前独立一跳与 query 不能串接冒充原子视图。这三项分为定位、租约清理、同视图查询三个本地检查点；本节只交付第一项。P3 通用历史账本/版本 GC、Segment compaction 不进入收尾。

本批工程选择：只定位已持久保存的内联文本，不增加解析器/上传服务。UTF-8 字节半开区间对应一个 top-level string 字段；页码对应上层给出的文本页字符串数组。数组自身提供可信可校验的页数边界，底座不验证它是否忠实对应某个外部 PDF。普通 document 中的 URL/MIME 标签不是已认证原文。未新增业务策略默认。

### 20.2 实际模型和接口

`EvidenceRef` 增加可选 `locator`：

```json
{"record_id":"source","routing_key":"route","version":1,
 "locator":{"kind":"utf8","field":"body","start":1,"end":6}}
```

若原文为 `A中éZ`，上述字节区间取 `中é`；start/end 必须为合法 UTF-8 边界，start < end <= 字节长度，不接受字符索引。页定位为 `{"kind":"page","field":"pages","page":2}`，仅接受该字段为字符串数组且 1 <= page <= len；不解析 PDF，空页可返回空文本。字段名非空且至多 256 UTF-8 字节。locator 只能指向 Source，间接派生依赖仍使用无 locator 的精确 Record 引用；关系端点/seeds 拒绝 locator。沿用一个 source 身份至多一个 evidence_ref 的预算/去重规则，generation source locator 自动继承，重复同 source 的不同 locator 不悄悄合并。

`POST /api/v1/index/{id}/evidence` 接受完整上述 EvidenceRef，返回 `reference,text,content_sha256,media_type,visible_seq,evaluation_time_ms`。SHA-256 是完整文本字段（utf8）或完整所选页（page）的 UTF-8 字节摘要，不是摘录片段摘要；media_type 固定 `text/plain; charset=utf-8` 描述实际支持的内联表示，不把用户字段当认证文件类型。摘要由权威正文确定性计算，不新增内容副本或双权威 hash 状态。

写入验证器先验证精确当前版本及位置，再交现有 WAL/发布器；operation_id 重试包括 locator 语义。`locator.rs` 单独管理位置/内容验证及摘录，evidence/generation 调用其规则，HTTP/UI 不复制规则。恢复用当前 Record 或 source_revisions 的精确历史正文验证已持久 locator；staging 中的 source/chunk 引用也校验。V10 journal 拒绝旧 V9 等格式，无迁移。

普通 get/query 仍隐藏 stale 派生；显式历史摘录仅允许当前 source 仍有效时访问保留的精确旧修订。当前删除、TTL、恢复隔离、封存均拒绝，不以“历史审计”绕过。每次 HTTP 重新授权并复用双输出屏障，source 更新/删除使已准备输出许可失效；撤权始终复核。备份/接管使用原权威状态及当前删除水位，不新增灾备激活入口。UI 记录页提供精确 reference 输入、摘录与摘要展示，身份切换清空，不新增本地持久缓存。

### 20.3 验证状态与后续

2026-10-21 本地 Linux 最终源码验证完成，独立审查未发现阻塞缺陷：

| 命令/范围 | Linux | Windows 本批 |
| --- | --- | --- |
| `cargo test --workspace --all-targets --quiet` | **35 suites，214 passed，0 failed，10 ignored** | 未运行 |
| `cargo build --workspace` | 通过 | 未运行 |
| `python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser` | 通过，真实摘录/摘要 UI、源修订/删除/撤权、恢复隔离/接管 | 未运行 |
| `cargo build -p seekpower_server --features fault-injection` 后 smoke `--faults --browser` | 通过，定位引用在 unknown/durable_pending/持久后退出/可见后退出的精确重试；同时回归既有来源/generation/关系/TTL/feed 矩阵 | 未运行 |
| `python3 scripts/check_core_boundary.py` | default/no-default-features 独立构建通过 | 未运行 |
| `target/debug/seekpower_server generate_openapi=true` 与 locator schema/route 断言 | 通过 | 未运行 |
| `git diff --check` | 通过 | 不适用 |

10 个 ignored 仍为 9 个父测试启动的子进程 worker 和 1 个既有额外参数跨构建 fixture。新增核心测试覆盖 Unicode 字节边界/页边界、拒绝前无提交、精确历史正文摘要、损坏历史 locator 拒绝恢复验证、删除输出屏障、TTL 和 staged source 更新后重启/拒绝发布。HTTP 使用临时 collection/keys/archives，不操作真实用户数据。

首轮 Chromium 因折叠面板内输入框未展开超时，HTTP 已通过但未将该轮算全通过；现将摘录控件放到独立面板并明确展开，最终默认和 fault 两轮浏览器测试均成功。测试日志 `/tmp/p2-locator-final-{all,build,http,fault-build,fault-http,boundary,openapi}.log` 仅为本地证据。Windows 本批未运行（没有本分支发布/CI授权），系统断电未验收（需隔离环境与外部确认见证），不使用关系或 feed 的结果替代。

后续独立批次：staging 租约及安全清理；关系与全文/向量同视图融合。管理型 Blob 仍依真实大文件需求选择，不能把外部 URL 当已备份文件。通用非 Source 的任意历史修订读取未新增；本批只提供计划所需的精确内联 Source 摘录。

## 21. P2 收尾第二批：staging 租约与私有正文清理

分支 `feat/57-p2-staging-leases` 堆叠于来源定位 `50459fa9c8287eee33e826b58ecd8fe7c9673833`；此前关系/定位检查点保持独立，未获本批推送授权。只处理尚未发布的 staging，不压缩已发布正文、证据修订、墓碑或普通操作历史。

### 21.1 租约、fence 与上传边界

工程参数沿用原设计的 30 分钟：Stage 的持久 observed_time 决定 `deadline=observed_time+1800000`（UTC Unix ms）。`generation:{action:"renew",generation_id}` 与现有 record_id/routing_key/operation_id/expected Match 一起提交；仅未到期 staging 可续租，期限为本次固定可信时间 + 30 分钟，control version 同时递增。control version 就是 fencing token，不再平行引入第二个计数器。Publish/Abort/Renew 都必须 Match 当前版本，到期先于所有新请求处理；旧 fence 拒绝。相同 operation_id 重试返回原回执，不因为到期而重新创建 staging。Renew 回执 `staged_only=true`，从不声称成员可检索。

`generation_status` 增加 lease_expires_at_ms、lease_fence、lease_requires_renewal、private_cleanup_pending。UI 可检查期限/fence，并通过现有持久重试编辑器发送 renew，避免另建一套重试存储。当前上传仍是完整有界 Stage 请求：HTTP 完整解码前没有可回收的服务器 staging 对象；成功 Stage 后在租期内保留私有内容，上层长任务显式续租。不宣称提供分块文件上传或网络连接本身能无限延长租约。

staging 绑定创建/续租时 owner 的 feed scope。正常同 owner 重启保留 scope 和剩余期限；独立快照仍隔离、不推进期限，受控接管换 scope 后必须新授权显式 Renew，再按新版本发布。旧 owner 已持久封存；scope 仅作 fencing 身份，不赋予授权。期限已到的目标不能以续租复活；需新的 generation ID，原 ID 永久 spent。无需在认证接管摘要中忽略 staging 状态。

### 21.2 持久过期与有限清理

`lease.rs` 处理有界到期集合，在同一 PartitionJournal 追加 LeaseExpired，携带 seq、可信 evaluation_time、每个 document/generation 的精确原版本与期限；应用后控制版本递增、释放 manifest 私有成员。记录参与全局连续提交/可信时间校验，但不投影内容 feed（staging/renew/abort/expire 都是私有控制变更），feed 游标允许这些 seq 间隙。沿用 generation 接受时的终止/完整 checkpoint 预留；资源不足先失败关闭，不把未持久过期当已回收。

仅 Abort 或 lease expiration 会将对应、从未发布的 Stage 操作正文/成员清单替换为规范化请求 SHA-256；其 operation_id、原 receipt、命令身份及时间仍在账本。规范化递归排序对象 key，保留数组顺序，以匹配原请求对象等价语义。原请求重试重新计算摘要，相同则返回原回执，改变正文/清单则冲突；不重新 Stage。已发布 Stage 不走此清理，普通操作不走此清理。恢复验证 redaction 具备对应 Abort/LeaseExpired 终态且没有仍活动或已发布成员引用，不能靠一个 hash 字段任意丢正文。

清理先是已持久的逻辑退休，再在后续入口发布完整 checkpoint，释放旧 journal owner 后回收旧 WAL；不删除已有独立备份中的内容，也不承诺安全擦除。备份 pin 对不可变独立 archive 的保护沿用原协议，不在这里跨目录级联。checkpoint 已发布与旧代回收分别确认：回收失败保持可重试 private_cleanup_pending；已知未发布不破坏终态，发布结果未知则 fence。重开必须先成功回收旧代，否则拒绝服务。故障后成功重试不会复活内容。

该 pending 是本地回收状态：checkpoint 已持久发布 false 但旧代回收失败时运行态恢复 true 并重算快照预算；若此时重启，PartitionJournal 必须先成功清理旧代才能打开。seal/备份仍持久化其实际完整切面。没有额外 GC 服务；无请求时不承诺及时清理，下一入口推进到期/回收。累计回执、spent generation ID 仍占既有配额，长期运行依赖 P3。

### 21.3 审查与验证

2026-10-21 本地 Linux 最终验证完成。独立审查发现旧 checkpoint API 在旧 WAL 删除失败时仍返回成功，原实现错误清空 pending；已增加显式 reclaim_obsolete 确认与故障回归，定点及最后复审通过，无未修复阻塞项。private_cleanup_pending 是 partition 级旧 WAL 回收状态，不表示当前活跃 staging 正在被删除。

| 命令/范围 | Linux 最终源码结果 | Windows 本批 |
| --- | --- | --- |
| `cargo test --workspace --all-targets --quiet` | **35 suites，219 passed，0 failed，10 ignored** | 未运行 |
| `cargo build --workspace` | 通过 | 未运行 |
| `python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser` | 通过；真实 renew/fence、租约 UI、接管须续租、abort 清理及原回执 | 未运行 |
| `cargo build -p seekpower_server --features fault-injection` 后 smoke `--faults --browser` | 通过；可控时钟到期、unknown/durable_pending/持久后退出/可见后退出、旧代删除失败与恢复、全套既有矩阵 | 未运行 |
| `python3 scripts/check_core_boundary.py` | default/no-default-features 独立构建通过 | 未运行 |
| OpenAPI 生成与 Renew 协议断言 | 通过 | 未运行 |
| `git diff --check` | 通过 | 不适用 |

新增五个核心测试覆盖租约延期/到期、canonical 对象键顺序幂等、到期恢复/owner scope、同 fence Renew/Publish 并发单赢家，以及 cleanup 失败保持 pending→重试→仅剩一个 WAL 代且不含私有正文。10 个 ignored 仍为 9 个父测试运行的子进程 worker 和 1 个既有额外参数跨构建 fixture。补完竞争与恢复原因校验后重新执行最终全量，未以之前通过结果替代。

首轮 HTTP 夹具因浏览器先占满 32 个备份槽而失败；将本批备份/接管流程安排在浏览器前，未来时钟到期故障安排在浏览器后，最终两种服务矩阵均通过，没有放宽生产预算。全部删除/封存/故障仅针对 TemporaryDirectory 中的测试服务数据。日志 `/tmp/p2-lease-final-{all,build,http,fault-build,fault-http,boundary,openapi}.log` 为本地证据。

本批未推送、未创建 PR。Windows 本批未运行（未获发布/CI授权）；系统崩溃/断电未验收，需要隔离可丢弃双平台环境和外部确认见证。Linux 进程退出故障不能替代这些验收。

下一独立批次仍为关系与全文/向量同 ReadView 融合。本节不将三项 P2 收尾或总体 P2 标成全部完成。

## 22. 同 ReadView 关系/全文/向量融合（本地检查点完成）

独立本地分支 `feat/57-p2-relation-fusion`，基于租约检查点 `6a05aa2573d651e37536469ce94c9674fd82d205`。没有推送/PR 授权，不修改已有 PR。前述阶段是历史记录，本节与下一节作为最新收敛索引。

### 22.1 实际实现与契约

- 新 `record/fusion.rs` 只编排查询，不新增持久权威状态。通用 `Inner::query_view` 与 `graph::neighbors_view` 由同一 partition guard 调用；请求入口推进时间一次，全文、向量、邻接、回表、统计共用同一 `visible_seq/evaluation_time_ms`。复用 `metadata_filter` 编译结果过滤邻居，包含系统时间字段；不在 HTTP/JS 重写过滤逻辑。
- 默认 `/query` 接受显式 `relation_fusion`，没有此字段仍走原评分路径。复用 `RrfOptions` 和共享零基 rank 算式，独立取各通道候选后相加；同邻居多条边/多个种子只贡献一次。邻接 rank 为请求 seed 次序、neighbor ID/edge ID 顺序，最终分数相同按 Record ID。不是语义边权排序或检索质量提升承诺。
- `relation_weight` 有限且正；文本/向量可用原权重 0 禁用，但至少开启其中一个。开启通道必须给显式候选上限，全部通道上限之和不超过 collection max_candidates；关系扫描有界、隐藏/过滤项也计预算。过滤通过后才计 per_seed。默认超限整体失败，显式 allow_partial 才返回邻接窗口。字段排序、Count-only、重复 top-level RRF 配置拒绝。
- `fusion.count_relation=candidate_window`、候选 union 数与 `relation_complete/relation_reason` 独立披露。完整邻接不表示全文/ANN 全库完整召回；count_total 只是候选 union，分页不改变它。不把 ANN 当 oracle 精确召回。4 MiB 输出预算超限拒绝。
- 原授权/data 双输出屏障保留，取消只丢弃响应，不取消已接收的时间/可见性维护。source/generation/TTL/端点变更作用于原统一活动索引，无第二份可见集合。查询无新持久状态，WAL 仍 V11；恢复与接管重建相同检索/邻接索引。
- Web UI 沿用实体关系面板，传递显式 query JSON 和现有 seed/budget 控件；服务负责规则，界面显示候选窗口与不完整原因。实际请求见 [server README](../seekpower_server/README.md#explicit-same-view-relation-fusion)。

### 22.2 审查与最终验证

独立审查已确认共同 guard、过滤/统计/预算及 HTTP 双屏障，指出零关系权重仍遍历的歧义；现拒绝零关系权重。定向 core 两项通过，覆盖独立通道排序 oracle、重复邻居去重、恢复排序、输出屏障、窗口/预算与时间 TTL。generation 切代/间接证据测试追加融合断言。真实 HTTP 融合测试加入已有 graph 恢复/接管/撤权/四故障流程，Chromium 操作真实融合按钮。最终源码全量与默认/fault HTTP/Chromium 均通过；最后独立复审无未修复阻塞。

2026-10-21 最终验证（无新分支推送/PR；全部测试仅临时数据）：

| 命令/范围 | Linux | Windows 本批 |
| --- | --- | --- |
| `cargo test --workspace --all-targets --quiet` | **35 suites，221 passed，0 failed，10 ignored** | 未运行 |
| `cargo build --workspace` | 通过 | 未运行 |
| `python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser` | 通过；真实融合 API/按钮、预算拒绝/partial、撤权/恢复/接管 | 未运行 |
| `cargo build -p seekpower_server --features fault-injection` 后同 smoke `--faults --browser` | 通过；四种写入故障重试恢复后的融合查询，以及既有 TTL/lease/feed/service 故障矩阵 | 未运行 |
| `python3 scripts/check_core_boundary.py` | default/no-default-features 通过 | 未运行 |
| OpenAPI 生成及 RelationFusion/NeighborRequest/RRF 断言 | 通过 | 未运行 |
| `git diff --check` | 通过 | 不适用 |

10 个 ignored 是 9 个由父测试显式运行的子进程 worker 与 1 个既有额外参数跨构建 fixture，不将 ignored 记为通过。全量在最后 OpenAPI 源码改动后重跑；默认和 fault 服务依次构建/运行，未复用祖先分支 CI。日志 `/tmp/p2-fusion-final-{all,build,http,fault-build,fault-http,boundary,openapi}.log` 为本地证据。原生 Windows 与双平台系统断电仍未验收，后者需要隔离环境及外部确认见证。

## 23. P2 原设计需求—实现—测试收敛表

本表对照 §1–8 与 §19.3 的历史缺口，不以融合一项代表全部验收。所有代码在当前本地堆叠分支，不意味着已合入主干。

| 原计划 | 当前实现/代码责任 | 可核验测试与边界 | 剩余/设计收敛 |
| --- | --- | --- | --- |
| P2-1 来源、版本、直接/间接证据与定位 | evidence/enrichment/mutation；Source 历史版本；locator 精确 UTF-8 字节或 inline 页及原内容 hash | source/revision/locator core；record_locator_smoke；删除、stale、精确版本/路由、循环/深度预算、恢复与输出屏障 | 小内联原文已实现；外部 URL 不是已备份原文。管理型大文件 Blob/解析器未提供，按实际输入再决定，不能声称已有 |
| P2-2 generation/staging 原子发布 | generation/publication/lease；一份完整有界 Stage + Publish/Abort/Renew，持久但未发布不可见；30分钟租约、scope+版本 fence、安全私有清理 | generation/lease core；HTTP/Chromium；取消/重试/多组件退出/新 owner 续租/清理失败重试 | 此融合检查点仍只有整份Stage，不等价于issue明确的分批上传；后续多批次实现见§24。不是无限上传/后台GC，空闲时不承诺即时物理清理 |
| P2-3 可选向量与后补 | vector_patch/enrichment，精确正文版本与 embedding space，缺向量不占位 | vector_patch/generation core；服务重试/冲突/备份接管流程 | 模型/向量生成任务调度属于上层；没有计算服务或质量承诺 |
| P2-4 时间/TTL | temporal，固定毫秒 evaluation_time、事件/入库/业务时间、请求驱动不可逆过期及依赖隐藏 | temporal core + record_temporal_smoke，边界/回拨/恢复/输出/统计；融合 temporal oracle | 无独立后台调度服务；独立时间候选/排序是§3.5的工程建议而非首版硬门槛；当前为issue要求的时间硬过滤；不把物理回收算作 TTL 已完成 |
| P2-5 显式实体/一跳/共同查询 | graph 重建缓存 + fusion 同 guard，精确双端、过滤/排序/预算窗口 | relation_*、fusion_* core；record_graph_smoke + Chromium；generation/TTL/source失效、撤权、重启/接管 | 同 collection/partition、非关系端点；无自动抽取/消歧、多跳语言。原文未要求的跨分区图不加入 |
| P2-6 持久 change feed | change_feed + 通用 Applied/Expiration，同权威提交；至少7天、容量背压、scope/floor/bootstrap | feed core + record_feed_smoke；阶段退出/游标过期/恢复/隐藏/私有 staging 不泄露 | UI 是检查器，不是自动同步消费者；消费者自行幂等至少一次处理，过期必须重建 |
| P2-7 HTTP/UI、恢复、授权与验收 | 默认 record_service/catalog/data 输出；service/collection snapshot 与同服务受控接管 | 完整服务脚本含共享 key/撤权、旧授权不复活、原 owner 封存、恢复故障、实际 Chromium | 新服务缺可信最新删除历史仍隔离；不提供强制绕过/跨服务自动灾备。当前最终本地 SHA 的 Windows 原生测试未运行；系统断电未验收 |

经§24原issue审计校正：通用RecordRevision历史API、独立时间候选是内部工程细化/可选能力，当前未实现，不自动扩大首版硬门槛。逐命中通道与原始分数则是issue首版明确要求，当前只有融合总分/窗口元信息，须在后续独立批次补齐。多批次staging也是明确要求，不能把完整Stage当等价替代；实现见§24。

真实尚未完成的验收项：本批 Linux 全套/真实服务与复审已通过；本地关系、定位、租约、融合链最终 SHA 的 Windows 原生 CI 待批准发布后运行；双平台系统崩溃/断电需要可丢弃隔离环境和外部已确认写入/撤权/删除见证，不能用进程退出代替。P1-A 可靠性门槛未因此取消。

非本阶段：P3 通用历史版本/幂等账本长期 GC、Segment compaction 和稳定持续运行容量；P4/P5 对象存储/indexer/cache/云协议；P6 分区扩展。当前只有 Source 提供专用历史证据读取，其他对象历史保存在权威账本，不宣称通用历史 MVCC 读取服务。没有升级迁移/旧格式兼容。多批次staging属于原issue明确要求（见§24），不能划入P3；无限流式文件上传和管理型Blob则按实际输入需求选用，当前未提供，不把上层URL文件当已备份内容。

## 24. 有界多批次 generation 上传（本地检查点完成）

分支 `feat/57-p2-generation-batches`，基线 `58b571616e8b40572c9e27d305f4c07cda7bf7e5`。本批只补原 issue 明确的大文档分批 staging 缺口；逐命中通道/原始分数解释在下一独立检查点实现。仅本地实施、测试、提交，不推送或修改 PR。

### 24.1 范围审计校正

重新读取 issue #57（更新时间 2026-10-21T01:39:56Z）：原「记忆与知识的更新语义」明确要求大文档分批 staging 后最终发布，不能以此前完整单条 Stage 视为等价完成；「查询与排名」首版结果明确要求命中通道和原始分数。它们是需补齐项。§23 当时把其余设计差异并列为硬缺口不准确，按以下分类收敛：

- 通用任意 RecordRevision 历史读取是本文 §3.2 的内部设计细化；原目标要求来源/证据版本可追踪，不要求无限期历史快照或任意历史审计 API。当前 Source 历史、Derived 不覆盖原身份及失效隐藏的开放语义保持；通用历史 API 未实现，但不自动增加首版硬门槛。
- 独立时间排名是 §3.5 的工程建议；issue 首版要求范围硬过滤，可选时间召回/近期偏好不等于强制第三排名通道。当前时间仅过滤，独立时间排名未实现，不以新文档扩大原目标。
- 管理型 Blob 按输入需要选用；issue 允许完整来源由上层系统管理。当前未提供大文件上传/BlobStore，URL 所指文件没有因此被 SeekPower 备份。不得将 Blob 自动划为仅 P4 能做，也不因没有托管大文件而否定内联首版。
- §6 的 1000 chunks/64 MiB 是工程建议，不是已批准容量承诺；当前保持明确较小可恢复边界。P3 历史 GC/Segment compaction 不进入本批。

### 24.2 实际协议与模块

`record/upload.rs` 负责成员清单、批次累计、摘要和恢复核验；`generation.rs` 继续承担控制版本、资源规划和整代发布；`lease.rs` 复用私有清理/幂等摘要。HTTP/JS 不复制业务规则。

- Begin 固定 source/version、generation ID、routing 和完整有序成员清单，并预留全部成员身份；未接收正文也不能被普通写抢占。StageBatch 只追加尚未收到的声明成员，每批独立 operation_id/expected。不能修改 source、替换已收到正文或添加未声明成员；修正需 Abort + 新 generation。
- 单命令仍≤1 MiB，整代 received chunk JSON≤4 MiB、成员≤100；总身份、操作、完整 checkpoint/WAL 预算继续生效。不是无限内存暂存或流式文件系统；当前权威状态的内存和复制受这些硬上限约束。未sealed额外预留Seal槽，完整批次成功后仍保有Seal→Publish终结路径。
- 每批独立 Applied WAL；Seal 校验完整成员及 canonical SHA-256，Publish 仅小型控制命令，均不重复装入全量 chunks。完成 Seal 前所有批次不可搜索，最终仍由既有增量发布一次切换正文、索引、证据和统计。
- Begin/新批次/Seal/Renew 刷新30分钟租约；相同请求重试仅返回原回执，不延期。恢复owner需显式Renew后才能追加/Seal/Publish，旧fence失败。取消沿用owned executor；未知结果保留原operation_id处理。
- Abort/过期redact全部私有StageBatch正文，保留原请求摘要/回执，之后安全checkpoint回收旧WAL。Begin仅保留清单和来源元数据，无私有正文。已发布批次不由此回收。
- checkpoint 恢复不能独立信任 `sealed`：从保留 Stage/Begin/StageBatch/Seal/Renew 决定重建当前 staging，校验摘要、正文、顺序、截止时间，再与状态比较。伪造sealed、篡改Seal摘要、调整chunks顺序均拒绝恢复。journal V12，拒绝旧格式，不提供迁移。

请求与摘要编码见 [server README](../seekpower_server/README.md#bounded-multipart-generations-v12)。现有小文档完整Stage继续可用；两种入口共用一次权威提交和发布路径。

### 24.3 最终验证记录

四项定向core测试通过：两批各低于1 MiB、总正文超过1 MiB、实际Applied逐批且Publish<2 KiB；完整状态重启/检查点；过期后每批redaction与原回执；三类损坏checkpoint实际open拒绝；满额仍能Seal→Publish。独立审查发现并修复sealed来由缺口和少预留Seal槽，复审无未修复阻塞。

最终源码验证全部通过（2026-10-21，Linux）：

| 命令/范围 | Linux | Windows 本批 |
| --- | --- | --- |
| `cargo test --workspace --all-targets --quiet` | **35 suites，225 passed，0 failed，10 ignored** | 未运行 |
| `cargo build --workspace` | 通过 | 未运行 |
| `python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser` | 通过；真实1.2 MB正文分两批、接管中途续传、摘要拒绝、checkpoint/restart与浏览器上传发布 | 未运行 |
| `cargo build -p seekpower_server --features fault-injection` 后同smoke `--faults --browser` | 通过；Begin/StageBatch/Seal/Publish各四故障点，以及全部既有服务矩阵 | 未运行 |
| `python3 scripts/check_core_boundary.py` | default/no-default-features通过 | 未运行 |
| OpenAPI生成/多批协议断言、`git diff --check` | 通过 | 未运行/不适用 |

10 ignored为9个由父测试启动的子进程worker和1个既有跨构建辅助fixture，不单独算通过。初轮到期测试在重开后忘记重新绑定测试时钟而报ClockUncertain，已修正夹具，未放松时钟保证。最终全量在恢复/容量修复后执行；默认服务与fault服务依次构建/验证。日志 `/tmp/p2-batches-final-{all,build,http,fault-build,fault-http,boundary,openapi}.log`。全部删除/封存/退出故障只使用临时数据。

本批仅本地提交；Windows原生未运行，系统断电仍需隔离环境和外部确认见证。下一独立批次仍为逐命中通道/原始分数/RRF贡献，不将本检查点称为全部P2验收完成。
