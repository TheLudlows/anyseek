# Issue #57 后续核心路线：Segment、云端权威提交与 collection 扩展

状态：**设计待审阅，未开始 P3/P4/P5 编码**。2026-10-21。本文依据 [issue #57](https://github.com/TheLudlows/seekpower/issues/57) 原规划、[P1 实施记录](implementation-57.md)、[P2 实施记录](implementation-57-p2.md) 及用户“本轮结束之后加速后续规划，边缘非核心功能可以先遗留，先整理设计文档”的指示。加速指减少串行边角交付、并行推进核心模块，不取消持久化、一致性、权限和恢复门槛。

文中 **[确认]** 是已确认边界，**[建议]** 是本次工程方案，**[待决]** 必须在相应实施前确定；伪接口、布局和序列是设计，不是现有 API。本文不授权合入、部署、真实数据封存或启动新服务。

## 1. 实际基线与本轮收口

代码基线：`b0c16c0882b48cb21d565322a13ae7c7d60f9825`，包含至 PR #67 的堆叠实现及Windows测试观察顺序修正；功能检查点为`309fcea8d2b9532ab01dbb8f935e6c905f7f52ac`，最终平台head见第10节。master 核验为 `64b1c8f656394a55f5b8a1239754d03d2872efd5`（PR #61）；PR #62–67 为草稿，不能当作已合入。PR #66 最终 head `ecada2d23eb6857e1415d88662e8024a191df9a1`（功能检查点`58b5716`）。当前文档分支只新增设计及导航，不改变功能或 PR 的代码范围。

| 分类 | 当前事实 | 下一步影响 |
| --- | --- | --- |
| 已实现 | Record 稳定 ID/路由/版本、ABA 防护、操作去重、权威 WAL、持久化见证、统一可见发布、活动统计、checkpoint、共享 key/collection/grant 与输出屏障 | 保留业务契约，将全量内存状态和可重建临时索引逐步替换为分段布局，不建立第二套业务写入 API |
| 已实现 | 来源精确版本与证据、stale/删除隐藏、generation、多批次 staging/租约、可选向量、时间过滤/不可逆 TTL、至少7天 feed、显式实体/一跳、同视图融合及逐命中解释 | 这些状态都必须在新持久布局、索引构建、缓存和恢复中继续生效 |
| 已实现但有界 | 单 collection 一个持久 partition；局部增量发布；完整状态 checkpoint；进程内统一 guard；私有 staging 清理 | 不能称已具备不可变多 Segment 引擎、通用长期回收、分布式 ReadView 或远端索引尾部检索 |
| 已实现但受限 | 完整 service/catalog 与 collection 备份；同服务受控接管追平最终封存水位；无可信最新删除历史的新恢复目标隔离 | 不能用旧备份证明后续没有删除/撤权；云端故障接管要有最新权威状态与 fence，不复活旧授权 |
| 验证 | 最终 Linux 35 suites、226 passed、0 failed、10 ignored；默认及故障注入 HTTP/Chromium 通过 | 两个最终 SHA 原生 CI 结果见第10节；系统崩溃/断电仍未验收 |
| 未实现 | Segment/manifest 驱动的持续回收、对象存储提交、远端 Indexer/cache、跨节点 collection 路由、跨 partition 搜索 | 本文核心路线，不把目录/trait 壳视作交付 |

当前权威与依赖落点：`seekpower/src/record.rs` 的 State/Inner；`record/{mutation,publication,evidence,generation,upload,lease,temporal,change_feed,graph,fusion}.rs`；`storage/{journal,partition,platform}.rs`；`catalog.rs`；服务 `record_service.rs`。Record 语义决定进入通用持久提交，HTTP/UI 只做适配。现有磁盘检索内核与 Record 可重建索引不能混称为已实现统一 Segment manifest。

本轮功能收口为 PR #66/#67 已列范围。**[建议]** 后续发现非阻塞体验改进进入遗留清单；已确认写入丢失、失效内容泄漏、双活、错误成功、回收破坏恢复等问题仍阻塞相应阶段退出。没有已证明长期磁盘稳定性前，不宣称持续更新删除的正式生产验收完成。

## 2. 保持不变的契约

- **[确认]** 成功写入 durable 且 visible；持久后应用失败需 fence 并恢复，不能回报成功或伪装已知未提交。响应丢失/提交见证不明为 outcome_unknown；同 operation_id/原请求确认结果，不能换 ID 猜测重试。
- **[确认]** record_id 与 routing_key 绑定、版本条件和删除后 ABA 保护不退化；原始正文/向量/显式关系及版本状态是权威数据，倒排、ANN、邻接、缓存可重建。
- **[确认]** 一次查询固定完整版本视图、generation 和 evaluation_time；来源删除、stale、撤权遵循现有立即失效和输出屏障；TTL保持固定evaluation_time语义，响应期间单纯跨越期限不要求改换读视图，后续请求观察不可逆过期。旧视图不能成为绕过立即隐藏的理由，权限与数据生命周期分离。
- **[确认]** 至少7天 feed、幂等有效期与过期返回、备份/接管、租约/私有 staging 清理都进入回收约束；不是“索引已持久便可删 WAL”。资源不足先背压，不静默丢保留窗口事件。
- **[确认]** 同 collection/partition 来源与关系范围；没有跨 collection 事务或全局时刻快照。租户身份来自可信服务上下文，路由/缓存命中不授予访问资格。
- **[确认]** Linux、Windows 均为原生验收平台。无需升级迁移/旧格式兼容，但必须恢复、备份、校验和不匹配拒绝。保留 Web UI；不引入模型运行时、SQL/JOIN 或完整图语言。

## 3. 主线依赖和交付组合

**[建议]** 用四个可运行主线包代替大量串行边角 PR。包内可有本地提交/短审查分支，按共同契约集成，只有端到端结果算完成。段大小等默认参数先选有界可测配置，不启动性能专项。

| 包 | 对应原阶段 | 最小交付 | 依赖/退出条件 |
| --- | --- | --- | --- |
| A 本地不可变布局与安全回收 | P3 | 实际读写走 Segment+manifest+tail；flush、compaction、恢复、备份与回收可运行 | 依赖冻结 P1/P2 契约；跨段对照、原子发布、故障恢复、长读/备份/feed pin 和重复更新删除资源验收通过 |
| B 云端权威提交与尾部检索 | P4 核心 | 选定一个后端，一次真实写入经权威发布后从全新无缓存节点检索；恢复/接管可运行 | 依赖 A 布局及第6节路线决定；认证能力、并发协议模型、远端未知结果、断网与丢节点故障通过 |
| C Indexer/cache 与 collection 服务化 | P4 后半+P5 | 独立可调度 Indexer、可丢弃缓存、collection owner/读节点路由、公平预算与接管 | 依赖 B 的权威视图及 fence；多 collection 授权隔离、旧 owner、缓存全删、索引落后、资源背压通过 |
| D 超大 collection | P6，后置 | 先稳定路由/静态分片，再管理 split/merge，最后自动调度 | 仅真实容量/恢复时间超出单 partition 预算时启动；不阻塞 A–C |

A 内可并行：① Segment 编解码/校验与 Reader；② manifest/tail 发布和恢复；③可见性/统计/候选 oracle；④ pin/备份/feed 回收验证。共享 manifest descriptor、view stamp、业务 envelope 先冻结，再并行，不允许各模块自建权威版本。

A 开发期间可并行做 B 的后端能力实验、两路线协议模型和故障工具，**不接生产权威路径**；B 选定协议后，缓存下载与 Indexer 构建可以并行，发布权由唯一 CommitAuthority 收口。C 的路由和资源调度可并行开发但必须等 fence 集成测试才启用。UI 保持旧入口可用，新增管理体验不阻塞后端正确性包。

## 4. 最少必要接口与分层

下表为内部契约草案，名称可在实现时调整；不提前泛化任意数据库/图/服务框架。

| 边界/建议代码落点 | 输入与输出契约 | 明确不承担 |
| --- | --- | --- |
| `record/*` 语义层 | Command + 当前权威版本 → 已校验决定；固定版本/幂等/来源/时间/generation 规则 | 不直接写对象存储、不过问缓存位置 |
| `storage/authority` | `read_head`、`commit(expected_head, owner_fence, prepared_refs)`、`resolve(operation_id)`、`publish_index` → head/已知冲突/未知；一个后端一份提交顺序 | 不将成功上传当业务提交；不做模型任务 |
| `storage/manifest` | 认证不可变 manifest、组件校验与覆盖范围，恢复 anchor、版本/删除/收据/feed 根 | 不能仅列索引文件遗漏权威正文 |
| `segment/{reader,writer}` | 不可变 descriptor → 有界候选/正文/原始向量/版本读取；writer 完整校验后产 prepared descriptor | 不独立分配业务版本或改变 generation |
| `record/view` | `acquire_view` → 受保护 head、manifest、tail、统计上下文、固定 evaluation_time；输出复核最新隐藏/授权 fence | 不承诺分布式任意历史事务 |
| `storage/pins`、`compaction` | 构建/读者/备份 pin、退休集合、回收证明；清理任务可重试 | 不用 TTL 延迟猜测对象仍无人使用 |
| `object_store`、`cache` | 不可变 key+hash+length 的读写校验/有界下载；缓存缺失可重建 | 不缓存授权决定作为永久凭据；不自行决定 committed_seq |
| `indexer` | 受保护且已提交范围 → 新段/manifest 提案；有界队列/取消/fence | 不提交应用写入；不覆盖期间新 tail |
| 服务 catalog/router | 可信 key→collection grant，owner epoch/位置，预算；HTTP/UI 调用统一核心 | 不另建账户平台，不以路由位置替代写权 |

不要一开始拆服务：A 仍核心库+现有服务，Indexer 先独立执行队列；C 才需要远端运行角色。可替换的是 I/O 与发布协议，已确认 Record 语义和错误类型保持一处定义。

## 5. A：本地 Segment、manifest 与回收

### 5.1 数据布局与权威关系

**[建议布局，未实现]** 每 partition 保留唯一当前 head；引用不可变 manifest，manifest 引用权威状态段、检索段和完整恢复 anchor。逻辑布局：

```text
partition/<id>/head                       # 可恢复原子选择记录
  manifests/<manifest-id>                # immutable，所有组件/长度/hash/范围
  segments/<segment-id>/{records,vectors,terms,postings,facets,relations,meta}
  state/<state-id>/{versions,source-history,generations,receipts,feed,barriers}
  log/<block-id>                           # immutable或本地认证WAL尾
  pins/<pin-id>                           # 后端可观察的保留保护
```

records/raw vectors/显式关系是恢复所需权威；postings/ANN/邻接可重建。即便物理合为文件也必须分清角色和覆盖范围。版本目录、删除/来源历史、generation 控制/未发布批次、operation 收据/摘要、时钟水位、feed scope/floor、owner/sealed 状态都不能仅存在索引或内存。

head 最少含 partition/lineage/owner_epoch、head_version、committed_seq、wal_tip、manifest_ref、indexed_seq、log_base/anchor。`indexed_seq <= committed_seq`；indexed_seq 代表所有必要索引组件及相应统计共同覆盖的连续业务决定范围，不是某个索引文件最大序号。只有完整 manifest+已认证日志链构成权威；孤立上传/临时文件从不自动恢复成提交。

本地发布延续 Linux/Windows 同步、目录替换及见证规则；文件句柄/mmap 关闭后才删除退休文件。manifest 不是把现有 checkpoint JSON 改名：必须实际支持多段读取和尾部覆盖，消除每次恢复必须全量重建所有检索内容的唯一途径。

### 5.2 写入、索引切换与读取

1. 校验授权/owner/operation/版本与容量，生成一个业务决定并持久化 WAL。持久化不明确则保持未知；明确持久后应用失败进入 fenced recovery。
2. 在当前 tail overlay 完整应用决定，所有正文/索引/向量/证据/统计共同可见，才返回成功。generation Publish 只切完整成员集，不按段或批次暴露。
3. Indexer 固定 head H 和连续范围 `(I,C]`，在 pin 下读取，构建不可变段和状态索引；保留原始精确版本及必要失效信息。
4. 验证文件校验和、范围无洞、版本与统计，原子发布新 manifest/I。发布时保留 H 之后新增的 tail/committed_seq；基础或 owner 不兼容则重新验证或放弃，不能覆盖新写入。
5. 查询固定 `(manifest,I,C,tail<=C,evaluation_time)`，tail 中更新/删除覆盖段旧版本，按逻辑 record_id/version 去重。旧无效高分候选不能占满后就声称完整 Top-K；有界补枚举，耗尽则显式不完整/错误。结果回表仍在同一视图，输出前再过当前隐藏/授权屏障。

评分使用同一活动集合的 N/df/长度等上下文，跨段不得直接合并各段局部 BM25 分数作为同一语料分数。安全剪枝上界必须适用于该活动上下文；无法证明时使用正确的保守枚举路径，而非不安全剪枝。ANN 保持明确预算和近似声明，精确对照用于验证版本/过滤，不将质量专项作为当前前置。

### 5.3 Compaction 与安全回收

Compaction 读取受 pin 保护的输入集合，输出等价段及新的版本/统计目录；不改变业务 seq、Record version、operation receipt 或重复生成 feed。新 manifest 一次发布，读者持有旧视图至结束/租约失效；旧文件进入 retired，不立即删除。

不能用一个 `min(indexed_seq)` 解决所有 GC：用**可达对象集合+按语义分开的保留证明**。保护当前 head/恢复 anchor、活跃读者、构建/staging、有效备份、feed 最低保留时刻与游标 floor、幂等有效 epoch、精确 source/evidence 引用及 ABA 防护。仅当新 checkpoint 已持久认证所需状态且旧 WAL 不再承担上述职责才截断；prev 链在认证 anchor 停止，避免永远依赖全部旧日志。

先保守回收确定无引用的旧索引/私有正文；A默认保留现有可读Source修订，不能仅因没有派生证据引用就回收：当前精确excerpt API允许在Source有效时读取保留的旧修订。未来回收这类历史须先定义保留与过期读取契约，不作为纯物理GC悄悄实施。不能因追求空间稳定清除有效来源历史。持久历史仍被证据/备份引用时，明确报告保留原因并背压，不能承诺任意工作负载下恒定磁盘。身份版本高水位/墓碑必须继续阻止 ABA，保留策略不能静默允许旧ID重置。

pin 登记与退休竞争采用“登记保护→重读验证 head/退休世代→使用”协议；本地进程内读者也要覆盖异常释放和 Windows 文件生命周期。多节点的 pin 设计可在 A 定义接口，但其远端持久性与租约认证在 B 实现。过期 pin 的任务必须取消且无法再发布。先保留再删除；磁盘满、同步失败、清理中断不能移除恢复必需对象。

## 6. B：云端权威存储路线（待决）

### 6.1 共同要求

目标是任何计算节点和本地缓存全部丢失后，仍可从选定权威后端恢复已确认写入。原始数据/日志/不可变段采用对象存储，**元数据提交见证是否也完全在对象存储中尚未决定**。每 partition 先一个活跃写队列，非无 leader 多写者。

共同 CommitAuthority 契约：全序 head 变更、owner fence、条件版本、operation 结果可恢复查询、manifest 与 tail 原子关联；不得维护两个均可宣布提交的 head。数据对象先上传并验证，再发布其可达性。上传成功未发布是孤儿；元数据成功但响应丢失是未知待确认，不是重写机会。

| 选择 | A：对象存储条件写 root | B：关系数据库元数据事务 |
| --- | --- | --- |
| 权威点 | 条件写替换单 partition root 是提交见证 | 数据库事务提交 partition head/owner/操作结果或其可达锚点是提交见证；对象存储保存不可变字节 |
| 必要假设 | 精确匹配版本条件写、线性化读写、不可变引用对象读后可得、校验和/错误语义经认证 | 选定数据库事务隔离和持久性/HA满足承诺；按 partition 竞争更新和fence在同一事务；对象存储字节先持久可读 |
| 优点（条件成立时） | 不新增元数据库运行依赖；备份/恢复权威集中 | owner/head/目录/pin可在清晰事务内关联；适合已有可信DB控制面 |
| 代价/风险 | head竞争、响应丢失、读者pin/GC协调更复杂；不能靠LIST判提交 | 新增DB可用性/灾备/运维依赖；DB与对象并非跨系统事务，需严格先对象后元数据与孤儿回收 |
| 故障恢复 | root链和checkpoint给唯一答案；不可达强读失败 | 恢复DB必须与对象可达集合及归档水位配套；旧DB备份可能复活旧owner/删除状态，禁止直接对外开放 |
| 禁止做法 | 假设任何“S3兼容”或OBS自动满足CAS | DB存一个root同时再以对象root独立决定提交，形成双权威 |

**[待决 D1]** 选择哪条元数据权威路线，以及首个后端/部署环境。**[建议]** 若没有现成受运维保障的数据库且目标强调对象存储自足，可先对 A 做小协议认证；若已有数据库且接受其成为恢复必要条件，B 有明确工程价值。不能在未得知环境时宣称一种必然更快、更便宜或更可靠。选择 B 是对“仅对象存储足以恢复”目标的调整，需用户明确接受。关系数据库若被选择只承担内部元数据权威，不增加用户SQL/JOIN查询面。此决定不阻塞 A 的本地 Segment 工作，但阻塞云端生产实现。

未认证 OBS 的条件更新/读后写/错误行为，本文不宣称其具备可用 CAS。首个后端需要以官方协议、真实并发实验及故障模型逐条确认：匹配/不匹配条件、版本token、写成功响应丢失、跨客户端读取、对象覆盖禁止、校验失败、超时/重试、读者租约。能力不足则禁用此候选，不能用“先HEAD再PUT”冒充原子条件写。

### 6.2 两路线的具体提交和未知结果

A：上传唯一不可变日志块（prev anchor、seq范围、operation摘要、payload refs），CAS `(expected_head_version, owner_epoch)` 更新 root；CAS冲突后查询同 operation 的持久结果，重新读取并校验业务条件，不能将旧预校验决定拼入新链。接管通过权威条件更新提升 owner_epoch，旧owner即使读到新root也不获得写资格。

B：上传并校验同样不可变日志块；事务锁定/条件更新 partition head，校验 owner_epoch 与 expected head，再原子记录新head与可追溯操作结果。事务是否提交未知时，用新连接查询可信提交状态；DB不可用不得返回成功或从本地缓存宣布失败。对象已上传但DB事务失败只产生待回收对象。不得先写DB指向尚未完成上传的对象。

两者都要给 Indexer 独立 `publish_index(expected_basis, owner_epoch, descriptor)`；更新 manifest/indexed_seq 时保留当前 committed_seq/wal_tip。物理对象不可变且带校验和；新owner通过权威状态接管，不复制旧授权或仅靠DNS/路由切换。备份必须包含可认证snapshot、archive_seq及对应元数据/幂等/feed/barrier状态；恢复保持新位置隔离直到可信授权及删除水位建立。

### 6.3 提交尾部必须可检索

查询先认证当前 head、登记pin并复核，固定 manifest 覆盖 I 与 committed C。使用持久索引 `<=I` 加日志尾 `(I,C]` 的可重建 overlay；overlay 包含小倒排/精确向量扫描/元数据及逻辑版本覆盖，所有通道都应用同一 tail，不能全文读新正文而向量只读旧段。

尾部路径必须在 B 首次交付接通，不等 C 的独立 Indexer。测试暂停索引后成功写入、新无缓存节点立即全文/向量/关系/过滤查询；generation/stale/delete/TTL和统计仍完整。tail缓存按 lineage/manifest/seq区间复用，只增量推进；限制尾部字节、操作数、块数及远端I/O/等待。不每查询从第一条日志串行重放，也不能凭并发口号消除无限链依赖。达到尾部预算前背压写入；固定C所需的权威版本、删除/失效、generation、授权及统计依据必须完整；缺少必要tail时失败关闭，不能仅标partial就返回旧段。partial只允许在已完整认证可见性之后减少候选召回，绝不放宽隐藏/权限或静默仅搜旧索引。

## 7. C：Indexer/cache 与多 collection 服务

Indexer 输入已提交 head/范围和 pin，输出完整可校验段；任务具有 owner/build epoch、资源预算和期限。取消或过期不得发布，重试按确定构建ID查已有对象，发布仍验证当前基础。队列有限，不为每collection先创建无限任务。后台索引和compaction与前台读写分别预算，但共享有界总I/O/内存。

缓存仅保存可验证不可变对象/块，key含可信命名空间、collection、lineage、object id/hash。第一版完整小段下载至本地mmap即可；按需块读取/预取后置到真实容量需求。磁盘缓存丢失必须可恢复，下载临时文件验证长度/hash后原子入缓存；淘汰不能删仍被reader映射的文件，Windows需真实验证。

Head、grant和查询结果不可按不可变对象缓存规则处理。先不跨请求缓存最终结果；若以后加入，必须含视图/授权与隐藏版本且最终复核。强一致读取不能因为对象存储或DB不可达退回旧缓存。多节点撤权/源删除不能只靠异步广播：写入发布和输出许可要与当前权威屏障有序；首版建议所有实际响应交付经owner统一门控；远端节点拿到一次许可后自由输出不能作为默认实现。若后续采用分布式短生命周期输出许可，必须验证时钟/续期/失联和发送阶段的可撤销边界，撤销等待已进入发送阶段许可结束并禁止新许可，失联节点停止输出。确切分布式许可协议必须模型验证；无法保证时保留输出经owner统一门控，而非放松即时撤权。

collection 路由先一个collection一个partition/写owner，多个集合分布到不同节点；读节点依受保护head和tail服务。路由只指位置，owner_epoch来自权威提交。故障接管提升fence后恢复已确认水位；旧owner恢复网络不得提交/发布/输出。共享key仍只授予指定collection，撤销不删除数据；不另建账户/计费平台。

集群必须按可信主体/collection限制并发、队列、候选、尾部、cache、下载和后台任务；热点/冷集合互不耗尽全部资源。监控只保留验收必要指标：committed-visible-indexed差、tail字节、段数、retired/pin阻塞、失败/unknown、恢复时间、资源用量与隔离拒绝。详细计费看板和自动调度可后置。

## 8. D 与明确遗留项

P6仅在真实单collection容量/恢复预算触发后做。稳定routing_key和ID目录先在A布局中保留；不使用变化partition_count的取模。先静态partition、显式各分片视图及有界扇出，再持久 PREPARE→BUILD→CATCH_UP→FENCE→PUBLISH→CLEANUP，最后自动调度。源/关系跨partition边界未定义前不能自动分裂并宣称完整一跳；共置文档不等于任意图可天然共置。节点扩展优先按collection，不把P6强行插入云端主线。

以下均为**[建议遗留]**，不删除已有能力，也不把建议写成用户逐项批准：

| 可遗留 | 用户能力影响 | 不可借此省略 |
| --- | --- | --- |
| 任意 Record 历史快照/通用审计 API | 暂不能按任意历史时刻查询所有对象；保留现有精确Source证据读取 | 恢复、必要来源历史、版本/ABA与收据 |
| 独立时间排名/近期偏好 | 时间继续作硬过滤，不增加排名通道 | 固定evaluation_time、TTL隐藏与统计 |
| 托管大文件Blob/解析器 | 文件由上层存管；URL文件不在现有备份内 | 内联原始正文/向量及已经接受的数据完整备份 |
| UI管理向导、漂亮监控看板 | 沿用现有编辑/搜索/检查器，部分运维由受控API完成 | WebUI保留、真实API一致性、鉴权 |
| 自动调度、预取、块级cache优化 | 首版显式预算/人工部署，冷读可能较慢 | 有界内存/I/O、cache可丢弃、超限明确失败 |
| 跨collection联邦检索/自动语义关系 | 用户分别查询授权collection，上层融合；没有自动抽取 | 现有共享授权和同collection一跳/融合 |
| P6动态分裂/跨分片关系 | 单collection仍受单partition容量上限 | 跨collection扩容、安全接管与路由fence |

## 9. 最低验收与失败矩阵

每包提交文档、代码和证据一起审阅；先核心正确性与必要资源试验，不恢复旧benchmark/调参专项，不承诺未经测量吞吐/时延。Linux/Windows分别记录OS、文件系统、命令、最终SHA和通过/失败/未运行。

| 交错/故障 | 必须观察到的结果 | 包 |
| --- | --- | --- |
| 同一逻辑数据单段/多段/tail/compacted对照 | 精确检索与活动统计/计数/排序一致；ANN声明预算；无旧版/失效泄漏 | A |
| 任意写入/flush/manifest切换点进程退出、同步错误/磁盘满 | 已确认决定可恢复；unknown可查；无半generation或错误成功 | A |
| 长读/构建/备份/feed/幂等保留与GC并发，Windows映射未释放 | 在用/恢复必需对象不被删除；期限结束不能继续发布；容量背压可解释 | A |
| 固定活跃语料持续更新删除、证据保留与备份pin | 可回收部分收敛，无引用部分不无限泄漏；被保留数据明确计量，不伪称恒定空间 | A |
| 上传完成未发布、条件冲突、元数据响应丢失、owner接管 | 唯一权威提交，孤儿不可见，旧owner无权，operation精确重试 | B |
| Indexer落后/暂停，全部计算节点缓存丢失 | committed tail仍可查，完整恢复已确认写入，不能只返回indexed前缀 | B/C |
| 新写与索引发布交错、stale/删除/TTL与读者缓存交错 | 无tail覆盖丢失、各通道同视图；输出不穿过隐藏/撤权屏障 | B/C |
| DB或对象后端不可达、pin续租失败、网络分区 | fail closed；不使用旧缓存/旧授权假装强一致 | B/C |
| 多collection共享key、撤权/慢查询/慢发送/旧owner恢复 | 不越权、不删共享数据、撤销顺序正确；限流不串租户 | C |
| 备份后删除/撤权，新位置恢复/接管各阶段失败 | 不复活授权/旧内容，不双活；缺可信最新历史继续隔离 | A–C |
| 隔离VM系统崩溃/断电，外部确认见证 | 成功写入/删除/撤权恢复后成立；按平台独立记录 | A–C验收，当前未运行 |

协议模型最少覆盖 writer/indexer/takeover/pin/GC 五类参与者、响应丢失、重试和有限对象集合；验证已确认持久性、单owner、view完整、在用不删等不变量。模型通过不替代真实后端/文件系统故障。CI的进程kill只证明相应进程故障，不证明电源丢失。

只有一个源码最终SHA满足该包完整必需矩阵才标包验收通过。未有断电隔离条件可继续开发并记录“实现完成、系统故障待验收”，不能移除门槛。必要隔离条件：可销毁Linux/Windows VM及测试卷、可控制硬断电/重启、外部独立记录确认结果、可自动重放oracle；不得重启共享或用户电脑。

## 10. 当前 CI 证据与执行边界

首次 PR #66 run [39601083564](https://github.com/TheLudlows/seekpower/actions/runs/39601083564)、PR #67 run [39601191368](https://github.com/TheLudlows/seekpower/actions/runs/39601191368) 均为 Linux 两job通过、Windows失败。原因是清理测试在持有 WAL 字节范围锁时另开句柄读文件，错误33；修复只调整测试观察顺序，生产锁未变，独立复核通过。最终 PR #66 head `ecada2d23eb6857e1415d88662e8024a191df9a1` / [run39602448147](https://github.com/TheLudlows/seekpower/actions/runs/39602448147)、PR #67 head `b0c16c0882b48cb21d565322a13ae7c7d60f9825` / [run39602500783](https://github.com/TheLudlows/seekpower/actions/runs/39602500783) 均已完成且三个job全部成功：build-and-smoke、Persistence primitives (ubuntu-latest)、Persistence primitives (windows-latest)。远端PR head/base、draft及各job已回读核验；没有用祖先CI替代最终head。系统崩溃/断电仍未验收。

最终b0c16c0本地全量补跑：35 suites、226 passed、0 failed、10 ignored。首次补跑因编译缓存占满磁盘失败，清理可重建incremental缓存后重跑成功，未删除源码/用户数据或放松测试。日志`/tmp/p2-ci67-fixed-all-retry.log`；最初失败日志保留为`/tmp/p2-ci67-fixed-all.log`。

本轮未开始 A/P3 功能编码。本设计只本地提交并交付下载，待审阅；不自动推送设计PR、不改issue、不合入。下一步是审阅A的布局/契约和D1云端元数据路线，A实现不必等待D1，但云端生产协议必须等待决定和认证。

## 11. 独立文档审查

已完成独立只读审查并修订三处：缺必要tail必须失败关闭，partial不豁免删除/授权依据；保留当前可读Source旧修订，不能按“无证据引用”静默物理回收；TTL维持固定evaluation_time，区分单纯时间流逝和已提交失效屏障。没有因此新增功能或产品确认，未开始P3编码。
