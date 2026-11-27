# 持久化、对象存储与恢复

**简体中文** · [English](storage.en.md)

本文集中说明本地 WAL/Segment 与 SQLite/S3 的持久化、checkpoint、备份和回收协议。请求语义见[Record 契约](record-model.md)，配置见[服务指南](../anyseek/src/server/README.md)，实现与剩余验收见[路线](roadmap.md)。

本地和 S3 模式都使用 `metadata.sqlite` 中的 Catalog 7。S3 authority 使用 schema 3 和备份快照格式 2。打开已有根目录时校验格式及所需元数据，遇到不支持的格式即拒绝打开。

## 本地 Segment 与共同视图

每个向量/元数据组件记录包必须完整保存与 keys 一一对应的 revisions 和 fingerprint（SHA-256）。加载时统一检查数量及活动身份；缺失字段、空 fingerprint 或身份数量不一致均会导致加载失败。空组件也显式保存空身份数组。

`record/anchor.rs` 认证不可变 anchor、Segment 和 payload 闭包。完整原始 Document（含输入向量）保存在规范 payload pool；记录与操作回执通过引用共享。派生 Segment 不再存一份完整搜索正文，量化向量不能替代原始输入。全文使用锁定的 Tantivy 0.27.0 原生包，向量和元数据使用独立组件。数据访问统一使用 RecordPartition。当前持久化格式为 RecordCheckpointV19、完整和增量 state descriptor 10/7（包含分组状态时为 11/9）、私有向量和元数据 descriptor 3、anchor manifest 9、检索 package manifest 3。

`record/view.rs` 在 partition guard 下捕获冻结段与可变 tail 的派生拓扑。评分后，查询只固定入选候选的记录来源，在 guard 外定位封存行、读取正文并复制输出字段。交付前复核发布健康、组修订号、当前身份和 TTL。全文按选中的活动身份构建跨段 N/df/字段长度上下文，每个通道先形成全局排名再融合。异步消费者可处于不同进度，不能将权威 visible_seq 当作完整索引水位，也不能承诺查询、回表及返回统计来自同一不可变业务快照；最终重验可能缩短结果页。

认证 checkpoint 可复用冻结段；compaction 重建完整等价闭包。可变 tail 按字节和 128 次原生更新额度准入。全文可查增量在原生物化完成前参与查询，工作区在 WAL 前预留；其可见发布、物化交接和派生缓存封存/轮换都不推进或替换 WAL 权威，恢复依据完整已提交 suffix。尾段和 owner 保护详见[架构](architecture.md#wal-支撑的可查全文尾段)。权威 checkpoint 先完成构建、校验及同步，再发布选择证书并安装视图；发布后应用失败保持 fenced，退休失败不回滚已提交状态。

私有批量构建结束时，`finish_build` 复用有界小段合并，再由 Tantivy 按活跃文件引用执行 GC，之后封存并收集库存。1,024 文件上限保留；不得直接删除仍被旧读者、构建或备份引用的文件。预算不足或无法进一步合并时仍可能安全拒绝，不承诺任意规模都可构建。

本地读者和构建由 partition guard 与包 pin 保护。全文 Searcher 通过固定的文件句柄按范围读取；固定 Searcher、原生 worker、向量和元数据视图使用的文件在最后一个持有者退出前不能删除。备份导出独立权威，不借用将被源端 GC 的可变路径。构建与回收有显式预算，准入使用保守估算。

向量和元数据文件沿用组件格式和发布顺序。向量读取使用请求或构建持有的至多 64 KiB 对齐窗口，批次结束释放该窗口及额度。过滤先读取固定字段字节，重复访问同一块时，在该查询的组件读取器内复用至多 4 KiB 的读块。跨块字段直接读取。自定义排序先保存有界候选的字段值再比较，读失败会终止查询。读取和顺序写入经过文件 I/O 准入，缓冲按进程和请求资源约束计量。操作系统可缓存普通文件页。

### 磁盘记录目录组件

首期组件由一个已认证 checkpoint 描述选择集合代次、主键索引、记录段、正文对象和已提交切点。主键树使用 4 KiB 的普通文件页，按字节限制页缓存。长 ID 的后缀存于单独文件，从页引用按需校验。叶页认证 `(segment_id, doc_id)` 上的记录行，`doc_id` 是段内零基行号。行保存逻辑版本、时间、墓碑或候选重验指纹及正文定位。指纹与正文 SHA-256 分开。点查与分页按调用时刻判断 TTL，只有读取正文时才为选中的范围分配缓冲并校验正文。文件句柄由 checkpoint 的共享持有者保留。

近期增量按已提交序号连续装入，通过较新记录和墓碑覆盖封存行。增量整批校验和按字节准入，拒绝时保留先前切点。压实可以重排记录行并沿用原正文引用。目录和段随同一个根切换，旧读者持有原来的根。

新建完整 checkpoint 的当前记录元数据保存在磁盘目录。State 帧保存其余元数据和操作历史。两者引用同一个正文池，由同一 anchor 认证。本地库冷开带封存目录的 checkpoint 时，先认证文件，核算行元数据和回执引用，再重放选中切点之后的增量。封存正文按需解码。近期记录和请求沿用 WAL 与共享内存准入。GET 在锁外固定文件并读取，交付前复核发布序号和有效期。

已有完整 descriptor 6 和 8 及 S3 恢复保留原有解码契约。本地检索仍按地址预算在内存保留索引位置。组元数据、回执元数据和正文定位符与正文读取分别计量。冷开认证仍可能顺序读取整文件校验摘要。本阶段尚未建立全进程 RSS 或冷开 I/O 上限，见 [#224](https://github.com/TheLudlows/anyseek/issues/224)。

## 权威与数据归属

在 S3 模式下，Record 分区的可变提交协调权威是 SQLite，保存 head/owner epoch、提交和索引水位、不可变引用、有限期幂等协调、对象依赖、pin、退休状态和备份/恢复协调记录。正文、原始向量、索引和 WAL 内容均在对象存储中。分区的本地 Record 日志、payload 和索引是可以重建的缓存；Catalog 与 S3 authority 的 SQLite 元数据是持久权威，不属于缓存。

Catalog 使用服务根目录的 `metadata.sqlite`，仅建立四张目录表：
`catalog_meta`、`catalog_collections`、`catalog_backups`、`catalog_operations`。
集合状态、备份归属、退休状态和管理回执在短事务中提交；完整 Catalog 编码有 16 MiB
预算，不复制 Record 正文、向量或索引。没有租户注册表、Key 表或 Catalog 原生日志。
单 Catalog owner 文件锁防止两个内存目录视图同时服务；提交不确定时保持隔离，重开后确认。
完整服务归档通过原有屏障固定 Catalog 与各集合，恢复不携带服务凭据，并保留历史操作隔离。

S3 模式在同库增加十张 authority 协调表，保存分区 head、幂等提交、对象登记/依赖/pin、
索引构建、备份恢复及退休状态。Catalog 与 Record 各自使用短事务，不把跨对象网络操作
包进事务；集合生命周期仍依靠意图、执行屏障与恢复协议。SQLite 使用 WAL 与
`synchronous=FULL`，不在事务内执行对象上传。Record 对象后端按分区持有稳定的本地 owner 文件锁；同一分区不能由两个 Record 实例同时持有，不同分区可以共享该数据库。集合备份只导出该分区的当前对象闭包和协调记录；恢复绑定按分区隔离，GC 只退休和删除本分区前缀的对象，全库可达性仍保护其他分区及 pin。集合本地目录直接派生为 `collections/{tenant_id}/{collection_id}/{generation}`，不保存名称到生成目录的映射。对象前缀为 `anyseek/{tenant_id}/{collection_id}/`，下分 `wal/records/lexical/vector/manifests`；内部代次编码在对象 ID 中。相同名称重建使用新代次，旧任务、回执与 GC 继续绑定原代次；客户端不传代次。没有自动 namespace 或 Bucket 归属标记，不同独立服务必须使用不同 bucket。

普通写入先登记上传身份，再完成不可变 PUT、读回长度/字节/SHA-256 校验，最后由 SQLite 事务条件发布 head 与回执，再应用本地权威状态并同步或排队派生索引。`/ingest` 不等待两个索引消费者追平。对象缺失或校验失败不会回退为旧缓存成功。

## SQLite 使用边界与连接

SQLite 保存 Catalog 管理元数据及 S3 协调状态，默认构建保留 `rusqlite`/bundled SQLite。正文和检索不依赖 SQL。
对象身份、分区 head、构建令牌与发布结果定义在 `storage/authority.rs`，不绑定数据库驱动。

- 新协调库由初始化入口创建。已有库只校验 schema 3 及必要表/列；缺表或不兼容版本拒绝打开，
  不自动补表。首次启用 S3 时在已有 Catalog 库中建立 authority 表。
- 上传/后台构建使用严格的 `open_existing`，不会创建缺失的 metadata 文件或执行建表事务。
  已有 WAL 模式不重复设置，连接仍使用 `synchronous=FULL` 和原 busy timeout。
- 上传预留在同一事务中返回已观察的 durable 状态，避免再次查询相同对象；登记时一次读取摘要、
  长度和状态。备份控制对象的预留及 pin 只执行一次，原故障注入边界保留。
- 预留观察只决定是否复用既有不可变对象。IO 后仍在短事务内复核状态；并发退休不能被旧观察
  撤销。head CAS、回执、pin 和 GC 的提交与保护规则保留，不跨网络持有数据库事务。

Catalog 保留必要的管理元数据事务，S3 路径减少重复 SQL、建表写锁和类型耦合；不据此推定生产吞吐或 P99 改善。

## 远端检查点与 Segment 发布

复用 `anchor::Reference` 的认证闭包及规范 payload pool，Segment 包含 Tantivy 原生文件与独立向量/元数据组件。远端 checkpoint descriptor 映射所选 anchor/Segment/payload 文件到有界对象块，保存原文件长度和 SHA-256，不再制作一套正文/向量镜像。对象块按不可变文件身份和内容摘要命名，未变化的所选文件可以复用。

在 partition 串行屏障下：

1. 构建并验证完整本地候选 anchor/index。
2. 上传并校验全部成员，再上传 descriptor，登记 descriptor → 每个成员块的依赖。
3. SQLite 以 expected head version、owner epoch 和精确 committed seq 条件发布 manifest、indexed seq、log base，并清空已覆盖的 live WAL tip。
4. 再发布本地恢复证书与共同检索视图。SQLite 发布后的本地失败保持 fenced，重开时采用远端选择结果。

同步维护在完整当前 cut 上推进检查点；后台 Indexer 在固定已提交 cut 构建、条件发布，并保留期间新增 tail，详见下文。冷恢复下载并验证完整所选文件，直接加载已有 Segment，然后重放检查点之后的 tail。即使本地 seq 相同，也必须采用权威选择的新 manifest，因为同序号维护可能改变历史 GC 切点等状态。

### 有界校验对象缓存

`*.object-cache` 位于对应 SQLite 权威库旁，是完全可丢弃的派生缓存。容量为每库 256 MiB、4096 个块；按最早插入淘汰，不把命中频率写回磁盘。缓存身份绑定 endpoint/bucket/region、完整对象 key、SHA-256 和长度；不保存凭据。每次命中重新检查长度和 hash；损坏作为 miss 删除，无法访问或锁竞争时直接回源。远端流按认证长度有界累积，过长、缺失或校验失败不能入缓存。

私有临时文件完整写入并同步后同目录原子发布；跨进程非阻塞文件锁保护扫描、删除与发布；锁文件在缓存目录旁，清空缓存不能替换锁 inode。临时文件、孤儿块计入清理，删除失败不释放额度；启动后不要求缓存持久化成功。PUT 和不确定 PUT 的确认始终从远端读回，不能用缓存代替后端持久化证明。SQLite 仍独立验证当前 owner/head/水位，缓存命中不授予读取或输出权限。

缓存只返回自有字节，不供 mmap；materialize 后的 Segment 位于独立目录，由现有认证流程校验和保护。淘汰、损坏和清空缓存均不删除视图固定的 Segment 文件。预取和直接映射缓存块暂不实现。服务已有分级准入与显式缓冲/I/O 预算，跨 collection 缓存总容量和完整资源隔离验收仍待完成；该缓存上限不是全服务磁盘或 RSS 硬上限。

### 有界后台 cut 发布

`RecordPartition::start_index_committed` 在准入后返回 owned job，`finish` 等待结果；丢弃句柄不取消已接受工作。`index_committed` 是等待式包装。HTTP PATCH 在集合生命周期屏障内启动任务，屏障外等待；已接管任务保留原命名空间，不重新查询上游用户权限，失败 fence 的输出保护仍保留。

当前进程最多两个构建，每 partition 一个；显式构建准入超额返回 Busy，自动维护有下文所述的有界等待。期限由调用者指定，最大五分钟，HTTP 为三十秒。快照与新增 tail 分别使用既有共享内存准入；捕获 tail 上限为 journal 预算与 64 MiB 的较小值，超限取消本轮构建，保留前台已确认操作，构建文件及并发 WAL 使用同一个磁盘 scope；这是保守估算，继续不承诺 RSS 硬上限。本地构建固定封存目录并保留有界尾部。完整 checkpoint 按固定 cut 构建，另有自动时间回收、日志压力维护和派生缓存合并，不等同于通用增量 checkpoint。

SQLite 新增每 partition 一行 `index_builds`，只保存单调 build epoch、owner/cut/基础 manifest 与期限等发布绑定，完成后保留 epoch 防止 ABA，不复制 Record 或索引。登记与输入闭包 pin 同事务提交；输出对象在 PUT 前原子登记并 pin；后台构建期间新的共同 S3 备份返回 Busy，避免把未完成上传作为备份输入。对象 I/O 使用同一 SQLite 文件的独立连接，避免跨网络持有前台 authority mutex；不会产生另一套 head。pin 不因时间经过自行解除，过期只禁止发布，清理必须精确绑定 epoch。取得独占 owner 锁后，重开会取消未完成任务及其 pin。

候选在 partition 屏障外构建和认证；本地 owned worker 保护候选及输入文件，期间显式 GC 返回 Busy，同步维护不能替换基础。发布时在 guard 内重放有界新增 tail，对比完整逻辑状态，检查本地基础和期限；SQLite 再检查持久 build/owner epoch、基础 manifest/indexed seq 和期限。它只推进 manifest/indexed/log base 至固定 cut，保留后续 committed seq 和 WAL tip，不消费业务序号。

本地证书与完整新增 suffix 在私有 generation 中一起同步，然后发布目录，避免证书可见但后续已确认写入未落入新 generation。S3 在 SQLite 发布后本地失败继续 fenced，冷启动从远端 manifest 加完整 tail 恢复。构建失败、过期或 panic 不能把旧 cut 当成当前视图；panic 关闭输出，必须重开恢复。物理回收在候选文件句柄释放和任务结束后执行。

### collection 生命周期与快照执行屏障

锁顺序为全局快照屏障 → Tenant/Collection 执行屏障 → Catalog 元数据。普通工作在捕获租户所属集合后释放元数据锁，数据执行在锁外完成。集合创建、激活、删除按租户限定的集合键串行；完整服务快照和备份退休仍通过必要屏障协调。不得在持元数据锁时等待执行屏障。

用户权限由上游网关判定，AnySeek 不维护租户凭据、Grant revision 或 Catalog 输出授权许可。已接收的工作仍归 owned task 所有，调用方取消不提前释放屏障。异常工作只关闭后续 Catalog 操作，不建立新的角色/权限系统；Record 数据保护保持独立。

前台、恢复和后台采用分级准入、共享/每集合显式缓冲与 I/O 额度；取消后的 owned task 和传输 Bytes 保留租约。S3 GET 的租约随返回缓冲，而非下载函数返回就结束；PUT 读后验证仍访问远端。能证明 SQLite 发布前的预算拒绝为 Busy，其他不确定错误继续 fenced；既有 reader/build/backup 保护与 GC 闭包不变。默认值与范围见[服务指南](../anyseek/src/server/README.md)，组合隔离验收见[路线](roadmap.md)。

## 日志与回执历史保留

完整 checkpoint 包含当前记录的正文/向量/普通元数据、技术版本与墓碑、TTL/时间水位、近期操作回执、认证 GC 切点及独立恢复 scope；不包含业务来源历史。因此 `log_base_seq` 可推进至该 checkpoint cut。新 WAL 链从新 base 开始；旧链的不可变 predecessor 边不修改，仍被备份或其他 pin 保护时不能删除。

内部历史裁剪通过**同序号完整 checkpoint**认证 GC 切点和时间水位；冷恢复仍得到相同边界，不额外消耗业务序号。回执 GC 同时检查历史保留期（默认/最低两日，可显式更长）和当前/前一 UTC 日重试窗口；两日是最早资格，不是硬性清理期限。SQLite publication receipts 用于精确 tail/索引切点与恢复，不能等同这个 API 窗口直接删除。当前格式使用 `RecordCheckpointV19`、`RecordAppliedV16`、`RecordAnchorV5`、`history` 和 `RecordHistoryTrimV14`，移除旧通知配额占位字段与旧裁剪算法；旧库、WAL 和备份明确拒绝，不做迁移。外部 bootstrap/changes 同步接口已移除。

物理删除不只看 indexed seq：checkpoint 已认证的提交基准、幂等及回执裁剪状态、当前存储根、对象引用闭包、读者/构建/备份 pin 必须共同满足。当前单机读者和构建受 partition guard 保护；多节点租约/外部 indexer 尚不在本批。

## 上传、存储引用与 GC

- PUT 前持久登记 key、长度和摘要，响应丢失/进程退出也有可回收的对象身份；上传登记本身不提交记录。
- 所有远端 checkpoint 成员和 WAL predecessor 均登记依赖，当前 bootstrap 也是 GC root。
- 可达性由当前 SQLite roots 与显式 pin 出发遍历不可变边；不使用 LIST 判定提交，不使用固定等待时间代替引用保护，不新增 JOIN。
- 退休为单调状态，退休 key 不能重新登记、pin 或发布。先认证退休，再 DELETE，再记录删除完成；NotFound 是幂等删除成功。
- 已验证完成上传的删除使用完成 tombstone，避免每轮 GC 重删所有历史对象。上传结果未知的库存使用单独 tombstone，后续重试 DELETE，处理迟到 PUT；即便对象迟到，也不能成为已提交数据。
- GC 在单 owner 的 partition guard 下运行，完整视图已发布后收集，删除失败保留待重试状态。显式 `collect_s3_garbage` 可重试回收。
- 备份元数据对象也在 PUT 前原子登记并 pin，避免中断后尚未形成 descriptor 的 metadata snapshot 被 GC。

对象库存与 tombstone 是协调元数据，不是 payload 副本。后续需量化长期元数据规模与冷恢复/对象请求成本；当前不承诺无界容量。

## 共同备份与恢复

### 备份

在同一个受控 cut 捕获逻辑 SQLite 快照并 pin 完整对象闭包。`backup_exports` 暂存**协调元数据快照字节**，用于进程中断后用同 backup ID 恢复原 cut，不保存正文、向量或索引内容。这项重复是可恢复导出的必要工作状态，显式退休时清除。

metadata snapshot 和 descriptor 使用不可变对象；备份 ID 按 Tenant、集合代次和 Operation ID 派生。重试复用原快照时间、水位和闭包，不把后续写入冒充原备份。服务指针使用私有文件 + sync + rename，未完成指针可重试；指针退休与本地 PartitionJournal 退休分别处理，并在启动时重试 catalog 已认证的退休动作。

本地最多保留 32 个 collection 备份指针。中断留下的同 ID 目录已经占用一个槽位，达到上限时仍允许修复该指针；新 ID 在分配前拒绝。重试只清理该目录内本实现拥有的 `.marker-<32hex>` 普通临时文件，继续使用原远端备份 cut。

### 恢复

先校验 descriptor、SQLite 快照和完整对象闭包。中断后的相同快照导入可继续，不覆盖另一个快照或回退已推进的 head。

恢复到新 collection 时，不复制一份能独立 GC 的原对象引用权威。先用不服务、不运行 GC 的临时导入库重放原 cut，再把验证后的规范文件复制到**目标集合新代次的对象 ID**，发布目标独立 checkpoint。原库和恢复目标因此可以独立压缩、退休备份和回收；重复恢复不能回退恢复目标后续写入。

恢复保留 Record 版本、tombstone、原始向量和操作回执。独立快照不自动获得当前删除历史权威：保留 `independent_snapshot`，并更换历史恢复 scope；不包含服务凭据。

完整服务归档复用既有 catalog/本地不可变归档协议，新增对 S3 collection backup 的私有物化输入。该归档包含完整独立数据，不依赖源 bucket 继续保留那些 collection backup pin。服务凭据不进入归档，由目标服务进程配置提供；在 S3 模式首次启动时，已认证的独立归档被发布到目标独立 bucket。归档中的 collection backup 仍可再次恢复。

私有输入逐个物化为认证 journal 路径，随后由 catalog 逐个打开、预检和导出，每次释放其索引与预算租约；不同时持有所有 retained collection/备份的检索视图。私有工作目录在成功或失败后清理，现有共享内存与归档大小预算继续生效。

完整服务备份完成后，同一操作重试先核验 manifest 与完成见证，直接返回原回执，避免每次重新物化全部输入而再次超过 HTTP 期限。这是已完成操作的结果查询；恢复仍须重新验证完整归档闭包。未完成的归档继续既有发布/修复协议。

### 本地 WAL 窗口与指定提交切点

普通 HTTP 恢复按 `backup_id` 恢复备份自身的切点。本地离线工具在保存该备份后，从停机的原分区捕获当前保留的已提交 WAL 后缀。备份必须对应当前选中的日志起始切点。工具核对原分区身份、备份内容、组决定和连续业务序号，并返回绑定整个窗口的 SHA-256 见证。把见证同备份分开保存。若 checkpoint 已回收此前的日志代次，则需使用此前已捕获的窗口。

```sh
cargo build --locked -p anyseek --no-default-features --example local_archive_cut
target/debug/examples/local_archive_cut capture SOURCE BACKUP ARCHIVE BUDGET_BYTES > witness.json
target/debug/examples/local_archive_cut restore BACKUP ARCHIVE NEW_ROOT BUDGET_BYTES TARGET_SEQ witness.json
```

恢复先复核整段窗口和基础备份，再重放到指定业务决定（含该决定）并原子发布新路径。组写的成员和原回执属于同一个提交序号；目标决定后的变更及同序号维护不纳入输出。原始路径、服务目录和 Origence SQLite 配套备份由调用方管理。多代恢复使用 `restore-chain BACKUP NEW_ROOT BUDGET_BYTES TARGET_SEQ parts.json`。JSON 列表按顺序记录最多 64 段，每项含 `path` 和 `witness`。工具核验所有相邻边界和清单中的全部段，包括目标切点之后的段。每个代次需在其活动日志回收前捕获，见 [#224](https://github.com/TheLudlows/anyseek/issues/224)。

## 维护发布与失败边界

S3 模式由 SQLite 选择远端 manifest，owner epoch 与失败 fence 阻止陈旧写者和不可信输出。普通写入和 TTL 共用缓存追加入口；SQLite 已明确提交成功时，缓存追加失败返回 `durable_pending` 并保持输出封锁，重开恢复原回执。SQLite 提交结果未知仍为 `outcome_unknown`，不能据此另发新操作。

| 路径 | 持久发布与恢复依据 | 失败/重试边界 |
| --- | --- | --- |
| 普通写入、TTL | 不可变 WAL + SQLite head/回执 | 提交后缓存失败不回滚、不开放旧输出；无缓存重放 |
| flush、compaction、历史裁剪、seal | 完整对象闭包 + SQLite 所选 manifest | 同序号维护也必须远端认证；失败保持 fenced |
| 冷启动 | 独占权威 owner 锁，认证 bootstrap/manifest/tail | 新建缓存前校验 bootstrap 预算、配置与初始序号；错误不发布缓存目录 |
| 备份与指针 | SQLite 中原 ID 的 snapshot/pin + 不可变 descriptor | 中断重试维持原 cut；指针第 32 槽仍可修复 |
| 恢复、服务归档 | 校验源闭包，目标集合新代次（完整服务恢复使用独立 bucket） | catalog 激活前完成目标远端发布；归档包含 retained 源 |
| GC、备份退休 | SQLite 可达性、pin、单调退休/删除记录及 catalog 退休意图 | 删除失败留待重试；不回滚 head，不复活已退休身份 |

## 验证入口与边界

- `cargo test --workspace --all-targets`：Record/catalog、Segment、SQLite、上传/依赖/GC 和现有检索回归。
- `scripts/record_service_smoke.py`：默认本地后端、HTTP、授权与恢复；独立 UI 浏览器流程见[控制台指南](../console/README.md#验证)；`--cold-restarts --section …` 将同一组语义用于 S3，每次重启删除 collection 与 `*.object-cache` 两类缓存。
- `scripts/s3_storage_smoke.py --faults`：真实服务进程连接可丢弃 HTTP 测试后端，覆盖远端发布前后、缓存追加失败、无缓存查询、历史 GC 切点、四个备份中断边界、恢复重试、独立 GC 和完整服务归档。
- `scripts/s3_acceptance.py`：统一执行上述存储矩阵、六组共享语义（启用 --faults 时）与不可变对象/共同备份合同；`--backend real` 只接受专用测试配置。命令及报告边界见[开发指南](development.md)。
- 性能证据入口和未完成验收集中在[路线](roadmap.md)；完整矩阵的触发及覆盖范围见[开发指南](development.md#完整验收)。旧阶段 CI 不替代当前源码与依赖变更的验证。

HTTP fixture 不等于真实 S3 服务认证，实际 endpoint、系统/文件系统崩溃与硬断电单独待验收。SQLite 为单主机持久卷权威，不提供跨主机 HA 或权威卷丢失零 RPO。

Catalog 管理操作在写事务之前仅做一次完整预算/可恢复性校验。完整服务归档格式为 2；恢复只保留源 Catalog cut/hash、来源身份及必要旧 Operation ID 隔离，不复制完整历史 Catalog，数据库和归档均不包含 Key 验证材料。HTTP 中间件在读取 body 前认证一次，已认证上下文供 handler 和 owned 工作复用；集合实际执行仍在生命周期屏障内检查代次。

整服务备份重开保留分区时，封存行校验使用文件事务额度。该额度在原生索引工作之外取得和释放。发布前的资源拒绝返回 `busy`，存储故障与发布结果未知继续使用各自的恢复语义。

## 已删除集合代次的最终回收

删除请求持久提交逻辑删除后返回 `physical_reclamation: pending`。服务只有一个有界回收循环：每秒选择一个旧代次，最多删除 64 个本地文件项或 16 个远端对象；另每 30 秒重试最多 16 个单调对象墓碑。受资源准入拒绝、owner/固定读者或备份保护时等待后续重试；进度与失败写入服务日志。

Catalog 先持久记录 reclaiming 意图，新完整服务备份排除该代次；现有独立备份不修改。仍有有效集合备份关联时不开始回收。代次回收使用内部代次互斥，不占用同名新集合的业务锁；完整服务备份屏障保留到本轮结束。请求、后台任务和固定 package 读者释放后，将本地根原子移入稳定 trash；中断后从 trash 继续，不重新打开已删除的 Record 日志。

S3 先撤销旧 owner 的构建状态并确认 pins 已释放，再退役该代次的 head，删除业务回执/构建/恢复元数据及已清理对象的依赖边。待删对象由 SQLite 按 key 游标分页；每次 DELETE 前及登记完成前，校验活动 head 并沿目标对象的反向依赖检查 head/pin 可达性，不再反复加载完整待删列表。每轮最多清理 16 条依赖边。旧 partition 身份保留最小墓碑，禁止旧发布或同身份重建。未知 PUT 的墓碑在 Catalog 代次记录删除后仍由有界服务 sweep 重试，防止迟到对象永久遗留；这些协调墓碑不属于可以直接清空的历史残留。批次限制文件/对象/依赖边处理数和返回的候选数量，校验成本仍随活动 head 和相关依赖图增长，不能据此承诺整进程 CPU 或 RSS 硬上限。

物理清理完成后删除 Catalog 的旧代次行，保留期限内管理回执和持久 epoch 拒绝规则。旧任务、旧 GC 和旧删除回执不能修改同名新代次。普通客户端仅携带名称，仍须在删除/重建时停止旧请求与重试。真实云 S3、系统掉电和 Windows 验收分别记录，不由本地 fixture 推定通过。

### 自动维护额度等待

进程仍最多同时执行两个维护 worker。自动维护首次被 worker 或集合准入额度拒绝后，最多保存 64 个按集合实例去重的待办；使用现有 worker semaphore 的 FIFO 等待与释放唤醒，不需要新的客户端请求。集合准入或同集合已有构建暂不可用时，释放 worker 并退避 100ms。每个待办最多等待 300 秒，等待时仅持弱引用，不持有 owner、reader/build pin，也不会按路径重新打开已删除或换代的集合。已接受的构建继续遵守原有 owned-task 和发布保护语义。

`record_maintenance` 日志区分 `admission_deferred`、`admission_backoff`、`retry_started` 和 `worker_finished`；重试日志包含待办年龄 `age_ms` 与尝试次数 `attempts`，完成日志区分安装成功、执行 Busy、恢复要求及其他失败。上限已满或等待超时分别记录 `retry_capacity_exhausted`、`retry_stopped`，保留维护意图并停止自动等待。已启动的执行失败不自动循环重试。

运维看到上述停止或失败事件时，应先确认额度恢复，排查存储/恢复错误；需要恢复时先走现有恢复流程，然后对该集合重试授权的 `PATCH /api/v1/index/{id}` checkpoint（库调用为 `index_committed`）。成功的显式 checkpoint 会再次触发尚未完成的自动维护。容量超过 64、作业长期不完成或持续拒绝的负载不承诺自动公平或完成时限；FIFO 保证仅适用于已经接纳且尚未超时的 worker 等待。
