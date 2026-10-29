# 持久化、对象存储与恢复

本地 Segment 已由 PR #71 合入；SQLite/S3 协调、远端 checkpoint、保留与共同备份已由 [PR #72](https://github.com/TheLudlows/anyseek/pull/72) 合入；[PR #73](https://github.com/TheLudlows/anyseek/pull/73) 和 [PR #74](https://github.com/TheLudlows/anyseek/pull/74) 的维护路径、审计修复及无缓存验收也已合入。实现与剩余验收统一见 [STATUS](../STATUS.md)，配置见 [服务指南](../anyseek/src/server/README.md)。

## 本地 Segment 与共同视图

`record/anchor.rs` 认证不可变 anchor、Segment 和 payload 闭包。完整原始 Document（含输入向量）保存在规范 payload pool；记录、来源修订、generation 与操作回执通过引用共享。派生 Segment 不再存一份完整搜索正文，量化向量不能替代原始输入。全文使用锁定的 Tantivy 0.27.0 原生包，向量/元数据使用独立组件；旧低层 Index API 已移除，数据访问统一使用 RecordPartition。仅支持新库的 RecordCheckpointV14/manifest 3，不读取旧格式、迁移或双写。

`record/view.rs` 在同一 partition guard 下检索多个冻结段与可变 tail。更新/墓碑先排除旧版本，再截断 Top-K；各段共享活动 N/df/字段长度上下文，每个通道先形成全局排名再融合。查询、过滤、回表与统计固定同一视图，缺少必要 tail 时失败，不能只搜旧段。

Flush 复用未变冻结段、冻结当前 tail；compaction 重建完整等价闭包。写入前按整个业务操作的原生成员数进行准入，超过 tail 的 128 次更新额度时先完成认证轮换；generation 不会因轮换而拆成多个业务提交。先完成构建、校验及同步，再发布选择证书，最后安装内存共同视图。发布后应用失败保持 fenced，恢复只采用认证选择结果。构建拒绝不得改变当前选择；退休失败可以重试，不回滚已提交状态。

本地读者/构建由 partition guard 和包 pin 保护；固定 Searcher、原生 worker 与 mmap 使用的文件在最后一个持有者退出前不能删除。备份导出独立权威，不借用将被源端 GC 的可变路径。构建与回收有显式预算，当前仍保留完整内存 State 与保守准入，不承诺全进程 RSS 硬上限。

## 权威与数据归属

SQLite 是唯一可变协调权威，保存 head/owner epoch、提交和索引水位、不可变引用、有限期幂等协调、对象依赖、pin、退休状态和备份/恢复协调记录。正文、原始向量、索引和 WAL 内容均在对象存储中。本地日志、payload 和索引是可以重建的缓存。

SQLite 使用 WAL 与 `synchronous=FULL`。Record 对象后端增加稳定的本地 owner 文件锁；同一权威不能由两个 Record 实例同时持有。服务根目录的 `s3-namespace` 是持久协调身份，同 collection ID 在独立服务根/恢复目标中不会共享可写对象命名空间；它应与 catalog、SQLite 权威库一起保留，不能当作缓存删除。

普通写入先登记上传身份，再完成不可变 PUT、读回长度/字节/SHA-256 校验，最后由 SQLite 事务条件发布 head 与回执，再发布本地完整视图。对象缺失或校验失败不会回退为旧缓存成功。

## 1. 远端检查点与 Segment 发布

复用 `anchor::Reference` 的认证闭包及规范 payload pool，Segment 包含 Tantivy 原生文件与独立向量/元数据组件。远端 checkpoint descriptor 映射所选 anchor/Segment/payload 文件到有界对象块，保存原文件长度和 SHA-256，不再制作一套正文/向量镜像。对象块按不可变文件身份和内容摘要命名，未变化的所选文件可以复用。

在 partition 串行屏障下：

1. 构建并验证完整本地候选 anchor/index。
2. 上传并校验全部成员，再上传 descriptor，登记 descriptor → 每个成员块的依赖。
3. SQLite 以 expected head version、owner epoch 和精确 committed seq 条件发布 manifest、indexed seq、log base，并清空已覆盖的 live WAL tip。
4. 再发布本地恢复证书与共同检索视图。SQLite 发布后的本地失败保持 fenced，重开时采用远端选择结果。

同步维护在完整当前 cut 上推进检查点；C1 已支持后台 Indexer 在固定已提交 cut 构建、条件发布，并保留期间新增 tail，详见下文。冷恢复下载并验证完整所选文件，直接加载已有 Segment，然后重放检查点之后的 tail。即使本地 seq 相同，也必须采用权威选择的新 manifest，因为同序号维护可能改变 feed floor 等状态。

### 有界校验对象缓存（C2，PR #76 已合入）

`*.object-cache` 位于对应 SQLite 权威库旁，是完全可丢弃的派生缓存。容量为每库 256 MiB、4096 个块；按最早插入淘汰，不把命中频率写回磁盘。缓存身份绑定 endpoint/bucket/region、完整对象 key、SHA-256 和长度；不保存凭据。每次命中重新检查长度和 hash；损坏作为 miss 删除，无法访问或锁竞争时直接回源。远端流按认证长度有界累积，过长、缺失或校验失败不能入缓存。

私有临时文件完整写入并同步后同目录原子发布；跨进程非阻塞文件锁保护扫描、删除与发布；锁文件在缓存目录旁，清空缓存不能替换锁 inode。临时文件、孤儿块计入清理，删除失败不释放额度；启动后不要求缓存持久化成功。PUT 和不确定 PUT 的确认始终从远端读回，不能用缓存代替后端持久化证明。SQLite 仍独立验证当前 owner/head/水位，缓存命中不授予读取或输出权限。

缓存只返回自有字节，不供 mmap；materialize 后的 Segment 位于独立目录，由现有认证流程映射与保护。淘汰、损坏和清空缓存均不删除在用 Segment mmap。预取和直接映射缓存块暂不实现。C3-2 已提供分级准入与显式缓冲/I/O 预算；跨 collection 的缓存总容量及 C3-3 完整资源隔离验收仍待完成；该缓存上限不是全服务磁盘或 RSS 硬上限。

### 有界后台 cut 发布（PR #75 已合入）

`RecordPartition::start_index_committed` 在准入后返回 owned job，`finish` 等待结果；丢弃句柄不取消已接受工作。`index_committed` 是等待式包装。HTTP PATCH 在 catalog 授权锁内只启动任务，锁外等待，交付仍验证原授权许可，因此构建等待不阻断普通写入或撤权。

当前进程最多两个构建，每 partition 一个，无额外等待队列；超额返回 Busy。期限由调用者指定，最大五分钟，HTTP 为三十秒。快照与新增 tail 分别使用既有共享内存准入；捕获 tail 上限为 journal 预算与 64 MiB 的较小值，超限取消本轮构建，保留前台已确认操作，构建文件及并发 WAL 使用同一个磁盘 scope；这是保守估算，继续不承诺 RSS 硬上限。完整 State 仍驻内存。此批按固定 cut 全量构建，增量后台构建与自动调度后置。

SQLite 新增每 partition 一行 `index_builds`，只保存单调 build epoch、owner/cut/基础 manifest 与期限等发布绑定，完成后保留 epoch 防止 ABA，不复制 Record 或索引。登记与输入闭包 pin 同事务提交；输出对象在 PUT 前原子登记并 pin；后台构建期间新的共同 S3 备份返回 Busy，避免把未完成上传作为备份输入。对象 I/O 使用同一 SQLite 文件的独立连接，避免跨网络持有前台 authority mutex；不会产生另一套 head。pin 不因时间经过自行解除，过期只禁止发布，清理必须精确绑定 epoch。取得独占 owner 锁后，重开会取消未完成任务及其 pin。

候选在 partition 屏障外构建和认证；本地 owned worker 保护候选及输入文件，期间显式 GC 返回 Busy，同步维护不能替换基础。发布时在 guard 内重放有界新增 tail，对比完整逻辑状态，检查本地基础和期限；SQLite 再检查持久 build/owner epoch、基础 manifest/indexed seq 和期限。它只推进 manifest/indexed/log base 至固定 cut，保留后续 committed seq 和 WAL tip，不消费业务序号。

本地证书与完整新增 suffix 在私有 generation 中一起同步，然后发布目录，避免证书可见但后续已确认写入未落入新 generation。S3 在 SQLite 发布后本地失败继续 fenced，冷启动从远端 manifest 加完整 tail 恢复。构建失败、过期或 panic 不能把旧 cut 当成当前视图；panic 关闭输出，必须重开恢复。物理回收在候选 mmap 释放和任务结束后执行。

[PR #75](https://github.com/TheLudlows/anyseek/pull/75) 已合入为 `09d30ae4`，最终提交 `96306e95` 的 [CI 五项全部成功](https://github.com/TheLudlows/anyseek/actions/runs/39891546462)。合入后主干 CI 单独记录。

### collection 授权执行屏障（C3-1，PR #77 已合入）

锁顺序为全局快照屏障 → collection 执行屏障 → catalog 元数据。普通已接收工作持前两层共享 guard，只在捕获当前 collection entry 与 grant revision 时持元数据锁；Record future 在锁外执行。单 collection 授权变更与删除先取得该 collection 独占 guard，排在它之后的工作取得共享 guard 后重新查授权。不得在持 catalog 元数据锁时等待执行屏障。

整 key 撤销、备份退休使用全局独占屏障；完整服务快照与显式 catalog compaction 也先排空普通工作。已接收但未完成的工作仍归 owned task 所有，调用方取消不能释放它的屏障；共同快照包含排空前的完整已接收操作，不混入快照之后排队的写入。全局操作仍是有意的停顿点。

异常 guard 不依赖异步获取 catalog 锁即可同步关闭输出。共享的 sticky failure latch 在重开前不清除，其他 collection 即使正在持久发布或压缩，也不能重新开放旧授权输出；普通可证明未发布的 compaction 失败保留原有恢复语义。输出交付仍检查 grant revision 与 Record 数据屏障，已交付字节不可撤回。

C3-1 已合入。C3-2 已通过 PR #78 合入，提供前台/恢复/后台分级准入、共享/每集合显式缓冲与 I/O 额度；取消后的 owned task 和传输 Bytes 保留租约。S3 GET 的租约随返回缓冲，而非下载函数返回就结束；PUT 读后验证仍访问远端。能证明 SQLite 发布前的预算拒绝为 Busy，其他不确定错误继续 fenced；既有 reader/build/backup 保护与 GC 闭包不变。默认值与范围见[服务指南](../anyseek/src/server/README.md)。服务缓存总容量及完整 C3-3 隔离验收未完成。

## 2. 日志与 feed 保留

完整 checkpoint 包含版本、删除、来源/generation、原始数据引用、操作回执、feed 状态和 staging 恢复状态，因此 `log_base_seq` 可推进至该 checkpoint cut。新 WAL 链从新 base 开始；旧链的不可变 predecessor 边不修改，仍被备份或其他 pin 保护时不能删除。

FeedTrim 不再在 S3 模式跳过。它以**同序号完整 checkpoint**认证 feed floor 和时间水位，而非只向本地缓存追加事件。发布成功后更新内存 feed cache；冷恢复仍得到相同保留边界。七天保留契约、过期游标显式重建和资源背压继续生效，不额外消耗业务操作序号。

物理删除不只看 indexed seq：checkpoint 已保存的 feed/幂等/staging/来源状态、当前根、完整依赖图、读者/构建/备份 pin 必须共同满足。当前单机读者和构建受 partition guard 保护；多节点租约/外部 indexer 尚不在本批。

## 3. 上传、依赖与 GC

- PUT 前持久登记 key、长度和摘要，响应丢失/进程退出也有可回收的对象身份；上传登记本身不提交记录。
- 所有远端 checkpoint 成员和 WAL predecessor 均登记依赖，当前 bootstrap 也是 GC root。
- 可达性由当前 SQLite roots 与显式 pin 出发遍历不可变边；不使用 LIST 判定提交，不使用固定等待时间代替引用保护，不新增 JOIN。
- 退休为单调状态，退休 key 不能重新登记、pin 或发布。先认证退休，再 DELETE，再记录删除完成；NotFound 是幂等删除成功。
- 已验证完成上传的删除使用完成 tombstone，避免每轮 GC 重删所有历史对象。上传结果未知的库存使用单独 tombstone，后续重试 DELETE，处理迟到 PUT；即便对象迟到，也不能成为已提交数据。
- GC 在单 owner 的 partition guard 下运行，完整视图已发布后收集，删除失败保留待重试状态。显式 `collect_s3_garbage` 可重试回收。
- 备份元数据对象也在 PUT 前原子登记并 pin，避免中断后尚未形成 descriptor 的 metadata snapshot 被 GC。

对象库存与 tombstone 是协调元数据，不是 payload 副本。后续需量化长期元数据规模与冷恢复/对象请求成本；当前不承诺无界容量。

## 4. 共同备份与恢复

### 备份

在同一个受控 cut 捕获逻辑 SQLite 快照并 pin 完整对象闭包。`backup_exports` 暂存**协调元数据快照字节**，用于进程中断后用同 backup ID 恢复原 cut，不保存正文、向量或索引内容。这项重复是可恢复导出的必要工作状态，显式退休时清除。

metadata snapshot 和 descriptor 使用不可变对象；备份 ID 按 partition 命名空间派生。重试复用原快照时间、水位和闭包，不把后续写入冒充原备份。服务指针使用私有文件 + sync + rename，未完成指针可重试；指针退休与本地 PartitionJournal 退休分别处理，并在启动时重试 catalog 已认证的退休动作。

本地最多保留 32 个 collection 备份指针。中断留下的同 ID 目录已经占用一个槽位，达到上限时仍允许修复该指针；新 ID 在分配前拒绝。重试只清理该目录内本实现拥有的 `.marker-<32hex>` 普通临时文件，继续使用原远端备份 cut。

### 恢复

先校验 descriptor、SQLite 快照和完整对象闭包。中断后的相同快照导入可继续，不覆盖另一个快照或回退已推进的 head。

恢复到新 collection 时，不复制一份能独立 GC 的原对象引用权威。先用不服务、不运行 GC 的临时导入库重放原 cut，再把验证后的规范文件复制到**新对象命名空间**，发布目标独立 checkpoint。原库和恢复目标因此可以独立压缩、退休备份和回收；重复恢复不能回退恢复目标后续写入。

恢复保留 Record 版本、tombstone、原始向量和操作回执。独立快照不自动获得当前删除历史权威：保留 `independent_snapshot`，隔离受保护证据并更换 feed scope；不恢复原 collection 的 grants。

完整服务归档复用既有 catalog/本地不可变归档协议，新增对 S3 collection backup 的私有物化输入。该归档包含完整独立数据，不依赖源 bucket 继续保留那些 collection backup pin。恢复服务只启用当前 master，历史 key/grant 不复活；在 S3 模式首次启动时，已认证的独立归档被发布到新的服务 namespace。归档中的 collection backup 仍可再次恢复。

私有输入逐个物化为认证 journal 路径，随后由 catalog 逐个打开、预检和导出，每次释放其索引与预算租约；不同时持有所有 retained collection/备份的检索视图。私有工作目录在成功或失败后清理，现有共享内存与归档大小预算继续生效。

完整服务备份完成后，同一操作重试先核验 manifest 与完成见证，直接返回原回执，避免每次重新物化全部输入而再次超过 HTTP 期限。这是已完成操作的结果查询；恢复仍须重新验证完整归档闭包。未完成的归档继续既有发布/修复协议。

## 维护与独立恢复

checkpoint 与索引尾段封存仍通过 anchor 发布协议选择权威闭包；S3 模式由 SQLite 选择远端 manifest，旧 owner epoch 和失败后的输出 fencing 继续防止陈旧写者或不可信响应。

独立备份恢复校验源闭包，在目标独立 namespace 发布完整远端状态后才由 catalog 激活。恢复保留版本和回执、更新 feed scope，不恢复历史 grants；typed evidence 保持隔离，不能据旧备份证明后续没有删除。

完整服务归档保留 collection 与其备份的恢复输入；普通 restore 的持久意图、备份 pin、私有目录清理和安全对象 GC 仍然有效。

维护审计的发布点与失败边界如下。普通写入、TTL 和 staging 租约过期共用缓存追加入口；SQLite 已明确提交成功时，缓存追加失败返回 `durable_pending` 并保持输出封锁，重开恢复原回执。SQLite 提交结果未知仍为 `outcome_unknown`，不能据此另发新操作。

| 路径 | 持久发布与恢复依据 | 失败/重试边界 |
| --- | --- | --- |
| 普通写入、TTL、租约过期 | 不可变 WAL + SQLite head/回执 | 提交后缓存失败不回滚、不开放旧输出；无缓存重放 |
| flush、compaction、feed trim、私有 staging 清理、seal | 完整对象闭包 + SQLite 所选 manifest | 同序号维护也必须远端认证；失败保持 fenced |
| 冷启动 | 独占权威 owner 锁，认证 bootstrap/manifest/tail | 新建缓存前校验 bootstrap 预算、配置与初始序号；错误不发布缓存目录 |
| 备份与指针 | SQLite 中原 ID 的 snapshot/pin + 不可变 descriptor | 中断重试维持原 cut；指针第 32 槽仍可修复 |
| 恢复、服务归档 | 校验源闭包，目标独立 namespace；typed evidence 保持隔离 | catalog 激活前完成目标远端发布；归档包含 retained 源 |
| GC、备份退休 | SQLite 可达性、pin、单调退休/删除记录及 catalog 退休意图 | 删除失败留待重试；不回滚 head，不复活已退休身份 |

## 验证入口与边界

- `cargo test --workspace --features server --all-targets`：Record/catalog、Segment、SQLite、上传/依赖/GC 和现有检索回归。
- `scripts/record_service_smoke.py`：默认本地后端、HTTP/UI、授权与恢复；`--cold-restarts --section …` 将同一组语义用于 S3，每次重启删除 collection 与 `*.object-cache` 两类缓存。
- `scripts/s3_storage_smoke.py --faults`：真实服务进程连接可丢弃 HTTP 测试后端，覆盖远端发布前后、缓存追加失败、无缓存查询、feed floor、四个备份中断边界、恢复重试、独立 GC 和完整服务归档。
- `scripts/s3_acceptance.py`：统一执行上述存储矩阵、七组共享语义与不可变对象/共同备份合同；`--backend real` 只接受专用测试配置。命令及报告边界见[开发指南](development.md)。
- [PR #74 CI](https://github.com/TheLudlows/anyseek/actions/runs/39814329888) 对最终 head `eb9431a3` 的五个任务全部通过，包含 Linux/Windows 原生持久化和 S3 语义/合同；合入源码为 `afbe323d`，与测试 head 的 Git tree 完全相同。主干 `a36e71e0` 的 [CI 五项也已通过](https://github.com/TheLudlows/anyseek/actions/runs/39884779455)；开发分支另行验收。

HTTP fixture 不等于真实 S3 服务认证。本轮先交付合同工具与用例，实际 endpoint 尚未运行；最终修改的 PR 原生 Linux/Windows CI 已通过；主干 `a36e71e0` CI 已核对；开发分支 CI 与实际目标后端验收仍待完成。系统/文件系统崩溃与硬断电单独待验收。SQLite 为单主机持久卷权威，不提供跨主机 HA 或权威卷丢失零 RPO。
