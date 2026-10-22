# 持久化、对象存储与恢复

本地 Segment 已由 PR #71 合入；SQLite/S3 协调、远端 checkpoint、保留与共同备份已由 [PR #72](https://github.com/TheLudlows/anyseek/pull/72) 合入。实现与剩余验收统一见 [STATUS](../STATUS.md)，配置见 [服务指南](../anyseek/server/README.md)。

## 本地 Segment 与共同视图

`record/anchor.rs` 认证不可变 anchor、Segment 和 payload 闭包。完整原始 Document（含输入向量）保存在规范 payload pool；记录、来源修订、generation 与操作回执通过引用共享。派生 Segment 不再存一份完整搜索正文，量化向量不能替代原始输入。低层独立 Index API 仍可保存自己的投影。

`record/view.rs` 在同一 partition guard 下检索多个冻结段与可变 tail。更新/墓碑先排除旧版本，再截断 Top-K；各段共享活动 N/df/字段长度上下文，每个通道先形成全局排名再融合。查询、过滤、回表与统计固定同一视图，缺少必要 tail 时失败，不能只搜旧段。

Flush 复用未变冻结段、冻结当前 tail；compaction 重建完整等价闭包。先完成构建、校验及同步，再发布选择证书，最后安装内存共同视图。发布后应用失败保持 fenced，恢复只采用认证选择结果。构建拒绝不得改变当前选择；退休失败可以重试，不回滚已提交状态。

本地读者/构建由 partition guard 保护；仍被 mmap 使用的文件不能删除。备份导出独立权威，不借用将被源端 GC 的可变路径。构建与回收有显式预算，当前仍保留完整内存 State 与保守准入，不承诺全进程 RSS 硬上限。

## 权威与数据归属

SQLite 是唯一可变协调权威，保存 head/owner epoch、提交和索引水位、不可变引用、有限期幂等协调、对象依赖、pin、退休状态和备份/恢复协调记录。正文、原始向量、索引和 WAL 内容均在对象存储中。本地日志、payload 和索引是可以重建的缓存。

SQLite 使用 WAL 与 `synchronous=FULL`。Record 对象后端增加稳定的本地 owner 文件锁；同一权威不能由两个 Record 实例同时持有。服务根目录的 `s3-namespace` 是持久协调身份，同 collection ID 在独立服务根/恢复目标中不会共享可写对象命名空间；它应与 catalog、SQLite 权威库一起保留，不能当作缓存删除。

普通写入先登记上传身份，再完成不可变 PUT、读回长度/字节/SHA-256 校验，最后由 SQLite 事务条件发布 head 与回执，再发布本地完整视图。对象缺失或校验失败不会回退为旧缓存成功。

## 1. 远端检查点与 Segment 发布

复用 A 的 `anchor::Reference`、已有 Segment 编码和规范 payload pool。远端 checkpoint descriptor 映射所选 anchor/Segment/payload 文件到有界对象块，保存原文件长度和 SHA-256，不再制作一套正文/向量镜像。对象块按不可变文件身份和内容摘要命名，未变化的所选文件可以复用。

在 partition 串行屏障下：

1. 构建并验证完整本地候选 anchor/index。
2. 上传并校验全部成员，再上传 descriptor，登记 descriptor → 每个成员块的依赖。
3. SQLite 以 expected head version、owner epoch 和精确 committed seq 条件发布 manifest、indexed seq、log base，并清空已覆盖的 live WAL tip。
4. 再发布本地恢复证书与共同检索视图。SQLite 发布后的本地失败保持 fenced，重开时采用远端选择结果。

当前实现只在完整当前 cut 上推进检查点，不支持后台 indexer 与新写入并行发布的优化；这属于 C。冷恢复下载并验证完整所选文件，直接加载已有 Segment，然后重放检查点之后的 tail。即使本地 seq 相同，也必须采用权威选择的新 manifest，因为同序号维护可能改变 feed floor 等状态。

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

### 恢复

先校验 descriptor、SQLite 快照和完整对象闭包。中断后的相同快照导入可继续，不覆盖另一个快照或回退已推进的 head。

恢复到新 collection 时，不复制一份能独立 GC 的原对象引用权威。先用不服务、不运行 GC 的临时导入库重放原 cut，再把验证后的规范文件复制到**新对象命名空间**，发布目标独立 checkpoint。原库和恢复目标因此可以独立压缩、退休备份和回收；重复恢复不能回退恢复目标后续写入。

恢复保留 Record 版本、tombstone、原始向量和操作回执。独立快照不自动获得当前删除历史权威：保留 `independent_snapshot`，隔离受保护证据并更换 feed scope；不恢复原 collection 的 grants。

完整服务归档复用既有 catalog/本地不可变归档协议，新增对 S3 collection backup 的私有物化输入。该归档包含完整独立数据，不依赖源 bucket 继续保留那些 collection backup pin。恢复服务只启用当前 master，历史 key/grant 不复活；在 S3 模式首次启动时，已认证的独立归档被发布到新的服务 namespace。归档中的 collection backup 仍可再次恢复。

## 维护与受控接管

封存复用完整 anchor 发布协议，S3 模式必须先由 SQLite 选择包含 seal 的远端 manifest，不能只更新本地 checkpoint。封存关闭当前输出许可；发布失败保持 fenced，恢复读取唯一权威再以原意图重试，同序号维护不会消耗业务序号。

同服务接管通过独立临时导入库校验 retained S3 备份；相同 descriptor 的私有物化采用确定 feed scope，使准备与重试绑定相同摘要。备份只证明谱系，后继复制最终封存状态，包含备份之后的已确认写入、删除和回执。后继在独立 namespace 发布并认证完整远端状态后，catalog 才能激活；旧源永久封存，旧 grants 不复制。

接管中断恢复按 catalog 意图打开远端源，不能把缓存当本地权威。中断留下的私有校验目录可在获得服务根独占锁后清理。完整服务备份同样从远端打开已删除/封存的 retained collection，清空本地缓存不能导致丢失归档输入。

## 验证入口与边界

- `cargo test --workspace --features server --all-targets`：Record/catalog、Segment、SQLite、上传/依赖/GC 和现有检索回归。
- `scripts/record_service_smoke.py`：默认本地后端、HTTP/UI、授权、恢复与受控接管。
- `scripts/s3_storage_smoke.py --faults`：真实服务进程连接可丢弃 HTTP 测试后端，覆盖远端发布前后、无缓存查询、feed floor、四个备份中断边界、恢复重试、独立 GC 和完整服务归档。
- [CI #307](https://github.com/TheLudlows/anyseek/actions/runs/39784279098) 对 PR #72 最终 head 的 Linux、Windows 与构建/冒烟任务全部通过。这是该提交的证据，不替代后续修改验证。

HTTP fixture 不等于真实 S3 服务认证。剩余 B 工作为维护路径审计、更多查询/失效场景及实际 endpoint 合同测试；系统/文件系统崩溃与硬断电单独待验收。SQLite 为单主机持久卷权威，不提供跨主机 HA 或权威卷丢失零 RPO。
