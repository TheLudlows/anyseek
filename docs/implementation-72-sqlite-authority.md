# B：SQLite + S3 对象持久化实施记录

状态：**PR #72 为 Draft。用户本批指定的 1–4 项已实现并通过本地验证；不把本批交付等同于 B 全部验收或生产可靠性完成。** 本批基于 `f28e35414c7e90dcec6c2f9d4b578568ff575747`，A/P3 已在主干 `5cd76e3b`。

## 本批范围

1. 远端检查点、Segment 与 manifest 发布。
2. 远端日志保留边界与 feed 截断。
3. 对象依赖、上传登记与安全 GC。
4. SQLite/对象共同备份、恢复以及服务入口的中断重试。

独立 Indexer、块缓存优化、分布式路由、跨主机 HA 及超大 collection 不在本批。

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

## 验证

本批 Linux 本地验证结果如下；不得用旧 Run #305 代替新代码 CI，不得把 HTTP test double 当成真实云后端认证。

| 验证 | 结果 |
| --- | --- |
| workspace 全 targets 回归 | 34 suites，280 passed，0 failed，11 ignored；随后追加未完成上传库存恢复回归 |
| 最终核心 lib（含新增库存恢复测试） | 181 passed，0 failed，10 ignored；最终源代码验证 |
| 上传库存/迟到 PUT 专项 | 2 passed，0 failed |
| doctest | 44 passed，0 failed |
| 固定版本 fault-injection binary 的 S3 HTTP 矩阵 | 全部通过：检查点前/后退出、无缓存多通道恢复、feed floor、备份四个边界（含 PUT 前 inventory）、恢复导入中断、独立 GC、完整服务归档/恢复/旧 key 禁用及再次冷启动 |
| 默认本地后端原生 HTTP 回归 | `record_service_smoke.py` 通过，覆盖共享授权、版本、备份、受控接管、generation、TTL、feed、关系、定位等既有路径 |
| 文档/品牌/本地链接 | 25 Markdown 文件、105 executable surfaces 检查通过 |
| 新提交原生 Linux/Windows CI | 提交后独立跟踪，不提前标为通过 |

十个核心 ignored 为九个父测试调用的进程 worker 与一个真实 S3 endpoint 合同测试；workspace 另有异构 AES fixture。父测试调用的 worker 不等于从未执行，真实 endpoint 合同仍未执行。

```bash
cargo test --workspace --features server --all-targets
cargo test --features server --doc
cargo build --features server,fault-injection
python scripts/s3_storage_smoke.py --server target/debug/anyseek_server --faults
python scripts/record_service_smoke.py --server target/debug/anyseek_server
python scripts/check_docs_brand.py
```

Windows 使用 `target/debug/anyseek_server.exe`。CI 原生 Linux/Windows 矩阵已加入 S3 专项 HTTP 进程故障检查。

新增验证覆盖：

- checkpoint 之后仍有 tail，更新/删除/原始回执经无缓存恢复，全文/向量/混合结果及计数一致。
- checkpoint 在 SQLite 发布前/后中断，仅采用权威选择的完整根。
- feed floor 经缓存清空仍生效，旧游标明确过期，后续写入连续。
- bootstrap、transitive pin 与所选成员不会误回收；丢失成员失败关闭。
- 备份在捕获元数据、上传 snapshot、发布 descriptor 后进程退出，重试保留原 cut。
- 恢复导入后进程退出可重试；源备份退休、两侧 GC 和冷重启不破坏另一侧数据。
- 迟到 PUT 不可重新发布，可被重试 DELETE。
- 完整服务归档包含 retained S3 collection backup；独立服务 namespace、禁用历史 key 和再次冷重启。

## 保留的 B 验收缺口

- 本批之外的权威/维护路径全面审计，尤其受控接管/封存等路径，仍须逐项确认，不把四项收尾视为该审计完成。
- 真实目标 S3 endpoint/bucket 的条件创建、响应丢失、读后可见性、错误与删除合同测试尚未完成。
- 本批最终原生 Windows CI 结果须单独记录；Linux 本地证据不能替代。
- 系统/文件系统崩溃及硬断电仍未验收；进程退出测试不替代该门槛。
- SQLite 单主机、单持久卷协调；没有跨主机 HA 或权威卷丢失零 RPO 承诺。恢复依赖经过验证的共同备份。

PR #72 保持 Draft，#57 保持开放。
