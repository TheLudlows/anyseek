# 检索引擎职责收敛

AnySeek 的边界是可靠、可更新的全文与向量检索。原文、向量、普通元数据、稳定 Record ID、Absent/Any/Match、有限窗口幂等与查账、异步索引进度、WAL/恢复/检查点/备份、安全回收与资源预算保留。中文全文、ANN、全文向量 RRF、过滤、Top-K、分页和评分保持。

来源版本、切块、证据依赖、派生重算、业务关系及记忆策略由应用负责。document_id/chunk_index/章节/source_id/source_version 等仍可作为普通 document 字段保存。旧版正文摘录和字节/页定位不属于引擎 API。

## 实施状态

职责收敛已通过 [#187](https://github.com/TheLudlows/anyseek/pull/187) 合入。第一阶段撤下顶层 system 写入、evidence/entities/neighbors 路由、relation_fusion 查询及其 OpenAPI/控制台入口；第二阶段移除强类型来源模型、连续来源历史、定位符、证据依赖和专用关系融合，保留普通元数据、技术版本及全文向量 RRF。

以下是 #187 开发期间的历史验证，不是当前主干重测：本地分批补跑累计覆盖 459 项非忽略用例，均有通过结果，但不是一次全组无失败运行。曾修复 checkpoint 借用编码 V18/V19 标签不一致及旧测试依赖读取触发 TTL 的假设，另有驱动工作目录错误和超时后补跑，失败日志保留。HTTP 基础/TTL/回执、本地 S3 fixture 六组语义验证及浏览器九项通过，本地 S3 备份/恢复故障 fixture 通过；正常 checkpoint 对明确 Busy 在原 60 秒预算内重试，其他持久错误不吞掉。本地未运行 Windows 或真实云 S3；#187 最终修复 head 的 Linux/Windows 默认 CI 结果及覆盖范围见该 PR，完整手工矩阵不计为通过。

TTL 实现改为读取时排除过期正文，后台通过原有 WAL/远端提交协议回收，允许 ID 复用。服务每秒最多调度一个已加载 partition，复用有界后台 worker；繁忙、预算或 I/O 会推迟回收，没有固定物理删除时限。Rust 嵌入调用方可周期调用 maintenance_tick。读操作不再持久推进过期或 history trim。

同一进程的 evaluation_time 不回退，并受已持久化写入/维护时间下界约束。重启只保留该持久下界：若过期正文尚未回收，墙钟回退跨过 deadline 后可能重新可见。大幅时钟偏差返回 clock_uncertain。WAL、备份和旧快照可保留正文，TTL 不是隐私删除承诺。ID 复用继续递增技术版本；旧向量不能补到新正文。已经持久回收的旧版本不会自行复活。

查询在捕获时刻应用 TTL 过滤，最终返回候选再按最新评估时刻验证，期间到期可能短页。count_total 是捕获索引查询的模式相关观测数，不能当作最终时刻精确总数。BM25 语料统计在后台移除过期索引前仍可能包含这些记录。分页游标在 seq 改变或跨越捕获视图最早 TTL 时冲突，需要重新遍历。

普通提交借用同一 partition 的实际 writer guard，并在提交时绑定 Inner guard，去掉 full base clone 和全量基准 proof；[#191](https://github.com/TheLudlows/anyseek/pull/191) 进一步以本批成功前缀 PlanningView 替代完整 shadow clone。结构合法性依据见[写入证明](record-write-proof.md)。回执前缀检查、切段地址重映射、持久化及索引仍有各自成本，不能宣称整个写入只随批次增长。

回执默认/最低回收年龄已改为两日，保留窗口及认证切点双条件；SQLite 恢复回执保留。政策的两日边界、认证切点、当前/前日保护及显式长窗口配置已通过回归。恢复 SourceProof 及恢复/维护/诊断必要全量校验、未知结果和备份保护保留。#187 的性能对照使用独立保存的 master024d9d9 基线；#191 另有增量规划及地址移动实验，整段流程收益尚未确立且保留不利样本。每轮测量绑定各自精确提交、二进制及原始日志，不能将中间提交数字改标为最终结果；本地合成负载不代表真实业务或长期容量验收。

不支持旧格式升级、迁移或 SQL/JOIN。已有派生索引有界后台合并；这不代表支持 collection 自动分裂合并。主干合入不代表已发布功能。

`/doc/{record_id}/status` 保留为技术状态接口，用于当前版本和 pending_vector_fields，不再包含来源依赖业务状态。
