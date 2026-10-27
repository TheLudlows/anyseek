# anyseek 当前状态

2026-10-27 新目标：轻量追加写入、最终一致的异步索引与独立合并。接收层与独立全文消费已由 [PR #86](https://github.com/TheLudlows/anyseek/pull/86) 合入 `f072eb6`。后续实现将 Record 决策、全文、向量/元数据拆成三个独立推进的任务；普通及关系融合查询在检索期间释放 Record 锁，返回前校验当前原文，保留删除和撤权屏障。202 只表示命令持久接收；Record 成功回执不代表任一索引已追上。物理行号彻底解绑、State/checkpoint 简化、持续回收和轻量合并尚未完成。见[异步写入实施](docs/async-ingestion.md)。下方是历史验收，不代表新分支的完整验收。

核验日期：2026-10-25。已验收代码 SHA 为 `94d0a60079ae7b8604223b130559bc14004816fe`（[CI 9/9 成功](https://github.com/TheLudlows/anyseek/actions/runs/40063214949)）；后续仅文档提交不改变该代码验收结论。[PR #81](https://github.com/TheLudlows/anyseek/pull/81) 已将生产全文替换为 Tantivy 0.27.0，数据访问统一使用 `RecordPartition`，仅支持新库；已合入 `6d28798`，主干 [CI 9/9](https://github.com/TheLudlows/anyseek/actions/runs/40080424776) 通过。预算调整已由 PR #83 合入 `702c385`；标准分词器已由 [PR #84](https://github.com/TheLudlows/anyseek/pull/84) 合入 `34dc46f`；下表平台数字仍是 #80 历史验收。具体契约、验证版本与忽略项见 [#80 实施与验收](docs/issue80-implementation.md)。

| 范围 | 状态 |
| --- | --- |
| Record / P2 | 稳定身份、版本、幂等、来源与证据、generation、TTL、Feed、显式关系和输出授权屏障已实现 |
| 持久化 | 本地认证发布、SQLite/S3 唯一协调权威、不可变包、恢复、备份和安全 GC 已实现 |
| #80 全文替换 | 原生 Tantivy 构建与固定 Searcher；独立向量/元数据组件；活动统计和正式评分契约保留；旧全文引擎及公开低层 API 已删除 |
| Indexer / 缓存 / 准入 | 有界后台构建、每 authority 校验缓存、collection 授权屏障与分级资源预算已实现 |
| 平台验证 | Linux 全目标 341 passed / 13 ignored；Windows 常规 337 + 四个独立大用例 = 341 passed / 13 ignored；双平台 no-default 各 198 passed，两组 doctest 各 9 passed；HTTP、备份/GC、授权和 S3 fixture 全部通过 |
| 尚未验收 | 真实 S3、系统/文件系统崩溃及硬断电、手工跨 AES 验证 |
| 核心剩余 | Record 操作账本持续 GC、完整 State 驻内存限制、全服务缓存总额、组合资源隔离验收 |

SQLite 保存必要协调元数据，Payload 唯一保存原始正文与向量，Segment 保存派生检索数据；不增加重复正文、向量或倒排镜像。S3 模式必须保留 catalog、SQLite 权威库及其 WAL、`s3-namespace`，不能仅靠 bucket 恢复权威。

下一步建议及完成标准集中在[路线](docs/roadmap.md)，能力与限制见[项目全貌](docs/project-overview.md)，使用见[服务指南](anyseek/server/README.md)。不实施旧格式迁移，不引入模型，不重启性能专项；不承诺无限容量、跨主机 HA 或权威卷丢失零 RPO。[#57](https://github.com/TheLudlows/anyseek/issues/57) 仍开放。
