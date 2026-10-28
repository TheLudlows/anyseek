# anyseek 当前状态

2026-10-28：#86/#87 已合入 master `50f8839`。本轮已实现稳定索引身份、认证时间前缀与接收日志持续回收、有界对象登记，以及脱离权威 checkpoint 的全文/向量派生缓存合并。202 只表示命令持久接收；业务回执和两类索引水位分别核验。已按用户要求发布 [Draft PR #88](https://github.com/TheLudlows/anyseek/pull/88)，随后小步完成增量计数、TTL 下界、固定 cut 来源捕获、owned Compact/原生包复用及本地、远端锁外接收快照。远端首次 stale 后使用新命令背压及 pending 排空保障维护机会；普通远端追加仍持 Spool 锁跨网络。两次基准暴露的共享 I/O 配额问题已按 Journal、checkpoint、索引、接收/终态和回收分步修复并通过专项；最终冻结 `adc15e2` 已完成 35 targets，556 通过、0 失败、13 忽略；独立 core 双配置与 9 项 doctest 通过。最新 `1359d7e` 固定产物的同数据同资源 Meilisearch 对比已完成双方 2305 文档、393 查询及重启核验，必需维护阶段覆盖完整；AnySeek 索引水位 p99 929 ms 的长尾仍明确保留，小规模串行结果不代表饱和吞吐或相关性质量，结果统一记录在[异步写入实施](docs/async-ingestion.md)。真实 S3 和硬断电尚未验收。下方保留历史验收记录，不代表本轮结果。

核验日期：2026-10-25。已验收代码 SHA 为 `94d0a60079ae7b8604223b130559bc14004816fe`（[CI 9/9 成功](https://github.com/TheLudlows/anyseek/actions/runs/40063214949)）；后续仅文档提交不改变该代码验收结论。[PR #81](https://github.com/TheLudlows/anyseek/pull/81) 已将生产全文替换为 Tantivy 0.27.0，数据访问统一使用 `RecordPartition`，仅支持新库；已合入 `6d28798`，主干 [CI 9/9](https://github.com/TheLudlows/anyseek/actions/runs/40080424776) 通过。预算调整已由 PR #83 合入 `702c385`；标准分词器已由 [PR #84](https://github.com/TheLudlows/anyseek/pull/84) 合入 `34dc46f`；下表平台数字仍是 #80 历史验收。

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

下一步建议及完成标准集中在[路线](docs/roadmap.md)，能力与限制见[项目全貌](docs/project-overview.md)，使用见[服务指南](anyseek/server/README.md)。不实施旧格式迁移，不引入模型，性能验证仅使用本轮冻结最终版本；不承诺无限容量、跨主机 HA 或权威卷丢失零 RPO。[#57](https://github.com/TheLudlows/anyseek/issues/57) 仍开放。
