# anyseek 当前状态

核验日期：2026-10-24。本任务分支已将生产全文替换为 Tantivy 0.27.0，数据访问统一使用 `RecordPartition`，仅支持新库；尚未合入。主干文档已同步至 `31a4d059`。具体契约、验证版本与忽略项见 [#80 实施与验收](docs/issue80-implementation.md)。

| 范围 | 状态 |
| --- | --- |
| Record / P2 | 稳定身份、版本、幂等、来源与证据、generation、TTL、Feed、显式关系和输出授权屏障已实现 |
| 持久化 | 本地认证发布、SQLite/S3 唯一协调权威、不可变包、恢复、备份和安全 GC 已实现 |
| #80 全文替换 | 原生 Tantivy 构建与固定 Searcher；独立向量/元数据组件；活动统计和正式评分契约保留；旧全文引擎及公开低层 API 已删除 |
| Indexer / 缓存 / 准入 | 有界后台构建、每 authority 校验缓存、collection 授权屏障与分级资源预算已实现 |
| 本地验证 | 本轮全目标编译、受影响回归 265 passed、no-default 197 passed、两组 doctest 各 9 passed；完整最终平台矩阵待 PR CI。清理前 338 passed 与 HTTP/S3 仅为历史证据 |
| 尚未验收 | 当前分支 Windows CI、真实 S3、系统/文件系统崩溃及硬断电、手工跨 AES 验证 |
| 核心剩余 | Record 操作账本持续 GC、完整 State 驻内存限制、全服务缓存总额、组合资源隔离验收 |

SQLite 保存必要协调元数据，Payload 唯一保存原始正文与向量，Segment 保存派生检索数据；不增加重复正文、向量或倒排镜像。S3 模式必须保留 catalog、SQLite 权威库及其 WAL、`s3-namespace`，不能仅靠 bucket 恢复权威。

下一步建议及完成标准集中在[路线](docs/roadmap.md)，能力与限制见[项目全貌](docs/project-overview.md)，使用见[服务指南](anyseek/server/README.md)。不实施旧格式迁移，不引入模型，不重启性能专项；不承诺无限容量、跨主机 HA 或权威卷丢失零 RPO。[#57](https://github.com/TheLudlows/anyseek/issues/57) 仍开放。
