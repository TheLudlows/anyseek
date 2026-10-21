# anyseek 文档索引

按阅读目的进入，不把历史计划当作已实现功能。项目品牌为小写 **anyseek**，远端仓库为 [TheLudlows/anyseek](https://github.com/TheLudlows/anyseek)。

| 目的 | 文档 |
| --- | --- |
| 首次运行 | [README](../README.md)、[HTTP/UI 服务指南](../anyseek_server/README.md)、[请求示例](../anyseek_server/test_api.rest) |
| 确认可用能力与边界 | [功能清单](../FEATURES.md)、[当前状态](../STATUS.md)、[当前架构](../ARCHITECTURE.md) |
| 开发与验证 | [开发指南](development.md)、[Rust 库示例](library-examples.md) |
| 检索配置 | [过滤与排序](../FACETED_SEARCH.md)、[N-gram](../NGRAM_SEARCH.md)、[文本归一化](text-normalization.md)、[字段长度归一化](field-length-normalization.md) |
| 排序与持久词项 | [Weighted RRF](weighted-rrf.md)、[稳定排序](stable-ranking.md)、[固定词项哈希](persistent-term-hash.md) |
| 已实现机制和阶段证据 | [P1 入口](implementation-57.md)、[P2 设计与阶段记录](implementation-57-p2.md)、[P1 验收清单](validation/issue57-p1-checkpoint.json) |
| 后续设计，未实现 | [Segment / SQLite / 对象存储 / Indexer / collection 路线](implementation-57-core-roadmap.md) |
| 本次品牌整理 | [改名矩阵、保留项与验证](validation/anyseek-brand.md) |
| 语料与归属 | [MIRACL 说明](../miracl-corpus-v1.0-zh/README.md)、[上游来源](../UPSTREAM.md)、[许可证](../LICENSE) |

## 历史资料

[早期状态](history/early-status.md)、[历史验证](../VALIDATION.md)、[字段长度阶段验证](validation/stage-c-field-lengths.md) 和 [Changelog](../CHANGELOG.md) 保留原始命令、提交、失败及测试计数。旧名称表示当时的事实，不代表另一个受支持的发行入口。

维护约定：当前用法放服务/开发指南；当前状态只在 STATUS 汇总；实现细节和实际测试放对应阶段记录；未来能力明确标设计。变更当前 API 时同步服务指南和可执行示例，不复制另一份独立接口规范。SQLite 尚未编码，系统断电仍未验收。

已删除被现行指南取代的旧迁移说明和早期规划，删除范围与保留依据见 [README 收敛记录](validation/readme-cleanup.md)。
