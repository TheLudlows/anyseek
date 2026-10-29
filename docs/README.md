# anyseek 文档索引

| 目的 | 文档 |
| --- | --- |
| 启动与 API | [README](../README.md)、[服务指南](../anyseek/server/README.md)、[请求示例](../anyseek/server/test_api.rest) |
| 轻量写入新目标 | [异步接收实施、接口、验收与后续](async-ingestion.md) |
| 当前能力 | [状态](../STATUS.md)、[功能与边界](../FEATURES.md)、[架构](../ARCHITECTURE.md) |
| 全貌与决策 | [项目全貌与下一步建议](project-overview.md)、[Tantivy 已落地设计](tantivy-evaluation.md) |
| 一致性与持久化 | [Record 语义](record-model.md)、[存储协议](storage.md) |
| 开发与后续 | [开发与验证](development.md)、[Rust 示例](library-examples.md)、[后续路线](roadmap.md) |
| 检索配置 | [N-gram](../NGRAM_SEARCH.md)、[文本归一化](text-normalization.md)、[字段长度](field-length-normalization.md) |
| 排序与词项 | [Weighted RRF](weighted-rrf.md)、[稳定排序](stable-ranking.md)、[分析身份与包契约](analysis-package-contract.md) |
| 数据与来源 | [MIRACL 语料](../miracl-corpus-v1.0-zh/README.md)、[许可证](../LICENSE) |

当前状态集中在 STATUS，接口用法集中在服务指南；协议文档只保留现行不变量，历史阶段计划、导入校验及临时机器结果通过 Git 历史和对应 PR/CI 查阅。变更 API 时同步服务指南及可执行示例。
