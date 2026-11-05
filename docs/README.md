# AnySeek 文档

| 目的 | 唯一入口 |
| --- | --- |
| 最快开始使用 | [仓库 README](../README.md#快速开始) |
| API 启动、配置与接入 | [服务指南](../anyseek/src/server/README.md)：密钥、S3、字段、写入/重试、查询、备份及配额；[HTTP 请求示例](../anyseek/src/server/test_api.rest) |
| 控制台使用与部署 | [控制台指南](../console/README.md)：初始化、导入、嵌入服务、API 代理、HTTPS 与运行状态 |
| 功能与限制 | [能力概览](features.md) |
| 检索配置与排序 | [文本归一化、N-gram、字段长度、稳定排序与 Weighted RRF](search.md) |
| 架构与模块 | [当前架构](architecture.md)：模块、数据归属、Segment 与认证包 |
| 一致性与持久化 | [Record 语义](record-model.md)、[普通写入证明](record-write-proof.md)、[存储协议](storage.md) |
| 故障与关闭 | [失败边界与服务排空](runtime-fault-boundaries.md) |
| 开发与验证 | [开发指南](development.md)、[CI 验收](ci-validation.md) |
| 后续工作与性能证据 | [路线与证据索引](roadmap.md)；原始数据和复现命令保留在对应 benchmarks 目录 |
| 语料与许可 | [MIRACL 语料](../miracl-corpus-v1.0-zh/README.md)、[许可证](../LICENSE) |

接口变更同步服务指南和可执行示例；设计不变量写入相应协议文档。历史计划、已完成任务流水和旧机器结果通过 Git 历史、PR 与 CI 查阅，不再维护第二份当前状态清单。
