# AnySeek 文档

| 目的 | 唯一入口 |
| --- | --- |
| 最快开始使用 | [仓库 README](../README.md#快速开始) |
| API 启动、配置与接入 | [服务指南](../anyseek/src/server/README.md)：密钥、S3、字段、写入/重试、查询、备份及配额；[HTTP 请求示例](../anyseek/src/server/test_api.rest) |
| 控制台使用与部署 | [控制台指南](../console/README.md)：初始化、导入、嵌入服务、API 代理、HTTPS 与运行状态 |
| 功能与限制 | [能力概览](features.md) |
| 检索配置与排序 | [文本归一化、N-gram、字段长度、稳定排序与 Weighted RRF](search.md) |
| 架构、故障与关闭 | [当前架构](architecture.md)：模块、可查尾段、Segment 所有权、失败封锁与服务排空 |
| 一致性与写入证明 | [Record 契约](record-model.md)：身份、重试、TTL、授权与普通写入的提交基准 |
| 持久化与恢复 | [存储协议](storage.md)：本地与 SQLite/S3 权威、checkpoint、备份和回收 |
| 开发与验证 | [开发指南](development.md)：本地构建、故障测试、S3 验收、CI 分工与完整矩阵 |
| 后续工作与性能证据 | [路线与证据索引](roadmap.md)；原始数据和复现命令保留在对应 benchmarks 目录 |
| 语料与许可 | [MIRACL 语料](../miracl-corpus-v1.0-zh/README.md)、[许可证](../LICENSE) |

接口变更同步服务指南和可执行示例；设计不变量写入相应协议文档，验证命令集中在开发指南。性能报告保留其固定版本、原始证据和复现步骤，由路线文档统一索引。历史计划、已完成任务流水和测试整理过程通过 Git 历史、PR 与 CI 查阅。
