# 当前功能与边界

**简体中文** · [English](features.en.md)

HTTP/Web UI 数据请求统一由服务内部的 Record/catalog 处理；旧低层全文 Rust API 已删除。职责边界见[Record 契约](record-model.md)；系统断电与长期持续运行容量仍未验收。[服务使用](../anyseek/src/server/README.md) · [架构](architecture.md) · [路线与验收证据](roadmap.md)

## 检索和数据

| 领域 | 当前能力与边界 |
| --- | --- |
| 全文 | BM25F/邻近度、布尔/短语、多字段、中文分词、停用词和 N-gram |
| 向量 | 外部 JSON/base64、多向量字段、F32/I8、三种距离、量化、IVF ANN；模型推理由调用方提供 |
| 混合 | 全文/向量共同视图 RRF，支持权重和候选预算、逐命中 rank/原始分数/贡献；候选窗口不当作全库精确并集 |
| 时间与生命周期 | UTC 毫秒时间过滤、捕获 evaluation_time 与最终候选重验、TTL 后台回收；重启时钟回退边界见[Record 契约](record-model.md#时间ttl-与查询视图) |
| 过滤和排序 | 共享元数据谓词、字段排序、分页和投影；ANN/Hybrid 候选内重排不保证全库字段 Top-K |
| 身份和写入 | 稳定 Record ID、版本条件、墓碑、防 ABA、原 operation_id 重试 |
| 原始数据 | 权威状态保留完整 Document 和原始向量；schema.store 控制查询投影，不控制原始数据持久保存 |
| 浏览 | 空查询浏览活动记录，包括纯向量 collection；iterator 按稳定 ID 分页，visible_seq 改变或跨越视图最早 TTL 使 cursor 失效 |
| 持久化 | 本地 Segment + tail，或 SQLite/S3 对象提交、远端 checkpoint 与 GC；同步写入 durable + visible；异步索引入口返回持久业务结果；真实 S3/系统断电尚待验收 |
| 服务认证与租户命名空间 | 用户权限由上游网关处理；AnySeek 校验服务凭据，按可信租户头隔离集合、备份及回执，不注册或签发租户 Key |
| 备份 | collection 独立快照、备份退役、完整 service/catalog 备份、新未启动根恢复 |

全文派生索引使用 Tantivy 与 WAL 支撑的可查增量，后台物化不建立第二个提交权威；向量/元数据使用独立组件。规范原文与原始向量由 Payload 保存。可查尾段、额度预留与文件所有权见[架构](architecture.md)。仅支持新库，不提供旧格式升级或迁移。

## 接入与限制

- [服务指南](../anyseek/src/server/README.md)：启动、认证、HTTP 示例、查询、备份与恢复。
- [控制台指南](../console/README.md)：租户连接、导入、嵌入服务、检索与部署。
- [资源和验收边界](../anyseek/src/server/README.md#当前资源和验收边界)：容量档、请求/响应、并发、字节与 I/O 预算。
- [后续路线与验收证据](roadmap.md)：剩余工作、测量范围及未验证环境。

集合按资源预算接收写入。查询候选上限为 10,000。完整逻辑 State 驻内存，不承诺无限写入、稳定磁盘占用或全进程 RSS 硬上限。回执最早两日后具备回收资格，仍受安全切点与保护条件限制，不是两日清理承诺。

每个 collection 一个 partition；不提供跨 collection 事务、任意历史读取、SQL/JOIN、完整图语言或分布式 HA。来源、关系、解析切块、Embedding 与 Reflect 由上层应用负责。保存 URL 不等于托管或备份外部文件。

字段支持、数值精度与空值规则统一见[字段类型与数值](../anyseek/src/server/README.md#字段类型与数值)。
