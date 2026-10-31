# anyseek 当前状态

## 当前主干（2026-10-31）

核对基线为 `761ec13476018a98e26ebc71583df3ffac2d0643`（#114 合入后）。本次更新只修正文档链接、当前状态和使用说明；历史阶段的测试计数保留在 Git 历史及对应 PR，不作为该基线的全量验收结果。

anyseek 是有界单机记录与检索底座：一层 Record WAL、SQLite 协调元数据、Tenant/Collection 身份、Tantivy 全文和独立向量/元数据组件已经生效。同步 `/doc`、`/batch` 与异步索引 `/ingest` 同时存在，成功含义按入口区分，见[服务指南](anyseek/src/server/README.md#异步索引写入)。

| 已合入工作 | 当前实现与证据 |
| --- | --- |
| 写入及身份收敛 | #90/#98/#102：持久业务回执与异步索引；普通 Key 认证 Tenant，Catalog 与 S3 authority 共用 SQLite，移除 User/Grant/namespace；必要协调状态与规范 Payload 分离 |
| 检索契约清理 | [#108](https://github.com/TheLudlows/anyseek/pull/108) 已合入 `3754b28`：#103 fingerprint 检查、#104 Hybrid/RRF 入口统一及 #105 无效配置/测试清理；不再列为待实施 |
| 已删除集合代次回收 | [#109](https://github.com/TheLudlows/anyseek/pull/109) 已合入 `84280a7`：#106 有界后台回收、本地 trash 恢复、S3 head/pin/依赖保护、迟到对象删除重试；保护未释放时继续 pending |
| 向量组件所有权 | [#111](https://github.com/TheLudlows/anyseek/pull/111) 已合入 `e9599d1`：#94 移除重复逻辑 shard 状态，保留 tail/冻结 Segment、ANN 和全局 Top-K；[对照记录](benchmarks/vector-segments/README.md)不宣称性能收益 |
| 独立控制台 | [#112](https://github.com/TheLudlows/anyseek/pull/112) 已合入 `71a44bc`：React/TypeScript/Vite，Rust 只提供 API；原始数字与原请求身份保留、真实 API 浏览器测试 |
| 启动与文档清理 | [#113](https://github.com/TheLudlows/anyseek/pull/113) 已合入 `f029f08`：UI 启动说明与品牌检查脚本移除；[#114](https://github.com/TheLudlows/anyseek/pull/114) 已合入 `761ec13`：文档精简与依赖更新 |
| 历史与容量 | Record history trim、过期操作安全 GC、WAL/对象 GC 已有实现；默认 1 万身份/保留操作，可显式选有界 10 万档，受保留期限、状态内存及字节预算约束 |

## 验收证据与未运行项

- 基线前一主干 `f029f08c` 的 [PR feedback](https://github.com/TheLudlows/anyseek/actions/runs/41152427079) 已成功；它不能替代 #114 的依赖更新验证。
- 基线 `761ec134` 的 [PR feedback](https://github.com/TheLudlows/anyseek/actions/runs/41153915721) 单独跟踪。完整矩阵仅手工运行，快速 CI 成功不等于全量验收；准确命令和覆盖范围见 [CI 指南](docs/ci-validation.md)。
- 本次文档修订验证相对文件链接、章节锚点及文档与代码入口的一致性，不重新宣称 Rust、Windows、S3 或浏览器回归全部通过。
- 真实云 S3 endpoint、系统/文件系统崩溃、硬断电及手工跨 AES 验证仍需实际环境证据。fixture、进程退出和编译通过均不能替代。

## 使用边界与下一步

完整逻辑 State 仍驻内存；10 万档是准入上界，不是任意数据形态的容量承诺。操作历史至少保留七天，窗口内持续更新也会消耗额度；删除不立即释放所有身份、证据或备份保护。已实现回收不等于长期稳定磁盘占用已经验收。

S3 模式必须保留 `metadata.sqlite`、SQLite WAL 和 owner 锁文件，不能只凭 bucket 重建权威。SQLite 为单主机持久卷，不提供跨主机 HA 或权威卷丢失零 RPO。不支持旧格式读取、升级或迁移。

当前接口与限制以[服务指南](anyseek/src/server/README.md)为准；剩余工作统一维护在[路线](docs/roadmap.md)，模块职责见[架构](ARCHITECTURE.md)。
