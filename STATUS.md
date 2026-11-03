# anyseek 当前状态

## 当前本地分支（2026-11-03）

本次职责收敛基于主干 `024d9d90f569dda90443787d36671b6544378962`，尚未推送或创建 PR。内核、API/控制台、TTL、回执政策及普通写 proof 的变化和本地验证见[实施说明](docs/engine-scope.md)。下列历史 PR/CI 仅保留合入记录，不替代本分支验证。

AnySeek 是有界单机记录与检索服务：一层 Record WAL、SQLite 协调元数据、Tenant/Collection 身份、Tantivy 全文和独立向量/元数据组件已经生效。同步 `/batch` 与异步索引 `/ingest` 同时存在，成功含义按入口区分，见[服务指南](anyseek/src/server/README.md#异步索引写入)。

| 历史合入工作 | 当时实现与证据 |
| --- | --- |
| 写入及身份收敛 | #90/#98/#102：持久业务回执与异步索引；普通 Key 认证 Tenant，Catalog 与 S3 authority 共用 SQLite，移除 User/Grant/namespace；必要协调状态与规范 Payload 分离 |
| 检索契约清理 | [#108](https://github.com/TheLudlows/anyseek/pull/108) 已合入 `3754b28`：#103 fingerprint 检查、#104 Hybrid/RRF 入口统一及 #105 无效配置/测试清理；不再列为待实施 |
| 已删除集合代次回收 | [#109](https://github.com/TheLudlows/anyseek/pull/109) 已合入 `84280a7`：#106 有界后台回收、本地 trash 恢复、S3 head/pin/依赖保护、迟到对象删除重试；保护未释放时继续 pending |
| 向量组件所有权 | [#111](https://github.com/TheLudlows/anyseek/pull/111) 已合入 `e9599d1`：#94 移除重复逻辑 shard 状态，保留 tail/冻结 Segment、ANN 和全局 Top-K；[对照记录](benchmarks/vector-segments/README.md)不宣称性能收益 |
| 独立控制台 | [#112](https://github.com/TheLudlows/anyseek/pull/112) 已合入 `71a44bc`：React/TypeScript/Vite，Rust 只提供 API；原始数字与原请求身份保留、真实 API 浏览器测试 |
| 启动与文档清理 | [#113](https://github.com/TheLudlows/anyseek/pull/113) 已合入 `f029f08`：UI 启动说明与品牌检查脚本移除；[#114](https://github.com/TheLudlows/anyseek/pull/114) 已合入 `761ec13`：文档精简与依赖更新 |
| 历史与容量 | Record history trim、过期操作安全 GC、WAL/对象 GC 已有实现；默认 1 万身份/保留操作，可显式选有界 10 万档，受保留期限、状态内存及字节预算约束 |

## 验收证据与未运行项

- 历史主干 `f029f08c` 的 [PR feedback](https://github.com/TheLudlows/anyseek/actions/runs/41152427079) 已成功；它不能替代 #114 的依赖更新验证。
- 历史主干 `761ec134` 的 [PR feedback](https://github.com/TheLudlows/anyseek/actions/runs/41153915721) 单独跟踪。完整矩阵仅手工运行，快速 CI 成功不等于全量验收；准确命令和覆盖范围见 [CI 指南](docs/ci-validation.md)。
- 本分支本地测试按[实施说明](docs/engine-scope.md)披露分批覆盖、修复和补跑；不将历史 CI 当作当前提交 CI。
- 真实云 S3 endpoint、系统/文件系统崩溃、硬断电及手工跨 AES 验证仍需实际环境证据。fixture、进程退出和编译通过均不能替代。

## 使用边界与下一步

完整逻辑 State 仍驻内存；10 万档是准入上界，不是任意数据形态的容量承诺。Record 回执默认/最低两日才具备回收资格，还需认证切点和 epoch 条件；这不是清理时限。窗口内持续更新仍消耗额度，SQLite 恢复回执尚未收敛；删除不立即释放所有身份或备份保护。已实现回收不等于长期稳定磁盘占用已经验收。

S3 模式必须保留 `metadata.sqlite`、SQLite WAL 和 owner 锁文件，不能只凭 bucket 重建权威。SQLite 为单主机持久卷，不提供跨主机 HA 或权威卷丢失零 RPO。不支持旧格式读取、升级或迁移。

当前接口与限制以[服务指南](anyseek/src/server/README.md)为准；剩余工作统一维护在[路线](docs/roadmap.md)，模块职责见[架构](ARCHITECTURE.md)。
