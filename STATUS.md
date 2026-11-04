# anyseek 当前状态

## 当前主干（2026-11-03）

核对基线为 `c01e2e172e9a3747da07fb008d2bf02ae6d1dea9`。职责收敛、普通写增量规划、查询锁范围和服务关闭改造均已合入；合入不等于已发布版本或完整性能验收。内核、TTL 和回执政策见[Record 契约](docs/record-model.md)，普通写所有权及规划边界见[写入证明](docs/record-write-proof.md)。

AnySeek 是有界单机记录与检索服务：一层 Record WAL、SQLite 协调元数据、Tenant/Collection 身份、Tantivy 全文和独立向量/元数据组件已经生效。同步 `/batch` 与异步索引 `/ingest` 同时存在，成功含义按入口区分，见[服务指南](anyseek/src/server/README.md#异步索引写入)。

| 最近合入工作 | 当前实现与剩余边界 |
| --- | --- |
| [#184](https://github.com/TheLudlows/anyseek/pull/184)：#183 文件库存超限 | 私有批量构建先完成有界小段合并及引用感知 GC，再收集库存；保留 1,024 文件上限。已有原故障数据验证，不再列为尚未实现的修复；更大负载及资源压力仍受预算限制 |
| [#187](https://github.com/TheLudlows/anyseek/pull/187)：引擎职责与提交校验 | 删除专用来源/关系业务模型；普通写不再复制 full base 或执行全量基准 proof，恢复和维护继续完整校验 |
| [#188](https://github.com/TheLudlows/anyseek/pull/188)：全文锁范围 | 纯全文 Count、默认及 ID/score 排序在评分前释放组件锁；Hybrid 与字段排序仍保留原路径，性能验收未完成 |
| [#189](https://github.com/TheLudlows/anyseek/pull/189)：服务关闭 | 停止准入、排空已接收工作并等待资源释放，最后释放根锁；不新增强制 checkpoint 或持久性保证 |
| [#186](https://github.com/TheLudlows/anyseek/pull/186)：读写交错回归 | 验证编码/上传持有 writer 时查询和 TTL 重验仍能完成，且读取不持久修改写入基准 |
| [#191](https://github.com/TheLudlows/anyseek/pull/191)：写入规划与 tail 轮换 | PlanningView 替代完整 shadow，只保留本批成功前缀；轮换移动地址字符串并原位更新位置。历史回执检查、切段和索引仍有额外成本，不宣称整个写入为 O(batch) |

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

查询辅助成本现已合并过滤通过集合与身份位置映射，并复用已有 TTL 下界跳过确定无到期记录的计数扫描；[测量、回归与剩余瓶颈](benchmarks/query-cost/README.md)记录适用范围与退化样本。Hybrid/字段排序组件锁、语料统计和物理版本扫描仍保留，#155 未因此关闭。

- 各改造的精确提交、历史测试与不利样本保留在上述 PR 及 Git 历史中，不将开发期间累计用例数或旧 CI 当作当前提交重测。完整矩阵仅手工运行，快速 CI 成功不等于全量验收；准确命令和覆盖范围见 [CI 指南](docs/ci-validation.md)。
- #183 在上述基线重新构建的 Linux 默认特性测试二进制中复核，8 项现有定向回归全部通过：4,000 条约 8 KiB 正文确实生成超过 1,024 个文件，覆盖有界合并、固定旧读者、GC、完整库存认证及重开，以及合并失败/取消、硬压力后恢复进度、无客户端流量时维护重试、组件数量边界与日志压力下未知结果。原故障库继续写到 5,000 条的证据属于 #184，不能冒充本次重测；本次不是完整服务容量或真实 S3 验收。
- #178 的全文有界候选已合入；[#190](https://github.com/TheLudlows/anyseek/pull/190) 为待合入验收材料，固定比较 #178 前后版本，不代表当前主干所有后续改动的性能。
- 真实云 S3 endpoint、系统/文件系统崩溃、硬断电及手工跨 AES 验证仍需实际环境证据。fixture、进程退出和编译通过均不能替代。

## 使用边界与下一步

完整逻辑 State 仍驻内存；10 万档是准入上界，不是任意数据形态的容量承诺。Record 回执默认/最低两日才具备回收资格，还需认证切点和 epoch 条件；这不是清理时限。窗口内持续更新仍消耗额度，SQLite 恢复回执尚未收敛；删除不立即释放所有身份或备份保护。已实现回收不等于长期稳定磁盘占用已经验收。

S3 模式必须保留 `metadata.sqlite`、SQLite WAL 和 owner 锁文件，不能只凭 bucket 重建权威。SQLite 为单主机持久卷，不提供跨主机 HA 或权威卷丢失零 RPO。不支持旧格式读取、升级或迁移。

当前接口与限制以[服务指南](anyseek/src/server/README.md)为准；剩余工作统一维护在[路线](docs/roadmap.md)，模块职责见[架构](ARCHITECTURE.md)。
