# anyseek 当前状态

anyseek 是 AI 记忆与知识的存储、检索底座。PR #71 本地 Segment、[PR #72](https://github.com/TheLudlows/anyseek/pull/72) SQLite/S3 及 [PR #73](https://github.com/TheLudlows/anyseek/pull/73) 维护路径与清理、[PR #74](https://github.com/TheLudlows/anyseek/pull/74) 审计修复与无缓存验收已合入 master（源码基线 `afbe323d`）；B 尚未全部验收。

进度核验：2026-10-23。

| 范围 | 当前状态 |
| --- | --- |
| Record 与 P2 | 已合入：稳定身份/版本、幂等、来源/证据、generation、TTL、feed、显式关系、解释和授权屏障；见[语义](docs/record-model.md) |
| A：本地 Segment | 已合入：多不可变段 + tail、认证 anchor、增量 flush、compaction、安全回收与资源准入 |
| B：SQLite + S3 | 已合入：唯一协调权威、对象先持久化、提交 tail 查询、远端 checkpoint、日志/feed 保留、依赖 GC 和共同备份恢复；见[存储协议](docs/storage.md) |
| B：维护路径 | 已合入远端 seal、S3 受控接管及 retained collection 归档；PR #74 已合入，修复缓存失败分类、bootstrap 校验、备份指针容量边界及多备份归档同时持有索引导致的预算耗尽 |
| B：无缓存语义 | base、locator、graph、temporal、upload、lease、feed 七组已通过本地及 PR 原生 Linux/Windows 验收；每次重启删除 collection 缓存，保留 SQLite/catalog/namespace |
| B：后端合同 | `scripts/s3_acceptance.py` 统一 fixture/专用真实 bucket 入口；真实目标 endpoint 尚未运行，不能标为已认证 |
| B：剩余工作 | 主干 a36e71e0 的 CI 五项已通过；实际目标 S3 合同与故障矩阵、系统故障/断电验收仍待完成 |
| PR #74 验证 | 最终 head `eb9431a3` 的 [CI 五项全部成功](https://github.com/TheLudlows/anyseek/actions/runs/39814329888)，包含构建/浏览器与 Linux/Windows 原生持久化、S3 语义及合同；本地 281 tests、44 doctests 通过；[主干 a36e71e0 CI 五项通过](https://github.com/TheLudlows/anyseek/actions/runs/39884779455) |
| C1：开发分支 | 已开始，未验收/未合入：有界后台 Indexer、构建 pin/epoch/期限、SQLite 条件发布与并行写入 tail 保留；见[路线](docs/roadmap.md) |
| C 后续 / D | 校验缓存与 collection 资源公平后续推进；超大 collection 按容量需求后置，均未实现 |
| 系统故障 | 系统/文件系统崩溃和硬断电尚未验收；进程退出测试不能替代 |

开发分支新增有界后台构建及并发 tail 发布，Rust/HTTP/原生平台回归仍待当前提交 CI，不能按已交付能力使用。当前运行态 State 仍驻内存，原同步 compaction 保留串行屏障；资源方案为保守准入与显式缓冲检查，不是全进程 RSS 硬上限。Record 操作账本尚无持续 GC，不承诺无界规模或稳定磁盘占用。

S3 服务根中的 catalog、SQLite 权威库及 `s3-namespace` 是持久协调状态；collection 内日志、payload 和索引是可重建缓存。独立恢复使用不同对象命名空间。SQLite 单主机、单持久卷，不提供跨主机 HA 或权威卷丢失零 RPO。

保留 Web UI。不引入模型推理、解析切块、SQL/JOIN、完整图语言或旧格式升级迁移。[#57](https://github.com/TheLudlows/anyseek/issues/57) 保持开放。当前能力见 [FEATURES](FEATURES.md)，配置与使用见[服务指南](anyseek/server/README.md)。
