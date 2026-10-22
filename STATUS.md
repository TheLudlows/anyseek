# anyseek 当前状态

anyseek 是 AI 记忆与知识的存储、检索底座。PR #71 本地 Segment 和 [PR #72](https://github.com/TheLudlows/anyseek/pull/72) SQLite/S3 已合入 master；B 尚未全部验收。

| 范围 | 当前状态 |
| --- | --- |
| Record 与 P2 | 已合入：稳定身份/版本、幂等、来源/证据、generation、TTL、feed、显式关系、解释和授权屏障；见[语义](docs/record-model.md) |
| A：本地 Segment | 已合入：多不可变段 + tail、认证 anchor、增量 flush、compaction、安全回收与资源准入 |
| B：SQLite + S3 | 已合入：唯一协调权威、对象先持久化、提交 tail 查询、远端 checkpoint、日志/feed 保留、依赖 GC 和共同备份恢复；见[存储协议](docs/storage.md) |
| B：后续维护分支 | `chore/storage-maintenance-cleanup`，未合入：统一源 seal 发布、S3 受控接管及无缓存 retained collection 归档；独立验证本次改动 |
| B：剩余工作 | 维护路径审计、扩大无缓存查询/失效矩阵、真实目标 S3 合同验收 |
| PR #72 验证 | 最终 head 的 [CI #307](https://github.com/TheLudlows/anyseek/actions/runs/39784279098) 与合入 `b2c59b44` 后的[主干 CI](https://github.com/TheLudlows/anyseek/actions/runs/39798112741) 均三个任务通过，含原生 Linux/Windows；HTTP 后端为测试 fixture |
| C / D | 独立 Indexer/cache 和 collection 扩展后续推进；超大 collection 按实际容量需求后置；见[路线](docs/roadmap.md) |
| 系统故障 | 系统/文件系统崩溃和硬断电尚未验收；进程退出测试不能替代 |

当前运行态 State 仍驻内存，compaction 持有 partition 串行屏障；资源方案为保守准入与显式缓冲检查，不是全进程 RSS 硬上限。Record 操作账本尚无持续 GC，不承诺无界规模或稳定磁盘占用。

S3 服务根中的 catalog、SQLite 权威库及 `s3-namespace` 是持久协调状态；collection 内日志、payload 和索引是可重建缓存。独立恢复使用不同对象命名空间。SQLite 单主机、单持久卷，不提供跨主机 HA 或权威卷丢失零 RPO。

保留 Web UI。不引入模型推理、解析切块、SQL/JOIN、完整图语言或旧格式升级迁移。[#57](https://github.com/TheLudlows/anyseek/issues/57) 保持开放。当前能力见 [FEATURES](FEATURES.md)，配置与使用见[服务指南](anyseek/server/README.md)。
