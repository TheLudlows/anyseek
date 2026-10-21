# anyseek 当前状态

品牌与文档 PR #69 已合入 `452ff222efed9f53b6663040cc14ccb1d87914c6`，仓库现名为 [TheLudlows/anyseek](https://github.com/TheLudlows/anyseek)。本分支基于此提交，包含 README 收敛、单 package 整合及三份文档精简；分支验证见[记录](docs/validation/single-package.md)，不以主干 CI 替代本分支检查。

| 范围 | 真实状态 | 证据与说明 |
| --- | --- | --- |
| P1-A | 默认 Record/catalog、可靠写入/恢复、共享 key、备份已实现 | [P1 记录](docs/implementation-57.md)，PR #61 已合入 |
| P2 功能批次 | 来源证据、受控接管、修订/stale、可选向量、generation、TTL、feed、关系、定位、租约、融合、多批上传、命中解释已合入 | PR #62–#67；[P2 阶段与验收](docs/implementation-57-p2.md) |
| 后续核心路线 | 设计文档已合入；SQLite 为已确认的首个云端元数据后端，未编码 | PR #68；[核心路线](docs/implementation-57-core-roadmap.md) |
| 持续运行 | 有明确容量上限，不承诺无限更新或稳定磁盘占用 | P3 Segment/manifest/安全回收尚未实现 |
| 原生验证 | 主干 `452ff222` 三个 jobs 全部成功；旧 master Windows 首次超时，重跑因新 master 运行被替代 | [新 master run](https://github.com/TheLudlows/anyseek/actions/runs/39618351141)；这是主干证据，不代表本分支已通过双平台 CI |
| 系统故障 | 系统/文件系统崩溃与断电仍未验收 | 需要隔离 Linux/Windows 环境及外部已确认写入见证；进程退出不等于断电 |

不将 #57、全部 P1/P2 或生产可靠性标为完成。不提供模型推理、解析切块、SQL/JOIN、完整图语言、旧格式升级迁移。PR #60 不在基线中；#57 保持开放。

当前能力见 [FEATURES](FEATURES.md)，使用方法见 [服务指南](anyseek/server/README.md)，开发验证见 [开发指南](docs/development.md)。历史合入与测试总数保存在 [历史验证](VALIDATION.md) 和各阶段记录中，不覆盖它们当时的事实。
