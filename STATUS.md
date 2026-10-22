# anyseek 当前状态

anyseek 是 AI 记忆与知识的存储、检索底座。主干基线为 `5cd76e3b`（PR #71）；下表区分已合入能力与本分支实现，分支功能不计作主干完成。

| 范围 | 状态 | 证据与边界 |
| --- | --- | --- |
| P1-A 可靠 Record | 已合入 | 稳定身份/版本、条件写、幂等、持久化恢复、共同视图、共享 key/catalog、备份；见 [P1](docs/implementation-57.md) |
| P2 已界定批次 | 已合入 | 来源/证据、修订 stale、generation、可选向量、TTL、feed、实体与有界关系、命中解释；见 [P2](docs/implementation-57-p2.md) |
| A/P3 本地 Segment | 已合入，PR #71 | 不可变多段 + tail、认证 anchor、增量 flush、compaction、安全回收与资源准入；见 [A](docs/implementation-57-a-segments.md) |
| B SQLite + S3 | PR #72 Draft，未合入 | SQLite 唯一协调权威、object-first Record/服务接入；本批四项已实现并通过本地验证：远端 checkpoint/Segment 发布、日志/feed 保留、安全 GC 和共同备份恢复；见 [B](docs/implementation-72-sqlite-authority.md) |
| B 验收 | 尚未全部完成 | 最终双平台 CI、其他维护路径审计、真实目标 S3 合同测试仍须分别满足；不以 HTTP test double 冒称云后端认证 |
| C Indexer/cache 与服务扩展 | 后续 | 独立后台任务、缓存优化与 collection 路由等，不含于本批 |
| D 超大 collection | 按真实容量需求后置 | 静态分片 → 在线 split/merge → 自动调度 |
| 系统故障 | 尚未验收 | Linux/Windows 系统、文件系统崩溃和硬断电需要隔离环境；进程退出测试不能替代 |

当前运行态 State 仍驻内存，compaction 持有 partition 串行屏障；资源方案为保守准入与显式缓冲检查，不是全进程 RSS 硬上限。不承诺无界规模、跨主机 HA 或 SQLite 权威卷丢失零 RPO。

S3 服务根中的 catalog、SQLite 权威库及 `s3-namespace` 属于持久协调状态；collection 内的日志、payload 和索引是可重建缓存。独立恢复使用不同对象命名空间，避免两份权威互相 GC。

保留 Web UI。不引入模型推理、解析切块、SQL/JOIN、完整图语言、旧格式升级迁移。PR #60 不在本分支基线；[#57](https://github.com/TheLudlows/anyseek/issues/57) 保持开放。

当前能力见 [FEATURES](FEATURES.md)，配置与使用见 [服务指南](anyseek/server/README.md)，历史验证保留在 [VALIDATION](VALIDATION.md) 与各阶段记录中。
