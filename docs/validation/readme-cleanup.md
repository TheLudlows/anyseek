# README 与冗余文档收敛

状态：本地文档批次，未推送。基线为已合入 PR #69 的 `452ff222efed9f53b6663040cc14ccb1d87914c6`；分支 `docs/anyseek-readme-cleanup`。已只读确认远端仓库为 TheLudlows/anyseek，本批未执行仓库改名或合并。

## 实际改动

README 按通用 AI 记忆/知识底座目标重写，分开当前 P1-A/P2 能力、资源/可靠性边界、快速开始、HTTP/Rust 接入与后续设计。保留 Web UI；模型、解析、embedding 和重算明确留在上层。Segment、SQLite 元数据和对象存储明确未实现，系统断电未验收。

| 删除文档 | 原因 | 仍有效内容的现行落点 |
| --- | --- | --- |
| MIGRATION.md | 旧裁剪协议、格式7、向量必填及旧授权接口已过时；项目不提供升级迁移，继续保留易误导接入 | 服务指南、FEATURES 和 P1/P2 记录定义当前接口与恢复；VALIDATION/CHANGELOG 保留历史变更证据 |
| SEEKPOWER_PLAN.md | S1/S2/D 旧计划已被 issue #57 和核心路线替代，含“稳定ID未实施”等错误当前状态 | core-roadmap 定义 P3–P6；保留跨 partition 各通道全局排名后 RRF、全局统计/安全剪枝、分片失败等后续正确性约束 |

未找到 ANYSEEK_PLAN.md，无需删除替代副本。不增加同内容归档文件；需要旧原文时访问基线 Git 历史。UPSTREAM 的历史规划链接固定到删除前提交，VALIDATION 中的历史文件名保留并说明其性质。

其他文档按用途保留：FEATURES/ARCHITECTURE/服务指南是当前契约；P1/P2 与 core-roadmap 是实现证据和有效设计；VALIDATION、CHANGELOG、早期状态及机器验证文件保留历史事实；检索专题和 Rust 示例仍被指南/测试使用；第三方来源和 LICENSE 不删除。文档索引和状态页同步，功能代码、脚本、CI 配置均未修改。

## 验证

- `python3 scripts/check_docs_brand.py`：通过：25 份 Markdown 文件/标题链接、92 个执行面品牌检查。
- `git diff --check`：通过。
- 独立文档审查：通过，无必要阻塞项。
- 本批仅 Markdown，无运行时代码/接口/构建变更，不重跑全量 Rust 或 HTTP。此前品牌 CI 和新 master CI 分别跟踪，不能作为本批新测试结果。
- Linux/Windows 系统崩溃与断电仍未验收。
