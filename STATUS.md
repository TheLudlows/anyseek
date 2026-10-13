# SeekPower 执行状态

更新日期：2026-10-13（Asia/Singapore）。

入口：[实施计划](SEEKPOWER_PLAN.md) · [上游基线](UPSTREAM.md) · [验证记录](VALIDATION.md) · [S1 实施 Issue #1](https://github.com/TheLudlows/seekpower/issues/1)

## 当前阶段

**R0 已完成；独立 S0 已取消，直接以 S1 剥离裁剪为当前工作阶段。S1.1 已实施 workspace/MCP/SDK 裁剪并迁移 HTTP 冒烟；[PR #2](https://github.com/TheLudlows/seekpower/pull/2) 已通过构建和 HTTP/UI 冒烟，继承的同分边界测试夹具已修复，45 个既有测试全部通过，已合入主干（`61f11306b45af0cf29c0e040708b14f76c8ee1f2`）；S1.2 在 [PR #3](https://github.com/TheLudlows/seekpower/pull/3) 代码与本批验证完成，待审查合入。**

不再要求先完成原版全量构建/性能/UI/可靠性审计才能开始裁剪。编译、受影响功能和 UI 冒烟随每批裁剪执行；完整可靠性与单机发布验收留在 S2。

| 阶段 | 状态 | 说明 |
| --- | --- | --- |
| R0 原样代码导入 | 已完成 | `1986b14215c82a07fac77d7c5b169b90743d76a4`；与上游树完全相同 |
| R0 计划文档 | 已提交并调整 | 已取消独立基线阶段，改为直接实施 S1 |
| S1 外围裁剪 | S1.1 已合入，S1.2 已验证、待合入 | [#1](https://github.com/TheLudlows/seekpower/issues/1)；仅保留核心与服务两个 member，Web UI 原样保留 |
| S2 可靠单机 | 待执行 | ID/版本、可见性、日志/检查点/恢复、过滤和候选接口 |
| D1 静态分布式 | 待执行 | 分片路由、全局两路候选、RRF、回表 |
| D2 副本与恢复 | 待执行 | 有序复制、选主、fencing、快照追赶 |
| D3 弹性调度 | 待执行 | 迁移、均衡、热点与路由切换 |
| D4 对象存储 | 后续独立阶段 | 不纳入首轮单机裁剪 |

## 已固定的边界

- Web UI 保留并复用，不删除、不另起项目重写；后续按能力调整控件与 API。
- workspace 已收敛为 `seekstorm`、`seekstorm_server`；S1.2 分支已移除模型/切块依赖，PDF 等依赖待 S1.3 移除，不代表 S1 全部完成。
- 目标主分支继续使用 `master`；旧代码保存在导入提交的父历史中。
- S1.1 未改保留 crate 的运行源码、UI 资源、检索算法或索引格式；只额外修正一个继承测试的数据前提，未引入分布式组件。
- 删除独立 S0 不等于删除测试或宣称未执行测试已经通过；也不以另一个名称恢复全量基线前置要求。

## 当前批次：S1.1

- 删除 MCP 和 Rust SDK 两个工程及 workspace 入口；清理 README 中相应产品、SDK 发布和适配器入口。
- 经保留源码、manifest、UI 与测试检索，服务及 UI 不依赖 SDK；UI 直接请求 HTTP API。
- 将原 SDK 的 live、建 key、建索引、单条/批量写入、commit、query、get、clear 场景迁移为 `scripts/http_smoke.py`；增加鉴权拒绝和页面引用静态资源检查。
- 新增 `.github/workflows/ci.yml`，执行保留 workspace 构建、HTTP/UI 冒烟及既有核心/服务测试。
- 本地 Python 语法/CLI 与静态检查通过；CI 构建及 HTTP/UI 冒烟通过，继承测试中的同分边界夹具修复后 45 个测试全部通过。浏览器交互未验证。详细命令与失败记录见 VALIDATION。

## 当前批次：S1.2

- 移除内置模型、下载/管理、自动向量化和自动切块；保留 External 配置、ANN/exact/RRF、分词和现有向量记录格式。
- 写入和查询校验外部向量；非法批量输入在入队前拒绝，非法更新在删除前拒绝；HTTP 返回 400。
- Web UI 保留，默认 Lexical，Vector/Hybrid 支持 JSON 向量输入与可见错误。
- 替换依赖模型下载的 3 个测试，新增外部向量契约测试及 API/Chromium 回归。[CI 38373701225](https://github.com/TheLudlows/seekpower/actions/runs/38373701225) 已通过构建、48 个测试、HTTP/Chromium 交互与 OpenAPI 一致性检查。

- 针对回归失败修正相似度缩放/符号、小维度 SIMD 分派及 commit 响应计数；保留格式和算法，具体最小改动见 VALIDATION。
- 限制：Vector/Hybrid 非空 `facet_filter` 明确报错；上游向量分支原本未应用该过滤。词法过滤保留，共同候选过滤在 S2 补齐。

## 下一步

1. 审查并合入 [PR #3](https://github.com/TheLudlows/seekpower/pull/3)；代码与本批验证完成，失败历史和修复证据见 VALIDATION。
2. S1.2 合入后推进 S1.3 文件导入裁剪，再按 S1.4—S1.5 顺序推进。

各批完成时同步 commit/PR、命令、环境、结果及未解决问题。完整可靠性和性能验收留在 S2。
