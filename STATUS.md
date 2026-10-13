# SeekPower 执行状态

更新日期：2026-10-13（Asia/Shanghai）。[当前功能](FEATURES.md) · [后续计划](SEEKPOWER_PLAN.md) · [验证记录](VALIDATION.md) · [迁移说明](MIGRATION.md)

S1.1—S1.5 与 B1/B2 已合入主干 `af56be6`，Issue #1 已关闭，PR #10 已合入。本分支继续实施已确认的聚合裁剪，尚未合入。

| 批次 | 范围 | 状态 | 本批已通过测试数 |
| --- | --- | --- | --- |
| S1.1 / [#2](https://github.com/TheLudlows/seekpower/pull/2) | workspace、MCP/SDK 与 HTTP 冒烟迁移 | 已合入 `61f1130` | 45 |
| S1.2 / [#3](https://github.com/TheLudlows/seekpower/pull/3) | 外置模型、推理和切块，外部向量 API/UI | 已合入 `fc47cf8` | 48 |
| S1.3 / [#4](https://github.com/TheLudlows/seekpower/pull/4) | 外置文件解析/导入和 PDF 运行时 | 已合入 `567d8a2` | 48 |
| S1.4a / [#5](https://github.com/TheLudlows/seekpower/pull/5) | 移除纠错、补全和查询改写 | 已合入 `e5e0850` | 50 |
| S1.4b / [#6](https://github.com/TheLudlows/seekpower/pull/6) | 移除地理搜索，保留普通元数据 | 已合入 `fb90463` | 53 |
| S1.4c / [#7](https://github.com/TheLudlows/seekpower/pull/7) | 移除高亮/摘要，保留原文展示 | 已合入 `8026d37` | 55 |
| S1.4d / [#8](https://github.com/TheLudlows/seekpower/pull/8) | 移除配额/限流，保留鉴权和 key 作用域 | 已合入 `ff79175` | 57 |
| S1.5 / [#9](https://github.com/TheLudlows/seekpower/pull/9) | 依赖、参数、文档与 UI 闭环 | 已合入 `4df02a8` | 58 |
| B1/B2 / [#10](https://github.com/TheLudlows/seekpower/pull/10) | 删除二进制查询；搜索/遍历统一 POST JSON | 已合入 `af56be6` | 58 |

## 上一批验证

代码 `f8d084a` 的 [CI 38410014728](https://github.com/TheLudlows/seekpower/actions/runs/38410014728) 与文档回填提交 `c25d169` 的 [CI 38410851269](https://github.com/TheLudlows/seekpower/actions/runs/38410851269) 均成功：独立核心构建、workspace 构建、依赖检查、58 个 Rust 测试、HTTP/Chromium、11 个 UI 静态资源、OpenAPI 精确对比。

## 本次聚合裁剪

本次删除分面计数、直方图、范围聚合和字段 min/max，保留普通过滤、排序、命中总数及分页；UI 改为手动日期输入。验证结果见 VALIDATION。

## 下一步与边界

1. 验证并审查本次聚合裁剪；其他裁剪项需另行确认。
2. 按计划进入 S2.1：向量候选过滤与词法一致、稳定业务 ID/版本契约（聚合分离在本批完成）。
3. 单机可靠性验收后再推进分布式路由、复制恢复和弹性。

保留 Rust 核心、HTTP 服务与 Web UI。Vector/Hybrid 仍拒绝非空元数据过滤；稳定 ID、WAL/检查点、共同可见视图与生产可靠性验收尚未完成。完整现状统一维护在 [FEATURES.md](FEATURES.md)，不以路线图替代实现清单。
