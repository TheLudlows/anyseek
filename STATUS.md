# SeekPower 执行状态

更新日期：2026-10-14（Asia/Shanghai）。[当前功能](FEATURES.md) · [后续计划](SEEKPOWER_PLAN.md) · [验证记录](VALIDATION.md) · [迁移说明](MIGRATION.md)

S1.1—S1.5 与 B1/B2 已合入主干 `af56be6`，Issue #1 已关闭，PR #10 已合入。PR #11 聚合裁剪已合入 `97bea80`。PR #12 共享过滤已合入 `ea32aa1`。PR #13 已合入 `7bd1602`；PR #14 HTTP/OpenAPI 与结构化信息/日志边界已合入 `74d5012`。PR #15 首次 key 初始化与索引创建界面已合入 `08bf119`。PR #16 按需内存与 JSON 写入界面已合入 `4a459f9`。PR #17 C1～C5 已合入 `ebe7151`。PR #18 N-gram 修复与 D1～D3、PR #19 UI 外链清理已合入；PR #20 D5 已合入 `d1254da`；本分支执行 E1/E2，固定 AHash 并清理旧路由专用提示。

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
| 聚合裁剪 / [#11](https://github.com/TheLudlows/seekpower/pull/11) | 删除统计聚合；保留过滤/排序和日期输入 | 已合入 `97bea80` | 58 |
| 共享过滤 / [#12](https://github.com/TheLudlows/seekpower/pull/12) | 三种检索模式共享元数据过滤 | 已合入 `ea32aa1` | 60 |
| 兼容清理 / [#13](https://github.com/TheLudlows/seekpower/pull/13) | 删除旧索引兼容与历史基准描述 | 已合入 `7bd1602` | 62 |
| 核心边界 / [#14](https://github.com/TheLudlows/seekpower/pull/14) | HTTP/OpenAPI 下移，结构化信息与日志 | 已合入 `74d5012` | 63 |
| 启动与界面 / [#15](https://github.com/TheLudlows/seekpower/pull/15) | 首次随机 key、界面连接与创建索引 | 已合入 `08bf119` | 63 |
| 内存与写入 / [#16](https://github.com/TheLudlows/seekpower/pull/16) | 按需分配缓冲、JSON 写入与提交界面 | 已合入 `4a459f9` | 64 |
| C1～C5 / [#17](https://github.com/TheLudlows/seekpower/pull/17) | 闲置代码、控制台/demo key/表格、历史资源、重复入口裁剪 | 已合入 `ebe7151` | 64 |
| N-gram / D1～D3 | 修复独立 N-gram 标志；删除 half、LZ4/Zstd、RAM 全量加载 | 已合入 PR #18 | 68 |

| UI 外链 / [#19](https://github.com/TheLudlows/seekpower/pull/19) | 删除上游宣传与文档链接 | 已合入 `3c3de11` | 68 |
| D5 / [#20](https://github.com/TheLudlows/seekpower/pull/20) | 删除同义词、词干器、接口和 Snowball 依赖 | 已合入 `d1254da` | 68 |
| E1/E2 | 固定 AHash、清理旧协议专用提示 | 本分支实施 | 见 VALIDATION.md |

## 最近已合入验证

PR #17 的 [CI 38507685545](https://github.com/TheLudlows/seekpower/actions/runs/38507685545) 全部通过：独立核心边界、workspace 构建、依赖检查、64 项 Rust 测试、HTTP/Chromium、三种模式 JSON 写入、9 个 UI 静态资源及 OpenAPI 精确对比。早期批次记录见 [VALIDATION.md](VALIDATION.md)。

## 下一步与边界

1. HTTP/OpenAPI 下移、结构化 IndexInfo 和调用方日志配置已合入；首次启动与索引创建界面也已合入；JSON 写入与按需内存分配已合入；C1～C5 已合入；N-gram 修复与 D1～D3、UI 外链清理已合入；同义词/词干器删除已合入；本次执行 E1/E2。
2. 随后推进 S2.1 的稳定业务 ID/版本契约；其他裁剪项需另行确认。
3. 单机可靠性验收后再推进分布式路由、复制恢复和弹性。

保留 Rust 核心、HTTP 服务与 Web UI。三种模式现已在主干支持元数据过滤；ANN 保留簇探测限制；稳定 ID、WAL/检查点、共同可见视图与生产可靠性验收尚未完成。完整现状统一维护在 [FEATURES.md](FEATURES.md)，不以路线图替代实现清单。
