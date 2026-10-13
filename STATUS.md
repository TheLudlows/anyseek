# SeekPower 执行状态

更新日期：2026-10-13（Asia/Shanghai）。

入口：[实施计划](SEEKPOWER_PLAN.md) · [上游基线](UPSTREAM.md) · [验证记录](VALIDATION.md) · [S1 实施 Issue #1](https://github.com/TheLudlows/seekpower/issues/1)

## 当前阶段

**R0 已完成；独立 S0 已取消，直接以 S1 剥离裁剪为当前工作阶段。S1.1 已实施 workspace/MCP/SDK 裁剪并迁移 HTTP 冒烟；[PR #2](https://github.com/TheLudlows/seekpower/pull/2) 已通过构建和 HTTP/UI 冒烟，继承的同分边界测试夹具已修复，45 个既有测试全部通过，已合入主干（`61f11306b45af0cf29c0e040708b14f76c8ee1f2`）；S1.2 在 [PR #3](https://github.com/TheLudlows/seekpower/pull/3) 已合入主干（`fc47cf83a1482f18e249c8f9c6f8e8ec87e47acc`）。**

不再要求先完成原版全量构建/性能/UI/可靠性审计才能开始裁剪。编译、受影响功能和 UI 冒烟随每批裁剪执行；完整可靠性与单机发布验收留在 S2。

| 阶段 | 状态 | 说明 |
| --- | --- | --- |
| R0 原样代码导入 | 已完成 | `1986b14215c82a07fac77d7c5b169b90743d76a4`；与上游树完全相同 |
| R0 计划文档 | 已提交并调整 | 已取消独立基线阶段，改为直接实施 S1 |
| S1 外围裁剪 | S1.1—S1.4b 已合入；S1.4c 高亮裁剪已验证、待合入 | [#1](https://github.com/TheLudlows/seekpower/issues/1)；仅保留核心与服务两个 member，Web UI 保留并按裁剪结果调整 |
| S2 可靠单机 | 待执行 | ID/版本、可见性、日志/检查点/恢复、过滤和候选接口 |
| D1 静态分布式 | 待执行 | 分片路由、全局两路候选、RRF、回表 |
| D2 副本与恢复 | 待执行 | 有序复制、选主、fencing、快照追赶 |
| D3 弹性调度 | 待执行 | 迁移、均衡、热点与路由切换 |
| D4 对象存储 | 后续独立阶段 | 不纳入首轮单机裁剪 |

## 已固定的边界

- Web UI 保留并复用，不删除、不另起项目重写；后续按能力调整控件与 API。
- workspace 已收敛为 `seekstorm`、`seekstorm_server`；S1.2 已移除模型/切块依赖，S1.3 已移除文件解析依赖，不代表 S1 全部完成。
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

## 当前批次：S1.3

- 移除 PDF/CSV/目录扫描及本地 JSON/SIFT 导入；保留 JSON 单条/批量 API、记录读取、更新、删除与提交。
- 移除 PDF HTTP 接口及 UI 内部附件预览；外部 URL 按普通链接处理，原文预览保留。
- 清理 pdf feature、pdfium-render/csv/walkdir 依赖；移除 PDF 附件写入/读取和多余 files 目录创建，不删除既有附件，不改变索引编码。
- 核心 Rust 单条写入改为 `index_document(document)`；控制台信息展示从 ingest 模块迁至 info 模块。
- 扩展 HTTP/浏览器回归并同步 API 描述、使用说明。[PR #4](https://github.com/TheLudlows/seekpower/pull/4) / `dbd22c7` 的 [CI 38384079928](https://github.com/TheLudlows/seekpower/actions/runs/38384079928) 全部通过：构建、48 个测试、依赖图、HTTP/Chromium 与 OpenAPI 一致性。

## 当前批次：S1.4a — 纠错、补全与改写

S1.3 已通过最终 head CI 38384967519 并合入 master（PR #4，`567d8a22908179236e239e4d82155a2546095273`）。

- 删除 SymSpell/补全词典生成、加载、保存与搜索改写分支，移除 symspell_complete_rs 依赖；保留中文分词字典及词法 n-gram 匹配。
- 删除配置、Schema 字段和 Rust 搜索改写参数；HTTP 旧参数明确返回 400；旧索引启用纠错/补全时明确拒绝打开，null 配置继续兼容。
- 删除 UI 自动补全脚本、建议面板、错词提示和查询改写请求，保留原输入、即时搜索及来源/文本预览。
- 增加精确输入、短语/布尔、中文、标识符、关闭重开和旧元数据回归；[PR #5](https://github.com/TheLudlows/seekpower/pull/5) / `a652e6d` 的 [CI 38386749094](https://github.com/TheLudlows/seekpower/actions/runs/38386749094) 全部通过（50 tests、HTTP/Chromium、依赖图、OpenAPI）；首次浏览器检查失败及请求头修复见 VALIDATION。
- S1.4a 已通过最终 head CI 38387377669 并合入 master（PR #5，`e5e0850ef255032d5a389663bafab7d350659715`）。

## 当前批次：S1.4b — 地理搜索裁剪

- 删除 geo_search 模块、Point 类型、Morton 编码、距离过滤/排序/分面和返回距离计算；保留普通字段编码、过滤、排序、分面和 ANN 距离度量。
- 移除 Rust get_document 距离参数、ResultSort.base；HTTP 旧地理参数返回 400，旧 Point schema 返回重建提示。分片打开错误向上传递，避免 panic。
- Web UI 时间排序不再发送 base；同时修正日期范围过滤的 JSON Range 形状，使用实际 DATE_FIELD。
- 新增地理请求拒绝、普通元数据多分片 RAM/MMAP 提交前后/重开回归、旧 schema 拒绝；扩展 HTTP 与 Chromium 日期排序/过滤回归。[PR #6](https://github.com/TheLudlows/seekpower/pull/6) / `1a73048` 的 [CI 38391526830](https://github.com/TheLudlows/seekpower/actions/runs/38391526830) 全部通过：53 tests、HTTP/Chromium、依赖图和 OpenAPI。
- 回归发现并修复继承的实时 Count 分面遗漏，以及已提交多词 TopkCount + 排序遗漏元数据汇总；复用现有逐记录/bitmap 计数路径，无约束快速路径保留，失败和修复详见 VALIDATION。
- 高亮、配额及 S1.5 仍待执行，S1.4 不宣称全部完成。

## 当前批次：S1.4c — 高亮/摘要裁剪

- S1.4b PR #6 的最终 head CI 38392170443 全部通过，已合入 master（`fb90463d476871830deb24126684dee21a266e41`）。
- 删除内核 highlighter/KWIC 和直接 aho-corasick 依赖；保留 regex 的间接依赖、中文分词和词法响应 query_terms。
- Rust get_document 只保留 doc_id、include_uncommitted、fields；移除搜索 highlights 和读取 query_terms/highlights，HTTP 旧参数返回 400。
- 原文和嵌套 JSON 不改写，字段投影保留；UI 不再请求高亮或使用 body2 作为预览，使用文本 DOM，完整预览保留换行，外部 HTTP(S) 来源链接保留。
- 保留 53 个既有测试并迁移读取签名，新增 2 个契约测试及 HTTP/Chromium 原文回归；[PR #7](https://github.com/TheLudlows/seekpower/pull/7) / `03da2f2` 的 [CI 38394080671](https://github.com/TheLudlows/seekpower/actions/runs/38394080671) 全部通过：55 tests、构建、依赖图、HTTP/Chromium 和 OpenAPI 精确一致性；完整原文支持滚动。
- 本批不修改索引格式、检索算法和普通元数据能力；配额与 S1.5 仍待执行。

## 下一步

1. 审查并合入 S1.4c [PR #7](https://github.com/TheLudlows/seekpower/pull/7)。
2. 推进配额，再完成 S1.5 清理闭环。

各批完成时同步 commit/PR、命令、环境、结果及未解决问题。完整可靠性和性能验收留在 S2。
