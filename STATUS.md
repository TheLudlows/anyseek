# SeekPower 执行状态

更新日期：2026-10-13（Asia/Shanghai）。

入口：[实施计划](SEEKPOWER_PLAN.md) · [上游基线](UPSTREAM.md) · [验证记录](VALIDATION.md) · [S1 实施 Issue #1](https://github.com/TheLudlows/seekpower/issues/1)

## 当前阶段

**S1.1—S1.5 已合入主干（PR #9 合并提交 `4df02a8`）；Issue #1 已关闭。用户另行确认 B1/B2 接口裁剪，本分支已实施，CI 待验证，尚未合入。**

R0 原样导入完成（`1986b14215c82a07fac77d7c5b169b90743d76a4`），导入树与固定上游一致。独立 S0 已取消；编译、受影响功能和 UI 回归随每批裁剪执行，完整可靠性、故障注入与性能发布验收留在 S2。

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

历史失败、修复、运行命令、工具链、代码提交和 CI 证据完整保留在 [VALIDATION.md](VALIDATION.md)。

## S1.5 当前交付

- 核心 OpenAPI 改为可选 `api-schema`，由服务显式开启；终端表格和日期展示移至服务层，核心 `collect_index_info` 保留统计采集。
- 清理无引用直接依赖，`tokio-test` 改为开发依赖；保留 tokenizer 所需 `regex-syntax`、中文分词、压缩、SIMD、哈希和检索执行库。
- CLI 拒绝未知、非法和重复参数，支持路径中的等号及裸写/方括号 IPv6；文档读取和迭代器拒绝未支持选项，非法迭代起点返回错误。
- 清理失效功能宣传、安装/发布入口和 API 链接；`publish = false` 防止以上游 crate 名称误发布，保留来源与许可证。
- CI 新增 workspace 外部消费者的默认/无默认特性依赖图和构建，避免服务 feature 合并掩盖核心依赖边界。原 57 个测试保留，新增 CLI 契约测试与 HTTP 参数回归。

代码 `7d5d20c` 的 [CI 38400058396](https://github.com/TheLudlows/seekpower/actions/runs/38400058396) 全部通过：独立核心两种特性构建、workspace 构建、58 tests（0 failed/ignored/filtered）、HTTP/Chromium、11 个本地 UI 资源与 OpenAPI 精确一致性。随后只回填计划/状态/验证文档，最新提交检查见 PR Checks。

## 已固定的产品边界与限制

- 产品保留 `seekstorm` 核心库、`seekstorm_server` HTTP 服务和原有 Web UI；继续使用 `master`，不重写 UI 或全面重命名 crate。
- 保留全文/中文、ANN/exact/RRF、普通元数据过滤、结构化 JSON 写入、文档读取/删除、提交路径、鉴权和 key 数据作用域。旧裁剪选项明确拒绝，具体迁移约定见 README。
- Vector/Hybrid 对非空 `facet_filter` 仍明确报错；词法过滤保留，两路共享候选过滤归 S2.1。
- S1 测试不替代稳定 ID/版本、WAL/检查点/恢复、共同可见性、故障注入、生产性能或完整安全审计。

## 下一步

1. 完成已确认的 B1/B2 接口裁剪验证与审查：移除二进制 v2，仅保留 POST JSON 搜索/遍历。
2. 进入 S2.1：共享候选过滤、过滤与聚合分离及稳定记录 ID/版本契约；随后推进日志/恢复与可见性。
3. S2 可靠单机验收后再推进 D1 静态分布式、D2 副本恢复、D3 弹性调度；D4 对象存储为后续独立阶段。

## B1/B2：接口裁剪（用户确认，2026-10-13）

- 基于 master `4df02a860d9cacf37a9265819294be4147b5a536`；只实施 B1、B2，不包含其余候选裁剪项。
- 移除 rkyv 二进制查询实现及服务 rkyv/url 直接依赖；旧 v2 路由返回 404。
- 搜索与 iterator 仅接受 POST JSON；旧 GET/QUERY 返回 405 + Allow，POST URL 参数返回 400，OPTIONS 正确声明方法。
- 保留普通 GET、外部 JSON/base64 向量、鉴权、Web UI、内核检索/迭代/存储及原 58 个 Rust 测试；扩展 HTTP 契约和迭代器分页/投影回归。
- 本地 Python 语法、OpenAPI JSON/YAML 结构一致性和差异检查通过；当前环境无 Rust 工具链，编译/运行验证由 PR CI 执行，尚未宣称通过。
