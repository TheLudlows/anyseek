# SeekPower 执行状态

更新日期：2026-10-13（Asia/Singapore）。

入口：[实施计划](SEEKPOWER_PLAN.md) · [上游基线](UPSTREAM.md) · [验证记录](VALIDATION.md) · [S1 实施 Issue #1](https://github.com/TheLudlows/seekpower/issues/1)

## 当前阶段

**R0 已完成；独立 S0 已取消，直接以 S1 剥离裁剪为当前工作阶段。S1.1 已实施 workspace/MCP/SDK 裁剪并迁移 HTTP 冒烟；正在等待 PR 编译与运行验证，尚未合入主干。**

不再要求先完成原版全量构建/性能/UI/可靠性审计才能开始裁剪。编译、受影响功能和 UI 冒烟随每批裁剪执行；完整可靠性与单机发布验收留在 S2。

| 阶段 | 状态 | 说明 |
| --- | --- | --- |
| R0 原样代码导入 | 已完成 | `1986b14215c82a07fac77d7c5b169b90743d76a4`；与上游树完全相同 |
| R0 计划文档 | 已提交并调整 | 已取消独立基线阶段，改为直接实施 S1 |
| S1 外围裁剪 | S1.1 代码已实施，待验证/合入 | [#1](https://github.com/TheLudlows/seekpower/issues/1)；仅保留核心与服务两个 member，Web UI 原样保留 |
| S2 可靠单机 | 待执行 | ID/版本、可见性、日志/检查点/恢复、过滤和候选接口 |
| D1 静态分布式 | 待执行 | 分片路由、全局两路候选、RRF、回表 |
| D2 副本与恢复 | 待执行 | 有序复制、选主、fencing、快照追赶 |
| D3 弹性调度 | 待执行 | 迁移、均衡、热点与路由切换 |
| D4 对象存储 | 后续独立阶段 | 不纳入首轮单机裁剪 |

## 已固定的边界

- Web UI 保留并复用，不删除、不另起项目重写；后续按能力调整控件与 API。
- workspace 已收敛为 `seekstorm`、`seekstorm_server`；模型/PDF 等依赖仍待 S1.2/S1.3 移除，不代表 S1 全部完成。
- 目标主分支继续使用 `master`；旧代码保存在导入提交的父历史中。
- S1.1 未改保留 crate 的 Rust 源码、UI 资源、检索算法或索引格式；未引入分布式组件。
- 删除独立 S0 不等于删除测试或宣称未执行测试已经通过；也不以另一个名称恢复全量基线前置要求。

## 当前批次：S1.1

- 删除 MCP 和 Rust SDK 两个工程及 workspace 入口；清理 README 中相应产品、SDK 发布和适配器入口。
- 经保留源码、manifest、UI 与测试检索，服务及 UI 不依赖 SDK；UI 直接请求 HTTP API。
- 将原 SDK 的 live、建 key、建索引、单条/批量写入、commit、query、get、clear 场景迁移为 `scripts/http_smoke.py`；增加鉴权拒绝和页面引用静态资源检查。
- 新增 `.github/workflows/ci.yml`，执行保留 workspace 构建、HTTP/UI 冒烟及既有核心/服务测试。
- 本地 Python 语法/CLI 与静态检查通过；当前环境无 Rust 工具链，尚不能宣称编译、运行时或浏览器交互通过。详细结果见 VALIDATION。

## 下一步

1. 完成本批 PR 的编译、HTTP/UI 冒烟与既有测试；失败需定位并记录，不删除测试换取通过。
2. 本批验证并合入后推进 S1.2：移除模型推理/自动切块，贯通外部向量及 UI 输入。
3. 按 S1.3—S1.5 顺序裁剪文件导入和其他旁支；每批独立提交与验证。

各批完成时必须把对应 commit/PR、命令、环境、结果和未解决问题写入 VALIDATION，并同步本状态、计划和 Issue。不能把代码已提交或静态检查通过写成运行验收通过。
