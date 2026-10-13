# SeekPower 验证记录

记录日期：2026-10-13（Asia/Singapore）。

## R0：上游完整快照导入

| 项目 | 证据/结果 |
| --- | --- |
| 固定上游提交 | `edf6f77e554c472b3729a547b8e08fb84a590a76` |
| 固定上游 tree | `df5ce14d7348ffaba9ab7fa3b9b8e196e8545e55` |
| 原 master | `3bd806e9d01c8cdbfc8881c4271cfbc76e519e6d` |
| 导入提交 | `1986b14215c82a07fac77d7c5b169b90743d76a4` |
| 导入提交 tree | `df5ce14d7348ffaba9ab7fa3b9b8e196e8545e55`，与上游一致 |
| 导入提交 parent | 原 master；保留旧版历史，不做 force push |
| 执行工作流 | https://github.com/TheLudlows/seekpower/actions/runs/38363655157 |
| 工作流 job | `115183532507`，`import`，成功 |
| 上游/导入空差异 | 工作流 `git diff --exit-code` 成功 |
| Git 对象完整性 | 工作流 `git fsck --no-dangling` 成功 |
| 临时导入分支远端校验 | push 后使用 ls-remote 校验 SHA，成功 |

源与导入 Git tree 相同，表示其受 Git 跟踪的文件内容、路径、子树和文件模式相同，包括上游 Web UI、静态/二进制资源和许可。该验证不等价于程序可运行或界面交互正确。

R0 主分支更新采用带预期旧 SHA 的快进操作。原文档提交 `006605a12110d2d85cca0b97060b9d6600fbeae0` 只增加 SEEKPOWER_PLAN.md、UPSTREAM.md、STATUS.md、VALIDATION.md；代码树不做裁剪、改名或行为修改。

## 2026-10-13：取消独立基线阶段，建立裁剪 Issue

实施入口：[Issue #1：取消 S0，直接剥离裁剪，保留 Web UI](https://github.com/TheLudlows/seekpower/issues/1)。

本次变更范围：

- 修改 `SEEKPOWER_PLAN.md`、`STATUS.md`、`VALIDATION.md`，删除独立 S0 的里程碑、执行清单和全量基线前置要求，关联 Issue #1。
- 直接以 S1.1 为首个代码实施批次；编译、受影响功能与 UI 冒烟随每批裁剪完成，不另设开始前的全量审计阶段。
- 保留 Web UI、HTTP 接口和数据/检索内核的约束不变；完整日志/恢复补强、故障注入与单机发布验收仍在 S2。
- 此提交仅调整文档，不包含功能裁剪、Rust 代码、Cargo 配置、Web UI 资源或索引格式变更。Issue/计划提交不代表代码已执行。

## 导入与本次文档调整均未执行的运行时验证

| 验证 | 状态 |
| --- | --- |
| cargo build/check/test | 未执行 |
| Rust 工具链/依赖完整可复现性 | 未验证 |
| 上游索引/API 的运行时回归 | 未执行 |
| 浏览器/UI 交互与静态资源 HTTP 冒烟 | 未执行 |
| ANN 召回、混合检索质量与延迟基准 | 未执行 |
| 崩溃、磁盘满、损坏文件与恢复注入 | 未执行 |
| 分布式、高可用和对象存储 | 尚未实现，未执行 |

不能将导入一致性工作流称为完整主干 CI 或生产就绪验证。以上历史未执行记录保留，但不再组成开始裁剪的独立前置阶段：S1 逐批完成编译、受影响功能与 UI 验证；S2 完成完整可靠性、性能和单机发布验收。取消阶段不等于跳过或虚报测试。

## 后续验证记录模板

每轮记录：阶段/提交 SHA、工具链和硬件、输入数据与随机种子、命令/配置、实际结果、日志/工作流链接、与适用对照的差异、未解决问题、该批是否可合入。S2/D 阶段另记录对应发布/阶段退出结论。失败和未执行项必须保留，不通过删除测试获得表面通过。


## 2026-10-13：S1.1 workspace 与产品入口收敛

- 起点：`9d269bbf7d0148631d227a49f01da088f60404b4`；实施分支：`codex/s1-1-prune-workspace`；Refs #1。
- 范围：移除 MCP/SDK 两个完整工程、workspace member、README 产品/发布入口；保留核心/服务 Rust 源码、Web UI 子树及全部保留路径的许可证。
- 依赖核对：在保留 crate manifest、Rust 源码、测试、Web UI、Dockerfile 中没有 SDK/MCP 引用；Web UI 通过 `master.js` 的 HTTP API 调用服务，无 SDK 运行依赖。上游本次快照没有 CI/发布工作流和 Cargo.lock。
- 测试迁移：原 SDK 测试中 live、create_apikey、create_index、index_documents、commit、query_index、get_document、clear_index 场景迁到 `scripts/http_smoke.py`；直接 HTTP 请求，无 SDK 或 Python 第三方依赖。另校验 401、首页和 HTML 引用的 CSS/JS/SVG/favicon。使用临时索引目录、动态高位端口、就绪轮询和失败后子进程回收。
- 原客户端多个测试间依赖磁盘状态、固定端口和固定等待改为单次隔离场景；不再逐个请求重启服务，因此不把此脚本称为恢复测试。完整恢复验证仍归 S2。

以下为首个裁剪提交时的检查快照；CI 后续结果记录在下方。

| 命令/检查 | 实际结果 |
| --- | --- |
| `python3 -m py_compile scripts/http_smoke.py` | 通过；仅语法检查 |
| `python3 scripts/http_smoke.py --help` | 通过；CLI 可用 |
| Cargo manifest/TOML、保留路径引用检查 | 通过；仅两个 workspace member，保留代码无 SDK/MCP 依赖 |
| `git diff --check` | 通过 |
| Git tree 范围核对 | 本批不修改保留 Rust 源码、Web UI 和索引编码；删除目录包含各自源码、测试、README、manifest 和 logo |
| 本地 `cargo build/test`、HTTP/UI 运行 | 未执行；当前执行环境没有 cargo/rustc，直接 Git HTTPS 克隆也未获得凭证，使用授权 GitHub 插件读取固定提交并提交补丁 |
| 新增 CI | `.github/workflows/ci.yml`：`cargo build --workspace` → HTTP/UI smoke → `cargo test --workspace --all-targets`；等待运行结果 |
| 浏览器交互、ANN/混合质量、故障注入 | 未执行；不能用静态资源 HTTP 检查代替完整浏览器交互或后续批次验收 |

S1.1 首个提交时结论：代码已实施，等待构建和运行验证；未合入、未勾选阶段完成。S1.2—S1.5 尚未实施。

### 首次 CI 与继承测试夹具修复

- 裁剪提交：[`db5e9ee`](https://github.com/TheLudlows/seekpower/commit/db5e9eea9a5ebb8b558dd1e02d458f518385caa3)，[PR #2](https://github.com/TheLudlows/seekpower/pull/2)。
- [首次运行 38367081026](https://github.com/TheLudlows/seekpower/actions/runs/38367081026)，job `115194668745`，Ubuntu x86_64、rustc 1.99.0 / cargo 1.99.0。
- `cargo build --workspace` 通过（2m28s）；HTTP/UI 冒烟全部通过，覆盖首页及 12 个本地资源、401、key/index 创建、单条/批量写入、commit、lexical query、文档读取与 clear。
- 既有测试执行到 `scan32_distinct_scores_exact_selection` 失败；此前及同目标内共 43 个测试通过，包括内部/外部向量基本路径。后续目标因 Cargo fail-fast 尚未运行，不能记全量通过。
- 失败定位：400 个文档用 `i % 13` 重复填充长度，Top-100 截断在同分组内；日志中的两组结果仅互换 6 个同分文档，所有差异项 score bits 均为 `1013987301`，分数多重集完全一致。测试声称“分数全不同”的前提不成立。
- 独立修复提交：[`4121118`](https://github.com/TheLudlows/seekpower/commit/412111866d464227ca58a8bb259b382b27acdb8f)。改为 20 组、每组 20 个文档，让 Top-100 落在完整组之间；新增第 100 名分数严格高于第 101 名的前提断言，保留 400 个输入、100 个结果和严格 doc_id/score 集合比较。测试改名 `scan32_score_gap_exact_selection`；没有删/跳过测试或修改检索实现。
- [修复后运行 38367772996](https://github.com/TheLudlows/seekpower/actions/runs/38367772996)**成功**，job `115196936219`；构建、HTTP/UI 冒烟及 `cargo test --workspace --all-targets` 全部通过：**45 passed / 0 failed / 0 ignored / 0 filtered out**。S1.1 仍未合入。

### S1.1 本批结论

本批代码和上述验证已完成，PR #2 可审查，等待合入；S1 整体未完成。测试修复后的生产源码 `seekstorm/src/`、HTTP 服务与 Web UI 内容仍与基点完全相同。浏览器交互、独立混合检索质量基准、完整恢复与故障注入未执行；不以本批 45 个测试替代 S1.2—S1.5/S2 的验收。

本次结果回填仅修改计划/状态/验证文档，运行代码与已通过 CI 的 `4121118` 相同；CI 链接和结论明确对应上述代码提交，不将未发生的后续主干运行记为通过。

## 2026-10-13：S1.1 合入与 S1.2 实施

- S1.1 PR #2 已合入 master，merge commit `61f11306b45af0cf29c0e040708b14f76c8ee1f2`；合入前 head `5054b8244ab1b6b153b7276801fe1edf5b4594f1` 的 CI 38368352623 通过。
- S1.2 PR #3，首个实现提交 `3cc1e0cf6ab952f48cc3e605e46b93db2da221d6`。移除模型/自动切块、明确外部向量契约、API 400 与 UI 向量输入。
- 本地执行 `python -m py_compile scripts/http_smoke.py`、`node --check seekstorm_server/web/js/master.js`，通过。当前环境无 Rust 工具链，Rust 与 HTTP/浏览器测试由 GitHub Actions 执行；等待实际结果，不以静态检查替代。
- 新增测试覆盖非法配置/维度/数值/精度、缺失向量、base64 非有限数、批量写入与更新校验、全文/向量/混合、外部向量关闭重开；原 3 个内置模型测试改用固定外部向量，不再下载模型。
- 记录格式、native ANN/exact/RRF 与分词器不改；崩溃恢复、性能与 ANN recall 大规模基准仍属于 S2，未执行。

首轮 CI [38370570453](https://github.com/TheLudlows/seekpower/actions/runs/38370570453) / job 115206243467 在编译阶段失败：迭代器初始化遗漏新增的 `ResultObject.error` 字段。已补 `error: None`，其余测试未运行，待重跑。

第二轮 CI [38370931173](https://github.com/TheLudlows/seekpower/actions/runs/38370931173) / job 115207441554：workspace 构建、HTTP 与 Chromium 全部通过；新增关闭重开回归失败。定位到继承的 TopK 阈值换算对 F32 也使用 I8 的 1/16129 系数，0.7 阈值错误排除了相同 F32 向量。最小修复按实际存储精度选缩放系数（同时用于结果与 medoid 阈值、调试分数），保留 I8 公式、SIMD/距离计算、候选算法与格式；保留失败断言，不降低阈值规避。

调用链复查发现上游 vector 分支根本不接收 facet_filter，Hybrid 仅过滤词法分支。S1.2 明确拒绝 Vector/Hybrid 非空 facet_filter（HTTP 400），避免无声返回未过滤数据；词法元数据过滤保留并增加回归。共同向量候选过滤仍归 S2，本批不宣称三种模式已具有共同过滤语义。追加 Binary/base64 写入查询和旧模型元数据拒绝测试。

第三轮 CI [38371668492](https://github.com/TheLudlows/seekpower/actions/runs/38371668492) / job 115209891638：构建通过，既有 HTTP 冒烟在 commit 返回数量断言失败，后续测试未运行。定位到服务的 commit/close 端点在等待异步写入之前读取数量，因此响应可能落后一条。将计数读取移到 commit/close 完成之后；保留原四条文档断言，不增加睡眠或重试来掩盖竞争。核心提交屏障和持久化路径不改。

第四轮 CI [38372039419](https://github.com/TheLudlows/seekpower/actions/runs/38372039419) / job 115211134080：构建和原 commit 数量断言通过；新增过滤测试误用继承文档中的 `String` 枚举，实际 API 为 `String16`，导致请求解码 400。修正测试 payload，并断言向量过滤拒绝响应包含具体原因，避免把反序列化失败误判为过滤保护通过。
