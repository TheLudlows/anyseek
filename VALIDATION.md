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

第五轮 CI [38372367599](https://github.com/TheLudlows/seekpower/actions/runs/38372367599) / job 115212236591：构建、完整 HTTP/Chromium 通过；3 维量化阈值回归触发 SIGABRT（malloc invalid next size）。源码定位到 SIMD 查询打包按整 lane 加载，以及小于 8 维的 TurboQuant AVX2 FWHT 按 8 元素写入。修复为按有效维度/精度选择原有标量路径，并在标量分派时不构建 SIMD query；不改 SIMD kernel、ANN 算法或数据格式。保留 3 维，追加 32 维覆盖 SIMD 路径。CI 中核心测试与文档生成在构建成功后独立执行，以便 HTTP 测试失败时仍取得诊断结果；失败仍使整个 job 失败。

第六轮 CI [38373018666](https://github.com/TheLudlows/seekpower/actions/runs/38373018666) / job 115214402403：构建、HTTP/Chromium、基础外部契约与关闭重开通过，小维度内存错误消失；TurboQuant cosine 阈值断言仍失败。进一步发现其原有路径丢弃每向量 scale、误用 ScalarQuant 的 127² 常量，而启用 scale 的 TurboQuant dot/cosine 包装又错误取负。修正评分适配：恢复 TQ cosine 的每向量 scale，dot/cosine 保持正向相似度，按实际分数单位换算阈值（原始 I8/ScalarQuant cosine 保留固定比例）。仅修正包装/分派，不改 SIMD 运算 kernel、量化算法、ANN 候选算法、RRF 或索引格式。回归矩阵扩为 F32/ScalarQuant/TurboQuant × 3/32 维 × Cosine/Dot/Euclidean × All/Nprobe，保留原阈值与精确文档断言。OpenAPI 已成功生成。

### S1.2 最终代码验证结果

代码提交：`9e0d23dff2ed1c08cd8593e49a9a3115d9483383`。
完整通过：[CI 38373701225](https://github.com/TheLudlows/seekpower/actions/runs/38373701225)，job `115216672899`。
环境：GitHub ubuntu-latest、x86_64/AVX2，rustc 1.99.0 / cargo 1.99.0，Playwright 1.63.0 / Chromium 153.0.8010.12。

| 命令/检查 | 实际结果 |
| --- | --- |
| `cargo build --workspace` | 通过 |
| `cargo test --workspace --all-targets` | 48 passed，0 failed/ignored/filtered；含原 45 个测试（3 个内置模型场景改为固定外部向量）和 3 个新增契约测试 |
| `SEEKPOWER_BROWSER_TEST=1 python3 scripts/http_smoke.py --server target/debug/seekstorm_server` | 通过：健康、首页/12 资源、鉴权、基本 CRUD/commit、JSON/base64 外部向量、三类查询、非法输入 400、批量及更新拒绝保护、词法过滤和向量过滤明确拒绝 |
| Chromium 交互 | Lexical 默认、Vector/Hybrid 输入查询、缺失及错误维度反馈通过，无 pageerror |
| 距离/量化回归 | Cosine/Dot/Euclidean × F32/ScalarQuant/TurboQuant × 3/32 维 × All/Nprobe，共 36 组阈值/命中文档断言通过；另有关闭重开与旧模型元数据拒绝 |
| `seekstorm_server generate_openapi=true` + JSON/YAML `diff -u` | 生成结果与提交文件一致；无 Model schema/内置模型示例 |
| 本地 Python/JS 语法与 `git diff --check` | 通过 |

未执行/限制：AArch64/NEON 运行验证、真正多聚类的大数据 ANN recall/延迟基准、崩溃与故障注入、生产恢复验收尚未完成。小矩阵不代表全量 ANN 质量或性能认证。向量/混合 facet_filter 显式拒绝，共同过滤能力待 S2；不将“不再静默忽略”写成“已实现向量过滤”。本批保持索引格式与 SIMD kernel/量化算法/ANN 候选/RRF 实现，最小评分适配、分派和服务响应修正及全部失败记录见上文。

PR #3 完成代码及本批验证，待审查合入；Issue #1 保持打开，S1.3—S1.5 未实施。最后的状态/验证回填为文档变更，其 CI 可从 PR 检查项查看。

### 文档提交复跑暴露的 CI 缓存问题

[CI 38374281458](https://github.com/TheLudlows/seekpower/actions/runs/38374281458) / job 115218635945，文档提交 `d53f48b`：构建和 OpenAPI 一致性通过，但 HTTP 在首次创建词法索引时进程断连，既有 bitmap 测试收到 SIGILL。该轮命中上一成功轮保存的完整 target 缓存，仅重编 workspace。仓库使用 `target-cpu=native`，跨异构 runner 复用依赖机器码不具备指令集兼容保证，是本次缓存命中后故障的首要原因；此前源码不变的成功证据保留，不把失败称为通过。

CI 改用新的缓存命名空间且 `cache-targets: false`，只缓存可跨机器复用的依赖源码/工具下载；每台 runner 本地重编全部 native 目标，继续保留生产构建参数。增加 lscpu 记录。PR 暂回草稿，待无旧目标产物的完整复跑通过。

缓存修正后的提交 `9a82532b534db87395971469120937b233ef824e` 已在 [CI 38374841361](https://github.com/TheLudlows/seekpower/actions/runs/38374841361) / job `115220524747` **完整通过**：从无目标产物的状态全量构建、48 个测试（0 failed/ignored/filtered）、HTTP/Chromium 和 OpenAPI 一致性均通过。日志确认 `cache-targets: false`，缓存路径不再包含仓库 target。之前成功代码的功能结论保持，CI 跨 runner native 目标缓存问题已处理；PR 恢复可审查，仍待合入。


## S1.3 — 文件导入外置（2026-10-13）

基线：S1.2 已合入 master，PR #3 / `fc47cf83a1482f18e249c8f9c6f8e8ec87e47acc`。该 PR 最终 head `55fb315` 的 CI 38375267442 全部通过；这不是本批验证结果。

本批移除 PDF、CSV、目录扫描及本地 JSON/SIFT 导入、附件读写、HTTP 文件端点与 UI 内部文件请求。只保留已处理 JSON 记录的写入/查询/读取路径。`display_index_info` 原样迁入 info 模块；不改压缩、索引编码和检索内核。旧 files 目录不主动删除。

验证计划：workspace 构建与既有 48 个测试、HTTP 单条/批量写入与更新/删除/读取、文件端点拒绝且无写入、CLI 旧配置拒绝、Chromium 外部 PDF URL 与纯文本预览、API 生成一致性、Cargo 解析依赖中无 pdfium-render/csv/walkdir。本地无 Rust 工具链，Rust/浏览器验证由 CI 执行。下方记录实际结果；AArch64、Docker 镜像运行、质量/性能和故障恢复未验证，仍归后续阶段。


**本批结果：通过。** [PR #4](https://github.com/TheLudlows/seekpower/pull/4)，代码提交 [`dbd22c77f41f8c1ed17dc249419febefe659dc8f`](https://github.com/TheLudlows/seekpower/commit/dbd22c77f41f8c1ed17dc249419febefe659dc8f)，[CI 38384079928](https://github.com/TheLudlows/seekpower/actions/runs/38384079928)，job `115251391454`。Ubuntu x86_64，Rust/Cargo 1.99.0；依赖源码缓存沿用，native target 不跨 runner 复用。

| 命令/检查 | 实际结果 |
| --- | --- |
| `cargo build --workspace` | 通过；核心库与 HTTP 服务完整编译 |
| `cargo metadata --format-version 1` + 解析后的 packages 检查 | 无 pdfium-render、csv、walkdir（包括传递依赖） |
| `SEEKPOWER_BROWSER_TEST=1 python3 scripts/http_smoke.py --server target/debug/seekstorm_server` | 通过；鉴权、首页及 12 个资源、JSON 单条/批量写入、更新/删除/读取/提交、外部向量/混合回归保留 |
| 文件入口与 UI 定向检查 | ingest_path 返回退出码 2；旧 POST/GET 文件端点返回 404 且文档计数不变；新索引不创建 files 目录；Chromium 外部 PDF URL 未改写为内部接口，文本预览正常，0 pageerror |
| `cargo test --workspace --all-targets` | 48 passed、0 failed、0 ignored、0 filtered；未删除或跳过既有测试，仅迁移单条写入签名 |
| `generate_openapi=true` + JSON/YAML diff | 通过；保留操作未变，只删除两条 PDF 文件路径及标签 |
| 本地静态检查 | Python AST、Node 脚本语法、git diff --check 通过 |

本批首次完整 CI 即通过。此后的状态/验证回填仅修改 Markdown；最终 head 的检查结果见 PR Checks。PR 尚未合入，S1.3 清单暂不勾选，S1.4—S1.5 尚未实施。


## S1.4a — 纠错补全与查询改写裁剪（2026-10-13）

S1.3 最终 head `242815d` 的 CI 38384967519 已成功，PR #4 合入提交 `567d8a22908179236e239e4d82155a2546095273`。本批基于该提交，范围仅覆盖 S1.4 第一项；地理/高亮/配额保持待执行。

删除查询改写及 SymSpell/PruningRadixTrie 词典生命周期；移除分词器中仅用于补全统计的分支，保留原有分词、中文字典、n-gram、布尔/短语匹配。保留 query/original_query 响应字段，两者均为调用方原文；suggestions 移除。HTTP 创建/查询/Schema 旧配置必须报错，Rust 搜索移除末尾 query_rewriting 参数。旧索引 null 元数据仍可读，启用已移除功能的元数据明确返回重建提示。

验证待回填：原有 48 个测试保留，新增 2 个契约测试；HTTP 参数拒绝及未改写结果、Chromium 输入不被补全或替换、依赖图及 OpenAPI 一致性。本地无 Rust 工具链，Rust/浏览器结果以 CI 为准。没有执行生产性能、恢复或 AArch64 测试。


首轮 `bc634a4` / [CI 38386035710](https://github.com/TheLudlows/seekpower/actions/runs/38386035710)，job `115257952954`：编译、依赖图、50 个 Rust 测试及 OpenAPI 一致性通过；HTTP 契约检查通过，但 Chromium 新增输入检查等待目标请求超时。排查发现继承 UI 将 JSON 请求体使用 jQuery 默认的 application/x-www-form-urlencoded 发送，Playwright 按声明的内容类型将其解析为表单，无法匹配 query 字段。本批将查询请求 Content-Type 显式设为 application/json，并增加内容类型断言；保留原浏览器断言，未跳过失败检查。修复后的结果见下方。


**S1.4a 验证通过。** 修复提交 [`a652e6d069e91fa9d970785ee8a184affbf374f6`](https://github.com/TheLudlows/seekpower/commit/a652e6d069e91fa9d970785ee8a184affbf374f6)，[CI 38386749094](https://github.com/TheLudlows/seekpower/actions/runs/38386749094)，job `115260312309`。Ubuntu x86_64 / Rust、Cargo 1.99.0；native 目标不跨 runner 复用。

| 验证 | 实际结果 |
| --- | --- |
| `cargo build --workspace` | 通过 |
| Cargo metadata 解析依赖图 | 无 symspell_complete_rs，亦无此前裁剪的 pdfium-render/csv/walkdir |
| `cargo test --workspace --all-targets` | 50 passed，0 failed/ignored/filtered；既有 48 个测试保留，新增 2 个契约测试 |
| 查询契约 | 原始 query/original_query 一致；错词不自动改写，短语、布尔、中文、标识符和 n-gram 路径通过；关闭重开兼容、旧 null 元数据可读、启用已移除配置返回错误 |
| HTTP/UI | 鉴权、首页及 11 个资源、JSON 记录写/读/更新/删除、向量/混合及原过滤检查通过；旧创建/Schema/查询选项返回 400 |
| Chromium | 输入查询的 JSON 请求头正确，错词保持原样且不产生建议面板；词法/向量/混合、外部来源和文本预览通过；0 pageerror |
| OpenAPI | Rust 生成 JSON/YAML 与仓库一致；退役参数、类型及 suggestions 字段移除 |
| 本地静态 | Node 语法、Python AST、JSON/YAML 语义对齐、git diff --check 通过 |

不改变索引二进制编码或 ANN/评分/压缩算法。Rust 搜索与 schema 构造 API 存在显式签名变化，迁移说明已写入 README。未执行 AArch64、生产恢复/性能验证。PR #5 尚未合入；S1.4 其余三项及 S1.5 仍未实施。之后仅回填 Markdown 证据，最新提交的自动检查见 PR Checks。


## S1.4b — 地理搜索裁剪（2026-10-13）

S1.4a 最终 head `a5da5b9` 的 [CI 38387377669](https://github.com/TheLudlows/seekpower/actions/runs/38387377669) 已成功，PR #5 合入 master（`e5e0850ef255032d5a389663bafab7d350659715`）。以下基于该主干。

移除 geo_search/Point/Morton/距离派生字段及其专用过滤、范围统计和堆排序分支；保留普通字段的字节宽度、偏移和算法，不改倒排/ANN/压缩/评分。移除 ResultSort.base 和 get_document 最后一个距离参数，同步 Rust 调用、REST 示例、UI 和 OpenAPI。旧 Point schema 在根/分片打开时明确拒绝，并将分片打开错误向上传递；不迁移或删除用户旧数据。GET 查询 URL 仅接受已有实际解析的五个参数，其他参数明确拒绝；复杂查询继续通过 JSON body。

静态审查发现 UI 时间过滤原来发送数组，API Range 要求 start/end 对象；修正请求形状和日期字段引用，增加真实浏览器检查。

新增 3 个 Rust 契约测试：旧地理请求拒绝；字符串/集合/数值/时间过滤、多字段排序、空查询/Union/Intersection、数值分面在双分片 RAM/MMAP、实时/提交/重开场景下的结果；根和分片旧 Point schema 明确报错且不改写。原有 50 个测试保留。HTTP 检查旧 GET/POST 参数/Schema 返回 400、普通元数据过滤/排序；Chromium 检查日期排序无 base、时间范围过滤。验证结果待 CI 回填。

本地 Python 语法、Node 语法、JSON/YAML 语义一致和 git diff --check 已通过。无本地 Rust 工具链，编译和实际测试由 CI 执行；没有把静态检查当作运行验证。完整质量/性能及故障恢复仍归 S2。


首轮 `4e12a148` 的 [CI 38388969948](https://github.com/TheLudlows/seekpower/actions/runs/38388969948)，job `115267692631`：workspace 构建、依赖、OpenAPI 一致性及 HTTP 地理旧参数拒绝通过；新增测试夹具有误，普通元数据 HTTP schema 缺失必填 index_lexical，Rust 测试误把 Option<Value> 传给需要 Option<Embedding> 的内核。修正夹具显式关闭非文本字段词法索引，纯词法测试传 None；没有删测试或放宽断言。修复后的运行结果另行回填。


第二轮 `b6ca0ca` 的 [CI 38389512410](https://github.com/TheLudlows/seekpower/actions/runs/38389512410)，job `115269526246`：构建、依赖、完整 HTTP/Chromium、OpenAPI 全部通过。新增普通元数据回归在未提交数据的 Count 分面断言失败（总数 4，但分面为空）。审查显示继承的 realtime_search 单词计数捷径不考虑 query_facets，单词逐记录函数也仅在有过滤时统计分面；多词 Count bitmap 捷径同样不能执行过滤/分面。最小修正为有这些请求时使用已有逐记录路径，保持无约束计数捷径。增加同分片多词及带过滤计数覆盖，不移除原失败断言；没有改评分、编码或 ANN 算法。新的验证结果待回填。


第三轮 `c1cb814` 的 [CI 38390277784](https://github.com/TheLudlows/seekpower/actions/runs/38390277784)，job `115272091103`：构建、依赖、HTTP/Chromium 和 OpenAPI 通过；实时分面已返回正确计数。测试错误地要求同计数分面按标签 low/high 排序，而现有根聚合按计数排序，同分项顺序没有保证。修正测试先按标签规范化后严格比较完整标签/计数列表；仍检查精确总数、全部分面、过滤结果和记录排序，不修改产品的同分排序规则。


第四轮 `b3a72ff` 的 [CI 38390866235](https://github.com/TheLudlows/seekpower/actions/runs/38390866235)，job `115274028287`：实时过滤/计数/分面通过，下一断言定位到已提交 north/south 多词 TopkCount + 排序/分面的空汇总。原 union 扫描跳过逐记录 facet_count，而只有大于 32 词的 TopkCount 做独立 bitmap 汇总；本批复用既有 union_count，为请求元数据统计的 TopkCount 补全精确计数/分面，同时避免仅过滤时访问空 query_facets。无过滤/分面的原快速路径保持；编码/评分不变。增加同分片多词仅过滤覆盖，保留失败断言。这会为要求元数据精确统计的排序查询执行必要的额外统计遍历，不宣称获得性能提升；完整性能基线在 S2。


**S1.4b 验证通过。** [PR #6](https://github.com/TheLudlows/seekpower/pull/6)，代码提交 [`1a73048c24fd6a60cfb4486de21f5095abf07d4e`](https://github.com/TheLudlows/seekpower/commit/1a73048c24fd6a60cfb4486de21f5095abf07d4e)，[CI 38391526830](https://github.com/TheLudlows/seekpower/actions/runs/38391526830)，job `115276230370`。Ubuntu x86_64 / Rust、Cargo 1.99.0；native 目标不跨 runner 复用。

| 检查 | 实际结果 |
| --- | --- |
| `cargo build --workspace` | 通过；核心与 HTTP 服务完整构建 |
| Cargo metadata 依赖图 | 通过；此前裁剪的四个依赖均未重新引入 |
| `cargo test --workspace --all-targets` | **53 passed，0 failed/ignored/filtered**；既有 50 个测试保留，新增 3 个契约测试 |
| 普通元数据契约 | 双分片 RAM/MMAP，实时/提交/重开均通过；字符串/集合/数值/时间过滤、空查询、Union/Intersection、多字段排序、带过滤和不带过滤的单词/多词 Count/TopkCount 范围汇总 |
| 旧地理能力拒绝 | Point 创建/过滤/分面、sort.base、distance_fields 均报错；GET URL 和 JSON body 均覆盖；根/分片旧 schema 明确重建提示且原文件未改写 |
| HTTP | 鉴权、首页及 11 个静态资源、JSON 写读更新删除提交、外部向量/混合、普通元数据过滤/排序通过 |
| Chromium | 词法/向量/混合、来源/文本预览、输入原文保留、日期新旧排序和日期区间筛选通过；无 pageerror |
| OpenAPI | Rust 生成 JSON/YAML 与仓库逐字节一致；地理类型/参数移除，普通 API 保留 |
| 本地静态 | Python AST、Node 语法、JSON/YAML 语义一致、git diff --check 通过 |

新增 metadata_contract 三项测试耗时 0.48 秒，这是该小型功能夹具的总运行时间，不是吞吐/延迟基线。修正触及的是必须统计完整匹配集时的路径选择；无过滤/分面的原快速计数、索引编码、评分及 ANN 算法未改。未执行 AArch64、生产规模性能和故障恢复验证；完整性能/可靠性验收仍在 S2。

PR #6 可审查、尚未合入。S1.4 高亮/配额及 S1.5 仍待执行，Issue #1 保持打开。此后仅回填 Markdown 记录，最新提交自动检查见 PR Checks。

## 2026-10-13：S1.4b 合入与 S1.4c 高亮/摘要裁剪

- PR #6 最终文档 head `c33d160f6f29f27f5c22fd0545a9677e3d9d745b` 的 CI 38392170443（job 115278372125）全部通过；merge commit `fb90463d476871830deb24126684dee21a266e41`，作为 S1.4c 基点。
- 删除 highlighter 模块及直接 aho-corasick 依赖，迁移所有 Rust 读取调用与示例；删除 HTTP 请求高亮选项和 OpenAPI Highlight，保留搜索响应 query_terms。regex 间接依赖仍保留，CI 只检查 core 不再直接引用 aho-corasick。
- 存储格式、索引/搜索算法不变。新增两个 Rust 契约测试：旧参数拒绝，双分片 RAM/MMAP 提交前后/关闭重开完整原文与嵌套 JSON 投影。原 53 个测试保留，只迁移读取签名。
- HTTP 增加 GET/POST/URL 旧高亮参数 400、原文/嵌套 JSON/投影一致性；Chromium 增加超过旧 4000 字符上限的中文/换行原文、HTML 字符按文本呈现、非 HTTP URL 不成为活动链接、普通 body2 字段不覆盖预览、JSON 类型保留。既有词法/向量/混合、日期排序/过滤与外部来源预览继续执行。
- 本地 `node --check seekstorm_server/web/js/master.js`、Python 编译、`git diff --check`、OpenAPI JSON/YAML 语义一致性通过；本地没有 Rust 工具链，构建/测试/生成器精确对比等待 GitHub Actions，不记为已通过。
- 完整可靠性、恢复/故障注入、性能和 ANN 质量验收仍留在 S2。

### S1.4c 验证结果与本批结论

- 首次代码提交 `c29b4b278fa3a7f3179c7ba5b428b607e0cad5bd` 的 [CI 38393632751](https://github.com/TheLudlows/seekpower/actions/runs/38393632751)（job 115283267172）全部通过；55 tests，0 failed/ignored/filtered。没有通过删除或跳过测试规避失败。
- 审查长原文预览发现原页面没有可滚动文本区域；提交 `03da2f26fa9e780fd8174b259a32c68e813018b6` 增加 `max-height:60vh` 和 `overflow-y:auto`，并补充 Chromium 实际滚动断言。此前测试只验证完整文本保留，不能代表长文可读到底，此处补齐。
- 最新代码 [CI 38394080671](https://github.com/TheLudlows/seekpower/actions/runs/38394080671)（job 115284782392）**全部通过**：`cargo build --workspace`；resolved dependency 检查；`SEEKPOWER_BROWSER_TEST=1 python3 scripts/http_smoke.py --server target/debug/seekstorm_server`；`cargo test --workspace --all-targets`，**55 passed / 0 failed / 0 ignored / 0 filtered out**；OpenAPI JSON 与 YAML 均和 Rust 生成器逐字节一致。
- 首轮环境：Ubuntu x86_64、rustc 1.99.0 / cargo 1.99.0、AMD EPYC 9V74；实际 runner 环境与完整步骤输出见各运行日志。本地只做静态检查，没有把 CI 运行记作本地验证。
- HTTP/Chromium 通过原文/嵌套 JSON/投影、旧参数拒绝、长文滚动及安全文本展示；现有词法/外部向量/混合、更新删除、认证、来源链接和日期排序过滤均通过。新增双分片 RAM/MMAP 提交前后/关闭重开契约通过，原 53 个测试全部保留。
- [PR #7](https://github.com/TheLudlows/seekpower/pull/7) 代码和验证已完成，待审查合入。本次结果回填只修改计划/状态/验证文档，运行代码与上述已通过 CI 的 `03da2f2` 相同。S1.4 配额和 S1.5 尚待执行；不把本批测试等同完整可靠性、性能或生产恢复验收。

## 2026-10-13：S1.4c 合入与 S1.4d 配额裁剪

- PR #7 已由用户合入；基点 master `8026d378eb6169749924aeceed66406aa1d88d62`，tree `454f4b50e5fca97a2cc74fd4d987686e176c6ee9`。
- 删除 ApikeyQuotaObject、服务端 rate_limit 和各路由限流调用，移除服务层直接 chrono 依赖（核心 info 仍使用 chrono）。容量/文档/操作配额此前仅配置和显示，没有新增或保留虚假的配额执行承诺。
- ApikeyObject 与创建/删除请求下移服务层；key 哈希算法、ID、索引目录和按 key 查找路径保留。创建请求只接受 demo，旧 quota 字段/URL 参数明确 400；旧文件的 quota 仅为读取兼容忽略，不回写或迁移目录。
- 修正重复创建固定 demo key 会覆盖 index_list 的继承问题：已有身份直接复用。该修复随 key 创建契约验证。
- 两个新增服务测试覆盖请求拒绝和旧身份反序列化；HTTP 增加两组 key 同索引 ID、互相不可见/跨作用域读写删拒绝、master 管理权限、重复 demo 创建、旧 quota 为零重启兼容、新格式 key 重启、撤销只影响目标 key。
- 本地 Python 语法、diff 和 OpenAPI JSON/YAML 语义对比通过；无本地 Rust 工具链。等待 GitHub Actions 的构建、57 个测试、HTTP/Chromium 与 OpenAPI 精确对比，不把未执行写作通过。
- 配额退出后需要限流的部署由外围网关处理。认证/隔离保留不等同完整安全审计；完整恢复/性能与 S2 验收未在本批执行。

### S1.4d 验证结果与本批结论

- 代码提交 `b60d9f65cc62fed0266894d09fba49846b64862a` 的 [CI 38396745382](https://github.com/TheLudlows/seekpower/actions/runs/38396745382)（job 115293773999）**全部通过**。
- `cargo build --workspace`、依赖检查、`SEEKPOWER_BROWSER_TEST=1 python3 scripts/http_smoke.py --server target/debug/seekstorm_server`、`cargo test --workspace --all-targets` 均通过；**57 passed / 0 failed / 0 ignored / 0 filtered out**。保留原 55 个测试，新增两个服务契约测试，没有删除或跳过测试。
- HTTP/Chromium 全部通过：既有全文/外部向量/混合、原文与日期交互，新增 key 创建旧参数拒绝、重复 demo 创建不丢失数据、两组 key 同索引 ID 隔离、跨作用域读写删拒绝、master 管理权限、旧 quota 为零的实际进程重启、新格式 key 重启、撤销 key 后失效且另一个 key 数据不受影响。
- OpenAPI JSON/YAML 与 Rust 生成器逐字节一致。具体工具链、CPU、步骤输出见 CI 日志；本地无 Rust 工具链，仅执行静态检查。
- [PR #8](https://github.com/TheLudlows/seekpower/pull/8) 代码和验证完成，待审查合入。本次结果回填仅修改计划/状态/验证文档，运行代码与已通过 CI 的 `b60d9f6` 相同。S1.5 尚待执行，完整安全审计、恢复故障注入和性能验收不由本批替代。

## 2026-10-13：S1.4d 合入与 S1.5 收尾

- 基点 master `ff79175b264a9ecfe0739b658c78c1bc7951b877`（用户合入 PR #8），tree `27f4119e66923497e790dcde4702a0c5ad030733`。
- 核心 utoipa/ToSchema/schema 属性改为 opt-in api-schema；服务显式启用。tabled/chrono 展示移到服务层；core info::collect_index_info 采集统计，替代直接控制台输出。文件创建时间不可用时由服务显示 unavailable；既有统计采集/文件同步保持。
- 删除核心无引用的 regex、lazy_static、url、sha2、rayon、colored、rkyv 直接依赖，移出 tabled/chrono；tokio-test 改 dev-dependency，删除服务无引用 ahash。保留 tokenizer 使用的 regex-syntax、中文字典、哈希、压缩和执行库。依赖删除以源码引用审计和编译/回归共同验证。
- 新增 scripts/check_core_boundary.py：独立临时消费者分别启用默认/无默认特性，检查 normal/build 图不含 utoipa/tabled/chrono/colored/tokio-test 或已裁剪运行库，并执行 cargo check；避免 workspace 服务开启特性影响结论。
- CLI 使用 split_once 保留路径内等号，未知/非法/重复参数启动前返回 2；文档读取 URL 与迭代器未知参数明确拒绝，非法 document_id 不再退化为空。新增 CLI 单测，HTTP 覆盖保留与拒绝路径。保留原 57 个测试。
- README/服务说明移除已退役宣传、上游 registry/image 安装入口，路线图链接当前计划；manifest publish=false 防止以上游名称误发布。crate 名称、来源、许可证与历史 CHANGELOG 保留。
- 本地 manifest/Python 语法、diff、OpenAPI JSON/YAML 语义一致性检查通过；本地无 Rust，构建/58 tests/HTTP/Chromium/OpenAPI 精确对比在首次提交时等待 CI，最终结果如下。本批不改检索算法或索引编码，不替代 S2 性能、故障恢复和安全审计。

### S1.5 最终验证

- 首轮代码 `ca734259e1f028306968f762d24933aa5339a334` 的 [CI 38399374592](https://github.com/TheLudlows/seekpower/actions/runs/38399374592) 全部通过（58 tests）。复查发现 CLI 应同时保留裸写和方括号 IPv6，且监听地址不能用冒号字符串拼接；后续 `7d5d20c1ab7a15e5de8dab8377420a7fd6da4372` 规范化 CLI 地址并使用 `SocketAddr::new`，补充括号 IPv6 单测。
- 最终代码的 [CI 38400058396 / job 115305067330](https://github.com/TheLudlows/seekpower/actions/runs/38400058396/job/115305067330) 全部通过。PR 合并测试提交为 `1125e76352825f3f09eaee0f0ab6d48c1d81cc4c`（`7d5d20c` + master `ff79175`）。环境：Ubuntu 24.04.5 x86_64、Intel Xeon Platinum 8573C、rustc 1.99.0 (`b940084d7`)、cargo 1.99.0 (`5f94df478`)，沿用仓库 native 目标设置。

| 命令 / 门禁 | 结果 |
| --- | --- |
| `python3 scripts/check_core_boundary.py` | 默认与 `--no-default-features` 独立消费者依赖图及 `cargo check` 均通过，无 API/终端展示或已裁剪运行依赖泄漏 |
| `cargo build --workspace` | 通过 |
| `cargo metadata --format-version 1` 与 CI 依赖断言 | 通过，已裁剪解析/模型/改写依赖不在解析图中 |
| `SEEKPOWER_BROWSER_TEST=1 python3 scripts/http_smoke.py --server target/debug/seekstorm_server` | 通过，11 个 UI 本地资源、全文/向量/混合、元数据、原文、鉴权/key 隔离/重启、CLI 和文档/迭代器参数回归 |
| `cargo test --workspace --all-targets` | 58 passed，0 failed、ignored、filtered；含 CLI 裸写/方括号 IPv6 参数测试 |
| `generate_openapi=true` 后 JSON/YAML `diff -u` | 两份精确一致 |

- 尚有非阻塞 Rust 警告：未使用的字段/函数、导入/变量、无默认特性下的变量，以及既有 query_contract 测试文档缺失；不宣称构建零警告。未执行真实 IPv6 网络连接、AArch64、生产性能与恢复故障注入。
- [PR #9](https://github.com/TheLudlows/seekpower/pull/9) 代码和本批验证完成，等待审查合入。此后仅回填三份 Markdown，运行代码与上述通过 CI 的 `7d5d20c` 相同；最新 head 的自动检查见 PR Checks。状态文档合并为当前里程碑视图，历史过程保留在本记录。
- S1.5 清单暂不勾选，Issue #1 保持打开；合入后核对 S1 退出证据。两路共同过滤、稳定 ID/版本、日志/恢复和完整可靠性/性能验收仍属 S2。

## B1/B2：二进制协议与重复方法裁剪（2026-10-13）

- 基准：master `4df02a860d9cacf37a9265819294be4147b5a536`；用户确认仅 B1/B2。
- 删除 v2 rkyv 解码/检索/编码实现与 rkyv/url 服务依赖；v2 查询返回 404。
- v1 搜索和遍历统一 POST JSON；GET/QUERY 返回 405 + `Allow: POST, OPTIONS`；POST URL 参数返回 400，OPTIONS 保留。
- 同步 OpenAPI、HTTP 示例和说明；保持核心检索/迭代/存储代码不变，保留所有既有 Rust 测试。旧方法的 HTTP 断言迁移为拒绝契约，POST 的旧参数校验继续覆盖。新增 iterator 正向/反向分页、字段投影、旧协议退出与方法/鉴权回归。
- 本地已执行：`python -m py_compile scripts/http_smoke.py`、`git diff --check`；OpenAPI JSON/YAML 解析结果相等，组件 schema 和其他路径相对基准保持一致。
- 本地无 Rust/cargo，未执行或宣称本地构建、Rust/HTTP/Chromium 测试通过；PR CI 结果待回填。

### 首次 CI 与测试修正

- 代码 `5f621acad23964feb9fcece5f4cf0723d9321e94` 的 [CI 38409081606](https://github.com/TheLudlows/seekpower/actions/runs/38409081606)：独立核心构建、workspace 构建、依赖图、58 个 Rust 测试和 OpenAPI 精确对比通过；HTTP 在新增反向遍历断言失败，未执行后续浏览器场景，不能算整体验证通过。
- 原因是本次新增测试误按旧文档“sorted ascending”比较正反向结果；核对未改动的 `iterator.rs::get_iterator`，负 take 分支按 docid 递减依次 push，实际返回降序。
- 修正新增测试为与正向末页严格逆序比较，并同步服务接口/OpenAPI 描述。没有删除测试、放宽为无序集合比较或修改核心迭代行为。修复后的 CI 待执行。

### B1/B2 修复后验证通过

- PR：[#10](https://github.com/TheLudlows/seekpower/pull/10)，尚未合入。
- 裁剪提交：`5f621acad23964feb9fcece5f4cf0723d9321e94`；测试/说明修正提交：`f8d084abcfae4b1005cc2291cf4c51f70231529f`。
- [CI 38410014728](https://github.com/TheLudlows/seekpower/actions/runs/38410014728) **success**：独立核心默认/无默认特性构建、workspace 构建、依赖图（无 rkyv）、58 tests（0 failed/ignored/filtered）、HTTP/UI 11 个资源、Chromium 交互及生成 OpenAPI JSON/YAML 与仓库文件精确一致。
- 新契约验证：旧二进制 GET/POST/QUERY/OPTIONS 全部 404；搜索/遍历 GET/QUERY（含尾斜杠与 URL 参数）405 + Allow；POST 鉴权/非法 JSON/URL 参数校验；迭代正向分页、反向顺序和正向字段投影。
- 保留路径验证：JSON CRUD、外部 JSON/base64 向量与 hybrid、普通过滤/日期排序、原文展示、鉴权隔离/重启/撤销。没有修改核心索引格式或检索实现，也没有删除/跳过既有 Rust 测试。
- 此后仅回填计划/状态/验证文档，最新 head CI 见 PR Checks。本记录不替代 S2 的可靠性和性能验收。

## 2026-10-13：当前功能清单与文档收敛

基于 PR #10 的 `c25d169d86e7aea8fec363659526ce5c06d5338f`。该提交的 [CI 38410851269](https://github.com/TheLudlows/seekpower/actions/runs/38410851269) 已成功；主干仍为 `4df02a8`，B1/B2 尚未合入。

- 新增 FEATURES：逐项核对核心配置/检索、实际 HTTP 路由、终端命令、Web UI 与构建特性。覆盖全部 15 个 OpenAPI 操作，另列 4 个未登记的 v1 操作（关闭索引及同义词 GET/POST/PUT）、version、静态资源和预检。
- README 从 1177 行缩为 64 行，清除旧模型/切块/纠错宣传、旧性能数字、上游演示和重复教程入口；服务、架构、N-gram、分面、状态和计划同步收敛。迁移信息移至 MIGRATION。
- 原 README 的 29 个 Rust 示例迁至 docs/library-examples.md，并更新 rustdoc include；保留 FACETED_SEARCH 的 11 个 Rust 示例。移除示例中的已删除 QueryRewriting 引用并修正旧 FacetFilter 名称；核心 rustdoc 修正 String15 拼写和误注释的 clustering 字段。服务 rustdoc 删除重复且过时的 curl 示例。
- 本地检查通过：Markdown 文件链接/代码块完整性、JSON/shell 示例语法、API 清单覆盖、Rust 示例数量与允许的文本差异、git diff --check。剔除注释及 rustdoc include 路径后，两处修改的 Rust 文件可执行部分与父提交完全相同。
- 本次仅改文档、文档示例和 rustdoc 引用；保留原有测试、验证历史及许可证。当前环境无 Rust 工具链，未声称本地执行 cargo 或 rustdoc 编译；本次提交的远端结果以 PR Checks 为准。现有 CI 的 all-targets 不包含 doctest，不将示例静态检查称作编译通过。


## 2026-10-13：分面计数、直方图、范围聚合与字段 min/max 裁剪

- 基点：已合入 B1/B2 的主干 `af56be64113596eb95bb7f31b99b26e17ce772c5`。
- 移除聚合请求/响应、Rust 类型、索引统计维护、查询计数与跨分片合并，以及直方图 UI 与四个专用资源。
- 保留字符串/集合/数字/时间过滤、多字段排序、文档命中总数和分页。日期筛选改为 UTC 日期输入，截止日期包含当天。
- 原有过滤/排序回归保留；原聚合专用用例改为旧请求拒绝用例。`metadata_contract` 同时验证旧字典计数和 min/max 被忽略、字典 ID/顺序及重开后的过滤排序保持有效。磁盘旧统计槽只写 0/None，不计算或返回聚合。

| 验证 | 结果 |
| --- | --- |
| workspace build（Rust 1.99.0） | 通过 |
| `cargo test --workspace --all-targets` | 58 passed，0 failed |
| `cargo test -p seekstorm --doc` | 44 passed，0 failed；修复两处旧构造参数示例 |
| HTTP API / 静态资源 smoke | 通过；7 个 UI 资源、旧聚合请求 400、响应无聚合字段、普通过滤排序保留 |
| OpenAPI JSON/YAML | 从本次服务重新生成 |
| JavaScript / Python 语法、`git diff --check` | 通过 |
| 本地 Chromium | 未执行；浏览器下载得到损坏 ZIP，交由 CI 验证日期输入/清空、排序及其余既有 UI 场景 |

本地默认并行 codegen 链接曾出现缺失 hidden symbols/空对象文件；清理后以 `CARGO_INCREMENTAL=0`、对应 `CARGO_PROFILE_DEV_CODEGEN_UNITS=1` / `CARGO_PROFILE_TEST_CODEGEN_UNITS=1`、`CARGO_PROFILE_DEV_DEBUG=0` / `CARGO_PROFILE_TEST_DEBUG=0`、`-j 2` 完成构建和测试。未修改仓库构建配置。CI 仍使用原标准命令。

聚合裁剪已由 PR #11 合入主干 `97bea80133a5f708260449f44573dec4c943e498`。提交 `cc26f9e` 的 [CI 38420880047](https://github.com/TheLudlows/seekpower/actions/runs/38420880047) 全部通过：独立核心边界、标准 workspace 构建、依赖检查、58 个 Rust 测试、HTTP/Chromium（日期输入及清空）、7 个 UI 资源、OpenAPI 精确对比。


## 2026-10-13：S2.1 共享元数据候选过滤（稳定业务 ID/版本不在本批）

基点 `97bea80133a5f708260449f44573dec4c943e498`；聚合裁剪合入记录已回填。

- 抽出共享 schema 校验、每分片字典编译与谓词；Lexical/Vector/Hybrid 使用同一条件。谓词 AND（包括同字段重复条件）、值 OR、范围左闭右开；未知字段、未配置 facet、类型错误和反向/非有限范围明确报错。按查询删除的非法过滤返回 HTTP 400，避免错误被包装成 200。
- 已提交与未提交向量在评分/Top-K 入堆前排除不匹配及删除记录，Hybrid 两路过滤后融合；向量字段选择读取 index_vector。ANN 保留簇探测与阈值限制，不自动扩大 Nprobe，结果可能不足 K。
- 新回归发现原实现未提交集合成员映射缺失及 clear 后映射残留；已随索引字典增量维护并在 clear 清理。集合谓词匹配真实成员，不将拼接字典键当成员。
- 两个新增测试覆盖 Ram/Mmap、1/3 本机分片、所有数值过滤类型、字符串/集合16/32、多条件/重复条件、空值/未知值/范围、过滤选择率、K 超过有效记录数、实时/提交/重开/更新/删除；另覆盖 F32/I8、三种量化与距离度量、四种 ANN 模式、多向量字段选择以及全簇 F32 余弦结果与已知顺序对照。

| 检查 | 提交前结果 |
| --- | --- |
| workspace build | 通过，Rust 1.99.0 |
| workspace all-targets tests | 60 passed，0 failed |
| 核心 doctest | 44 passed，0 failed |
| HTTP 与 7 个 UI 静态资源 | 通过；三模式合法/非法过滤，Vector/Hybrid 的 filtered Top-1，鉴权及已有接口回归 |
| OpenAPI JSON/YAML | 根据实际服务重新生成 |
| Python/JS 语法与 diff 检查 | 通过 |
| 本地浏览器交互 | 未执行；此前环境的 Chromium 下载 ZIP 损坏；CI 增加三模式原生日期输入回归，结果以 PR 为准 |

本地沿用上批的非增量、单 codegen unit、debug=0、-j 2 参数避免环境链接问题，仓库配置未修改。未执行生产规模性能/召回基准、故障注入和共同可见视图验证，不将本批记录解释为 S2.1 全部完成或持久化保证。提交时尚未合入。
