# anyseek 品牌与文档整理

状态：**本地实现及 Linux 验证完成，待用户检视，尚未发布**。基线 `daa92239dc81f6a64e66c1c0970fbe01ffd36cff`；分支 `chore/anyseek-brand-docs`。本批不做 P3 实现，不改远端仓库名称，不增加升级迁移接口。

## 范围与设计

| 对象 | 本批处理 |
| --- | --- |
| 产品展示 | README、指南、UI 标题/logo、服务 banner、API 联系名称统一小写 anyseek |
| Rust 包/目录/二进制 | `seekpower` → `anyseek`，`seekpower_server` → `anyseek_server`；导入、features、path 依赖、测试、CI 同步 |
| Python 客户端工具 | `scripts/anyseek_client.py`，调用方同步；不是新增 SDK |
| 测试/客户端环境变量 | `SEEKPOWER_` → `ANYSEEK_`，故障注入/测试脚本及客户端 API key 入口同步，不新增兼容别名 |
| 默认服务目录 | `seekstorm_index` → `anyseek_index`；已有服务必须显式传原 `index_path`，不会搜索或迁移旧目录 |
| 当前文档 | 功能/状态/架构与已合入 P2 对齐；独立用户、开发、设计、历史导航 |
| 历史证据 | 保留旧名称、原测试命令、SHA/PR、失败和忽略项，不冒充改名后验证 |

## 有意保留的旧标识

以下不是当前展示品牌，不做协议变更：

- `https://github.com/TheLudlows/seekpower` 及仓库标识是真实远端；未重命名远端或发布新 crates.io 包。
- 浏览器 `seekpower.apiKey/index/pendingWrites/pendingCreate/pendingTakeover/pendingService*/serviceBackupId` 槽位保留，使已有待确认请求和原 operation_id 不因改名消失；不创建双存储或自动重发。
- `.seekpower-service-*.lock` 保留同一服务根的 owner 锁命名，不能让不同品牌二进制绕过彼此的排他锁。
- `seekpower-feed-successor` 是确定性恢复 scope 的哈希域，保持重试身份语义。
- `__seekpower_time_*` 是内部保留字段命名空间，保留 schema 拒绝边界和查询缓存一致性。
- SeekStorm 的来源、版权与历史发布记录，OpenCC/词典/MIRACL 等第三方归属保持原样。

当前显示标题和包标识必须使用 anyseek；历史文档中的原命令不执行替换。协议路由、JSON 字段、格式版本、Record 身份、授权和恢复算法不因品牌改名改变。

## 验证记录

| 检查 | Linux | Windows |
| --- | --- | --- |
| `cargo test --workspace --all-targets` | 最终源码 35 suites，226 passed / 0 failed / 10 ignored | 本批未运行 |
| `cargo test --workspace --doc` | 44 passed / 0 failed | 本批未运行 |
| `cargo build --workspace` / `cargo doc --workspace --no-deps` | 通过 | 本批未运行 |
| `python3 scripts/check_core_boundary.py` | 默认及 no-default-features 独立构建/依赖检查通过 | 本批未运行 |
| `record_service_smoke.py --server target/debug/anyseek_server --browser` | 退出 0，26 个 PASS 检查点 | 本批未运行 |
| fault-injection 构建及同脚本 `--browser --faults` | 退出 0，34 个 PASS 检查点 | 本批未运行 |
| OpenAPI | 生成成功，28 paths，标题 `anyseek Record service` | 本批未运行 |
| `python3 scripts/check_docs_brand.py` | 26 份 Markdown 文件/锚点链接、92 个执行面品牌检查通过 | 本批未运行 |
| `git diff --check`、独立审查 | 通过；历史 JSON 与基线无差异，第三方许可/资源只移动 | 不适用 |
| 系统崩溃/断电 | 未运行、未验收 | 未运行、未验收 |

10 个默认忽略项中，9 个是父测试显式调用的子进程 worker；`cross_build_same_index` 需要相反 AES flags 与外部 fixture，本批未运行该组合。不将忽略数计入通过数。构建保留原有 missing_docs、unused 等警告，未混入无关代码清理。

真实 HTTP/浏览器覆盖：新包启动、共享 key、原 operation_id 丢响应重试、来源/版本/stale/TTL、可选向量、整代多批发布、定位、关系融合与命中解释、feed/bootstrap、租约清理、备份和接管。fault 模式另外执行日志未知/已持久/进程退出、完整服务备份恢复、TTL/feed/租约故障边界；只操作临时测试数据。

首次浏览器运行因本环境 Playwright 需要 Chromium 1248，而本地仅有 1234，在启动浏览器时失败。安装匹配版本后默认与故障流程均重跑通过；没有修改测试绕过浏览器。本地日志：`/tmp/anyseek-final-all.log`、`/tmp/anyseek-doctest.log`、`/tmp/anyseek-http-final.log`、`/tmp/anyseek-fault-http.log`、`/tmp/anyseek-boundary.log`、`/tmp/anyseek-openapi-final.log`。这些临时路径是本次运行证据，不是仓库下载产物。

独立审查修复：清除当前服务指南的历史能力/格式矛盾；恢复误改的历史验证 JSON；在恢复入口明确隔离；修正 graph 模块路径及 Docker 默认数据目录；保留协议标识和第三方来源。

## 基线 CI 的独立阻塞

主干 [run 39610099854](https://github.com/TheLudlows/seekpower/actions/runs/39610099854) 对应 `daa9223`，不是本批品牌 SHA。两 Linux jobs 成功；Windows job `119257158456` 于 14:17:23 UTC 结束为 cancelled，最后一步为真实 HTTP 故障矩阵。配置超时 30 分钟，job 约 35 分钟结束；没有日志证明具体原因，不能据此断言代码缺陷或宣称通过。日志下载返回 Forbidden，后续 API 401 阻止核实整个 run 终态。需恢复读取权限取得日志后另行诊断，不把修复混入本品牌批次。

当前不推送、不创建 PR、不部署；Windows 品牌验证、容器镜像构建及系统断电均未运行。当前 package `publish=false`，没有发布新 crate。后续发布必须在本批最终 SHA 上重新运行 Linux/Windows CI。
