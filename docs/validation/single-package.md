# 单 anyseek package 整合验证

状态：Linux 本地验证完成，未推送、未提交 PR。

## 范围与基线

基线为 master `452ff222efed9f53b6663040cc14ccb1d87914c6`，依赖本地 README 清理提交 `3af29ab6fb62abee46bcac21864c97103f0fa900`。本批仅整合 Cargo package 与路径，不改变 P2 协议、数据格式、授权、持久化或评分语义，不提供升级迁移。

- workspace 仅保留 `anyseek` 一个 package；仍有核心 library 和 `anyseek_server` binary 两个编译目标。
- 核心 `anyseek/src/` 不依赖服务；`anyseek/server/` 保留服务实现，公共 DTO/schema 位于可选 `anyseek::server` 模块。Web 资源在 `anyseek/web/`。
- `server` 默认关闭；axum、http-body、env_logger、utoipa、utoipa-axum 为 optional。二进制 `required-features = ["server"]`。默认 zh/vb 不变。
- 六组服务集成测试迁入同 package，并由 server feature 门控。二进制名称、接口及资源内容保持不变。
- Windows winapi 功能取原两 package 所需集合；Docker、CI、脚本、文档更新构建命令和路径。CI 原覆盖不减少。
- 独立消费者分别解析默认和无默认 feature，防止 workspace feature 统一掩盖 HTTP 依赖泄漏；静态检查递归扫描核心，拒绝核心反向引用服务。

## Linux 验证

| 命令 | 实际结果 |
|---|---|
| `cargo check --features server --all-targets` | 通过 |
| `python3 scripts/check_core_boundary.py` | 默认及无默认 feature 外部消费者依赖图与编译均通过 |
| `cargo test --workspace --features server --all-targets` | 34 suites，226 passed，0 failed，10 ignored |
| `cargo test -p anyseek --lib` | 125 passed，0 failed，9 ignored |
| `cargo test -p anyseek --no-default-features --lib` | 124 passed，0 failed，9 ignored |
| `cargo test -p anyseek --doc` | 44 passed |
| `cargo build --features server` | 通过 |
| `python scripts/record_service_smoke.py --server target/debug/anyseek_server --browser` | 通过，26 个 PASS 检查点 |
| `cargo build --features server,fault-injection --bin anyseek_server` | 通过 |
| `python scripts/record_service_smoke.py --server target/debug/anyseek_server --browser --faults` | 通过，34 个 PASS 检查点 |
| `target/debug/anyseek_server generate_openapi=true` | 通过；anyseek Record service，28 paths |
| `python3 scripts/check_docs_brand.py`、`git diff --check` | 通过 |

相对原 35 suites 少一个空的服务库测试目标；原 226 个通过用例未减少。忽略项为九个由父测试调用的进程故障/取消辅助程序，加一个需要异构 AES 构建夹具的 `cross_build_same_index`；不将后者列为通过。无默认 feature 少的用例为 zh 专用测试。

独立只读审查未发现阻塞缺陷：核对模块/依赖门控、服务文件与资源内容、测试路径、Windows 功能和 CI 覆盖；建议的核心递归扫描已落实。

故障验证后已重新构建仅启用 `server` 的正常二进制，未将故障注入留在默认服务构建中。

## 验证限制

本批 Windows 原生运行、容器镜像构建尚未运行。主干 `452ff222` 的双平台 CI [39618351141](https://github.com/TheLudlows/anyseek/actions/runs/39618351141) 已通过，但不是本批证据。Linux 进程退出故障测试不能替代系统崩溃/断电；后者仍需隔离环境和外部确认见证，未验收。
