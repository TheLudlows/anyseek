# anyseek 开发与验证

仅有一个 `anyseek` Cargo package：核心库位于 `anyseek/src/`，服务实现及 HTTP 契约位于 `anyseek/server/`，Web UI 位于 `anyseek/web/`。默认关闭 `server` feature；嵌入库不引入 HTTP/OpenAPI/服务日志依赖。使用本仓库 path/git 依赖；本次改名不表示已向 crates.io 发布。实际远端为 `TheLudlows/anyseek`。

## 构建与测试

从仓库根运行，使用已安装 Rust 工具链：

```sh
cargo check -p anyseek
cargo check -p anyseek --no-default-features
cargo build --workspace --features server
cargo test --workspace --features server --all-targets
cargo test -p anyseek --lib
cargo test -p anyseek --no-default-features --lib
cargo test --workspace --doc
cargo doc --workspace --no-deps
python3 scripts/check_core_boundary.py
python3 scripts/check_docs_brand.py
```

核心默认启用 `zh`、`vb`。独立边界脚本验证核心可脱离 HTTP 依赖构建；不是新的 SDK 或模型运行时。`.cargo/config.toml` 使用本机 CPU 指令，不将这类产物跨不同 CPU runner 复用。

## 真实 HTTP 与浏览器

所有冒烟使用临时数据，不连接用户服务。浏览器测试需 Python Playwright 和 Chromium：

```sh
python3 -m pip install playwright
python3 -m playwright install chromium
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server --browser
cargo build -p anyseek --features server,fault-injection
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server --browser --faults
python3 scripts/s3_storage_smoke.py --server target/debug/anyseek_server --faults
```

Windows 使用 `target/debug/anyseek_server.exe`。fault-injection 仅用于测试；测试后重新 `cargo build --workspace --features server` 得到默认二进制。测试环境变量前缀为 `ANYSEEK_`（例如 `ANYSEEK_TEST_ROOT`、`ANYSEEK_BROWSER_TEST`）；正常服务主密钥仍为 `MASTER_KEY_SECRET`。

## API 与文档

```sh
target/debug/anyseek_server generate_openapi=true
```

生成文件位于可执行文件旁，通常为 `target/debug/openapi.json`、`openapi.yml`。HTTP DTO 和 schema 归可选 `anyseek::server` 模块；低层 Rust Index API 与默认 Record 服务的持久化契约不同。参见 [服务指南](../anyseek/server/README.md) 和 [Rust 示例](library-examples.md)。

## 验证边界

Linux 与 Windows 都是原生可靠性验收平台；交叉编译不代替 Windows 原生运行。CI 包含真实服务 HTTP、子进程故障、恢复和浏览器流程，但不能代替系统崩溃/断电。后者需要可中断的隔离环境、持久设备和外部已确认写入见证，不自行重启共享环境。

记录每个最终源码版本的命令、结果、失败/忽略项和未运行项。目录或包名调整前的历史计数不可冒充改名后的验证。历史阶段结果通过 Git 历史及对应 PR/CI 查阅，当前测试命令与剩余验收须保持可复现。
