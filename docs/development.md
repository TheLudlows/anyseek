# anyseek 开发与验证

仅有一个 `anyseek` Cargo package：核心库位于 `anyseek/src/`，服务实现及 HTTP 契约位于 `anyseek/src/server/`，独立 Web 控制台位于 `console/`，使用 Node.js 24/React/TypeScript/Vite；Rust 服务不嵌入任何前端资源。默认关闭 `server` feature；嵌入库不引入 HTTP/OpenAPI/服务日志依赖。使用本仓库 path/git 依赖；本次改名不表示已向 crates.io 发布。实际远端为 `TheLudlows/anyseek`。

## 本地运行

API 与 UI 使用两个独立进程。按[快速开始](../README.md#快速开始)启动 API 后，在另一个终端从仓库根运行：

```sh
cd console
npm ci
npm run dev
```

默认 UI 为 `http://127.0.0.1:5173`，默认 API 为 `http://127.0.0.1:8080`。UI 启动前通过 `API_TARGET` 指定其他 API 地址；具体 Linux / PowerShell 命令、远程访问与生产反向代理见[控制台指南](../console/README.md)。

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
npm ci --prefix console
npm test --prefix console
npm run build --prefix console
```

核心默认启用 `zh`、`vb`。独立边界脚本验证核心可脱离 HTTP 依赖构建；不是新的 SDK 或模型运行时。`.cargo/config.toml` 使用本机 CPU 指令，不将这类产物跨不同 CPU runner 复用。

## 真实 HTTP 与浏览器

所有冒烟使用临时数据，不连接用户服务。独立控制台浏览器测试需 Node.js 24、Playwright 和 Chromium（见[控制台指南](../console/README.md)）：

```sh
(cd console && npx playwright install chromium && npm run test:browser)
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server
cargo build -p anyseek --features server,fault-injection
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server --faults
python3 scripts/s3_storage_smoke.py --server target/debug/anyseek_server --faults
```

Windows 使用 `target/debug/anyseek_server.exe`。fault-injection 仅用于测试；测试后重新 `cargo build --workspace --features server` 得到默认二进制。测试环境变量前缀为 `ANYSEEK_`（例如 `ANYSEEK_TEST_ROOT`）；正常服务主密钥仍为 `MASTER_KEY_SECRET`。

## S3 无缓存与合同验收

统一入口复用现有测试，不维护第二份业务语义。`semantic` 分为 base、locator、graph、temporal、upload、lease、history 七个隔离服务根；每次重启删除 `collections/*/*/*` 与 `*.object-cache` 两类缓存目录，保留 SQLite 协调元数据和 owner 锁文件。`storage` 覆盖 checkpoint、GC、共同备份、恢复中断；`contract` 运行显式 ignored 的对象合同测试，覆盖条件创建竞争、不可变重试、读回/HEAD、hash/长度、缺失/超限、旧 owner、共同备份原 cut 及删除。`all` 顺序运行三类。完整服务恢复使用第二个独立测试 bucket，真实后端需额外设置 `ANYSEEK_S3_TEST_RESTORE_BUCKET`；fixture 自动提供第二个 bucket。

```sh
cargo build -p anyseek --features server,fault-injection
python3 scripts/s3_acceptance.py --backend fixture --suite all \
  --server target/debug/anyseek_server --faults --report target/s3-acceptance.json
```

可用 `--suite semantic|storage|contract` 单独运行；合同单独运行不要求 `--server`，但需要 cargo。入口在运行前复制固定服务二进制，避免合同测试重新构建时替换正在验收的程序。报告记录二进制 SHA-256、每组退出状态和耗时，不记录凭据。进程退出或损坏对象的故障注入只在明确的测试构建启用。服务归档超过 HTTP 完成期限可能返回 `outcome_unknown`；脚本用**同一请求和 operation_id** 有界重试，不能通过新 ID 绕过原结果。

真实模式必须使用专用、可销毁的测试 bucket，并从进程环境提供以下配置；不会回退读取正常服务的 S3 配置。测试会创建和删除对象，端到端矩阵留下本轮测试对象，由测试 bucket 的管理者在验收后销毁；底层合同仅清理本轮显式记录的 key，不执行 bucket LIST 或批量前缀删除。

```sh
# 在当前进程环境设置 ANYSEEK_S3_TEST_ENDPOINT、ANYSEEK_S3_TEST_BUCKET、
# ANYSEEK_S3_TEST_REGION、ANYSEEK_S3_TEST_ACCESS_KEY_ID、
# ANYSEEK_S3_TEST_SECRET_ACCESS_KEY；HTTP 另需 ANYSEEK_S3_TEST_ALLOW_HTTP=1。
python3 scripts/s3_acceptance.py --backend real --suite all \
  --server target/debug/anyseek_server --faults --report target/s3-real-acceptance.json
```

报告必须同时核对 `backend=real`、`real_endpoint_exercised=true`、`all_requested_checks_passed=true` 和所运行 suite；fixture 通过、空配置拒绝或只运行合同均不代表完整真实后端认证。本轮按约定先交付工具与用例，未运行真实 endpoint。CI 的 `s3-native-acceptance` 在 Linux/Windows 使用 fixture 跑合同、权威回归及七组无缓存语义，原 native-storage 矩阵继续跑存储故障验收。

## API 与文档

```sh
target/debug/anyseek_server generate_openapi=true
```

生成文件位于可执行文件旁，通常为 `target/debug/openapi.json`、`openapi.yml`。HTTP DTO 和 schema 归可选 `anyseek::server` 模块；Rust 数据访问统一使用 RecordPartition，旧低层 Index API 已移除。参见 [服务指南](../anyseek/src/server/README.md) 和 [Rust 示例](library-examples.md)。

## 验证边界

Linux 与 Windows 都是原生可靠性验收平台；交叉编译不代替 Windows 原生运行。CI 包含真实服务 HTTP、子进程故障、恢复和浏览器流程，但不能代替系统崩溃/断电。后者需要可中断的隔离环境、持久设备和外部已确认写入见证，不自行重启共享环境。

记录每个最终源码版本的命令、结果、失败/忽略项和未运行项。目录或包名调整前的历史计数不可冒充改名后的验证。历史阶段结果通过 Git 历史及对应 PR/CI 查阅，当前测试命令与剩余验收须保持可复现。
