# anyseek 开发与验证

AnySeek 是服务产品，仓库仅有一个 `anyseek` Cargo package。存储和检索模块位于 `anyseek/src/`，HTTP 实现位于 `anyseek/src/server/`；`lib.rs` 用于内部模块组织和集成测试复用。独立 Web 控制台位于 `console/`，使用 Node.js 24/React/TypeScript/Vite，Rust 服务不嵌入前端资源。默认构建包含 HTTP、OpenAPI、服务日志及本地/S3 存储，不提供独立库发行入口。仓库为 `TheLudlows/anyseek`。

## 本地运行

API 与 UI 使用两个独立进程。最短流程见[快速开始](../README.md#快速开始)，完整配置见[服务指南](../anyseek/src/server/README.md#构建与启动)和[控制台指南](../console/README.md#本地启动-ui)。

## 构建与测试

从仓库根运行，使用已安装 Rust 工具链：

```sh
cargo check -p anyseek
cargo check -p anyseek --no-default-features
cargo build --workspace
cargo test --workspace --all-targets
cargo test -p anyseek --no-default-features --lib --bins
cargo test --workspace --doc
cargo doc --workspace --no-deps
python3 scripts/check_console_boundary.py
npm ci --prefix console
npm test --prefix console
npm run build --prefix console
```

仓库根运行 `cargo build` 构建服务；`cargo run -- <name=value...>` 启动服务；`cargo test` 包含服务与模块的单元测试、集成测试和 doctest。默认启用 `zh`（中文分词）、`vb`（结果诊断），`--no-default-features` 关闭这两项，仍构建完整服务。`fault-injection` 仅用于显式故障测试构建。前端边界脚本检查静态资源未嵌入 Rust 服务。`.cargo/config.toml` 使用本机 CPU 指令，不将这类产物跨不同 CPU runner 复用。

## 真实 HTTP 与浏览器

所有冒烟使用临时数据，不连接用户服务。独立控制台浏览器测试需 Node.js 24、Playwright 和 Chromium（见[控制台指南](../console/README.md)）：

```sh
(cd console && npx playwright install chromium && npm run test:browser)
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server
cargo build -p anyseek --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server --faults
python3 scripts/s3_storage_smoke.py --server target/debug/anyseek_server --faults
```

Windows 使用 `target/debug/anyseek_server.exe`。fault-injection 仅用于测试；测试后重新 `cargo build --workspace ` 得到默认二进制。测试环境变量前缀为 `ANYSEEK_`（例如 `ANYSEEK_TEST_ROOT`）；正常服务主密钥仍为 `MASTER_KEY_SECRET`。

## S3 无缓存与合同验收

统一入口复用现有测试，不维护第二份业务语义。`semantic` 分为 base、temporal、history、ingest 四个隔离服务根；启用 `--faults` 时另运行 indexer、authorization 两个要求故障注入的分组；每次重启删除 `collections/*/*/*` 与 `*.object-cache` 两类缓存目录，保留 SQLite 协调元数据和 owner 锁文件。`storage` 覆盖 checkpoint、GC、共同备份、恢复中断；`contract` 运行显式 ignored 的对象合同测试，覆盖条件创建竞争、不可变重试、读回/HEAD、hash/长度、缺失/超限、旧 owner、共同备份原 cut 及删除。`all` 顺序运行三类。完整服务恢复使用第二个独立测试 bucket，真实后端需额外设置 `ANYSEEK_S3_TEST_RESTORE_BUCKET`；fixture 自动提供第二个 bucket。

```sh
cargo build -p anyseek --features fault-injection
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

报告必须同时核对 `backend=real`、`real_endpoint_exercised=true`、`all_requested_checks_passed=true` 和所运行 suite；fixture 通过、空配置拒绝或只运行合同均不代表完整真实后端认证。本轮按约定先交付工具与用例，未运行真实 endpoint。CI 的 `s3-native-acceptance` 在 Linux/Windows 使用 fixture 跑对象合同及六组无缓存语义（base、temporal、history、ingest、indexer、authorization）；同 feature 的权威库回归和存储故障验收归 persistence-native 矩阵。

## API 与文档

```sh
target/debug/anyseek_server generate_openapi=true
```

生成文件位于可执行文件旁，通常为 `target/debug/openapi.json`、`openapi.yml`。HTTP DTO 和 schema 位于 `anyseek::server` 模块，数据请求由内部 RecordPartition 处理。外部调用通过 HTTP API，参见 [服务指南](../anyseek/src/server/README.md)。

## 验证边界

Linux 与 Windows 都是原生可靠性验收平台；交叉编译不代替 Windows 原生运行。CI 包含真实服务 HTTP、子进程故障、恢复和浏览器流程，但不能代替系统崩溃/断电。后者需要可中断的隔离环境、持久设备和外部已确认写入见证，不自行重启共享环境。

记录每个最终源码版本的命令、结果、失败/忽略项和未运行项。目录或包名调整前的历史计数不可冒充改名后的验证。历史阶段结果通过 Git 历史及对应 PR/CI 查阅，当前测试命令与剩余验收须保持可复现。

## 服务内部模块

[RecordPartition](../anyseek/src/record.rs) 负责记录持久化和检索，[异步索引实现](../anyseek/src/record/committed.rs) 负责持久业务结果与索引进度。`lib.rs` 让服务二进制和集成测试复用这些模块。`cargo doc -p anyseek --no-deps` 用于查看内部类型和方法；服务对外契约以 HTTP API 为准，语义和重试边界见 [Record 契约](record-model.md)。
