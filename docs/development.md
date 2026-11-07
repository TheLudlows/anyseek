# 开发与验证

AnySeek 是服务产品，仓库仅有一个 `anyseek` Cargo package；`lib.rs` 用于内部模块组织和集成测试复用，不提供独立库发行入口。存储与检索位于 `anyseek/src/`，HTTP 实现位于 `anyseek/src/server/`，独立 React/TypeScript/Vite 控制台位于 `console/`。仓库为 `TheLudlows/anyseek`。

## 本地运行

API 与 UI 使用两个独立进程。需要 Rust，前端另需 Node.js 24 与 npm。最短流程见[快速开始](../README.md#快速开始)，配置见[服务指南](../anyseek/src/server/README.md#构建与启动)和[控制台指南](../console/README.md#本地启动-ui)。

## 构建与测试

从仓库根执行：

```sh
cargo check -p anyseek
cargo check -p anyseek --no-default-features
cargo build --workspace
cargo test --workspace --all-targets
cargo test -p anyseek --no-default-features --lib --bins
cargo test --workspace --doc
cargo doc --workspace --no-deps
python3 scripts/check_console_boundary.py
python3 scripts/check_retired_dependencies.py
python3 scripts/check_lexical_partition.py --source-only
python3 scripts/test_s3_acceptance.py
npm ci --prefix console
npm test --prefix console
npm run build --prefix console
```

默认启用 `zh`（中文分词）与 `vb`（结果诊断）；`--no-default-features` 关闭二者，仍构建完整 HTTP 与本地/S3 服务。`fault-injection` 仅用于显式故障测试。`.cargo/config.toml` 使用本机 CPU 指令，这类产物不能跨不同 CPU runner 复用。

`ANYSEEK_TEST_SCALE` 缺省为 `small`，只允许 `small` 或 `full`；完整验收显式设为 `full`。本地默认 `cargo test` 不能代替 full 规模运行；数据规模与分工见下文[查询夹具规模](#查询夹具规模)。

## HTTP、浏览器与故障测试

冒烟使用临时数据，不连接用户服务。Windows 的 server 路径为 `target/debug/anyseek_server.exe`。浏览器测试需 Playwright 与 Chromium，安装和 bootstrap 命令见[控制台验证](../console/README.md#验证)。

```sh
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server
cargo build -p anyseek --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server --faults
python3 scripts/s3_storage_smoke.py --server target/debug/anyseek_server --faults
python3 scripts/ci_write_path_cases.py
```

写入定向入口按 `scripts/ci_write_path_cases.json` 编译 fault-injection 测试，并逐项在独立进程运行；覆盖可查增量、原生物化、WAL 前预留、额度转移、取消和冷恢复。缺失或改名用例会失败，不接受零匹配。小共享预算用例由入口设置进程环境，避免 LazyLock 配置被前一个测试污染。

生产 panic 行为需要真实 release 子进程检查，不能用 `cargo test --release` 替代：

```sh
cargo build -p anyseek --locked --release --features fault-injection --example runtime_panic_probe
python3 scripts/release_panic_smoke.py --probe target/release/examples/runtime_panic_probe
```

探针覆盖向量消费者部分修改后 panic、队列发布前 panic，以及发布后回执交付前 panic，并以原 Operation ID 重开查账/重试。它们是本地进程故障检查，不证明 S3、所有发布边界或硬断电恢复。Linux 的真实信号检查另用 `scripts/record_shutdown_smoke.py`；关闭契约见[架构](architecture.md#服务关闭)。

测试环境变量前缀为 `ANYSEEK_`（如 `ANYSEEK_TEST_ROOT`）；正常主密钥仍为 `MASTER_KEY_SECRET`。完成故障测试后重新 `cargo build --workspace` 得到默认服务二进制。

## S3 无缓存与合同验收

统一入口复用现有测试。`semantic` 分为 base、temporal、history、ingest 四个隔离服务根；`--faults` 另加 indexer、authorization。每次重启删除 `collections/*/*/*` 与 `*.object-cache` 缓存目录，保留 SQLite 协调元数据和 owner 锁。`storage` 覆盖 checkpoint、GC、共同备份与恢复中断；`contract` 运行显式 ignored 的不可变对象合同；`all` 顺序运行三类。

```sh
cargo build -p anyseek --features fault-injection
python3 scripts/s3_acceptance.py --backend fixture --suite all \
  --server target/debug/anyseek_server --faults --report target/s3-acceptance.json
```

可单选 `--suite semantic|storage|contract`；合同不要求 server，但需要 cargo。入口运行前复制固定二进制，避免合同编译替换被验收程序。报告记录二进制 SHA-256、退出状态与耗时，不记录凭据。备份等操作超过 HTTP 完成期限可能返回 outcome_unknown，必须以同一请求与 operation_id 有界重试。

真实模式只接受专用、可销毁的测试 bucket 与当前进程环境中的测试配置，不回退正常服务 S3 配置。完整服务恢复另需 `ANYSEEK_S3_TEST_RESTORE_BUCKET`；fixture 自动提供第二个 bucket。

```sh
# 当前进程设置 ANYSEEK_S3_TEST_ENDPOINT、ANYSEEK_S3_TEST_BUCKET、
# ANYSEEK_S3_TEST_REGION、ANYSEEK_S3_TEST_ACCESS_KEY_ID、
# ANYSEEK_S3_TEST_SECRET_ACCESS_KEY；HTTP 另需 ANYSEEK_S3_TEST_ALLOW_HTTP=1。
python3 scripts/s3_acceptance.py --backend real --suite all \
  --server target/debug/anyseek_server --faults --report target/s3-real-acceptance.json
```

测试创建和删除对象，端到端矩阵留下本轮对象，由测试 bucket 管理者验收后销毁；底层合同只清理显式记录的 key，不执行 LIST 或批量前缀删除。合同覆盖条件创建竞争、不可变重试、读回/HEAD、hash/长度、缺失/超限、旧 owner、原 cut 备份及删除。

报告须同时核对 `backend=real`、`real_endpoint_exercised=true`、`all_requested_checks_passed=true` 和 suite。fixture 通过、空配置拒绝或只跑合同都不代表完整真实后端认证；当前缺口统一见[路线](roadmap.md)。

## CI 快速反馈

以下分工按当前 [.github/workflows/ci.yml](../.github/workflows/ci.yml) 核对。`PR feedback` 在 PR、master push 与手工触发时运行：

| 通道 | 当前检查 |
| --- | --- |
| static（所有变更） | S3 验收入口单元检查、全文测试源分区检查 |
| static（代码变更） | 控制台构建/单测、前后端边界、退役依赖检查 |
| core（Linux/Windows） | 精确 core 清单、五个删除/排名集成 target、同步与异步 HTTP、故障 fence/关闭、服务配置与 OpenAPI、local/S3-fixture 旧代次回收 |
| lexical（Linux/Windows） | 独立运行 lexical_correctness，小规模成员与评分 oracle |
| write-path（Linux/Windows） | 精确 fault-injection 写入清单，每项隔离进程 |

Linux core 另跑真实 SIGTERM 与独立浏览器套件（含 bootstrap）。core 清单位于 `scripts/ci_core_cases.json`，写入清单位于 `scripts/ci_write_path_cases.json`；运行器都校验精确用例存在且执行成功。

文档变更跳过编译矩阵、控制台构建、边界和依赖图检查，保留 static 的两项源检查；Markdown 文件链接与锚点需另外检查。快速通道使用通用 x86-64 CPU flags 以便同 OS 缓存产物，不能替代原生完整验收。

## 完整验收

手工对固定候选 ref 运行：

```sh
gh workflow run ci.yml --ref YOUR_CANDIDATE_REF -F full=true
```

也可直接触发 `Full acceptance (manual)`。该命令会执行选中提交的 [.github/workflows/full-acceptance.yml](../.github/workflows/full-acceptance.yml)；查看配置不需要触发运行。使用结果前核对精确 head SHA，后续提交须重新验收。手工运行互不取消，也不由普通 PR feedback 取消。

完整工作流使用 `ANYSEEK_TEST_SCALE=full`、原生 CPU 配置与仅源码缓存，共九个展开后的 job：

| 覆盖范围 | 所属 job |
| --- | --- |
| Linux 默认 feature 全部模块与集成测试、正常 HTTP/浏览器、release panic、OpenAPI | build-and-smoke |
| Linux/Windows 原生持久化、fault-injection 库/服务、no-default 服务与默认/无默认 doctest | persistence-native × 2 |
| Windows fault-injection 集成 | persistence-native 的剩余集 + windows-lexical 四个独立用例 |
| Windows 正常 HTTP；两平台 fault HTTP、Indexer/授权、存储故障与恢复 | persistence-native |
| 两平台 S3 fixture 对象合同与六组无缓存语义 | s3-native-acceptance × 2 |

S3 remote-checkpoint 单测由同 feature 的 persistence 矩阵覆盖，fixture 对象合同与 HTTP 语义仍由 S3 job 负责。Linux 默认集成由 all-targets 负责，fault 库/二进制由 persistence 负责；这不意味着不同 feature 可视为等同。

快速清单不运行以下两个 2,305 次更新的长恢复用例，完整 all-targets 与 fault 库测试继续覆盖：

- `record::cache_merge::tests::repeated_updates_beyond_sixteen_tails_recover_from_original_wal`
- `record::cache_merge::remote_tests::remote_long_suffix_merges_caches_without_replacing_anchor_and_cold_recovers`

### 查询夹具规模

下表仅描述检索夹具；生产容量、超时、故障与发布边界不随 small/full 改变。

| 夹具 | small | full | 主要断言 |
| --- | ---: | ---: | --- |
| mixed / five-term，两种字段长度策略 | 各 514 | 各 8194 | 精确成员、独立 BM25 oracle、Top1/10/100；mixed 覆盖即时/checkpoint/reopen 与查询词序 |
| frequent terms | 256 | 3000 | 100%/50% 频率、交集、两种 Top10 与精确计数 |
| 高低分 union | 160 | 1000 | 40 个高分记录、12 个低分组、完整 oracle 与 Top100 |
| 稀疏 uniform union | 200 | 2000 | 20 组、超过 100 候选、堆与分数边界 |
| forty-term union | 120 | 1000 | 40 词、精确总数、Top100 填满 |
| disjoint union | 760，重复 2 次 | 760，重复 20 次 | 首查/重复 Count 与 TopkCount 一致 |
| dense deletion AND/OR | 2048 | 2048 | 四组 AND/OR Count/TopkCount |

576/522 的追加/晚到命中和 400/160 的排名夹具保持固定规模。65,536、10,001、>16 MiB、64→65、128→129、8192-package 等独立边界不由小规模夹具替代。Windows 全文分区由 `scripts/check_lexical_partition.py` 校验；改变用例名或分区时须同步清单、矩阵及 remainder 排除项。

## API 与内部模块

```sh
target/debug/anyseek_server generate_openapi=true
```

生成文件位于可执行文件旁，通常为 `target/debug/openapi.json`、`openapi.yml`。HTTP DTO/schema 在 `anyseek::server`，对外契约以[服务指南](../anyseek/src/server/README.md)为准。

### 服务内部模块

[RecordPartition](../anyseek/src/record.rs) 负责记录持久化和检索，[异步索引实现](../anyseek/src/record/committed.rs) 负责持久业务结果与消费进度。`lib.rs` 供服务二进制与集成测试复用；内部类型与方法可用 `cargo doc -p anyseek --no-deps` 查看。语义与重试见[Record 契约](record-model.md)，所有权与故障见[架构](architecture.md)。

## 验证边界

Linux 与 Windows 都是原生可靠性验收平台，交叉编译不能代替 Windows 原生执行。存储、格式或恢复变更需要固定候选 SHA 的完整矩阵；快速 CI 通过不能消除未运行或失败的完整检查。

真实 S3 endpoint、系统/文件系统崩溃、硬断电、跨 AES 和长期持续运行须分别取证。进程 kill/exit、fixture、fsync 调用和编译通过均不能替代这些验收。断电验证需要隔离环境、持久设备及外部确认写入见证，不重启共享环境。

记录最终源码的命令、结果、失败/忽略项与未运行项。历史计数、不同版本测量和 PR 记录不冒充当前重测；待验收工作和证据入口统一维护在[路线](roadmap.md)。
