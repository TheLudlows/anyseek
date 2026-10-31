# anyseek

**面向 AI 记忆与知识的通用存储和检索底座。** 将原始记录、来源证据、版本、时间与显式关系放在同一个可恢复、可检索的数据视图中，通过 Rust 核心库、HTTP API 和独立 Web 控制台使用。

anyseek 负责保存、校验、检索和恢复；模型推理、文档解析、切块、embedding、实体抽取及记忆反思/重算由上层应用完成。自定义内容保存在 `document`，引擎管理稳定身份和系统元数据。

[服务指南](anyseek/src/server/README.md) · [UI 启动与部署](console/README.md) · [功能与限制](FEATURES.md) · [当前架构](ARCHITECTURE.md) · [项目全貌与下一步](docs/project-overview.md) · [文档索引](docs/README.md)

## 当前能力

当前是 **有界单机实现**，P1-A 与已界定 P2 功能批次已经合入：

| 能力 | 当前行为 |
| --- | --- |
| 可信记录 | 稳定 Record ID、条件版本、防 ABA、原 operation_id 幂等重试；同步写入持久且可见，异步索引入口返回持久业务结果；未知结果明确区分 |
| 来源与证据 | 精确来源版本、内联文本/页码定位、直接及间接依赖；来源删除立即隐藏，修订后的旧派生内容 stale 默认隐藏 |
| 文档发布 | 有界多批 staging、租约和清理、完整 generation 原子发布；正文可先发布，向量按精确版本后补 |
| 共同检索 | 全文 BM25F、外部向量、元数据/时间过滤、显式一跳关系融合、稳定排序和逐命中解释 |
| 时间与生命周期 | 固定查询 evaluation_time、不可逆 TTL |
| 授权与恢复 | API Key 认证 Tenant，同租户 Key 访问一致；撤销拒绝后续认证，数据生命周期独立；WAL/checkpoint、备份与独立恢复 |

[独立控制台](console/README.md)支持连接、collection 创建、JSON/Record 写入与原请求重试、搜索和预览，以及来源、关系、租约等检查入口。完整请求与操作限制见 [服务指南](anyseek/src/server/README.md)。

## 边界与后续路线

- 当前每个 collection 一个持久 partition，不支持跨 collection 事务或来源依赖。
- 默认 Record 身份、保留操作各有 10,000 上限，可显式选有界 100,000 档；候选上限仍为 10,000；单操作 1 MiB，checkpoint 单帧 16 MiB。多批 generation 累计正文最多 4 MiB、100 成员，仍受整体预算限制；不承诺无限写入或稳定磁盘占用。
- ANN 与有界融合不等于全库精确召回；预算不足、未知写入、过期游标均有明确结果，不能将其当作完整成功。
- 独立备份不证明后续没有删除；恢复隔离与授权重新建立是独立条件，不提供强制绕过。保存 URL 不等于托管或备份外部文件。
- Linux/Windows 原生与进程故障测试不能替代系统崩溃或断电验收，后者仍未完成。各 SHA 的验证结论见 [当前状态](STATUS.md) 与阶段记录。
- 不可变 Segment、compaction、SQLite/S3 提交、远端 checkpoint、安全回收和共同备份已合入。维护路径审计与真实 S3 后端验收仍在推进；持续运行与容量验收见[后续路线](docs/roadmap.md)。
- 不提供旧磁盘格式升级或迁移，不运行模型，不自动解析或抽取知识。

## 快速开始

API 和 UI 分别启动。API 需要 Rust 工具链，UI 需要 Node.js 24 和 npm。

### 1. 启动 API

从 [TheLudlows/anyseek](https://github.com/TheLudlows/anyseek) 获取源码，在仓库根构建：

```sh
cargo build --workspace --features server
```

Linux / macOS，在第一个终端运行：

```sh
export MASTER_KEY_SECRET='replace-with-your-private-secret'
./target/debug/anyseek_server local_ip=127.0.0.1 local_port=8080
```

Windows PowerShell：

```powershell
$env:MASTER_KEY_SECRET = 'replace-with-your-private-secret'
.\target\debug\anyseek_server.exe local_ip=127.0.0.1 local_port=8080
```

访问 `http://127.0.0.1:8080/api/v1/live` 可查看服务版本。使用启动日志中的 `Master API key` 按[服务指南](anyseek/src/server/README.md)创建 Tenant 并签发普通 Tenant API Key，供 UI 连接使用。

默认数据目录为可执行文件旁的 `anyseek_index`；首次目录必须不存在。重启保留原目录和 `MASTER_KEY_SECRET`，已有部署应显式设置原 `index_path`。其他启动参数见[服务指南](anyseek/src/server/README.md)。

### 2. 启动 UI

保持 API 运行，在第二个终端从仓库根执行；以下命令适用于 Linux / macOS 和 Windows PowerShell：

```sh
cd console
npm ci
npm run dev
```

打开 `http://127.0.0.1:5173`（以终端显示的地址为准），输入 **Tenant API Key**，连接后即可创建集合、导入数据并调试检索。Master Key 不能用于集合连接。

UI 默认将 `/api` 转发至 `http://127.0.0.1:8080`。API 根地址 `/` 返回 404 是预期行为，浏览器页面由独立 UI 提供。其他 API 地址、远程访问及生产部署见 [UI 启动与部署](console/README.md)。

## API 与接入

HTTP 使用 `/api/v1`、`apikey` 请求头和 JSON。创建 collection 时声明 schema；写入携带 `operation_id`、`record_id`、固定 `routing_key`、`expected` 和 `document`。发送前保存完整请求，未知结果重试原身份，不生成新的操作。

- [HTTP 请求示例](anyseek/src/server/test_api.rest)与[服务契约](anyseek/src/server/README.md)：记录、generation、查询、授权、备份等完整流程。
- `POST /api/v1/index/{id}/query`：文本、向量或混合查询；空对象浏览可见记录。
- `target/debug/anyseek_server generate_openapi=true`：在可执行文件旁生成 OpenAPI JSON/YAML。
- [Rust 库接入](docs/development.md#rust-核心库接入)：从本仓库使用 path/git 依赖；统一使用 RecordPartition 访问数据；旧低层 Index API 已移除。

单 package 同时提供核心库与 `anyseek_server` 二进制；服务须显式启用 `server` feature，默认库依赖不包含 HTTP/OpenAPI。

## 开发与设计

```sh
cargo test --workspace --features server --all-targets
```

[开发与验证指南](docs/development.md)包含独立核心、rustdoc、HTTP/Chromium 和故障测试命令。[Record 语义](docs/record-model.md)与[存储协议](docs/storage.md)说明现行不变量；[后续路线](docs/roadmap.md)列出剩余工作。
