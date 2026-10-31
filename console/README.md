# AnySeek Console

独立的中文开发者控制台，使用 React、TypeScript 与 Vite。源码与构建产物位于 `console/`，不参与 Cargo 构建；Rust 服务只提供 HTTP API。

## 本地启动 UI

### 1. 启动 API

API 需要保持运行。完整的 Linux / macOS、Windows PowerShell 启动命令见仓库[快速开始](../README.md#快速开始)，Tenant 和普通 API Key 的签发流程见[服务指南](../anyseek/src/server/README.md#构建与启动)。

默认 API 地址为 `http://127.0.0.1:8080`，访问 `/api/v1/live` 应返回服务版本。**UI 的集合连接使用 Tenant API Key，不能使用 Master Key。**

### 2. 启动前端

安装 Node.js 24 和 npm，在第二个终端从仓库根执行。以下命令适用于 Linux / macOS 和 Windows PowerShell：

```sh
cd console
npm ci
npm run dev
```

API 已运行时只需执行本步骤。`npm ci` 用于首次安装或锁文件更新后同步依赖，日常启动运行 `npm run dev` 即可。

### 3. 打开界面

打开终端显示的地址，默认是 `http://127.0.0.1:5173`。已有 Tenant API Key 时，输入 Key 并点击“连接服务”。首次启动只有 Master Key 时，点击“首次使用？使用 Master Key 初始化”，填写新 Tenant 名称和 Master Key，确认创建并签发普通 Key。复制保存生成的普通 Key，再确认进入工作区，即可创建集合、导入 JSON 并调试检索。

初始化只复用现有 Tenant 与 Key 接口，Master Key 仅在内存中使用。初始化请求和生成的普通 Key 保留在当前标签页的 sessionStorage；失败、丢响应、返回或刷新后，从同一入口重试原请求，不生成新身份或密钥。刷新后需重新输入 Master Key。重试仅在当前或前一 UTC 日的 operation_id 有效期内；期间不要关闭标签页、清除会话或切换 API 服务。返回不会撤销 Tenant 或 Key。已有 Tenant 冲突不会当作成功，请使用管理员已签发的普通 Key 连接；本入口不管理已有 Tenant 的权限或密钥。

| 地址 | 用途 |
| --- | --- |
| `http://127.0.0.1:5173` | UI 页面，由 Vite 提供 |
| `http://127.0.0.1:8080/api/v1/live` | API 存活检查，返回服务版本 |
| `http://127.0.0.1:8080/` | API 根路径返回 404，不提供 UI |

端口被占用时，Vite 可能选择下一个端口，以终端输出为准。Ctrl-C 停止当前终端中的进程，API 和 UI 分别停止。

## 配置 API 地址

浏览器始终请求当前 UI 域名下的 `/api`，由前端服务器转发到 AnySeek。API 地址由启动前端服务器的环境配置决定，不在浏览器填写，不需要修改内核 CORS。

| 场景 | 配置项 | 默认值 | 生效时机 |
| --- | --- | --- | --- |
| `npm run dev` / `npm run preview` | `API_TARGET` | `http://127.0.0.1:8080` | 启动 Vite 时 |
| 前端 Docker 镜像 | `API_UPSTREAM` | `http://anyseek:8080` | 启动容器时 |
| 自行托管静态产物 | Web 服务器的 `/api/` 反向代理 | 由部署者配置 | 加载服务器配置时 |

填写 API 服务的基础地址，**不要追加 `/api` 或 `/api/v1`**。修改环境变量后重启对应前端进程。

例如 API 运行在 `http://127.0.0.1:9090`，在 `console/` 目录启动：

Linux / macOS：

```sh
API_TARGET=http://127.0.0.1:9090 npm run dev
```

Windows PowerShell：

```powershell
$env:API_TARGET = 'http://127.0.0.1:9090'
npm run dev
```

Windows CMD：

```bat
set API_TARGET=http://127.0.0.1:9090
npm run dev
```

`API_TARGET` 只供 Vite 代理使用，`npm run build` 不会把它变成生产 API 配置；生产需单独配置反向代理。

## 从其他电脑访问开发 UI

默认只监听 `127.0.0.1`。需要从其他电脑访问时，在 `console/` 目录运行：

```sh
npm run dev -- --host 0.0.0.0
```

浏览器访问 `http://前端主机IP:5173`，实际端口以终端为准。如果 API 也在该主机的 8080 端口，仍可使用默认代理；如果 API 在其他主机，按上一节设置 `API_TARGET`。这个地址应当能被**运行 UI 的主机**访问。

Vite 用于开发调试，生产使用下面的静态构建和 Web 服务器部署。

## 构建与部署

在 `console/` 目录运行：

```sh
npm ci
npm run build
```

将 `console/dist/` 部署到静态 Web 服务器，并把同源 `/api/` 代理到 AnySeek HTTP 服务，保留 `apikey` 请求头。前端与 API 可独立部署，但浏览器通过同一个对外入口访问。

仓库提供独立的前端镜像。从仓库根运行，将网络与 API 地址替换为实际配置：

```sh
docker build -f console/Dockerfile -t anyseek-console .
docker run --rm -p 8081:80 --network YOUR_NETWORK \
  -e API_UPSTREAM=http://YOUR_API_HOST:8080 anyseek-console
```

浏览器打开 `http://127.0.0.1:8081`。`API_UPSTREAM` 必须是前端容器能访问的 API 地址；容器内的 `127.0.0.1` 指向前端容器自身。该变量在启动时配置代理，不写入浏览器产物。生产对外入口配置 HTTPS。Nginx 配置见 [nginx.conf.template](nginx.conf.template)，仓库根 Dockerfile 仍只构建 API 镜像。

本地检查构建产物可运行 `npm run preview`，默认打开 `http://127.0.0.1:4173`，支持相同的 `API_TARGET`；预览服务器不用于生产。

## 常见启动问题

| 现象 | 检查与处理 |
| --- | --- |
| 打开 8080 根地址得到 404 | 这是 API 端口；打开 UI 终端显示的地址，默认 5173 |
| 页面能打开，连接请求失败 | 确认 API 已启动；检查 Vite 终端中的代理错误及 `API_TARGET`，不要追加 `/api/v1` |
| 提示 Key 无效或无权访问集合 | 使用管理员签发的 Tenant API Key；Master Key 只用于管理操作 |
| 别的电脑打不开 UI | 用 `--host 0.0.0.0` 启动开发服务器，并访问前端主机 IP |

## 使用

- **集合**：查看集合、记录数、检索模式与 Schema；创建 Lexical、Vector 或 Hybrid 集合。
- **数据**：游标分页、原始记录预览、精确 JSON 复制、文件或粘贴导入。Record envelope 支持条件更新、删除、向量补写与 generation 操作。
- **检索调试**：关键词、外部向量及混合检索；日期、时间和高级参数；查看逐命中得分与通道。向量由调用方使用相同 embedding 模型生成。
- **高级检查**：实体目录、一跳关系、关系融合、来源证据、generation 与租约检查。
- **连接与管理**：切换 Tenant Key；单独输入 Master Key 备份服务或恢复到新的停止状态目录，Master Key 不持久保存。

Tenant API Key 保存在当前标签页的 sessionStorage，Master Key 不保存。创建与写入在发送前保留原始请求，刷新或网络中断后可以重试；待确认请求绑定连接指纹与原集合。**断开连接保留待确认请求**，用原 Key 和原集合继续处理。结果未知时不提供丢弃入口，明确拒绝后可修改。逐条导入不提供批次原子性，generation staging 与发布区分提示。

写入和重试保持输入数字 token；预览、复制及游标续页保持服务响应中的数字 token，避免 JavaScript 的 i64 精度损失。游标冲突时从第一页刷新。

## 验证

从仓库根先构建服务，再运行前端验证：

```sh
cargo build --features server --bin anyseek_server
cd console
npm ci
npm test
npm run build
npx playwright install chromium
npm run test:browser
```

浏览器测试会启动独立临时数据根的真实 API（127.0.0.1:18180）和 Vite（127.0.0.1:4173），不连接用户服务，也不使用配置中的 S3 bucket。请保持这两个端口空闲。可通过 `ANYSEEK_SERVER` 指定服务二进制绝对路径，通过 `CHROMIUM_PATH` 指定已安装的 Chromium。

测试覆盖内核静态路由清理、连接与集合创建、创建回包丢失、i64 导入/回包丢失/刷新重试/精确复制/条件编辑、三种真实检索、空态、高级入口及移动端。截图位于 `test-results/screenshots/`；失败时保留 trace。

首次初始化的隔离浏览器测试使用 mock API，不需要启动 Rust 服务：

```sh
npx playwright test --config playwright.bootstrap.config.ts
```

覆盖正常初始化、Tenant/Key 丢响应与刷新重试、连接失败重试、普通 Key 连接、取消返回、重复点击与 Tenant 冲突；不创建真实部署凭据。

## 界面

以下为临时测试数据下的实际界面。

![集合管理](docs/collections.png)

![检索调试与原始记录预览](docs/search.png)
