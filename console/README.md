# AnySeek Console

独立的中文开发者控制台，使用 React、TypeScript 与 Vite。前端源码、依赖和构建产物均位于 `console/`，不参与 Cargo 构建；Rust 服务只提供 HTTP API，不再托管首页或静态资源。

## 界面

以下为临时测试数据下的实际界面。

![集合管理](docs/collections.png)

![检索调试与原始记录预览](docs/search.png)

## 开发

需要 Node.js 24 和 npm。先按[服务指南](../anyseek/src/server/README.md)启动 API，并创建 Tenant 与普通 API Key。Master Key 不能用于集合访问。

```sh
cd console
npm ci
npm run dev
```

打开终端显示的地址（默认 `http://127.0.0.1:5173`），输入 Tenant API Key。开发服务器默认把 `/api` 转发至 `http://127.0.0.1:8080`。需要其他地址时，在启动前设置 `API_TARGET`；浏览器仍请求当前控制台域名，不需要修改内核 CORS。

```sh
# Linux / macOS
API_TARGET=http://127.0.0.1:9090 npm run dev
```

```powershell
# Windows PowerShell
$env:API_TARGET = 'http://127.0.0.1:9090'
npm run dev
```

## 构建与部署

```sh
npm run build
```

`dist/` 是可独立托管的静态产物。生产 Web 服务器必须将同源 `/api/` 代理到 AnySeek HTTP 服务，并保留 `apikey` 请求头；静态资源由前端服务器提供。API 服务的 `/` 返回 404 是预期行为。浏览器密钥、查询与数据不写入构建配置。

仓库提供独立的前端镜像；从仓库根运行：

```sh
docker build -f console/Dockerfile -t anyseek-console .
docker run --rm -p 8081:80 --network YOUR_NETWORK \
  -e API_UPSTREAM=http://YOUR_API_HOST:8080 anyseek-console
```

将网络及 API 地址替换为实际配置。生产对外入口应配置 HTTPS；前端和 API 通过代理保持同源。`API_UPSTREAM` 在前端容器启动时生效，不编译到浏览器代码。Nginx 示例见 [nginx.conf.template](nginx.conf.template)。现有仓库根 Dockerfile 仍只构建 API 镜像。

也可以用 `npm run preview` 本地检视 `dist/`（支持相同的 `API_TARGET`，用于预览，不作为生产服务器）。

## 使用

- **集合**：查看集合、记录数、检索模式与 Schema；创建 Lexical、Vector 或 Hybrid 集合。
- **数据**：游标分页、原始记录预览、精确 JSON 复制、文件或粘贴导入。Record envelope 支持条件更新、删除、向量补写与 generation 操作。
- **检索调试**：关键词、外部向量及混合检索；日期、时间和高级参数；查看逐命中得分与通道。向量须由调用方使用相同 embedding 模型生成。
- **高级检查**：实体目录、一跳关系、关系融合、来源证据、generation 与租约检查。请求 JSON 保留 API 的参数表达能力。
- **连接与管理**：切换 Tenant Key；使用单独输入且不持久保存的 Master Key 备份服务，或恢复到新的停止状态目录。

API Key 仅保存在当前标签页的 sessionStorage。写入与创建在发送前保留原始请求，刷新和网络中断后可以重试；待确认请求绑定连接指纹与原集合，不能从其他连接重放。**断开连接保留待确认请求**，用原 Key 和原集合继续处理。结果未知时不提供丢弃入口；明确拒绝后可修改。逐条导入不提供批次原子性，generation staging 与发布区分提示。

原始 JSON 数字 token 在写入、重试、响应预览、复制和游标续页时保持完整，避免 JavaScript 的 i64 精度损失。游标冲突应从第一页刷新，不能混接不同数据视图。

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
