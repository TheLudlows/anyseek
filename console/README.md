# AnySeek Web 控制台

**简体中文** · [English](README.en.md)

控制台通过同源网关访问 AnySeek，提供集合管理、记录写入、检索调试及服务备份恢复。
浏览器只选择租户命名空间，不签发或保存租户 Key，也不持有 AnySeek master key。
用户登录及权限由 Origence／网关处理；网关校验所选租户和管理操作权限后，向 AnySeek
注入服务凭据。AnySeek 不提供用户登录或租户 Key 管理接口。

## 本地启动 UI

API 构建与启动见[服务指南](../anyseek/src/server/README.md#构建与启动)。在 `console/` 运行：

```sh
npm ci
API_TARGET=http://your-authenticated-gateway npm run dev
```

打开终端显示的地址，输入租户命名空间并连接。集合列表通过 `GET /api/v1/index` 获取。
命名空间采用 `^[a-z0-9][a-z0-9_-]{0,62}$`（支持 UUID），不需要在 AnySeek 注册。

本机开发调试可直接连接绑定到本机的 AnySeek，由 **Vite 服务端**注入服务凭据：

```sh
export API_TARGET=http://127.0.0.1:8080
export ANYSEEK_SERVICE_KEY='<configured service API key>'
npm run dev
```

Windows PowerShell：

```powershell
$env:API_TARGET='http://127.0.0.1:8080'
$env:ANYSEEK_SERVICE_KEY='<configured service API key>'
npm run dev
```

`ANYSEEK_SERVICE_KEY` 是服务指南中的 `print_master_key=true` 输出；只保存在开发代理进程
环境，不写入前端构建或浏览器存储。开发服务器默认只监听 `127.0.0.1`；部署时使用认证网关，
不要把具有完整服务权限的开发代理作为公开入口。

## 构建与部署

```sh
npm run build
```

静态文件输出到 `dist/`。生产将页面及 `/api/` 放在同一认证网关后，保留登录会话；网关
验证或重写 `X-AnySeek-Tenant`，并使用自己的服务凭据连接 AnySeek。浏览器发送的租户头
只是命名空间选择，不能直接作为用户授权依据。服务备份恢复接口须由网关限制为管理员操作。

Docker 中设置 `API_UPSTREAM` 指向认证网关；该变量只用于 Nginx 运行时代理，不进入 JS。
Rust API 只提供数据接口，根路径 `/` 不托管控制台。Web Crypto 需要 HTTPS，或本机
`http://localhost` / `http://127.0.0.1`；连接指纹按来源和租户绑定。

## 重试和会话

租户选择保留在当前标签页的 sessionStorage，写入请求先保存原始操作 ID 和正文，再发送。
超时或未知结果重试原请求，不生成新 ID；切换租户不会把另一租户的待确认请求自动重放。
服务备份和恢复也保留原请求以供确认。升级时清除旧租户 Key 和旧签发流程的会话项，
保留独立的记录重试数据；旧连接的待确认写入不会自动绑定到新命名空间。

Embedding 提供方的模型凭据属于单独配置，与 AnySeek 服务认证分离，仍按原有会话与
显式保存规则处理。

## 验证

```sh
npm test
npm run build
npx playwright test --config=playwright.bootstrap.config.ts
ANYSEEK_SERVER=/absolute/path/to/anyseek_server npx playwright test
```

真实服务测试启动独立临时根目录，服务凭据只用于测试服务和测试代理，浏览器使用租户选择。
测试不访问用户正在运行的服务，也不使用外部 S3 bucket。
