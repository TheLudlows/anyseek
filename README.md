# AnySeek

**可靠、可更新的全文与向量检索引擎。** 保存原文、向量和元数据，通过 HTTP API 或独立控制台写入、检索、更新和删除。当前为有界单机实现，模型推理由调用方提供。

## 快速开始

需要 Rust 工具链；控制台另需 Node.js 24 和 npm。从仓库根目录执行：

```sh
cargo build --workspace
export MASTER_KEY_SECRET='replace-with-your-private-secret'
./target/debug/anyseek_server print_master_key=true
./target/debug/anyseek_server local_ip=127.0.0.1 local_port=8080
```

首次启动请将占位符替换为自己的私密值，保存输出的 Master Key；重启保持原 secret 和数据目录。Windows PowerShell 启动及完整配置见[服务指南](anyseek/src/server/README.md#构建与启动)。

保持 API 运行，在另一个终端启动控制台：

```sh
cd console
npm ci
npm run dev
```

打开终端显示的地址（默认 `http://127.0.0.1:5173`）。首次使用通过 Master Key 初始化 Tenant 并签发普通 Key；已有 Tenant API Key 可直接连接，然后创建集合、导入数据和检索。

[HTTP API 与示例](anyseek/src/server/README.md) · [控制台与部署](console/README.md) · [文档索引](docs/README.md)
