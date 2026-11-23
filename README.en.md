# AnySeek

**English** · [简体中文](README.md)

**A reliable, updatable full-text and vector search engine.** Store original text, vectors, and metadata, and write, search, update, or delete them through the HTTP API or standalone console. The current implementation runs on one machine with bounded resources. The caller provides model inference.

## Quick start

The Rust toolchain is required. The console also requires Node.js 24 and npm. Run the following commands from the repository root.

```sh
cargo build --workspace
export MASTER_KEY_SECRET='replace-with-your-private-secret'
./target/debug/anyseek_server print_master_key=true
./target/debug/anyseek_server local_ip=127.0.0.1 local_port=8080
```

On first startup, replace the placeholder with your own secret and configure Origence or the gateway with the generated service credential. On restart, reuse the original data directory and keep the process and gateway credential settings consistent. See the [service guide](anyseek/src/server/README.en.md#build-and-start) for Windows PowerShell startup and the complete configuration.

Keep the API running and start the console in another terminal.

```sh
cd console
npm ci
export ANYSEEK_SERVICE_KEY='<print_master_key 的输出，仅本机开发代理使用>'
npm run dev
```

Open the address shown in the terminal (default `http://127.0.0.1:5173`). Select a tenant namespace, then create a collection, import data, and search. No tenant preregistration or key issuance is required. The browser holds no service credential. In production, Origence or the gateway handles authentication and authorization, and the gateway injects the service-to-service credential.

[HTTP API and examples](anyseek/src/server/README.en.md) · [Console and deployment](console/README.en.md) · [Documentation index](docs/README.en.md)

## Embedded Rust SDK

Depend on the `anyseek` library and open the local Catalog with `anyseek::sdk::LocalEngine`.
The caller supplies the authorized tenant and collection on each operation. Create a collection,
atomically replace Record groups with a stable operation ID, query its text or vectors, and await
`shutdown()` before reopening the directory. The SDK uses local disk and takes an exclusive root lock.

Use `default-features = false` when the host needs the standard `serde_json` object ordering.
The snapshot codec retains stored field order independently of the host JSON map.
The default standalone build enables `ordered-json`. A standalone build that disables default features
can select `--features ordered-json` to retain the same JSON ordering. Keep the selected build features
with the catalog version when reopening or restoring a catalog.

[Public SDK usage test](anyseek/tests/local_sdk.rs)
