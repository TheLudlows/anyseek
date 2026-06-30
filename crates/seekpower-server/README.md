# seekpower-server

SeekPower 的 HTTP 服务层。基于 axum 0.7 框架，将 `seekpower-core` 库 API 映射为 RESTful 端点。

## 快速开始

```bash
# 使用默认配置启动（内存存储，监听 127.0.0.1:3000）
cargo run -p seekpower-server

# 指定配置文件启动
cargo run -p seekpower-server -- path/to/seekpower.toml

# 或通过环境变量指定配置
SEEKPOWER_CONFIG=path/to/seekpower.toml cargo run -p seekpower-server
```

## 配置说明

服务启动时读取 TOML 配置文件（默认：工作目录下的 `seekpower.toml`）。如果文件不存在，则使用默认值。

```toml
[server]
addr = "127.0.0.1:3000"
log_level = "info"

[storage]
kind = "inmemory"   # 可选值: "inmemory" | "local"
path = "./data"
```

配置文件路径可通过第一个命令行参数或 `SEEKPOWER_CONFIG` 环境变量设置。

日志级别优先使用 `RUST_LOG` 环境变量，未设置时使用配置文件中的 `log_level` 字段。

## 架构设计

### 多索引支持

服务采用多索引架构，支持同时管理多个独立索引：

```rust
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use seekpower_core::Index;
use seekpower_storage::KvStore;

pub type SharedState = Arc<AppState>;

pub struct AppState {
    pub indexes: Mutex<HashMap<String, Arc<Index>>>,
    pub db: KvStore,
}
```

- `indexes`: 索引名称到 `Arc<Index>` 的映射，支持动态创建/删除索引
- `db`: 共享的 SlateDB 实例，每个索引通过 `for_index(path)` 前缀视图实现数据隔离

### 为什么使用 `spawn_blocking`

所有对 `seekpower-core` 的调用都在 `tokio::task::spawn_blocking` 中执行。原因：

1. axum 要求 handler 的 future 必须是 `Send`
2. `seekpower-core` 返回的 future 捕获了 `std::sync::RwLock` 守卫，不是 `Send`
3. 在阻塞线程池执行可恢复 `Send` 约束
4. 代价是需要为每次调用构建 current-thread runtime

## API 路由

| 方法 | 路径 | 请求体 | 成功响应 |
| --- | --- | --- | --- |
| `POST` | `/index/create` | `CreateIndexRequest` | `201` + `CreateIndexResponse` |
| `DELETE` | `/index/{path}` | — | `200` + `{"deleted": true, "name": "..."}` |
| `POST` | `/document` | `IndexDocumentRequest` | `201` + `IndexDocumentResponse` |
| `POST` | `/documents` | `IndexDocumentsRequest` | `201` + `IndexDocumentsResponse` |
| `GET` | `/document/{id}` | — | `200` + `GetDocumentResponse` / `404` |
| `DELETE` | `/document/{id}` | — | `200` + `{"deleted": true, "doc_id": ...}` |
| `POST` | `/commit` | — | `200` + `{"committed": true}` |
| `POST` | `/search` | `SearchRequestHttp` | `200` + `{"query", "result_count", "results"}` |

## 请求/响应格式

### 创建索引

```json
POST /index/create
{
  "name": "my_index",
  "schema": [
    {
      "field": "title",
      "field_type": "text",
      "store": true,
      "index_lexical": true,
      "boost": 2.0
    },
    {
      "field": "embedding",
      "field_type": "vector",
      "store": false,
      "index_vector": true
    }
  ],
  "config": {
    "tokenizer": "Unicode",
    "lowercase": true,
    "vector_dimensions": 768,
    "vector_similarity": "Cosine"
  },
  "object_store_kind": "inmemory"
}
```

响应：
```json
{
  "name": "my_index",
  "field_count": 2
}
```

### 删除索引

```
DELETE /index/my_index
```

响应：
```json
{
  "deleted": true,
  "name": "my_index"
}
```

### 索引单个文档

```json
POST /document
{
  "document": {
    "title": "Rust 编程指南",
    "body": "Rust 是一门系统编程语言...",
    "embedding": [0.1, 0.2, 0.3, ...]
  }
}
```

响应：
```json
{
  "doc_id": 42
}
```

### 批量索引文档

```json
POST /documents
{
  "documents": [
    {"title": "文档一", "body": "内容..."},
    {"title": "文档二", "body": "内容..."},
    {"title": "文档三", "body": "内容..."}
  ]
}
```

响应：
```json
{
  "doc_ids": [0, 1, 2]
}
```

### 获取文档

```
GET /document/42
```

响应：
```json
{
  "doc_id": 42,
  "document": {
    "title": "Rust 编程指南",
    "body": "Rust 是一门系统编程语言..."
  }
}
```

### 删除文档

```
DELETE /document/42
```

响应：
```json
{
  "deleted": true,
  "doc_id": 42
}
```

### 提交变更

```
POST /commit
```

响应：
```json
{
  "committed": true
}
```

### 搜索

```json
POST /search
{
  "query_string": "Rust 编程",
  "query_vector": null,
  "query_type": "union",
  "search_mode": "lexical",
  "offset": 0,
  "length": 10
}
```

**搜索模式 (`search_mode`)**:
- `"lexical"`: 词法搜索（BM25F 评分）
- `"vector"`: 向量搜索（ANN 近似最近邻）
- `"hybrid"`: 混合搜索（RRF 融合）

**查询类型 (`query_type`)**:
- `"union"`: OR 查询，匹配任意词
- `"intersection"` / `"and"`: AND 查询，匹配所有词
- `"phrase"`: 短语查询，要求词位置连续

**向量搜索参数 (`ann_mode`)**:
- `"all"`: 搜索所有聚类（精确搜索）
- `"nprobe:N"`: 搜索 top-N 个聚类（如 `"nprobe:10"`）
- `"threshold:T"`: 搜索相似度超过阈值的聚类（如 `"threshold:0.5"`）

**混合搜索权重**:
- `w_lexical`: 词法搜索权重（默认 1.0）
- `w_vector`: 向量搜索权重（默认 1.0）

响应：
```json
{
  "query": "Rust 编程",
  "result_count": 5,
  "results": [
    {
      "doc_id": 42,
      "score": 12.34,
      "result_type": "Lexical"
    },
    {
      "doc_id": 17,
      "score": 10.56,
      "result_type": "Lexical"
    }
  ]
}
```

## Schema 字段定义

`SchemaFieldSpec` 结构：

```rust
pub struct SchemaFieldSpec {
    pub field: String,              // 字段名
    pub field_type: Option<String>, // "text" | "vector"，默认 "text"
    pub store: Option<bool>,        // 是否存储原文，默认 true
    pub index_lexical: Option<bool>, // 是否建立词法索引
    pub index_vector: Option<bool>,  // 是否建立向量索引
    pub boost: Option<f32>,         // BM25F 字段权重，默认 1.0
}
```

## 存储后端

`object_store_kind` 字段选择存储后端：

- `"inmemory"`: 使用 `object_store::memory::InMemory`，数据仅存于内存
- `"local"`: 预留接口，当前回退到内存存储（本地文件系统支持待实现）

## 错误映射

| 错误类型 | HTTP 状态码 |
| --- | --- |
| `SeekPowerError::IndexNotFound(_)` | `404 NOT_FOUND` |
| `SeekPowerError::IndexNotLoaded(_)` | `404 NOT_FOUND` |
| `SeekPowerError::InvalidQuery(_)` | `400 BAD_REQUEST` |
| `SeekPowerError::SchemaError(_)` | `400 BAD_REQUEST` |
| 其他错误 | `500 INTERNAL_SERVER_ERROR` |
| `tokio::task::JoinError` | `500 INTERNAL_SERVER_ERROR` |

错误响应格式：
```json
{
  "error": "错误描述信息"
}
```

## 应用组装

```rust
use seekpower_server::create_app;
use seekpower_server::state::{SharedState, AppState};
use seekpower_storage::KvStore;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

// 创建共享的 KvStore
let object_store = Arc::new(object_store::memory::InMemory::new());
let db = KvStore::open("my_db", object_store).await.unwrap();

// 构建应用状态
let state = Arc::new(AppState {
    indexes: Mutex::new(HashMap::new()),
    db,
});

// 创建路由
let app = create_app(state);
```

`create_app(SharedState) -> axum::Router` 构建完整路由。测试时可使用 `tower::ServiceExt::oneshot` 发送请求而无需绑定真实端口——参见 `src/handler/query.rs` 和 `src/router.rs` 中的 `#[cfg(test)]` 示例。

## 源码结构

```
src/
├── main.rs          # 二进制入口：TOML 配置、tracing 初始化、axum::serve
├── lib.rs           # 模块声明，导出 create_app 和 SharedState
├── state.rs         # SharedState 类型定义和索引访问辅助函数
├── model.rs         # 请求/响应类型、ObjectStoreKind、SchemaFieldSpec、build_object_store()
├── error.rs         # HTTP 错误映射（map_error, map_join_err, map_io_err）
├── router.rs        # create_app() 路由注册
└── handler/         # axum handler，按领域划分：
    ├── schema.rs    # 索引生命周期：create / open / delete
    ├── write.rs     # 写操作：index documents / commit / delete document
    └── query.rs     # 读操作：get document / search
```

## 使用示例

### 完整流程

```bash
# 1. 启动服务
cargo run -p seekpower-server

# 2. 创建索引
curl -X POST http://localhost:3000/index/create \
  -H "Content-Type: application/json" \
  -d '{
    "name": "articles",
    "schema": [
      {"field": "title", "field_type": "text", "boost": 2.0},
      {"field": "body", "field_type": "text", "boost": 1.0}
    ]
  }'

# 3. 索引文档
curl -X POST http://localhost:3000/document \
  -H "Content-Type: application/json" \
  -d '{
    "document": {
      "title": "Rust 并发编程",
      "body": "Rust 提供了强大的并发原语..."
    }
  }'

# 4. 提交变更
curl -X POST http://localhost:3000/commit

# 5. 搜索
curl -X POST http://localhost:3000/search \
  -H "Content-Type: application/json" \
  -d '{
    "query_string": "Rust 并发",
    "search_mode": "lexical",
    "query_type": "union"
  }'

# 6. 删除索引
curl -X DELETE http://localhost:3000/index/articles
```

## 性能考虑

- **批量索引**: 使用 `/documents` 端点批量索引文档，减少 HTTP 开销
- **定期提交**: 调用 `/commit` 将内存缓冲刷新到 SlateDB，确保持久化
- **缓存配置**: 通过 `IndexConfig` 的 `posting_cache_size_mb` 和 `cluster_cache_size_mb` 调整缓存大小
- **向量搜索**: 使用 `nprobe` 参数控制搜索精度和延迟的权衡

## 后续计划

- [ ] 实现本地文件系统存储后端
- [ ] 添加索引统计信息端点
- [ ] 支持索引配置热更新
- [ ] 添加 Prometheus 监控指标
- [ ] 支持批量删除操作
- [ ] 实现索引备份/恢复功能
