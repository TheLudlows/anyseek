# SeekPower

**Rust 对象存储原生混合搜索引擎** — BM25F 词法搜索 + IVF 向量搜索 + RRF 融合

基于自建 LSM 存储引擎，所有数据持久化到对象存储（本地文件系统 / S3 / 内存）。

---

## 核心特性

- **对象存储原生** — 所有数据直接存储在对象存储中（FS/S3/InMemory），自建 SST + WAL + Compaction，不依赖 SlateDB
- **混合搜索** — BM25F 词法搜索 + IVF 向量搜索 + 加权 RRF 融合
- **KV-原生 IVF** — 聚类即键值，medoid 常驻内存，cluster 按需加载
- **多语言分词** — 内置 jieba 中文分词 + Unicode 分词 + 可插拔自定义分词器
- **真实 BM25F 评分** — 使用实际词频 (tf) 和文档长度 (dl)
- **WAL 崩溃安全** — 写前日志保证未 commit 数据可恢复
- **HTTP 服务** — 内置 axum HTTP 服务器，提供 RESTful API
- **多索引隔离** — 目录级隔离，多索引共享同一对象存储实例

---

## 快速上手

### 作为库使用

```rust
use std::sync::Arc;
use seekpower_objectstore::InMemoryObjectStore;
use seekpower_core::SeekPower;
use seekpower_schema::{SchemaField, IndexConfig};
use seekpower_search::{SearchRequest, SearchMode, QueryType};

#[tokio::main]
async fn main() {
    // 创建对象存储后端
    let store = Arc::new(InMemoryObjectStore::new());

    // 打开引擎
    let engine = SeekPower::open(store).await.unwrap();

    // 创建索引
    let schema = vec![
        SchemaField::text("title").with_boost(2.0),
        SchemaField::text("body").with_store(true),
        SchemaField::vector("embedding", 768),
    ];
    let index = engine.create_index("articles", IndexConfig::default(), schema).await.unwrap();

    // 索引文档
    let mut doc = std::collections::HashMap::new();
    doc.insert("title".to_string(), serde_json::json!("Rust programming"));
    doc.insert("body".to_string(), serde_json::json!("Fast and safe systems language"));
    seekpower_core::document::index_document(&index, doc).await.unwrap();

    // 提交（WAL → SST → 对象存储）
    seekpower_core::commit::commit(&index).await.unwrap();

    // 词法搜索
    let request = SearchRequest {
        query_string: Some("programming".to_string()),
        search_mode: SearchMode::Lexical,
        query_type: QueryType::Union,
        ..Default::default()
    };
    let response = seekpower_core::search::search(&index, request).await.unwrap();
    println!("Found {} results", response.result_count);

    engine.close().await.unwrap();
}
```

### 使用本地文件系统存储

```rust
use seekpower_objectstore::FsObjectStore;
let store = Arc::new(FsObjectStore::new("./data").await.unwrap());
let engine = SeekPower::open(store).await?;
```

### 使用 S3 存储

```rust
use seekpower_objectstore::S3ObjectStore;
let store = Arc::new(S3ObjectStore::new(
    "https://s3.amazonaws.com",
    "my-bucket",
    "us-east-1",
    &access_key,
    &secret_key,
).await?);
let engine = SeekPower::open(store).await?;
```

### 通过 HTTP 服务使用

```bash
# 启动服务器（本地文件系统）
cargo run -p seekpower-server

# 搜索
curl -X POST http://localhost:3000/search \
  -d '{"query_string": "programming", "search_mode": "lexical"}'
```

---

## 架构概览

```
┌───────────────────────────────────────────────────────────┐
│                    SeekPower Library API                    │
├───────────────────────────────────────────────────────────┤
│  Index Operations: create / open / close / delete          │
│  Document Operations: index / delete / get                 │
│  Search Operations: lexical / vector / hybrid              │
│  Commit: WAL → SST → ObjectStore                          │
├───────────────────────────────────────────────────────────┤
│                                                            │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────────┐   │
│  │ Tokenizer│  │ Ranking  │  │ Hybrid Fusion (RRF)   │   │
│  └──────────┘  └──────────┘  └───────────────────────┘   │
│                                                            │
│  ┌──────────┐  ┌──────────┐                              │
│  │VectorIndex│  │ Posting  │                              │
│  │  (IVF)   │  │  Codec   │                              │
│  └─────┬────┘  └────┬─────┘                              │
│        │             │                                     │
│  ┌─────┴─────────────┴─────────────────────────────┐      │
│  │              seekpower-lsm                       │      │
│  │  SST Writer/Reader │ WAL Writer/Reader           │      │
│  │  Compaction │ IndexManifest │ Frame Envelope     │      │
│  └──────────────────┬──────────────────────────────┘      │
│                      │                                     │
│  ┌──────────────────┴──────────────────────────────┐      │
│  │         seekpower-objectstore                    │      │
│  │  ObjectStore trait (7 methods, 2 CAS 原语)       │      │
│  │  ┌──────────┐ ┌───────────┐ ┌──────────┐       │      │
│  │  │    FS    │ │    S3     │ │ InMemory │       │      │
│  │  │(rename)  │ │(If-Match) │ │ (HashMap)│       │      │
│  │  └──────────┘ └───────────┘ └──────────┘       │      │
│  │  CachingObjectStore │ MeteredObjectStore         │      │
│  └─────────────────────────────────────────────────┘      │
└───────────────────────────────────────────────────────────┘
```

---

## 文档

| 文档 | 说明 |
|---|---|
| [架构设计](docs/ARCHITECTURE.md) | 完整架构真相（对象存储 + LSM + 搜索引擎） |
| [使用指南](docs/guide.md) | 库 API + HTTP API 完整参考 |
| [决策日志](docs/PROGRESS.md) | 架构决策记录 + 里程碑 |
| [待办事项](docs/TODO.md) | 优先级待办 + 路线图 |
| [贡献指南](CONTRIBUTING.md) | 代码规范 + 贡献流程 |

---

## 构建

```bash
cargo build
cargo test

# 带 S3 后端支持
cargo build --features seekpower-objectstore/s3

# 带 jieba 中文分词
cargo build --features seekpower-tokenizer/jieba

# 运行 HTTP 服务器
cargo run -p seekpower-server
```

---

## 依赖

| 类别 | 依赖 |
|---|---|
| 对象存储 | seekpower-objectstore (自建: FS/S3/InMemory + Cache + Metered) |
| LSM 引擎 | seekpower-lsm (自建: SST + WAL + Compaction + Manifest) |
| 异步 | tokio |
| HTTP | axum 0.7 |
| 序列化 | serde, serde_json, postcard |
| 搜索 | ahash, indexmap, rayon, smallvec |
| NLP | jieba-rs (feature-gated), stop-words |
| 工具 | thiserror, bytemuck, itertools, sha2, crc32fast, bytes |

---

## License

MIT
