# StormDB

> 基于 SlateDB 的混合搜索引擎，结合词法搜索(BM25F)和向量搜索(IVF)，使用 RRF 融合。

## 特性

- **混合搜索**: BM25F 词法搜索 + IVF 向量搜索 + RRF 融合
- **云原生存储**: 所有数据持久化到 SlateDB，支持对象存储后端
- **实时索引**: Level-0 缓冲区提供即时可用性
- **多种查询**: Union、Intersection、Phrase 搜索
- **自适应压缩**: Posting lists 使用 Array/Bitmap/RLE 自适应压缩

## 架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         StormDB Index                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
│  │  词法搜索(BM25F) │  │  向量搜索(IVF)   │  │   RRF 融合    │ │
│  └──────────────────┘  └──────────────────┘  └──────────────┘ │
│                                                                 │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  │
│  │ Posting Lists  │  │ Vector Clusters│  │   Facets       │  │
│  │  (从SlateDB加载)│  │  (从SlateDB加载)│  │   (无索引)    │  │
│  └────────────────┘  └────────────────┘  └────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SlateDB (KV Storage)                       │
│              所有持久化数据的唯一存储层                           │
├─────────────────────────────────────────────────────────────────┤
│  idx:  全文索引 (压缩的 Posting Lists)                           │
│  vec:  向量索引 (IVF 聚类结构)                                   │
│  doc:  文档原文 (压缩的 JSON)                                    │
│  facet: Facet 数据 (按文档存储，无索引)                          │
│  dell: 删除标记                                                  │
│  meta: 元数据                                                    │
│  len:  文档长度 (BM25 压缩值)                                     │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Object Storage Backend                        │
│              (Local FS / S3 / GCS / Azure Blob)                 │
└─────────────────────────────────────────────────────────────────┘
```

## 快速开始

### 安装

```bash
cargo build --release
```

### 基础用法

```rust
use storm_db::index::{create_index, commit, search};
use storm_db::document::index_document;
use storm_db::schema::{SchemaField, IndexConfig, SearchMode};
use object_store::memory::InMemory;
use std::sync::Arc;

#[tokio::main]
async fn main() -> storm_db::error::Result<()> {
    let schema = vec![
        SchemaField::text("title"),
        SchemaField::text("body"),
        SchemaField::vector("embedding"),
    ];

    let store = Arc::new(InMemory::new());
    let index = create_index(
        "/path/to/index",
        store,
        IndexConfig::default(),
        schema,
    ).await?;

    // 索引文档
    let mut doc = std::collections::HashMap::new();
    doc.insert("title".to_string(), serde_json::json!("Rust 编程"));
    doc.insert("body".to_string(), serde_json::json!("系统编程语言"));
    index_document(&index, doc)?;

    // 提交持久化
    commit(&index).await?;

    // 混合搜索
    let response = search(&index, storm_db::search::SearchRequest {
        query_string: Some("编程".to_string()),
        query_vector: Some(vec![/* query embedding */]),
        search_mode: SearchMode::Hybrid {
            ann_mode: storm_db::search::AnnMode::All,
            similarity_threshold: 0.5,
        },
        ..Default::default()
    }).await?;

    Ok(())
}
```

## 测试

```bash
cargo test --lib
```

## 文档

- [架构设计](docs/ARCHITECTURE.md) - 详细的技术架构和实现规范
- [开发指南](agent.md) - 开发者指南

## 许可证

MIT OR Apache-2.0

## 致谢

- 灵感来自 [SeekStorm](https://github.com/seekstorm/seekstorm)
- 基于 [SlateDB](https://github.com/slatedb-io/slatedb) 构建