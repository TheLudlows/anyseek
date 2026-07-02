# seekpower-core

SeekPower 的核心聚合 crate。管理内存中的 `Index` 结构、写缓冲区、文档 ID 计数器、删除集合和读缓存。

该 crate 重新导出所有底层 crate 的公共 API，调用者可以通过 `use seekpower_core::*;` 一次性访问 schema、posting codec、评分器、向量索引、搜索类型和错误类型。

完整的设计规范请参阅项目根目录的 `readme.md`。本文档是使用指南，而非完整规范。

## 核心特性

- **混合搜索**: BM25F 词法搜索 + IVF 向量搜索 + RRF 融合
- **多索引支持**: 多个索引共享同一个 SlateDB 实例，通过前缀隔离
- **近实时搜索**: 写缓冲区 + 双缓冲提交，搜索可同时查询未提交数据
- **字段级评分**: 支持多字段索引，每个字段可配置独立的 BM25F boost 权重
- **文档压缩**: 支持 Snappy/LZ4/Zstd 压缩存储文档原文

## 索引生命周期

### 创建索引

```rust
use std::sync::Arc;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use seekpower_core::{
    create_index, IndexConfig, SchemaField,
};

// 打开共享的 SlateDB 实例
let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
let shared_db = seekpower_storage::KvStore::open("my_db", store).await?;

// 定义 schema
let schema = vec![
    SchemaField::text("title").with_boost(2.0),          // 标题字段，权重 2.0
    SchemaField::text("body").with_boost(1.0),           // 正文字段，权重 1.0
    SchemaField::vector("embedding", 768),               // 向量字段，768 维
];

// 配置索引
let config = IndexConfig {
    vector_dimensions: 768,
    ..Default::default()
};

// 创建索引
let index = create_index(&shared_db, "articles", config, schema).await?;
```

`create_index(shared_db, name, config, schema_fields)` 会在 SlateDB 中写入以下元数据：
- `m:schema` - 字段定义
- `m:config` - 索引配置
- `m:stats` - 索引统计信息
- `m:fields` - 字段顺序（用于 field_id 映射）

### 打开已有索引

```rust
use seekpower_core::open_index;

let index = open_index(&shared_db, "articles").await?;
```

`open_index` 会从 SlateDB 读取 schema/config/stats，重建内存中的删除集合（扫描 `l:` 前缀）。

### 关闭索引

```rust
use seekpower_core::close_index;

close_index(&index).await?;
```

注意：关闭底层 `KvStore` 实例。

### 删除索引

```rust
use seekpower_core::delete_index;

// 物理删除索引的所有数据
delete_index(&shared_db, "articles").await?;
```

## 文档操作

所有文档操作函数都是异步的，接受 `&Index` 参数。

### 索引文档

```rust
use seekpower_core::index_document;
use std::collections::HashMap;

let mut doc = HashMap::new();
doc.insert("title".to_string(), serde_json::json!("Rust 编程指南"));
doc.insert("body".to_string(), serde_json::json!("Rust 是一门系统编程语言..."));
doc.insert("embedding".to_string(), serde_json::json!([0.1, 0.2, 0.3, ...]));

let doc_id = index_document(&index, doc).await?;
```

`index_document` 的工作流程：
1. 通过 `AtomicU64::fetch_add` 分配单调递增的 `doc_id`
2. 根据 schema 对每个字段进行分词
3. 计算每个 term 的 `key_hash = ahash(field_id.to_le_bytes() ++ term_bytes)`
4. 将 posting entries、文档长度、压缩后的文档字节累积到 `WriteBuffer`
5. 如果有向量字段，将向量添加到 `VectorIndex` 的缓冲区

### 获取文档

```rust
use seekpower_core::get_document;

let doc = get_document(&index, doc_id).await?;
// doc: Option<HashMap<String, serde_json::Value>>
```

`get_document` 先检查缓冲区，再检查 SlateDB。读取时自动解压缩。文档不存在时返回 `Ok(None)`。

### 删除文档

```rust
use seekpower_core::delete_document;

delete_document(&index, doc_id).await?;
```

`delete_document` 将 doc_id 插入缓冲区的删除集合和内存中的 `delete_set`，用于查询时的快速过滤。实际删除标记在 `commit` 时持久化到 SlateDB。

## 提交流程

`commit(&Index) -> Result<()>` 实现了双缓冲模式：

```rust
use seekpower_core::commit;

commit(&index).await?;
```

提交流程：

1. **交换缓冲区**: 使用 `std::mem::take` 将当前 `WriteBuffer` 替换为空缓冲区，新写入不会阻塞
2. **写入删除标记**: 在 `l:` 前缀下写入删除标记
3. **合并 Posting List**: 对每个 term，执行 read-modify-write：
   - 从 SlateDB 读取现有 posting list
   - 使用 `merge_postings` 合并新 entries
   - 重新编码并写回
4. **写入文档长度**: 在 `s:` 前缀下写入文档长度（u8 压缩值）
5. **写入文档原文**: 在 `d:` 前缀下写入压缩后的文档
6. **刷新向量索引**: 调用 `VectorIndex::flush()` 将新向量合并到 IVF 索引
7. **更新统计信息**: 更新 `IndexStats`（doc_count、total_doc_length、avg_doc_length）并持久化到 `m:stats`

## 搜索

### 词法搜索（BM25F）

```rust
use seekpower_core::{search, SearchRequest, SearchMode, QueryType};

let request = SearchRequest {
    query_string: Some("Rust 编程".to_string()),
    search_mode: SearchMode::Lexical,
    query_type: QueryType::Union,  // OR 查询
    offset: 0,
    length: 10,
    ..Default::default()
};

let response = search(&index, request).await?;
```

**查询类型**:
- `QueryType::Union` - OR 查询，匹配任意 term，分数累加
- `QueryType::Intersection` - AND 查询，匹配所有 term
- `QueryType::Phrase` - 短语查询，要求 term 位置连续

**BM25F 评分公式**:
```
score = Σ_f boost_f × IDF(term) × (tf_f × (K + 1)) / (tf_f + K × (1 - B + B × dl_f / avgdl_f))
```

其中：
- `boost_f` - 字段权重（schema 中定义）
- `tf_f` - term 在字段 f 中的词频
- `dl_f` - 字段 f 的文档长度
- `avgdl_f` - 字段 f 的平均文档长度
- `IDF = ln((N - df + 0.5) / (df + 0.5) + 1)`
- `K = 1.2`, `B = 0.75`（默认参数）

### 向量搜索

```rust
use seekpower_core::{search, SearchRequest, SearchMode, AnnMode};

let request = SearchRequest {
    query_vector: Some(vec![0.1, 0.2, 0.3, ...]),
    search_mode: SearchMode::Vector {
        ann_mode: AnnMode::Nprobe(10),  // 搜索 top-10 聚类
    },
    offset: 0,
    length: 10,
    ..Default::default()
};

let response = search(&index, request).await?;
```

**ANN 模式**:
- `AnnMode::All` - 搜索所有聚类（精确搜索）
- `AnnMode::Nprobe(n)` - 搜索 top-n 个最相似的聚类
- `AnnMode::SimilarityThreshold(t)` - 搜索相似度超过阈值的聚类

### 混合搜索

```rust
use seekpower_core::{search, SearchRequest, SearchMode, AnnMode};

let request = SearchRequest {
    query_string: Some("Rust 编程".to_string()),
    query_vector: Some(vec![0.1, 0.2, 0.3, ...]),
    search_mode: SearchMode::Hybrid {
        ann_mode: AnnMode::Nprobe(10),
        w_lexical: 1.0,  // 词法搜索权重
        w_vector: 1.0,   // 向量搜索权重
    },
    offset: 0,
    length: 10,
    ..Default::default()
};

let response = search(&index, request).await?;
```

混合搜索使用 **RRF（Reciprocal Rank Fusion）** 融合词法和向量搜索结果：

```
score_hybrid = w_lexical / (K + rank_lexical) + w_vector / (K + rank_vector)
```

其中 `K = 60`（默认值），`rank` 为 1-based 排名。

## 内部类型

### Index 结构

```rust
pub struct Index {
    pub db: KvStore,                                    // 共享 SlateDB 实例
    pub index_id: u32,                                  // 索引前缀 ID（name 哈希）
    pub name: String,                                   // 索引名称
    pub schema: SchemaMap,                              // 字段定义映射
    pub field_names: Vec<String>,                       // 字段顺序（index = field_id）
    pub config: IndexConfig,                            // 索引配置
    pub stats: tokio::sync::RwLock<IndexStats>,         // 索引统计信息
    pub next_doc_id: std::sync::atomic::AtomicU64,      // 文档 ID 计数器
    pub lexical_index: LexicalIndex,                    // 词法索引
    pub vector_index: IvfIndex,                         // 向量索引
    pub query_engine: QueryEngine,                      // 查询引擎
    pub delete_set: tokio::sync::RwLock<AHashSet<u64>>, // 删除集合
}
```

### WriteBuffer 结构

```rust
pub struct WriteBuffer {
    pub postings: AHashMap<u64, Vec<PostingEntry>>,     // term_hash → posting entries
    pub documents: AHashMap<u64, Vec<u8>>,              // doc_id → 压缩文档数据
    pub doc_lengths: AHashMap<u64, Vec<(u16, u16)>>,    // doc_id → [(field_id, length)]
    pub deletions: AHashSet<u64>,                       // 待删除的 doc_id
}
```

## 并发模型

- `buffer: tokio::sync::RwLock<WriteBuffer>` — 在 `index_document`/`delete_document`/`commit` 交换步骤时写锁定，在搜索时读锁定
- `delete_set: tokio::sync::RwLock<AHashSet<u64>>` — 在 `delete_document` 和 `commit` 时写锁定，在每个 `search` 开始时读锁定并克隆
- `stats: tokio::sync::RwLock<IndexStats>` — 由 `commit` 写入
- `next_doc_id: AtomicU64` — 无锁原子操作

所有搜索同时查询未提交缓冲区和 SlateDB，这使得设计接近实时，无需每次写入都刷新。

## KV 存储布局

索引数据在 SlateDB 中的物理键格式：`data_type (1B) ‖ index_id (4B BE) ‖ business_key`

通过 `KvStore` 的 `prefixed_*` 方法访问时，`data_type` + `index_id` 前缀自动附加/剥离，调用方只需使用逻辑业务键：

| data_type | 业务键 | 值类型 |
|-----------|--------|--------|
| `TYPE_META (0)` | `schema` | Schema JSON |
| `TYPE_META (0)` | `config` | IndexConfig JSON |
| `TYPE_META (0)` | `stats` | IndexStats JSON |
| `TYPE_META (0)` | `fields` | Vec<String> JSON |
| `TYPE_IDX (2)` | `term_hash (u64 LE)` | PostingListBlock (binary) |
| `TYPE_VEC (3)` | `meta` | IvfMeta JSON |
| `TYPE_VEC (3)` | `medoid:{id}` | MedoidData (binary) |
| `TYPE_VEC (3)` | `cluster:{id}` | ClusterData (binary) |
| `TYPE_DOC (1)` | `doc_id (u64 LE)` | 压缩文档 JSON |
| `TYPE_DELL (4)` | `doc_id (u64 LE)` | 删除标记 (tombstone) |
| `TYPE_LEN (5)` | `doc_id (u64 LE) ‖ field_id (u16 LE)` | 文档长度 (u8) |

`term_hash` 计算方式：
```rust
term_hash = ahash(field_id.to_le_bytes() ++ term_bytes)
```

将字段信息编码进 hash，使同一 term 在不同字段中拥有不同的 posting list。

## 重导出

```rust
// 错误类型
pub use seekpower_error::Result as SeekResult;

// Posting 编解码
pub use seekpower_posting::{PostingEntry, PostingListBlock};

// 评分工具
pub use seekpower_ranking::{compress_doc_length, decompress_doc_length};

// Schema 类型
pub use seekpower_schema::{
    ClusteringConfig, DocCompression, FieldType, IndexConfig, IndexStats,
    QuantType, SchemaField, SchemaMap, Similarity, TokenizerType,
};

// 搜索类型
pub use seekpower_search::*;

// 向量索引类型
pub use seekpower_vector::{AnnMode, IvfMeta, MedoidData, VectorIndex, VectorSearchResult};

// 版本信息
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
```

## 完整示例

```rust
use std::sync::Arc;
use std::collections::HashMap;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use seekpower_core::*;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. 打开共享数据库
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let shared_db = seekpower_storage::KvStore::open("my_db", store).await?;

    // 2. 创建索引
    let schema = vec![
        SchemaField::text("title").with_boost(2.0),
        SchemaField::text("body"),
    ];
    let config = IndexConfig::default();
    let index = create_index(&shared_db, "articles", config, schema).await?;

    // 3. 索引文档
    let mut doc1 = HashMap::new();
    doc1.insert("title".to_string(), serde_json::json!("Rust 编程指南"));
    doc1.insert("body".to_string(), serde_json::json!("Rust 是一门系统编程语言..."));
    let doc_id = index_document(&index, doc1).await?;

    // 4. 提交变更
    commit(&index).await?;

    // 5. 搜索
    let request = SearchRequest {
        query_string: Some("Rust".to_string()),
        search_mode: SearchMode::Lexical,
        query_type: QueryType::Union,
        ..Default::default()
    };
    let response = search(&index, request).await?;
    println!("找到 {} 条结果", response.result_count);

    // 6. 获取文档
    let doc = get_document(&index, doc_id).await?;
    println!("文档: {:?}", doc);

    // 7. 删除文档
    delete_document(&index, doc_id).await?;
    commit(&index).await?;

    // 8. 删除索引
    delete_index(&shared_db, "articles").await?;

    Ok(())
}
```

## 性能考虑

- **批量索引**: 多次调用 `index_document` 后统一 `commit`，减少 read-modify-write 次数
- **字段权重**: 合理设置 `boost` 值，标题等关键字段可设置较高权重
- **向量搜索**: 使用 `AnnMode::Nprobe` 控制搜索精度和延迟的权衡

## 测试

该 crate 包含完整的集成测试，覆盖：
- 索引创建和关闭
- 文档索引、获取、删除
- Commit 持久化
- 词法搜索（Union/Intersection/Phrase）
- 字段权重评分
- 未提交数据搜索
- 删除标记过滤
- 多字段搜索
- 索引重新打开

运行测试：
```bash
cargo test -p seekpower-core
```

## 源码结构

完整的 API 表面和集成测试请参阅 `src/lib.rs`。
