# SeekPower 混合搜索引擎设计文档

## 1. 项目概述

### 1.1 定位

SeekPower 是一个 Rust 实现的混合搜索引擎库，结合词法搜索 (BM25F) 和向量搜索 (IVF)，使用 Reciprocal Rank Fusion (RRF) 融合结果。面向**嵌入式集成**和**企业知识库/RAG** 场景，支持千万级文档规模。

### 1.2 核心特性

- **混合搜索**: BM25F 词法搜索 + IVF 向量搜索 + 加权 RRF 融合
- **LSM-Tree 原生存储**: 所有数据持久化到 SlateDB，利用其 compaction 机制管理数据布局
- **KV-原生 IVF**: 聚类即键值，medoid 常驻内存，cluster 按需加载
- **多语言分词**: 内置 jieba 中文分词 + Unicode 分词 + 可插拔自定义分词器
- **真实 BM25F 评分**: 使用实际词频 (tf) 和文档长度 (dl)，非硬编码值
- **并发安全**: tokio::RwLock + 双缓冲，commit 期间不丢数据
- **千万级规模**: 按需加载 + LRU 缓存，内存占用可控
- **云原生存储后端**: SlateDB + object_store，支持 Local FS / S3 / GCS / Azure Blob
- **文档存储分离**: 索引仅保存 doc_id，文档原文按需存储

### 1.3 设计目标

| 目标 | 度量 |
|---|---|
| 索引吞吐量 | > 10,000 docs/sec (批量) |
| 词法搜索延迟 | < 50ms (P99, 1000万文档) |
| 向量搜索延迟 | < 100ms (P99, nprobe=10, 1000万向量) |
| 内存占用 | < 1GB (1000万文档，不含文档原文缓存) |
| 召回率 | 向量搜索 recall@10 > 95% (nprobe=10) |

---

## 2. 整体架构

### 2.1 架构图

```
┌──────────────────────────────────────────────────────────────┐
│                     SeekPower Library API                     │
├──────────────────────────────────────────────────────────────┤
│  Index Operations: create / open / close / drop              │
│  Document Operations: index / delete / get                   │
│  Search Operations: lexical / vector / hybrid                │
│  Commit: flush buffer → SlateDB                              │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐   │
│  │  Tokenizer  │  │  Ranking     │  │  Hybrid Fusion    │   │
│  │  (trait)    │  │  (BM25F)     │  │  (RRF / weighted) │   │
│  └─────────────┘  └──────────────┘  └───────────────────┘   │
│                                                               │
│  ┌─────────────┐  ┌──────────────┐                          │
│  │  VectorIndex│  │  PostingList │                          │
│  │  (trait)    │  │  Codec       │                          │
│  └──────┬──────┘  └──────┬───────┘                          │
│         │                │                                    │
│  ┌──────┴────────────────┴───────┐                           │
│  │        Storage Layer          │                           │
│  │   (Read-through Cache +       │                           │
│  │    SlateDB KV Layout)         │                           │
│  └──────────────┬────────────────┘                           │
│                 │                                             │
├─────────────────┼─────────────────────────────────────────────┤
│           SlateDB (LSM-Tree KV)                              │
│  所有索引数据和文档原文通过 KV 接口持久化                       │
│  依赖 SlateDB 内部 compaction 管理数据布局                    │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 Workspace & Crate 划分

所有模块通过独立 crate 划分，确保职责单一、依赖方向清晰、可独立编译测试。

```
seekpower/
├── Cargo.toml                  # workspace 根
├── crates/
│   ├── seekpower-core/         # Index 主结构、生命周期、API 入口
│   ├── seekpower-schema/       # Schema 定义、字段类型、索引配置
│   ├── seekpower-tokenizer/    # Tokenizer trait + 内置实现 + 注册表
│   ├── seekpower-posting/      # Posting list 编解码、查询迭代
│   ├── seekpower-ranking/      # BM25F 评分、WAND 优化、MinHeap
│   ├── seekpower-vector/       # VectorIndex trait + IVF 实现
│   ├── seekpower-clustering/   # 聚类算法 trait + K-Medoid 实现
│   ├── seekpower-quantize/     # 向量量化 trait + SQ-I8 实现
│   ├── seekpower-search/       # 查询引擎 (Union/Intersection/Phrase) + 混合搜索
│   ├── seekpower-storage/      # SlateDB KV 封装 + ReadCache
│   └── seekpower-error/        # 错误类型 (所有 crate 共享)
└── crates/seekpower-server/    # HTTP 服务 (独立 crate，后续实现)
```

### 2.3 Crate 详细说明

| Crate | 职责 | 关键 trait/结构 | 依赖 |
|---|---|---|---|
| `seekpower-error` | 全局错误类型 | `SeekPowerError`, `Result<T>` | thiserror |
| `seekpower-schema` | Schema、字段类型、配置 | `SchemaField`, `FieldType`, `IndexConfig` | seekpower-error, serde |
| `seekpower-storage` | SlateDB KV 封装 + LRU 缓存 | `KvStore`, `ReadCache` | seekpower-error, slatedb, object_store |
| `seekpower-tokenizer` | 分词器 trait + 内置实现 | `Tokenizer` trait, `JiebaTokenizer`, `UnicodeTokenizer`, `TokenizerRegistry` | seekpower-error, jieba-rs |
| `seekpower-posting` | Posting list 编解码 | `PostingCodec`, `PostingIterator`, `PostingListBlock` | seekpower-error, seekpower-schema |
| `seekpower-ranking` | BM25F 评分、WAND、MinHeap | `Scorer`, `MinHeap`, `SearchResult` | seekpower-error, seekpower-schema |
| `seekpower-quantize` | 向量量化 | `Quantizer` trait, `ScalarQuantI8` | seekpower-error |
| `seekpower-clustering` | 聚类算法 | `Clusterer` trait, `KMedoidClusterer` | seekpower-error, seekpower-quantize |
| `seekpower-vector` | 向量索引 trait + IVF | `VectorIndex` trait, `IvfIndex` | seekpower-error, seekpower-schema, seekpower-storage, seekpower-clustering, seekpower-quantize |
| `seekpower-search` | 查询引擎 + 混合搜索 | `QueryEngine`, `FusionStrategy`, `RrfFusion` | seekpower-error, seekpower-schema, seekpower-posting, seekpower-ranking, seekpower-vector, seekpower-tokenizer |
| `seekpower-core` | Index 主结构、生命周期、API | `Index`, `create_index`, `open_index`, `search`, `commit` | 所有上述 crate |
| `seekpower-server` | HTTP 服务 (后续) | axum handlers | seekpower-core, axum |

### 2.4 Crate 依赖关系图

```
                    seekpower-core
                         │
          ┌──────────────┼──────────────┐
          │              │              │
    seekpower-search  seekpower-storage  seekpower-tokenizer
          │              │
    ┌─────┼─────┐        │
    │     │     │        │
 posting ranking vector  │
    │     │     │        │
    │     │  ┌──┼──┐     │
    │     │  │  │  │     │
 schema  │ clustering quantize
    │     │     │        │
    └─────┴─────┴────────┘
              │
         seekpower-error
```

**依赖规则**:
- 依赖方向只能从上往下，禁止循环依赖
- `seekpower-error` 是最底层 crate，无依赖
- `seekpower-schema` 仅依赖 `seekpower-error`
- `seekpower-core` 依赖所有 crate，是最顶层聚合 crate
- `seekpower-server` 独立，仅依赖 `seekpower-core`

### 2.5 Crate 化的设计优势

1. **编译隔离**: 修改一个 crate 只需重编译该 crate 及其下游，开发迭代更快
2. **职责边界清晰**: 每个 crate 有独立的 pub API，内部实现可自由重构
3. **独立测试**: 每个 crate 可独立运行 `cargo test`，CI 可并行
4. **可选依赖**: 如 `seekpower-tokenizer` 的 jieba 可作为 feature gate，不需要中文分词时可不编译
5. **版本独立**: 未来可以独立发布 crate 到 crates.io

### 2.6 架构决策

| 决策 | 选择 | 理由 |
|---|---|---|
| 模块组织 | Workspace 多 crate | 职责单一、编译隔离、独立测试、可选依赖 |
| 存储架构 | LSM-Tree 原生 (利用 SlateDB compaction) | 不重复发明轮子，减少应用层复杂度 |
| 向量索引 | IVF 为主 + VectorIndex trait | 适合 KV 存储和增量写入，trait 预留扩展 |
| 分词 | 多语言 + 可插拔 Tokenizer trait | 中文 RAG 需要 jieba，但保留灵活性 |
| 并发模型 | tokio::RwLock + 双缓冲 | 异步友好，commit 安全 |
| 服务层 | 库优先，HTTP 后续 | 核心设计不被服务器需求影响 |
| 目标规模 | 千万级 | 按需加载 + LRU 缓存 |

---

## 3. 存储层设计

### 3.1 SlateDB KV 布局

所有数据持久化到 SlateDB，通过键前缀分区。

**物理存储格式**: `{index_name}:{逻辑键}`，其中 `index_name:` 由 `KvStore::for_index(path)` 自动添加。

**逻辑键格式** (索引内部视角，不含索引前缀):

```
┌──────────┬──────────────────────────────────┬──────────────────────────────┐
│ 前缀     │ 逻辑键格式                         │ 值类型                        │
├──────────┼──────────────────────────────────┼──────────────────────────────┤
│ m:       │ m:schema                         │ Schema JSON                  │
│ m:       │ m:config                         │ IndexConfig JSON             │
│ m:       │ m:stats                          │ 索引统计 JSON                 │
├──────────┼──────────────────────────────────┼──────────────────────────────┤
│ i:       │ i:{term_hash}                    │ PostingListBlock (binary)    │
├──────────┼──────────────────────────────────┼──────────────────────────────┤
│ v:       │ v:meta                           │ IvfMeta JSON                 │
│ v:       │ v:medoid:{id}                    │ MedoidData (f32 binary)      │
│ v:       │ v:cluster:{id}                   │ ClusterData (binary)         │
├──────────┼──────────────────────────────────┼──────────────────────────────┤
│ d:       │ d:{doc_id}                       │ 压缩文档 JSON                 │
├──────────┼──────────────────────────────────┼──────────────────────────────┤
│ l:       │ l:{doc_id}                       │ 删除标记 (tombstone)         │
└──────────┴──────────────────────────────────┴──────────────────────────────┘
```

**term_hash 计算**:

```
term_hash = ahash(field_id(u16).to_le_bytes() ++ term_bytes)
```

将字段信息编码进 hash，使同一 term 在不同字段中拥有不同的 posting list。

### 3.2 多索引隔离

`KvStore::for_index(path)` 为每个索引创建独立视图，所有键自动添加 `{path}:` 前缀。多个索引共享同一个 SlateDB 实例，数据完全隔离。每个索引拥有独立的 schema，索引内所有文档共享同一 schema：

```
物理存储示例:
  blog:m:schema          → SchemaA JSON
  blog:i:{term_hash}     → PostingListBlock
  blog:d:{doc_id}        → 文档原文
  shop:m:schema          → SchemaB JSON
  shop:i:{term_hash}     → PostingListBlock
  shop:d:{doc_id}        → 文档原文
```

```rust
let shared_db = KvStore::open("my_db", object_store).await?;
let blog = shared_db.for_index("blog");  // 键前缀: "blog:"
let shop = shared_db.for_index("shop");  // 键前缀: "shop:"

blog.put(b"m:schema", schema_a).await?;  // 实际存储: "blog:m:schema"
shop.put(b"m:schema", schema_b).await?;  // 实际存储: "shop:m:schema"
```

**关键设计**: 文档键 (`d:` / `l:` / `s:`) 中不包含 schema 标识。一个索引内的所有文档隐式属于该索引的 schema，不存在同一索引下多 schema 文档的机制。

### 3.3 文档 ID 方案

```
doc_id: u64 全局单调递增

分配方式: AtomicU64::fetch_add(1, Ordering::SeqCst)
优势:
  - 全局唯一，无需 level/local 拆分
  - 单调递增有利于 delta + varint 编码
  - 理论容量 2^64，无上限约束
```

### 3.4 写缓冲区 (WriteBuffer)

索引文档时不直接写入 SlateDB，先写入内存缓冲区，commit 时批量合并。

```
WriteBuffer {
    // term_hash → Vec<PostingEntry>
    postings: AHashMap<u64, Vec<PostingEntry>>,
    // doc_id → 压缩文档数据
    documents: AHashMap<u64, Vec<u8>>,
    // 待删除 doc_id
    deletions: AHashSet<u64>,
}
```

### 3.5 Commit 流程

```
1. 获取 WriteBuffer 的 write lock
2. std::mem::take 交换数据 (原子交换，新写入进入空 buffer)
3. 释放 write lock

4. 并行处理旧数据:
   ├─ 词法索引:
   │   for each term in buffer.postings:
   │     a. 从 SlateDB 读取现有 posting list (如果存在)
   │     b. 解码 → 合并新 posting (含 doc_length) → 重新编码
   │     c. 写回 SlateDB
   │
   ├─ 向量索引:
   │   a. 新向量分配到最近 medoid 聚类 (或触发重新聚类)
   │   b. 读取目标 cluster → 追加向量 → 写回
   │   c. 更新 IvfMeta
   │
   ├─ 文档原文:
   │   for each doc in buffer.documents:
   │     压缩 → 写入 d: 前缀
   │
   └─ 删除标记:
       for each id in buffer.deletions:
         写入 l: 前缀

5. 更新索引统计 (m:stats)
```

---

## 4. 词法搜索

### 4.1 Posting List 编码

```
PostingList 存储格式 (binary):
┌──────────────────────────────────────────┐
│ Header (固定 16 字节)                     │
│ ├─ posting_count: u32                    │
│ ├─ max_score: f32 (WAND 优化)            │
│ ├─ compression_type: u8 (1=Array,2=Bitmap,3=RLE) │
│ └─ block_count: u8                       │
├──────────────────────────────────────────┤
│ Blocks[]                                 │
│ 每个 Block:                               │
│ ├─ max_block_score: f32                  │
│ ├─ posting_count: u16                    │
│ ├─ max_doc_id: u64 (WAND 跳跃用)         │
│ └─ data: 压缩的 doc_id + doc_length + position 数据 │
│     doc_ids: delta 编码 + varint          │
│     doc_lengths: u8 压缩值 (BM25F 评分用) │
│     positions: delta 编码 + varint         │
└──────────────────────────────────────────┘
```

**doc_id 编码**:

```
doc_ids 在 block 内按升序排列
存储为 delta 值 + varint 编码:
  原始: [100, 105, 107, 120]
  delta: [100, 5, 2, 13]
  varint: 每个值用 1-9 字节，小值仅 1 字节
```

**position 编码**: 同理，delta + varint。

### 4.2 压缩策略

```
自适应选择:
  posting_count < 4096:
    先尝试 RLE，回退到 Array
  posting_count >= 4096:
    先尝试 RLE，回退到 Bitmap

Array: 排序 u64 delta + varint
Bitmap: 固定大小，适合稠密 posting list
RLE: Run-length 编码，适合连续 doc_id
```

### 4.3 BM25F 评分

```
score = Σ_f boost_f × IDF(term) × (tf_f × (K + 1)) / (tf_f + K × (1 - B + B × dl_f / avgdl_f))

参数:
  K = 1.2
  B = 0.75
  tf_f: 字段 f 中 term 的实际词频 (从 posting list position 数据获取)
  dl_f: 字段 f 的文档长度 (从 posting list 中内嵌的 doc_length 读取，u8 压缩值)
  avgdl_f: 字段 f 的平均文档长度 (从 m:stats 读取)
  boost_f: 字段权重 (schema 定义)
  IDF = ln((N - df + 0.5) / (df + 0.5) + 1)
    N: 总文档数
    df: 包含该 term 的文档数
```

**文档长度压缩**:

文档长度内嵌在 posting list 的每个 entry 中 (1 字节 u8)，评分时无需额外 KV 查找。

```rust
// 将文档长度映射到 u8 (0-255)
// index 128 → 1.0 (中性值)
pub const DOCUMENT_LENGTH_COMPRESSION: [f32; 256] = {
    [2.0_f32.powi(i as i32 - 128) for i in 0..256]
};

// 存储: u8 压缩值 (内嵌在 PostingEntry 中)
// 查询: dl = DOCUMENT_LENGTH_COMPRESSION[compressed_length]
```

### 4.4 查询类型

| 类型 | 行为 | 实现方式 |
|---|---|---|
| **Union (OR)** | 匹配任意 term，分数累加 | 多路归并 + WAND 剪枝 |
| **Intersection (AND)** | 匹配所有 term | 多指针归并取交集 |
| **Phrase** | 连续位置匹配 | AND + 位置验证 |

**Phrase 搜索位置验证**:

```
1. Intersection 找到候选文档
2. 对每个候选文档:
   a. 解码各 term 的 position 列表
   b. 检查是否存在连续位置序列:
      term_0 在 pos, term_1 在 pos+1, ..., term_n 在 pos+n
3. 仅返回通过验证的文档
```

### 4.5 WAND 优化

```
1. 对每个 term 的 posting list 按 max_block_score 排序
2. 维护 MinHeap(top-k)，capacity = request.length
3. 遍历 posting list 的 block:
   if heap.is_full() && block.max_block_score <= heap.min_score:
     skip_block()  // 该 block 不可能产生更高分结果
4. 仅对未跳过的 block 执行解码和评分
```

### 4.6 写缓冲区查询

```
查询流程:
1. 搜索 SlateDB 中的 posting list (通过 ReadCache 缓存)
2. 搜索 PostingBuffer 中的未提交 posting
3. 合并两路结果 (相同 doc_id 的分数取 max 或 sum，取决于查询类型)
4. 过滤 delete_set 中的文档
5. 排序 + offset/take
```

---

## 5. 向量搜索

### 5.1 基于 KV 的 IVF 设计

传统 IVF 将聚类数据放在一起，KV 存储更适合"键值对"模式。重新设计为**聚类即键值**:

```
v:meta → IvfMeta {
    dim: u32,                // 向量维度
    n_clusters: u32,         // 聚类数
    similarity: Similarity,  // Dot/Cosine/Euclidean
    quantization: QuantType, // None/SQ-I8
    total_vectors: u64,      // 总向量数
}

v:medoid:{id} → MedoidData {
    medoid_id: u32,
    vector: Vec<f32>,        // medoid 原始向量 (f32, 不量化)
    child_count: u32,        // 聚类内向量数
}

v:cluster:{id} → ClusterData (binary) {
    header: ClusterHeader {
        cluster_id: u32,
        vector_count: u32,
        byte_size: u32,
    },
    vectors: Vec<QuantizedVector> {
        doc_id: u64,
        field_id: u32,
        chunk_id: u32,
        scale: f32,          // 量化缩放因子
        norm: f32,           // 原始向量范数
        zero_point: i16,     // 量化零点
        data: Vec<i8>,       // 量化后的向量
    }
}
```

### 5.2 向量索引生命周期

```
索引文档时:
  1. 量化向量 (f32 → i8)
  2. 放入 VectorBuffer (内存 PostingBuffer.vectors)

Commit 时:
  1. 如果 VectorBuffer 非空:
     a. 从 SlateDB 读取 IvfMeta 和 medoid 数据
     b. 判断是否需要重新聚类 (见 §5.3)
     c. 增量模式:
        - 新向量分配到最近的现有 medoid 聚类
        - 读取目标 cluster → 追加向量 → 写回
     d. 重新聚类模式:
        - 读取所有 cluster 数据 → 合并新向量
        - 执行 K-Medoid 聚类
        - 写回新的 medoid 和 cluster 数据
  2. 更新 IvfMeta
  3. 清空 VectorBuffer

查询时:
  1. 从 ReadCache 获取 IvfMeta (常驻内存)
  2. 从 ReadCache 获取 medoid 数据 (常驻内存)
  3. 计算 query 与 medoid 相似度 → 选择 top-n 聚类
  4. 从 ReadCache / SlateDB 按需读取选中的 cluster 数据
  5. 反序列化 + 量化相似度计算 → Top-K
```

### 5.3 重新聚类触发策略

```
if total_vectors < 1000:
    // 小规模: 无聚类，全量 Flat 搜索
    n_clusters = 0
elif buffer_vector_count > total_vectors * 0.2:
    // 增量超过 20%: 全量重新 K-Medoid 聚类
    全量重新聚类
else:
    // 增量追加到最近聚类
    增量模式
```

**聚类数量计算**:

```
Auto:  n_clusters = sqrt(total_vectors) * 2
Fixed: n_clusters = user_config
```

### 5.4 K-Medoid 聚类算法

```
1. 首个 medoid: 向量质心最近邻
2. 后续 medoid: 贪心选择最大相似度增益，采样步长 max(n/100, 1)
3. 迭代优化 (最多 20 轮):
   a. 计算每个聚类的质心
   b. 选择最接近质心的成员为新 medoid
   c. 重分配所有向量
   d. 收敛条件: < 1% 向量重新分配
```

### 5.5 向量量化 (SQ-I8)

```
量化过程:
  scale = (max - min) / 255.0
  zero_point = round(-min / scale)
  quantized[i] = round((vector[i] - min) / scale) as i8

存储:
  scale: f32
  norm: f32 (原始向量 L2 范数)
  zero_point: i16
  data: Vec<i8>

相似度计算 (Dot Product):
  dot_q = Σ q_i * query_i  (使用 i8 × f32，SIMD 友好)
  score ≈ scale * dot_q + zero_point * Σ query_i

空间节省: f32(4B) → i8(1B), 减少 75%
```

### 5.6 VectorIndex trait

```rust
pub trait VectorIndex: Send + Sync {
    /// 搜索最相似的 k 个向量
    fn search(
        &self,
        query: &[f32],
        k: usize,
        ann_mode: &AnnMode,
        delete_set: &HashSet<u64>,
    ) -> Vec<SearchResult>;

    /// 将新向量加入缓冲
    fn add_vector(&self, doc_id: u64, field_id: u32, vector: Vec<f32>);

    /// 刷新缓冲到持久化存储
    async fn flush(&self) -> Result<()>;

    /// 聚类数量
    fn n_clusters(&self) -> usize;
}

pub enum AnnMode {
    All,                      // 搜索所有聚类 (精确)
    Nprobe(usize),            // 搜索 top-n 聚类
    SimilarityThreshold(f32), // 搜索相似度超过阈值的聚类
}

pub enum Similarity {
    Dot,
    Cosine,
    Euclidean,
}
```

---

## 6. 混合搜索与融合

### 6.1 加权 RRF 融合

```
score_hybrid = w_lexical / (K + rank_lexical) + w_vector / (K + rank_vector)

参数:
  K = 60 (默认，可配置)
  w_lexical = 1.0 (默认，可配置)
  w_vector = 1.0 (默认，可配置)
  rank 为 1-based
```

### 6.2 FusionStrategy trait

```rust
pub trait FusionStrategy: Send + Sync {
    fn fuse(
        &self,
        lexical_results: Vec<SearchResult>,
        vector_results: Vec<SearchResult>,
    ) -> Vec<SearchResult>;
}

pub struct RrfFusion {
    pub k: u32,          // 默认 60
    pub w_lexical: f32,  // 默认 1.0
    pub w_vector: f32,   // 默认 1.0
}
```

### 6.3 混合搜索流程

```
SearchRequest
    │
    ├─── query_string? ──► Tokenizer ──► Terms
    │                                        │
    │                                        ▼
    │                              SlateDB + Buffer 合并查询
    │                                        │
    │                                        ▼
    │                              BM25F 评分 + WAND ──► Top-K
    │
    ├─── query_vector? ──► 量化对齐 ──► Medoid 搜索
    │                                      │
    │                                      ▼
    │                              Top-N 聚类扫描 ──► Top-K
    │
    └──────────────────────────────────────┘
                        │
                   tokio::join! (并行)
                        │
                        ▼
                   RRF 融合
                        │
                        ▼
                  Sort + Offset/Take ──► SearchResponse
```

---

## 7. 分词器设计

### 7.1 Tokenizer trait

```rust
pub trait Tokenizer: Send + Sync {
    /// 将文本分词为 token 列表
    fn tokenize(&self, text: &str) -> Vec<Token>;

    /// 分词器名称
    fn name(&self) -> &str;
}

pub struct Token {
    pub text: String,        // token 原文
    pub position: u32,       // 位置偏移
    pub start_offset: usize, // 在原文中的起始字节偏移
    pub end_offset: usize,   // 在原文中的结束字节偏移
}
```

### 7.2 内置分词器

| 分词器 | 语言 | 实现 | 说明 |
|---|---|---|---|
| `UnicodeTokenizer` | 通用 | Unicode 字母数字切分 | 默认分词器 |
| `JiebaTokenizer` | 中文 | jieba-rs | 中文分词，搜索模式 |
| `SimpleTokenizer` | 纯 ASCII | 空格 + 标点切分 | 仅 ASCII 字母 |

### 7.3 分词流水线

```
输入文本
  │
  ▼
Tokenizer.tokenize()     → Vec<Token>
  │
  ▼
Lowercase (可选)         → 小写化
  │
  ▼
Stemmer (可选)           → 词干化 (预留接口)
  │
  ▼
StopWordFilter (可选)    → 停用词过滤
  │
  ▼
TermObjects              → (key_hash, field_id, positions)
```

### 7.4 自定义分词器注册

```rust
pub struct TokenizerRegistry {
    tokenizers: HashMap<String, Box<dyn Tokenizer>>,
}

impl TokenizerRegistry {
    pub fn global() -> &'static RwLock<Self>;
    pub fn register(&mut self, name: &str, tokenizer: Box<dyn Tokenizer>);
    pub fn get(&self, name: &str) -> Option<&dyn Tokenizer>;
}
```

### 7.5 索引配置

```rust
pub struct IndexConfig {
    pub tokenizer: TokenizerType,
    pub stemmer_language: Option<String>,
    pub stop_words: Option<Vec<String>>,
    pub lowercase: bool,  // 默认 true

    // 向量配置
    pub vector_dimensions: u32,
    pub vector_similarity: Similarity,
    pub clustering: ClusteringConfig,
    pub quantization: QuantType,

    // 压缩配置
    pub doc_compression: DocCompression,

    // 缓存配置
    pub posting_cache_size_mb: usize,   // 默认 256
    pub cluster_cache_size_mb: usize,    // 默认 512
}

pub enum TokenizerType {
    Simple,
    Unicode,
    Jieba,
    Custom(String),
}

pub enum DocCompression {
    None,
    Snappy,
    Lz4,
    Zstd,
}
```

---

## 8. 并发模型

### 8.1 锁策略

```
Index 内部锁:
  WriteBuffer:      tokio::RwLock  (读写频繁，异步友好)
  VectorIndex:      内部 tokio::RwLock
  DeleteSet:        tokio::RwLock
  DocIdCounter:     AtomicU64      (无锁)
  Stats:            tokio::RwLock
  ReadCache:        tokio::RwLock  (内部使用 concurrent hash map)
```

### 8.2 双缓冲 (Double Buffer)

```
索引文档时:
  buffer.write().await.push(doc_data)   // 写入当前 buffer

Commit 时:
  let old_data = {
      let mut buf = buffer.write().await;
      std::mem::take(&mut *buf)         // 原子交换，新写入进入空 buffer
  };
  // 释放 write lock
  // old_data 在 commit 期间不受新写入影响
  flush_to_slatedb(old_data).await
```

这解决了旧版代码的 "commit 期间并发写入丢数据" 问题:
- `take` 和 `commit` 之间，新写入进入新 buffer
- 旧 buffer 的数据不会丢失

### 8.3 查询并发

- 词法搜索和向量搜索使用 `tokio::join!` 并行执行
- 多个查询请求可并发执行 (读锁不互斥)
- Commit 与查询可并发 (写缓冲与读 SlateDB 互不干扰)

**Lock poisoning**: 使用 `tokio::sync::RwLock` (不会中毒)，或对 `std::sync::RwLock` 使用 `unwrap_or_else(|e| e.into_inner())` 恢复。

---

## 9. 缓存与性能

### 9.1 缓存策略

使用 SlateDB 内置缓存处理底层 block 缓存。应用层 `ReadCache` 负责 posting list 和 cluster 数据的读缓存，基于容量淘汰。

### 9.2 性能优化清单

| 优化项 | 策略 | 预期效果 |
|---|---|---|
| 并行搜索 | `tokio::join!` 词法 + 向量 | 搜索延迟减半 |
| WAND 剪枝 | block 级别跳过低分 | 减少 50-80% posting list 解码 |
| Posting 缓存 | LRU 缓存热 posting list | 减少 SlateDB 读取 |
| Cluster 按需加载 | 只加载选中的聚类 | 内存占用可控 |
| 批量写入 | buffer + batch commit | 减少 read-modify-write 次数 |
| Varint delta 编码 | doc_id delta + varint | posting list 压缩 60-80% |
| SQ-I8 量化 | f32 → i8 | 向量存储减少 75% |
| Medoid 常驻 | 小数据量常驻内存 | 向量搜索无额外 IO |

---

## 10. Library API

### 10.1 索引生命周期

```rust
// 打开共享数据库 (多索引共享同一 SlateDB 实例)
let shared_db = KvStore::open("db_path", object_store).await?;

// 创建索引
async fn create_index(
    shared_db: &KvStore,
    path: &str,
    config: IndexConfig,
    schema: Vec<SchemaField>,
) -> Result<Index>;

// 打开已有索引
async fn open_index(
    shared_db: &KvStore,
    path: &str,
) -> Result<Index>;

// 关闭数据库
async fn close_index(index: &Index) -> Result<()>;

// 删除索引 (物理删除所有数据)
async fn delete_index(
    shared_db: &KvStore,
    path: &str,
) -> Result<()>;
```

### 10.2 文档操作

```rust
async fn index_document(
    index: &Index,
    document: HashMap<String, Value>,
) -> Result<u64>;

async fn index_documents(
    index: &Index,
    documents: Vec<HashMap<String, Value>>,
) -> Result<Vec<u64>>;

async fn delete_document(index: &Index, doc_id: u64) -> Result<()>;

async fn get_document(
    index: &Index,
    doc_id: u64,
) -> Result<Option<HashMap<String, Value>>>;

async fn commit(index: &Index) -> Result<()>;
```

### 10.3 搜索

```rust
async fn search(
    index: &Index,
    request: SearchRequest,
) -> Result<SearchResponse>;
```

### 10.4 数据结构

```rust
pub struct SearchRequest {
    pub query_string: Option<String>,
    pub query_vector: Option<Vec<f32>>,
    pub search_mode: SearchMode,
    pub query_type: QueryType,
    pub ann_mode: AnnMode,
    pub offset: usize,
    pub length: usize,
    pub field_filter: Option<FieldFilter>,
}

pub enum SearchMode {
    Lexical,
    Vector { ann_mode: AnnMode },
    Hybrid { ann_mode: AnnMode, w_lexical: f32, w_vector: f32 },
}

pub enum QueryType {
    Union,
    Intersection,
    Phrase,
}

pub struct SearchResponse {
    pub query: String,
    pub result_count: usize,
    pub results: Vec<SearchResult>,
}

pub struct SearchResult {
    pub doc_id: u64,
    pub score: f32,
    pub result_type: ResultType,
    pub document: Option<HashMap<String, Value>>,
}

pub enum ResultType {
    Lexical,
    Vector,
    Hybrid,
}

pub struct FieldFilter {
    pub field: String,
    pub condition: FilterCondition,
}

pub enum FilterCondition {
    Equals(Value),
    Range { min: Option<Value>, max: Option<Value> },
    In(Vec<Value>),
}
```

---

## 11. 错误处理

```rust
#[derive(Debug, thiserror::Error)]
pub enum SeekPowerError {
    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("schema error: {0}")]
    SchemaError(String),

    #[error("encoding error: {0}")]
    EncodingError(String),

    #[error("vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("index corrupted: {0}")]
    Corrupted(String),
}

pub type Result<T> = std::result::Result<T, SeekPowerError>;
```

---

## 12. Schema 设计

```rust
pub struct SchemaField {
    pub field: String,         // 字段名
    pub store: bool,           // 是否存储文档原文
    pub index_lexical: bool,   // 是否建立词法索引
    pub index_vector: bool,    // 是否建立向量索引
    pub field_type: FieldType, // 字段类型
    pub boost: f32,            // BM25F 字段权重
}

pub enum FieldType {
    // 数值类型
    U8, U16, U32, U64,
    I8, I16, I32, I64,
    F32, F64,
    // 布尔
    Bool,
    // 时间
    Timestamp,
    // 文本
    Text,           // 全文索引文本
    String16,       // 短字符串 (max 16 bytes)
    String32,       // 中字符串 (max 32 bytes)
    // 集合
    StringSet16,    // 短字符串集合
    StringSet32,    // 中字符串集合
    // 向量
    Vector,         // 嵌入向量
}
```

---

## 13. 测试策略

### 13.1 测试层级

| 层级 | 覆盖内容 | 工具 |
|---|---|---|
| **单元测试** | 每个 trait 实现、编解码、评分公式 | `#[test]` + `proptest` |
| **集成测试** | 完整 索引→提交→搜索 流程 | `#[tokio::test]` + InMemory store |
| **并发测试** | 多线程并发读写、commit 期间写入 | `tokio::spawn` + loom |
| **性能测试** | 搜索延迟、索引吞吐量 | `criterion` |
| **模糊测试** | Tokenizer、Posting 编解码 | `cargo-fuzz` |

### 13.2 关键测试场景

1. 词法搜索正确性: 索引 N 篇文档 → 搜索验证 recall/precision
2. 向量搜索正确性: 暴力搜索对比 IVF 搜索的 recall@K
3. 混合搜索: 验证 RRF 融合结果排序合理性
4. 增量索引: commit → 继续索引 → commit → 搜索验证新旧数据可查
5. 并发安全: N 线程同时 index_document + commit → 无数据丢失
6. 大规模测试: 100 万文档性能基准
7. Phrase 搜索: 位置连续性检查

### 13.3 测试数据

- 中文: 维基百科摘要 (10,000 篇)
- 英文: MSMARCO passage ranking
- 向量: 768 维随机向量 + 已知最近邻

---

## 14. 依赖清单

### 14.1 核心

| 依赖 | 用途 |
|---|---|
| slatedb 0.13 | KV 存储引擎 |
| object_store | 对象存储抽象 |
| tokio | 异步运行时 |
| serde / serde_json | 序列化 |

### 14.2 搜索

| 依赖 | 用途 |
|---|---|
| ahash | 快速哈希表 |
| indexmap | 有序哈希表 |
| rayon | 并行处理 |
| smallvec | 小向量优化 |

### 14.3 压缩

| 依赖 | 用途 |
|---|---|
| snap | Snappy 压缩 |
| lz4_flex | LZ4 压缩 |
| zstd | Zstd 压缩 |

### 14.4 NLP

| 依赖 | 用途 |
|---|---|
| jieba-rs | 中文分词 |

### 14.5 工具

| 依赖 | 用途 |
|---|---|
| thiserror | 错误类型派生 |
| bytemuck | 安全字节转换 |
| itertools | 迭代器工具 |

---

## 15. 扩展性设计

### 15.1 短期 (V2)

- 自定义分词器插件 (TokenizerRegistry)
- 自定义评分函数
- 字段过滤器 (数值范围、日期范围)
- 嵌入模型集成 (model2vec / ONNX Runtime)

### 15.2 中期 (V3)

- HTTP 服务层 (axum)
- HNSW 向量索引 (VectorIndex trait 扩展)
- Geo 搜索 (Point + R-Tree)
- 监控指标 (Prometheus)

### 15.3 长期

- 分布式分片
- 分布式查询合并
- 自定义 compaction 策略 (posting list 语义感知合并)
