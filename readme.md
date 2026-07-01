# SeekPower

**Rust 混合搜索引擎库** — BM25F 词法搜索 + IVF 向量搜索 + RRF 融合

面向嵌入式集成和企业知识库 / RAG 场景，支持千万级文档规模。

---

## 目录

- [核心特性](#核心特性)
- [设计目标](#设计目标)
- [整体架构](#整体架构)
- [存储层设计](#存储层设计)
- [词法搜索](#词法搜索)
- [向量搜索](#向量搜索)
- [混合搜索与融合](#混合搜索与融合)
- [分词器设计](#分词器设计)
- [并发模型](#并发模型)
- [Library API](#library-api)
- [错误处理](#错误处理)
- [Schema 设计](#schema-设计)
- [测试策略](#测试策略)
- [依赖清单](#依赖清单)
- [扩展性设计](#扩展性设计)

---

## 核心特性

- **混合搜索** — BM25F 词法搜索 + IVF 向量搜索 + 加权 RRF 融合
- **LSM-Tree 原生存储** — 所有数据持久化到 SlateDB，利用其 compaction 机制管理数据布局
- **KV-原生 IVF** — 聚类即键值，medoid 常驻内存，cluster 按需加载
- **多语言分词** — 内置 jieba 中文分词 + Unicode 分词 + 可插拔自定义分词器
- **真实 BM25F 评分** — 使用实际词频 (tf) 和文档长度 (dl)，非硬编码值
- **并发安全** — tokio::RwLock + 双缓冲，commit 期间不丢数据
- **千万级规模** — 按需加载 + LRU 缓存，内存占用可控
- **云原生存储后端** — SlateDB + object_store，支持 Local FS / S3 / GCS / Azure Blob
- **文档存储分离** — 索引仅保存 doc_id，文档原文按需存储

## 设计目标

| 目标 | 度量 |
|---|---|
| 索引吞吐量 | > 10,000 docs/sec (批量) |
| 词法搜索延迟 | < 50ms (P99, 1000万文档) |
| 向量搜索延迟 | < 100ms (P99, nprobe=10, 1000万向量) |
| 内存占用 | < 1GB (1000万文档，不含文档原文缓存) |
| 召回率 | 向量搜索 recall@10 > 95% (nprobe=10) |

---

## 整体架构

### 架构图

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
│  │   (KvStore with prefixed      │                           │
│  │    index isolation +          │                           │
│  │    SlateDB KV Layout)         │                           │
│  └──────────────┬────────────────┘                           │
│                 │                                             │
├─────────────────┼─────────────────────────────────────────────┤
│           SlateDB (LSM-Tree KV)                              │
│  所有索引数据和文档原文通过 KV 接口持久化                       │
│  依赖 SlateDB 内部 compaction 管理数据布局                    │
└──────────────────────────────────────────────────────────────┘
```

### Workspace & Crate 划分

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
│   ├── seekpower-vector/       # VectorIndex trait + IVF 实现 + 聚类算法
│   ├── seekpower-quantize/     # 向量量化 trait + SQ-I8 实现
│   ├── seekpower-search/       # 查询引擎 (Union/Intersection/Phrase) + 混合搜索
│   ├── seekpower-storage/      # SlateDB KV 封装 + 定长前缀索引隔离
│   └── seekpower-error/        # 错误类型 (所有 crate 共享)
└── crates/seekpower-server/    # HTTP 服务 (独立 crate，后续实现)
```

### Crate 详细说明

| Crate | 职责 | 关键 trait/结构 | 依赖 |
|---|---|---|---|
| `seekpower-error` | 全局错误类型 | `SeekPowerError`, `Result<T>` | thiserror |
| `seekpower-schema` | Schema、字段类型、配置 | `SchemaField`, `FieldType`, `IndexConfig` | seekpower-error, serde |
| `seekpower-storage` | SlateDB KV 封装 + 定长前缀索引隔离 | `KvStore`, `encode_key`, `name_to_id` | seekpower-error, slatedb, object_store |
| `seekpower-tokenizer` | 分词器 trait + 内置实现 | `Tokenizer` trait, `JiebaTokenizer`, `UnicodeTokenizer`, `TokenizerRegistry` | seekpower-error, jieba-rs |
| `seekpower-posting` | Posting list 编解码 | `PostingCodec`, `PostingIterator`, `PostingListBlock` | seekpower-error, seekpower-schema |
| `seekpower-ranking` | BM25F 评分、WAND、MinHeap | `Scorer`, `MinHeap`, `SearchResult` | seekpower-error, seekpower-schema |
| `seekpower-quantize` | 向量量化 | `Quantizer` trait, `ScalarQuantI8` | seekpower-error |
| `seekpower-vector` | 向量索引 trait + IVF + 聚类 | `VectorIndex` trait, `IvfIndex`, `Clusterer` trait, `KMedoidClusterer` | seekpower-error, seekpower-schema, seekpower-storage, seekpower-quantize |
| `seekpower-search` | 查询引擎 + 混合搜索 | `QueryEngine`, `FusionStrategy`, `RrfFusion` | seekpower-error, seekpower-schema, seekpower-posting, seekpower-ranking, seekpower-vector, seekpower-tokenizer |
| `seekpower-core` | Index 主结构、生命周期、API | `Index`, `create_index`, `open_index`, `search`, `commit` | 所有上述 crate |
| `seekpower-server` | HTTP 服务 (后续) | axum handlers | seekpower-core, axum |

---

## 存储层设计

### SlateDB KV 布局

为了在单一 KV 引擎中实现多租户与多索引，Key 的设计采用**定长前缀层次结构**。索引路径映射为内部定长 ID（`u32` 大端字节流，4 字节），数据类型用 `u8`（1 字节）标识，确保字典序紧凑且解析高效。

$ \text{Key} = \text{Index\_ID (u32 BE)} \parallel \text{Data\_Type (u8)} \parallel \text{Business\_Key} $

调用方通过 `KvStore::prefixed_*` 方法操作索引数据，传入 `index_id` + `data_type`，无需关心物理键编码。`name_to_id(name)` 将索引名称映射为 `u32` ID。`TYPE_META`/`TYPE_DOC`/`TYPE_IDX`/`TYPE_VEC`/`TYPE_DELL`/`TYPE_LEN` 定义各数据类型标签。

| **data_type (u8)** | **逻辑含义** | **business_key 格式** | **Value 负载设计** |
|---|---|---|---|
| `0` (TYPE_META) | 元数据 | `schema` / `config` / `stats` / `fields` | JSON 序列化的 Schema、IndexConfig、IndexStats 等 |
| `1` (TYPE_DOC) | 文档原文 | `doc_id (u64 LE)` | 序列化后的文档数据 (JSON) |
| `2` (TYPE_IDX) | 倒排表 | `term_hash (u64 LE)` | PostingListBlock (Delta + VByte 压缩) |
| `3` (TYPE_VEC) | 向量索引 | `meta` / `medoid:{id}` / `cluster:{id}` | IvfMeta JSON / MedoidData / ClusterData (binary) |
| `4` (TYPE_DELL) | 删除标记 | `doc_id (u64 LE)` | tombstone (b"1") |
| `5` (TYPE_LEN) | 文档长度 | `doc_id (u64 LE) ‖ field_id (u16 LE)` | u8 压缩值 |

### 文档 ID 方案

```
doc_id: u64 索引内单调递增
```

### 写入链路

为了避免对象存储上的小 I/O 惩罚，写入必须是批量的、内存缓冲的。

1. **文档摄入与分词** — 输入文档流，经由 Analyzer 提取 Terms。
2. **内存倒排构建 (In-Memory Buffer)**
   - 在内存中维护一个微型倒排索引结构（例如基于 SkipList 或 BTreeMap）
   - 新文档的 DocID 被追加到内存中各 Term 的倒排链表末尾
3. **Flush 机制**
   - 当内存 Buffer 达到容量阈值（如 64MB）或时间窗口（如 5 秒）触发 Flush
   - 后台线程将内存中的倒排数据和正排数据转换为 SlateDB 要求的 KV 格式，**严格按字典序排序**后，调用 SlateDB 的批量写入接口下刷。这完全迎合了 LSM-tree 的追加写特性

### 读取链路

查询时，需要在多路数据间进行交并集计算及动态打分。

1. **条件解析** — 将用户查询（如 `TermA AND TermB`）解析为逻辑执行树。
2. **前缀定位与扫描 (Seek & Scan)**
   - 利用 SlateDB 的 `Iterator`，构造起始 Key（如 `idx:Tenant1:Index1:TermA:\x00`）
   - 顺序 Scan 扫描该 Term 下的所有 `<BlockID>`，将物理上分散的多个 `PostingsList` 分片读入内存
3. **多路归并 (Merge & Intersect)**
   - 对读取到的多个 `PostingsList`（TermA 的集合与 TermB 的集合）在内存中执行双指针求交集或位图 (Bitmap) 运算，得出候选 `DocID` 集合
4. **动态打分与截断 (Scoring & Top-K)**
   - 结合 `meta` 表中的全局统计信息，计算 TF-IDF 或 BM25
   - 维护一个大小为 K 的最小堆（Min-Heap），选出得分最高的 Top-K DocID
5. **回表与组装 (Fetch Forward Data)**
   - 并发向 SlateDB 发起 `doc:<TenantID>:<IndexID>:<DocID>` 的 Get 请求，提取原文并返回

### 空间放大与压缩算法

倒排列表是极其稠密的递增整数序列。直接存储原生的 `uint64` 会造成巨大的写放大和存储浪费。

- **编码** — 必须在写入 SlateDB 前，对 `PostingsList` 中的 DocID 进行差值编码（Delta Encoding）。例如 `[1000, 1005, 1008]` 编码为 `[1000, 5, 3]`。
- **压缩** — 使用 VByte、Simple8b 或 Roaring Bitmap 算法对差值进行极限压缩。在读取时利用 SIMD 指令加速解压。

### 读时合并与 Compaction 联动

随着时间推移，同一个 Term 会在 SlateDB 中产生大量的 `<BlockID>` 分片。

- **Read-time Merge** — 依靠上文提到的 Scan 机制在查询时进行多版本归并。
- **Compaction Filter 扩展** — 如果在系统层面能够扩展 SlateDB 的 Compaction 逻辑，可以在底层 SSTable 合并时，拦截相同 Term 的相邻 Block，将短的 `PostingsList` 反序列化、合并、重新压缩后再写入下一层。这能显著降低未来的读放大。

### 索引的秒级删除

得益于前缀设计，删除某个租户的特定索引时，无需执行耗时的“逐条查询并删除”操作。

- **方案** — 直接调用或模拟范围删除（Range Tombstone）。下发从 `Data_Type:Tenant_ID:Index_ID:\x00` 到 `Data_Type:Tenant_ID:Index_ID:\xFF` 的墓碑标记。
- **效果** — 逻辑上立即生效（查询被拦截），物理空间在 SlateDB 后续的后台 Compaction 过程中被自动回收，对线上读写链路的冲击极小。

---

## 词法搜索

### Posting List 编码

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

**doc_id 编码**

```
doc_ids 在 block 内按升序排列
存储为 delta 值 + varint 编码:
  原始: [100, 105, 107, 120]
  delta: [100, 5, 2, 13]
  varint: 每个值用 1-9 字节，小值仅 1 字节
```

**position 编码** — 同理，delta + varint。

### BM25F 评分

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

**文档长度压缩**

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

### 查询类型

| 类型 | 行为 | 实现方式 |
|---|---|---|
| **Union (OR)** | 匹配任意 term，分数累加 | 多路归并 + WAND 剪枝 |
| **Intersection (AND)** | 匹配所有 term | 多指针归并取交集 |
| **Phrase** | 连续位置匹配 | AND + 位置验证 |

**Phrase 搜索位置验证**

```
1. Intersection 找到候选文档
2. 对每个候选文档:
   a. 解码各 term 的 position 列表
   b. 检查是否存在连续位置序列:
      term_0 在 pos, term_1 在 pos+1, ..., term_n 在 pos+n
3. 仅返回通过验证的文档
```

### WAND 优化

```
1. 对每个 term 的 posting list 按 max_block_score 排序
2. 维护 MinHeap(top-k)，capacity = request.length
3. 遍历 posting list 的 block:
   if heap.is_full() && block.max_block_score <= heap.min_score:
     skip_block()  // 该 block 不可能产生更高分结果
4. 仅对未跳过的 block 执行解码和评分
```

### 写缓冲区查询

```
查询流程:
1. 搜索 SlateDB 中的 posting list (通过 KvStore::prefixed_get)
2. 搜索 PostingBuffer 中的未提交 posting
3. 合并两路结果 (相同 doc_id 的分数取 max 或 sum，取决于查询类型)
4. 过滤 delete_set 中的文档
5. 排序 + offset/take
```

---

## 向量搜索

### 基于 KV 的 IVF 设计

传统 IVF 将聚类数据放在一起，KV 存储更适合"键值对"模式。重新设计为**聚类即键值**：

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

### 向量索引生命周期

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
  1. 获取 IvfMeta (常驻内存)
  2. 获取 medoid 数据 (常驻内存)
  3. 计算 query 与 medoid 相似度 → 选择 top-n 聚类
  4. 通过 KvStore::prefixed_get 按需读取选中的 cluster 数据
  5. 反序列化 + 量化相似度计算 → Top-K
```

### 重新聚类触发策略

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

**聚类数量计算**

```
Auto:  n_clusters = sqrt(total_vectors) * 2
Fixed: n_clusters = user_config
```

### K-Medoid 聚类算法

```
1. 首个 medoid: 向量质心最近邻
2. 后续 medoid: 贪心选择最大相似度增益，采样步长 max(n/100, 1)
3. 迭代优化 (最多 20 轮):
   a. 计算每个聚类的质心
   b. 选择最接近质心的成员为新 medoid
   c. 重分配所有向量
   d. 收敛条件: < 1% 向量重新分配
```

### 向量量化 (SQ-I8)

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

### VectorIndex trait

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

## 混合搜索与融合

### 加权 RRF 融合

```
score_hybrid = w_lexical / (K + rank_lexical) + w_vector / (K + rank_vector)

参数:
  K = 60 (默认，可配置)
  w_lexical = 1.0 (默认，可配置)
  w_vector = 1.0 (默认，可配置)
  rank 为 1-based
```

### FusionStrategy trait

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

### 混合搜索流程

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

## 分词器设计

### Tokenizer trait

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

### 内置分词器

| 分词器 | 语言 | 实现 | 说明 |
|---|---|---|---|
| `UnicodeTokenizer` | 通用 | Unicode 字母数字切分 | 默认分词器 |
| `JiebaTokenizer` | 中文 | jieba-rs | 中文分词，搜索模式 |
| `SimpleTokenizer` | 纯 ASCII | 空格 + 标点切分 | 仅 ASCII 字母 |

### 分词流水线

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

### 自定义分词器注册

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

### 索引配置

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

## 并发模型

### 锁策略

```
Index 内部锁:
  WriteBuffer:      tokio::RwLock  (读写频繁，异步友好)
  VectorIndex:      内部 tokio::RwLock
  DeleteSet:        tokio::RwLock
  DocIdCounter:     AtomicU64      (无锁)
  Stats:            tokio::RwLock
```

### 双缓冲 (Double Buffer)

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

### 查询并发

- 词法搜索和向量搜索使用 `tokio::join!` 并行执行
- 多个查询请求可并发执行 (读锁不互斥)
- Commit 与查询可并发 (写缓冲与读 SlateDB 互不干扰)

**Lock poisoning** — 使用 `tokio::sync::RwLock` (不会中毒)，或对 `std::sync::RwLock` 使用 `unwrap_or_else(|e| e.into_inner())` 恢复。

---

## Library API

### 索引生命周期

```rust
// 打开共享数据库 (多索引共享同一 SlateDB 实例)
let shared_db = KvStore::open("db_path", object_store).await?;

// 创建索引
async fn create_index(
    shared_db: &KvStore,
    name: &str,
    config: IndexConfig,
    schema: Vec<SchemaField>,
) -> Result<Index>;

// 打开已有索引
async fn open_index(
    shared_db: &KvStore,
    name: &str,
) -> Result<Index>;

// 关闭数据库
async fn close_index(index: &Index) -> Result<()>;

// 删除索引 (物理删除所有数据)
async fn delete_index(
    shared_db: &KvStore,
    name: &str,
) -> Result<()>;
```

### 文档操作

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

### 搜索

```rust
async fn search(
    index: &Index,
    request: SearchRequest,
) -> Result<SearchResponse>;
```

### 数据结构

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

## 错误处理

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

## Schema 设计

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

## 测试策略

### 测试层级

| 层级 | 覆盖内容 | 工具 |
|---|---|---|
| **单元测试** | 每个 trait 实现、编解码、评分公式 | `#[test]` + `proptest` |
| **集成测试** | 完整 索引→提交→搜索 流程 | `#[tokio::test]` + InMemory store |
| **并发测试** | 多线程并发读写、commit 期间写入 | `tokio::spawn` + loom |
| **性能测试** | 搜索延迟、索引吞吐量 | `criterion` |
| **模糊测试** | Tokenizer、Posting 编解码 | `cargo-fuzz` |

### 关键测试场景

1. 词法搜索正确性: 索引 N 篇文档 → 搜索验证 recall/precision
2. 向量搜索正确性: 暴力搜索对比 IVF 搜索的 recall@K
3. 混合搜索: 验证 RRF 融合结果排序合理性
4. 增量索引: commit → 继续索引 → commit → 搜索验证新旧数据可查
5. 并发安全: N 线程同时 index_document + commit → 无数据丢失
6. 大规模测试: 100 万文档性能基准
7. Phrase 搜索: 位置连续性检查

### 测试数据

- 中文: 维基百科摘要 (10,000 篇)
- 英文: MSMARCO passage ranking
- 向量: 768 维随机向量 + 已知最近邻

---

## 依赖清单

### 核心

| 依赖 | 用途 |
|---|---|
| slatedb 0.13 | KV 存储引擎 |
| object_store | 对象存储抽象 |
| tokio | 异步运行时 |
| serde / serde_json | 序列化 |

### 搜索

| 依赖 | 用途 |
|---|---|
| ahash | 快速哈希表 |
| indexmap | 有序哈希表 |
| rayon | 并行处理 |
| smallvec | 小向量优化 |

### 压缩

| 依赖 | 用途 |
|---|---|
| snap | Snappy 压缩 |
| lz4_flex | LZ4 压缩 |
| zstd | Zstd 压缩 |

### NLP

| 依赖 | 用途 |
|---|---|
| jieba-rs | 中文分词 |

### 工具

| 依赖 | 用途 |
|---|---|
| thiserror | 错误类型派生 |
| bytemuck | 安全字节转换 |
| itertools | 迭代器工具 |

---

## 扩展性设计

### 短期 (V2)

- 自定义分词器插件 (TokenizerRegistry)
- 自定义评分函数
- 字段过滤器 (数值范围、日期范围)
- 嵌入模型集成 (model2vec / ONNX Runtime)

### 中期 (V3)

- HTTP 服务层 (axum)
- HNSW 向量索引 (VectorIndex trait 扩展)
- Geo 搜索 (Point + R-Tree)
- 监控指标 (Prometheus)

### 长期

- 分布式分片
- 分布式查询合并
- 自定义 compaction 策略 (posting list 语义感知合并)
