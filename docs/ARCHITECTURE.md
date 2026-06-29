# StormDB 架构设计文档

## 概述

StormDB 是一个混合搜索引擎，结合了词法搜索（BM25F）和向量搜索（IVF + K-Medoid），使用 Reciprocal Rank Fusion (RRF) 进行结果融合。底层存储基于 SlateDB，提供云原生的可扩展性。

## 1. 系统架构

### 1.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                         StormDB Index                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
│  │  Lexical Search  │  │  Vector Search   │  │   RRF Fusion  │ │
│  │      (BM25F)     │  │   (IVF+K-Medoid) │  │   (K=0.6)     │ │
│  └──────────────────┘  └──────────────────┘  └──────────────┘ │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Query Engine                          │   │
│  │  (Union/Intersection/Phrase + WAND Optimization)        │   │
│  └─────────────────────────────────────────────────────────┘   │
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
│  idx: 全文索引 ( Posting Lists - 压缩 )                         │
│  vec: 向量索引 ( IVF聚类 - medoids + clusters )                 │
│  doc: 文档原文 ( Compressed JSON )                              │
│  facet: Facet数据 ( 无索引，按文档存储 )                         │
│  dell: 删除标记                                                 │
│  meta: 元数据 ( schema, config, stats )                        │
│  len: 文档长度 ( BM25压缩值 )                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Object Storage Backend                        │
│              (Local FS / S3 / GCS / Azure Blob)                 │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 数据流

#### 写入流程（索引构建）

```
文档 (Document)
    │
    ├──► 文本字段 (Text fields)
    │       │
    │       ▼
    │   Tokenizer (分词)
    │       │
    │       ▼
    │   Terms + Positions
    │       │
    │       ▼
    │   Posting Lists (level-0 buffer)
    │       │  构建词法索引：term → [doc_id, positions]
    │       ▼
    │   Commit → 压缩 → SlateDB (idx: 前缀)
    │
    ├──► 向量字段 (Vector fields)
    │       │
    │       ▼
    │   量化 (ScalarQuant I8)
    │       │
    │       ▼
    │   Vector Buffer
    │       │  向量索引的原始数据
    │       ▼
    │   Commit → K-Medoid聚类 → SlateDB (vec: 前缀)
    │
    ├──► Facet字段 (Facet fields)
    │       │
    │       ▼
    │   Facet Row (二进制编码)
    │       │  仅存储，无索引
    │       ▼
    │   Commit → SlateDB (facet: 前缀)
    │
    └──► 文档原文
            │
            ▼
        Compressed JSON
            │
            ▼
        Commit → SlateDB (doc: 前缀)
```

#### 查询流程

```
Query String → Tokenizer → Terms
                    │
                    ▼
              Posting Lists → Decode → Filter (delete_set)
                    │
                    ▼
              BM25F Score → MinHeap (Top-K)

Query Vector ────────┘

                    │
                    ▼
                 RRF Fusion
                    │
                    ▼
                Sort → Return Results

                 Facet Counting (查询结果集遍历聚合)
```

## 2. 存储层设计

### 2.1 SlateDB KV布局

**所有数据持久化到SlateDB**，通过键前缀分区存储在统一的SlateDB实例中：

| 前缀    | 键格式                              | 值类型                     | 持久化内容                               |
|---------|-------------------------------------|----------------------------|------------------------------------------|
| `meta:` | `meta:schema`                       | Schema JSON                | 索引Schema定义                           |
| `meta:` | `meta:config`                       | IndexConfig JSON           | 索引配置                                 |
| `meta:` | `meta:stats`                        | Stats JSON                 | 运行时统计（文档数、向量数、level数）    |
| `idx:`  | `idx:{level}:{strip}:{term_hash}`   | PostingListBlock (binary)  | **全文索引**：压缩的posting lists        |
| `vec:`  | `vec:{level}:medoids`               | Medoid list (binary)       | **向量索引**：聚类中心点列表              |
| `vec:`  | `vec:{level}:cluster:{id}`          | Cluster data (binary)      | **向量索引**：聚类内的向量数据            |
| `doc:`  | `doc:{doc_id}`                      | Compressed document JSON   | **文档原文**：压缩的文档JSON             |
| `facet:`| `facet:{doc_id}`                    | Facet row (binary)         | Facet值（无索引，仅存储）                |
| `facet:`| `facet:dict`                        | String dictionary JSON     | Facet字符串字典                          |
| `dell:` | `dell:{doc_id}`                     | Deletion marker (空值)     | 删除标记                                 |
| `len:`  | `len:{level}:{doc_id}`              | Compressed doc length (u8) | 文档长度压缩值（BM25评分使用）           |

**核心持久化数据**：
1. **idx: 前缀** - 全文索引（Posting Lists），commit时从level-0压缩后写入
2. **vec: 前缀** - 向量索引（IVF聚类结构），commit时聚类后写入
3. **doc: 前缀** - 文档原文，commit时压缩后写入

### 2.2 文档ID编码

```
global_doc_id = (level as u64) << 16 | local_docid

- level: 每次commit完成后递增（如果level已满）
- local_docid: 每个level内0-65535
- 支持约281万亿文档 (2^48)
```

### 2.3 Level设计

每个Level最多包含65,536个文档：

- **Level-0**: 写入缓冲区（未提交数据）
  - `segments_level0`: 未提交的posting lists
  - `vector_buffer`: 未提交的向量数据
  - `pending_docs`: 未提交的文档和facet

- **Level-N**: 已提交的持久化数据
  - `segments_index`: 已提交的posting lists
  - `vector_levels`: 已提交的向量索引结构

**Level合并策略**：
```
if (上一level文档数 < 65536) {
    从SlateDB读取上一level的posting lists
    解码回level-0格式
    与当前level-0合并
    commit到同一level
} else {
    创建新level
}
```

## 3. 核心数据结构

### 3.1 Index结构

```rust
pub struct Index {
    // 存储层
    pub db: KvStore,                          // SlateDB包装器

    // Schema和配置
    pub schema: SchemaMap,                    // 字段名 → SchemaField
    pub config: IndexConfig,                  // 索引配置

    // 运行时统计
    pub stats: RwLock<IndexStats>,            // 文档数、向量数、level数

    // 词法索引 - 内存加速查询
    pub segments_index: RwLock<Vec<SegmentIndex>>,      // 已提交的posting lists
    pub segments_level0: RwLock<SegmentLevel0>,         // 未提交的缓冲区

    // 向量索引
    pub vector_levels: RwLock<Vec<VectorLevel>>,        // 已提交的向量数据
    pub vector_buffer: RwLock<Vec<ParentMedoid>>,      // 未提交的向量缓冲

    // BM25统计
    pub level_index: RwLock<Vec<LevelIndex>>,           // 每level文档长度数组
    pub bm25_cache: RwLock<[f32; 256]>,                // 文档长度压缩查找表

    // 删除标记
    pub delete_set: RwLock<AHashSet<usize>>,           // 已删除的global doc_id

    // Facet支持（无索引，仅字典）
    pub facet_dict: RwLock<IndexMap<String, (Vec<String>, usize)>>,

    // 嵌入模型
    pub embedding_model: Option<EmbeddingModel>,

    // 文档计数
    pub indexed_doc_count: RwLock<u64>,
    pub indexed_vector_count: RwLock<u64>,
}
```

### 3.2 SchemaField

```rust
pub struct SchemaField {
    pub field: String,        // 字段名
    pub store: bool,          // 是否在文档存储中保留原文
    pub index_lexical: bool,  // 是否建立词法索引
    pub index_vector: bool,   // 是否建立向量索引
    pub field_type: FieldType,// 字段类型
    pub facet: bool,          // 是否存储为facet（无索引）
    pub boost: f32,           // BM25字段权重
}

pub enum FieldType {
    // 数值类型
    U8, U16, U32, U64,
    I8, I16, I32, I64,
    F32, F64,
    Bool,
    Timestamp,

    // 字符串类型
    String16,  // 16-bit string id
    String32,  // 32-bit string id
    StringSet16, StringSet32,
    Text,      // 全文索引文本

    // 特殊类型
    Point,     // 地理位置
    Vector,    // 嵌入向量
}
```

### 3.3 Posting List结构（词法索引）

```rust
// 已提交的posting list（压缩存储）
pub struct PostingListObjectIndex {
    pub posting_count: u32,           // 文档总数
    pub max_list_score: f32,          // 最大BM25分数
    pub blocks: Vec<BlockObjectIndex>,// 块列表
    pub byte_array: Vec<u8>,          // 压缩的数据块
}

pub struct BlockObjectIndex {
    pub max_block_score: f32,         // 块最大分数（WAND优化）
    pub block_id: u32,                // 块ID
    pub compression_type_pointer: u32, // 压缩类型 + 偏移量
    pub posting_count: u16,           // 块中文档数
    pub max_docid: u16,              // 块中文档ID最大值
    pub max_p_docid: u16,            // 位置中文档ID最大值
    pub pointer_pivot_p_docid: u16,  // 位置数据偏移
}

// 未提交的posting list（内存缓冲）
pub struct PostingListObject0 {
    pub docids: Vec<u16>,            // 文档ID列表
    pub positions: Vec<Vec<u16>>,    // 每个posting的位置列表
    pub posting_count: usize,        // posting总数
    pub max_block_score: f32,        // 最大分数
    pub max_docid: u16,              // 最大文档ID
    pub max_p_docid: u16,            // 位置最大文档ID
}
```

**词法索引结构**：
```
term → Posting List → [doc_id, positions]
                    → [doc_id, positions]
                    → ...
```

### 3.4 向量索引结构（IVF）

```rust
// 向量level
pub struct VectorLevel {
    pub medoids: Vec<Medoid>,           // 聚类中心点列表
    pub clusters: Vec<Vec<u8>>,         // 序列化的聚类数据
    pub cluster_headers: Vec<ClusterHeader>, // 聚类元数据
}

pub struct Medoid {
    pub medoid_index: usize,            // 中心点索引
    pub child_count: usize,             // 子节点数量
}

pub struct ClusterHeader {
    pub start_index: u32,               // 起始索引
    pub child_count: u32,               // 子节点数量
}

// 向量头部（序列化格式）
#[repr(C, packed)]
pub struct VectorHeader {
    pub doc_id: u16,
    pub field_id: u32,
    pub chunk_id: u32,
    pub scale: f32,                     // 量化缩放因子
    pub norm: f32,                      // 向量范数
    pub zero_point: i16,                // 量化零点
    pub sum_q: i32,                     // 量化值和
}
```

**向量索引结构**：
```
IVF (Inverted File)
    ├── Cluster 0 (medoid0)
    │   ├── vector1 → similarity
    │   ├── vector2 → similarity
    │   └── ...
    ├── Cluster 1 (medoid1)
    │   ├── vector3 → similarity
    │   └── ...
    └── ...

搜索时：
    query → 计算与各medoid的相似度 → 选择top-n聚类 → 扫描聚类内向量 → Top-K
```

### 3.5 压缩类型

```rust
pub enum CompressionType {
    Delta = 0,    // Delta编码（保留，暂未启用）
    Array = 1,    // 排序的u16数组: posting_count * 2字节
    Bitmap = 2,   // Roaring Bitmap: 固定8192字节（65536位）
    Rle = 3,      // Run-length编码: runs_count * 4字节
}
```

**压缩决策逻辑**：
- `posting_count < 4096`: 先尝试RLE，回退到Array
- `posting_count >= 4096`: 先尝试RLE，回退到Bitmap

## 4. 搜索算法

### 4.1 词法搜索 (BM25F)

**BM25F评分公式**：

```
score = Σ_f boost_f × IDF_f × (tf_f × (K + 1)) / (tf_f + K × (1 - B + B × dl_f / avgdl_f))
```

**参数**：
- K = 1.2（词频饱和参数）
- B = 0.75（文档长度归一化）
- dl_f: 文档在字段f的长度（压缩值）
- avgdl_f: 字段f的平均文档长度

**IDF计算**：

```
IDF = ln((N - df + 0.5) / (df + 0.5) + 1)
```

**查询类型**：
- **Union (OR)**: 匹配任意term的文档，分数累加
- **Intersection (AND)**: 匹配所有term的文档
- **Phrase**: 匹配精确短语，验证位置相邻

**WAND优化**：
```
if heap.is_full() && block.max_block_score <= heap.min_score {
    skip_block()  // 跳过无法超越当前堆最小分数的块
}
```

### 4.2 向量搜索 (IVF + K-Medoid)

**流程**：

1. **Query准备**: 归一化（cosine）或使用原始向量
2. **Medoid搜索**: 计算query与所有medoid的相似度
3. **聚类选择**: 根据ANN模式选择top-n聚类
4. **聚类搜索**: 扫描选中聚类中的所有向量，计算相似度
5. **Top-K排序**: 返回相似度最高的k个结果

**ANN模式**：

```rust
pub enum AnnMode {
    All,                      // 搜索所有聚类（精确）
    Nprobe(usize),            // 只搜索相似度最高的n个聚类
    SimilarityThreshold(f32), // 搜索相似度超过阈值的聚类
}
```

**相似度类型**：

```rust
pub enum VectorSimilarity {
    Dot,        // 点积
    Cosine,     // 余弦相似度（归一化后的点积）
    Euclidean,  // 负平方欧氏距离
}
```

### 4.3 混合搜索 (RRF)

**RRF融合公式**：

```
score = 1/(K + rank_lexical) + 1/(K + rank_vector)
```

**参数**：
- K = 0.6（与SeekStorm一致）
- rank从1开始

**结果来源标记**：
- Lexical: 仅词法搜索结果
- Vector: 仅向量搜索结果
- Hybrid: RRF融合结果

### 4.4 Facet查询

**无索引设计**：

Facet数据仅按文档存储为二进制行，查询时：
1. 执行搜索获取结果文档列表
2. 遍历结果集的facet行
3. 聚合统计facet值的出现次数

**Facet行格式**：

```
| field1 (N bytes) | field2 (M bytes) | ... | fieldN (K bytes) |
```

**类型编码**：
- 数值类型: 直接二进制编码
- String16: u16字典索引
- String32: u32字典索引

## 5. K-Medoid聚类算法

### 5.1 算法流程

1. **聚类数量确定**：
   - Auto: `sqrt(vector_count) * 2`
   - Fixed(n): 用户指定n
   - None: 1个聚类

2. **第一个medoid选择**：
   - 采样所有向量计算质心
   - 找到与质心最近的向量作为第一个medoid

3. **后续medoid选择**：
   - 对每个候选向量，计算总相似度增益
   - 选择增益最高的向量

4. **分配**：
   - 每个新medoid后，贪婪重分配所有向量到最佳medoid

5. **迭代优化**：
   - 计算每个聚类的质心
   - 找到最接近质心的成员作为新medoid
   - 重分配所有向量
   - 重复直到收敛

### 5.2 收敛条件

- 最大迭代次数: 100
- Medoid变化阈值: < 1%向量重新分配

## 6. 分词器设计

### 6.1 分词类型

```rust
pub enum TokenizerType {
    AsciiAlphabetic,           // 仅ASCII字母
    UnicodeAlphanumeric,       // Unicode字母数字
    UnicodeAlphanumericFolded, // Unicode字母数字（去除变音符号）
    UnicodeAlphanumericZH,     // Unicode + 中文（jieba分词）
}
```

### 6.2 查询操作符

- `+`: 必须包含（AND）
- `-`: 排除（NOT）
- `""`: 精确短语匹配
- 默认: OR

### 6.3 分词流程

**索引模式**:
1. 文本标准化（小写、去除变音符号）
2. 分词
3. 词干化（可选）
4. 停用词过滤（可选）
5. 计算key_hash和key0
6. 生成TermObject

**查询模式**:
1. 解析操作符（+, -, ""）
2. 分词（同索引模式）
3. 生成unique_terms和non_unique_terms
4. 确定默认查询类型

## 7. Commit流程

### 7.1 完整流程

**Commit将所有数据持久化到SlateDB**：

```
1. Merge incomplete level (如果上一level < 65536文档)
   - 从SlateDB读取上一level的posting lists
   - 解码回level-0格式
   - 与当前level-0合并

2. 写入全文索引到SlateDB (idx:前缀) ⭐
   - 按strip分组（segment_number_bits决定分区数）
   - 按key_hash排序
   - 压缩每个term的posting list (Array/Bitmap/RLE)
   - 写入 "idx:{level}:{strip}:{term_hash}"

3. 写入文档长度到SlateDB (len:前缀)
   - 计算每个文档的压缩长度
   - 写入 "len:{level}:{doc_id}"

4. 写入向量索引到SlateDB (vec:前缀) ⭐
   - K-Medoid聚类
   - 写入 "vec:{level}:medoids"
   - 写入 "vec:{level}:cluster:{id}"

5. 写入文档原文到SlateDB (doc:前缀) ⭐
   - 压缩文档JSON (Snappy/LZ4/Zstd)
   - 写入 "doc:{doc_id}"

6. 写入Facet到SlateDB (facet:前缀，无索引)
   - 写入 "facet:{doc_id}" (二进制行)
   - 更新并写入 "facet:dict" (字符串字典)

7. 写入删除标记到SlateDB (dell:前缀)
   - 写入 "dell:{doc_id}"

8. 更新统计到SlateDB (meta:前缀)
   - 写入 "meta:stats"

9. 更新内存索引（用于查询加速）
   - 加载新posting lists到segments_index
   - 加载新向量数据到vector_levels

10. 清空缓冲区
    - segments_level0
    - vector_buffer
    - pending_docs
```

**三大核心持久化数据**（标记⭐）：
1. **全文索引** (idx:) - 压缩的posting lists，从level-0构建
2. **向量索引** (vec:) - IVF聚类结构，从vector buffer构建
3. **文档原文** (doc:) - 压缩的JSON文档

### 7.2 压缩算法

**文档压缩**:
- Snappy（默认，平衡速度和压缩率）
- LZ4（最快）
- Zstd（最高压缩率）
- None（不压缩）

**Posting List压缩**: Array/Bitmap/RLE（自适应）

## 8. 文档长度压缩

### 8.1 DOCUMENT_LENGTH_COMPRESSION

**目的**: 将文档长度压缩到u8（0-255）范围，减少存储和计算开销

**查找表**:

```rust
pub const DOCUMENT_LENGTH_COMPRESSION: [f32; 256] = {
    [2.0_f32.powi(i as i32 - 128) for i in 0..256]
};
```

**使用**:
- 存储: 压缩的length作为u8存储
- 计算: `dl_compressed = DOCUMENT_LENGTH_COMPRESSION[compressed_length]`

## 9. API设计

### 9.1 Library API

```rust
// 索引管理
fn create_index(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    config: IndexConfig,
    schema: Vec<SchemaField>
) -> Result<Index>;

fn open_index(
    path: &str,
    object_store: Arc<dyn ObjectStore>
) -> Result<Index>;

fn close_index(index: &Index) -> Result<()>;

// 文档操作
fn index_document(
    index: &Index,
    document: HashMap<String, Value>
) -> Result<u64>;

fn index_documents(
    index: &Index,
    documents: Vec<HashMap<String, Value>>
) -> Result<Vec<u64>>;

fn delete_document(index: &Index, doc_id: u64) -> Result<()>;

fn delete_documents_by_query(
    index: &Index,
    query: &str,
    query_type: QueryType
) -> Result<usize>;

fn get_document(index: &Index, doc_id: u64)
    -> Result<HashMap<String, Value>>;

fn commit(index: &Index) -> Result<()>;

// 搜索
fn search(index: &Index, request: SearchRequest)
    -> Result<SearchResponse>;

// 迭代器
fn get_iterator(index: &Index, skip: usize, take: usize)
    -> Result<Vec<HashMap<String, Value>>>;
```

### 9.2 HTTP API

| 方法   | 路径                     | 描述                          |
|--------|--------------------------|-------------------------------|
| POST   | /index/create            | 创建索引                      |
| POST   | /index/open              | 打开索引                      |
| DELETE | /index/{path}            | 删除索引                      |
| POST   | /document                | 索引单个文档                  |
| POST   | /documents               | 批量索引文档                  |
| DELETE | /document/{doc_id}       | 删除文档                      |
| GET    | /document/{doc_id}       | 获取文档                      |
| POST   | /commit                  | 提交更改                      |
| POST   | /search                  | 执行搜索                      |

## 10. 性能优化策略

### 10.1 内存优化

- **Posting lists**: 压缩存储，按需解码
- **Vector levels**: 序列化存储，查询时反序列化
- **Facet字典**: 增量更新，合并写入

### 10.2 查询优化

- **WAND剪枝**: 基于max_block_score跳过低分块
- **Top-K MinHeap**: 固定大小堆，O(log k)操作
- **IVF聚类**: 减少向量搜索范围
- **并行查询**: 词法和向量搜索可并行执行

### 10.3 存储优化

- **自适应压缩**: 根据posting密度选择压缩算法
- **批量写入**: commit时批量写入SlateDB
- **压缩文档**: 使用Snappy/LZ4/Zstd压缩

## 11. 扩展性设计

### 11.1 水平扩展（未来）

当前架构为单机设计，未来可扩展：

1. **分片策略**:
   - 按doc_id范围分片
   - 按term hash分片
   - 一致性哈希

2. **分布式查询**:
   - 并行查询各分片
   - 合并结果并重新排序

### 11.2 功能扩展

- **Embedding模型**: 支持多种模型接口
- **自定义分词器**: 插件化分词器接口
- **自定义评分**: 可配置的评分函数
- **过滤器**: 丰富的过滤条件支持

## 12. 依赖清单

### 12.1 核心依赖

```
slatedb          # KV存储引擎
object_store     # 对象存储抽象
tokio            # 异步运行时
serde            # 序列化
serde_json       # JSON支持
```

### 12.2 搜索依赖

```
ahash            # 快速哈希表
indexmap         # 有序哈希表
rayon            # 并行处理
itertools        # 迭代器工具
smallvec         # 小向量优化
```

### 12.3 压缩依赖

```
snap             # Snappy压缩
lz4_flex         # LZ4压缩
zstd             # Zstd压缩
```

### 12.4 NLP依赖（可选）

```
jieba-rs         # 中文分词
snowball_stemmers_rs  # 英文词干化
```

### 12.5 服务器依赖

```
axum             # HTTP框架
utoipa           # OpenAPI文档
```

## 13. 测试策略

### 13.1 测试层级

1. **单元测试**: 每个模块的核心函数
2. **集成测试**: 完整的索引-搜索-删除流程
3. **性能测试**: 基准测试和性能回归检测
4. **服务器测试**: HTTP API集成测试

### 13.2 测试覆盖

- 压缩算法往返测试
- 分词器输出验证
- 搜索结果正确性
- RRF融合正确性
- 并发安全性

## 14. 安全考虑

### 14.1 输入验证

- 查询字符串长度限制
- 向量维度验证
- 字段名称验证
- JSON大小限制

### 14.2 资源限制

- 最大文档大小
- 查询超时
- 内存使用限制
- 并发查询限制

## 15. 监控和可观测性

### 15.1 指标

- 文档索引速度
- 查询延迟分布
- 缓存命中率
- 压缩比率

### 15.2 日志

- 错误日志
- 慢查询日志
- 操作审计日志
