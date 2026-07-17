

# SeekPower 架构设计（v2）

> **当前状态真相** — 本文档描述 SeekPower 引擎的设计方式。
> 所有章节描述当前设计或即将实现的架构。代码实现按阶段推进。

---

## 目录

- [§1 概述与定位](#1-概述与定位)
- [§2 Workspace 与 Crate 划分](#2-workspace-与-crate-划分)
- [§3 对象存储层](#3-对象存储层)
- [§4 词法搜索（自研倒排索引）](#4-词法搜索自研倒排索引)
- [§5 向量搜索（RaBitQ）](#5-向量搜索rabitq)
- [§6 混合搜索与融合](#6-混合搜索与融合)
- [§7 IndexManifest 元数据](#7-indexmanifest-元数据)
- [§8 对象存储 Key 布局](#8-对象存储-key-布局)
- [§9 Schema 与类型系统](#9-schema-与类型系统)
- [§10 并发模型](#10-并发模型)
- [§11 错误处理](#11-错误处理)
- [§12 HTTP 服务器](#12-http-服务器)
- [§13 核心不变量](#13-核心不变量)
- [§14 已知限制与差距](#14-已知限制与差距)

---

## §1 概述与定位

SeekPower 是一个**对象存储原生的嵌入式混合搜索引擎**，使用 Rust 编写。

**核心能力**：BM25F 词法搜索（自研倒排索引） + RaBitQ 向量搜索 + 加权 RRF 融合。

**核心架构决策**：
- 词法搜索采用自研倒排索引，支持 BM25F、Phrase 查询、WAND 动态剪枝。
- 向量搜索使用 RaBitQ（随机正交旋转 + 1-bit 量化），无需存储全精度向量。
- 索引组织为不可变段（Segment），段合并后台进行。
- 文档原文独立存储于对象存储，与索引分离。

### 设计目标

| 目标 | 度量 |
|---|---|
| 索引吞吐量 | > 10,000 docs/sec (批量) |
| 词法搜索延迟 | < 50ms (P99, 1000万文档) |
| 向量搜索延迟 | < 100ms (P99, nprobe=10, 1000万向量) |
| 内存占用 | < 1GB (1000万文档，不含文档原文缓存) |
| 召回率 | 向量搜索 recall@10 > 95% (nprobe=10) |

---

## §2 Workspace 与 Crate 划分

```
seekpower/
├── seekpower-core        # 核心引擎：索引管理、搜索协调、Manifest、段合并
├── seekpower-lexical     # 自研倒排索引：词项词典、倒排列表、查询执行
├── seekpower-vector      # RaBitQ 向量索引
├── seekpower-schema      # Schema 定义、文档值模型
├── seekpower-objectstore # 对象存储 trait 及实现 (FS, S3, InMemory, 缓存)
├── seekpower-docstore    # 文档原文存储
├── seekpower-server      # HTTP / gRPC 服务
└── seekpower-cli         # 命令行工具
```

不再依赖 Tantivy。

---

## §3 对象存储层

不变，沿用原文档的 `ObjectStore` trait、内容寻址、三种后端、装饰器栈。

---

## §4 词法搜索（自研倒排索引）

自研倒排索引栈，直接解决之前列举的 P0/P1 缺陷，达到与 Tantivy 同等甚至更优的能力。

### 4.1 核心设计

- **按字段索引**：每个文本字段独立构建倒排索引，支持 BM25F（字段级 avgdl、字段权重）。
- **不可变段（LexicalSegment）**：每批写入生成一个不可变的词法段，查询时合并多个段的结果。
- **段内结构**：FST 词项字典 + 倒排列表（包含 doc_id、term_freq、positions）。
- **查询引擎**：支持 Union、Intersection、Phrase、WAND 动态剪枝。

### 4.2 段文件格式

一个词法段在对象存储中对应一个文件 `segments/seg_{id}/lex.bin`，内部布局：

```
┌──────────────────────────────────┐
│ Header (segment_id, doc_count,  │
│         field_metadata, ...)     │
├──────────────────────────────────┤
│ Per-Field Indexes (offset table) │
├──────────────────────────────────┤
│ Field A:                         │
│  - FST (term -> postings_ptr)   │
│  - Postings data:                │
│      For each term:              │
│        - doc_count               │
│        - doc_id list (delta +    │
│          varint)                 │
│        - term_freq list (varint) │
│        - positions list (delta + │
│          varint per doc)         │
│        - optional: field_length  │
│          per doc                  │
│  - Field statistics:             │
│      - sum_total_tf, avg_doclen  │
│      - doc_count for BM25F      │
├──────────────────────────────────┤
│ Field B: ...                     │
└──────────────────────────────────┘
```

**词项字典**：使用 `fst` crate 构建，支持前缀搜索和范围搜索，占用内存小，支持 mmap 或直接读取字节块。

**倒排列表压缩**：doc_id 使用 delta + SIMD varint（如 stream-vbyte），term_freq 和 position 使用 varint。

**删除处理**：通过全局 delete bitmap（在 manifest 中）来过滤段内已删除文档，查询时检查 doc_id 是否在删除集。段合并时物理清除删除。

### 4.3 写入路径

1. `index_document` → 分词、计算词频、收集位置。
2. 存入内存缓冲 `InMemoryLexicalBuffer`（按 field、term 聚合）。
3. `commit` 触发：
    - 构建 FST、压缩倒排列表。
    - 写入段文件到 `lex.bin`。
    - 更新 manifest 中词法段列表。
    - 若缓冲数据量达到阈值（如 10MB 或 1万文档），也会自动生成段。

### 4.4 查询执行

**查询表示**：`LexicalQuery` enum，支持：
- `Term(field, word)`
- `Phrase(field, words, slop)`
- `Boolean { must, should, must_not }`（AND / OR / NOT）
- `MatchAll`

**执行流程**（以 OR 查询为例）：
1. 从 manifest 获取所有活跃词法段。
2. 对每个段，加载其 FST（可缓存），定位词项的倒排列表。
3. 使用 WAND 迭代器动态合并 doc_id，按 BM25F 计算 score。
4. 各段结果通过 ScoreHeap 合并，最终返回 Top-K 的 `doc_id` 和分数。

**BM25F 计算**：
- 统计每个字段的 `avg_doclen` 和文档总数。
- 字段权重可配置，混合字段得分：`score = Σ (weight_field * BM25(term_freq, doc_len, field_avg_len, ...))`。

**Phrase 查询**：利用 positions 列表，检查多个词项在文档内的位置偏移是否满足 slop。不再退化为 AND。

### 4.5 性能保证
- 索引吞吐：利用 Rust 高性能结构，批量构建 FST，目标 >10k docs/s。
- 搜索延迟：FST 查找 O(len(term))，倒排列表解压使用 SIMD，WAND 剪枝大幅减少需要计算分数的文档数。
- 内存占用：FST 自身仅几十 MB（百万词项），倒排列表按需加载（通过 ObjectStore range read），配合 LRU 缓存。

### 4.6 与 Tantivy 对比优势

| 能力 | 自研倒排索引 | Tantivy（之前方案） |
|---|---|---|
| 自定义存储 | 原生 ObjectStore 集成，数据直写对象存储 | 需适配 Directory trait |
| 锁与并发 | 无外部写锁，段文件生成后不可变 | IndexWriter 单线程，分布式需额外锁 |
| 段布局控制 | 自主设计，利于缓存和并行 | 依赖 Tantivy 内部 segment 布局 |
| 依赖体积 | 纯 Rust，少量依赖（fst, varint, 分词器） | 引入 tantivy 全家桶（~20+ crate） |

分词器复用现有设计（Jieba、Simple、Unicode 等），通过 `Tokenizer` trait 统一。

---

## §5 向量搜索（RaBitQ）

采用 RaBitQ：随机正交旋转 + 1-bit 量化。无需存储全精度向量，距离修正可精确估计。

### 5.1 索引存储

每个向量段 `segments/seg_{id}/vec.bin`：

- 段头：随机种子、聚类中心信息（二值码 + 校正参数）、向量数、维度等。
- 向量数据：所有向量的二值码连续打包（dim bits/vec）。
- 可选：RaBitQ 残差信息用于两阶段重排序（按需实现）。

### 5.2 查询流程

1. 用随机种子旋转查询向量。
2. 计算与聚类中心汉明距离，选 probe 个聚类。
3. 加载聚类内二值码，计算汉明距离作为近似距离。
4. 利用 RaBitQ 公式校正距离，输出近似准确距离。
5. Top-K 合并。

recall@10 > 95% 通过足够的 probe 数量保证。

---

## §6 混合搜索与融合

不变，仍使用加权 RRF 融合词法和向量分数。融合时只返回 `doc_id` 和融合分数，原文由上层按需通过 `DocStore` 拉取。

---

## §7 IndexManifest 元数据

调整为基于不可变段的管理：

```rust
struct IndexManifest {
    format_version: u32,
    index_name: String,
    index_id: u32,
    schema: Schema,
    config: IndexConfig,
    stats: IndexStats,
    generation: u64,
    segments: Vec<SegmentMeta>,      // 所有活跃段（词法+向量联合）
    delete_set: Option<DeleteSet>,   // 全局已删除 doc_id 集合（位图或 Roaring）
    created_at_ms: i64,
    updated_at_ms: i64,
}

struct SegmentMeta {
    seg_id: u64,
    doc_count: u64,
    min_doc_id: u64,
    max_doc_id: u64,
    lexical: Option<LexicalSegmentRef>,   // 若该段包含词法数据
    vector: Option<VectorSegmentRef>,      // 若该段包含向量数据
}

struct LexicalSegmentRef {
    key: String,          // "segments/seg_{id}/lex.bin"
    size_bytes: u64,
    // 可选：词项数、字段统计等
}

struct VectorSegmentRef {
    key: String,          // "segments/seg_{id}/vec.bin"
    size_bytes: u64,
    num_vectors: u64,
}
```

- 一段可能同时包含词法和向量数据（如果同时索引了文本和向量字段），由同一个 `seg_id` 关联。
- `delete_set` 记录已删除的 doc_id，各段查询时用于过滤。

**ManifestStore** 接口不变。

---

## §8 对象存储 Key 布局

```
indexes/{index_name}/
  manifest.json
  segments/
    seg_{00001}/
      lex.bin               # 词法索引段
      vec.bin               # 向量索引段
      field_meta.json       # 可选：字段长度统计等
  docs/                     # 文档原文存储
    doc_{doc_id}.json       # 或分块存储
registry.json
```

**段 ID 单调递增**，永不重用。段合并时生成新的大段，旧段标记为 obsolete，随后异步删除。

---

## §9 Schema 与类型系统

基本不变，依然支持 Text、U64、I64、F64、Bool、Timestamp、Vector 等。字段权重、分词器配置保留。

---

## §10 并发模型

调整为：

```
SeekPower:  store(Arc)  indexes(RwLock)
Index:      manifest(RwLock)  stats(RwLock)
            lexical: LexicalIndex (包含 segment 缓存、删除集)
            vector: VectorIndex (包含 segment 缓存)
            docstore: DocStore (Arc)
            next_doc_id: AtomicU64
            buffer: Mutex<IndexBuffer>  (词法+向量缓冲)
```

- 写入时加 buffer 锁，追加词条和向量到内存缓冲。
- commit 时根据 buffer 生成新段，更新 manifest（写锁），清空 buffer。
- 查询时获取 manifest 读锁，收集活跃段列表，然后释放锁，并行读取各段数据（段数据不可变，无需锁）。

---

## §11 错误处理 / §12 HTTP 服务器 / §13 核心不变量 / §14 已知限制

这些部分需根据新设计微调，但结构保持一致。例如 HTTP 接口提供：

- `POST /{index}/search` 混合搜索
- `POST /{index}/documents` 批量索引
- `GET /{index}/documents/{id}` 获取原文

已知限制补充：自研词法索引第一阶段可能缺少 WAND 的部分高级优化，将在后续迭代中完善。

---

## 总结

这份修订后的设计文档全面贯彻了你的决策：
- **自研词法索引**，完全掌控存储和执行，与对象存储深度集成。
- **RaBitQ 向量索引**，超高压缩，无全精度样本。
- **不可变段 + 段合并**，实现弹性写入和查询。
- **文档独立存储**，分离索引与原文。
- 架构保持嵌入式优先，未来可无损升级到分布式版本。
