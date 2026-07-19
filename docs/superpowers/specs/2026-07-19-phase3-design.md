# SeekPower v3 阶段三设计文档

> 文档存储与查询 - 核心存储层
>
> **周期**：3 周
>
> **目标**：实现文档原文管理，作为系统的主存储
>
> **依赖**：Phase 1（对象存储）, Phase 2（Schema）

---

## 目录

- [§1 阶段目标](#1-阶段目标)
- [§2 核心架构](#2-核心架构)
- [§3 数据结构](#3-数据结构)
- [§4 核心 API](#4-核心-api)
- [§5 文档处理流程](#5-文档处理流程)
- [§6 分块策略](#6-分块策略)
- [§7 并发控制](#7-并发控制)
- [§8 错误处理](#8-错误处理)
- [§9 验收标准](#9-验收标准)
- [§10 目录结构](#10-目录结构)
- [§11 技术栈](#11-技术栈)

---

## §1 阶段目标

### 1.1 总体目标

实现文档原文管理，作为系统的主存储层。提供可靠的 CRUD 操作，支持文档写入、读取、删除、更新，以及大文档分块和按需加载。

### 1.2 关键成果

- Document 结构体定义（依赖 Phase 2 Schema）
- 文档存储 API
- 文档查询 API
- 软删除机制 + 删除集管理
- 5MB 分块策略
- 文档更新机制
- AtomicU64 的 doc_id 分配器

### 1.3 核心价值

- **稳定依赖**：为后续索引阶段（Phase 4、5、6）提供稳定的文档 API
- **Schema-Free 集成**：与 Phase 2 的 Schema 管理无缝集成
- **大文档支持**：自动分块策略，支持任意大小的文档
- **并发安全**：RwLock 并发控制，支持高并发读写

---

## §2 核心架构

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                     DocumentStore                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  next_doc_id: AtomicU64 (doc_id 分配器)                 │  │
│  │  schema: Arc<RwLock<Schema>> (Schema 引用)              │  │
│  │  delete_set: Arc<RwLock<RoaringBitmap>> (删除集)        │  │
│  └───────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                       ObjectStore                            │
│              (Phase 1: S3/FS/Memory)                         │
├─────────────────────────────────────────────────────────────┤
│                       Schema                                 │
│              (Phase 2: Schema-Free)                          │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 设计原则

| 原则 | 说明 |
|------|------|
| **Schema-Free 集成** | Document 是 JSON 值的包装，依赖 Schema 理解结构 |
| **基础 CRUD 优先** | 先实现可靠的文档写入、读取、删除，再考虑高级功能 |
| **软删除机制** | 删除集标记，后台任务定期清理 |
| **并发安全** | RwLock 保护共享状态，doc_id 原子操作 |
| **大文档分块** | 5MB 阈值自动分块，按需加载 |

---

## §3 数据结构

### 3.1 Document 结构

```rust
/// 文档结构（依赖 Schema）
///
/// Document 是 `serde_json::Value` 的包装，字段结构由 Schema 理解。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// 文档 ID（全局唯一）
    pub doc_id: u64,
    /// 文档字段（JSON 值，由 Schema 理解结构）
    pub fields: serde_json::Value,
    /// 时间戳（毫秒）
    pub timestamp: u64,
}

impl Document {
    /// 从 JSON 值创建文档（不含 doc_id，写入时自动分配）
    pub fn from_json(fields: serde_json::Value) -> Self {
        Self {
            doc_id: 0,
            fields,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        }
    }
}
```

### 3.2 分块元数据

```rust
/// 分块元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    /// 文档 ID
    pub doc_id: u64,
    /// 分块总数
    pub total_chunks: u32,
    /// 分块大小（字节）
    pub chunk_size: usize,
    /// 创建时间（毫秒）
    pub created_at: u64,
}
```

### 3.3 DocumentStore 结构

```rust
/// 文档存储实例
pub struct DocumentStore {
    /// 对象存储引用
    storage: Arc<dyn ObjectStore>,
    /// 索引前缀
    index_prefix: String,
    /// Schema 引用
    schema: Arc<RwLock<Schema>>,
    /// 下一个 doc_id（原子计数器）
    next_doc_id: AtomicU64,
    /// 删除集
    delete_set: Arc<RwLock<RoaringBitmap>>,
}
```

---

## §4 核心 API

### 4.1 构造函数

```rust
impl DocumentStore {
    /// 创建文档存储实例
    ///
    /// # 参数
    /// - `storage`: 对象存储引用（Phase 1）
    /// - `schema`: Schema 引用（Phase 2）
    /// - `index_prefix`: 索引前缀（如 "tenant-123/indexes/my-index"）
    pub async fn new(
        storage: Arc<dyn ObjectStore>,
        schema: Arc<RwLock<Schema>>,
        index_prefix: String,
    ) -> Result<Self, DocumentError> {
        // 从存储加载删除集
        // 初始化 doc_id 分配器
        Ok(Self { ... })
    }
}
```

### 4.2 写入 API

```rust
impl DocumentStore {
    /// 写入文档（自动分配 doc_id）
    ///
    /// # 返回
    /// - 成功：返回分配的 doc_id
    /// - 失败：返回 DocumentError
    pub async fn add_document(&self, doc: &Document) -> Result<u64, DocumentError> {
        // 1. 通过 SchemaManager.process_document 更新 Schema
        // 2. 分配 doc_id
        // 3. 检查文档大小，决定是否分块
        // 4. 存储文档文件
        // 5. 返回 doc_id
    }

    /// 批量写入文档
    ///
    /// 返回所有分配的 doc_id 列表
    pub async fn add_documents(&self, docs: &[Document]) -> Result<Vec<u64>, DocumentError> {
        // 并行处理多个文档
        // 返回所有 doc_id
    }
}
```

### 4.3 读取 API

```rust
impl DocumentStore {
    /// 读取文档（按需加载分块）
    ///
    /// # 返回
    /// - 文档存在且未删除：返回 Some(Document)
    /// - 文档不存在或已删除：返回 None
    pub async fn get_document(&self, doc_id: u64) -> Result<Option<Document>, DocumentError> {
        // 1. 检查删除集
        // 2. 读取文档文件
        // 3. 如果是分块文档，合并分块
        // 4. 返回 Document
    }

    /// 批量读取文档（并行）
    ///
    /// 返回与输入顺序对应的文档列表（缺失的为 None）
    pub async fn get_documents(&self, doc_ids: &[u64]) -> Result<Vec<Option<Document>>, DocumentError> {
        // 并行读取多个文档
        // 保持输入顺序
    }
}
```

### 4.4 更新 API

```rust
impl DocumentStore {
    /// 更新文档
    ///
    /// 覆盖整个文档，doc_id 不变
    pub async fn update_document(&self, doc_id: u64, doc: &Document) -> Result<(), DocumentError> {
        // 1. 检查文档是否存在
        // 2. 通过 SchemaManager.process_document 更新 Schema
        // 3. 覆盖文档文件
        // 4. 如果是分块文档，重新分块
    }
}
```

### 4.5 删除 API

```rust
impl DocumentStore {
    /// 删除文档（软删除）
    ///
    /// 将 doc_id 加入删除集，不立即删除文件
    pub async fn delete_document(&self, doc_id: u64) -> Result<(), DocumentError> {
        // 1. 检查文档是否存在
        // 2. 将 doc_id 加入删除集（RwLock 写锁）
        // 3. 返回成功
    }

    /// 批量删除文档
    pub async fn delete_documents(&self, doc_ids: &[u64]) -> Result<(), DocumentError> {
        // 批量将 doc_id 加入删除集
    }

    /// 检查文档是否已删除
    pub fn is_deleted(&self, doc_id: u64) -> bool {
        self.delete_set.read().unwrap().contains(doc_id)
    }

    /// 持久化删除集（定期调用）
    pub async fn flush_delete_set(&self) -> Result<(), DocumentError> {
        // 将删除集持久化到对象存储
    }
}
```

---

## §5 文档处理流程

### 5.1 写入流程

```
1. 接收 Document（不含 doc_id）
   ↓
2. 通过 SchemaManager.process_document 更新 Schema（Phase 2）
   ↓
3. 分配 doc_id（AtomicU64.fetch_add）
   ↓
4. 检查文档大小
   ├── ≤5MB：直接存储为 doc_{doc_id}.json
   └── >5MB：分块存储
       ├── 创建 chunks/doc_{doc_id}/ 目录
       ├── 分块存储 chunk_0.json, chunk_1.json, ...
       └── 创建 metadata.json 记录分块信息
   ↓
5. 返回 doc_id
```

### 5.2 读取流程

```
1. 接收 doc_id
   ↓
2. 检查删除集（如果在删除集中，返回 None）
   ↓
3. 读取文档文件
   ├── 单文件：直接读取 doc_{doc_id}.json
   └── 分块文件：读取 metadata.json，然后并行读取所有 chunk
   ↓
4. 反序列化为 Document
   ↓
5. 返回 Document
```

### 5.3 删除流程（软删除）

```
1. 将 doc_id 加入删除集（RwLock 写锁）
   ↓
2. 返回成功
   ↓
3. 后台任务定期清理：
   ├── 将文档移动到 docs/deleted/ 目录
   └── 保留一段时间后彻底删除
```

### 5.4 更新流程

```
1. 接收 doc_id 和新 Document
   ↓
2. 检查文档是否存在
   ↓
3. 通过 SchemaManager.process_document 更新 Schema
   ↓
4. 检查新文档大小，决定是否分块
   ↓
5. 覆盖旧文档文件
   ↓
6. 返回成功
```

---

## §6 分块策略

### 6.1 分块规则

| 参数 | 值 | 说明 |
|------|---|------|
| 阈值 | 5MB | 文档大小超过 5MB 时触发分块 |
| 分块大小 | ~5MB | 每个分块约 5MB |
| 存储位置 | `chunks/doc_{doc_id}/` | 分块文件存储目录 |
| 元数据文件 | `chunks/doc_{doc_id}/metadata.json` | 记录分块信息 |

### 6.2 分块实现

```rust
impl DocumentStore {
    const CHUNK_SIZE_THRESHOLD: usize = 5 * 1024 * 1024; // 5MB

    /// 检查文档是否需要分块
    fn should_chunk(doc: &Document) -> bool {
        let size = serde_json::to_vec(doc).map(|b| b.len()).unwrap_or(0);
        size > Self::CHUNK_SIZE_THRESHOLD
    }

    /// 分块文档
    async fn chunk_document(&self, doc: &Document, doc_id: u64) -> Result<(), DocumentError> {
        let data = serde_json::to_vec(doc)?;
        let total_chunks = (data.len() / Self::CHUNK_SIZE_THRESHOLD) + 1;

        // 创建分块元数据
        let metadata = ChunkMetadata {
            doc_id,
            total_chunks: total_chunks as u32,
            chunk_size: Self::CHUNK_SIZE_THRESHOLD,
            created_at: doc.timestamp,
        };

        // 存储 metadata.json
        let meta_path = format!("{}/chunks/doc_{}/metadata.json", self.index_prefix, doc_id);
        let meta_data = serde_json::to_vec(&metadata)?;
        self.storage.put(&meta_path, meta_data.into()).await?;

        // 分块存储
        for (i, chunk) in data.chunks(Self::CHUNK_SIZE_THRESHOLD).enumerate() {
            let chunk_path = format!("{}/chunks/doc_{}/chunk_{}.json", self.index_prefix, doc_id, i);
            self.storage.put(&chunk_path, chunk.to_vec().into()).await?;
        }

        Ok(())
    }

    /// 合并分块
    async fn merge_chunks(&self, doc_id: u64) -> Result<Document, DocumentError> {
        // 读取 metadata.json
        let meta_path = format!("{}/chunks/doc_{}/metadata.json", self.index_prefix, doc_id);
        let meta_data = self.storage.get(&meta_path).await?;
        let metadata: ChunkMetadata = serde_json::from_slice(&meta_data)?;

        // 并行读取所有 chunk 文件
        let mut chunks = Vec::with_capacity(metadata.total_chunks as usize);
        for i in 0..metadata.total_chunks {
            let chunk_path = format!("{}/chunks/doc_{}/chunk_{}.json", self.index_prefix, doc_id, i);
            let chunk_data = self.storage.get(&chunk_path).await?;
            chunks.push(chunk_data);
        }

        // 合并为完整 Document
        let full_data = chunks.into_iter().flatten().collect::<Vec<_>>();
        let doc: Document = serde_json::from_slice(&full_data)?;

        Ok(doc)
    }
}
```

### 6.3 存储布局

```
{index_prefix}/docs/
├── doc_1.json                    # 单文档存储
├── doc_2.json
├── deleted/                      # 已删除文档（延迟清理）
│   └── doc_3.json
└── chunks/                       # 大文档分块
    └── doc_4/
        ├── metadata.json         # 分块元数据
        ├── chunk_0.json
        ├── chunk_1.json
        └── chunk_2.json
```

---

## §7 并发控制

### 7.1 锁策略

| 资源 | 锁类型 | 用途 |
|------|--------|------|
| next_doc_id | AtomicU64 | doc_id 分配，无锁原子操作 |
| schema | RwLock | Schema 读取（读锁）和更新（写锁） |
| delete_set | RwLock | 删除集读取（读锁）和添加（写锁） |

### 7.2 并发实现

```rust
impl DocumentStore {
    /// 分配 doc_id（原子操作，无需锁）
    fn allocate_doc_id(&self) -> u64 {
        self.next_doc_id.fetch_add(1, Ordering::SeqCst)
    }

    /// 检查删除状态（读锁，允许并发读）
    pub fn is_deleted(&self, doc_id: u64) -> bool {
        self.delete_set.read().unwrap().contains(doc_id)
    }

    /// 添加到删除集（写锁，阻塞其他写）
    pub async fn delete_document(&self, doc_id: u64) -> Result<(), DocumentError> {
        let mut delete_set = self.delete_set.write().unwrap();
        delete_set.insert(doc_id);
        Ok(())
    }

    /// 获取 Schema 快照（读锁，允许并发读）
    pub fn get_schema(&self) -> Schema {
        self.schema.read().unwrap().clone()
    }

    /// 批量删除（单次写锁）
    pub async fn delete_documents(&self, doc_ids: &[u64]) -> Result<(), DocumentError> {
        let mut delete_set = self.delete_set.write().unwrap();
        for &doc_id in doc_ids {
            delete_set.insert(doc_id);
        }
        Ok(())
    }
}
```

### 7.3 并发安全保证

- **doc_id 唯一性**：AtomicU64.fetch_add 保证全局唯一
- **删除集一致性**：RwLock 写锁保证删除操作原子性
- **Schema 读取安全**：RwLock 读锁允许并发读取
- **对象存储安全**：OpenDAL 本身并发安全

---

## §8 错误处理

### 8.1 DocumentError 枚举

```rust
#[derive(Debug, thiserror::Error)]
pub enum DocumentError {
    /// 文档未找到
    #[error("Document not found: {doc_id}")]
    NotFound { doc_id: u64 },

    /// 文档已删除
    #[error("Document deleted: {doc_id}")]
    AlreadyDeleted { doc_id: u64 },

    /// 分块失败
    #[error("Failed to chunk document: {reason}")]
    ChunkFailed { reason: String },

    /// 合并分块失败
    #[error("Failed to merge chunks: {reason}")]
    MergeFailed { reason: String },

    /// 序列化失败
    #[error("Serialization failed: {reason}")]
    SerializationFailed { reason: String },

    /// 反序列化失败
    #[error("Deserialization failed: {reason}")]
    DeserializationFailed { reason: String },

    /// 存储错误
    #[error("Storage error: {reason}")]
    StorageError { reason: String },

    /// Schema 错误
    #[error("Schema error: {reason}")]
    SchemaError { reason: String },
}
```

### 8.2 错误转换

```rust
impl From<StoreError> for DocumentError {
    fn from(e: StoreError) -> Self {
        DocumentError::StorageError { reason: e.to_string() }
    }
}

impl From<SchemaError> for DocumentError {
    fn from(e: SchemaError) -> Self {
        DocumentError::SchemaError { reason: e.to_string() }
    }
}

impl From<serde_json::Error> for DocumentError {
    fn from(e: serde_json::Error) -> Self {
        if e.is_io() {
            DocumentError::SerializationFailed { reason: e.to_string() }
        } else {
            DocumentError::DeserializationFailed { reason: e.to_string() }
        }
    }
}
```

---

## §9 验收标准

### 9.1 功能测试

- [ ] 文档写入成功，返回正确的 doc_id
- [ ] doc_id 全局唯一且递增
- [ ] 文档读取返回正确的 Document
- [ ] 批量读写正确处理
- [ ] 文档更新正确覆盖原数据
- [ ] 软删除后查询返回 None
- [ ] 删除集正确维护
- [ ] is_deleted 正确返回状态

### 9.2 分块测试

- [ ] <5MB 文档不分块
- [ ] >5MB 文档正确分块
- [ ] 分块文档读取后正确合并
- [ ] 分块元数据正确记录

### 9.3 并发测试

- [ ] 多线程并发写入，doc_id 不重复
- [ ] 多线程并发读取，无死锁
- [ ] 读写并发，数据一致性正确

### 9.4 集成测试

- [ ] 与 Schema 集成正常
- [ ] 与 ObjectStore 集成正常
- [ ] S3/FS 两种后端都通过测试

### 9.5 性能目标

| 指标 | 目标值 | 测试场景 |
|------|--------|----------|
| 单文档写入 | < 10ms | 1KB 文档 |
| 单文档读取 | < 50ms | 1KB 文档 |
| 批量写入吞吐 | > 5,000 docs/sec | 批量写入 1000 个文档 |
| 并发读取吞吐 | > 10,000 docs/sec | 10 个线程并发读取 |

---

## §10 目录结构

```
seekpower/
├── crates/
│   ├── seekpower-error/
│   │   └── src/lib.rs                # 错误类型（含 DocumentError）
│   │
│   ├── seekpower-storage/
│   │   └── src/
│   │       ├── lib.rs                # ObjectStore trait
│   │       ├── s3_store.rs
│   │       └── fs_store.rs
│   │
│   ├── seekpower-schema/
│   │   └── src/
│   │       ├── lib.rs                # Schema 相关
│   │       ├── schema_manager.rs
│   │       └── ...
│   │
│   └── seekpower-document/           # 新建 crate
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs                # 重新导出
│           ├── document.rs           # Document 结构
│           ├── chunk_metadata.rs     # ChunkMetadata
│           ├── document_store.rs     # DocumentStore 实现
│           ├── error.rs              # DocumentError
│           └── chunk.rs              # 分块逻辑
│
└── docs/
    ├── ARCHITECTURE_V3.md
    ├── PHASE_1_DESIGN.md
    ├── PHASE_2.md
    ├── PHASE_3.md                    # 本文档
    └── superpowers/
        └── specs/
            └── 2026-07-19-phase3-design.md  # 本文档
```

---

## §11 技术栈

| 组件 | 技术 | 理由 |
|------|------|------|
| 序列化 | serde/serde_json | 标准 JSON 序列化 |
| 并发 | tokio + RwLock | 异步运行时 + 读写锁 |
| 删除集 | roaring-rs | 高效位图压缩 |
| 错误处理 | thiserror | 标准错误处理 |
| 存储 | seekpower-storage | Phase 1 的对象存储 |
| Schema | seekpower-schema | Phase 2 的 Schema |

---

**文档版本**: v1.0
**创建时间**: 2026-07-19
**作者**: Claude (Phase 3 Design)
**状态**: 设计完成，待用户审查