# SeekPower v3 阶段二：Schema-Free 设计与实现

> Schema-Free 设计与自动推断
>
> **周期**：3 周
>
> **目标**：实现无需预定义 schema 的自动字段类型推断机制
>
> **版本**: v1.2 (已整合设计审查修正)

---

## 目录

- [§1 阶段目标](#1-阶段目标)
- [§2 核心任务](#2-核心任务)
- [§3 设计原则](#3-设计原则)
- [§4 FieldType 类型系统](#4-fieldtype-类型系统)
- [§5 自动推断逻辑](#5-自动推断逻辑)
- [§6 类型收敛规则](#6-类型收敛规则)
- [§7 Schema 持久化](#7-schema-持久化)
- [§8 Schema 演化](#8-schema-演化)
- [§9 Schema API 设计](#9-schema-api-设计)
- [§10 向量维度约束](#10-向量维度约束)
- [§11 错误处理](#11-错误处理)
- [§12 目录结构](#12-目录结构)
- [§13 验收标准](#13-验收标准)
- [附录A：设计审查与修正](#附录a设计审查与修正)
- [附录B：使用示例](#附录b使用示例)
- [附录C：Schema 文件格式](#附录c-schema-文件格式)

---

## §1 阶段目标

### 1.1 总体目标

实现 Schema-Free 机制，支持无需预定义 schema 的自动字段类型推断，降低使用门槛，提升开发体验。

### 1.2 关键成果

- 完整的 FieldType 类型系统
- 自动字段类型推断引擎
- 类型冲突的收敛规则
- Schema 持久化和版本化机制
- 向量维度固定后的一致性保证
- Schema 查询和管理 API

### 1.3 核心价值

- **零配置启动**：无需预定义 schema，开箱即用
- **智能推断**：自动识别字段类型，减少手动配置
- **渐进式类型**：支持类型演化，向后兼容
- **类型安全**：收敛规则保证数据一致性

---

## §2 核心任务

### 2.1 任务清单

1. 实现 FieldType 类型定义
2. 实现字段类型推断逻辑
3. 实现类型收敛规则引擎
4. 实现 Schema 持久化机制
5. 实现 Schema 演化和版本管理
6. 实现 Schema 查询 API
7. 实现向量维度约束验证
8. 编写单元测试

### 2.2 任务依赖

```
FieldType 定义 (任务1)
    ├── 类型推断逻辑 (任务2)
    ├── Schema 持久化 (任务4)
    ├── 类型收敛规则 (任务3)
    ├── 向量维度约束 (任务7)
    ├── Schema 演化 (任务5)
    └── Schema API (任务6)
```

---

## §3 设计原则

### 3.1 Schema-Free 原则

| 原则 | 说明 |
|------|------|
| **自动推断** | 首次写入时自动推断字段类型 |
| **类型收敛** | 类型冲突时升级为更通用类型 |
| **向后兼容** | 类型演化不影响已有数据 |
| **一致性优先** | 向量维度固定后不可更改 |
| **零配置** | 无需预定义 schema |

### 3.2 设计权衡

| 决策 | 选择 | 理由 |
|------|------|------|
| Schema 变更 | 自动演化 | 降低使用门槛，快速迭代 |
| 类型冲突 | 升级而非拒绝 | 兼容性优先，减少错误 |
| 向量维度 | 固定后不可改 | 保证索引一致性 |
| 类型推断 | 基于首值 | 简单快速，足够准确 |
| Schema 存储 | JSON 格式 | 可读性高，易于调试 |

---

## §4 FieldType 类型系统

### 4.1 FieldType 枚举

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldType {
    /// 文本类型（分词索引）
    Text,
    /// 关键词类型（精确匹配）
    Keyword,
    /// 64 位无符号整数
    U64,
    /// 64 位有符号整数
    I64,
    /// 64 位浮点数
    F64,
    /// 布尔值
    Bool,
    /// 时间戳（Unix 毫秒）
    Timestamp,
    /// 向量（固定维度）
    Vector(u32),
    /// JSON 对象（不索引，仅存储）
    Json,
}
```

### 4.2 类型特性

| 类型 | 索引方式 | 存储方式 | 典型值 |
|------|----------|----------|--------|
| Text | 分词倒排 | 原文存储 | "这是一段较长的文本内容" |
| Keyword | 精确倒排 | 原文存储 | "产品编号" |
| U64 | 数值倒排 | 8 字节 | 1234567890 |
| I64 | 数值倒排 | 8 字节 | -1234567890 |
| F64 | 数值倒排 | 8 字节 | 3.14159 |
| Bool | 布尔倒排 | 1 字节 | true |
| Timestamp | 数值倒排 | 8 字节 | 1698765432000 |
| Vector(dim) | 聚类索引 | dim×4 字节 | [0.1, 0.2, ...] |
| Json | **不索引** | JSON 原文 | {"nested": "value"} |

### 4.3 FieldMetadata 结构

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMetadata {
    /// 字段名称
    pub name: String,
    /// 字段类型
    pub r#type: FieldType,
    /// 是否索引
    ///
    /// 注意：Json 类型始终为 false
    pub indexed: bool,
    /// 是否存储
    pub stored: bool,
    /// 首次出现时间
    pub first_seen_at: SystemTime,
    /// 最后更新时间
    pub updated_at: SystemTime,
    /// 样本数量（用于推断验证）
    pub sample_count: u64,
}
```

### 4.4 Schema 结构

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Schema 版本
    pub version: u64,
    /// 字段映射：字段名 → 字段元数据
    pub fields: HashMap<String, FieldMetadata>,
    /// 创建时间
    pub created_at: SystemTime,
    /// 更新时间
    pub updated_at: SystemTime,
}
```

---

## §5 自动推断逻辑

### 5.1 推断策略（已修正）

```rust
impl FieldType {
    /// 推断字段类型
    ///
    /// 返回 None 表示无法推断（需要用户手动配置）
    pub fn infer(value: &serde_json::Value) -> Option<Self> {
        match value {
            // 短文本 → Keyword（< 100 字符）
            serde_json::Value::String(s) if s.len() < 100 => {
                Some(FieldType::Keyword)
            }
            // 长文本 → Text（>= 100 字符）
            serde_json::Value::String(_) => {
                Some(FieldType::Text)
            }
            // 纯数字数组 → Vector（2~4096 维）
            serde_json::Value::Array(arr) if arr.iter().all(|v| v.is_number()) => {
                let len = arr.len() as u32;
                if (2..=4096).contains(&len) {
                    Some(FieldType::Vector(len))
                } else {
                    None  // 维度不合理，可能是普通数组
                }
            }
            // 字符串数组 → Json（关键词集合）
            serde_json::Value::Array(arr) if arr.iter().all(|v| v.is_string()) => {
                Some(FieldType::Json)
            }
            // 其他数组 → None（不推断）
            serde_json::Value::Array(_) => None,
            // 数字（修正 U64 推断）
            serde_json::Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    Some(FieldType::U64)
                } else if let Some(i) = n.as_i64() {
                    Some(if i >= 0 { FieldType::U64 } else { FieldType::I64 })
                } else if n.is_f64() {
                    Some(FieldType::F64)
                } else {
                    None
                }
            }
            // 布尔值
            serde_json::Value::Bool(_) => Some(FieldType::Bool),
            // 对象 → Json
            serde_json::Value::Object(_) => Some(FieldType::Json),
            // Null → None
            serde_json::Value::Null => None,
        }
    }
}
```

### 5.2 推断示例

| 字段值 | 推断类型 | 理由 |
|--------|----------|------|
| `"user123"` | Keyword | 短文本，精确匹配 |
| `"这是一段较长的文本内容"` | Text | 长文本，需要分词 |
| `[0.1, 0.2, 0.3]` | Vector(3) | 纯数字数组，3 维 |
| `[1, 2, 3]` | Vector(3) | 纯数字数组，3 维 |
| `12345` | U64 | 非负整数 |
| `-12345` | I64 | 负整数 |
| `3.14159` | F64 | 浮点数 |
| `true` | Bool | 布尔值 |
| `{"nested": "value"}` | Json | 嵌套对象，不索引 |
| `["tag1", "tag2"]` | Json | 字符串数组，关键词集合 |
| `[1]` | None | 数组太短，非向量 |
| `[]` | None | 空数组，不推断 |
| `[1, "a"]` | None | 混合类型，不推断 |

---

## §6 类型收敛规则

### 6.1 收敛函数（已修正）

```rust
impl FieldType {
    /// 类型收敛：合并两个可能的类型
    ///
    /// 返回兼容两种输入的通用类型（兼容优先，不报错）
    ///
    /// 收敛规则：
    /// 1. 相同类型 → 直接返回
    /// 2. 文本冲突 → Text（支持分词是超集）
    /// 3. 数值冲突 → F64（浮点是最通用的数值类型）
    /// 4. 其他冲突 → Json（不索引的通用容器）
    pub fn converge(&self, other: &Self) -> Self {
        match (self, other) {
            // 相同类型，直接返回
            (t1, t2) if t1 == t2 => t1.clone(),

            // 文本升级：Keyword + Text → Text（分词是超集）
            (FieldType::Keyword, FieldType::Text) |
            (FieldType::Text, FieldType::Keyword) => FieldType::Text,

            // 数值升级：U64 + I64 → I64
            (FieldType::U64, FieldType::I64) |
            (FieldType::I64, FieldType::U64) => FieldType::I64,

            // 数值升级：U64 + F64 → F64
            (FieldType::U64, FieldType::F64) |
            (FieldType::F64, FieldType::U64) => FieldType::F64,

            // 数值升级：I64 + F64 → F64
            (FieldType::I64, FieldType::F64) |
            (FieldType::F64, FieldType::I64) => FieldType::F64,

            // 其他所有冲突 → Json（不报错，兼容优先）
            (_, _) => FieldType::Json,
        }
    }
}
```

### 6.2 收敛规则表

| 现有类型 | 新类型 | 收敛结果 | 说明 |
|----------|--------|----------|------|
| Keyword | Text | Text | 文本升级，支持分词 |
| Text | Keyword | Text | 已支持分词，无需降级 |
| I64 | F64 | F64 | 浮点覆盖整数 |
| F64 | I64 | F64 | 已支持浮点，无需降级 |
| U64 | I64 | I64 | 有符号覆盖无符号 |
| U64 | F64 | F64 | 浮点覆盖整数 |
| Vector(3) | Vector(4) | **Error** | 向量维度冲突（唯一报错情况） |
| Text | I64 | Json | 不兼容，退化到 Json |
| Keyword | Json | Json | Json 覆盖所有 |
| Bool | I64 | Json | 不兼容，退化到 Json |
| **任何非 Vector** | **Vector** | Json | Vector 只能与 Vector 收敛 |

**重要说明**：
- 向量维度冲突是**唯一**会报错的收敛情况
- 其他所有类型冲突都升级为 Json（兼容优先）
- Json 类型可以与任何类型收敛为 Json

### 6.3 收敛处理流程

```
1. 检查字段是否已存在
   ├── 否：直接推断并创建新字段
   └── 是：进行类型收敛
       ├── 类型相同：更新样本计数
       ├── 向量维度冲突：返回错误（拒绝写入）
       └── 其他冲突：升级为 Json，记录警告，允许写入
```

---

## §7 Schema 持久化

### 7.1 存储位置

Schema 文件存储在索引根目录下：

```
{index_prefix}/
└── schema.json          # 当前 schema（通过 OpenDAL）
```

### 7.2 持久化时机

| 操作 | 触发条件 |
|------|----------|
| Schema 更新 | 每次新增字段或类型演化后 |
| Schema 保存 | 异步批量保存，避免频繁 I/O |
| Schema 版本 | 每次保存时递增 version |

### 7.3 持久化实现

```rust
impl SchemaManager {
    /// 保存 Schema 到对象存储
    pub async fn save_to_storage(&self, schema: &Schema) -> Result<(), SchemaError> {
        let path = format!("{}/schema.json", self.index_prefix);

        // 序列化为 JSON
        let json = serde_json::to_vec(schema)
            .map_err(|e| SchemaError::SerializationError(e.to_string()))?;

        // 写入对象存储
        self.storage
            .put(&path, json)
            .await
            .map_err(|e| SchemaError::PersistenceError(e.to_string()))?;

        Ok(())
    }

    /// 从对象存储加载 Schema
    pub async fn load_schema(&self) -> Result<Option<Schema>, SchemaError> {
        let path = format!("{}/schema.json", self.index_prefix);

        match self.storage.get(&path).await {
            Ok(data) => {
                let schema: Schema = serde_json::from_slice(&data)
                    .map_err(|e| SchemaError::DeserializationError(e.to_string()))?;
                Ok(Some(schema))
            }
            Err(StoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(SchemaError::PersistenceError(e.to_string())),
        }
    }
}
```

---

## §8 Schema 演化

### 8.1 演化策略

Schema 支持以下演化操作：

| 操作 | 允许 | 说明 |
|------|------|------|
| 新增字段 | ✅ | 自动推断类型 |
| 类型升级 | ✅ | 通过收敛规则 |
| 删除字段 | ✅ | 标记为废弃，不立即删除 |
| 向量维度变更 | ❌ | 固定后不可改 |

### 8.2 演化流程

```
1. 接收文档
   ↓
2. 解析字段
   ↓
3. 检查字段是否存在于 Schema
   ├── 不存在：
   │   ├── 推断字段类型
   │   ├── 创建 FieldMetadata
   │   ├── 添加到 Schema
   │   └── 标记 Schema 需要保存
   └── 存在：
       ├── 检查类型是否一致
       ├── 类型相同：更新样本计数
       └── 类型不同：
           ├── 应用收敛规则
           ├── 升级字段类型
           ├── 标记类型演化
           └── 记录演化历史
   ↓
4. 返回处理结果
```

---

## §9 Schema API 设计

### 9.1 SchemaManager 结构

```rust
pub struct SchemaManager {
    /// 对象存储引用
    storage: Arc<dyn ObjectStore>,
    /// 索引前缀
    index_prefix: String,
    /// 内存 Schema 缓存（使用 RwLock 实现线程安全）
    schema: Arc<RwLock<Schema>>,
    /// 保存触发器（使用 Mutex 实现线程安全）
    save_trigger: Arc<Mutex<SaveTrigger>>,
}

impl SchemaManager {
    /// 创建新的 SchemaManager
    ///
    /// 使用 Arc<RwLock> 和 Arc<Mutex> 实现内部可变性，
    /// 因此方法可以使用 &self 而非 &mut self。
    pub fn new(storage: Arc<dyn ObjectStore>, index_prefix: String) -> Self;
}
```

### 9.1.1 SaveTrigger 结构

```rust
/// 保存触发器
///
/// 用于批量保存 Schema，避免频繁 I/O
#[derive(Debug)]
pub struct SaveTrigger {
    /// 是否有待处理的保存
    pending: bool,
    /// 上次保存时间
    last_save_time: Instant,
    /// 保存间隔（避免过于频繁）
    save_interval: Duration,
    /// 待保存的 Schema
    pending_schema: Option<Schema>,
}

impl SaveTrigger {
    /// 标记需要保存
    pub fn mark_dirty(&mut self, schema: Schema) {
        self.pending = true;
        self.pending_schema = Some(schema);
    }

    /// 检查是否需要保存
    pub fn should_save(&self) -> bool {
        self.pending && self.last_save_time.elapsed() >= self.save_interval
    }

    /// 获取并清空待保存的 Schema
    pub fn take_pending(&mut self) -> Option<Schema> {
        if self.should_save() {
            self.pending = false;
            self.last_save_time = Instant::now();
            self.pending_schema.take()
        } else {
            None
        }
    }
}
```

### 9.2 核心方法

```rust
impl SchemaManager {
    /// 初始化：从存储加载 Schema 或创建新的
    pub async fn initialize(&self) -> Result<(), SchemaError>;

    /// 获取当前 Schema（只读快照）
    pub fn get_schema(&self) -> Schema;

    /// 处理文档，更新 Schema
    pub async fn process_document(&self, doc: &Document) -> Result<SchemaUpdate, SchemaError>;

    /// 强制刷新 Schema（从存储重新加载）
    pub async fn flush(&self) -> Result<(), SchemaError>;
}
```

### 9.3 SchemaUpdate 结构

```rust
#[derive(Debug, Clone)]
pub enum SchemaUpdate {
    /// 无变化
    NoChange,
    /// 新增字段
    FieldAdded {
        field_name: String,
        field_type: FieldType,
    },
    /// 类型演化
    TypeEvolved {
        field_name: String,
        old_type: FieldType,
        new_type: FieldType,
    },
}
```

---

## §10 向量维度约束

### 10.1 维度固定规则

向量字段在首次推断后固定维度：

```rust
impl SchemaManager {
    /// 检查向量维度
    pub fn check_vector_dimension(
        &self,
        field_name: &str,
        new_dim: u32,
    ) -> Result<(), SchemaError> {
        let schema = self.schema.read().unwrap();

        if let Some(metadata) = schema.fields.get(field_name) {
            if let FieldType::Vector(existing_dim) = metadata.r#type {
                if existing_dim != new_dim {
                    return Err(SchemaError::VectorDimensionConflict {
                        field_name: field_name.to_string(),
                        existing_dim,
                        new_dim,
                    });
                }
            }
        }

        Ok(())
    }
}
```

### 10.2 维度冲突示例

```rust
// 文档 1
{
    "embedding": [0.1, 0.2, 0.3]
}
// → 推断: embedding: Vector(3)

// 文档 2（错误）
{
    "embedding": [0.1, 0.2, 0.3, 0.4]
}
// → 错误: VectorDimensionConflict {
//     field_name: "embedding",
//     existing_dim: 3,
//     new_dim: 4,
// }
```

---

## §11 错误处理

### 11.1 SchemaError 类型

```rust
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    /// 序列化错误
    #[error("Serialization failed: {0}")]
    SerializationError(String),

    /// 反序列化错误
    #[error("Deserialization failed: {0}")]
    DeserializationError(String),

    /// 向量维度冲突（唯一会报错的收敛情况）
    #[error("Vector dimension conflict for field '{field_name}': existing {existing_dim} vs new {new_dim}")]
    VectorDimensionConflict {
        field_name: String,
        existing_dim: u32,
        new_dim: u32,
    },

    /// 持久化错误
    #[error("Persistence error: {0}")]
    PersistenceError(String),

    /// 内部错误
    #[error("Internal error: {0}")]
    Internal(String),
}
```

### 11.2 错误处理策略

| 错误类型 | 处理策略 |
|----------|----------|
| SerializationError | 日志记录，返回错误 |
| DeserializationError | 尝试加载历史版本，失败则创建新 Schema |
| VectorDimensionConflict | 返回错误，拒绝写入 |
| PersistenceError | 重试 3 次，失败则降级到内存 Schema |

**类型冲突处理**：
- 非向量类型的冲突自动收敛到 Json，不报错
- 收敛到 Json 时记录警告日志
- 向量维度冲突是唯一会报错的情况

---

## §12 目录结构

```
seekpower/
├── crates/
│   ├── seekpower-error/
│   │   └── src/lib.rs                # 错误类型（含 SchemaError）
│   │
│   ├── seekpower-storage/
│   │   └── src/
│   │       ├── lib.rs                # 重新导出
│   │       ├── object_store.rs       # ObjectStore trait
│   │       ├── builder.rs            # Builder 模式
│   │       ├── s3_store.rs           # S3Store 实现
│   │       └── fs_store.rs           # FsStore 实现
│   │
│   └── seekpower-schema/             # Schema 模块
│       └── src/
│           ├── lib.rs                # 重新导出
│           ├── field_type.rs         # FieldType 定义
│           ├── field_metadata.rs     # FieldMetadata 定义
│           ├── schema.rs             # Schema 定义
│           ├── infer.rs              # 推断逻辑
│           ├── converge.rs           # 收敛规则
│           ├── schema_manager.rs     # SchemaManager 实现
│           └── error.rs              # SchemaError 定义
│
└── docs/
    ├── ARCHITECTURE_V3.md
    └── PHASE_2.md                    # 本文档
```

---

## §13 验收标准

### 13.1 单元测试

- [ ] FieldType 推断正确性测试
  - [ ] 短文本推断为 Keyword
  - [ ] 长文本推断为 Text
  - [ ] 浮点数组推断为 Vector
  - [ ] U64 推断（修正后）
  - [ ] I64 推断（负数）
  - [ ] F64 推断（浮点数）
  - [ ] 布尔值推断为 Bool
  - [ ] 对象推断为 Json

- [ ] 类型收敛规则测试
  - [ ] Keyword + Text → Text
  - [ ] I64 + F64 → F64
  - [ ] U64 + I64 → I64
  - [ ] 不兼容类型（非向量）→ Json
  - [ ] Vector(3) + Vector(4) → Error

- [ ] 向量维度约束测试
  - [ ] 首次推断固定维度
  - [ ] 维度冲突返回错误
  - [ ] 维度相同验证通过

---

## 附录A：设计审查与修正

### 发现的问题

| ID | 严重程度 | 问题描述 | 状态 |
|----|----------|----------|------|
| P1 | 🔴 严重 | U64 类型永远无法被推断（as_i64() 先返回） | ✅ 已修正 |
| P2 | 🔴 严重 | 类型收敛规则不完整，缺少许多组合 | ✅ 已修正 |
| P3 | 🔴 严重 | SchemaManager API 使用 &self 而非 &mut self | ✅ 已修正（使用 Arc<RwLock>） |
| P4 | 🔴 严重 | Json 字段 indexed 标记错误 | ✅ 已修正 |
| P5 | 🟡 中等 | 数组推断策略不明确 | ✅ 已修正 |
| P6 | 🟡 中等 | Schema JSON 序列化格式不一致 | ✅ 已修正 |
| P7 | 🟡 中等 | SaveTrigger 未定义 | ✅ 已补充 |

### 修正内容

**P1 修正** - U64 推断：
```rust
// 修正前（错误）
serde_json::Value::Number(n) if n.is_i64() => Some(FieldType::I64),
serde_json::Value::Number(n) => Some(FieldType::F64),

// 修正后（正确）
serde_json::Value::Number(n) => {
    if let Some(u) = n.as_u64() { Some(FieldType::U64) }
    else if let Some(i) = n.as_i64() { if i >= 0 { Some(U64) } else { Some(I64) } }
    else if n.is_f64() { Some(F64) }
    else { None }
}
```

**P2 修正** - 收敛规则完整性：
```rust
// 修正后添加默认兜底
// 其他所有冲突 → Json（不报错，兼容优先）
(_, _) => FieldType::Json,
```

---

## 附录B：使用示例

### B.1 基本 Schema 使用

```rust
use seekpower_schema::{SchemaManager, Document};
use seekpower_storage::{ObjectStoreBuilder, BackendType};

// 创建存储
let store = ObjectStoreBuilder::new(BackendType::fs("/tmp/data".into()))
    .build()
    .await?;

// 创建 SchemaManager（默认 1 秒保存间隔）
let schema_manager = SchemaManager::new(
    store.clone(),
    "tenant-123/indexes/my-index".to_string(),
);
schema_manager.initialize().await?;

// 处理文档
let doc = Document::from_json(r#"{
    "title": "Hello World",
    "content": "This is a longer text content",
    "embedding": [0.1, 0.2, 0.3],
    "score": 95.5,
    "active": true
}"#)?;

let update = schema_manager.process_document(&doc).await?;

match update {
    SchemaUpdate::NoChange => println!("Schema unchanged"),
    SchemaUpdate::FieldAdded { field_name, field_type } => {
        println!("Added field: {} -> {:?}", field_name, field_type);
    }
    SchemaUpdate::TypeEvolved { field_name, old_type, new_type } => {
        println!("Evolved field: {} {:?} -> {:?}", field_name, old_type, new_type);
    }
}
```

### B.2 向量维度约束示例

```rust
// 第一次写入，推断为 Vector(3)
let doc1 = Document::from_json(r#"{
    "embedding": [0.1, 0.2, 0.3]
}"#)?;
schema_manager.process_document(&doc1).await?;

// 第二次写入，维度相同，成功
let doc2 = Document::from_json(r#"{
    "embedding": [0.4, 0.5, 0.6]
}"#)?;
schema_manager.process_document(&doc2).await?;

// 第三次写入，维度不同，失败
let doc3 = Document::from_json(r#"{
    "embedding": [0.1, 0.2, 0.3, 0.4]
}"#)?;
let result = schema_manager.process_document(&doc3).await;
assert!(result.is_err());
```

---

## 附录C：Schema 文件格式

### C.1 schema.json 示例

```json
{
  "version": 5,
  "fields": {
    "title": {
      "name": "title",
      "type": "Keyword",
      "indexed": true,
      "stored": true,
      "first_seen_at": 1698765432000,
      "updated_at": 1698765432000,
      "sample_count": 1000
    },
    "content": {
      "name": "content",
      "type": "Text",
      "indexed": true,
      "stored": true,
      "first_seen_at": 1698765432000,
      "updated_at": 1698765432000,
      "sample_count": 1000
    },
    "embedding": {
      "name": "embedding",
      "type": {
        "Vector": 768
      },
      "indexed": true,
      "stored": true,
      "first_seen_at": 1698765432000,
      "updated_at": 1698765432000,
      "sample_count": 1000
    },
    "score": {
      "name": "score",
      "type": "F64",
      "indexed": true,
      "stored": true,
      "first_seen_at": 1698765432000,
      "updated_at": 1698765432000,
      "sample_count": 1000
    }
  },
  "created_at": 1698765432000,
  "updated_at": 1698765432000
}
```

### C.2 FieldType 序列化说明

FieldType 使用 tagged enum 序列化格式：

| FieldType | JSON 格式 |
|-----------|-----------|
| Text | `{"type": "Text"}` |
| Keyword | `{"type": "Keyword"}` |
| U64 | `{"type": "U64"}` |
| I64 | `{"type": "I64"}` |
| F64 | `{"type": "F64"}` |
| Bool | `{"type": "Bool"}` |
| Timestamp | `{"type": "Timestamp"}` |
| Vector | `{"type": {"Vector": N}}` |
| Json | `{"type": "Json"}` |

---

**文档版本**: v1.2 (已整合并修正设计审查问题)
**创建时间**: 2026-07-18
**更新时间**: 2026-07-18