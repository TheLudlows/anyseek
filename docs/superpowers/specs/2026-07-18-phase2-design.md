# SeekPower v3 阶段二实现设计

> Schema-Free 设计与自动推断
>
> **版本**: v1.2 (已修正设计审查问题)
> **创建时间**: 2026-07-18
> **更新时间**: 2026-07-18

---

## 概述

实现 Schema-Free 机制，支持无需预定义 schema 的自动字段类型推断，降低使用门槛。

## 架构

### 整体结构

创建 `seekpower-schema` crate，包含以下模块：

1. **数据结构层** - `field_type.rs`, `field_metadata.rs`, `schema.rs`
2. **核心逻辑层** - `infer.rs`, `converge.rs`
3. **管理层** - `schema_manager.rs`
4. **错误层** - `error.rs`

### 依赖关系

```
FieldType, FieldMetadata, Schema (数据结构)
    ↓
infer, converge (核心逻辑)
    ↓
SchemaManager (管理器)
    ↓
seekpower-storage (持久化)
```

---

## 数据结构设计

### FieldType

定义 9 种字段类型：

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

### FieldMetadata

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMetadata {
    pub name: String,
    pub r#type: FieldType,
    pub indexed: bool,
    pub stored: bool,
    pub first_seen_at: SystemTime,
    pub updated_at: SystemTime,
    pub sample_count: u64,
}
```

**注意**: Json 类型的 `indexed` 字段必须为 `false`。

### Schema

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub version: u64,
    pub fields: HashMap<String, FieldMetadata>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}
```

---

## 核心逻辑设计

### 类型推断

修正设计审查 P1 问题（U64 推断）：

```rust
impl FieldType {
    pub fn infer(value: &serde_json::Value) -> Option<Self> {
        match value {
            // 短文本 → Keyword
            Value::String(s) if s.len() < 100 => Some(Keyword),
            // 长文本 → Text
            Value::String(_) => Some(Text),
            // 数字数组（2~4096维）→ Vector
            Value::Array(arr) if arr.iter().all(|v| v.is_number()) => {
                let len = arr.len() as u32;
                if (2..=4096).contains(&len) { Some(Vector(len)) } else { None }
            }
            // 字符串数组 → Json
            Value::Array(arr) if arr.iter().all(|v| v.is_string()) => Some(Json),
            // 其他数组 → 不推断
            Value::Array(_) => None,
            // 数字（修正 U64 推断）
            Value::Number(n) => {
                if let Some(u) = n.as_u64() { Some(U64) }
                else if let Some(i) = n.as_i64() { if i >= 0 { Some(U64) } else { Some(I64) } }
                else if n.is_f64() { Some(F64) }
                else { None }
            }
            Value::Bool(_) => Some(Bool),
            Value::Object(_) => Some(Json),
            Value::Null => None,
        }
    }
}
```

### 类型收敛

修正设计审查 P2 问题（收敛规则不完整）：

```rust
impl FieldType {
    pub fn converge(&self, other: &Self) -> Self {
        match (self, other) {
            (t1, t2) if t1 == t2 => t1.clone(),

            // 文本升级
            (Keyword, Text) | (Text, Keyword) => Text,

            // 数值升级
            (U64, I64) | (I64, U64) => I64,
            (U64, F64) | (F64, U64) | (I64, F64) | (F64, I64) => F64,

            // 其他冲突 → Json（兼容优先）
            (_, _) => Json,
        }
    }
}
```

**收敛规则表**：
| 现有类型 | 新类型 | 收敛结果 |
|----------|--------|----------|
| Keyword | Text | Text |
| U64 | I64 | I64 |
| U64 | F64 | F64 |
| I64 | F64 | F64 |
| Vector(3) | Vector(4) | **Error** |
| 其他任意冲突 | Json |

---

## SchemaManager 设计

### 核心结构

```rust
pub struct SchemaManager {
    storage: Arc<dyn ObjectStore>,
    index_prefix: String,
    schema: Arc<RwLock<Schema>>,
    save_trigger: Arc<Mutex<SaveTrigger>>,
}

pub struct SaveTrigger {
    pending: bool,
    last_save_time: Instant,
    save_interval: Duration,
    pending_schema: Option<Schema>,
}
```

### API 方法

```rust
impl SchemaManager {
    pub fn new(storage: Arc<dyn ObjectStore>, index_prefix: String) -> Self;

    pub async fn initialize(&self) -> Result<(), SchemaError>;

    pub fn get_schema(&self) -> Schema;

    pub async fn process_document(&self, doc: &Document) -> Result<SchemaUpdate, SchemaError>;

    pub async fn flush(&self) -> Result<(), SchemaError>;
}
```

### SchemaUpdate

```rust
#[derive(Debug, Clone)]
pub enum SchemaUpdate {
    NoChange,
    FieldAdded { field_name: String, field_type: FieldType },
    TypeEvolved { field_name: String, old_type: FieldType, new_type: FieldType },
}
```

### 处理流程

```
process_document(doc)
  ↓
遍历 doc.fields
  ↓
对每个字段：
  - 不存在 → 推断类型，添加到 Schema
  - 存在且类型相同 → 更新 sample_count
  - 存在且类型不同 → 应用 converge()
    - Vector 维度冲突 → 返回错误
    - 其他冲突 → 升级类型，记录变更
  ↓
标记 Schema 需要保存
  ↓
返回 SchemaUpdate
```

---

## 错误处理

### SchemaError

```rust
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("Serialization failed: {0}")]
    SerializationError(String),

    #[error("Deserialization failed: {0}")]
    DeserializationError(String),

    #[error("Vector dimension conflict for field '{field_name}': existing {existing_dim} vs new {new_dim}")]
    VectorDimensionConflict { field_name: String, existing_dim: u32, new_dim: u32 },

    #[error("Persistence error: {0}")]
    PersistenceError(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
```

---

## 持久化

### 存储位置

```
{index_prefix}/schema.json
```

### 持久化逻辑

```rust
impl SchemaManager {
    async fn save_to_storage(&self, schema: &Schema) -> Result<(), SchemaError> {
        let path = format!("{}/schema.json", self.index_prefix);
        let json = serde_json::to_vec(schema)
            .map_err(|e| SchemaError::SerializationError(e.to_string()))?;

        self.storage
            .put(&path, json)
            .await
            .map_err(|e| SchemaError::PersistenceError(e.to_string()))?;

        Ok(())
    }
}
```

---

## 测试策略

专注于单元测试快速验证核心逻辑：
- FieldType 推断正确性
- 类型收敛规则
- Schema 演化逻辑
- 错误处理

---

## 目录结构

```
seekpower/
└── crates/
    └── seekpower-schema/
        └── src/
            ├── lib.rs
            ├── field_type.rs
            ├── field_metadata.rs
            ├── schema.rs
            ├── infer.rs
            ├── converge.rs
            ├── schema_manager.rs
            └── error.rs
```