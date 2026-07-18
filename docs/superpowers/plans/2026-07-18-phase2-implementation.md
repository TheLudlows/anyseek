# SeekPower v3 阶段二实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Schema-Free 设计与自动字段类型推断，创建 seekpower-schema crate

**Architecture:** 创建新 crate，按依赖顺序实现：数据结构 → 核心逻辑 → SchemaManager → 持久化集成

**Tech Stack:** Rust, serde/serde_json, seekpower-storage, tokio, thiserror

## Global Constraints

- 使用 seekpower-storage 作为存储后端，通过 ObjectStore trait
- 单元测试覆盖所有核心逻辑
- 使用 Arc<RwLock> 实现 SchemaManager 的内部可变性
- Json 类型的 indexed 字段强制为 false
- 向量维度冲突是唯一报错的收敛情况

---

## 文件结构

```
seekpower/
└── crates/
    └── seekpower-schema/
        ├── Cargo.toml
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

---

### Task 1: 创建 seekpower-schema crate

**Files:**
- Create: `crates/seekpower-schema/Cargo.toml`

**Interfaces:**
- 无

**Dependencies:**
- workspace 依赖: serde, serde_json, tokio, thiserror, seekpower-storage

- [ ] **Step 1: 创建 Cargo.toml**

```toml
[package]
name = "seekpower-schema"
version = "0.1.0"
edition = "2021"
rust-version = "1.75"

[dependencies]
seekpower-storage.workspace = true
seekpower-error.workspace = true

serde.workspace = true
serde_json.workspace = true
tokio.workspace = true
thiserror.workspace = true
```

- [ ] **Step 2: 创建空的 lib.rs**

```rust
//! SeekPower Schema 模块
//!
//! 实现 Schema-Free 的自动字段类型推断和收敛机制

pub mod error;
pub mod field_type;
pub mod field_metadata;
pub mod schema;
pub mod infer;
pub mod converge;
pub mod schema_manager;

pub use error::SchemaError;
pub use field_type::FieldType;
pub use field_metadata::FieldMetadata;
pub use schema::Schema;
pub use schema_manager::SchemaManager;
```

- [ ] **Step 3: 更新 workspace Cargo.toml 添加新成员**

在 workspace members 中添加 `"crates/seekpower-schema"`

- [ ] **Step 4: 运行 cargo check 验证**

Run: `cargo check --package seekpower-schema`
Expected: OK

- [ ] **Step 5: 提交**

```bash
git add crates/seekpower-schema/Cargo.toml crates/seekpower-schema/src/lib.rs Cargo.toml
git commit -m "feat: create seekpower-schema crate skeleton"
```

---

### Task 2: 实现错误类型 SchemaError

**Files:**
- Create: `crates/seekpower-schema/src/error.rs`

**Interfaces:**
- Consumes: 无
- Produces: `SchemaError` 枚举

- [ ] **Step 1: 写入错误类型定义**

```rust
use thiserror::Error;

/// Schema 操作错误
#[derive(Debug, Error)]
pub enum SchemaError {
    /// 序列化失败
    #[error("Serialization failed: {0}")]
    SerializationError(String),

    /// 反序列化失败
    #[error("Deserialization failed: {0}")]
    DeserializationError(String),

    /// 向量维度冲突
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

impl From<seekpower_error::StoreError> for SchemaError {
    fn from(e: seekpower_error::StoreError) -> Self {
        SchemaError::PersistenceError(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = SchemaError::VectorDimensionConflict {
            field_name: "embedding".to_string(),
            existing_dim: 3,
            new_dim: 4,
        };
        assert!(err.to_string().contains("Vector dimension conflict"));
        assert!(err.to_string().contains("embedding"));
    }
}
```

- [ ] **Step 2: 运行测试验证**

Run: `cargo test --package seekpower-schema --lib error`
Expected: PASS

- [ ] **Step 3: 提交**

```bash
git add crates/seekpower-schema/src/error.rs
git commit -m "feat: add SchemaError type"
```

---

### Task 3: 实现 FieldType 枚举

**Files:**
- Create: `crates/seekpower-schema/src/field_type.rs`

**Interfaces:**
- Consumes: 无
- Produces: `FieldType` 枚举，可序列化

- [ ] **Step 1: 写入测试（先写失败的测试）**

```rust
use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fieldtype_equality() {
        assert_eq!(FieldType::Text, FieldType::Text);
        assert_eq!(FieldType::Vector(3), FieldType::Vector(3));
        assert_ne!(FieldType::Vector(3), FieldType::Vector(4));
    }

    #[test]
    fn test_fieldtype_serialization() {
        let text = FieldType::Text;
        let json = serde_json::to_string(&text).unwrap();
        assert_eq!(json, r#"{"Text":[]}"#);

        let vector = FieldType::Vector(768);
        let json = serde_json::to_string(&vector).unwrap();
        assert_eq!(json, r#"{"Vector":[768]}"#);
    }

    #[test]
    fn test_fieldtype_deserialization() {
        let text: FieldType = serde_json::from_str(r#"{"Text":[]}"#).unwrap();
        assert_eq!(text, FieldType::Text);

        let vector: FieldType = serde_json::from_str(r#"{"Vector":[768]}"#).unwrap();
        assert_eq!(vector, FieldType::Vector(768));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cargo test --package seekpower-schema field_type`
Expected: FAIL (field_type 模块未定义)

- [ ] **Step 3: 实现最小代码使测试通过**

```rust
use serde::{Deserialize, Serialize};

/// 字段类型枚举
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", content = "args")]
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

- [ ] **Step 4: 运行测试验证通过**

Run: `cargo test --package seekpower-schema field_type`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/seekpower-schema/src/field_type.rs
git commit -m "feat: add FieldType enum"
```

---

### Task 4: 实现 FieldMetadata 结构

**Files:**
- Create: `crates/seekpower-schema/src/field_metadata.rs`

**Interfaces:**
- Consumes: `FieldType`
- Produces: `FieldMetadata` 结构

- [ ] **Step 1: 写入测试**

```rust
use std::time::SystemTime;
use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_metadata_creation() {
        let now = SystemTime::now();
        let meta = FieldMetadata {
            name: "title".to_string(),
            r#type: FieldType::Keyword,
            indexed: true,
            stored: true,
            first_seen_at: now,
            updated_at: now,
            sample_count: 0,
        };
        assert_eq!(meta.name, "title");
        assert_eq!(meta.sample_count, 0);
    }

    #[test]
    fn test_field_metadata_serialization() {
        let now = SystemTime::UNIX_EPOCH;
        let meta = FieldMetadata {
            name: "title".to_string(),
            r#type: FieldType::Keyword,
            indexed: true,
            stored: true,
            first_seen_at: now,
            updated_at: now,
            sample_count: 100,
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("title"));
        assert!(json.contains("Keyword"));
        assert!(json.contains("sample_count"));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cargo test --package seekpower-schema field_metadata`
Expected: FAIL

- [ ] **Step 3: 实现最小代码**

```rust
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

use crate::FieldType;

/// 字段元数据
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

impl FieldMetadata {
    pub fn new(name: String, r#type: FieldType) -> Self {
        let now = SystemTime::now();
        let indexed = !matches!(r#type, FieldType::Json);
        Self {
            name,
            r#type,
            indexed,
            stored: true,
            first_seen_at: now,
            updated_at: now,
            sample_count: 0,
        }
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cargo test --package seekpower-schema field_metadata`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/seekpower-schema/src/field_metadata.rs
git commit -m "feat: add FieldMetadata struct"
```

---

### Task 5: 实现 Schema 结构

**Files:**
- Create: `crates/seekpower-schema/src/schema.rs`

**Interfaces:**
- Consumes: `FieldMetadata`
- Produces: `Schema` 结构

- [ ] **Step 1: 写入测试**

```rust
use std::time::SystemTime;
use super::*;
use crate::FieldType;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new();
        assert_eq!(schema.version, 0);
        assert!(schema.fields.is_empty());
    }

    #[test]
    fn test_schema_add_field() {
        let mut schema = Schema::new();
        let meta = FieldMetadata::new("title".to_string(), FieldType::Keyword);
        schema.add_field(meta);
        assert_eq!(schema.fields.len(), 1);
        assert!(schema.fields.contains_key("title"));
    }

    #[test]
    fn test_schema_serialization() {
        let mut schema = Schema::new();
        let meta = FieldMetadata::new("title".to_string(), FieldType::Keyword);
        schema.add_field(meta);
        let json = serde_json::to_string(&schema).unwrap();
        assert!(json.contains("fields"));
        assert!(json.contains("version"));
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cargo test --package seekpower-schema schema`
Expected: FAIL

- [ ] **Step 3: 实现最小代码**

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

use crate::FieldMetadata;

/// Schema 定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub version: u64,
    pub fields: HashMap<String, FieldMetadata>,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

impl Schema {
    pub fn new() -> Self {
        let now = SystemTime::now();
        Self {
            version: 0,
            fields: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn add_field(&mut self, metadata: FieldMetadata) {
        self.version += 1;
        self.updated_at = SystemTime::now();
        self.fields.insert(metadata.name.clone(), metadata);
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldMetadata> {
        self.fields.get(name)
    }

    pub fn has_field(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cargo test --package seekpower-schema schema`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/seekpower-schema/src/schema.rs
git commit -m "feat: add Schema struct"
```

---

### Task 6: 实现类型推断逻辑

**Files:**
- Create: `crates/seekpower-schema/src/infer.rs`

**Interfaces:**
- Consumes: `FieldType`
- Produces: `FieldType::infer()` 方法

- [ ] **Step 1: 写入测试**

```rust
use serde_json::json;
use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_short_text() {
        let value = json!("short");
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Keyword);
    }

    #[test]
    fn test_infer_long_text() {
        let long_text = "a".repeat(150);
        let value = json!(long_text);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Text);
    }

    #[test]
    fn test_infer_u64() {
        let value = json!(12345);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::U64);
    }

    #[test]
    fn test_infer_negative_i64() {
        let value = json!(-12345);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::I64);
    }

    #[test]
    fn test_infer_f64() {
        let value = json!(3.14159);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::F64);
    }

    #[test]
    fn test_infer_bool() {
        let value = json!(true);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Bool);
    }

    #[test]
    fn test_infer_vector() {
        let value = json!([0.1, 0.2, 0.3]);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Vector(3));
    }

    #[test]
    fn test_infer_string_array() {
        let value = json!(["tag1", "tag2"]);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Json);
    }

    #[test]
    fn test_infer_object() {
        let value = json!({"nested": "value"});
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Json);
    }

    #[test]
    fn test_infer_null_returns_none() {
        let value = json!(null);
        let inferred = FieldType::infer(&value);
        assert!(inferred.is_none());
    }

    #[test]
    fn test_infer_short_array_returns_none() {
        let value = json!([1]);
        let inferred = FieldType::infer(&value);
        assert!(inferred.is_none());
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cargo test --package seekpower-schema infer`
Expected: FAIL

- [ ] **Step 3: 实现推断逻辑**

```rust
use serde_json::Value;

use crate::FieldType;

impl FieldType {
    /// 推断字段类型
    pub fn infer(value: &Value) -> Option<Self> {
        match value {
            // 短文本 → Keyword（< 100 字符）
            Value::String(s) if s.len() < 100 => Some(FieldType::Keyword),
            // 长文本 → Text（>= 100 字符）
            Value::String(_) => Some(FieldType::Text),
            // 纯数字数组 → Vector（2~4096 维）
            Value::Array(arr) if arr.iter().all(|v| v.is_number()) => {
                let len = arr.len() as u32;
                if (2..=4096).contains(&len) {
                    Some(FieldType::Vector(len))
                } else {
                    None
                }
            }
            // 字符串数组 → Json
            Value::Array(arr) if arr.iter().all(|v| v.is_string()) => Some(FieldType::Json),
            // 其他数组 → 不推断
            Value::Array(_) => None,
            // 数字（修正 U64 推断）
            Value::Number(n) => {
                if let Some(u) = n.as_u64() {
                    Some(FieldType::U64)
                } else if let Some(i) = n.as_i64() {
                    if i >= 0 { Some(FieldType::U64) } else { Some(FieldType::I64) }
                } else if n.is_f64() {
                    Some(FieldType::F64)
                } else {
                    None
                }
            }
            Value::Bool(_) => Some(FieldType::Bool),
            Value::Object(_) => Some(FieldType::Json),
            Value::Null => None,
        }
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cargo test --package seekpower-schema infer`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/seekpower-schema/src/infer.rs
git commit -m "feat: add type inference logic"
```

---

### Task 7: 实现类型收敛规则

**Files:**
- Create: `crates/seekpower-schema/src/converge.rs`

**Interfaces:**
- Consumes: `FieldType`
- Produces: `FieldType::converge()` 方法

- [ ] **Step 1: 写入测试**

```rust
use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_converge_same_type() {
        assert_eq!(
            FieldType::Text.converge(&FieldType::Text),
            FieldType::Text
        );
    }

    #[test]
    fn test_converge_keyword_text() {
        assert_eq!(
            FieldType::Keyword.converge(&FieldType::Text),
            FieldType::Text
        );
        assert_eq!(
            FieldType::Text.converge(&FieldType::Keyword),
            FieldType::Text
        );
    }

    #[test]
    fn test_converge_u64_i64() {
        assert_eq!(
            FieldType::U64.converge(&FieldType::I64),
            FieldType::I64
        );
    }

    #[test]
    fn test_converge_i64_f64() {
        assert_eq!(
            FieldType::I64.converge(&FieldType::F64),
            FieldType::F64
        );
    }

    #[test]
    fn test_converge_u64_f64() {
        assert_eq!(
            FieldType::U64.converge(&FieldType::F64),
            FieldType::F64
        );
    }

    #[test]
    fn test_converge_incompatible_types() {
        assert_eq!(
            FieldType::Text.converge(&FieldType::I64),
            FieldType::Json
        );
        assert_eq!(
            FieldType::Bool.converge(&FieldType::F64),
            FieldType::Json
        );
    }

    #[test]
    fn test_converge_vector_dimension_mismatch() {
        assert_eq!(
            FieldType::Vector(3).converge(&FieldType::Vector(4)),
            FieldType::Json
        );
    }

    #[test]
    fn test_converge_json_with_any() {
        assert_eq!(
            FieldType::Json.converge(&FieldType::Text),
            FieldType::Json
        );
    }
}
```

- [ ] **Step 2: 运行测试验证失败**

Run: `cargo test --package seekpower-schema converge`
Expected: FAIL

- [ ] **Step 3: 实现收敛规则**

```rust
use crate::FieldType;

impl FieldType {
    /// 类型收敛：合并两个可能的类型
    pub fn converge(&self, other: &Self) -> Self {
        match (self, other) {
            (t1, t2) if t1 == t2 => t1.clone(),

            // 文本升级
            (FieldType::Keyword, FieldType::Text) |
            (FieldType::Text, FieldType::Keyword) => FieldType::Text,

            // 数值升级
            (FieldType::U64, FieldType::I64) |
            (FieldType::I64, FieldType::U64) => FieldType::I64,
            (FieldType::U64, FieldType::F64) |
            (FieldType::F64, FieldType::U64) |
            (FieldType::I64, FieldType::F64) |
            (FieldType::F64, FieldType::I64) => FieldType::F64,

            // 其他所有冲突 → Json
            (_, _) => FieldType::Json,
        }
    }
}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cargo test --package seekpower-schema converge`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add crates/seekpower-schema/src/converge.rs
git commit -m "feat: add type convergence rules"
```

---

### Task 8: 实现 SaveTrigger

**Files:**
- Modify: `crates/seekpower-schema/src/schema_manager.rs`

**Interfaces:**
- 无

- [ ] **Step 1: 在 schema_manager.rs 中添加 SaveTrigger 定义和测试**

```rust
use std::time::{Duration, Instant};

/// 保存触发器
#[derive(Debug)]
pub struct SaveTrigger {
    pending: bool,
    last_save_time: Instant,
    save_interval: Duration,
    pending_schema: Option<crate::Schema>,
}

impl SaveTrigger {
    pub fn new(save_interval: Duration) -> Self {
        Self {
            pending: false,
            last_save_time: Instant::now(),
            save_interval,
            pending_schema: None,
        }
    }

    pub fn mark_dirty(&mut self, schema: crate::Schema) {
        self.pending = true;
        self.pending_schema = Some(schema);
    }

    pub fn should_save(&self) -> bool {
        self.pending && self.last_save_time.elapsed() >= self.save_interval
    }

    pub fn take_pending(&mut self) -> Option<crate::Schema> {
        if self.should_save() {
            self.pending = false;
            self.last_save_time = Instant::now();
            self.pending_schema.take()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod save_trigger_tests {
    use super::*;

    #[test]
    fn test_save_trigger_initial_state() {
        let trigger = SaveTrigger::new(Duration::from_millis(100));
        assert!(!trigger.should_save());
        assert!(trigger.take_pending().is_none());
    }

    #[test]
    fn test_save_trigger_mark_and_wait() {
        let mut trigger = SaveTrigger::new(Duration::from_millis(10));
        let schema = crate::Schema::new();
        trigger.mark_dirty(schema);
        assert!(!trigger.should_save()); // 时间未到
        std::thread::sleep(Duration::from_millis(20));
        assert!(trigger.should_save());
    }
}
```

- [ ] **Step 2: 运行测试验证**

Run: `cargo test --package seekpower-schema save_trigger`
Expected: PASS

- [ ] **Step 3: 提交**

```bash
git add crates/seekpower-schema/src/schema_manager.rs
git commit -m "feat: add SaveTrigger"
```

---

### Task 9: 实现 SchemaManager 核心方法

**Files:**
- Modify: `crates/seekpower-schema/src/schema_manager.rs`

**Interfaces:**
- Consumes: `Schema`, `FieldType`, `SchemaError`
- Produces: `SchemaManager` 结构和方法

- [ ] **Step 1: 添加 SchemaUpdate 枚举和 SchemaManager 结构**

```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use serde_json::Value;

use crate::{Schema, FieldMetadata, FieldType, SchemaError};
use seekpower_storage::ObjectStore;

/// Schema 更新结果
#[derive(Debug, Clone)]
pub enum SchemaUpdate {
    NoChange,
    FieldAdded { field_name: String, field_type: FieldType },
    TypeEvolved { field_name: String, old_type: FieldType, new_type: FieldType },
}

/// Schema 管理器
pub struct SchemaManager {
    storage: Arc<dyn ObjectStore>,
    index_prefix: String,
    schema: Arc<RwLock<Schema>>,
    save_trigger: Arc<Mutex<SaveTrigger>>,
}

impl SchemaManager {
    pub fn new(storage: Arc<dyn ObjectStore>, index_prefix: String) -> Self {
        Self {
            storage,
            index_prefix,
            schema: Arc::new(RwLock::new(Schema::new())),
            save_trigger: Arc::new(Mutex::new(SaveTrigger::new(Duration::from_secs(1)))),
        }
    }

    pub fn get_schema(&self) -> Schema {
        self.schema.read().unwrap().clone()
    }
}
```

- [ ] **Step 2: 添加 initialize 方法**

```rust
impl SchemaManager {
    pub async fn initialize(&self) -> Result<(), SchemaError> {
        let path = format!("{}/schema.json", self.index_prefix);
        match self.storage.get(&path).await {
            Ok(data) => {
                let loaded: Schema = serde_json::from_slice(&data)
                    .map_err(|e| SchemaError::DeserializationError(e.to_string()))?;
                *self.schema.write().unwrap() = loaded;
                Ok(())
            }
            Err(seekpower_error::StoreError::NotFound(_)) => {
                Ok(())
            }
            Err(e) => Err(SchemaError::from(e)),
        }
    }
}
```

- [ ] **Step 3: 添加 check_vector_dimension 方法**

```rust
impl SchemaManager {
    pub fn check_vector_dimension(
        &self,
        field_type: &FieldType,
        new_dim: u32,
        field_name: &str,
    ) -> Result<(), SchemaError> {
        if let FieldType::Vector(existing_dim) = field_type {
            if existing_dim != new_dim {
                return Err(SchemaError::VectorDimensionConflict {
                    field_name: field_name.to_string(),
                    existing_dim: *existing_dim,
                    new_dim,
                });
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 4: 添加测试**

```rust
#[cfg(test)]
mod schema_manager_tests {
    use super::*;
    use crate::FieldType;

    #[test]
    fn test_schema_update_equality() {
        let update = SchemaUpdate::FieldAdded {
            field_name: "title".to_string(),
            field_type: FieldType::Keyword,
        };
        match update {
            SchemaUpdate::FieldAdded { field_name, .. } => {
                assert_eq!(field_name, "title");
            }
            _ => panic!("Unexpected update type"),
        }
    }

    #[test]
    fn test_check_vector_dimension_same() {
        let storage = Arc::new(NullStore::new());
        let manager = SchemaManager::new(storage, "test".to_string());
        let result = manager.check_vector_dimension(&FieldType::Vector(3), 3, "embedding");
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_vector_dimension_different() {
        let storage = Arc::new(NullStore::new());
        let manager = SchemaManager::new(storage, "test".to_string());
        let result = manager.check_vector_dimension(&FieldType::Vector(3), 4, "embedding");
        assert!(result.is_err());
        assert!(matches!(result, Err(SchemaError::VectorDimensionConflict { .. })));
    }
}

// 简单的测试用存储实现
struct NullStore;

impl NullStore {
    fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl ObjectStore for NullStore {
    async fn get(&self, _key: &str) -> Result<bytes::Bytes, seekpower_error::StoreError> {
        Err(seekpower_error::StoreError::NotFound(_key.to_string()))
    }

    async fn put(&self, _key: &str, _value: bytes::Bytes) -> Result<(), seekpower_error::StoreError> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<(), seekpower_error::StoreError> {
        Ok(())
    }
}
```

- [ ] **Step 5: 运行测试验证**

Run: `cargo test --package seekpower-schema schema_manager`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add crates/seekpower-schema/src/schema_manager.rs
git commit -m "feat: add SchemaManager core methods"
```

---

### Task 10: 实现 process_document 方法

**Files:**
- Modify: `crates/seekpower-schema/src/schema_manager.rs`

**Interfaces:**
- Consumes: `Schema`, `FieldType::infer`, `FieldType::converge`, `SchemaError`
- Produces: `SchemaUpdate`

- [ ] **Step 1: 添加 process_document 方法**

```rust
impl SchemaManager {
    pub async fn process_document(&self, doc: &Value) -> Result<SchemaUpdate, SchemaError> {
        let mut updates = Vec::new();
        let mut schema = self.schema.write().unwrap();

        if let Value::Object(fields) = doc {
            for (field_name, field_value) in fields {
                // 推断字段类型
                let inferred_type = match FieldType::infer(field_value) {
                    Some(t) => t,
                    None => continue, // 无法推断的类型跳过
                };

                // 检查字段是否已存在
                if let Some(existing) = schema.get_field(field_name) {
                    // 检查向量维度冲突
                    if let (FieldType::Vector(existing_dim), FieldType::Vector(new_dim)) =
                        (&existing.r#type, &inferred_type)
                    {
                        if existing_dim != new_dim {
                            return Err(SchemaError::VectorDimensionConflict {
                                field_name: field_name.clone(),
                                existing_dim: *existing_dim,
                                new_dim: *new_dim,
                            });
                        }
                    }

                    // 类型收敛
                    if existing.r#type != inferred_type {
                        let converged = existing.r#type.converge(&inferred_type);
                        if converged != existing.r#type {
                            let old_type = existing.r#type.clone();
                            let new_type = converged.clone();

                            let mut new_metadata = existing.clone();
                            new_metadata.r#type = converged;
                            new_metadata.updated_at = std::time::SystemTime::now();
                            new_metadata.sample_count += 1;

                            schema.add_field(new_metadata);
                            updates.push(SchemaUpdate::TypeEvolved {
                                field_name: field_name.clone(),
                                old_type,
                                new_type,
                            });
                        }
                    } else {
                        // 类型相同，更新样本计数
                        let mut new_metadata = existing.clone();
                        new_metadata.sample_count += 1;
                        schema.fields.insert(field_name.clone(), new_metadata);
                    }
                } else {
                    // 新字段
                    let metadata = FieldMetadata::new(field_name.clone(), inferred_type);
                    schema.add_field(metadata);
                    updates.push(SchemaUpdate::FieldAdded {
                        field_name: field_name.clone(),
                        field_type: inferred_type,
                    });
                }
            }
        }

        // 标记需要保存
        if !updates.is_empty() {
            let schema_clone = schema.clone();
            self.save_trigger.lock().await.mark_dirty(schema_clone);
        }

        Ok(updates.into_iter().next().unwrap_or(SchemaUpdate::NoChange))
    }
}
```

- [ ] **Step 2: 添加测试**

```rust
#[tokio::test]
async fn test_process_document_new_fields() {
    use serde_json::json;

    let storage = Arc::new(NullStore::new());
    let manager = SchemaManager::new(storage, "test".to_string());
    manager.initialize().await.unwrap();

    let doc = json!({
        "title": "Hello",
        "count": 42,
        "active": true
    });

    let update = manager.process_document(&doc).await.unwrap();
    match update {
        SchemaUpdate::FieldAdded { field_name, .. } => {
            assert_eq!(field_name, "title"); // 第一个添加的字段
        }
        _ => panic!("Expected FieldAdded"),
    }

    let schema = manager.get_schema();
    assert_eq!(schema.fields.len(), 3);
}

#[tokio::test]
async fn test_process_document_vector_conflict() {
    use serde_json::json;

    let storage = Arc::new(NullStore::new());
    let manager = SchemaManager::new(storage, "test".to_string());
    manager.initialize().await.unwrap();

    let doc1 = json!({ "embedding": [0.1, 0.2, 0.3] });
    manager.process_document(&doc1).await.unwrap();

    let doc2 = json!({ "embedding": [0.1, 0.2, 0.3, 0.4] });
    let result = manager.process_document(&doc2).await;
    assert!(result.is_err());
    assert!(matches!(result, Err(SchemaError::VectorDimensionConflict { .. })));
}

#[tokio::test]
async fn test_process_document_type_evolution() {
    use serde_json::json;

    let storage = Arc::new(NullStore::new());
    let manager = SchemaManager::new(storage, "test".to_string());
    manager.initialize().await.unwrap();

    let doc1 = json!({ "value": 42 });
    manager.process_document(&doc1).await.unwrap();

    let doc2 = json!({ "value": 3.14 });
    let update = manager.process_document(&doc2).await.unwrap();
    match update {
        SchemaUpdate::TypeEvolved { old_type, new_type, .. } => {
            assert_eq!(old_type, FieldType::U64);
            assert_eq!(new_type, FieldType::F64);
        }
        _ => panic!("Expected TypeEvolved"),
    }
}
```

- [ ] **Step 3: 运行测试验证**

Run: `cargo test --package seekpower-schema process_document`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add crates/seekpower-schema/src/schema_manager.rs
git commit -m "feat: add process_document method"
```

---

### Task 11: 实现 flush 方法

**Files:**
- Modify: `crates/seekpower-schema/src/schema_manager.rs`

**Interfaces:**
- Consumes: `Schema`, `SchemaError`
- Produces: 无

- [ ] **Step 1: 添加 flush 方法**

```rust
impl SchemaManager {
    pub async fn flush(&self) -> Result<(), SchemaError> {
        if let Some(schema) = self.save_trigger.lock().await.take_pending() {
            let path = format!("{}/schema.json", self.index_prefix);
            let json = serde_json::to_vec(&schema)
                .map_err(|e| SchemaError::SerializationError(e.to_string()))?;

            self.storage
                .put(&path, bytes::Bytes::from(json))
                .await?;

            // 更新内存中的 version
            self.schema.write().unwrap().version = schema.version;
        }
        Ok(())
    }
}
```

- [ ] **Step 2: 添加测试**

```rust
#[tokio::test]
async fn test_flush_persists_schema() {
    use serde_json::json;

    let storage = Arc::new(TestStore::new());
    let manager = SchemaManager::new(storage.clone(), "test".to_string());
    manager.initialize().await.unwrap();

    let doc = json!({ "title": "Hello" });
    manager.process_document(&doc).await.unwrap();
    manager.flush().await.unwrap();

    let saved = storage.get_data().await;
    assert!(saved.is_some());
    let saved_str = std::str::from_utf8(&saved.unwrap()).unwrap();
    assert!(saved_str.contains("title"));
}

// 测试用存储实现
struct TestStore {
    data: Arc<Mutex<Option<bytes::Bytes>>>,
}

impl TestStore {
    fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_data(&self) -> Option<bytes::Bytes> {
        self.data.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl ObjectStore for TestStore {
    async fn get(&self, _key: &str) -> Result<bytes::Bytes, seekpower_error::StoreError> {
        match self.data.lock().await.as_ref() {
            Some(data) => Ok(data.clone()),
            None => Err(seekpower_error::StoreError::NotFound(_key.to_string())),
        }
    }

    async fn put(&self, _key: &str, value: bytes::Bytes) -> Result<(), seekpower_error::StoreError> {
        *self.data.lock().await = Some(value);
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<(), seekpower_error::StoreError> {
        *self.data.lock().await = None;
        Ok(())
    }
}
```

- [ ] **Step 3: 运行测试验证**

Run: `cargo test --package seekpower-schema flush`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add crates/seekpower-schema/src/schema_manager.rs
git commit -m "feat: add flush method"
```

---

### Task 12: 运行全部测试验证

**Files:**
- 无修改

**Interfaces:**
- 无

- [ ] **Step 1: 运行全部单元测试**

Run: `cargo test --package seekpower-schema --lib`
Expected: 所有测试 PASS

- [ ] **Step 2: 运行 clippy 检查**

Run: `cargo clippy --package seekpower-schema -- -D warnings`
Expected: 无警告

- [ ] **Step 3: 运行 fmt 检查**

Run: `cargo fmt --package seekpower-schema -- --check`
Expected: 格式正确

- [ ] **Step 4: 提交最终版本**

```bash
git add -A
git commit -m "test: verify complete seekpower-schema implementation"
```

---

## Self-Review 结果

### Spec Coverage
✅ FieldType 类型系统 - Task 3
✅ 自动类型推断 - Task 6
✅ 类型收敛规则 - Task 7
✅ Schema 持久化 - Task 8, 11
✅ Schema 演化 - Task 10
✅ Schema API - Task 9, 10, 11
✅ 向量维度约束 - Task 9, 10
✅ 错误处理 - Task 2

### Placeholder Scan
✅ 无 "TBD", "TODO" 占位符
✅ 所有步骤包含完整代码
✅ 所有命令包含确切输出预期

### Type Consistency
✅ FieldType 定义与使用一致
✅ SchemaError 变体命名一致
✅ SchemaUpdate 枚举定义一致
✅ 方法签名在整个计划中一致