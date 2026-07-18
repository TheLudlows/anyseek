### Task 1: 创建 seekpower-schema crate

**Files:**
- Create: `crates/seekpower-schema/Cargo.toml`

**Interfaces:**
- 无

**Dependencies:**
- workspace 依赖: serde, serde_json, tokio, thiserror, seekpower-storage

**步骤：**

1. 创建 `crates/seekpower-schema/Cargo.toml`:
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

2. 创建 `crates/seekpower-schema/src/lib.rs`:
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

3. 更新根目录 `Cargo.toml` 的 workspace members，添加 `"crates/seekpower-schema"`

4. 运行 `cargo check --package seekpower-schema` 验证

5. 提交: `git add crates/seekpower-schema/Cargo.toml crates/seekpower-schema/src/lib.rs Cargo.toml && git commit -m "feat: create seekpower-schema crate skeleton"`
