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
