//! SeekPower Schema 模块
//!
//! 实现 Schema-Free 的自动字段类型推断和收敛机制

pub mod error;
pub mod field_type;

pub use error::SchemaError;
pub use field_type::FieldType;
