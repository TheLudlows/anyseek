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
