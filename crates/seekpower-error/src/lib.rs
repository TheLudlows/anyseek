#[derive(Debug, thiserror::Error)]
pub enum SeekPowerError {
    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("schema error: {0}")]
    SchemaError(String),

    #[error("encoding error: {0}")]
    EncodingError(String),

    #[error("vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("index corrupted: {0}")]
    Corrupted(String),

    #[error("invalid index name: {0}")]
    InvalidIndexName(String),

    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("index not loaded: {0}")]
    IndexNotLoaded(String),
}

pub type Result<T> = std::result::Result<T, SeekPowerError>;
