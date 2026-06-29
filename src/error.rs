use thiserror::Error;

#[derive(Debug, Error)]
pub enum StormDbError {
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("document not found: {0}")]
    DocumentNotFound(u64),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("schema error: {0}")]
    SchemaError(String),
    #[error("storage error: {0}")]
    StorageError(String),
    #[error("io error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, StormDbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let e = StormDbError::IndexNotFound("test".to_string());
        assert_eq!(e.to_string(), "index not found: test");
        let e = StormDbError::DocumentNotFound(42);
        assert_eq!(e.to_string(), "document not found: 42");
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err: StormDbError = io_err.into();
        assert!(matches!(err, StormDbError::IoError(_)));
    }
}
