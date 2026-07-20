use seekpower_error::Error;
use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DocStoreError>;

#[derive(Error, Debug)]
pub enum DocStoreError {
    #[error("Document not found: {doc_id}")]
    DocumentNotFound { doc_id: u64 },

    #[error("Document already exists: {doc_id}")]
    DocumentAlreadyExists { doc_id: u64 },

    #[error("Invalid document value")]
    InvalidDocumentValue,

    #[error("Document too large: {size} bytes, max {max_size}")]
    DocumentTooLarge { size: usize, max_size: usize },

    #[error("Chunk error: {0}")]
    ChunkError(String),

    #[error("Storage error: {0}")]
    StorageError(#[from] Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] io::Error),

    #[error("Delete set error: {0}")]
    DeleteSetError(String),

    #[error("SST error: {0}")]
    SstError(String),

    #[error("Compaction error: {0}")]
    CompactionError(String),

    #[error("WAL error: {0}")]
    WalError(String),

    #[error("Invalid SST format: {0}")]
    InvalidSstFormat(String),

    #[error("Invalid WAL format: {0}")]
    InvalidWalFormat(String),

    #[error("Backpressure: WAL too large")]
    Backpressure,

    #[error("CAS conflict")]
    CasConflict,

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("No documents to index")]
    NoDocuments,

    #[error("Sequence error: {0}")]
    SequenceError(String),
}