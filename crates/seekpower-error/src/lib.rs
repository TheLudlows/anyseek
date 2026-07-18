//! SeekPower 统一错误类型定义
//!
//! 本 crate 定义了整个 SeekPower 项目的错误类型体系，包括：
//! - 存储错误 (StoreError)
//! - 持久化错误 (PersistenceError)
//! - 并发错误 (ConcurrencyError)

use std::fmt;
use std::io;
use std::time::SystemTime;

/// 存储操作错误
///
/// 涵盖所有对象存储操作可能出现的错误情况
#[derive(Debug, Clone, thiserror::Error)]
pub enum StoreError {
    /// 对象不存在
    #[error("object not found: {0}")]
    NotFound(String),

    /// 对象已存在
    #[error("object already exists: {0}")]
    AlreadyExists(String),

    /// 权限不足
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// 参数无效
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// 内部错误
    #[error("internal error: {0}")]
    Internal(String),

    /// 服务不可用
    #[error("service unavailable: {0}")]
    Unavailable(String),

    /// IO 错误
    #[error("IO error: {0}")]
    Io(String),

    /// 版本冲突 (CAS 操作失败)
    #[error("version conflict for {0}: expected {1}, got {2}")]
    VersionConflict(String, u64, u64),

    /// ETag 不匹配
    #[error("ETag mismatch for {0}: expected {1}, got {2}")]
    ETagMismatch(String, String, String),

    /// 超时
    #[error("operation timeout: {0}")]
    Timeout(String),
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e.to_string())
    }
}

impl StoreError {
    /// 判断错误是否可重试
    pub fn is_retryable(&self) -> bool {
        matches!(self, StoreError::Timeout(_) | StoreError::Unavailable(_))
    }

    /// 获取错误类型名称
    pub fn error_type(&self) -> &'static str {
        match self {
            StoreError::NotFound(_) => "NotFound",
            StoreError::AlreadyExists(_) => "AlreadyExists",
            StoreError::PermissionDenied(_) => "PermissionDenied",
            StoreError::InvalidArgument(_) => "InvalidArgument",
            StoreError::Internal(_) => "Internal",
            StoreError::Unavailable(_) => "Unavailable",
            StoreError::Io(_) => "Io",
            StoreError::VersionConflict(_, _, _) => "VersionConflict",
            StoreError::ETagMismatch(_, _, _) => "ETagMismatch",
            StoreError::Timeout(_) => "Timeout",
        }
    }
}

/// 持久化操作错误
///
/// 涵盖 WAL、Manifest 等持久化操作可能出现的错误
#[derive(Debug, Clone, thiserror::Error)]
pub enum PersistenceError {
    /// 序列化失败
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// 反序列化失败
    #[error("deserialization error: {0}")]
    DeserializationError(String),

    /// 数据损坏
    #[error("corrupted data: {0}")]
    CorruptedData(String),

    /// 校验和不匹配
    #[error("checksum mismatch: expected {0}, got {1}")]
    ChecksumMismatch(u32, u32),

    /// IO 错误
    #[error("IO error: {0}")]
    Io(String),

    /// 内部错误
    #[error("internal error: {0}")]
    Internal(String),

    /// 无效的 WAL 文件格式
    #[error("invalid WAL format: {0}")]
    InvalidWalFormat(String),

    /// Manifest 版本冲突
    #[error("manifest version conflict: expected {0}, got {1}")]
    VersionConflict(u64, u64),

    /// 找不到 Manifest
    #[error("manifest not found: {0}")]
    ManifestNotFound(String),

    /// 操作超时
    #[error("operation timeout: {0}")]
    Timeout(String),
}

impl From<std::io::Error> for PersistenceError {
    fn from(e: std::io::Error) -> Self {
        PersistenceError::Io(e.to_string())
    }
}

/// 并发操作错误
///
/// 涵盖并发控制可能出现的错误
#[derive(Debug, thiserror::Error)]
pub enum ConcurrencyError {
    /// 版本冲突
    #[error("version conflict: expected {0}, got {1}")]
    Conflict(u64, u64),

    /// 操作超时
    #[error("operation timeout: {0}")]
    Timeout(String),

    /// 重试耗尽
    #[error("retry exhausted after {0} attempts: {1}")]
    RetryExhausted(usize, String),

    /// 锁获取失败
    #[error("lock acquisition failed: {0}")]
    LockFailed(String),
}

/// 重试配置
///
/// 定义重试策略的参数
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// 最大重试次数
    pub max_retries: usize,
    /// 初始延迟（毫秒）
    pub initial_delay_ms: u64,
    /// 退避因子
    pub backoff_factor: u32,
    /// 最大延迟（毫秒）
    pub max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 100,
            backoff_factor: 2,
            max_delay_ms: 5000,
        }
    }
}

impl RetryConfig {
    /// 创建新的重试配置
    pub fn new() -> Self {
        Self::default()
    }

    /// 设置最大重试次数
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// 设置初始延迟
    pub fn with_initial_delay(mut self, delay_ms: u64) -> Self {
        self.initial_delay_ms = delay_ms;
        self
    }

    /// 设置退避因子
    pub fn with_backoff_factor(mut self, factor: u32) -> Self {
        self.backoff_factor = factor;
        self
    }

    /// 设置最大延迟
    pub fn with_max_delay(mut self, delay_ms: u64) -> Self {
        self.max_delay_ms = delay_ms;
        self
    }

    /// 计算指定重试次数的延迟时间
    pub fn delay_for_attempt(&self, attempt: usize) -> std::time::Duration {
        let factor = self.backoff_factor.pow(attempt as u32) as u64;
        let delay_ms = (self.initial_delay_ms * factor).min(self.max_delay_ms);
        std::time::Duration::from_millis(delay_ms)
    }
}

/// 对象元数据
///
/// 描述存储对象的元信息
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    /// 对象大小（字节）
    pub size: u64,
    /// 实体标签（用于 CAS 操作）
    pub etag: String,
    /// 最后修改时间
    pub last_modified: SystemTime,
}

impl ObjectMetadata {
    /// 创建新的对象元数据
    pub fn new(size: u64, etag: String) -> Self {
        Self {
            size,
            etag,
            last_modified: SystemTime::now(),
        }
    }

    /// 创建带指定时间的对象元数据
    pub fn with_time(size: u64, etag: String, last_modified: SystemTime) -> Self {
        Self {
            size,
            etag,
            last_modified,
        }
    }
}

impl fmt::Display for ObjectMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ObjectMetadata {{ size: {}, etag: {}, last_modified: {:?} }}",
            self.size, self.etag, self.last_modified
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_delay_ms, 100);
        assert_eq!(config.backoff_factor, 2);
        assert_eq!(config.max_delay_ms, 5000);
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::new()
            .with_max_retries(5)
            .with_initial_delay(50)
            .with_backoff_factor(3)
            .with_max_delay(10000);

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_delay_ms, 50);
        assert_eq!(config.backoff_factor, 3);
        assert_eq!(config.max_delay_ms, 10000);
    }

    #[test]
    fn test_retry_delay_calculation() {
        let config = RetryConfig::new()
            .with_initial_delay(100)
            .with_backoff_factor(2)
            .with_max_delay(1000);

        assert_eq!(config.delay_for_attempt(0).as_millis(), 100);
        assert_eq!(config.delay_for_attempt(1).as_millis(), 200);
        assert_eq!(config.delay_for_attempt(2).as_millis(), 400);
        assert_eq!(config.delay_for_attempt(3).as_millis(), 800);
        // 应该被 max_delay 限制
        assert_eq!(config.delay_for_attempt(4).as_millis(), 1000);
    }

    #[test]
    fn test_store_error_is_retryable() {
        assert!(StoreError::Timeout("test".to_string()).is_retryable());
        assert!(StoreError::Unavailable("test".to_string()).is_retryable());
        assert!(!StoreError::NotFound("test".to_string()).is_retryable());
        assert!(!StoreError::InvalidArgument("test".to_string()).is_retryable());
    }

    #[test]
    fn test_object_metadata() {
        let meta = ObjectMetadata::new(1024, "abc123".to_string());
        assert_eq!(meta.size, 1024);
        assert_eq!(meta.etag, "abc123");
    }
}