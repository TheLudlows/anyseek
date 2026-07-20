//! SeekPower 统一错误类型定义
//!

pub type Result<T, E = Error> = std::result::Result<T, E>;
#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
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

    /// Version 不匹配
    #[error("Version mismatch for {0}: expected {1}, got {2}")]
    VersionMismatch(String, String, String),

    /// 超时
    #[error("operation timeout: {0}")]
    Timeout(String),

    /// 序列化失败
    #[error("Serialization failed: {0}")]
    SerializationError(String),

    /// 反序列化失败
    #[error("Deserialization failed: {0}")]
    DeserializationError(String),

    #[error("invalid object range {start}..{end} for object of size {size}")]
    InvalidRange { start: u64, end: u64, size: u64 },
    /// 向量维度冲突
    #[error("Vector dimension conflict for field '{field_name}': existing {existing_dim} vs new {new_dim}")]
    VectorDimensionConflict {
        field_name: String,
        existing_dim: u32,
        new_dim: u32,
    },
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

impl Error {
    /// 判断错误是否可重试
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Timeout(_) | Error::Unavailable(_))
    }

    /// 获取错误类型名称
    pub fn error_type(&self) -> &'static str {
        match self {
            Error::NotFound(_) => "NotFound",
            Error::AlreadyExists(_) => "AlreadyExists",
            Error::PermissionDenied(_) => "PermissionDenied",
            Error::InvalidArgument(_) => "InvalidArgument",
            Error::Internal(_) => "Internal",
            Error::Unavailable(_) => "Unavailable",
            Error::Io(_) => "Io",
            Error::VersionMismatch(_, _, _) => "VersionMismatch",
            Error::Timeout(_) => "Timeout",
            Error::SerializationError(_) => "SerializationError",
            Error::DeserializationError(_) => "DeserializationError",
            Error::VectorDimensionConflict { .. } => "VectorDimensionConflict",
            Error::InvalidRange { .. } => "InvalidRange"
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
        assert!(Error::Timeout("test".to_string()).is_retryable());
        assert!(Error::Unavailable("test".to_string()).is_retryable());
        assert!(!Error::NotFound("test".to_string()).is_retryable());
        assert!(!Error::InvalidArgument("test".to_string()).is_retryable());
    }
}