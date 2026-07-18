//! 统一重试策略
//!
//! 实现带有指数退避的重试策略

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use seekpower_error::StoreError;

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
    pub fn delay_for_attempt(&self, attempt: usize) -> Duration {
        let factor = self.backoff_factor.pow(attempt as u32) as u64;
        let delay_ms = (self.initial_delay_ms * factor).min(self.max_delay_ms);
        Duration::from_millis(delay_ms)
    }
}

/// 重试函数
///
/// 使用指数退避策略重试可重试的错误
pub async fn retry_with_backoff<F, T, E>(
    f: F,
    config: &RetryConfig,
) -> Result<T, StoreError>
where
    F: Fn() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>>,
    E: Into<StoreError>,
{
    let mut attempt = 0;

    loop {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                let error: StoreError = e.into();

                // 如果不可重试或达到最大重试次数，直接返回错误
                if !error.is_retryable() || attempt >= config.max_retries {
                    if attempt >= config.max_retries {
                        return Err(StoreError::Internal(format!(
                            "retry exhausted after {} attempts: {}",
                            attempt + 1,
                            error
                        )));
                    }
                    return Err(error);
                }

                // 计算延迟并等待
                let delay = config.delay_for_attempt(attempt);
                tokio::time::sleep(delay).await;

                attempt += 1;
            }
        }
    }
}

/// 带有重试装饰器的函数
///
/// 为任何异步函数添加重试能力
pub fn with_retry<F, T, E>(f: F) -> impl Fn() -> Pin<Box<dyn Future<Output = Result<T, StoreError>> + Send>>
where
    F: Fn() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>> + Send + Sync + Clone + 'static,
    E: Into<StoreError> + Send + 'static,
    T: Send + 'static,
{
    move || {
        let f = f.clone();
        Box::pin(async move { f().await.map_err(|e| e.into()) })
    }
}

/// 重试装饰器
///
/// 可以包装任何返回 Result 的异步函数
pub struct RetryDecorator<F, T, E>
where
    F: Fn() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>>,
    E: Into<StoreError>,
{
    f: F,
    config: RetryConfig,
}

impl<F, T, E> RetryDecorator<F, T, E>
where
    F: Fn() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>>,
    E: Into<StoreError>,
{
    /// 创建新的重试装饰器
    pub fn new(f: F, config: RetryConfig) -> Self {
        Self { f, config }
    }

    /// 执行带有重试的操作
    pub async fn execute(&self) -> Result<T, StoreError> {
        retry_with_backoff(&self.f, &self.config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_retry_success_on_first_attempt() {
        let mut call_count = 0;
        let result = retry_with_backoff(
            || {
                Box::pin(async move {
                    call_count += 1;
                    Ok::<i32, StoreError>(42)
                })
            },
            &RetryConfig::default(),
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count, 1);
    }

    #[tokio::test]
    async fn test_retry_success_after_failure() {
        let mut call_count = 0;
        let result = retry_with_backoff(
            || {
                Box::pin(async move {
                    call_count += 1;
                    if call_count < 2 {
                        Err::<i32, _>(StoreError::Timeout("test".to_string()))
                    } else {
                        Ok(42)
                    }
                })
            },
            &RetryConfig::default(),
        )
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(call_count, 2);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let result = retry_with_backoff(
            || {
                Box::pin(async {
                    Err::<i32, _>(StoreError::Timeout("test".to_string()))
                })
            },
            &RetryConfig::default().with_max_retries(2),
        )
        .await;

        assert!(result.is_err());
        match result {
            Err(StoreError::Internal(msg)) => {
                assert!(msg.contains("retry exhausted"));
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[tokio::test]
    async fn test_non_retryable_error() {
        let result = retry_with_backoff(
            || {
                Box::pin(async {
                    Err::<i32, _>(StoreError::NotFound("test".to_string()))
                })
            },
            &RetryConfig::default(),
        )
        .await;

        assert!(result.is_err());
        match result {
            Err(StoreError::NotFound(_)) => {}
            _ => panic!("Expected NotFound error"),
        }
    }
}