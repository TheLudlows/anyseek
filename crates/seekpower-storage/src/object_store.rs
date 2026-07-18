//! ObjectStore trait 定义
//!
//! 定义了统一的对象存储抽象接口

use bytes::Bytes;
use std::ops::Range;

use seekpower_error::{ObjectMetadata, StoreError};

/// 对象存储 Trait
///
/// 定义了所有存储后端必须实现的核心接口
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// 存储对象（覆盖写入）
    ///
    /// 如果对象已存在，将被覆盖
    async fn put(&self, location: &str, bytes: Bytes) -> Result<(), StoreError>;

    /// 获取对象
    ///
    /// 如果对象不存在，返回 NotFound 错误
    async fn get(&self, location: &str) -> Result<Bytes, StoreError>;

    /// 获取对象范围数据
    ///
    /// 支持 offset 开始读取指定长度的数据
    /// 如果 len 为 None，读取到文件末尾
    async fn range(&self, location: &str, offset: u64, len: Option<u64>) -> Result<Bytes, StoreError>;

    /// 删除对象
    ///
    /// 如果对象不存在，也返回成功
    async fn delete(&self, location: &str) -> Result<(), StoreError>;

    /// 列举前缀下的所有对象
    ///
    /// 返回指定前缀下的所有对象键
    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>, StoreError>;

    /// 条件写入（不存在才写入）
    ///
    /// 如果对象已存在，返回 false
    /// 如果对象不存在，写入并返回 true
    async fn put_if_absent(&self, location: &str, bytes: Bytes) -> Result<bool, StoreError>;

    /// 条件更新（ETag 匹配才更新）
    ///
    /// 如果 ETag 匹配，更新并返回新的 ETag
    /// 如果 ETag 不匹配，返回错误
    async fn put_if_match(
        &self,
        location: &str,
        bytes: Bytes,
        etag: &str,
    ) -> Result<String, StoreError>;

    /// 获取对象元数据
    ///
    /// 如果对象不存在，返回 NotFound 错误
    async fn metadata(&self, location: &str) -> Result<ObjectMetadata, StoreError>;
}

/// 对象范围查询辅助类型
pub type ObjectRange = Range<u64>;

/// 存储后端类型
#[derive(Debug, Clone)]
pub enum BackendType {
    /// S3 存储
    S3 {
        bucket: String,
        region: String,
        prefix: String,
        /// 自定义 endpoint（用于 S3 兼容服务如 MinIO）
        endpoint: Option<String>,
    },
    /// 文件系统存储
    Fs { root: std::path::PathBuf },
}

impl BackendType {
    /// 创建 S3 后端配置
    pub fn s3(bucket: String, region: String, prefix: String, endpoint: Option<String>) -> Self {
        Self::S3 {
            bucket,
            region,
            prefix,
            endpoint,
        }
    }

    /// 创建文件系统后端配置
    pub fn fs(root: std::path::PathBuf) -> Self {
        Self::Fs { root }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_type_s3() {
        let backend = BackendType::s3("bucket".to_string(), "us-east-1".to_string(), "prefix".to_string(), None);
        match backend {
            BackendType::S3 { bucket, region, prefix, endpoint } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(region, "us-east-1");
                assert_eq!(prefix, "prefix");
                assert_eq!(endpoint, None);
            }
            _ => panic!("Expected S3 backend"),
        }
    }

    #[test]
    fn test_backend_type_s3_with_endpoint() {
        let backend = BackendType::s3(
            "bucket".to_string(),
            "us-east-1".to_string(),
            "prefix".to_string(),
            Some("http://localhost:9000".to_string()),
        );
        match backend {
            BackendType::S3 { bucket, region, prefix, endpoint } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(region, "us-east-1");
                assert_eq!(prefix, "prefix");
                assert_eq!(endpoint, Some("http://localhost:9000".to_string()));
            }
            _ => panic!("Expected S3 backend"),
        }
    }

    #[test]
    fn test_backend_type_fs() {
        let backend = BackendType::fs("/tmp/test".into());
        match backend {
            BackendType::Fs { root } => {
                assert_eq!(root, std::path::PathBuf::from("/tmp/test"));
            }
            _ => panic!("Expected Fs backend"),
        }
    }
}