use bytes::Bytes;
use std::ops::Range;

use seekpower_error::Result;
use crate::{GetResult, ObjectMeta, ObjectVersion};

#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put(&self, location: &str, bytes: Bytes) -> Result<ObjectVersion>;

    async fn get(&self, location: &str) -> Result<GetResult>;

    async fn range(&self, location: &str, range: Range<u64>) -> Result<Bytes>;

    async fn delete(&self, location: &str) -> Result<()>;

    async fn list(&self, prefix: Option<&str>) -> Result<Vec<ObjectMeta>>;

    async fn put_if_absent(&self, location: &str, bytes: Bytes) -> Result<ObjectVersion>;

    async fn put_if_match(
        &self,
        location: &str,
        bytes: Bytes,
        version: ObjectVersion,
    ) -> Result<ObjectVersion>;
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

    /// 创建 S3 后端 Builder
    pub fn s3_builder() -> S3BackendBuilder {
        S3BackendBuilder::default()
    }

    /// 创建文件系统后端配置
    pub fn fs(root: std::path::PathBuf) -> Self {
        Self::Fs { root }
    }

    /// 创建文件系统后端 Builder
    pub fn fs_builder() -> FsBackendBuilder {
        FsBackendBuilder::default()
    }
}

#[derive(Debug, Default)]
pub struct S3BackendBuilder {
    bucket: Option<String>,
    region: Option<String>,
    prefix: Option<String>,
    endpoint: Option<String>,
}

impl S3BackendBuilder {
    /// 设置 S3 bucket 名称
    pub fn bucket(mut self, bucket: String) -> Self {
        self.bucket = Some(bucket);
        self
    }

    /// 设置 S3 region
    pub fn region(mut self, region: String) -> Self {
        self.region = Some(region);
        self
    }

    /// 设置 S3 key 前缀
    pub fn prefix(mut self, prefix: String) -> Self {
        self.prefix = Some(prefix);
        self
    }

    /// 设置自定义 endpoint（用于 S3 兼容服务如 MinIO）
    pub fn endpoint(mut self, endpoint: Option<String>) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// 构建 BackendType::S3
    ///
    /// bucket、region、prefix 为必填项，缺失时 panic
    pub fn build(self) -> BackendType {
        BackendType::S3 {
            bucket: self.bucket.expect("S3 bucket is required"),
            region: self.region.expect("S3 region is required"),
            prefix: self.prefix.expect("S3 prefix is required"),
            endpoint: self.endpoint,
        }
    }
}

/// 文件系统后端 Builder
///
/// 使用 Builder 模式构建文件系统后端配置
/// # Example
/// ```
/// use seekpower_storage::BackendType;
///
/// let backend = BackendType::fs_builder()
///     .root("/tmp/test".into())
///     .build();
/// ```
#[derive(Debug, Default)]
pub struct FsBackendBuilder {
    root: Option<std::path::PathBuf>,
}

impl FsBackendBuilder {
    /// 设置文件系统根路径
    pub fn root(mut self, root: std::path::PathBuf) -> Self {
        self.root = Some(root);
        self
    }

    /// 构建 BackendType::Fs
    ///
    /// root 为必填项，缺失时 panic
    pub fn build(self) -> BackendType {
        BackendType::Fs {
            root: self.root.expect("Fs root is required"),
        }
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

    #[test]
    fn test_s3_builder() {
        let backend = BackendType::s3_builder()
            .bucket("my-bucket".to_string())
            .region("us-east-1".to_string())
            .prefix("tenant-123".to_string())
            .build();
        match backend {
            BackendType::S3 { bucket, region, prefix, endpoint } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, "us-east-1");
                assert_eq!(prefix, "tenant-123");
                assert_eq!(endpoint, None);
            }
            _ => panic!("Expected S3 backend"),
        }
    }

    #[test]
    fn test_s3_builder_with_endpoint() {
        let backend = BackendType::s3_builder()
            .bucket("my-bucket".to_string())
            .region("us-east-1".to_string())
            .prefix("data".to_string())
            .endpoint(Some("http://localhost:9000".to_string()))
            .build();
        match backend {
            BackendType::S3 { bucket, region, prefix, endpoint } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, "us-east-1");
                assert_eq!(prefix, "data");
                assert_eq!(endpoint, Some("http://localhost:9000".to_string()));
            }
            _ => panic!("Expected S3 backend"),
        }
    }

    #[test]
    fn test_fs_builder() {
        let backend = BackendType::fs_builder()
            .root("/tmp/test".into())
            .build();
        match backend {
            BackendType::Fs { root } => {
                assert_eq!(root, std::path::PathBuf::from("/tmp/test"));
            }
            _ => panic!("Expected Fs backend"),
        }
    }
}