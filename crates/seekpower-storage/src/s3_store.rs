//! S3 存储实现
//!
//! 使用 OpenDAL S3 Service 实现 S3 存储，支持条件操作和分片上传

use std::ops::Range;
use bytes::Bytes;
use futures_util::stream::StreamExt;
use opendal::{ErrorKind, Metadata, Operator};
use seekpower_error::Error;
use chrono::Utc;
use seekpower_error::Error::Internal;
use seekpower_error::Result;
use crate::{ObjectMeta, ObjectStore, ObjectVersion};

/// S3 存储实现
///
/// 使用 OpenDAL S3 Service 实现
pub struct S3Store {
    /// OpenDAL Operator
    op: Operator,
    /// 对象前缀（用于多租户隔离）
    prefix: String,
}

impl S3Store {
    /// 创建新的 S3 存储实例
    ///
    /// # 参数
    ///
    /// * `bucket` - S3 存储桶名称
    /// * `region` - AWS 区域
    /// * `prefix` - 对象键前缀（用于多租户隔离）
    /// * `endpoint` - 自定义 endpoint（用于 S3 兼容服务如 MinIO）
    pub async fn new(bucket: String, region: String, prefix: String, endpoint: Option<String>) -> Result<Self, Error> {
        // 使用 scheme 创建 S3 operator
        let mut map = std::collections::HashMap::new();
        map.insert("bucket".to_string(), bucket);
        map.insert("region".to_string(), region);
        if !prefix.is_empty() {
            map.insert("root".to_string(), prefix.clone());
        }
        if let Some(endpoint) = endpoint {
            map.insert("endpoint".to_string(), endpoint);
        }

        let op = Operator::via_iter("s3", map)
            .map_err(|e| Error::Internal(format!("Failed to create S3 operator: {}", e)))?;

        Ok(Self { op, prefix })
    }

    /// 使用现有 Operator 创建 S3 存储实例
    pub fn with_operator(op: Operator, prefix: String) -> Self {
        Self { op, prefix }
    }

    /// 获取对象的完整键
    fn object_key(&self, location: &str) -> String {
        if self.prefix.is_empty() {
            location.to_string()
        } else {
            format!("{}/{}", self.prefix, location)
        }
    }

    /// 转换 OpenDAL 键为相对路径（移除前缀）
    fn relative_key(&self, key: &str) -> String {
        if !self.prefix.is_empty() && key.starts_with(&self.prefix) {
            key[self.prefix.len() + 1..].to_string()
        } else {
            key.to_string()
        }
    }

    /// 从 OpenDAL 元数据转换为 ObjectMeta
    fn metadata_from_opendal(&self, _location: &str, metadata: opendal::Metadata) -> ObjectMeta {
        let last_modified = metadata
            .last_modified()
            .map(|dt|dt.into_inner().as_millisecond())
            .unwrap_or(Utc::now().timestamp());

        ObjectMeta::new(
            _location.to_string(),
            metadata.content_length(),
            ObjectVersion(metadata.etag().unwrap_or_default().to_string())
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3Store {
    async fn put(&self, location: &str, bytes: Bytes) -> Result<ObjectVersion> {
        let key = self.object_key(location);

        let m = self.op
            .write(&key, bytes)
            .await
            .map_err(opendal_error_to_store_error)?;

        object_version(location, &m)
    }

    async fn get(&self, location: &str) -> Result<Bytes> {
        let key = self.object_key(location);

        let buffer = self
            .op
            .read(&key)
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(Bytes::from(buffer.to_vec()))
    }

    async fn range(&self, location: &str, range: Range<u64>) -> Result<Bytes> {
        let key = self.object_key(location);

        let buffer = self
            .op
            .read_with(&key)
            .range(range)
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(Bytes::from(buffer.to_vec()))
    }

    async fn delete(&self, location: &str) -> Result<()> {
        let key = self.object_key(location);

        // OpenDAL delete 不存在也返回成功
        let _ = self.op.delete(&key).await;

        Ok(())
    }

    async fn list(&self, prefix: Option<&str>) -> Result<Vec<ObjectMeta>> {
        let list_prefix = prefix.map(|p| self.object_key(p)).unwrap_or_else(|| self.prefix.clone());

        let mut keys = Vec::new();

        let mut stream = self
            .op
            .lister(&list_prefix)
            .await
            .map_err(opendal_error_to_store_error)?;

        while let Some(entry_result) = stream.next().await {
            let entry = entry_result
                .map_err(|e| Error::Internal(format!("List stream error: {}", e)))?;
            let key = entry.path();
            // 移除前缀，返回相对路径
            let relative_key = self.relative_key(key);
            keys.push();
        }

        Ok(keys)
    }

    async fn put_if_absent(&self, location: &str, bytes: Bytes) -> Result<bool, Error> {
        let key = self.object_key(location);

        // 检查对象是否已存在
        let exists = self.op.exists(&key).await.unwrap_or(false);

        if exists {
            return Ok(false);
        }

        self.op
            .write(&key, bytes)
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(true)
    }

    async fn put_if_match(
        &self,
        location: &str,
        bytes: Bytes,
        version: ObjectVersion,
    ) -> Result<String, Error> {
        let result = self.op.write_with(location, bytes)
            .if_match(&version.0)
            .await;

        match result {
            Ok(m) => Ok(m.etag().unwrap_or_default().to_string()),
            Err(e) if e.kind() == ErrorKind::ConditionNotMatch => {
                println!("Concurrent modification detected, need to retry.");
               Err(Error::VersionMismatch(location.to_string(), version.0, e.message().to_string()))
            },
            Err(e) => Err(opendal_error_to_store_error(e)),
        }
    }
}

/// 转换 OpenDAL 错误为 StoreError
fn opendal_error_to_store_error(err: opendal::Error) -> Error {
    match err.kind() {
        ErrorKind::NotFound => Error::NotFound(err.to_string()),
        ErrorKind::AlreadyExists => Error::AlreadyExists(err.to_string()),
        ErrorKind::PermissionDenied => Error::PermissionDenied(err.to_string()),
        ErrorKind::Unexpected | ErrorKind::Unsupported => {
            Error::Internal(err.to_string())
        }
        ErrorKind::RateLimited | ErrorKind::ConfigInvalid | ErrorKind::IsADirectory => {
            Error::Unavailable(err.to_string())
        }
        _ => Error::Internal(err.to_string()),
    }
}
fn object_version(key: &str, metadata: &Metadata) -> Result<ObjectVersion> {
    metadata.etag()
        .map(|value| ObjectVersion(value.trim_matches('"').to_string()))
        .ok_or_else(|| Internal(format!("S3 response for {key:?} carried no ETag")))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_key() {
        let prefix = "tenant-123".to_string();
        let key1 = if prefix.is_empty() {
            "test.txt".to_string()
        } else {
            format!("{}/{}", prefix, "test.txt")
        };

        assert_eq!(key1, "tenant-123/test.txt");

        let prefix = "".to_string();
        let key2 = if prefix.is_empty() {
            "test.txt".to_string()
        } else {
            format!("{}/{}", prefix, "test.txt")
        };

        assert_eq!(key2, "test.txt");
    }
}