//! S3 存储实现
//!
//! 使用 OpenDAL S3 Service 实现 S3 存储，支持条件操作和分片上传

use bytes::Bytes;
use futures_util::stream::StreamExt;
use opendal::{ErrorKind, Operator};
use seekpower_error::{ObjectMetadata, StoreError};
use std::time::SystemTime;

use crate::ObjectStore;

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
    pub async fn new(bucket: String, region: String, prefix: String, endpoint: Option<String>) -> Result<Self, StoreError> {
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

        let op = Operator::via_iter(opendal::Scheme::S3, map)
            .map_err(|e| StoreError::Internal(format!("Failed to create S3 operator: {}", e)))?;

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

    /// 从 OpenDAL 元数据转换为 ObjectMetadata
    fn metadata_from_opendal(&self, _location: &str, metadata: opendal::Metadata) -> ObjectMetadata {
        let last_modified = metadata
            .last_modified()
            .map(|dt| SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(dt.timestamp() as u64))
            .unwrap_or_else(SystemTime::now);

        ObjectMetadata::with_time(
            metadata.content_length() as u64,
            metadata.etag().unwrap_or_default().to_string(),
            last_modified,
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3Store {
    async fn put(&self, location: &str, bytes: Bytes) -> Result<(), StoreError> {
        let key = self.object_key(location);

        self.op
            .write(&key, bytes)
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(())
    }

    async fn get(&self, location: &str) -> Result<Bytes, StoreError> {
        let key = self.object_key(location);

        let buffer = self
            .op
            .read(&key)
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(Bytes::from(buffer.to_vec()))
    }

    async fn range(&self, location: &str, offset: u64, len: Option<u64>) -> Result<Bytes, StoreError> {
        let key = self.object_key(location);

        let buffer = self
            .op
            .read_with(&key)
            .range(offset..offset + len.unwrap_or(u64::MAX - offset))
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(Bytes::from(buffer.to_vec()))
    }

    async fn delete(&self, location: &str) -> Result<(), StoreError> {
        let key = self.object_key(location);

        // OpenDAL delete 不存在也返回成功
        let _ = self.op.delete(&key).await;

        Ok(())
    }

    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>, StoreError> {
        let list_prefix = prefix.map(|p| self.object_key(p)).unwrap_or_else(|| self.prefix.clone());

        let mut keys = Vec::new();

        let mut stream = self
            .op
            .lister(&list_prefix)
            .await
            .map_err(opendal_error_to_store_error)?;

        while let Some(entry_result) = stream.next().await {
            let entry = entry_result
                .map_err(|e| StoreError::Internal(format!("List stream error: {}", e)))?;
            let key = entry.path();
            // 移除前缀，返回相对路径
            let relative_key = self.relative_key(key);
            keys.push(relative_key);
        }

        Ok(keys)
    }

    async fn put_if_absent(&self, location: &str, bytes: Bytes) -> Result<bool, StoreError> {
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
        etag: &str,
    ) -> Result<String, StoreError> {
        let key = self.object_key(location);

        // 获取当前对象的元数据
        let metadata = self
            .op
            .stat(&key)
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => StoreError::NotFound(location.to_string()),
                _ => StoreError::Internal(format!("Stat failed: {}", e)),
            })?;

        let current_etag = metadata.etag().ok_or_else(|| {
            StoreError::Internal("No ETag in metadata".to_string())
        })?;

        if current_etag != etag {
            return Err(StoreError::ETagMismatch(
                location.to_string(),
                etag.to_string(),
                current_etag.to_string(),
            ));
        }

        self.op
            .write(&key, bytes)
            .await
            .map_err(opendal_error_to_store_error)?;

        // 获取新的 ETag
        let metadata = self
            .op
            .stat(&key)
            .await
            .map_err(opendal_error_to_store_error)?;

        let new_etag = metadata.etag().unwrap_or_default();
        Ok(new_etag.to_string())
    }

    async fn metadata(&self, location: &str) -> Result<ObjectMetadata, StoreError> {
        let key = self.object_key(location);

        let metadata = self
            .op
            .stat(&key)
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => StoreError::NotFound(location.to_string()),
                _ => StoreError::Internal(format!("Stat failed: {}", e)),
            })?;

        Ok(self.metadata_from_opendal(location, metadata))
    }
}

/// 转换 OpenDAL 错误为 StoreError
fn opendal_error_to_store_error(err: opendal::Error) -> StoreError {
    match err.kind() {
        ErrorKind::NotFound => StoreError::NotFound(err.to_string()),
        ErrorKind::AlreadyExists => StoreError::AlreadyExists(err.to_string()),
        ErrorKind::PermissionDenied => StoreError::PermissionDenied(err.to_string()),
        ErrorKind::Unexpected | ErrorKind::Unsupported => {
            StoreError::Internal(err.to_string())
        }
        ErrorKind::RateLimited | ErrorKind::ConfigInvalid | ErrorKind::IsADirectory => {
            StoreError::Unavailable(err.to_string())
        }
        _ => StoreError::Internal(err.to_string()),
    }
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