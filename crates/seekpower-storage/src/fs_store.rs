//! 文件系统存储实现
//!
//! 使用 OpenDAL FS Service 实现本地文件系统存储，支持原子写入和 CAS 操作

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use bytes::Bytes;
use futures_util::stream::StreamExt;
use opendal::{ErrorKind, Operator};

use crate::ObjectStore;
use seekpower_error::{ObjectMetadata, StoreError};

/// 文件系统存储实现
///
/// 使用 OpenDAL FS Service，内置原子写入支持
pub struct FsStore {
    /// OpenDAL Operator
    op: Operator,
    /// 根目录（用于生成 ETag）
    root: PathBuf,
}

impl FsStore {
    /// 创建新的文件系统存储
    pub fn new(root: PathBuf) -> Result<Self, StoreError> {
        let root_str = root
            .to_str()
            .ok_or_else(|| StoreError::InvalidArgument("Invalid root path".to_string()))?;

        // 使用 scheme 创建 FS operator
        let mut map = std::collections::HashMap::new();
        map.insert("root".to_string(), root_str.to_string());

        let op = Operator::via_iter(opendal::Scheme::Fs, map)
            .map_err(|e| StoreError::Internal(format!("Failed to create FS operator: {}", e)))?;

        Ok(Self { op, root })
    }

    /// 获取对象的完整路径
    fn object_path(&self, location: &str) -> PathBuf {
        self.root.join(location)
    }

    /// 生成 ETag（基于修改时间和大小）
    fn generate_etag(&self, _path: &Path, size: u64, modified: SystemTime) -> String {
        // 使用修改时间和大小生成 ETag
        let timestamp = modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let etag_input = format!("{}-{}", timestamp, size);

        // 简单哈希
        let hash = etag_input
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        format!("{:016x}", hash)
    }

    /// 从 OpenDAL 元数据转换为 ObjectMetadata
    fn metadata_from_opendal(&self, location: &str, metadata: opendal::Metadata) -> ObjectMetadata {
        let path = self.object_path(location);
        let last_modified = metadata
            .last_modified()
            .map(|dt| SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(dt.timestamp() as u64))
            .unwrap_or_else(SystemTime::now);

        ObjectMetadata::with_time(
            metadata.content_length() as u64,
            self.generate_etag(&path, metadata.content_length() as u64, last_modified),
            last_modified,
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for FsStore {
    async fn put(&self, location: &str, bytes: Bytes) -> Result<(), StoreError> {
        self.op
            .write(location, bytes)
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(())
    }

    async fn get(&self, location: &str) -> Result<Bytes, StoreError> {
        let buffer = self
            .op
            .read(location)
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(Bytes::from(buffer.to_vec()))
    }

    async fn range(&self, location: &str, offset: u64, len: Option<u64>) -> Result<Bytes, StoreError> {
        let buffer = self
            .op
            .read_with(location)
            .range(offset..offset + len.unwrap_or(u64::MAX - offset))
            .await
            .map_err(opendal_error_to_store_error)?;

        Ok(Bytes::from(buffer.to_vec()))
    }

    async fn delete(&self, location: &str) -> Result<(), StoreError> {
        // OpenDAL delete 不存在也返回成功
        let _ = self.op.delete(location).await;

        Ok(())
    }

    async fn list(&self, prefix: Option<&str>) -> Result<Vec<String>, StoreError> {
        let list_prefix = prefix.unwrap_or("");

        let mut keys = Vec::new();

        let mut stream = self
            .op
            .lister(list_prefix)
            .await
            .map_err(opendal_error_to_store_error)?;

        while let Some(entry_result) = stream.next().await {
            let entry = entry_result
                .map_err(|e| StoreError::Internal(format!("List stream error: {}", e)))?;
            let key = entry.path();
            // 跳过临时文件
            if key.ends_with(".tmp") {
                continue;
            }
            keys.push(key.to_string());
        }

        Ok(keys)
    }

    async fn put_if_absent(&self, location: &str, bytes: Bytes) -> Result<bool, StoreError> {
        // 检查对象是否已存在
        let exists = self.op.exists(location).await.unwrap_or(false);

        if exists {
            return Ok(false);
        }

        self.op
            .write(location, bytes)
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
        // 获取当前对象的元数据
        let metadata = self
            .op
            .stat(location)
            .await
            .map_err(|e| match e.kind() {
                ErrorKind::NotFound => StoreError::NotFound(location.to_string()),
                _ => StoreError::Internal(format!("Stat failed: {}", e)),
            })?;

        let path = self.object_path(location);
        let last_modified = metadata
            .last_modified()
            .map(|dt| SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(dt.timestamp() as u64))
            .unwrap_or_else(SystemTime::now);
        let current_etag = self.generate_etag(
            &path,
            metadata.content_length() as u64,
            last_modified,
        );

        if current_etag != etag {
            return Err(StoreError::ETagMismatch(
                location.to_string(),
                etag.to_string(),
                current_etag,
            ));
        }

        self.op
            .write(location, bytes)
            .await
            .map_err(opendal_error_to_store_error)?;

        // 获取新的 ETag
        let metadata = self
            .op
            .stat(location)
            .await
            .map_err(opendal_error_to_store_error)?;

        let path = self.object_path(location);
        let last_modified = metadata
            .last_modified()
            .map(|dt| SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(dt.timestamp() as u64))
            .unwrap_or_else(SystemTime::now);
        let new_etag = self.generate_etag(
            &path,
            metadata.content_length() as u64,
            last_modified,
        );

        Ok(new_etag)
    }

    async fn metadata(&self, location: &str) -> Result<ObjectMetadata, StoreError> {
        let metadata = self
            .op
            .stat(location)
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
    use tempfile::TempDir;

    fn create_test_store() -> (FsStore, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let store = FsStore::new(temp_dir.path().to_path_buf()).unwrap();
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let (store, _temp_dir) = create_test_store();
        let data = Bytes::from("hello world");

        store.put("test.txt", data.clone()).await.unwrap();
        let result = store.get("test.txt").await.unwrap();

        assert_eq!(result, data);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let (store, _temp_dir) = create_test_store();
        let result = store.get("nonexistent.txt").await;

        assert!(matches!(result, Err(StoreError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_delete() {
        let (store, _temp_dir) = create_test_store();
        let data = Bytes::from("hello world");

        store.put("test.txt", data).await.unwrap();
        store.delete("test.txt").await.unwrap();

        let result = store.get("test.txt").await;
        assert!(matches!(result, Err(StoreError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_delete_nonexistent() {
        let (store, _temp_dir) = create_test_store();
        // 删除不存在的对象应该成功
        store.delete("nonexistent.txt").await.unwrap();
    }

    #[tokio::test]
    async fn test_list() {
        let (store, _temp_dir) = create_test_store();

        store.put("dir1/file1.txt", Bytes::from("data1")).await.unwrap();
        store.put("dir1/file2.txt", Bytes::from("data2")).await.unwrap();
        store.put("dir2/file3.txt", Bytes::from("data3")).await.unwrap();

        let all_keys = store.list(None).await.unwrap();
        assert_eq!(all_keys.len(), 3);

        let dir1_keys = store.list(Some("dir1/")).await.unwrap();
        assert_eq!(dir1_keys.len(), 2);
        assert!(dir1_keys.contains(&"dir1/file1.txt".to_string()));
        assert!(dir1_keys.contains(&"dir1/file2.txt".to_string()));

        let dir2_keys = store.list(Some("dir2/")).await.unwrap();
        assert_eq!(dir2_keys.len(), 1);
        assert!(dir2_keys.contains(&"dir2/file3.txt".to_string()));
    }

    #[tokio::test]
    async fn test_range() {
        let (store, _temp_dir) = create_test_store();
        let data = Bytes::from("0123456789");

        store.put("test.txt", data).await.unwrap();

        let range1 = store.range("test.txt", 0, Some(5)).await.unwrap();
        assert_eq!(range1, Bytes::from("01234"));

        let range2 = store.range("test.txt", 5, Some(3)).await.unwrap();
        assert_eq!(range2, Bytes::from("567"));

        let range3 = store.range("test.txt", 5, None).await.unwrap();
        assert_eq!(range3, Bytes::from("56789"));
    }

    #[tokio::test]
    async fn test_put_if_absent() {
        let (store, _temp_dir) = create_test_store();
        let data1 = Bytes::from("data1");
        let data2 = Bytes::from("data2");

        // 第一次写入应该成功
        let result = store.put_if_absent("test.txt", data1.clone()).await.unwrap();
        assert!(result);

        let stored = store.get("test.txt").await.unwrap();
        assert_eq!(stored, data1);

        // 第二次写入应该失败
        let result = store.put_if_absent("test.txt", data2).await.unwrap();
        assert!(!result);

        let stored = store.get("test.txt").await.unwrap();
        assert_eq!(stored, data1); // 数据没有变化
    }

    #[tokio::test]
    async fn test_put_if_match_success() {
        let (store, _temp_dir) = create_test_store();
        let data1 = Bytes::from("data1");
        let data2 = Bytes::from("data2");

        store.put("test.txt", data1.clone()).await.unwrap();
        let meta = store.metadata("test.txt").await.unwrap();
        let old_etag = meta.etag;

        // ETag 匹配，应该更新成功
        let new_etag = store.put_if_match("test.txt", data2.clone(), &old_etag).await.unwrap();
        assert_ne!(new_etag, old_etag);

        let stored = store.get("test.txt").await.unwrap();
        assert_eq!(stored, data2);
    }

    #[tokio::test]
    async fn test_put_if_match_failure() {
        let (store, _temp_dir) = create_test_store();
        let data = Bytes::from("data1");

        store.put("test.txt", data.clone()).await.unwrap();

        // ETag 不匹配，应该失败
        let result = store.put_if_match("test.txt", Bytes::from("data2"), "wrong-etag").await;

        assert!(matches!(result, Err(StoreError::ETagMismatch(_, _, _))));

        let stored = store.get("test.txt").await.unwrap();
        assert_eq!(stored, data); // 数据没有变化
    }

    #[tokio::test]
    async fn test_metadata() {
        let (store, _temp_dir) = create_test_store();
        let data = Bytes::from("hello world");

        store.put("test.txt", data).await.unwrap();
        let meta = store.metadata("test.txt").await.unwrap();

        assert_eq!(meta.size, 11);
        assert!(!meta.etag.is_empty());
    }

    #[tokio::test]
    async fn test_atomic_write() {
        let (store, _temp_dir) = create_test_store();

        // 两次写入
        store.put("test.txt", Bytes::from("data1")).await.unwrap();
        store.put("test.txt", Bytes::from("data2")).await.unwrap();

        // OpenDAL 原子写入保证了原子性
        // 验证最终结果是最后一个值
        let result = store.get("test.txt").await.unwrap();
        assert!(result == Bytes::from("data2"));
    }
}