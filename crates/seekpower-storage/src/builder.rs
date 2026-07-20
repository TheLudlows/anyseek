//! Builder 模式实现
//!
//! 支持配置后端类型（S3/FS）

use std::sync::Arc;

use crate::{fs_store::FsStore, BackendType, ObjectStore, S3Store};
use seekpower_error::Error;

/// 对象存储构建器
///
/// 使用 Builder 模式创建配置好的对象存储实例
pub struct ObjectStoreBuilder {
    backend: BackendType,
}

impl ObjectStoreBuilder {
    /// 创建新的构建器
    pub fn new(backend: BackendType) -> Self {
        Self { backend }
    }

    /// 构建对象存储实例
    pub async fn build(self) -> Result<Arc<dyn ObjectStore>, Error> {
        let store: Arc<dyn ObjectStore> = match self.backend {
            BackendType::Fs { root } => Arc::new(FsStore::new(root)?),
            BackendType::S3 {
                bucket,
                region,
                prefix,
                endpoint,
            } => Arc::new(S3Store::new(bucket, region, prefix, endpoint).await?),
        };

        Ok(store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_build_fs_store() {
        let temp_dir = tempfile::tempdir().unwrap();
        let store = ObjectStoreBuilder::new(BackendType::Fs {
            root: temp_dir.path().to_path_buf(),
        })
        .build()
        .await
        .unwrap();

        let data = Bytes::from("hello world");
        store.put("test.txt", data.clone()).await.unwrap();
        let result = store.get("test.txt").await.unwrap();

        assert_eq!(result, data);
    }
}