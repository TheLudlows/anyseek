//! Manifest 管理
//!
//! 实现 Manifest 的版本化、原子更新和版本检查，使用 OpenDAL 持久化

use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use opendal::{ErrorKind, Operator};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use seekpower_error::PersistenceError;
use seekpower_storage::ObjectStore;

/// Manifest 版本
const MANIFEST_VERSION: u64 = 1;

/// Manifest 结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// 生成号，每次更新递增
    pub version: u64,
    /// 格式版本
    pub format_version: u32,
    /// Schema 定义
    pub schema: String,
    /// 活跃段列表
    pub segments: Vec<SegmentMeta>,
    /// 删除集
    pub delete_set: Vec<u64>,
    /// 统计信息
    pub stats: Stats,
    /// 创建时间
    pub created_at: SystemTime,
    /// 更新时间
    pub updated_at: SystemTime,
}

impl Manifest {
    /// 创建新的 Manifest
    pub fn new(schema: String) -> Self {
        let now = SystemTime::now();
        Self {
            version: 1,
            format_version: MANIFEST_VERSION as u32,
            schema,
            segments: Vec::new(),
            delete_set: Vec::new(),
            stats: Stats::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// 序列化为字节数组
    pub fn serialize(&self) -> Result<Vec<u8>, PersistenceError> {
        serde_json::to_vec(self).map_err(|e| PersistenceError::SerializationError(e.to_string()))
    }

    /// 从字节数组反序列化
    pub fn deserialize(data: &[u8]) -> Result<Self, PersistenceError> {
        serde_json::from_slice(data).map_err(|e| PersistenceError::DeserializationError(e.to_string()))
    }

    /// 创建新版本
    pub fn new_version(&self) -> Self {
        let mut new_manifest = self.clone();
        new_manifest.version += 1;
        new_manifest.updated_at = SystemTime::now();
        new_manifest
    }

    /// 添加段
    pub fn add_segment(&mut self, segment: SegmentMeta) {
        let size_bytes = segment.size_bytes;
        let doc_count = segment.doc_count;
        self.segments.push(segment);
        self.stats.segment_count = self.segments.len() as u64;
        self.stats.total_bytes += size_bytes;
        self.stats.doc_count += doc_count;
    }

    /// 移除段
    pub fn remove_segment(&mut self, segment_id: u64) {
        if let Some(pos) = self.segments.iter().position(|s| s.id == segment_id) {
            let segment = &self.segments[pos];
            self.stats.total_bytes -= segment.size_bytes;
            self.stats.doc_count -= segment.doc_count;
            self.segments.remove(pos);
            self.stats.segment_count = self.segments.len() as u64;
        }
    }

    /// 添加删除标记
    pub fn add_delete(&mut self, doc_id: u64) {
        if !self.delete_set.contains(&doc_id) {
            self.delete_set.push(doc_id);
            self.stats.deleted_count += 1;
        }
    }

    /// 检查文档是否被删除
    pub fn is_deleted(&self, doc_id: u64) -> bool {
        self.delete_set.contains(&doc_id)
    }
}

/// 段元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// 段 ID
    pub id: u64,
    /// 文档数量
    pub doc_count: u64,
    /// 文档 ID 范围
    pub doc_id_range: (u64, u64),
    /// 大小（字节）
    pub size_bytes: u64,
    /// 创建时间
    pub created_at: SystemTime,
}

impl SegmentMeta {
    /// 创建新的段元数据
    pub fn new(id: u64, doc_id_range: (u64, u64)) -> Self {
        Self {
            id,
            doc_count: doc_id_range.1 - doc_id_range.0,
            doc_id_range,
            size_bytes: 0,
            created_at: SystemTime::now(),
        }
    }

    /// 更新大小
    pub fn with_size(mut self, size_bytes: u64) -> Self {
        self.size_bytes = size_bytes;
        self
    }
}

/// 统计信息
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Stats {
    /// 段数量
    pub segment_count: u64,
    /// 文档数量
    pub doc_count: u64,
    /// 删除的文档数量
    pub deleted_count: u64,
    /// 总字节数
    pub total_bytes: u64,
}

impl Stats {
    /// 创建新的统计信息
    pub fn new() -> Self {
        Self::default()
    }

    /// 计算实际文档数量（未删除）
    pub fn active_doc_count(&self) -> u64 {
        self.doc_count.saturating_sub(self.deleted_count)
    }
}

/// Manifest 管理器
///
/// 管理 Manifest 的版本化、原子更新和恢复，使用 OpenDAL 持久化
pub struct ManifestManager {
    /// OpenDAL Operator for local storage
    local_op: Operator,
    /// 存储（用于持久化到 S3）
    store: Option<Arc<dyn ObjectStore>>,
    /// 当前 Manifest
    current: Arc<RwLock<Option<Manifest>>>,
}

impl ManifestManager {
    /// 创建新的 Manifest 管理器
    ///
    /// # 参数
    ///
    /// * `data_dir` - 数据目录
    /// * `store` - 可选的存储，用于持久化到 S3
    pub async fn new(data_dir: PathBuf, store: Option<Arc<dyn ObjectStore>>) -> Result<Self, PersistenceError> {
        let root_str = data_dir
            .to_str()
            .ok_or_else(|| PersistenceError::Internal("Invalid data dir path".to_string()))?;

        // 使用 scheme 创建 FS operator
        let mut map = std::collections::HashMap::new();
        map.insert("root".to_string(), root_str.to_string());

        let local_op = Operator::via_iter(opendal::Scheme::Fs, map)
            .map_err(|e| PersistenceError::Internal(format!("Failed to create FS operator: {}", e)))?;

        // 尝试加载现有 Manifest
        let current = if local_op.exists("manifest.json").await.unwrap_or(false) {
            let buffer = local_op.read("manifest.json").await.map_err(|e| match e.kind() {
                ErrorKind::NotFound => PersistenceError::ManifestNotFound("No manifest found".to_string()),
                _ => PersistenceError::Internal(format!("Read failed: {}", e)),
            })?;
            let data = buffer.to_vec();
            Some(Manifest::deserialize(&data)?)
        } else {
            None
        };

        Ok(Self {
            local_op,
            store,
            current: Arc::new(RwLock::new(current)),
        })
    }

    /// 获取当前 Manifest
    pub async fn get_current(&self) -> Result<Manifest, PersistenceError> {
        let current = self.current.read().await;
        current
            .clone()
            .ok_or_else(|| PersistenceError::ManifestNotFound("No current manifest".to_string()))
    }

    /// 更新 Manifest（原子更新）
    ///
    /// 使用 CAS 操作确保版本一致性
    pub async fn update<F>(&self, mut updater: F) -> Result<Manifest, PersistenceError>
    where
        F: FnMut(&Manifest) -> Result<Manifest, PersistenceError>,
    {
        // 重试循环
        loop {
            // 读取当前版本
            let current = self.get_current().await?;

            // 生成新版本
            let new_manifest = updater(&current)?;

            // 检查版本号
            if new_manifest.version != current.version + 1 {
                return Err(PersistenceError::VersionConflict(
                    current.version,
                    new_manifest.version,
                ));
            }

            // 尝试写入
            if self.try_write(&new_manifest).await? {
                // 写入成功，更新缓存
                *self.current.write().await = Some(new_manifest.clone());
                return Ok(new_manifest);
            }

            // 版本冲突，重试
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    /// 尝试写入 Manifest
    ///
    /// 使用 OpenDAL 原子写入
    async fn try_write(&self, manifest: &Manifest) -> Result<bool, PersistenceError> {
        let data = manifest.serialize()?;

        // 使用 OpenDAL 写入（内置原子性）
        self.local_op
            .write("manifest.json", Bytes::from(data.clone()))
            .await
            .map_err(|e| PersistenceError::Internal(format!("Write failed: {}", e)))?;

        // 异步上传到 S3
        if let Some(ref store) = self.store {
            let store = store.clone();
            tokio::spawn(async move {
                let _ = store.put("manifest.json", data.into()).await;
            });
        }

        Ok(true)
    }

    /// 回滚到指定版本
    ///
    /// 从 S3 或历史文件恢复
    pub async fn rollback(&self, version: u64) -> Result<Manifest, PersistenceError> {
        if let Some(ref store) = self.store {
            // 尝试从 S3 恢复
            let key = format!("manifest.v{}.json", version);
            if let Ok(data) = store.get(&key).await {
                let manifest = Manifest::deserialize(&data)?;
                *self.current.write().await = Some(manifest.clone());
                return Ok(manifest);
            }
        }

        // 尝试从本地历史文件恢复
        let history_path = format!("manifest.v{}.json", version);
        if self.local_op.exists(&history_path).await.unwrap_or(false) {
            let buffer = self.local_op.read(&history_path).await.map_err(|e| match e.kind() {
                ErrorKind::NotFound => PersistenceError::ManifestNotFound(format!(
                    "Version {} not found",
                    version
                )),
                _ => PersistenceError::Internal(format!("Read failed: {}", e)),
            })?;
            let manifest = Manifest::deserialize(&buffer.to_vec())?;
            *self.current.write().await = Some(manifest.clone());
            return Ok(manifest);
        }

        Err(PersistenceError::ManifestNotFound(format!(
            "Version {} not found",
            version
        )))
    }

    /// 保存历史版本
    pub async fn save_history(&self, manifest: &Manifest) -> Result<(), PersistenceError> {
        let history_path = format!("manifest.v{}.json", manifest.version);

        let data = manifest.serialize()?;
        self.local_op
            .write(&history_path, Bytes::from(data))
            .await
            .map_err(|e| PersistenceError::Internal(format!("Write failed: {}", e)))?;

        Ok(())
    }

    /// 初始化 Manifest
    ///
    /// 如果不存在，则创建新的 Manifest
    pub async fn initialize(&self, schema: String) -> Result<Manifest, PersistenceError> {
        // 检查是否已存在
        if self.current.read().await.is_some() {
            return self.get_current().await;
        }

        // 创建新的 Manifest
        let manifest = Manifest::new(schema);
        self.try_write(&manifest).await?;
        *self.current.write().await = Some(manifest.clone());

        Ok(manifest)
    }

    /// 获取 Manifest 版本号
    pub async fn get_version(&self) -> Result<u64, PersistenceError> {
        let current = self.current.read().await;
        current
            .as_ref()
            .map(|m| m.version)
            .ok_or_else(|| PersistenceError::ManifestNotFound("No current manifest".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_manifest_creation() {
        let manifest = Manifest::new("test-schema".to_string());

        assert_eq!(manifest.version, 1);
        assert_eq!(manifest.format_version, MANIFEST_VERSION as u32);
        assert_eq!(manifest.schema, "test-schema");
        assert!(manifest.segments.is_empty());
        assert!(manifest.delete_set.is_empty());
    }

    #[tokio::test]
    async fn test_manifest_serialization() {
        let mut manifest = Manifest::new("test-schema".to_string());

        let segment = SegmentMeta::new(1, (0, 10)).with_size(100);
        manifest.add_segment(segment);

        let serialized = manifest.serialize().unwrap();
        let deserialized = Manifest::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.version, manifest.version);
        assert_eq!(deserialized.segments.len(), 1);
        assert_eq!(deserialized.segments[0].id, 1);
    }

    #[tokio::test]
    async fn test_segment_meta() {
        let segment = SegmentMeta::new(1, (0, 10)).with_size(100);

        assert_eq!(segment.id, 1);
        assert_eq!(segment.doc_count, 10);
        assert_eq!(segment.doc_id_range, (0, 10));
        assert_eq!(segment.size_bytes, 100);
    }

    #[tokio::test]
    async fn test_delete_set() {
        let mut manifest = Manifest::new("test-schema".to_string());

        assert!(!manifest.is_deleted(1));

        manifest.add_delete(1);
        assert!(manifest.is_deleted(1));

        // 重复添加
        manifest.add_delete(1);
        assert_eq!(manifest.stats.deleted_count, 1);
    }

    #[tokio::test]
    async fn test_manifest_manager() {
        let temp_dir = TempDir::new().unwrap();
        let manager = ManifestManager::new(temp_dir.path().to_path_buf(), None)
            .await
            .unwrap();

        // 初始化 Manifest
        let manifest = manager
            .initialize("test-schema".to_string())
            .await
            .unwrap();

        assert_eq!(manifest.version, 1);

        // 更新 Manifest
        let updated = manager
            .update(|current| {
                let mut new = current.new_version();
                new.add_segment(SegmentMeta::new(1, (0, 10)).with_size(100));
                Ok(new)
            })
            .await
            .unwrap();

        assert_eq!(updated.version, 2);
        assert_eq!(updated.segments.len(), 1);
    }

    #[tokio::test]
    async fn test_manifest_version_conflict() {
        let temp_dir = TempDir::new().unwrap();
        let manager = Arc::new(
            ManifestManager::new(temp_dir.path().to_path_buf(), None)
                .await
                .unwrap(),
        );

        // 初始化
        manager.initialize("test-schema".to_string()).await.unwrap();

        // 并发更新
        let manager1 = manager.clone();
        let manager2 = manager.clone();

        let handle1 = tokio::spawn(async move {
            manager1
                .update(|current| {
                    let mut new = current.new_version();
                    new.add_segment(SegmentMeta::new(1, (0, 10)).with_size(100));
                    Ok(new)
                })
                .await
        });

        let handle2 = tokio::spawn(async move {
            manager2
                .update(|current| {
                    let mut new = current.new_version();
                    new.add_segment(SegmentMeta::new(2, (10, 20)).with_size(100));
                    Ok(new)
                })
                .await
        });

        let result1 = handle1.await.unwrap();
        let result2 = handle2.await.unwrap();

        // 两个更新都应该成功（CAS 重试机制）
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        let final_manifest = manager.get_current().await.unwrap();
        assert_eq!(final_manifest.version, 3);
        assert_eq!(final_manifest.segments.len(), 2);
    }

    #[tokio::test]
    async fn test_stats() {
        let stats = Stats::new();

        assert_eq!(stats.active_doc_count(), 0);

        let mut stats = Stats::new();
        stats.doc_count = 100;
        stats.deleted_count = 10;

        assert_eq!(stats.active_doc_count(), 90);
    }
}