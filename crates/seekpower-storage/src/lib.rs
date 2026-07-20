//! SeekPower 存储模块
//!
//! 提供统一的对象存储抽象和多种存储后端实现

pub mod builder;
mod fs;
pub mod object_store;
pub mod s3_store;

pub use builder::ObjectStoreBuilder;
use bytes::Bytes;
pub use object_store::{BackendType, FsBackendBuilder, ObjectStore, S3BackendBuilder};
pub use s3_store::S3Store;
use serde::{Deserialize, Serialize};
use std::fmt;

/// SeekPower 存储模块版本
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub version: ObjectVersion,
}
impl ObjectMeta {
    /// 创建新的对象元数据
    pub fn new(key: String, size: u64, version: ObjectVersion) -> Self {
        Self { key, size, version }
    }
}

impl fmt::Display for ObjectMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ObjectMetadata {{ size: {}, version: {:?}, key: {} }}",
            self.size, self.version, self.key
        )
    }
}

#[derive(Clone, Debug)]
pub struct GetResult {
    pub bytes: Bytes,
    pub version: ObjectVersion,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, Default, Serialize, Deserialize)]
pub struct ObjectVersion(pub String);

pub fn version_of(bytes: &[u8]) -> ObjectVersion {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(bytes);
    ObjectVersion(format!("sha256-{digest:x}"))
}


