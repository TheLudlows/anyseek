//! SeekPower 存储模块
//!
//! 提供统一的对象存储抽象和多种存储后端实现

pub mod builder;
pub mod fs_store;
pub mod object_store;
pub mod retry;
pub mod s3_store;

pub use builder::ObjectStoreBuilder;
pub use object_store::{BackendType, ObjectStore};
pub use fs_store::FsStore;
pub use retry::{retry_with_backoff, RetryDecorator};
pub use s3_store::S3Store;

/// SeekPower 存储模块版本
pub const VERSION: &str = env!("CARGO_PKG_VERSION");