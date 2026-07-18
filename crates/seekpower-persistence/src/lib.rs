//! SeekPower 持久化模块
//!
//! 提供双层 WAL 和 Manifest 管理功能

pub mod local_wal;
pub mod manifest;
pub mod wal;
pub mod wal_manager;

pub use local_wal::LocalWAL;
pub use manifest::{Manifest, ManifestManager, SegmentMeta, Stats};
pub use wal::{WALRecord, WALRecordType};
pub use wal_manager::{WALConfig, WALManager};

/// SeekPower 持久化模块版本
pub const VERSION: &str = env!("CARGO_PKG_VERSION");