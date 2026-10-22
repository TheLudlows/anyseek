//! Explicit object-backed partition journal API.
//!
//! This is a storage-level integration point, not a replacement for RecordPartition
//! yet: the bootstrap bytes are caller-owned canonical state and log payloads are
//! caller-owned canonical operations. The Record adapter must encode/decode those
//! payloads and apply replay to the same read view before exposing search results.
use super::{
    object_log::{append_and_commit, load_committed_tail, AppendCommitOutcome, ObjectLogEntry, TailError},
    s3_compatible::{S3CompatibleStore, UploadRegisterError},
    sqlite_authority::{PartitionHead, SqliteAuthority},
};
use sha2::{Digest, Sha256};
use std::{path::{Path, PathBuf}, sync::Arc, time::Duration};
use tokio::sync::Mutex;

/// Connection and safety limits for one S3-backed partition.
#[derive(Clone, Debug)]
pub(crate) struct ObjectPartitionConfig {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub allow_http: bool,
    pub owner_epoch: u64,
    pub busy_timeout: Duration,
    pub max_object_bytes: usize,
    pub max_tail_blocks: usize,
    pub max_tail_bytes: usize,
}

/// An object-backed durable operation log with a SQLite authoritative head.
pub(crate) struct ObjectBackedPartition {
    partition_id: String,
    sqlite_path: PathBuf,
    owner_epoch: u64,
    store: Arc<S3CompatibleStore>,
    authority: Mutex<SqliteAuthority>,
    max_tail_blocks: usize,
    max_tail_bytes: usize,
}

/// Snapshot captured from a single SQLite head. The caller applies the tail entries
/// to the bootstrap state before making the resulting Record view visible.
#[derive(Clone, Debug)]
pub(crate) struct ObjectPartitionSnapshot {
    pub head: PartitionHead,
    pub bootstrap: Vec<u8>,
    pub tail: Vec<ObjectLogEntry>,
}

#[derive(Debug)]
pub(crate) enum ObjectPartitionError {
    Metadata(rusqlite::Error),
    Upload(UploadRegisterError),
    Tail(TailError),
    MissingBootstrap,
    Conflict,
}
impl std::fmt::Display for ObjectPartitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Metadata(e) => write!(f, "SQLite authority error: {e}"),
            Self::Upload(e) => write!(f, "object upload/registration error: {e}"),
            Self::Tail(e) => write!(f, "committed tail error: {e}"),
            Self::MissingBootstrap => write!(f, "object-backed partition has no bootstrap checkpoint"),
            Self::Conflict => write!(f, "partition owner epoch or head changed; retry against the current head"),
        }
    }
}
impl std::error::Error for ObjectPartitionError {}

impl ObjectBackedPartition {
    /// Initialize SQLite authority and persist the caller's canonical initial
    /// checkpoint to S3 before the partition is returned to the caller.
    pub(crate) async fn create(
        sqlite_path: &Path,
        partition_id: &str,
        bootstrap_checkpoint: &[u8],
        config: ObjectPartitionConfig,
    ) -> Result<Self, ObjectPartitionError> {
        if partition_id.is_empty() || config.max_object_bytes == 0
            || config.max_tail_blocks == 0 || config.max_tail_bytes == 0 {
            return Err(ObjectPartitionError::MissingBootstrap);
        }
        let store = Arc::new(S3CompatibleStore::connect(
            &config.endpoint, &config.bucket, &config.region, &config.access_key_id,
            &config.secret_access_key, config.allow_http, config.max_object_bytes,
        ).map_err(|e| ObjectPartitionError::Tail(TailError::ObjectStore(e)))?);
        let mut authority = SqliteAuthority::open(sqlite_path, config.busy_timeout)
            .map_err(ObjectPartitionError::Metadata)?;
        authority.create_partition(partition_id, config.owner_epoch)
            .map_err(ObjectPartitionError::Metadata)?;
        store.publish_bootstrap(
            &mut authority, partition_id, config.owner_epoch, bootstrap_checkpoint, now_ms(),
        ).await.map_err(ObjectPartitionError::Upload)?;
        Ok(Self {
            partition_id: partition_id.to_owned(),
            sqlite_path: sqlite_path.to_path_buf(),
            owner_epoch: config.owner_epoch,
            store,
            authority: Mutex::new(authority),
            max_tail_blocks: config.max_tail_blocks,
            max_tail_bytes: config.max_tail_bytes,
        })
    }

    /// Open an existing local SQLite authority. Credentials are never read from SQLite.
    pub(crate) async fn open(
        sqlite_path: &Path,
        partition_id: &str,
        config: ObjectPartitionConfig,
    ) -> Result<Self, ObjectPartitionError> {
        let store = Arc::new(S3CompatibleStore::connect(
            &config.endpoint, &config.bucket, &config.region, &config.access_key_id,
            &config.secret_access_key, config.allow_http, config.max_object_bytes,
        ).map_err(|e| ObjectPartitionError::Tail(TailError::ObjectStore(e)))?);
        let authority = SqliteAuthority::open(sqlite_path, config.busy_timeout)
            .map_err(ObjectPartitionError::Metadata)?;
        let head = authority.read_head(partition_id).map_err(ObjectPartitionError::Metadata)?
            .ok_or(ObjectPartitionError::MissingBootstrap)?;
        if head.owner_epoch != config.owner_epoch || head.bootstrap.is_none() {
            return Err(ObjectPartitionError::MissingBootstrap);
        }
        Ok(Self {
            partition_id: partition_id.to_owned(),
            sqlite_path: sqlite_path.to_path_buf(),
            owner_epoch: config.owner_epoch,
            store,
            authority: Mutex::new(authority),
            max_tail_blocks: config.max_tail_blocks,
            max_tail_bytes: config.max_tail_bytes,
        })
    }

    /// Append an immutable canonical operation. The digest is computed from the exact
    /// payload bytes; a repeated operation ID with different bytes is rejected by the
    /// authority receipt. The payload is not searchable until the caller applies it.
    pub(crate) async fn append(
        &self,
        operation_id: &str,
        canonical_operation: Vec<u8>,
    ) -> Result<AppendCommitOutcome, ObjectPartitionError> {
        if operation_id.is_empty() {
            return Err(ObjectPartitionError::Conflict);
        }
        let digest: [u8;32] = Sha256::digest(&canonical_operation).into();
        let mut authority = self.authority.lock().await;
        append_and_commit(
            &self.store, &mut authority, &self.partition_id, self.owner_epoch,
            operation_id, digest, canonical_operation, now_ms(),
            self.store.max_object_bytes,
        ).await.map_err(ObjectPartitionError::Tail)
    }

    /// Capture the authoritative head and load its immutable bootstrap plus the exact
    /// committed-but-unindexed tail. The caller must publish its derived Record state
    /// atomically; partial replay is never returned.
    pub(crate) async fn snapshot(&self) -> Result<ObjectPartitionSnapshot, ObjectPartitionError> {
        let authority = self.authority.lock().await;
        let head = authority.read_head(&self.partition_id).map_err(ObjectPartitionError::Metadata)?
            .ok_or(ObjectPartitionError::MissingBootstrap)?;
        if head.owner_epoch != self.owner_epoch {
            return Err(ObjectPartitionError::Conflict);
        }
        let bootstrap_ref = head.bootstrap.as_ref().ok_or(ObjectPartitionError::MissingBootstrap)?;
        let bootstrap = self.store.get_verified(bootstrap_ref).await
            .map_err(|e| ObjectPartitionError::Tail(TailError::ObjectStore(e)))?;
        let tail = load_committed_tail(
            &self.store, &head, self.max_tail_blocks, self.max_tail_bytes,
        ).await.map_err(ObjectPartitionError::Tail)?;
        Ok(ObjectPartitionSnapshot { head, bootstrap, tail })
    }

    pub(crate) fn sqlite_path(&self) -> &Path { &self.sqlite_path }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default().as_millis().min(i64::MAX as u128) as i64
}
