//! Shared object and coordination metadata; no SQL connections or driver errors.
//! SQLite implements publication and fencing using these unchanged wire contracts.
use serde::{Deserialize, Serialize};

/// Immutable object identity; payload bytes are never stored in SQLite.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectRef {
    pub key: String,
    pub sha256: [u8; 32],
    pub length: u64,
}

/// Authoritative partition watermarks and immutable object references.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PartitionHead {
    pub partition_id: String,
    pub head_version: u64,
    pub owner_epoch: u64,
    pub committed_seq: u64,
    pub wal_tip: Option<ObjectRef>,
    pub manifest: Option<ObjectRef>,
    /// Immutable Record configuration + initial empty checkpoint for object-backed recovery.
    pub bootstrap: Option<ObjectRef>,
    pub indexed_seq: u64,
    pub log_base_seq: u64,
    pub log_anchor: Option<ObjectRef>,
}

/// Fixed committed input and fencing token for a bounded background index build.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexBuild {
    pub partition_id: String,
    pub build_epoch: u64,
    pub owner_epoch: u64,
    pub cut_seq: u64,
    pub base_manifest: Option<ObjectRef>,
    pub base_indexed_seq: u64,
    pub cut_wal_tip: Option<ObjectRef>,
    pub deadline_ms: i64,
}
/// Publication result. Uncertain I/O errors must be resolved with the same operation ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishOutcome {
    Published(PartitionHead),
    AlreadyPublished(PartitionHead),
    Conflict,
}
