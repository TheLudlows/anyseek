//! Single-partition incremental record engine backed by the existing retrieval
//! engine. The default HTTP service uses this authoritative storage path. See
//! docs/record-model.md for the bounded P1-A scope and validation evidence.
use crate::{
    component::Commit,
    index::{Document, IndexArc, IndexDocuments, IndexMetaObject, SchemaField, create_index},
    search::{FacetFilter, QueryType, ResultSort, ResultType, SearchMode},
    storage::{journal::CreateError, partition::PartitionJournal},
    vector::Embedding,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Semaphore};

mod evidence;
mod enrichment;
mod generation;
mod upload;
mod temporal;
mod change_feed;
mod graph;
mod fusion;
pub use fusion::{RelationFusion, FusionWindow};
pub use graph::{RelationMetadata, RelationDirection, NeighborRequest, NeighborPage, NeighborHit, RelationCount};
pub use change_feed::{FeedConfig, ChangeKind, ChangeEvent, ChangePage, BootstrapPage};
pub use temporal::{TemporalMetadata, TemporalFilter, TimeRange};
pub use generation::{GenerationChunk, GenerationCommand, GenerationMember, GenerationStatus};
mod output;
pub use evidence::{EvidenceKind, EvidenceMetadata, EvidenceRef, EvidenceVisibility};
pub use output::DataOutputPermit;
mod lease;
mod locator;
pub use locator::{EvidenceLocator, EvidenceExcerpt};
mod mutation;
mod write;
mod index_consumers;
mod publication;
mod query;
mod replay;
mod indexer;
mod maintenance;
mod cache_merge;
mod ingest_log;
mod ingestion;
pub use ingestion::{IngestPipeline, IngestReceipt, IngestStatus, IngestProgress, IngestFailure};
pub use indexer::IndexJob;
mod segment_state;
mod state_chunks;
mod state_memory;
mod state_values;
mod state_proof;
mod capacity;
mod snapshot;
mod checkpoint_codec;
mod segment;
mod anchor;
mod anchor_image;
mod package_pins;
#[cfg(feature="s3-object-store")]
mod remote_checkpoint;
pub(crate) mod build_budget;
mod view;
mod recovery;
// Native package implementation; authority routing switches atomically after integration.
pub(crate) mod lexical;
pub use recovery::{RecoveryIdentity, SealCertificate, SealRequest};

/// Failures distinguish rejected requests from operations that require recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordError {
    /// Trusted clock moved outside the supported calibration budget.
    ClockUncertain,
    /// Retained feed plus mandatory invalidation reserve is full.
    FeedFull,
    /// Cursor is outside retained history or belongs to another recovery scope.
    CursorExpired(u64),
    /// Bootstrap pin is gone, expired, or invalidated; discard partial state.
    BootstrapExpired,
    /// Admission refused before operation lookup; prior attempts may have succeeded.
    Busy,
    /// Invalid request or an exhausted resource budget; no write accepted.
    Rejected(String),
    /// Conditional version, routing key, or operation digest conflict.
    Conflict(String),
    /// The operation's epoch is outside the supported retry window.
    OperationExpired,
    /// Durable operation could not be applied; reopen/replay is required.
    DurablePending(u64),
    /// Persistence may have committed the operation. Do not invent a new ID.
    OutcomeUnknown(String),
    /// This instance cannot serve reads or writes until recovery.
    RecoveryRequired,
    /// Corrupt state or failure while opening/building the engine.
    Storage(String),
}
impl std::fmt::Display for RecordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for RecordError {}
fn storage(error: impl std::fmt::Display) -> RecordError {
    RecordError::Storage(error.to_string())
}

/// Immutable collection configuration, persisted with authoritative state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordConfig {
    /// Durable replay window and admission budgets.
    #[serde(default)]
    pub change_feed: FeedConfig,
    /// Existing retrieval configuration; this partition uses one shard.
    pub index: IndexMetaObject,
    /// Search schema. `store` controls projections, not authoritative retention.
    pub schema: Vec<SchemaField>,
    /// Model/space identity, required when vectors are configured.
    pub embedding_space_id: Option<String>,
    /// Cap on live records plus retained tombstones (no implicit eviction).
    pub max_records: usize,
    /// Per-command serialized byte budget.
    pub max_record_bytes: usize,
    /// Cap on retained operation results (backpressure, never silent eviction).
    pub max_operations: usize,
    /// Maximum bytes per journal generation; checkpoint publication may use two.
    pub max_journal_bytes: u64,
    /// Candidate/final-result bound for this partition.
    pub max_candidates: usize,
}
/// Shared admission check for collection intent and physical initialization.
pub fn validate_config(config: &RecordConfig) -> Result<(), RecordError> {
    change_feed::validate_config(&config.change_feed)?;
    if config.schema.is_empty()
        || config.schema.len() > 64
        || config
            .schema
            .iter()
            .any(|f| f.field.is_empty() || !f.boost.is_finite() || f.boost <= 0.0)
    {
        return Err(RecordError::Rejected("invalid or oversized schema".into()));
    }
    let mut names = std::collections::BTreeSet::new();
    if config.schema.iter().any(|f| !names.insert(&f.field)) {
        return Err(RecordError::Rejected("duplicate schema field".into()));
    }
    if let crate::vector::Inference::External { dimensions, .. } = &config.index.inference {
        if *dimensions > 4096 {
            return Err(RecordError::Rejected(
                "vector dimension budget exceeded".into(),
            ));
        }
    }
    if config.max_records == 0
        || config.max_records > capacity::MAX_RECORDS
        || config.max_operations == 0
        || config.max_candidates == 0
        || config.max_record_bytes == 0
        || config.max_record_bytes > 1024 * 1024
    {
        return Err(RecordError::Rejected("invalid record-engine limits".into()));
    }
    if config.schema.iter().any(|f| temporal::reserved(&f.field)) { return Err(RecordError::Rejected("reserved system time field prefix".into())); }
    crate::vector::validate_vector_schema(&config.index.inference, &config.schema)
        .map_err(RecordError::Rejected)?;
    if config.schema.iter().any(|f| f.index_vector)
        && config
            .embedding_space_id
            .as_ref()
            .is_none_or(|s| s.is_empty())
    {
        return Err(RecordError::Rejected("embedding_space_id required".into()));
    }
    let bytes = encode_entry(&Entry::Checkpoint {
        config: config.clone(),
        state: State::default(),
    })?;
    PartitionJournal::validate_initial(bytes.len(), config.max_journal_bytes)
        .map_err(|e| RecordError::Rejected(e.to_string()))?;
    Ok(())
}
/// Client operation identity includes its UTC day epoch. Current and previous
/// day are accepted, giving at least 24 hours for retries; older epochs explicitly
/// expire. Reusing a nonce in another epoch is a *different operation*.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct OperationId {
    /// UTC day number since Unix epoch.
    pub epoch: u64,
    /// Caller-generated unique nonce, scoped to this collection and epoch.
    pub nonce: String,
}
impl OperationId {
    /// Create a retry identity. Persist and reuse it unchanged for retries.
    pub fn new(nonce: String) -> Self {
        Self {
            epoch: current_epoch(),
            nonce,
        }
    }
    /// Retry admission ends at the start of epoch + 2 (UTC seconds). An
    /// operation issued in its own UTC day therefore has at least 24 hours.
    /// A clock rollback never extends a persisted server epoch watermark.
    pub fn expires_at_unix_seconds(&self) -> u64 {
        self.epoch.saturating_add(2).saturating_mul(86400)
    }
}
fn current_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 86400
}
/// Write precondition; absence is distinct from unconditional replacement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExpectedVersion {
    /// No live record may exist. A tombstone's version is still advanced.
    Absent,
    /// Match the current live or tombstone version exactly.
    Match(u64),
    /// Explicit last-writer-wins behavior in the partition's serial order.
    Any,
}
/// Full replacement or ordered deletion.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordWrite {
    /// Retry identity.
    pub operation_id: OperationId,
    /// Collection-unique opaque business identity; never a path.
    pub record_id: String,
    /// Immutable colocation key, retained through delete/recreate.
    pub routing_key: String,
    /// Conditional write contract.
    pub expected: ExpectedVersion,
    /// Complete original input including vectors; None deletes unless vector_patch is set.
    pub document: Option<Document>,
    /// Fill previously absent schema vector fields for an exact body version.
    /// Mutually exclusive with document/system; preserves body version.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_patch: Option<Document>,
    /// Durable staging / atomic publication command on a document control ID.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<GenerationCommand>,
    /// UTC millisecond metadata for a complete body revision; omitted inherits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal: Option<TemporalMetadata>,
    /// Optional engine-owned source/evidence semantics; business document fields
    /// with the same name have no effect on visibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<EvidenceMetadata>,
}
/// Authoritative record. Deleted identities retain version and routing key.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    /// Stable identity.
    pub record_id: String,
    /// Immutable colocation key.
    pub routing_key: String,
    /// Monotonic revision including tombstones.
    pub version: u64,
    /// Original unnormalized input, absent for a tombstone.
    pub document: Option<Document>,
    /// Engine-owned retirement when a newer document generation is published.
    pub retired_generation: bool,
    /// Published generation provenance, retained even after retirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation_member: Option<GenerationMember>,
    /// Business time and irreversible TTL deadline for this body revision.
    pub temporal: TemporalMetadata,
    /// Trusted body ingestion time in UTC Unix milliseconds.
    pub ingested_at_ms: u64,
    /// Durable terminal expiry marker; this identity can never be revived.
    pub expired_at_ms: Option<u64>,
    /// Optional engine-owned source/evidence semantics; business document fields
    /// with the same name have no effect on visibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<EvidenceMetadata>,
}
/// A confirmed durable operation. Staging explicitly does not publish content.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteReceipt {
    /// True only for durable generation staging; members are not searchable.
    pub staged_only: bool,
    /// Logical partition operation position, independent of physical WAL frames.
    pub seq: u64,
    /// Record revision after this operation.
    pub version: u64,
    /// Business identity.
    pub record_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Applied {
    request_digest: Option<String>,
    graph_invalidation: bool,
    request: RecordWrite,
    receipt: WriteReceipt,
    observed_epoch: u64,
    observed_time_ms: u64,
}
#[derive(Debug, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(Clone))]
struct State {
    #[serde(skip)]
    residency: Option<Arc<state_memory::Charge>>,
    lineage: String,
    feed: change_feed::FeedState,
    sealed_for: Option<SealCertificate>,
    takeover_proof: Option<SealCertificate>,
    independent_snapshot: bool,
    seq: u64,
    epoch_watermark: u64,
    time_watermark_ms: u64,
    #[serde(default,skip_serializing_if="Option::is_none")]
    retired_operation_prefix: Option<temporal::RetiredPrefix>,
    expirations: Vec<temporal::Expiration>,
    lease_expirations: Vec<lease::Expiration>,
    private_cleanup_pending: bool,
    // Independent snapshots cannot prove the absence of later source deletes.
    // This persistent restore gate is deliberately not cleared by ordinary writes.
    evidence_quarantined: bool,
    records: state_values::Records,
    source_revisions: state_values::Values<Record>,
    generations: BTreeMap<String, generation::Manifest>,
    // Vec avoids JSON's restriction on structured map keys. Bounded by config.
    operations: state_values::Values<Applied>,
    #[serde(skip)]
    operation_lookup: BTreeMap<OperationId, usize>,
    #[serde(skip)]
    lookup_bytes: usize,
}
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Clone))]
enum Entry {
    #[serde(rename = "RecordAnchorV1")]
    Anchor(anchor::Reference),
    #[serde(rename = "RecordCheckpointV14")]
    Checkpoint { config: RecordConfig, state: State },
    #[serde(rename = "RecordAppliedV12")]
    Applied(Applied),
    #[serde(rename = "RecordExpiredV12")]
    Expired(temporal::Expiration),
    #[serde(rename = "RecordFeedTrimV12")]
    FeedTrim(change_feed::Trim),
    #[serde(rename = "RecordLeaseExpiredV12")]
    LeaseExpired(lease::Expiration),
}
struct Inner {
    background: Option<indexer::Tail>,
    maintenance_requested:Option<&'static str>,
    maintenance_scheduled:bool,
    maintenance_compacted_bytes:Option<u64>,
    maintenance_trim:Option<change_feed::Trim>,
    maintenance_gc_pending:bool,
    anchor:Option<anchor::Reference>,
    payloads:crate::storage::payload_pool::Closure,
    config: RecordConfig,
    state: State,
    index: view::RetrievalView,
    ordinals: Vec<String>,
    locations: BTreeMap<String, usize>,
    snapshot_bytes: usize,
    healthy: bool,
    clock: Arc<dyn temporal::Clock>,
    evaluation_time_ms: u64,
    feed_cache: change_feed::Cache,
    graph_cache: graph::Cache,
    bootstrap: Option<change_feed::Bootstrap>,
    bootstrap_epoch: String,
    output: Arc<std::sync::RwLock<output::OutputState>>,
    #[cfg(feature = "s3-object-store")]
    remote: Option<RemotePersistence>,
    #[cfg(test)]
    fail_after_durable: bool,
    #[cfg(test)]
    fail_publication_stage: u8,
    // Fields drop in declaration order: release all index/mmap handles before
    // releasing the partition lock, so a concurrent opener can clean caches.
    journal: PartitionJournal,
}
// A permit may outlive the last partition handle. It must not survive owner
// release and permit old data to be delivered after a new owner deletes it.
impl Drop for Inner {
    fn drop(&mut self) {
        if let Ok(mut state) = self.output.write() { state.fenced = true; }
    }
}
#[cfg(feature = "s3-object-store")]
struct RemotePersistence {
    store: crate::storage::s3_compatible::S3CompatibleStore,
    authority: tokio::sync::Mutex<crate::storage::sqlite_authority::SqliteAuthority>,
    partition_id: String,
    owner_epoch: u64,
    max_tail_blocks: usize,
    max_tail_bytes: usize,
    max_object_bytes: usize,
    _owner: Option<std::fs::File>,
}

/// Configuration for the first S3-compatible durable Record backend.
/// Credentials are runtime-only and are never written into SQLite or backup metadata.
/// Reads use a discardable verified object cache beside `authority_path`, bounded
/// to 256 MiB / 4096 blocks per authority. Cache failure falls back to the backend.
#[cfg(feature = "s3-object-store")]
pub struct S3CompatibleRecordConfig {
    pub authority_path: PathBuf,
    pub partition_id: String,
    pub owner_epoch: u64,
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub allow_http: bool,
    pub max_object_bytes: usize,
    pub max_tail_blocks: usize,
    pub max_tail_bytes: usize,
}

#[cfg(feature = "s3-object-store")]
#[derive(Debug)]
enum RemoteCommitError { Busy, Unknown(String) }
#[cfg(feature = "s3-object-store")]
impl From<String> for RemoteCommitError { fn from(error: String) -> Self { Self::Unknown(error) } }
#[cfg(feature = "s3-object-store")]
impl From<&str> for RemoteCommitError { fn from(error: &str) -> Self { Self::Unknown(error.into()) } }
#[cfg(feature = "s3-object-store")]
impl std::fmt::Display for RemoteCommitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self { Self::Busy => f.write_str("resource budget exhausted before commit publication"), Self::Unknown(error) => f.write_str(error) }
    }
}
#[cfg(feature = "s3-object-store")]
impl RemotePersistence {
    fn open(config: &S3CompatibleRecordConfig) -> Result<Self, RecordError> {
        if config.partition_id.is_empty()
            || !config.partition_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || config.max_object_bytes == 0 || config.max_tail_blocks == 0 || config.max_tail_bytes == 0 {
            return Err(RecordError::Rejected("invalid S3 persistence configuration".into()));
        }
        let owner=std::fs::OpenOptions::new().create(true).truncate(false).read(true).write(true)
            .open(config.authority_path.with_extension("sqlite.owner")).map_err(storage)?;
        owner.try_lock().map_err(storage)?;
        let store = crate::storage::s3_compatible::S3CompatibleStore::connect(
            &config.endpoint, &config.bucket, &config.region, &config.access_key_id,
            &config.secret_access_key, config.allow_http, config.max_object_bytes,
        ).map_err(|error| storage(error))?.with_cache(
            crate::storage::verified_cache::VerifiedCache::new(
                config.authority_path.with_extension("object-cache"),
                &[&config.endpoint, &config.bucket, &config.region],
                crate::storage::verified_cache::DEFAULT_BYTES,
            ),
        );
        let authority = crate::storage::sqlite_authority::SqliteAuthority::open(
            &config.authority_path, std::time::Duration::from_secs(5),
        ).map_err(storage)?;
        Ok(Self {
            store, authority: tokio::sync::Mutex::new(authority), partition_id: config.partition_id.clone(),
            owner_epoch: config.owner_epoch, max_tail_blocks: config.max_tail_blocks,
            max_tail_bytes: config.max_tail_bytes, max_object_bytes: config.max_object_bytes,
            _owner:Some(owner),
        })
    }

    async fn commit(&mut self, applied: &Applied, payload: &[u8]) -> Result<(), RemoteCommitError> {
        let operation_id = format!("{}:{}", applied.request.operation_id.epoch, applied.request.operation_id.nonce);
        self.commit_entry(applied.receipt.seq, operation_id, payload).await
    }

    async fn commit_entry(&mut self, seq: u64, operation_id: String, payload: &[u8]) -> Result<(), RemoteCommitError> {
        use sha2::{Digest, Sha256};
        let mut authority = self.authority.lock().await;
        let head = authority.read_head(&self.partition_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "remote partition authority is missing".to_owned())?;
        if seq != head.committed_seq.saturating_add(1) {
            return Err(format!("remote commit sequence conflict: local={seq}, remote={}", head.committed_seq).into());
        }
        if head.owner_epoch != self.owner_epoch {
            return Err("remote owner epoch changed; this writer is fenced".into());
        }
        let digest: [u8; 32] = Sha256::digest(payload).into();
        let block = crate::storage::object_log::ObjectLogBlock {
            partition_id: self.partition_id.clone(), first_seq: seq, last_seq: seq,
            previous: head.wal_tip.clone(),
            entries: vec![crate::storage::object_log::ObjectLogEntry {
                seq, operation_id: operation_id.clone(), request_sha256: digest, payload: payload.to_vec(),
            }],
        };
        let key = format!("partitions/{}/wal/{seq:020}-{}", self.partition_id,
            digest.iter().map(|byte| format!("{byte:02x}")).collect::<String>());
        let reference = crate::storage::object_log::upload_log_block(
            &self.store, &mut authority, &key, &block,
            unix_time_ms_i64(), self.max_object_bytes,
        ).await.map_err(|error| match error {
            crate::storage::object_log::TailError::ObjectStore(ref object) if crate::storage::s3_compatible::is_resource_error(object) => RemoteCommitError::Busy,
            error => RemoteCommitError::Unknown(error.to_string()),
        })?;
        match authority.publish_commit(
            &self.partition_id, head.head_version, self.owner_epoch, &operation_id,
            digest, seq, reference, None,
        ).map_err(|error| error.to_string())? {
            crate::storage::sqlite_authority::PublishOutcome::Published(published)
            | crate::storage::sqlite_authority::PublishOutcome::AlreadyPublished(published)
                if published.committed_seq == seq => Ok(()),
            crate::storage::sqlite_authority::PublishOutcome::Conflict =>
                Err("remote SQLite commit conflicted; partition requires recovery".into()),
            _ => Err("remote operation receipt does not match committed sequence".into()),
        }
    }

    /// Capture or resume a metadata/object backup at its original committed cut.
    async fn create_backup_bundle(
        &mut self,
        backup_id: &str,
        created_at_ms: i64,
        max_snapshot_bytes: usize,
    ) -> Result<(String,u64), String> {
        let mut authority = self.authority.lock().await;
        let closure=match authority.saved_backup_closure(backup_id).map_err(|e|e.to_string())? {
            Some(closure)=>closure,
            None=>authority.reachable_objects().map_err(|e|e.to_string())?,
        };
        let bundle = self.store.create_backup_bundle(
            &mut authority, backup_id, &closure, created_at_ms, max_snapshot_bytes,
        ).await.map_err(|error| error.to_string())?;
        let bytes=self.store.get_verified(&bundle.descriptor.metadata_snapshot).await.map_err(|e|e.to_string())?;
        let snapshot:crate::storage::sqlite_authority::AuthorityBackupSnapshot=serde_json::from_slice(&bytes).map_err(|e|e.to_string())?;
        let seq=snapshot.heads.iter().find(|h|h.partition_id==self.partition_id)
            .ok_or_else(||"backup omits partition".to_owned())?.committed_seq;
        Ok((serde_json::to_string(&bundle).map_err(|e|e.to_string())?,seq))
    }

    fn safe_backup_id(backup_id: &str) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(backup_id.as_bytes()).iter()
            .map(|byte| format!("{byte:02x}")).collect()
    }

    async fn reconcile_journal(&mut self, inner: &mut Inner) -> Result<(), RecordError> {
        let head = self.authority.lock().await.read_head(&self.partition_id).map_err(storage)?
            .ok_or_else(|| storage("remote partition authority is missing"))?;
        if head.owner_epoch != self.owner_epoch
            || inner.state.seq > head.committed_seq
            || inner.state.seq < head.log_base_seq {
            return Err(RecordError::RecoveryRequired);
        }
        let bootstrap_ref = head.bootstrap.as_ref().ok_or(RecordError::RecoveryRequired)?;
        let bootstrap_bytes = self.store.get_verified(bootstrap_ref).await.map_err(storage)?;
        let (bootstrap_config, bootstrap_state) = match serde_json::from_slice::<Entry>(&bootstrap_bytes).map_err(storage)? {
            Entry::Checkpoint { config, state } => (config, state),
            _ => return Err(storage("remote bootstrap is not a Record checkpoint")),
        };
        if serde_json::to_vec(&bootstrap_config).map_err(storage)?
                != serde_json::to_vec(&inner.config).map_err(storage)?
            || bootstrap_state.lineage != inner.state.lineage
            || bootstrap_state.feed.scope != inner.state.feed.scope
            || bootstrap_state.seq != 0 {
            return Err(storage("local partition does not match the authoritative remote bootstrap"));
        }
        let local_seq = inner.state.seq;
        let tail = crate::storage::object_log::load_committed_entries_after(
            &self.store, &head, local_seq, self.max_tail_blocks, self.max_tail_bytes,
        ).await.map_err(storage)?;
        let mut expected = local_seq;
        for entry in tail {
            if entry.seq != expected.saturating_add(1) {
                return Err(RecordError::RecoveryRequired);
            }
            use sha2::{Digest, Sha256};
            let digest: [u8; 32] = Sha256::digest(&entry.payload).into();
            if digest != entry.request_sha256 { return Err(RecordError::RecoveryRequired); }
            let parsed: Entry = serde_json::from_slice(&entry.payload).map_err(storage)?;
            let (seq, operation_id) = match &parsed {
                Entry::Applied(applied) => (
                    applied.receipt.seq,
                    format!("{}:{}", applied.request.operation_id.epoch, applied.request.operation_id.nonce),
                ),
                Entry::Expired(event) => (event.seq, format!("system:expired:{}", event.seq)),
                Entry::LeaseExpired(event) => (event.seq, format!("system:lease-expired:{}", event.seq)),
                Entry::Checkpoint { .. } | Entry::Anchor(_) | Entry::FeedTrim(_) =>
                    return Err(RecordError::RecoveryRequired),
            };
            if seq != entry.seq || operation_id != entry.operation_id {
                return Err(RecordError::RecoveryRequired);
            }
            inner.journal.validate_append_with_reserve(entry.payload.len(), 0)
                .map_err(|_| RecordError::RecoveryRequired)?;
            inner.journal.append(&entry.payload).map_err(storage)?;
            expected = seq;
        }
        if expected != head.committed_seq { return Err(RecordError::RecoveryRequired); }
        Ok(())
    }
}

#[cfg(feature = "s3-object-store")]
fn unix_time_ms_i64() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis().min(i64::MAX as u128) as i64).unwrap_or(0)
}

/// Single-partition Record decisions are serialized. Index consumption and
/// retrieval run independently; results briefly reenter the Record guard for validation.
#[derive(Clone)]
pub struct RecordPartition {
    inner: Arc<Mutex<Inner>>,
    admission: Arc<Semaphore>,
    output: Arc<std::sync::RwLock<output::OutputState>>,
    indexes: Arc<index_consumers::Cursors>,
}
/// Stateless continuation at one logical position. A changed position conflicts;
/// a cursor never silently combines pages from different views.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordCursor {
    /// Position captured on the first page.
    pub visible_seq: u64,
    /// Exclusive stable identity boundary.
    pub after: String,
}
/// One bounded page of active records in stable identity order.
#[derive(Clone, Debug, Serialize)]
pub struct RecordPage {
    /// Fixed time shared with the records and statistics.
    pub evaluation_time_ms: u64,
    /// Position shared by records and statistics.
    pub visible_seq: u64,
    /// Exact original active records.
    pub records: Vec<Record>,
    /// Continuation, absent at end of view.
    pub next: Option<RecordCursor>,
    /// Active statistics captured under the same guard.
    pub statistics: RecordStatistics,
}
/// A retrieved candidate reconciled with the current visible canonical record.
#[derive(Clone, Debug, Serialize)]
pub struct RecordHit {
    /// Per-channel observed ranking evidence, with unmodified raw scores.
    pub channels: Vec<crate::search::ChannelExplanation>,
    /// The exact original record corresponding to the ranked candidate.
    pub record: Record,
    /// Existing engine score (BM25, vector, or RRF according to query mode).
    pub score: f32,
}
/// Retrieval options for independently published caches and current Record validation.
pub struct RecordQuery {
    /// Optional system temporal predicates, applied before candidate selection.
    pub temporal: Option<TemporalFilter>,
    pub text: String,
    pub vector: Option<Embedding>,
    pub mode: SearchMode,
    pub query_type: QueryType,
    pub offset: usize,
    pub length: usize,
    pub result_type: ResultType,
    pub fields: Vec<String>,
    pub filters: Vec<FacetFilter>,
    pub sort: Vec<ResultSort>,
}
/// Search response; hybrid/ANN counts are deliberately not called exact totals.
#[derive(Clone, Debug, Serialize)]
pub struct RecordSearch {
    /// Present only for explicit relation fusion; counts describe candidate windows.
    #[serde(skip_serializing_if="Option::is_none")]
    pub fusion: Option<FusionWindow>,
    /// Fixed trusted UTC milliseconds used to decide TTL visibility.
    pub evaluation_time_ms: u64,
    /// Mode-specific reported total, not an unconditional exact corpus count.
    pub count_total: usize,
    /// Analyzed query terms from this same search.
    pub query_terms: Vec<String>,
    /// Authoritative record position; asynchronous index consumers may lag.
    pub visible_seq: u64,
    /// Ranked effective records.
    pub hits: Vec<RecordHit>,
    /// Active and retained resource counts from this same read view.
    pub statistics: RecordStatistics,
}

/// Content-free status from the same view as version and visibility. Hidden
/// historical bodies are not made available by this inspection API.
#[derive(Clone, Debug, Serialize)]
pub struct RecordStatus {
    /// Fixed time shared with the observed version and visibility.
    pub evaluation_time_ms: u64,
    /// Stable current identity.
    pub record_id: String,
    /// Current authority version, including tombstones.
    pub version: u64,
    /// Why ordinary reads include or hide this identity.
    pub visibility: EvidenceVisibility,
    /// Schema vector fields not yet supplied on a live body.
    pub pending_vector_fields: Vec<String>,
    /// Common authoritative view waterline.
    pub visible_seq: u64,
}

/// Common-view counts; retained physical versions are not live record counts.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct RecordStatistics {
    /// Fixed trusted UTC millisecond evaluation of this common view.
    pub evaluation_time_ms: u64,
    /// Authoritative position published to every retrieval component.
    pub visible_seq: u64,
    /// Non-tombstoned records contributing to scoring statistics.
    pub active_records: usize,
    /// Stable identities including tombstones, generation controls and staged reservations.
    pub retained_identities: usize,
    /// Durable generation members not yet published into retrieval.
    pub staged_records: usize,
    /// Retry results currently retained by the operation ledger.
    pub retained_operations: usize,
    /// Immutable prior source bodies retained independently of retry expiry.
    pub retained_source_revisions: usize,
    /// Physical indexed versions, including candidates filtered as retired.
    pub physical_versions: usize,
    /// Source/evidence data in an independent snapshot is withheld until a
    /// trusted deletion-history reconciliation protocol is available.
    pub evidence_quarantined: bool,
    /// Exact size of the complete authoritative checkpoint payload.
    pub checkpoint_bytes: usize,
}
impl Inner {
    // In S3 mode the caller has already received SQLite commit confirmation.
    // A cache append error cannot turn that known durable operation into an
    // unknown remote commit. The caller keeps health/output fenced until reopen.
    fn append_operation(&mut self, bytes: &[u8], seq: u64) -> Result<(), RecordError> {
        let result = (|| {
            #[cfg(test)]
            if self.journal.root().join(".test-cache-append-failure").exists() {
                return Err(std::io::Error::other("injected cache append failure"));
            }
            #[cfg(feature="fault-injection")]
            if std::env::var("ANYSEEK_CACHE_APPEND_FAULT").as_deref()==Ok("1") {
                return Err(std::io::Error::other("injected cache append failure"));
            }
            self.journal.append(bytes)
        })();
        if let Err(error) = result {
            #[cfg(feature="s3-object-store")]
            if self.remote.is_some() { return Err(RecordError::DurablePending(seq)); }
            let _ = seq;
            return Err(RecordError::OutcomeUnknown(error.to_string()));
        }
        if let Some(tail) = self.background.as_mut() {
            if let Some(revision)=tail.revision.checked_add(1) {tail.revision=revision;}
            else {tail.overflowed=true;}
            // Capture has its own pre-reserved bound. Exceeding it abandons this
            // build, never reclassifies an acknowledged foreground write or lets
            // the worker publish an incomplete suffix.
            if !tail.overflowed {
                if let Some(total)=tail.bytes.checked_add(bytes.len() as u64)
                    .and_then(|n|n.checked_add((2*std::mem::size_of::<Vec<u8>>()) as u64))
                    .filter(|n|*n<=tail.max_bytes) {
                    tail.entries.push(bytes.to_vec());tail.bytes=total;
                } else {tail.overflowed=true;tail.entries.clear();}
            }
        }
        Ok(())
    }
    fn statistics(&self) -> RecordStatistics {
        RecordStatistics {
            evaluation_time_ms: self.evaluation_time_ms,            visible_seq: self.state.seq,
            active_records: self.locations.len(),
            retained_identities: self.state.records.len() + self.state.generations.len() + generation::reserved(&self.state),
            staged_records: generation::reserved(&self.state),
            retained_operations: self.state.operations.len(),
            retained_source_revisions: self.state.source_revisions.len(),
            physical_versions: self.ordinals.iter().filter(|id|!id.is_empty()).count(),
            evidence_quarantined: self.state.evidence_quarantined,
            checkpoint_bytes: self.snapshot_bytes,
        }
    }
}

fn creation_error(error: CreateError) -> RecordError {
    match error {
        CreateError::NotPublished(error) => RecordError::Rejected(error.to_string()),
        CreateError::PublicationUnknown(error) => RecordError::OutcomeUnknown(error.to_string()),
    }
}

// Match the recovery decoder before admitting authoritative bytes. This also
// protects future envelope/schema changes from encoding/decoding asymmetry.
fn encode_entry(entry: &Entry) -> Result<Vec<u8>, RecordError> {
    let bytes = serde_json::to_vec(entry).map_err(storage)?;
    serde_json::from_slice::<Entry>(&bytes)
        .map_err(|e| RecordError::Rejected(format!("unrecoverable entry: {e}")))?;
    Ok(bytes)
}

fn validate_document_depth(document: &Document) -> Result<(), RecordError> {
    fn visit(value: &serde_json::Value, depth: usize) -> Result<(), RecordError> {
        if depth > 64 {
            return Err(RecordError::Rejected("document nesting exceeds 64".into()));
        }
        match value {
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1)?;
                }
            }
            serde_json::Value::Object(values) => {
                for value in values.values() {
                    visit(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    for value in document.values() {
        visit(value, 1)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn query_test_gate() {
    if let Some(path) = std::env::var_os("ANYSEEK_RECORD_QUERY_GATE") {
        let path = PathBuf::from(path);
        std::fs::write(path.join("query-entered"), b"ready").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !path.join("query-release").exists() {
            assert!(std::time::Instant::now() < deadline, "query gate timed out");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
}

// Only call while holding the exclusive journal lock, with no surviving index
// tasks from the preceding owner. Unknown files and symlinks are never followed.
fn clean_recovery_caches(path: &Path) -> Result<(), RecordError> {
    for entry in std::fs::read_dir(path).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(suffix) = name.strip_prefix("cache-") else {
            continue;
        };
        if suffix.len() == 32 && suffix.bytes().all(|c| c.is_ascii_hexdigit()) {
            if !entry.file_type().map_err(storage)?.is_dir() {
                return Err(storage("derived cache path is not a directory"));
            }
            package_pins::retire_if_unpinned(&entry.path(),||std::fs::remove_dir_all(entry.path()).map_err(storage))?;
        }
    }
    Ok(())
}

// Search files contain candidates and scoring data, never a second copy of
// authoritative Record JSON. Public schema.store still controls HTTP projection.
fn retrieval_schema(config: &RecordConfig) -> Vec<SchemaField> {
    let mut schema = temporal::schema(config);
    for field in &mut schema { field.store = false; field.index_lexical = false; }
    schema
}

async fn build(
    path: &Path,
    config: &RecordConfig,
    state: &State,
) -> Result<(view::RetrievalView, Vec<String>), RecordError> {
    let cache = path.join(format!("cache-{:032x}", rand::random::<u128>()));
    let (index,ordinals)=build_at(path,&cache,config,state).await?;
    Ok((view::RetrievalView::single(index),ordinals))
}
async fn build_at(path:&Path,cache:&Path,config:&RecordConfig,state:&State)->Result<(IndexArc,Vec<String>),RecordError>{
    build_at_selected(path,cache,config,state,None).await
}
async fn build_at_selected(path:&Path,cache:&Path,config:&RecordConfig,state:&State,selected:Option<&std::collections::BTreeSet<String>>)->Result<(IndexArc,Vec<String>),RecordError>{
    let visible = evidence::visibility(&state.records)?;
    let records: Vec<_> = state.records.values().filter(|record|
        visible.get(&record.record_id) == Some(&true)
        && selected.is_none_or(|keys|keys.contains(&record.record_id))
        && !(state.evidence_quarantined && temporal::protected(record))).collect();
    build_records_at(path,cache,config,state.seq,records).await
}
async fn build_records_at(path:&Path,cache:&Path,config:&RecordConfig,sequence:u64,records:Vec<&Record>)->Result<(IndexArc,Vec<String>),RecordError>{
    let disk_limit=config.max_journal_bytes.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024)).ok_or_else(||storage("build disk limit overflow"))?;
    let _disk=crate::storage::build_budget::ensure_scope(path,disk_limit).map_err(storage)?;
    let reservation=build_budget::admit(config,records.iter().copied())?;
    let index = create_index(
        &cache,
        config.index.clone(),
        &retrieval_schema(config),
        Some(1),
    )
    .await
    .map_err(storage)?;
    // Break legacy ownership cycles so a rejected candidate can release mapped
    // files on Windows. The partition owns the root for the complete query.
    index.read().await.shard_vec[0].write().await.build_reservation=Some(reservation);
    index.read().await.shard_vec[0].write().await.build_input_covered=true;
    for shard in &index.read().await.shard_vec {
        let mut shard=shard.write().await;
        shard.active=Some(crate::active::ActiveStatistics::default());
        // Record segments must not choose a quantizer from the first arriving
        // vector. Pin the non-affine scalar layout across tail/flush/recovery.
        if shard.quantization==crate::vector::Quantization::ScalarQuantizationI8
            && shard.vector_similarity==crate::vector_similarity::VectorSimilarity::Euclidean {
            shard.min_vector_value=0.0;shard.max_vector_value=f32::MIN;
        }
    }
    #[cfg(test)]
    if std::env::var_os("ANYSEEK_RECORD_BUILD_GATE").as_deref() == Some(path.as_os_str()) {
        std::fs::write(path.join("build-entered"), b"ready").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !path.join("build-release").exists() {
            assert!(std::time::Instant::now() < deadline, "build gate timed out");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    let native = lexical::runtime::Runtime::create(&cache.join("native"), config)?;
    for record in &records {
        native.append(lexical::Identity { id:record.record_id.clone(),version:record.version,revision:sequence },record.document.as_ref().unwrap(),false).await?;
        index.read().await.shard_vec[0]
            .write()
            .await
            .active
            .as_mut()
            .unwrap()
            .append_identity(record.record_id.clone(), record.version, sequence,
                crate::active::fingerprint(&temporal::indexed(record.document.as_ref().unwrap(), &record.temporal, record.ingested_at_ms)).map_err(storage)?);
        index
            .index_documents(vec![temporal::indexed(record.document.as_ref().unwrap(), &record.temporal, record.ingested_at_ms)])
            .await
            .map_err(storage)?;
    }
    native.flush().await?;
    index.write().await.record_lexical.push(native);
    index.commit().await.map_err(storage)?;
    index.read().await.shard_vec[0].write().await.build_input_covered=false;
    Ok((
        index,
        records.into_iter().map(|r| r.record_id.clone()).collect(),
    ))
}
fn validate_restored_state(config:&RecordConfig,state:&State)->Result<(),RecordError>{
        state.records.validate_counts()?;
    validate_config(config)?;
        evidence::validate_history(&state.records, &state.source_revisions).map_err(storage)?;
        locator::validate_history(&state).map_err(storage)?;
        generation::validate_state(&state, &config)?;
        lease::validate_state(&state)?;
        temporal::validate_state(&state)?;
        if state.records.len() > config.max_records
            || state.source_revisions.len() + state.records.values().filter(|r| evidence::retained_revision(r)).count() > config.max_records
            || state.operations.len() > config.max_operations
        {
            return Err(storage("recovered state exceeds configured limits"));
        }
    Ok(())
}
fn restore_operation_lookup(state:&mut State)->Result<(),RecordError>{
    let bytes=state.operations.iter().try_fold(0usize,|n,operation| {
        n.checked_add(checkpoint_codec::json_length(&operation.request.operation_id)?).ok_or_else(||storage("lookup byte overflow"))
    })?;
    state.admit_lookup(bytes)?;
    state.operation_lookup.clear();
        for (position, operation) in state.operations.iter().enumerate() {
            if state
                .operation_lookup
                .insert(operation.request.operation_id.clone(), position)
                .is_some()
            {
                return Err(storage("duplicate checkpoint operation identity"));
            }
        }
    Ok(())
}
fn apply(state: &mut State, applied: &Applied) -> Result<(), RecordError> {
    let prepared=if applied.request.generation.is_none(){Some(state_memory::Prepared::new(state,applied)?)}else{None};
    apply_prepared(state,applied,prepared)
}
fn apply_prepared(state: &mut State, applied: &Applied, prepared: Option<state_memory::Prepared>) -> Result<(), RecordError> {
    if applied.receipt.seq != state.seq + 1 || applied.observed_time_ms < state.time_watermark_ms {
        return Err(storage("non-contiguous record sequence"));
    }
    let r = &applied.request;
    if applied.receipt.record_id != r.record_id || state.operation_lookup.contains_key(&r.operation_id) {
        return Err(storage("inconsistent committed operation identity"));
    }
    if r.generation.is_some() {
        generation::apply(state, applied)?;
    } else {
    let previous = state.records.get(&r.record_id);
    if applied.receipt.version != previous.map_or(1, |r| if applied.request.vector_patch.is_some() { r.version } else { r.version.saturating_add(1) })
        || applied.receipt.record_id != r.record_id
        || previous.is_some_and(|p| p.routing_key != r.routing_key)
        || state.operation_lookup.contains_key(&r.operation_id)
    {
        return Err(storage("inconsistent committed record operation"));
    }
    if previous.is_some_and(|record| evidence::retained_revision(record) && applied.request.vector_patch.is_none()) {
        state.source_revisions.push_shared(state.records.shared(&r.record_id).ok_or_else(||storage("missing source owner"))?)?;
    }
    let prepared=prepared.ok_or_else(||storage("missing prepared value ownership"))?;
    state.records.insert_shared(r.record_id.clone(),prepared.record)?;
    state.operations.push_shared(prepared.operation)?;
    }
    state.seq = applied.receipt.seq;
    state.epoch_watermark = state.epoch_watermark.max(applied.observed_epoch);
    state.time_watermark_ms = state.time_watermark_ms.max(applied.observed_time_ms);
    state
        .operation_lookup
        .insert(r.operation_id.clone(), if r.generation.is_none(){state.operations.len()-1}else{state.operations.len()});
    state.lookup_bytes=state.lookup_bytes.checked_add(checkpoint_codec::json_length(&r.operation_id)?).ok_or_else(||storage("lookup byte overflow"))?;
    if r.generation.is_some() {state.operations.push(applied.clone())?;}
    Ok(())
}
impl RecordPartition {
    /// Capture before reading data, then use with the authorization permit at
    /// output. A concurrent deletion cancels this response rather than mixing views.
    pub fn output_permit(&self) -> Result<DataOutputPermit, RecordError> {
        let state = self.output.read().map_err(|_| RecordError::RecoveryRequired)?;
        if state.fenced { return Err(RecordError::RecoveryRequired); }
        Ok(DataOutputPermit::new(self.output.clone(), state.revision))
    }
    /// Create a Record partition whose accepted writes are committed to S3 + SQLite
    /// before local journal/index publication. The local journal remains a rebuildable
    /// cache; use open_s3 after restart to reconcile any remotely committed tail.
    #[cfg(feature = "s3-object-store")]
    pub async fn create_s3(
        path: &Path,
        config: RecordConfig,
        remote_config: S3CompatibleRecordConfig,
    ) -> Result<Self, RecordError> {
        let remote = RemotePersistence::open(&remote_config)?;
        let existing = remote.authority.lock().await.read_head(&remote.partition_id).map_err(storage)?;
        match existing {
            Some(head) if head.owner_epoch != remote.owner_epoch => {
                Err(RecordError::RecoveryRequired)
            }
            Some(head) if head.bootstrap.is_some() => {
                drop(remote);
                Self::open_s3(path, config, remote_config).await
            }
            Some(head) if head.committed_seq == 0 => {
                // Recover an interrupted create: the authority row may exist even if
                // publishing its first bootstrap object previously failed.
                let partition = if path.exists() {
                    Self::open(path, config.max_journal_bytes).await?
                } else {
                    Self::create(path, config).await?
                };
                let (checkpoint, seq) = {
                    let inner = partition.inner.lock().await;
                    (encode_entry(&Entry::Checkpoint {
                        config: inner.config.clone(),
                        state: inner.state.try_clone()?,
                    })?, inner.state.seq)
                };
                if seq != 0 {
                    return Err(RecordError::RecoveryRequired);
                }
                {
                    let mut authority = remote.authority.lock().await;
                    remote.store.publish_bootstrap(
                        &mut authority, &remote.partition_id, remote.owner_epoch,
                        &checkpoint, unix_time_ms_i64(),
                    ).await.map_err(storage)?;
                }
                {
                    let mut inner = partition.inner.lock().await;
                    inner.remote = Some(remote);
                }
                Ok(partition)
            }
            Some(_) => Err(RecordError::RecoveryRequired),
            None => {
                // A new remote authority must be bootstrapped from a fresh local
                // partition. Never silently promote an existing local-only journal:
                // it may contain acknowledged data absent from the remote commit log.
                if path.exists() {
                    return Err(RecordError::Rejected(
                        "cannot attach an empty S3 authority to an existing local partition".into()
                    ));
                }
                remote.authority.lock().await
                    .create_partition(&remote.partition_id, remote.owner_epoch).map_err(storage)?;
                let partition = Self::create(path, config).await?;
                let checkpoint = {
                    let inner = partition.inner.lock().await;
                    encode_entry(&Entry::Checkpoint {
                        config: inner.config.clone(),
                        state: inner.state.try_clone()?,
                    })?
                };
                {
                    let mut authority = remote.authority.lock().await;
                    remote.store.publish_bootstrap(
                        &mut authority, &remote.partition_id, remote.owner_epoch,
                        &checkpoint, unix_time_ms_i64(),
                    ).await.map_err(storage)?;
                }
                {
                    let mut inner = partition.inner.lock().await;
                    inner.remote = Some(remote);
                }
                Ok(partition)
            }
        }
    }
    /// Open a local partition and reconcile every SQLite-committed S3 log entry
    /// before making its read view available. A missing/corrupt committed object
    /// fails closed if neither the backend nor an authenticated exact-reference
    /// cache copy supplies it. A cached journal never selects the current head.
    #[cfg(feature = "s3-object-store")]
    pub async fn open_s3(
        path: &Path,
        config: RecordConfig,
        remote_config: S3CompatibleRecordConfig,
    ) -> Result<Self, RecordError> {
        let remote = RemotePersistence::open(&remote_config)?;
        Self::open_s3_with_remote(path, config.max_journal_bytes, remote).await
    }

    /// Rebuild the local journal/index cache from the remote bootstrap and committed
    /// log when the local directory is missing, or reconcile its missing remote tail.
    /// Kept separate so crash recovery can be tested with an in-memory object store.
    #[cfg(feature = "s3-object-store")]
    async fn open_s3_with_remote(
        path: &Path,
        max_journal_bytes: u64,
        mut remote: RemotePersistence,
    ) -> Result<Self, RecordError> {
        {
            // Acquire the stable local owner lock without opening cached indexes.
            // Same-sequence maintenance may have replaced the remote manifest even
            // when the cached sequence is unchanged, so reinstall its certificate.
            let mut journal = if path.exists() {
                PartitionJournal::open(path,max_journal_bytes).map_err(storage)?.0
            } else {
                let head=remote.authority.lock().await.read_head(&remote.partition_id).map_err(storage)?
                    .ok_or(RecordError::RecoveryRequired)?;
                if head.owner_epoch!=remote.owner_epoch {return Err(RecordError::RecoveryRequired);}
                let initial=remote.store.get_verified(head.bootstrap.as_ref().ok_or(RecordError::RecoveryRequired)?).await.map_err(storage)?;
                PartitionJournal::validate_initial(initial.len(),max_journal_bytes)
                    .map_err(|e|RecordError::Rejected(e.to_string()))?;
                match serde_json::from_slice::<Entry>(&initial).map_err(storage)? {
                    Entry::Checkpoint {config,state} if config.max_journal_bytes==max_journal_bytes && state.seq==0 => validate_config(&config)?,
                    _=>return Err(RecordError::Rejected("remote bootstrap configuration or initial sequence is invalid".into())),
                }
                PartitionJournal::create(path,max_journal_bytes,&initial).map_err(creation_error)?
            };
            remote.authority.lock().await.cancel_owner_index_builds(&remote.partition_id,remote.owner_epoch).map_err(storage)?;
            if let Some(anchor)=remote.materialize_checkpoint(path,max_journal_bytes).await? {
                journal.checkpoint(&anchor).map_err(creation_error)?;
            }
        }
        let partition = Self::open(path,max_journal_bytes).await?;
        {
            let mut inner = partition.inner.lock().await;
            remote.reconcile_journal(&mut inner).await?;
            partition.indexes.immediate(inner.state.seq);
        }
        drop(partition);
        let partition = Self::open(path, max_journal_bytes).await?;
        {
            let mut inner = partition.inner.lock().await;
            let head = remote.authority.lock().await.read_head(&remote.partition_id).map_err(storage)?
                .ok_or_else(|| storage("remote partition authority is missing"))?;
            if inner.state.seq != head.committed_seq || head.owner_epoch != remote.owner_epoch {
                return Err(RecordError::RecoveryRequired);
            }
            inner.remote = Some(remote);
        }
        Ok(partition)
    }
    /// Create an immutable joint S3/SQLite backup. The returned JSON descriptor
    /// is a small local pointer; the metadata snapshot and all payload objects remain
    /// in object storage. The source authority pins the complete reachable closure.
    #[cfg(feature = "s3-object-store")]
    pub async fn create_s3_backup(
        &self,
        backup_id: &str,
        created_at_ms: i64,
        max_snapshot_bytes: usize,
    ) -> Result<(String, u64, String), RecordError> {
        let owner=self.inner.clone();let backup_id=backup_id.to_owned();
        crate::resources::spawn(async move {
        let mut inner = owner.lock().await;
        if inner.background.is_some() { return Err(RecordError::Busy); }
        if !inner.healthy || inner.state.sealed_for.is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        let remote = inner.remote.as_mut().ok_or_else(|| RecordError::Rejected(
            "partition is not using S3 storage".into()
        ))?;
        let partition_id = remote.partition_id.clone();
        let (bundle,visible_seq) = remote.create_backup_bundle(
            &RemotePersistence::safe_backup_id(&format!("{}:{backup_id}",remote.partition_id)), created_at_ms, max_snapshot_bytes,
        ).await.map_err(storage)?;
        Ok((bundle, visible_seq, partition_id))
        }).await.map_err(|e|RecordError::OutcomeUnknown(e.to_string()))?
    }

    /// Retry pending remote retirement/deletion under the single owner's guard.
    #[cfg(feature="s3-object-store")]
    pub async fn collect_s3_garbage(&self)->Result<usize,RecordError> {
        let owner=self.inner.clone();
        crate::resources::spawn(async move {
            let mut inner=owner.lock().await;
        if inner.background.is_some() { return Err(RecordError::Busy); }
            if !inner.healthy {return Err(RecordError::RecoveryRequired);}
            inner.remote.as_mut().ok_or_else(||RecordError::Rejected("not an S3 partition".into()))?
                .collect_objects().await
        }).await.map_err(|e|RecordError::OutcomeUnknown(e.to_string()))?
    }

    /// Release a retained joint backup's pins only after its descriptor has been retired.
    #[cfg(feature = "s3-object-store")]
    pub async fn release_s3_backup_pins(&self, backup_id: &str) -> Result<usize, RecordError> {
        let mut inner = self.inner.lock().await;
        let remote = inner.remote.as_mut().ok_or_else(|| RecordError::Rejected(
            "partition is not using S3 storage".into()
        ))?;
        let id=RemotePersistence::safe_backup_id(&format!("{}:{backup_id}",remote.partition_id));
        remote.authority.lock().await.release_backup_pins(&id).map_err(storage)
    }

    /// Release backup pins after the catalog has permanently retired the backup.
    #[cfg(feature = "s3-object-store")]
    pub async fn retire_s3_backup(
        authority_path: &Path,
        backup_id: &str,
    ) -> Result<usize, RecordError> {
        let mut authority = crate::storage::sqlite_authority::SqliteAuthority::open(
            authority_path, std::time::Duration::from_secs(5),
        ).map_err(storage)?;
        let Some(partition_id)=authority.single_partition_id().map_err(storage)? else{return Ok(0)};
        let id=RemotePersistence::safe_backup_id(&format!("{partition_id}:{backup_id}"));
        authority.release_backup_pins(&id).map_err(storage)
    }

    /// Delete the backup descriptor and metadata snapshot after all authority copies
    /// have released this backup's payload pins.
    #[cfg(feature = "s3-object-store")]
    pub async fn delete_s3_backup_metadata(
        authority_path: &Path,
        bundle_json: &str,
    ) -> Result<(), RecordError> {
        let bundle: crate::storage::s3_compatible::BackupBundle =
            serde_json::from_str(bundle_json).map_err(storage)?;
        let endpoint = std::env::var("ANYSEEK_S3_ENDPOINT").map_err(storage)?;
        let bucket = std::env::var("ANYSEEK_S3_BUCKET").map_err(storage)?;
        let access_key_id = std::env::var("ANYSEEK_S3_ACCESS_KEY_ID").map_err(storage)?;
        let secret_access_key = std::env::var("ANYSEEK_S3_SECRET_ACCESS_KEY").map_err(storage)?;
        let region = std::env::var("ANYSEEK_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
        let allow_http = std::env::var("ANYSEEK_S3_ALLOW_HTTP")
            .map(|value| value == "1" || value.eq_ignore_ascii_case("true")).unwrap_or(false);
        let config = S3CompatibleRecordConfig {
            authority_path: authority_path.to_path_buf(),
            partition_id: "backup-retirement".into(),
            owner_epoch: 1,
            endpoint,
            bucket,
            region,
            access_key_id,
            secret_access_key,
            allow_http,
            max_object_bytes: 1024 * 1024 * 1024,
            max_tail_blocks: 1,
            max_tail_bytes: 1,
        };
        let store=crate::storage::s3_compatible::S3CompatibleStore::connect(
            &config.endpoint,&config.bucket,&config.region,&config.access_key_id,
            &config.secret_access_key,config.allow_http,config.max_object_bytes).map_err(storage)?;
        let mut authority=crate::storage::sqlite_authority::SqliteAuthority::open(
            authority_path,std::time::Duration::from_secs(5)).map_err(storage)?;
        store.retire_backup_metadata(&mut authority,&bundle,unix_time_ms_i64()).await.map_err(storage)
    }

    /// Restore a joint S3/SQLite backup into an empty authority database, verify its
    /// complete object closure, then reconstruct the local journal/index cache.
    #[cfg(feature = "s3-object-store")]
    pub async fn restore_s3_backup(
        path: &Path,
        config: RecordConfig,
        remote_config: S3CompatibleRecordConfig,
        bundle_json: &str,
        max_snapshot_bytes: usize,
        max_objects: usize,
        max_total_object_bytes: usize,
    ) -> Result<Self, RecordError> {
        let remote = RemotePersistence::open(&remote_config)?;
        Self::restore_s3_with_remote(path,config,remote,bundle_json,
            max_snapshot_bytes,max_objects,max_total_object_bytes).await
    }

    /// Create a new authoritative partition. Parent directory must exist.
    pub async fn create(path: &Path, config: RecordConfig) -> Result<Self, RecordError> {
        let path = path.to_owned();
        crate::resources::spawn(async move { Self::create_owned(&path, config).await })
            .await
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?
    }

    async fn create_owned(path: &Path, mut config: RecordConfig) -> Result<Self, RecordError> {
        validate_config(&config)?;
        // Preserve deterministic legacy schema identity in persisted configuration;
        // this tag no longer chooses the native lexical encoding.
        if !config
            .schema
            .iter()
            .any(|field| field.index_lexical && field.longest)
        {
            if let Some(field) = config.schema.iter_mut().find(|field| field.index_lexical) {
                field.longest = true;
            }
        }
        let mut state = State { feed: change_feed::FeedState {scope:format!("{:032x}",rand::random::<u128>()),floor:0}, lineage: format!("{:032x}", rand::random::<u128>()), ..State::default() };
        state.admit_bytes(checkpoint_codec::json_length(&state)?)?;
        let entry = encode_entry(&Entry::Checkpoint {
            config: config.clone(),
            state: state.try_clone()?,
        })?;
        PartitionJournal::validate_initial(entry.len(), config.max_journal_bytes)
            .map_err(|e| RecordError::Rejected(e.to_string()))?;
        let journal = PartitionJournal::create(path, config.max_journal_bytes, &entry)
            .map_err(creation_error)?;
        let (index, ordinals) = build(path, &config, &state).await?;
        let output = Arc::new(std::sync::RwLock::new(output::OutputState::default()));
        Ok(Self {
            indexes: index_consumers::Cursors::recovered(0),
            output: output.clone(),
            inner: Arc::new(Mutex::new(Inner {
                background:None,maintenance_requested:None,maintenance_scheduled:false,maintenance_compacted_bytes:None,maintenance_trim:None,maintenance_gc_pending:false,anchor:None,payloads:Default::default(),
                journal,
                config,
                state,
                index,
                locations: ordinals
                    .iter()
                    .enumerate()
                    .map(|(id, key)| (key.clone(), id))
                    .collect(),
                ordinals,
                snapshot_bytes: entry.len(),
                healthy: true,
                clock: temporal::clock()?,
                evaluation_time_ms: 0,
                feed_cache: change_feed::Cache::default(),
                graph_cache: graph::Cache::default(),
                bootstrap: None, bootstrap_epoch:format!("{:016x}",rand::random::<u64>()),
                output,
                #[cfg(feature = "s3-object-store")]
                remote: None,
                #[cfg(test)]
                fail_after_durable: false,
                #[cfg(test)]
                fail_publication_stage: 0,
            })),
            admission: Arc::new(Semaphore::new(16)),
        })
    }
    /// Recover complete authoritative state before serving any query. The budget
    /// is supplied before reading files, not trusted from a potentially bad file.
    pub async fn open(path: &Path, max_journal_bytes: u64) -> Result<Self, RecordError> {
        let path = path.to_owned();
        // The owned task retains the journal lock until every indexing task has
        // drained, even if the caller cancels recovery.
        crate::resources::spawn(async move { Self::open_owned(&path, max_journal_bytes).await })
            .await
            .map_err(storage)?
    }

    async fn open_owned(path: &Path, max_journal_bytes: u64) -> Result<Self, RecordError> {
        let _frames = crate::storage::build_budget::reserve_memory(max_journal_bytes).map_err(storage)?;
        let (mut journal, entries) =
            PartitionJournal::open(path, max_journal_bytes).map_err(storage)?;
        clean_recovery_caches(path)?;
        let mut entries = entries.into_iter();
        let first = entries
            .next()
            .ok_or_else(|| storage("missing initial checkpoint"))?;
        let mut pending=Vec::new();
        let mut pending_decode=Vec::new();
        let first_decode = crate::storage::build_budget::reserve_memory((first.len() as u64).checked_mul(32).ok_or_else(||storage("initial frame estimate overflow"))?).map_err(storage)?;
        let (config,mut state,prepared,selected,payloads)=match serde_json::from_slice(&first).map_err(storage)? {
            Entry::Checkpoint{config,state}=>(config,state,None,None,Default::default()),
            Entry::Anchor(reference)=>{
                let loaded=reference.load(path,max_journal_bytes).await?;
                (loaded.config,loaded.state,Some((loaded.index,loaded.ordinals,loaded.locations)),Some(reference),loaded.payloads)
            },
            _=>return Err(storage("missing initial checkpoint")),
        };
        journal
            .restrict_budget(config.max_journal_bytes)
            .map_err(storage)?;
        state.admit_bytes(checkpoint_codec::json_length(&state)?)?;
        drop(first_decode);
        restore_operation_lookup(&mut state)?;
        state.admit_bytes(checkpoint_codec::json_length(&state)?)?;
        for bytes in entries {
            let _entry_decode = crate::storage::build_budget::reserve_memory((bytes.len() as u64).checked_mul(32).ok_or_else(||storage("tail decode estimate overflow"))?).map_err(storage)?;
            if selected.is_none() {
                let size=checkpoint_codec::json_length(&state)?.checked_add(bytes.len().checked_mul(3).ok_or_else(||storage("tail growth overflow"))?).ok_or_else(||storage("tail growth overflow"))?;
                state.admit_bytes(size)?;
            }
            let entry=serde_json::from_slice(&bytes).map_err(storage)?;
            if selected.is_some(){pending_decode.push(_entry_decode);pending.push(entry);continue;}
            match entry {
                Entry::Applied(applied) => apply(&mut state, &applied)?,
                Entry::Expired(event) => temporal::apply(&mut state, &event)?,
                Entry::LeaseExpired(event) => lease::apply(&mut state,&event)?,
                Entry::FeedTrim(trim) => change_feed::apply_trim(&mut state,&config.change_feed,&trim)?,
                Entry::Checkpoint { .. } | Entry::Anchor(_) => {
                    return Err(storage("unexpected checkpoint in operation stream"));
                }
            }
        }
        validate_restored_state(&config,&state)?;
        let snapshot_bytes = checkpoint_codec::length(&config, &state)?;
        let feed_cache = change_feed::Cache::rebuild(&state,&config.change_feed)?;
        let graph_cache=graph::Cache::rebuild(&state)?;
        let (mut index,mut ordinals,mut locations)=match prepared {
            Some((index,ordinals,locations))=>(view::RetrievalView::single(index),ordinals,locations),
            None=>{let (index,ordinals)=build(path,&config,&state).await?;
                let locations=ordinals.iter().enumerate().map(|(i,key)|(key.clone(),i)).collect();
                (index,ordinals,locations)},
        };
        if let Some(reference)=selected.as_ref(){
            reference.reclaim_unselected_segments(path)?;
            let target=path.join(format!("segment-{:032x}",rand::random::<u128>()));
            let (tail,_)=build_at(path,&target,&config,&State::default()).await?;
            index.rotate_tail(tail,&mut ordinals,&mut locations,view::RotationBudget{max_segments:64,max_address_slots:config.max_records.checked_add(config.max_operations).and_then(|n|n.checked_mul(64)).ok_or_else(||storage("view address budget overflow"))?}).await?;
        }
        let output = Arc::new(std::sync::RwLock::new(output::OutputState {
            revision: 0, fenced: state.sealed_for.is_some(),
        }));
        let mut inner=Inner {
                background:None,maintenance_requested:None,maintenance_scheduled:false,maintenance_compacted_bytes:None,maintenance_trim:None,maintenance_gc_pending:false,anchor:selected,payloads,
                journal,
                config,
                state,
                index,
                locations,
                ordinals,
                snapshot_bytes,
                healthy: true,
                clock: temporal::clock()?,
                evaluation_time_ms: 0,
                feed_cache,
                graph_cache,
                bootstrap: None, bootstrap_epoch:format!("{:016x}",rand::random::<u64>()),                output:output.clone(),
                #[cfg(feature = "s3-object-store")]
                remote: None,
                #[cfg(test)]
                fail_after_durable: false,
                #[cfg(test)]
                fail_publication_stage: 0,
            };
        for entry in pending {
            inner.replay_anchor_tail(entry).await?;
            #[cfg(test)]
            if path.join(".test-recovery-after-derived-seal").exists() && inner.index.detached_cache {
                return Err(storage("injected recovery interruption after derived seal"));
            }
        }
        validate_restored_state(&inner.config,&inner.state)?;
        inner.feed_cache=change_feed::Cache::rebuild(&inner.state,&inner.config.change_feed)?;
        inner.graph_cache=graph::Cache::rebuild(&inner.state)?;
        inner.reclaim_anchor_objects().await?;
        let indexes = index_consumers::Cursors::recovered(inner.state.seq);
        Ok(Self {output,indexes,inner:Arc::new(Mutex::new(inner)),admission:Arc::new(Semaphore::new(16))})
    }
    /// A cancellation-safe accepted write: dropping this future does not cancel
    /// an already spawned operation. Query its original operation ID afterward.
    pub async fn write(&self, request: RecordWrite) -> Result<WriteReceipt, RecordError> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let inner = self.inner.clone();
        let partition=self.clone();
        let indexes=self.indexes.clone();
        crate::resources::spawn(async move {
            let result=async move {
                let _permit = permit;
                let mut inner = inner.lock().await;
                inner.index.drain_indexes().await?;
                let result = inner.write_one(request).await;
                if inner.healthy { indexes.immediate(inner.state.seq); }
                result
            }.await;
            partition.kick_maintenance().await;
            result
            })
            .await
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?
    }
    /// Lookup the confirmed result. None does not prove a timed-out write failed.
    pub async fn operation(&self, id: &OperationId) -> Result<Option<WriteReceipt>, RecordError> {
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        let epoch = current_epoch().max(inner.state.epoch_watermark);
        if id.epoch > epoch || id.epoch.saturating_add(1) < epoch {
            return Err(RecordError::OperationExpired);
        }
        Ok(inner
            .state
            .operation_lookup
            .get(id)
            .map(|&position| inner.state.operations[position].receipt.clone()))
    }
    /// Get the exact live record from a complete published view.
    pub async fn get(&self, id: &str) -> Result<Option<Record>, RecordError> {
        self.advance_time().await?;
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        Ok(inner
            .state
            .records
            .get(id)
            .filter(|r| inner.locations.contains_key(&r.record_id))
            .cloned())
    }
    /// Inspect visibility without returning stale/deleted original content.
    pub async fn record_status(&self, id: &str) -> Result<Option<RecordStatus>, RecordError> {
        self.advance_time().await?;
        let _permit = self.admission.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some() { return Err(RecordError::RecoveryRequired); }
        let Some(record) = inner.state.records.get(id) else { return Ok(None); };
        let visibility = if inner.state.evidence_quarantined && temporal::protected(record) {
            EvidenceVisibility::Quarantined
        } else { evidence::status(&inner.state.records, id)? };
        let pending_vector_fields = inner.config.schema.iter().filter(|f| f.index_vector
            && record.document.as_ref().is_some_and(|d| !d.contains_key(&f.field))).map(|f| f.field.clone()).collect();
        Ok(Some(RecordStatus { evaluation_time_ms: inner.evaluation_time_ms, record_id: id.into(), version: record.version, visibility, pending_vector_fields, visible_seq: inner.state.seq }))
    }
    /// Search published caches outside the Record guard and reconcile bounded
    /// candidates with canonical records. Index progress may lag durable intake.
    pub async fn search(
        &self,
        text: String,
        vector: Option<Embedding>,
        mode: SearchMode,
        filters: Vec<FacetFilter>,
        limit: usize,
    ) -> Result<RecordSearch, RecordError> {
        self.query(RecordQuery {
            temporal: None,
            text,
            vector,
            mode,
            query_type: QueryType::Union,
            offset: 0,
            length: limit,
            result_type: ResultType::Topk,
            fields: vec![],
            filters,
            sort: vec![],
        })
        .await
    }
    /// Retrieve independent cache observations, then revalidate current records.
    pub async fn query(&self, query: RecordQuery) -> Result<RecordSearch, RecordError> {
        let permit=self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let partition=self.clone();
        crate::resources::spawn(async move {
            let _permit=permit;
            let result=partition.query_independent(query).await;
            partition.kick_maintenance().await;
            result
        }).await.map_err(storage)?
    }
    /// Iterate active records by stable identity. Changing the logical position
    /// invalidates continuation rather than silently skipping or duplicating rows.
    pub async fn page(
        &self,
        cursor: Option<RecordCursor>,
        limit: usize,
    ) -> Result<RecordPage, RecordError> {
        self.advance_time().await?;
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        if limit == 0 || limit > inner.config.max_candidates {
            return Err(RecordError::Rejected(
                "page size outside candidate budget".into(),
            ));
        }
        if cursor
            .as_ref()
            .is_some_and(|c| c.visible_seq != inner.state.seq)
        {
            return Err(RecordError::Conflict(
                "iterator view changed; restart traversal".into(),
            ));
        }
        let start = cursor.map_or(std::ops::Bound::Unbounded, |c| {
            std::ops::Bound::Excluded(c.after)
        });
        let mut rows = inner
            .state
            .records
            .range((start, std::ops::Bound::Unbounded))
            .filter(|(_, record)| inner.locations.contains_key(&record.record_id));
        let records: Vec<_> = rows
            .by_ref()
            .take(limit)
            .map(|(_, record)| record.clone())
            .collect();
        let next = rows.next().and_then(|_| {
            records.last().map(|record| RecordCursor {
                visible_seq: inner.state.seq,
                after: record.record_id.clone(),
            })
        });
        Ok(RecordPage {
            evaluation_time_ms: inner.evaluation_time_ms,
            visible_seq: inner.state.seq,
            records,
            next,
            statistics: inner.statistics(),
        })
    }

    /// Resume a never-served initial directory after interrupted publication.
    /// The caller must exclusively own its parent catalog, including across
    /// missing-target windows. Certified corruption is never permission to reset.
    pub async fn resume_initialization(path: &Path, budget: u64) -> Result<(), RecordError> {
        let path = path.to_owned();
        crate::resources::spawn(async move {
                PartitionJournal::resume_initial(&path, budget).map_err(|e| match e {
                    CreateError::NotPublished(e) => storage(e),
                    CreateError::PublicationUnknown(e) => {
                        RecordError::OutcomeUnknown(e.to_string())
                    }
                })
            })
            .await
            .map_err(storage)?
    }

    /// Remove an explicitly retired independent snapshot. The caller must hold
    /// the parent service lock and durably retire the backup identity first.
    pub async fn retire_snapshot(path: &Path) -> Result<(), RecordError> {
        let path = path.to_owned();
        crate::resources::spawn(async move { PartitionJournal::retire_snapshot(&path).map_err(creation_error) })
            .await
            .map_err(storage)?
    }

    /// Return the immutable configuration from this recovered partition.
    pub async fn configuration(&self) -> Result<RecordConfig, RecordError> {
        let inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        Ok(inner.config.clone())
    }

    /// Read resource and active-version counts at one published position.
    pub async fn statistics(&self) -> Result<RecordStatistics, RecordError> {
        self.advance_time().await?;
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        Ok(inner.statistics())
    }

    /// Publish an authenticated state/index anchor, retaining canonical payload
    /// objects and retiring the covered WAL only after its certificate is durable.
    pub async fn compact_journal(&self) -> Result<u64, RecordError> {
        let permit=self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let owner=self.inner.clone();
        crate::resources::spawn(async move {
            let _permit=permit;let mut inner=owner.lock().await;
            if !inner.healthy{return Err(RecordError::RecoveryRequired);}
            inner.publish_anchor().await
        }).await.map_err(|error|RecordError::OutcomeUnknown(error.to_string()))?
    }

    /// Create an independently recoverable snapshot at a new location. No live
    /// files are copied; original data, configuration, versions and dedup results
    /// are serialized under the same view. Use `compact_journal` for live WAL retirement.
    pub async fn checkpoint(&self, destination: &Path) -> Result<u64, RecordError> {
        self.checkpoint_with_scope(destination, None).await
    }

    // A retained remote backup has an immutable descriptor identity. Repeated
    // private materializations must preserve its digest for takeover binding.
    async fn checkpoint_with_scope(&self, destination: &Path, scope: Option<String>) -> Result<u64, RecordError> {
        // A sealed owner may still be archived without advancing its final cut.
        let advance = { let inner = self.inner.lock().await; inner.state.sealed_for.is_none() && !inner.state.independent_snapshot };
        if advance { self.advance_time().await?; }
        let permit = self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let owner=self.inner.clone();let destination=destination.to_owned();
        crate::resources::spawn(async move {
            let _permit=permit;
            let inner=owner.lock().await;
            if !inner.healthy { return Err(RecordError::RecoveryRequired); }
            let mut state=inner.state.try_clone()?;
            state.independent_snapshot=true;
            state.feed.scope=scope.unwrap_or_else(||format!("{:032x}",rand::random::<u128>()));
            state.evidence_quarantined|=state.records.values().any(temporal::protected);
            let seq=state.seq;let config=inner.config.clone();
            drop(inner);
            snapshot::create(&destination,config,state).await?;
            Ok(seq)
        }).await.map_err(|e|RecordError::OutcomeUnknown(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{
        AccessType, Clustering, DocumentCompression, FieldLengthNormalization, FrequentwordType,
        LexicalSimilarity, StopwordType, TextNormalization, TokenizerType,
    };
    use crate::vector::Inference;
    use crate::vector_similarity::AnnMode;
    pub(super) fn path() -> PathBuf {
        let base = std::env::var_os("ANYSEEK_TEST_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&base).unwrap();
        base.join(format!("anyseek-record-{:032x}", rand::random::<u128>()))
    }
    pub(super) fn config() -> RecordConfig {
        RecordConfig {change_feed:FeedConfig::default(),index:IndexMetaObject{id:0,name:"records".into(),lexical_similarity:LexicalSimilarity::Bm25f,
            tokenizer:TokenizerType::UnicodeAlphanumeric,text_normalization:TextNormalization::None,
            field_length_normalization:FieldLengthNormalization::Shared,stop_words:StopwordType::None,
            frequent_words:FrequentwordType::None,ngram_indexing:0,document_compression:DocumentCompression::Snappy,
            access_type:AccessType::Mmap,clustering:Clustering::None,
            inference:serde_json::from_value::<Inference>(serde_json::json!({"External":{"dimensions":2,"precision":"F32","quantization":"None","similarity":"Cosine"}})).unwrap()},
            schema:serde_json::from_value(serde_json::json!([
                {"field":"body","field_type":"Text","store":false,"index_lexical":true},
                {"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true}
            ])).unwrap(),embedding_space_id:Some("test-model-f32-cosine-v1".into()),max_records:32,max_record_bytes:4096,
            max_operations:100,max_journal_bytes:1024*1024,max_candidates:100}
    }
    pub(super) fn write(nonce: &str, id: &str, body: &str, expected: ExpectedVersion) -> RecordWrite {
        RecordWrite {
            vector_patch: None,
            generation: None,
            temporal: None,
            system: None,            operation_id: OperationId::new(nonce.into()),
            record_id: id.into(),
            routing_key: "document-1".into(),
            expected,
            document: Some(
                serde_json::from_value(
                    serde_json::json!({"body":body,"vector":[2.0,0.0],"not_indexed":"preserved"}),
                )
                .unwrap(),
            ),
        }
    }
    pub(super) async fn query(p: &RecordPartition, mode: SearchMode) -> RecordSearch {
        let vector = if mode == SearchMode::Lexical {
            None
        } else {
            Some(Embedding::F32(vec![1.0, 0.0]))
        };
        p.search("alpha".into(), vector, mode, vec![], 10)
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn record_retrieval_files_do_not_duplicate_authoritative_json() {
        let location = path();
        let mut settings = config();
        for field in &mut settings.schema { field.store = true; }
        let p = RecordPartition::create(&location, settings.clone()).await.unwrap();
        let request = write("unique-payload", "a", "alpha", ExpectedVersion::Absent);
        let original = request.document.clone().unwrap();
        let receipt = p.write(request.clone()).await.unwrap();
        for checkpoint in [false, true] {
            if checkpoint { p.compact_journal().await.unwrap(); }
            let inner = p.inner.lock().await;
            let index = inner.index.test_index().read().await;
            assert!(index.stored_field_names.is_empty());
            for shard in &index.shard_vec {
                assert!(shard.read().await.stored_field_names.is_empty());
            }
            drop(index);
            drop(inner);
            assert_eq!(p.get("a").await.unwrap().unwrap().document.as_ref(), Some(&original));
            let result = query(&p, SearchMode::Lexical).await;
            assert_eq!(result.hits.len(), 1);
            assert_eq!(result.hits[0].record.document.as_ref(), Some(&original));
            assert_eq!(p.configuration().await.unwrap().schema[0].store, true);
        }
        drop(p);
        let p = RecordPartition::open(&location, settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.write(request).await.unwrap(), receipt);
        assert_eq!(p.get("a").await.unwrap().unwrap().document.as_ref(), Some(&original));
        assert!(p.inner.lock().await.index.test_index().read().await.stored_field_names.is_empty());
        let result = query(&p, SearchMode::Vector { similarity_threshold: None, ann_mode: AnnMode::All }).await;
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].record.document.as_ref(), Some(&original));
        drop(p);
        std::fs::remove_dir_all(location).unwrap();
    }

    pub(super) fn generation_write(nonce: &str, expected: ExpectedVersion, generation: GenerationCommand) -> RecordWrite {
        RecordWrite { operation_id:OperationId::new(nonce.into()), record_id:"doc".into(), routing_key:"document-1".into(),
            expected, document:None, vector_patch:None, system:None, temporal:None, generation:Some(generation) }
    }
    pub(super) fn stage_generation(id: &str, members: &[&str]) -> GenerationCommand {
        GenerationCommand::Stage { generation_id:id.into(), source:EvidenceRef { locator:None, record_id:"s".into(), routing_key:"document-1".into(), version:1 },
            members:members.iter().map(|s| (*s).into()).collect(), chunks:members.iter().map(|id| GenerationChunk { record_id:(*id).into(),
                document:serde_json::from_value(serde_json::json!({"body":"generationword alpha"})).unwrap(), temporal:None, evidence_refs:vec![] }).collect() }
    }
    struct TestClock(std::sync::atomic::AtomicU64);
    impl temporal::Clock for TestClock {
        fn now_ms(&self) -> Result<u64, RecordError> { Ok(self.0.load(std::sync::atomic::Ordering::SeqCst)) }
    }
    impl TestClock {
        fn set(&self, value:u64) { self.0.store(value,std::sync::atomic::Ordering::SeqCst); }
    }
    async fn test_clock(p:&RecordPartition) -> (Arc<TestClock>,u64) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let clock=Arc::new(TestClock(std::sync::atomic::AtomicU64::new(now)));
        p.inner.lock().await.clock=clock.clone(); (clock,now)
    }
    pub(super) async fn wait_maintenance_idle(p:&RecordPartition) {
        tokio::time::timeout(std::time::Duration::from_secs(20),async {
            while p.inner.lock().await.maintenance_scheduled {tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();
    }
    pub(super) async fn write_after_maintenance(p:&RecordPartition,request:RecordWrite)->Result<WriteReceipt,RecordError> {
        tokio::time::timeout(std::time::Duration::from_secs(20),async {
            loop {
                match p.write(request.clone()).await {
                    Err(RecordError::Busy) if p.inner.lock().await.maintenance_requested.is_some()=>wait_maintenance_idle(p).await,
                    result=>return result,
                }
            }
        }).await.expect("bounded maintenance retry")
    }
    pub(super) async fn settle_maintenance(p:&RecordPartition) {
        tokio::time::timeout(std::time::Duration::from_secs(20),async {
            loop {match p.advance_time().await {
                Ok(())=>break,
                Err(RecordError::Busy)=>{wait_maintenance_idle(p).await;tokio::time::sleep(std::time::Duration::from_millis(5)).await;},
                Err(error)=>panic!("unexpected maintenance failure: {error}"),
            }}
        }).await.unwrap();
    }
    #[tokio::test]
    async fn staging_lease_renew_expire_redact_and_exact_retry() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        p.write(source_write("source","s",&[])).await.unwrap();
        let stage=generation_write("lease-stage",ExpectedVersion::Absent,stage_generation("g",&["chunk"]));
        let receipt=p.write(stage.clone()).await.unwrap();let status=p.generation_status("doc").await.unwrap().unwrap();assert_eq!(status.lease_expires_at_ms,Some(now+1_800_000));
        clock.set(now+900_000);let renew=generation_write("renew",ExpectedVersion::Match(1),GenerationCommand::Renew{generation_id:"g".into()});
        let renewed=p.write(renew.clone()).await.unwrap();assert_eq!(p.write(renew).await.unwrap(),renewed);
        assert!(p.write(generation_write("old-owner",ExpectedVersion::Match(1),GenerationCommand::Publish{generation_id:"g".into()})).await.is_err());
        clock.set(now+1_800_000);assert_eq!(p.generation_status("doc").await.unwrap().unwrap().staged_members,1);
        clock.set(now+2_700_000);settle_maintenance(&p).await;let status=p.generation_status("doc").await.unwrap().unwrap();assert_eq!(status.staged_members,0);assert_eq!(status.version,3);assert!(!status.private_cleanup_pending);
        assert_eq!(p.write(stage.clone()).await.unwrap(),receipt);
        let mut changed=stage.clone();if let Some(GenerationCommand::Stage{chunks,..})=&mut changed.generation {chunks[0].document.insert("body".into(),serde_json::json!("different"));}
        assert!(matches!(p.write(changed).await,Err(RecordError::Conflict(_))));
        let state=p.inner.lock().await.state.clone();assert!(state.operations[1].request_digest.is_some());assert!(state.lease_expirations.len()==1);
        if let Some(GenerationCommand::Stage{chunks,members,..})=&state.operations[1].request.generation {assert!(chunks.is_empty()&&members.is_empty());}else{panic!();}
        assert_eq!(p.changes(None,100).await.unwrap().events.len(),1);
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();p.inner.lock().await.clock=clock;
        assert_eq!(p.write(stage).await.unwrap(),receipt);assert_eq!(p.generation_status("doc").await.unwrap().unwrap().staged_members,0);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn staging_renew_publish_race_has_one_fenced_winner() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();p.write(source_write("s","s",&[])).await.unwrap();
        p.write(generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["chunk"]))).await.unwrap();
        let (renew,publish)=tokio::join!(p.write(generation_write("renew-race",ExpectedVersion::Match(1),GenerationCommand::Renew{generation_id:"g".into()})),p.write(generation_write("publish-race",ExpectedVersion::Match(1),GenerationCommand::Publish{generation_id:"g".into()})));
        assert_ne!(renew.is_ok(),publish.is_ok());
        if renew.is_ok(){assert!(p.get("chunk").await.unwrap().is_none());p.write(generation_write("publish-current",ExpectedVersion::Match(2),GenerationCommand::Publish{generation_id:"g".into()})).await.unwrap();}
        assert!(p.get("chunk").await.unwrap().is_some());
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();assert!(p.get("chunk").await.unwrap().is_some());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn staging_cleanup_failure_stays_pending_and_retries() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();p.write(source_write("s","s",&[])).await.unwrap();
        let stage=generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["chunk"]));let receipt=p.write(stage.clone()).await.unwrap();
        p.write(generation_write("abort",ExpectedVersion::Match(1),GenerationCommand::Abort{generation_id:"g".into()})).await.unwrap();
        std::fs::write(root.join(".test-cleanup-failure"),b"fail old generation retirement").unwrap();
        assert!(matches!(p.generation_status("doc").await,Err(RecordError::Busy)));
        wait_maintenance_idle(&p).await;
        {let inner=p.inner.lock().await;assert!(inner.state.private_cleanup_pending||inner.maintenance_gc_pending);}
        assert!(matches!(p.write(stage.clone()).await,Err(RecordError::Busy)));
        wait_maintenance_idle(&p).await;
        {let inner=p.inner.lock().await;assert!(inner.state.private_cleanup_pending||inner.maintenance_gc_pending);}

        std::fs::remove_file(root.join(".test-cleanup-failure")).unwrap();
        settle_maintenance(&p).await;
        assert!(!p.generation_status("doc").await.unwrap().unwrap().private_cleanup_pending);
        let generations:Vec<_>=std::fs::read_dir(&root).unwrap().filter_map(Result::ok).filter(|e|e.file_name().to_string_lossy().starts_with("generation-")).collect();
        assert_eq!(generations.len(),1);
        assert_eq!(p.write(stage).await.unwrap(),receipt);
        // Windows byte-range owner locks forbid a second reader of the live WAL.
        // Finish retry assertions under the owner, then inspect persisted bytes after close.
        drop(p);
        assert!(!std::fs::read(generations[0].path().join("wal")).unwrap().windows(b"generationword alpha".len()).any(|w|w==b"generationword alpha"));
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();assert!(!p.generation_status("doc").await.unwrap().unwrap().private_cleanup_pending);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn staging_certified_cleanup_gc_failure_blocks_recovery_until_retired() {
        let root=path();
        let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(source_write("s","s",&[])).await.unwrap();
        let stage=generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["chunk"]));
        let receipt=p.write(stage.clone()).await.unwrap();
        p.compact_journal().await.unwrap();
        let old_payloads=p.inner.lock().await.payloads.clone();
        assert!(!old_payloads.objects.is_empty());
        p.write(generation_write("abort",ExpectedVersion::Match(1),GenerationCommand::Abort{generation_id:"g".into()})).await.unwrap();
        std::fs::write(root.join(".test-anchor-post-certificate-gc"),b"fail after certificate").unwrap();
        assert!(matches!(p.generation_status("doc").await,Err(RecordError::Busy)));
        wait_maintenance_idle(&p).await;
        assert!(p.inner.lock().await.maintenance_gc_pending);
        // The new certificate is authoritative, but the old private bytes still
        // exist because physical retirement failed after its publication.
        assert!(old_payloads.objects.keys().all(|id|root.join(format!("payload-{id}")).exists()));
        drop(p);
        assert!(RecordPartition::open(&root,config().max_journal_bytes).await.is_err());
        assert!(old_payloads.objects.keys().all(|id|root.join(format!("payload-{id}")).exists()));
        std::fs::remove_file(root.join(".test-anchor-gc-failure")).unwrap();
        std::fs::remove_file(root.join(".test-anchor-post-certificate-gc")).unwrap();
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert!(old_payloads.objects.keys().all(|id|!root.join(format!("payload-{id}")).exists()));
        assert!(!p.generation_status("doc").await.unwrap().unwrap().private_cleanup_pending);
        assert_eq!(p.write(stage).await.unwrap(),receipt);
        assert!(p.get("chunk").await.unwrap().is_none());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn staging_abort_releases_private_payload_but_keeps_canonical_retry() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();p.write(source_write("s","s",&[])).await.unwrap();
        let mut stage=generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["chunk"]));
        if let Some(GenerationCommand::Stage{chunks,..})=&mut stage.generation{chunks[0].document.insert("extra".into(),serde_json::json!({"b":2,"a":1}));}
        let receipt=p.write(stage.clone()).await.unwrap();
        p.write(generation_write("abort",ExpectedVersion::Match(1),GenerationCommand::Abort{generation_id:"g".into()})).await.unwrap();
        settle_maintenance(&p).await;
        if let Some(GenerationCommand::Stage{chunks,..})=&mut stage.generation{chunks[0].document.insert("extra".into(),serde_json::json!({"a":1,"b":2}));}
        assert_eq!(p.write(stage.clone()).await.unwrap(),receipt);
        assert!(p.write(generation_write("spent",ExpectedVersion::Match(2),stage_generation("g",&["chunk"]))).await.is_err());
        p.write(generation_write("new",ExpectedVersion::Match(2),stage_generation("g2",&["chunk"]))).await.unwrap();
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();assert_eq!(p.write(stage).await.unwrap(),receipt);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn staging_expiry_durable_pending_and_owner_scope_fence() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        p.write(source_write("s","s",&[])).await.unwrap();p.write(generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["chunk"]))).await.unwrap();
        // A new certified owner's scope differs; it must explicitly renew.
        p.inner.lock().await.state.feed.scope="other-owner".into();
        assert!(p.generation_status("doc").await.unwrap().unwrap().lease_requires_renewal);
        assert!(p.write(generation_write("publish",ExpectedVersion::Match(1),GenerationCommand::Publish{generation_id:"g".into()})).await.is_err());
        p.write(generation_write("renew",ExpectedVersion::Match(1),GenerationCommand::Renew{generation_id:"g".into()})).await.unwrap();
        assert!(!p.generation_status("doc").await.unwrap().unwrap().lease_requires_renewal);
        clock.set(now+1_800_000);p.inner.lock().await.fail_after_durable=true;
        assert!(matches!(p.generation_status("doc").await,Err(RecordError::DurablePending(_))));
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();p.inner.lock().await.clock=clock;
        settle_maintenance(&p).await;
        assert_eq!(p.generation_status("doc").await.unwrap().unwrap().staged_members,0);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn locator_utf8_pages_history_and_delete_barrier() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        let mut source=source_write("located-source","s",&[]);
        source.document.as_mut().unwrap().insert("body".into(),serde_json::json!("A中éZ"));
        source.document.as_mut().unwrap().insert("pages".into(),serde_json::json!(["第一页","page two"]));
        p.write(source.clone()).await.unwrap();
        let reference=EvidenceRef{record_id:"s".into(),routing_key:"document-1".into(),version:1,locator:Some(EvidenceLocator::Utf8{field:"body".into(),start:1,end:6})};
        assert_eq!(p.evidence_excerpt(reference.clone()).await.unwrap().text,"中é");
        let mut derived=source_write("located-derived","d",&[("s",1)]);derived.system.as_mut().unwrap().evidence_refs[0]=reference.clone();
        let receipt=p.write(derived.clone()).await.unwrap();assert_eq!(p.write(derived).await.unwrap(),receipt);
        let before=p.statistics().await.unwrap().visible_seq;
        for (start,end) in [(2,4),(1,5),(6,6),(0,99)] {
            let mut bad=source_write(&format!("invalid-{start}-{end}"),"bad",&[("s",1)]);
            bad.system.as_mut().unwrap().evidence_refs[0].locator=Some(EvidenceLocator::Utf8{field:"body".into(),start,end});
            assert!(p.write(bad).await.is_err());
        }
        assert_eq!(p.statistics().await.unwrap().visible_seq,before);
        let mut page=reference.clone();page.locator=Some(EvidenceLocator::Page{field:"pages".into(),page:2});
        assert_eq!(p.evidence_excerpt(page.clone()).await.unwrap().text,"page two");
        page.locator=Some(EvidenceLocator::Page{field:"pages".into(),page:0});assert!(p.evidence_excerpt(page).await.is_err());
        let original=p.evidence_excerpt(reference.clone()).await.unwrap();
        source.operation_id=OperationId::new("source-revision".into());source.expected=ExpectedVersion::Match(1);
        source.document.as_mut().unwrap().insert("body".into(),serde_json::json!("short"));p.write(source).await.unwrap();
        assert!(p.get("d").await.unwrap().is_none());
        assert_eq!(p.evidence_excerpt(reference.clone()).await.unwrap().content_sha256,original.content_sha256);
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.evidence_excerpt(reference.clone()).await.unwrap().text,"中é");
        let mut corrupt=p.inner.lock().await.state.clone();corrupt.records.edit("d",256,|record|{record.system.as_mut().unwrap().evidence_refs[0].locator=Some(EvidenceLocator::Utf8{field:"body".into(),start:1,end:2});Ok(())}).unwrap();
        assert!(locator::validate_history(&corrupt).is_err());
        let permit=p.output_permit().unwrap();let mut deletion=write("delete-source","s","",ExpectedVersion::Match(2));deletion.document=None;p.write(deletion).await.unwrap();
        assert!(permit.deliver(||()).is_err());assert!(p.evidence_excerpt(reference.clone()).await.is_err());
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();assert!(p.evidence_excerpt(reference).await.is_err());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn locator_generation_and_ttl_keep_source_gates() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        let mut source=source_write("source","s",&[]);source.temporal=Some(TemporalMetadata{expire_at_ms:Some(now+100),..Default::default()});p.write(source).await.unwrap();
        let mut stage=stage_generation("g",&["chunk"]);
        if let GenerationCommand::Stage{source,..}=&mut stage {source.locator=Some(EvidenceLocator::Utf8{field:"body".into(),start:0,end:8});}
        p.write(generation_write("stage",ExpectedVersion::Absent,stage)).await.unwrap();
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();p.inner.lock().await.clock=clock.clone();
        p.write(generation_write("publish",ExpectedVersion::Match(1),GenerationCommand::Publish{generation_id:"g".into()})).await.unwrap();
        let reference=p.get("chunk").await.unwrap().unwrap().system.unwrap().evidence_refs[0].clone();
        assert_eq!(p.evidence_excerpt(reference.clone()).await.unwrap().text,"evidence");
        let mut next=stage_generation("g2",&["next"]);if let GenerationCommand::Stage{source,..}=&mut next {*source=reference.clone();}
        p.write(generation_write("stage-next",ExpectedVersion::Match(2),next)).await.unwrap();
        let mut update=source_write("update","s",&[]);update.expected=ExpectedVersion::Match(1);update.document.as_mut().unwrap().insert("body".into(),serde_json::json!("x"));p.write(update).await.unwrap();
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();p.inner.lock().await.clock=clock.clone();
        assert!(p.write(generation_write("publish-stale",ExpectedVersion::Match(3),GenerationCommand::Publish{generation_id:"g2".into()})).await.is_err());
        assert_eq!(p.evidence_excerpt(reference.clone()).await.unwrap().text,"evidence");
        clock.set(now+100);assert!(p.evidence_excerpt(reference).await.is_err());assert!(p.get("chunk").await.unwrap().is_none());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    fn entity_write(nonce:&str,id:&str)->RecordWrite {
        let mut request=write(nonce,id,"alpha entity",ExpectedVersion::Absent);
        request.system=Some(EvidenceMetadata{kind:EvidenceKind::Entity,relation:None,evidence_refs:vec![]});request
    }
    fn edge_write(nonce:&str,id:&str,from:&str,to:&str)->RecordWrite {
        let mut request=write(nonce,id,"alpha edge",ExpectedVersion::Absent);
        let endpoint=|id:&str|EvidenceRef{locator:None,record_id:id.into(),routing_key:"document-1".into(),version:1};
        request.system=Some(EvidenceMetadata{kind:EvidenceKind::Relation,relation:Some(RelationMetadata{from:endpoint(from),to:endpoint(to),relation_type:"knows".into(),weight:Some(0.5)}),evidence_refs:vec![]});request
    }
    fn neighbor_request(id:&str)->NeighborRequest {
        NeighborRequest{seeds:vec![EvidenceRef{locator:None,record_id:id.into(),routing_key:"document-1".into(),version:1}],direction:RelationDirection::Both,relation_types:vec![],per_seed:128,max_candidates:100,allow_partial:false}
    }
    fn fusion_query() -> RecordQuery {
        RecordQuery{temporal:None,text:"alpha".into(),vector:Some(Embedding::F32(vec![1.0,0.0])),mode:SearchMode::Hybrid{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All},query_type:QueryType::Union,offset:0,length:20,result_type:ResultType::Topk,fields:vec![],filters:vec![],sort:vec![]}
    }
    fn fusion_options() -> RelationFusion {
        let mut neighbors=neighbor_request("a");neighbors.max_candidates=20;
        RelationFusion{neighbors,rrf:crate::search::RrfOptions{constant:3.0,keyword_weight:0.5,vector_weight:2.0,keyword_candidates:Some(20),vector_candidates:Some(20)},relation_weight:1.5}
    }
    #[tokio::test]
    async fn fusion_rank_oracle_recovery_and_output_barrier() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(entity_write("a","a")).await.unwrap();
        p.write(write("b","b","alpha alpha",ExpectedVersion::Absent)).await.unwrap();
        p.write(write("c","c","unrelated",ExpectedVersion::Absent)).await.unwrap();
        for (id,to) in [("ab","b"),("ab2","b"),("ac","c")] {p.write(edge_write(id,id,"a",to)).await.unwrap();}
        let options=fusion_options();let mut oracle=BTreeMap::<String,f32>::new();
        for (mode,weight) in [(SearchMode::Lexical,0.5),(SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All},2.0)] {
            let mut q=fusion_query();q.mode=mode.clone();if mode==SearchMode::Lexical{q.vector=None;}
            let channel=p.query(q).await.unwrap();for(rank,hit)in channel.hits.iter().enumerate(){*oracle.entry(hit.record.record_id.clone()).or_default()+=weight/(3.0+rank as f32);}
        }
        *oracle.entry("b".into()).or_default()+=1.5/3.0;*oracle.entry("c".into()).or_default()+=1.5/4.0;
        let mut expected:Vec<_>=oracle.into_iter().collect();expected.sort_by(|a,b|b.1.total_cmp(&a.1).then_with(||a.0.cmp(&b.0)));
        let result=p.query_with_relations(fusion_query(),options.clone()).await.unwrap();
        assert_eq!(result.hits.iter().map(|h|(h.record.record_id.clone(),h.score)).collect::<Vec<_>>(),expected);
        for hit in &result.hits {assert_eq!(hit.channels.iter().map(|c|c.rrf_contribution.unwrap()).sum::<f32>(),hit.score);assert_eq!(hit.channels.iter().filter(|c|c.channel=="relation").count(),usize::from(["b","c"].contains(&hit.record.record_id.as_str())));for c in &hit.channels{assert_eq!(c.raw_score.is_none(),c.channel=="relation");}}

        assert_eq!(result.count_total,6);assert!(result.fusion.unwrap().relation_complete);
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.query_with_relations(fusion_query(),options.clone()).await.unwrap().hits.iter().map(|h|(h.record.record_id.clone(),h.score)).collect::<Vec<_>>(),expected);
        let output=p.output_permit().unwrap();let mut deletion=write("delete-b","b","",ExpectedVersion::Match(1));deletion.document=None;p.write(deletion).await.unwrap();
        assert!(output.deliver(||()).is_err());
        let result=p.query_with_relations(fusion_query(),options).await.unwrap();assert_eq!(result.statistics.active_records,3);
        assert!(!result.hits.iter().any(|h|["b","ab","ab2"].contains(&h.record.record_id.as_str())));
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn fusion_budget_and_temporal_predicate_before_ranks() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        p.write(entity_write("a","a")).await.unwrap();
        let mut b=write("b","b","alpha",ExpectedVersion::Absent);b.temporal=Some(TemporalMetadata{event_time_ms:Some(now as i64),expire_at_ms:Some(now+100),..Default::default()});p.write(b).await.unwrap();
        p.write(write("c","c","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.write(edge_write("ab","ab","a","b")).await.unwrap();p.write(edge_write("ac","ac","a","c")).await.unwrap();
        let mut options=fusion_options();options.neighbors.max_candidates=1;
        assert!(p.query_with_relations(fusion_query(),options.clone()).await.is_err());options.neighbors.allow_partial=true;
        let result=p.query_with_relations(fusion_query(),options).await.unwrap();assert!(!result.fusion.unwrap().relation_complete);
        let mut q=fusion_query();q.temporal=Some(TemporalFilter{event:Some(TimeRange{from_ms:Some(now as i64),to_ms:Some((now+1) as i64)}),..Default::default()});
        let result=p.query_with_relations(q,fusion_options()).await.unwrap();assert_eq!(result.hits.len(),1);assert_eq!(result.hits[0].record.record_id,"b");
        clock.set(now+100);let result=p.query_with_relations(fusion_query(),fusion_options()).await.unwrap();assert_eq!(result.evaluation_time_ms,now+100);assert!(!result.hits.iter().any(|h|h.record.record_id=="b"||h.record.record_id=="ab"));
        let mut bad=fusion_options();bad.rrf.keyword_candidates=Some(100);assert!(p.query_with_relations(fusion_query(),bad).await.is_err());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn query_explanations_preserve_raw_scores_ranks_and_pagination() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        for id in ["c","a","b"] {p.write(write(id,id,"alpha",ExpectedVersion::Absent)).await.unwrap();}
        for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:AnnMode::All}] {
            let mut q=fusion_query();q.mode=mode.clone();if mode==SearchMode::Lexical{q.vector=None;}q.offset=1;q.length=1;
            let r=p.query(q).await.unwrap();assert_eq!(r.hits.len(),1);let hit=&r.hits[0];assert_eq!(hit.channels.len(),1);assert_eq!(hit.channels[0].rank,1);assert_eq!(hit.channels[0].raw_score,Some(hit.score));assert!(hit.channels[0].rrf_contribution.is_none());
        }
        let mut q=fusion_query();q.mode=SearchMode::WeightedHybrid{similarity_threshold:None,ann_mode:AnnMode::All,rrf:crate::search::RrfOptions{constant:3.0,keyword_weight:0.5,vector_weight:2.0,keyword_candidates:Some(3),vector_candidates:Some(2)}};
        let all=p.query(q).await.unwrap();
        let lexical_raw=query(&p,SearchMode::Lexical).await.hits[0].score;
        let vector_raw=query(&p,SearchMode::Vector{similarity_threshold:None,ann_mode:AnnMode::All}).await.hits[0].score;
        assert!(all.hits.iter().flat_map(|h|&h.channels).all(|c|c.raw_score==Some(if c.channel=="lexical"{lexical_raw}else{vector_raw})));
        for (rank,hit) in all.hits.iter().enumerate(){assert_eq!(hit.record.record_id,["a","b","c"][rank]);assert_eq!(hit.channels.iter().map(|c|c.rrf_contribution.unwrap()).sum::<f32>(),hit.score);let lex=hit.channels.iter().find(|c|c.channel=="lexical").unwrap();assert_eq!(lex.rank,rank);assert_eq!(lex.rrf_contribution,Some(0.5/(3.0+rank as f32)));assert_eq!(hit.channels.iter().any(|c|c.channel=="vector"),rank<2);}
        let mut q=fusion_query();q.mode=SearchMode::Hybrid{similarity_threshold:None,ann_mode:AnnMode::All};q.offset=1;q.length=1;
        let page=p.query(q).await.unwrap();assert_eq!(page.hits[0].channels[0].rank,1);assert_eq!(page.hits[0].channels[1].rank,1);
        let mut q=fusion_query();q.mode=SearchMode::WeightedHybrid{similarity_threshold:None,ann_mode:AnnMode::All,rrf:crate::search::RrfOptions{constant:3.0,keyword_weight:0.0,vector_weight:1.0,keyword_candidates:Some(3),vector_candidates:Some(3)}};
        let vector_only=p.query(q).await.unwrap();assert!(!vector_only.hits.is_empty());assert!(vector_only.hits.iter().all(|h|h.channels.len()==1&&h.channels[0].channel=="vector"&&h.channels[0].rrf_contribution==Some(h.score)));
        let mut q=fusion_query();q.mode=SearchMode::Lexical;q.vector=None;q.sort=vec![crate::search::ResultSort{field:"_id".into(),order:crate::search::SortOrder::Descending}];q.offset=1;q.length=1;
        let sorted=p.query(q).await.unwrap();assert_eq!(sorted.hits.len(),1);assert_eq!(sorted.hits[0].channels[0].rank_basis,"requested_order");assert_eq!(sorted.hits[0].channels[0].rank,1);
        let browse=p.search(String::new(),None,SearchMode::Lexical,vec![],10).await.unwrap();assert!(browse.hits.iter().all(|h|h.channels[0].channel=="browse"&&h.channels[0].raw_score.is_none()));
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();let restored=query(&p,SearchMode::Hybrid{similarity_threshold:None,ann_mode:AnnMode::All}).await;assert!(restored.hits.iter().all(|h|h.channels.len()==2));
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn multipart_generation_exceeds_command_limit_with_small_publish() {
        let root=path();let mut cfg=config();cfg.max_record_bytes=1024*1024;cfg.max_journal_bytes=64*1024*1024;
        let p=RecordPartition::create(&root,cfg.clone()).await.unwrap();p.write(source_write("s","s",&[])).await.unwrap();
        let GenerationCommand::Stage{source,members,mut chunks,..}=stage_generation("g",&["one","two"]) else{unreachable!()};
        for chunk in &mut chunks {chunk.document.insert("payload".into(),serde_json::json!("x".repeat(600_000)));}
        let begin=generation_write("begin",ExpectedVersion::Absent,GenerationCommand::Begin{generation_id:"g".into(),source,members});
        p.write(begin.clone()).await.unwrap();assert!(p.write(write("reserved","one","alpha",ExpectedVersion::Absent)).await.is_err());
        let first=generation_write("batch1",ExpectedVersion::Match(1),GenerationCommand::StageBatch{generation_id:"g".into(),chunks:vec![chunks[1].clone()]});
        let r=p.write(first.clone()).await.unwrap();assert!(r.staged_only);assert_eq!(p.write(first.clone()).await.unwrap(),r);
        assert!(p.get("two").await.unwrap().is_none());
        let incomplete=p.generation_status("doc").await.unwrap().unwrap();assert_eq!(incomplete.expected_members,2);assert_eq!(incomplete.staged_members,1);
        assert!(p.write(generation_write("earlyseal",ExpectedVersion::Match(2),GenerationCommand::Seal{generation_id:"g".into(),manifest_sha256:incomplete.manifest_sha256.unwrap()})).await.is_err());
        assert!(p.write(generation_write("earlypublish",ExpectedVersion::Match(2),GenerationCommand::Publish{generation_id:"g".into()})).await.is_err());
        let second=generation_write("batch2",ExpectedVersion::Match(2),GenerationCommand::StageBatch{generation_id:"g".into(),chunks:vec![chunks[0].clone()]});
        p.write(second.clone()).await.unwrap();
        assert!(serde_json::to_vec(&first).unwrap().len()<1024*1024);assert!(serde_json::to_vec(&second).unwrap().len()<1024*1024);
        assert!(serde_json::to_vec(&first).unwrap().len()+serde_json::to_vec(&second).unwrap().len()>1024*1024);
        let status=p.generation_status("doc").await.unwrap().unwrap();
        assert!(p.write(generation_write("badseal",ExpectedVersion::Match(3),GenerationCommand::Seal{generation_id:"g".into(),manifest_sha256:"0".repeat(64)})).await.is_err());
        let seal=generation_write("seal",ExpectedVersion::Match(3),GenerationCommand::Seal{generation_id:"g".into(),manifest_sha256:status.manifest_sha256.unwrap()});
        p.write(seal.clone()).await.unwrap();assert!(serde_json::to_vec(&seal).unwrap().len()<1024);
        drop(p);let p=RecordPartition::open(&root,cfg.max_journal_bytes).await.unwrap();
        assert!(p.generation_status("doc").await.unwrap().unwrap().sealed);assert!(p.get("one").await.unwrap().is_none());
        assert_eq!(p.write(first).await.unwrap(),r);
        let publish=generation_write("publish",ExpectedVersion::Match(4),GenerationCommand::Publish{generation_id:"g".into()});
        let receipt=p.write(publish.clone()).await.unwrap();assert!(!receipt.staged_only);
        let inner=p.inner.lock().await;
        let applied=inner.state.operations.last().unwrap();assert!(encode_entry(&Entry::Applied(applied.clone())).unwrap().len()<2048);
        let batches:Vec<_>=inner.state.operations.iter().filter(|a|matches!(a.request.generation,Some(GenerationCommand::StageBatch{..}))).collect();assert_eq!(batches.len(),2);
        assert!(batches.iter().all(|a|encode_entry(&Entry::Applied((*a).clone())).unwrap().len()<1024*1024));drop(inner);
        for id in ["one","two"]{assert_eq!(p.get(id).await.unwrap().unwrap().document.unwrap()["payload"].as_str().unwrap().len(),600_000);}
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,cfg.max_journal_bytes).await.unwrap();
        assert_eq!(p.write(publish).await.unwrap(),receipt);assert_eq!(p.generation_status("doc").await.unwrap().unwrap().visible_members,2);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn multipart_checkpoint_cannot_forge_seal_or_order() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();p.write(source_write("s","s",&[])).await.unwrap();
        let GenerationCommand::Stage{source,members,chunks,..}=stage_generation("g",&["one","two"]) else{unreachable!()};
        p.write(generation_write("begin",ExpectedVersion::Absent,GenerationCommand::Begin{generation_id:"g".into(),source,members})).await.unwrap();
        p.write(generation_write("batch",ExpectedVersion::Match(1),GenerationCommand::StageBatch{generation_id:"g".into(),chunks})).await.unwrap();
        let inner=p.inner.lock().await;let state=inner.state.clone();let cfg=inner.config.clone();drop(inner);
        let mut bad=state.clone();bad.generations.get_mut("doc").unwrap().staged.as_mut().unwrap().sealed=true;
        let mut order=state;order.generations.get_mut("doc").unwrap().staged.as_mut().unwrap().chunks.reverse();
        let hash=p.generation_status("doc").await.unwrap().unwrap().manifest_sha256.unwrap();
        p.write(generation_write("seal",ExpectedVersion::Match(2),GenerationCommand::Seal{generation_id:"g".into(),manifest_sha256:hash})).await.unwrap();
        let mut wrong=p.inner.lock().await.state.clone();wrong.operations.edit(wrong.operations.len()-1,0,|op|{if let Some(GenerationCommand::Seal{manifest_sha256,..})=&mut op.request.generation{*manifest_sha256="0".repeat(64);}Ok(())}).unwrap();
        for state in [bad,order,wrong]{let damaged=path();let bytes=encode_entry(&Entry::Checkpoint{config:cfg.clone(),state}).unwrap();drop(PartitionJournal::create(&damaged,cfg.max_journal_bytes,&bytes).unwrap());assert!(RecordPartition::open(&damaged,cfg.max_journal_bytes).await.is_err());std::fs::remove_dir_all(damaged).unwrap();}
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn multipart_completion_reserves_seal_and_publish() {
        let root=path();let mut cfg=config();cfg.max_operations=7;
        let p=RecordPartition::create(&root,cfg).await.unwrap();p.write(source_write("s","s",&[])).await.unwrap();
        let GenerationCommand::Stage{source,members,chunks,..}=stage_generation("g",&["one"]) else{unreachable!()};
        p.write(generation_write("begin",ExpectedVersion::Absent,GenerationCommand::Begin{generation_id:"g".into(),source,members})).await.unwrap();
        p.write(generation_write("batch",ExpectedVersion::Match(1),GenerationCommand::StageBatch{generation_id:"g".into(),chunks})).await.unwrap();
        let hash=p.generation_status("doc").await.unwrap().unwrap().manifest_sha256.unwrap();
        p.write(generation_write("seal",ExpectedVersion::Match(2),GenerationCommand::Seal{generation_id:"g".into(),manifest_sha256:hash})).await.unwrap();
        p.write(generation_write("publish",ExpectedVersion::Match(3),GenerationCommand::Publish{generation_id:"g".into()})).await.unwrap();assert!(p.get("one").await.unwrap().is_some());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn multipart_expiry_redacts_each_batch_and_retains_retries() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        p.write(source_write("s","s",&[])).await.unwrap();
        let GenerationCommand::Stage{source,members,chunks,..}=stage_generation("g",&["one","two"]) else{unreachable!()};
        p.write(generation_write("begin",ExpectedVersion::Absent,GenerationCommand::Begin{generation_id:"g".into(),source,members})).await.unwrap();
        clock.set(now+100);let batch=generation_write("batch",ExpectedVersion::Match(1),GenerationCommand::StageBatch{generation_id:"g".into(),chunks:vec![chunks[0].clone()]});let receipt=p.write(batch.clone()).await.unwrap();
        clock.set(now+1_800_100);
        assert!(matches!(p.generation_status("doc").await, Err(RecordError::Busy)));
        settle_maintenance(&p).await;
        assert!(p.generation_status("doc").await.unwrap().unwrap().staged_generation.is_none());
        assert_eq!(p.write(batch.clone()).await.unwrap(),receipt);
        let state=p.inner.lock().await.state.clone();let op=state.operations.iter().find(|o|o.request.operation_id==batch.operation_id).unwrap();assert!(op.request_digest.is_some());
        assert!(matches!(&op.request.generation,Some(GenerationCommand::StageBatch{chunks,..}) if chunks.is_empty()));lease::validate_state(&state).unwrap();
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();p.inner.lock().await.clock=clock.clone();assert_eq!(p.write(batch).await.unwrap(),receipt);
        p.write(write("reuse","two","alpha",ExpectedVersion::Absent)).await.unwrap();
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn relation_cycles_exact_endpoints_aba_and_rebuild() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(entity_write("entity-a","a")).await.unwrap();
        let normal=write("node-b","b","alpha",ExpectedVersion::Absent);p.write(normal.clone()).await.unwrap();
        let original_feed=p.changes(None,100).await.unwrap().events;
        let edge=edge_write("edge-ab","ab","a","b");let receipt=p.write(edge.clone()).await.unwrap();
        assert_eq!(p.write(edge).await.unwrap(),receipt);
        p.write(edge_write("edge-ba","ba","b","a")).await.unwrap();
        p.write(edge_write("edge-aa","aa","a","a")).await.unwrap();
        assert_eq!(p.neighbors(neighbor_request("a")).await.unwrap().hits.len(),3);
        assert_eq!(p.entities(None,10).await.unwrap().records.len(),1);
        assert_eq!(&p.changes(None,100).await.unwrap().events[..2],original_feed.as_slice());
        let output=p.output_permit().unwrap();
        let mut replace=normal;replace.operation_id=OperationId::new("node-b-update".into());replace.expected=ExpectedVersion::Match(1);
        p.write(replace).await.unwrap();assert!(output.deliver(||()).is_err());
        assert!(p.get("ab").await.unwrap().is_none());assert!(p.get("ba").await.unwrap().is_none());
        let result=p.neighbors(neighbor_request("a")).await.unwrap();assert_eq!(result.hits.len(),1);assert_eq!(result.hits[0].edge.record_id,"aa");
        assert_eq!(result.count.value,1);assert!(result.complete);assert_eq!(result.statistics.active_records,3);
        assert!(p.changes(None,100).await.unwrap().events.last().unwrap().rescan_required);
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.neighbors(neighbor_request("a")).await.unwrap().count.value,1);
        assert!(matches!(p.write(edge_write("stale-edge","new","a","b")).await,Err(RecordError::Conflict(_))));
        let mut deletion=write("delete-b","b","alpha",ExpectedVersion::Match(2));deletion.document=None;
        assert_eq!(p.write(deletion).await.unwrap().version,3);
        assert_eq!(p.write(write("recreate-b","b","alpha",ExpectedVersion::Match(3))).await.unwrap().version,4);
        assert_eq!(p.neighbors(neighbor_request("a")).await.unwrap().count.value,1);
        assert!(p.get("ab").await.unwrap().is_none());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn relation_generation_switch_and_transitive_proof_recovery() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(source_write("s","s",&[])).await.unwrap();
        p.write(entity_write("a","a")).await.unwrap();
        p.write(generation_write("stage1",ExpectedVersion::Absent,stage_generation("g1",&["old"]))).await.unwrap();
        p.write(generation_write("publish1",ExpectedVersion::Match(1),GenerationCommand::Publish{generation_id:"g1".into()})).await.unwrap();
        p.write(edge_write("edge-old","edge-old","a","old")).await.unwrap();
        p.write(source_write("proof","proof",&[("edge-old",1)])).await.unwrap();
        assert_eq!(p.neighbors(neighbor_request("a")).await.unwrap().count.value,1);
        p.write(generation_write("stage2",ExpectedVersion::Match(2),stage_generation("g2",&["new"]))).await.unwrap();
        assert_eq!(p.neighbors(neighbor_request("a")).await.unwrap().count.value,1);
        let permit=p.output_permit().unwrap();
        let publish=generation_write("publish2",ExpectedVersion::Match(3),GenerationCommand::Publish{generation_id:"g2".into()});
        let receipt=p.write(publish.clone()).await.unwrap();assert_eq!(p.write(publish).await.unwrap(),receipt);
        assert!(permit.deliver(||()).is_err());
        for id in ["old","edge-old","proof"] {assert!(p.get(id).await.unwrap().is_none());}
        let fused=p.query_with_relations(fusion_query(),fusion_options()).await.unwrap();
        assert!(!fused.hits.iter().any(|h|["old","edge-old","proof"].contains(&h.record.record_id.as_str())));
        assert!(fused.hits.iter().any(|h|h.record.record_id=="new"));
        assert_eq!(p.neighbors(neighbor_request("a")).await.unwrap().count.value,0);
        p.write(edge_write("edge-new","edge-new","a","new")).await.unwrap();
        let mut illegal=edge_write("retarget","edge-new","a","a");illegal.expected=ExpectedVersion::Match(1);
        assert!(p.write(illegal).await.is_err());
        let mut deletion=source_write("delete-source","s",&[]);deletion.expected=ExpectedVersion::Match(1);deletion.document=None;deletion.system=None;
        p.write(deletion).await.unwrap();
        assert_eq!(p.neighbors(neighbor_request("a")).await.unwrap().count.value,0);
        let fused=p.query_with_relations(fusion_query(),fusion_options()).await.unwrap();assert_eq!(fused.hits.len(),1);assert_eq!(fused.hits[0].record.record_id,"a");
        assert_eq!(p.statistics().await.unwrap().active_records,1);
        assert!(p.changes(None,100).await.unwrap().events.last().unwrap().rescan_required);
        p.compact_journal().await.unwrap();drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.neighbors(neighbor_request("a")).await.unwrap().count.value,0);
        assert_eq!(p.statistics().await.unwrap().active_records,1);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn relation_endpoint_depth_rejected_before_durable_append() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(source_write("root","s",&[])).await.unwrap();let mut parent="s".to_string();
        for n in 1..=16 {let id=format!("d{n}");p.write(source_write(&id,&id,&[(&parent,1)])).await.unwrap();parent=id;}
        p.write(entity_write("a","a")).await.unwrap();
        let before=p.statistics().await.unwrap();let permit=p.output_permit().unwrap();
        assert!(matches!(p.write(edge_write("too-deep","edge","a",&parent)).await,Err(RecordError::Rejected(_))));
        let after=p.statistics().await.unwrap();assert_eq!(before.visible_seq,after.visible_seq);assert_eq!(before.active_records,after.active_records);assert!(permit.deliver(||()).is_ok());
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();assert_eq!(p.statistics().await.unwrap().active_records,18);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn relation_budget_source_expiry_and_common_statistics() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        p.write(entity_write("a","a")).await.unwrap();
        let mut source=source_write("s","s",&[]);source.temporal=Some(TemporalMetadata{expire_at_ms:Some(now+100),..Default::default()});p.write(source).await.unwrap();
        p.write(edge_write("as","as","a","s")).await.unwrap();p.write(edge_write("aa","aa","a","a")).await.unwrap();
        let mut limited=neighbor_request("a");limited.per_seed=1;
        assert!(matches!(p.neighbors(limited.clone()).await,Err(RecordError::Rejected(_))));
        limited.allow_partial=true;let page=p.neighbors(limited).await.unwrap();assert!(!page.complete);assert_eq!(page.count.relation,"window");assert_eq!(page.count.value,1);
        clock.set(now+100);let page=p.neighbors(neighbor_request("a")).await.unwrap();assert!(page.complete);assert_eq!(page.count.value,1);assert_eq!(page.hits[0].edge.record_id,"aa");
        assert_eq!(page.evaluation_time_ms,page.statistics.evaluation_time_ms);assert_eq!(page.statistics.active_records,2);
        let mut illegal=edge_write("edge-endpoint","bad","a","aa");assert!(matches!(p.write(illegal.clone()).await,Err(RecordError::Rejected(_))));
        illegal.system.as_mut().unwrap().relation.as_mut().unwrap().weight=Some(f64::INFINITY);assert!(p.write(illegal).await.is_err());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn feed_order_retry_retention_and_restore_scope() {
        let root=path(); let copy=path(); let p=RecordPartition::create(&root,config()).await.unwrap();
        let (clock,now)=test_clock(&p).await;
        let first=write("feed-first","a","alpha",ExpectedVersion::Absent);
        p.write(first.clone()).await.unwrap();
        let page=p.changes(None,1).await.unwrap(); assert_eq!(page.events.len(),1);
        assert_eq!(page.events[0].kind,ChangeKind::RecordChanged);
        p.write(first.clone()).await.unwrap(); assert!(p.changes(Some(page.next.clone()),1).await.unwrap().events.is_empty());
        p.write(write("feed-second","b","beta",ExpectedVersion::Absent)).await.unwrap();
        let all=p.changes(None,1).await.unwrap(); assert_eq!(all.events[0].seq,1);
        let second=p.changes(Some(all.next),1).await.unwrap(); assert_eq!(second.events[0].seq,2);
        p.checkpoint(&copy).await.unwrap();
        let restored=RecordPartition::open(&copy,config().max_journal_bytes).await.unwrap();
        assert!(matches!(restored.changes(Some(second.next.clone()),1).await,Err(RecordError::CursorExpired(_))));
        drop(restored);
        clock.set(now+7*86_400_000+1);
        assert!(matches!(p.changes(None,10).await,Err(RecordError::CursorExpired(2))));
        assert!(p.changes(Some(second.next.clone()),10).await.unwrap().events.is_empty());
        assert_eq!(p.write(first).await.unwrap().seq,1);
        p.compact_journal().await.unwrap();drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        p.inner.lock().await.clock=clock;
        assert!(matches!(p.changes(None,10).await,Err(RecordError::CursorExpired(2))));
        drop(p);std::fs::remove_dir_all(root).unwrap();std::fs::remove_dir_all(copy).unwrap();
    }
    #[tokio::test]
    async fn feed_backpressure_preserves_reserved_source_deletion() {
        let root=path();let mut cfg=config(); cfg.change_feed.max_events=3;
        let p=RecordPartition::create(&root,cfg).await.unwrap();
        p.write(source_write("feed-source","s",&[])).await.unwrap();
        p.write(write("feed-normal","n","alpha",ExpectedVersion::Absent)).await.unwrap();
        assert!(matches!(p.write(write("feed-full","x","alpha",ExpectedVersion::Absent)).await,Err(RecordError::FeedFull)));
        let mut delete=source_write("feed-delete","s",&[]);delete.document=None;delete.system=None;delete.expected=ExpectedVersion::Match(1);
        p.write(delete).await.unwrap();
        let events=p.changes(None,10).await.unwrap().events;
        assert_eq!(events.len(),3);assert_eq!(events[2].kind,ChangeKind::SourceDeleted);assert!(events[2].rescan_required);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn feed_ttl_reserved_event_and_private_generation_commands() {
        let root=path();let mut cfg=config();cfg.change_feed.max_events=2;
        let p=RecordPartition::create(&root,cfg).await.unwrap();let(clock,now)=test_clock(&p).await;
        let mut request=write("feed-ttl","t","alpha",ExpectedVersion::Absent);
        request.temporal=Some(TemporalMetadata{expire_at_ms:Some(now+1),..Default::default()});
        p.write(request).await.unwrap();
        assert!(matches!(p.write(write("full","x","alpha",ExpectedVersion::Absent)).await,Err(RecordError::FeedFull)));
        clock.set(now+1);let events=p.changes(None,10).await.unwrap().events;
        assert_eq!(events.len(),2);assert_eq!(events[1].kind,ChangeKind::TtlExpired);assert!(events[1].record_id.is_none());
        assert_eq!(p.statistics().await.unwrap().active_records,0);drop(p);std::fs::remove_dir_all(root).unwrap();
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(source_write("feed-source","s",&[])).await.unwrap();
        p.write(generation_write("feed-stage",ExpectedVersion::Absent,stage_generation("g",&["c"]))).await.unwrap();
        let staged=p.changes(None,10).await.unwrap();assert_eq!(staged.events.len(),1);assert_eq!(staged.visible_seq,2);
        p.write(generation_write("feed-publish",ExpectedVersion::Match(1),GenerationCommand::Publish{generation_id:"g".into()})).await.unwrap();
        let page=p.changes(Some(staged.next),10).await.unwrap();assert_eq!(page.events.len(),1);assert_eq!(page.events[0].kind,ChangeKind::GenerationPublished);
        let before=serde_json::to_value(p.changes(None,10).await.unwrap().events).unwrap();
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(serde_json::to_value(p.changes(None,10).await.unwrap().events).unwrap(),before);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn feed_trim_checkpoint_budget_does_not_block_expiration() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        for i in 0..10 {p.write(write(&format!("trim-{i}"),&format!("r{i}"),"alpha",ExpectedVersion::Absent)).await.unwrap();}
        let mut request=write("trim-ttl","t","alpha",ExpectedVersion::Absent);
        request.temporal=Some(TemporalMetadata{expire_at_ms:Some(now+7*86_400_000),..Default::default()});p.write(request).await.unwrap();
        // Narrow the in-memory checkpoint budget to the exact pre-maintenance
        // state plus emergency reserve. WAL still has space: exercise the
        // checkpoint admission branch independently of physical WAL admission.
        {let mut inner=p.inner.lock().await;
            inner.config.max_journal_bytes=100_000;
            loop {
                let size=encode_entry(&Entry::Checkpoint{config:inner.config.clone(),state:inner.state.clone()}).unwrap().len()+generation::reserve_bytes(&inner.state);
                let mut minimum=size as u64;
                while PartitionJournal::validate_initial(size,minimum).is_err(){minimum+=1;}
                if inner.config.max_journal_bytes==minimum {break;}inner.config.max_journal_bytes=minimum;
            }
        }
        clock.set(now+7*86_400_000);p.advance_time().await.unwrap();
        let inner=p.inner.lock().await;assert!(inner.healthy);assert_eq!(inner.state.feed.floor,0);
        assert!(inner.state.records["t"].expired_at_ms.is_some());drop(inner);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn feed_bootstrap_release_and_lease_rotate_retry_identity() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        let id=format!("{}aaaaaaaaaaaaaaaa",p.bootstrap_identity().await.unwrap());
        p.bootstrap(id.clone(),None,1).await.unwrap();p.release_bootstrap(&id).await.unwrap();
        assert!(matches!(p.bootstrap(id,None,1).await,Err(RecordError::BootstrapExpired)));
        let id=format!("{}bbbbbbbbbbbbbbbb",p.bootstrap_identity().await.unwrap());
        p.bootstrap(id.clone(),None,1).await.unwrap();clock.set(now+300_000);
        assert!(matches!(p.bootstrap(id,None,1).await,Err(RecordError::BootstrapExpired)));
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn feed_bootstrap_cut_concurrent_write_hiding_and_restart() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(source_write("boot-source","s",&[])).await.unwrap();
        p.write(source_write("boot-derived","d",&[("s",1)])).await.unwrap();
        let id=format!("{}aaaaaaaaaaaaaaaa",p.bootstrap_identity().await.unwrap());
        let first=p.bootstrap(id.clone(),None,1).await.unwrap();assert!(first.next.is_some());
        p.write(write("boot-new","n","alpha",ExpectedVersion::Absent)).await.unwrap();
        let retry=p.bootstrap(id.clone(),None,1).await.unwrap();assert_eq!(retry.visible_seq,first.visible_seq);
        assert_eq!(retry.records[0].record_id,first.records[0].record_id);
        let next=p.bootstrap(id.clone(),first.next.clone(),1).await.unwrap();assert_eq!(next.visible_seq,2);assert!(next.next.is_none());
        let tail=p.changes(Some(first.continuation.clone()),10).await.unwrap();assert_eq!(tail.events.len(),1);assert_eq!(tail.events[0].record_id.as_deref(),Some("n"));
        let mut delete=source_write("boot-delete","s",&[]);delete.document=None;delete.system=None;delete.expected=ExpectedVersion::Match(1);
        p.write(delete).await.unwrap();
        assert!(matches!(p.bootstrap(id.clone(),first.next.clone(),1).await,Err(RecordError::BootstrapExpired)));
        assert!(matches!(p.bootstrap(id.clone(),None,1).await,Err(RecordError::BootstrapExpired)));
        let fresh=format!("{}bbbbbbbbbbbbbbbb",p.bootstrap_identity().await.unwrap());
        let current=p.bootstrap(fresh,None,1).await.unwrap();assert_eq!(current.records.len(),1);assert_eq!(current.records[0].record_id,"n");
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert!(matches!(p.bootstrap(id,first.next,1).await,Err(RecordError::BootstrapExpired)));
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn ttl_boundary_cascade_retry_and_irreversible_recovery() {
        let root=path(); let p=RecordPartition::create(&root,config()).await.unwrap();
        let (clock,now)=test_clock(&p).await;
        let mut source=source_write("ttl-source","s",&[]);
        source.temporal=Some(TemporalMetadata { expire_at_ms:Some(now+100),..Default::default() });
        let receipt=p.write(source.clone()).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        p.write(source_write("ttl-derived","d",&[("s",1)])).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        p.write(source_write("ttl-indirect","e",&[("d",1)])).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        clock.set(now+99); assert_eq!(p.statistics().await.unwrap().active_records,3);
        clock.set(now+100); assert_eq!(p.statistics().await.unwrap().active_records,0);
        p.inner.lock().await.state.records.validate_counts().unwrap();
        for id in ["s","d","e"] { assert!(p.get(id).await.unwrap().is_none()); }
        let state=p.inner.lock().await; assert_eq!(state.state.records["s"].version,2);
        assert_eq!(state.state.records["s"].expired_at_ms,Some(now+100));
        assert_eq!(state.state.expirations.len(),1); drop(state);
        assert_eq!(p.write(source).await.unwrap(),receipt);
        p.inner.lock().await.state.records.validate_counts().unwrap();
        let mut revive=source_write("ttl-revive","s",&[]); revive.expected=ExpectedVersion::Match(2);
        revive.temporal=Some(TemporalMetadata {expire_at_ms:Some(now+10000),..Default::default()});
        assert!(p.write(revive).await.is_err());
        p.inner.lock().await.state.records.validate_counts().unwrap();
        clock.set(now); assert!(p.get("s").await.unwrap().is_none());
        p.compact_journal().await.unwrap(); drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        assert_eq!(p.statistics().await.unwrap().active_records,0);
        assert_eq!(p.record_status("s").await.unwrap().unwrap().version,2);
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn ttl_durable_pending_replay_and_clock_failure() {
        let root=path(); let p=RecordPartition::create(&root,config()).await.unwrap();
        let (clock,now)=test_clock(&p).await;
        let mut request=write("ttl-plain","plain","alpha",ExpectedVersion::Absent);
        request.temporal=Some(TemporalMetadata {expire_at_ms:Some(now+100),..Default::default()});
        p.write(request).await.unwrap(); clock.set(now+100);
        p.inner.lock().await.fail_after_durable=true;
        assert!(matches!(p.get("plain").await,Err(RecordError::DurablePending(2))));
        assert!(matches!(p.get("plain").await,Err(RecordError::RecoveryRequired)));
        drop(p); let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        let (clock,_)=test_clock(&p).await;
        assert!(p.get("plain").await.unwrap().is_none());
        assert_eq!(p.inner.lock().await.state.records["plain"].version,2);
        clock.set(now-300001); assert!(matches!(p.get("plain").await,Err(RecordError::ClockUncertain)));
        clock.set(now+101); assert!(p.get("plain").await.unwrap().is_none());
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn ttl_backup_cut_is_immutable_and_long_identity_rejected() {
        let root=path(); let archive=path(); let copy=path();
        let p=RecordPartition::create(&root,config()).await.unwrap();
        let (clock,now)=test_clock(&p).await;
        let mut long=write("ttl-long",&"\"".repeat(257),"alpha",ExpectedVersion::Absent);
        long.temporal=Some(TemporalMetadata {expire_at_ms:Some(now+100),..Default::default()});
        assert!(matches!(p.write(long).await,Err(RecordError::Rejected(_))));
        let mut request=write("ttl-backup","plain","alpha",ExpectedVersion::Absent);
        request.temporal=Some(TemporalMetadata {expire_at_ms:Some(now+100),..Default::default()});
        p.write(request).await.unwrap(); p.checkpoint(&archive).await.unwrap();
        let a=RecordPartition::open(&archive,config().max_journal_bytes).await.unwrap();
        let (archive_clock,_)=test_clock(&a).await; archive_clock.set(now+101);
        let before=a.recovery_identity().await.unwrap();
        assert!(a.get("plain").await.unwrap().is_none());
        assert!(a.statistics().await.unwrap().evidence_quarantined);
        a.checkpoint(&copy).await.unwrap();
        assert_eq!(a.recovery_identity().await.unwrap(),before);
        assert_eq!(a.inner.lock().await.state.seq,1);
        clock.set(now+100); assert!(p.get("plain").await.unwrap().is_none());
        drop(a); drop(p);
        for path in [root,archive,copy] { std::fs::remove_dir_all(path).unwrap(); }
    }
    #[tokio::test]
    async fn ttl_generation_cannot_bypass_independent_owner_gate() {
        let root=path(); let archive=path();
        let p=RecordPartition::create(&root,config()).await.unwrap(); p.checkpoint(&archive).await.unwrap();
        let a=RecordPartition::open(&archive,config().max_journal_bytes).await.unwrap();
        let (_,now)=test_clock(&a).await;
        a.write(source_write("new-source","s",&[])).await.unwrap();
        let mut stage=stage_generation("g",&["chunk"]);
        if let GenerationCommand::Stage {chunks,..}=&mut stage { chunks[0].temporal=Some(TemporalMetadata {expire_at_ms:Some(now+100),..Default::default()}); }
        assert!(matches!(a.write(generation_write("stage-ttl",ExpectedVersion::Absent,stage)).await,Err(RecordError::Rejected(_))));
        assert!(a.get("chunk").await.unwrap().is_none());
        drop(a); drop(p); for path in [root,archive] {std::fs::remove_dir_all(path).unwrap();}
    }
    #[tokio::test]
    async fn temporal_filters_apply_before_topk_in_all_channels_and_recovery() {
        let root=path(); let p=RecordPartition::create(&root,config()).await.unwrap();
        let (clock,now)=test_clock(&p).await;
        for (id,event,from,to) in [("a",Some(50),Some(0),Some(100)),("b",Some(100),Some(100),Some(200)),("missing",None,None,None)] {
            let mut request=write(id,id,"alpha",ExpectedVersion::Absent);
            request.temporal=Some(TemporalMetadata {event_time_ms:event,valid_from_ms:from,valid_to_ms:to,expire_at_ms:None});
            p.write(request).await.unwrap();
        }
        clock.set(now+1);
        for mode in [SearchMode::Lexical,SearchMode::Vector { similarity_threshold:None,ann_mode:AnnMode::All },SearchMode::Hybrid {similarity_threshold:None,ann_mode:AnnMode::All}] {
            let result=p.query(RecordQuery {temporal:Some(TemporalFilter {event:Some(TimeRange {from_ms:Some(100),to_ms:Some(101)}),ingested:Some(TimeRange {from_ms:Some(now as i64),to_ms:Some(now as i64+1)}),valid_at_ms:Some(100)}),text:"alpha".into(),vector:if mode==SearchMode::Lexical {None}else{Some(Embedding::F32(vec![1.0,0.0]))},mode,query_type:QueryType::Intersection,offset:0,length:1,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![]}).await.unwrap();
            assert_eq!(result.hits.len(),1); assert_eq!(result.hits[0].record.record_id,"b");
            assert_eq!(result.evaluation_time_ms,now+1);assert_eq!(result.statistics.evaluation_time_ms,now+1);
            assert!(!result.hits[0].record.document.as_ref().unwrap().keys().any(|f|temporal::reserved(f)));
        }
        p.compact_journal().await.unwrap();drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        let result=p.query(RecordQuery {temporal:Some(TemporalFilter {event:Some(TimeRange::default()),..Default::default()}),text:"alpha".into(),vector:None,mode:SearchMode::Lexical,query_type:QueryType::Intersection,offset:0,length:0,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![]}).await.unwrap();
        assert_eq!(result.count_total,2); assert!(result.hits.is_empty());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn ttl_generation_deadline_blocks_publish_and_recovery_rejects_lost_cause() {
        let root=path();let bad=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        p.write(source_write("source","s",&[])).await.unwrap();
        let mut stage=stage_generation("g",&["chunk"]);
        if let GenerationCommand::Stage {chunks,..}=&mut stage {chunks[0].temporal=Some(TemporalMetadata {expire_at_ms:Some(now+100),..Default::default()});}
        p.write(generation_write("stage",ExpectedVersion::Absent,stage)).await.unwrap();
        clock.set(now+100);
        assert!(matches!(p.write(generation_write("publish",ExpectedVersion::Match(1),GenerationCommand::Publish{generation_id:"g".into()})).await,Err(RecordError::Rejected(_))));
        assert!(p.get("chunk").await.unwrap().is_none());
        let mut request=write("expire","plain","alpha",ExpectedVersion::Absent);request.temporal=Some(TemporalMetadata {expire_at_ms:Some(now+101),..Default::default()});p.write(request).await.unwrap();
        clock.set(now+101);p.advance_time().await.unwrap();
        let mut state=p.inner.lock().await.state.clone();
        let mut duplicate=serde_json::to_value(&state).unwrap();duplicate["expirations"][0]["seq"]=serde_json::json!(1);
        let duplicate:State=serde_json::from_value(duplicate).unwrap();assert!(temporal::validate_state(&duplicate).is_err());
        state.expirations.clear();
        let bytes=encode_entry(&Entry::Checkpoint{config:config(),state}).unwrap();
        drop(PartitionJournal::create(&bad,config().max_journal_bytes,&bytes).unwrap());
        assert!(RecordPartition::open(&bad,config().max_journal_bytes).await.is_err());
        drop(p);for path in [root,bad] {std::fs::remove_dir_all(path).unwrap();}
    }
    #[tokio::test]
    async fn ttl_extension_before_deadline_and_competing_late_updates() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();let(clock,now)=test_clock(&p).await;
        let mut source=source_write("s","s",&[]);source.document.as_mut().unwrap().shift_remove("vector");
        source.temporal=Some(TemporalMetadata {expire_at_ms:Some(now+100),..Default::default()});p.write(source.clone()).await.unwrap();
        p.write(source_write("derived","derived",&[("s",1)])).await.unwrap();
        clock.set(now+99);source.operation_id=OperationId::new("extend".into());source.expected=ExpectedVersion::Match(1);
        source.temporal.as_mut().unwrap().expire_at_ms=Some(now+200);p.write(source.clone()).await.unwrap();
        clock.set(now+100);assert!(p.get("s").await.unwrap().is_some());assert!(p.get("derived").await.unwrap().is_none());
        let old_view=p.output_permit().unwrap();clock.set(now+200);
        let mut patch=source.clone();patch.operation_id=OperationId::new("late-vector".into());patch.expected=ExpectedVersion::Match(2);patch.document=None;patch.system=None;patch.temporal=None;patch.vector_patch=Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        source.operation_id=OperationId::new("late-extend".into());source.expected=ExpectedVersion::Match(2);source.temporal.as_mut().unwrap().expire_at_ms=Some(now+300);
        let(a,b)=tokio::join!(p.write(source),p.write(patch));assert!(a.is_err()&&b.is_err());
        assert_eq!(p.record_status("s").await.unwrap().unwrap().version,3);assert_eq!(p.statistics().await.unwrap().active_records,0);
        assert!(old_view.deliver(||()).is_ok()); // already fixed-time output, not a fresh read
        assert!(p.get("s").await.unwrap().is_none());drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn generation_stage_publish_switch_and_enrich() {
        let root = path(); let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("source", "s", &[])).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        let stage = generation_write("stage", ExpectedVersion::Absent, stage_generation("g1", &["a","b"]));
        let receipt = p.write(stage.clone()).await.unwrap(); assert!(receipt.staged_only); assert_eq!(receipt.version,1);
        p.inner.lock().await.state.records.validate_counts().unwrap();
        assert_eq!(p.write(stage).await.unwrap(), receipt);
        p.inner.lock().await.state.records.validate_counts().unwrap();
        assert!(p.get("a").await.unwrap().is_none()); assert_eq!(p.statistics().await.unwrap().active_records,1);
        assert!(p.write(write("steal", "a", "body", ExpectedVersion::Absent)).await.is_err());
        p.inner.lock().await.state.records.validate_counts().unwrap();
        p.compact_journal().await.unwrap(); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        assert_eq!(p.generation_status("doc").await.unwrap().unwrap().staged_members,2);
        assert!(p.get("a").await.unwrap().is_none());
        let publish = generation_write("publish", ExpectedVersion::Match(1), GenerationCommand::Publish { generation_id:"g1".into() });
        let mut competing = publish.clone(); competing.operation_id = OperationId::new("competing".into());
        let (a,b) = tokio::join!(p.write(publish),p.write(competing)); assert_ne!(a.is_ok(), b.is_ok());
        p.inner.lock().await.state.records.validate_counts().unwrap();
        assert_eq!(p.generation_status("doc").await.unwrap().unwrap().visible_members,2);
        assert_eq!(p.statistics().await.unwrap().active_records,3);
        assert_eq!(query(&p, SearchMode::Lexical).await.hits.len(),2);
        assert_eq!(query(&p, SearchMode::Vector { similarity_threshold:None,ann_mode:AnnMode::All }).await.hits.len(),1);
        let mut patch = write("fill-a", "a", "", ExpectedVersion::Match(1)); patch.document = None;
        patch.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        p.write(patch.clone()).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        p.write(source_write("downstream", "downstream", &[("a",1)])).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        p.write(generation_write("stage2", ExpectedVersion::Match(2), stage_generation("g2", &["c","d"]))).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        let old_output = p.output_permit().unwrap();
        p.write(generation_write("publish2", ExpectedVersion::Match(3), GenerationCommand::Publish { generation_id:"g2".into() })).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        assert!(old_output.deliver(|| ()).is_err());
        for id in ["a","b","downstream"] { assert!(p.get(id).await.unwrap().is_none()); assert_eq!(p.record_status(id).await.unwrap().unwrap().visibility,EvidenceVisibility::Stale); }
        assert_eq!(p.statistics().await.unwrap().active_records,3);
        assert_eq!(query(&p, SearchMode::Lexical).await.hits.iter().map(|h| h.record.record_id.as_str()).collect::<Vec<_>>(), vec!["c","d"]);
        patch.operation_id = OperationId::new("late-fill".into()); assert!(p.write(patch).await.is_err());
        p.inner.lock().await.state.records.validate_counts().unwrap();
        p.compact_journal().await.unwrap(); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        p.inner.lock().await.state.records.validate_counts().unwrap();
        assert_eq!(p.generation_status("doc").await.unwrap().unwrap().active_generation.as_deref(),Some("g2"));
        assert!(p.get("a").await.unwrap().is_none()); assert!(p.get("c").await.unwrap().is_some());
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn generation_manifest_failure_abort_and_source_barrier() {
        let root = path(); let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("s", "s", &[])).await.unwrap();
        let mut incomplete = stage_generation("bad", &["a","b"]);
        if let GenerationCommand::Stage { chunks, .. } = &mut incomplete { chunks.pop(); }
        assert!(p.write(generation_write("bad", ExpectedVersion::Absent, incomplete)).await.is_err());
        assert!(p.generation_status("doc").await.unwrap().is_none());
        p.write(generation_write("stage", ExpectedVersion::Absent, stage_generation("g1", &["a","b"]))).await.unwrap();
        p.write(write("source-update", "s", "new source", ExpectedVersion::Match(1))).await.unwrap();
        assert!(p.write(generation_write("publish", ExpectedVersion::Match(1), GenerationCommand::Publish { generation_id:"g1".into() })).await.is_err());
        p.write(generation_write("abort", ExpectedVersion::Match(1), GenerationCommand::Abort { generation_id:"g1".into() })).await.unwrap();
        settle_maintenance(&p).await;
        assert_eq!(p.generation_status("doc").await.unwrap().unwrap().staged_members,0);
        assert!(p.write(generation_write("reuse", ExpectedVersion::Match(2), stage_generation("g1", &["a","b"]))).await.is_err());
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn generation_rejects_replacement_that_retires_its_evidence() {
        for indirect in [false,true] {
            let root = path(); let p = RecordPartition::create(&root, config()).await.unwrap();
            test_clock(&p).await;
            p.write(source_write("s", "s", &[])).await.unwrap();
            p.write(generation_write("stage1", ExpectedVersion::Absent, stage_generation("g1", &["old"]))).await.unwrap();
            p.write(generation_write("publish1", ExpectedVersion::Match(1), GenerationCommand::Publish { generation_id:"g1".into() })).await.unwrap();
            let reference = if indirect { p.write(source_write("dependent", "dependent", &[("old",1)])).await.unwrap(); "dependent" } else { "old" };
            let mut next = stage_generation("g2", &["new"]);
            if let GenerationCommand::Stage { chunks, .. } = &mut next {
                chunks[0].evidence_refs.push(EvidenceRef { locator:None, record_id:reference.into(), routing_key:"document-1".into(), version:1 });
            }
            p.write(generation_write("stage2", ExpectedVersion::Match(2), next)).await.unwrap();
            let before = p.statistics().await.unwrap(); let permit = p.output_permit().unwrap();
            assert!(matches!(p.write(generation_write("publish2", ExpectedVersion::Match(3), GenerationCommand::Publish { generation_id:"g2".into() })).await, Err(RecordError::Conflict(_))));
            assert_eq!(before,p.statistics().await.unwrap()); assert!(permit.deliver(|| ()).is_ok());
            assert_eq!(p.generation_status("doc").await.unwrap().unwrap().active_generation.as_deref(),Some("g1"));
            assert!(p.get("old").await.unwrap().is_some()); assert!(p.get("new").await.unwrap().is_none());
            drop(p); std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[tokio::test]
    async fn generation_reserves_source_delete_and_abort_under_pressure() {
        let root=path(); let mut settings=config(); settings.max_records=4; settings.max_operations=6;
        let p=RecordPartition::create(&root,settings).await.unwrap(); p.write(source_write("s","s",&[])).await.unwrap();
        p.write(generation_write("stage",ExpectedVersion::Absent,stage_generation("g1",&["a","b"]))).await.unwrap();
        assert!(p.write(write("pressure","other","alpha",ExpectedVersion::Absent)).await.is_err());
        let mut delete=write("delete","s","",ExpectedVersion::Match(1)); delete.document=None; p.write(delete).await.unwrap();
        assert!(p.write(generation_write("publish",ExpectedVersion::Match(1),GenerationCommand::Publish { generation_id:"g1".into() })).await.is_err());
        p.write(generation_write("abort",ExpectedVersion::Match(1),GenerationCommand::Abort { generation_id:"g1".into() })).await.unwrap();
        settle_maintenance(&p).await;
        assert_eq!(p.statistics().await.unwrap().active_records,0);
        drop(p); let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.generation_status("doc").await.unwrap().unwrap().staged_members,0);
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn generation_cancelled_call_still_commits_owned_stage_and_publish() {
        let root=path(); let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(source_write("s","s",&[])).await.unwrap();
        for command in [generation_write("stage",ExpectedVersion::Absent,stage_generation("g1",&["a","b"])),
            generation_write("publish",ExpectedVersion::Match(1),GenerationCommand::Publish { generation_id:"g1".into() })] {
            let available=p.admission.available_permits(); let guard=p.inner.lock().await;
            let owned=p.clone(); let request=command.clone();
            let task=tokio::spawn(async move { owned.write(request).await });
            tokio::time::timeout(std::time::Duration::from_secs(5),async {
                while p.admission.available_permits()==available { tokio::task::yield_now().await; }
            }).await.unwrap();
            task.abort(); assert!(task.await.unwrap_err().is_cancelled()); drop(guard);
            let receipt=tokio::time::timeout(std::time::Duration::from_secs(5),async {
                loop { if let Some(r)=p.operation(&command.operation_id).await.unwrap() { break r; } tokio::task::yield_now().await; }
            }).await.unwrap();
            assert_eq!(p.write(command).await.unwrap(),receipt);
            assert_eq!(p.generation_status("doc").await.unwrap().unwrap().visible_members,if receipt.staged_only {0} else {2});
        }
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn generation_partial_publication_fences_and_recovers_whole_set() {
        let root = path(); let archive = path(); let target = path();
        let p = RecordPartition::create(&root, config()).await.unwrap(); p.write(source_write("s", "s", &[])).await.unwrap();
        p.write(generation_write("stage", ExpectedVersion::Absent, stage_generation("g1", &["a","b"]))).await.unwrap();
        p.checkpoint(&archive).await.unwrap();
        let backup = RecordPartition::open(&archive, config().max_journal_bytes).await.unwrap();
        assert!(backup.write(generation_write("unsafe", ExpectedVersion::Match(1), GenerationCommand::Publish { generation_id:"g1".into() })).await.is_err());
        let publish = generation_write("publish", ExpectedVersion::Match(1), GenerationCommand::Publish { generation_id:"g1".into() });
        p.inner.lock().await.fail_publication_stage = 1;
        assert!(matches!(p.write(publish.clone()).await, Err(RecordError::DurablePending(_))));
        assert!(p.get("a").await.is_err()); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert!(!p.write(publish.clone()).await.unwrap().staged_only);
        assert!(p.get("a").await.unwrap().is_some()); assert!(p.get("b").await.unwrap().is_some());
        let binding = seal_request(backup.recovery_identity().await.unwrap().digest);
        let seal = p.seal_for_takeover(binding.clone()).await.unwrap(); p.create_takeover_successor(&backup,&target,binding).await.unwrap();
        let next = RecordPartition::open(&target, config().max_journal_bytes).await.unwrap(); next.certify_takeover_successor(&seal).await.unwrap();
        assert_eq!(next.generation_status("doc").await.unwrap().unwrap().visible_members,2);
        assert_eq!(next.write(publish).await.unwrap().version,2);
        let mut delete = write("delete-source", "s", "", ExpectedVersion::Match(1)); delete.document=None; next.write(delete).await.unwrap();
        assert_eq!(next.generation_status("doc").await.unwrap().unwrap().visible_members,0);
        drop(next); drop(p); drop(backup); for path in [root, archive, target] { std::fs::remove_dir_all(path).unwrap(); }
    }
    #[tokio::test]
    async fn optional_vectors_enrich_exact_revision_and_recover() {
        let root = path(); let archive = path();
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let mut source = source_write("s", "s", &[]);
        source.document.as_mut().unwrap().remove("vector");
        source.document.as_mut().unwrap().insert("body".into(), serde_json::json!("alpha original"));
        p.write(source).await.unwrap();
        let mut derived = source_write("d", "d", &[("s", 1)]);
        derived.document.as_mut().unwrap().remove("vector");
        derived.document.as_mut().unwrap().insert("body".into(), serde_json::json!("alpha derived"));
        p.write(derived).await.unwrap();
        let vector = SearchMode::Vector { similarity_threshold: None, ann_mode: AnnMode::All };
        assert!(query(&p, vector.clone()).await.hits.is_empty());
        assert_eq!(query(&p, SearchMode::Lexical).await.hits.len(), 2);
        assert_eq!(query(&p, SearchMode::Hybrid { similarity_threshold: None, ann_mode: AnnMode::All }).await.hits.len(), 2);
        assert_eq!(p.statistics().await.unwrap().active_records, 2);
        let mut patch = write("enrich", "s", "unused", ExpectedVersion::Match(1));
        patch.document = None;
        patch.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        let receipt = p.write(patch.clone()).await.unwrap();
        assert_eq!(receipt.version, 1);
        assert_eq!(p.write(patch.clone()).await.unwrap(), receipt);
        assert_eq!(p.statistics().await.unwrap().retained_source_revisions, 0);
        assert!(p.get("d").await.unwrap().is_some());
        assert_eq!(query(&p, vector.clone()).await.hits.len(), 1);
        let mut collision = patch.clone(); collision.operation_id = OperationId::new("collision".into());
        assert!(matches!(p.write(collision).await, Err(RecordError::Conflict(_))));
        p.compact_journal().await.unwrap(); p.checkpoint(&archive).await.unwrap(); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert_eq!(p.write(patch).await.unwrap(), receipt);
        assert_eq!(query(&p, vector).await.hits.len(), 1);
        let mut update = write("newsource", "s", "alpha new", ExpectedVersion::Match(1));
        update.document.as_mut().unwrap().remove("vector"); p.write(update).await.unwrap();
        let mut late = write("late", "d", "unused", ExpectedVersion::Match(1)); late.document = None;
        late.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        assert!(p.write(late).await.is_err());
        assert_eq!(p.record_status("d").await.unwrap().unwrap().visibility, EvidenceVisibility::Stale);
        drop(p); std::fs::remove_dir_all(root).unwrap(); std::fs::remove_dir_all(archive).unwrap();
    }
    #[tokio::test]
    async fn optional_vectors_pending_recovery_and_race() {
        let root = path(); let p = RecordPartition::create(&root, config()).await.unwrap();
        let mut initial = write("body", "r", "alpha", ExpectedVersion::Absent);
        initial.document.as_mut().unwrap().remove("vector"); p.write(initial).await.unwrap();
        let mut patch = write("patch", "r", "", ExpectedVersion::Match(1)); patch.document = None;
        patch.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        let mut bad = patch.clone(); bad.vector_patch.as_mut().unwrap().insert("body".into(), serde_json::json!("changed"));
        assert!(p.write(bad).await.is_err());
        let mut bad = patch.clone(); bad.expected = ExpectedVersion::Any; assert!(p.write(bad).await.is_err());
        p.inner.lock().await.fail_after_durable = true;
        assert!(matches!(p.write(patch.clone()).await, Err(RecordError::DurablePending(2)))); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert_eq!(p.write(patch).await.unwrap().version, 1);
        assert_eq!(p.get("r").await.unwrap().unwrap().document.unwrap()["body"], "alpha");
        let mut update = write("revision", "r", "alpha", ExpectedVersion::Match(1)); update.document.as_mut().unwrap().remove("vector"); p.write(update).await.unwrap();
        let mut left = write("left", "r", "", ExpectedVersion::Match(2)); left.document = None;
        left.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        let mut right = left.clone(); right.operation_id = OperationId::new("right".into());
        let (a,b) = tokio::join!(p.write(left),p.write(right)); assert_ne!(a.is_ok(), b.is_ok());
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn record_source_revision_stale_recompute_delete_and_recovery() {
        let root = path(); let archive = path(); let target = path();
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("s1", "s", &[])).await.unwrap();
        p.write(source_write("d1", "d", &[("s", 1)])).await.unwrap();
        p.write(source_write("i1", "i", &[("d", 1)])).await.unwrap();
        p.checkpoint(&archive).await.unwrap();
        let old_output = p.output_permit().unwrap();
        let update = write("s2", "s", "alpha current", ExpectedVersion::Match(1));
        let receipt = p.write(update.clone()).await.unwrap();
        assert_eq!(receipt.version, 2);
        assert_eq!(p.write(update.clone()).await.unwrap(), receipt);
        assert!(old_output.deliver(|| ()).is_err());
        for id in ["d", "i"] {
            assert!(p.get(id).await.unwrap().is_none());
            assert_eq!(p.record_status(id).await.unwrap().unwrap().visibility, EvidenceVisibility::Stale);
        }
        assert_eq!(p.get("s").await.unwrap().unwrap().version, 2);
        assert_eq!(p.statistics().await.unwrap().active_records, 1);
        assert_eq!(p.statistics().await.unwrap().retained_source_revisions, 1);
        for mode in [SearchMode::Lexical, SearchMode::Vector { similarity_threshold: None, ann_mode: AnnMode::All },
            SearchMode::Hybrid { similarity_threshold: None, ann_mode: AnnMode::All }] {
            let found = query(&p, mode).await; assert_eq!(found.hits.len(), 1); assert_eq!(found.hits[0].record.record_id, "s");
        }
        assert!(p.write(source_write("old-ref", "bad", &[("s", 1)])).await.is_err());
        p.write(source_write("recomputed", "fresh", &[("s", 2)])).await.unwrap();
        assert_eq!(p.statistics().await.unwrap().active_records, 2);
        p.compact_journal().await.unwrap(); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert_eq!(p.record_status("i").await.unwrap().unwrap().visibility, EvidenceVisibility::Stale);
        assert_eq!(p.write(update).await.unwrap(), receipt);
        assert_eq!(p.inner.lock().await.state.source_revisions[0].document.as_ref().unwrap()["body"], "evidence original");
        let backup = RecordPartition::open(&archive, config().max_journal_bytes).await.unwrap();
        let binding = seal_request(backup.recovery_identity().await.unwrap().digest);
        let seal = p.seal_for_takeover(binding.clone()).await.unwrap();
        p.create_takeover_successor(&backup, &target, binding).await.unwrap();
        let next = RecordPartition::open(&target, config().max_journal_bytes).await.unwrap();
        next.certify_takeover_successor(&seal).await.unwrap();
        assert_eq!(next.record_status("d").await.unwrap().unwrap().visibility, EvidenceVisibility::Stale);
        let mut deletion = write("delete-latest", "s", "unused", ExpectedVersion::Match(2)); deletion.document = None;
        next.write(deletion).await.unwrap();
        for id in ["s", "d", "i", "fresh"] {
            assert_eq!(next.record_status(id).await.unwrap().unwrap().visibility, EvidenceVisibility::Deleted);
            assert!(next.get(id).await.unwrap().is_none());
        }
        assert_eq!(next.statistics().await.unwrap().retained_source_revisions, 2);
        drop(next); drop(p); drop(backup);
        for path in [root, archive, target] { std::fs::remove_dir_all(path).unwrap(); }
    }
    #[tokio::test]
    async fn record_source_revision_budget_reserves_delete_and_conflicting_update() {
        let root = path(); let mut settings = config(); settings.max_records = 2;
        let p = RecordPartition::create(&root, settings).await.unwrap();
        p.write(source_write("s", "s", &[])).await.unwrap();
        let first = write("left", "s", "alpha left", ExpectedVersion::Match(1));
        let second = write("right", "s", "alpha right", ExpectedVersion::Match(1));
        let (a,b) = tokio::join!(p.write(first),p.write(second)); assert_ne!(a.is_ok(), b.is_ok());
        assert!(p.write(write("full", "s", "alpha full", ExpectedVersion::Match(2))).await.is_err());
        let mut delete = write("delete", "s", "unused", ExpectedVersion::Match(2)); delete.document = None;
        p.write(delete).await.unwrap();
        assert_eq!(p.statistics().await.unwrap().retained_source_revisions, 2);
        drop(p); let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert!(p.get("s").await.unwrap().is_none()); drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn record_source_revision_durable_pending_recovers_stale_once() {
        let root = path(); let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("s", "s", &[])).await.unwrap();
        p.write(source_write("d", "d", &[("s", 1)])).await.unwrap();
        let permit = p.output_permit().unwrap(); p.inner.lock().await.fail_after_durable = true;
        let update = write("update", "s", "alpha new", ExpectedVersion::Match(1));
        assert!(matches!(p.write(update.clone()).await, Err(RecordError::DurablePending(3))));
        assert!(permit.deliver(|| ()).is_err()); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert_eq!(p.write(update).await.unwrap().version, 2);
        assert_eq!(p.record_status("d").await.unwrap().unwrap().visibility, EvidenceVisibility::Stale);
        assert_eq!(p.statistics().await.unwrap().retained_source_revisions, 1);
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    fn seal_request(digest: String) -> SealRequest {
        SealRequest { intent_id: "a".repeat(64), target_collection_id: "next".into(), backup_digest: digest }
    }
    #[tokio::test]
    async fn record_takeover_seal_successor_preserves_final_state_and_rejects_archives() {
        let root = path(); let backup_path = path(); let target = path(); let copied = path();
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("s", "source", &[])).await.unwrap();
        p.write(source_write("d", "derived", &[("source", 1)])).await.unwrap();
        p.checkpoint(&backup_path).await.unwrap();
        let backup = RecordPartition::open(&backup_path, config().max_journal_bytes).await.unwrap();
        let binding = seal_request(backup.recovery_identity().await.unwrap().digest);
        assert!(matches!(backup.seal_for_takeover(binding.clone()).await, Err(RecordError::Rejected(_))));
        let latest = write("latest", "ordinary", "alpha final", ExpectedVersion::Absent);
        let receipt = p.write(latest.clone()).await.unwrap();
        let mut deletion = write("del", "source", "unused", ExpectedVersion::Match(1)); deletion.document = None;
        p.write(deletion).await.unwrap();
        let old_feed = p.changes(None,100).await.unwrap();
        let old_output = p.output_permit().unwrap();
        let old_owner = p.clone();
        let seal = p.seal_for_takeover(binding.clone()).await.unwrap();
        assert_eq!(seal.final_seq, 4);
        assert!(old_output.deliver(|| ()).is_err());
        assert!(old_owner.write(latest.clone()).await.is_err());
        assert!(p.get("ordinary").await.is_err());
        assert!(p.statistics().await.is_err());
        assert_eq!(p.seal_for_takeover(binding.clone()).await.unwrap(), seal);
        let mut other = binding.clone(); other.target_collection_id = "different".into();
        assert!(matches!(p.seal_for_takeover(other).await, Err(RecordError::Conflict(_))));
        p.compact_journal().await.unwrap();
        p.checkpoint(&copied).await.unwrap();
        drop(old_owner); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert!(p.output_permit().is_err());
        assert_eq!(p.seal_for_takeover(binding.clone()).await.unwrap(), seal);
        p.create_takeover_successor(&backup, &target, binding.clone()).await.unwrap();
        let next = RecordPartition::open(&target, config().max_journal_bytes).await.unwrap();
        next.certify_takeover_successor(&seal).await.unwrap();
        assert!(matches!(next.changes(Some(old_feed.next),100).await,Err(RecordError::CursorExpired(_))));
        let next_feed=next.changes(None,100).await.unwrap();
        assert_eq!(next_feed.events.len(),old_feed.events.len());
        assert_ne!(next_feed.events[0].scope,old_feed.events[0].scope);
        assert_eq!(next_feed.events[0].partition_id,old_feed.events[0].partition_id);
        assert_eq!(next.write(latest).await.unwrap(), receipt);
        assert!(next.get("derived").await.unwrap().is_none());
        assert_eq!(query(&next, SearchMode::Lexical).await.hits.len(), 1);
        assert_eq!(next.statistics().await.unwrap().active_records, 1);
        let copy = RecordPartition::open(&copied, config().max_journal_bytes).await.unwrap();
        assert!(copy.output_permit().is_err());
        assert!(copy.seal_for_takeover(binding).await.is_err());
        drop(copy); drop(next); drop(backup); drop(p);
        for root in [root, backup_path, target, copied] { std::fs::remove_dir_all(root).unwrap(); }
    }
    #[tokio::test]
    async fn record_takeover_empty_archive_cannot_gain_current_authority() {
        let root = path(); let copy_path = path();
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.checkpoint(&copy_path).await.unwrap();
        let copy = RecordPartition::open(&copy_path, config().max_journal_bytes).await.unwrap();
        assert!(!copy.statistics().await.unwrap().evidence_quarantined);
        copy.write(source_write("new", "source", &[])).await.unwrap();
        assert!(copy.seal_for_takeover(seal_request("b".repeat(64))).await.is_err());
        drop(copy); drop(p);
        std::fs::remove_dir_all(root).unwrap(); std::fs::remove_dir_all(copy_path).unwrap();
    }
    #[tokio::test]
    async fn record_takeover_durable_failure_and_cancelled_waiter_remain_sealed() {
        for cancel in [false, true] {
            let root = path(); let p = RecordPartition::create(&root, config()).await.unwrap();
            let binding = seal_request("b".repeat(64));
            if cancel {
                let guard = p.inner.lock().await;
                let owner = p.clone(); let request = binding.clone();
                let task = tokio::spawn(async move { owner.seal_for_takeover(request).await });
                while p.admission.available_permits() == 16 { tokio::task::yield_now().await; }
                task.abort(); let _ = task.await; drop(guard);
                // The accepted owned task retains the permit until publication.
                while p.admission.available_permits() != 16 { tokio::task::yield_now().await; }
                assert!(p.output_permit().is_err());
            } else {
                p.inner.lock().await.fail_after_durable = true;
                assert!(matches!(p.seal_for_takeover(binding.clone()).await, Err(RecordError::DurablePending(0))));
                assert!(p.output_permit().is_err());
            }
            drop(p);
            let recovered = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
            assert_eq!(recovered.seal_for_takeover(binding).await.unwrap().final_seq, 0);
            assert!(recovered.write(write("late", "x", "alpha", ExpectedVersion::Absent)).await.is_err());
            drop(recovered); std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[tokio::test]
    async fn record_takeover_preflight_rejects_capacity_before_closing_owner() {
        let root = path(); let mut settings = config(); settings.max_journal_bytes = 8000;
        let p = RecordPartition::create(&root, settings).await.unwrap();
        for n in 0..100 {
            let request = write(&format!("pressure{n}"), "a", &"x".repeat(128), ExpectedVersion::Any);
            if p.write(request).await.is_err() { break; }
            p.compact_journal().await.unwrap();
        }
        let mut binding = seal_request("b".repeat(64)); binding.target_collection_id = "\u{1}".repeat(256);
        let identity = p.recovery_identity().await.unwrap();
        assert!(matches!(p.validate_takeover_capacity(&binding, &identity.lineage).await, Err(RecordError::Rejected(_))));
        assert!(p.get("a").await.unwrap().is_some());
        assert!(p.output_permit().unwrap().deliver(|| ()).is_ok());
        assert!(p.recovery_identity().await.unwrap().seal.is_none());
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }
    pub(super) fn source_write(nonce: &str, id: &str, dependencies: &[(&str, u64)]) -> RecordWrite {
        let mut request = write(nonce, id, "evidence original", ExpectedVersion::Absent);
        request.system = Some(EvidenceMetadata {
            kind: if dependencies.is_empty() { EvidenceKind::Source } else { EvidenceKind::Derived }, relation:None,
            evidence_refs: dependencies.iter().map(|(id, version)| EvidenceRef {
                locator:None, record_id: (*id).into(), routing_key: "document-1".into(), version: *version,
            }).collect(),
        });
        request
    }

    #[tokio::test]
    async fn source_delete_hides_transitive_content_statistics_outputs_and_recovers() {
        let root = path(); let backup = path();
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("source", "s", &[])).await.unwrap();
        p.write(source_write("direct", "d", &[("s", 1)])).await.unwrap();
        p.write(source_write("indirect", "i", &[("d", 1)])).await.unwrap();
        p.write(write("ordinary", "o", "evidence original", ExpectedVersion::Absent)).await.unwrap();
        let output = p.output_permit().unwrap();
        assert_eq!(p.statistics().await.unwrap().active_records, 4);
        assert_eq!(p.page(None, 10).await.unwrap().records.len(), 4);
        let mut delete = write("delete-source", "s", "", ExpectedVersion::Match(1));
        delete.document = None;
        let receipt = p.write(delete.clone()).await.unwrap();
        assert_eq!(p.write(delete.clone()).await.unwrap(), receipt);
        assert!(output.deliver(|| ()).is_err());
        for id in ["s", "d", "i"] { assert!(p.get(id).await.unwrap().is_none()); }
        let result = p.search("evidence".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap();
        assert_eq!(result.hits.iter().map(|h|h.record.record_id.as_str()).collect::<Vec<_>>(), vec!["o"]);
        assert_eq!(result.statistics.active_records, 1);
        for mode in [SearchMode::Vector { similarity_threshold: None, ann_mode: AnnMode::All },
                     SearchMode::Hybrid { similarity_threshold: None, ann_mode: AnnMode::All }] {
            let result = p.search("evidence".into(), Some(Embedding::F32(vec![1.0, 0.0])), mode, vec![], 10).await.unwrap();
            assert_eq!(result.hits.iter().map(|h| h.record.record_id.as_str()).collect::<Vec<_>>(), vec!["o"]);
            assert_eq!(result.statistics.active_records, 1);
        }

        assert_eq!(p.page(None, 10).await.unwrap().records.len(), 1);
        assert!(p.write(source_write("late", "late", &[("d", 1)])).await.is_err());
        let mut update = source_write("update", "d", &[("s", 1)]); update.expected = ExpectedVersion::Any;
        assert!(p.write(update).await.is_err());
        assert!(p.write(source_write("recreate", "s", &[])).await.is_err());
        p.compact_journal().await.unwrap();
        p.checkpoint(&backup).await.unwrap();
        let old_owner = p.output_permit().unwrap(); drop(p);
        assert!(old_owner.deliver(|| ()).is_err());
        for root in [&root, &backup] {
            let restored = RecordPartition::open(root, config().max_journal_bytes).await.unwrap();
            assert_eq!(restored.statistics().await.unwrap().active_records, 1);
            assert!(restored.get("i").await.unwrap().is_none());
            assert_eq!(restored.operation(&delete.operation_id).await.unwrap(), Some(receipt.clone()));
            assert_eq!(restored.inner.lock().await.state.records["i"].document.as_ref().unwrap()["body"], "evidence original");
            drop(restored);
        }
        std::fs::remove_dir_all(root).unwrap(); std::fs::remove_dir_all(backup).unwrap();
    }

    #[tokio::test]
    async fn source_durable_failure_fences_old_output_and_replays_hiding() {
        let root = path();
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("s", "s", &[])).await.unwrap();
        p.write(source_write("d", "d", &[("s", 1)])).await.unwrap();
        let output = p.output_permit().unwrap();
        p.inner.lock().await.fail_after_durable = true;
        let mut delete = write("delete", "s", "", ExpectedVersion::Match(1)); delete.document = None;
        assert!(matches!(p.write(delete).await, Err(RecordError::DurablePending(_))));
        assert!(output.deliver(|| ()).is_err());
        assert!(p.get("d").await.is_err()); drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert!(p.get("d").await.unwrap().is_none());
        assert_eq!(p.statistics().await.unwrap().active_records, 0);
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn source_delete_reserved_capacity_and_old_snapshot_quarantine() {
        let root = path(); let backup = path();
        let mut cfg = config(); cfg.max_operations = 3;
        let p = RecordPartition::create(&root, cfg.clone()).await.unwrap();
        p.write(source_write("s", "s", &[])).await.unwrap();
        p.checkpoint(&backup).await.unwrap();
        p.write(write("ordinary", "o", "one", ExpectedVersion::Absent)).await.unwrap();
        assert!(p.write(write("full", "x", "two", ExpectedVersion::Absent)).await.is_err());
        let mut delete = write("delete", "s", "", ExpectedVersion::Match(1)); delete.document=None;
        p.write(delete).await.unwrap(); // reserved operation slot remains available
        let old = RecordPartition::open(&backup, cfg.max_journal_bytes).await.unwrap();
        assert!(old.statistics().await.unwrap().evidence_quarantined);
        assert!(old.get("s").await.unwrap().is_none());
        assert!(old.write(source_write("bypass", "new-source", &[])).await.is_err());
        assert!(old.inner.lock().await.state.records["s"].document.is_some());
        drop(old); drop(p);
        std::fs::remove_dir_all(root).unwrap(); std::fs::remove_dir_all(backup).unwrap();
    }

    #[tokio::test]
    async fn source_delete_has_wal_and_checkpoint_reserve_after_ordinary_pressure() {
        let root = path(); let mut cfg = config(); cfg.max_journal_bytes = 24 * 1024;
        let p = RecordPartition::create(&root, cfg.clone()).await.unwrap();
        p.write(source_write("s", "s", &[])).await.unwrap();
        let mut rejected = false;
        for n in 0..100 {
            let result = p.write(write(&format!("fill-{n}"), "o", &"x".repeat(1500), ExpectedVersion::Any)).await;
            if matches!(result, Err(RecordError::Rejected(_))) { rejected = true; break; }
            result.unwrap();
        }
        assert!(rejected);
        let mut delete = write("delete", "s", "", ExpectedVersion::Match(1)); delete.document=None;
        p.write(delete).await.unwrap();
        p.compact_journal().await.unwrap(); drop(p);
        let p = RecordPartition::open(&root, cfg.max_journal_bytes).await.unwrap();
        assert!(p.get("s").await.unwrap().is_none());
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn source_protocol_rejects_p1_checkpoint_without_migration() {
        let root = path(); let cfg = config();
        let checkpoint = encode_entry(&Entry::Checkpoint { config: cfg.clone(), state: State::default() }).unwrap();
        let legacy = String::from_utf8(checkpoint).unwrap().replacen("RecordCheckpointV14", "Checkpoint", 1);
        let journal = PartitionJournal::create(&root, cfg.max_journal_bytes, legacy.as_bytes()).unwrap(); drop(journal);
        assert!(RecordPartition::open(&root, cfg.max_journal_bytes).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn source_references_are_precise_scoped_immutable_and_bounded() {
        let root = path();
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("s", "s", &[])).await.unwrap();
        assert!(p.write(source_write("wrong-version", "bad", &[("s", 2)])).await.is_err());
        let mut wrong = source_write("wrong-route", "bad", &[("s", 1)]);
        wrong.system.as_mut().unwrap().evidence_refs[0].routing_key = "wrong".into();
        assert!(p.write(wrong).await.is_err());
        assert!(p.write(source_write("missing", "bad", &[("other-collection-record", 1)])).await.is_err());
        assert!(p.write(source_write("self", "self", &[("self", 1)])).await.is_err());
        assert!(p.write(source_write("many", "many", &vec![("s", 1); 33])).await.is_err());
        let mut previous = "s".to_string();
        for depth in 1..=16 {
            let id = format!("d{depth}"); p.write(source_write(&id, &id, &[(&previous, 1)])).await.unwrap(); previous = id;
        }
        assert!(p.write(source_write("too-deep", "deep", &[(&previous, 1)])).await.is_err());
        assert_eq!(p.statistics().await.unwrap().active_records, 17);
        let value = serde_json::json!({"kind":"derived","evidence_refs":[{"collection_id":"other","record_id":"s","routing_key":"document-1","version":1}]});
        assert!(serde_json::from_value::<EvidenceMetadata>(value).is_err());
        drop(p); std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn record_planning_is_read_only_and_recovery_ignores_retry_clock() {
        let config = config();
        let mut state = State::default();
        let checkpoint = encode_entry(&Entry::Checkpoint {
            config: config.clone(), state: state.clone(),
        }).unwrap();
        let mut request = write("planned", "r", "original", ExpectedVersion::Absent);
        request.operation_id.epoch = 100;
        let mutation::PreparedWrite::Append { applied, bytes, snapshot_bytes, .. } =
            mutation::prepare(&state, &config, checkpoint.len(), request.clone(), 100).unwrap()
        else { panic!("new operation must be planned"); };
        // Planning has no authority/index side effects, even for accepted input.
        assert_eq!(checkpoint, encode_entry(&Entry::Checkpoint {
            config: config.clone(), state: state.clone(),
        }).unwrap());
        // The durable decision is sufficient to recover; wall time does not
        // invalidate an acknowledged write merely because retry admission expired.
        let Entry::Applied(recovered) = serde_json::from_slice(&bytes).unwrap()
        else { panic!("expected applied decision"); };
        apply(&mut state, &recovered).unwrap();
        assert_eq!(state.records["r"].version, 1);
        assert_eq!(snapshot_bytes, encode_entry(&Entry::Checkpoint {
            config: config.clone(), state: state.clone(),
        }).unwrap().len());
        assert!(matches!(mutation::prepare(&state, &config, snapshot_bytes,
            request.clone(), 102), Err(RecordError::OperationExpired)));
        // The persisted watermark prevents a wall-clock rollback extending retries.
        state.epoch_watermark = 102;
        assert!(matches!(mutation::prepare(&state, &config, snapshot_bytes,
            request, 100), Err(RecordError::OperationExpired)));
        assert_eq!(applied.receipt, recovered.receipt);
    }

    #[test]
    fn record_planning_replays_before_capacity_and_validates_original_vectors() {
        let mut config = config();
        config.max_operations = 1;
        let mut state = State::default();
        let checkpoint_bytes = encode_entry(&Entry::Checkpoint {
            config: config.clone(), state: state.clone(),
        }).unwrap().len();
        let mut invalid = write("invalid", "r", "original", ExpectedVersion::Absent);
        invalid.document.as_mut().unwrap().insert("vector".into(), serde_json::json!([0,0]));
        assert!(matches!(mutation::prepare(&state, &config, checkpoint_bytes,
            invalid, current_epoch()), Err(RecordError::Rejected(_))));
        assert_eq!(state.seq, 0);
        let request = write("valid", "r", "original", ExpectedVersion::Absent);
        let mutation::PreparedWrite::Append { applied, snapshot_bytes, .. } =
            mutation::prepare(&state, &config, checkpoint_bytes, request.clone(), current_epoch()).unwrap()
        else { panic!("new operation"); };
        apply(&mut state, &applied).unwrap();
        assert_eq!(state.records["r"].document, request.document);
        assert!(matches!(mutation::prepare(&state, &config, snapshot_bytes,
            request.clone(), current_epoch()), Ok(mutation::PreparedWrite::Replay(_))));
        let mut changed = request;
        changed.document.as_mut().unwrap().insert("body".into(), serde_json::json!("different"));
        assert!(matches!(mutation::prepare(&state, &config, snapshot_bytes,
            changed, current_epoch()), Err(RecordError::Conflict(_))));
        assert!(matches!(mutation::prepare(&state, &config, snapshot_bytes,
            write("new", "other", "another", ExpectedVersion::Absent), current_epoch()),
            Err(RecordError::Rejected(_))));
    }

    #[tokio::test]
    async fn record_vector_only_browse_preserves_live_ids_filters_and_recovery() {
        let location = path();
        let mut cfg = config();
        cfg.schema.iter_mut().for_each(|field| field.index_lexical = false);
        cfg.schema.push(serde_json::from_value(serde_json::json!({
            "field":"rank", "field_type":"U64", "facet":true, "store":false,
            "index_lexical":false
        })).unwrap());
        let mut p = RecordPartition::create(&location, cfg.clone()).await.unwrap();
        for (id, rank) in [("z", 1), ("a", 2), ("m", 1), ("b", 3)] {
            let mut request = write(id, id, "original", ExpectedVersion::Absent);
            request.document.as_mut().unwrap().insert("rank".into(), serde_json::json!(rank));
            p.write(request).await.unwrap();
        }
        let mut replace = write("replace", "z", "replacement", ExpectedVersion::Match(1));
        replace.document.as_mut().unwrap().insert("rank".into(), serde_json::json!(2));
        p.write(replace).await.unwrap();
        let mut delete = write("delete", "m", "", ExpectedVersion::Match(1));
        delete.document = None;
        p.write(delete).await.unwrap();

        for stage in 0..3 {
            let options = || RecordQuery {
                temporal: None,
                text: String::new(), vector: None, mode: SearchMode::Lexical,
                query_type: QueryType::Union, offset: 0, length: 10,
                result_type: ResultType::TopkCount, fields: vec![], filters: vec![], sort: vec![],
            };
            let ids = |r: &RecordSearch| r.hits.iter().map(|h| h.record.record_id.clone()).collect::<Vec<_>>();
            let result = p.query(options()).await.unwrap();
            assert_eq!(ids(&result), ["a", "b", "z"], "stage {stage}");
            assert_eq!(result.count_total, 3);
            assert_eq!(result.visible_seq, 6);
            assert_eq!(result.hits[2].record.version, 2);
            let mut q = options(); q.offset = 1; q.length = 1;
            assert_eq!(ids(&p.query(q).await.unwrap()), ["b"]);
            let mut q = options(); q.length = 0;
            let result = p.query(q).await.unwrap();
            assert_eq!(result.count_total, 3); assert!(result.hits.is_empty());
            let mut q = options();
            q.filters = vec![FacetFilter::U64 { field: "rank".into(), filter: 2..3 }];
            let result = p.query(q).await.unwrap();
            assert_eq!(ids(&result), ["a", "z"]); assert_eq!(result.count_total, 2);
            let mut q = options(); q.offset = 1; q.length = 1;
            q.sort = vec![ResultSort { field: "rank".into(), order: crate::search::SortOrder::Ascending }];
            assert_eq!(ids(&p.query(q).await.unwrap()), ["z"]);
            let mut q = options(); q.text = "original".into();
            assert!(p.query(q).await.unwrap().hits.is_empty());
            match stage {
                0 => p.inner.lock().await.index.test_index().commit().await.unwrap(),
                1 => {
                    p.compact_journal().await.unwrap();
                    drop(p);
                    p = RecordPartition::open(&location, cfg.max_journal_bytes).await.unwrap();
                }
                _ => {}
            }
        }
        drop(p);
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    async fn record_busy_retry_does_not_disprove_prior_commit() {
        let location = path();
        let p = RecordPartition::create(&location, config()).await.unwrap();
        let request = write("retry", "r", "alpha", ExpectedVersion::Absent);
        let receipt = p.write(request.clone()).await.unwrap();
        let occupied = p.admission.clone().acquire_many_owned(16).await.unwrap();
        assert_eq!(p.write(request.clone()).await.unwrap_err(), RecordError::Busy);
        drop(occupied);
        assert_eq!(p.write(request).await.unwrap(), receipt);
        assert_eq!(p.statistics().await.unwrap().retained_operations, 1);
        drop(p);
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    async fn record_depth_rejection_preserves_authority_and_accepted_depth_recovers() {
        let location = path();
        let snapshot = path();
        let mut partition = RecordPartition::create(&location, config()).await.unwrap();
        let nested = |depth: usize| {
            let mut value = serde_json::Value::Null;
            for _ in 1..depth {
                value = serde_json::Value::Array(vec![value]);
            }
            value
        };
        let mut accepted = write("nested-ok", "ok", "alpha", ExpectedVersion::Absent);
        accepted
            .document
            .as_mut()
            .unwrap()
            .insert("nested".into(), nested(64));
        partition.write(accepted.clone()).await.unwrap();
        drop(partition); // Windows enforces the journal owner byte-range lock.
        let wal = std::fs::read(location.join("generation-0000000000000000").join("wal")).unwrap();
        partition = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        for depth in [65, 200] {
            let mut rejected = write("nested-reject", "bad", "alpha", ExpectedVersion::Absent);
            rejected
                .document
                .as_mut()
                .unwrap()
                .insert("nested".into(), nested(depth));
            assert!(matches!(
                partition.write(rejected).await,
                Err(RecordError::Rejected(_))
            ));
            assert_eq!(query(&partition, SearchMode::Lexical).await.visible_seq, 1);
            drop(partition);
            assert_eq!(
                std::fs::read(location.join("generation-0000000000000000").join("wal")).unwrap(),
                wal
            );
            partition = RecordPartition::open(&location, config().max_journal_bytes)
                .await
                .unwrap();
        }
        partition.checkpoint(&snapshot).await.unwrap();
        drop(partition);
        for directory in [&location, &snapshot] {
            let recovered = RecordPartition::open(directory, config().max_journal_bytes)
                .await
                .unwrap();
            assert_eq!(
                recovered.get("ok").await.unwrap().unwrap().document,
                accepted.document
            );
            assert!(recovered.get("bad").await.unwrap().is_none());
            drop(recovered);
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn record_create_budget_rejection_does_not_occupy_destination() {
        let location = path();
        let mut limited = config();
        limited.max_journal_bytes = 8;
        assert!(matches!(
            RecordPartition::create(&location, limited).await,
            Err(RecordError::Rejected(_))
        ));
        assert!(!location.exists());
        let partition = RecordPartition::create(&location, config()).await.unwrap();
        assert!(matches!(
            RecordPartition::create(&location, config()).await,
            Err(RecordError::Rejected(_))
        ));
        assert!(matches!(
            partition.checkpoint(&location).await,
            Err(RecordError::Rejected(_))
        ));
        drop(partition);
        let recovered = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(query(&recovered, SearchMode::Lexical).await.visible_seq, 0);
        drop(recovered);
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    #[ignore = "invoked only by parent cancellation test"]
    async fn record_cancelled_recovery_worker() {
        let location = PathBuf::from(std::env::var_os("ANYSEEK_RECORD_BUILD_GATE").unwrap());
        let task_path = location.clone();
        let task = tokio::spawn(async move {
            RecordPartition::open(&task_path, config().max_journal_bytes).await
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !location.join("build-entered").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "recovery did not enter build"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        // The public future is cancelled, but its owned recovery still holds
        // all mapped files and the authority lock. A new owner cannot clean it.
        assert!(PartitionJournal::open(&location, config().max_journal_bytes).is_err());
        std::fs::write(location.join("build-release"), b"continue").unwrap();
        loop {
            if let Ok((journal, _)) = PartitionJournal::open(&location, config().max_journal_bytes)
            {
                drop(journal);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "cancelled owner did not drain"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let recovered = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(query(&recovered, SearchMode::Lexical).await.hits.len(), 1);
    }

    #[tokio::test]
    async fn record_cancelled_recovery_retains_lock_until_index_handles_are_released() {
        let location = path();
        let partition = RecordPartition::create(&location, config()).await.unwrap();
        partition
            .write(write("first", "id", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        drop(partition);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "record::tests::record_cancelled_recovery_worker",
            ])
            .env("ANYSEEK_RECORD_BUILD_GATE", &location)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    #[ignore = "invoked only by parent query cancellation test"]
    async fn record_cancelled_query_worker() {
        let location = PathBuf::from(std::env::var_os("ANYSEEK_RECORD_QUERY_GATE").unwrap());
        let partition = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            query(
                &partition,
                SearchMode::Hybrid {
                    similarity_threshold: None,
                    ann_mode: AnnMode::All,
                },
            )
            .await
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !location.join("query-entered").exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "query did not enter shard gate"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // Aborting also drops the last public partition owner.
        task.abort();
        assert!(matches!(task.await, Err(error) if error.is_cancelled()));
        assert!(PartitionJournal::open(&location, config().max_journal_bytes).is_err());
        std::fs::write(location.join("query-release"), b"continue").unwrap();
        loop {
            if let Ok((journal, _)) = PartitionJournal::open(&location, config().max_journal_bytes)
            {
                drop(journal);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "cancelled query did not drain"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let recovered = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(query(&recovered, SearchMode::Lexical).await.hits.len(), 1);
    }

    #[tokio::test]
    async fn record_cancelled_query_retains_lock_until_shard_task_releases_mmaps() {
        let location = path();
        let partition = RecordPartition::create(&location, config()).await.unwrap();
        partition
            .write(write("first", "id", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        drop(partition);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "record::tests::record_cancelled_query_worker",
            ])
            .env("ANYSEEK_RECORD_QUERY_GATE", &location)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    async fn record_repeated_recovery_reclaims_only_owned_cache_directories() {
        let location = path();
        let partition = RecordPartition::create(&location, config()).await.unwrap();
        partition
            .write(write("first", "id", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        drop(partition);
        let unrelated = location.join("cache-not-a-generation");
        std::fs::create_dir(&unrelated).unwrap();
        for _ in 0..5 {
            // Models an interrupted build left by an earlier process.
            let orphan = location.join(format!("cache-{:032x}", rand::random::<u128>()));
            std::fs::create_dir(&orphan).unwrap();
            std::fs::write(orphan.join("partial"), b"incomplete").unwrap();
            let recovered = RecordPartition::open(&location, config().max_journal_bytes)
                .await
                .unwrap();
            assert!(!orphan.exists());
            assert!(unrelated.is_dir());
            assert_eq!(query(&recovered, SearchMode::Lexical).await.hits.len(), 1);
            let caches = std::fs::read_dir(&location)
                .unwrap()
                .filter(|entry| {
                    let name = entry.as_ref().unwrap().file_name();
                    let name = name.to_string_lossy();
                    name.starts_with("cache-") && name.len() == 38
                })
                .count();
            assert_eq!(caches, 1);
            // A competing open cannot clean the currently mapped generation.
            assert!(
                RecordPartition::open(&location, config().max_journal_bytes)
                    .await
                    .is_err()
            );
            assert_eq!(query(&recovered, SearchMode::Lexical).await.hits.len(), 1);
            drop(recovered);
        }
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    async fn record_durable_replace_delete_retry_aba_and_checkpoint() {
        let path = path();
        let snapshot = path.with_extension("snapshot");
        let partition = RecordPartition::create(&path, config()).await.unwrap();
        let first = write(
            "create",
            "business-id",
            "alpha original",
            ExpectedVersion::Absent,
        );
        let r = partition.write(first.clone()).await.unwrap();
        assert_eq!(r.version, 1);
        assert_eq!(partition.write(first.clone()).await.unwrap(), r);
        assert_eq!(
            partition
                .get("business-id")
                .await
                .unwrap()
                .unwrap()
                .document
                .unwrap()["vector"],
            serde_json::json!([2.0, 0.0])
        );
        let mut collision = first;
        collision.document = None;
        assert!(matches!(
            partition.write(collision).await,
            Err(RecordError::Conflict(_))
        ));
        let update = write(
            "update",
            "business-id",
            "alpha replacement",
            ExpectedVersion::Match(1),
        );
        let r = partition.write(update).await.unwrap();
        assert_eq!(r.version, 2);
        for mode in [
            SearchMode::Lexical,
            SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: AnnMode::All,
            },
            SearchMode::Hybrid {
                similarity_threshold: None,
                ann_mode: AnnMode::All,
            },
        ] {
            let hits = query(&partition, mode).await;
            assert_eq!(hits.visible_seq, 2);
            assert_eq!(hits.hits.len(), 1);
            assert_eq!(hits.hits[0].record.version, 2);
            assert_eq!(
                hits.hits[0].record.document.as_ref().unwrap()["body"],
                "alpha replacement"
            );
        }
        let mut deletion = write("delete", "business-id", "unused", ExpectedVersion::Match(2));
        deletion.document = None;
        assert_eq!(partition.write(deletion).await.unwrap().version, 3);
        assert!(query(&partition, SearchMode::Lexical).await.hits.is_empty());
        assert!(partition.get("business-id").await.unwrap().is_none());
        assert_eq!(
            partition
                .write(write(
                    "recreate",
                    "business-id",
                    "alpha resurrected",
                    ExpectedVersion::Absent
                ))
                .await
                .unwrap()
                .version,
            4
        );
        assert!(matches!(
            partition
                .write(write(
                    "stale",
                    "business-id",
                    "wrong",
                    ExpectedVersion::Match(1)
                ))
                .await,
            Err(RecordError::Conflict(_))
        ));
        assert_eq!(partition.checkpoint(&snapshot).await.unwrap(), 4);
        drop(partition);
        for location in [&path, &snapshot] {
            let recovered = RecordPartition::open(location, 1024 * 1024).await.unwrap();
            let hits = query(&recovered, SearchMode::Lexical).await;
            assert_eq!(hits.visible_seq, 4);
            assert_eq!(hits.hits[0].record.version, 4);
            assert_eq!(
                recovered
                    .operation(&OperationId::new("create".into()))
                    .await
                    .unwrap(),
                Some(WriteReceipt {
                    staged_only: false,
                    seq: 1,
                    version: 1,
                    record_id: "business-id".into()
                })
            );
            drop(recovered);
            std::fs::remove_dir_all(location).unwrap();
        }
    }
    #[tokio::test]
    async fn record_durable_application_failure_fences_and_replays() {
        let path = path();
        let partition = RecordPartition::create(&path, config()).await.unwrap();
        partition.inner.lock().await.fail_after_durable = true;
        let request = write("pending", "id", "alpha", ExpectedVersion::Absent);
        assert_eq!(
            partition.write(request.clone()).await.unwrap_err(),
            RecordError::DurablePending(1)
        );
        assert!(matches!(
            partition.get("id").await,
            Err(RecordError::RecoveryRequired)
        ));
        assert_eq!(
            partition
                .write(write("later", "id2", "alpha", ExpectedVersion::Absent))
                .await
                .unwrap_err(),
            RecordError::RecoveryRequired
        );
        drop(partition);
        let recovered = RecordPartition::open(&path, 1024 * 1024).await.unwrap();
        assert_eq!(recovered.write(request).await.unwrap().seq, 1);
        assert_eq!(query(&recovered, SearchMode::Lexical).await.hits.len(), 1);
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn record_online_checkpoint_preserves_view_retry_aba_and_original_data() {
        let location = path();
        let partition = RecordPartition::create(&location, config()).await.unwrap();
        let first = write("a", "a", "alpha original", ExpectedVersion::Absent);
        let receipt = partition.write(first.clone()).await.unwrap();
        partition
            .write(write(
                "a2",
                "a",
                "alpha replacement",
                ExpectedVersion::Match(1),
            ))
            .await
            .unwrap();
        let mut deletion = write("delete", "a", "unused", ExpectedVersion::Match(2));
        deletion.document = None;
        partition.write(deletion).await.unwrap();
        let cache = partition
            .inner
            .lock()
            .await
            .index.test_index()
            .read()
            .await
            .index_path_string
            .clone();
        for round in 1..=2 {
            assert_eq!(partition.compact_journal().await.unwrap(), 3);
            // The checkpoint now selects an authenticated immutable index;
            // the original mutable cache is no longer the serving root.
            assert_ne!(
                partition
                    .inner
                    .lock()
                    .await
                    .index.test_index()
                    .read()
                    .await
                    .index_path_string,
                cache
            );
            assert!(
                !location
                    .join(format!("generation-{:016x}", round - 1))
                    .exists()
            );
            assert!(location.join(format!("generation-{round:016x}")).exists());
            assert_eq!(partition.write(first.clone()).await.unwrap(), receipt);
        }
        assert_eq!(
            partition
                .write(write("a3", "a", "alpha restored", ExpectedVersion::Absent))
                .await
                .unwrap()
                .version,
            4
        );
        let expected = query(
            &partition,
            SearchMode::Hybrid {
                similarity_threshold: None,
                ann_mode: AnnMode::All,
            },
        )
        .await;
        drop(partition);
        let restored = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(restored.write(first).await.unwrap(), receipt);
        assert!(matches!(
            restored
                .write(write("aba", "a", "bad", ExpectedVersion::Match(1)))
                .await,
            Err(RecordError::Conflict(_))
        ));
        let actual = query(
            &restored,
            SearchMode::Hybrid {
                similarity_threshold: None,
                ann_mode: AnnMode::All,
            },
        )
        .await;
        assert_eq!(actual.visible_seq, expected.visible_seq);
        assert_eq!(actual.hits[0].record.version, 4);
        assert_eq!(
            actual.hits[0].record.document,
            expected.hits[0].record.document
        );
        drop(restored);
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    #[ignore = "invoked only by parent checkpoint process test"]
    async fn record_checkpoint_crash_worker() {
        let location = PathBuf::from(std::env::var_os("ANYSEEK_CHECKPOINT_PATH").unwrap());
        if std::env::var_os("ANYSEEK_PARTITION_RECOVERY_FAIL").is_some() {
            assert!(
                RecordPartition::open(&location, config().max_journal_bytes)
                    .await
                    .is_err()
            );
            std::process::exit(78);
        }
        let partition = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        if std::env::var_os("ANYSEEK_CHECKPOINT_EXPECT_UNKNOWN").is_some() {
            assert!(matches!(
                partition.compact_journal().await,
                Err(RecordError::OutcomeUnknown(_))
            ));
            assert!(matches!(
                partition.get("a").await,
                Err(RecordError::RecoveryRequired)
            ));
            std::process::exit(77);
        }
        if std::env::var_os("ANYSEEK_PARTITION_RECOVER").is_none() {
            partition.compact_journal().await.unwrap();
        }
        panic!("checkpoint crash boundary not reached");
    }

    #[tokio::test]
    async fn record_online_checkpoint_process_crashes_recover_and_accept_new_writes() {
        for stage in [
            "log_synced",
            "first_witness_torn",
            "initial_prepared",
            "initial_published",
            "certified",
            "switched",
            "reclaimed",
            "old_partial",
        ] {
            let location = path();
            let partition = RecordPartition::create(&location, config()).await.unwrap();
            let first = write("first", "a", "alpha original", ExpectedVersion::Absent);
            let receipt = partition.write(first.clone()).await.unwrap();
            drop(partition);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "record::tests::record_checkpoint_crash_worker",
                ])
                .env("ANYSEEK_CHECKPOINT_PATH", &location)
                .env("ANYSEEK_JOURNAL_CRASH", stage)
                .env("ANYSEEK_PARTITION_CRASH", stage)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(
                    if ["certified", "switched", "reclaimed", "old_partial"].contains(&stage) {
                        76
                    } else {
                        73
                    }
                ),
                "{stage}: {output:?}"
            );
            let restored = RecordPartition::open(&location, config().max_journal_bytes)
                .await
                .unwrap();
            assert_eq!(restored.write(first).await.unwrap(), receipt);
            restored
                .write(write(
                    "after-recovery",
                    "a",
                    "alpha next",
                    ExpectedVersion::Match(1),
                ))
                .await
                .unwrap();
            // Prove recovery did not keep writing an older generation which a
            // second recovery would ignore in favor of an uncertified directory.
            drop(restored);
            let restored = RecordPartition::open(&location, config().max_journal_bytes)
                .await
                .unwrap();
            assert_eq!(
                restored.get("a").await.unwrap().unwrap().version,
                2,
                "{stage}"
            );
            assert_eq!(query(&restored, SearchMode::Lexical).await.hits.len(), 1);
            drop(restored);
            std::fs::remove_dir_all(location).unwrap();
        }
    }

    #[tokio::test]
    async fn record_uncertified_generation_cleanup_can_be_interrupted_again() {
        for stage in ["discard_renamed", "discard_partial"] {
            let location = path();
            let partition = RecordPartition::create(&location, config()).await.unwrap();
            partition
                .write(write("a", "a", "alpha", ExpectedVersion::Absent))
                .await
                .unwrap();
            drop(partition);
            let worker = || {
                let mut command = std::process::Command::new(std::env::current_exe().unwrap());
                command
                    .args([
                        "--ignored",
                        "--exact",
                        "record::tests::record_checkpoint_crash_worker",
                    ])
                    .env("ANYSEEK_CHECKPOINT_PATH", &location);
                command
            };
            let output = worker()
                .env("ANYSEEK_JOURNAL_CRASH", "initial_published")
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(73));
            let output = worker()
                .env("ANYSEEK_PARTITION_CRASH", stage)
                .env("ANYSEEK_PARTITION_RECOVER", "1")
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(76), "{stage}: {output:?}");
            let recovered = RecordPartition::open(&location, config().max_journal_bytes)
                .await
                .unwrap();
            assert_eq!(recovered.get("a").await.unwrap().unwrap().version, 1);
            recovered
                .write(write("b", "a", "alpha new", ExpectedVersion::Match(1)))
                .await
                .unwrap();
            drop(recovered);
            let recovered = RecordPartition::open(&location, config().max_journal_bytes)
                .await
                .unwrap();
            assert_eq!(recovered.get("a").await.unwrap().unwrap().version, 2);
            drop(recovered);
            std::fs::remove_dir_all(location).unwrap();
        }
    }

    #[tokio::test]
    async fn record_incremental_crosses_physical_block_with_retired_history() {
        let location = path();
        let mut settings = config();
        settings.max_operations = 100_000;
        settings.max_journal_bytes = 128 * 1024 * 1024;
        settings.schema.push(serde_json::from_value(serde_json::json!({"field":"region", "field_type":"String16", "store":false, "index_lexical":false, "facet":true})).unwrap());
        let partition = RecordPartition::create(&location, settings).await.unwrap();
        let mut original = write("first", "a", "alpha", ExpectedVersion::Absent);
        original
            .document
            .as_mut()
            .unwrap()
            .insert("region".into(), "eu".into());
        partition.write(original.clone()).await.unwrap();
        // Seed synthetic retired physical history through the actual engine,
        // bypassing WAL/admission to preserve the low-level 64K boundary oracle.
        // Production admission now flushes such a tail earlier; this fixture is
        // not evidence that 65K retained mutations fit the resource budget.
        // The final boundary crossing still uses authoritative publication.
        {
            let mut inner = partition.inner.lock().await;
            let shard = inner.index.test_index().read().await.shard_vec[0].clone();
            let native=inner.index.test_index().read().await.record_lexical[0].clone();
            shard.write().await.build_input_covered=true;
            for id in 1..65536 {
                native.append(lexical::Identity{id:"a".into(),version:id as u64+1,revision:0},&original.document.clone().unwrap(),false).await.unwrap();
                shard
                    .write()
                    .await
                    .active
                    .as_mut()
                    .unwrap()
                    .append_identity("a".into(), id as u64 + 1, 0,
                        crate::active::fingerprint(original.document.as_ref().unwrap()).unwrap());
                inner
                    .index.test_index()
                    .index_documents(vec![original.document.clone().unwrap()])
                    .await
                    .unwrap();
                let mut shard = shard.write().await;
                shard.delete_hashset.insert(id - 1);
                shard.active.as_mut().unwrap().retire(id - 1);
                inner.ordinals.push("a".into());
            }
            native.flush().await.unwrap();
            shard.write().await.build_input_covered=false;
            inner.locations.insert("a".into(), 65535);
        }
        partition
            .write(write(
                "cross-block",
                "a",
                "alpha next",
                ExpectedVersion::Match(1),
            ))
            .await
            .unwrap();
        for mode in [
            SearchMode::Lexical,
            SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: AnnMode::All,
            },
            SearchMode::Hybrid {
                similarity_threshold: None,
                ann_mode: AnnMode::All,
            },
        ] {
            let result = query(&partition, mode).await;
            assert_eq!(result.hits.len(), 1);
            assert_eq!(result.hits[0].record.version, 2);
            assert_eq!(result.statistics.active_records, 1);
            assert_eq!(result.statistics.physical_versions, 65537);
            assert!(result.hits[0].score.is_finite());
        }
        partition.inner.lock().await.index.test_index().commit().await.unwrap();
        partition
            .write(write(
                "partial-append",
                "a",
                "alpha final",
                ExpectedVersion::Match(2),
            ))
            .await
            .unwrap();
        let result = query(
            &partition,
            SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: AnnMode::All,
            },
        )
        .await;
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].record.version, 3);
        drop(partition);
        let restored = RecordPartition::open(&location, 128 * 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(restored.get("a").await.unwrap().unwrap().version, 3);
        drop(restored);
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    async fn record_checkpoint_sync_failure_fences_and_recovery_failure_preserves_authority() {
        let location = path();
        let partition = RecordPartition::create(&location, config()).await.unwrap();
        partition
            .write(write("a", "a", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        drop(partition);
        let worker = || {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--ignored",
                    "--exact",
                    "record::tests::record_checkpoint_crash_worker",
                ])
                .env("ANYSEEK_CHECKPOINT_PATH", &location)
                .env("ANYSEEK_PUBLISH_FAIL", "after_rename")
                .env("ANYSEEK_PUBLISH_FAIL_SCOPE", "journal");
            command
        };
        let output = worker()
            .env("ANYSEEK_CHECKPOINT_EXPECT_UNKNOWN", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(77), "{output:?}");
        // Interrupted recovery retirement also must not serve or erase the
        // old certified authority; a subsequent recovery is safe.
        let output = worker()
            .env("ANYSEEK_PARTITION_RECOVERY_FAIL", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(78), "{output:?}");
        // The leftover .prepare itself must pass a fresh retirement sync,
        // rather than being deleted merely because its name is now invisible.
        let output = worker()
            .env("ANYSEEK_PARTITION_RECOVERY_FAIL", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(78), "{output:?}");
        let recovered = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(recovered.get("a").await.unwrap().unwrap().version, 1);
        recovered
            .write(write("b", "a", "alpha new", ExpectedVersion::Match(1)))
            .await
            .unwrap();
        drop(recovered);
        let recovered = RecordPartition::open(&location, config().max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(recovered.get("a").await.unwrap().unwrap().version, 2);
        drop(recovered);
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    async fn record_incremental_partial_publication_fences_all_views_and_replays() {
        for stage in [1, 2] {
            let location = path();
            let partition = RecordPartition::create(&location, config()).await.unwrap();
            partition
                .write(write("a", "a", "alpha old", ExpectedVersion::Absent))
                .await
                .unwrap();
            partition.inner.lock().await.fail_publication_stage = stage;
            let request = write("update", "a", "alpha new", ExpectedVersion::Match(1));
            assert_eq!(
                partition.write(request.clone()).await.unwrap_err(),
                RecordError::DurablePending(2)
            );
            assert!(matches!(
                partition.statistics().await,
                Err(RecordError::RecoveryRequired)
            ));
            assert!(matches!(
                partition.get("a").await,
                Err(RecordError::RecoveryRequired)
            ));
            assert!(matches!(
                partition
                    .search("alpha".into(), None, SearchMode::Lexical, vec![], 1)
                    .await,
                Err(RecordError::RecoveryRequired)
            ));
            drop(partition);
            let restored = RecordPartition::open(&location, config().max_journal_bytes)
                .await
                .unwrap();
            assert_eq!(restored.write(request).await.unwrap().version, 2);
            assert_eq!(restored.statistics().await.unwrap().active_records, 1);
            let result = query(
                &restored,
                SearchMode::Hybrid {
                    similarity_threshold: None,
                    ann_mode: AnnMode::All,
                },
            )
            .await;
            assert_eq!(result.hits.len(), 1);
            assert_eq!(result.hits[0].record.version, 2);
            drop(restored);
            std::fs::remove_dir_all(location).unwrap();
        }
    }

    #[tokio::test]
    async fn record_recovery_read_budget_does_not_expand_persisted_write_budget() {
        let location = path();
        let mut settings = config();
        settings.max_journal_bytes = 4000;
        drop(RecordPartition::create(&location, settings).await.unwrap());
        let partition = RecordPartition::open(&location, 1024 * 1024).await.unwrap();
        let mut rejected = false;
        for operation in 0..100 {
            match partition
                .write(write(
                    &format!("op-{operation}"),
                    "a",
                    "alpha x",
                    ExpectedVersion::Any,
                ))
                .await
            {
                Ok(_) => {}
                Err(RecordError::Rejected(_)) => {
                    rejected = true;
                    break;
                }
                other => panic!("unexpected admission result: {other:?}"),
            }
        }
        assert!(rejected);
        assert!(
            std::fs::metadata(location.join("generation-0000000000000000/wal"))
                .unwrap()
                .len()
                <= 4000
        );
        assert_eq!(query(&partition, SearchMode::Lexical).await.hits.len(), 1);
        drop(partition);
        std::fs::remove_dir_all(location).unwrap();
    }

    async fn ranked(
        partition: &RecordPartition,
        text: &str,
        mode: SearchMode,
        limit: usize,
    ) -> Vec<(String, u32)> {
        let vector = if mode == SearchMode::Lexical {
            None
        } else {
            Some(Embedding::F32(vec![1.0, 0.0]))
        };
        partition
            .search(text.into(), vector, mode, vec![], limit)
            .await
            .unwrap()
            .hits
            .into_iter()
            .map(|hit| (hit.record.record_id, hit.score.to_bits()))
            .collect()
    }

    #[tokio::test]
    async fn record_incremental_same_score_boundary_survives_replace_commit_and_recovery() {
        let location = path();
        let mut partition = RecordPartition::create(&location, config()).await.unwrap();
        let cache = partition
            .inner
            .lock()
            .await
            .index.test_index()
            .read()
            .await
            .index_path_string
            .clone();
        for id in ["z", "c", "b", "a"] {
            partition
                .write(write(id, id, "alpha beta", ExpectedVersion::Absent))
                .await
                .unwrap();
        }
        // The retired lexical heap's pruning rules are not part of the native engine.
        // The following end-to-end windows verify stable business-ID ties before
        // checkpoint, after checkpoint and after reopening the selected package.
        for step in 0..3 {
            for (text, mode) in [
                ("alpha", SearchMode::Lexical),
                ("", SearchMode::Lexical),
                (
                    "alpha",
                    SearchMode::Vector {
                        similarity_threshold: None,
                        ann_mode: AnnMode::All,
                    },
                ),
                (
                    "alpha",
                    SearchMode::Hybrid {
                        similarity_threshold: None,
                        ann_mode: AnnMode::All,
                    },
                ),
                (
                    "alpha",
                    SearchMode::WeightedHybrid {
                        similarity_threshold: None,
                        ann_mode: AnnMode::All,
                        rrf: crate::search::RrfOptions {
                            keyword_candidates: Some(2),
                            vector_candidates: Some(2),
                            ..Default::default()
                        },
                    },
                ),
            ] {
                let result = ranked(&partition, text, mode, 2).await;
                assert_eq!(
                    result.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
                    ["a", "b"],
                    "step {step}"
                );
            }
            if step == 0 {
                partition
                    .write(write(
                        "replace-a",
                        "a",
                        "alpha beta",
                        ExpectedVersion::Match(1),
                    ))
                    .await
                    .unwrap();
                assert_eq!(
                    partition
                        .inner
                        .lock()
                        .await
                        .index.test_index()
                        .read()
                        .await
                        .index_path_string,
                    cache
                );
                assert_eq!(partition.inner.lock().await.ordinals.len(), 5);
                let result = query(&partition, SearchMode::Lexical).await;
                assert_eq!(result.visible_seq, result.statistics.visible_seq);
                assert_eq!(result.statistics.active_records, 4);
                assert_eq!(result.statistics.physical_versions, 5);
                partition.inner.lock().await.index.test_index().commit().await.unwrap();
            } else if step == 1 {
                drop(partition);
                partition = RecordPartition::open(&location, config().max_journal_bytes)
                    .await
                    .unwrap();
                // Recovery rebuilds a partial block; this append must not be
                // interpreted as belonging to physical block 1 by vector search.
                partition
                    .write(write(
                        "replace-again",
                        "a",
                        "alpha beta",
                        ExpectedVersion::Match(2),
                    ))
                    .await
                    .unwrap();
            }
        }
        drop(partition);
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    async fn record_incremental_active_statistics_match_effective_rebuild() {
        for normalization in [
            FieldLengthNormalization::Shared,
            FieldLengthNormalization::PerField,
        ] {
            let location = path();
            let clean_location = path();
            let mut settings = config();
            settings.index.field_length_normalization = normalization;
            settings.index.ngram_indexing = 127;
            settings.index.frequent_words =
                serde_json::from_value(serde_json::json!({"Custom":{"terms":["alpha", "beta"]}}))
                    .unwrap();
            settings.schema.push(serde_json::from_value(serde_json::json!({"field":"title", "field_type":"Text", "store":false, "index_lexical":true, "boost":2.0})).unwrap());
            let partition = RecordPartition::create(&location, settings.clone())
                .await
                .unwrap();
            let clean = RecordPartition::create(&clean_location, settings)
                .await
                .unwrap();
            let make = |nonce: &str, id: &str, body: &str, title: &str| {
                let mut request = write(nonce, id, body, ExpectedVersion::Any);
                request
                    .document
                    .as_mut()
                    .unwrap()
                    .insert("title".into(), title.into());
                request
            };
            partition
                .write(make(
                    "old-a",
                    "a",
                    "alpha alpha alpha beta old",
                    "obsolete long long long",
                ))
                .await
                .unwrap();
            partition
                .write(make("old-b", "b", "beta old", "alpha beta"))
                .await
                .unwrap();
            partition.inner.lock().await.index.test_index().commit().await.unwrap();
            partition
                .write(make("a", "a", "alpha beta new", ""))
                .await
                .unwrap();
            let mut deletion = write("delete-b", "b", "unused", ExpectedVersion::Any);
            deletion.document = None;
            partition.write(deletion).await.unwrap();
            for (id, body, title) in [
                ("c", "", "alpha beta alpha beta"),
                ("d", "alpha beta extra extra", "beta"),
            ] {
                partition.write(make(id, id, body, title)).await.unwrap();
                clean.write(make(id, id, body, title)).await.unwrap();
            }
            clean
                .write(make("a", "a", "alpha beta new", ""))
                .await
                .unwrap();
            for text in ["alpha", "alpha beta", "\"alpha beta\"", "old", ""] {
                let expected = ranked(&clean, text, SearchMode::Lexical, 10).await;
                assert_eq!(
                    ranked(&partition, text, SearchMode::Lexical, 10).await,
                    expected,
                    "before commit: {text}"
                );
                partition.inner.lock().await.index.test_index().commit().await.unwrap();
                assert_eq!(
                    ranked(&partition, text, SearchMode::Lexical, 10).await,
                    expected,
                    "after commit: {text}"
                );
            }
            let expected = ranked(&partition, "alpha beta", SearchMode::Lexical, 10).await;
            drop(partition);
            let recovered = RecordPartition::open(&location, config().max_journal_bytes)
                .await
                .unwrap();
            assert_eq!(
                ranked(&recovered, "alpha beta", SearchMode::Lexical, 10).await,
                expected
            );
            drop(recovered);
            drop(clean);
            std::fs::remove_dir_all(location).unwrap();
            std::fs::remove_dir_all(clean_location).unwrap();
        }
    }

    #[tokio::test]
    async fn record_concurrent_conditions_and_active_scoring_match_clean_index() {
        let path = path();
        let clean_path = path.with_extension("clean");
        let partition = RecordPartition::create(&path, config()).await.unwrap();
        partition
            .write(write("a", "a", "alpha alpha old", ExpectedVersion::Absent))
            .await
            .unwrap();
        partition
            .write(write("b", "b", "alpha beta", ExpectedVersion::Absent))
            .await
            .unwrap();
        let (a, b) = tokio::join!(
            partition.write(write("x", "a", "alpha new", ExpectedVersion::Match(1))),
            partition.write(write("y", "a", "alpha new", ExpectedVersion::Match(1)))
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let clean = RecordPartition::create(&clean_path, config())
            .await
            .unwrap();
        clean
            .write(write("a", "a", "alpha new", ExpectedVersion::Absent))
            .await
            .unwrap();
        clean
            .write(write("b", "b", "alpha beta", ExpectedVersion::Absent))
            .await
            .unwrap();
        let actual = query(&partition, SearchMode::Lexical).await;
        let expected = query(&clean, SearchMode::Lexical).await;
        let scores = |r: RecordSearch| {
            r.hits
                .into_iter()
                .map(|h| (h.record.record_id, h.score.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(scores(actual), scores(expected));
        drop(partition);
        drop(clean);
        std::fs::remove_dir_all(path).unwrap();
        std::fs::remove_dir_all(clean_path).unwrap();
    }
    #[tokio::test]
    #[ignore = "launched only by native record crash test"]
    async fn record_crash_worker() {
        let path = PathBuf::from(std::env::var_os("ANYSEEK_RECORD_PATH").unwrap());
        let partition = RecordPartition::open(&path, 1024 * 1024).await.unwrap();
        partition
            .write(write(
                "crash-update",
                "a",
                "alpha replacement",
                ExpectedVersion::Match(1),
            ))
            .await
            .unwrap();
        panic!("crash point not reached");
    }
    #[tokio::test]
    async fn record_native_process_crash_and_response_loss_recovery() {
        for stage in [
            "log_synced",
            "first_witness_torn",
            "second_witness_torn",
            "second_witness_synced",
            "published",
        ] {
            let path = path();
            let partition = RecordPartition::create(&path, config()).await.unwrap();
            partition
                .write(write(
                    "initial",
                    "a",
                    "alpha original",
                    ExpectedVersion::Absent,
                ))
                .await
                .unwrap();
            drop(partition);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", "record::tests::record_crash_worker"])
                .env("ANYSEEK_RECORD_PATH", &path)
                .env("ANYSEEK_JOURNAL_CRASH", stage)
                .env("ANYSEEK_RECORD_CRASH", stage)
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(if stage == "published" { 74 } else { 73 }),
                "{stage}: {output:?}"
            );
            let recovered = RecordPartition::open(&path, 1024 * 1024).await.unwrap();
            let previous = recovered.get("a").await.unwrap().unwrap();
            assert_eq!(
                previous.version,
                if matches!(stage, "log_synced" | "first_witness_torn") {
                    1
                } else {
                    2
                },
                "{stage}"
            );
            let receipt = recovered
                .write(write(
                    "crash-update",
                    "a",
                    "alpha replacement",
                    ExpectedVersion::Match(1),
                ))
                .await
                .unwrap();
            assert_eq!(receipt.version, 2);
            assert_eq!(receipt.seq, 2);
            assert_eq!(
                query(&recovered, SearchMode::Lexical).await.hits[0]
                    .record
                    .version,
                2
            );
            drop(recovered);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
    #[tokio::test]
    async fn record_rejections_leave_complete_view_usable() {
        let path = path();
        let mut config = config();
        config.max_operations = 1;
        let partition = RecordPartition::create(&path, config).await.unwrap();
        let first = write("a", "a", "alpha", ExpectedVersion::Absent);
        let receipt = partition.write(first.clone()).await.unwrap();
        assert!(matches!(
            partition
                .write(write("b", "b", "alpha", ExpectedVersion::Absent))
                .await,
            Err(RecordError::Rejected(_))
        ));
        assert_eq!(partition.write(first).await.unwrap(), receipt);
        assert_eq!(query(&partition, SearchMode::Lexical).await.hits.len(), 1);
        let mut expired = write("expired", "a", "alpha", ExpectedVersion::Any);
        expired.operation_id.epoch = current_epoch() - 2;
        assert_eq!(
            partition
                .operation(&expired.operation_id)
                .await
                .unwrap_err(),
            RecordError::OperationExpired
        );
        let future = OperationId {
            epoch: current_epoch() + 1,
            nonce: "future".into(),
        };
        assert_eq!(
            partition.operation(&future).await.unwrap_err(),
            RecordError::OperationExpired
        );
        assert_eq!(
            partition.write(expired).await.unwrap_err(),
            RecordError::OperationExpired
        );
        drop(partition);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[cfg(feature = "s3-object-store")]
    #[tokio::test]
    async fn s3_partition_rebuilds_missing_local_cache_from_bootstrap_and_log() {
        use crate::storage::{s3_compatible::S3CompatibleStore, sqlite_authority::SqliteAuthority};
        use object_store::memory::InMemory;
        use std::{sync::Arc, time::Duration};

        let source_path=path();
        let target_path=path();
        let authority_path=target_path.with_extension("authority.sqlite");
        let object_store=Arc::new(InMemory::new());
        let store=S3CompatibleStore::from_store(object_store.clone(),1024*1024);
        let source=RecordPartition::create(&source_path,config()).await.unwrap();
        let checkpoint={
            let inner=source.inner.lock().await;
            encode_entry(&Entry::Checkpoint{config:inner.config.clone(),state:inner.state.clone()}).unwrap()
        };
        let mut authority=SqliteAuthority::open(&authority_path,Duration::from_secs(1)).unwrap();
        authority.create_partition("record-rebuild",1).unwrap();
        store.publish_bootstrap(&mut authority,"record-rebuild",1,&checkpoint,unix_time_ms_i64()).await.unwrap();
        let mut remote=RemotePersistence{ _owner:None,
            store,
            authority:tokio::sync::Mutex::new(authority),
            partition_id:"record-rebuild".into(),
            owner_epoch:1,
            max_tail_blocks:32,
            max_tail_bytes:1024*1024,
            max_object_bytes:1024*1024,
        };
        let prepared={
            let inner=source.inner.lock().await;
            mutation::prepare_at(
                &inner.state,&inner.config,inner.snapshot_bytes,
                write("first-remote","a","alpha remote first",ExpectedVersion::Absent),
                current_epoch(),inner.evaluation_time_ms,
            ).unwrap()
        };
        let (applied,bytes)=match prepared {
            mutation::PreparedWrite::Append{applied,bytes,..}=>(applied,bytes),
            mutation::PreparedWrite::Replay(_)=>panic!("unexpected replay"),
        };
        remote.commit(&applied,&bytes).await.unwrap();
        drop(source);
        std::fs::remove_dir_all(&source_path).unwrap();

        let recovered=RecordPartition::open_s3_with_remote(
            &target_path,config().max_journal_bytes,remote,
        ).await.unwrap();
        settle_maintenance(&recovered).await;
        assert_eq!(recovered.get("a").await.unwrap().unwrap().document.unwrap()["body"],"alpha remote first");
        recovered.write(write("second-remote","b","alpha remote second",ExpectedVersion::Absent)).await.unwrap();
        let revision=write("update-remote","a","alpha remote updated",ExpectedVersion::Match(1));
        recovered.write(revision).await.unwrap();
        let mut tombstone=write("delete-remote","b","",ExpectedVersion::Match(1));
        tombstone.document=None;
        recovered.write(tombstone).await.unwrap();
        drop(recovered);
        std::fs::remove_dir_all(&target_path).unwrap();

        // Reopen from a missing local cache. SQLite/S3 bootstrap plus committed
        // create/update/delete entries must reconstruct the same queryable Record view.
        let authority=SqliteAuthority::open(&authority_path,Duration::from_secs(1)).unwrap();
        let remote=RemotePersistence{ _owner:None,
            store:S3CompatibleStore::from_store(object_store,1024*1024),
            authority:tokio::sync::Mutex::new(authority),
            partition_id:"record-rebuild".into(),
            owner_epoch:1,
            max_tail_blocks:32,
            max_tail_bytes:1024*1024,
            max_object_bytes:1024*1024,
        };
        let recovered=RecordPartition::open_s3_with_remote(
            &target_path,config().max_journal_bytes,remote,
        ).await.unwrap();
        assert_eq!(recovered.get("a").await.unwrap().unwrap().document.unwrap()["body"],"alpha remote updated");
        assert!(recovered.get("b").await.unwrap().is_none());
        assert_eq!(recovered.statistics().await.unwrap().visible_seq,4);
        drop(recovered);
        std::fs::remove_dir_all(&target_path).unwrap();
        let _=std::fs::remove_file(&authority_path);
        let _=std::fs::remove_file(authority_path.with_extension("sqlite-wal"));
        let _=std::fs::remove_file(authority_path.with_extension("sqlite-shm"));
    }

    #[cfg(feature = "s3-object-store")]
    #[tokio::test]
    async fn sqlite_committed_remote_tail_recovers_before_local_journal_append() {
        use crate::storage::{s3_compatible::S3CompatibleStore, sqlite_authority::SqliteAuthority};
        use object_store::memory::InMemory;
        use std::{sync::Arc, time::Duration};

        let path = path();
        let authority_path = path.with_extension("authority.sqlite");
        let object_store = Arc::new(InMemory::new());
        let store = S3CompatibleStore::from_store(object_store.clone(), 1024 * 1024);
        let mut authority = SqliteAuthority::open(&authority_path, Duration::from_secs(1)).unwrap();
        authority.create_partition("record-test", 1).unwrap();

        let partition = RecordPartition::create(&path, config()).await.unwrap();
        {
            let inner = partition.inner.lock().await;
            let checkpoint = encode_entry(&Entry::Checkpoint {
                config: inner.config.clone(),
                state: inner.state.clone(),
            }).unwrap();
            drop(inner);
            store.publish_bootstrap(
                &mut authority, "record-test", 1, &checkpoint, unix_time_ms_i64(),
            ).await.unwrap();
        }
        // Native packages add postings/positions/fast-field files; this test covers
        // remote reconciliation, not a 32-object capacity limit. Keep the limit finite.
        let mut remote = RemotePersistence { _owner:None,
            store,
            authority: tokio::sync::Mutex::new(authority),
            partition_id: "record-test".into(),
            owner_epoch: 1,
            max_tail_blocks: 128,
            max_tail_bytes: 1024 * 1024,
            max_object_bytes: 1024 * 1024,
        };
        {
            let mut inner = partition.inner.lock().await;
            let prepared = mutation::prepare_at(
                &inner.state, &inner.config, inner.snapshot_bytes,
                write("remote-tail", "remote-record", "alpha remote durable", ExpectedVersion::Absent),
                current_epoch(), inner.evaluation_time_ms,
            ).unwrap();
            let (applied, bytes) = match prepared {
                mutation::PreparedWrite::Append { applied, bytes, .. } => (applied, bytes),
                mutation::PreparedWrite::Replay(_) => panic!("unexpected replay"),
            };
            // Simulate a crash after the SQLite commit point but before the local
            // journal append/index publication. Recovery must reconstruct the tail.
            remote.commit(&applied, &bytes).await.unwrap();
        }
        drop(partition);

        let recovered = RecordPartition::open(&path, config().max_journal_bytes).await.unwrap();
        let authority = SqliteAuthority::open(&authority_path, Duration::from_secs(1)).unwrap();
        let store = S3CompatibleStore::from_store(object_store.clone(), 1024 * 1024);
        let mut remote = RemotePersistence { _owner:None,
            store, authority: tokio::sync::Mutex::new(authority), partition_id: "record-test".into(),
            owner_epoch: 1, max_tail_blocks: 128, max_tail_bytes: 1024 * 1024, max_object_bytes: 1024 * 1024,
        };
        {
            let mut inner = recovered.inner.lock().await;
            remote.reconcile_journal(&mut inner).await.unwrap();
        }
        drop(recovered);
        let recovered = RecordPartition::open(&path, config().max_journal_bytes).await.unwrap();
        {
            let mut inner = recovered.inner.lock().await;
            assert_eq!(inner.state.seq, 1);
            inner.remote = Some(remote);
        }
        settle_maintenance(&recovered).await;
        let record = recovered.get("remote-record").await.unwrap().unwrap();
        assert_eq!(record.version, 1);
        assert_eq!(record.document.unwrap()["body"], "alpha remote durable");

        // A normal RecordPartition write must pass through the same remote commit
        // authority, then publish locally.
        let receipt = recovered.write(write(
            "remote-write", "remote-record-2", "second remote durable",
            ExpectedVersion::Absent,
        )).await.unwrap();
        assert_eq!(receipt.seq, 2);
        let second = recovered.get("remote-record-2").await.unwrap().unwrap();
        assert_eq!(second.document.unwrap()["body"], "second remote durable");
        {
            let mut inner = recovered.inner.lock().await;
            let future = inner.state.time_watermark_ms
                .saturating_add(9 * 86_400_000);
            inner.clock = Arc::new(TestClock(std::sync::atomic::AtomicU64::new(future)));
            assert!(matches!(inner.trim_feed(future).await, Err(RecordError::Busy)));
        }
        recovered.kick_maintenance().await;
        settle_maintenance(&recovered).await;
        // Feed trimming publishes a complete remote checkpoint at this same
        // sequence; it must survive removal of the rebuildable local cache.
        assert!(recovered.inner.lock().await.state.feed.floor > 0);
        let (bundle_json, visible_seq, partition_id) =
            recovered.create_s3_backup("record-backup", 200, 1024 * 1024).await.unwrap();
        assert_eq!(visible_seq, 2);
        assert_eq!(partition_id, "record-test");
        let bundle: crate::storage::s3_compatible::BackupBundle =
            serde_json::from_str(&bundle_json).unwrap();
        assert_eq!(bundle.descriptor.backup_id, RemotePersistence::safe_backup_id("record-test:record-backup"));

        drop(recovered);
        let _ = std::fs::remove_dir_all(&path);
        let _ = std::fs::remove_file(&authority_path);
        let _ = std::fs::remove_file(authority_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(authority_path.with_extension("sqlite-shm"));
    }
}

#[cfg(test)]
impl Inner {
    async fn query_view(&self, query: RecordQuery) -> Result<RecordSearch, RecordError> {
        let RecordQuery {
            temporal,
            text,
            vector,
            mode,
            query_type,
            offset,
            length: limit,
            result_type,
            fields,
            mut filters,
            sort,
        } = query;
                if offset
                    .checked_add(limit)
                    .is_none_or(|n| n > self.config.max_candidates)
                {
                    return Err(RecordError::Rejected("candidate budget exceeded".into()));
                }
                if let SearchMode::WeightedHybrid { rrf, .. } = &mode {
                    if rrf
                        .keyword_candidates
                        .is_some_and(|n| n > self.config.max_candidates)
                        || rrf
                            .vector_candidates
                            .is_some_and(|n| n > self.config.max_candidates)
                    {
                        return Err(RecordError::Rejected(
                            "fusion candidate budget exceeded".into(),
                        ));
                    }
                }
                if let Some(temporal) = temporal { filters.extend(temporal::filters(temporal)?); }
                let channel=match &mode{SearchMode::Lexical=>Some(if text.trim().is_empty(){"browse"}else{"lexical"}),SearchMode::Vector{..}=>Some("vector"),_=>None};
                let rank_basis=if !sort.is_empty(){"requested_order"}else if channel==Some("browse"){"browse_order"}else{"score"};
                let mut result = self
                    .index
                    .search(
                        text,
                        vector,
                        query_type,
                        mode,
                        offset,
                        limit,
                        result_type,
                        fields,
                        filters,
                        sort,
                        lexical::Cut { sequence:self.state.seq,evaluation_time_ms:self.evaluation_time_ms },
                    )
                    .await;
                if let Some(error) = result.error {
                    return Err(storage(error));
                }
                let mut hits = Vec::new();
                for (rank,hit) in result.results.into_iter().enumerate() {
                    let id = self
                        .ordinals
                        .get(hit.doc_id)
                        .ok_or_else(|| storage("candidate ordinal outside view"))?;
                    let channels=if let Some(channel)=channel {vec![crate::search::ChannelExplanation{channel:channel.into(),rank:offset+rank,rank_basis:rank_basis.into(),raw_score:if channel=="browse"{None}else{Some(hit.score)},rrf_contribution:None}]}else{result.explanations.remove(&hit.doc_id).ok_or_else(||storage("hybrid candidate explanation missing"))?};
                    hits.push(RecordHit {
                        channels,
                        record: self.state.records[id].clone(),
                        score: hit.score,
                    });
                }
                Ok(RecordSearch {
                    fusion: None,
                    evaluation_time_ms: self.evaluation_time_ms,
                    count_total: result.result_count_total,
                    query_terms: result.query_terms,
                    visible_seq: self.state.seq,
                    hits,
                    statistics: self.statistics(),
                })
    }
}
