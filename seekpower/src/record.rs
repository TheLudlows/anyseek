//! Single-partition incremental record engine backed by the existing retrieval
//! engine. The default HTTP service uses this authoritative storage path. See
//! docs/implementation-57.md for the bounded P1-A scope and validation evidence.
use crate::{
    commit::Commit,
    index::{Document, IndexArc, IndexDocuments, IndexMetaObject, SchemaField, create_index},
    search::{FacetFilter, QueryType, ResultSort, ResultType, Search, SearchMode},
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
mod output;
pub use evidence::{EvidenceKind, EvidenceMetadata, EvidenceRef, EvidenceVisibility};
pub use output::DataOutputPermit;
mod mutation;
mod publication;
mod recovery;
pub use recovery::{RecoveryIdentity, SealCertificate, SealRequest};

/// Failures distinguish rejected requests from operations that require recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordError {
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
        || config.max_records > 10_000
        || config.max_operations == 0
        || config.max_candidates == 0
        || config.max_record_bytes == 0
        || config.max_record_bytes > 1024 * 1024
    {
        return Err(RecordError::Rejected("invalid record-engine limits".into()));
    }
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
    /// Complete original input including original vector fields; None deletes.
    pub document: Option<Document>,
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
    /// Optional engine-owned source/evidence semantics; business document fields
    /// with the same name have no effect on visibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system: Option<EvidenceMetadata>,
}
/// A confirmed durable and published operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteReceipt {
    /// Logical partition operation position, independent of physical WAL frames.
    pub seq: u64,
    /// Record revision after this operation.
    pub version: u64,
    /// Business identity.
    pub record_id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Applied {
    request: RecordWrite,
    receipt: WriteReceipt,
    observed_epoch: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct State {
    lineage: String,
    sealed_for: Option<SealCertificate>,
    takeover_proof: Option<SealCertificate>,
    independent_snapshot: bool,
    seq: u64,
    epoch_watermark: u64,
    // Independent snapshots cannot prove the absence of later source deletes.
    // This persistent restore gate is deliberately not cleared by ordinary writes.
    evidence_quarantined: bool,
    records: BTreeMap<String, Record>,
    source_revisions: Vec<Record>,
    // Vec avoids JSON's restriction on structured map keys. Bounded by config.
    operations: Vec<Applied>,
    #[serde(skip)]
    operation_lookup: BTreeMap<OperationId, usize>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Entry {
    #[serde(rename = "RecordCheckpointV4")]
    Checkpoint { config: RecordConfig, state: State },
    #[serde(rename = "RecordAppliedV4")]
    Applied(Applied),
}
struct Inner {
    config: RecordConfig,
    state: State,
    index: IndexArc,
    ordinals: Vec<String>,
    locations: BTreeMap<String, usize>,
    snapshot_bytes: usize,
    healthy: bool,
    output: Arc<std::sync::RwLock<output::OutputState>>,
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
/// Serial single-partition implementation. Queries and writes share
/// one guard through candidate retrieval and authoritative record lookup.
#[derive(Clone)]
pub struct RecordPartition {
    inner: Arc<Mutex<Inner>>,
    admission: Arc<Semaphore>,
    output: Arc<std::sync::RwLock<output::OutputState>>,
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
    /// Position shared by records and statistics.
    pub visible_seq: u64,
    /// Exact original active records.
    pub records: Vec<Record>,
    /// Continuation, absent at end of view.
    pub next: Option<RecordCursor>,
    /// Active statistics captured under the same guard.
    pub statistics: RecordStatistics,
}
/// A record hit from one complete visible position.
#[derive(Clone, Debug, Serialize)]
pub struct RecordHit {
    /// The exact original record corresponding to the ranked candidate.
    pub record: Record,
    /// Existing engine score (BM25, vector, or RRF according to query mode).
    pub score: f32,
}
/// Retrieval options evaluated against one authoritative view.
pub struct RecordQuery {
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
    /// Mode-specific reported total, not an unconditional exact corpus count.
    pub count_total: usize,
    /// Analyzed query terms from this same search.
    pub query_terms: Vec<String>,
    /// Common visible position for candidates and records.
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
    /// Stable current identity.
    pub record_id: String,
    /// Current authority version, including tombstones.
    pub version: u64,
    /// Why ordinary reads include or hide this identity.
    pub visibility: EvidenceVisibility,
    /// Common authoritative view waterline.
    pub visible_seq: u64,
}

/// Common-view counts; retained physical versions are not live record counts.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct RecordStatistics {
    /// Authoritative position published to every retrieval component.
    pub visible_seq: u64,
    /// Non-tombstoned records contributing to scoring statistics.
    pub active_records: usize,
    /// Stable identities, including tombstones needed for ABA prevention.
    pub retained_identities: usize,
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
    fn statistics(&self) -> RecordStatistics {
        RecordStatistics {
            visible_seq: self.state.seq,
            active_records: self.locations.len(),
            retained_identities: self.state.records.len(),
            retained_operations: self.state.operations.len(),
            retained_source_revisions: self.state.source_revisions.len(),
            physical_versions: self.ordinals.len(),
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
    if let Some(path) = std::env::var_os("SEEKPOWER_RECORD_QUERY_GATE") {
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
            std::fs::remove_dir_all(entry.path()).map_err(storage)?;
        }
    }
    Ok(())
}

async fn detach(index: &IndexArc) {
    for shard in &index.read().await.shard_vec {
        shard.write().await.index_option = None;
    }
}
async fn build(
    path: &Path,
    config: &RecordConfig,
    state: &State,
) -> Result<(IndexArc, Vec<String>), RecordError> {
    let cache = path.join(format!("cache-{:032x}", rand::random::<u128>()));
    let index = create_index(
        &cache,
        config.index.clone(),
        &config.schema,
        11,
        true,
        Some(1),
    )
    .await
    .map_err(storage)?;
    // Break legacy ownership cycles so a rejected candidate can release mapped
    // files on Windows. The partition owns the root for the complete query.
    detach(&index).await;
    for shard in &index.read().await.shard_vec {
        shard.write().await.active = Some(crate::active::ActiveStatistics::default());
    }
    #[cfg(test)]
    if std::env::var_os("SEEKPOWER_RECORD_BUILD_GATE").as_deref() == Some(path.as_os_str()) {
        std::fs::write(path.join("build-entered"), b"ready").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !path.join("build-release").exists() {
            assert!(std::time::Instant::now() < deadline, "build gate timed out");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    let visible = evidence::visibility(&state.records)?;
    let records: Vec<_> = state.records.values().filter(|record|
        visible.get(&record.record_id) == Some(&true)
        && !(state.evidence_quarantined && record.system.is_some())).collect();
    for record in &records {
        index.read().await.shard_vec[0]
            .write()
            .await
            .active
            .as_mut()
            .unwrap()
            .keys
            .push(record.record_id.clone());
        index
            .index_documents(vec![record.document.clone().unwrap()])
            .await
            .map_err(storage)?;
    }
    index.commit().await.map_err(storage)?;
    Ok((
        index,
        records.into_iter().map(|r| r.record_id.clone()).collect(),
    ))
}
fn apply(state: &mut State, applied: &Applied) -> Result<(), RecordError> {
    if applied.receipt.seq != state.seq + 1 {
        return Err(storage("non-contiguous record sequence"));
    }
    let r = &applied.request;
    let previous = state.records.get(&r.record_id);
    if applied.receipt.version != previous.map_or(1, |r| r.version.saturating_add(1))
        || applied.receipt.record_id != r.record_id
        || previous.is_some_and(|p| p.routing_key != r.routing_key)
        || state.operation_lookup.contains_key(&r.operation_id)
    {
        return Err(storage("inconsistent committed record operation"));
    }
    if let Some(record) = previous.filter(|r| evidence::retained_revision(r)) {
        state.source_revisions.push(record.clone());
    }
    state.records.insert(
        r.record_id.clone(),
        Record {
            record_id: r.record_id.clone(),
            routing_key: r.routing_key.clone(),
            version: applied.receipt.version,
            document: r.document.clone(),
            system: r.system.clone().or_else(|| previous.and_then(|p| p.system.clone())),
        },
    );
    state.seq = applied.receipt.seq;
    state.epoch_watermark = state.epoch_watermark.max(applied.observed_epoch);
    state
        .operation_lookup
        .insert(r.operation_id.clone(), state.operations.len());
    state.operations.push(applied.clone());
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
    /// Create a new authoritative partition. Parent directory must exist.
    pub async fn create(path: &Path, config: RecordConfig) -> Result<Self, RecordError> {
        let path = path.to_owned();
        crate::INDEX_RUNTIME
            .spawn(async move { Self::create_owned(&path, config).await })
            .await
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?
    }

    async fn create_owned(path: &Path, mut config: RecordConfig) -> Result<Self, RecordError> {
        validate_config(&config)?;
        // Pin an encoding choice in persisted configuration, independent of
        // insertion order or which record survives compaction/recovery.
        if !config
            .schema
            .iter()
            .any(|field| field.index_lexical && field.longest)
        {
            if let Some(field) = config.schema.iter_mut().find(|field| field.index_lexical) {
                field.longest = true;
            }
        }
        let state = State { lineage: format!("{:032x}", rand::random::<u128>()), ..State::default() };
        let entry = encode_entry(&Entry::Checkpoint {
            config: config.clone(),
            state: state.clone(),
        })?;
        PartitionJournal::validate_initial(entry.len(), config.max_journal_bytes)
            .map_err(|e| RecordError::Rejected(e.to_string()))?;
        let journal = PartitionJournal::create(path, config.max_journal_bytes, &entry)
            .map_err(creation_error)?;
        let (index, ordinals) = build(path, &config, &state).await?;
        let output = Arc::new(std::sync::RwLock::new(output::OutputState::default()));
        Ok(Self {
            output: output.clone(),
            inner: Arc::new(Mutex::new(Inner {
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
                output,
                #[cfg(test)]
                fail_after_durable: false,
                #[cfg(test)]
                fail_publication_stage: 0,
            })),
            admission: Arc::new(Semaphore::new(8)),
        })
    }
    /// Recover complete authoritative state before serving any query. The budget
    /// is supplied before reading files, not trusted from a potentially bad file.
    pub async fn open(path: &Path, max_journal_bytes: u64) -> Result<Self, RecordError> {
        let path = path.to_owned();
        // The owned task retains the journal lock until every indexing task has
        // drained, even if the caller cancels recovery.
        crate::INDEX_RUNTIME
            .spawn(async move { Self::open_owned(&path, max_journal_bytes).await })
            .await
            .map_err(storage)?
    }

    async fn open_owned(path: &Path, max_journal_bytes: u64) -> Result<Self, RecordError> {
        let (mut journal, entries) =
            PartitionJournal::open(path, max_journal_bytes).map_err(storage)?;
        clean_recovery_caches(path)?;
        let mut entries = entries.into_iter();
        let first = entries
            .next()
            .ok_or_else(|| storage("missing initial checkpoint"))?;
        let Entry::Checkpoint { config, mut state } =
            serde_json::from_slice(&first).map_err(storage)?
        else {
            return Err(storage("missing initial checkpoint"));
        };
        journal
            .restrict_budget(config.max_journal_bytes)
            .map_err(storage)?;
        for (position, operation) in state.operations.iter().enumerate() {
            if state
                .operation_lookup
                .insert(operation.request.operation_id.clone(), position)
                .is_some()
            {
                return Err(storage("duplicate checkpoint operation identity"));
            }
        }
        for bytes in entries {
            match serde_json::from_slice(&bytes).map_err(storage)? {
                Entry::Applied(applied) => apply(&mut state, &applied)?,
                Entry::Checkpoint { .. } => {
                    return Err(storage("unexpected checkpoint in operation stream"));
                }
            }
        }
        evidence::validate_history(&state.records, &state.source_revisions).map_err(storage)?;
        if state.records.len() > config.max_records
            || state.source_revisions.len() + state.records.values().filter(|r| evidence::retained_revision(r)).count() > config.max_records
            || state.operations.len() > config.max_operations
        {
            return Err(storage("recovered state exceeds configured limits"));
        }
        let snapshot_bytes = encode_entry(&Entry::Checkpoint {
            config: config.clone(),
            state: state.clone(),
        })?
        .len();
        let (index, ordinals) = build(path, &config, &state).await?;
        let output = Arc::new(std::sync::RwLock::new(output::OutputState {
            revision: 0, fenced: state.sealed_for.is_some(),
        }));
        Ok(Self {
            output: output.clone(),
            inner: Arc::new(Mutex::new(Inner {
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
                snapshot_bytes,
                healthy: true,
                output,
                #[cfg(test)]
                fail_after_durable: false,
                #[cfg(test)]
                fail_publication_stage: 0,
            })),
            admission: Arc::new(Semaphore::new(8)),
        })
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
        crate::INDEX_RUNTIME
            .spawn(async move {
                let _permit = permit;
                let mut inner = inner.lock().await;
                if !inner.healthy || inner.state.sealed_for.is_some() {
                    return Err(RecordError::RecoveryRequired);
                }
                let (applied, bytes, snapshot_bytes, reserve_bytes) = match mutation::prepare(
                    &inner.state, &inner.config, inner.snapshot_bytes, request, current_epoch(),
                )? {
                    mutation::PreparedWrite::Replay(receipt) => return Ok(receipt),
                    mutation::PreparedWrite::Append { applied, bytes, snapshot_bytes, reserve_bytes } =>
                        (applied, bytes, snapshot_bytes, reserve_bytes),
                };
                inner.journal.validate_append_with_reserve(bytes.len(), reserve_bytes)
                    .map_err(|e| RecordError::Rejected(e.to_string()))?;
                let receipt = applied.receipt.clone();
                #[cfg(feature = "fault-injection")]
                let injected = std::env::var("SEEKPOWER_RECORD_FAULT_NONCE")
                    .ok()
                    .filter(|nonce| nonce == &applied.request.operation_id.nonce)
                    .and_then(|_| std::env::var("SEEKPOWER_RECORD_FAULT").ok());
                #[cfg(feature = "fault-injection")]
                if injected.as_deref() == Some("before_append") {
                    return Err(RecordError::Rejected("injected before append".into()));
                }
                let hides_evidence = inner.state.records.get(&applied.request.record_id).is_some_and(|r| r.system.is_some());
                inner.healthy = false;
                {
                    let mut output = inner.output.write().map_err(|_| RecordError::RecoveryRequired)?;
                    output.fenced = true;
                    if hides_evidence { output.revision = output.revision.checked_add(1)
                        .ok_or_else(|| storage("data barrier revision exhausted"))?; }
                }
                inner
                    .journal
                    .append(&bytes)
                    .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
                #[cfg(feature = "fault-injection")]
                match injected.as_deref() {
                    Some("after_append_unknown") => {
                        return Err(RecordError::OutcomeUnknown(
                            "injected uncertain append outcome".into(),
                        ));
                    }
                    Some("after_durable") => return Err(RecordError::DurablePending(receipt.seq)),
                    Some("crash_after_durable") => std::process::exit(86),
                    _ => {}
                }
                #[cfg(test)]
                if inner.fail_after_durable {
                    return Err(RecordError::DurablePending(receipt.seq));
                }
                inner.publish(&applied, snapshot_bytes).await?;
                inner.healthy = true;
                inner.output.write().map_err(|_| RecordError::RecoveryRequired)?.fenced = false;
                #[cfg(feature = "fault-injection")]
                if injected.as_deref() == Some("crash_after_visible") {
                    std::process::exit(86);
                }
                #[cfg(test)]
                if std::env::var("SEEKPOWER_RECORD_CRASH").as_deref() == Ok("published") {
                    std::process::exit(74);
                }
                Ok(receipt)
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
        let _permit = self.admission.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some() { return Err(RecordError::RecoveryRequired); }
        let Some(record) = inner.state.records.get(id) else { return Ok(None); };
        let visibility = if inner.state.evidence_quarantined && record.system.is_some() {
            EvidenceVisibility::Quarantined
        } else { evidence::status(&inner.state.records, id)? };
        Ok(Some(RecordStatus { record_id: id.into(), version: record.version, visibility, visible_seq: inner.state.seq }))
    }
    /// Search the existing lexical/vector/hybrid engine and read original records
    /// under the same partition guard. Retired candidates are filtered before TopK.
    pub async fn search(
        &self,
        text: String,
        vector: Option<Embedding>,
        mode: SearchMode,
        filters: Vec<FacetFilter>,
        limit: usize,
    ) -> Result<RecordSearch, RecordError> {
        self.query(RecordQuery {
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
    /// Full retrieval contract for service callers, using one published view.
    pub async fn query(&self, query: RecordQuery) -> Result<RecordSearch, RecordError> {
        let RecordQuery {
            text,
            vector,
            mode,
            query_type,
            offset,
            length: limit,
            result_type,
            fields,
            filters,
            sort,
        } = query;
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let owner = self.inner.clone();
        crate::INDEX_RUNTIME
            .spawn(async move {
                let _permit = permit;
                let inner = owner.lock().await;
                if !inner.healthy || inner.state.sealed_for.is_some() {
                    return Err(RecordError::RecoveryRequired);
                }
                if offset
                    .checked_add(limit)
                    .is_none_or(|n| n > inner.config.max_candidates)
                {
                    return Err(RecordError::Rejected("candidate budget exceeded".into()));
                }
                if let SearchMode::WeightedHybrid { rrf, .. } = &mode {
                    if rrf
                        .keyword_candidates
                        .is_some_and(|n| n > inner.config.max_candidates)
                        || rrf
                            .vector_candidates
                            .is_some_and(|n| n > inner.config.max_candidates)
                    {
                        return Err(RecordError::Rejected(
                            "fusion candidate budget exceeded".into(),
                        ));
                    }
                }
                let result = inner
                    .index
                    .search(
                        text,
                        vector,
                        query_type,
                        mode,
                        true,
                        offset,
                        limit,
                        result_type,
                        true,
                        fields,
                        filters,
                        sort,
                    )
                    .await;
                if let Some(error) = result.error {
                    return Err(storage(error));
                }
                let mut hits = Vec::new();
                for hit in result.results {
                    let id = inner
                        .ordinals
                        .get(hit.doc_id)
                        .ok_or_else(|| storage("candidate ordinal outside view"))?;
                    hits.push(RecordHit {
                        record: inner.state.records[id].clone(),
                        score: hit.score,
                    });
                }
                Ok(RecordSearch {
                    count_total: result.result_count_total,
                    query_terms: result.query_terms,
                    visible_seq: inner.state.seq,
                    hits,
                    statistics: inner.statistics(),
                })
            })
            .await
            .map_err(storage)?
    }
    /// Iterate active records by stable identity. Changing the logical position
    /// invalidates continuation rather than silently skipping or duplicating rows.
    pub async fn page(
        &self,
        cursor: Option<RecordCursor>,
        limit: usize,
    ) -> Result<RecordPage, RecordError> {
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
        crate::INDEX_RUNTIME
            .spawn(async move {
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
        crate::INDEX_RUNTIME
            .spawn(async move { PartitionJournal::retire_snapshot(&path).map_err(creation_error) })
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

    /// Publish a complete local checkpoint without rebuilding the serving
    /// index, then retire the old WAL generation. Retry identities, tombstones,
    /// original input and configuration remain in the complete snapshot.
    pub async fn compact_journal(&self) -> Result<u64, RecordError> {
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let mut inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        let payload = encode_entry(&Entry::Checkpoint {
            config: inner.config.clone(),
            state: inner.state.clone(),
        })?;
        inner.healthy = false;
        match inner.journal.checkpoint(&payload) {
            Ok(()) => {
                inner.healthy = true;
                Ok(inner.state.seq)
            }
            Err(error @ CreateError::NotPublished(_)) => {
                inner.healthy = true;
                Err(creation_error(error))
            }
            Err(error) => Err(creation_error(error)),
        }
    }

    /// Create an independently recoverable snapshot at a new location. No live
    /// files are copied; original data, configuration, versions and dedup results
    /// are serialized under the same view. Use `compact_journal` for live WAL retirement.
    pub async fn checkpoint(&self, destination: &Path) -> Result<u64, RecordError> {
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        let mut snapshot_state = inner.state.clone();
        // A snapshot cut cannot attest to deletions that occur after that cut.
        // Preserve originals, but do not silently revive typed evidence when the
        // snapshot is opened/restored under a different owner/root.
        snapshot_state.independent_snapshot = true;
        snapshot_state.evidence_quarantined |= snapshot_state.records.values().any(|r| r.system.is_some());
        let payload = encode_entry(&Entry::Checkpoint {
            config: inner.config.clone(),
            state: snapshot_state,
        })?;
        PartitionJournal::validate_initial(payload.len(), inner.config.max_journal_bytes)
            .map_err(|e| RecordError::Rejected(e.to_string()))?;
        let _target =
            PartitionJournal::create(destination, inner.config.max_journal_bytes, &payload)
                .map_err(creation_error)?;
        Ok(inner.state.seq)
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
    fn path() -> PathBuf {
        let base = std::env::var_os("SEEKPOWER_TEST_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&base).unwrap();
        base.join(format!("seekpower-record-{:032x}", rand::random::<u128>()))
    }
    fn config() -> RecordConfig {
        RecordConfig {index:IndexMetaObject{id:0,name:"records".into(),lexical_similarity:LexicalSimilarity::Bm25f,
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
    fn write(nonce: &str, id: &str, body: &str, expected: ExpectedVersion) -> RecordWrite {
        RecordWrite {
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
    async fn query(p: &RecordPartition, mode: SearchMode) -> RecordSearch {
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
                while p.admission.available_permits() == 8 { tokio::task::yield_now().await; }
                task.abort(); let _ = task.await; drop(guard);
                // The accepted owned task retains the permit until publication.
                while p.admission.available_permits() != 8 { tokio::task::yield_now().await; }
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
    fn source_write(nonce: &str, id: &str, dependencies: &[(&str, u64)]) -> RecordWrite {
        let mut request = write(nonce, id, "evidence original", ExpectedVersion::Absent);
        request.system = Some(EvidenceMetadata {
            kind: if dependencies.is_empty() { EvidenceKind::Source } else { EvidenceKind::Derived },
            evidence_refs: dependencies.iter().map(|(id, version)| EvidenceRef {
                record_id: (*id).into(), routing_key: "document-1".into(), version: *version,
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
        let legacy = String::from_utf8(checkpoint).unwrap().replacen("RecordCheckpointV4", "Checkpoint", 1);
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
                0 => p.inner.lock().await.index.commit().await.unwrap(),
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
        let occupied = p.admission.clone().acquire_many_owned(8).await.unwrap();
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
        let location = PathBuf::from(std::env::var_os("SEEKPOWER_RECORD_BUILD_GATE").unwrap());
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
            .env("SEEKPOWER_RECORD_BUILD_GATE", &location)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        std::fs::remove_dir_all(location).unwrap();
    }

    #[tokio::test]
    #[ignore = "invoked only by parent query cancellation test"]
    async fn record_cancelled_query_worker() {
        let location = PathBuf::from(std::env::var_os("SEEKPOWER_RECORD_QUERY_GATE").unwrap());
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
            .env("SEEKPOWER_RECORD_QUERY_GATE", &location)
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
            .index
            .read()
            .await
            .index_path_string
            .clone();
        for round in 1..=2 {
            assert_eq!(partition.compact_journal().await.unwrap(), 3);
            assert_eq!(
                partition
                    .inner
                    .lock()
                    .await
                    .index
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
        let location = PathBuf::from(std::env::var_os("SEEKPOWER_CHECKPOINT_PATH").unwrap());
        if std::env::var_os("SEEKPOWER_PARTITION_RECOVERY_FAIL").is_some() {
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
        if std::env::var_os("SEEKPOWER_CHECKPOINT_EXPECT_UNKNOWN").is_some() {
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
        if std::env::var_os("SEEKPOWER_PARTITION_RECOVER").is_none() {
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
                .env("SEEKPOWER_CHECKPOINT_PATH", &location)
                .env("SEEKPOWER_JOURNAL_CRASH", stage)
                .env("SEEKPOWER_PARTITION_CRASH", stage)
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
                    .env("SEEKPOWER_CHECKPOINT_PATH", &location);
                command
            };
            let output = worker()
                .env("SEEKPOWER_JOURNAL_CRASH", "initial_published")
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(73));
            let output = worker()
                .env("SEEKPOWER_PARTITION_CRASH", stage)
                .env("SEEKPOWER_PARTITION_RECOVER", "1")
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
        // Seed a realistic retired physical history through the actual engine,
        // without 65K unrelated WAL fsyncs. The final boundary crossing itself
        // uses the authoritative Record write path and its publication guard.
        {
            let mut inner = partition.inner.lock().await;
            let shard = inner.index.read().await.shard_vec[0].clone();
            for id in 1..65536 {
                shard
                    .write()
                    .await
                    .active
                    .as_mut()
                    .unwrap()
                    .keys
                    .push("a".into());
                inner
                    .index
                    .index_documents(vec![original.document.clone().unwrap()])
                    .await
                    .unwrap();
                let mut shard = shard.write().await;
                shard.delete_hashset.insert(id - 1);
                shard.active.as_mut().unwrap().retire(id - 1);
                inner.ordinals.push("a".into());
            }
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
        partition.inner.lock().await.index.commit().await.unwrap();
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
                .env("SEEKPOWER_CHECKPOINT_PATH", &location)
                .env("SEEKPOWER_PUBLISH_FAIL", "after_rename");
            command
        };
        let output = worker()
            .env("SEEKPOWER_CHECKPOINT_EXPECT_UNKNOWN", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(77), "{output:?}");
        // Interrupted recovery retirement also must not serve or erase the
        // old certified authority; a subsequent recovery is safe.
        let output = worker()
            .env("SEEKPOWER_PARTITION_RECOVERY_FAIL", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(78), "{output:?}");
        // The leftover .prepare itself must pass a fresh retirement sync,
        // rather than being deleted merely because its name is now invisible.
        let output = worker()
            .env("SEEKPOWER_PARTITION_RECOVERY_FAIL", "1")
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
            .index
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
        {
            let inner = partition.inner.lock().await;
            let index = inner.index.read().await;
            let shard = index.shard_vec[0].read().await;
            let sorts = vec![];
            let mut heap = crate::min_heap::MinHeap::new(1, &shard, false, &sorts);
            heap.add_topk(
                crate::min_heap::Result {
                    doc_id: 0,
                    score: 1.0,
                    ..Default::default()
                },
                1,
            );
            assert!(!heap.can_prune(1.0, 3, 1)); // physical 3 is logical "a", better than "z"
            assert!(heap.can_prune(0.5, 3, 1));
        }
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
                        .index
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
                partition.inner.lock().await.index.commit().await.unwrap();
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
            partition.inner.lock().await.index.commit().await.unwrap();
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
                partition.inner.lock().await.index.commit().await.unwrap();
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
        let path = PathBuf::from(std::env::var_os("SEEKPOWER_RECORD_PATH").unwrap());
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
                .env("SEEKPOWER_RECORD_PATH", &path)
                .env("SEEKPOWER_JOURNAL_CRASH", stage)
                .env("SEEKPOWER_RECORD_CRASH", stage)
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
}
