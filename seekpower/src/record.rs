//! Single-partition reliable-record prototype backed by the existing retrieval
//! engine. This module is not yet the HTTP service's storage path. See
//! docs/implementation-57.md for the remaining P1-A integration gates.
use crate::{
    commit::Commit,
    index::{Document, IndexArc, IndexDocuments, IndexMetaObject, SchemaField, create_index},
    search::{FacetFilter, QueryType, ResultType, Search, SearchMode},
    storage::journal::{CreateError, Journal},
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

/// Failures distinguish rejected requests from operations that require recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordError {
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
    /// Existing retrieval configuration; the prototype always uses one shard.
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
    /// Maximum authoritative journal size. No rotation in this prototype.
    pub max_journal_bytes: u64,
    /// Candidate/final-result bound for this reference implementation.
    pub max_candidates: usize,
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
    seq: u64,
    epoch_watermark: u64,
    records: BTreeMap<String, Record>,
    // Vec avoids JSON's restriction on structured map keys. Bounded by config.
    operations: Vec<Applied>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Entry {
    Checkpoint { config: RecordConfig, state: State },
    Applied(Applied),
}
struct Inner {
    path: PathBuf,
    config: RecordConfig,
    state: State,
    index: IndexArc,
    ordinals: Vec<String>,
    healthy: bool,
    #[cfg(test)]
    fail_after_durable: bool,
    // Fields drop in declaration order: release all index/mmap handles before
    // releasing the partition lock, so a concurrent opener can clean caches.
    journal: Journal,
}
/// Serial single-partition reference implementation. Queries and writes share
/// one guard through candidate retrieval and authoritative record lookup.
#[derive(Clone)]
pub struct RecordPartition {
    inner: Arc<Mutex<Inner>>,
    admission: Arc<Semaphore>,
}
/// A record hit from one complete visible position.
#[derive(Clone, Debug)]
pub struct RecordHit {
    /// The exact original record corresponding to the ranked candidate.
    pub record: Record,
    /// Existing engine score (BM25, vector, or RRF according to query mode).
    pub score: f32,
}
/// Search response; hybrid/ANN counts are deliberately not called exact totals.
#[derive(Clone, Debug)]
pub struct RecordSearch {
    /// Common visible position for candidates and records.
    pub visible_seq: u64,
    /// Ranked effective records.
    pub hits: Vec<RecordHit>,
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
    #[cfg(test)]
    if std::env::var_os("SEEKPOWER_RECORD_BUILD_GATE").as_deref() == Some(path.as_os_str()) {
        std::fs::write(path.join("build-entered"), b"ready").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !path.join("build-release").exists() {
            assert!(std::time::Instant::now() < deadline, "build gate timed out");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    let records: Vec<_> = state
        .records
        .values()
        .filter(|r| r.document.is_some())
        .collect();
    for record in &records {
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
        || state
            .operations
            .iter()
            .any(|o| o.request.operation_id == r.operation_id)
    {
        return Err(storage("inconsistent committed record operation"));
    }
    state.records.insert(
        r.record_id.clone(),
        Record {
            record_id: r.record_id.clone(),
            routing_key: r.routing_key.clone(),
            version: applied.receipt.version,
            document: r.document.clone(),
        },
    );
    state.seq = applied.receipt.seq;
    state.epoch_watermark = state.epoch_watermark.max(applied.observed_epoch);
    state.operations.push(applied.clone());
    Ok(())
}
impl RecordPartition {
    /// Create a new authoritative partition. Parent directory must exist.
    pub async fn create(path: &Path, config: RecordConfig) -> Result<Self, RecordError> {
        let path = path.to_owned();
        crate::INDEX_RUNTIME
            .spawn(async move { Self::create_owned(&path, config).await })
            .await
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?
    }

    async fn create_owned(path: &Path, config: RecordConfig) -> Result<Self, RecordError> {
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
            return Err(RecordError::Rejected(
                "invalid reference-engine limits".into(),
            ));
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
        let state = State::default();
        let entry = encode_entry(&Entry::Checkpoint {
            config: config.clone(),
            state: state.clone(),
        })?;
        Journal::validate_initial(entry.len(), config.max_journal_bytes)
            .map_err(|e| RecordError::Rejected(e.to_string()))?;
        let journal = Journal::create_initialized(path, config.max_journal_bytes, Some(&entry))
            .map_err(creation_error)?;
        let (index, ordinals) = build(path, &config, &state).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                journal,
                path: path.into(),
                config,
                state,
                index,
                ordinals,
                healthy: true,
                #[cfg(test)]
                fail_after_durable: false,
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
        let (journal, entries) = Journal::open(path, max_journal_bytes).map_err(storage)?;
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
        for bytes in entries {
            match serde_json::from_slice(&bytes).map_err(storage)? {
                Entry::Applied(applied) => apply(&mut state, &applied)?,
                Entry::Checkpoint { .. } => {
                    return Err(storage("unexpected checkpoint in operation stream"));
                }
            }
        }
        if state.records.len() > config.max_records
            || state.operations.len() > config.max_operations
        {
            return Err(storage("recovered state exceeds configured limits"));
        }
        let (index, ordinals) = build(path, &config, &state).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                journal,
                path: path.into(),
                config,
                state,
                index,
                ordinals,
                healthy: true,
                #[cfg(test)]
                fail_after_durable: false,
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
            .map_err(|_| RecordError::Rejected("partition queue full".into()))?;
        let inner = self.inner.clone();
        crate::INDEX_RUNTIME
            .spawn(async move {
                let _permit = permit;
                let mut inner = inner.lock().await;
                if !inner.healthy {
                    return Err(RecordError::RecoveryRequired);
                }
                let epoch = current_epoch().max(inner.state.epoch_watermark);
                if request.operation_id.epoch > epoch
                    || request.operation_id.epoch.saturating_add(1) < epoch
                {
                    return Err(RecordError::OperationExpired);
                }
                if request.record_id.is_empty()
                    || request.routing_key.is_empty()
                    || request.operation_id.nonce.is_empty()
                {
                    return Err(RecordError::Rejected("empty identity".into()));
                }
                if let Some(document) = &request.document {
                    validate_document_depth(document)?;
                }
                // serde_json::Value equality ignores object insertion order; the
                // whole semantic request is compared, not a collision-prone digest.
                if let Some(old) = inner
                    .state
                    .operations
                    .iter()
                    .find(|o| o.request.operation_id == request.operation_id)
                {
                    return if serde_json::to_value(&old.request).map_err(storage)?
                        == serde_json::to_value(&request).map_err(storage)?
                    {
                        Ok(old.receipt.clone())
                    } else {
                        Err(RecordError::Conflict(
                            "operation identity reused with another request".into(),
                        ))
                    };
                }
                if serde_json::to_vec(&request).map_err(storage)?.len()
                    > inner.config.max_record_bytes
                    || inner.state.operations.len() >= inner.config.max_operations
                {
                    return Err(RecordError::Rejected(
                        "record/operation budget exceeded".into(),
                    ));
                }
                let previous = inner.state.records.get(&request.record_id);
                if previous.is_some_and(|r| r.routing_key != request.routing_key) {
                    return Err(RecordError::Conflict("routing key cannot change".into()));
                }
                let matches = match request.expected {
                    ExpectedVersion::Any => true,
                    ExpectedVersion::Absent => previous.is_none_or(|r| r.document.is_none()),
                    ExpectedVersion::Match(v) => previous.is_some_and(|r| r.version == v),
                };
                if !matches {
                    return Err(RecordError::Conflict("version precondition failed".into()));
                }
                if previous.is_none() && inner.state.records.len() >= inner.config.max_records {
                    return Err(RecordError::Rejected("identity budget exceeded".into()));
                }
                if let Some(document) = &request.document {
                    crate::vector::validate_document_vectors(&*inner.index.read().await, document)
                        .map_err(RecordError::Rejected)?;
                }
                let receipt = WriteReceipt {
                    seq: inner
                        .state
                        .seq
                        .checked_add(1)
                        .ok_or_else(|| storage("sequence exhausted"))?,
                    version: previous
                        .map_or(Some(1), |r| r.version.checked_add(1))
                        .ok_or_else(|| storage("version exhausted"))?,
                    record_id: request.record_id.clone(),
                };
                let applied = Applied {
                    request,
                    receipt: receipt.clone(),
                    observed_epoch: epoch,
                };
                let mut next = inner.state.clone();
                apply(&mut next, &applied)?;
                let snapshot = encode_entry(&Entry::Checkpoint {
                    config: inner.config.clone(),
                    state: next.clone(),
                })?;
                if snapshot.len() > 16 * 1024 * 1024
                    || snapshot.len() as u64 + 32 > inner.config.max_journal_bytes
                {
                    return Err(RecordError::Rejected(
                        "complete checkpoint budget exceeded".into(),
                    ));
                }
                let bytes = encode_entry(&Entry::Applied(applied))?;
                inner
                    .journal
                    .validate_append(bytes.len())
                    .map_err(|e| RecordError::Rejected(e.to_string()))?;
                inner.healthy = false;
                inner
                    .journal
                    .append(&bytes)
                    .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
                #[cfg(test)]
                if inner.fail_after_durable {
                    return Err(RecordError::DurablePending(receipt.seq));
                }
                let (candidate, ordinals) = build(&inner.path, &inner.config, &next)
                    .await
                    .map_err(|_| RecordError::DurablePending(receipt.seq))?;
                let retired = PathBuf::from(&inner.index.read().await.index_path_string);
                inner.index = candidate;
                inner.ordinals = ordinals;
                inner.state = next;
                inner.healthy = true;
                // No reader can retain the retired index outside this guard. Drop
                // handles before removal, including on Windows.
                if let Err(error) = std::fs::remove_dir_all(retired) {
                    log::warn!("retired index cleanup failed: {error}");
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
            .map_err(|_| RecordError::Rejected("partition queue full".into()))?;
        let inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        let epoch = current_epoch().max(inner.state.epoch_watermark);
        if id.epoch.saturating_add(1) < epoch {
            return Err(RecordError::OperationExpired);
        }
        Ok(inner
            .state
            .operations
            .iter()
            .find(|o| &o.request.operation_id == id)
            .map(|o| o.receipt.clone()))
    }
    /// Get the exact live record from a complete published view.
    pub async fn get(&self, id: &str) -> Result<Option<Record>, RecordError> {
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Rejected("partition queue full".into()))?;
        let inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        Ok(inner
            .state
            .records
            .get(id)
            .filter(|r| r.document.is_some())
            .cloned())
    }
    /// Search the existing lexical/vector/hybrid engine and read original records
    /// under the same partition guard. Each index contains only live versions.
    pub async fn search(
        &self,
        text: String,
        vector: Option<Embedding>,
        mode: SearchMode,
        filters: Vec<FacetFilter>,
        limit: usize,
    ) -> Result<RecordSearch, RecordError> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Rejected("partition queue full".into()))?;
        let owner = self.inner.clone();
        crate::INDEX_RUNTIME
            .spawn(async move {
                let _permit = permit;
                let inner = owner.lock().await;
                if !inner.healthy {
                    return Err(RecordError::RecoveryRequired);
                }
                if limit == 0 || limit > inner.config.max_candidates {
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
                        QueryType::Union,
                        mode,
                        true,
                        0,
                        limit,
                        ResultType::Topk,
                        false,
                        vec![],
                        filters,
                        vec![],
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
                    visible_seq: inner.state.seq,
                    hits,
                })
            })
            .await
            .map_err(storage)?
    }
    /// Create an independently recoverable snapshot at a new location. No live
    /// files are copied; original data, configuration, versions and dedup results
    /// are serialized under the same view. This does not yet truncate the WAL.
    pub async fn checkpoint(&self, destination: &Path) -> Result<u64, RecordError> {
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Rejected("partition queue full".into()))?;
        let inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        let payload = encode_entry(&Entry::Checkpoint {
            config: inner.config.clone(),
            state: inner.state.clone(),
        })?;
        Journal::validate_initial(payload.len(), inner.config.max_journal_bytes)
            .map_err(|e| RecordError::Rejected(e.to_string()))?;
        let _target = Journal::create_initialized(
            destination,
            inner.config.max_journal_bytes,
            Some(&payload),
        )
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
            operation_id: OperationId::new(nonce.into()),
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
    async fn record_depth_rejection_preserves_authority_and_accepted_depth_recovers() {
        let location = path();
        let snapshot = path();
        let partition = RecordPartition::create(&location, config()).await.unwrap();
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
        let wal = std::fs::read(location.join("wal")).unwrap();
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
            assert_eq!(std::fs::read(location.join("wal")).unwrap(), wal);
            assert_eq!(query(&partition, SearchMode::Lexical).await.visible_seq, 1);
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
        assert!(Journal::open(&location, config().max_journal_bytes).is_err());
        std::fs::write(location.join("build-release"), b"continue").unwrap();
        loop {
            if let Ok((journal, _)) = Journal::open(&location, config().max_journal_bytes) {
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
        assert!(Journal::open(&location, config().max_journal_bytes).is_err());
        std::fs::write(location.join("query-release"), b"continue").unwrap();
        loop {
            if let Ok((journal, _)) = Journal::open(&location, config().max_journal_bytes) {
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
            partition.write(expired).await.unwrap_err(),
            RecordError::OperationExpired
        );
        drop(partition);
        std::fs::remove_dir_all(path).unwrap();
    }
}
