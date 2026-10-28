//! Durable intake, canonical Record outcomes and index progress are separate.
//! Record and index materializers never own the intake journal lock.
use super::*;
use futures::FutureExt;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "s3-object-store")]
#[path = "ingestion_remote.rs"]
mod remote;

const BATCH_RECORDS: usize = 64;
const BATCH_BYTES: usize = 4 * 1024 * 1024;

/// Receipt for a durably accepted command, not a successful conditional update.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestReceipt {
    /// Sequence in this intake stream; distinct from the materialized Record seq.
    pub durable_seq: u64,
    /// Original identity used for submission retries and outcome lookup.
    pub operation_id: OperationId,
}
/// A terminal validation/conditional error produced by the materializer.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestFailure {
    /// Stable error category.
    pub code: String,
    /// Validation detail, without a document body.
    pub message: String,
}
/// Outcome of one accepted command. Missing completion means pending.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestStatus {
    /// Durable intake receipt.
    pub accepted: IngestReceipt,
    /// Present after canonical Record materialization; indexes may still lag.
    pub receipt: Option<WriteReceipt>,
    /// Present after a definite rejection; acceptance itself is not rolled back.
    pub error: Option<IngestFailure>,
}
impl IngestStatus {
    fn completed(&self) -> bool {
        self.receipt.is_some() || self.error.is_some()
    }
}
/// Independent intake/materialization progress. Completion includes rejections.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct IngestProgress {
    /// Highest durably accepted command.
    pub durable_seq: u64,
    /// Highest consecutive command with a durable terminal outcome.
    pub completed_seq: u64,
    /// Last successful Record receipt sequence (a different sequence space).
    pub materialized_seq: u64,
    /// Record sequence applied by the independently published lexical cache.
    pub lexical_seq: u64,
    /// Record sequence applied by vector/metadata (not an intake sequence).
    pub vector_seq: u64,
    /// A derived-index failure; reopen rebuilds from the durable Record log.
    pub index_error: Option<String>,
    /// Accepted commands without a terminal outcome.
    pub pending: usize,
    /// Bytes retained in the intake WAL, including control metadata.
    pub retained_bytes: u64,
    /// An uncertain materializer/persistence failure requires reopening.
    pub blocked: Option<String>,
}
#[derive(Serialize, Deserialize)]
enum Event {
    Reset { base: u64, epoch_floor: u64, materialized_seq: u64 },
    Retain { digest: [u8; 32], status: IngestStatus },
    Accept {
        first: u64,
        requests: Vec<RecordWrite>,
    },
    Complete {
        first: u64,
        results: Vec<Result<WriteReceipt, IngestFailure>>,
    },
}
struct Row {
    request: Option<RecordWrite>,
    digest: [u8; 32],
    status: IngestStatus,
    _memory: crate::storage::build_budget::MemoryLease,
}
struct Spool {
    log: DurableLog,
    rows: Vec<Row>,
    operations: BTreeMap<OperationId, usize>,
    completed: usize,
    base: u64,
    epoch_floor: u64,
    completed_since_compaction: usize,
    materialized_seq: u64,
    completion_reserve: u64,
    max_pending: usize,
    max_operations: usize,
    max_record_bytes: usize,
    blocked: Option<String>,
}
enum DurableLog {
    Local(Arc<std::sync::Mutex<ingest_log::Log>>),
    #[cfg(feature = "s3-object-store")]
    Remote(remote::Log),
}
impl DurableLog {
    fn bytes(&self) -> u64 {
        match self {
            Self::Local(log) => log.lock().map(|log| log.bytes()).unwrap_or(u64::MAX),
            #[cfg(feature = "s3-object-store")]
            Self::Remote(log) => log.bytes(),
        }
    }
    async fn append(&mut self, event: &Event) -> Result<(), RecordError> {
        match self {
            Self::Local(log) => {
                let log = log.clone();
                let bytes = serde_json::to_vec(event).map_err(storage)?;
                tokio::task::spawn_blocking(move || {
                    log.lock()
                        .map_err(|_| RecordError::RecoveryRequired)?
                        .append_serialized(&bytes)
                })
                .await
                .map_err(|error| RecordError::OutcomeUnknown(error.to_string()))?
            }
            #[cfg(feature = "s3-object-store")]
            Self::Remote(log) => log.append(event).await,
        }
    }
    async fn compact(&mut self, events: Vec<Vec<u8>>, completion_slots: u64, completion_reserve: u64) -> Result<bool, RecordError> {
        match self {
            Self::Local(log) => {
                let log = log.clone();
                let completion_bytes = completion_headroom(completion_reserve, 1)?;
                tokio::task::spawn_blocking(move || {
                    log.lock().map_err(|_| RecordError::RecoveryRequired)?.compact(&events, completion_bytes)
                }).await.map_err(|e| RecordError::OutcomeUnknown(e.to_string()))??;
                Ok(true)
            }
            #[cfg(feature = "s3-object-store")]
            Self::Remote(log) => { log.compact(events, completion_slots, completion_headroom(completion_reserve, 5)?).await?; Ok(true) },
        }
    }
    fn admit_accept(&self, bytes: u64, completion_reserve: u64, completion_slots: u64) -> Result<(), RecordError> {
        let (available, factor) = match self {
            Self::Local(log) => (log.lock().map(|log| log.available()).unwrap_or(0), 1),
            #[cfg(feature = "s3-object-store")]
            Self::Remote(log) => {
                log.admit_objects(completion_slots.checked_add(1).ok_or(RecordError::Busy)?)?;
                (log.available(), 5)
            },
        };
        if bytes
            .checked_add(completion_reserve)
            .and_then(|n| n.checked_mul(factor))
            .and_then(|n| n.checked_add(4096))
            .is_none_or(|n| n > available)
        {
            Err(RecordError::Busy)
        } else {
            Ok(())
        }
    }
}
// Match acceptance's backend expansion factor and fixed WAL rollover margin.
// Empty pending sets need no completion space reservation.
fn completion_headroom(reserve: u64, factor: u64) -> Result<u64, RecordError> {
    if reserve == 0 { return Ok(0); }
    reserve.checked_mul(factor).and_then(|n| n.checked_add(4096)).ok_or(RecordError::Busy)
}
fn request_digest(request: &RecordWrite) -> Result<[u8; 32], RecordError> {
    // Preserve structural JSON equality: object insertion order is irrelevant.
    let mut value = serde_json::to_value(request).map_err(storage)?;
    value.sort_all_objects();
    fn normalize_zero(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Number(number) if number.is_f64() && number.as_f64() == Some(0.0) => {
                *number = serde_json::Number::from_f64(0.0).expect("finite zero");
            }
            serde_json::Value::Array(values) => values.iter_mut().for_each(normalize_zero),
            serde_json::Value::Object(values) => values.values_mut().for_each(normalize_zero),
            _ => {},
        }
    }
    normalize_zero(&mut value);
    checkpoint_codec::json_digest(&value)
}
impl Spool {
    async fn reclaim(&mut self, epoch: u64) -> Result<(), RecordError> {
        let floor = self.epoch_floor.max(epoch.saturating_sub(1));
        let expired = self.rows[..self.completed].iter()
            .take_while(|r| r.status.accepted.operation_id.epoch < floor).count();
        if expired == 0 && self.completed_since_compaction < 64.max(self.completed / 2) { return Ok(()); }
        let _phase = maintenance::Phase::new("intake_compaction_locked", self.base + self.completed as u64);
        let base = self.base.checked_add(expired as u64).ok_or_else(|| storage("intake sequence overflow"))?;
        let snapshot_bytes = self.rows[expired..].iter().try_fold(1024u64, |total, row| {
            let bytes = if let Some(request) = &row.request { checkpoint_codec::json_length(request)? }
                else { checkpoint_codec::json_length(&row.status)? + 256 };
            total.checked_add(bytes as u64).ok_or(RecordError::Busy)
        })?;
        let Ok(_snapshot_memory) = crate::storage::build_budget::reserve_memory(
            snapshot_bytes.checked_mul(34).ok_or(RecordError::Busy)?) else { return Ok(()); };
        let mut events = vec![serde_json::to_vec(&Event::Reset {
            base, epoch_floor: floor, materialized_seq: self.materialized_seq,
        }).map_err(storage)?];
        for row in &self.rows[expired..] {
            let event = if let Some(request) = &row.request {
                Event::Accept { first: row.status.accepted.durable_seq, requests: vec![request.clone()] }
            } else {
                Event::Retain { digest: row.digest, status: row.status.clone() }
            };
            events.push(serde_json::to_vec(&event).map_err(storage)?);
        }
        match self.log.compact(events, (self.rows.len() - self.completed) as u64, self.completion_reserve).await {
            Ok(true) => {},
            Ok(false) | Err(RecordError::Busy) => return Ok(()),
            Err(error) => return Err(error),
        }
        self.rows.drain(..expired);
        self.completed -= expired;
        self.base = base;
        self.epoch_floor = floor;
        self.completed_since_compaction = 0;
        self.operations = self.rows.iter().enumerate()
            .map(|(i, r)| (r.status.accepted.operation_id.clone(), i)).collect();
        Ok(())
    }
    fn apply(&mut self, event: Event) -> Result<(), RecordError> {
        match event {
            Event::Reset { base, epoch_floor, materialized_seq } => {
                if !self.rows.is_empty() || self.base != 0 || base.checked_add(self.max_operations as u64).is_none() { return Err(storage("intake reset after entries")); }
                self.base = base;
                self.epoch_floor = epoch_floor;
                self.materialized_seq = materialized_seq;
            }
            Event::Retain { digest, status } => {
                if !status.completed() || status.receipt.is_some() == status.error.is_some()
                    || self.rows.len() != self.completed
                    || self.rows.len() >= self.max_operations
                    || status.accepted.durable_seq != self.base + self.rows.len() as u64 + 1
                    || self.operations.contains_key(&status.accepted.operation_id) {
                    return Err(storage("invalid compacted intake ledger"));
                }
                let memory = crate::storage::build_budget::reserve_memory(
                    checkpoint_codec::json_length(&status)? as u64 * 32 + 1024).map_err(storage)?;
                self.operations.insert(status.accepted.operation_id.clone(), self.rows.len());
                self.rows.push(Row { request: None, digest, status, _memory: memory });
                self.completed += 1;
            }
            Event::Accept { first, requests } => {
                if requests.is_empty() || first != self.base + self.rows.len() as u64 + 1 {
                    return Err(storage("invalid intake batch sequence"));
                }
                for request in requests {
                    if self.rows.len() >= self.max_operations
                        || self.operations.contains_key(&request.operation_id)
                    {
                        return Err(storage("duplicate or oversized intake ledger"));
                    }
                    let bytes = checkpoint_codec::json_length(&request)? as u64;
                    let memory = crate::storage::build_budget::reserve_memory(
                        bytes
                            .checked_mul(32)
                            .ok_or_else(|| storage("intake residency overflow"))?,
                    )
                    .map_err(storage)?;
                    self.completion_reserve = self
                        .completion_reserve
                        .checked_add(bytes + 16384)
                        .ok_or_else(|| storage("intake completion reserve overflow"))?;
                    let accepted = IngestReceipt {
                        durable_seq: self.base + self.rows.len() as u64 + 1,
                        operation_id: request.operation_id.clone(),
                    };
                    self.operations
                        .insert(request.operation_id.clone(), self.rows.len());
                    self.rows.push(Row {
                        digest: request_digest(&request)?,
                        request: Some(request),
                        status: IngestStatus {
                            accepted,
                            receipt: None,
                            error: None,
                        },
                        _memory: memory,
                    });
                }
            }
            Event::Complete { first, results } => {
                if results.is_empty()
                    || first != self.base + self.completed as u64 + 1
                    || self.completed + results.len() > self.rows.len()
                {
                    return Err(storage("invalid intake completion sequence"));
                }
                for result in results {
                    let row = &mut self.rows[self.completed];
                    self.completion_reserve = self
                        .completion_reserve
                        .checked_sub(checkpoint_codec::json_length(row.request.as_ref().ok_or_else(|| storage("missing pending request"))?)? as u64 + 16384)
                        .ok_or_else(|| storage("invalid completion reserve"))?;
                    match result {
                        Ok(receipt) => {
                            self.materialized_seq = receipt.seq;
                            row.status.receipt = Some(receipt);
                        }
                        Err(error) => row.status.error = Some(error),
                    }
                    row.request = None;
                    row._memory = crate::storage::build_budget::reserve_memory(
                        checkpoint_codec::json_length(&row.status)? as u64 * 32 + 1024).map_err(storage)?;
                    self.completed += 1;
                    self.completed_since_compaction += 1;
                }
            }
        }
        Ok(())
    }
}
/// Append-only intake in front of an independently locked Record materializer.
/// Conditions are evaluated later; inspect status for the terminal outcome.
#[derive(Clone)]
pub struct IngestPipeline {
    spool: Arc<Mutex<Spool>>,
    running: Arc<AtomicBool>,
    worker: Arc<std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    partition: RecordPartition,
    #[cfg(test)]
    paused: Arc<AtomicBool>,
}
impl IngestPipeline {
    /// Open the collection's intake WAL and resume pending commands. S3 intake
    /// commits through a separate SQLite metadata stream, independent of caches.
    pub async fn open(partition: RecordPartition) -> Result<Self, RecordError> {
        let (root, config) = {
            let inner = partition.inner.lock().await;
            (inner.journal.root().join("intake"), inner.config.clone())
        };
        let identity = checkpoint_codec::json_digest(&config)?
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let segment_bytes = (8 * 1024 * 1024)
            .min(config.max_journal_bytes)
            .max(BATCH_BYTES as u64 + 4096);
        let max_bytes = config
            .max_journal_bytes
            .checked_mul(8)
            .ok_or_else(|| storage("intake byte budget overflow"))?;
        let local = || -> Result<(ingest_log::Log, Vec<Event>), RecordError> {
            Ok(if root.exists() {
                ingest_log::Log::open(&root, &identity, segment_bytes, max_bytes)?
            } else {
                (
                    ingest_log::Log::create(&root, &identity, segment_bytes, max_bytes)?,
                    Vec::new(),
                )
            })
        };
        #[cfg(feature = "s3-object-store")]
        let source = {
            let inner = partition.inner.lock().await;
            if let Some(source) = &inner.remote {
                Some((
                    source.store.clone(),
                    source.authority.lock().await.database_path().to_owned(),
                    source.partition_id.clone(),
                    source.owner_epoch,
                    source.max_object_bytes,
                ))
            } else {
                None
            }
        };
        #[cfg(feature = "s3-object-store")]
        let (log, events) = if let Some(source) = source {
            let (log, events) =
                remote::Log::open(source, &identity, config.max_operations, max_bytes).await?;
            (DurableLog::Remote(log), events)
        } else {
            let (log, events) = local()?;
            (
                DurableLog::Local(Arc::new(std::sync::Mutex::new(log))),
                events,
            )
        };
        #[cfg(not(feature = "s3-object-store"))]
        let (log, events) = {
            let (log, events) = local()?;
            (
                DurableLog::Local(Arc::new(std::sync::Mutex::new(log))),
                events,
            )
        };
        let mut spool = Spool {
            log,
            rows: Vec::new(),
            operations: BTreeMap::new(),
            completed: 0,
            base: 0,
            epoch_floor: 0,
            completed_since_compaction: 0,
            materialized_seq: 0,
            completion_reserve: 0,
            max_pending: config.max_operations.min(1024),
            max_operations: config.max_operations,
            max_record_bytes: config.max_record_bytes,
            blocked: None,
        };
        for event in events {
            spool.apply(event)?;
        }
        spool.reclaim(current_epoch()).await?;
        let pipeline = Self {
            spool: Arc::new(Mutex::new(spool)),
            running: Arc::new(AtomicBool::new(false)),
            worker: Arc::new(std::sync::Mutex::new(None)),
            partition,
            #[cfg(test)]
            paused: Arc::new(AtomicBool::new(false)),
        };
        pipeline.kick();
        Ok(pipeline)
    }
    /// Append up to 64 commands with one WAL commit. Same-identity retries
    /// return the original receipt without enqueueing the command again.
    pub async fn submit(
        &self,
        requests: Vec<RecordWrite>,
    ) -> Result<Vec<IngestReceipt>, RecordError> {
        if requests.is_empty() || requests.len() > BATCH_RECORDS {
            return Err(RecordError::Rejected(
                "intake batch requires 1..=64 commands".into(),
            ));
        }
        let pipeline = self.clone();
        crate::resources::spawn(async move {
            let result = pipeline.submit_owned(requests).await;
            pipeline.kick();
            result
        })
        .await
        .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?
    }
    async fn submit_owned(
        &self,
        requests: Vec<RecordWrite>,
    ) -> Result<Vec<IngestReceipt>, RecordError> {
        let mut spool = self.spool.lock().await;
        if spool.blocked.is_none() {
            if let Err(error) = spool.reclaim(current_epoch()).await {
                if matches!(error, RecordError::OutcomeUnknown(_) | RecordError::RecoveryRequired) {
                    spool.blocked = Some(error.to_string());
                }
                return Err(error);
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut fresh = Vec::new();
        let mut receipts = Vec::new();
        let mut bytes = 0usize;
        for request in requests {
            if !seen.insert(request.operation_id.clone()) {
                return Err(RecordError::Rejected(
                    "duplicate operation identity within intake batch".into(),
                ));
            }
            let length = checkpoint_codec::json_length(&request)?;
            bytes = bytes
                .checked_add(length)
                .filter(|n| *n <= BATCH_BYTES)
                .ok_or_else(|| RecordError::Rejected("intake batch exceeds 4 MiB".into()))?;
            if let Some(&position) = spool.operations.get(&request.operation_id) {
                if spool.rows[position].digest != request_digest(&request)?
                {
                    return Err(RecordError::Conflict(
                        "operation identity reused with another request".into(),
                    ));
                }
                receipts.push(spool.rows[position].status.accepted.clone());
                continue;
            }
            let epoch = current_epoch();
            if request.operation_id.epoch > epoch
                || request.operation_id.epoch.saturating_add(1) < epoch
                || request.operation_id.epoch < spool.epoch_floor
            {
                return Err(RecordError::OperationExpired);
            }
            if length > spool.max_record_bytes
                || request.record_id.is_empty()
                || request.routing_key.is_empty()
                || request.operation_id.nonce.is_empty()
            {
                return Err(RecordError::Rejected(
                    "invalid intake identity or record size".into(),
                ));
            }
            if let Some(document) = &request.document {
                validate_document_depth(document)?;
            }
            receipts.push(IngestReceipt {
                durable_seq: spool.base + (spool.rows.len() + fresh.len()) as u64 + 1,
                operation_id: request.operation_id.clone(),
            });
            fresh.push(request);
        }
        if fresh.is_empty() {
            return Ok(receipts);
        }
        if spool.blocked.is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        if spool.rows.len() + fresh.len() > spool.max_operations
            || spool.rows.len() - spool.completed + fresh.len() > spool.max_pending
        {
            return Err(RecordError::Busy);
        }
        // Retained decoded requests are admitted before the durable commit.
        let mut memory = Vec::new();
        for request in &fresh {
            memory.push(
                crate::storage::build_budget::reserve_memory(
                    (checkpoint_codec::json_length(request)? as u64)
                        .checked_mul(32)
                        .ok_or_else(|| storage("intake residency overflow"))?,
                )
                .map_err(storage)?,
            );
        }
        let first = spool.base + spool.rows.len() as u64 + 1;
        let event = Event::Accept {
            first,
            requests: fresh,
        };
        let extra_reserve = match &event {
            Event::Accept { requests, .. } => requests.iter().try_fold(0u64, |n, r| {
                n.checked_add(checkpoint_codec::json_length(r)? as u64 + 16384)
                    .ok_or(RecordError::Busy)
            })?,
            _ => unreachable!(),
        };
        spool.log.admit_accept(
            checkpoint_codec::json_length(&event)? as u64,
            spool
                .completion_reserve
                .checked_add(extra_reserve)
                .ok_or(RecordError::Busy)?,
            (spool.rows.len() - spool.completed + memory.len()) as u64,
        )?;
        spool.rows.try_reserve(receipts.len()).map_err(storage)?;
        if let Err(error) = spool.log.append(&event).await {
            if matches!(
                error,
                RecordError::OutcomeUnknown(_) | RecordError::RecoveryRequired
            ) {
                spool.blocked = Some(error.to_string());
            }
            return Err(error);
        }
        let Event::Accept { requests, .. } = event else {
            unreachable!()
        };
        for (request, charge) in requests.into_iter().zip(memory) {
            spool.completion_reserve += checkpoint_codec::json_length(&request)? as u64 + 16384;
            let accepted = IngestReceipt {
                durable_seq: spool.base + spool.rows.len() as u64 + 1,
                operation_id: request.operation_id.clone(),
            };
            let position = spool.rows.len();
            spool
                .operations
                .insert(request.operation_id.clone(), position);
            spool.rows.push(Row {
                digest: request_digest(&request)?,
                request: Some(request),
                status: IngestStatus {
                    accepted,
                    receipt: None,
                    error: None,
                },
                _memory: charge,
            });
        }
        Ok(receipts)
    }
    /// Inspect an identity without acquiring the Record/index mutex.
    pub async fn operation(&self, id: &OperationId) -> Option<IngestStatus> {
        let spool = self.spool.lock().await;
        spool
            .operations
            .get(id)
            .map(|&position| spool.rows[position].status.clone())
    }
    /// Inspect independent intake and completion watermarks.
    pub async fn progress(&self) -> IngestProgress {
        let spool = self.spool.lock().await;
        IngestProgress {
            durable_seq: spool.base + spool.rows.len() as u64,
            completed_seq: spool.base + spool.completed as u64,
            materialized_seq: spool.materialized_seq,
            lexical_seq: self.partition.indexes.lexical.load(Ordering::Acquire),
            vector_seq: self.partition.indexes.vector.load(Ordering::Acquire),
            index_error: self.partition.indexes.error(),
            pending: spool.rows.len() - spool.completed,
            retained_bytes: spool.log.bytes(),
            blocked: spool.blocked.clone(),
        }
    }
    /// Administrative callers must exclude concurrent submissions and drain
    /// intake before exporting or replacing the materialized partition.
    pub async fn require_drained(&self) -> Result<(), RecordError> {
        let progress = self.progress().await;
        if progress.blocked.is_some() || progress.index_error.is_some() {
            Err(RecordError::RecoveryRequired)
        } else if progress.pending != 0
            || progress.lexical_seq < progress.materialized_seq
            || progress.vector_seq < progress.materialized_seq
        {
            Err(RecordError::Busy)
        } else {
            // A published cursor can precede the final task-owner drop. Join
            // before management releases the source owner or removes mmap files.
            self.finish().await?;
            let inner=self.partition.inner.lock().await;
            if inner.background.is_some() || inner.maintenance_scheduled {Err(RecordError::Busy)}else{Ok(())}
        }
    }
    /// Explicitly wait for pending work and its owned task to finish. Normal
    /// submission never calls this. Concurrent producers can add more work.
    /// Optional cache compaction may still own the partition after this returns;
    /// administrative callers use `require_drained` before releasing its owner.
    pub async fn finish(&self) -> Result<IngestProgress, RecordError> {
        loop {
            let handle = self
                .worker
                .lock()
                .map_err(|_| RecordError::RecoveryRequired)?
                .take();
            if let Some(handle) = handle {
                handle.await.map_err(storage)?;
            }
            let progress = self.progress().await;
            if progress.blocked.is_some() {
                return Err(RecordError::RecoveryRequired);
            }
            if progress.pending == 0 && !self.running.load(Ordering::Acquire) {
                self.partition
                    .inner
                    .lock()
                    .await
                    .index
                    .drain_indexes()
                    .await?;
                return Ok(self.progress().await);
            }
            self.kick();
            tokio::task::yield_now().await;
        }
    }
    fn kick(&self) {
        #[cfg(test)]
        if self.paused.load(Ordering::Acquire) {
            return;
        }
        let Ok(mut worker) = self.worker.lock() else {
            return;
        };
        if self
            .running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let pipeline = self.clone();
        let handle = crate::resources::spawn_background(async move {
            let result = std::panic::AssertUnwindSafe(pipeline.consume())
                .catch_unwind()
                .await;
            if result.is_err() {
                pipeline.spool.lock().await.blocked =
                    Some("intake materializer panicked; reopen required".into());
            }
            pipeline.running.store(false, Ordering::Release);
            let more = {
                let spool = pipeline.spool.lock().await;
                spool.blocked.is_none() && spool.completed < spool.rows.len()
            };
            if more {
                pipeline.kick();
            }
        });
        // Registration and start share this short guard; finish() takes the
        // handle before awaiting. Cancellation never aborts accepted work.
        *worker = Some(handle);
    }
    async fn consume(&self) {
        loop {
            #[cfg(test)]
            if self.paused.load(Ordering::Acquire) {
                return;
            }
            let next = {
                let mut spool = self.spool.lock().await;
                if spool.blocked.is_some() || spool.completed == spool.rows.len() {
                    return;
                }
                let mut requests = Vec::new();
                let mut bytes = 0usize;
                for row in spool.rows[spool.completed..].iter().take(BATCH_RECORDS) {
                    let length = match checkpoint_codec::json_length(row.request.as_ref().expect("pending row")) {
                        Ok(length) => length,
                        Err(error) => {
                            spool.blocked = Some(error.to_string());
                            return;
                        }
                    };
                    if bytes.saturating_add(length) > BATCH_BYTES {
                        break;
                    }
                    bytes += length;
                    requests.push(row.request.as_ref().expect("pending row").clone());
                }
                (spool.base + spool.completed as u64 + 1, requests)
            };
            let (seq, requests) = next;
            let started = std::time::Instant::now();
            let batch = loop {
                let result = self.partition.write_ingest_batch(requests.clone()).await;
                let busy = matches!(&result, Err(RecordError::Busy))
                    || matches!(&result, Ok(batch) if batch.results.is_empty()
                        && matches!(batch.stop, Some(RecordError::Busy)));
                if busy && started.elapsed() < std::time::Duration::from_secs(300) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                match result {
                    Ok(batch) => break batch,
                    Err(error) => {
                        self.spool.lock().await.blocked = Some(error.to_string());
                        return;
                    }
                }
            };
            let stop = batch.stop;
            let results = batch
                .results
                .into_iter()
                .map(|result| {
                    result.map_err(|error| {
                        let code = match error {
                            RecordError::Rejected(_) => "rejected",
                            RecordError::Conflict(_) => "conflict",
                            RecordError::OperationExpired => "operation_expired",
                            _ => unreachable!("only definite errors enter the completed prefix"),
                        };
                        IngestFailure {
                            code: code.into(),
                            message: error.to_string().chars().take(1024).collect(),
                        }
                    })
                })
                .collect::<Vec<_>>();
            if results.is_empty() {
                self.spool.lock().await.blocked =
                    Some(stop.unwrap_or(RecordError::RecoveryRequired).to_string());
                return;
            }
            let mut spool = self.spool.lock().await;
            let event = Event::Complete {
                first: seq,
                results,
            };
            if let Err(error) = spool.log.append(&event).await {
                spool.blocked = Some(error.to_string());
                return;
            }
            if let Err(error) = spool.apply(event) {
                spool.blocked = Some(error.to_string());
                return;
            }
            if let Err(error) = spool.reclaim(current_epoch()).await {
                spool.blocked = Some(error.to_string());
                return;
            }
            if let Some(error) = stop {
                if error != RecordError::Busy {
                    spool.blocked = Some(error.to_string());
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{config, write};
    use super::*;
    #[tokio::test]
    async fn compacted_terminal_digest_preserves_json_equality_and_original_receipt() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone()).await.unwrap();
        let pipeline = paused(p.clone()).await;
        let mut request = write("canonical", "a", "alpha", ExpectedVersion::Absent);
        request.document = Some(serde_json::from_str(r#"{"text":"alpha","nested":{"a":1,"b":[{"x":2,"y":3}],"zero":-0.0}}"#).unwrap());
        let original = pipeline.submit(vec![request.clone()]).await.unwrap();
        let mut reordered = request.clone();
        reordered.document = Some(serde_json::from_str(r#"{"nested":{"zero":0.0,"b":[{"y":3,"x":2}],"a":1},"text":"alpha"}"#).unwrap());
        assert_eq!(pipeline.submit(vec![reordered.clone()]).await.unwrap(), original);
        pipeline.paused.store(false, Ordering::Release);
        drained(&pipeline).await;
        {
            let mut spool = pipeline.spool.lock().await;
            assert!(spool.rows[0].request.is_none());
            spool.completed_since_compaction = 64;
            spool.reclaim(current_epoch()).await.unwrap();
        }
        assert_eq!(pipeline.submit(vec![reordered.clone()]).await.unwrap(), original);
        drained(&pipeline).await;
        drop(pipeline); drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes).await.unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        assert_eq!(pipeline.submit(vec![reordered.clone()]).await.unwrap(), original);
        reordered.document = Some(serde_json::from_str(r#"{"text":"different"}"#).unwrap());
        assert!(matches!(pipeline.submit(vec![reordered]).await, Err(RecordError::Conflict(_))));
        assert_eq!(p.statistics().await.unwrap().retained_operations, 1);
    }
    #[tokio::test]
    async fn uncertain_gc_selection_blocks_until_reopen_and_keeps_retry_receipt() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone()).await.unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        let request = write("gc-unknown", "a", "alpha", ExpectedVersion::Absent);
        let original = pipeline.submit(vec![request.clone()]).await.unwrap();
        drained(&pipeline).await;
        pipeline.spool.lock().await.completed_since_compaction = 64;
        let fault = root.join("intake/.test-intake-after-select");
        std::fs::write(&fault, b"").unwrap();
        assert!(matches!(pipeline.submit(vec![request.clone()]).await, Err(RecordError::OutcomeUnknown(_))));
        assert!(pipeline.progress().await.blocked.is_some());
        assert!(pipeline.finish().await.is_err());
        drop(pipeline); drop(p);
        std::fs::remove_file(fault).unwrap();
        let p = RecordPartition::open(&root, settings.max_journal_bytes).await.unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        assert_eq!(pipeline.submit(vec![request.clone()]).await.unwrap(), original);
        drained(&pipeline).await;
        assert_eq!(pipeline.operation(&request.operation_id).await.unwrap().receipt.unwrap().seq, 1);
        assert_eq!(p.statistics().await.unwrap().retained_operations, 1);
    }
    #[tokio::test]
    async fn expired_terminal_prefix_is_reclaimed_without_reusing_sequences_or_identities() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone()).await.unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        let mut old = write("old", "a", "alpha", ExpectedVersion::Absent);
        old.operation_id.epoch = current_epoch() - 1;
        pipeline.submit(vec![old.clone()]).await.unwrap();
        drained(&pipeline).await;
        pipeline.spool.lock().await.reclaim(current_epoch() + 1).await.unwrap();
        assert_eq!(pipeline.progress().await.completed_seq, 1);
        assert!(pipeline.operation(&old.operation_id).await.is_none());
        assert!(matches!(pipeline.submit(vec![old.clone()]).await, Err(RecordError::OperationExpired)));
        let next = write("new", "b", "beta", ExpectedVersion::Absent);
        assert_eq!(pipeline.submit(vec![next.clone()]).await.unwrap()[0].durable_seq, 2);
        drained(&pipeline).await;
        drained(&pipeline).await;
        drop(pipeline); drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes).await.unwrap();
        let pipeline = IngestPipeline::open(p).await.unwrap();
        assert_eq!(pipeline.progress().await.completed_seq, 2);
        assert!(matches!(pipeline.submit(vec![old]).await, Err(RecordError::OperationExpired)));
        assert_eq!(pipeline.submit(vec![next]).await.unwrap()[0].durable_seq, 2);
    }
    async fn drained(pipeline: &IngestPipeline) {
        tokio::time::timeout(std::time::Duration::from_secs(20), pipeline.finish())
            .await
            .unwrap()
            .unwrap();
    }
    async fn paused(partition: RecordPartition) -> IngestPipeline {
        let pipeline = IngestPipeline::open(partition).await.unwrap();
        pipeline.paused.store(true, Ordering::Release);
        pipeline.finish().await.unwrap();
        pipeline
    }
    #[tokio::test]
    async fn lexical_lag_does_not_block_vector_materialization_or_intake_and_commits_one_batch() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let mut settings = config();
        settings.max_records = 256;
        settings.max_operations = 512;
        settings.max_journal_bytes = 16 * 1024 * 1024;
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let pipeline = paused(p.clone()).await;
        std::fs::write(root.join(".test-lexical-consumer-pause"), b"pause").unwrap();
        let requests = (0..64)
            .map(|i| {
                write(
                    &format!("op-{i}"),
                    &format!("id-{i}"),
                    "alpha",
                    ExpectedVersion::Absent,
                )
            })
            .collect::<Vec<_>>();
        pipeline.submit(requests.clone()).await.unwrap();
        pipeline.paused.store(false, Ordering::Release);
        pipeline.kick();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while pipeline.progress().await.completed_seq != 64 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pipeline.progress().await.vector_seq != 64 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let progress = pipeline.progress().await;
        assert_eq!((progress.lexical_seq, progress.vector_seq), (0, 64));
        assert!(matches!(
            pipeline.require_drained().await,
            Err(RecordError::Busy)
        ));
        assert!(p.get("id-63").await.unwrap().is_some());
        assert_eq!(
            super::super::tests::query(
                &p,
                SearchMode::Vector {
                    similarity_threshold: None,
                    ann_mode: crate::vector_similarity::AnnMode::All
                }
            )
            .await
            .count_total,
            64
        );
        assert_eq!(
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            0
        );
        // Reception also remains independent of the paused lexical task.
        let next = write("next", "next", "beta", ExpectedVersion::Absent);
        let guard = p.inner.lock().await;
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            pipeline.submit(vec![next]),
        )
        .await
        .unwrap()
        .unwrap();
        std::fs::remove_file(root.join(".test-lexical-consumer-pause")).unwrap();
        // Conversely, lexical publication needs no Record/vector guard. The
        // second batch cannot materialize while this guard is held.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pipeline.progress().await.lexical_seq != 64 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let first_native = guard.index.test_index().read().await.record_lexical[0].clone();
        assert_eq!(first_native.published_shape().await, (64, 1));
        drop(first_native);
        drop(guard);
        drained(&pipeline).await;
        let native = p
            .inner
            .lock()
            .await
            .index
            .test_index()
            .read()
            .await
            .record_lexical[0]
            .clone();
        let (documents, segments) = native.published_shape().await;
        assert!(
            segments <= 2,
            "64 documents share one native commit, followed by the next batch"
        );
        assert_eq!(documents, 65);
        assert_eq!(pipeline.progress().await.lexical_seq, 65);
        drop(native);
        drop(pipeline);
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            64
        );
        assert_eq!(p.write(requests[63].clone()).await.unwrap().seq, 64);
    }
    #[tokio::test]
    async fn vector_lag_allows_record_and_lexical_progress_and_masks_queued_old_versions() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let pipeline = paused(p.clone()).await;
        std::fs::write(root.join(".test-vector-consumer-pause"), b"pause").unwrap();
        let first = write("first", "a", "alpha", ExpectedVersion::Absent);
        pipeline.submit(vec![first.clone()]).await.unwrap();
        pipeline.paused.store(false, Ordering::Release);
        pipeline.kick();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pipeline.progress().await.lexical_seq != 1
                || pipeline.progress().await.completed_seq != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pipeline.progress().await.vector_seq, 0);
        assert!(p.get("a").await.unwrap().is_some());
        assert!(matches!(
            pipeline.require_drained().await,
            Err(RecordError::Busy)
        ));
        // The lexical cache is ahead; shared metadata has not arrived, so its
        // available prefix may be empty without producing a topology error.
        assert_eq!(
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            0
        );
        let update = write("second", "a", "beta", ExpectedVersion::Match(1));
        let mut delete = write("third", "a", "", ExpectedVersion::Match(2));
        delete.document = None;
        let survivor = write("fourth", "b", "gamma", ExpectedVersion::Absent);
        pipeline
            .submit(vec![update, delete, survivor])
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pipeline.progress().await.completed_seq != 4
                || pipeline.progress().await.lexical_seq != 4
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pipeline.progress().await.vector_seq, 0);
        assert!(p.get("a").await.unwrap().is_none());
        std::fs::remove_file(root.join(".test-vector-consumer-pause")).unwrap();
        drained(&pipeline).await;
        let result = super::super::tests::query(
            &p,
            SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: crate::vector_similarity::AnnMode::All,
            },
        )
        .await;
        assert_eq!(result.count_total, 1);
        assert_eq!(result.hits[0].record.record_id, "b");
        assert_eq!(
            p.search("alpha beta".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            0
        );
        p.compact_journal().await.unwrap();
        drop(pipeline);
        drop(p);
        let p = RecordPartition::open(&root, config().max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(
            super::super::tests::query(
                &p,
                SearchMode::Vector {
                    similarity_threshold: None,
                    ann_mode: crate::vector_similarity::AnnMode::All
                }
            )
            .await
            .count_total,
            1
        );
    }
    #[tokio::test]
    async fn vector_failure_keeps_record_receipts_and_both_consumers_recover_from_log() {
        for failure in [
            ".test-vector-consumer-fail",
            ".test-vector-consumer-fail-after-append",
            ".test-vector-consumer-panic-after-append",
        ] {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path().join("records");
            let p = RecordPartition::create(&root, config()).await.unwrap();
            let pipeline = paused(p.clone()).await;
            std::fs::write(root.join(failure), b"fail").unwrap();
            let request = write("failed-vector", "a", "alpha", ExpectedVersion::Absent);
            pipeline.submit(vec![request.clone()]).await.unwrap();
            pipeline.paused.store(false, Ordering::Release);
            pipeline.kick();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while pipeline.progress().await.index_error.is_none()
                    || pipeline.progress().await.completed_seq != 1
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(pipeline.finish().await.is_err());
            assert!(
                pipeline
                    .operation(&request.operation_id)
                    .await
                    .unwrap()
                    .receipt
                    .is_some()
            );
            assert!(p.get("a").await.unwrap().is_some());
            assert!(
                p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                    .await
                    .is_err()
            );
            if failure.ends_with("after-append") {
                assert!(
                    p.inner
                        .lock()
                        .await
                        .index
                        .test_index()
                        .read()
                        .await
                        .shard_vec[0]
                        .read()
                        .await
                        .failed
                );
            }
            std::fs::remove_file(root.join(failure)).unwrap();
            drop(pipeline);
            drop(p);
            let p = RecordPartition::open(&root, config().max_journal_bytes)
                .await
                .unwrap();
            let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
            drained(&pipeline).await;
            assert_eq!(
                (
                    pipeline.progress().await.vector_seq,
                    pipeline.progress().await.lexical_seq
                ),
                (1, 1)
            );
            assert_eq!(p.write(request).await.unwrap().seq, 1);
        }
    }
    #[tokio::test]
    async fn dropping_public_handles_keeps_owner_until_each_index_consumer_exits() {
        for lane in ["lexical", "vector"] {
            let temporary = tempfile::tempdir().unwrap();
            let root = temporary.path().join("records");
            let p = RecordPartition::create(&root, config()).await.unwrap();
            let pipeline = paused(p.clone()).await;
            let gate = root.join(format!(".test-{lane}-consumer-pause"));
            std::fs::write(&gate, b"pause").unwrap();
            pipeline
                .submit(vec![write("one", "a", "alpha", ExpectedVersion::Absent)])
                .await
                .unwrap();
            pipeline.paused.store(false, Ordering::Release);
            pipeline.kick();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while pipeline.progress().await.completed_seq != 1
                    || !root.join(format!(".test-{lane}-consumer-started")).exists()
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            drop(pipeline);
            drop(p);
            assert!(
                PartitionJournal::open(&root, config().max_journal_bytes).is_err(),
                "active {lane} work must retain the journal owner"
            );
            std::fs::remove_file(&gate).unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if let Ok((journal, _)) =
                        PartitionJournal::open(&root, config().max_journal_bytes)
                    {
                        drop(journal);
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            let p = RecordPartition::open(&root, config().max_journal_bytes)
                .await
                .unwrap();
            assert!(p.get("a").await.unwrap().is_some());
        }
    }
    #[tokio::test]
    async fn lexical_failure_keeps_durable_record_outcomes_and_reopen_rebuilds_indexes() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let pipeline = paused(p.clone()).await;
        std::fs::write(root.join(".test-lexical-consumer-fail"), b"fail").unwrap();
        let request = write(
            "durable-index-failure",
            "a",
            "alpha",
            ExpectedVersion::Absent,
        );
        pipeline.submit(vec![request.clone()]).await.unwrap();
        pipeline.paused.store(false, Ordering::Release);
        pipeline.kick();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pipeline.progress().await.index_error.is_none()
                || pipeline.progress().await.completed_seq != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            pipeline
                .operation(&request.operation_id)
                .await
                .unwrap()
                .receipt
                .is_some()
        );
        assert!(pipeline.finish().await.is_err());
        assert!(p.get("a").await.unwrap().is_some());
        std::fs::remove_file(root.join(".test-lexical-consumer-fail")).unwrap();
        drop(pipeline);
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        drained(&pipeline).await;
        assert!(pipeline.progress().await.index_error.is_none());
        assert_eq!(
            (
                pipeline.progress().await.lexical_seq,
                pipeline.progress().await.vector_seq
            ),
            (1, 1)
        );
        assert_eq!(
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            1
        );
        assert_eq!(p.write(request).await.unwrap().seq, 1);
    }
    #[tokio::test]
    async fn batched_updates_deletes_and_rejections_preserve_order_and_synchronous_visibility() {
        let temporary = tempfile::tempdir().unwrap();
        let p = RecordPartition::create(&temporary.path().join("records"), config())
            .await
            .unwrap();
        let pipeline = paused(p.clone()).await;
        let mut delete = write("delete", "a", "", ExpectedVersion::Match(2));
        delete.document = None;
        let requests = vec![
            write("first", "a", "alpha", ExpectedVersion::Absent),
            write("update", "a", "beta", ExpectedVersion::Match(1)),
            write("rejected", "a", "wrong", ExpectedVersion::Absent),
            delete,
            write("survivor", "b", "alpha", ExpectedVersion::Absent),
        ];
        pipeline.submit(requests.clone()).await.unwrap();
        pipeline.paused.store(false, Ordering::Release);
        pipeline.kick();
        drained(&pipeline).await;
        assert_eq!(pipeline.progress().await.completed_seq, 5);
        assert_eq!(
            pipeline
                .operation(&requests[2].operation_id)
                .await
                .unwrap()
                .error
                .unwrap()
                .code,
            "conflict"
        );
        assert_eq!(
            pipeline
                .operation(&requests[3].operation_id)
                .await
                .unwrap()
                .receipt
                .unwrap()
                .seq,
            3
        );
        assert!(p.get("a").await.unwrap().is_none());
        assert_eq!(
            p.search("beta".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            0
        );
        assert_eq!(
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            1
        );
        let request = write("sync", "b", "gamma", ExpectedVersion::Match(1));
        p.write(request).await.unwrap();
        assert_eq!(
            p.search("gamma".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            1
        );
        assert_eq!(
            (
                pipeline.progress().await.lexical_seq,
                pipeline.progress().await.vector_seq
            ),
            (5, 5)
        );
    }
    #[tokio::test]
    async fn batch_crossing_native_rotation_drains_its_prefix_before_sealing_and_reopens() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let mut settings = config();
        settings.max_records = 256;
        settings.max_operations = 512;
        settings.max_journal_bytes = 16 * 1024 * 1024;
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        p.write(write("seed", "seed", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        p.compact_journal().await.unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        let mut offset = 0;
        for count in [64, 62, 4] {
            pipeline
                .submit(
                    (offset..offset + count)
                        .map(|i| {
                            write(
                                &format!("o-{i}"),
                                &format!("r-{i}"),
                                "alpha",
                                ExpectedVersion::Absent,
                            )
                        })
                        .collect(),
                )
                .await
                .unwrap();
            drained(&pipeline).await;
            offset += count;
        }
        assert!(p.inner.lock().await.index.components().await.len()>2);
        assert_eq!(pipeline.progress().await.lexical_seq, 131);
        assert_eq!(
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            131
        );
        drop(pipeline);
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                .await
                .unwrap()
                .count_total,
            131
        );
        assert_eq!(
            p.write(write("o-129", "r-129", "alpha", ExpectedVersion::Absent))
                .await
                .unwrap()
                .seq,
            131
        );
    }
    #[tokio::test]
    async fn intake_acknowledges_while_materializer_lock_is_held_and_retries_once() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let pipeline = paused(p.clone()).await;
        let request = write("one", "a", "alpha", ExpectedVersion::Absent);
        let guard = p.inner.lock().await;
        let receipt = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            pipeline.submit(vec![request.clone()]),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(receipt[0].durable_seq, 1);
        assert_eq!(pipeline.progress().await.pending, 1);
        assert_eq!(
            pipeline.submit(vec![request.clone()]).await.unwrap(),
            receipt
        );
        let mut conflict = request.clone();
        conflict
            .document
            .as_mut()
            .unwrap()
            .insert("body".into(), serde_json::json!("different"));
        assert!(matches!(
            pipeline.submit(vec![conflict]).await,
            Err(RecordError::Conflict(_))
        ));
        assert_eq!(guard.state.seq, 0);
        drop(guard);
        pipeline.paused.store(false, Ordering::Release);
        pipeline.kick();
        drained(&pipeline).await;
        let status = pipeline.operation(&request.operation_id).await.unwrap();
        assert_eq!(status.receipt.unwrap().seq, 1);
        assert_eq!(
            p.get("a").await.unwrap().unwrap().document.unwrap()["body"],
            "alpha"
        );
        assert_eq!(p.statistics().await.unwrap().retained_operations, 1);
    }
    #[tokio::test]
    async fn pending_batch_survives_reopen_and_conditional_rejections_are_terminal() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let pipeline = paused(p.clone()).await;
        let first = write("first", "a", "alpha", ExpectedVersion::Absent);
        let second = write("conflict", "a", "beta", ExpectedVersion::Absent);
        let receipts = pipeline
            .submit(vec![first.clone(), second.clone()])
            .await
            .unwrap();
        assert_eq!(receipts.len(), 2);
        assert!(matches!(
            pipeline.require_drained().await,
            Err(RecordError::Busy)
        ));
        drop(pipeline);
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        drained(&pipeline).await;
        let status = pipeline.operation(&second.operation_id).await.unwrap();
        assert_eq!(status.error.unwrap().code, "conflict");
        assert_eq!(pipeline.progress().await.completed_seq, 2);
        drop(pipeline);
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        drained(&pipeline).await;
        assert!(
            pipeline
                .operation(&first.operation_id)
                .await
                .unwrap()
                .receipt
                .is_some()
        );
        assert!(
            pipeline
                .operation(&second.operation_id)
                .await
                .unwrap()
                .error
                .is_some()
        );
        assert_eq!(p.statistics().await.unwrap().retained_operations, 1);
    }
    #[tokio::test]
    async fn uncertain_materialization_stays_pending_and_reopen_resolves_original_receipt() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let pipeline = paused(p.clone()).await;
        p.inner.lock().await.fail_after_durable = true;
        let request = write("durable", "a", "alpha", ExpectedVersion::Absent);
        pipeline.submit(vec![request.clone()]).await.unwrap();
        pipeline.paused.store(false, Ordering::Release);
        pipeline.kick();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while pipeline.progress().await.blocked.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !pipeline
                .operation(&request.operation_id)
                .await
                .unwrap()
                .completed()
        );
        assert!(pipeline.finish().await.is_err());
        drop(pipeline);
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        drained(&pipeline).await;
        assert_eq!(
            pipeline
                .operation(&request.operation_id)
                .await
                .unwrap()
                .receipt
                .unwrap()
                .seq,
            1
        );
        assert_eq!(p.statistics().await.unwrap().retained_operations, 1);
    }
    #[tokio::test]
    async fn intake_pending_budget_refuses_before_commit() {
        let temporary = tempfile::tempdir().unwrap();
        let p = RecordPartition::create(&temporary.path().join("records"), config())
            .await
            .unwrap();
        let pipeline = paused(p).await;
        pipeline.spool.lock().await.max_pending = 1;
        let first = write("one", "a", "alpha", ExpectedVersion::Absent);
        let second = write("two", "b", "beta", ExpectedVersion::Absent);
        pipeline.submit(vec![first.clone()]).await.unwrap();
        assert!(matches!(
            pipeline.submit(vec![second.clone()]).await,
            Err(RecordError::Busy)
        ));
        assert!(pipeline.operation(&second.operation_id).await.is_none());
        assert_eq!(
            pipeline.submit(vec![first]).await.unwrap()[0].durable_seq,
            1
        );
    }
    #[cfg(feature = "s3-object-store")]
    #[tokio::test]
    async fn remote_intake_recovers_pending_commands_and_receipts_after_cache_loss() {
        use crate::storage::{s3_compatible::S3CompatibleStore, sqlite_authority::SqliteAuthority};
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let database = temporary.path().join("authority.sqlite");
        let settings = config();
        let objects = Arc::new(object_store::memory::InMemory::new());
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let store = S3CompatibleStore::from_store(objects.clone(), 1024 * 1024);
        let mut authority =
            SqliteAuthority::open(&database, std::time::Duration::from_secs(1)).unwrap();
        authority.create_partition("source", 1).unwrap();
        let bootstrap = {
            let inner = p.inner.lock().await;
            encode_entry(&Entry::Checkpoint {
                config: inner.config.clone(),
                state: inner.state.try_clone().unwrap(),
            })
            .unwrap()
        };
        store
            .publish_bootstrap(&mut authority, "source", 1, &bootstrap, unix_time_ms_i64())
            .await
            .unwrap();
        p.inner.lock().await.remote = Some(RemotePersistence {
            store,
            authority: Mutex::new(authority),
            partition_id: "source".into(),
            owner_epoch: 1,
            max_tail_blocks: 128,
            max_tail_bytes: 1024 * 1024,
            max_object_bytes: 1024 * 1024,
            _owner: None,
        });
        let pipeline = paused(p.clone()).await;
        let request = write("remote-one", "a", "alpha", ExpectedVersion::Absent);
        let receipt = pipeline.submit(vec![request.clone()]).await.unwrap();
        assert_eq!(receipt[0].durable_seq, 1);
        assert!(!root.join("intake").exists());
        drop(pipeline);
        drop(p);
        std::fs::remove_dir_all(&root).unwrap();
        let remote = RemotePersistence {
            store: S3CompatibleStore::from_store(objects.clone(), 1024 * 1024),
            authority: Mutex::new(
                SqliteAuthority::open(&database, std::time::Duration::from_secs(1)).unwrap(),
            ),
            partition_id: "source".into(),
            owner_epoch: 1,
            max_tail_blocks: 128,
            max_tail_bytes: 1024 * 1024,
            max_object_bytes: 1024 * 1024,
            _owner: None,
        };
        let p = RecordPartition::open_s3_with_remote(&root, settings.max_journal_bytes, remote)
            .await
            .unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        drained(&pipeline).await;
        assert_eq!(
            pipeline
                .operation(&request.operation_id)
                .await
                .unwrap()
                .accepted,
            receipt[0]
        );
        assert_eq!(
            p.get("a").await.unwrap().unwrap().document.unwrap()["body"],
            "alpha"
        );
        drop(pipeline);
        drop(p);
        std::fs::remove_dir_all(&root).unwrap();
        let remote = RemotePersistence {
            store: S3CompatibleStore::from_store(objects.clone(), 1024 * 1024),
            authority: Mutex::new(
                SqliteAuthority::open(&database, std::time::Duration::from_secs(1)).unwrap(),
            ),
            partition_id: "source".into(),
            owner_epoch: 1,
            max_tail_blocks: 128,
            max_tail_bytes: 1024 * 1024,
            max_object_bytes: 1024 * 1024,
            _owner: None,
        };
        let p = RecordPartition::open_s3_with_remote(&root, settings.max_journal_bytes, remote)
            .await
            .unwrap();
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        drained(&pipeline).await;
        assert!(
            pipeline
                .operation(&request.operation_id)
                .await
                .unwrap()
                .receipt
                .is_some()
        );
        assert_eq!(p.statistics().await.unwrap().retained_operations, 1);
    }
}
