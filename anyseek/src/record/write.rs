//! Shared bounded Record execution. Successful results are durable business decisions.
//! Index publication can be synchronous or asynchronous; both use the same canonical commit path.
use super::*;
use futures::FutureExt;
mod planning;
use planning::PlanningView;

/// Only the completed prefix has definite outcomes. Busy and uncertain
/// outcomes keep their original identity and leave the suffix pending.
pub(super) struct BatchResult {
    pub results: Vec<Result<WriteReceipt, RecordError>>,
    pub stop: Option<RecordError>,
}

/// In-memory scope only: entering publication is not proof of commitment.
#[derive(Default, Debug)]
pub(super) struct AttemptScope {
    pub persistence_attempted: Vec<usize>,
    pub replayed: Vec<(usize, WriteReceipt)>,
}

#[derive(Debug)]
pub(super) struct BatchFailure {
    pub error: RecordError,
    /// None means task infrastructure failed before returning its scope.
    pub scope: Option<AttemptScope>,
}
impl From<RecordError> for BatchFailure {
    fn from(error: RecordError) -> Self {
        Self {
            error,
            scope: Some(AttemptScope::default()),
        }
    }
}
fn requires_recovery(error: &RecordError) -> bool {
    matches!(
        error,
        RecordError::RecoveryRequired
            | RecordError::OutcomeUnknown(_)
            | RecordError::DurablePending(_)
    )
}

pub(super) fn definite(error: &RecordError) -> bool {
    matches!(
        error,
        RecordError::Rejected(_) | RecordError::Conflict(_) | RecordError::OperationExpired
    )
}

const MAX_BATCH_RECORDS: usize = 64;
const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy)]
pub(super) enum IndexDelivery {
    Synchronous,
    SynchronousBatch,
    Asynchronous,
}

// This borrow is the ordinary write's basis proof. The owned writer was acquired
// before capture and cannot be released until this batch is consumed/discarded.
// All live canonical mutators use this same mutex; see docs/record-write-proof.md.
struct PreparedBatch<'writer> {
    _writer: &'writer tokio::sync::OwnedMutexGuard<()>,
    plans: Vec<mutation::PreparedWrite>,
    publication: Vec<Option<state_memory::Prepared>>,
    result: BatchResult,
    base_seq: u64,
    expires_at: Option<u64>,
}

struct RemoteUpload {
    store: crate::storage::s3_compatible::S3CompatibleStore,
    path: std::path::PathBuf,
    head: crate::storage::sqlite_authority::PartitionHead,
    block: crate::storage::object_log::ObjectLogBlock,
    max_bytes: usize,
    #[cfg(test)]
    root: std::path::PathBuf,
}
struct UploadedBatch {
    head: crate::storage::sqlite_authority::PartitionHead,
    block: crate::storage::object_log::ObjectLogBlock,
    reference: crate::storage::sqlite_authority::ObjectRef,
}
impl RemoteUpload {
    async fn upload(self) -> Result<UploadedBatch, RecordError> {
        #[cfg(test)]
        if self.root.join(".test-write-upload-panic").exists() {
            panic!("injected upload unwind");
        }
        #[cfg(test)]
        if self.root.join(".test-write-upload-pause").exists() {
            std::fs::write(
                self.root.join(".test-write-upload-started"),
                b"outside state lock",
            )
            .map_err(storage)?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while self.root.join(".test-write-upload-pause").exists() {
                if std::time::Instant::now() >= deadline {
                    return Err(RecordError::Busy);
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        let digest = checkpoint_codec::json_digest(&self.block)?;
        let key = crate::storage::object_keys::key(&self.head.partition_id, "wal", &format!(
            "{:020}-{}",
            self.block.first_seq,
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ));
        // Independent short SQLite transactions avoid holding the shared authority
        // connection while awaiting network I/O. Retired objects cannot resurrect.
        let mut authority = crate::storage::sqlite_authority::SqliteAuthority::open(
            &self.path,
            std::time::Duration::from_secs(5),
        )
        .map_err(storage)?;
        let reference = crate::storage::object_log::upload_log_block(
            &self.store,
            &mut authority,
            &key,
            &self.block,
            unix_time_ms_i64(),
            self.max_bytes,
        )
        .await
        .map_err(|error| match RemoteCommitError::from(error) {
            RemoteCommitError::Busy => RecordError::Busy,
            RemoteCommitError::Unknown(error) => RecordError::OutcomeUnknown(error),
        })?;
        Ok(UploadedBatch {
            head: self.head,
            block: self.block,
            reference,
        })
    }
}
impl UploadedBatch {
    async fn publish(self, remote: &mut RemotePersistence) -> Result<(), RecordError> {
        if remote.partition_id != self.head.partition_id
            || remote.owner_epoch != self.head.owner_epoch
        {
            return Err(RecordError::RecoveryRequired);
        }
        let operations: Vec<_> = self
            .block
            .entries
            .iter()
            .map(|entry| (entry.operation_id.as_str(), entry.request_sha256, entry.seq))
            .collect();
        let result = remote
            .authority
            .lock()
            .await
            .publish_batch(
                &remote.partition_id,
                self.head.head_version,
                remote.owner_epoch,
                &operations,
                self.reference,
                None,
            )
            .map_err(|error| RecordError::OutcomeUnknown(error.to_string()))?;
        match result {
            crate::storage::sqlite_authority::PublishOutcome::Published(head)
            | crate::storage::sqlite_authority::PublishOutcome::AlreadyPublished(head)
                if head.committed_seq == self.block.last_seq =>
            {
                Ok(())
            }
            _ => Err(RecordError::OutcomeUnknown(
                "remote batch head changed before publication".into(),
            )),
        }
    }
}
impl Inner {
    async fn prepare_remote_batch(
        &self,
        prepared: &PreparedBatch<'_>,
    ) -> Result<Option<RemoteUpload>, RecordError> {
        use sha2::{Digest, Sha256};
        let Some(remote) = self.remote.as_ref() else {
            return Ok(None);
        };
        if prepared.plans.is_empty() {
            return Ok(None);
        }
        let authority = remote.authority.lock().await;
        let head = authority
            .read_head(&remote.partition_id)
            .map_err(storage)?
            .ok_or(RecordError::RecoveryRequired)?;
        if head.owner_epoch != remote.owner_epoch || head.committed_seq != prepared.base_seq {
            return Err(RecordError::RecoveryRequired);
        }
        let entries: Vec<_> = prepared
            .plans
            .iter()
            .map(|plan| match plan {
                mutation::PreparedWrite::Append { applied, bytes, .. } => {
                    crate::storage::object_log::ObjectLogEntry {
                        seq: applied.receipt.seq,
                        operation_id: format!(
                            "{}:{}",
                            applied.request.operation_id.epoch, applied.request.operation_id.nonce
                        ),
                        request_sha256: Sha256::digest(bytes).into(),
                        payload: bytes.clone(),
                    }
                }
                _ => unreachable!(),
            })
            .collect();
        let block = crate::storage::object_log::ObjectLogBlock {
            partition_id: remote.partition_id.clone(),
            first_seq: entries.first().unwrap().seq,
            last_seq: entries.last().unwrap().seq,
            previous: head.wal_tip.clone(),
            entries,
        };
        Ok(Some(RemoteUpload {
            store: remote.store.clone(),
            path: authority.database_path().to_path_buf(),
            head,
            block,
            max_bytes: remote.max_object_bytes,
            #[cfg(test)]
            root: self.journal.root().to_path_buf(),
        }))
    }
}

impl RecordPartition {
    /// Run an admitted prefix in order, deciding each command against preceding
    /// planned state. The admitted prefix shares a durable publication; it is
    /// not an all-or-nothing business batch.
    /// A definite rejection ends the prefix; an unprocessed suffix has no result.
    pub(super) async fn execute_writes(
        &self,
        requests: Vec<RecordWrite>,
        delivery: IndexDelivery,
    ) -> Result<BatchResult, RecordError> {
        self.execute_writes_detailed(requests, delivery)
            .await
            .map_err(|failure| {
                // Single-write callers retain their existing recovery error contract.
                let BatchFailure {
                    error,
                    scope: _scope,
                } = failure;
                error
            })
    }

    pub(super) async fn execute_writes_detailed(
        &self,
        requests: Vec<RecordWrite>,
        delivery: IndexDelivery,
    ) -> Result<BatchResult, BatchFailure> {
        if requests.is_empty() || requests.len() > MAX_BATCH_RECORDS {
            return Err(RecordError::Rejected("invalid Record batch size".into()).into());
        }
        let bytes = requests.iter().try_fold(0usize, |total, request| {
            total
                .checked_add(checkpoint_codec::json_length(request)?)
                .filter(|total| *total <= MAX_BATCH_BYTES)
                .ok_or_else(|| RecordError::Rejected("Record batch byte budget exceeded".into()))
        })?;
        // Retain queue/planner ownership until the owned task finishes, even if
        // the caller cancels. The existing admission semaphore bounds waiters.
        let memory = crate::storage::build_budget::reserve_memory(bytes as u64)
            .map_err(|_| RecordError::Busy)?;
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let partition = self.clone();
        crate::resources::spawn(async move {
            let _permit = permit;
            let _memory = memory;
            let writer = partition.writer.clone().lock_owned().await;
            let mut scope = AttemptScope::default();
            let result = std::panic::AssertUnwindSafe(async {
                let mut inner = partition.inner.lock().await;
                if !inner.healthy {
                    return Err(RecordError::RecoveryRequired);
                }
                inner.observe_time()?;
                inner.index.attach_cursors(partition.indexes.clone());
                let buffered = !matches!(delivery, IndexDelivery::Synchronous);
                let synchronous = !matches!(delivery, IndexDelivery::Asynchronous);
                if synchronous {
                    inner.index.drain_indexes().await?;
                }
                if buffered {
                    inner
                        .index
                        .begin_batch(partition.indexes.clone(), Arc::downgrade(&partition.inner))
                        .await?;
                }
                drop(inner);
                let prepared = partition.prepare_batch(&writer, requests).await;
                inner = partition.inner.lock().await;
                if let Ok(prepared) = &prepared {
                    scope.replayed = prepared
                        .result
                        .results
                        .iter()
                        .enumerate()
                        .filter_map(|(i, result)| {
                            result
                                .as_ref()
                                .ok()
                                .filter(|r| r.seq <= prepared.base_seq)
                                .map(|r| (i, r.clone()))
                        })
                        .collect();
                    if let Some(error) = prepared
                        .result
                        .stop
                        .as_ref()
                        .filter(|e| requires_recovery(e))
                    {
                        return Err(error.clone());
                    }
                }
                let result = match prepared {
                    Ok(prepared) => {
                        let upload = inner.prepare_remote_batch(&prepared).await;
                        match upload {
                            Ok(Some(upload)) => {
                                drop(inner);
                                let uploaded = upload.upload().await;
                                inner = partition.inner.lock().await;
                                match uploaded {
                                    Ok(uploaded) => {
                                        partition
                                            .commit_batch(&mut inner, prepared, &mut scope, Some(uploaded))
                                            .await
                                    }
                                    Err(error) => Err(error),
                                }
                            }
                            Ok(None) => partition.commit_batch(&mut inner, prepared, &mut scope, None).await,
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err(error),
                };
                let stop = match &result {
                    Ok(batch) => batch.stop.as_ref(),
                    Err(error) => Some(error),
                };
                if stop.is_some_and(requires_recovery) {
                    inner.fence_failed_write();
                }
                if !inner.healthy {
                    if buffered {
                        inner.index.abort_batch();
                    }
                    return Err(match result {
                        Err(error) => error,
                        Ok(batch) => batch.stop.unwrap_or(RecordError::RecoveryRequired),
                    });
                }
                // Preserve sparse known replays when publication never starts (e.g.
                // TTL revalidation or Busy). The scoped failure carries their receipts.
                let interrupted = result.as_ref().err().cloned();
                let batch = match result {
                    Ok(batch) => batch,
                    Err(error) => BatchResult {
                        results: Vec::new(),
                        stop: Some(error),
                    },
                };
                if !buffered {
                    partition.indexes.immediate(inner.state.seq);
                    return interrupted.map_or(Ok(batch), Err);
                }
                // Consumers do not acquire Inner. Synchronous batches join them
                // under this guard; asynchronous ingestion returns after submission.
                inner
                    .output
                    .write()
                    .map_err(|_| RecordError::RecoveryRequired)?
                    .fenced = true;
                let sequence = inner.state.seq;
                let publication = async {
                    inner.index.finish_batch(sequence, partition.indexes.clone()).await?;
                    if synchronous {
                        // The owned writer and output fence survive cancellation
                        // and remain held until both consumers have published.
                        inner.index.drain_indexes().await?;
                    }
                    Ok::<_, RecordError>(())
                }.await;
                if let Err(error) = publication {
                    inner.healthy = false;
                    log::error!(
                        "Record batch publication failed at sequence {}: {:?}",
                        inner.state.seq,
                        error
                    );
                    return Err(RecordError::DurablePending(inner.state.seq));
                }
                inner
                    .output
                    .write()
                    .map_err(|_| RecordError::RecoveryRequired)?
                    .fenced = false;
                interrupted.map_or(Ok(batch), Err)
            })
            .catch_unwind()
            .await;
            let result = result.unwrap_or_else(|_| {
                Err(RecordError::OutcomeUnknown(
                    "Record batch task unwound".into(),
                ))
            });
            if result.as_ref().err().is_some_and(requires_recovery) {
                // Keep writer ownership through cleanup, including unwind during upload.
                partition.inner.lock().await.fence_failed_write();
            }
            drop(writer);
            partition.kick_maintenance().await;
            result.map_err(|error| BatchFailure {
                error,
                scope: Some(scope),
            })
        })
        .await
        .map_err(|error| BatchFailure {
            error: RecordError::OutcomeUnknown(error.to_string()),
            scope: None,
        })?
    }
}

impl Inner {
    fn fence_failed_write(&mut self) {
        self.healthy = false;
        self.index.abort_batch();
        if let Ok(mut output) = self.output.write() {
            output.fenced = true;
        }
    }
}

impl RecordPartition {
    async fn prepare_batch<'writer>(
        &self,
        writer: &'writer tokio::sync::OwnedMutexGuard<()>,
        requests: Vec<RecordWrite>,
    ) -> Result<PreparedBatch<'writer>, RecordError> {
        // A guard from another partition cannot authorize this captured basis.
        if !Arc::ptr_eq(tokio::sync::OwnedMutexGuard::mutex(writer), &self.writer) {
            return Err(RecordError::RecoveryRequired);
        }
        // Keep helper calls here canonical-read-only: index rotation, admission
        // and checkpoint requests may change derived state, never install State.
        let inner = self.inner.lock().await;
        let config = inner.config.clone();
        let evaluation_time_ms = inner.evaluation_time_ms;
        #[cfg(test)]
        let root = inner.journal.root().to_path_buf();
        let base_seq = inner.state.seq;
        let mut candidate = PlanningView::new(&inner.state)?;
        let mut snapshot = inner.snapshot_bytes;
        let mut plans = Vec::new();
        let mut publication = Vec::new();
        let mut result = BatchResult {
            results: Vec::new(),
            stop: None,
        };
        let mut growth_total = 0u64;
        let mut updates_total = 0usize;
        let mut journal_bytes = 0usize;
        let mut expires_at:Option<u64>=None;
        let mut container_bound = inner.state.container_bytes(snapshot)?;
        drop(inner);
        for request in requests {
            let plan = async {
                let mut inner = self.inner.lock().await;
                if !inner.healthy {
                    return Err(RecordError::RecoveryRequired);
                }
                let (previous, operation) = candidate.read(&inner.state, &request);
                let input = candidate.input(previous.as_deref(), operation.as_deref());
                if input.operation.is_none() {
                    if inner.maintenance_requested.is_some() {
                        return Err(RecordError::Busy);
                    }
                    // New prefix decisions are above the fixed history floor;
                    // only the unchanged canonical prefix can require GC.
                    if maintenance::has_expired_operations(&inner.state, current_epoch().max(candidate.epoch_watermark))? {
                        return Err(inner.request_checkpoint("expired_operation_receipts"));
                    }
                }
                drop(inner);
                #[cfg(test)]
                pause_encoding(&root).await;
                let plan = mutation::prepare_input(
                    &input,
                    &config,
                    snapshot,
                    request,
                    current_epoch(),
                    evaluation_time_ms,
                )?;
                let mut inner = self.inner.lock().await;
                let mutation::PreparedWrite::Append {
                    applied,
                    bytes,
                    snapshot_bytes,
                    reserve_bytes,
                    ..
                } = &plan
                else {
                    return Ok(plan);
                };
                let (growth, updates) =
                    build_budget::write_estimate_for(input.previous, &inner.config, applied)?;
                if growth > 0 {
                    let fresh = (inner.config.schema.len() as u64)
                        .checked_mul(65536 * 32)
                        .and_then(|n| n.checked_add(growth))
                        .ok_or_else(|| storage("batch admission overflow"))?;
                    crate::storage::build_budget::validate_single(fresh)
                        .map_err(|e| RecordError::Rejected(e.to_string()))?;
                    if updates > 128 {
                        return Err(RecordError::Rejected(
                            "operation exceeds mutable native update bound".into(),
                        ));
                    }
                }
                let total = growth_total
                    .checked_add(growth)
                    .ok_or_else(|| storage("batch growth overflow"))?;
                let native = updates_total
                    .checked_add(updates)
                    .ok_or_else(|| storage("batch native bound overflow"))?;
                if !inner.index.can_admit(total, native).await {
                    if plans.is_empty() {
                        inner.maintain_index_tail().await?;
                    }
                    if !inner.index.can_admit(total, native).await {
                        return Err(RecordError::Busy);
                    }
                }
                // One physical frame per decision; account for every 24-byte header.
                let physical = journal_bytes
                    .checked_add(bytes.len())
                    .and_then(|n| n.checked_add(24))
                    .ok_or_else(|| storage("batch journal bound overflow"))?;
                if let Err(error) = inner
                    .ensure_journal_room(physical - 24, *reserve_bytes)
                    .await
                {
                    // Aggregate pressure must not durably reject a valid item.
                    return Err(if plans.is_empty() || !definite(&error) {
                        error
                    } else {
                        RecordError::Busy
                    });
                }
                if let Some(remote) = inner.remote.as_ref() {
                    let mut entries: Vec<_> = plans
                        .iter()
                        .filter_map(|plan| match plan {
                            mutation::PreparedWrite::Append { applied, bytes, .. } => Some((
                                applied.receipt.seq,
                                format!(
                                    "{}:{}",
                                    applied.request.operation_id.epoch,
                                    applied.request.operation_id.nonce
                                ),
                                bytes.as_slice(),
                            )),
                            _ => None,
                        })
                        .collect();
                    entries.push((
                        applied.receipt.seq,
                        format!(
                            "{}:{}",
                            applied.request.operation_id.epoch, applied.request.operation_id.nonce
                        ),
                        bytes,
                    ));
                    match remote.batch_tail_needs_checkpoint(&entries).await {
                        Ok(false) => {}
                        Ok(true) => return Err(inner.request_checkpoint("remote_tail_room")),
                        Err(error) => {
                            return Err(if plans.is_empty() || !definite(&error) {
                                error
                            } else {
                                RecordError::Busy
                            });
                        }
                    }
                }
                // Only this command's read/constructed rows can affect its
                // TTL decision. Unrelated expired bodies await background GC.
                let prior_deadline=input.previous
                    .filter(|r|temporal::visible(r,evaluation_time_ms)).and_then(|r|r.temporal.expire_at_ms);
                let mut prepared = state_memory::Prepared::from_previous(input.previous, applied)?;
                if let Some(document)=&prepared.record.document {
                    prepared.component=Some(inner.index.reserve_component(&prepared.record,&inner.config).await?);
                    prepared.lexical=Some(if growth<=crate::storage::build_budget::EXPLICIT_BUFFERS {
                        lexical::memory::Admission::Visible(inner.index.reserve_visible(document,&inner.config,&prepared.record.record_id,inner.journal.owner_guard()).await?)
                    }else{
                        // Later escrows must count this row even while an
                        // asynchronous fallback has not published its base yet.
                        lexical::memory::Admission::Fallback(inner.index.reserve_fallback(&prepared.record,inner.journal.owner_guard()).await?)
                    });
                }
                let next_deadline=Some(&*prepared.record)
                    .filter(|r|r.document.is_some()).and_then(|r|r.temporal.expire_at_ms);
                let container = candidate.advance(&input, applied, &prepared, *snapshot_bytes)?;
                expires_at=[expires_at,prior_deadline,next_deadline].into_iter().flatten().min();
                snapshot = *snapshot_bytes;
                container_bound = container_bound.max(container);
                growth_total = total;
                updates_total = native;
                journal_bytes = physical;
                publication.push(Some(prepared));
                Ok(plan)
            }
            .await;
            match plan {
                Ok(mutation::PreparedWrite::Replay(receipt)) => result.results.push(Ok(receipt)),
                Ok(plan) => {
                    if let mutation::PreparedWrite::Append { applied, .. } = &plan {
                        result.results.push(Ok(applied.receipt.clone()));
                    }
                    plans.push(plan);
                }
                Err(error) if definite(&error) => {
                    result.results.push(Err(error));
                    break;
                }
                Err(error) => {
                    result.stop = Some(error);
                    break;
                }
            }
        }
        let mut inner = self.inner.lock().await;
        if plans.is_empty() {
            return Ok(PreparedBatch {
                _writer: writer,
                plans,
                publication: Vec::new(),
                result,
                base_seq,
                expires_at: None,
            });
        }
        // Admit live container growth before persistence. Values are separately owned.
        inner.state.admit_containers(container_bound)?;
        drop(candidate);
        Ok(PreparedBatch {
            _writer: writer,
            plans,
            publication,
            result,
            base_seq,
            expires_at,
        })
    }
}

impl RecordPartition {
    async fn commit_batch(
        &self,
        guard: &mut tokio::sync::MutexGuard<'_, Inner>,
        prepared: PreparedBatch<'_>,
        scope: &mut AttemptScope,
        uploaded: Option<UploadedBatch>,
    ) -> Result<BatchResult, RecordError> {
        // Bind both borrowed guards to this partition before touching authority.
        // A held writer for another partition, or another Inner, is insufficient.
        if !Arc::ptr_eq(tokio::sync::OwnedMutexGuard::mutex(prepared._writer), &self.writer)
            || !std::ptr::eq(tokio::sync::MutexGuard::mutex(guard), self.inner.as_ref()) {
            return Err(RecordError::RecoveryRequired);
        }
        let inner=&mut **guard;
        if !inner.healthy || inner.state.seq != prepared.base_seq {
            return Err(RecordError::RecoveryRequired);
        }
        // Wall time can move while uploading even though this task owns the writer.
        // Reobserve time before selecting the candidate, then discard any
        // candidate whose captured basis or own TTL has become stale.
        inner.observe_time()?;
        // The borrowed writer excludes canonical basis changes, including
        // same-sequence trim/GC/install. This is ownership, not seq equality or
        // a cached validity claim. Time and independent failure fences can move.
        if prepared.expires_at.is_some_and(|deadline| deadline <= inner.evaluation_time_ms) {
            return Err(RecordError::Busy);
        }
        let PreparedBatch {
            plans,
            publication,
            result,
            base_seq,
            ..
        } = prepared;
        if plans.is_empty() {
            return Ok(result);
        }
        let payloads: Vec<_> = plans
            .iter()
            .map(|plan| match plan {
                mutation::PreparedWrite::Append { bytes, .. } => bytes.as_slice(),
                _ => unreachable!(),
            })
            .collect();
        let sequence = match plans.last().unwrap() {
            mutation::PreparedWrite::Append { applied, .. } => applied.receipt.seq,
            _ => unreachable!(),
        };
        #[cfg(feature="fault-injection")]
        let injected = std::env::var("ANYSEEK_RECORD_FAULT_NONCE").ok().filter(|nonce| plans.iter().any(|plan|
            matches!(plan, mutation::PreparedWrite::Append { applied, .. } if &applied.request.operation_id.nonce == nonce)))
            .and_then(|_|std::env::var("ANYSEEK_RECORD_FAULT").ok());
        #[cfg(feature = "fault-injection")]
        if injected.as_deref() == Some("crash_before_append") {std::process::exit(86);}
        #[cfg(feature = "fault-injection")]
        if injected.as_deref() == Some("busy_before_append") {
            return Err(RecordError::Busy);
        }
        #[cfg(feature = "fault-injection")]
        if injected.as_deref() == Some("before_append") {
            return Err(RecordError::Rejected("injected before append".into()));
        }
        inner.healthy = false;
        {
            let mut output = inner
                .output
                .write()
                .map_err(|_| RecordError::RecoveryRequired)?;
            output.fenced = true;
        }
        scope.persistence_attempted = result
            .results
            .iter()
            .enumerate()
            .filter_map(|(i, result)| result.as_ref().ok().filter(|r| r.seq > base_seq).map(|_| i))
            .collect();
        if let Some(uploaded) = uploaded {
            let remote = inner.remote.as_mut().ok_or(RecordError::RecoveryRequired)?;
            uploaded.publish(remote).await?;
        }
        inner.index.attach_cursors(self.indexes.clone());
        inner.append_operations(&payloads, sequence).await?;
        #[cfg(feature = "fault-injection")]
        if let Some(path) = std::env::var_os("ANYSEEK_RECORD_BATCH_TRACE") {
            use std::io::Write;
            let mut trace = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .map_err(storage)?;
            writeln!(trace, "{} {}", sequence, payloads.len()).map_err(storage)?;
        }
        #[cfg(feature = "fault-injection")]
        match injected.as_deref() {
            Some("after_append_unknown") => {
                return Err(RecordError::OutcomeUnknown(
                    "injected uncertain batch append".into(),
                ));
            }
            Some("after_durable") => return Err(RecordError::DurablePending(sequence)),
            Some("crash_after_durable") => std::process::exit(86),
            _ => {}
        }
        #[cfg(test)]
        if inner.fail_after_durable {
            return Err(RecordError::DurablePending(sequence));
        }
        for (plan, prepared) in plans.into_iter().zip(publication) {
            if let mutation::PreparedWrite::Append {
                applied,
                snapshot_bytes,
                ..
            } = plan
            {
                inner.publish_prepared(&applied, snapshot_bytes, prepared)
                    .await?;
            }
        }
        inner.healthy = true;
        inner.output
            .write()
            .map_err(|_| RecordError::RecoveryRequired)?
            .fenced = false;
        #[cfg(feature = "fault-injection")]
        if injected.as_deref() == Some("crash_after_visible") {
            std::process::exit(86);
        }
        #[cfg(test)]
        if std::env::var("ANYSEEK_RECORD_CRASH").as_deref() == Ok("published") {
            std::process::exit(74);
        }
        Ok(result)
    }
}

#[cfg(test)]
async fn pause_encoding(root: &std::path::Path) {
    let pause = root.join(".test-write-encode-pause");
    if pause.exists() {
        std::fs::write(root.join(".test-write-encode-started"), b"started").unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while pause.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("encoding test gate released");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, write};
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn planning_requires_the_same_partition_writer() {
        let root=tempfile::tempdir().unwrap();
        let a=RecordPartition::create(&root.path().join("a"),config()).await.unwrap();
        let b=RecordPartition::create(&root.path().join("b"),config()).await.unwrap();
        let wrong=b.writer.clone().lock_owned().await;
        let request=write("new","r","alpha",ExpectedVersion::Absent);
        assert!(matches!(a.prepare_batch(&wrong,vec![request.clone()]).await,Err(RecordError::RecoveryRequired)));
        drop(wrong);
        let writer=a.writer.clone().lock_owned().await;
        let mut target=b.inner.lock().await;
        let mut scope=AttemptScope::default();
        let prepared=a.prepare_batch(&writer,vec![request.clone()]).await.unwrap();
        assert!(matches!(a.commit_batch(&mut target,prepared,&mut scope,None).await,Err(RecordError::RecoveryRequired)),"wrong Inner guard");
        let prepared=a.prepare_batch(&writer,vec![request.clone()]).await.unwrap();
        assert!(matches!(b.commit_batch(&mut target,prepared,&mut scope,None).await,Err(RecordError::RecoveryRequired)),"wrong writer guard");
        drop(target);drop(writer);
        assert_eq!(a.write(request).await.unwrap().seq,1);
        assert_eq!(b.write(write("other","r","beta",ExpectedVersion::Absent)).await.unwrap().seq,1);
    }

    #[tokio::test]
    async fn planning_excludes_history_install_but_allows_read_time() {
        struct Clock(std::sync::atomic::AtomicU64);
        impl temporal::Clock for Clock {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0.load(Ordering::SeqCst))}}
        let root=tempfile::tempdir().unwrap();let path=root.path().join("records");
        let settings=config();let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
        let clock=Arc::new(Clock(std::sync::atomic::AtomicU64::new(1000)));
        p.inner.lock().await.clock=clock.clone();
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(path.join(".test-write-encode-pause"),b"pause").unwrap();
        let request=write("second","b","beta",ExpectedVersion::Absent);
        let pending={let p=p.clone();let request=request.clone();tokio::spawn(async move {p.write(request).await})};
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !path.join(".test-write-encode-started").exists(){tokio::task::yield_now().await;}
        }).await.unwrap();
        clock.0.store(1001+settings.history.retention_ms,Ordering::SeqCst);
        assert_eq!(p.statistics().await.unwrap().visible_seq,1);
        p.maintenance_tick().await;
        {let inner=p.inner.lock().await;assert_eq!(inner.state.history.floor,0);assert!(inner.maintenance_scheduled);}
        assert!(p.writer.try_lock().is_err());
        std::fs::remove_file(path.join(".test-write-encode-pause")).unwrap();
        let receipt=pending.await.unwrap().unwrap();assert_eq!(receipt.seq,2);
        crate::record::tests::wait_maintenance_idle(&p).await;
        {let inner=p.inner.lock().await;assert_eq!(inner.state.seq,2);assert_eq!(inner.state.history.floor,2);}
        drop(p);let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.write(request).await.unwrap(),receipt);
    }

    // Exercise cancellation at the Record ownership boundary, after the actual
    // native add, rather than cancelling Runtime's own result receiver.
    async fn owned_fallback_caller_cancellation(asynchronous:bool) {
        let directory=tempfile::tempdir().unwrap();let root=directory.path().join("records");
        let mut settings=config();settings.max_record_bytes=512*1024;settings.max_journal_bytes=16*1024*1024;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let gate=native.package_path().join(".test-fallback-native-block");
        let vector_pause=root.join(".test-vector-consumer-pause");
        std::fs::write(&gate,b"pause").unwrap();std::fs::write(&vector_pause,b"pause").unwrap();
        let request=write("owned-large","large",&"alpha ".repeat(50_000),ExpectedVersion::Absent);
        assert!(build_budget::document_estimate(request.document.as_ref().unwrap(),&settings.schema).unwrap()
            >crate::storage::build_budget::EXPLICIT_BUFFERS);
        let delivery=if asynchronous {IndexDelivery::Asynchronous}else{IndexDelivery::SynchronousBatch};
        let caller={let owner=p.clone();let request=request.clone();tokio::spawn(async move {
            owner.execute_writes(vec![request],delivery).await
        })};
        let (caller,ack)=if asynchronous {
            let batch=tokio::time::timeout(std::time::Duration::from_secs(5),caller).await.unwrap().unwrap().unwrap();
            assert_eq!(batch.stop,None);assert_eq!(batch.results.len(),1);
            (None,Some(batch.results.into_iter().next().unwrap().unwrap()))
        }else{(Some(caller),None)};
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !gate.with_extension("started").exists(){tokio::task::yield_now().await;}
        }).await.unwrap();
        assert_eq!(p.indexes.lexical.load(Ordering::Acquire),0,"native add is not yet published");
        let cancelled=if let Some(caller)=caller {caller}else{
            let (ready,started)=tokio::sync::oneshot::channel();let owner=p.clone();
            let waiter=tokio::spawn(async move {
                let inner=owner.inner.lock().await;
                let mut drain=Box::pin(inner.index.drain_indexes());
                assert!(futures::poll!(drain.as_mut()).is_pending(),"waiter must actually reach the running worker");
                ready.send(()).unwrap();drain.await.unwrap();
                // Match execute_writes' result type without invoking another write.
                Ok::<BatchResult,RecordError>(BatchResult{results:vec![],stop:None})
            });
            started.await.unwrap();waiter
        };
        let occupied=crate::storage::build_budget::used_memory_for_test();
        cancelled.abort();assert!(matches!(cancelled.await,Err(error) if error.is_cancelled()));
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),occupied,
            "caller cancellation cannot refund the running native/component workspace");
        assert!(gate.exists());assert_eq!(p.indexes.lexical.load(Ordering::Acquire),0);
        let pressure=crate::storage::build_budget::fill_memory_for_test();
        std::fs::remove_file(&gate).unwrap();std::fs::remove_file(&vector_pause).unwrap();
        // A cancelled drain waiter relinquishes its JoinHandle; cursor progress
        // AND released Inner ownership prove the detached service worker exited.
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while p.indexes.lexical.load(Ordering::Acquire)<1 || p.indexes.vector.load(Ordering::Acquire)<1
                || Arc::strong_count(&p.inner)!=1 {tokio::task::yield_now().await;}
            let inner=p.inner.lock().await;
            inner.index.drain_indexes().await.unwrap();inner.index.join_materializers().await.unwrap();
            assert!(inner.healthy);assert!(!inner.output.read().unwrap().fenced);assert_eq!(inner.state.seq,1);
            assert!(p.indexes.error().is_none());
        }).await.unwrap();
        assert_eq!(native.snapshot().await.unwrap().identities.len(),1,"exactly one native add despite cancellation");
        drop(pressure);
        let receipt=p.write(request.clone()).await.unwrap();assert_eq!(receipt.seq,1);
        if let Some(ack)=ack {assert_eq!(receipt,ack);}
        assert_eq!(p.write(request.clone()).await.unwrap(),receipt);
        assert_eq!(p.search("alpha".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().count_total,1);
        let next=write("after-cancel","next","beta",ExpectedVersion::Absent);
        let next_receipt=p.write(next.clone()).await.unwrap();assert_eq!(next_receipt.seq,2);
        {let inner=p.inner.lock().await;inner.index.drain_indexes().await.unwrap();inner.index.join_materializers().await.unwrap();}
        p.finish_service_shutdown().await.unwrap();drop(native);drop(p);
        let reopened=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        assert_eq!(reopened.write(request).await.unwrap(),receipt);assert_eq!(reopened.write(next).await.unwrap(),next_receipt);
        assert_eq!(reopened.search("alpha".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().count_total,1);
        assert_eq!(reopened.get("next").await.unwrap().unwrap().version,1);
        reopened.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn synchronous_batch_caller_cancel_keeps_running_fallback_owned() {
        owned_fallback_caller_cancellation(false).await;
    }

    #[tokio::test]
    async fn acknowledged_async_index_waiter_cancel_keeps_running_fallback_owned() {
        owned_fallback_caller_cancellation(true).await;
    }

    #[tokio::test]
    async fn synchronous_fallback_temporal_patch_uses_exact_indexed_workspace() {
        let directory=tempfile::tempdir().unwrap();let root=directory.path().join("records");
        let mut settings=config();settings.max_record_bytes=512*1024;settings.max_journal_bytes=16*1024*1024;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        struct Clock(std::sync::atomic::AtomicU64);
        impl temporal::Clock for Clock {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0.load(std::sync::atomic::Ordering::SeqCst))}}
        let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let clock=Arc::new(Clock(std::sync::atomic::AtomicU64::new(now)));p.inner.lock().await.clock=clock.clone();
        let mut initial=write("temporal-large","large",&"alpha ".repeat(50_000),ExpectedVersion::Absent);
        initial.document.as_mut().unwrap().remove("vector");
        initial.temporal=Some(TemporalMetadata{event_time_ms:Some(now as i64-1_000),valid_from_ms:Some(now as i64-2_000),
            valid_to_ms:Some(now as i64+86_400_000),expire_at_ms:Some(now+86_400_000)});
        let first=p.write(initial.clone()).await.unwrap();
        clock.0.store(now+1_000,std::sync::atomic::Ordering::SeqCst);
        let mut patch=write("temporal-patch","large","",ExpectedVersion::Match(1));patch.document=None;
        patch.vector_patch=Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        let second=p.write(patch.clone()).await.unwrap();
        {let inner=p.inner.lock().await;assert!(inner.healthy);assert_eq!(inner.state.seq,2);
            assert_eq!(inner.state.records["large"].ingested_at_ms,now);
            assert_eq!(inner.state.records["large"].temporal.expire_at_ms,Some(now+86_400_000));}
        p.finish_service_shutdown().await.unwrap();drop(p);
        let reopened=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        assert_eq!(reopened.write(initial).await.unwrap(),first);assert_eq!(reopened.write(patch).await.unwrap(),second);
        reopened.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn fallback_and_mixed_prefixes_finish_at_full_memory_with_old_snapshots() {
        let directory=tempfile::tempdir().unwrap();let root=directory.path().join("records");
        let mut settings=config();settings.max_record_bytes=512*1024;settings.max_journal_bytes=16*1024*1024;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let mut pinned=Vec::new();let mut accepted=Vec::new();let mut total=0;
        for prefix in 0..3 {
            for lane in ["lexical","vector"] {std::fs::write(root.join(format!(".test-{lane}-consumer-pause")),b"pause").unwrap();}
            let id=format!("large-{prefix}");let large=write(&id,&id,&"alpha ".repeat(50_000),ExpectedVersion::Absent);
            assert!(build_budget::document_estimate(large.document.as_ref().unwrap(),&settings.schema).unwrap()>crate::storage::build_budget::EXPLICIT_BUFFERS);
            let small_id=format!("small-{prefix}");let small=write(&small_id,&small_id,"beta",ExpectedVersion::Absent);
            let requests=match prefix {0=>vec![large],1=>vec![large,small],_=>vec![small,large]};
            let result=tokio::time::timeout(std::time::Duration::from_secs(5),
                p.execute_writes(requests.clone(),IndexDelivery::Asynchronous)).await.unwrap().unwrap();
            assert_eq!(result.stop,None);assert_eq!(result.results.len(),requests.len());total+=requests.len();
            for (request,result) in requests.into_iter().zip(result.results) {accepted.push((request,result.unwrap()));}
            // The ACK precedes consumer work, and no memory headroom remains.
            let pressure=crate::storage::build_budget::fill_memory_for_test();
            for lane in ["lexical","vector"] {std::fs::remove_file(root.join(format!(".test-{lane}-consumer-pause"))).unwrap();}
            tokio::time::timeout(std::time::Duration::from_secs(10),async {
                let inner=p.inner.lock().await;inner.index.drain_indexes().await.unwrap();inner.index.join_materializers().await.unwrap();
                assert!(inner.healthy);assert!(!inner.output.read().unwrap().fenced);
            }).await.unwrap();
            let snapshot=native.snapshot().await.unwrap();assert_eq!(snapshot.identities.len(),total);
            pinned.push(snapshot);drop(pressure);
        }
        for (request,receipt) in &accepted {assert_eq!(p.write(request.clone()).await.unwrap(),*receipt);}
        p.finish_service_shutdown().await.unwrap();drop(pinned);drop(native);drop(p);
        let reopened=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        for (request,receipt) in accepted {assert_eq!(reopened.write(request).await.unwrap(),receipt);}
        reopened.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn fallback_and_visible_escrows_cover_mixed_and_async_queued_prefixes() {
        for (delivery,cross_batch) in [(IndexDelivery::Synchronous,false),
            (IndexDelivery::SynchronousBatch,false),(IndexDelivery::Asynchronous,false),
            (IndexDelivery::Asynchronous,true)] {
            let delivery_name=match delivery {IndexDelivery::Synchronous=>"synchronous",IndexDelivery::SynchronousBatch=>"synchronous_batch",IndexDelivery::Asynchronous=>"asynchronous"};
            eprintln!("fallback escrow case delivery={delivery_name} cross_batch={cross_batch} phase=start");
            let directory=tempfile::tempdir().unwrap();let root=directory.path().join("records");
            let mut settings=config();settings.max_record_bytes=512*1024;settings.max_journal_bytes=16*1024*1024;
            // 50k legal tokens select the real >64MiB estimate fallback while
            // remaining within wire, token, per-reservation and batch bounds.
            let large=write("large","large",&"alpha ".repeat(50_000),ExpectedVersion::Absent);
            assert!(build_budget::document_estimate(large.document.as_ref().unwrap(),&settings.schema).unwrap()
                >crate::storage::build_budget::EXPLICIT_BUFFERS);
            let small=write("small","small","beta",ExpectedVersion::Absent);
            let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
            let requests=vec![large.clone(),small.clone()];
            let receipts=if cross_batch {
                // Force the next planner to run before the fallback can publish
                // a new base. Async ACK must remain independent of this worker.
                let pause=root.join(".test-lexical-consumer-pause");std::fs::write(&pause,b"pause").unwrap();
                let first=tokio::time::timeout(std::time::Duration::from_secs(5),
                    p.execute_writes(vec![large.clone()],delivery)).await.unwrap().unwrap();
                assert_eq!(first.stop,None);assert_eq!(first.results.len(),1);
                let second=tokio::time::timeout(std::time::Duration::from_secs(5),
                    p.execute_writes(vec![small.clone()],delivery)).await.unwrap().unwrap();
                assert_eq!(second.stop,None);assert_eq!(second.results.len(),1);
                std::fs::remove_file(pause).unwrap();
                vec![first.results[0].as_ref().unwrap().clone(),second.results[0].as_ref().unwrap().clone()]
            }else{
                let batch=p.execute_writes(requests.clone(),delivery).await.unwrap();
                assert_eq!(batch.stop,None);assert_eq!(batch.results.len(),2);
                batch.results.into_iter().map(Result::unwrap).collect::<Vec<_>>()
            };
            {let inner=p.inner.lock().await;inner.index.drain_indexes().await.unwrap();inner.index.join_materializers().await.unwrap();
                assert!(inner.healthy);assert!(!inner.output.read().unwrap().fenced);assert_eq!(inner.state.seq,2);}
            let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
            assert_eq!(native.snapshot().await.unwrap().identities.len(),2,"no repeated native append");
            let retry=p.execute_writes(requests.clone(),delivery).await.unwrap();
            assert_eq!(retry.stop,None);assert_eq!(retry.results,receipts.iter().cloned().map(Ok).collect::<Vec<_>>());
            eprintln!("fallback escrow case delivery={delivery_name} cross_batch={cross_batch} phase=retry_complete inner_owners={}",Arc::strong_count(&p.inner));
            // An async receipt replay still submits empty consumer batches.
            // Service teardown requires those owned tasks to have drained.
            {let inner=p.inner.lock().await;inner.index.drain_indexes().await.unwrap();inner.index.join_materializers().await.unwrap();}
            assert_eq!(Arc::strong_count(&p.inner),1,"teardown must follow consumer joins");
            eprintln!("fallback escrow case delivery={delivery_name} cross_batch={cross_batch} phase=shutdown");
            p.finish_service_shutdown().await.unwrap();drop(native);drop(p);
            let reopened=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
            for (request,receipt) in requests.into_iter().zip(receipts) {assert_eq!(reopened.write(request).await.unwrap(),receipt);}
            assert_eq!(reopened.get("large").await.unwrap().unwrap().version,1);
            assert_eq!(reopened.get("small").await.unwrap().unwrap().version,1);
            reopened.finish_service_shutdown().await.unwrap();
            eprintln!("fallback escrow case delivery={delivery_name} cross_batch={cross_batch} phase=complete");
        }
    }

    #[tokio::test]
    async fn batch_reuses_ordered_values_across_vector_patch_and_body_updates() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone()).await.unwrap();
        let mut initial = crate::record::tests::write("initial","s","plain original",ExpectedVersion::Absent);
        initial.document.as_mut().unwrap().remove("vector");
        let mut patch = write("patch", "s", "", ExpectedVersion::Match(1));
        patch.document = None;
        patch.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        let update = write("update", "s", "updated", ExpectedVersion::Match(1));
        let other = crate::record::tests::write("peer","d","plain original",ExpectedVersion::Absent);
        let conflict = write("conflict", "s", "bad", ExpectedVersion::Match(1));
        let suffix = write("suffix", "unused", "unused", ExpectedVersion::Absent);
        let requests = vec![initial, patch, update, other, conflict, suffix];

        // Compare every retained value with an independent sequential projection,
        // including the vector-only revision followed by an ordinary body update.
        let writer=p.writer.clone().lock_owned().await;
        let mut oracle=p.inner.lock().await.state.try_clone().unwrap();
        let prepared = p.prepare_batch(&writer, requests.clone()).await.unwrap();
        assert_eq!(prepared.publication.len(), 4);
        for (plan, retained) in prepared.plans.iter().zip(&prepared.publication) {
            let mutation::PreparedWrite::Append { applied, .. } = plan else { panic!("append expected") };
            apply(&mut oracle, applied).unwrap();
            let retained = retained.as_ref().unwrap();
            assert_eq!(serde_json::to_value(&retained.record).unwrap(),
                serde_json::to_value(oracle.records.get(&applied.request.record_id).unwrap()).unwrap());
            assert_eq!(serde_json::to_value(&retained.operation).unwrap(), serde_json::to_value(oracle.operations.last().unwrap()).unwrap());
        }
        drop(prepared);drop(writer);
        let result = p.execute_writes(requests.clone(), IndexDelivery::Asynchronous).await.unwrap();
        assert_eq!(result.results.len(), 5);
        assert!(matches!(result.results[4], Err(RecordError::Conflict(_))));
        assert_eq!(result.results[..4].iter().map(|r| r.as_ref().unwrap().version).collect::<Vec<_>>(), [1, 1, 2, 1]);
        p.inner.lock().await.index.drain_indexes().await.unwrap();
        p.finish_service_shutdown().await.unwrap();drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.get("s").await.unwrap().unwrap().document.unwrap()["body"], "updated");
        assert!(p.get("d").await.unwrap().is_some());
        assert!(p.get("unused").await.unwrap().is_none());
        for (request, receipt) in requests[..4].iter().zip(&result.results) {
            assert_eq!(p.write(request.clone()).await.unwrap(), *receipt.as_ref().unwrap());
        }
    }

    #[tokio::test]
    async fn failed_batch_retains_only_attempted_prefix_and_prior_replays() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let old = write("scope-old", "old", "alpha", ExpectedVersion::Absent);
        let receipt = p.write(old.clone()).await.unwrap();
        let new = write("scope-new", "r", "beta", ExpectedVersion::Absent);
        let conflict = write("scope-conflict", "r", "bad", ExpectedVersion::Absent);
        let suffix = write("scope-suffix", "s", "gamma", ExpectedVersion::Absent);
        p.inner.lock().await.fail_after_durable = true;
        let failure = p
            .execute_writes_detailed(
                vec![
                    old,
                    new.clone(),
                    new.clone(),
                    conflict.clone(),
                    suffix.clone(),
                ],
                IndexDelivery::Asynchronous,
            )
            .await
            .err()
            .unwrap();
        assert!(matches!(failure.error, RecordError::DurablePending(2)));
        let scope = failure.scope.unwrap();
        assert_eq!(scope.persistence_attempted, vec![1, 2]);
        assert_eq!(scope.replayed, vec![(0, receipt)]);
        p.finish_service_shutdown().await.unwrap();drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(p.write(new).await.unwrap().seq, 2);
        assert!(p.operation(&conflict.operation_id).await.unwrap().is_none());
        assert!(p.operation(&suffix.operation_id).await.unwrap().is_none());
        assert!(p.get("s").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn durable_batch_unknown_reopens_all_decisions_and_replays_original_ids() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let requests = vec![
            write("batch-a", "r", "alpha", ExpectedVersion::Absent),
            write("batch-b", "r", "beta", ExpectedVersion::Match(1)),
        ];
        p.inner.lock().await.fail_after_durable = true;
        assert!(matches!(
            p.execute_writes(requests.clone(), IndexDelivery::Asynchronous)
                .await,
            Err(RecordError::DurablePending(2))
        ));
        assert!(!p.inner.lock().await.healthy);
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(p.get("r").await.unwrap().unwrap().version, 2);
        let retry = p
            .execute_writes(requests, IndexDelivery::Asynchronous)
            .await
            .unwrap();
        assert_eq!(
            retry
                .results
                .iter()
                .map(|r| r.as_ref().unwrap().seq)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(p.inner.lock().await.state.seq, 2);
    }

    #[tokio::test]
    async fn committed_prefix_decides_versions_before_reply_without_waiting_for_indexes() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        for lane in ["lexical", "vector"] {
            std::fs::write(root.join(format!(".test-{lane}-consumer-pause")), b"pause").unwrap();
        }
        let first = write("first", "r", "alpha", ExpectedVersion::Absent);
        let second = write("second", "r", "beta", ExpectedVersion::Match(1));
        let conflict = write("conflict", "r", "bad", ExpectedVersion::Match(1));
        let suffix = write("suffix", "s", "suffix", ExpectedVersion::Absent);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            p.execute_writes(
                vec![
                    first.clone(),
                    second.clone(),
                    conflict.clone(),
                    suffix.clone(),
                ],
                IndexDelivery::Asynchronous,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.results.len(), 3);
        let receipts: Vec<_> = result.results[..2]
            .iter()
            .map(|r| r.clone().unwrap())
            .collect();
        assert_eq!((receipts[0].version, receipts[1].version), (1, 2));
        assert!(matches!(result.results[2], Err(RecordError::Conflict(_))));
        assert!(result.stop.is_none());
        assert_eq!(p.indexes.lexical.load(Ordering::Acquire), 0);
        assert_eq!(p.indexes.vector.load(Ordering::Acquire), 0);
        assert_eq!(
            p.get("r").await.unwrap().unwrap().document.unwrap()["body"],
            "beta"
        );
        assert!(p.get("s").await.unwrap().is_none());
        assert!(p.operation(&conflict.operation_id).await.unwrap().is_none());
        assert!(p.operation(&suffix.operation_id).await.unwrap().is_none());
        assert!(!root.join("intake").exists());
        for lane in ["lexical", "vector"] {
            std::fs::remove_file(root.join(format!(".test-{lane}-consumer-pause"))).unwrap();
        }
        p.inner.lock().await.index.drain_indexes().await.unwrap();
        p.finish_service_shutdown().await.unwrap();drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        for (request, receipt) in [first, second].into_iter().zip(receipts) {
            assert_eq!(p.write(request).await.unwrap(), receipt);
        }
        assert!(matches!(
            p.write(conflict).await,
            Err(RecordError::Conflict(_))
        ));
        assert_eq!(p.get("r").await.unwrap().unwrap().version, 2);
        assert!(p.get("s").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn committed_executor_retains_durable_unknown_outcome_for_original_retry() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let request = write("unknown", "r", "alpha", ExpectedVersion::Absent);
        p.inner.lock().await.fail_after_durable = true;
        let result = p
            .execute_writes(vec![request.clone()], IndexDelivery::Asynchronous)
            .await;
        assert!(matches!(result, Err(RecordError::DurablePending(1))));
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        let receipt = p.operation(&request.operation_id).await.unwrap().unwrap();
        assert_eq!(p.write(request).await.unwrap(), receipt);
        assert_eq!(p.get("r").await.unwrap().unwrap().version, 1);
    }

    #[tokio::test]
    async fn committed_executor_bounds_and_cancelled_waiter_do_not_change_commit_contract() {
        let directory = tempfile::tempdir().unwrap();
        let p = RecordPartition::create(&directory.path().join("records"), config())
            .await
            .unwrap();
        let request = write("kept", "r", "alpha", ExpectedVersion::Absent);
        for requests in [
            vec![],
            vec![request.clone(); MAX_BATCH_RECORDS + 1],
            vec![write(
                "oversized",
                "big",
                &"x".repeat(MAX_BATCH_BYTES),
                ExpectedVersion::Absent,
            )],
        ] {
            assert!(matches!(
                p.execute_writes(requests, IndexDelivery::Asynchronous)
                    .await,
                Err(RecordError::Rejected(_))
            ));
        }
        assert_eq!(p.inner.lock().await.state.seq, 0);
        let guard = p.inner.lock().await;
        let available = p.admission.available_permits();
        let worker = p.clone();
        let command = request.clone();
        let task = tokio::spawn(async move {
            worker
                .execute_writes(
                    vec![
                        command,
                        write("kept-second", "r", "beta", ExpectedVersion::Match(1)),
                    ],
                    IndexDelivery::Asynchronous,
                )
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while p.admission.available_permits() == available {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while p.admission.available_permits() != available {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let receipt = p.operation(&request.operation_id).await.unwrap().unwrap();
        assert_eq!(p.write(request).await.unwrap(), receipt);
        assert_eq!(p.get("r").await.unwrap().unwrap().version, 2);
    }
}
