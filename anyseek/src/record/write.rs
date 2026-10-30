//! Shared bounded Record execution. Successful results are durable business decisions.
//! Index publication can be synchronous or asynchronous; intake is only a caller.
use super::*;
use futures::FutureExt;

/// Only the completed prefix may be recorded in intake. Busy and uncertain
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
    Asynchronous,
}

struct PreparedBatch {
    plans: Vec<mutation::PreparedWrite>,
    publication: Vec<Option<state_memory::Prepared>>,
    result: BatchResult,
    base_seq: u64,
    base_state: State,
    expires_at: Option<u64>,
}

#[cfg(feature = "s3-object-store")]
struct RemoteUpload {
    store: crate::storage::s3_compatible::S3CompatibleStore,
    path: std::path::PathBuf,
    head: crate::storage::sqlite_authority::PartitionHead,
    block: crate::storage::object_log::ObjectLogBlock,
    max_bytes: usize,
    #[cfg(test)]
    root: std::path::PathBuf,
}
#[cfg(feature = "s3-object-store")]
struct UploadedBatch {
    head: crate::storage::sqlite_authority::PartitionHead,
    block: crate::storage::object_log::ObjectLogBlock,
    reference: crate::storage::sqlite_authority::ObjectRef,
}
#[cfg(feature = "s3-object-store")]
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
        let key = format!(
            "partitions/{}/wal/{:020}-{}",
            self.head.partition_id,
            self.block.first_seq,
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
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
#[cfg(feature = "s3-object-store")]
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
#[cfg(feature = "s3-object-store")]
impl Inner {
    async fn prepare_remote_batch(
        &self,
        prepared: &PreparedBatch,
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
    // Transitional adapter: retained intake still owns its Accept/Complete recovery.
    pub(super) async fn write_ingest_batch(
        &self,
        requests: Vec<RecordWrite>,
    ) -> Result<BatchResult, RecordError> {
        self.execute_writes(requests, IndexDelivery::Asynchronous)
            .await
    }

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
                // Legacy callers retain their recovery contract until HTTP switches.
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
            let writer = partition.writer.lock().await;
            let mut scope = AttemptScope::default();
            let result = std::panic::AssertUnwindSafe(async {
                let mut inner = partition.inner.lock().await;
                if !inner.healthy {
                    return Err(RecordError::RecoveryRequired);
                }
                inner.advance_time().await?;
                let asynchronous = matches!(delivery, IndexDelivery::Asynchronous);
                if asynchronous {
                    inner
                        .index
                        .begin_batch(partition.indexes.clone(), Arc::downgrade(&partition.inner))
                        .await?;
                } else {
                    inner.index.drain_indexes().await?;
                }
                let prepared = inner.prepare_batch(requests).await;
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
                #[cfg(feature = "s3-object-store")]
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
                                        inner
                                            .commit_batch(prepared, &mut scope, Some(uploaded))
                                            .await
                                    }
                                    Err(error) => Err(error),
                                }
                            }
                            Ok(None) => inner.commit_batch(prepared, &mut scope, None).await,
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => Err(error),
                };
                #[cfg(not(feature = "s3-object-store"))]
                let result = match prepared {
                    Ok(prepared) => inner.commit_batch(prepared, &mut scope).await,
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
                    if asynchronous {
                        inner.index.abort_batch();
                    }
                    return Err(match result {
                        Err(error) => error,
                        Ok(batch) => batch.stop.unwrap_or(RecordError::RecoveryRequired),
                    });
                }
                let batch = match result {
                    Ok(batch) => batch,
                    Err(error) => BatchResult {
                        results: Vec::new(),
                        stop: Some(error),
                    },
                };
                if !asynchronous {
                    partition.indexes.immediate(inner.state.seq);
                    return Ok(batch);
                }
                // Both index consumers proceed independently after canonical
                // Record materialization releases its guard.
                inner
                    .output
                    .write()
                    .map_err(|_| RecordError::RecoveryRequired)?
                    .fenced = true;
                let sequence = inner.state.seq;
                if let Err(error) = inner
                    .index
                    .finish_batch(sequence, partition.indexes.clone())
                    .await
                {
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
                Ok(batch)
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

    async fn prepare_batch(
        &mut self,
        requests: Vec<RecordWrite>,
    ) -> Result<PreparedBatch, RecordError> {
        let base_state = self.state.try_clone()?;
        let mut shadow = self.state.try_clone()?;
        let mut snapshot = self.snapshot_bytes;
        let mut plans = Vec::new();
        let mut result = BatchResult {
            results: Vec::new(),
            stop: None,
        };
        let mut growth_total = 0u64;
        let mut updates_total = 0usize;
        let mut journal_bytes = 0usize;
        let mut container_bound = self.state.container_bytes(snapshot)?;
        for request in requests {
            let plan = async {
                if !shadow.operation_lookup.contains_key(&request.operation_id) {
                    if self.maintenance_requested.is_some() {
                        return Err(RecordError::Busy);
                    }
                    if maintenance::has_expired_operations(&shadow, current_epoch())? {
                        return Err(self.request_checkpoint("expired_operation_receipts"));
                    }
                }
                let plan = mutation::prepare_at(
                    &shadow,
                    &self.config,
                    snapshot,
                    request,
                    current_epoch(),
                    self.evaluation_time_ms,
                )?;
                let mutation::PreparedWrite::Append {
                    applied,
                    bytes,
                    snapshot_bytes,
                    reserve_bytes,
                    candidate,
                    ..
                } = &plan
                else {
                    return Ok(plan);
                };
                let (growth, updates) =
                    build_budget::write_estimate(&shadow, &self.config, applied)?;
                if growth > 0 {
                    let fresh = (self.config.schema.len() as u64)
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
                if !self.index.can_admit(total, native).await {
                    if plans.is_empty() {
                        self.maintain_index_tail().await?;
                    }
                    if !self.index.can_admit(total, native).await {
                        return Err(RecordError::Busy);
                    }
                }
                // One physical frame per decision; account for every 24-byte header.
                let physical = journal_bytes
                    .checked_add(bytes.len())
                    .and_then(|n| n.checked_add(24))
                    .ok_or_else(|| storage("batch journal bound overflow"))?;
                if let Err(error) = self
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
                #[cfg(feature = "s3-object-store")]
                if let Some(remote) = self.remote.as_ref() {
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
                        Ok(true) => return Err(self.request_checkpoint("remote_tail_room")),
                        Err(error) => {
                            return Err(if plans.is_empty() || !definite(&error) {
                                error
                            } else {
                                RecordError::Busy
                            });
                        }
                    }
                }
                if let Some(candidate) = candidate {
                    shadow = candidate.try_clone()?;
                } else {
                    let prepared = state_memory::Prepared::new(&shadow, applied)?;
                    prepared.admit_containers(&mut shadow, *snapshot_bytes)?;
                    apply_prepared(&mut shadow, applied, Some(prepared))?;
                }
                snapshot = *snapshot_bytes;
                container_bound = container_bound.max(shadow.container_bytes(snapshot)?);
                growth_total = total;
                updates_total = native;
                journal_bytes = physical;
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
        if plans.is_empty() {
            return Ok(PreparedBatch {
                plans,
                publication: Vec::new(),
                result,
                base_seq: self.state.seq,
                base_state,
                expires_at: None,
            });
        }
        // Admit live container growth before persistence. Values are separately owned.
        self.state.admit_containers(container_bound)?;
        // Retain all ordinary publication values before the first durable byte.
        let mut publication = Vec::new();
        let mut prior = self.state.try_clone()?;
        for plan in &mut plans {
            if let mutation::PreparedWrite::Append {
                applied,
                snapshot_bytes,
                candidate,
                ..
            } = plan
            {
                let prepared = if let Some(candidate) = candidate {
                    candidate.admit_containers(container_bound)?;
                    prior = candidate.try_clone()?;
                    None
                } else {
                    let prepared = state_memory::Prepared::new(&prior, applied)?;
                    prepared.admit_containers(&mut prior, *snapshot_bytes)?;
                    apply_prepared(
                        &mut prior,
                        applied,
                        Some(state_memory::Prepared {
                            record: prepared.record.clone(),
                            operation: prepared.operation.clone(),
                        }),
                    )?;
                    Some(prepared)
                };
                publication.push(prepared);
            }
        }
        drop(prior);
        let expires_at =
            shadow
                .records
                .values()
                .filter(|record| record.document.is_some())
                .filter_map(|record| record.temporal.expire_at_ms)
                .chain(shadow.generations.values().filter_map(|generation| {
                    generation.staged.as_ref().map(|stage| stage.deadline_ms)
                }))
                .min();
        drop(shadow);
        Ok(PreparedBatch {
            plans,
            publication,
            result,
            base_seq: self.state.seq,
            base_state,
            expires_at,
        })
    }

    async fn commit_batch(
        &mut self,
        prepared: PreparedBatch,
        scope: &mut AttemptScope,
        #[cfg(feature = "s3-object-store")] uploaded: Option<UploadedBatch>,
    ) -> Result<BatchResult, RecordError> {
        if !self.healthy || self.state.seq != prepared.base_seq {
            return Err(RecordError::RecoveryRequired);
        }
        // Wall time can move while uploading even though this task owns the writer.
        // Apply due transitions before selecting the candidate, then discard any
        // candidate whose business basis or own TTL/lease has become stale.
        self.advance_time().await?;
        if state_proof::verify(&self.config, &prepared.base_state, &self.state).is_err()
            || prepared
                .expires_at
                .is_some_and(|deadline| deadline <= self.evaluation_time_ms)
        {
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
        if injected.as_deref() == Some("busy_before_append") {
            return Err(RecordError::Busy);
        }
        #[cfg(feature = "fault-injection")]
        if injected.as_deref() == Some("before_append") {
            return Err(RecordError::Rejected("injected before append".into()));
        }
        self.healthy = false;
        {
            let mut output = self
                .output
                .write()
                .map_err(|_| RecordError::RecoveryRequired)?;
            output.fenced = true;
            // A conservative barrier covers source, evidence and generation changes.
            output.revision = output
                .revision
                .checked_add(1)
                .ok_or_else(|| storage("data barrier revision exhausted"))?;
        }
        scope.persistence_attempted = result
            .results
            .iter()
            .enumerate()
            .filter_map(|(i, result)| result.as_ref().ok().filter(|r| r.seq > base_seq).map(|_| i))
            .collect();
        #[cfg(feature = "s3-object-store")]
        if let Some(uploaded) = uploaded {
            let remote = self.remote.as_mut().ok_or(RecordError::RecoveryRequired)?;
            uploaded.publish(remote).await?;
        }
        self.append_operations(&payloads, sequence).await?;
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
        if self.fail_after_durable {
            return Err(RecordError::DurablePending(sequence));
        }
        for (plan, prepared) in plans.into_iter().zip(publication) {
            if let mutation::PreparedWrite::Append {
                applied,
                snapshot_bytes,
                candidate,
                ..
            } = plan
            {
                self.publish_prepared(&applied, snapshot_bytes, prepared, candidate)
                    .await?;
            }
        }
        self.healthy = true;
        self.output
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
mod tests {
    use super::*;
    use crate::record::tests::{config, write};
    use std::sync::atomic::Ordering;

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
        drop(p);
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
        drop(p);
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
