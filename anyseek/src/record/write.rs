//! Shared bounded Record execution. Successful results are durable business decisions.
//! Index publication can be synchronous or asynchronous; intake is only a caller.
use super::*;

/// Only the completed prefix may be recorded in intake. Busy and uncertain
/// outcomes keep their original identity and leave the suffix pending.
pub(super) struct BatchResult {
    pub results: Vec<Result<WriteReceipt, RecordError>>,
    pub stop: Option<RecordError>,
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
        if requests.is_empty() || requests.len() > MAX_BATCH_RECORDS {
            return Err(RecordError::Rejected("invalid Record batch size".into()));
        }
        let bytes = requests.iter().try_fold(0usize, |total, request| {
            total
                .checked_add(checkpoint_codec::json_length(request)?)
                .filter(|total| *total <= MAX_BATCH_BYTES)
                .ok_or_else(|| RecordError::Rejected("Record batch byte budget exceeded".into()))
        })?;
        // Retain queue/planner ownership until the owned task finishes, even if
        // the caller cancels. The existing admission semaphore bounds waiters.
        let memory = crate::storage::build_budget::reserve_memory(bytes as u64).map_err(storage)?;
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let partition = self.clone();
        crate::resources::spawn(async move {
            let result = async {
                let _permit = permit;
                let _memory = memory;
                let mut inner = partition.inner.lock().await;
                if !inner.healthy {
                    return Err(RecordError::RecoveryRequired);
                }
                let asynchronous = matches!(delivery, IndexDelivery::Asynchronous);
                if asynchronous {
                    inner
                        .index
                        .begin_batch(partition.indexes.clone(), Arc::downgrade(&partition.inner))
                        .await?;
                } else {
                    inner.index.drain_indexes().await?;
                }
                let result = inner.write_batch(requests).await;
                if !inner.healthy {
                    if asynchronous {
                        inner.index.abort_batch();
                    }
                    return Err(result.err().unwrap_or(RecordError::RecoveryRequired));
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
            }
            .await;
            partition.kick_maintenance().await;
            result
        })
        .await
        .map_err(|error| RecordError::OutcomeUnknown(error.to_string()))?
    }
}

impl Inner {
    async fn write_batch(
        &mut self,
        requests: Vec<RecordWrite>,
    ) -> Result<BatchResult, RecordError> {
        if requests.len() == 1 {
            let result = self.write_one(requests.into_iter().next().unwrap()).await;
            return match result {
                Ok(receipt) => Ok(BatchResult {
                    results: vec![Ok(receipt)],
                    stop: None,
                }),
                Err(error) if self.healthy && definite(&error) => Ok(BatchResult {
                    results: vec![Err(error)],
                    stop: None,
                }),
                Err(error) if self.healthy => Ok(BatchResult {
                    results: vec![],
                    stop: Some(error),
                }),
                Err(error) => Err(error),
            };
        }
        self.advance_time().await?;
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
                    return Err(if plans.is_empty() {
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
                            return Err(if plans.is_empty() {
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
            return Ok(result);
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
        drop(shadow);
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
        #[cfg(feature = "s3-object-store")]
        if let Some(remote) = self.remote.as_mut() {
            let entries: Vec<_> = plans
                .iter()
                .map(|plan| match plan {
                    mutation::PreparedWrite::Append { applied, bytes, .. } => (
                        applied.receipt.seq,
                        format!(
                            "{}:{}",
                            applied.request.operation_id.epoch, applied.request.operation_id.nonce
                        ),
                        bytes.as_slice(),
                    ),
                    _ => unreachable!(),
                })
                .collect();
            match remote.commit_entries(&entries).await {
                Ok(()) => {}
                Err(RemoteCommitError::Busy) => {
                    self.healthy = true;
                    self.output
                        .write()
                        .map_err(|_| RecordError::RecoveryRequired)?
                        .fenced = false;
                    return Err(RecordError::Busy);
                }
                Err(RemoteCommitError::Unknown(error)) => {
                    return Err(RecordError::OutcomeUnknown(error));
                }
            }
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
        Ok(result)
    }

    pub(super) async fn write_one(
        &mut self,
        request: RecordWrite,
    ) -> Result<WriteReceipt, RecordError> {
        if !self.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        // Known hard pressure must not repeatedly plan/copy a new
        // request's State while the owned rebuild is already pending.
        // Known identities still use the original epoch/fingerprint
        // replay checks below (and required time maintenance).
        if self.maintenance_requested.is_some()
            && !self
                .state
                .operation_lookup
                .contains_key(&request.operation_id)
        {
            return Err(RecordError::Busy);
        }
        self.advance_time().await?;
        if !self
            .state
            .operation_lookup
            .contains_key(&request.operation_id)
            && maintenance::has_expired_operations(&self.state, current_epoch())?
        {
            return Err(self.request_checkpoint("expired_operation_receipts"));
        }
        let (applied, bytes, snapshot_bytes, reserve_bytes, mut candidate, _planning) =
            match mutation::prepare_at(
                &self.state,
                &self.config,
                self.snapshot_bytes,
                request,
                current_epoch(),
                self.evaluation_time_ms,
            )? {
                mutation::PreparedWrite::Replay(receipt) => return Ok(receipt),
                mutation::PreparedWrite::Append {
                    applied,
                    bytes,
                    snapshot_bytes,
                    reserve_bytes,
                    candidate,
                    planning,
                } => (
                    applied,
                    bytes,
                    snapshot_bytes,
                    reserve_bytes,
                    candidate,
                    planning,
                ),
            };
        if self.maintenance_requested.is_some() {
            return Err(RecordError::Busy);
        }
        let (growth, native_updates) =
            build_budget::write_estimate(&self.state, &self.config, &applied)?;
        if growth > 0 {
            let fresh_component = (self.config.schema.len() as u64)
                .checked_mul(65536 * 32)
                .and_then(|n| n.checked_add(growth))
                .ok_or_else(|| storage("fresh component admission overflow"))?;
            crate::storage::build_budget::validate_single(fresh_component)
                .map_err(|e| RecordError::Rejected(e.to_string()))?;
            if native_updates > 128 {
                return Err(RecordError::Rejected(
                    "operation exceeds mutable native update bound".into(),
                ));
            }
        }
        if growth > 0 && !self.index.can_admit(growth, native_updates).await {
            // One bounded flush attempt before rejecting, never discard
            // selected history to manufacture admission space.
            self.maintain_index_tail().await?;
            if !self.index.can_admit(growth, native_updates).await {
                return Err(RecordError::Rejected(
                    "record build admission budget exceeded".into(),
                ));
            }
        }
        let prepared = if applied.request.generation.is_none() {
            Some(state_memory::Prepared::new(&self.state, &applied)?)
        } else {
            None
        };
        if let Some(prepared) = &prepared {
            prepared.admit_containers(&mut self.state, snapshot_bytes)?;
        } else if let Some(candidate) = candidate.as_mut() {
            candidate.admit_bytes(snapshot_bytes)?;
        }
        self.ensure_journal_room(bytes.len(), reserve_bytes).await?;
        #[cfg(feature = "s3-object-store")]
        self.ensure_remote_tail_room(
            applied.receipt.seq,
            &format!(
                "{}:{}",
                applied.request.operation_id.epoch, applied.request.operation_id.nonce
            ),
            &bytes,
        )
        .await?;
        self.validate_journal_room(bytes.len(), reserve_bytes)?;
        let receipt = applied.receipt.clone();
        #[cfg(feature = "fault-injection")]
        let injected = std::env::var("ANYSEEK_RECORD_FAULT_NONCE")
            .ok()
            .filter(|nonce| nonce == &applied.request.operation_id.nonce)
            .and_then(|_| std::env::var("ANYSEEK_RECORD_FAULT").ok());
        #[cfg(feature = "fault-injection")]
        if injected.as_deref() == Some("before_append") {
            return Err(RecordError::Rejected("injected before append".into()));
        }
        let hides_evidence = matches!(
            applied.request.generation,
            Some(GenerationCommand::Publish { .. })
        ) || (self
            .state
            .records
            .get(&applied.request.record_id)
            .is_some_and(|r| r.system.is_some())
            || (applied.request.vector_patch.is_none()
                && self.graph_cache.references(&applied.request.record_id)));
        self.healthy = false;
        {
            let mut output = self
                .output
                .write()
                .map_err(|_| RecordError::RecoveryRequired)?;
            output.fenced = true;
            if hides_evidence {
                output.revision = output
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| storage("data barrier revision exhausted"))?;
            }
        }
        #[cfg(feature = "s3-object-store")]
        if let Some(remote) = self.remote.as_mut() {
            match remote.commit(&applied, &bytes).await {
                Ok(()) => {}
                Err(RemoteCommitError::Busy) => {
                    // Only the upload stage can produce this typed refusal;
                    // SQLite publication has not been attempted. Orphan
                    // objects remain registered for ordinary dependency GC.
                    self.healthy = true;
                    self.output
                        .write()
                        .map_err(|_| RecordError::RecoveryRequired)?
                        .fenced = false;
                    return Err(RecordError::Busy);
                }
                Err(RemoteCommitError::Unknown(error)) => {
                    return Err(RecordError::OutcomeUnknown(error));
                }
            }
        }
        self.append_operation(&bytes, receipt.seq).await?;
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
        if self.fail_after_durable {
            return Err(RecordError::DurablePending(receipt.seq));
        }
        self.publish_prepared(&applied, snapshot_bytes, prepared, candidate)
            .await?;
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
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, write};
    use std::sync::atomic::Ordering;

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
