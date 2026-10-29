//! Shared durable Record decision path and bounded intake materialization.
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

impl RecordPartition {
    pub(super) async fn write_ingest_batch(
        &self,
        requests: Vec<RecordWrite>,
    ) -> Result<BatchResult, RecordError> {
        if requests.is_empty() || requests.len() > 64 {
            return Err(RecordError::Rejected(
                "invalid materialization batch size".into(),
            ));
        }
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let partition = self.clone();
        crate::resources::spawn(async move {
            let result = async {
                let _permit = permit;
                let mut inner = partition.inner.lock().await;
                if !inner.healthy {
                    return Err(RecordError::RecoveryRequired);
                }
                inner.index.begin_batch(partition.indexes.clone(),Arc::downgrade(&partition.inner)).await?;
                let mut batch = BatchResult {
                    results: Vec::new(),
                    stop: None,
                };
                for request in requests {
                    let result = inner.write_one(request).await;
                    if !inner.healthy {
                        // Incomplete publication cannot certify an index view.
                        // Recovery resolves all original operation identities.
                        inner.index.abort_batch();
                        return Err(result.err().unwrap_or(RecordError::RecoveryRequired));
                    }
                    match result {
                        Ok(receipt) => batch.results.push(Ok(receipt)),
                        Err(error) if definite(&error) => {
                            // Persist a rejection before executing later intake
                            // commands; a batch is never a conditional transaction.
                            batch.results.push(Err(error));
                            break;
                        }
                        Err(error) => {
                            batch.stop = Some(error);
                            break;
                        }
                    }
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
        if !self.state.operation_lookup.contains_key(&request.operation_id)
            && maintenance::has_expired_operations(&self.state,current_epoch())? {
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
