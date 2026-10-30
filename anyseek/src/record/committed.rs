//! Non-atomic committed ingestion; derived indexes remain asynchronous.
use super::write::IndexDelivery;
use super::*;

/// Result of one input position, in request order.
#[derive(Debug)]
pub enum CommittedItem {
    /// A durable canonical receipt, including an exact replay.
    Committed(WriteReceipt),
    /// A definite business rejection; later inputs may still execute.
    Rejected(RecordError),
    /// Publication was attempted but its result is not confirmed.
    OutcomeUnknown,
    /// This invocation did not attempt canonical publication for this input.
    NotAttempted,
}
/// Non-atomic results and an optional interruption requiring retry/recovery.
#[derive(Debug)]
pub struct CommittedBatch {
    /// Exactly one result per input position.
    pub results: Vec<CommittedItem>,
    /// None means every input received a definite result, not that all succeeded.
    pub stop: Option<RecordError>,
}
/// Record and derived-index watermarks share the Record sequence space.
#[derive(Serialize)]
pub struct CommittedProgress {
    /// Last materialized durable Record sequence; null while recovery is required.
    pub committed_seq: Option<u64>,
    /// Lexical consumer's published sequence.
    pub lexical_seq: u64,
    /// Vector/metadata consumer's published sequence.
    pub vector_seq: u64,
    /// Derived-index failure, if any.
    pub index_error: Option<String>,
    /// Canonical state requires recovery before further writes.
    pub recovery_required: bool,
}
impl RecordPartition {
    /// Commit independent commands in order, continuing after business rejection.
    /// Cancellation does not cancel the owned execution. Indexing is asynchronous.
    pub async fn commit_writes(
        &self,
        requests: Vec<RecordWrite>,
    ) -> Result<CommittedBatch, RecordError> {
        execute(self.clone(), requests, None).await
    }
    /// Inspect progress without advancing time or waiting for indexes.
    pub async fn committed_progress(&self) -> CommittedProgress {
        let inner = self.inner.lock().await;
        CommittedProgress {
            committed_seq: inner.healthy.then_some(inner.state.seq),
            lexical_seq: self
                .indexes
                .lexical
                .load(std::sync::atomic::Ordering::Acquire),
            vector_seq: self
                .indexes
                .vector
                .load(std::sync::atomic::Ordering::Acquire),
            index_error: self.indexes.error(),
            recovery_required: !inner.healthy,
        }
    }
}
pub(super) async fn execute(
    partition: RecordPartition,
    requests: Vec<RecordWrite>,
    legacy: Option<IngestPipeline>,
) -> Result<CommittedBatch, RecordError> {
    if requests.is_empty() || requests.len() > 64 {
        return Err(RecordError::Rejected(
            "ingest requires 1..64 commands".into(),
        ));
    }
    let bytes = requests.iter().try_fold(0usize, |total, request| {
        total
            .checked_add(checkpoint_codec::json_length(request)?)
            .filter(|n| *n <= 4 * 1024 * 1024)
            .ok_or_else(|| RecordError::Rejected("ingest exceeds 4 MiB".into()))
    })?;
    let memory = crate::storage::build_budget::reserve_memory(bytes as u64)
        .map_err(|_| RecordError::Busy)?;
    crate::resources::spawn(async move {
        let _memory = memory;
        let mut batch = CommittedBatch {
            results: requests
                .iter()
                .map(|_| CommittedItem::NotAttempted)
                .collect(),
            stop: None,
        };
        // Old durable Accepts must reach a terminal outcome before the new path.
        let recovered = match &legacy {
            Some(legacy) => legacy.require_completed().await,
            None => Ok(()),
        };
        if let Err(error) = recovered {
            batch.stop = Some(error);
            return batch;
        }
        let mut offset = 0;
        'inputs: while offset < requests.len() {
            // Retained legacy identities keep their digest and terminal outcome.
            // A legacy entry splits the batch; fresh runs still share one commit.
            let mut end = offset;
            while end < requests.len() {
                let retained = match &legacy {
                    Some(legacy) => legacy.retained_result(&requests[end]).await,
                    None => None,
                };
                if let Some(result) = retained {
                    if end > offset {
                        break;
                    }
                    match result {
                        Ok(receipt) => batch.results[end] = CommittedItem::Committed(receipt),
                        Err(error) if write::definite(&error) => {
                            batch.results[end] = CommittedItem::Rejected(error)
                        }
                        Err(error) => {
                            batch.stop = Some(error);
                            return batch;
                        }
                    }
                    offset += 1;
                    continue 'inputs;
                }
                end += 1;
            }
            match partition
                .execute_writes_detailed(
                    requests[offset..end].to_vec(),
                    IndexDelivery::Asynchronous,
                )
                .await
            {
                Ok(result) => {
                    let count = result.results.len();
                    for (position, result) in result.results.into_iter().enumerate() {
                        batch.results[offset + position] = match result {
                            Ok(receipt) => CommittedItem::Committed(receipt),
                            Err(error) => CommittedItem::Rejected(error),
                        };
                    }
                    offset += count;
                    if let Some(error) = result.stop {
                        batch.stop = Some(error);
                        break;
                    }
                    if count == 0 {
                        batch.stop = Some(RecordError::RecoveryRequired);
                        break;
                    }
                }
                Err(failure) => {
                    if let Some(scope) = failure.scope {
                        for position in scope.persistence_attempted {
                            batch.results[offset + position] = CommittedItem::OutcomeUnknown;
                        }
                        for (position, receipt) in scope.replayed {
                            batch.results[offset + position] = CommittedItem::Committed(receipt);
                        }
                    } else {
                        for result in &mut batch.results[offset..end] {
                            *result = CommittedItem::OutcomeUnknown;
                        }
                    }
                    batch.stop = Some(failure.error);
                    break;
                }
            }
        }
        batch
    })
    .await
    .map_err(|error| RecordError::OutcomeUnknown(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, write};
    #[tokio::test]
    async fn committed_ingest_continues_after_conflict_without_waiting_for_indexes() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        for lane in ["lexical", "vector"] {
            std::fs::write(root.join(format!(".test-{lane}-consumer-pause")), b"pause").unwrap();
        }
        let requests = vec![
            write("first", "r", "alpha", ExpectedVersion::Absent),
            write("conflict", "r", "bad", ExpectedVersion::Absent),
            write("later", "s", "beta", ExpectedVersion::Absent),
        ];
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), p.commit_writes(requests))
                .await
                .unwrap()
                .unwrap();
        assert!(result.stop.is_none());
        assert!(matches!(
            &result.results[..],
            [
                CommittedItem::Committed(_),
                CommittedItem::Rejected(RecordError::Conflict(_)),
                CommittedItem::Committed(_)
            ]
        ));
        let progress = p.committed_progress().await;
        assert_eq!(progress.committed_seq, Some(2));
        assert_eq!(progress.lexical_seq, 0);
        assert_eq!(progress.vector_seq, 0);
        assert!(!root.join("intake").exists());
        for lane in ["lexical", "vector"] {
            std::fs::remove_file(root.join(format!(".test-{lane}-consumer-pause"))).unwrap();
        }
        p.inner.lock().await.index.drain_indexes().await.unwrap();
    }
    #[tokio::test]
    async fn committed_ingest_unknown_preserves_scope_and_recovers_original_ids() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let settings = config();
        let p = RecordPartition::create(&root, settings.clone())
            .await
            .unwrap();
        let requests = vec![
            write("first", "r", "alpha", ExpectedVersion::Absent),
            write("conflict", "r", "bad", ExpectedVersion::Absent),
            write("later", "s", "beta", ExpectedVersion::Absent),
        ];
        p.inner.lock().await.fail_after_durable = true;
        let result = p.commit_writes(requests.clone()).await.unwrap();
        assert!(matches!(result.stop, Some(RecordError::DurablePending(1))));
        assert!(matches!(
            &result.results[..],
            [
                CommittedItem::OutcomeUnknown,
                CommittedItem::NotAttempted,
                CommittedItem::NotAttempted
            ]
        ));
        assert!(p.committed_progress().await.recovery_required);
        drop(p);
        let p = RecordPartition::open(&root, settings.max_journal_bytes)
            .await
            .unwrap();
        let result = p.commit_writes(requests).await.unwrap();
        assert!(result.stop.is_none());
        assert!(
            matches!(&result.results[..],[CommittedItem::Committed(r),CommittedItem::Rejected(_),CommittedItem::Committed(s)] if r.seq==1 && s.seq==2)
        );
        p.inner.lock().await.index.drain_indexes().await.unwrap();
    }
}
