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
        enqueue(self.clone(), requests).await
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
// Counts include in-flight inputs until their request result has been delivered.
#[derive(Default)]
pub(super) struct Queue {
    pending: std::collections::VecDeque<Queued>,
    records: usize,
    bytes: usize,
    running: bool,
}
struct Queued {
    requests: Vec<RecordWrite>,
    count: usize,
    bytes: usize,
    reply: tokio::sync::oneshot::Sender<Result<CommittedBatch, RecordError>>,
    _memory: crate::storage::build_budget::MemoryLease,
}
pub(super) async fn enqueue(
    partition: RecordPartition,
    requests: Vec<RecordWrite>,
) -> Result<CommittedBatch, RecordError> {
    let bytes = request_bytes(&requests)?;
    let memory = crate::storage::build_budget::reserve_memory(bytes as u64)
        .map_err(|_| RecordError::Busy)?;
    let (reply, receive) = tokio::sync::oneshot::channel();
    {
        let mut queue = partition
            .committed_queue
            .lock()
            .map_err(|_| RecordError::RecoveryRequired)?;
        if queue.records + requests.len() > 64 || queue.bytes + bytes > 4 * 1024 * 1024 {
            return Err(RecordError::Busy);
        }
        queue.records += requests.len();
        queue.bytes += bytes;
        let count = requests.len();
        queue.pending.push_back(Queued {
            requests,
            count,
            bytes,
            reply,
            _memory: memory,
        });
        #[cfg(feature = "fault-injection")]
        if let Some(path) = std::env::var_os("ANYSEEK_COMMITTED_PAUSE_FILE") {
            // Explicit fault-binary observation, never part of the HTTP API.
            let _ = std::fs::write(
                std::path::Path::new(&path).with_extension("queued"),
                queue.records.to_string(),
            );
        }
        if !queue.running {
            queue.running = true;
            let worker = partition.clone();
            crate::resources::spawn(async move {
                use futures::FutureExt;
                let outcome = std::panic::AssertUnwindSafe(run_queue(worker.clone()))
                    .catch_unwind()
                    .await;
                if let Ok(replies) = outcome {
                    drop(worker);
                    for (reply, result) in replies {
                        let _ = reply.send(result);
                    }
                } else {
                    // Active senders were dropped during unwind (unknown); queued work never ran.
                    let pending = {
                        let mut queue = worker
                            .committed_queue
                            .lock()
                            .unwrap_or_else(|e| e.into_inner());
                        queue.running = false;
                        queue.records = 0;
                        queue.bytes = 0;
                        std::mem::take(&mut queue.pending)
                    };
                    for request in pending {
                        let _ = request.reply.send(Ok(CommittedBatch {
                            results: (0..request.count)
                                .map(|_| CommittedItem::NotAttempted)
                                .collect(),
                            stop: Some(RecordError::RecoveryRequired),
                        }));
                    }
                }
            });
        }
    }
    receive
        .await
        .map_err(|_| RecordError::OutcomeUnknown("committed queue worker exited".into()))?
}
type Delivery = (
    tokio::sync::oneshot::Sender<Result<CommittedBatch, RecordError>>,
    Result<CommittedBatch, RecordError>,
);
async fn run_queue(partition: RecordPartition) -> Vec<Delivery> {
    loop {
        #[cfg(feature = "fault-injection")]
        if let Some(path) = std::env::var_os("ANYSEEK_COMMITTED_PAUSE_FILE") {
            let path = std::path::Path::new(&path);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
            while path.exists() && std::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        let mut group = {
            let mut queue = partition.committed_queue.lock().unwrap();
            let Some(first) = queue.pending.pop_front() else {
                queue.running = false;
                return Vec::new();
            };
            let mut group = vec![first];
            while !queue.pending.is_empty() {
                group.push(queue.pending.pop_front().unwrap());
            }
            group
        };
        let mut requests = Vec::new();
        for request in &mut group {
            requests.append(&mut request.requests);
        }
        let result = execute(partition.clone(), requests).await;
        let count = group.iter().map(|r| r.count).sum::<usize>();
        let bytes = group.iter().map(|r| r.bytes).sum::<usize>();
        let mut replies = Vec::new();
        match result {
            Ok(batch) => {
                let mut results = batch.results.into_iter();
                for request in group {
                    let rows: Vec<_> = results.by_ref().take(request.count).collect();
                    let interrupted = rows.iter().any(|r| {
                        matches!(
                            r,
                            CommittedItem::OutcomeUnknown | CommittedItem::NotAttempted
                        )
                    });
                    replies.push((
                        request.reply,
                        Ok(CommittedBatch {
                            results: rows,
                            stop: if interrupted {
                                batch.stop.clone()
                            } else {
                                None
                            },
                        }),
                    ));
                }
            }
            Err(error) => {
                for request in group {
                    replies.push((request.reply, Err(error.clone())));
                }
            }
        }
        // End the owner before delivering the last batch, so reopen cannot race its drop.
        let last = {
            let mut queue = partition.committed_queue.lock().unwrap();
            queue.records -= count;
            queue.bytes -= bytes;
            let last = queue.pending.is_empty();
            if last {
                queue.running = false;
            }
            last
        };
        if last {
            return replies;
        }
        for (reply, result) in replies {
            let _ = reply.send(result);
        }
    }
}
fn request_bytes(requests: &[RecordWrite]) -> Result<usize, RecordError> {
    if requests.is_empty() || requests.len() > 64 {
        return Err(RecordError::Rejected(
            "ingest requires 1..64 commands".into(),
        ));
    }
    requests.iter().try_fold(0usize, |total, request| {
        total
            .checked_add(checkpoint_codec::json_length(request)?)
            .filter(|n| *n <= 4 * 1024 * 1024)
            .ok_or_else(|| RecordError::Rejected("ingest exceeds 4 MiB".into()))
    })
}
#[cfg(test)]
impl RecordPartition {
    pub(super) fn committed_admitted(&self) -> usize {
        self.committed_queue.lock().unwrap().records
    }
}
pub(super) async fn execute(
    partition: RecordPartition,
    requests: Vec<RecordWrite>,
) -> Result<CommittedBatch, RecordError> {
    let bytes = request_bytes(&requests)?;
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
        let mut offset = 0;
        while offset < requests.len() {
            let end = requests.len();
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
    async fn committed_queue_bounds_include_inflight_and_cancelled_requests() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let lock = p.writer.lock().await;
        let owner = p.clone();
        let first = tokio::spawn(async move {
            owner
                .commit_writes(vec![write(
                    "seed",
                    "seed",
                    "alpha",
                    ExpectedVersion::Absent,
                )])
                .await
        });
        while p.committed_admitted() != 1 {
            tokio::task::yield_now().await;
        }
        let request = write("cancelled", "r", "beta", ExpectedVersion::Absent);
        let owner = p.clone();
        let commands = vec![request.clone(); 63];
        let waiting = tokio::spawn(async move { owner.commit_writes(commands).await });
        while p.committed_admitted() != 64 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            p.commit_writes(vec![write(
                "overflow",
                "other",
                "gamma",
                ExpectedVersion::Absent
            )])
            .await,
            Err(RecordError::Busy)
        ));
        waiting.abort();
        let _ = waiting.await;
        drop(lock);
        first.await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while p.committed_admitted() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let receipt = p.write(request).await.unwrap();
        assert_eq!(receipt.seq, 2);
        assert!(p.get("other").await.unwrap().is_none());
        let lock = p.writer.lock().await;
        let large = write(
            "large",
            "large",
            &"x".repeat(3 * 1024 * 1024),
            ExpectedVersion::Absent,
        );
        let owner = p.clone();
        let blocked = tokio::spawn(async move { owner.commit_writes(vec![large]).await });
        while p.committed_admitted() != 1 {
            tokio::task::yield_now().await;
        }
        assert!(matches!(
            p.commit_writes(vec![write(
                "bytes",
                "bytes",
                &"x".repeat(2 * 1024 * 1024),
                ExpectedVersion::Absent
            )])
            .await,
            Err(RecordError::Busy)
        ));
        drop(lock);
        assert!(matches!(
            &blocked.await.unwrap().unwrap().results[..],
            [CommittedItem::Rejected(_)]
        ));
    }
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
