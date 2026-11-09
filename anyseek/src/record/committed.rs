//! Ordered non-atomic commits with synchronous or asynchronous index publication.
use super::write::IndexDelivery;
use super::*;
mod independent;
pub use independent::IndependentItem;

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
    /// Commit up to 64 independent commands and publish both indexes before
    /// confirming new receipts. Definite rejections do not end the batch;
    /// interrupted inputs retain their unknown/not-attempted classification.
    /// Dropping the caller does not cancel the admitted owned execution.
    pub async fn write_batch(
        &self,
        requests: &[RecordWrite],
    ) -> Result<CommittedBatch, RecordError> {
        // The caller retains its requests for an unattempted-input fallback.
        // Admit this owned copy before cloning, using the document expansion
        // bound. Transfer the lease with the copy so caller cancellation cannot
        // release its charge while the owned executions are still queued.
        let bytes = request_bytes(requests)?;
        let copy_memory = crate::storage::build_budget::reserve_memory((bytes as u64) * 32)
            .map_err(|_| RecordError::Busy)?;
        execute_with_delivery(self.clone(), requests.to_vec(), IndexDelivery::Synchronous, Some(copy_memory)).await
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
        #[cfg(feature = "fault-injection")]
        if partition.inner.lock().await.journal.root().join(".test-committed-panic").exists() {
            panic!("injected committed worker panic before publication");
        }
        let result = execute(partition.clone(), requests).await;
        #[cfg(feature = "fault-injection")]
        if partition.inner.lock().await.journal.root().join(".test-committed-panic-after-publication").exists() {
            panic!("injected committed worker panic after publication");
        }
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
    // run_queue already owns this request, its resource context and activity.
    // Keep the former executor budget and panic boundary without another task.
    let bytes = request_bytes(&requests)?;
    let _memory = crate::storage::build_budget::reserve_memory(bytes as u64)
        .map_err(|_| RecordError::Busy)?;
    use futures::FutureExt;
    std::panic::AssertUnwindSafe(execute_owned(&partition, &requests, IndexDelivery::Asynchronous))
        .catch_unwind().await
        .map_err(|_| RecordError::OutcomeUnknown("Record batch owner unwound".into()))
}
pub(super) async fn execute_with_delivery(
    partition: RecordPartition,
    requests: Vec<RecordWrite>,
    delivery: IndexDelivery,
    copy_memory: Option<crate::storage::build_budget::MemoryLease>,
) -> Result<CommittedBatch, RecordError> {
    let bytes = request_bytes(&requests)?;
    let memory = crate::storage::build_budget::reserve_memory(bytes as u64)
        .map_err(|_| RecordError::Busy)?;
    crate::resources::spawn(async move {
        let _copy_memory = copy_memory;
        let _memory = memory;
        execute_owned(&partition, &requests, delivery).await
    })
    .await
    .map_err(|error| RecordError::OutcomeUnknown(error.to_string()))
}

// Called only by the detached synchronous owner or the owned queue worker.
// Requests, suffix classification and all per-prefix leases stay in that owner.
async fn execute_owned(
    partition: &RecordPartition,
    requests: &[RecordWrite],
    delivery: IndexDelivery,
) -> CommittedBatch {
    let mut batch = CommittedBatch {
        results: requests
            .iter()
            .map(|_| CommittedItem::NotAttempted)
            .collect(),
        stop: None,
    };
    let mut input = match write::BoundedInputs::new(requests) {
        Ok(input) => input,
        Err(error) => { batch.stop = Some(error); return batch; }
    };
    while input.offset() < requests.len() {
        let offset = input.offset();
        let end = requests.len();
        match partition
            .execute_input(&input, delivery)
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
                input.advance(count);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, write};
    #[tokio::test]
    async fn owned_entry_cancellation_keeps_context_leases_and_shutdown() {
        // Global build accounting must be isolated from unrelated tests.
        if std::env::var_os("ANYSEEK_WRITE_OWNER_CHILD").is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "record::committed::tests::owned_entry_cancellation_keeps_context_leases_and_shutdown", "--nocapture"])
                .env("ANYSEEK_WRITE_OWNER_CHILD", "1").output().unwrap();
            assert!(output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
                "{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
            return;
        }
        use crate::resources::{Governor, Lane, Lifecycle};
        use std::time::Duration;
        for entry in ["single", "batch", "ingest"] {
            for io_wait in [false, true] {
                let initial = crate::storage::build_budget::used_memory_for_test();
                let dir = tempfile::tempdir().unwrap();
                let root = dir.path().join("records");
                let p = RecordPartition::create(&root, config()).await.unwrap();
                let before = crate::storage::build_budget::used_memory_for_test();
                let permits = p.admission.available_permits();
                let scope = Arc::new(Governor::default()).admit("write-owner".into(), Lane::Foreground, 0).unwrap();
                let lifecycle = Arc::new(Lifecycle::default());
                let activity = lifecycle.enter().unwrap();
                let mut held_io = Vec::new();
                if io_wait {
                    while let Ok(lease) = scope.io() { held_io.push(lease); }
                    assert!(!held_io.is_empty());
                }
                let writer = if io_wait { None } else { Some(p.writer.lock().await) };
                let requests = if entry == "single" {
                    vec![write("a", "a", "alpha", ExpectedVersion::Absent)]
                } else {
                    vec![write("a", "a", "alpha", ExpectedVersion::Absent),
                         write("b", "b", "alpha", ExpectedVersion::Absent)]
                };
                let owner = p.clone();
                let commands = requests.clone();
                let request_scope = scope.clone();
                let mut caller = tokio::spawn(activity.run(request_scope.run(async move {
                    match entry {
                        "single" => { owner.write(commands.into_iter().next().unwrap()).await.unwrap(); }
                        "batch" => { owner.write_batch(&commands).await.unwrap(); }
                        _ => { owner.commit_writes(commands).await.unwrap(); }
                    }
                })));
                tokio::time::timeout(Duration::from_secs(5), async {
                    while p.admission.available_permits() == permits { tokio::task::yield_now().await; }
                }).await.unwrap();
                // With I/O exhausted, completion would expose lost Scope propagation.
                assert!(tokio::time::timeout(Duration::from_millis(30), &mut caller).await.is_err(), "{entry}, io={io_wait}");
                lifecycle.stop();
                caller.abort();
                assert!(caller.await.unwrap_err().is_cancelled());
                assert!(crate::storage::build_budget::used_memory_for_test() > before);
                assert!(tokio::time::timeout(Duration::from_millis(30), lifecycle.drained()).await.is_err(),
                    "cancelled caller released the accepted activity: {entry}, io={io_wait}");
                drop(writer);
                drop(held_io);
                tokio::time::timeout(Duration::from_secs(10), lifecycle.drained()).await.unwrap();
                assert_eq!(p.admission.available_permits(), permits);
                assert_eq!(p.committed_admitted(), 0);
                assert!(scope.io().is_ok());
                assert_eq!(Arc::strong_count(&p.inner), 1, "writer/worker owner retained after drain");
                let mut receipts = Vec::new();
                for request in &requests { receipts.push(p.operation(&request.operation_id).await.unwrap().unwrap()); }
                assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), requests.len());
                p.finish_service_shutdown().await.unwrap();
                drop(p);
                assert_eq!(crate::storage::build_budget::used_memory_for_test(), initial, "leaked owner budget: {entry}, io={io_wait}");
                let reopened = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
                for (request, receipt) in requests.into_iter().zip(receipts) { assert_eq!(reopened.write(request).await.unwrap(), receipt); }
                assert_eq!(reopened.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), if entry == "single" {1} else {2});
                reopened.finish_service_shutdown().await.unwrap();
                drop(reopened);
                assert_eq!(crate::storage::build_budget::used_memory_for_test(), initial);
            }
        }
    }

    #[tokio::test]
    async fn executor_unscoped_panic_keeps_unknown_classification_and_releases_owner() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let gate = root.join(".test-write-owner-panic");
        std::fs::write(&gate, b"panic").unwrap();
        let request = write("owner-panic", "r", "alpha", ExpectedVersion::Absent);
        let failure = p.execute_writes_detailed(std::slice::from_ref(&request), IndexDelivery::Synchronous).await.err().unwrap();
        assert!(matches!(failure.error, RecordError::OutcomeUnknown(_)));
        assert!(failure.scope.is_none());
        for asynchronous in [false, true] {
            let result = if asynchronous { p.commit_writes(vec![request.clone()]).await }
                else { p.write_batch(std::slice::from_ref(&request)).await };
            let result = result.unwrap();
            assert!(matches!(&result.results[..], [CommittedItem::OutcomeUnknown]));
            assert!(matches!(result.stop, Some(RecordError::OutcomeUnknown(_))));
        }
        assert!(p.writer.try_lock().is_ok());
        assert_eq!(p.committed_admitted(), 0);
        assert!(p.operation(&request.operation_id).await.unwrap().is_none());
        std::fs::remove_file(gate).unwrap();
        let receipt = p.write(request.clone()).await.unwrap();
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let reopened = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert_eq!(reopened.write(request).await.unwrap(), receipt);
        reopened.finish_service_shutdown().await.unwrap();
    }

    #[cfg(feature = "fault-injection")]
    #[tokio::test]
    async fn queue_panic_preserves_unattempted_pending_and_drains_activity() {
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let writer = p.writer.lock().await;
        let permits = p.admission.available_permits();
        let lifecycle = Arc::new(crate::resources::Lifecycle::default());
        let first = write("first", "a", "alpha", ExpectedVersion::Absent);
        let second = write("pending", "b", "beta", ExpectedVersion::Absent);
        let owner = p.clone(); let request = first.clone();
        let active = tokio::spawn(lifecycle.enter().unwrap().run(async move { owner.commit_writes(vec![request]).await }));
        tokio::time::timeout(Duration::from_secs(5), async {
            while p.admission.available_permits() == permits { tokio::task::yield_now().await; }
        }).await.unwrap();
        let owner = p.clone(); let request = second.clone();
        let pending = tokio::spawn(lifecycle.enter().unwrap().run(async move { owner.commit_writes(vec![request]).await }));
        tokio::time::timeout(Duration::from_secs(5), async {
            while p.committed_admitted() != 2 { tokio::task::yield_now().await; }
        }).await.unwrap();
        let gate = root.join(".test-committed-panic-after-publication");
        std::fs::write(&gate, b"panic").unwrap();
        lifecycle.stop();
        drop(writer);
        assert!(matches!(active.await.unwrap(), Err(RecordError::OutcomeUnknown(_))));
        let result = pending.await.unwrap().unwrap();
        assert!(matches!(&result.results[..], [CommittedItem::NotAttempted]));
        assert!(matches!(result.stop, Some(RecordError::RecoveryRequired)));
        tokio::time::timeout(Duration::from_secs(10), lifecycle.drained()).await.unwrap();
        assert_eq!(p.committed_admitted(), 0);
        assert_eq!(p.admission.available_permits(), permits);
        assert_eq!(Arc::strong_count(&p.inner), 1);
        let receipt = p.operation(&first.operation_id).await.unwrap().unwrap();
        assert!(p.operation(&second.operation_id).await.unwrap().is_none());
        std::fs::remove_file(gate).unwrap();
        p.finish_service_shutdown().await.unwrap(); drop(p);
        let reopened = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert_eq!(reopened.write(first).await.unwrap(), receipt);
        assert_eq!(reopened.write(second).await.unwrap().seq, receipt.seq + 1);
        reopened.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn single_and_one_item_batch_are_visible_durable_and_replayable() {
        let directory = tempfile::tempdir().unwrap();
        let request = write("same-operation", "record", "alpha", ExpectedVersion::Absent);
        let mut receipts = Vec::new();
        for batch in [false, true] {
            let root = directory.path().join(if batch { "batch" } else { "single" });
            let p = RecordPartition::create(&root, config()).await.unwrap();
            let receipt = if batch {
                let result = p.write_batch(std::slice::from_ref(&request)).await.unwrap();
                assert!(result.stop.is_none());
                match result.results.into_iter().next().unwrap() {
                    CommittedItem::Committed(receipt) => receipt,
                    item => panic!("unexpected batch result: {item:?}"),
                }
            } else { p.write(request.clone()).await.unwrap() };
            for mode in [SearchMode::Lexical, SearchMode::Vector {
                similarity_threshold: None, ann_mode: crate::vector_similarity::AnnMode::All,
            }] {
                let result = crate::record::tests::query(&p,mode).await;
                assert_eq!(result.hits.len(), 1);
            }
            let progress = p.committed_progress().await;
            assert_eq!((progress.committed_seq,progress.lexical_seq,progress.vector_seq),(Some(1),1,1));
            p.finish_service_shutdown().await.unwrap();
            drop(p);
            let p = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
            assert_eq!(p.operation(&request.operation_id).await.unwrap(), Some(receipt.clone()));
            assert_eq!(p.write(request.clone()).await.unwrap(), receipt);
            assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 1);
            receipts.push(receipt);
            p.finish_service_shutdown().await.unwrap();
        }
        assert_eq!(receipts[0], receipts[1]);
    }

    #[tokio::test]
    async fn synchronous_batch_keeps_same_id_conditions_vector_patch_and_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let mut stale = write("stale", "same", "unused", ExpectedVersion::Match(1));
        stale.document = None;
        stale.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector":[0.0,2.0]})).unwrap());
        let mut patch = stale.clone();
        patch.operation_id = OperationId::new("patch".into());
        patch.expected = ExpectedVersion::Match(2);
        let mut delete = write("delete", "same", "unused", ExpectedVersion::Match(2));
        delete.document = None;
        let mut second = write("second", "same", "beta", ExpectedVersion::Match(1));
        second.document.as_mut().unwrap().remove("vector");
        let requests = vec![
            write("first", "same", "alpha", ExpectedVersion::Absent),
            second,
            stale, patch,
            write("other", "other", "gamma", ExpectedVersion::Absent),
            delete,
        ];
        let batch = p.write_batch(&requests).await.unwrap();
        assert!(batch.stop.is_none());
        assert!(matches!(batch.results[2], CommittedItem::Rejected(RecordError::Conflict(_))));
        let receipts: Vec<_> = batch.results.iter().map(|item| match item {
            CommittedItem::Committed(receipt) => Some(receipt.clone()),
            CommittedItem::Rejected(_) => None,
            other => panic!("unexpected result: {other:?}"),
        }).collect();
        assert_eq!(receipts.iter().flatten().map(|r|r.seq).collect::<Vec<_>>(), vec![1,2,3,4,5]);
        assert_eq!(receipts[3].as_ref().unwrap().version, 2);
        assert!(p.get("same").await.unwrap().is_none());
        for word in ["alpha", "beta"] {
            assert!(p.search(word.into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.is_empty());
        }
        assert_eq!(p.search("gamma".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 1);
        let progress = p.committed_progress().await;
        assert_eq!((progress.committed_seq,progress.lexical_seq,progress.vector_seq),(Some(5),5,5));
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let p = RecordPartition::open(&root, 1024 * 1024).await.unwrap();
        // Receipt-only retries must not create new index work. Failure gates
        // remain armed so either accidental empty consumer task fails the test.
        let pauses=[root.join(".test-lexical-consumer-fail"),root.join(".test-vector-consumer-fail")];
        for path in &pauses {std::fs::write(path,b"fail").unwrap();}
        let replay = p.write_batch(&requests).await.unwrap();
        assert!(replay.stop.is_none());
        for (item, expected) in replay.results.iter().zip(receipts) {
            if let Some(receipt) = expected {
                assert!(matches!(item, CommittedItem::Committed(actual) if *actual == receipt));
            } else { assert!(matches!(item, CommittedItem::Rejected(RecordError::Conflict(_)))); }
        }
        assert_eq!(p.committed_progress().await.committed_seq, Some(5));
        assert!(!p.committed_progress().await.recovery_required);
        for path in pauses {std::fs::remove_file(path).unwrap();}
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn synchronous_batch_consumer_failure_keeps_unknown_scope_and_output_fence() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let old = write("old", "old", "alpha", ExpectedVersion::Absent);
        let receipt = p.write(old.clone()).await.unwrap();
        let output = p.output_permit().unwrap();
        std::fs::write(root.join(".test-lexical-consumer-fail"), b"fail").unwrap();
        let requests = vec![old, write("a", "a", "beta", ExpectedVersion::Absent),
            write("b", "b", "beta", ExpectedVersion::Absent)];
        let batch = p.write_batch(&requests).await.unwrap();
        assert!(matches!(batch.stop, Some(RecordError::DurablePending(3))));
        assert!(matches!(&batch.results[0], CommittedItem::Committed(actual) if *actual == receipt));
        assert!(batch.results[1..].iter().all(|r|matches!(r,CommittedItem::OutcomeUnknown)));
        assert!(matches!(output.deliver(|| ()), Err(RecordError::RecoveryRequired)));
        assert!(p.committed_progress().await.recovery_required);
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        std::fs::remove_file(root.join(".test-lexical-consumer-fail")).unwrap();
        let p = RecordPartition::open(&root, 1024 * 1024).await.unwrap();
        let replay = p.write_batch(&requests).await.unwrap();
        assert!(replay.stop.is_none());
        assert!(replay.results.iter().all(|r|matches!(r,CommittedItem::Committed(_))));
        assert_eq!(p.committed_progress().await.committed_seq, Some(3));
        assert_eq!(p.search("beta".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 2);
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn synchronous_batch_cancelled_waiter_keeps_publication_owned() {
        // Memory accounting is process-wide; keep this check isolated from
        // unrelated tests without adding another cancellation scenario.
        if std::env::var_os("ANYSEEK_SYNC_BATCH_CANCEL_CHILD").is_none() {
            let output=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","record::committed::tests::synchronous_batch_cancelled_waiter_keeps_publication_owned","--nocapture"])
                .env("ANYSEEK_SYNC_BATCH_CANCEL_CHILD","1").output().unwrap();
            assert!(output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),"{}\n{}",String::from_utf8_lossy(&output.stdout),String::from_utf8_lossy(&output.stderr));
            return;
        }
        let initial_used = crate::storage::build_budget::used_memory_for_test();
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let output = p.output_permit().unwrap();
        let pause = root.join(".test-lexical-consumer-pause");
        std::fs::write(&pause, b"pause").unwrap();
        let requests = vec![write("a", "a", "alpha", ExpectedVersion::Absent),
            write("b", "b", "alpha", ExpectedVersion::Absent)];
        let bytes = request_bytes(&requests).unwrap() as u64;
        let writer = p.writer.lock().await;
        let permits = p.admission.available_permits();
        let used = crate::storage::build_budget::used_memory_for_test();
        let owner = p.clone();
        let commands = requests.clone();
        let task = tokio::spawn(async move { owner.write_batch(&commands).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            // Admission follows the detached executor's planner reservation.
            while p.admission.available_permits() == permits { tokio::task::yield_now().await; }
        }).await.unwrap();
        // Partition, caller and the single detached execution owner.
        assert_eq!(Arc::strong_count(&p.inner), 3);
        let queued = crate::storage::build_budget::used_memory_for_test();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while Arc::strong_count(&p.inner) != 2 { tokio::task::yield_now().await; }
        }).await.unwrap();
        let cancelled = crate::storage::build_budget::used_memory_for_test();
        drop(writer);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !root.join(".test-lexical-consumer-started").exists() { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert!(p.writer.try_lock().is_err(), "owned writer must include index publication");
        assert!(matches!(output.deliver(|| ()), Err(RecordError::RecoveryRequired)));
        std::fs::remove_file(pause).unwrap();
        let receipt = tokio::time::timeout(std::time::Duration::from_secs(10),p.operation(&requests[1].operation_id)).await.unwrap().unwrap().unwrap();
        assert_eq!(receipt.seq, 2);
        assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 2);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while Arc::strong_count(&p.inner) != 1 { tokio::task::yield_now().await; }
        }).await.unwrap();
        p.finish_service_shutdown().await.unwrap();
        assert!(queued > used, "queued copies must be admitted");
        assert_eq!(cancelled, queued, "cancelling a waiter must not release an owned request copy's lease (wire bytes: {bytes})");
        drop(p);
        assert_eq!(crate::storage::build_budget::used_memory_for_test(), initial_used,
            "completed owners must return their memory leases");
    }

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
