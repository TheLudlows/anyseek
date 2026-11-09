//! Synchronous independent-write policy. HTTP only maps these decisions to rows.
use super::*;
use futures::FutureExt;

/// One synchronous independent-write result, in input order.
#[derive(Debug)]
pub enum IndependentItem {
    /// A durable receipt whose indexes are queryable.
    Applied(WriteReceipt),
    /// This input's error, including Busy or an uncertain persistence outcome.
    /// Uncertain outcomes must be resolved using the original operation ID.
    Failed(RecordError),
    /// An earlier fatal interruption prevented this input from being attempted.
    NotAttempted,
}

impl RecordPartition {
    /// Execute 1..100 independent writes with the bounded 64-command kernel.
    /// Known-unattempted inputs may fall back to scalar execution on admission
    /// pressure. Unknown inputs are never retried. One owner retains every
    /// request and any caller resource context until completion, even if the caller leaves.
    /// Incoming storage is transferred from the caller; additional planner
    /// copies are charged separately before allocation.
    pub async fn write_independent_batch(
        &self,
        requests: Vec<RecordWrite>,
    ) -> Result<Vec<IndependentItem>, RecordError> {
        if requests.is_empty() || requests.len() > 100 {
            return Err(RecordError::Rejected("batch requires 1..100 independent writes".into()));
        }
        // The already allocated input is moved, not copied. spawn retains its
        // caller's resource scope/activity, just as the HTTP handler retained
        // the parsed input. Do not add aggregate admission here: a large refused
        // group must still allow a later small write or deletion to proceed.
        // Extra planning storage is admitted per bounded window before cloning.
        let partition = self.clone();
        crate::resources::spawn(async move {
            run(&partition, &requests).await
        }).await.map_err(|error| RecordError::OutcomeUnknown(error.to_string()))
    }
}

fn stops_batch(error: &RecordError) -> bool {
    matches!(error, RecordError::OutcomeUnknown(_) | RecordError::DurablePending(_)
        | RecordError::RecoveryRequired | RecordError::Storage(_))
}

async fn attempt(partition: &RecordPartition, requests: &[RecordWrite]) -> Result<CommittedBatch, RecordError> {
    // Keep the existing 64-input/4 MiB group admission before any attempt.
    // A refused group may still contain individually admissible commands.
    request_bytes(requests)?;
    std::panic::AssertUnwindSafe(execute_owned(partition, requests, IndexDelivery::Synchronous))
        .catch_unwind().await
        .map_err(|_| RecordError::OutcomeUnknown("Record batch owner unwound".into()))
}

async fn single(partition: &RecordPartition, request: &RecordWrite) -> Result<WriteReceipt, RecordError> {
    let mut batch = attempt(partition, std::slice::from_ref(request)).await?;
    match batch.results.pop() {
        Some(CommittedItem::Committed(receipt)) => Ok(receipt),
        Some(CommittedItem::Rejected(error)) => Err(error),
        _ => Err(batch.stop.unwrap_or_else(|| storage("single write produced no decision"))),
    }
}

async fn run(partition: &RecordPartition, requests: &[RecordWrite]) -> Vec<IndependentItem> {
    let mut rows = Vec::with_capacity(requests.len());
    let mut offset = 0;
    let mut stopped = false;
    while offset < requests.len() {
        if stopped {
            rows.extend((offset..requests.len()).map(|_| IndependentItem::NotAttempted));
            break;
        }
        let end = (offset + 64).min(requests.len());
        let chunk = &requests[offset..end];
        let (items, stop) = match attempt(partition, chunk).await {
            Ok(batch) => (batch.results, batch.stop),
            Err(error @ (RecordError::Busy | RecordError::Rejected(_))) => (
                chunk.iter().map(|_| CommittedItem::NotAttempted).collect(), Some(error)),
            Err(error) => (chunk.iter().map(|_| CommittedItem::OutcomeUnknown).collect(), Some(error)),
        };
        let interrupted = stop.as_ref().is_some_and(stops_batch);
        let regroup = matches!(stop, Some(RecordError::Busy));
        let mut stop_reported = false;
        let mut remaining = items.into_iter();
        while let Some(item) = remaining.next() {
            let fallback = matches!(item, CommittedItem::NotAttempted);
            let result = match item {
                CommittedItem::Committed(receipt) => Some(Ok(receipt)),
                CommittedItem::Rejected(error) => Some(Err(error)),
                CommittedItem::OutcomeUnknown => {
                    stop_reported = true;
                    Some(Err(stop.clone().unwrap_or_else(|| RecordError::OutcomeUnknown("batch execution exited".into()))))
                }
                CommittedItem::NotAttempted if interrupted && !stop_reported => {
                    stop_reported = true;
                    Some(Err(stop.clone().unwrap()))
                }
                CommittedItem::NotAttempted if stopped || interrupted => None,
                // Busy is local to this input. Later deletions or smaller writes
                // must still get their own admission decision.
                CommittedItem::NotAttempted => Some(single(partition, &requests[offset]).await),
            };
            let progressed = result.as_ref().is_some_and(Result::is_ok);
            rows.push(match result {
                Some(Ok(receipt)) => IndependentItem::Applied(receipt),
                Some(Err(error)) => {
                    stopped |= stops_batch(&error);
                    IndependentItem::Failed(error)
                }
                None => IndependentItem::NotAttempted,
            });
            offset += 1;
            // A successful scalar decision crosses a native-tail boundary. Only
            // regroup the known-unattempted suffix, using an index, not a copy.
            if regroup && fallback && progressed && !stopped && !interrupted
                && remaining.as_slice().iter().all(|item| matches!(item, CommittedItem::NotAttempted)) {
                break;
            }
        }
        stopped |= interrupted;
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, write};

    fn settings() -> RecordConfig {
        let mut config = config();
        config.max_records = 128;
        config.max_operations = 256;
        config
    }

    #[tokio::test]
    async fn independent_rejections_keep_100_64_limits_order_visibility_and_replay() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let p = RecordPartition::create(&root, settings()).await.unwrap();
        let requests: Vec<_> = (0..100).map(|i| write(&format!("mixed-{i}"), "same", "alpha",
            if i == 0 || i % 2 == 1 { ExpectedVersion::Absent } else { ExpectedVersion::Match(i / 2) })).collect();
        assert!(matches!(p.write_batch(&requests[..65]).await, Err(RecordError::Rejected(_))));
        assert!(matches!(p.commit_writes(requests[..65].to_vec()).await, Err(RecordError::Rejected(_))));
        assert!(matches!(p.write_independent_batch(vec![]).await, Err(RecordError::Rejected(_))));
        assert!(matches!(p.write_independent_batch(vec![requests[0].clone(); 101]).await, Err(RecordError::Rejected(_))));
        assert_eq!(p.committed_progress().await.committed_seq, Some(0));
        let result = p.write_independent_batch(requests.clone()).await.unwrap();
        assert_eq!(result.len(), 100);
        for (i, item) in result.iter().enumerate() {
            if i % 2 == 0 {
                assert!(matches!(item, IndependentItem::Applied(receipt) if receipt.seq == (i / 2 + 1) as u64 && receipt.version == receipt.seq));
            } else {
                assert!(matches!(item, IndependentItem::Failed(RecordError::Conflict(_))));
            }
        }
        let progress = p.committed_progress().await;
        assert_eq!((progress.committed_seq, progress.lexical_seq, progress.vector_seq), (Some(50), 50, 50));
        assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 1);
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let p = RecordPartition::open(&root, settings().max_journal_bytes).await.unwrap();
        let replay = p.write_independent_batch(requests).await.unwrap();
        for (actual, expected) in replay.iter().zip(&result) {
            match expected {
                IndependentItem::Applied(expected) => assert!(matches!(actual, IndependentItem::Applied(actual) if actual == expected)),
                _ => assert!(matches!(actual, IndependentItem::Failed(RecordError::Conflict(_)))),
            }
        }
        assert_eq!(p.committed_progress().await.committed_seq, Some(50));
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn independent_unknown_never_retries_and_recovers_original_receipts() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let p = RecordPartition::create(&root, settings()).await.unwrap();
        let seed = write("seed", "seed", "alpha", ExpectedVersion::Absent);
        let receipt = p.write(seed.clone()).await.unwrap();
        let mut requests = vec![seed,
            write("new", "same", "alpha", ExpectedVersion::Absent),
            write("rejected", "same", "bad", ExpectedVersion::Absent)];
        requests.extend((3..100).map(|i| write(&format!("tail-{i}"), &format!("tail-{i}"), "alpha", ExpectedVersion::Absent)));
        p.inner.lock().await.fail_after_durable = true;
        let result = p.write_independent_batch(requests.clone()).await.unwrap();
        assert!(matches!(&result[0], IndependentItem::Applied(actual) if *actual == receipt));
        assert!(matches!(&result[1], IndependentItem::Failed(RecordError::DurablePending(2))));
        assert!(result[2..].iter().all(|item| matches!(item, IndependentItem::NotAttempted)));
        assert!(p.committed_progress().await.recovery_required);
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let p = RecordPartition::open(&root, settings().max_journal_bytes).await.unwrap();
        assert_eq!(p.operation(&requests[1].operation_id).await.unwrap().unwrap().seq, 2);
        for request in &requests[2..] {
            assert!(p.operation(&request.operation_id).await.unwrap().is_none());
        }
        let replay = p.write_independent_batch(requests).await.unwrap();
        for (i, item) in replay.iter().enumerate() {
            if i == 2 { assert!(matches!(item, IndependentItem::Failed(RecordError::Conflict(_)))); }
            else { assert!(matches!(item, IndependentItem::Applied(_))); }
        }
        assert!(matches!(&replay[0], IndependentItem::Applied(actual) if *actual == receipt));
        assert!(matches!(&replay[1], IndependentItem::Applied(actual) if actual.seq == 2));
        assert_eq!(p.committed_progress().await.committed_seq, Some(99));
        p.finish_service_shutdown().await.unwrap();
    }

    #[cfg(feature = "fault-injection")]
    #[tokio::test]
    async fn independent_busy_still_admits_later_delete_and_regroups_suffix() {
        if std::env::var_os("ANYSEEK_INDEPENDENT_BUSY_CHILD").is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "record::committed::independent::tests::independent_busy_still_admits_later_delete_and_regroups_suffix", "--nocapture"])
                .env("ANYSEEK_INDEPENDENT_BUSY_CHILD", "1")
                .env("ANYSEEK_RECORD_FAULT_NONCE", "blocked")
                .env("ANYSEEK_RECORD_FAULT", "busy_before_append")
                .output().unwrap();
            assert!(output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
                "{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let p = RecordPartition::create(&root, settings()).await.unwrap();
        p.write(write("seed", "seed", "alpha", ExpectedVersion::Absent)).await.unwrap();
        let mut delete = write("delete", "seed", "unused", ExpectedVersion::Match(1));
        delete.document = None;
        let requests = vec![write("blocked", "blocked", "alpha", ExpectedVersion::Absent), delete,
            write("suffix", "suffix", "beta", ExpectedVersion::Absent)];
        let result = p.write_independent_batch(requests.clone()).await.unwrap();
        assert!(matches!(&result[..], [IndependentItem::Failed(RecordError::Busy),
            IndependentItem::Applied(a), IndependentItem::Applied(b)] if a.seq == 2 && b.seq == 3));
        assert!(p.operation(&requests[0].operation_id).await.unwrap().is_none());
        assert!(p.get("seed").await.unwrap().is_none());
        assert!(p.get("suffix").await.unwrap().is_some());
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let p = RecordPartition::open(&root, settings().max_journal_bytes).await.unwrap();
        assert!(p.operation(&requests[0].operation_id).await.unwrap().is_none());
        assert_eq!(p.operation(&requests[1].operation_id).await.unwrap().unwrap().seq, 2);
        assert_eq!(p.operation(&requests[2].operation_id).await.unwrap().unwrap().seq, 3);
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn independent_group_memory_pressure_keeps_scalar_replay_admissible() {
        // Build reservations use a process-global ledger, so isolate both the
        // pressure and its assertion from all other tests.
        if std::env::var_os("ANYSEEK_INDEPENDENT_MEMORY_CHILD").is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "record::committed::independent::tests::independent_group_memory_pressure_keeps_scalar_replay_admissible", "--nocapture"])
                .env("ANYSEEK_INDEPENDENT_MEMORY_CHILD", "1").output().unwrap();
            assert!(output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed; 0 failed"),
                "{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let p = RecordPartition::create(&dir.path().join("records"), settings()).await.unwrap();
        let seed = write("seed", "seed", "alpha", ExpectedVersion::Absent);
        let receipt = p.write(seed.clone()).await.unwrap();
        let requests = vec![write("large", "large", &"x".repeat(2 * 1024 * 1024), ExpectedVersion::Absent), seed];
        // The remaining 1 MiB cannot hold even the aggregate wire bytes, but
        // does cover the replay's complete plan and original receipt.
        let held = crate::storage::build_budget::fill_memory_except_for_test(1024 * 1024);
        let result = p.write_independent_batch(requests.clone()).await.unwrap();
        assert!(matches!(&result[..], [IndependentItem::Failed(RecordError::Busy),
            IndependentItem::Applied(actual)] if *actual == receipt));
        drop(held);
        assert!(p.operation(&requests[0].operation_id).await.unwrap().is_none());
        assert_eq!(p.committed_progress().await.committed_seq, Some(1));
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn independent_cancel_retains_one_owner_through_all_100_inputs() {
        use crate::resources::{Governor, Lane, Lifecycle};
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("records");
        let p = RecordPartition::create(&root, settings()).await.unwrap();
        let requests: Vec<_> = (0..100).map(|i| write(&format!("cancel-{i}"), "same", "alpha",
            if i == 0 { ExpectedVersion::Absent } else { ExpectedVersion::Match(i) })).collect();
        let last = requests.last().unwrap().operation_id.clone();
        let lifecycle = Arc::new(Lifecycle::default());
        let scope = Arc::new(Governor::default()).admit("independent-owner".into(), Lane::Foreground, 0).unwrap();
        let writer = p.writer.lock().await;
        let permits = p.admission.available_permits();
        let owner = p.clone();
        let caller = tokio::spawn(lifecycle.enter().unwrap().run(scope.run(async move {
            owner.write_independent_batch(requests).await
        })));
        tokio::time::timeout(Duration::from_secs(5), async {
            while p.admission.available_permits() == permits { tokio::task::yield_now().await; }
        }).await.unwrap();
        assert_eq!(Arc::strong_count(&p.inner), 3, "caller and exactly one batch owner");
        lifecycle.stop();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(Arc::strong_count(&p.inner), 2);
        assert!(tokio::time::timeout(Duration::from_millis(20), lifecycle.drained()).await.is_err());
        drop(writer);
        tokio::time::timeout(Duration::from_secs(30), lifecycle.drained()).await.unwrap();
        assert_eq!(p.admission.available_permits(), permits);
        assert_eq!(Arc::strong_count(&p.inner), 1);
        let receipt = p.operation(&last).await.unwrap().unwrap();
        assert_eq!((receipt.seq, receipt.version), (100, 100));
        let progress = p.committed_progress().await;
        assert_eq!((progress.committed_seq, progress.lexical_seq, progress.vector_seq), (Some(100), 100, 100));
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let p = RecordPartition::open(&root, settings().max_journal_bytes).await.unwrap();
        assert_eq!(p.operation(&last).await.unwrap(), Some(receipt));
        p.finish_service_shutdown().await.unwrap();
    }
}
