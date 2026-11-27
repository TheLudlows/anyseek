//! Engine-side eligibility for closing an idle generation under memory pressure.
use super::RecordPartition;
use std::sync::Arc;

impl RecordPartition {
    /// Called while the service excludes new lookups of the generation. Raw
    /// partition owners cover accepted work and backups. Publication permits
    /// also pin the data fence independently of their original request.
    pub(crate) fn idle_for_residency(&self) -> bool {
        let Ok(_writer) = self.writer.try_lock() else {
            return false;
        };
        let Ok(inner) = self.inner.try_lock() else {
            return false;
        };
        inner.healthy
            && inner.background.is_none()
            && !inner.maintenance_scheduled
            && !inner.maintenance_pending
            && self.committed_queue.try_lock().is_ok_and(|queue| queue.is_idle())
            && Arc::strong_count(&self.inner) == 1
            // The controller and the partition state each own the fence.
            && Arc::strong_count(&self.output) == 2
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        ExpectedVersion,
        tests::{config, write},
    };
    use super::*;
    use crate::{
        resources::residency::{Lookup, Residency},
        storage::build_budget::shared_memory_usage,
    };
    use std::{path::Path, time::Duration};

    async fn wait_for_marker(path: &Path) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn accepted_write_and_output_prevent_retirement_and_reopen_keeps_the_cut() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("records");
        let backup = directory.path().join("backup");
        let partition = RecordPartition::create(&root, config()).await.unwrap();
        let request = write(
            "retained",
            "record",
            "Original body",
            ExpectedVersion::Absent,
        );

        // Stop the actual lexical consumer after accepting a durable write.
        let pause = root.join(".test-lexical-consumer-pause");
        std::fs::write(&pause, b"pause").unwrap();
        let owner = partition.clone();
        let accepted_request = request.clone();
        let caller = tokio::spawn(async move { owner.write(accepted_request).await });
        wait_for_marker(&root.join(".test-lexical-consumer-started")).await;
        assert!(!partition.idle_for_residency());
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert!(!partition.idle_for_residency());
        std::fs::remove_file(pause).unwrap();
        let receipt = tokio::time::timeout(
            Duration::from_secs(5),
            partition.operation(&request.operation_id),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let original = partition.get("record").await.unwrap().unwrap();

        let index_gate = root.join(".test-indexer-pause");
        std::fs::write(&index_gate, b"pause").unwrap();
        let job = partition
            .start_index_committed(Duration::from_secs(10))
            .unwrap();
        wait_for_marker(&root.join(".test-indexer-started")).await;
        assert!(!partition.idle_for_residency());
        std::fs::remove_file(index_gate).unwrap();
        assert_eq!(job.finish().await.unwrap(), receipt.seq);

        let backup_gate = backup.with_extension("pause-before-publish");
        std::fs::write(&backup_gate, b"pause").unwrap();
        let owner = partition.clone();
        let destination = backup.clone();
        let task = tokio::spawn(async move { owner.checkpoint(&destination).await });
        wait_for_marker(&backup.with_extension("snapshot-entered")).await;
        assert!(!partition.idle_for_residency());
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!partition.idle_for_residency());
        std::fs::remove_file(backup_gate).unwrap();
        wait_for_marker(&backup).await;

        tokio::time::timeout(Duration::from_secs(5), async {
            while !partition.idle_for_residency() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let output = partition.output_permit_after_publication().await.unwrap();
        let (used, _) = shared_memory_usage().unwrap();
        let table = Arc::new(Residency::new(used).unwrap());
        let scope = "tenant/records/0000000000000001";
        drop(table.install(scope.into(), partition).ok().unwrap());
        assert!(!table.has_room(1));
        assert!(
            table
                .claim_cold(1, RecordPartition::idle_for_residency)
                .is_none()
        );
        assert_eq!(
            output
                .deliver_after_publication(|| original.version)
                .await
                .unwrap(),
            receipt.version
        );
        drop(output);

        let retirement = table
            .claim_cold(1, RecordPartition::idle_for_residency)
            .unwrap();
        assert!(matches!(table.lookup(scope), Lookup::Retiring));
        retirement.value().finish_service_shutdown().await.unwrap();
        retirement.finish();
        assert!(matches!(table.lookup(scope), Lookup::Missing));
        assert!(table.has_room(1));

        for path in [&root, &backup] {
            let reopened = RecordPartition::open(path, config().max_journal_bytes)
                .await
                .unwrap();
            let row = reopened.get("record").await.unwrap().unwrap();
            assert_eq!(row.version, original.version);
            assert_eq!(row.document, original.document);
            assert_eq!(reopened.write(request.clone()).await.unwrap(), receipt);
            reopened.finish_service_shutdown().await.unwrap();
        }
    }
}
