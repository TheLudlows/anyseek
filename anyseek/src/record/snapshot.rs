//! Materialize a full independent root privately; only the final directory
//! publication makes it discoverable at the requested destination.
use super::*;

pub(super) async fn create(
    destination: &Path,
    config: RecordConfig,
    state: State,
) -> Result<(), RecordError> {
    if destination.exists() {
        return Err(creation_error(
            std::io::Error::new(std::io::ErrorKind::AlreadyExists, "snapshot target exists").into(),
        ));
    }
    checkpoint_codec::validate_size(
        checkpoint_codec::length(&config, &state)?,
        generation::reserve_bytes(&state),
        config.max_journal_bytes,
    )
    .map_err(|e| RecordError::Rejected(e.to_string()))?;
    let parent = destination
        .parent()
        .ok_or_else(|| storage("snapshot destination needs parent"))?;
    let prepared = parent.join(format!(".record-snapshot-{:032x}", rand::random::<u128>()));
    let mut owned = false;
    let built: Result<(), RecordError> = async {
        let target = RecordPartition::create(&prepared, config).await?;
        owned = true;
        {
            let mut inner = target.inner.lock().await;
            inner.publish_anchor_state(state, false).await?;
        }
        // Windows directory publication requires all writer/mmap/owner handles
        // to be released first. No async worker is detached from this owner.
        drop(target);
        clean_recovery_caches(&prepared)?;
        segment::sync_tree(&prepared)?;
        #[cfg(test)]
        {
            if destination.with_extension("reject-before-publish").exists() {
                return Err(storage("injected private snapshot rejection"));
            }
            let gate = destination.with_extension("pause-before-publish");
            if gate.exists() {
                std::fs::write(destination.with_extension("snapshot-entered"), b"ready")
                    .map_err(storage)?;
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
                while gate.exists() {
                    if std::time::Instant::now() >= deadline {
                        return Err(storage("snapshot test gate timed out"));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }
        }
        #[cfg(feature = "fault-injection")]
        publication_fault(destination, "prepared");
        Ok(())
    }
    .await;
    if let Err(error) = built {
        if owned {
            if let Err(cleanup) = std::fs::remove_dir_all(&prepared) {
                log::warn!(
                    "private snapshot cleanup pending at {}: {cleanup}",
                    prepared.display()
                );
            }
        }
        return Err(error);
    }
    match PartitionJournal::publish_prepared_root(&prepared, destination) {
        Ok(()) => {
            #[cfg(feature = "fault-injection")]
            publication_fault(destination, "published");
            #[cfg(test)]
            if destination.with_extension("reject-after-publish").exists() {
                return Err(RecordError::OutcomeUnknown(
                    "injected error after durable snapshot publication".into(),
                ));
            }
            Ok(())
        }
        Err(error @ CreateError::NotPublished(_)) => {
            if let Err(cleanup) = std::fs::remove_dir_all(&prepared) {
                log::warn!(
                    "unpublished snapshot cleanup pending at {}: {cleanup}",
                    prepared.display()
                );
            }
            Err(creation_error(error))
        }
        // Do not remove either root after a rename attempt. Recovery/readback
        // must determine the outcome; absence of a return value is not rollback.
        Err(error) => Err(creation_error(error)),
    }
}

// Faults target the actual independent-snapshot protocol: before and after
// atomic publication of an already certified root, not initial-root repair.
#[cfg(feature = "fault-injection")]
fn publication_fault(destination: &Path, stage: &str) {
    let prefix = std::env::var("ANYSEEK_SNAPSHOT_ROOT_PREFIX").unwrap_or_default();
    if !prefix.is_empty()
        && destination.file_name().is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
        && std::env::var("ANYSEEK_SNAPSHOT_FAULT").as_deref() == Ok(stage)
    {
        std::process::exit(87);
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{config, write};
    use super::*;

    #[tokio::test]
    async fn independent_snapshot_exceeds_one_frame_preserving_all_receipts() {
        let root = tempfile::tempdir().unwrap();
        let mut settings = config();
        settings.max_records = 512;
        settings.max_operations = 1024;
        settings.max_record_bytes = 64 * 1024;
        settings.max_journal_bytes = 128 * 1024 * 1024;
        let source = RecordPartition::create(&root.path().join("source"), settings.clone())
            .await
            .unwrap();
        let blob = "z".repeat(32 * 1024);
        let mut accepted = Vec::new();
        for n in 0..300 {
            let mut request = write(
                &format!("large-{n}"),
                &format!("id-{n}"),
                "alpha",
                ExpectedVersion::Absent,
            );
            request
                .document
                .as_mut()
                .unwrap()
                .insert("original-opaque".into(), serde_json::json!(blob));
            let receipt = source.write(request.clone()).await.unwrap();
            accepted.push((request, receipt));
            if (n + 1) % 50 == 0 {
                log::info!("large-snapshot accepted={}/300", n + 1);
            }
        }
        let bytes = source.inner.lock().await.snapshot_bytes;
        log::info!("large-snapshot canonical_bytes={bytes}");
        assert!(bytes > 16 * 1024 * 1024);
        assert!(PartitionJournal::validate_initial(bytes, settings.max_journal_bytes).is_err());
        let target = root.path().join("snapshot");
        assert_eq!(source.checkpoint(&target).await.unwrap(), 300);
        assert!(!std::fs::read_dir(&target).unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with("cache-")));
        let restored = RecordPartition::open(&target, settings.max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(restored.inner.lock().await.state.seq, 300);
        assert!(restored.inner.lock().await.state.independent_snapshot);
        for (request, receipt) in accepted {
            assert_eq!(restored.write(request).await.unwrap(), receipt);
        }
        assert_eq!(
            restored
                .get("id-299")
                .await
                .unwrap()
                .unwrap()
                .document
                .unwrap()["original-opaque"],
            serde_json::json!(blob)
        );
        assert_eq!(source.inner.lock().await.state.seq, 300);
        assert_eq!(restored.inner.lock().await.state.seq, 300);
    }
    #[tokio::test]
    async fn private_snapshot_failure_and_unknown_outcome_preserve_source_and_destination() {
        let root = tempfile::tempdir().unwrap();
        let settings = config();
        let source = RecordPartition::create(&root.path().join("source"), settings.clone())
            .await
            .unwrap();
        let request = write("original", "id", "alpha", ExpectedVersion::Absent);
        let receipt = source.write(request.clone()).await.unwrap();
        let target = root.path().join("snapshot");
        std::fs::write(target.with_extension("reject-before-publish"), b"fail").unwrap();
        assert!(source.checkpoint(&target).await.is_err());
        assert!(!target.exists());
        assert!(!std::fs::read_dir(root.path()).unwrap().any(|e| e
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with(".record-snapshot-")));
        assert_eq!(source.write(request.clone()).await.unwrap(), receipt);
        std::fs::remove_file(target.with_extension("reject-before-publish")).unwrap();
        std::fs::write(target.with_extension("reject-after-publish"), b"fail").unwrap();
        assert!(matches!(
            source.checkpoint(&target).await,
            Err(RecordError::OutcomeUnknown(_))
        ));
        let restored = RecordPartition::open(&target, settings.max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(restored.write(request.clone()).await.unwrap(), receipt);
        drop(restored);
        std::fs::write(target.join("unrelated-marker"), b"retain").unwrap();
        assert!(source.checkpoint(&target).await.is_err());
        assert_eq!(
            std::fs::read(target.join("unrelated-marker")).unwrap(),
            b"retain"
        );
        assert_eq!(source.write(request).await.unwrap(), receipt);
    }
    #[tokio::test]
    async fn cancelled_snapshot_waiter_keeps_owned_publication_task() {
        let root = tempfile::tempdir().unwrap();
        let settings = config();
        let source = Arc::new(
            RecordPartition::create(&root.path().join("source"), settings.clone())
                .await
                .unwrap(),
        );
        source
            .write(write("original", "id", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        let target = root.path().join("snapshot");
        let gate = target.with_extension("pause-before-publish");
        std::fs::write(&gate, b"wait").unwrap();
        let owner = source.clone();
        let destination = target.clone();
        let task = tokio::spawn(async move { owner.checkpoint(&destination).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !target.with_extension("snapshot-entered").exists() {
            assert!(std::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!target.exists());
        std::fs::remove_file(gate).unwrap();
        while !target.exists() {
            assert!(std::time::Instant::now() < deadline);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let restored = RecordPartition::open(&target, settings.max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(restored.inner.lock().await.state.seq, 1);
        assert_eq!(source.inner.lock().await.state.seq, 1);
    }
    #[test]
    fn logical_state_growth_does_not_relax_future_business_frame_limit() {
        assert!(checkpoint_codec::validate_size(20 * 1024 * 1024, 0, 64 * 1024 * 1024).is_ok());
        assert!(checkpoint_codec::validate_size(1, 20 * 1024 * 1024, 64 * 1024 * 1024).is_err());
        assert!(checkpoint_codec::validate_size(usize::MAX, 1, u64::MAX).is_err());
    }
}
