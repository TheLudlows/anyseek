//! Materialize a full independent root privately; only the final directory
//! publication makes it discoverable at the requested destination.
use super::*;

// The caller first fully validates an immutable service archive. The expected
// digest is its manifest's length-framed journal hash, not a trusted boolean.
// Hold the journal owner across export; verify all referenced files again, but
// do not build a second source retrieval view or a source recovery cache.
impl RecordPartition {
    pub(crate) async fn checkpoint_authenticated_archive(
        source:&Path, destination:&Path, budget:u64, expected_hash:&str, expected_seq:u64,
    )->Result<(),RecordError>{
        let source=source.to_owned();let destination=destination.to_owned();let expected_hash=expected_hash.to_owned();
        crate::resources::spawn(async move {
            use sha2::{Digest,Sha256};
            let frames=crate::storage::build_budget::reserve_memory(budget).map_err(storage)?;
            let (mut journal,entries)=PartitionJournal::open(&source,budget).map_err(storage)?;
            let mut hash=Sha256::new();
            for entry in &entries {hash.update((entry.len() as u64).to_le_bytes());hash.update(entry);}
            let actual:String=hash.finalize().iter().map(|byte|format!("{byte:02x}")).collect();
            if actual!=expected_hash || entries.len()!=1 {return Err(storage("certified archive journal changed"));}
            let first=entries.into_iter().next().unwrap();
            let decode=crate::storage::build_budget::reserve_memory((first.len() as u64).checked_mul(32).ok_or_else(||storage("archive decode overflow"))?).map_err(storage)?;
            let reference=match serde_json::from_slice(&first).map_err(storage)? {
                Entry::Anchor(reference)=>reference,
                _=>return Err(storage("certified archive must select an independent anchor")),
            };
            drop(first);drop(decode);drop(frames);
            let (config,mut state,_config_owner)=reference.archive_state(&source,budget)?;
            drop(reference);
            journal.restrict_budget(config.max_journal_bytes).map_err(storage)?;
            restore_operation_lookup(&mut state)?;
            validate_restored_state(&config,&state)?;
            if !state.independent_snapshot || state.seq!=expected_seq {return Err(storage("certified archive cut mismatch"));}
            state.history.scope=format!("{:032x}",rand::random::<u128>());
            state.evidence_quarantined|=state.records.values().any(temporal::protected);
            create(&destination,config,state).await?;
            drop(journal);
            Ok(())
        }).await.map_err(storage)?
    }
}

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
        validate_config(&config)?;
        let empty=State {history:history::HistoryState {scope:format!("{:032x}",rand::random::<u128>()),floor:0},lineage:format!("{:032x}",rand::random::<u128>()),..State::default()};
        let initial_size=checkpoint_codec::length(&config,&empty)?;
        let initial_memory=crate::storage::build_budget::reserve_memory((initial_size as u64).checked_mul(32).ok_or_else(||storage("snapshot initial overflow"))?).map_err(storage)?;
        let initial=encode_entry(&Entry::Checkpoint {config:config.clone(),state:empty})?;
        let mut journal=PartitionJournal::create(&prepared,config.max_journal_bytes,&initial).map_err(creation_error)?;
        owned=true;
        drop(initial);drop(initial_memory);
        // Neither the initial empty runtime nor a mutable tail can be queried
        // in this private snapshot; do not construct and immediately discard them.
        let (payload,_payload_owner,_disk_owner)=anchor::snapshot_payload(&prepared,&config,&state).await?;
        journal.checkpoint(&payload).map_err(creation_error)?;
        drop(journal);
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

    fn archive_hash(path:&Path,budget:u64)->String {
        use sha2::{Digest,Sha256};
        let (owner,entries)=PartitionJournal::open(path,budget).unwrap();
        let mut hash=Sha256::new();for entry in entries {hash.update((entry.len() as u64).to_le_bytes());hash.update(entry);}
        drop(owner);hash.finalize().iter().map(|byte|format!("{byte:02x}")).collect()
    }
    #[tokio::test]
    async fn authenticated_archive_export_binds_cut_history_and_private_failure() {
        let root=tempfile::tempdir().unwrap();let settings=config();let budget=settings.max_journal_bytes;
        let source=RecordPartition::create(&root.path().join("source"),settings).await.unwrap();
        let request=write("original","id","alpha",ExpectedVersion::Absent);
        let receipt=source.write(request.clone()).await.unwrap();
        let archive=root.path().join("archive");source.checkpoint(&archive).await.unwrap();
        let validated=RecordPartition::open(&archive,budget).await.unwrap();
        let old_scope=validated.inner.lock().await.state.history.scope.clone();drop(validated);
        let hash=archive_hash(&archive,budget);let target=root.path().join("target");
        assert!(RecordPartition::checkpoint_authenticated_archive(&archive,&target,budget,&"0".repeat(64),1).await.is_err());
        assert!(!target.exists());
        assert!(RecordPartition::checkpoint_authenticated_archive(&archive,&target,budget,&hash,2).await.is_err());
        assert!(!target.exists());
        std::fs::write(target.with_extension("reject-before-publish"),b"fail").unwrap();
        assert!(RecordPartition::checkpoint_authenticated_archive(&archive,&target,budget,&hash,1).await.is_err());
        assert!(!target.exists());assert_eq!(archive_hash(&archive,budget),hash);
        std::fs::remove_file(target.with_extension("reject-before-publish")).unwrap();
        let caches=||std::fs::read_dir(&archive).unwrap().filter_map(Result::ok).filter(|e|e.file_name().to_string_lossy().starts_with("cache-")).count();
        let before=caches();
        RecordPartition::checkpoint_authenticated_archive(&archive,&target,budget,&hash,1).await.unwrap();
        assert_eq!(caches(),before,"export must not construct a second source retrieval cache");
        let recovered=RecordPartition::open(&target,budget).await.unwrap();
        assert_eq!(recovered.write(request).await.unwrap(),receipt);
        assert_ne!(recovered.inner.lock().await.state.history.scope,old_scope);
        assert_eq!(archive_hash(&archive,budget),hash);
    }
    #[tokio::test]
    async fn authenticated_archive_export_rechecks_native_closure() {
        let root=tempfile::tempdir().unwrap();let settings=config();let budget=settings.max_journal_bytes;
        let source=RecordPartition::create(&root.path().join("source"),settings).await.unwrap();
        source.write(write("op","id","alpha",ExpectedVersion::Absent)).await.unwrap();
        let archive=root.path().join("archive");source.checkpoint(&archive).await.unwrap();
        let validated=RecordPartition::open(&archive,budget).await.unwrap();drop(validated);
        let hash=archive_hash(&archive,budget);
        // Reopening also creates an unselected empty tail. Corrupt the package
        // selected by the certificate, rather than an arbitrary directory whose
        // enumeration order varies across filesystems and random segment IDs.
        let (owner, entries)=PartitionJournal::open(&archive,budget).unwrap();
        let Entry::Anchor(reference)=serde_json::from_slice(&entries[0]).unwrap() else {panic!("archive has no anchor")};
        let selected=serde_json::to_value(reference).unwrap();
        let segment=archive.join(format!("segment-{}",selected["segments"][0]["id"].as_str().unwrap()));
        drop(owner);
        std::fs::write(segment.join("schema.json"),b"corrupt").unwrap();
        let target=root.path().join("target");
        assert!(RecordPartition::checkpoint_authenticated_archive(&archive,&target,budget,&hash,1).await.is_err());
        assert!(!target.exists());
    }
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
            let receipt = super::super::tests::write_after_maintenance(&source, request.clone()).await.unwrap();
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
