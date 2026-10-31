//! Complete local service snapshots. Caller owns source control/root and target
//! root locks. No live file copying, in-place recovery, or historical-key revival.
use super::*;
use crate::record::RecordPartition;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

/// Certified snapshot cut and provenance. Every record sequence belongs to the
/// same catalog barrier; the sequence numbers are not a distributed clock.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceSnapshot {
    /// Stable request-derived identity, chosen by the trusted service.
    pub identity: String,
    /// Protocol version, not an upgrade compatibility promise.
    pub format: u32,
    /// Canonical authoritative journal payload hashes keyed by derived relative path.
    pub objects: BTreeMap<String, String>,
    /// Independent live collection backup sequences (archives excluded).
    pub collection_backups: BTreeMap<String, u64>,
    /// Original source catalog cut.
    pub catalog_seq: u64,
    /// All collection partitions, including retained deleted collections.
    pub record_seqs: BTreeMap<String, u64>,
    /// Proven deleted-before-activation identities whose data never existed.
    pub absent_collections: std::collections::BTreeSet<String>,
    /// Present only on a restored root; identifies its immutable input archive.
    pub restored_from: Option<String>,
    /// Original Catalog hash for a restored service; no full historical Catalog copy.
    pub source_catalog_hash: Option<String>,
}
const COMPLETE: &str = "service-snapshot-complete";
const MANIFEST: &str = "service-snapshot-manifest";
fn staging(target: &Path) -> Result<PathBuf, CatalogError> {
    let name = target
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| storage("invalid target"))?;
    Ok(target.with_file_name(format!(".{name}.staging")))
}
fn publish(stage: &Path, target: &Path) -> Result<(), CatalogError> {
    crate::storage::partition::publish_bundle(stage, target)
        .map_err(|e| CatalogError::OutcomeUnknown(format!("bundle publication: {e}")))
}
fn read_manifest(root: &Path) -> Result<ServiceSnapshot, CatalogError> {
    let (journal, entries) =
        PartitionJournal::open(&root.join(MANIFEST), BUDGET).map_err(storage)?;
    drop(journal);
    if entries.len() != 1 {
        return Err(storage("invalid service manifest"));
    }
    serde_json::from_slice(&entries[0]).map_err(storage)
}
fn object_hash(root: &Path) -> Result<String, CatalogError> {
    if root.join(sqlite::DATABASE).exists() {
        let (store, state) = sqlite::Store::open(root)?;
        drop(store);
        let bytes = serde_json::to_vec(&state).map_err(storage)?;
        return Ok(Sha256::digest(&bytes).iter().map(|b| format!("{b:02x}")).collect());
    }
    let (journal, entries) = PartitionJournal::open(root, 256 * 1024 * 1024).map_err(storage)?;
    drop(journal);
    let mut hash = Sha256::new();
    for entry in entries {
        hash.update((entry.len() as u64).to_le_bytes());
        hash.update(entry);
    }
    Ok(hash.finalize().iter().map(|b| format!("{b:02x}")).collect())
}
fn safe_directory(name: &str) -> bool {
    let parts = name.split('/').collect::<Vec<_>>();
    parts.len() == 4 && parts[0] == "collections"
        && super::validate_name(parts[1]).is_ok() && super::validate_name(parts[2]).is_ok()
        && parts[3].len() == 16 && parts[3].bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn safe_backup(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn complete(root: &Path, receipt: &ServiceSnapshot) -> Result<(), CatalogError> {
    let path = root.join(COMPLETE);
    PartitionJournal::resume_initial(&path, BUDGET).map_err(|e| storage(format!("{e:?}")))?;
    let bytes = serde_json::to_vec(receipt).map_err(storage)?;
    if path.exists() {
        let (journal, entries) = PartitionJournal::open(&path, BUDGET).map_err(storage)?;
        drop(journal);
        if entries != vec![bytes] {
            return Err(storage("completion certificate mismatch"));
        }
    } else {
        drop(
            PartitionJournal::create(&path, BUDGET, &bytes)
                .map_err(|e| storage(format!("{e:?}")))?,
        );
    }
    Ok(())
}
async fn validate(root: &Path) -> Result<ServiceSnapshot, CatalogError> {
    let manifest = read_manifest(root)?;
    if manifest.format != 2 {
        return Err(storage("unsupported service snapshot format"));
    }
    if manifest.restored_from.is_some() != manifest.source_catalog_hash.is_some() {
        return Err(storage("restore provenance mismatch"));
    }
    for (name, expected) in &manifest.objects {
        if !name.is_empty()
            && !safe_directory(name)
            && !name.strip_prefix("backup-").is_some_and(safe_backup)
        {
            return Err(storage("invalid snapshot object path"));
        }
        if object_hash(&root.join(name))? != *expected {
            return Err(storage("snapshot content identity mismatch"));
        }
    }
    let catalog = Catalog::open(root).await?;
    let entries = catalog.collections().await?;
    let seq = catalog.inner.lock().await.state.seq;
    let expected = manifest.catalog_seq + u64::from(manifest.restored_from.is_some());
    if seq != expected
        || entries.len() != manifest.record_seqs.len() + manifest.absent_collections.len()
    {
        return Err(storage("service cut mismatch"));
    }
    let mut names = std::collections::BTreeSet::from([String::new()]);
    for entry in entries {
        if manifest.absent_collections.contains(&entry.scope()) {
            if !entry.deleted || entry.initialized || root.join(&entry.directory()).exists() {
                return Err(storage("invalid absent collection proof"));
            }
            continue;
        }
        if !safe_directory(&entry.directory()) {
            return Err(storage("invalid collection directory"));
        }
        names.insert(entry.directory());
        let partition =
            RecordPartition::open(&root.join(&entry.directory()), entry.config.max_journal_bytes)
                .await
                .map_err(storage)?;
        if partition.recovery_identity().await.map_err(storage)?.seq
            != *manifest
                .record_seqs
                .get(&entry.scope())
                .ok_or_else(|| storage("missing partition"))?
        {
            return Err(storage("record cut mismatch"));
        }
    }
    for (id, seq) in &manifest.collection_backups {
        if !safe_backup(id) {
            return Err(storage("invalid backup identity"));
        }
        let name = format!("backup-{id}");
        names.insert(name.clone());
        let p = RecordPartition::open(&root.join(name), 256 * 1024 * 1024)
            .await
            .map_err(storage)?;
        if p.recovery_identity().await.map_err(storage)?.seq != *seq {
            return Err(storage("backup cut mismatch"));
        }
    }
    if names != manifest.objects.keys().cloned().collect() {
        return Err(storage("snapshot object inventory mismatch"));
    }
    Ok(manifest)
}
async fn finish_existing(
    target: &Path,
    identity: &str,
) -> Result<Option<ServiceSnapshot>, CatalogError> {
    let stage = staging(target)?;
    if target.exists() && stage.exists() {
        return Err(storage("ambiguous bundle roots"));
    }
    if target.exists() {
        if let Some(receipt) = Catalog::completed_service_restore(target)? {
            if receipt.identity != identity {
                return Err(CatalogError::Conflict("restore identity mismatch".into()));
            }
            return Ok(Some(receipt));
        }
    }
    let candidate = if target.exists() {
        target
    } else {
        stage.as_path()
    };
    if !candidate.exists() {
        return Ok(None);
    }
    if !candidate.join(MANIFEST).exists() {
        if candidate == target {
            return Err(storage("published service root missing manifest"));
        }
        // Unpublished preparation has no visible result. Its contents are never
        // served; a retry can take a fresh cut only before a manifest exists.
        std::fs::remove_dir_all(candidate).map_err(storage)?;
        return Ok(None);
    }
    PartitionJournal::resume_initial(&candidate.join(MANIFEST), BUDGET)
        .map_err(|e| storage(format!("{e:?}")))?;
    let receipt = validate(candidate).await?;
    if receipt.identity != identity {
        return Err(CatalogError::Conflict("snapshot identity mismatch".into()));
    }
    // A previous rename may have returned unknown after becoming visible.
    // Close all journals/mappings, then re-establish durable parent publication.
    if candidate == target && target.join(COMPLETE).exists() {
        complete(target, &receipt)?;
        return Ok(Some(receipt));
    }
    if candidate == target {
        publish(target, &stage)?;
    }
    publish(&stage, target)?;
    #[cfg(feature = "fault-injection")]
    fault("bundle_published");
    complete(target, &receipt)?;
    Ok(Some(receipt))
}
#[cfg(feature = "fault-injection")]
fn fault(stage: &str) {
    if std::env::var("ANYSEEK_SERVICE_SNAPSHOT_FAULT").as_deref() == Ok(stage) {
        std::process::exit(88);
    }
}
impl Catalog {
    /// Return the original completed backup receipt without reopening every
    /// child index. Restore still validates the entire immutable archive closure.
    pub fn completed_service_backup(root: &Path) -> Result<Option<ServiceSnapshot>, CatalogError> {
        Self::completed_service_snapshot(root,false)
    }

    /// Immutable completion lookup. A used restored root need not retain its
    /// original data cut; this never opens or overwrites live data/catalog.
    pub fn completed_service_restore(root: &Path) -> Result<Option<ServiceSnapshot>, CatalogError> {
        Self::completed_service_snapshot(root,true)
    }

    fn completed_service_snapshot(root: &Path, restored: bool) -> Result<Option<ServiceSnapshot>, CatalogError> {
        if !root.join(COMPLETE).exists() {
            return Ok(None);
        }
        let receipt = read_manifest(root)?;
        if receipt.restored_from.is_some()!=restored {
            return Ok(None);
        }
        let (journal, entries) = match PartitionJournal::open(&root.join(COMPLETE), BUDGET) {
            Ok(value) => value,
            Err(e) if crate::storage::partition::is_uncertified(&e) => return Ok(None),
            Err(e) => return Err(storage(e)),
        };
        drop(journal);
        if entries != vec![serde_json::to_vec(&receipt).map_err(storage)?] {
            return Err(storage("service completion mismatch"));
        }
        Ok(Some(receipt))
    }

    /// Refuse a restored/archive root whose outer publication is incomplete.
    pub async fn require_service_completion(&self, root: &Path) -> Result<(), CatalogError> {
        let marker = self.inner.lock().await.state.service_snapshot.clone();
        if let Some((identity, restored)) = marker {
            if !restored {
                return Err(storage("archive cannot be served"));
            }
            if !root.join(MANIFEST).exists() || read_manifest(root)?.identity != identity {
                return Err(storage("restored root manifest missing or mismatched"));
            }
        }
        if root.join(MANIFEST).exists() {
            let receipt = read_manifest(root)?;
            if receipt.restored_from.is_none() {
                return Err(storage("archive cannot be served; restore to a new root"));
            }
            if !root.join(COMPLETE).exists() {
                return Err(storage(
                    "restore publication incomplete; retry through trusted source",
                ));
            }
            let (journal, entries) =
                PartitionJournal::open(&root.join(COMPLETE), BUDGET).map_err(storage)?;
            drop(journal);
            if entries != vec![serde_json::to_vec(&receipt).map_err(storage)?] {
                return Err(storage("restore completion mismatch"));
            }
        }
        Ok(())
    }

    /// Freeze the service's existing authorized Record path and snapshot the
    /// complete catalog plus every retained collection. Caller holds source
    /// service control/root ownership; direct unmanaged Record writers are not
    /// permitted. Existing service archives are excluded; live collection backups are included.
    pub async fn backup_service(
        &self,
        source_root: PathBuf,
        target: PathBuf,
        identity: String,
        records: BTreeMap<String, RecordPartition>,
    ) -> Result<ServiceSnapshot, CatalogError> {
        self.backup_service_with_archives(source_root,target,identity,records,BTreeMap::new()).await
    }

    /// Complete-service snapshot with privately materialized journal paths keyed
    /// by derived collection/backup directory. Open one archive at a time so
    /// retained backups do not all consume live index reservations. The server
    /// holds the control barrier, private paths and backup retention pins.
    pub async fn backup_service_with_archives(
        &self,source_root:PathBuf,target:PathBuf,identity:String,
        records:BTreeMap<String,RecordPartition>,archives:BTreeMap<String,PathBuf>,
    )->Result<ServiceSnapshot,CatalogError> {
        let admission = self.admission.clone().try_acquire_owned().map_err(|_| CatalogError::Busy)?;
        let owner = self.inner.clone();
        let execution = self.execution.clone();
        crate::resources::spawn(async move {
                let _admission = admission;
                let _execution = execution.freeze().await;
                let inner = owner.lock().await;
                if !inner.is_healthy() {
                    return Err(CatalogError::RecoveryRequired);
                }
                if let Some(receipt) = finish_existing(&target, &identity).await? {
                    return Ok(receipt);
                }
                if inner
                    .state
                    .collections
                    .values()
                    .any(|e| !e.active && !e.deleted)
                {
                    return Err(CatalogError::Conflict(
                        "unfinished collection must recover before service backup".into(),
                    ));
                }
                // Preflight bounded authoritative payload before creating the
                // archive. Private snapshots carry certified immutable packages;
                // no live cache is copied. Per-partition closure budgets also apply.
                let mut total = serde_json::to_vec(&inner.state).map_err(storage)?.len() as u64;
                for entry in inner.state.collections.values().filter(|entry| !entry.reclaiming) {
                    if entry.deleted
                        && !entry.initialized
                        && !source_root.join(&entry.directory()).exists()
                    {
                        continue;
                    }
                    let p = match records.get(&entry.scope()) {
                        Some(p) => p.clone(),
                        None => RecordPartition::open(
                            &archives.get(&entry.directory()).cloned().unwrap_or_else(||source_root.join(&entry.directory())),
                            entry.config.max_journal_bytes,
                        )
                        .await
                        .map_err(storage)?,
                    };
                    total = total.saturating_add(
                        p.recovery_identity().await.map_err(storage)?.checkpoint_bytes as u64 + 4096,
                    );
                }
                for row in std::fs::read_dir(&source_root).map_err(storage)? {
                    let row = row.map_err(storage)?;
                    let name = row.file_name().to_string_lossy().into_owned();
                    if let Some(id) = name.strip_prefix("backup-").filter(|id| safe_backup(id)) {
                        if inner.state.retired_backups.contains(id) {
                            continue;
                        }
                        let p=if let Some(path)=archives.get(&name) {
                            RecordPartition::open(path,256*1024*1024).await.map_err(storage)?
                        }else {
                            RecordPartition::resume_initialization(&row.path(),256*1024*1024).await.map_err(storage)?;
                            RecordPartition::open(&row.path(),256*1024*1024).await.map_err(storage)?
                        };
                        total = total.saturating_add(
                            p.recovery_identity().await.map_err(storage)?.checkpoint_bytes as u64 + 4096,
                        );
                    }
                }
                if total > 1024 * 1024 * 1024 {
                    return Err(CatalogError::Rejected(
                        "service snapshot payload budget (1 GiB) exceeded".into(),
                    ));
                }
                let stage = staging(&target)?;
                let mut archived = inner.state.clone();
                archived.service_snapshot = Some((identity.clone(), false));
                archived.collections.retain(|_, entry| !entry.reclaiming);
                drop(Catalog::create_state(&stage, archived).await?);
                let mut record_seqs = BTreeMap::new();
                let mut absent_collections = std::collections::BTreeSet::new();
                let mut objects = BTreeMap::from([(String::new(), object_hash(&stage)?)]);
                for entry in inner.state.collections.values().filter(|entry| !entry.reclaiming) {
                    if entry.deleted
                        && !entry.initialized
                        && !source_root.join(&entry.directory()).exists()
                    {
                        absent_collections.insert(entry.scope());
                        continue;
                    }
                    let partition = match records.get(&entry.scope()) {
                        Some(p) => p.clone(),
                        None => RecordPartition::open(
                            &archives.get(&entry.directory()).cloned().unwrap_or_else(||source_root.join(&entry.directory())),
                            entry.config.max_journal_bytes,
                        )
                        .await
                        .map_err(storage)?,
                    };
                    entry.prepare_parent(&stage)?;
                    let seq = partition
                        .checkpoint(&stage.join(&entry.directory()))
                        .await
                        .map_err(storage)?;
                    record_seqs.insert(entry.scope(), seq);
                    objects.insert(
                        entry.directory(),
                        object_hash(&stage.join(&entry.directory()))?,
                    );
                }
                let mut collection_backups = BTreeMap::new();
                for row in std::fs::read_dir(&source_root).map_err(storage)? {
                    let row = row.map_err(storage)?;
                    let name = row.file_name().to_string_lossy().into_owned();
                    if let Some(id) = name.strip_prefix("backup-").filter(|id| safe_backup(id)) {
                        if inner.state.retired_backups.contains(id) {
                            continue;
                        }
                        let p=if let Some(path)=archives.get(&name) {
                            RecordPartition::open(path,256*1024*1024).await.map_err(storage)?
                        }else {
                            RecordPartition::resume_initialization(&row.path(),256*1024*1024).await.map_err(storage)?;
                            RecordPartition::open(&row.path(),256*1024*1024).await.map_err(storage)?
                        };
                        let seq = p.checkpoint(&stage.join(&name)).await.map_err(storage)?;
                        collection_backups.insert(id.into(), seq);
                        objects.insert(name.clone(), object_hash(&stage.join(&name))?);
                    }
                }
                #[cfg(feature = "fault-injection")]
                fault("backup_children");
                let receipt = ServiceSnapshot {
                    identity,
                    format: 2,
                    objects,
                    collection_backups,
                    catalog_seq: inner.state.seq,
                    record_seqs,
                    absent_collections,
                    restored_from: None,
                    source_catalog_hash: None,
                };
                drop(
                    PartitionJournal::create(
                        &stage.join(MANIFEST),
                        BUDGET,
                        &serde_json::to_vec(&receipt).map_err(storage)?,
                    )
                    .map_err(|e| storage(format!("{e:?}")))?,
                );
                #[cfg(feature = "fault-injection")]
                fault("backup_manifest");
                publish(&stage, &target)?;
                #[cfg(feature = "fault-injection")]
                fault("backup_published");
                complete(&target, &receipt)
                    .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?;
                Ok(receipt)
            })
            .await
            .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?
    }
    /// Restore a complete archive into a new, non-serving root. All historical
    /// ordinary keys remain disabled; the target master issues fresh tenant keys. Original authority is retained separately for audit, never loaded
    /// by normal Service::open. Caller owns target's external service-root lock.
    pub async fn restore_service(
        archive: PathBuf,
        target: PathBuf,
        identity: String,
        current_master: String,
    ) -> Result<ServiceSnapshot, CatalogError> {
        crate::resources::spawn(async move {
                #[cfg(feature = "fault-injection")]
                let restore_started = std::time::Instant::now();
                let input = validate(&archive).await?;
                #[cfg(feature = "fault-injection")]
                log::info!("service restore input validated elapsed_ms={} collections={} backups={}", restore_started.elapsed().as_millis(), input.record_seqs.len(), input.collection_backups.len());
                if !archive.join(COMPLETE).exists() {
                    return Err(storage("archive is not certified complete"));
                }
                complete(&archive, &input)?;
                if input.restored_from.is_some() {
                    return Err(CatalogError::Rejected(
                        "restore input must be an archive".into(),
                    ));
                }
                if let Some(receipt) = finish_existing(&target, &identity).await? {
                    if receipt.restored_from.as_ref() != Some(&input.identity) {
                        return Err(CatalogError::Conflict("restore input changed".into()));
                    }
                    return Ok(receipt);
                }
                let original = Catalog::open(&archive).await?;
                let mut state = original.inner.lock().await.state.clone();
                state.service_snapshot = Some((identity.clone(), true));
                if state
                    .keys
                    .iter()
                    .any(|(id, key)| id != "master" && key.verifier == current_master)
                {
                    return Err(CatalogError::Rejected(
                        "current master collides with historical non-master identity".into(),
                    ));
                }
                state
                    .historical_operations
                    .retain(|(_,id)| id.epoch.saturating_add(1) >= epoch());
                state.historical_operations.extend(
                    state
                        .operations
                        .iter()
                        .map(|e| (e.request.command.tenant().to_owned(),e.request.operation_id.clone())),
                );
                state.operations.clear();
                for key in state.keys.values_mut() {
                    key.active = false;
                }
                state.seq = state
                    .seq
                    .checked_add(1)
                    .ok_or_else(|| storage("catalog sequence exhausted"))?;
                state.epoch_watermark = state.epoch_watermark.max(epoch());
                state.keys.insert(
                    "master".into(),
                    Key {
                        verifier: current_master,
                        active: true,
                        tenant_id: None,
                    },
                );
                let stage = staging(&target)?;
                drop(Catalog::create_state(&stage, state.clone()).await?);
                for entry in state.collections.values() {
                    if input.absent_collections.contains(&entry.scope()) {
                        continue;
                    }
                    entry.prepare_parent(&stage)?;
                    RecordPartition::checkpoint_authenticated_archive(
                        &archive.join(&entry.directory()), &stage.join(&entry.directory()), entry.config.max_journal_bytes,
                        input.objects.get(&entry.directory()).ok_or_else(||storage("missing authenticated child"))?,
                        *input.record_seqs.get(&entry.scope()).ok_or_else(||storage("missing authenticated cut"))?,
                    ).await.map_err(storage)?;
                    #[cfg(feature = "fault-injection")]
                    log::info!("service restore child complete elapsed_ms={} child={}", restore_started.elapsed().as_millis(), entry.directory());
                }
                for id in input.collection_backups.keys() {
                    let name = format!("backup-{id}");
                    RecordPartition::checkpoint_authenticated_archive(
                        &archive.join(&name), &stage.join(&name), 256*1024*1024,
                        input.objects.get(&name).ok_or_else(||storage("missing authenticated backup"))?,
                        input.collection_backups[id],
                    ).await.map_err(storage)?;
                    #[cfg(feature = "fault-injection")]
                    log::info!("service restore backup complete elapsed_ms={} child={}", restore_started.elapsed().as_millis(), name);
                }
                let mut objects = BTreeMap::new();
                for name in input
                    .objects
                    .keys()
                {
                    objects.insert(name.clone(), object_hash(&stage.join(name))?);
                }
                #[cfg(feature = "fault-injection")]
                fault("restore_children");
                let receipt = ServiceSnapshot {
                    identity,
                    format: 2,
                    objects,
                    collection_backups: input.collection_backups,
                    catalog_seq: input.catalog_seq,
                    record_seqs: input.record_seqs,
                    absent_collections: input.absent_collections,
                    restored_from: Some(input.identity),
                    source_catalog_hash: input.objects.get("").cloned(),
                };
                drop(
                    PartitionJournal::create(
                        &stage.join(MANIFEST),
                        BUDGET,
                        &serde_json::to_vec(&receipt).map_err(storage)?,
                    )
                    .map_err(|e| storage(format!("{e:?}")))?,
                );
                #[cfg(feature = "fault-injection")]
                fault("restore_manifest");
                publish(&stage, &target)?;
                #[cfg(feature = "fault-injection")]
                fault("restore_published");
                complete(&target, &receipt)
                    .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?;
                Ok(receipt)
            })
            .await
            .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?
    }
}
