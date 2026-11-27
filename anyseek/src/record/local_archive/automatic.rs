use super::*;
use crate::storage::partition::ARCHIVE_POLICY;
use std::{
    fs,
    io::{Read, Write},
};

const POLICY_BUDGET: u64 = 16 * 1024;
const WINDOW_PREFIX: &str = "window-";

/// A selected local archive target. The primary owner serializes enrollment and
/// capture. The same identity on the archive medium detects a missing mount.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::record) struct Policy {
    directory: PathBuf,
    identity: String,
    first_generation: u64,
    base_seq: u64,
    base_sha256: [u8; 32],
    max_bytes: u64,
    max_generations: usize,
}

impl Policy {
    pub(in crate::record) fn load(root: &Path) -> Result<Option<Self>, RecordError> {
        let path = root.join(ARCHIVE_POLICY);
        if !path.try_exists().map_err(storage)? {
            return Ok(None);
        }
        // The external stream is synced before enrollment publishes its
        // primary policy. Finish an interrupted initial policy publication.
        PartitionJournal::resume_initial(&path, POLICY_BUDGET).map_err(creation_error)?;
        let (_owner, frames) = PartitionJournal::open(&path, POLICY_BUDGET).map_err(storage)?;
        let [frame] = frames.as_slice() else {
            return Err(storage("invalid archive policy journal"));
        };
        let policy: Self = serde_json::from_slice(frame).map_err(storage)?;
        policy.validate()?;
        Ok(Some(policy))
    }

    fn validate(&self) -> Result<(), RecordError> {
        if self.identity.len() != 32
            || !self.identity.bytes().all(|byte| byte.is_ascii_hexdigit())
            || self.directory.file_name().and_then(|name| name.to_str())
                != Some(format!("stream-{}", self.identity).as_str())
            || !self.directory.is_absolute()
            || !(1..=MAX_PARTS).contains(&self.max_generations)
            || self.max_bytes < POLICY_BUDGET
        {
            return Err(storage("invalid automatic archive policy"));
        }
        Ok(())
    }

    fn from_medium(path: &Path) -> Result<Self, RecordError> {
        let meta = path.join("stream.json");
        if !fs::symlink_metadata(&meta).map_err(storage)?.is_file() {
            return Err(storage("archive stream witness is not a regular file"));
        }
        let mut bytes = Vec::new();
        fs::File::open(meta)
            .map_err(storage)?
            .take(POLICY_BUDGET + 1)
            .read_to_end(&mut bytes)
            .map_err(storage)?;
        if bytes.len() as u64 > POLICY_BUDGET {
            return Err(storage("archive stream witness exceeds its bound"));
        }
        let policy: Self = serde_json::from_slice(&bytes).map_err(storage)?;
        policy.validate()?;
        Ok(policy)
    }

    fn medium(&self) -> Result<(), RecordError> {
        if self.directory.canonicalize().map_err(storage)? != self.directory
            || Self::from_medium(&self.directory)? != *self
        {
            return Err(storage("archive medium identity mismatch"));
        }
        Ok(())
    }

    fn windows(&self, directory: &Path) -> Result<BTreeMap<u64, PathBuf>, RecordError> {
        let mut windows = BTreeMap::new();
        for (entry_index, item) in fs::read_dir(directory).map_err(storage)?.enumerate() {
            if entry_index >= 4096 {
                return Err(storage(
                    "archive stream directory exceeds its inventory bound",
                ));
            }
            let item = item.map_err(storage)?;
            let name = item.file_name();
            let Some(number) = name
                .to_str()
                .and_then(|name| name.strip_prefix(WINDOW_PREFIX))
            else {
                continue;
            };
            let generation = u64::from_str_radix(number, 16).map_err(storage)?;
            if number != format!("{generation:016x}")
                || !item.file_type().map_err(storage)?.is_dir()
            {
                return Err(storage("invalid archive window path"));
            }
            windows.insert(generation, item.path());
            if windows.len() > self.max_generations {
                return Err(storage("archive exceeds its generation bound"));
            }
        }
        for (offset, &generation) in windows.keys().enumerate() {
            if self.first_generation.checked_add(offset as u64) != Some(generation) {
                return Err(storage("archive generation gap"));
            }
        }
        Ok(windows)
    }

    fn prior(
        &self,
        windows: &BTreeMap<u64, PathBuf>,
        generation: u64,
        wal_budget: u64,
    ) -> Result<(u64, [u8; 32]), RecordError> {
        if generation < self.first_generation
            || windows
                .last_key_value()
                .is_some_and(|(&last, _)| last > generation)
        {
            return Err(storage(
                "archive generation is ahead of the live Record journal",
            ));
        }
        if generation == self.first_generation {
            return Ok((self.base_seq, self.base_sha256));
        }
        let previous = windows
            .get(&(generation - 1))
            .ok_or_else(|| storage("missing preceding archive generation"))?;
        let header = inspect(previous, wal_budget)?.0;
        Ok((header.last_seq, header.end_sha256))
    }

    pub(in crate::record) fn archive_prefix(
        &self,
        generation: u64,
        config: &RecordConfig,
        state: &mut State,
        source: &maintenance::SourceProof,
        gc_epoch: Option<u64>,
    ) -> Result<(), RecordError> {
        let started = std::time::Instant::now();
        let _phase = maintenance::Phase::new("local_archive_prefix", state.seq);
        self.medium()?;
        let windows = self.windows(&self.directory)?;
        let budget = config.max_journal_bytes;
        let (base_seq, base_sha256) = self.prior(&windows, generation, budget)?;
        let (first, suffix) = source
            .frames()
            .split_first()
            .ok_or_else(|| storage("empty certified WAL cut"))?;
        let mut base = if let Some(anchor) = source.anchor() {
            anchor.checkpoint_state()?.try_clone()?
        } else {
            let _decode = frame_memory(first)?;
            proof_decode::bootstrap(first)?.1
        };
        if base.seq != base_seq || boundary_digest(config, &mut base)? != base_sha256 {
            return Err(storage(
                "archive endpoint does not match the selected checkpoint",
            ));
        }
        drop(base);
        let path = self
            .directory
            .join(format!("{WINDOW_PREFIX}{generation:016x}"));
        if !path.exists() && windows.len() >= self.max_generations {
            return Err(RecordError::Busy);
        }
        let header = serde_json::to_vec(&Header {
            format: 3,
            base_seq,
            last_seq: state.seq,
            base_sha256,
            end_sha256: boundary_digest(config, state)?,
            decisions_sha256: Some(hash_frames(suffix.iter().map(Vec::as_slice))),
            gc_epoch,
        })
        .map_err(storage)?;
        // A retry can extend a window prepared by an interrupted checkpoint.
        // Before overwriting its archive certificate, prove the old decisions
        // are an exact prefix. A successor live generation freezes this slot.
        let _frames = crate::storage::build_budget::reserve_memory(budget).map_err(memory_error)?;
        let previous = if windows.contains_key(&generation) {
            let (journal, prior) = PartitionJournal::open(&path, budget).map_err(storage)?;
            let (prior_header, _) = read_window(&prior)?;
            if (prior_header.base_seq, prior_header.base_sha256) != (base_seq, base_sha256)
                || prior_header.last_seq > state.seq
                || !suffix.starts_with(&prior[1..])
            {
                return Err(storage(
                    "existing archive window conflicts with the committed prefix",
                ));
            }
            Some(journal)
        } else {
            None
        };
        let stage = self
            .directory
            .join(format!(".window-stage-{generation:016x}"));
        self.cleanup_preparations(&stage)?;
        let bytes = suffix
            .iter()
            .try_fold(header.len() as u64 + 512, |total, frame| {
                total
                    .checked_add(frame.len() as u64 + 24)
                    .ok_or_else(|| storage("archive byte budget overflow"))
            })?;
        let current = footprint(&self.directory, 4, &mut 4096)?;
        if current
            .checked_add(bytes)
            .is_none_or(|used| used > self.max_bytes)
        {
            return Err(RecordError::Busy);
        }
        if let Some(mut journal) = previous {
            journal
                .checkpoint_with_tail(&header, suffix)
                .map_err(creation_error)?;
        } else {
            publish_window(&stage, &path, budget, &header, suffix)?;
        }
        log::info!(
            "local_archive published generation={generation} base_seq={base_seq} archived_seq={} bytes_bound={bytes} elapsed_ms={}",
            state.seq,
            started.elapsed().as_millis()
        );
        Ok(())
    }

    fn cleanup_preparations(&self, stage: &Path) -> Result<(), RecordError> {
        // Only the primary owner can prepare windows in this private stream.
        // A killed initial root creation can leave a generic PartitionJournal
        // staging name before our per-generation private name even exists.
        for item in fs::read_dir(&self.directory).map_err(storage)? {
            let item = item.map_err(storage)?;
            let name = item.file_name();
            let journal_preparation = name
                .to_str()
                .and_then(|name| name.strip_prefix(".partition-prepare-"))
                .is_some_and(|id| {
                    id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit())
                });
            if item.path() != stage && !journal_preparation {
                continue;
            }
            if !item.file_type().map_err(storage)?.is_dir() {
                return Err(storage("invalid archive staging directory"));
            }
            fs::remove_dir_all(item.path()).map_err(storage)?;
        }
        Ok(())
    }
}

fn footprint(path: &Path, depth: usize, remaining: &mut usize) -> Result<u64, RecordError> {
    let metadata = fs::symlink_metadata(path).map_err(storage)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() || depth == 0 {
        return Err(storage("unexpected archive medium entry"));
    }
    let mut bytes = 0u64;
    for item in fs::read_dir(path).map_err(storage)? {
        *remaining = remaining
            .checked_sub(1)
            .ok_or_else(|| storage("archive file inventory exceeds its bound"))?;
        bytes = bytes
            .checked_add(footprint(
                &item.map_err(storage)?.path(),
                depth - 1,
                remaining,
            )?)
            .ok_or_else(|| storage("archive file inventory overflow"))?;
    }
    Ok(bytes)
}

fn inspect(path: &Path, budget: u64) -> Result<(Header, LocalArchiveWindow), RecordError> {
    let _frames = crate::storage::build_budget::reserve_memory(budget).map_err(memory_error)?;
    let (_journal, frames) = PartitionJournal::open(path, budget).map_err(storage)?;
    read_window(&frames)
}

fn enroll(
    source: &Path,
    backup: &Path,
    medium: &Path,
    budget: u64,
    max_bytes: u64,
    max_generations: usize,
    renew: bool,
) -> Result<PathBuf, RecordError> {
    let source = source.canonicalize().map_err(storage)?;
    let medium = medium.canonicalize().map_err(storage)?;
    let backup = backup.canonicalize().map_err(storage)?;
    if medium.starts_with(&source) || source.starts_with(&medium) || backup == source {
        return Err(RecordError::Rejected(
            "archive medium must be outside the source tree".into(),
        ));
    }
    let _frames = crate::storage::build_budget::reserve_memory(budget).map_err(memory_error)?;
    let (owner, frames) = PartitionJournal::open(&source, budget).map_err(storage)?;
    let old = Policy::load(&source)?;
    if old.is_some() != renew {
        return Err(RecordError::Rejected(
            "enroll requires an unused source; renew requires an existing stream".into(),
        ));
    }
    let first_generation = owner.generation();
    let (config, mut base) = decode_checkpoint(
        frames
            .first()
            .ok_or_else(|| storage("archive source lacks its checkpoint"))?,
        &source,
        budget,
    )?;
    if base.independent_snapshot {
        return Err(RecordError::Rejected(
            "archive source must be a live Record partition".into(),
        ));
    }
    let base_seq = base.seq;
    let base_sha256 = boundary_digest(&config, &mut base)?;
    drop(base);
    drop(frames);
    let (backup_config, mut snapshot, _backup_owner) = read_backup(&backup, budget)?;
    if snapshot.seq != base_seq || boundary_digest(&backup_config, &mut snapshot)? != base_sha256 {
        return Err(RecordError::Rejected(
            "backup does not match the selected WAL base".into(),
        ));
    }
    if let Some(old) = &old {
        old.medium()?;
        let windows = old.windows(&old.directory)?;
        if old.prior(&windows, first_generation, config.max_journal_bytes)?
            != (base_seq, base_sha256)
        {
            return Err(storage("cannot renew across an unarchived checkpoint"));
        }
    }
    let identity = format!("{:032x}", rand::random::<u128>());
    let directory = medium.join(format!("stream-{identity}"));
    let pending = medium.join(format!(".prepare-stream-{identity}"));
    let policy = Policy {
        directory: directory.clone(),
        identity,
        first_generation,
        base_seq,
        base_sha256,
        max_bytes,
        max_generations,
    };
    policy.validate()?;
    let raw = serde_json::to_vec(&policy).map_err(storage)?;
    PartitionJournal::validate_initial(raw.len(), POLICY_BUDGET).map_err(storage)?;
    fs::create_dir(&pending).map_err(storage)?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(pending.join("stream.json"))
        .map_err(storage)?;
    file.write_all(&raw)
        .and_then(|_| file.sync_all())
        .map_err(storage)?;
    drop(file);
    PartitionJournal::publish_prepared_root(&pending, &directory).map_err(creation_error)?;
    let selected = source.join(ARCHIVE_POLICY);
    if renew {
        let (mut journal, _) = PartitionJournal::open(&selected, POLICY_BUDGET).map_err(storage)?;
        journal.checkpoint(&raw).map_err(creation_error)?;
    } else {
        PartitionJournal::create(&selected, POLICY_BUDGET, &raw).map_err(creation_error)?;
    }
    drop(owner);
    Ok(directory)
}

impl RecordPartition {
    /// Export the selected base of a stopped live WAL. Any newer committed
    /// suffix remains in the source and belongs to the following window.
    pub async fn export_local_archive_base(
        source: &Path,
        destination: &Path,
        wal_budget: u64,
    ) -> Result<u64, RecordError> {
        let (source, destination) = (source.to_owned(), destination.to_owned());
        crate::resources::spawn(async move {
            let (_owner, config, mut state) = crate::resources::spawn_blocking(move || {
                let _memory = crate::storage::build_budget::reserve_memory(wal_budget)
                    .map_err(memory_error)?;
                let (owner, frames) =
                    PartitionJournal::open(&source, wal_budget).map_err(storage)?;
                let (config, state) = decode_checkpoint(
                    frames
                        .first()
                        .ok_or_else(|| storage("archive source lacks its checkpoint"))?,
                    &source,
                    wal_budget,
                )?;
                if state.independent_snapshot {
                    return Err(RecordError::Rejected(
                        "archive base export requires a live Record partition".into(),
                    ));
                }
                Ok::<_, RecordError>((owner, config, state))
            })
            .await
            .map_err(storage)??;
            state.independent_snapshot = true;
            state.history.scope = format!("{:032x}", rand::random::<u128>());
            let seq = state.seq;
            snapshot::create(&destination, config, state).await?;
            Ok(seq)
        })
        .await
        .map_err(storage)?
    }

    /// Enroll a stopped local Record partition, or select a new archive window
    /// after verifying the existing stream reaches the selected WAL base.
    /// The caller retains the matching independent backup on separate media.
    pub async fn enroll_local_archive(
        source: &Path,
        backup: &Path,
        medium: &Path,
        wal_budget: u64,
        max_bytes: u64,
        max_generations: usize,
        renew: bool,
    ) -> Result<PathBuf, RecordError> {
        let (source, backup, medium) = (source.to_owned(), backup.to_owned(), medium.to_owned());
        crate::resources::spawn_blocking(move || {
            enroll(
                &source,
                &backup,
                &medium,
                wal_budget,
                max_bytes,
                max_generations,
                renew,
            )
        })
        .await
        .map_err(storage)?
    }

    /// Read an ordered manifest from the certified archive slots. Save the
    /// result independently alongside the paired backup for restore-chain.
    pub async fn local_archive_manifest(
        stream: &Path,
        wal_budget: u64,
    ) -> Result<Vec<LocalArchivePart>, RecordError> {
        let stream = stream.to_owned();
        crate::resources::spawn_blocking(move || {
            let policy = Policy::from_medium(&stream)?;
            let windows = policy.windows(&stream)?;
            let mut endpoint = (policy.base_seq, policy.base_sha256);
            let mut parts = Vec::with_capacity(windows.len());
            for path in windows.into_values() {
                let (header, witness) = inspect(&path, wal_budget)?;
                if (header.base_seq, header.base_sha256) != endpoint {
                    return Err(storage("archive window boundary mismatch"));
                }
                endpoint = (header.last_seq, header.end_sha256);
                parts.push(LocalArchivePart { path, witness });
            }
            Ok(parts)
        })
        .await
        .map_err(storage)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, write};
    use std::{process::Command, time::Duration};

    async fn setup(root: &Path, max_bytes: u64) -> (PathBuf, PathBuf, PathBuf) {
        let source = root.join("live");
        let medium = root.join("medium");
        fs::create_dir(&medium).unwrap();
        let base = medium.join("base-a");
        let p = RecordPartition::create(&source, config()).await.unwrap();
        p.write(write("a", "a", "base", ExpectedVersion::Absent))
            .await
            .unwrap();
        assert_eq!(p.compact_journal().await.unwrap(), 1);
        assert_eq!(p.checkpoint(&base).await.unwrap(), 1);
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let stream = enroll(
            &source,
            &base,
            &medium,
            config().max_journal_bytes,
            max_bytes,
            2,
            false,
        )
        .unwrap();
        (source, base, stream)
    }

    fn generation(root: &Path) -> PathBuf {
        let mut paths = fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("generation-")
            });
        let path = paths.next().unwrap();
        assert!(paths.next().is_none());
        path
    }

    #[tokio::test]
    async fn retention_pins_unarchived_wal_and_aligns_concurrent_suffix() {
        let directory = tempfile::tempdir().unwrap();
        let budget = config().max_journal_bytes;
        let (root, base, stream) = setup(directory.path(), 2 * budget).await;
        let old = generation(&root);
        {
            let (mut journal, frames) = PartitionJournal::open(&root, budget).unwrap();
            assert!(matches!(
                journal.checkpoint_with_tail(&frames[0], &[]),
                Err(CreateError::NotPublished(_))
            ));
        }
        let p = RecordPartition::open(&root, budget).await.unwrap();
        let request_b = write("b", "b", "second", ExpectedVersion::Absent);
        let receipt_b = p.write(request_b.clone()).await.unwrap();

        let medium = stream.parent().unwrap();
        let offline = medium.with_file_name("offline-medium");
        fs::rename(medium, &offline).unwrap();
        assert!(matches!(
            p.compact_journal().await,
            Err(RecordError::Storage(_))
        ));
        assert_eq!(generation(&root), old);
        assert_eq!(
            p.get("b").await.unwrap().unwrap().version,
            receipt_b.version
        );
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let p = RecordPartition::open(&root, budget).await.unwrap();
        assert_eq!(generation(&root), old);
        assert_eq!(p.write(request_b.clone()).await.unwrap(), receipt_b);
        fs::rename(&offline, medium).unwrap();

        let marker = root.join(".test-indexer-pause");
        fs::write(&marker, b"hold the captured A-B cut").unwrap();
        let task = {
            let p = p.clone();
            tokio::spawn(async move { p.compact_journal().await })
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            while !root.join(".test-indexer-started").exists() {
                assert!(!task.is_finished());
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let receipt_c = p
            .write(write(
                "c",
                "c",
                "concurrent suffix",
                ExpectedVersion::Absent,
            ))
            .await
            .unwrap();
        fs::remove_file(marker).unwrap();
        assert_eq!(task.await.unwrap().unwrap(), receipt_b.seq);
        assert!(
            !old.exists(),
            "retire the old primary WAL only after durable archive publication"
        );
        assert_eq!(
            p.get("c").await.unwrap().unwrap().version,
            receipt_c.version
        );
        let parts = RecordPartition::local_archive_manifest(&stream, budget)
            .await
            .unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(
            (parts[0].witness.base_seq, parts[0].witness.last_seq),
            (1, receipt_b.seq)
        );

        assert_eq!(p.compact_journal().await.unwrap(), receipt_c.seq);
        let parts = RecordPartition::local_archive_manifest(&stream, budget)
            .await
            .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(
            (parts[1].witness.base_seq, parts[1].witness.last_seq),
            (receipt_b.seq, receipt_c.seq)
        );
        let restored_path = directory.path().join("restored-b");
        RecordPartition::restore_local_archive_chain(
            &base,
            &parts,
            &restored_path,
            budget,
            receipt_b.seq,
        )
        .await
        .unwrap();
        let restored = RecordPartition::open(&restored_path, budget).await.unwrap();
        assert!(restored.get("c").await.unwrap().is_none());
        assert_eq!(restored.write(request_b).await.unwrap(), receipt_b);
        restored.finish_service_shutdown().await.unwrap();

        let old = generation(&root);
        let seed_next = medium.join("base-c");
        p.write(write("d", "d", "after the window", ExpectedVersion::Absent))
            .await
            .unwrap();
        assert!(matches!(p.compact_journal().await, Err(RecordError::Busy)));
        assert_eq!(generation(&root), old);
        assert!(p.get("d").await.unwrap().is_some());

        // The first missing generation is a gap, even with a later complete
        // slot. Refuse both a new manifest and a restore to its earlier cut.
        let moved = medium.join("missing-first");
        fs::rename(&parts[0].path, &moved).unwrap();
        assert!(
            RecordPartition::local_archive_manifest(&stream, budget)
                .await
                .is_err()
        );
        let rejected = directory.path().join("rejected");
        assert!(
            RecordPartition::restore_local_archive_chain(
                &base,
                &parts,
                &rejected,
                budget,
                receipt_b.seq
            )
            .await
            .is_err()
        );
        assert!(!rejected.exists());
        fs::rename(&moved, &parts[0].path).unwrap();
        p.finish_service_shutdown().await.unwrap();
        drop(p);

        assert_eq!(
            RecordPartition::export_local_archive_base(&root, &seed_next, budget)
                .await
                .unwrap(),
            receipt_c.seq,
        );
        let next = enroll(&root, &seed_next, medium, budget, 2 * budget, 2, true).unwrap();
        let p = RecordPartition::open(&root, budget).await.unwrap();
        assert_eq!(p.compact_journal().await.unwrap(), receipt_c.seq + 1);
        assert!(!old.exists());
        let next_parts = RecordPartition::local_archive_manifest(&next, budget)
            .await
            .unwrap();
        assert_eq!(next_parts[0].witness.base_seq, receipt_c.seq);
        assert_eq!(
            RecordPartition::local_archive_manifest(&stream, budget)
                .await
                .unwrap()[0]
                .witness,
            parts[0].witness
        );
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn interruption_after_archive_sync_preserves_and_extends_the_live_prefix() {
        let directory = tempfile::tempdir().unwrap();
        let budget = config().max_journal_bytes;
        let (root, base, stream) = setup(directory.path(), 2 * budget).await;
        let old = generation(&root);
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "record::local_archive::automatic::tests::archive_crash_worker",
                "--ignored",
            ])
            .env("ANYSEEK_ARCHIVE_CRASH_ROOT", &root)
            .env("ANYSEEK_INDEXER_TEST_CRASH", "archive_durable")
            .output()
            .unwrap();
        assert_eq!(child.status.code(), Some(93), "{child:?}");
        assert_eq!(generation(&root), old);
        let archived = RecordPartition::local_archive_manifest(&stream, budget)
            .await
            .unwrap();
        assert_eq!(archived[0].witness.last_seq, 2);

        let p = RecordPartition::open(&root, budget).await.unwrap();
        let request_b = write(
            "crash-b",
            "b",
            "archived before crash",
            ExpectedVersion::Absent,
        );
        let receipt_b = p.write(request_b.clone()).await.unwrap();
        assert_eq!(receipt_b.seq, 2);
        p.write(write("c", "c", "after the crash", ExpectedVersion::Absent))
            .await
            .unwrap();
        assert_eq!(p.compact_journal().await.unwrap(), 3);
        assert!(!old.exists());
        let longer = RecordPartition::local_archive_manifest(&stream, budget)
            .await
            .unwrap();
        assert_eq!(
            longer.len(),
            1,
            "a retry extends the same physical generation"
        );
        assert_eq!(longer[0].witness.last_seq, 3);
        let destination = directory.path().join("restored");
        RecordPartition::restore_local_archive_chain(&base, &longer, &destination, budget, 2)
            .await
            .unwrap();
        let restored = RecordPartition::open(&destination, budget).await.unwrap();
        assert!(restored.get("c").await.unwrap().is_none());
        assert_eq!(restored.write(request_b).await.unwrap(), receipt_b);
        restored.finish_service_shutdown().await.unwrap();
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn archive_byte_quota_refuses_checkpoint_before_retiring_its_wal() {
        let directory = tempfile::tempdir().unwrap();
        let budget = config().max_journal_bytes;
        let (root, _, stream) = setup(directory.path(), POLICY_BUDGET).await;
        let old = generation(&root);
        let abandoned = stream.join(format!(".partition-prepare-{:032x}", 0));
        fs::create_dir(&abandoned).unwrap();
        fs::write(abandoned.join("partial"), b"interrupted staging").unwrap();
        let p = RecordPartition::open(&root, budget).await.unwrap();
        let body = "large ".repeat(500);
        for n in 0..6 {
            let id = format!("r-{n}");
            p.write(write(&id, &id, &body, ExpectedVersion::Absent))
                .await
                .unwrap();
        }
        assert!(matches!(p.compact_journal().await, Err(RecordError::Busy)));
        assert_eq!(generation(&root), old);
        assert!(!abandoned.exists());
        assert!(
            RecordPartition::local_archive_manifest(&stream, budget)
                .await
                .unwrap()
                .is_empty()
        );
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let p = RecordPartition::open(&root, budget).await.unwrap();
        assert_eq!(
            p.get("r-5").await.unwrap().unwrap().document.unwrap()["body"],
            body
        );
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn archive_replays_checkpoint_receipt_gc_only_when_crossing_its_cut() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("live");
        let medium = directory.path().join("medium");
        let base = medium.join("base");
        fs::create_dir(&medium).unwrap();
        let settings = config();
        let budget = settings.max_journal_bytes;
        let p = RecordPartition::create(&root, settings).await.unwrap();
        let at = unix_time_ms_i64() as u64 - 8 * 86_400_000;
        let mut old = write("a", "a", "old receipt", ExpectedVersion::Absent);
        old.operation_id.epoch = at / 86_400_000;
        {
            let mut inner = p.inner.lock().await;
            let mutation::PreparedWrite::Append { applied, bytes, .. } = mutation::prepare_at(
                &inner.state,
                &inner.config,
                inner.snapshot_bytes,
                old,
                at / 86_400_000,
                at,
            )
            .unwrap() else {
                panic!("seed write was not new")
            };
            inner
                .append_operation(&bytes, applied.receipt.seq)
                .await
                .unwrap();
            inner
                .replay_context()
                .replay(Entry::Applied(applied))
                .await
                .unwrap();
        }
        p.compact_journal().await.unwrap();
        p.checkpoint(&base).await.unwrap();
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let stream = enroll(&root, &base, &medium, budget, 2 * budget, 2, false).unwrap();
        let p = RecordPartition::open(&root, budget).await.unwrap();
        let b = p
            .write(write(
                "b",
                "b",
                "before receipt GC",
                ExpectedVersion::Absent,
            ))
            .await
            .unwrap()
            .seq;
        p.start_index_job(Duration::from_secs(15), true)
            .unwrap()
            .finish()
            .await
            .unwrap();
        assert!(
            p.inner
                .lock()
                .await
                .state
                .operations
                .iter()
                .all(|op| op.receipt.seq != 1)
        );
        let c = p
            .write(write("c", "c", "after receipt GC", ExpectedVersion::Absent))
            .await
            .unwrap()
            .seq;
        p.compact_journal().await.unwrap();
        let parts = RecordPartition::local_archive_manifest(&stream, budget)
            .await
            .unwrap();
        assert!(
            inspect(&parts[0].path, budget)
                .unwrap()
                .0
                .gc_epoch
                .is_some()
        );
        for (cut, keeps_old) in [(b, true), (c, false)] {
            let destination = directory.path().join(cut.to_string());
            RecordPartition::restore_local_archive_chain(&base, &parts, &destination, budget, cut)
                .await
                .unwrap();
            let restored = RecordPartition::open(&destination, budget).await.unwrap();
            assert_eq!(
                restored
                    .inner
                    .lock()
                    .await
                    .state
                    .operations
                    .iter()
                    .any(|op| op.receipt.seq == 1),
                keeps_old
            );
            assert!(restored.get("a").await.unwrap().is_some());
            restored.finish_service_shutdown().await.unwrap();
        }
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn archive_crash_worker() {
        let root = PathBuf::from(std::env::var_os("ANYSEEK_ARCHIVE_CRASH_ROOT").unwrap());
        let p = RecordPartition::open(&root, config().max_journal_bytes)
            .await
            .unwrap();
        p.write(write(
            "crash-b",
            "b",
            "archived before crash",
            ExpectedVersion::Absent,
        ))
        .await
        .unwrap();
        p.compact_journal().await.unwrap();
        panic!("archive checkpoint unexpectedly returned after selected crash boundary");
    }
}
