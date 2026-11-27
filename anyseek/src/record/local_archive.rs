//! Offline replay from a fixed backup and its certified local journal suffix.
use super::*;
use sha2::{Digest, Sha256};

mod automatic;
pub(super) use automatic::Policy;

/// The caller retains this witness independently alongside the selected backup.
/// The digest binds the archive header and every length-framed committed entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalArchiveWindow {
    /// Business sequence of the paired base backup.
    pub base_seq: u64,
    /// Highest business sequence certified by the captured journal.
    pub last_seq: u64,
    /// SHA-256 of the complete window, including metadata-only entries.
    pub archive_sha256: [u8; 32],
}

/// An ordered archive part and the witness saved at capture time.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalArchivePart {
    /// Path of the certified retained journal window.
    pub path: PathBuf,
    /// Independently retained identity of that complete window.
    pub witness: LocalArchiveWindow,
}

const MAX_PARTS: usize = 64;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format: u32,
    base_seq: u64,
    last_seq: u64,
    base_sha256: [u8; 32],
    end_sha256: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    decisions_sha256: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    gc_epoch: Option<u64>,
}

fn read_window(frames: &[Vec<u8>]) -> Result<(Header, LocalArchiveWindow), RecordError> {
    let (raw, suffix) = frames.split_first().ok_or_else(|| storage("archive has no header"))?;
    if raw.len() > 4096 {
        return Err(storage("archive header exceeds its bound"));
    }
    let header: Header = serde_json::from_slice(raw).map_err(storage)?;
    match header.format {
        2 if header.decisions_sha256.is_none() && header.gc_epoch.is_none() => {}
        3 if header.decisions_sha256 == Some(hash_frames(suffix.iter().map(Vec::as_slice))) => {}
        _ => return Err(storage("unsupported or damaged archive window")),
    }
    if header.last_seq < header.base_seq {
        return Err(storage("archive window sequence regression"));
    }
    let witness = LocalArchiveWindow {
        base_seq: header.base_seq,
        last_seq: header.last_seq,
        archive_sha256: hash_frames(frames.iter().map(Vec::as_slice)),
    };
    Ok((header, witness))
}

impl RecordPartition {
    /// Export the retained decision suffix of a stopped local partition. The
    /// paired backup must match the selected source generation's initial cut.
    /// A journal reset after that backup makes the older interval unavailable.
    /// The result must be retained with the matching backup for targeted restore.
    pub async fn capture_local_archive(
        source: &Path,
        backup: &Path,
        archive: &Path,
        budget: u64,
    ) -> Result<LocalArchiveWindow, RecordError> {
        let (source, backup, archive) = (source.to_owned(), backup.to_owned(), archive.to_owned());
        crate::resources::spawn_blocking(move || capture(&source, &backup, &archive, budget))
            .await
            .map_err(storage)?
    }

    /// Restore an inclusive business-sequence cut to a new local partition.
    /// The cut ends at its decisive operation. Later maintenance at the same
    /// sequence is excluded. The base backup is used unchanged at its own cut.
    /// The complete archived window must match the independently saved witness.
    pub async fn restore_local_archive(
        backup: &Path,
        archive: &Path,
        destination: &Path,
        budget: u64,
        cut: u64,
        witness: &LocalArchiveWindow,
    ) -> Result<(), RecordError> {
        let archive = LocalArchivePart {
            path: archive.into(),
            witness: witness.clone(),
        };
        Self::restore_local_archive_chain(backup, &[archive], destination, budget, cut).await
    }

    /// Restore from a complete, ordered list of retained local windows. Check
    /// every window and its boundary even after finding the requested cut.
    /// A gap, wrong base, missing part, or mismatched endpoint fails before
    /// publication. At a boundary the decisive operation is the cutoff.
    pub async fn restore_local_archive_chain(
        backup: &Path,
        parts: &[LocalArchivePart],
        destination: &Path,
        budget: u64,
        cut: u64,
    ) -> Result<(), RecordError> {
        if !(1..=MAX_PARTS).contains(&parts.len()) {
            return Err(RecordError::Rejected(
                "local archive part count outside 1..=64".into(),
            ));
        }
        let (backup, destination, parts) =
            (backup.to_owned(), destination.to_owned(), parts.to_vec());
        crate::resources::spawn(async move {
            let (config, mut state) =
                crate::resources::spawn_blocking(move || recover(&backup, &parts, budget, cut))
                    .await
                    .map_err(storage)??;
            state.history.scope = format!("{:032x}", rand::random::<u128>());
            snapshot::create(&destination, config, state).await
        })
        .await
        .map_err(storage)?
    }
}

fn decode_checkpoint(
    frame: &[u8],
    root: &Path,
    budget: u64,
) -> Result<(RecordConfig, State), RecordError> {
    let _decode = frame_memory(frame)?;
    let (config, mut state) = match serde_json::from_slice(frame).map_err(storage)? {
        Entry::Anchor(reference) => {
            let (config, state, _config) = reference.archive_state(root, budget)?;
            (config, state)
        }
        Entry::Checkpoint { config, state } => (config, state),
        _ => return Err(storage("archive source lacks an initial checkpoint")),
    };
    restore_operation_lookup(&mut state)?;
    state.admit_bytes(checkpoint_codec::json_length(&state)?)?;
    validate_restored_state(&config, &state)?;
    Ok((config, state))
}

fn read_backup(
    backup: &Path,
    budget: u64,
) -> Result<(RecordConfig, State, PartitionJournal), RecordError> {
    let _frames = crate::storage::build_budget::reserve_memory(budget).map_err(memory_error)?;
    let (journal, frames) = PartitionJournal::open(backup, budget).map_err(storage)?;
    let [frame] = frames.as_slice() else {
        return Err(RecordError::Rejected(
            "archive base must be a fixed backup".into(),
        ));
    };
    let (config, state) = decode_checkpoint(frame, backup, budget)?;
    if !state.independent_snapshot {
        return Err(RecordError::Rejected(
            "archive base must be an independent snapshot".into(),
        ));
    }
    Ok((config, state, journal))
}

fn frame_memory(frame: &[u8]) -> Result<crate::storage::build_budget::MemoryLease, RecordError> {
    let estimate = (frame.len() as u64)
        .checked_mul(32)
        .ok_or_else(|| storage("archive decode budget overflow"))?;
    crate::storage::build_budget::reserve_memory(estimate).map_err(memory_error)
}

fn hash_frames<'a>(frames: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut hash = Sha256::new();
    for frame in frames {
        hash.update((frame.len() as u64).to_le_bytes());
        hash.update(frame);
    }
    hash.finalize().into()
}

fn replay(config: &RecordConfig, state: &mut State, frame: &[u8]) -> Result<(), RecordError> {
    let _decode = frame_memory(frame)?;
    let entry = proof_decode::mutation(frame)?;
    if !matches!(entry, Entry::HistoryTrim(_)) && state.seq == u64::MAX {
        return Err(storage("archive business sequence overflow"));
    }
    let state_bytes = state.admit_replay_frame(frame.len())?;
    match entry {
        Entry::Applied(op) => apply(state, &op),
        Entry::GroupApplied(decision) => group_plan::apply(state, decision, state_bytes),
        Entry::Expired(event) => temporal::apply(state, &event),
        Entry::HistoryTrim(trim) => history::apply_trim(state, &config.history, &trim),
        _ => Err(storage("archive suffix contains a new checkpoint")),
    }
}

// Independent backup export changes only the history namespace. Canonicalize
// that namespace and the backup flag without dropping receipts or GC state.
fn boundary_digest(config: &RecordConfig, state: &mut State) -> Result<[u8; 32], RecordError> {
    let scope = std::mem::take(&mut state.history.scope);
    let independent = std::mem::replace(&mut state.independent_snapshot, false);
    let result = checkpoint_codec::json_digest(&(config, &*state));
    state.history.scope = scope;
    state.independent_snapshot = independent;
    result
}

fn capture(
    source: &Path,
    backup: &Path,
    archive: &Path,
    budget: u64,
) -> Result<LocalArchiveWindow, RecordError> {
    // Own the source and base through certification. Open scans their persisted
    // witnesses and never guesses a cut from a caller-supplied sequence.
    let (backup_config, mut backup_state, _backup_owner) = read_backup(backup, budget)?;
    let _frames = crate::storage::build_budget::reserve_memory(budget).map_err(memory_error)?;
    let (_source_owner, source_frames) = PartitionJournal::open(source, budget).map_err(storage)?;
    let (base, suffix) = source_frames
        .split_first()
        .ok_or_else(|| storage("archive source has no frames"))?;
    let (config, mut state) = decode_checkpoint(base, source, budget)?;
    let base_seq = state.seq;
    let base_sha256 = boundary_digest(&config, &mut state)?;
    if base_seq != backup_state.seq
        || base_sha256 != boundary_digest(&backup_config, &mut backup_state)?
    {
        return Err(RecordError::Rejected(
            "backup does not match the retained journal base cut".into(),
        ));
    }
    drop(backup_state);
    drop(backup_config);
    for frame in suffix {
        replay(&config, &mut state, frame)?;
    }
    validate_restored_state(&config, &state)?;
    let last_seq = state.seq;
    let header = serde_json::to_vec(&Header {
        format: 2,
        base_seq,
        last_seq,
        base_sha256,
        end_sha256: boundary_digest(&config, &mut state)?,
        decisions_sha256: None,
        gc_epoch: None,
    })
    .map_err(storage)?;
    let witness = LocalArchiveWindow {
        base_seq,
        last_seq,
        archive_sha256: hash_frames(
            std::iter::once(header.as_slice()).chain(suffix.iter().map(Vec::as_slice)),
        ),
    };
    let parent = archive
        .parent()
        .ok_or_else(|| storage("archive needs a parent directory"))?;
    let prepared = parent.join(format!(".record-archive-{:032x}", rand::random::<u128>()));
    publish_window(&prepared, archive, budget, &header, suffix)?;
    Ok(witness)
}

fn publish_window(
    prepared: &Path, archive: &Path, budget: u64, header: &[u8], suffix: &[Vec<u8>],
) -> Result<(), RecordError> {
    let mut journal =
        PartitionJournal::create(prepared, budget, header).map_err(creation_error)?;
    let written = if suffix.is_empty() { Ok(()) } else {
        journal.append_batch(&suffix.iter().map(Vec::as_slice).collect::<Vec<_>>()).map(|_| ())
    };
    drop(journal);
    if let Err(error) = written {
        discard(prepared);
        return Err(storage(error));
    }
    match PartitionJournal::publish_prepared_root(prepared, archive) {
        Ok(()) => Ok(()),
        Err(error) => {
            if matches!(error, CreateError::NotPublished(_)) {
                discard(prepared);
            }
            Err(creation_error(error))
        }
    }
}

fn discard(path: &Path) {
    if let Err(error) = std::fs::remove_dir_all(path) {
        log::warn!("private archive preparation cleanup pending: {error}");
    }
}

fn recover(
    backup: &Path,
    parts: &[LocalArchivePart],
    budget: u64,
    cut: u64,
) -> Result<(RecordConfig, State), RecordError> {
    let (config, mut state, _backup_owner) = read_backup(backup, budget)?;
    if cut < state.seq {
        return Err(RecordError::Rejected(
            "requested cut precedes archive base".into(),
        ));
    }
    let (mut endpoint, mut digest) = (state.seq, boundary_digest(&config, &mut state)?);
    for LocalArchivePart { path, witness } in parts {
        // Bound transient memory to one retained generation. Fully authenticate
        // its bytes even if the requested cut is in an earlier part.
        let _frames = crate::storage::build_budget::reserve_memory(budget).map_err(memory_error)?;
        let (_archive_owner, frames) = PartitionJournal::open(path, budget).map_err(storage)?;
        let (header, actual) = read_window(&frames)?;
        if actual != *witness {
            return Err(storage("archive window differs from its retained witness"));
        }
        if header.base_seq != endpoint
            || header.base_sha256 != digest
        {
            return Err(storage(
                "archive windows do not form a continuous chain from the supplied backup",
            ));
        }
        if state.seq != cut {
            for frame in &frames[1..] {
                replay(&config, &mut state, frame)?;
                if state.seq == cut {
                    break;
                }
            }
            if state.seq != cut && let Some(epoch) = header.gc_epoch {
                maintenance::collect_operations(&mut state, epoch)?;
            }
            if state.seq != cut
                && (state.seq != header.last_seq
                    || boundary_digest(&config, &mut state)? != header.end_sha256)
            {
                return Err(storage(
                    "archive replay differs from its certified endpoint",
                ));
            }
        }
        (endpoint, digest) = (header.last_seq, header.end_sha256);
    }
    if state.seq != cut {
        return Err(RecordError::Rejected(
            "requested cut outside the archived chain".into(),
        ));
    }
    validate_restored_state(&config, &state)?;
    Ok((config, state))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{config, write};
    use super::*;

    #[tokio::test]
    async fn archive_cut_keeps_atomic_group_and_receipt_before_withdrawal() {
        let tmp = tempfile::tempdir().unwrap();
        let live = tmp.path().join("live");
        let backup = tmp.path().join("base");
        let archive = tmp.path().join("window");
        let restored = tmp.path().join("restored");
        let budget = config().max_journal_bytes;
        let partition = RecordPartition::create(&live, config()).await.unwrap();
        partition
            .write(write("seed", "seed", "alpha base", ExpectedVersion::Absent))
            .await
            .unwrap();
        let a = partition.compact_journal().await.unwrap();
        assert_eq!(partition.checkpoint(&backup).await.unwrap(), a);
        let group = GroupReplace {
            operation_id: OperationId::new("save".into()),
            group_id: "turn".into(),
            expected_group_version: 0,
            records: ["body", "projection"]
                .into_iter()
                .map(|id| GroupRecord {
                    record_id: id.into(),
                    document: write("unused", id, "alpha paired", ExpectedVersion::Absent)
                        .document
                        .unwrap(),
                    temporal: None,
                })
                .collect(),
        };
        let b = partition.replace_group(group.clone()).await.unwrap();
        let body = partition.get("body").await.unwrap().unwrap();
        let projection = partition.get("projection").await.unwrap().unwrap();
        let c = partition
            .replace_group(GroupReplace {
                operation_id: OperationId::new("withdraw".into()),
                group_id: group.group_id.clone(),
                expected_group_version: b.group_version,
                records: vec![],
            })
            .await
            .unwrap();
        assert!(partition.get("body").await.unwrap().is_none());
        partition.finish_service_shutdown().await.unwrap();
        drop(partition);

        let captured = RecordPartition::capture_local_archive(&live, &backup, &archive, budget)
            .await
            .unwrap();
        assert_eq!((captured.base_seq, captured.last_seq), (a, c.seq));
        let mut bad_hash = captured.clone();
        bad_hash.archive_sha256[0] ^= 1;
        assert!(
            RecordPartition::restore_local_archive(
                &backup, &archive, &restored, budget, b.seq, &bad_hash
            )
            .await
            .is_err()
        );
        assert!(
            RecordPartition::restore_local_archive(
                &backup,
                &archive,
                &restored,
                budget,
                c.seq + 1,
                &captured
            )
            .await
            .is_err()
        );
        assert!(!restored.exists());
        RecordPartition::restore_local_archive(
            &backup, &archive, &restored, budget, b.seq, &captured,
        )
        .await
        .unwrap();
        let partition = RecordPartition::open(&restored, budget).await.unwrap();
        assert_eq!(partition.statistics().await.unwrap().visible_seq, b.seq);
        assert_eq!(
            partition.group_version("turn").await.unwrap(),
            Some(b.group_version)
        );
        assert_eq!(partition.replace_group(group).await.unwrap(), b);
        for before in [body, projection] {
            let after = partition.get(&before.record_id).await.unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(after).unwrap(),
                serde_json::to_value(before).unwrap()
            );
        }
        partition.finish_service_shutdown().await.unwrap();
    }
}
