//! Segmented append-only intake log. Rolling adds a small directory reference;
//! it does not construct a Record checkpoint or touch a search index.
use super::*;
use crate::storage::journal::Journal;
use std::fs;

const FORMAT: u32 = 1;
const CONTROL_BYTES: u64 = 1024 * 1024;
#[derive(Serialize, Deserialize)]
struct Header {
    format: u32,
    identity: String,
}
#[derive(Serialize, Deserialize)]
struct Segment {
    id: u64,
    first_event: u64,
}
#[derive(Serialize, Deserialize)]
struct Envelope<T> {
    event: u64,
    value: T,
}

pub(super) struct Log {
    control: Journal,
    // Root control remains the stable owner across generation swaps.
    base_control: Option<Journal>,
    selection_root: PathBuf,
    identity: String,
    active: Journal,
    root: PathBuf,
    segment: u64,
    event: u64,
    segment_bytes: u64,
    max_bytes: u64,
    retained_bytes: u64,
    failed: bool,
}
fn failure(error: CreateError) -> RecordError {
    match error {
        CreateError::NotPublished(e) => storage(e),
        CreateError::PublicationUnknown(e) => RecordError::OutcomeUnknown(e.to_string()),
    }
}
impl Log {
    pub(super) fn create(
        root: &Path,
        identity: &str,
        segment_bytes: u64,
        max_bytes: u64,
    ) -> Result<Self, RecordError> {
        if root.exists() {
            return Err(storage("intake log already exists"));
        }
        let parent = root
            .parent()
            .ok_or_else(|| storage("intake log needs parent"))?;
        let stage = parent.join(format!(".intake-prepare-{:032x}", rand::random::<u128>()));
        fs::create_dir(&stage).map_err(storage)?;
        let mut preparation = Preparation { root: stage.clone(), selector: stage.join("unused-selector"), armed: true };
        let header = serde_json::to_vec(&Header {
            format: FORMAT,
            identity: identity.into(),
        })
        .map_err(storage)?;
        let segment = serde_json::to_vec(&Segment {
            id: 0,
            first_event: 1,
        })
        .map_err(storage)?;
        drop(
            Journal::create_initialized(
                &stage.join("segment-0000000000000000"),
                segment_bytes,
                Some(&segment),
            )
            .map_err(failure)?,
        );
        drop(
            Journal::create_initialized_frames(
                &stage.join("control"),
                CONTROL_BYTES,
                &[&header, &segment],
            )
            .map_err(failure)?,
        );
        PartitionJournal::publish_prepared_root(&stage, root).map_err(failure)?;
        preparation.armed = false;
        Self::open::<serde_json::Value>(root, identity, segment_bytes, max_bytes)
            .map(|(log, _)| log)
    }
    pub(super) fn open<T: for<'de> Deserialize<'de>>(
        root: &Path,
        identity: &str,
        segment_bytes: u64,
        max_bytes: u64,
    ) -> Result<(Self, Vec<T>), RecordError> {
        // The stable control journal lock excludes a second owner throughout a roll.
        let (control, frames) =
            Journal::open(&root.join("control"), CONTROL_BYTES).map_err(storage)?;
        let selector = root.join("CURRENT");
        if selector.exists() {
            let name = fs::read_to_string(&selector).map_err(storage)?;
            if !name.strip_prefix("generation-").is_some_and(|s|
                s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit())) {
                return Err(storage("invalid intake generation selector"));
            }
            let selected_root = root.join(&name);
            if !fs::symlink_metadata(&selected_root).map_err(storage)?.file_type().is_dir()
                || selected_root.join("CURRENT").exists() {
                return Err(storage("nested or invalid intake generation"));
            }
            let (mut selected, values) = Self::open::<T>(
                &selected_root, identity, segment_bytes, max_bytes)?;
            if selected.base_control.is_some() {
                return Err(storage("nested intake generation selector"));
            }
            selected.base_control = Some(control);
            selected.selection_root = root.into();
            if selected.bytes() > max_bytes { return Err(RecordError::Busy); }
            selected.cleanup_generations()?;
            return Ok((selected, values));
        }
        let header: Header = serde_json::from_slice(
            frames
                .first()
                .ok_or_else(|| storage("missing intake header"))?,
        )
        .map_err(storage)?;
        if header.format != FORMAT || header.identity != identity {
            return Err(storage("intake format/configuration mismatch"));
        }
        let segments: Vec<Segment> = frames
            .iter()
            .skip(1)
            .map(|b| serde_json::from_slice(b).map_err(storage))
            .collect::<Result<_, _>>()?;
        if segments.is_empty() {
            return Err(storage("missing intake segment"));
        }
        let mut values = Vec::new();
        let mut event = 0u64;
        let mut retained_bytes = control.physical_bytes();
        let mut active = None;
        for (ordinal, segment) in segments.iter().enumerate() {
            if segment.id != ordinal as u64
                || segment.first_event
                    != event
                        .checked_add(1)
                        .ok_or_else(|| storage("intake sequence overflow"))?
            {
                return Err(storage("missing or unordered intake segment"));
            }
            let path = root.join(format!("segment-{:016x}", segment.id));
            let (journal, entries) = Journal::open(&path, segment_bytes).map_err(storage)?;
            let actual: Segment = serde_json::from_slice(
                entries
                    .first()
                    .ok_or_else(|| storage("missing segment header"))?,
            )
            .map_err(storage)?;
            if actual.id != segment.id || actual.first_event != segment.first_event {
                return Err(storage("intake segment identity mismatch"));
            }
            retained_bytes = retained_bytes
                .checked_add(journal.physical_bytes())
                .filter(|n| *n <= max_bytes)
                .ok_or(RecordError::Busy)?;
            for bytes in entries.iter().skip(1) {
                let entry: Envelope<T> = serde_json::from_slice(bytes).map_err(storage)?;
                event = event
                    .checked_add(1)
                    .ok_or_else(|| storage("intake sequence overflow"))?;
                if entry.event != event {
                    return Err(storage("intake event sequence gap"));
                }
                values.push(entry.value);
            }
            if ordinal + 1 == segments.len() {
                active = Some(journal);
            }
        }
        // Only control-selected segments can contain acknowledged commands.
        // Remove unselected preparations under stable ownership before reuse.
        for row in fs::read_dir(root).map_err(storage)? {
            let row = row.map_err(storage)?;
            let name = row.file_name().to_string_lossy().into_owned();
            if let Some(id) = name
                .strip_prefix("segment-")
                .filter(|s| s.len() == 16)
                .and_then(|s| u64::from_str_radix(s, 16).ok())
            {
                if id >= segments.len() as u64 {
                    fs::remove_dir_all(row.path()).map_err(storage)?;
                }
            } else if name
                .strip_prefix(".prepare-")
                .is_some_and(|s| s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                if !row.file_type().map_err(storage)?.is_dir() {
                    return Err(storage("intake preparation is not a directory"));
                }
                fs::remove_dir_all(row.path()).map_err(storage)?;
            }
        }
        Ok((
            Self {
                control,
                base_control: None,
                selection_root: root.into(),
                identity: identity.into(),
                active: active.unwrap(),
                root: root.into(),
                segment: segments.len() as u64 - 1,
                event,
                segment_bytes,
                max_bytes,
                retained_bytes,
                failed: false,
            },
            values,
        ))
    }
    /// Publish a replacement event chain before reclaiming any selected file.
    /// CURRENT is replaced atomically while the original control owner stays held.
    pub(super) fn compact(&mut self, events: &[Vec<u8>], completion_bytes: u64) -> Result<(), RecordError> {
        if self.failed { return Err(RecordError::RecoveryRequired); }
        let name = format!("generation-{:032x}", rand::random::<u128>());
        let root = self.selection_root.join(&name);
        // One selected chain plus one bounded candidate; no unbounded retries.
        let estimate = events.iter().try_fold(1024u64, |sum, e| sum.checked_add(e.len() as u64 + 1024))
            .filter(|n| *n <= self.max_bytes).ok_or(RecordError::Busy)?;
        let _peak = self.bytes().checked_add(estimate).filter(|n| *n <= self.max_bytes.saturating_mul(2))
            .ok_or(RecordError::Busy)?;
        self.cleanup_generations()?;
        let temporary = self.selection_root.join(format!(".selector-{:032x}", rand::random::<u128>()));
        let mut preparation = Preparation { root: root.clone(), selector: temporary.clone(), armed: true };
        let mut candidate = Self::create(&root, &self.identity, self.segment_bytes, self.max_bytes)?;
        for event in events { candidate.append_serialized(event)?; }
        let owner_bytes = self.base_control.as_ref().unwrap_or(&self.control).physical_bytes();
        if candidate.bytes().checked_add(owner_bytes).and_then(|n| n.checked_add(completion_bytes)).is_none_or(|n| n > self.max_bytes) {
            return Err(RecordError::Busy);
        }
        #[cfg(test)] if self.selection_root.join(".test-intake-before-select").exists() {
            return Err(storage("injected intake failure before selection"));
        }
        use std::io::Write;
        let mut file = fs::OpenOptions::new().write(true).create_new(true).open(&temporary).map_err(storage)?;
        file.write_all(name.as_bytes()).map_err(storage)?;
        file.sync_all().map_err(storage)?;
        drop(file);
        // From rename onwards the selected chain is uncertain on any error.
        self.failed = true;
        preparation.armed = false;
        replace_selector(&temporary, &self.selection_root.join("CURRENT"))
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
        #[cfg(test)] if self.selection_root.join(".test-intake-after-select").exists() {
            return Err(RecordError::OutcomeUnknown("injected intake failure after selection".into()));
        }
        sync_directory(&self.selection_root)
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
        candidate.selection_root = self.selection_root.clone();
        let old = std::mem::replace(self, candidate);
        self.base_control = old.base_control.or(Some(old.control));
        drop(old.active);
        // Cleanup failure leaks obsolete files, never changes the committed chain.
        let _ = self.cleanup_generations();
        Ok(())
    }
    fn cleanup_generations(&self) -> Result<(), RecordError> {
        let _phase = maintenance::Phase::new("intake_local_cleanup", self.event);
        for row in fs::read_dir(&self.selection_root).map_err(storage)? {
            let row = row.map_err(storage)?;
            let name = row.file_name().to_string_lossy().into_owned();
            let generation = name.strip_prefix("generation-").is_some_and(|s|
                s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()));
            let original_segment = self.base_control.is_some() && name.strip_prefix("segment-").is_some_and(|s|
                s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit()));
            let selector = name.strip_prefix(".selector-").is_some_and(|s|
                s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()));
            if selector { fs::remove_file(row.path()).map_err(storage)?; }
            else if (generation && row.path() != self.root) || original_segment {
                fs::remove_dir_all(row.path()).map_err(storage)?;
            }
        }
        sync_directory(&self.selection_root).map_err(storage)
    }
    pub(super) fn bytes(&self) -> u64 {
        self.retained_bytes + self.base_control.as_ref().map_or(0, Journal::physical_bytes)
    }
    pub(super) fn available(&self) -> u64 {
        self.max_bytes.saturating_sub(self.bytes())
    }
    pub(super) fn append<T: Serialize>(&mut self, value: &T) -> Result<(), RecordError> {
        self.append_serialized(&serde_json::to_vec(value).map_err(storage)?)
    }
    pub(super) fn append_serialized(&mut self, value: &[u8]) -> Result<(), RecordError> {
        if self.failed {
            return Err(RecordError::RecoveryRequired);
        }
        let event = self
            .event
            .checked_add(1)
            .ok_or_else(|| storage("intake sequence exhausted"))?;
        let mut bytes = format!("{{\"event\":{event},\"value\":").into_bytes();
        bytes.extend_from_slice(value);
        bytes.push(b'}');
        if bytes.len() as u64 + 256 > self.segment_bytes {
            return Err(RecordError::Rejected(
                "intake batch exceeds WAL segment budget".into(),
            ));
        }
        let roll = self.active.validate_append(bytes.len()).is_err();
        let overhead = if roll { 512 } else { 0 };
        if self
            .bytes()
            .checked_add(bytes.len() as u64 + 24 + overhead)
            .is_none_or(|n| n > self.max_bytes)
        {
            return Err(RecordError::Busy);
        }
        if roll {
            let next = self
                .segment
                .checked_add(1)
                .ok_or_else(|| storage("intake segment sequence exhausted"))?;
            let header = serde_json::to_vec(&Segment {
                id: next,
                first_event: event,
            })
            .map_err(storage)?;
            self.failed = true;
            let candidate = Journal::create_initialized(
                &self.root.join(format!("segment-{next:016x}")),
                self.segment_bytes,
                Some(&header),
            )
            .map_err(failure)?;
            let before = self.control.physical_bytes();
            self.control
                .append(&header)
                .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
            self.retained_bytes +=
                self.control.physical_bytes() - before + candidate.physical_bytes();
            self.active = candidate;
            self.segment = next;
            self.failed = false;
        }
        self.failed = true;
        let before = self.active.physical_bytes();
        self.active
            .append(&bytes)
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
        self.retained_bytes += self.active.physical_bytes() - before;
        self.event = event;
        self.failed = false;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compaction_preserves_pending_completion_byte_headroom() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("intake");
        let mut log = Log::create(&root, "config", 8192, 16384).unwrap();
        log.append(&"pending").unwrap();
        let before = log.bytes();
        let completion_bytes = log.available() - 1024;
        // A valid alternate JSON encoding of the same cut, larger than the
        // selected chain but still fitting the standalone generation budget.
        let snapshot = format!("{}\"pending\"", " ".repeat(4096)).into_bytes();
        assert!(snapshot.len() as u64 + 2048 < log.max_bytes);
        assert!(matches!(log.compact(&[snapshot], completion_bytes), Err(RecordError::Busy)));
        assert_eq!(log.bytes(), before);
        assert!(!root.join("CURRENT").exists());
        assert!(!fs::read_dir(&root).unwrap().filter_map(Result::ok)
            .any(|row| row.file_name().to_string_lossy().starts_with("generation-")));
        // Rejected maintenance must leave the original append chain usable.
        log.append(&"completed").unwrap();
        drop(log);
        let (_, values) = Log::open::<String>(&root, "config", 8192, 16384).unwrap();
        assert_eq!(values, vec!["pending".to_owned(), "completed".to_owned()]);
    }
    #[test]
    fn compaction_keeps_stable_owner_and_only_selected_snapshot() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("intake");
        let mut log = Log::create(&root, "config", 1024, 16384).unwrap();
        for n in 0..12u64 { log.append(&(n, "x".repeat(180))).unwrap(); }
        let before = log.bytes();
        log.compact(&[serde_json::to_vec(&(12u64, "snapshot".to_owned())).unwrap()], 0).unwrap();
        assert!(log.bytes() < before);
        assert!(Log::open::<(u64, String)>(&root, "config", 1024, 16384).is_err());
        log.append(&(13u64, "next".to_owned())).unwrap();
        log.compact(&[serde_json::to_vec(&(14u64, "second".to_owned())).unwrap()], 0).unwrap();
        drop(log);
        let (_, values) = Log::open::<(u64, String)>(&root, "config", 1024, 16384).unwrap();
        assert_eq!(values, vec![(14, "second".to_owned())]);
        assert_eq!(fs::read_dir(&root).unwrap().filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("generation-")).count(), 1);
    }
    #[test]
    fn compaction_failure_before_and_after_selection_recovers_exact_chain() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("intake");
        let mut log = Log::create(&root, "config", 1024, 16384).unwrap();
        log.append(&1u64).unwrap();
        fs::write(root.join(".test-intake-before-select"), b"").unwrap();
        for _ in 0..3 { assert!(log.compact(&[b"2".to_vec()], 0).is_err()); }
        assert!(!root.join("CURRENT").exists());
        assert_eq!(fs::read_dir(&root).unwrap().filter_map(Result::ok)
            .filter(|e| e.file_name().to_string_lossy().starts_with("generation-")).count(), 0);
        fs::remove_file(root.join(".test-intake-before-select")).unwrap();
        log.append(&3u64).unwrap();
        fs::write(root.join(".test-intake-after-select"), b"").unwrap();
        assert!(matches!(log.compact(&[b"4".to_vec()], 0), Err(RecordError::OutcomeUnknown(_))));
        assert!(matches!(log.append(&5u64), Err(RecordError::RecoveryRequired)));
        drop(log);
        let (log, values) = Log::open::<u64>(&root, "config", 1024, 16384).unwrap();
        assert_eq!(values, vec![4]);
        drop(log);
        let selected = fs::read_to_string(root.join("CURRENT")).unwrap();
        fs::write(root.join(&selected).join("CURRENT"), &selected).unwrap();
        assert!(Log::open::<u64>(&root, "config", 1024, 16384).is_err());
        fs::remove_file(root.join(&selected).join("CURRENT")).unwrap();
        fs::remove_dir_all(root.join(selected)).unwrap();
        assert!(Log::open::<u64>(&root, "config", 1024, 16384).is_err());
    }
    #[test]
    fn rolls_without_checkpoint_and_reopens_the_complete_chain() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("intake");
        let mut log = Log::create(&root, "config", 1024, 16384).unwrap();
        for i in 0..12 {
            log.append(&(i, "x".repeat(180))).unwrap();
        }
        assert!(log.segment > 0);
        drop(log);
        let (mut log, values) = Log::open::<(usize, String)>(&root, "config", 1024, 16384).unwrap();
        assert_eq!(
            values,
            (0..12).map(|i| (i, "x".repeat(180))).collect::<Vec<_>>()
        );
        log.append(&(12, "last".to_owned())).unwrap();
        drop(log);
        assert_eq!(
            Log::open::<(usize, String)>(&root, "config", 1024, 16384)
                .unwrap()
                .1
                .len(),
            13
        );
        assert!(Log::open::<(usize, String)>(&root, "wrong", 1024, 16384).is_err());
    }
    #[test]
    fn missing_selected_segment_fails_and_unselected_successor_is_not_replayed() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("intake");
        let mut log = Log::create(&root, "config", 1024, 16384).unwrap();
        log.append(&42u64).unwrap();
        drop(log);
        let header = serde_json::to_vec(&Segment {
            id: 1,
            first_event: 2,
        })
        .unwrap();
        drop(
            Journal::create_initialized(
                &root.join("segment-0000000000000001"),
                1024,
                Some(&header),
            )
            .unwrap(),
        );
        let (log, values) = Log::open::<u64>(&root, "config", 1024, 16384).unwrap();
        assert_eq!(values, vec![42]);
        assert!(!root.join("segment-0000000000000001").exists());
        drop(log);
        fs::remove_dir_all(root.join("segment-0000000000000000")).unwrap();
        assert!(Log::open::<u64>(&root, "config", 1024, 16384).is_err());
    }
    #[test]
    fn budget_refusal_does_not_publish_another_command() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("intake");
        let mut log = Log::create(&root, "config", 1024, 1200).unwrap();
        let mut accepted = 0;
        loop {
            match log.append(&"x".repeat(180)) {
                Ok(()) => accepted += 1,
                Err(RecordError::Busy) => break,
                other => panic!("{other:?}"),
            }
        }
        drop(log);
        let (_, values) = Log::open::<String>(&root, "config", 1024, 1200).unwrap();
        assert_eq!(values.len(), accepted);
    }
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> { fs::File::open(path)?.sync_all() }
#[cfg(not(unix))]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    let _ = path;
    Ok(())
}

#[cfg(unix)]
fn replace_selector(source: &Path, target: &Path) -> std::io::Result<()> { fs::rename(source, target) }
#[cfg(windows)]
fn replace_selector(source: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use winapi::um::winbase::{MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH};
    let wide = |path: &Path| -> std::io::Result<Vec<u16>> {
        let absolute = path.parent().ok_or_else(|| std::io::Error::other("missing parent"))?
            .canonicalize()?.join(path.file_name().ok_or_else(|| std::io::Error::other("missing filename"))?);
        let mut value: Vec<u16> = absolute.as_os_str().encode_wide().collect();
        if value.contains(&0) { return Err(std::io::Error::other("NUL in selector path")); }
        value.push(0); Ok(value)
    };
    let (source, target) = (wide(source)?, wide(target)?);
    if unsafe { MoveFileExW(source.as_ptr(), target.as_ptr(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(not(any(unix, windows)))]
fn replace_selector(_: &Path, _: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "uncertified persistence platform"))
}

struct Preparation { root: PathBuf, selector: PathBuf, armed: bool }
impl Drop for Preparation {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.selector);
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}
