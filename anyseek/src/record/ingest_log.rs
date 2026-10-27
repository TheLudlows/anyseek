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
    pub(super) fn bytes(&self) -> u64 {
        self.retained_bytes
    }
    pub(super) fn available(&self) -> u64 {
        self.max_bytes.saturating_sub(self.retained_bytes)
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
            .retained_bytes
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
