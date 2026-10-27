use super::build_budget::{File,OpenOptions};
use std::{
    fs::{self},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

const MAGIC: &[u8; 8] = b"SPWAL001";
const SEAL_MAGIC: &[u8; 8] = b"SPSEAL01";
const SEAL_SIZE: usize = 32;
const FRAME_HEADER: usize = 24;
const MAX_FRAME: usize = 16 * 1024 * 1024;

fn corrupt(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn hash(bytes: &[u8]) -> u64 {
    xxhash_rust::xxh64::xxh64(bytes, 0)
}
fn number(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("fixed-width field"))
}
fn seal(seq: u64, end: u64) -> [u8; SEAL_SIZE] {
    let mut bytes = [0; SEAL_SIZE];
    bytes[..8].copy_from_slice(SEAL_MAGIC);
    bytes[8..16].copy_from_slice(&seq.to_le_bytes());
    bytes[16..24].copy_from_slice(&end.to_le_bytes());
    let checksum = hash(&bytes[..24]);
    bytes[24..].copy_from_slice(&checksum.to_le_bytes());
    bytes
}
fn decode_seal(bytes: &[u8]) -> io::Result<(u64, u64)> {
    if &bytes[..8] != SEAL_MAGIC || number(&bytes[24..]) != hash(&bytes[..24]) {
        return Err(corrupt("invalid commit witness"));
    }
    Ok((number(&bytes[8..16]), number(&bytes[16..24])))
}

/// Before publication, no destination was changed. Once publication is
/// attempted, a failure may follow rename/sync and must be resolved by opening.
#[derive(Debug)]
pub(crate) enum CreateError {
    NotPublished(io::Error),
    PublicationUnknown(io::Error),
}
impl From<io::Error> for CreateError {
    fn from(error: io::Error) -> Self {
        Self::NotPublished(error)
    }
}

/// Bounded, single-writer journal with a separately synchronized commit witness.
/// Success requires two independent, synchronized witness files. At most one
/// witness is overwritten at a time, oldest first. After interruption/corruption
/// of one file, a confirmed position still has another intact copy. Recovery
/// validates the highest surviving position and repairs redundancy before writes.
/// Two invalid witnesses fail closed: that exceeds the single-update fault model.
pub(crate) struct Journal {
    log: File,
    witnesses: Vec<File>,
    positions: [(u64, u64); 3],
    seq: u64,
    end: u64,
    max_bytes: u64,
    needs_recovery: bool,
}

// Restore the writer's cursor on success, ordinary errors and unwind. Never
// unlock or reopen the WAL: Windows byte-range locks belong to this handle.
struct RestoreCursor<'a, S: Seek> {
    input: &'a mut S,
    saved: u64,
    needs_recovery: &'a mut bool,
    attempted: bool,
}
impl<S: Seek> RestoreCursor<'_, S> {
    fn restore(&mut self) -> io::Result<()> {
        if self.attempted { return Ok(()); }
        self.attempted=true;
        match self.input.seek(SeekFrom::Start(self.saved)) {
            Ok(_) => Ok(()),
            Err(error) => { *self.needs_recovery=true; Err(error) }
        }
    }
}
impl<S: Seek> Drop for RestoreCursor<'_, S> {
    fn drop(&mut self) { let _=self.restore(); }
}

impl Journal {
    /// Visit the healthy writer's committed cut without reopening its owner or
    /// changing its append cursor. The caller holds the partition owner and
    /// supplies that owner's current generation directory. Callback results are
    /// private preparations: they must not be published until this returns Ok.
    /// A callback cannot retain the borrowed payload; any decoded retained value
    /// needs its own residency lease, separate from this frame's scratch lease.
    pub(crate) fn visit_committed(
        &mut self,
        path: &Path,
        mut accept: impl FnMut(u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        if self.needs_recovery {
            return Err(io::Error::other("journal requires recovery"));
        }
        let path = path.join("wal");
        if !fs::symlink_metadata(&path)?.file_type().is_file() {
            return Err(corrupt("journal is not a regular file"));
        }
        // A second handle cannot read this exclusive range on Windows. A clone
        // can share cursor state, and Windows seek_read also changes the cursor.
        // Exclusive &mut access plus restoration keeps both ownership and append
        // position intact across all callback outcomes.
        let (end,seq)=(self.end,self.seq);
        if end < MAGIC.len() as u64 || end > self.log.metadata()?.len() || end > self.max_bytes {
            return Err(corrupt("invalid committed journal extent"));
        }
        let saved=self.log.stream_position()?;
        let mut guard=RestoreCursor {input:&mut self.log,saved,needs_recovery:&mut self.needs_recovery,attempted:false};
        let result=(|| {
        let input=&mut *guard.input;
        input.seek(SeekFrom::Start(0))?;
        let mut magic = [0; 8];
        input.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(corrupt("unsupported journal format"));
        }
        let mut offset = MAGIC.len() as u64;
        let mut sequence = 0u64;
        while offset < end {
            if end - offset < FRAME_HEADER as u64 {
                return Err(corrupt("truncated committed frame"));
            }
            let mut header = [0; FRAME_HEADER];
            input.read_exact(&mut header)?;
            let length = number(&header[..8]);
            sequence = sequence.checked_add(1).ok_or_else(|| corrupt("sequence overflow"))?;
            if length > MAX_FRAME as u64
                || length > end - offset - FRAME_HEADER as u64
                || number(&header[8..16]) != sequence
            {
                return Err(corrupt("invalid committed frame length or sequence"));
            }
            let _scratch = super::build_budget::reserve_memory(
                length.checked_mul(32).ok_or_else(|| corrupt("frame admission overflow"))?,
            )?;
            let mut payload = vec![0; length as usize];
            input.read_exact(&mut payload)?;
            let mut checksum = xxhash_rust::xxh64::Xxh64::new(0);
            checksum.update(&header[..16]);
            checksum.update(&payload);
            if checksum.digest() != number(&header[16..]) {
                return Err(corrupt("committed frame checksum mismatch"));
            }
            accept(sequence, &payload)?;
            offset += FRAME_HEADER as u64 + length;
        }
        if sequence != seq {
            return Err(corrupt("commit witness sequence mismatch"));
        }
        Ok(())
        })();
        guard.restore().map_err(|error|io::Error::other(format!("committed WAL cursor restoration failed: {error}")))?;
        result
    }

    /// Build in a unique sibling directory, sync all files, then publish. No
    /// legacy directory is overwritten or initialized in place.
    #[cfg(test)]
    pub(crate) fn create(path: &Path, max_bytes: u64) -> io::Result<Self> {
        Self::create_initialized(path, max_bytes, None).map_err(|error| match error {
            CreateError::NotPublished(error) | CreateError::PublicationUnknown(error) => error,
        })
    }

    /// Prepare the complete initial authority before publishing its directory.
    /// A rejected payload never occupies the destination. Publication I/O errors
    /// remain uncertain to callers, including a failed parent-directory sync.
    pub(crate) fn create_initialized(
        path: &Path,
        max_bytes: u64,
        initial: Option<&[u8]>,
    ) -> Result<Self, CreateError> {
        Self::create_initialized_frames(path, max_bytes, &initial.into_iter().collect::<Vec<_>>())
    }

    /// All frames are synced in private staging before directory publication.
    pub(crate) fn create_initialized_frames(
        path: &Path, max_bytes: u64, frames: &[&[u8]],
    ) -> Result<Self, CreateError> {
        for payload in frames { Self::validate_initial(payload.len(), max_bytes)?; }
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("journal needs a parent"))?;
        if path.exists() {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "journal exists").into());
        }
        if max_bytes < MAGIC.len() as u64 {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput, "journal budget too small").into(),
            );
        }
        let prepared = parent.join(format!(".prepare-{:032x}", rand::random::<u128>()));
        fs::create_dir(&prepared)?;
        // Leave incomplete preparations identifiable on failure; never remove
        // the destination after an uncertain publication outcome.
        {
            let mut log = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(prepared.join("wal"))?;
            log.write_all(MAGIC)?;
            log.sync_all()?;
            for slot in 0..3 {
                let mut witness = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(prepared.join(format!("commit-{slot}")))?;
                witness.write_all(&seal(0, MAGIC.len() as u64))?;
                witness.sync_all()?;
            }
        }
        if !frames.is_empty() {
            let (mut journal, _) = Self::open(&prepared, max_bytes)?;
            for payload in frames { journal.append(payload)?; }
            // Close every file before directory rename (Windows).
            drop(journal);
        }
        #[cfg(test)]
        crash_boundary("initial_prepared");
        super::platform::publish_directory(&prepared, path).map_err(|error| match error {
            super::platform::PublishError::BeforeRename(error) => CreateError::NotPublished(error),
            super::platform::PublishError::RenameAttempted(error) => {
                CreateError::PublicationUnknown(error)
            }
        })?;
        #[cfg(test)]
        crash_boundary("initial_published");
        let (journal, entries) =
            Self::open(path, max_bytes).map_err(CreateError::PublicationUnknown)?;
        debug_assert_eq!(entries.len(), frames.len());
        Ok(journal)
    }

    pub(crate) fn open(path: &Path, max_bytes: u64) -> io::Result<(Self, Vec<Vec<u8>>)> {
        let mut log = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("wal"))?;
        log.try_lock().map_err(io::Error::other)?;
        let mut witnesses = Vec::new();
        let mut decoded = Vec::new();
        for slot in 0..3 {
            let mut witness = OpenOptions::new()
                .read(true)
                .write(true)
                .open(path.join(format!("commit-{slot}")))?;
            let mut bytes = [0; SEAL_SIZE];
            let position = if witness.metadata()?.len() == SEAL_SIZE as u64 {
                witness.read_exact(&mut bytes)?;
                decode_seal(&bytes).ok()
            } else {
                None
            };
            decoded.push(position);
            witnesses.push(witness);
        }
        let valid: Vec<_> = decoded.iter().flatten().copied().collect();
        if valid.len() < 2 {
            return Err(corrupt("multiple invalid commit witnesses"));
        }
        let (seq, end) = *valid.iter().max().unwrap();
        if valid
            .iter()
            .any(|&(s, e)| seq - s > 1 || (s == seq && e != end) || (s < seq && e >= end))
        {
            return Err(corrupt("inconsistent commit witnesses"));
        }
        if end < MAGIC.len() as u64 || end > log.metadata()?.len() || end > max_bytes {
            return Err(corrupt(
                "committed journal extent is missing or exceeds configured budget",
            ));
        }
        let mut magic = [0; 8];
        log.read_exact(&mut magic)?;
        if &magic != MAGIC {
            return Err(corrupt("unsupported journal format"));
        }
        let mut entries = Vec::new();
        let mut offset = MAGIC.len() as u64;
        let mut next = 1;
        while offset < end {
            if end - offset < FRAME_HEADER as u64 {
                return Err(corrupt("truncated committed frame"));
            }
            let mut header = [0; FRAME_HEADER];
            log.read_exact(&mut header)?;
            let length = number(&header[..8]);
            if length > MAX_FRAME as u64
                || length > end - offset - FRAME_HEADER as u64
                || number(&header[8..16]) != next
            {
                return Err(corrupt("invalid committed frame length or sequence"));
            }
            let mut payload = vec![0; length as usize];
            log.read_exact(&mut payload)?;
            let mut checked = header[..16].to_vec();
            checked.extend_from_slice(&payload);
            if hash(&checked) != number(&header[16..]) {
                return Err(corrupt("committed frame checksum mismatch"));
            }
            entries.push(payload);
            offset += FRAME_HEADER as u64 + length;
            next += 1;
        }
        if next - 1 != seq {
            return Err(corrupt("commit witness sequence mismatch"));
        }
        // Only this suffix is provably outside the committed extent. Its bytes
        // may contain a complete write whose caller never received success.
        if log.metadata()?.len() != end {
            log.set_len(end)?;
            log.sync_all()?;
        }
        // Preserve the highest valid copy while repairing all other files.
        // Do not expose a writer that can accumulate a second torn witness.
        // Repair invalid files first. Otherwise a crash while overwriting an
        // older valid file could leave two invalid witnesses on the next open.
        let mut repair_order = [0, 1, 2];
        repair_order.sort_by_key(|&slot| decoded[slot].is_some());
        for slot in repair_order {
            if decoded[slot] != Some((seq, end)) {
                witnesses[slot].seek(SeekFrom::Start(0))?;
                #[cfg(test)]
                if std::env::var("ANYSEEK_JOURNAL_RECOVERY_CRASH").as_deref() == Ok("torn") {
                    witnesses[slot].write_all(&seal(seq, end)[..12])?;
                    std::process::exit(75);
                }
                witnesses[slot].write_all(&seal(seq, end))?;
                witnesses[slot].set_len(SEAL_SIZE as u64)?;
                witnesses[slot].sync_all()?;
            }
        }
        log.seek(SeekFrom::Start(end))?;
        Ok((
            Self {
                log,
                witnesses,
                positions: [(seq, end); 3],
                seq,
                end,
                max_bytes,
                needs_recovery: false,
            },
            entries,
        ))
    }

    #[cfg(test)]
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    /// Logical complete-state budget for a separately framed authenticated anchor.
    /// Callers still validate each actual journal frame with validate_initial.
    pub(crate) fn validate_checkpoint_budget(length: usize, max_bytes: u64) -> io::Result<()> {
        let extent = (MAGIC.len() as u64).checked_add(FRAME_HEADER as u64).and_then(|n|n.checked_add(length as u64));
        if extent.is_none_or(|n|n>max_bytes) { return Err(io::Error::new(io::ErrorKind::InvalidInput,"complete checkpoint budget exceeded")); }
        Ok(())
    }

    pub(crate) fn validate_initial(length: usize, max_bytes: u64) -> io::Result<()> {
        let extent = (MAGIC.len() as u64)
            .checked_add(FRAME_HEADER as u64)
            .and_then(|n| n.checked_add(length as u64));
        if length > MAX_FRAME || extent.is_none_or(|n| n > max_bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "initial checkpoint budget exceeded",
            ));
        }
        Ok(())
    }

    pub(crate) fn restrict_budget(&mut self, max_bytes: u64) -> io::Result<()> {
        if self.end > max_bytes {
            return Err(corrupt("journal exceeds persisted resource budget"));
        }
        self.max_bytes = self.max_bytes.min(max_bytes);
        Ok(())
    }

    /// Only recoverable physical space pressure may request checkpointing.
    /// Recovery-required, oversized frames and exhausted sequence are not space.
    pub(crate) fn physical_bytes(&self)->u64 {self.end}
    pub(crate) fn checkpoint_can_make_room(&self,length:usize,reserve:usize)->bool {
        !self.needs_recovery && length<=MAX_FRAME && self.seq<u64::MAX
            && (FRAME_HEADER as u64).checked_add(length as u64).and_then(|n|n.checked_add(reserve as u64)).is_some_and(|n|n<=self.max_bytes)
            && self.end.checked_add(FRAME_HEADER as u64).and_then(|n|n.checked_add(length as u64))
                .and_then(|n|n.checked_add(reserve as u64)).is_some_and(|n|n>self.max_bytes)
    }
    pub(crate) fn validate_append(&self, length: usize) -> io::Result<()> {
        self.validate_append_with_reserve(length, 0)
    }
    pub(crate) fn validate_append_with_reserve(&self, length: usize, reserve: usize) -> io::Result<()> {
        if self.needs_recovery {
            return Err(io::Error::other("journal requires recovery"));
        }
        let end = self
            .end
            .checked_add(FRAME_HEADER as u64)
            .and_then(|n| n.checked_add(length as u64))
            .and_then(|n| n.checked_add(reserve as u64));
        if length > MAX_FRAME || end.is_none_or(|n| n > self.max_bytes) || self.seq == u64::MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal resource budget exceeded",
            ));
        }
        Ok(())
    }

    /// I/O errors after entering this method's write phase have unknown outcome.
    /// The caller must fence publication and recover; never continue appending.
    pub(crate) fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        self.validate_append(payload.len())?;
        let seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| io::Error::other("sequence exhausted"))?;
        let end = self
            .end
            .checked_add(FRAME_HEADER as u64 + payload.len() as u64)
            .ok_or_else(|| io::Error::other("journal extent overflow"))?;
        if payload.len() > MAX_FRAME || end > self.max_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal resource budget exceeded",
            ));
        }
        let mut checked = Vec::with_capacity(16 + payload.len());
        checked.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        checked.extend_from_slice(&seq.to_le_bytes());
        checked.extend_from_slice(payload);
        let checksum = hash(&checked);
        // Set before the first write, including before cancellation/panic.
        self.needs_recovery = true;
        self.log.write_all(&checked[..16])?;
        self.log.write_all(&checksum.to_le_bytes())?;
        self.log.write_all(payload)?;
        #[cfg(test)]
        crash_boundary("log_written");
        self.log.sync_all()?;
        #[cfg(test)]
        crash_boundary("log_synced");
        for copy in 0..2 {
            let slot = (0..3).min_by_key(|&slot| self.positions[slot].0).unwrap();
            self.witnesses[slot].seek(SeekFrom::Start(0))?;
            #[cfg(test)]
            {
                let stage = if copy == 0 {
                    "first_witness_torn"
                } else {
                    "second_witness_torn"
                };
                if std::env::var("ANYSEEK_JOURNAL_CRASH").as_deref() == Ok(stage) {
                    self.witnesses[slot].write_all(&seal(seq, end)[..12])?;
                    crash_boundary(stage);
                }
            }
            self.witnesses[slot].write_all(&seal(seq, end))?;
            #[cfg(test)]
            crash_boundary(if copy == 0 {
                "first_witness_written"
            } else {
                "second_witness_written"
            });
            self.witnesses[slot].sync_all()?;
            self.positions[slot] = (seq, end);
            #[cfg(test)]
            crash_boundary(if copy == 0 {
                "first_witness_synced"
            } else {
                "second_witness_synced"
            });
            let _ = copy;
        }
        self.seq = seq;
        self.end = end;
        self.needs_recovery = false;
        Ok(seq)
    }
}

#[cfg(test)]
fn crash_boundary(stage: &str) {
    if std::env::var("ANYSEEK_JOURNAL_CRASH").as_deref() == Ok(stage) {
        std::process::exit(73); // No Rust destructors or graceful shutdown.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn committed_stream_preserves_lock_and_cursor_on_unwind() {
        let root=path();let mut journal=Journal::create(&root,4096).unwrap();
        journal.append(b"first").unwrap();let cursor=journal.log.stream_position().unwrap();
        let panic=std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            journal.visit_committed(&root,|_,_| {
                assert!(Journal::open(&root,4096).is_err(),"stream must retain exclusive ownership");
                panic!("injected consumer unwind");
            }).unwrap();
        }));
        assert!(panic.is_err());assert_eq!(journal.log.stream_position().unwrap(),cursor);
        assert!(!journal.needs_recovery);
        journal.append(b"second").unwrap();drop(journal);
        assert_eq!(Journal::open(&root,4096).unwrap().1,vec![b"first".to_vec(),b"second".to_vec()]);
    }
    #[test]
    fn committed_stream_restore_failure_fences_owner() {
        struct FailedSeek;
        impl Seek for FailedSeek {fn seek(&mut self,_:SeekFrom)->io::Result<u64>{Err(io::Error::other("injected restore failure"))}}
        let mut input=FailedSeek;let mut recovery=false;
        {let mut guard=RestoreCursor {input:&mut input,saved:19,needs_recovery:&mut recovery,attempted:false};assert!(guard.restore().is_err());}
        assert!(recovery);
    }
    #[test]
    fn committed_stream_keeps_append_cursor_and_ignores_uncommitted_suffix() {
        let root = path();
        let mut journal = Journal::create(&root, 4096).unwrap();
        journal.append(b"first").unwrap();
        journal.append(b"second").unwrap();
        let saved = journal.log.stream_position().unwrap();
        journal.log.seek(SeekFrom::End(0)).unwrap();
        journal.log.write_all(b"uncommitted").unwrap();
        journal.log.seek(SeekFrom::Start(saved)).unwrap();
        let mut seen = Vec::new();
        journal.visit_committed(&root, |seq, bytes| { seen.push((seq, bytes.to_vec())); Ok(()) }).unwrap();
        assert_eq!(seen, vec![(1, b"first".to_vec()), (2, b"second".to_vec())]);
        assert_eq!(journal.log.stream_position().unwrap(), saved);
        // An interrupted consumer must not poison or seek the writer either.
        assert!(journal.visit_committed(&root, |_, _| Err(io::Error::other("consumer failed"))).is_err());
        assert_eq!(journal.log.stream_position().unwrap(), saved);
        assert!(!journal.needs_recovery);
        journal.append(b"third").unwrap();
        drop(journal);
        let (_, entries) = Journal::open(&root, 4096).unwrap();
        assert_eq!(entries, vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]);
    }
    #[test]
    fn committed_stream_rejects_corruption_and_uncertain_writer() {
        let root = path();
        let mut journal = Journal::create(&root, 4096).unwrap();
        journal.append(b"first").unwrap();
        journal.needs_recovery = true;
        assert!(journal.append(b"must not append while fenced").is_err());
        let mut called = false;
        assert!(journal.visit_committed(&root, |_, _| { called = true; Ok(()) }).is_err());
        assert!(!called);
        journal.needs_recovery = false;
        journal.log.seek(SeekFrom::Start((MAGIC.len() + FRAME_HEADER) as u64)).unwrap();
        journal.log.write_all(b"X").unwrap();
        assert!(journal.visit_committed(&root, |_, _| { called = true; Ok(()) }).is_err());
        assert!(!called);
    }
    #[test]
    fn committed_stream_late_corruption_does_not_certify_prefix() {
        let root = path();
        let mut journal = Journal::create(&root, 4096).unwrap();
        journal.append(b"first").unwrap();
        journal.append(b"second").unwrap();
        journal.log.seek(SeekFrom::Start((MAGIC.len() + FRAME_HEADER + 5 + FRAME_HEADER) as u64)).unwrap();
        journal.log.write_all(b"X").unwrap();
        let mut private = Vec::new();
        let result = journal.visit_committed(&root, |_, bytes| { private.push(bytes.to_vec()); Ok(()) });
        assert!(result.is_err());
        assert_eq!(private, vec![b"first".to_vec()]);
        // The complete visit, not each callback, gates candidate publication.
        let published = result.is_ok().then_some(private);
        assert!(published.is_none());
    }
    fn path() -> std::path::PathBuf {
        let base = std::env::var_os("ANYSEEK_TEST_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&base).unwrap();
        base.join(format!("anyseek-journal-{:032x}", rand::random::<u128>()))
    }
    #[test]
    fn journal_recovery_ignores_only_uncommitted_suffix_and_locks_writer() {
        let path = path();
        let mut journal = Journal::create(&path, 1024).unwrap();
        assert!(Journal::open(&path, 1024).is_err());
        assert_eq!(journal.append(b"original vector and record").unwrap(), 1);
        assert_eq!(journal.append(b"tombstone").unwrap(), 2);
        drop(journal);
        let mut tail = OpenOptions::new()
            .append(true)
            .open(path.join("wal"))
            .unwrap();
        tail.write_all(b"torn uncommitted frame").unwrap();
        tail.sync_all().unwrap();
        drop(tail);
        let (mut journal, entries) = Journal::open(&path, 1024).unwrap();
        assert_eq!(
            entries,
            [
                b"original vector and record".to_vec(),
                b"tombstone".to_vec()
            ]
        );
        assert_eq!(journal.append(b"next").unwrap(), 3);
        drop(journal);
        let (journal, entries) = Journal::open(&path, 1024).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(journal.seq(), 3);
        drop(journal);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn journal_committed_corruption_and_torn_witness_fail_closed() {
        for target in ["wal", "commit-0"] {
            let path = path();
            let mut journal = Journal::create(&path, 1024).unwrap();
            journal.append(b"acknowledged").unwrap();
            drop(journal);
            let mut file = OpenOptions::new()
                .write(true)
                .open(path.join(target))
                .unwrap();
            file.seek(SeekFrom::Start(if target == "wal" { 32 } else { 0 }))
                .unwrap();
            file.write_all(b"x").unwrap();
            file.sync_all().unwrap();
            drop(file);
            if target == "wal" {
                assert!(Journal::open(&path, 1024).is_err());
            } else {
                let (journal, entries) = Journal::open(&path, 1024).unwrap();
                assert_eq!(entries, [b"acknowledged".to_vec()]);
                drop(journal);
                fs::write(path.join("commit-0"), b"bad").unwrap();
                fs::write(path.join("commit-1"), b"bad").unwrap();
                assert!(Journal::open(&path, 1024).is_err());
            }
            fs::remove_dir_all(path).unwrap();
        }
    }
    #[test]
    #[ignore = "invoked only by parent process crash test"]
    fn journal_crash_worker() {
        let path = std::env::var_os("ANYSEEK_JOURNAL_PATH").expect("worker path");
        let (mut journal, _) = Journal::open(Path::new(&path), 1024).unwrap();
        journal.append(b"possibly acknowledged").unwrap();
        panic!("crash hook was not reached");
    }
    #[test]
    fn journal_native_process_crash_boundaries() {
        for stage in [
            "log_written",
            "log_synced",
            "first_witness_torn",
            "first_witness_written",
            "first_witness_synced",
            "second_witness_torn",
            "second_witness_written",
            "second_witness_synced",
        ] {
            let path = path();
            let mut journal = Journal::create(&path, 1024).unwrap();
            journal.append(b"previously acknowledged").unwrap();
            drop(journal);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "storage::journal::tests::journal_crash_worker",
                ])
                .env("ANYSEEK_JOURNAL_PATH", &path)
                .env("ANYSEEK_JOURNAL_CRASH", stage)
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(73), "{stage}: {output:?}");
            let (journal, entries) = Journal::open(&path, 1024).unwrap();
            assert_eq!(entries[0], b"previously acknowledged");
            let expected = if stage.starts_with("log") || stage == "first_witness_torn" {
                1
            } else {
                2
            };
            assert_eq!(entries.len(), expected, "{stage}");
            drop(journal);
            fs::remove_dir_all(path).unwrap();
        }
    }
    #[test]
    #[ignore = "invoked only by parent initialization crash test"]
    fn journal_initialization_worker() {
        let path = std::env::var_os("ANYSEEK_JOURNAL_PATH").expect("worker path");
        Journal::create_initialized(Path::new(&path), 1024, Some(b"complete snapshot")).unwrap();
        panic!("initialization crash hook was not reached");
    }

    #[test]
    fn journal_initialization_publishes_complete_snapshot_or_no_target() {
        for stage in [
            "log_synced",
            "first_witness_torn",
            "initial_prepared",
            "initial_published",
        ] {
            let root = path();
            fs::create_dir(&root).unwrap();
            let target = root.join("partition");
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "storage::journal::tests::journal_initialization_worker",
                ])
                .env("ANYSEEK_JOURNAL_PATH", &target)
                .env("ANYSEEK_JOURNAL_CRASH", stage)
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(73), "{stage}: {output:?}");
            if stage == "initial_published" {
                let (journal, entries) = Journal::open(&target, 1024).unwrap();
                assert_eq!(entries, [b"complete snapshot".to_vec()]);
                drop(journal);
            } else {
                assert!(!target.exists(), "{stage}");
                // The same destination remains usable after interrupted preparation.
                drop(Journal::create_initialized(&target, 1024, Some(b"retry snapshot")).unwrap());
                let (journal, entries) = Journal::open(&target, 1024).unwrap();
                assert_eq!(entries, [b"retry snapshot".to_vec()]);
                drop(journal);
            }
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn journal_budget_rejection_leaves_writer_usable() {
        let path = path();
        let mut journal = Journal::create(&path, 100).unwrap();
        assert!(journal.append(&[0; 100]).is_err());
        assert_eq!(journal.append(b"fits").unwrap(), 1);
        drop(journal);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    #[ignore = "launched only by recovery crash parent"]
    fn journal_recovery_worker() {
        let path = std::env::var_os("ANYSEEK_JOURNAL_PATH").unwrap();
        let _ = Journal::open(Path::new(&path), 1024).unwrap();
        panic!("recovery crash hook not reached");
    }
    #[test]
    fn journal_repair_crash_preserves_redundancy() {
        let path = path();
        let mut journal = Journal::create(&path, 1024).unwrap();
        journal.append(b"one").unwrap();
        journal.append(b"two").unwrap();
        drop(journal);
        // Slots are [2,1,2]. An invalid slot 2 must be repaired BEFORE rewriting
        // the older valid slot 1; otherwise another crash loses redundancy.
        fs::write(path.join("commit-2"), b"bad").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "storage::journal::tests::journal_recovery_worker",
            ])
            .env("ANYSEEK_JOURNAL_PATH", &path)
            .env("ANYSEEK_JOURNAL_RECOVERY_CRASH", "torn")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(75), "{output:?}");
        let (journal, entries) = Journal::open(&path, 1024).unwrap();
        assert_eq!(entries, [b"one".to_vec(), b"two".to_vec()]);
        drop(journal);
        fs::remove_dir_all(path).unwrap();
    }
}
