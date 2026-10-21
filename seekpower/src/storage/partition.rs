//! Local directory generations. Highest published generation is authoritative;
//! corruption never falls back to an older acknowledged state. This is a local
//! filesystem protocol, not the deferred cloud root protocol.
use super::{
    journal::{CreateError, Journal},
    platform,
};
use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

// Appended only AFTER directory publication completed successfully. The
// existing three-witness WAL protocol makes this a crash-safe certificate.
const PUBLISHED: &[u8] = b"SPPARTITION-PUBLISHED-1";
pub(crate) struct PartitionJournal {
    journal: Journal,
    path: PathBuf,
    generation: u64,
    max_bytes: u64,
    // Stable across generation replacement; released after every journal file.
    _owner: File,
}
fn generation_path(root: &Path, generation: u64) -> PathBuf {
    root.join(format!("generation-{generation:016x}"))
}
fn generations(root: &Path) -> io::Result<Vec<u64>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(suffix) = name.strip_prefix("generation-") {
            if suffix.len() == 16 && suffix.bytes().all(|c| c.is_ascii_hexdigit()) {
                if !entry.file_type()?.is_dir() {
                    return Err(io::Error::other("generation is not a directory"));
                }
                result.push(u64::from_str_radix(suffix, 16).map_err(io::Error::other)?);
            }
        }
    }
    result.sort_unstable();
    Ok(result)
}
fn publish(prepared: &Path, target: &Path) -> Result<(), CreateError> {
    platform::publish_directory(prepared, target).map_err(|error| match error {
        platform::PublishError::BeforeRename(error) => CreateError::NotPublished(error),
        platform::PublishError::RenameAttempted(error) => CreateError::PublicationUnknown(error),
    })
}
impl PartitionJournal {
    pub(crate) fn validate_initial(length: usize, max_bytes: u64) -> io::Result<()> {
        let payload_budget = max_bytes
            .checked_sub(24 + PUBLISHED.len() as u64)
            .ok_or_else(|| io::Error::other("checkpoint certificate budget exceeded"))?;
        Journal::validate_initial(length, payload_budget)
    }
    pub(crate) fn create(path: &Path, max_bytes: u64, initial: &[u8]) -> Result<Self, CreateError> {
        Self::validate_initial(initial.len(), max_bytes)?;
        if path.exists() {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "partition exists").into());
        }
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("partition needs parent"))?;
        let prepared = parent.join(format!(
            ".partition-prepare-{:032x}",
            rand::random::<u128>()
        ));
        fs::create_dir(&prepared)?;
        let owner = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(prepared.join("owner"))?;
        owner.sync_all()?;
        drop(owner);
        drop(
            Journal::create_initialized(&generation_path(&prepared, 0), max_bytes, Some(initial))
                .map_err(|error| match error {
                CreateError::NotPublished(error) | CreateError::PublicationUnknown(error) => {
                    CreateError::NotPublished(error)
                }
            })?,
        );
        publish(&prepared, path)?;
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("owner"))
            .map_err(CreateError::PublicationUnknown)?;
        owner
            .try_lock()
            .map_err(|error| CreateError::PublicationUnknown(io::Error::other(error)))?;
        // Root and generation directory entries are durable before this marker
        // can appear. A root without a certificate cannot have accepted writes.
        let (mut journal, _) = Journal::open(&generation_path(path, 0), max_bytes)
            .map_err(CreateError::PublicationUnknown)?;
        journal
            .append(PUBLISHED)
            .map_err(CreateError::PublicationUnknown)?;
        #[cfg(test)]
        if std::env::var_os("SEEKPOWER_PARTITION_CREATE_GATE").as_deref() == Some(path.as_os_str())
        {
            fs::write(path.join("create-entered"), b"ready")?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            while !path.join("create-release").exists() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "create gate timed out"
                );
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
        Ok(Self {
            journal,
            path: path.into(),
            generation: 0,
            max_bytes,
            _owner: owner,
        })
    }
    pub(crate) fn open(path: &Path, max_bytes: u64) -> io::Result<(Self, Vec<Vec<u8>>)> {
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("owner"))?;
        owner.try_lock().map_err(io::Error::other)?;
        let mut selected = None;
        let mut unpublished = Vec::new();
        for generation in generations(path)?.into_iter().rev() {
            // Corruption is never permission to fall back. Only the explicit
            // absence of the publication certificate permits choosing an older
            // generation: that candidate could not have served writes.
            let (journal, mut entries) =
                Journal::open(&generation_path(path, generation), max_bytes)?;
            if entries.get(1).is_some_and(|bytes| bytes == PUBLISHED) {
                entries.remove(1);
                selected = Some((generation, journal, entries));
                break;
            }
            if entries.len() != 1 {
                return Err(io::Error::other(
                    "invalid generation publication certificate",
                ));
            }
            unpublished.push(generation);
        }
        let (generation, journal, entries) = selected.ok_or_else(|| {
            io::Error::other("partition creation has no confirmed publication certificate")
        })?;
        let store = Self {
            journal,
            path: path.into(),
            generation,
            max_bytes,
            _owner: owner,
        };
        for generation in unpublished {
            // Never partially delete a directory which still participates in
            // authority selection. Retire its name durably before any removal.
            let trash = path.join(format!(".prepare-{:032x}", rand::random::<u128>()));
            publish(&generation_path(path, generation), &trash).map_err(|error| match error {
                CreateError::NotPublished(error) | CreateError::PublicationUnknown(error) => error,
            })?;
            #[cfg(test)]
            {
                crash_boundary("discard_renamed");
                if std::env::var("SEEKPOWER_PARTITION_CRASH").as_deref() == Ok("discard_partial") {
                    fs::remove_file(trash.join("wal"))?;
                    crash_boundary("discard_partial");
                }
            }
            fs::remove_dir_all(trash)?;
        }
        store.clean_obsolete()?;
        Ok((store, entries))
    }
    fn clean_obsolete(&self) -> io::Result<()> {
        for generation in generations(&self.path)? {
            if generation < self.generation {
                #[cfg(test)]
                if std::env::var("SEEKPOWER_PARTITION_CRASH").as_deref() == Ok("old_partial") {
                    fs::remove_file(generation_path(&self.path, generation).join("wal"))?;
                    crash_boundary("old_partial");
                }
                fs::remove_dir_all(generation_path(&self.path, generation))?;
            }
        }
        // A single root owner is responsible for every generation preparation.
        // No detached asynchronous generation publication exists.
        let preparations = fs::read_dir(&self.path)?.collect::<io::Result<Vec<_>>>()?;
        for entry in preparations {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(suffix) = name.strip_prefix(".prepare-") {
                if suffix.len() == 32 && suffix.bytes().all(|c| c.is_ascii_hexdigit()) {
                    if !entry.file_type()?.is_dir() {
                        return Err(io::Error::other("preparation is not directory"));
                    }
                    // A visible preparation may be the result of a retirement
                    // rename whose parent sync failed. Re-establish a durable
                    // non-candidate name before deleting any contents; never
                    // infer durable retirement merely from directory listing.
                    let retired = self
                        .path
                        .join(format!(".prepare-{:032x}", rand::random::<u128>()));
                    publish(&entry.path(), &retired).map_err(|error| match error {
                        CreateError::NotPublished(error)
                        | CreateError::PublicationUnknown(error) => error,
                    })?;
                    fs::remove_dir_all(retired)?;
                }
            }
        }
        Ok(())
    }
    pub(crate) fn restrict_budget(&mut self, max_bytes: u64) -> io::Result<()> {
        self.journal.restrict_budget(max_bytes)?;
        self.max_bytes = self.max_bytes.min(max_bytes);
        Ok(())
    }
    pub(crate) fn validate_append(&self, length: usize) -> io::Result<()> {
        self.journal.validate_append(length)
    }
    pub(crate) fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        self.journal.append(payload)
    }
    pub(crate) fn checkpoint(&mut self, payload: &[u8]) -> Result<(), CreateError> {
        Self::validate_initial(payload.len(), self.max_bytes)?;
        // Before allocating another generation, reclaim leftovers from the
        // last publication. At most current + one prepared/old generation.
        self.clean_obsolete()?;
        let next = self
            .generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("generation exhausted"))?;
        let mut candidate = Journal::create_initialized(
            &generation_path(&self.path, next),
            self.max_bytes,
            Some(payload),
        )?;
        candidate
            .append(PUBLISHED)
            .map_err(CreateError::PublicationUnknown)?;
        #[cfg(test)]
        crash_boundary("certified");
        // Candidate publication is durable before the old authority is closed.
        let previous = std::mem::replace(&mut self.journal, candidate);
        self.generation = next;
        drop(previous);
        #[cfg(test)]
        crash_boundary("switched");
        // A reclamation error cannot undo a durable checkpoint. Retrying cleanup
        // is mandatory before another generation can allocate more disk space.
        if let Err(error) = self.clean_obsolete() {
            log::warn!("checkpoint published; old generation cleanup pending: {error}");
        }
        #[cfg(test)]
        crash_boundary("reclaimed");
        Ok(())
    }
}
#[cfg(test)]
fn crash_boundary(stage: &str) {
    if std::env::var("SEEKPOWER_PARTITION_CRASH").as_deref() == Ok(stage) {
        std::process::exit(76);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> PathBuf {
        let base = std::env::var_os("SEEKPOWER_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        fs::create_dir_all(&base).unwrap();
        base.join(format!(
            "seekpower-partition-{:032x}",
            rand::random::<u128>()
        ))
    }
    #[test]
    #[ignore = "invoked by parent owner-lock test"]
    fn partition_creation_owner_worker() {
        let root = PathBuf::from(std::env::var_os("SEEKPOWER_PARTITION_CREATE_GATE").unwrap());
        let path = root.clone();
        let task = std::thread::spawn(move || PartitionJournal::create(&path, 4096, b"initial"));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !root.join("create-entered").exists() {
            assert!(std::time::Instant::now() < deadline);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(PartitionJournal::open(&root, 4096).is_err());
        fs::write(root.join("create-release"), b"ready").unwrap();
        let mut owner = task.join().unwrap().unwrap();
        owner.append(b"first accepted write").unwrap();
        assert!(PartitionJournal::open(&root, 4096).is_err());
        drop(owner);
        let (owner, entries) = PartitionJournal::open(&root, 4096).unwrap();
        assert_eq!(
            entries,
            [b"initial".to_vec(), b"first accepted write".to_vec()]
        );
        drop(owner);
    }
    #[test]
    fn partition_creation_holds_owner_through_return() {
        let root = root();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "storage::partition::tests::partition_creation_owner_worker",
            ])
            .env("SEEKPOWER_PARTITION_CREATE_GATE", &root)
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn partition_corrupt_certified_generation_never_falls_back() {
        use std::io::{Seek, SeekFrom, Write};
        let root = root();
        let backup = root.with_extension("saved");
        drop(PartitionJournal::create(&root, 4096, b"initial").unwrap());
        fs::create_dir(&backup).unwrap();
        for entry in fs::read_dir(generation_path(&root, 0)).unwrap() {
            let entry = entry.unwrap();
            fs::copy(entry.path(), backup.join(entry.file_name())).unwrap();
        }
        let (mut owner, _) = PartitionJournal::open(&root, 4096).unwrap();
        owner.checkpoint(b"current").unwrap();
        drop(owner);
        fs::rename(&backup, generation_path(&root, 0)).unwrap();
        let mut file = OpenOptions::new()
            .write(true)
            .open(generation_path(&root, 1).join("wal"))
            .unwrap();
        file.seek(SeekFrom::Start(32)).unwrap();
        file.write_all(b"X").unwrap();
        file.sync_all().unwrap();
        drop(file);
        assert!(PartitionJournal::open(&root, 4096).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
