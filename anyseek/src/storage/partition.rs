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

// Initial roots append this after directory publication. A background checkpoint
// stages the certificate and complete concurrent suffix before publishing the
// generation directory. The three-witness WAL authenticates all of these frames.
const PUBLISHED: &[u8] = b"SPPARTITION-PUBLISHED-1";
#[derive(Debug)]
struct UncertifiedPartition;
impl std::fmt::Display for UncertifiedPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "partition creation has no confirmed publication certificate"
        )
    }
}
impl std::error::Error for UncertifiedPartition {}
pub(crate) fn is_uncertified(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|e| e.is::<UncertifiedPartition>())
}
pub(crate) struct PartitionJournal {
    journal: Journal,
    path: PathBuf,
    generation: u64,
    max_bytes: u64,
    // Stable across generation replacement; released after every journal file.
    _owner: File,
}
pub(crate) struct CommittedCut {generation:u64, journal:super::journal::CommittedCut}
impl CommittedCut {
    pub(crate) fn end(&self)->u64 {self.journal.end}
    pub(crate) fn scan(&self)->super::journal::CommittedScan {super::journal::CommittedScan::new(self.journal)}
    pub(crate) fn is_publication(ordinal:u64,payload:&[u8])->io::Result<bool> {
        if (ordinal==2)!=(payload==PUBLISHED) {return Err(io::Error::new(io::ErrorKind::InvalidData,"invalid publication marker position"));}
        Ok(payload==PUBLISHED)
    }
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
pub(crate) fn publish_bundle(prepared: &Path, target: &Path) -> Result<(), io::Error> {
    publish(prepared, target).map_err(|error| match error {
        CreateError::NotPublished(e) | CreateError::PublicationUnknown(e) => e,
    })
}

fn publish(prepared: &Path, target: &Path) -> Result<(), CreateError> {
    platform::publish_directory(prepared, target).map_err(|error| match error {
        platform::PublishError::BeforeRename(error) => CreateError::NotPublished(error),
        platform::PublishError::RenameAttempted(error) => CreateError::PublicationUnknown(error),
    })
}
impl PartitionJournal {
    pub(crate) fn root(&self)->&Path {&self.path}
    pub(crate) fn validate_checkpoint_budget(length: usize, max_bytes: u64) -> io::Result<()> {
        let payload_budget=max_bytes.checked_sub(24+PUBLISHED.len() as u64).ok_or_else(||io::Error::other("checkpoint certificate budget exceeded"))?;
        Journal::validate_checkpoint_budget(length,payload_budget)
    }
    /// Publish an already certified private root. Preserve uncertain rename status.
    pub(crate) fn publish_prepared_root(prepared: &Path, target: &Path) -> Result<(), CreateError> {
        if target.exists() { return Err(io::Error::new(io::ErrorKind::AlreadyExists,"snapshot target exists").into()); }
        publish(prepared,target)
    }
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
        #[cfg(feature = "fault-injection")]
        initialization_fault(path, "published");
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
        if std::env::var_os("ANYSEEK_PARTITION_CREATE_GATE").as_deref() == Some(path.as_os_str())
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
    /// Complete only a certified-journal, never-served initial root. The caller
    /// must own the parent catalog lock, including when the target is absent.
    /// A stable repair lock serializes repair attempts; it never substitutes for
    /// that parent lock. Certified or corrupt roots are never retired/rebuilt.
    pub(crate) fn resume_initial(path: &Path, max_bytes: u64) -> Result<(), CreateError> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("missing parent"))?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::other("invalid root name"))?;
        let staging = parent.join(format!(".initial-resume-{name}"));
        // Parent catalog ownership serializes creation. Do not leave a repair
        // lock for every new request rejected by a higher-level resource limit.
        if !path.exists() && !staging.exists() {
            return Ok(());
        }
        let guard = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(parent.join(format!(".initial-resume-{name}.lock")))?;
        guard.try_lock().map_err(io::Error::other)?;
        if !path.exists() && !staging.exists() {
            return Ok(());
        }
        if path.exists() && staging.exists() {
            return Err(io::Error::other("ambiguous initial root and staging").into());
        }
        let candidate = if path.exists() { path } else { &staging };
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(candidate.join("owner"))?;
        owner.try_lock().map_err(io::Error::other)?;
        let ids = generations(candidate)?;
        if ids != vec![0] {
            // This is not an initial root. Validate through normal recovery later.
            if candidate == path {
                return Ok(());
            }
            return Err(io::Error::other("staged root is not an initial generation").into());
        }
        let (journal, entries) = Journal::open(&generation_path(candidate, 0), max_bytes)?;
        if entries.get(1).is_some_and(|bytes| bytes == PUBLISHED) {
            if candidate == path {
                return Ok(());
            }
            return Err(io::Error::other("certified root unexpectedly staged").into());
        }
        if entries.len() != 1 {
            return Err(io::Error::other("invalid initial publication").into());
        }
        // Close all child/owner handles before directory moves on Windows.
        drop(journal);
        drop(owner);
        if candidate == path {
            publish(path, &staging)?;
            #[cfg(feature = "fault-injection")]
            initialization_fault(path, "retired");
        }
        publish(&staging, path)?;
        // Both durable directory publications must finish before certification.
        let owner = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.join("owner"))?;
        owner.try_lock().map_err(io::Error::other)?;
        let (mut journal, entries) = Journal::open(&generation_path(path, 0), max_bytes)?;
        if entries.len() != 1 {
            return Err(io::Error::other("initial root changed during repair").into());
        }
        journal
            .append(PUBLISHED)
            .map_err(CreateError::PublicationUnknown)?;
        drop(journal);
        drop(owner);
        drop(guard);
        Ok(())
    }

    /// Retire an explicitly tombstoned snapshot under the parent service lock.
    /// Stable trash naming makes deletion retryable after partial cleanup.
    pub(crate) fn retire_snapshot(path: &Path) -> Result<(), CreateError> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("missing parent"))?;
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::other("invalid snapshot name"))?;
        let staging = parent.join(format!(".initial-resume-{name}"));
        let trash = parent.join(format!(".retired-{name}"));
        let retry = parent.join(format!(".retired-retry-{name}"));
        let candidates = [path, staging.as_path(), trash.as_path(), retry.as_path()];
        let present = candidates
            .into_iter()
            .filter(|p| p.exists())
            .collect::<Vec<_>>();
        if present.len() > 1 {
            return Err(io::Error::other("ambiguous retired snapshot").into());
        }
        if let Some(source) = present.first() {
            if *source == path || *source == staging {
                let owner = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(source.join("owner"))?;
                owner.try_lock().map_err(io::Error::other)?;
                drop(owner); // Parent ownership protects closed-handle Windows moves.
            }
            // Re-publish even recovered trash before deleting: its previous
            // rename may have become visible without completing directory sync.
            // Alternating names also handles a partially deleted trash tree.
            let target = if *source == trash { &retry } else { &trash };
            publish(source, target)?;
            fs::remove_dir_all(target)?;
        }
        let repair_lock = parent.join(format!(".initial-resume-{name}.lock"));
        if repair_lock.exists() {
            fs::remove_file(repair_lock)?;
        }
        Ok(())
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
        let (generation, journal, entries) =
            selected.ok_or_else(|| io::Error::other(UncertifiedPartition))?;
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
                if std::env::var("ANYSEEK_PARTITION_CRASH").as_deref() == Ok("discard_partial") {
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
                #[cfg(any(test,feature="fault-injection"))]
                if self.path.join(".test-cleanup-failure").exists(){return Err(io::Error::other("injected old generation cleanup failure"));}
                #[cfg(feature="fault-injection")]
                if std::env::var("ANYSEEK_CHECKPOINT_CLEANUP_FAULT").as_deref()==Ok("1"){return Err(io::Error::other("injected old generation cleanup failure"));}
                #[cfg(test)]
                if std::env::var("ANYSEEK_PARTITION_CRASH").as_deref() == Ok("old_partial") {
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
    /// Retry old-generation retirement independently of durable publication.
    pub(crate) fn reclaim_obsolete(&self)->io::Result<()> {self.clean_obsolete()}
    pub(crate) fn restrict_budget(&mut self, max_bytes: u64) -> io::Result<()> {
        self.journal.restrict_budget(max_bytes)?;
        self.max_bytes = self.max_bytes.min(max_bytes);
        Ok(())
    }
    pub(crate) fn physical_bytes(&self)->u64 {self.journal.physical_bytes()}
    pub(crate) fn checkpoint_can_make_room(&self,length:usize,reserve:usize)->bool {
        self.journal.checkpoint_can_make_room(length,reserve)
    }
    pub(crate) fn validate_append_with_reserve(&self, length: usize, reserve: usize) -> io::Result<()> {
        self.journal.validate_append_with_reserve(length, reserve)
    }
    pub(crate) fn append(&mut self, payload: &[u8]) -> io::Result<u64> {
        self.journal.append(payload)
    }
    pub(crate) fn capture_cut(&self)->io::Result<CommittedCut> {
        Ok(CommittedCut{generation:self.generation,journal:self.journal.capture_cut(&generation_path(&self.path,self.generation))?})
    }
    pub(crate) fn read_cut_chunk(&mut self,cut:&CommittedCut,offset:u64,bytes:&mut [u8])->io::Result<()> {
        if self.generation!=cut.generation {return Err(io::Error::other("captured journal generation changed"));}
        self.journal.read_cut_chunk(cut.journal,offset,bytes)
    }
    /// Stream the selected certificate and complete ordered suffix. The
    /// publication marker is protocol metadata, not a Record Entry. In
    /// particular, do not filter using business seq: maintenance entries can
    /// change State without incrementing that sequence.
    pub(crate) fn visit_committed(
        &mut self,
        mut accept: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut published = false;
        self.journal.visit_committed(&generation_path(&self.path, self.generation), |ordinal, payload| {
            if (ordinal == 2) != (payload == PUBLISHED) {
                return Err(io::Error::other("invalid publication marker position"));
            }
            if payload == PUBLISHED {
                if published { return Err(io::Error::other("duplicate publication marker")); }
                published = true;
                Ok(())
            } else {
                accept(payload)
            }
        })?;
        if !published { return Err(io::Error::other(UncertifiedPartition)); }
        Ok(())
    }
    /// The new certificate and the complete concurrent suffix are staged together.
    /// No generation containing a certificate but missing suffix can be selected.
    pub(crate) fn checkpoint_with_tail(&mut self, payload: &[u8], tail: &[Vec<u8>]) -> Result<(), CreateError> {
        Self::validate_initial(payload.len(), self.max_bytes)?;
        #[cfg(test)]
        if self.path.join(".test-checkpoint-before-certificate").exists() {
            return Err(CreateError::NotPublished(io::Error::other("injected before checkpoint certificate with suffix")));
        }
        self.clean_obsolete()?;
        let next = self.generation.checked_add(1).ok_or_else(|| io::Error::other("generation exhausted"))?;
        let mut frames: Vec<&[u8]> = vec![payload, PUBLISHED];
        frames.extend(tail.iter().map(Vec::as_slice));
        let candidate = Journal::create_initialized_frames(&generation_path(&self.path,next),self.max_bytes,&frames)?;
        #[cfg(test)]
        if self.path.join(".test-checkpoint-after-certificate").exists() {
            return Err(CreateError::PublicationUnknown(io::Error::other("injected after checkpoint certificate with suffix")));
        }
        #[cfg(test)]
        crash_boundary("certified");
        let previous = std::mem::replace(&mut self.journal,candidate);
        self.generation = next;
        drop(previous);
        #[cfg(test)]
        crash_boundary("switched");
        if let Err(error) = self.clean_obsolete() { log::warn!("checkpoint published; cleanup pending: {error}"); }
        #[cfg(test)]
        crash_boundary("reclaimed");
        Ok(())
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
        // Test-only unknown-outcome boundaries exercise both legal recovery
        // selections. They do not simulate OS torn writes or power loss.
        #[cfg(test)]
        if self.path.join(".test-checkpoint-before-certificate").exists() {
            return Err(CreateError::PublicationUnknown(io::Error::other("injected before checkpoint certificate")));
        }
        candidate
            .append(PUBLISHED)
            .map_err(CreateError::PublicationUnknown)?;
        #[cfg(test)]
        if self.path.join(".test-checkpoint-after-certificate").exists() {
            return Err(CreateError::PublicationUnknown(io::Error::other("injected after checkpoint certificate")));
        }
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
#[cfg(feature = "fault-injection")]
fn initialization_fault(path: &Path, stage: &str) {
    let prefix = std::env::var("ANYSEEK_INITIAL_ROOT_PREFIX").unwrap_or_default();
    if !prefix.is_empty()
        && path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
        && std::env::var("ANYSEEK_INITIAL_FAULT").as_deref() == Ok(stage)
    {
        std::process::exit(87);
    }
}

#[cfg(test)]
fn crash_boundary(stage: &str) {
    if std::env::var("ANYSEEK_PARTITION_CRASH").as_deref() == Ok(stage) {
        std::process::exit(76);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn chunked_cut_rejects_changed_generation_and_invalid_marker() {
        let path=root();let mut store=PartitionJournal::create(&path,4096,b"base").unwrap();
        let cut=store.capture_cut().unwrap();let mut buffer=vec![0;cut.end() as usize];
        store.read_cut_chunk(&cut,0,&mut buffer).unwrap();let mut scan=cut.scan();let mut published=false;
        scan.push(&buffer,|ordinal,payload|{published|=CommittedCut::is_publication(ordinal,payload)?;Ok(())}).unwrap();
        scan.finish().unwrap();assert!(published);
        assert!(CommittedCut::is_publication(1,PUBLISHED).is_err());
        assert!(CommittedCut::is_publication(2,b"wrong-marker").is_err());
        store.checkpoint(b"new-base").unwrap();assert!(store.read_cut_chunk(&cut,0,&mut buffer).is_err());
        drop(store);std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn committed_stream_preserves_checkpoint_suffix_order() {
        let path = root();
        let mut store = PartitionJournal::create(&path, 4096, b"base").unwrap();
        store.append(b"same-seq-maintenance").unwrap();
        let mut entries = Vec::new();
        store.visit_committed(|v| { entries.push(v.to_vec()); Ok(()) }).unwrap();
        assert_eq!(entries, vec![b"base".to_vec(), b"same-seq-maintenance".to_vec()]);
        store.checkpoint_with_tail(b"new-base", &[b"tail-one".to_vec(), b"tail-two".to_vec()]).unwrap();
        store.append(b"tail-three").unwrap();
        entries.clear();
        store.visit_committed(|v| { entries.push(v.to_vec()); Ok(()) }).unwrap();
        assert_eq!(entries, vec![b"new-base".to_vec(), b"tail-one".to_vec(), b"tail-two".to_vec(), b"tail-three".to_vec()]);
    }
    #[test]
    fn checkpoint_with_tail_reports_publication_unknown_and_reopens_complete_suffix() {
        let path=root();let mut store=PartitionJournal::create(&path,4096,b"base").unwrap();
        std::fs::write(path.join(".test-checkpoint-after-certificate"),b"fault").unwrap();
        assert!(matches!(store.checkpoint_with_tail(b"new-base",&[b"same-seq-tail".to_vec()]),Err(CreateError::PublicationUnknown(_))));
        drop(store);std::fs::remove_file(path.join(".test-checkpoint-after-certificate")).unwrap();
        let (mut store,_)=PartitionJournal::open(&path,4096).unwrap();let mut entries=Vec::new();
        store.visit_committed(|bytes|{entries.push(bytes.to_vec());Ok(())}).unwrap();
        assert_eq!(entries,vec![b"new-base".to_vec(),b"same-seq-tail".to_vec()]);
    }
    #[test]
    fn committed_stream_rejects_marker_after_uncertified_entry() {
        let path = root();
        let mut store = PartitionJournal::create(&path, 4096, b"base").unwrap();
        let next = store.generation + 1;
        let replacement = Journal::create_initialized_frames(
            &generation_path(&path, next), 4096, &[b"base", b"wrong-second-frame", PUBLISHED],
        ).unwrap();
        store.journal = replacement;
        store.generation = next;
        let mut calls = 0;
        assert!(store.visit_committed(|_| { calls += 1; Ok(()) }).is_err());
        // A valid prefix can already have been visited: callers must retain
        // private preparations until the complete visit succeeds.
        assert_eq!(calls, 1);
    }
    fn root() -> PathBuf {
        let base = std::env::var_os("ANYSEEK_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        fs::create_dir_all(&base).unwrap();
        base.join(format!(
            "anyseek-partition-{:032x}",
            rand::random::<u128>()
        ))
    }
    #[cfg(windows)]
    #[test]
    fn partition_publication_supports_bare_relative_root() {
        let path = PathBuf::from(format!(
            "anyseek-relative-{:032x}",
            rand::random::<u128>()
        ));
        drop(PartitionJournal::create(&path, 4096, b"relative-original").unwrap());
        let (journal, entries) = PartitionJournal::open(&path, 4096).unwrap();
        assert_eq!(entries, vec![b"relative-original".to_vec()]);
        drop(journal);
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn partition_publication_supports_deep_service_snapshot_paths() {
        let base = root();
        let parent = base
            .join("service-backup-".to_owned() + &"a".repeat(64))
            .join("backup-".to_owned() + &"b".repeat(64))
            .join("nested-".to_owned() + &"c".repeat(64));
        fs::create_dir_all(&parent).unwrap();
        let path = parent.join("snapshot");
        assert!(path.as_os_str().len() > 260);
        drop(PartitionJournal::create(&path, 4096, b"full-original").unwrap());
        let (journal, entries) = PartitionJournal::open(&path, 4096).unwrap();
        assert_eq!(entries, vec![b"full-original".to_vec()]);
        drop(journal);
        fs::remove_dir_all(base).unwrap();
    }
    #[test]
    fn partition_retirement_resumes_staging_and_partial_trash() {
        for prefix in ["", ".initial-resume-", ".retired-", ".retired-retry-"] {
            let path = root();
            let candidate = path.with_file_name(format!(
                "{prefix}{}",
                path.file_name().unwrap().to_str().unwrap()
            ));
            fs::create_dir(&candidate).unwrap();
            if prefix.is_empty() || prefix == ".initial-resume-" {
                fs::write(candidate.join("owner"), []).unwrap();
            }
            fs::write(candidate.join("remaining-data"), b"original").unwrap();
            PartitionJournal::retire_snapshot(&path).unwrap();
            assert!(!candidate.exists());
            assert!(!path.exists());
            PartitionJournal::retire_snapshot(&path).unwrap();
        }
    }
    #[test]
    fn partition_resume_initial_preserves_payload_and_rejects_certified_corruption() {
        let path = root();
        fs::create_dir(&path).unwrap();
        fs::write(path.join("owner"), []).unwrap();
        drop(
            Journal::create_initialized(
                &generation_path(&path, 0),
                4096,
                Some(b"complete original snapshot"),
            )
            .unwrap(),
        );
        assert!(PartitionJournal::open(&path, 4096).is_err());
        PartitionJournal::resume_initial(&path, 4096).unwrap();
        let (store, entries) = PartitionJournal::open(&path, 4096).unwrap();
        assert_eq!(entries, vec![b"complete original snapshot".to_vec()]);
        drop(store);
        PartitionJournal::resume_initial(&path, 4096).unwrap();
        let wal = generation_path(&path, 0).join("wal");
        let mut bytes = fs::read(&wal).unwrap();
        bytes[30] ^= 1;
        fs::write(&wal, &bytes).unwrap();
        assert!(PartitionJournal::resume_initial(&path, 4096).is_err());
        assert!(PartitionJournal::open(&path, 4096).is_err());
        assert_eq!(fs::read(&wal).unwrap(), bytes);
        fs::remove_dir_all(&path).unwrap();
        fs::remove_file(path.parent().unwrap().join(format!(
            ".initial-resume-{}.lock",
            path.file_name().unwrap().to_str().unwrap()
        )))
        .unwrap();
    }

    #[test]
    #[ignore = "invoked by parent owner-lock test"]
    fn partition_creation_owner_worker() {
        let root = PathBuf::from(std::env::var_os("ANYSEEK_PARTITION_CREATE_GATE").unwrap());
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
            .env("ANYSEEK_PARTITION_CREATE_GATE", &root)
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
