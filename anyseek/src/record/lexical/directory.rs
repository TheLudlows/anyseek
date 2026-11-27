//! Native writer accounting captured before Tantivy spawns its own threads.
//! This changes file access, never Tantivy's internal encoding.
use crate::{
    record::{RecordError, storage},
    resources,
    storage::build_budget::File,
};
use stable_deref_trait::StableDeref;
use std::{
    cell::RefCell,
    collections::BTreeSet,
    fmt, fs,
    io::{self, Write},
    ops::{Deref, Range},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, atomic::{AtomicBool, AtomicU64, Ordering}},
};
use tantivy::{
    HasLen,
    directory::{
        AntiCallToken, Directory, DirectoryLock, FileHandle, Lock, OwnedBytes, TerminatingWrite,
        WatchCallback, WatchHandle, WritePtr,
        error::{DeleteError, LockError, OpenReadError, OpenWriteError},
    },
};

// Native merge, indexing and reload share a gate per physical directory.
// Independent collections must not wait for each other's writes or disk syncs.
// Acquire capacity before the gate; retain neither between operations.
#[path = "directory_gate.rs"]
mod gate;

// One writable tail can admit 128 native updates. The writer's private JSON
// metadata, including its managed-file list, fits in 256 KiB before decoding.
const WRITER_METADATA_WORKSPACE: u64 = 8 * 256 * 1024;
// Native indexing threads read their field-normalization file while building
// a segment. Reserve this workspace before they can start.
const WRITER_READ_WORKSPACE: u64 = 2 * 1024 * 1024;

tokio::task_local! {
    static SNAPSHOT_READS: RefCell<Option<resources::MemoryReservation>>;
}

pub(super) fn with_snapshot_reads<T>(
    memory: Option<resources::MemoryReservation>,
    work: impl FnOnce() -> T,
) -> T {
    SNAPSHOT_READS.sync_scope(RefCell::new(memory), work)
}

struct Ledger {
    written: u64,
    paths: BTreeSet<PathBuf>,
}
pub(super) struct WriterReservation {
    _memory: resources::MemoryReservation,
    reads: AtomicU64,
}
impl WriterReservation {
    fn new(bytes: u64, scope: Option<&resources::Scope>) -> Result<Self, RecordError> {
        let bytes = bytes.checked_add(WRITER_METADATA_WORKSPACE + WRITER_READ_WORKSPACE)
            .ok_or_else(|| storage("native writer memory overflow"))?;
        Ok(Self {
            _memory: crate::record::build_budget::admit_memory(bytes, scope)?,
            reads: AtomicU64::new(0),
        })
    }

    fn reserve_read(self: &Arc<Self>, bytes: u64) -> Option<WriterReadReservation> {
        self.reads.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(bytes).filter(|n| *n <= WRITER_READ_WORKSPACE)
        }).ok()?;
        Some(WriterReadReservation { writer: self.clone(), bytes })
    }
}

struct WriterReadReservation {
    writer: Arc<WriterReservation>,
    bytes: u64,
}
impl Drop for WriterReadReservation {
    fn drop(&mut self) {
        self.writer.reads.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct Budget {
    io_gate: Arc<Mutex<()>>,
    cancelled: AtomicBool,
    #[cfg(test)]
    fail_writes: AtomicBool,
    #[cfg(test)]
    failed_writes: std::sync::atomic::AtomicUsize,
    pin: Mutex<Option<Arc<crate::record::package_pins::Pin>>>,
    writer_memory: Mutex<Option<Arc<WriterReservation>>>,
    pending_analysis: Mutex<Vec<resources::MemoryReservation>>,
    ledger: Mutex<Ledger>,
    bytes: u64,
    files: usize,
    scope: Option<resources::Scope>,
    // Native workers own Directory clones, so even an early error cannot free
    // the reservation while an internal worker still owns the candidate.
    memory: Mutex<resources::MemoryReservation>,
    build_memory: u64,
    // Directory clones retain the OS owner lock after a public handle drops.
    partition_owner: Mutex<Option<Arc<fs::File>>>,
}
// Keep the package pin and owner through Searcher destruction, including
// empty Searchers that have no segment file handles of their own.
#[derive(Clone)]
pub(super) struct DirectoryOwner { _budget: Arc<Budget> }
impl Budget {
    fn charge(&self, path: &Path, bytes: u64) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_writes.load(Ordering::Acquire) {
            self.failed_writes.fetch_add(1, Ordering::Relaxed);
            return Err(io::Error::other("injected native worker write failure"));
        }
        let mut ledger = self
            .ledger
            .lock()
            .map_err(|_| io::Error::other("native budget poisoned"))?;
        let next = ledger
            .written
            .checked_add(bytes)
            .filter(|n| *n <= self.bytes)
            .ok_or_else(|| {
                log::error!("native writer byte budget rejected: written={} requested={bytes} limit={} files={}", ledger.written, self.bytes, ledger.paths.len());
                io::Error::other("native writer byte budget exceeded")
            })?;
        if !ledger.paths.contains(path) && ledger.paths.len() >= self.files {
            return Err(io::Error::other("native writer file budget exceeded"));
        }
        ledger.paths.insert(path.to_owned());
        ledger.written = next;
        // Conservative: failed writes, deletes and atomic replacements never
        // refund charged traffic. Each new private build gets a fresh ledger.
        Ok(())
    }
    fn io<T>(&self, work: impl FnOnce() -> io::Result<T>) -> io::Result<T> {
        let current = resources::Scope::current();
        let gated = || {
            let _gate = self.io_gate.lock().map_err(|_| io::Error::other("native I/O gate poisoned"))?;
            work()
        };
        match self.scope.as_ref().or(current.as_ref()) {
            Some(scope) => scope.native_file_io(&self.cancelled, gated),
            None => gated(),
        }
    }

    fn admit_atomic_metadata(&self, length: u64) -> io::Result<()> {
        let metadata = length.checked_mul(8)
            .ok_or_else(|| io::Error::other("native metadata memory overflow"))?;
        if metadata <= WRITER_METADATA_WORKSPACE && self.writer_memory.lock()
            .map_err(|_| io::Error::other("native writer reservation poisoned"))?.is_some()
        {
            return Ok(());
        }
        let required = metadata.checked_add(self.build_memory)
            .ok_or_else(|| io::Error::other("native metadata memory overflow"))?;
        let mut memory = self.memory.lock().map_err(|_| io::Error::other("native metadata budget poisoned"))?;
        // Tantivy's atomic_read returns a bare Vec for its JSON metadata. Keep
        // the read and JSON decoding allowance with the directory instead.
        let additional = required.saturating_sub(memory.bytes());
        if additional > 0 {
            memory.grow(additional).map_err(io::Error::other)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct BudgetDirectory {
    root: PathBuf,
    selected: bool,
    budget: Arc<Budget>,
}
impl fmt::Debug for BudgetDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BudgetDirectory")
    }
}
impl BudgetDirectory {
    pub(super) fn owner_token(&self) -> DirectoryOwner { DirectoryOwner { _budget: self.budget.clone() } }

    pub(super) fn cancel_io_waiters(&self) {
        if let Some(scope) = &self.budget.scope { scope.cancel_native_io(&self.budget.cancelled); }
        else { self.budget.cancelled.store(true, Ordering::Release); }
    }

    #[cfg(test)]
    pub(super) fn reject_writes_for_test(&self) {
        self.budget.fail_writes.store(true, Ordering::Release);
    }
    #[cfg(test)]
    pub(super) fn failed_writes_for_test(&self) -> usize {
        self.budget.failed_writes.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(super) fn new(
        path: &Path,
        bytes: u64,
        files: usize,
        memory_bytes: u64,
    ) -> Result<Self, RecordError> {
        Self::with_scope(
            path,
            bytes,
            files,
            memory_bytes,
            resources::Scope::current(),
        )
    }
    pub(super) fn persistent(
        path: &Path,
        bytes: u64,
        files: usize,
        memory_bytes: u64,
    ) -> Result<Self, RecordError> {
        Self::with_scope(
            path,
            bytes,
            files,
            memory_bytes,
            resources::Scope::current().map(|scope| scope.retained_io()),
        )
    }
    /// Admit before the first worker starts. Directory clones retain the charge
    /// through worker shutdown, including failure/cancellation of its caller.
    pub(super) fn prepare_writer(&self,bytes:u64)->Result<Arc<WriterReservation>,RecordError>{
        if self.selected {return Err(storage("immutable native package"));}
        let reservation=self.budget.writer_memory.lock().map_err(|_|storage("native writer reservation poisoned"))?;
        if let Some(owner)=reservation.as_ref(){return Ok(owner.clone());}
        Ok(Arc::new(WriterReservation::new(bytes, self.budget.scope.as_ref())?))
    }
    pub(super) fn install_writer(&self,prepared:Arc<WriterReservation>)->Result<(),RecordError>{
        let mut reservation=self.budget.writer_memory.lock().map_err(|_|storage("native writer reservation poisoned"))?;
        if let Some(owner)=reservation.as_ref(){
            if !Arc::ptr_eq(owner,&prepared){return Err(storage("native writer workspace owner changed"));}
        }else{*reservation=Some(prepared);}
        Ok(())
    }
    pub(super) fn admit_writer(&self,bytes:u64)->Result<(),RecordError>{
        if self.selected{return Err(storage("immutable native package"));}
        let mut reservation=self.budget.writer_memory.lock().map_err(|_|storage("native writer reservation poisoned"))?;
        if reservation.is_none(){
            *reservation = Some(Arc::new(WriterReservation::new(bytes, self.budget.scope.as_ref())?));
        }
        Ok(())
    }
    /// Queued pre-tokenized documents outlive add_document's caller. Workers
    /// retain these leases through Directory clones, even if joining fails.
    pub(super) fn retain_analysis(
        &self,
        memory: resources::MemoryReservation,
    ) -> Result<(), RecordError> {
        self.budget.pending_analysis.lock().map_err(|_| storage("native analysis ownership poisoned"))?
            .push(memory);
        Ok(())
    }
    /// A successful commit has joined the workers consuming the previous queue.
    pub(super) fn release_committed_analysis(&self) {
        if let Ok(mut pending) = self.budget.pending_analysis.lock() { pending.clear(); }
    }
    // Directory clones outlive Runtime on a failed worker join. They must retain
    // the same GC pin as the worker's bytes, until the last worker/reader drops.
    pub(super) fn retain_partition_owner(&self, owner: Arc<fs::File>) -> Result<(), RecordError> {
        let mut held = self.budget.partition_owner.lock().map_err(|_| storage("native partition owner poisoned"))?;
        if let Some(existing) = held.as_ref() {
            if !Arc::ptr_eq(existing, &owner) { return Err(storage("native partition owner changed")); }
        } else { *held = Some(owner); }
        Ok(())
    }
    pub(super) fn retain_pin(&self,pin:Arc<crate::record::package_pins::Pin>)->Result<(),RecordError> {
        *self.budget.pin.lock().map_err(|_|storage("native pin ownership poisoned"))?=Some(pin);
        Ok(())
    }
    /// Called only after Tantivy has successfully joined indexing and merge
    /// workers. Fixed Searchers may retain this Directory, but no writer buffer.
    pub(super) fn release_joined_writer(&self) {
        self.release_committed_analysis();
        if let Ok(mut reservation)=self.budget.writer_memory.lock() {
            reservation.take();
        }
    }
    /// The caller has authenticated the complete immutable package closure.
    /// Keep reader metadata admission and I/O ownership with Directory clones.
    pub(super) fn open_selected(path: &Path, limit: u64) -> Result<Self, RecordError> {
        let scope = resources::Scope::current().map(|scope| scope.retained_io());
        let io = scope.as_ref().map(|s|s.native_io_wait(&AtomicBool::new(false))).transpose().map_err(storage)?;
        let io_gate=gate::for_directory(path).map_err(storage)?;
        let gate=io_gate.lock().map_err(|_|storage("native I/O gate poisoned"))?;
        let mut total=0u64;let mut metadata=65536u64;let mut count=0usize;
        for entry in fs::read_dir(path).map_err(storage)? {
            let entry=entry.map_err(storage)?;let kind=entry.file_type().map_err(storage)?;
            if !kind.is_file() {return Err(storage("native selected package contains a non-file"));}
            count+=1;if count>65536 {return Err(storage("native selected file budget exceeded"));}
            let length=entry.metadata().map_err(storage)?.len();
            total=total.checked_add(length).filter(|n|*n<=limit).ok_or_else(||storage("native selected byte budget exceeded"))?;
            if entry.path().extension().is_some_and(|s|s=="json") {
                metadata=length.checked_mul(8).and_then(|n|metadata.checked_add(n)).ok_or_else(||storage("native selected metadata overflow"))?;
            }
        }
        let memory=resources::MemoryReservation::new(metadata,scope.as_ref()).map_err(storage)?;
        let root = path.canonicalize().map_err(storage)?;
        drop(gate);drop(io);
        Ok(Self {root,selected:true,budget:Arc::new(Budget {
            io_gate,
            cancelled: AtomicBool::new(false),
            #[cfg(test)] fail_writes: AtomicBool::new(false),
            #[cfg(test)] failed_writes: std::sync::atomic::AtomicUsize::new(0),
            pin:Mutex::new(None),partition_owner:Mutex::new(None),writer_memory:Mutex::new(None),pending_analysis:Mutex::new(Vec::new()),ledger:Mutex::new(Ledger {written:0,paths:BTreeSet::new()}),bytes:0,files:1,scope,
            memory: Mutex::new(memory), build_memory: 0,
        })})
    }
    fn with_scope(
        path: &Path,
        bytes: u64,
        files: usize,
        memory_bytes: u64,
        scope: Option<resources::Scope>,
    ) -> Result<Self, RecordError> {
        let io_gate = gate::for_directory(path).map_err(storage)?;
        if fs::read_dir(path).map_err(storage)?.next().is_some() {
            return Err(storage("native candidate directory must be empty"));
        }
        let memory = resources::MemoryReservation::new(memory_bytes,scope.as_ref()).map_err(storage)?;
        Ok(Self {
            root: path.canonicalize().map_err(storage)?,
            selected: false,
            budget: Arc::new(Budget {
                io_gate,
                cancelled: AtomicBool::new(false),
                #[cfg(test)] fail_writes: AtomicBool::new(false),
                #[cfg(test)] failed_writes: std::sync::atomic::AtomicUsize::new(0),
                pin:Mutex::new(None),partition_owner:Mutex::new(None),writer_memory:Mutex::new(None),pending_analysis:Mutex::new(Vec::new()),
                ledger: Mutex::new(Ledger {
                    written: 0,
                    paths: BTreeSet::new(),
                }),
                bytes,
                files,
                scope,
                memory: Mutex::new(memory),
                build_memory: memory_bytes,
            }),
        })
    }

    fn resolve(&self, path: &Path) -> io::Result<PathBuf> {
        let mut components = path.components();
        if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "native index path must be a single file"));
        }
        Ok(self.root.join(path))
    }

    fn open_file(path: &Path) -> io::Result<(File, usize)> {
        let file = File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "native index entry is not a file"));
        }
        let length = usize::try_from(metadata.len()).map_err(io::Error::other)?;
        Ok((file, length))
    }

    fn replace_file(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let temporary = tempfile::NamedTempFile::new_in(&self.root)?.into_temp_path();
        let mut file = File::options().write(true).open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_data()?;
        drop(file);
        temporary.persist(path).map_err(|e| e.error)
    }

    fn sync_root(&self) -> io::Result<()> {
        #[cfg(unix)]
        fs::File::open(&self.root)?.sync_data()?;
        Ok(())
    }
}

struct ReadBuffer {
    bytes: Vec<u8>,
    _memory: ReadReservation,
}

enum ReadReservation {
    Owned { _memory: resources::MemoryReservation },
    Writer { _memory: WriterReadReservation },
}

impl Deref for ReadBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}
// SAFETY: this owner exposes only a slice, and moving it never moves or
// reallocates the Vec's heap allocation.
unsafe impl StableDeref for ReadBuffer {}

struct ReadHandle {
    file: File,
    length: usize,
    writer: bool,
    // The file closes before these package and partition owners can drop.
    budget: Arc<Budget>,
}
impl fmt::Debug for ReadHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandle").field("length", &self.length).finish()
    }
}
impl HasLen for ReadHandle {
    fn len(&self) -> usize {
        self.length
    }
}
impl ReadHandle {
    fn reserve(&self, bytes: u64, current: Option<&resources::Scope>) -> io::Result<ReadReservation> {
        if let Ok(Some(reservation)) = SNAPSHOT_READS.try_with(|workspace| {
            workspace.borrow_mut().as_mut().map(|memory| memory.split_off(bytes))
        }) {
            return Ok(ReadReservation::Owned { _memory: reservation.map_err(io::Error::other)? });
        }
        if self.writer
            && let Some(reservation) = self.budget.writer_memory.lock()
                .map_err(|_| io::Error::other("native writer reservation poisoned"))?
                .as_ref().and_then(|writer| writer.reserve_read(bytes))
        {
            return Ok(ReadReservation::Writer { _memory: reservation });
        }
        // Tantivy may retain a returned range in the Searcher itself. Such
        // allocations remain charged to the collection after the request ends.
        let memory = crate::record::build_budget::admit_memory(bytes, self.budget.scope.as_ref().or(current))
            .map_err(io::Error::other)?;
        Ok(ReadReservation::Owned { _memory: memory })
    }
}

impl FileHandle for ReadHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        if range.start > range.end || range.end > self.length {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid native index read range"));
        }
        if range.is_empty() {
            return Ok(OwnedBytes::empty());
        }
        let current = resources::Scope::current();
        let memory = self.reserve(range.len() as u64, current.as_ref())?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(range.len()).map_err(io::Error::other)?;
        bytes.resize(range.len(), 0);
        let mut read = || self.file.read_exact_at(range.start as u64, &mut bytes);
        match current.as_ref().or(self.budget.scope.as_ref()) {
            // Cancelling the writer must leave already published readers usable.
            Some(scope) if self.writer => scope.native_file_io(&self.budget.cancelled, read),
            Some(scope) => scope.native_file_io(&AtomicBool::new(false), read),
            None => read(),
        }?;
        Ok(OwnedBytes::new(ReadBuffer { bytes, _memory: memory }))
    }
}

struct Writer {
    inner: Option<io::BufWriter<File>>,
    budget: Arc<Budget>,
    path: PathBuf,
}
impl Drop for Writer {
    fn drop(&mut self) {
        if let Some(writer) = self.inner.take() {
            // Discard pending bytes from an abandoned writer. BufWriter's
            // Drop would otherwise attempt I/O without the directory permit.
            drop(writer.into_parts());
        }
    }
}
impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.budget.charge(&self.path, bytes.len() as u64)?;
        let writer = self.inner.as_mut().expect("native writer owns a buffer");
        self.budget.io(|| writer.write(bytes))
    }
    fn flush(&mut self) -> io::Result<()> {
        let writer = self.inner.as_mut().expect("native writer owns a buffer");
        self.budget.io(|| writer.flush())
    }
}
impl TerminatingWrite for Writer {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        let writer = self.inner.as_mut().expect("native writer owns a buffer");
        self.budget.io(|| {
            writer.flush()?;
            writer.get_ref().sync_data()
        })
    }
}
impl Directory for BudgetDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let (file, length) = self.budget.io(|| Self::open_file(&self.resolve(path)?))
            .map_err(|error| read_error(path, error))?;
        let writer = !self.selected && SNAPSHOT_READS.try_with(|_| ()).is_err();
        Ok(Arc::new(ReadHandle { file, length, writer, budget: self.budget.clone() }))
    }
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let result = if self.selected {
            Err(io::Error::other("immutable native package"))
        } else {
            self.budget.io(|| fs::remove_file(self.resolve(path)?))
        };
        result.map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                DeleteError::FileDoesNotExist(path.to_owned())
            } else {
                DeleteError::IoError { io_error: Arc::new(error), filepath: path.to_owned() }
            }
        })
    }
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.budget.io(|| self.resolve(path)?.try_exists())
            .map_err(|error| OpenReadError::wrap_io_error(error, path.to_owned()))
    }
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        if self.selected {return Err(OpenWriteError::wrap_io_error(io::Error::other("immutable native package"),path.to_owned()));}
        let file = self.budget.io(|| {
            let absolute = self.resolve(path)?;
            self.budget.charge(path, 0)?;
            File::options().write(true).create_new(true).open(absolute)
        }).map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                OpenWriteError::FileAlreadyExists(path.to_owned())
            } else {
                OpenWriteError::wrap_io_error(error, path.to_owned())
            }
        })?;
        let writer = Writer {
            inner: Some(io::BufWriter::new(file)),
            budget: self.budget.clone(),
            path: path.to_owned(),
        };
        // No extra buffering outside the accounting layer.
        Ok(io::BufWriter::with_capacity(0, Box::new(writer)))
    }
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.budget.io(|| {
            let (file, length) = Self::open_file(&self.resolve(path)?)?;
            self.budget.admit_atomic_metadata(length as u64)?;
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(length).map_err(io::Error::other)?;
            bytes.resize(length, 0);
            file.read_exact_at(0, &mut bytes)?;
            Ok(bytes)
        }).map_err(|error| read_error(path, error))
    }
    fn atomic_write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        if self.selected {return Err(io::Error::other("immutable native package"));}
        let absolute = self.resolve(path)?;
        self.budget.charge(path, bytes.len() as u64)?;
        self.budget.io(|| self.replace_file(&absolute, bytes))
    }
    fn sync_directory(&self) -> io::Result<()> {
        self.budget.io(|| self.sync_root())
    }
    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        if self.selected && lock.filepath != tantivy::directory::META_LOCK.filepath {
            return Err(LockError::wrap_io_error(io::Error::other("immutable native package")));
        }
        let path = self.resolve(&lock.filepath).map_err(LockError::wrap_io_error)?;
        self.budget.charge(&lock.filepath, 0).map_err(LockError::wrap_io_error)?;
        // Never hold scarce I/O capacity while blocking on an OS metadata
        // lock: its owner may need the same capacity to read before unlocking.
        loop {
            if self.budget.cancelled.load(Ordering::Acquire) {
                return Err(LockError::wrap_io_error(io::Error::other("native lock cancelled")));
            }
            let result = self.budget.io(|| {
                let file = File::options().write(true).create(true).truncate(false).open(&path)?;
                file.try_lock().map_err(|error| match error {
                    fs::TryLockError::WouldBlock => io::Error::from(io::ErrorKind::WouldBlock),
                    fs::TryLockError::Error(error) => error,
                })?;
                Ok(DirectoryLock::from(Box::new(NativeLock { _file: file, _budget: self.budget.clone() })))
            });
            match result {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if !lock.is_blocking {
                        return Err(LockError::LockBusy);
                    }
                    // No lease or native gate is retained between attempts.
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                result => return result.map_err(LockError::wrap_io_error),
            }
        }
    }
    fn watch(&self, _: WatchCallback) -> tantivy::Result<WatchHandle> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "native index readers require manual reload").into())
    }
}

struct NativeLock {
    _file: File,
    _budget: Arc<Budget>,
}

fn read_error(path: &Path, error: io::Error) -> OpenReadError {
    if error.kind() == io::ErrorKind::NotFound {
        OpenReadError::FileDoesNotExist(path.to_owned())
    } else {
        OpenReadError::wrap_io_error(error, path.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::lexical::tests::Temp;

    #[test]
    fn directory_gates_share_aliases_and_isolate_other_directories() {
        let root = tempfile::tempdir().unwrap();
        let first = root.path().join("first");
        let second = root.path().join("second");
        fs::create_dir(&first).unwrap();
        fs::create_dir(&second).unwrap();
        let a = BudgetDirectory::persistent(&first, 65536, 16, 0).unwrap();
        let alias = BudgetDirectory::persistent(&first.join("."), 65536, 16, 0).unwrap();
        let b = BudgetDirectory::persistent(&second, 65536, 16, 0).unwrap();
        assert!(Arc::ptr_eq(&a.budget.io_gate, &alias.budget.io_gate));
        assert!(!Arc::ptr_eq(&a.budget.io_gate, &b.budget.io_gate));
        a.atomic_write(Path::new("payload"), b"value").unwrap();
        let selected = BudgetDirectory::open_selected(&first, 65536).unwrap();
        assert!(Arc::ptr_eq(&a.budget.io_gate, &selected.budget.io_gate));
        let clone = a.clone();
        let held = a.budget.io_gate.lock().unwrap();
        assert!(clone.budget.io_gate.try_lock().is_err());
        assert!(selected.budget.io_gate.try_lock().is_err());
        // A blocked index must not stop another index's durable publication.
        let (done, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            b.atomic_write(Path::new("independent"), b"progress").unwrap();
            b.sync_directory().unwrap();
            done.send(()).unwrap();
        });
        let completed = receive.recv_timeout(std::time::Duration::from_secs(5));
        drop(held);
        worker.join().unwrap();
        completed.unwrap();
        drop(a);
        assert_eq!(selected.atomic_read(Path::new("payload")).unwrap(), b"value");
        assert!(Arc::ptr_eq(&clone.budget.io_gate, &alias.budget.io_gate));
    }

    #[cfg(unix)]
    #[test]
    fn directory_gates_resolve_symlinks_and_filesystem_identity() {
        let root = tempfile::tempdir().unwrap();
        let actual = root.path().join("actual");
        let alias = root.path().join("alias");
        fs::create_dir(&actual).unwrap();
        std::os::unix::fs::symlink(&actual, &alias).unwrap();
        let a = gate::for_directory(&actual).unwrap();
        let b = gate::for_directory(&alias).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        let renamed = root.path().join("renamed");
        fs::rename(&actual, &renamed).unwrap();
        assert!(Arc::ptr_eq(&a, &gate::for_directory(&renamed).unwrap()));
        let weak = Arc::downgrade(&a);
        drop(a);
        drop(b);
        assert!(weak.upgrade().is_none());
        assert!(gate::for_directory(&renamed).unwrap().try_lock().is_ok());
    }

    #[test]
    fn contended_metadata_lock_releases_io_for_lock_owner_and_can_cancel() {
        let root = Temp::new(); fs::create_dir(&root.0).unwrap();
        let scope = Arc::new(resources::Governor::default())
            .admit("meta-lock".into(), resources::Lane::Background, 0).unwrap();
        let directory = scope.run_sync(|| BudgetDirectory::persistent(&root.0, 65536, 16, 0)).unwrap();
        directory.atomic_write(Path::new("payload"), b"value").unwrap();
        let held = directory.acquire_lock(&tantivy::directory::META_LOCK).unwrap();
        let waiter = directory.clone();
        let (done, finished) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || { done.send(waiter.acquire_lock(&tantivy::directory::META_LOCK).is_ok()).unwrap(); });
        std::thread::sleep(std::time::Duration::from_millis(20));
        let reader = directory.clone();
        let (read, read_done) = std::sync::mpsc::channel();
        let reading = std::thread::spawn(move || { read.send(reader.atomic_read(Path::new("payload"))).unwrap(); });
        assert_eq!(read_done.recv_timeout(std::time::Duration::from_secs(5)).unwrap().unwrap(), b"value");
        reading.join().unwrap();
        assert!(finished.try_recv().is_err());
        drop(held);
        assert!(finished.recv_timeout(std::time::Duration::from_secs(5)).unwrap());
        worker.join().unwrap();
        let held = directory.acquire_lock(&tantivy::directory::META_LOCK).unwrap();
        let waiter = directory.clone();
        let (done, finished) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || { done.send(waiter.acquire_lock(&tantivy::directory::META_LOCK).is_err()).unwrap(); });
        std::thread::sleep(std::time::Duration::from_millis(20));
        directory.cancel_io_waiters();
        assert!(finished.recv_timeout(std::time::Duration::from_secs(5)).unwrap());
        worker.join().unwrap(); drop(held);
    }
    #[test]
    fn detached_directory_worker_retains_package_pin_and_reservation() {
        let root=Temp::new();fs::create_dir(&root.0).unwrap();
        let directory=BudgetDirectory::persistent(&root.0,1024,8,0).unwrap();
        directory.retain_pin(crate::record::package_pins::Pin::acquire(&[root.0.clone()]).unwrap()).unwrap();
        directory.admit_writer(20_000_000).unwrap();
        directory.retain_analysis(resources::MemoryReservation::new(4096,None).unwrap()).unwrap();
        let worker=directory.clone();drop(directory);
        assert!(worker.budget.writer_memory.lock().unwrap().is_some());
        assert_eq!(worker.budget.pending_analysis.lock().unwrap().len(), 1);
        assert!(!crate::record::package_pins::retire_if_unpinned(&root.0,||{panic!("worker owns package")}).unwrap());
        drop(worker);
        assert!(crate::record::package_pins::retire_if_unpinned(&root.0,||Ok(())).unwrap());
    }

    #[test]
    fn range_buffers_retain_admission_and_handles_protect_the_package() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("package");
        fs::create_dir(&package).unwrap();
        let scope = Arc::new(resources::Governor::default())
            .admit("range-reads".into(), resources::Lane::Background, 0).unwrap();
        let directory = scope.clone().run_sync(|| BudgetDirectory::persistent(&package, 1024, 8, 0)).unwrap();
        directory.retain_pin(crate::record::package_pins::Pin::acquire(&[package.clone()]).unwrap()).unwrap();
        let path = package.join("postings");
        fs::write(&path, b"abcdefgh").unwrap();
        let handle = directory.get_file_handle(Path::new("postings")).unwrap();
        drop(directory);

        let budget = scope.bytes(2048 * 1024 * 1024 - 3).unwrap();
        assert_eq!(super::super::native_io_error(handle.read_bytes(2..6).unwrap_err()), RecordError::Busy);
        let bytes = handle.read_bytes(3..6).unwrap();
        let tail = bytes.slice(1..3);
        drop(bytes);
        assert!(scope.bytes(1).is_err());
        assert_eq!(tail.as_slice(), b"ef");
        assert!(!crate::record::package_pins::retire_if_unpinned(&package, || panic!("open handle lost its pin")).unwrap());
        drop(budget);

        fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(4).unwrap();
        assert_eq!(handle.read_bytes(3..6).unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
        drop(handle);
        assert!(crate::record::package_pins::retire_if_unpinned(&package, || {
            fs::remove_dir_all(&package).map_err(storage)
        }).unwrap());
        assert_eq!(tail.as_slice(), b"ef");
        drop(tail);
        assert!(scope.bytes(2048 * 1024 * 1024).is_ok());
    }

    #[test]
    fn selected_package_admits_reads_but_rejects_mutation() {
        let root=Temp::new();fs::create_dir(&root.0).unwrap();
        fs::write(root.0.join("a"),b"payload").unwrap();
        assert!(BudgetDirectory::open_selected(&root.0,6).is_err());
        let selected=BudgetDirectory::open_selected(&root.0,7).unwrap();
        assert_eq!(selected.atomic_read(Path::new("a")).unwrap(),b"payload");
        assert!(selected.atomic_write(Path::new("a"),b"changed").is_err());
        assert!(selected.open_write(Path::new("b")).is_err());
        assert!(selected.delete(Path::new("a")).is_err());
        assert!(selected.acquire_lock(&tantivy::directory::INDEX_WRITER_LOCK).is_err());
        assert_eq!(fs::read(root.0.join("a")).unwrap(),b"payload");
    }

    #[tokio::test]
    async fn native_workers_serialize_single_subject_io_capacity() {
        let root = Temp::new();
        fs::create_dir(&root.0).unwrap();
        let governor = Arc::new(resources::Governor::default());
        let scope = governor.admit("native-io".into(), resources::Lane::Foreground, 0).unwrap();
        let directory = scope.run(async { BudgetDirectory::persistent(&root.0, 1024, 8, 0).unwrap() }).await;
        directory.atomic_write(Path::new("a"), b"content").unwrap();
        std::thread::scope(|threads| {
            for _ in 0..8 {
                let directory = &directory;
                threads.spawn(move || {
                    for _ in 0..100 {
                        assert_eq!(directory.atomic_read(Path::new("a")).unwrap(), b"content");
                    }
                });
            }
        });
    }

    #[test]
    fn reserve_before_native_write_and_no_refund_on_delete() {
        let root = Temp::new();
        fs::create_dir(&root.0).unwrap();
        let dir = BudgetDirectory::new(&root.0, 4, 2, 0).unwrap();
        dir.atomic_write(Path::new("a"), b"1234").unwrap();
        assert!(dir.atomic_write(Path::new("b"), b"x").is_err());
        assert!(!root.0.join("b").exists());
        dir.delete(Path::new("a")).unwrap();
        assert!(dir.atomic_write(Path::new("b"), b"x").is_err());
        assert!(dir.open_write(Path::new("../escape")).is_err());
    }

    #[test]
    fn native_streams_and_atomic_writes_share_one_file_budget() {
        let root = Temp::new();
        fs::create_dir(&root.0).unwrap();
        let dir = BudgetDirectory::new(&root.0, 8, 1, 0).unwrap();
        let mut writer = dir.open_write(Path::new("a")).unwrap();
        writer.write_all(b"1234").unwrap();
        assert!(writer.write_all(b"12345").is_err());
        writer.terminate().unwrap();
        assert!(dir.atomic_write(Path::new("b"), b"1").is_err());
        assert!(dir.clone().open_write(Path::new("b")).is_err());
    }

    #[tokio::test]
    async fn worker_directory_retains_accepted_task_and_bytes_after_caller_drops() {
        let root = Temp::new();
        fs::create_dir(&root.0).unwrap();
        let governor = Arc::new(resources::Governor::default());
        let lane = resources::Lane::Foreground;
        let scope = governor.admit("collection".into(), lane, 0).unwrap();
        let directory = scope
            .clone()
            .run(async { BudgetDirectory::new(&root.0, 1024, 8, 20_000_000).unwrap() })
            .await;
        let mut writer = directory.open_write(Path::new("owned")).unwrap();
        drop(directory);
        drop(scope);
        let other = (0..15)
            .map(|_| governor.admit("collection".into(), lane, 0).unwrap())
            .collect::<Vec<_>>();
        assert!(governor.admit("collection".into(), lane, 0).is_err());
        // The directory is gone; the actual writer still owns its charge.
        writer.write_all(b"still running").unwrap();
        writer.terminate().unwrap();
        assert!(governor.admit("collection".into(), lane, 0).is_ok());
        drop(other);
    }
}

// Explicit diagnostic only: no timing or environment switches in production.
#[cfg(test)]
#[path = "directory_diagnostic.rs"]
mod diagnostic;
