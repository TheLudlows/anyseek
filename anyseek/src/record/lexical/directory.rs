//! Native writer accounting captured before Tantivy spawns its own threads.
//! This changes file access, never Tantivy's internal encoding.
use crate::{
    record::{RecordError, storage},
    resources,
};
use std::{
    collections::BTreeSet,
    fmt,
    io::{self, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};
use tantivy::directory::{
    AntiCallToken, Directory, DirectoryLock, FileHandle, Lock, MmapDirectory, TerminatingWrite,
    WatchCallback, WatchHandle, WritePtr,
    error::{DeleteError, LockError, OpenReadError, OpenWriteError},
};

// Native merge, indexing and reload share a gate per physical directory.
// Independent collections must not wait for each other's writes or disk syncs.
// Acquire capacity before the gate; retain neither between operations.
#[path = "directory_gate.rs"]
mod gate;

struct Ledger {
    written: u64,
    paths: BTreeSet<PathBuf>,
}
pub(super) type WriterReservation = resources::MemoryReservation;
struct Budget {
    io_gate: Arc<Mutex<()>>,
    cancelled: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fail_writes: std::sync::atomic::AtomicBool,
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
    _memory: resources::MemoryReservation,
    // Directory clones retain the OS owner lock after a public handle drops.
    partition_owner: Mutex<Option<Arc<std::fs::File>>>,
}
// Snapshot keeps this after its Searcher: Tantivy drops Index before its mmap
// readers, so the Index's Directory alone is not a sufficient lifetime barrier.
#[derive(Clone)]
pub(super) struct DirectoryOwner { _budget: Arc<Budget> }
impl Budget {
    fn charge(&self, path: &Path, bytes: u64) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_writes.load(std::sync::atomic::Ordering::Acquire) {
            self.failed_writes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(io::Error::other("injected native worker write failure"));
        }
        let mut components = path.components();
        if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
            return Err(io::Error::other("native index path must be a single file"));
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
    fn io(&self) -> io::Result<(Option<resources::Lease>, std::sync::MutexGuard<'_, ()>)> {
        let lease = self.scope
            .as_ref()
            .map(|s| s.native_io_wait(&self.cancelled))
            .transpose()
            .map_err(io::Error::other)?;
        let gate = self.io_gate.lock().map_err(|_| io::Error::other("native I/O gate poisoned"))?;
        Ok((lease, gate))
    }
}

#[derive(Clone)]
pub(super) struct BudgetDirectory {
    inner: MmapDirectory,
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
        else { self.budget.cancelled.store(true, std::sync::atomic::Ordering::Release); }
    }

    #[cfg(test)]
    pub(super) fn reject_writes_for_test(&self) {
        self.budget.fail_writes.store(true, std::sync::atomic::Ordering::Release);
    }
    #[cfg(test)]
    pub(super) fn failed_writes_for_test(&self) -> usize {
        self.budget.failed_writes.load(std::sync::atomic::Ordering::Acquire)
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
        Ok(Arc::new(crate::record::build_budget::admit_memory(bytes,self.budget.scope.as_ref())?))
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
            let memory=resources::MemoryReservation::new(bytes,self.budget.scope.as_ref()).map_err(storage)?;
            *reservation=Some(Arc::new(memory));
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
    pub(super) fn retain_partition_owner(&self, owner: Arc<std::fs::File>) -> Result<(), RecordError> {
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
        let io = scope.as_ref().map(|s|s.native_io_wait(&std::sync::atomic::AtomicBool::new(false))).transpose().map_err(storage)?;
        let io_gate=gate::for_directory(path).map_err(storage)?;
        let gate=io_gate.lock().map_err(|_|storage("native I/O gate poisoned"))?;
        let mut total=0u64;let mut metadata=65536u64;let mut count=0usize;
        for entry in std::fs::read_dir(path).map_err(storage)? {
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
        let inner=MmapDirectory::open(path).map_err(storage)?;
        drop(gate);drop(io);
        Ok(Self {inner,selected:true,budget:Arc::new(Budget {
            io_gate,
            cancelled: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)] fail_writes: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)] failed_writes: std::sync::atomic::AtomicUsize::new(0),
            pin:Mutex::new(None),partition_owner:Mutex::new(None),writer_memory:Mutex::new(None),pending_analysis:Mutex::new(Vec::new()),ledger:Mutex::new(Ledger {written:0,paths:BTreeSet::new()}),bytes:0,files:1,scope,
            _memory:memory,
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
        if std::fs::read_dir(path).map_err(storage)?.next().is_some() {
            return Err(storage("native candidate directory must be empty"));
        }
        let memory = resources::MemoryReservation::new(memory_bytes,scope.as_ref()).map_err(storage)?;
        Ok(Self {
            inner: MmapDirectory::open(path).map_err(storage)?,
            selected: false,
            budget: Arc::new(Budget {
                io_gate,
                cancelled: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)] fail_writes: std::sync::atomic::AtomicBool::new(false),
                #[cfg(test)] failed_writes: std::sync::atomic::AtomicUsize::new(0),
                pin:Mutex::new(None),partition_owner:Mutex::new(None),writer_memory:Mutex::new(None),pending_analysis:Mutex::new(Vec::new()),
                ledger: Mutex::new(Ledger {
                    written: 0,
                    paths: BTreeSet::new(),
                }),
                bytes,
                files,
                scope,
                _memory: memory,
            }),
        })
    }
}
struct Writer {
    inner: WritePtr,
    budget: Arc<Budget>,
    path: PathBuf,
}
impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.budget.charge(&self.path, bytes.len() as u64)?;
        let _io = self.budget.io()?;
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        let _io = self.budget.io()?;
        self.inner.flush()
    }
}
impl TerminatingWrite for Writer {
    fn terminate_ref(&mut self, token: AntiCallToken) -> io::Result<()> {
        let _io = self.budget.io()?;
        self.inner.terminate_ref(token)
    }
}
impl Directory for BudgetDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let _io = self
            .budget
            .io()
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_owned()))?;
        self.inner.get_file_handle(path)
    }
    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        if self.selected {return Err(DeleteError::IoError {io_error:Arc::new(io::Error::other("immutable native package")),filepath:path.to_owned()});}
        let _io=self.budget.io().map_err(|error|DeleteError::IoError {io_error:Arc::new(error),filepath:path.to_owned()})?;
        self.inner.delete(path)
    }
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        let _io=self.budget.io().map_err(|e|OpenReadError::wrap_io_error(e,path.to_owned()))?;
        self.inner.exists(path)
    }
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        if self.selected {return Err(OpenWriteError::wrap_io_error(io::Error::other("immutable native package"),path.to_owned()));}
        self.budget
            .charge(path, 0)
            .map_err(|e| OpenWriteError::wrap_io_error(e, path.to_owned()))?;
        let _io = self
            .budget
            .io()
            .map_err(|e| OpenWriteError::wrap_io_error(e, path.to_owned()))?;
        let writer = Writer {
            inner: self.inner.open_write(path)?,
            budget: self.budget.clone(),
            path: path.to_owned(),
        };
        // No extra buffering outside the accounting layer.
        Ok(io::BufWriter::with_capacity(0, Box::new(writer)))
    }
    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let _io = self
            .budget
            .io()
            .map_err(|e| OpenReadError::wrap_io_error(e, path.to_owned()))?;
        self.inner.atomic_read(path)
    }
    fn atomic_write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        if self.selected {return Err(io::Error::other("immutable native package"));}
        self.budget.charge(path, bytes.len() as u64)?;
        let _io = self.budget.io()?;
        self.inner.atomic_write(path, bytes)
    }
    fn sync_directory(&self) -> io::Result<()> {
        let _io = self.budget.io()?;
        self.inner.sync_directory()
    }
    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        if self.selected && lock.filepath != tantivy::directory::META_LOCK.filepath {
            return Err(LockError::wrap_io_error(io::Error::other("immutable native package")));
        }
        self.budget
            .charge(&lock.filepath, 0)
            .map_err(LockError::wrap_io_error)?;
        // Never hold scarce I/O capacity while blocking on an OS metadata
        // lock: its owner may need the same capacity to read before unlocking.
        let attempt = Lock { filepath: lock.filepath.clone(), is_blocking: false };
        loop {
            if self.budget.cancelled.load(std::sync::atomic::Ordering::Acquire) {
                return Err(LockError::wrap_io_error(io::Error::other("native lock cancelled")));
            }
            let result = {
                let _io = self.budget.io().map_err(LockError::wrap_io_error)?;
                self.inner.acquire_lock(&attempt)
            };
            match result {
                Err(LockError::LockBusy) if lock.is_blocking => {
                    // Only dedicated native/blocking workers reach this path.
                    // No lease or native gate is retained between attempts.
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                result => return result,
            }
        }
    }
    fn watch(&self, callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(callback)
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
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
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
        std::fs::create_dir(&actual).unwrap();
        std::os::unix::fs::symlink(&actual, &alias).unwrap();
        let a = gate::for_directory(&actual).unwrap();
        let b = gate::for_directory(&alias).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        let renamed = root.path().join("renamed");
        std::fs::rename(&actual, &renamed).unwrap();
        assert!(Arc::ptr_eq(&a, &gate::for_directory(&renamed).unwrap()));
        let weak = Arc::downgrade(&a);
        drop(a);
        drop(b);
        assert!(weak.upgrade().is_none());
        assert!(gate::for_directory(&renamed).unwrap().try_lock().is_ok());
    }

    #[test]
    fn contended_metadata_lock_releases_io_for_lock_owner_and_can_cancel() {
        let root = Temp::new(); std::fs::create_dir(&root.0).unwrap();
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
        let root=Temp::new();std::fs::create_dir(&root.0).unwrap();
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
    fn selected_package_admits_reads_but_rejects_mutation() {
        let root=Temp::new();std::fs::create_dir(&root.0).unwrap();
        std::fs::write(root.0.join("a"),b"payload").unwrap();
        assert!(BudgetDirectory::open_selected(&root.0,6).is_err());
        let selected=BudgetDirectory::open_selected(&root.0,7).unwrap();
        assert_eq!(selected.atomic_read(Path::new("a")).unwrap(),b"payload");
        assert!(selected.atomic_write(Path::new("a"),b"changed").is_err());
        assert!(selected.open_write(Path::new("b")).is_err());
        assert!(selected.delete(Path::new("a")).is_err());
        assert!(selected.acquire_lock(&tantivy::directory::INDEX_WRITER_LOCK).is_err());
        assert_eq!(std::fs::read(root.0.join("a")).unwrap(),b"payload");
    }

    #[tokio::test]
    async fn native_workers_serialize_single_subject_io_capacity() {
        let root = Temp::new();
        std::fs::create_dir(&root.0).unwrap();
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
        std::fs::create_dir(&root.0).unwrap();
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
        std::fs::create_dir(&root.0).unwrap();
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
        std::fs::create_dir(&root.0).unwrap();
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
