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

struct Ledger {
    written: u64,
    paths: BTreeSet<PathBuf>,
}
struct Budget {
    ledger: Mutex<Ledger>,
    bytes: u64,
    files: usize,
    scope: Option<resources::Scope>,
    // Native workers own Directory clones, so even an early error cannot free
    // the reservation while an internal worker still owns the candidate.
    _memory: crate::storage::build_budget::MemoryLease,
    _buffer: Option<resources::Lease>,
}
impl Budget {
    fn charge(&self, path: &Path, bytes: u64) -> io::Result<()> {
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
            .ok_or_else(|| io::Error::other("native writer byte budget exceeded"))?;
        if !ledger.paths.contains(path) && ledger.paths.len() >= self.files {
            return Err(io::Error::other("native writer file budget exceeded"));
        }
        ledger.paths.insert(path.to_owned());
        ledger.written = next;
        // Conservative: failed writes, deletes and atomic replacements never
        // refund charged traffic. Each new private build gets a fresh ledger.
        Ok(())
    }
    fn io(&self) -> io::Result<Option<resources::Lease>> {
        self.scope
            .as_ref()
            .map(|s| s.io())
            .transpose()
            .map_err(io::Error::other)
    }
}

#[derive(Clone)]
pub(super) struct BudgetDirectory {
    inner: MmapDirectory,
    budget: Arc<Budget>,
}
impl fmt::Debug for BudgetDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BudgetDirectory")
    }
}
impl BudgetDirectory {
    pub(super) fn new(
        path: &Path,
        bytes: u64,
        files: usize,
        memory_bytes: u64,
    ) -> Result<Self, RecordError> {
        if std::fs::read_dir(path).map_err(storage)?.next().is_some() {
            return Err(storage("native candidate directory must be empty"));
        }
        let scope = resources::Scope::current();
        let memory = crate::storage::build_budget::reserve_memory(memory_bytes).map_err(storage)?;
        let buffer = scope
            .as_ref()
            .map(|s| s.bytes(memory_bytes))
            .transpose()
            .map_err(storage)?;
        Ok(Self {
            inner: MmapDirectory::open(path).map_err(storage)?,
            budget: Arc::new(Budget {
                ledger: Mutex::new(Ledger {
                    written: 0,
                    paths: BTreeSet::new(),
                }),
                bytes,
                files,
                scope,
                _memory: memory,
                _buffer: buffer,
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
        self.inner.delete(path)
    }
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }
    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
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
        self.budget.charge(path, bytes.len() as u64)?;
        let _io = self.budget.io()?;
        self.inner.atomic_write(path, bytes)
    }
    fn sync_directory(&self) -> io::Result<()> {
        let _io = self.budget.io()?;
        self.inner.sync_directory()
    }
    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.budget.charge(&lock.filepath, 0).map_err(LockError::wrap_io_error)?;
        let _io = self.budget.io().map_err(LockError::wrap_io_error)?;
        self.inner.acquire_lock(lock)
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
        let other = (0..3)
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
