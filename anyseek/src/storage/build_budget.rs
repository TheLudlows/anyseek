//! Scoped Record build accounting, not a process allocator or RSS limit.
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Seek, SeekFrom, Write},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        Arc, LazyLock, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
const SHARED_MEMORY: u64 = 4 * 1024 * 1024 * 1024;
pub(crate) const BUILD_MEMORY: u64 = 1024 * 1024 * 1024;
pub(crate) const EXPLICIT_BUFFERS: u64 = 64 * 1024 * 1024;
static MEMORY: LazyLock<Arc<AtomicU64>> = LazyLock::new(|| Arc::new(AtomicU64::new(0)));
#[derive(Debug, Clone, Copy)]
struct MemoryLimits { single: u64, shared: u64 }
impl MemoryLimits {
    fn parse(single: Option<String>, shared: Option<String>) -> Result<Self, String> {
        let read = |value: Option<String>, fallback, key| value.map_or(Ok(fallback), |s| s.parse::<u64>().map_err(|_| format!("invalid {key}")));
        let single = read(single, BUILD_MEMORY, "ANYSEEK_BUILD_MEMORY_BYTES")?;
        let shared = read(shared, SHARED_MEMORY, "ANYSEEK_BUILD_SHARED_MEMORY_BYTES")?;
        if single < 64 * 1024 * 1024 || single > shared || shared > 8 * 1024 * 1024 * 1024 {
            return Err("invalid build memory hierarchy: require 64MiB <= single <= shared <= 8GiB".into());
        }
        Ok(Self { single, shared })
    }
}
static MEMORY_LIMITS: LazyLock<Result<MemoryLimits, String>> = LazyLock::new(|| {
    let read = |key| match std::env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(format!("invalid {key}")),
    };
    MemoryLimits::parse(read("ANYSEEK_BUILD_MEMORY_BYTES")?, read("ANYSEEK_BUILD_SHARED_MEMORY_BYTES")?)
});
pub(crate) fn validate_configuration() -> Result<(), String> { MEMORY_LIMITS.as_ref().map(|_| ()).map_err(Clone::clone) }
fn memory_limits() -> io::Result<MemoryLimits> { MEMORY_LIMITS.as_ref().copied().map_err(|e| io::Error::other(e.clone())) }

fn exceeded() -> io::Error {
    io::Error::other("record build resource budget exceeded")
}
fn io_admission() -> io::Result<Option<crate::resources::Lease>> {
    crate::resources::Scope::current().map(|scope| scope.io()).transpose().map_err(io::Error::other)
}
pub(crate) struct MemoryLease(u64, Arc<AtomicU64>);
pub(crate) fn reserve_memory(bytes: u64) -> io::Result<MemoryLease> {
    reserve_from(&MEMORY, bytes)
}
// Validate recoverability without charging for a second allocation that is not made.
pub(crate) fn validate_single(bytes:u64)->io::Result<()> {
    let limits=memory_limits()?;
    if bytes > limits.single { return Err(io::Error::other(format!("record build resource budget exceeded: class=single requested={bytes} limit={}",limits.single))); }
    Ok(())
}
fn reserve_from(counter: &Arc<AtomicU64>, bytes: u64) -> io::Result<MemoryLease> {
    let limits = memory_limits()?;
    if bytes > limits.single {
        log::warn!("record build single reservation rejected: requested={bytes} limit={}", limits.single);
        return Err(io::Error::other(format!("record build resource budget exceeded: class=single requested={bytes} limit={}", limits.single)));
    }
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
            used.checked_add(bytes).filter(|n| *n <= limits.shared)
        })
        .map_err(|used| {
            log::warn!("record build shared reservation rejected: used={used} requested={bytes} limit={}", limits.shared);
            io::Error::other(format!("record build resource budget exceeded: class=shared used={used} requested={bytes} limit={}", limits.shared))
        })?;
    Ok(MemoryLease(bytes, counter.clone()))
}
impl MemoryLease {
    pub(crate) fn can_grow(&self, bytes: u64) -> bool {
        memory_limits().is_ok_and(|limits| self.0.checked_add(bytes).is_some_and(|n| n <= limits.single))
    }
    #[track_caller]
    pub(crate) fn grow(&mut self, bytes: u64) -> io::Result<()> {
        let limits = memory_limits()?;
        let next = self
            .0
            .checked_add(bytes)
            .filter(|n| *n <= limits.single)
            .ok_or_else(|| io::Error::other(format!("record build resource budget exceeded: class=single_growth used={} requested={bytes} limit={} caller={}", self.0, limits.single, std::panic::Location::caller())))?;
        let mut added = reserve_from(&self.1, bytes)?;
        self.0 = next;
        added.0 = 0;
        Ok(())
    }
}
impl Drop for MemoryLease {
    fn drop(&mut self) {
        self.1.fetch_sub(self.0, Ordering::AcqRel);
    }
}
struct Ledger {
    used: u64,
    files: BTreeMap<PathBuf, u64>,
}
struct Disk {
    limit: u64,
    ledger: Mutex<Ledger>,
    building: AtomicBool,
}
static SCOPES: LazyLock<Mutex<BTreeMap<PathBuf, Weak<Disk>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));
pub(crate) struct DiskScope {
    disk: Arc<Disk>,
}
impl DiskScope {
    pub(crate) fn begin(root: &Path, limit: u64) -> io::Result<Self> {
        let root = fs::canonicalize(root)?;
        let mut ledger = Ledger {
            used: 0,
            files: BTreeMap::new(),
        };
        fn scan(path: &Path, l: &mut Ledger, cap: u64) -> io::Result<()> {
            for e in fs::read_dir(path)? {
                let e = e?;
                let k = e.file_type()?;
                if k.is_symlink() {
                    return Err(io::Error::other("build budget rejects symlink"));
                }
                if k.is_dir() {
                    scan(&e.path(), l, cap)?;
                } else if k.is_file() {
                    let n = e.metadata()?.len();
                    l.used = l
                        .used
                        .checked_add(n)
                        .filter(|n| *n <= cap)
                        .ok_or_else(exceeded)?;
                    l.files.insert(e.path(), n);
                } else {
                    return Err(io::Error::other("build budget rejects special file"));
                }
            }
            Ok(())
        }
        scan(&root, &mut ledger, limit)?;
        let mut scopes = SCOPES.lock().map_err(|_| exceeded())?;
        scopes.retain(|_, w| w.strong_count() > 0);
        let disk = if let Some(d) = scopes.get(&root).and_then(Weak::upgrade) {
            if d.limit != limit || d.building.swap(true, Ordering::AcqRel) {
                return Err(io::Error::other("overlapping record build"));
            }
            *d.ledger.lock().map_err(|_| exceeded())? = ledger;
            d
        } else {
            if scopes
                .iter()
                .any(|(p, _)| root.starts_with(p) || p.starts_with(&root))
            {
                return Err(io::Error::other("overlapping record build roots"));
            }
            Arc::new(Disk {
                limit,
                ledger: Mutex::new(ledger),
                building: AtomicBool::new(true),
            })
        };
        scopes.insert(root.clone(), Arc::downgrade(&disk));
        Ok(Self { disk })
    }
}
impl Drop for DiskScope {
    fn drop(&mut self) {
        self.disk.building.store(false, Ordering::Release);
    }
}
fn lookup(path: &Path) -> io::Result<Option<Arc<Disk>>> {
    Ok(SCOPES
        .lock()
        .map_err(|_| exceeded())?
        .iter()
        .find_map(|(root, w)| path.starts_with(root).then(|| w.upgrade()).flatten()))
}
pub(crate) fn ensure_scope(root: &Path, limit: u64) -> io::Result<Option<DiskScope>> {
    let root = fs::canonicalize(root)?;
    if lookup(&root)?.is_some() {
        Ok(None)
    } else {
        DiskScope::begin(&root, limit).map(Some)
    }
}
/// Only the Record build's registered subtree is charged. Other library files
/// keep normal behavior. No refunds on truncate, failed writes, rename or unlink:
/// a new, serialized build rescans actual retained files after GC.
pub(crate) struct File {
    inner: fs::File,
    path: PathBuf,
    charge: Option<Arc<Disk>>,
    append: bool,
}
impl File {
    pub(crate) fn options() -> OpenOptions {
        OpenOptions {
            inner: fs::OpenOptions::new(),
            append: false,
        }
    }
    pub(crate) fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::options().read(true).open(path)
    }
    pub(crate) fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::options()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }
    fn charge(&self, end: u64) -> io::Result<()> {
        if let Some(disk) = lookup(&self.path)?.or_else(|| self.charge.clone()) {
            let path = &self.path;
            let mut l = disk.ledger.lock().map_err(|_| exceeded())?;
            let old = *l.files.get(path).unwrap_or(&0);
            let growth = end.saturating_sub(old);
            let used = l
                .used
                .checked_add(growth)
                .filter(|n| *n <= disk.limit)
                .ok_or_else(exceeded)?;
            l.used = used;
            l.files.insert(path.clone(), old.max(end));
        }
        Ok(())
    }
    pub(crate) fn set_len(&self, len: u64) -> io::Result<()> {
        let _io = io_admission()?;
        self.charge(len)?;
        self.inner.set_len(len)
    }
}
impl Deref for File {
    type Target = fs::File;
    fn deref(&self) -> &fs::File {
        &self.inner
    }
}
impl Write for File {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let _io = io_admission()?;
        let end = (if self.append {
            self.inner.metadata()?.len()
        } else {
            self.inner.stream_position()?
        })
        .checked_add(b.len() as u64)
        .ok_or_else(exceeded)?;
        self.charge(end)?;
        self.inner.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
impl Write for &File {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let _io = io_admission()?;
        let mut f = &self.inner;
        let end = (if self.append {
            f.metadata()?.len()
        } else {
            f.stream_position()?
        })
        .checked_add(b.len() as u64)
        .ok_or_else(exceeded)?;
        self.charge(end)?;
        f.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        (&self.inner).flush()
    }
}
impl Read for File {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let _io = io_admission()?;
        self.inner.read(b)
    }
}
impl Seek for File {
    fn seek(&mut self, p: SeekFrom) -> io::Result<u64> {
        self.inner.seek(p)
    }
}
#[cfg(unix)]
impl std::os::fd::AsRawFd for File {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.inner)
    }
}
#[cfg(windows)]
impl std::os::windows::io::AsRawHandle for File {
    fn as_raw_handle(&self) -> std::os::windows::io::RawHandle {
        std::os::windows::io::AsRawHandle::as_raw_handle(&self.inner)
    }
}
pub(crate) struct OpenOptions {
    inner: fs::OpenOptions,
    append: bool,
}
impl OpenOptions {
    pub(crate) fn new() -> Self {
        Self {
            inner: fs::OpenOptions::new(),
            append: false,
        }
    }
    pub(crate) fn append(&mut self, v: bool) -> &mut Self {
        self.append = v;
        self.inner.append(v);
        self
    }
    pub(crate) fn read(&mut self, v: bool) -> &mut Self {
        self.inner.read(v);
        self
    }
    pub(crate) fn write(&mut self, v: bool) -> &mut Self {
        self.inner.write(v);
        self
    }
    pub(crate) fn create(&mut self, v: bool) -> &mut Self {
        self.inner.create(v);
        self
    }
    pub(crate) fn create_new(&mut self, v: bool) -> &mut Self {
        self.inner.create_new(v);
        self
    }
    pub(crate) fn truncate(&mut self, v: bool) -> &mut Self {
        self.inner.truncate(v);
        self
    }
    pub(crate) fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        let _io = io_admission()?;
        let path = path.as_ref();
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        // Resolve the parent so relative aliases cannot escape registered scope.
        let absolute = fs::canonicalize(absolute.parent().ok_or_else(exceeded)?)?
            .join(absolute.file_name().ok_or_else(exceeded)?);
        let charge = lookup(&absolute)?;
        let inner = self.inner.open(path)?;
        let file = File {
            inner,
            path: absolute,
            charge,
            append: self.append,
        };
        file.charge(file.inner.metadata()?.len())?;
        Ok(file)
    }
}

impl Read for &File {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let _io = io_admission()?;
        (&self.inner).read(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> PathBuf {
        let p =
            std::env::temp_dir().join(format!("anyseek-budget-{:032x}", rand::random::<u128>()));
        fs::create_dir(&p).unwrap();
        p
    }
    #[test]
    fn disk_checks_before_growth_and_rescans_same_live_ledger() {
        let root = root();
        fs::write(root.join("selected"), [0; 8]).unwrap();
        let scope = DiskScope::begin(&root, 20).unwrap();
        let mut file = File::create(root.join("candidate")).unwrap();
        file.write_all(&[1; 8]).unwrap();
        assert!(file.write_all(&[2; 5]).is_err());
        assert_eq!(file.metadata().unwrap().len(), 8);
        assert!(file.set_len(13).is_err());
        assert_eq!(file.metadata().unwrap().len(), 8);
        drop(scope);
        // A surviving handle must charge the refreshed, same ledger.
        fs::remove_file(root.join("selected")).unwrap();
        let next = DiskScope::begin(&root, 20).unwrap();
        file.write_all(&[3; 12]).unwrap();
        assert!(file.write_all(&[4]).is_err());
        assert_eq!(file.metadata().unwrap().len(), 20);
        drop(next);
        drop(file);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn sibling_files_share_disk_limit_and_failure_keeps_reservation() {
        let root = root();
        let scope = DiskScope::begin(&root, 12).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let mut tasks = Vec::new();
        for n in 0..2 {
            let mut file = File::create(root.join(n.to_string())).unwrap();
            let b = barrier.clone();
            tasks.push(std::thread::spawn(move || {
                b.wait();
                file.write_all(&[1; 8]).is_ok()
            }));
        }
        assert_eq!(
            tasks
                .into_iter()
                .map(|t| usize::from(t.join().unwrap()))
                .sum::<usize>(),
            1
        );
        assert_eq!(scope.disk.ledger.lock().unwrap().used, 8);
        // Removing a file does not itself refund a reservation.
        for e in fs::read_dir(&root).unwrap() {
            fs::remove_file(e.unwrap().path()).unwrap();
        }
        let mut candidate = File::create(root.join("new")).unwrap();
        assert!(candidate.write_all(&[1; 8]).is_err());
        drop(candidate);
        drop(scope);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn renamed_open_handle_is_conservatively_charged_after_rescan() {
        assert_renamed_open_handle_charge(false);
    }
    #[cfg(not(windows))]
    #[test]
    fn renamed_parent_with_open_handle_is_conservatively_charged_after_rescan() {
        assert_renamed_open_handle_charge(true);
    }
    fn assert_renamed_open_handle_charge(rename_parent: bool) {
        let root = root();
        fs::create_dir(root.join("prepared")).unwrap();
        let scope = DiskScope::begin(&root, 16).unwrap();
        let mut file = File::create(root.join("prepared/data")).unwrap();
        file.write_all(&[1; 4]).unwrap();
        if rename_parent {
            fs::rename(root.join("prepared"), root.join("selected")).unwrap();
        } else {
            // Windows rejects renaming a directory with an open child. Moving
            // the shared-delete file preserves the same stale-path accounting
            // case without requiring that unsupported directory operation.
            fs::create_dir(root.join("selected")).unwrap();
            fs::rename(root.join("prepared/data"), root.join("selected/data")).unwrap();
        }
        drop(scope);
        let scope = DiskScope::begin(&root, 16).unwrap();
        file.write_all(&[2; 4]).unwrap();
        // Alias accounting can reject early but must never undercharge.
        assert_eq!(scope.disk.ledger.lock().unwrap().used, 12);
        assert!(file.write_all(&[3; 5]).is_err());
        assert_eq!(fs::metadata(root.join("selected/data")).unwrap().len(), 8);
        drop(file);
        drop(scope);
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn memory_configuration_rejects_unbounded_or_inconsistent_limits() {
        assert!(MemoryLimits::parse(None, None).is_ok());
        assert!(MemoryLimits::parse(Some("536870912".into()), Some("1073741824".into())).is_ok());
        for (single, shared) in [("0", "1073741824"), ("1073741824", "536870912"), ("bad", "1073741824"), ("1073741824", "18446744073709551615")] {
            assert!(MemoryLimits::parse(Some(single.into()), Some(shared.into())).is_err());
        }
    }
    #[test]
    fn memory_admission_rejects_overflow_and_releases() {
        let counter = Arc::new(AtomicU64::new(0));
        let before = 0;
        assert!(reserve_from(&counter, BUILD_MEMORY + 1).is_err());
        let lease = reserve_from(&counter, 123).unwrap();
        assert_eq!(counter.load(Ordering::Acquire), before + 123);
        drop(lease);
        assert_eq!(counter.load(Ordering::Acquire), before);
        let mut leases = Vec::new();
        while let Ok(lease) = reserve_from(&counter, BUILD_MEMORY) {
            leases.push(lease);
        }
        assert!(!leases.is_empty());
        assert!(reserve_from(&counter, BUILD_MEMORY).is_err());
        drop(leases);
        assert_eq!(counter.load(Ordering::Acquire), before);
    }
}
