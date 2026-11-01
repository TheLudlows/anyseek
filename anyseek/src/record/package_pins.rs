//! Process-local protection for immutable package builders and fixed readers.
//! Persistent selection/backup/build roots remain the authority's responsibility.
use super::*;
use std::{
    collections::BTreeSet,
    sync::{LazyLock, Mutex},
};

#[derive(Default)]
struct Registry {
    pins: BTreeMap<PathBuf, usize>,
    // true: removal is running; false: partial/failed removal awaits retry.
    retiring: BTreeMap<PathBuf, bool>,
}
static PINS: LazyLock<Mutex<Registry>> = LazyLock::new(|| Mutex::new(Registry::default()));

fn overlaps(a: &Path, b: &Path) -> bool { a.starts_with(b) || b.starts_with(a) }

fn key(path: &Path) -> Result<PathBuf, RecordError> {
    let parent = path
        .parent()
        .ok_or_else(|| storage("package pin needs parent"))?;
    let name = path
        .file_name()
        .ok_or_else(|| storage("package pin needs filename"))?;
    Ok(std::fs::canonicalize(parent).map_err(storage)?.join(name))
}
#[derive(Debug)]
pub(super) struct Pin {
    paths: Vec<PathBuf>,
}
impl Pin {
    pub(super) fn paths(&self) -> &[PathBuf] { &self.paths }
    pub(super) fn acquire(paths: &[PathBuf]) -> Result<Arc<Self>, RecordError> {
        let paths = paths
            .iter()
            .map(|path| key(path))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let mut registry = PINS.lock().map_err(|_| storage("package pins poisoned"))?;
        if paths.iter().any(|path| registry.retiring.keys().any(|p| overlaps(path, p))) {
            return Err(RecordError::Busy);
        }
        for path in &paths {
            if registry.pins.get(path).copied() == Some(usize::MAX) {
                return Err(storage("package pin overflow"));
            }
        }
        for path in &paths {
            *registry.pins.entry(path.clone()).or_default() += 1;
        }
        Ok(Arc::new(Self {
            paths: paths.into_iter().collect(),
        }))
    }
}
impl Drop for Pin {
    fn drop(&mut self) {
        // Poisoning must fail closed: leaked pins are safer than early deletion.
        if let Ok(mut registry) = PINS.lock() {
            for path in &self.paths {
                if let Some(count) = registry.pins.get_mut(path) {
                    *count -= 1;
                    if *count == 0 {
                        registry.pins.remove(path);
                    }
                }
            }
        }
    }
}
// Register before releasing the global mutex. Only overlapping paths are blocked;
// physical I/O and callbacks never run under that mutex.
struct Retirement {
    path: PathBuf,
    was_pending: bool,
    armed: bool,
}
impl Retirement {
    fn begin(path: &Path) -> Result<Option<Self>, RecordError> {
        let path = key(path)?;
        let mut registry = PINS.lock().map_err(|_| storage("package pins poisoned"))?;
        if registry.pins.keys().any(|p| overlaps(p, &path))
            || registry.retiring.iter().any(|(p, active)| {
                (*active && overlaps(p, &path)) || (p != &path && path.starts_with(p))
            }) {
            return Ok(None);
        }
        // An ancestor retry may subsume inactive failed child removals.
        let was_pending = registry.retiring.insert(path.clone(), true).is_some();
        Ok(Some(Self { path, was_pending, armed: true }))
    }
    fn complete(&mut self) -> Result<(), RecordError> {
        let mut registry = PINS.lock().map_err(|_| storage("package pins poisoned"))?;
        registry.retiring.retain(|p, _| !p.starts_with(&self.path));
        self.armed = false;
        Ok(())
    }
    fn cancel_before_removal(&mut self) -> Result<(), RecordError> {
        let mut registry = PINS.lock().map_err(|_| storage("package pins poisoned"))?;
        if !self.was_pending {
            registry.retiring.remove(&self.path);
            self.armed = false;
        }
        Ok(())
    }
}
impl Drop for Retirement {
    fn drop(&mut self) {
        if !self.armed { return; }
        // Error or unwind retains the barrier, but allows a later deletion retry.
        if let Ok(mut registry) = PINS.lock() {
            if let Some(active) = registry.retiring.get_mut(&self.path) { *active = false; }
        }
    }
}

pub(super) fn retire_if_unpinned(
    path: &Path,
    remove: impl FnOnce() -> Result<(), RecordError>,
) -> Result<bool, RecordError> {
    let Some(mut retirement) = Retirement::begin(path)? else { return Ok(false); };
    remove()?;
    retirement.complete()?;
    Ok(true)
}

/// A deleted root can only move after every fixed reader of its children ends.
pub(super) fn retire_tree_if_unpinned(
    path: &Path, remove: impl FnOnce() -> Result<bool, RecordError>,
) -> Result<bool, RecordError> {
    let Some(mut retirement) = Retirement::begin(path)? else { return Err(RecordError::Busy); };
    let result = remove();
    match &result {
        Ok(true) => retirement.complete()?,
        // The journal owner check returns Busy before any mutation. Do not
        // strand a live owner's packages behind a new retirement barrier.
        Err(RecordError::Busy) => retirement.cancel_before_removal()?,
        _ => {},
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc, time::Duration};

    #[test]
    fn slow_removal_blocks_only_overlapping_paths() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("package");
        std::fs::create_dir(&package).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (checked_tx, checked_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let deletion_path = &package;
            let deletion = scope.spawn(move || retire_if_unpinned(deletion_path, || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                std::fs::remove_dir_all(deletion_path).map_err(storage)
            }));
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let check = scope.spawn(|| {
                let unrelated = Pin::acquire(&[root.path().join("other")]).unwrap();
                for path in [package.clone(), package.join("../package"), package.join("child"), root.path().to_owned()] {
                    assert!(matches!(Pin::acquire(&[path]), Err(RecordError::Busy)));
                }
                assert!(!retire_if_unpinned(&package, || panic!("concurrent removal")).unwrap());
                assert!(matches!(retire_tree_if_unpinned(root.path(), || panic!("ancestor removal")), Err(RecordError::Busy)));
                drop(unrelated);
                checked_tx.send(()).unwrap();
            });
            let isolated = checked_rx.recv_timeout(Duration::from_secs(5));
            // Release even on failure, so a lock regression cannot hang the test.
            release_tx.send(()).unwrap();
            assert!(deletion.join().unwrap().unwrap());
            check.join().unwrap();
            isolated.unwrap();
        });
    }

    #[test]
    fn failed_removal_stays_closed_until_retry_or_ancestor_cleanup() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("package");
        std::fs::create_dir(&package).unwrap();
        std::fs::write(package.join("part"), b"x").unwrap();
        let pin = Pin::acquire(&[package.join("part")]).unwrap();
        assert!(!retire_if_unpinned(&package, || panic!("child pin lost")).unwrap());
        drop(pin);
        assert!(retire_if_unpinned(&package, || {
            std::fs::remove_file(package.join("part")).map_err(storage)?;
            Err(storage("injected partial deletion"))
        }).is_err());
        assert!(matches!(Pin::acquire(&[package.clone()]), Err(RecordError::Busy)));
        // Even a retry rejected before mutation must retain the earlier barrier.
        assert!(matches!(retire_tree_if_unpinned(&package, || Err(RecordError::Busy)), Err(RecordError::Busy)));
        assert!(matches!(Pin::acquire(&[package.clone()]), Err(RecordError::Busy)));
        assert!(retire_if_unpinned(&package, || std::fs::remove_dir_all(&package).map_err(storage)).unwrap());
        // Completed retirement does not prevent a new, explicitly rebuilt path.
        std::fs::create_dir(&package).unwrap();
        drop(Pin::acquire(&[package.clone()]).unwrap());
        assert!(retire_if_unpinned(&package, || Err(storage("retry via ancestor"))).is_err());
        assert!(retire_tree_if_unpinned(root.path(), || {
            assert!(!retire_if_unpinned(&package, || panic!("ancestor owns removal")).unwrap());
            std::fs::remove_dir_all(root.path()).map_err(storage)?;
            Ok(true)
        }).unwrap());
    }
}
