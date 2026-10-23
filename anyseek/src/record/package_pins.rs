//! Process-local protection for immutable package builders and fixed readers.
//! Persistent selection/backup/build roots remain the authority's responsibility.
use super::*;
use std::{
    collections::BTreeSet,
    sync::{LazyLock, Mutex},
};

static PINS: LazyLock<Mutex<BTreeMap<PathBuf, usize>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

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
    pub(super) fn acquire(paths: &[PathBuf]) -> Result<Arc<Self>, RecordError> {
        let paths = paths
            .iter()
            .map(|path| key(path))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let mut registry = PINS.lock().map_err(|_| storage("package pins poisoned"))?;
        for path in &paths {
            if registry.get(path).copied() == Some(usize::MAX) {
                return Err(storage("package pin overflow"));
            }
        }
        for path in &paths {
            *registry.entry(path.clone()).or_default() += 1;
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
                if let Some(count) = registry.get_mut(path) {
                    *count -= 1;
                    if *count == 0 {
                        registry.remove(path);
                    }
                }
            }
        }
    }
}
/// Serialize pin acquisition with the entire removal, not just its eligibility
/// check. A later loader either pins existing files or fails on an absent path.
pub(super) fn retire_if_unpinned(
    path: &Path,
    remove: impl FnOnce() -> Result<(), RecordError>,
) -> Result<bool, RecordError> {
    let path = key(path)?;
    let registry = PINS.lock().map_err(|_| storage("package pins poisoned"))?;
    if registry.contains_key(&path) {
        return Ok(false);
    }
    remove()?;
    drop(registry);
    Ok(true)
}
