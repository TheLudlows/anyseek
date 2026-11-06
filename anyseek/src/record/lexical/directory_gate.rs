//! Process-local coordination of handles to the same native index directory.
use std::{
    collections::BTreeMap,
    io,
    path::Path,
    sync::{Arc, LazyLock, Mutex, Weak},
};

// Unix filesystem identity also covers bind mounts and renamed paths. Elsewhere
// canonicalize resolves relative paths, dot components and filesystem aliases.
#[cfg(unix)]
type Key = (u64, u64);
#[cfg(not(unix))]
type Key = std::path::PathBuf;

static GATES: LazyLock<Mutex<BTreeMap<Key, Weak<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub(super) fn for_directory(path: &Path) -> io::Result<Arc<Mutex<()>>> {
    let canonical = std::fs::canonicalize(path)?;
    #[cfg(unix)]
    let key = {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(&canonical)?;
        (metadata.dev(), metadata.ino())
    };
    #[cfg(not(unix))]
    let key = canonical;
    let mut gates = GATES
        .lock()
        .map_err(|_| io::Error::other("native I/O registry poisoned"))?;
    // Only weak references live globally: retired collections must not retain
    // gates indefinitely. No filesystem I/O or capacity wait under this mutex.
    gates.retain(|_, gate| gate.strong_count() != 0);
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return Ok(gate);
    }
    let gate = Arc::new(Mutex::new(()));
    gates.insert(key, Arc::downgrade(&gate));
    Ok(gate)
}
