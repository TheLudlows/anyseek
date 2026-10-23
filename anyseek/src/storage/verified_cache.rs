//! Discardable, bounded immutable-object bytes. No cache file is ever mmap'ed:
//! consumers receive owned bytes and materialize independently certified files.
//! A cache hit proves content only, never current ownership, head or permission.
use super::sqlite_authority::ObjectRef;
use sha2::{Digest, Sha256};
use std::{fs, io::{self, Read, Write}, path::PathBuf};

pub(crate) const DEFAULT_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_ENTRIES: usize = 4096;

#[derive(Clone)]
pub(crate) struct VerifiedCache {
    root: PathBuf,
    namespace: [u8; 32],
    max_bytes: u64,
    max_entries: usize,
}

struct CacheLock(fs::File);
impl Drop for CacheLock {
    fn drop(&mut self) {
        // Explicitly release before closing: a concurrently spawned Unix child
        // can briefly inherit the open description until exec closes its copy.
        let _ = self.0.unlock();
    }
}

impl VerifiedCache {
    pub(crate) fn new(root: PathBuf, scope: &[&str], max_bytes: u64) -> Self {
        let mut hash = Sha256::new();
        hash.update(b"anyseek-verified-object-cache-v1");
        for part in scope {
            hash.update((part.len() as u64).to_le_bytes());
            hash.update(part.as_bytes());
        }
        Self { root, namespace: hash.finalize().into(), max_bytes, max_entries: DEFAULT_ENTRIES }
    }

    fn name(&self, reference: &ObjectRef) -> String {
        let mut hash = Sha256::new();
        hash.update(self.namespace);
        hash.update((reference.key.len() as u64).to_le_bytes());
        hash.update(reference.key.as_bytes());
        hash.update(reference.sha256);
        hash.update(reference.length.to_le_bytes());
        format!("block-{}", hex(&hash.finalize()))
    }

    // Every operation takes the same nonblocking OS lock, including across
    // processes. Contention or any filesystem failure simply bypasses caching.
    // No lock is held during remote I/O. No waiter queue is created.
    fn lock(&self) -> io::Result<CacheLock> {
        // Keep the coordination inode outside the discardable directory. A
        // concurrent cache clear must not create two independently locked roots.
        let path = self.root.with_extension("object-cache.lock");
        if fs::symlink_metadata(&path).is_ok_and(|m| !m.is_file()) {
            return Err(io::Error::other("cache lock must be a regular file"));
        }
        let lock = fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(path)?;
        lock.try_lock().map_err(io::Error::other)?;
        let lock = CacheLock(lock);
        fs::create_dir_all(&self.root)?;
        if !fs::symlink_metadata(&self.root)?.is_dir() {
            return Err(io::Error::other("cache root must be a directory"));
        }
        Ok(lock)
    }

    pub(crate) fn get(&self, reference: &ObjectRef) -> Option<Vec<u8>> {
        if reference.length > self.max_bytes { return None; }
        let _lock = self.lock().ok()?;
        let path = self.root.join(self.name(reference));
        let result = (|| {
            let meta = fs::symlink_metadata(&path)?;
            if !meta.is_file() || meta.len() != reference.length {
                return Err(io::Error::other("invalid cache file length/type"));
            }
            let mut bytes = Vec::new();
            fs::File::open(&path)?.take(reference.length.saturating_add(1)).read_to_end(&mut bytes)?;
            if !matches(reference, &bytes) {
                return Err(io::Error::other("invalid cache checksum"));
            }
            Ok(bytes)
        })();
        match result {
            Ok(bytes) => Some(bytes),
            Err(_) => { let _ = fs::remove_file(path); None }
        }
    }

    pub(crate) fn insert(&self, reference: &ObjectRef, bytes: &[u8]) {
        if reference.length > self.max_bytes || self.max_entries == 0 || !matches(reference, bytes) { return; }
        let Ok(_lock) = self.lock() else { return; };
        let _ = self.insert_locked(reference, bytes);
    }

    fn insert_locked(&self, reference: &ObjectRef, bytes: &[u8]) -> io::Result<()> {
        let target = self.root.join(self.name(reference));
        // No cache reader retains a handle after returning. Removing an older
        // copy never touches the independently materialized Segment mmap files.
        match fs::remove_file(&target) {
            Ok(()) => {},
            Err(e) if e.kind() == io::ErrorKind::NotFound => {},
            Err(e) => return Err(e),
        }
        let mut files = Vec::new();
        let mut total = 0u64;
        for (count, row) in fs::read_dir(&self.root)?.enumerate() {
            // Bound metadata memory/work even if the directory was externally
            // populated. Unknown names are never deleted.
            if count >= self.max_entries.saturating_mul(2).saturating_add(16) {
                return Err(io::Error::other("cache directory entry budget exceeded"));
            }
            let row = row?;
            let name = row.file_name();
            let name = name.to_string_lossy();
            if managed(&name, "temp-", 32) {
                fs::remove_file(row.path())?; // interrupted private publication
            } else if managed(&name, "block-", 64) {
                let meta = fs::symlink_metadata(row.path())?;
                if !meta.is_file() {
                    fs::remove_file(row.path())?;
                    continue;
                }
                total = total.checked_add(meta.len()).ok_or_else(|| io::Error::other("cache length overflow"))?;
                files.push((meta.modified()?, row.path(), meta.len()));
            }
        }
        // Oldest insertion first: hits do not rewrite metadata. Count and bytes
        // include existing orphans; deletion failure cannot release their budget.
        files.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        let mut count = files.len();
        for (_, path, length) in files {
            if total <= self.max_bytes - reference.length && count < self.max_entries { break; }
            fs::remove_file(path)?;
            total -= length;
            count -= 1;
        }
        if total > self.max_bytes - reference.length || count >= self.max_entries {
            return Err(io::Error::other("cache capacity unavailable"));
        }
        let temporary = self.root.join(format!("temp-{:032x}", rand::random::<u128>()));
        let result = (|| {
            let mut output = fs::OpenOptions::new().create_new(true).write(true).open(&temporary)?;
            output.write_all(bytes)?;
            #[cfg(test)] crash_at("written");
            output.sync_all()?;
            #[cfg(test)] crash_at("synced");
            drop(output);
            // Same-directory atomic promotion. A crash can lose cache entries;
            // every hit rechecks hash/length, so no cache durability is assumed.
            fs::rename(&temporary, &target)?;
            #[cfg(test)] crash_at("published");
            Ok(())
        })();
        if result.is_err() { let _ = fs::remove_file(temporary); }
        result
    }
}

fn managed(name: &str, prefix: &str, length: usize) -> bool {
    name.strip_prefix(prefix).is_some_and(|s| s.len() == length && s.bytes().all(|b| b.is_ascii_hexdigit()))
}
fn hex(bytes: &[u8]) -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() }
fn matches(reference: &ObjectRef, bytes: &[u8]) -> bool {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    bytes.len() as u64 == reference.length && digest == reference.sha256
}

#[cfg(test)]
fn crash_at(point: &str) {
    if std::env::var("ANYSEEK_CACHE_TEST_CRASH").as_deref() == Ok(point) { std::process::exit(94); }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn root() -> PathBuf { std::env::temp_dir().join(format!("anyseek-cache-{:032x}", rand::random::<u128>())) }
    fn reference(key: &str, bytes: &[u8]) -> ObjectRef {
        ObjectRef { key: key.into(), length: bytes.len() as u64, sha256: Sha256::digest(bytes).into() }
    }
    #[test]
    fn content_identity_namespace_corruption_and_loss() {
        let root = root();
        let a = VerifiedCache::new(root.clone(), &["endpoint", "bucket"], 16);
        let b = VerifiedCache::new(root.clone(), &["endpoint", "other-bucket"], 16);
        let r = reference("partitions/a/block", b"alpha");
        a.insert(&r, b"alpha");
        assert_eq!(a.get(&r).unwrap(), b"alpha");
        assert!(b.get(&r).is_none());
        assert!(a.get(&reference("partitions/b/block", b"alpha")).is_none());
        let mut altered = r.clone(); altered.length += 1;
        assert!(a.get(&altered).is_none());
        fs::write(root.join(a.name(&r)), b"other").unwrap();
        assert!(a.get(&r).is_none());
        a.insert(&r, b"WRONG");
        assert!(a.get(&r).is_none());
        a.insert(&r, b"alpha");
        fs::remove_dir_all(&root).unwrap();
        assert!(a.get(&r).is_none());
        a.insert(&r, b"alpha");
        assert_eq!(a.get(&r).unwrap(), b"alpha");
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn bounded_eviction_preserves_returned_bytes_and_independent_mmap() {
        let root = root();
        let mut cache = VerifiedCache::new(root.clone(), &["scope"], 5);
        cache.max_entries = 1;
        let first = reference("one", b"alpha"); let second = reference("two", b"beta");
        cache.insert(&first, b"alpha");
        let owned = cache.get(&first).unwrap();
        let materialized = root.with_extension("segment");
        fs::write(&materialized, &owned).unwrap();
        let file = fs::File::open(&materialized).unwrap();
        let mapped = unsafe { memmap2::Mmap::map(&file).unwrap() };
        cache.insert(&second, b"beta");
        assert!(cache.get(&first).is_none());
        assert_eq!(cache.get(&second).unwrap(), b"beta");
        assert_eq!(owned, b"alpha"); assert_eq!(&mapped[..], b"alpha");
        cache.insert(&reference("large", b"123456"), b"123456");
        assert_eq!(cache.get(&second).unwrap(), b"beta");
        drop(mapped); drop(file);
        fs::remove_file(materialized).unwrap(); fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn lock_contention_and_interrupted_publication_are_discardable() {
        let root = root(); let cache = VerifiedCache::new(root.clone(), &["scope"], 8);
        let r = reference("one", b"alpha");
        let lock = cache.lock().unwrap();
        fs::remove_dir_all(&root).unwrap();
        cache.insert(&r, b"alpha"); assert!(cache.get(&r).is_none());
        assert!(!root.exists(), "clearing the directory must not bypass an existing lock");
        drop(lock);
        fs::create_dir_all(&root).unwrap();
        let temp = root.join(format!("temp-{:032x}", 1)); fs::write(&temp, b"incomplete").unwrap();
        cache.insert(&r, b"alpha");
        assert!(!temp.exists()); assert_eq!(cache.get(&r).unwrap(), b"alpha");
        // A failed cache directory is also a miss, never a recovery failure.
        fs::remove_dir_all(&root).unwrap(); fs::write(&root, b"unavailable").unwrap();
        cache.insert(&r, b"alpha"); assert!(cache.get(&r).is_none());
        fs::remove_file(root).unwrap();
    }

    #[test]
    fn process_exit_during_cache_publication_never_selects_partial_bytes() {
        for point in ["written", "synced", "published"] {
            let root = root();
            let result = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "storage::verified_cache::tests::cache_crash_worker", "--ignored", "--nocapture"])
                .env("ANYSEEK_CACHE_TEST_ROOT", &root)
                .env("ANYSEEK_CACHE_TEST_CRASH", point).status().unwrap();
            assert_eq!(result.code(), Some(94), "worker did not reach {point}");
            let cache = VerifiedCache::new(root.clone(), &["scope"], 5);
            let r = reference("one", b"alpha");
            if let Some(bytes) = cache.get(&r) { assert_eq!(bytes, b"alpha"); }
            cache.insert(&r, b"alpha");
            assert_eq!(cache.get(&r).unwrap(), b"alpha");
            let rows = fs::read_dir(&root).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].metadata().unwrap().len(), 5);
            fs::remove_dir_all(&root).unwrap();
            fs::remove_file(root.with_extension("object-cache.lock")).unwrap();
        }
    }

    #[test]
    #[ignore = "invoked by the parent publication process-exit test"]
    fn cache_crash_worker() {
        let root = PathBuf::from(std::env::var_os("ANYSEEK_CACHE_TEST_ROOT").unwrap());
        let cache = VerifiedCache::new(root, &["scope"], 5);
        cache.insert(&reference("one", b"alpha"), b"alpha");
        panic!("cache crash point was not reached");
    }
}
