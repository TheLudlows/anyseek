//! Immutable, sorted primary index. Internal entries are authenticated high
//! fences. Each leaf authenticates the row at its segment/ordinal address.
use super::*;
use std::{cmp::Ordering, collections::VecDeque, sync::Mutex};

const PAGE_MAGIC: &[u8; 16] = b"ANYSEEK-PKEYS001";
const KEY_MAGIC: &[u8; 16] = b"ANYSEEK-KEYS-001";
const HEADER: u64 = 16;
const PAGE_BYTES: usize = 4096;
const KEY_PREFIX: usize = 32;
const ENTRY_BYTES: usize = 124;
const CAPACITY: usize = (PAGE_BYTES - 4) / ENTRY_BYTES;
const MAX_HEIGHT: usize = 16;
// Include the page owner and cache entry bookkeeping in the cached byte count.
const PAGE_CHARGE: usize = PAGE_BYTES + 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Block {
    index: u64,
    sha256: Digest,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Descriptor {
    pages: u64,
    keys_bytes: u64,
    root: Option<Block>,
}

#[derive(Clone, Copy)]
struct Key {
    length: u32,
    prefix: [u8; KEY_PREFIX],
    offset: u64,
    suffix_sha: Digest,
}

impl Key {
    fn head(&self) -> &[u8] {
        &self.prefix[..(self.length as usize).min(KEY_PREFIX)]
    }

    fn validate(&self, index: &Index) -> io::Result<()> {
        let length = self.length as usize;
        if length == 0 || length > index.max_key_bytes {
            return Err(invalid("primary key length outside budget"));
        }
        if length <= KEY_PREFIX {
            if self.offset != 0
                || self.suffix_sha != [0; 32]
                || self.prefix[length..].iter().any(|b| *b != 0)
            {
                return Err(invalid("invalid in-page primary key"));
            }
        } else if self.offset < HEADER
            || self
                .offset
                .checked_add((length - KEY_PREFIX) as u64)
                .is_none_or(|end| end > index.keys_bytes)
        {
            return Err(invalid("primary key reference outside selected file"));
        }
        Ok(())
    }

    fn compare(&self, id: &str, index: &Index) -> io::Result<Ordering> {
        self.validate(index)?;
        let id = id.as_bytes();
        let head = self.head().cmp(&id[..id.len().min(KEY_PREFIX)]);
        if head != Ordering::Equal {
            return Ok(head);
        }
        if self.length as usize <= KEY_PREFIX || id.len() <= KEY_PREFIX {
            return Ok((self.length as usize).cmp(&id.len()));
        }
        let bytes = self.bytes(index)?;
        Ok(bytes.bytes[KEY_PREFIX..].cmp(&id[KEY_PREFIX..]))
    }

    fn bytes(&self, index: &Index) -> io::Result<BodyBytes> {
        self.validate(index)?;
        let length = self.length as usize;
        let memory = reserve_memory(length as u64)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(io::Error::other)?;
        bytes.extend_from_slice(self.head());
        if length > KEY_PREFIX {
            bytes.resize(length, 0);
            let suffix = &mut bytes[KEY_PREFIX..];
            (index.read)(&index.keys, self.offset, suffix)?;
            if digest(suffix) != self.suffix_sha {
                return Err(invalid("primary key checksum mismatch"));
            }
        }
        Ok(BodyBytes {
            bytes,
            _memory: memory,
        })
    }

    fn owned(&self, index: &Index) -> io::Result<KeyOwned<'static>> {
        let BodyBytes { bytes, _memory } = self.bytes(index)?;
        let id = String::from_utf8(bytes).map_err(|_| invalid("primary key is not UTF-8"))?;
        Ok(KeyOwned::Disk(id, _memory))
    }
}

pub(crate) enum KeyOwned<'a> {
    Borrowed(&'a str),
    Disk(String, MemoryLease),
}

impl KeyOwned<'_> {
    pub(crate) fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(id) => id,
            Self::Disk(id, _) => id,
        }
    }
}

#[derive(Clone, Copy)]
struct Entry {
    key: Key,
    first: u64,
    second: u64,
    sha256: Digest,
}

impl Entry {
    fn decode(bytes: &[u8]) -> Self {
        Self {
            key: Key {
                length: u32::from_le_bytes(bytes[..4].try_into().expect("fixed key width")),
                prefix: bytes[4..36].try_into().expect("fixed key width"),
                offset: read_u64(&bytes[36..44]),
                suffix_sha: bytes[44..76].try_into().expect("fixed key width"),
            },
            first: read_u64(&bytes[76..84]),
            second: read_u64(&bytes[84..92]),
            sha256: bytes[92..124].try_into().expect("fixed entry width"),
        }
    }

    fn encode(&self, out: &mut [u8]) {
        out[..4].copy_from_slice(&self.key.length.to_le_bytes());
        out[4..36].copy_from_slice(&self.key.prefix);
        out[36..44].copy_from_slice(&self.key.offset.to_le_bytes());
        out[44..76].copy_from_slice(&self.key.suffix_sha);
        out[76..84].copy_from_slice(&self.first.to_le_bytes());
        out[84..92].copy_from_slice(&self.second.to_le_bytes());
        out[92..124].copy_from_slice(&self.sha256);
    }

    fn block(&self) -> Block {
        Block {
            index: self.first,
            sha256: self.sha256,
        }
    }

    fn row(&self) -> RowRef {
        RowRef {
            address: Address {
                segment_id: self.first,
                doc_id: self.second,
            },
            sha256: self.sha256,
        }
    }
}

struct Level {
    entries: Vec<Entry>,
    _memory: MemoryLease,
}

/// Build sorted pages in one pass. Only one pending page at each tree height
/// and the last input ID are retained, regardless of the number of records.
pub(crate) struct Writer {
    file: File,
    keys: File,
    pages: u64,
    keys_bytes: u64,
    levels: Vec<Level>,
    last: Option<KeyOwned<'static>>,
    max_key_bytes: usize,
    poisoned: bool,
}

impl Writer {
    pub(crate) fn create(dir: &Path, max_key_bytes: usize) -> io::Result<Self> {
        Ok(Self {
            file: create_file(&dir.join("primary-pages"), PAGE_MAGIC)?,
            keys: create_file(&dir.join("primary-keys"), KEY_MAGIC)?,
            pages: 0,
            keys_bytes: HEADER,
            levels: Vec::new(),
            last: None,
            max_key_bytes,
            poisoned: false,
        })
    }

    pub(crate) fn push(&mut self, id: &str, reference: RowRef) -> io::Result<()> {
        if self.poisoned {
            return Err(invalid("primary index requires discard"));
        }
        if id.is_empty()
            || id.len() > self.max_key_bytes
            || self.last.as_ref().is_some_and(|last| last.as_str() >= id)
        {
            return Err(invalid("primary IDs must be unique, bounded and ordered"));
        }
        let length = u32::try_from(id.len()).map_err(|_| invalid("primary key length overflow"))?;
        let memory = reserve_memory(id.len() as u64)?;
        let mut last = String::new();
        last.try_reserve_exact(id.len()).map_err(io::Error::other)?;
        last.push_str(id);

        let mut key = Key {
            length,
            prefix: [0; KEY_PREFIX],
            offset: 0,
            suffix_sha: [0; 32],
        };
        let split = id.len().min(KEY_PREFIX);
        key.prefix[..split].copy_from_slice(&id.as_bytes()[..split]);
        self.poisoned = true;
        if let Some(suffix) = id.as_bytes().get(KEY_PREFIX..)
            && !suffix.is_empty()
        {
            let end = self
                .keys_bytes
                .checked_add(suffix.len() as u64)
                .ok_or_else(|| invalid("primary key file extent overflow"))?;
            self.keys.write_all(suffix)?;
            key.offset = self.keys_bytes;
            key.suffix_sha = digest(suffix);
            self.keys_bytes = end;
        }
        self.feed(
            0,
            Entry {
                key,
                first: reference.address.segment_id,
                second: reference.address.doc_id,
                sha256: reference.sha256,
            },
        )?;
        self.last = Some(KeyOwned::Disk(last, memory));
        self.poisoned = false;
        Ok(())
    }

    fn feed(&mut self, level: usize, entry: Entry) -> io::Result<()> {
        if level >= MAX_HEIGHT {
            return Err(invalid("primary tree height exceeded"));
        }
        if level == self.levels.len() {
            let memory = reserve_memory((CAPACITY * size_of::<Entry>()) as u64)?;
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(CAPACITY)
                .map_err(io::Error::other)?;
            self.levels.push(Level {
                entries,
                _memory: memory,
            });
        }
        if self.levels[level].entries.len() == CAPACITY {
            let parent = self.flush(level)?;
            self.feed(level + 1, parent)?;
        }
        self.levels[level].entries.push(entry);
        Ok(())
    }

    fn flush(&mut self, level: usize) -> io::Result<Entry> {
        let mut bytes = [0; PAGE_BYTES];
        let entries = &mut self.levels[level].entries;
        let count = entries.len();
        let high = entries.last().expect("nonempty primary page").key;
        bytes[0] = level as u8;
        bytes[1] = count as u8;
        for (i, entry) in entries.drain(..).enumerate() {
            let start = 4 + i * ENTRY_BYTES;
            entry.encode(&mut bytes[start..start + ENTRY_BYTES]);
        }
        let next = self
            .pages
            .checked_add(1)
            .ok_or_else(|| invalid("primary page address overflow"))?;
        extent(HEADER, next, PAGE_BYTES as u64)?;
        self.file.write_all(&bytes)?;
        let parent = Entry {
            key: high,
            first: self.pages,
            second: 0,
            sha256: digest(&bytes),
        };
        self.pages = next;
        Ok(parent)
    }

    pub(crate) fn finish(mut self) -> io::Result<Descriptor> {
        if self.poisoned {
            return Err(invalid("primary index requires discard"));
        }
        let mut root = None;
        for level in 0..MAX_HEIGHT {
            if level == self.levels.len() {
                break;
            }
            let child = self.flush(level)?;
            if level + 1 == self.levels.len() {
                root = Some(child.block());
                break;
            }
            self.feed(level + 1, child)?;
        }
        if self.file.metadata()?.len() != extent(HEADER, self.pages, PAGE_BYTES as u64)?
            || self.keys.metadata()?.len() != self.keys_bytes
        {
            return Err(invalid("incomplete primary index"));
        }
        self.file.sync_all()?;
        self.keys.sync_all()?;
        Ok(Descriptor {
            pages: self.pages,
            keys_bytes: self.keys_bytes,
            root,
        })
    }
}

struct Page {
    bytes: [u8; PAGE_BYTES],
    block: Block,
    _memory: MemoryLease,
}
impl Page {
    fn level(&self) -> u8 {
        self.bytes[0]
    }
    fn len(&self) -> usize {
        usize::from(self.bytes[1])
    }
    fn entry(&self, i: usize) -> Entry {
        let start = 4 + i * ENTRY_BYTES;
        Entry::decode(&self.bytes[start..start + ENTRY_BYTES])
    }
}

#[derive(Default)]
struct Cache {
    pages: BTreeMap<u64, Arc<Page>>,
    fifo: VecDeque<u64>,
}

pub(super) struct Index {
    file: File,
    keys: File,
    pages: u64,
    keys_bytes: u64,
    root: Option<Block>,
    max_key_bytes: usize,
    cache_limit: usize,
    cache: Mutex<Cache>,
    read: ReadRange,
}

impl Index {
    pub(super) fn open(
        dir: &Path,
        descriptor: Descriptor,
        limits: Limits,
        read: ReadRange,
    ) -> io::Result<Self> {
        if descriptor.root.map(|block| block.index) != descriptor.pages.checked_sub(1)
            || descriptor.keys_bytes < HEADER
        {
            return Err(invalid("invalid primary index root"));
        }
        let file = open_file(
            &dir.join("primary-pages"),
            extent(HEADER, descriptor.pages, PAGE_BYTES as u64)?,
        )?;
        check_magic(&file, PAGE_MAGIC, read)?;
        let keys = open_file(&dir.join("primary-keys"), descriptor.keys_bytes)?;
        check_magic(&keys, KEY_MAGIC, read)?;
        Ok(Self {
            file,
            keys,
            pages: descriptor.pages,
            keys_bytes: descriptor.keys_bytes,
            root: descriptor.root,
            max_key_bytes: limits.max_record_bytes,
            cache_limit: limits.page_cache_bytes / PAGE_CHARGE,
            cache: Mutex::new(Cache::default()),
            read,
        })
    }

    fn load(&self, block: Block) -> io::Result<Arc<Page>> {
        if block.index >= self.pages {
            return Err(invalid("primary page outside selected file"));
        }
        let lock = || {
            self.cache
                .lock()
                .map_err(|_| invalid("primary cache poisoned"))
        };
        if let Some(page) = lock()?.pages.get(&block.index).cloned() {
            if page.block != block {
                return Err(invalid("inconsistent primary page reference"));
            }
            return Ok(page);
        }
        let memory = reserve_memory(PAGE_CHARGE as u64)?;
        let mut page = Page {
            bytes: [0; PAGE_BYTES],
            block,
            _memory: memory,
        };
        (self.read)(
            &self.file,
            extent(HEADER, block.index, PAGE_BYTES as u64)?,
            &mut page.bytes,
        )?;
        if digest(&page.bytes) != block.sha256
            || usize::from(page.level()) >= MAX_HEIGHT
            || !(1..=CAPACITY).contains(&page.len())
            || page.bytes[2..4] != [0; 2]
        {
            return Err(invalid("invalid or corrupt primary page"));
        }
        let page = Arc::new(page);
        if self.cache_limit > 0 {
            let mut cache = lock()?;
            if !cache.pages.contains_key(&block.index) {
                if cache.pages.len() == self.cache_limit {
                    let oldest = cache
                        .fifo
                        .pop_front()
                        .expect("full cache has an oldest page");
                    cache.pages.remove(&oldest);
                }
                cache.fifo.push_back(block.index);
                cache.pages.insert(block.index, page.clone());
            }
        }
        Ok(page)
    }

    pub(super) fn find(&self, id: &str) -> io::Result<Option<RowRef>> {
        let mut cursor = self.cursor(id, false)?;
        let Some(entry) = cursor.next_entry()? else {
            return Ok(None);
        };
        Ok((entry.key.compare(id, self)? == Ordering::Equal).then(|| entry.row()))
    }

    pub(super) fn cursor(&self, bound: &str, exclusive: bool) -> io::Result<Cursor<'_>> {
        let mut cursor = Cursor {
            index: self,
            path: Vec::new(),
        };
        let Some(block) = self.root else {
            return Ok(cursor);
        };
        let mut page = self.load(block)?;
        loop {
            let (mut lo, mut hi) = (0, page.len());
            while lo < hi {
                let mid = lo + (hi - lo) / 2;
                let order = page.entry(mid).key.compare(bound, self)?;
                if order == Ordering::Less || exclusive && order == Ordering::Equal {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if page.level() == 0 || lo == page.len() {
                cursor.path.push((page, lo));
                return Ok(cursor);
            }
            let child = cursor.child(&page, lo)?;
            cursor.path.push((page, lo + 1));
            page = child;
        }
    }

    #[cfg(test)]
    pub(super) fn cache_bytes(&self) -> usize {
        self.cache.lock().unwrap().pages.len() * PAGE_CHARGE
    }
}

pub(super) struct Cursor<'a> {
    index: &'a Index,
    path: Vec<(Arc<Page>, usize)>,
}

impl Cursor<'_> {
    fn child(&self, parent: &Page, offset: usize) -> io::Result<Arc<Page>> {
        let entry = parent.entry(offset);
        let child = entry.block();
        if entry.second != 0 || child.index >= parent.block.index {
            return Err(invalid("invalid primary child address"));
        }
        let page = self.index.load(child)?;
        if page.level() + 1 != parent.level() {
            return Err(invalid("invalid primary child height"));
        }
        Ok(page)
    }

    fn next_entry(&mut self) -> io::Result<Option<Entry>> {
        while let Some((page, at)) = self.path.pop() {
            if at == page.len() {
                continue;
            }
            self.path.push((page.clone(), at + 1));
            if page.level() == 0 {
                return Ok(Some(page.entry(at)));
            }
            let child = self.child(&page, at)?;
            self.path.push((child, 0));
        }
        Ok(None)
    }

    pub(super) fn next(&mut self) -> io::Result<Option<(KeyOwned<'static>, RowRef)>> {
        self.next_entry()?
            .map(|entry| Ok((entry.key.owned(self.index)?, entry.row())))
            .transpose()
    }
}
