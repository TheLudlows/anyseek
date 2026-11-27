//! Optional ordering of the canonical rows in one authenticated State base.
//! The partition selects and pins the owner. This module has no authority
//! separate from that owner and does not interpret later deltas or the WAL.
use super::*;

pub(crate) const DIRECTORY: &str = "event-time";
pub(crate) const MEMBERS: [&str; 3] = [
    "event-time/descriptor",
    "event-time/primary-keys",
    "event-time/primary-pages",
];
pub(crate) const MAX_DESCRIPTOR_BYTES: u64 = 4096;
const TIME_BYTES: usize = 16;
const RUN_HEADER: usize = 4 + 8 + 8 + 32;
const MAX_RUNS: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct BuildLimits {
    run_memory_bytes: u64,
    disk_bytes: u64,
}

impl BuildLimits {
    pub(crate) const DEFAULT: Self = Self {
        run_memory_bytes: 8 * 1024 * 1024,
        disk_bytes: 128 * 1024 * 1024,
    };

    #[cfg(test)]
    pub(crate) const fn for_test(run_memory_bytes: u64, disk_bytes: u64) -> Self {
        Self {
            run_memory_bytes,
            disk_bytes,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub(crate) owner: String,
    pub(crate) lineage: [u8; 16],
    pub(crate) cut: u64,
    pub(crate) rows_sha256: Digest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    format: u32,
    binding: Binding,
    count: u64,
    primary: primary::Descriptor,
}

#[derive(Debug)]
pub(crate) struct BuildLimit;
impl std::fmt::Display for BuildLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("event-time projection exceeded its build budget")
    }
}
impl std::error::Error for BuildLimit {}

fn refusal() -> io::Error {
    io::Error::other(BuildLimit)
}

fn key_prefix(event_ms: i64) -> u64 {
    !((event_ms as u64) ^ (1 << 63))
}

fn key_limit(max_id_bytes: usize) -> io::Result<usize> {
    max_id_bytes
        .checked_add(TIME_BYTES)
        .filter(|n| u32::try_from(*n).is_ok())
        .ok_or_else(|| invalid("event-time key limit overflow"))
}

fn make_key(event_ms: i64, id: &str, bytes: usize) -> io::Result<String> {
    use std::fmt::Write as _;
    let mut key = String::new();
    key.try_reserve_exact(bytes).map_err(io::Error::other)?;
    write!(key, "{:016x}", key_prefix(event_ms)).expect("writing to a String");
    key.push_str(id);
    Ok(key)
}

struct Entry {
    key: String,
    row: RowRef,
}

pub(crate) struct Builder {
    dir: std::path::PathBuf,
    limits: BuildLimits,
    max_key_bytes: usize,
    entries: Vec<Entry>,
    run_memory: u64,
    run_bytes: u64,
    runs: Vec<RunDescriptor>,
    spilled_bytes: u64,
    count: u64,
    key_bytes: u64,
    run_memory_lease: MemoryLease,
}

impl Builder {
    pub(crate) fn new(root: &Path, max_id_bytes: usize, limits: BuildLimits) -> io::Result<Self> {
        let max_key_bytes = key_limit(max_id_bytes)?;
        let memory = reserve_memory(limits.run_memory_bytes)?;
        let dir = root.join(DIRECTORY);
        std::fs::create_dir(&dir)?;
        Ok(Self {
            dir,
            limits,
            max_key_bytes,
            entries: Vec::new(),
            run_memory: 0,
            run_bytes: 0,
            runs: Vec::new(),
            spilled_bytes: 0,
            count: 0,
            key_bytes: 0,
            run_memory_lease: memory,
        })
    }

    pub(crate) fn push(&mut self, event_ms: i64, id: &str, row: RowRef) -> io::Result<()> {
        let bytes = id.len().checked_add(TIME_BYTES).ok_or_else(refusal)?;
        if id.is_empty() || bytes > self.max_key_bytes {
            return Err(invalid("invalid event-time row ID"));
        }
        // Cover the minimum Vec capacity and both slot allocations during
        // geometric growth. Strings reserve their exact logical capacity.
        let memory = bytes as u64 + (4 * size_of::<Entry>()) as u64;
        if memory > self.limits.run_memory_bytes {
            return Err(refusal());
        }
        if self.run_memory + memory > self.limits.run_memory_bytes {
            self.spill()?;
        }
        self.entries.try_reserve(1).map_err(io::Error::other)?;
        self.entries.push(Entry {
            key: make_key(event_ms, id, bytes)?,
            row,
        });
        self.run_memory += memory;
        self.run_bytes += (RUN_HEADER + bytes) as u64;
        self.count = self.count.checked_add(1).ok_or_else(refusal)?;
        self.key_bytes = self
            .key_bytes
            .checked_add(bytes as u64)
            .ok_or_else(refusal)?;
        Ok(())
    }

    fn spill(&mut self) -> io::Result<()> {
        if self.runs.len() >= MAX_RUNS {
            return Err(refusal());
        }
        self.spilled_bytes = self
            .spilled_bytes
            .checked_add(self.run_bytes)
            .filter(|n| *n <= self.limits.disk_bytes)
            .ok_or_else(refusal)?;
        self.entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        let mut run = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(run_path(&self.dir, self.runs.len()))?;
        let mut hash = Sha256::new();
        for entry in self.entries.drain(..) {
            let mut header = [0; RUN_HEADER];
            header[..4].copy_from_slice(&(entry.key.len() as u32).to_le_bytes());
            header[4..12].copy_from_slice(&entry.row.address.segment_id.to_le_bytes());
            header[12..20].copy_from_slice(&entry.row.address.doc_id.to_le_bytes());
            header[20..].copy_from_slice(&entry.row.sha256);
            run.write_all(&header)?;
            run.write_all(entry.key.as_bytes())?;
            hash.update(header);
            hash.update(entry.key.as_bytes());
        }
        self.runs.push(RunDescriptor {
            bytes: self.run_bytes,
            sha256: hash.finalize().into(),
        });
        self.run_memory = 0;
        self.run_bytes = 0;
        Ok(())
    }

    pub(crate) fn finish(mut self, binding: Binding) -> io::Result<()> {
        let output_bytes = primary::build_bytes(self.count, self.key_bytes)
            .and_then(|n| n.checked_add(MAX_DESCRIPTOR_BYTES))
            .ok_or_else(refusal)?;
        let pending_spill = if self.runs.is_empty() {
            0
        } else {
            self.run_bytes
        };
        self.spilled_bytes
            .checked_add(pending_spill)
            .and_then(|n| n.checked_add(output_bytes))
            .filter(|n| *n <= self.limits.disk_bytes)
            .ok_or_else(refusal)?;
        let mut writer = primary::Writer::create(&self.dir, self.max_key_bytes)?;
        if self.runs.is_empty() {
            self.entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));
            for entry in self.entries.drain(..) {
                writer.push(&entry.key, entry.row)?;
            }
        } else {
            if !self.entries.is_empty() {
                self.spill()?;
            }
            drop(std::mem::take(&mut self.entries));
            drop(self.run_memory_lease);
            let mut runs = self
                .runs
                .iter()
                .enumerate()
                .map(|(slot, run)| Run::open(&run_path(&self.dir, slot), run, self.max_key_bytes))
                .collect::<io::Result<Vec<_>>>()?;
            while let Some(run) = runs.iter_mut().filter(|r| r.head.is_some()).min_by(|a, b| {
                a.head
                    .as_ref()
                    .unwrap()
                    .0
                    .key
                    .cmp(&b.head.as_ref().unwrap().0.key)
            }) {
                let (entry, memory) = run.head.take().expect("nonempty run");
                writer.push(&entry.key, entry.row)?;
                drop((entry, memory));
                run.advance(self.max_key_bytes)?;
            }
            drop(runs);
            for slot in 0..self.runs.len() {
                std::fs::remove_file(run_path(&self.dir, slot))?;
            }
        }
        let descriptor = Descriptor {
            format: 1,
            binding,
            count: self.count,
            primary: writer.finish()?,
        };
        let raw = serde_json::to_vec(&descriptor)?;
        if raw.len() as u64 > MAX_DESCRIPTOR_BYTES {
            return Err(invalid("event-time descriptor exceeds its bound"));
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.dir.join("descriptor"))?;
        file.write_all(&raw)?;
        file.sync_all()?;
        Ok(())
    }
}

fn run_path(dir: &Path, slot: usize) -> std::path::PathBuf {
    dir.join(format!("run-{slot}"))
}

struct Run {
    file: File,
    descriptor: RunDescriptor,
    hash: Sha256,
    offset: u64,
    head: Option<(Entry, MemoryLease)>,
}

#[derive(Clone, Copy)]
struct RunDescriptor {
    bytes: u64,
    sha256: Digest,
}

impl Run {
    fn open(path: &Path, descriptor: &RunDescriptor, max_key_bytes: usize) -> io::Result<Self> {
        let file = open_file(path, descriptor.bytes)?;
        let mut run = Self {
            file,
            descriptor: *descriptor,
            hash: Sha256::new(),
            offset: 0,
            head: None,
        };
        run.advance(max_key_bytes)?;
        Ok(run)
    }

    fn advance(&mut self, max_key_bytes: usize) -> io::Result<()> {
        if self.offset == self.descriptor.bytes {
            let sha256: Digest = std::mem::take(&mut self.hash).finalize().into();
            if sha256 != self.descriptor.sha256 {
                return Err(invalid("event-time sort run checksum mismatch"));
            }
            return Ok(());
        }
        let mut header = [0; RUN_HEADER];
        self.file.read_exact_at(self.offset, &mut header)?;
        let length = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
        let start = self.offset + RUN_HEADER as u64;
        let end = start
            .checked_add(length as u64)
            .filter(|end| *end <= self.descriptor.bytes)
            .ok_or_else(|| invalid("truncated event-time sort run"))?;
        if length <= TIME_BYTES || length > max_key_bytes {
            return Err(invalid("invalid event-time sort key length"));
        }
        let memory = reserve_memory(length as u64)?;
        let mut key = Vec::new();
        key.try_reserve_exact(length).map_err(io::Error::other)?;
        key.resize(length, 0);
        self.file.read_exact_at(start, &mut key)?;
        let key = String::from_utf8(key).map_err(|_| invalid("invalid event-time sort key"))?;
        self.hash.update(header);
        self.hash.update(key.as_bytes());
        self.head = Some((
            Entry {
                key,
                row: RowRef {
                    address: Address {
                        segment_id: read_u64(&header[4..12]),
                        doc_id: read_u64(&header[12..20]),
                    },
                    sha256: header[20..].try_into().unwrap(),
                },
            },
            memory,
        ));
        self.offset = end;
        Ok(())
    }
}

fn open(
    root: &Path,
    expected: &Binding,
    max_id_bytes: usize,
    descriptor_bytes: u64,
    descriptor_sha256: Digest,
) -> io::Result<primary::Index> {
    if descriptor_bytes == 0 || descriptor_bytes > MAX_DESCRIPTOR_BYTES {
        return Err(invalid("invalid event-time descriptor size"));
    }
    let dir = root.join(DIRECTORY);
    let file = open_file(&dir.join("descriptor"), descriptor_bytes)?;
    let _memory = reserve_memory(MAX_DESCRIPTOR_BYTES * 2)?;
    let mut buf = [0; MAX_DESCRIPTOR_BYTES as usize];
    let raw = &mut buf[..descriptor_bytes as usize];
    file.read_exact_at(0, raw)?;
    if digest(raw) != descriptor_sha256 {
        return Err(invalid("event-time descriptor checksum mismatch"));
    }
    let descriptor: Descriptor = serde_json::from_slice(raw)?;
    if descriptor.format != 1 || descriptor.binding != *expected {
        return Err(invalid(
            "event-time projection differs from its selected State base",
        ));
    }
    let limits = Limits {
        page_cache_bytes: 0,
        max_record_bytes: key_limit(max_id_bytes)?,
        max_files: 2,
    };
    let index = primary::Index::open(&dir, descriptor.primary, limits, File::read_exact_at)?;
    index.verify_root(descriptor.count > 0)?;
    Ok(index)
}

pub(crate) fn verify(
    root: &Path,
    expected: &Binding,
    max_id_bytes: usize,
    descriptor_bytes: u64,
    descriptor_sha256: Digest,
) -> io::Result<()> {
    open(
        root,
        expected,
        max_id_bytes,
        descriptor_bytes,
        descriptor_sha256,
    )
    .map(|_| ())
}

#[cfg(test)]
pub(crate) fn lookup_for_test(
    root: &Path,
    binding: &Binding,
    max_id_bytes: usize,
    descriptor_bytes: u64,
    descriptor_sha256: Digest,
    event_ms: i64,
    id: &str,
) -> io::Result<Option<RowRef>> {
    open(
        root,
        binding,
        max_id_bytes,
        descriptor_bytes,
        descriptor_sha256,
    )?
    .find(&make_key(event_ms, id, id.len() + TIME_BYTES)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding() -> Binding {
        Binding {
            owner: "000000000000000000000000000004f6".into(),
            lineage: [5; 16],
            cut: 20,
            rows_sha256: [6; 32],
        }
    }

    fn row(n: u64) -> RowRef {
        RowRef {
            address: Address {
                segment_id: 0,
                doc_id: n,
            },
            sha256: [n as u8; 32],
        }
    }

    #[test]
    fn event_time_checkpoint_merges_spills_in_time_then_id_order() {
        let temp = tempfile::tempdir().unwrap();
        let max_id = 1024 * 1024;
        let long = "é".repeat(max_id / 2);
        let limits = BuildLimits::for_test(max_id as u64 + 400, 4 * 1024 * 1024);
        let mut builder = Builder::new(temp.path(), max_id, limits).unwrap();
        for (time, id, n) in [
            (i64::MIN, "negative", 0),
            (5, "z", 1),
            (i64::MAX, "max", 2),
            (5, "a", 3),
            (5, long.as_str(), 4),
            (-1, "another", 5),
        ] {
            builder.push(time, id, row(n)).unwrap();
        }
        assert!(builder.runs.len() >= 2);
        builder.finish(binding()).unwrap();
        let raw = std::fs::read(temp.path().join(MEMBERS[0])).unwrap();
        let index = open(
            temp.path(),
            &binding(),
            max_id,
            raw.len() as u64,
            digest(&raw),
        )
        .unwrap();
        let mut cursor = index.cursor("", false).unwrap();
        for (time, id, n) in [
            (i64::MAX, "max", 2),
            (5, "a", 3),
            (5, "z", 1),
            (5, long.as_str(), 4),
            (-1, "another", 5),
            (i64::MIN, "negative", 0),
        ] {
            let (key, reference) = cursor.next().unwrap().unwrap();
            assert_eq!(
                key.as_str(),
                make_key(time, id, id.len() + TIME_BYTES).unwrap()
            );
            assert_eq!(reference, row(n));
        }
        assert!(cursor.next().unwrap().is_none());
        assert_eq!(
            std::fs::read_dir(temp.path().join(DIRECTORY))
                .unwrap()
                .count(),
            MEMBERS.len()
        );
    }

    #[test]
    fn event_time_checkpoint_discards_corrupt_sort_run() {
        let temp = tempfile::tempdir().unwrap();
        let limits = BuildLimits::for_test(400, 32 * 1024);
        let mut builder = Builder::new(temp.path(), 4096, limits).unwrap();
        builder.push(1, "a", row(1)).unwrap();
        builder.push(2, "b", row(2)).unwrap();
        let run = run_path(&temp.path().join(DIRECTORY), 0);
        let mut raw = std::fs::read(&run).unwrap();
        raw[20] ^= 1; // The encoded row hash changes without changing extent or key order.
        std::fs::write(run, raw).unwrap();
        let error = builder.finish(binding()).unwrap_err();
        assert!(
            error.to_string().contains("run checksum mismatch"),
            "{error}"
        );
        assert!(!temp.path().join(MEMBERS[0]).exists());
    }
}
