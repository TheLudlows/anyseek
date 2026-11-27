//! Optional ordered entity declarations within one authenticated Record base.
use super::*;
use crate::{index::Document, record::EntityDirection};
use std::fmt::Write as _;

pub(crate) const DIRECTORY: &str = "entity-adjacency";
pub(crate) const MEMBERS: [&str; 3] = [
    "entity-adjacency/descriptor",
    "entity-adjacency/primary-keys",
    "entity-adjacency/primary-pages",
];
const MAX_DESCRIPTOR: usize = 4096;
const MAX_KEY: usize = 1800;
const MAX_KEYS: usize = 8192;
const MAX_BUILD_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const MAX_VISITS: usize = 256;

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
    count: usize,
    primary: primary::Descriptor,
}

#[derive(Debug)]
pub(crate) struct Rejected(&'static str);
impl std::fmt::Display for Rejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for Rejected {}

fn reject(reason: &'static str) -> io::Error {
    io::Error::other(Rejected(reason))
}

fn append_hex(key: &mut String, value: &str) {
    for byte in value.bytes() {
        write!(key, "{byte:02x}").expect("String write");
    }
}

fn token(value: &str, limit: usize) -> bool {
    !value.is_empty()
        && value.len() <= limit
        && !value.chars().any(|c| c.is_control() || c.is_whitespace())
}

fn entity_id(value: &str) -> bool {
    token(value, 256)
        && value
            .split_once(':')
            .is_some_and(|(ns, name)| !ns.is_empty() && !name.is_empty())
}

fn prefix(direction: EntityDirection, anchor: &str, kind: &str) -> String {
    let mut key = match direction {
        EntityDirection::Out => "o/".to_owned(),
        EntityDirection::In => "i/".to_owned(),
    };
    append_hex(&mut key, anchor);
    key.push('/');
    append_hex(&mut key, kind);
    key.push('/');
    key
}

pub(crate) struct Range {
    lower: String,
    upper: String,
    direction: EntityDirection,
    anchor: String,
    kind: String,
}

impl Range {
    pub(crate) fn new(direction: EntityDirection, anchor: &str, kind: &str) -> io::Result<Self> {
        if !entity_id(anchor) || !token(kind, 128) {
            return Err(reject("invalid entity adjacency range"));
        }
        let lower = prefix(direction, anchor, kind);
        // '/' sorts immediately before the smallest hex digit.
        let mut upper = lower[..lower.len() - 1].to_owned();
        upper.push('0');
        Ok(Self {
            lower,
            upper,
            direction,
            anchor: anchor.to_owned(),
            kind: kind.to_owned(),
        })
    }

    pub(crate) fn tail_keys(&self, id: &str, document: &Document) -> io::Result<Vec<String>> {
        let mut keys = Vec::new();
        visit_edges(id, document, |from, kind, to| {
            let (anchor, neighbor) = self.direction.endpoints(from, to);
            if anchor == self.anchor && kind == self.kind {
                keys.push(finish_key(self.lower.clone(), neighbor, id));
            }
            Ok(())
        })?;
        keys.sort_unstable();
        keys.dedup();
        Ok(keys)
    }

    pub(crate) fn identity(&self, key: &str) -> io::Result<(String, String)> {
        let (neighbor, id) = key
            .strip_prefix(&self.lower)
            .and_then(|suffix| suffix.split_once('/'))
            .ok_or_else(|| invalid("invalid entity posting key"))?;
        let neighbor = decode_hex(neighbor)?;
        if !entity_id(&neighbor) {
            return Err(invalid("invalid entity posting neighbor"));
        }
        Ok((neighbor, decode_hex(id)?))
    }
}

fn decode_hex(value: &str) -> io::Result<String> {
    if value.is_empty() || value.len() > 512 || value.len() % 2 != 0 {
        return Err(invalid("invalid entity posting component length"));
    }
    let hex = |byte| match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(invalid("invalid entity posting hex")),
    };
    let bytes = value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((hex(pair[0])? << 4) | hex(pair[1])?))
        .collect::<io::Result<Vec<_>>>()?;
    String::from_utf8(bytes).map_err(|_| invalid("entity posting component is not UTF-8"))
}

fn finish_key(mut key: String, neighbor: &str, id: &str) -> String {
    append_hex(&mut key, neighbor);
    key.push('/');
    append_hex(&mut key, id);
    key
}

fn visit_edges(
    id: &str,
    document: &Document,
    mut visit: impl FnMut(&str, &str, &str) -> io::Result<()>,
) -> io::Result<()> {
    let Some(edges) = document
        .get("relations")
        .and_then(|relations| relations.get("edges"))
    else {
        return Ok(());
    };
    let edges = edges
        .as_array()
        .filter(|edges| edges.len() <= 64)
        .ok_or_else(|| reject("relations.edges requires an array of at most 64 declarations"))?;
    if !edges.is_empty() && (id.is_empty() || id.len() > 256) {
        return Err(reject("entity declaration Record ID exceeds 256 bytes"));
    }
    for edge in edges {
        let field = |name| {
            edge.get(name)
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| reject("entity declaration requires from, type and to strings"))
        };
        let (from, kind, to) = (field("from")?, field("type")?, field("to")?);
        if !entity_id(from) || !entity_id(to) || !token(kind, 128) {
            return Err(reject("invalid entity declaration endpoint or type"));
        }
        visit(from, kind, to)?;
    }
    Ok(())
}

pub(crate) struct Builder {
    entries: Vec<(String, RowRef)>,
    memory: MemoryLease,
}

impl Builder {
    pub(crate) fn new() -> io::Result<Self> {
        Ok(Self {
            entries: Vec::new(),
            memory: reserve_memory(0)?,
        })
    }

    pub(crate) fn push(&mut self, id: &str, document: &Document, row: RowRef) -> io::Result<()> {
        visit_edges(id, document, |from, kind, to| {
            for direction in [EntityDirection::Out, EntityDirection::In] {
                let (anchor, neighbor) = direction.endpoints(from, to);
                let bytes = 2 * (anchor.len() + kind.len() + neighbor.len() + id.len()) + 5;
                // Include geometric String and Vec capacities and their reallocations.
                let charge = (4 * (bytes + size_of::<(String, RowRef)>())) as u64;
                if self.entries.len() >= MAX_KEYS || self.memory.bytes() + charge > MAX_BUILD_BYTES
                {
                    return Err(reject("entity adjacency build budget exhausted"));
                }
                self.memory.grow(charge)?;
                self.entries.try_reserve(1).map_err(io::Error::other)?;
                self.entries.push((
                    finish_key(prefix(direction, anchor, kind), neighbor, id),
                    row,
                ));
            }
            Ok(())
        })
    }

    pub(crate) fn finish(mut self, root: &Path, binding: Binding) -> io::Result<()> {
        self.entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        self.entries.dedup_by(|a, b| a.0 == b.0);
        let dir = root.join(DIRECTORY);
        std::fs::create_dir(&dir)?;
        let mut writer = primary::Writer::create(&dir, MAX_KEY)?;
        for (key, row) in &self.entries {
            writer.push(key, *row)?;
        }
        let descriptor = Descriptor {
            format: 1,
            binding,
            count: self.entries.len(),
            primary: writer.finish()?,
        };
        let raw = serde_json::to_vec(&descriptor)?;
        if raw.len() > MAX_DESCRIPTOR {
            return Err(invalid("entity adjacency descriptor exceeds its bound"));
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join("descriptor"))?;
        file.write_all(&raw)?;
        file.sync_all()
    }
}

pub(crate) struct Projection {
    index: primary::Index,
    cut: u64,
    _memory: MemoryLease,
}

impl Projection {
    pub(crate) fn open(
        root: &Path,
        binding: &Binding,
        bytes: u64,
        sha: Digest,
    ) -> io::Result<Self> {
        if !(1..=MAX_DESCRIPTOR as u64).contains(&bytes) {
            return Err(invalid("invalid entity adjacency descriptor extent"));
        }
        let dir = root.join(DIRECTORY);
        let file = open_file(&dir.join("descriptor"), bytes)?;
        let _scratch = reserve_memory(2 * MAX_DESCRIPTOR as u64)?;
        let mut raw = [0; MAX_DESCRIPTOR];
        let raw = &mut raw[..bytes as usize];
        file.read_exact_at(0, raw)?;
        if digest(raw) != sha {
            return Err(invalid("entity adjacency descriptor checksum mismatch"));
        }
        let descriptor: Descriptor = serde_json::from_slice(raw)?;
        if descriptor.format != 1 || descriptor.binding != *binding || descriptor.count > MAX_KEYS {
            return Err(invalid(
                "entity adjacency differs from its selected State base",
            ));
        }
        let memory = reserve_memory(1024 + 2 * dir.as_os_str().len() as u64)?;
        let index = primary::Index::open(
            &dir,
            descriptor.primary,
            Limits {
                page_cache_bytes: 0,
                max_record_bytes: MAX_KEY,
                max_files: 2,
            },
            File::read_exact_at,
        )?;
        index.verify_root(descriptor.count > 0)?;
        Ok(Self {
            index,
            cut: binding.cut,
            _memory: memory,
        })
    }

    pub(crate) fn cut(&self) -> u64 {
        self.cut
    }

    pub(crate) fn visit<E>(
        &self,
        range: &Range,
        map: impl Fn(io::Error) -> E,
        mut visit: impl FnMut(&str, RowRef) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut cursor = self.index.cursor(&range.lower, false).map_err(&map)?;
        while let Some((key, row)) = cursor.next().map_err(&map)? {
            let key = key.as_str();
            if key >= range.upper.as_str() {
                break;
            }
            if !key.starts_with(&range.lower) {
                return Err(map(invalid("entity posting outside selected range")));
            }
            visit(key, row)?;
        }
        Ok(())
    }

    pub(crate) fn locate<'a>(
        &self,
        base: &'a Checkpoint,
        id: &str,
        row: RowRef,
    ) -> io::Result<Located<'a>> {
        if base.committed_cut != self.cut || base.primary.find(id)? != Some(row) {
            return Err(invalid(
                "entity posting differs from selected canonical row",
            ));
        }
        let row = base.row(id, row, 0)?;
        if !row.metadata.is_live() {
            return Err(invalid("entity posting refers to a tombstone"));
        }
        Ok(row)
    }
}
