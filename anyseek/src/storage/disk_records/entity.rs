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
// A separate certified capability keeps the edge descriptor readable by
// format-1 readers. Its absence means the member range was never built.
pub(crate) const MEMBER_CAPABILITY: &str = "entity-membership";
const MEMBER_MAGIC: &[u8; 16] = b"ANYSEEK-EMBR-001";
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

pub(crate) struct Bounds {
    lower: String,
    upper: String,
}

impl Bounds {
    fn prefix(lower: String) -> Self {
        // '/' sorts immediately before the smallest hex digit.
        let mut upper = lower[..lower.len() - 1].to_owned();
        upper.push('0');
        Self { lower, upper }
    }

    fn suffix<'a>(&self, key: &'a str) -> io::Result<&'a str> {
        key.strip_prefix(&self.lower)
            .ok_or_else(|| invalid("invalid entity posting key"))
    }
}

pub(crate) struct Range {
    pub(crate) bounds: Bounds,
    direction: EntityDirection,
    anchor: String,
    kind: String,
}

impl Range {
    pub(crate) fn new(direction: EntityDirection, anchor: &str, kind: &str) -> io::Result<Self> {
        if !entity_id(anchor) || !token(kind, 128) {
            return Err(reject("invalid entity adjacency range"));
        }
        Ok(Self {
            bounds: Bounds::prefix(prefix(direction, anchor, kind)),
            direction,
            anchor: anchor.to_owned(),
            kind: kind.to_owned(),
        })
    }

    pub(crate) fn tail_keys(
        &self,
        id: &str,
        document: &Document,
        examine: impl FnMut() -> io::Result<()>,
    ) -> io::Result<Vec<String>> {
        let mut keys = Vec::new();
        self.matching_neighbors(id, document, examine, |neighbor| {
            keys.push(finish_key(self.bounds.lower.clone(), neighbor, id));
        })?;
        keys.sort_unstable();
        keys.dedup();
        Ok(keys)
    }

    pub(crate) fn matches_neighbor(
        &self,
        id: &str,
        document: &Document,
        neighbor: &str,
        examine: impl FnMut() -> io::Result<()>,
    ) -> io::Result<bool> {
        let mut matched = false;
        self.matching_neighbors(id, document, examine, |value| matched |= value == neighbor)?;
        Ok(matched)
    }

    fn matching_neighbors(
        &self,
        id: &str,
        document: &Document,
        mut examine: impl FnMut() -> io::Result<()>,
        mut accept: impl FnMut(&str),
    ) -> io::Result<()> {
        visit_edges(id, document, |from, kind, to| {
            examine()?;
            let (anchor, neighbor) = self.direction.endpoints(from, to);
            if anchor == self.anchor && kind == self.kind {
                accept(neighbor);
            }
            Ok(())
        })
    }

    pub(crate) fn identity(&self, key: &str) -> io::Result<(String, String)> {
        let (neighbor, id) = self
            .bounds
            .suffix(key)?
            .split_once('/')
            .ok_or_else(|| invalid("invalid entity posting key"))?;
        let neighbor = decode_hex(neighbor)?;
        if !entity_id(&neighbor) {
            return Err(invalid("invalid entity posting neighbor"));
        }
        Ok((neighbor, decode_hex(id)?))
    }
}

pub(crate) struct MemberRange {
    pub(crate) bounds: Bounds,
    entity: String,
}

fn member_prefix(entity: &str) -> String {
    let mut prefix = "m/".to_owned();
    append_hex(&mut prefix, entity);
    prefix.push('/');
    prefix
}

impl MemberRange {
    pub(crate) fn new(entity: &str) -> io::Result<Self> {
        if !entity_id(entity) {
            return Err(reject("invalid entity member range"));
        }
        Ok(Self {
            bounds: Bounds::prefix(member_prefix(entity)),
            entity: entity.to_owned(),
        })
    }

    pub(crate) fn record_id(&self, key: &str) -> io::Result<String> {
        decode_hex(self.bounds.suffix(key)?)
    }

    pub(crate) fn matches(
        &self,
        id: &str,
        document: &Document,
        mut examine: impl FnMut() -> io::Result<()>,
    ) -> io::Result<bool> {
        let mut matched = false;
        visit_members(id, document, |entity| {
            examine()?;
            matched |= entity == self.entity;
            Ok(())
        })?;
        Ok(matched)
    }
}

fn visit_members(
    id: &str,
    document: &Document,
    mut visit: impl FnMut(&str) -> io::Result<()>,
) -> io::Result<()> {
    let Some(members) = document.get("entity_ids") else {
        return Ok(());
    };
    let members = members
        .as_array()
        .filter(|items| items.len() <= 32)
        .ok_or_else(|| reject("entity_ids requires an array of at most 32 namespaced IDs"))?;
    if !members.is_empty() && (id.is_empty() || id.len() > 256) {
        return Err(reject("entity member Record ID exceeds 256 bytes"));
    }
    for member in members {
        let entity = member
            .as_str()
            .filter(|id| entity_id(id))
            .ok_or_else(|| reject("invalid entity member ID"))?;
        visit(entity)?;
    }
    Ok(())
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
                self.push_key(bytes, row, || {
                    finish_key(prefix(direction, anchor, kind), neighbor, id)
                })?;
            }
            Ok(())
        })?;
        visit_members(id, document, |entity| {
            self.push_key(2 * (entity.len() + id.len()) + 3, row, || {
                let mut key = member_prefix(entity);
                append_hex(&mut key, id);
                key
            })
        })
    }

    fn push_key(
        &mut self,
        bytes: usize,
        row: RowRef,
        key: impl FnOnce() -> String,
    ) -> io::Result<()> {
        // Include geometric String and Vec capacities and their reallocations.
        let charge = (4 * (bytes + size_of::<(String, RowRef)>())) as u64;
        if self.entries.len() >= MAX_KEYS || self.memory.bytes() + charge > MAX_BUILD_BYTES {
            return Err(reject("entity adjacency build budget exhausted"));
        }
        self.memory.grow(charge)?;
        self.entries.try_reserve(1).map_err(io::Error::other)?;
        self.entries.push((key(), row));
        Ok(())
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
        file.sync_all()?;
        create_file(&root.join(MEMBER_CAPABILITY), MEMBER_MAGIC)?.sync_all()
    }
}

pub(crate) struct Projection {
    index: primary::Index,
    cut: u64,
    members: bool,
    _memory: MemoryLease,
}

impl Projection {
    pub(crate) fn open(
        root: &Path,
        binding: &Binding,
        bytes: u64,
        sha: Digest,
        member_proof: Option<(u64, Digest)>,
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
        if let Some((bytes, sha)) = member_proof {
            if bytes != MEMBER_MAGIC.len() as u64 || sha != digest(MEMBER_MAGIC) {
                return Err(invalid("unsupported entity membership capability"));
            }
            let file = open_file(&root.join(MEMBER_CAPABILITY), bytes)?;
            check_magic(&file, MEMBER_MAGIC, File::read_exact_at)?;
        }
        Ok(Self {
            index,
            cut: binding.cut,
            members: member_proof.is_some(),
            _memory: memory,
        })
    }

    pub(crate) fn cut(&self) -> u64 {
        self.cut
    }

    pub(crate) fn has_members(&self) -> bool {
        self.members
    }

    pub(crate) fn visit<E>(
        &self,
        bounds: &Bounds,
        map: impl Fn(io::Error) -> E,
        mut examine: impl FnMut() -> Result<(), E>,
        mut visit: impl FnMut(&str, RowRef) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut cursor = self.index.cursor(&bounds.lower, false).map_err(&map)?;
        while let Some((key, row)) = cursor.next().map_err(&map)? {
            examine()?;
            let key = key.as_str();
            if key >= bounds.upper.as_str() {
                break;
            }
            if !key.starts_with(&bounds.lower) {
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
