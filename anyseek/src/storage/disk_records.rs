//! Prepared disk directories and pinned reads for the local checkpoint cut.
//! The partition will authenticate and select Root. This module never elects
//! a root from the filesystem, publishes WAL decisions, or retires files.
use super::build_budget::{File, MemoryLease, OpenOptions, reserve_memory};
use crate::record::ExpectedVersion;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeMap,
    io::{self, Write},
    path::Path,
    sync::Arc,
};

pub(crate) mod primary;
pub(crate) mod rows;

use rows::{Metadata, PayloadAddress};

type Digest = [u8; 32];
// #225 supplies build_budget::File::read_exact_at at the integration boundary.
// Keeping the operation injectable avoids a second platform implementation.
pub(crate) type ReadRange = fn(&File, u64, &mut [u8]) -> io::Result<()>;

fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

fn digest(bytes: &[u8]) -> Digest {
    Sha256::digest(bytes).into()
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("fixed integer width"))
}

fn extent(header: u64, count: u64, width: u64) -> io::Result<u64> {
    count
        .checked_mul(width)
        .and_then(|bytes| header.checked_add(bytes))
        .ok_or_else(|| invalid("record directory extent overflow"))
}

fn open_file(path: &Path, expected_len: u64) -> io::Result<File> {
    if !std::fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid("record directory member is not a regular file"));
    }
    let file = File::open(path)?;
    let meta = file.metadata()?;
    if !meta.is_file() || meta.len() != expected_len {
        return Err(invalid("record directory member length mismatch"));
    }
    Ok(file)
}

fn create_file(path: &Path, magic: &[u8; 16]) -> io::Result<File> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(magic)?;
    Ok(file)
}

fn check_magic(file: &File, magic: &[u8; 16], read: ReadRange) -> io::Result<()> {
    let mut header = [0; 16];
    read(file, 0, &mut header)?;
    if header != *magic {
        return Err(invalid("unsupported record directory member"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Address {
    pub segment_id: u64,
    pub doc_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RowRef {
    pub address: Address,
    sha256: Digest,
}

/// Part of the authenticated checkpoint, alongside its other selected inputs.
/// All filenames are derived locally from this descriptor.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Root {
    pub generation: [u8; 16],
    pub committed_cut: u64,
    pub primary: primary::Descriptor,
    pub segments: BTreeMap<u64, u64>,
    pub payloads: BTreeMap<String, u64>,
}

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub page_cache_bytes: usize,
    pub max_record_bytes: usize,
    pub max_files: usize,
}

pub(crate) struct Checkpoint {
    generation: [u8; 16],
    committed_cut: u64,
    primary: primary::Index,
    segments: BTreeMap<u64, (u64, File)>,
    payloads: BTreeMap<[u8; 16], (u64, File)>,
    max_record_bytes: usize,
    read: ReadRange,
    _memory: MemoryLease,
}

impl Checkpoint {
    /// Pin all selected members as opened files. The owner must retain this
    /// checkpoint in its GC roots while readers, builds or backups hold it.
    pub(crate) fn open(
        dir: &Path,
        payload_dir: &Path,
        root: Root,
        limits: Limits,
        read: ReadRange,
    ) -> io::Result<Arc<Self>> {
        if root
            .segments
            .len()
            .checked_add(root.payloads.len())
            .and_then(|n| n.checked_add(2))
            .is_none_or(|n| n > limits.max_files)
        {
            return Err(invalid("record directory file budget exceeded"));
        }
        // Charge retained descriptor/handle entries and paths, independently
        // from the page cache and any body returned to a reader.
        let members = root.segments.len() + root.payloads.len() + 2;
        let per_file = 256usize
            .checked_add(dir.as_os_str().len().max(payload_dir.as_os_str().len()))
            .ok_or_else(|| invalid("record directory path budget overflow"))?;
        let memory = reserve_memory(
            members
                .checked_mul(per_file)
                .ok_or_else(|| invalid("record directory memory budget overflow"))?
                as u64,
        )?;
        let primary = primary::Index::open(dir, root.primary, limits, read)?;
        let segments = root
            .segments
            .into_iter()
            .map(|(id, count)| Ok((id, (count, rows::open(dir, id, count, read)?))))
            .collect::<io::Result<_>>()?;
        let payloads = root
            .payloads
            .into_iter()
            .map(|(id, bytes)| {
                let path = super::payload_pool::path(payload_dir, &id)?;
                let id = u128::from_str_radix(&id, 16)
                    .map_err(|_| invalid("invalid payload object identity"))?
                    .to_be_bytes();
                let file = open_file(&path, bytes)?;
                super::segment::check_payload_header(bytes, |header| read(&file, 0, header))?;
                Ok((id, (bytes, file)))
            })
            .collect::<io::Result<_>>()?;
        Ok(Arc::new(Self {
            generation: root.generation,
            committed_cut: root.committed_cut,
            primary,
            segments,
            payloads,
            max_record_bytes: limits.max_record_bytes,
            read,
            _memory: memory,
        }))
    }

    fn row(&self, id: &str, reference: RowRef, at_ms: u64) -> io::Result<Located<'_>> {
        let (count, file) = self
            .segments
            .get(&reference.address.segment_id)
            .ok_or_else(|| invalid("record row references an unselected segment"))?;
        let row = rows::read(file, *count, id, reference, self.read)?;
        Ok(Located {
            metadata: row.metadata,
            address: Some(reference.address),
            at_ms,
            body: row.payload.map(|payload| Body::Sealed(self, payload)),
        })
    }

    /// Visit every authenticated base row in key order, including tombstones.
    /// The callback borrows one row at a time and must admit any retained value.
    pub(crate) fn visit_sealed<E>(
        &self,
        error: impl Fn(io::Error) -> E,
        mut accept: impl FnMut(&str, Located<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        let mut rows = self.primary.cursor("", false).map_err(&error)?;
        while let Some((id, reference)) = rows.next().map_err(&error)? {
            accept(
                id.as_str(),
                self.row(id.as_str(), reference, 0).map_err(&error)?,
            )?;
        }
        Ok(())
    }

    fn payload(&self, address: &PayloadAddress) -> io::Result<BodyBytes> {
        let (extent, file) = self
            .payloads
            .get(&address.object)
            .ok_or_else(|| invalid("record payload references an unselected object"))?;
        let length = address
            .range
            .bounded_length(*extent, self.max_record_bytes)?;
        let memory = reserve_memory(length as u64)?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(length).map_err(io::Error::other)?;
        bytes.resize(length, 0);
        (self.read)(file, address.range.offset, &mut bytes)?;
        address.range.verify(&bytes)?;
        Ok(BodyBytes {
            bytes,
            _memory: memory,
        })
    }
}

pub(crate) struct BodyBytes {
    bytes: Vec<u8>,
    _memory: MemoryLease,
}
impl AsRef<[u8]> for BodyBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

enum Body<'a> {
    Sealed(&'a Checkpoint, PayloadAddress),
    Recent(&'a [u8]),
}

pub(crate) struct Located<'a> {
    pub metadata: Metadata,
    pub address: Option<Address>,
    at_ms: u64,
    body: Option<Body<'a>>,
}

impl Located<'_> {
    /// Tombstones and expired records have no readable body at this view's time.
    /// Invalid references and short/corrupt reads remain storage errors.
    pub(crate) fn with_body<T>(&self, use_body: impl FnOnce(&[u8]) -> T) -> io::Result<Option<T>> {
        if !self.metadata.visible(self.at_ms) {
            return Ok(None);
        }
        self.with_canonical_body(use_body)?
            .map(Some)
            .ok_or_else(|| invalid("live record has no body"))
    }

    /// Load the selected bytes for checkpoint verification and replay, even
    /// when the original live row has crossed a request's TTL boundary.
    pub(crate) fn with_canonical_body<T>(
        &self,
        use_body: impl FnOnce(&[u8]) -> T,
    ) -> io::Result<Option<T>> {
        match self.body.as_ref() {
            Some(Body::Sealed(checkpoint, address)) => {
                let bytes = checkpoint.payload(address)?;
                Ok(Some(use_body(bytes.as_ref())))
            }
            Some(Body::Recent(bytes)) => Ok(Some(use_body(bytes))),
            None => Ok(None),
        }
    }

    /// Compaction may retain the authenticated object without reading its body.
    pub(crate) fn sealed_payload(&self) -> Option<&PayloadAddress> {
        match self.body.as_ref() {
            Some(Body::Sealed(_, address)) => Some(address),
            _ => None,
        }
    }
}

pub(crate) struct CommittedChange {
    pub id: String,
    pub metadata: Metadata,
    pub body: Option<Vec<u8>>,
}

struct RecentRow {
    metadata: Metadata,
    body: Option<Vec<u8>>,
    _memory: MemoryLease,
}

// The overlay retains every batch still needed by this published root. A
// checkpoint seals the cut before exceeding either the byte or depth budget.
const MAX_RECENT_BATCHES: usize = 64;
type RecentBatch = BTreeMap<String, RecentRow>;
type RecentCursor<'a> =
    std::iter::Peekable<std::collections::btree_map::Range<'a, String, RecentRow>>;

/// Single-root read view. Fill its bounded tail from already committed decisions
/// before sharing it with readers. Its Arc retains the selected file owner.
pub(crate) struct View {
    checkpoint: Arc<Checkpoint>,
    visible_seq: u64,
    recent_limit: usize,
    recent_bytes: usize,
    recent: Vec<Arc<RecentBatch>>,
}

pub(crate) struct Page<'a> {
    pub entries: Vec<(primary::KeyOwned<'a>, Located<'a>)>,
    pub has_more: bool,
    _memory: MemoryLease,
}

impl View {
    pub(crate) fn new(checkpoint: Arc<Checkpoint>, recent_limit: usize) -> Self {
        Self {
            visible_seq: checkpoint.committed_cut,
            checkpoint,
            recent_limit,
            recent_bytes: 0,
            recent: Vec::new(),
        }
    }

    pub(crate) fn stamp(&self) -> ([u8; 16], u64, u64) {
        (
            self.checkpoint.generation,
            self.checkpoint.committed_cut,
            self.visible_seq,
        )
    }

    /// Prepare a privately owned successor before committing authority. The
    /// bounded overlay shares bodies with old views and never copies the disk
    /// directory. After authority and indexes publish, the caller installs
    /// this complete successor with a pointer swap under its publication guard.
    pub(crate) fn prepare_successor(
        &self,
        seq: u64,
        changes: Vec<CommittedChange>,
    ) -> io::Result<Self> {
        let mut next = Self {
            checkpoint: self.checkpoint.clone(),
            visible_seq: self.visible_seq,
            recent_limit: self.recent_limit,
            recent_bytes: self.recent_bytes,
            recent: self.recent.clone(),
        };
        next.install_committed(seq, changes)?;
        Ok(next)
    }

    /// Install one complete committed decision. Failed admission or validation
    /// leaves the entire previous view usable, including its published cut.
    pub(crate) fn install_committed(
        &mut self,
        seq: u64,
        changes: Vec<CommittedChange>,
    ) -> io::Result<()> {
        if self.visible_seq.checked_add(1) != Some(seq) {
            return Err(invalid("nonconsecutive committed record decision"));
        }
        if !changes.is_empty() && self.recent.len() >= MAX_RECENT_BATCHES {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "recent decision depth exceeded",
            ));
        }
        let mut prepared = RecentBatch::new();
        let mut bytes = self.recent_bytes;
        for change in changes {
            let CommittedChange { id, metadata, body } = change;
            if id.is_empty()
                || id.len() > self.checkpoint.max_record_bytes
                || prepared.contains_key(id.as_str())
                || metadata.is_live() != body.is_some()
                || body
                    .as_ref()
                    .is_some_and(|body| body.len() > self.checkpoint.max_record_bytes)
            {
                return Err(invalid("invalid committed record change"));
            }
            metadata.validate()?;
            let charge = id
                .capacity()
                .checked_add(body.as_ref().map_or(0, Vec::capacity))
                .and_then(|bytes| bytes.checked_add(size_of::<RecentRow>() + 128))
                .ok_or_else(|| invalid("recent record budget overflow"))?;
            bytes = bytes
                .checked_add(charge)
                .filter(|bytes| *bytes <= self.recent_limit)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "recent record byte budget exceeded",
                    )
                })?;
            let memory = reserve_memory(charge as u64)?;
            prepared.insert(
                id,
                RecentRow {
                    metadata,
                    body,
                    _memory: memory,
                },
            );
        }
        if !prepared.is_empty() {
            self.recent.push(Arc::new(prepared));
        }
        self.recent_bytes = bytes;
        self.visible_seq = seq;
        Ok(())
    }

    pub(crate) fn located(&self, id: &str, at_ms: u64) -> io::Result<Option<Located<'_>>> {
        if let Some(row) = self.recent.iter().rev().find_map(|batch| batch.get(id)) {
            return Ok(Some(recent_location(row, at_ms)));
        }
        self.checkpoint
            .primary
            .find(id)?
            .map(|reference| self.checkpoint.row(id, reference, at_ms))
            .transpose()
    }

    pub(crate) fn get(&self, id: &str, at_ms: u64) -> io::Result<Option<Located<'_>>> {
        Ok(self
            .located(id, at_ms)?
            .filter(|row| row.metadata.visible(at_ms)))
    }

    pub(crate) fn matches(
        &self,
        id: &str,
        expected: &ExpectedVersion,
        at_ms: u64,
    ) -> io::Result<bool> {
        Ok(match expected {
            ExpectedVersion::Any => true,
            ExpectedVersion::Absent => self.get(id, at_ms)?.is_none(),
            ExpectedVersion::Match(version) => self
                .located(id, at_ms)?
                .is_some_and(|row| row.metadata.version == *version),
        })
    }

    /// Merge an exclusive ID cursor with the ordered tail. Returning a page
    /// touches metadata only; it does not allocate or verify unrelated bodies.
    pub(crate) fn page(&self, after: &str, limit: usize, at_ms: u64) -> io::Result<Page<'_>> {
        use std::{
            cmp::Ordering,
            ops::Bound::{Excluded, Unbounded},
        };
        if limit == 0 {
            return Err(invalid("empty record page limit"));
        }
        let capacity = limit
            .checked_mul(size_of::<(primary::KeyOwned<'_>, Located<'_>)>())
            .ok_or_else(|| invalid("record page budget overflow"))?;
        let memory = reserve_memory(capacity as u64)?;
        let mut entries = Vec::new();
        entries.try_reserve_exact(limit).map_err(io::Error::other)?;
        let mut disk = self.checkpoint.primary.cursor(after, true)?;
        let mut next_disk = disk.next()?;
        let mut tail: Vec<_> = self
            .recent
            .iter()
            .map(|batch| {
                batch
                    .range::<str, _>((Excluded(after), Unbounded))
                    .peekable()
            })
            .collect();
        let mut next_tail = next_recent(&mut tail);
        let has_more = loop {
            let use_disk = match (&next_disk, &next_tail) {
                (None, None) => break false,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((id, _)), Some((recent, _))) => match id.as_str().cmp(recent) {
                    Ordering::Less => true,
                    Ordering::Greater => false,
                    Ordering::Equal => {
                        next_disk = disk.next()?;
                        false
                    }
                },
            };
            let (id, row) = if use_disk {
                let (id, reference) = next_disk.take().expect("selected disk record");
                next_disk = disk.next()?;
                let row = self.checkpoint.row(id.as_str(), reference, at_ms)?;
                (id, row)
            } else {
                let (id, row) = next_tail.take().expect("selected recent record");
                next_tail = next_recent(&mut tail);
                (primary::KeyOwned::Borrowed(id), recent_location(row, at_ms))
            };
            if row.metadata.visible(at_ms) {
                if entries.len() == limit {
                    break true;
                }
                entries.push((id, row));
            }
        };
        Ok(Page {
            entries,
            has_more,
            _memory: memory,
        })
    }
}

fn next_recent<'a>(tail: &mut [RecentCursor<'a>]) -> Option<(&'a str, &'a RecentRow)> {
    let key = tail
        .iter_mut()
        .filter_map(|cursor| cursor.peek().map(|(id, _)| id.as_str()))
        .min()?;
    let mut selected = None;
    // Oldest to newest, consuming all entries for this ID. The latest
    // tombstone remains a real entry and shadows every earlier version.
    for cursor in tail {
        if cursor.peek().is_some_and(|(id, _)| id.as_str() == key) {
            selected = cursor.next();
        }
    }
    selected.map(|(id, row)| (id.as_str(), row))
}

fn recent_location(row: &RecentRow, at_ms: u64) -> Located<'_> {
    Located {
        metadata: row.metadata.clone(),
        address: None,
        at_ms,
        body: row.body.as_deref().map(Body::Recent),
    }
}

#[cfg(test)]
mod tests;
