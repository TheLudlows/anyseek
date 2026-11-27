//! Fixed-width rows addressed by physical segment and zero-based ordinal.
use super::*;
use crate::{record::TemporalMetadata, storage::segment::PayloadRef};

const MAGIC: &[u8; 16] = b"ANYSEEK-ROWS-001";
const ROW_BYTES: u64 = 160;
const HEADER: u64 = MAGIC.len() as u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Metadata {
    pub version: u64,
    pub ingested_at_ms: u64,
    pub temporal: TemporalMetadata,
    pub expired_at_ms: Option<u64>,
    /// Exact fingerprint already used for candidate validation, independent
    /// of the payload digest. A tombstone has neither fingerprint nor body.
    pub fingerprint: Option<Digest>,
}

impl Metadata {
    pub(crate) fn is_live(&self) -> bool {
        self.fingerprint.is_some()
    }

    pub(crate) fn visible(&self, at_ms: u64) -> bool {
        self.is_live() && self.temporal.expire_at_ms.is_none_or(|end| at_ms < end)
    }

    pub(super) fn validate(&self) -> io::Result<()> {
        if self.version == 0
            || self
                .temporal
                .valid_from_ms
                .zip(self.temporal.valid_to_ms)
                .is_some_and(|(from, to)| from >= to)
            || self.expired_at_ms.is_some_and(|expired| {
                self.is_live()
                    || self
                        .temporal
                        .expire_at_ms
                        .is_none_or(|deadline| expired < deadline)
            })
        {
            return Err(invalid("invalid record row metadata"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PayloadAddress {
    pub object: [u8; 16],
    pub range: PayloadRef,
}

pub(crate) struct Row {
    pub metadata: Metadata,
    pub payload: Option<PayloadAddress>,
}

impl Row {
    fn encode(&self) -> io::Result<[u8; ROW_BYTES as usize]> {
        self.metadata.validate()?;
        if self.metadata.is_live() != self.payload.is_some() {
            return Err(invalid("record row state and payload disagree"));
        }
        let meta = &self.metadata;
        let mut bytes = [0; ROW_BYTES as usize];
        bytes[..8].copy_from_slice(&meta.version.to_le_bytes());
        bytes[8..16].copy_from_slice(&meta.ingested_at_ms.to_le_bytes());
        let optional = [
            meta.temporal.event_time_ms.map(i64::to_le_bytes),
            meta.temporal.valid_from_ms.map(i64::to_le_bytes),
            meta.temporal.valid_to_ms.map(i64::to_le_bytes),
            meta.temporal.expire_at_ms.map(u64::to_le_bytes),
            meta.expired_at_ms.map(u64::to_le_bytes),
        ];
        for (i, value) in optional.into_iter().enumerate() {
            if let Some(value) = value {
                let offset = 16 + i * 8;
                bytes[offset..offset + 8].copy_from_slice(&value);
                bytes[56] |= 1 << i;
            }
        }
        if let Some(payload) = &self.payload {
            bytes[56] |= 1 << 5;
            bytes[64..96].copy_from_slice(&meta.fingerprint.expect("validated record state"));
            bytes[96..112].copy_from_slice(&payload.object);
            bytes[112..120].copy_from_slice(&payload.range.offset.to_le_bytes());
            bytes[120..128].copy_from_slice(&payload.range.length.to_le_bytes());
            bytes[128..160].copy_from_slice(&payload.range.sha256);
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8; ROW_BYTES as usize]) -> io::Result<Self> {
        let flags = bytes[56];
        let live = flags & (1 << 5) != 0;
        if flags >= 64 || bytes[57..64] != [0; 7] || (!live && bytes[64..] != [0; 96]) {
            return Err(invalid("invalid record row flags"));
        }
        let optional = |i: usize| -> Option<[u8; 8]> {
            let offset = 16 + i * 8;
            (flags & (1 << i) != 0).then(|| {
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("fixed row width")
            })
        };
        let metadata = Metadata {
            version: read_u64(&bytes[..8]),
            ingested_at_ms: read_u64(&bytes[8..16]),
            temporal: TemporalMetadata {
                event_time_ms: optional(0).map(i64::from_le_bytes),
                valid_from_ms: optional(1).map(i64::from_le_bytes),
                valid_to_ms: optional(2).map(i64::from_le_bytes),
                expire_at_ms: optional(3).map(u64::from_le_bytes),
            },
            expired_at_ms: optional(4).map(u64::from_le_bytes),
            fingerprint: live.then(|| bytes[64..96].try_into().expect("fixed row width")),
        };
        metadata.validate()?;
        Ok(Self {
            metadata,
            payload: live.then(|| PayloadAddress {
                object: bytes[96..112].try_into().expect("fixed row width"),
                range: PayloadRef {
                    offset: read_u64(&bytes[112..120]),
                    length: read_u64(&bytes[120..128]),
                    sha256: bytes[128..160].try_into().expect("fixed row width"),
                },
            }),
        })
    }
}

fn filename(id: u64) -> String {
    format!("records-{id:016x}")
}

fn checksum(id: &str, address: Address, bytes: &[u8; ROW_BYTES as usize]) -> Digest {
    let mut hash = Sha256::new();
    hash.update(MAGIC);
    hash.update(address.segment_id.to_le_bytes());
    hash.update(address.doc_id.to_le_bytes());
    hash.update(bytes);
    hash.update(id.as_bytes());
    hash.finalize().into()
}

pub(crate) struct SegmentWriter {
    file: File,
    id: u64,
    count: u64,
    poisoned: bool,
}

impl SegmentWriter {
    pub(crate) fn create(dir: &Path, id: u64) -> io::Result<Self> {
        Ok(Self {
            file: create_file(&dir.join(filename(id)), MAGIC)?,
            id,
            count: 0,
            poisoned: false,
        })
    }

    pub(crate) fn push(&mut self, id: &str, row: &Row) -> io::Result<RowRef> {
        if self.poisoned {
            return Err(invalid("record segment requires discard"));
        }
        let row = row.encode()?;
        let address = Address {
            segment_id: self.id,
            doc_id: self.count,
        };
        let next = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("record row ordinal overflow"))?;
        extent(HEADER, next, ROW_BYTES)?;
        self.poisoned = true;
        self.file.write_all(&row)?;
        self.count = next;
        self.poisoned = false;
        Ok(RowRef {
            address,
            sha256: checksum(id, address, &row),
        })
    }

    pub(crate) fn finish(self) -> io::Result<u64> {
        if self.poisoned || self.file.metadata()?.len() != extent(HEADER, self.count, ROW_BYTES)? {
            return Err(invalid("record segment requires discard"));
        }
        self.file.sync_all()?;
        Ok(self.count)
    }
}

pub(super) fn open(dir: &Path, id: u64, rows: u64, read: ReadRange) -> io::Result<File> {
    let file = open_file(&dir.join(filename(id)), extent(HEADER, rows, ROW_BYTES)?)?;
    check_magic(&file, MAGIC, read)?;
    Ok(file)
}

pub(super) fn read(
    file: &File,
    count: u64,
    id: &str,
    reference: RowRef,
    read: ReadRange,
) -> io::Result<Row> {
    let doc = reference.address.doc_id;
    if doc >= count {
        return Err(invalid("record row ordinal outside selected segment"));
    }
    let mut row = [0; ROW_BYTES as usize];
    read(file, extent(HEADER, doc, ROW_BYTES)?, &mut row)?;
    if checksum(id, reference.address, &row) != reference.sha256 {
        return Err(invalid("record row checksum mismatch"));
    }
    Row::decode(&row)
}
