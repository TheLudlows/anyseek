//! Current deadlines derived from Record metadata. The index is never serialized.
use super::*;
use crate::storage::build_budget::{MemoryLease, reserve_memory};

type Key = (u64, String);

struct Entry {
    version: u64,
    lease: MemoryLease,
}

pub(in crate::record) struct PreparedTtl {
    key: Key,
    entry: Entry,
}

fn bytes(id: &str) -> u64 {
    // Allow for the key allocation and the B-tree's node capacity.
    1024 + id.len() as u64
}

pub(super) fn live_deadline(record: &Record) -> Option<u64> {
    record.document.as_ref()?;
    record.temporal.expire_at_ms
}

impl PreparedTtl {
    pub(super) fn metadata(&self) -> (u64, &str, u64) {
        (self.key.0, &self.key.1, self.entry.version)
    }

    pub(in crate::record) fn for_record(record: &Record) -> Result<Option<Self>, RecordError> {
        live_deadline(record)
            .map(|at| Self::new(at, &record.record_id, record.version))
            .transpose()
    }

    pub(super) fn new(at: u64, id: &str, version: u64) -> Result<Self, RecordError> {
        let lease = reserve_memory(bytes(id)).map_err(memory_error)?;
        Ok(Self {
            key: (at, id.to_owned()),
            entry: Entry { version, lease },
        })
    }
}

#[derive(Default)]
pub(super) struct Deadlines(BTreeMap<Key, Entry>);

impl std::fmt::Debug for Deadlines {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Deadlines")
            .field("len", &self.len())
            .finish()
    }
}

impl Deadlines {
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }

    pub(super) fn has_due(&self, time: u64) -> bool {
        self.0
            .first_key_value()
            .is_some_and(|((at, _), _)| *at <= time)
    }

    pub(super) fn insert(&mut self, prepared: PreparedTtl) {
        let previous = self.0.insert(prepared.key, prepared.entry);
        debug_assert!(previous.is_none());
    }

    pub(super) fn matches(&self, key: &Key, version: u64) -> bool {
        self.0
            .get(key)
            .is_some_and(|entry| entry.version == version)
    }

    pub(super) fn remove(&mut self, key: &Key) {
        let removed = self.0.remove(key);
        debug_assert!(removed.is_some());
        if self.0.is_empty() {
            self.0.clear();
        }
    }

    pub(super) fn due(&self, time: u64) -> impl Iterator<Item = (u64, &str, u64)> {
        self.0
            .iter()
            .take_while(move |((at, _), _)| *at <= time)
            .map(|((at, id), entry)| (*at, id.as_str(), entry.version))
    }

    pub(super) fn try_clone(&self) -> Result<Self, RecordError> {
        if self.0.is_empty() {
            return Ok(Self::default());
        }
        let total = self.0.values().try_fold(0u64, |total, entry| {
            total
                .checked_add(entry.lease.bytes())
                .ok_or(RecordError::Busy)
        })?;
        let mut lease = reserve_memory(total).map_err(memory_error)?;
        self.0
            .iter()
            .map(|(key, entry)| {
                let lease = lease.split_off(entry.lease.bytes()).map_err(storage)?;
                Ok((
                    key.clone(),
                    Entry {
                        version: entry.version,
                        lease,
                    },
                ))
            })
            .collect::<Result<_, _>>()
            .map(Self)
    }
}
