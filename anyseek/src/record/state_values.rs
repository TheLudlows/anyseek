//! Immutable retained values. Containers copy keys/pointers, never record bodies
//! or historical requests. Mutations construct admitted replacements explicitly.
use super::state_memory::Shared;
use super::*;
use crate::storage::build_budget::reserve_memory;

mod ttl;
pub(super) use ttl::PreparedTtl;

impl<T: Serialize> Shared<T> {
    pub(super) fn wire_bytes(&self) -> usize {
        (self.charged_bytes() / 32) as usize
    }
}
/// Checkpointed operation payloads already protected by the selected manifest.
/// Keep their catalog and GC pin once per root.
pub(super) struct OperationPool {
    pub reader: crate::storage::payload_pool::PoolReader,
    _pin: Arc<package_pins::Pin>,
    _metadata: crate::storage::build_budget::MemoryLease,
}
impl OperationPool {
    pub(super) fn new(
        reader: crate::storage::payload_pool::PoolReader,
    ) -> Result<Self, RecordError> {
        let metadata =
            reserve_memory(reader.metadata_bytes().map_err(storage)?).map_err(memory_error)?;
        let paths = reader
            .object_paths()
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        let pin = package_pins::Pin::acquire(&paths)?;
        Ok(Self {
            reader,
            _pin: pin,
            _metadata: metadata,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) enum StoredOperation {
    Recent(Shared<Applied>),
    Sealed(Arc<SealedOperation>),
}

pub(super) struct SealedOperation {
    metadata: Applied,
    pool: Arc<OperationPool>,
    document: Option<crate::storage::payload_pool::Reference>,
    patch: Option<crate::storage::payload_pool::Reference>,
    bytes: usize,
    _lease: crate::storage::build_budget::MemoryLease,
}
impl std::fmt::Debug for SealedOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedOperation")
            .field("receipt", &self.metadata.receipt)
            .finish_non_exhaustive()
    }
}

impl StoredOperation {
    fn metadata(&self) -> &Applied {
        match self {
            Self::Recent(op) => op,
            Self::Sealed(op) => &op.metadata,
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::Recent(op) => op.wire_bytes(),
            Self::Sealed(op) => op.bytes,
        }
    }
    fn load(&self) -> Result<Shared<Applied>, RecordError> {
        match self {
            Self::Recent(op) => Ok(op.clone()),
            Self::Sealed(op) => {
                let size = (op.bytes as u64).checked_mul(32).ok_or(RecordError::Busy)?;
                let mut decode = reserve_memory(size).map_err(memory_error)?;
                let mut applied = op.metadata.clone();
                if let Some(reference) = &op.document {
                    let bytes = op
                        .pool
                        .reader
                        .read(reference)
                        .map_err(state_rows::read_error)?;
                    applied.request.document =
                        Some(snapshot_json::from_slice(&bytes).map_err(storage)?);
                }
                if let Some(reference) = &op.patch {
                    let bytes = op
                        .pool
                        .reader
                        .read(reference)
                        .map_err(state_rows::read_error)?;
                    applied.request.vector_patch =
                        Some(snapshot_json::from_slice(&bytes).map_err(storage)?);
                }
                if checkpoint_codec::json_length(&applied)? != op.bytes {
                    return Err(storage("sealed receipt payload size mismatch"));
                }
                Shared::from_admitted(applied, &mut decode)
            }
        }
    }
    fn same_value(&self, other: &Self) -> Result<bool, RecordError> {
        if let (Self::Sealed(a), Self::Sealed(b)) = (self, other) {
            if Arc::ptr_eq(a, b) {
                return Ok(true);
            }
        }
        self.load()?.same_value(&other.load()?)
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct Operations {
    values: Vec<StoredOperation>,
    bytes: usize,
}

impl Operations {
    pub(super) fn wire_length(&self) -> Result<usize, RecordError> {
        self.bytes
            .checked_add(2)
            .and_then(|n| n.checked_add(self.len().saturating_sub(1)))
            .ok_or_else(|| storage("history wire length overflow"))
    }
    pub(super) fn same_values(&self, other: &Self) -> Result<bool, RecordError> {
        if self.len() != other.len() || self.bytes != other.bytes {
            return Ok(false);
        }
        for (a, b) in self.values.iter().zip(&other.values) {
            if !a.same_value(b)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
    pub(super) fn load(&self, index: usize) -> Result<Shared<Applied>, RecordError> {
        self.values
            .get(index)
            .ok_or_else(|| storage("operation receipt outside history"))?
            .load()
    }
    pub(super) fn len(&self) -> usize {
        self.values.len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
    #[cfg(test)]
    pub(super) fn recent_count(&self) -> usize {
        self.values
            .iter()
            .filter(|value| matches!(value, StoredOperation::Recent(_)))
            .count()
    }

    #[cfg(test)]
    pub(super) fn last(&self) -> Option<&Applied> {
        self.values.last().map(StoredOperation::metadata)
    }
    /// Metadata only. Payload consumers must call `load`.
    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = &Applied> + ExactSizeIterator {
        self.values.iter().map(StoredOperation::metadata)
    }
    pub(super) fn push(&mut self, value: Applied) -> Result<(), RecordError> {
        self.push_shared(Shared::admit(value)?)
    }
    pub(super) fn push_shared(&mut self, value: Shared<Applied>) -> Result<(), RecordError> {
        self.append(StoredOperation::Recent(value))
    }
    pub(super) fn push_sealed(
        &mut self,
        mut operation: snapshot_json::Value,
        pool: Arc<OperationPool>,
        bytes: usize,
    ) -> Result<(), RecordError> {
        let request = operation
            .get_mut("request")
            .ok_or_else(|| storage("missing sealed operation request"))?;
        fn take(
            request: &mut snapshot_json::Value,
            key: &str,
        ) -> Result<Option<crate::storage::payload_pool::Reference>, RecordError> {
            let Some(value) = request.get_mut(key).filter(|v| !v.is_null()) else {
                return Ok(None);
            };
            let reference = value
                .get_mut("$payload")
                .ok_or_else(|| storage("missing sealed operation payload reference"))?
                .take();
            *value = snapshot_json::Value::Null;
            Ok(Some(snapshot_json::from_value(reference).map_err(storage)?))
        }
        let document = take(request, "document")?;
        let patch = take(request, "vector_patch")?;
        let metadata: Applied = snapshot_json::from_value(operation).map_err(storage)?;
        let metadata_bytes = checkpoint_codec::json_length(&metadata)?
            .checked_add(512)
            .ok_or(RecordError::Busy)?;
        let lease = reserve_memory(
            (metadata_bytes as u64)
                .checked_mul(32)
                .ok_or(RecordError::Busy)?,
        )
        .map_err(memory_error)?;
        self.append(StoredOperation::Sealed(Arc::new(SealedOperation {
            metadata,
            pool,
            document,
            patch,
            bytes,
            _lease: lease,
        })))
    }
    fn append(&mut self, value: StoredOperation) -> Result<(), RecordError> {
        let bytes = self
            .bytes
            .checked_add(value.bytes())
            .ok_or_else(|| storage("history size overflow"))?;
        self.values.push(value);
        self.bytes = bytes;
        Ok(())
    }
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&Applied) -> bool) {
        let bytes = &mut self.bytes;
        self.values.retain(|value| {
            if keep(value.metadata()) {
                true
            } else {
                *bytes -= value.bytes();
                false
            }
        });
    }
    #[cfg(test)]
    pub(super) fn edit(
        &mut self,
        index: usize,
        _growth: usize,
        change: impl FnOnce(&mut Applied) -> Result<(), RecordError>,
    ) -> Result<(), RecordError> {
        let mut next = (*self.load(index)?).clone();
        change(&mut next)?;
        let next = StoredOperation::Recent(Shared::admit(next)?);
        self.bytes = self
            .bytes
            .checked_sub(self.values[index].bytes())
            .and_then(|n| n.checked_add(next.bytes()))
            .ok_or_else(|| storage("history replacement overflow"))?;
        self.values[index] = next;
        Ok(())
    }
}
impl std::ops::Index<usize> for Operations {
    type Output = Applied;
    fn index(&self, index: usize) -> &Applied {
        self.values[index].metadata()
    }
}
impl<'a> IntoIterator for &'a Operations {
    type Item = &'a Applied;
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, StoredOperation>,
        fn(&'a StoredOperation) -> &'a Applied,
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter().map(StoredOperation::metadata)
    }
}
impl Serialize for Operations {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(self.len()))?;
        for value in &self.values {
            match value {
                StoredOperation::Recent(value) => seq.serialize_element(value)?,
                StoredOperation::Sealed(_) => {
                    seq.serialize_element(&value.load().map_err(serde::ser::Error::custom)?)?
                }
            }
        }
        seq.end()
    }
}
impl<'de> Deserialize<'de> for Operations {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut result = Self::default();
        for value in Vec::<Applied>::deserialize(d)? {
            result.push(value).map_err(serde::de::Error::custom)?;
        }
        Ok(result)
    }
}
/// The previous row's contribution to logical counters. Publication can apply
/// a prepared replacement without reading (or decoding) the old document.
#[derive(Clone, Copy)]
pub(super) struct PreviousRow {
    pub version: u64,
    pub bytes: usize,
    deadline: Option<u64>,
}
impl PreviousRow {
    pub(super) fn has_ttl(self) -> bool {
        self.deadline.is_some()
    }
    pub(super) fn measure(record: &Record) -> Result<Self, RecordError> {
        Ok(Self::from_size(
            record,
            checkpoint_codec::json_length(record)?,
        ))
    }
    pub(super) fn from_size(record: &Record, bytes: usize) -> Self {
        Self {
            version: record.version,
            bytes,
            deadline: ttl::live_deadline(record),
        }
    }
}
#[derive(Debug, Default)]
pub(super) struct Records {
    deadlines: ttl::Deadlines,
    base: Option<Arc<state_rows::Base>>,
    values: BTreeMap<String, Shared<Record>>,
    bytes: usize,
    keys_bytes: usize,
    sealed_keys_bytes: usize,
    count: usize,
}

pub(super) enum CurrentRow<'a> {
    Recent(&'a Shared<Record>),
    Sealed(crate::storage::disk_records::Located<'a>),
}

/// An owned read of one key from the published directory or recent suffix.
/// Capturing and comparing the source never touches the filesystem.
pub(super) enum RecordSource {
    Recent(Shared<Record>),
    Sealed(Arc<state_rows::Base>),
}
impl RecordSource {
    pub(super) fn find(&self, id: &str) -> Result<Option<CurrentRow<'_>>, RecordError> {
        match self {
            Self::Recent(row) => Ok(Some(CurrentRow::Recent(row))),
            Self::Sealed(base) => base
                .checkpoint
                .find_sealed(id)
                .map(|row| row.map(CurrentRow::Sealed))
                .map_err(state_rows::read_error),
        }
    }
    pub(super) fn still_selected(&self, records: &Records, id: &str) -> bool {
        match self {
            Self::Recent(row) => records
                .values
                .get(id)
                .is_some_and(|selected| row.ptr_eq(selected)),
            Self::Sealed(base) => {
                !records.values.contains_key(id)
                    && records
                        .base
                        .as_ref()
                        .is_some_and(|selected| Arc::ptr_eq(base, selected))
            }
        }
    }
}
impl CurrentRow<'_> {
    pub(super) fn version(&self) -> u64 {
        match self {
            Self::Recent(row) => row.version,
            Self::Sealed(row) => row.metadata.version,
        }
    }
    pub(super) fn live(&self) -> bool {
        match self {
            Self::Recent(row) => row.document.is_some(),
            Self::Sealed(row) => row.metadata.is_live(),
        }
    }
    fn deadline(&self) -> Option<u64> {
        if self.live() {
            self.temporal().expire_at_ms
        } else {
            None
        }
    }
    pub(super) fn temporal(&self) -> &TemporalMetadata {
        match self {
            Self::Recent(row) => &row.temporal,
            Self::Sealed(row) => &row.metadata.temporal,
        }
    }
    pub(super) fn ingested_at_ms(&self) -> u64 {
        match self {
            Self::Recent(row) => row.ingested_at_ms,
            Self::Sealed(row) => row.metadata.ingested_at_ms,
        }
    }
    pub(super) fn expired_at_ms(&self) -> Option<u64> {
        match self {
            Self::Recent(row) => row.expired_at_ms,
            Self::Sealed(row) => row.metadata.expired_at_ms,
        }
    }
    pub(super) fn visible(&self, time: u64) -> bool {
        self.live() && self.temporal().expire_at_ms.is_none_or(|end| time < end)
    }
    pub(super) fn matches_candidate(&self, version: u64, fingerprint: [u8; 32]) -> bool {
        self.version() == version
            && match self {
                Self::Recent(_) => true,
                Self::Sealed(row) => row.metadata.fingerprint == Some(fingerprint),
            }
    }
    pub(super) fn account(&self, id: &str) -> Result<PreviousRow, RecordError> {
        let bytes = match self {
            Self::Recent(row) => row.wire_bytes(),
            Self::Sealed(row) => {
                let shell = checkpoint_codec::json_length(&state_rows::output(id, row, None))?;
                if let Some(payload) = row.sealed_payload() {
                    let payload_bytes = usize::try_from(payload.range.length).map_err(storage)?;
                    shell
                        .checked_sub(4)
                        .and_then(|n| n.checked_add(payload_bytes))
                        .ok_or_else(|| storage("sealed record wire size overflow"))?
                } else {
                    shell
                }
            }
        };
        Ok(PreviousRow {
            version: self.version(),
            bytes,
            deadline: self.deadline(),
        })
    }
    pub(super) fn load(self, id: &str) -> Result<Shared<Record>, RecordError> {
        match self {
            Self::Recent(row) => Ok(row.clone()),
            Self::Sealed(row) => state_rows::decode(id, row, Shared::admit),
        }
    }
}

#[cfg(test)]
impl Clone for Records {
    fn clone(&self) -> Self {
        self.try_clone().expect("reserve test Record state clone")
    }
}

impl Records {
    pub(super) fn try_clone(&self) -> Result<Self, RecordError> {
        let deadlines = self.deadlines.try_clone()?;
        Ok(Self {
            deadlines,
            base: self.base.clone(),
            values: self.values.clone(),
            bytes: self.bytes,
            keys_bytes: self.keys_bytes,
            sealed_keys_bytes: self.sealed_keys_bytes,
            count: self.count,
        })
    }

    pub(super) fn has_sealed_base(&self) -> bool {
        self.base.is_some()
    }
    #[cfg(test)]
    pub(super) fn recent_count(&self) -> usize {
        self.values.len()
    }
    pub(super) fn from_base(base: state_rows::Base) -> Result<Self, RecordError> {
        let mut result = Self::default();
        base.visit(|id, row| {
            let row = CurrentRow::Sealed(row);
            let account = row.account(id)?;
            result.bytes = result
                .bytes
                .checked_add(account.bytes)
                .ok_or(RecordError::Busy)?;
            result.keys_bytes = result
                .keys_bytes
                .checked_add(Self::key_bytes(id)?)
                .ok_or(RecordError::Busy)?;
            result.count = result.count.checked_add(1).ok_or(RecordError::Busy)?;
            if let Some(at) = account.deadline {
                result
                    .deadlines
                    .insert(PreparedTtl::new(at, id, account.version)?);
            }
            Ok(())
        })?;
        result.base = Some(Arc::new(base));
        result.sealed_keys_bytes = result.keys_bytes;
        Ok(result)
    }

    fn key_bytes(id: &str) -> Result<usize, RecordError> {
        checkpoint_codec::json_length(&id)?
            .checked_add(1)
            .ok_or(RecordError::Busy)
    }
    pub(super) fn find(&self, id: &str) -> Result<Option<CurrentRow<'_>>, RecordError> {
        if let Some(record) = self.values.get(id) {
            return Ok(Some(CurrentRow::Recent(record)));
        }
        match &self.base {
            Some(base) => Ok(base
                .checkpoint
                .find_sealed(id)
                .map_err(state_rows::read_error)?
                .map(CurrentRow::Sealed)),
            None => Ok(None),
        }
    }
    pub(super) fn load(&self, id: &str) -> Result<Option<Shared<Record>>, RecordError> {
        self.find(id)?.map(|record| record.load(id)).transpose()
    }
    pub(super) fn sealed_source(&self, id: &str) -> Option<Arc<state_rows::Base>> {
        self.base
            .as_ref()
            .filter(|_| !self.values.contains_key(id))
            .cloned()
    }
    pub(super) fn source(&self, id: &str) -> Option<RecordSource> {
        match self.values.get(id) {
            Some(row) => Some(RecordSource::Recent(row.clone())),
            None => self.base.clone().map(RecordSource::Sealed),
        }
    }

    /// Visit the effective ordered table one row at a time while sharing its
    /// base. A tombstone in the journal suffix supersedes the same sealed key.
    pub(super) fn scan(
        &self,
        after: Option<&str>,
        mut visit: impl FnMut(&str, CurrentRow<'_>) -> Result<bool, RecordError>,
    ) -> Result<(), RecordError> {
        use std::ops::Bound::{Excluded, Unbounded};
        let mut recent = self
            .values
            .range::<str, _>((after.map_or(Unbounded, Excluded), Unbounded))
            .peekable();
        if let Some(base) = &self.base {
            let mut sealed = base
                .checkpoint
                .sealed_cursor(after)
                .map_err(state_rows::read_error)?;
            while let Some((id, row)) = sealed.next().map_err(state_rows::read_error)? {
                let key = id.as_str();
                while recent.peek().is_some_and(|(id, _)| id.as_str() < key) {
                    let (id, row) = recent.next().unwrap();
                    if !visit(id, CurrentRow::Recent(row))? {
                        return Ok(());
                    }
                }
                if recent.peek().is_some_and(|(id, _)| id.as_str() == key) {
                    let (_, row) = recent.next().unwrap();
                    if !visit(key, CurrentRow::Recent(row))? {
                        return Ok(());
                    }
                } else if !visit(key, CurrentRow::Sealed(row))? {
                    return Ok(());
                }
            }
        }
        for (id, row) in recent {
            if !visit(id, CurrentRow::Recent(row))? {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn may_have_due_ttl(&self, evaluation: u64) -> bool {
        self.deadlines.has_due(evaluation)
    }

    pub(super) fn visit_due_ttl(
        &self,
        evaluation: u64,
        mut visit: impl FnMut(&str, u64) -> Result<(), RecordError>,
    ) -> Result<(), RecordError> {
        for (at, id, version) in self.deadlines.due(evaluation) {
            // Verify the effective row, including sealed lookups and recent
            // overrides, before using a derived entry for statistics or WAL.
            let row = self
                .find(id)?
                .ok_or_else(|| storage("TTL identity missing"))?;
            if row.version() != version || row.deadline() != Some(at) {
                return Err(storage("stale Record TTL entry"));
            }
            visit(id, version)?;
        }
        Ok(())
    }

    pub(super) fn live_ttl_count(&self) -> usize {
        self.deadlines.len()
    }

    pub(super) fn validate_counts(&self) -> Result<(), RecordError> {
        let mut count = 0;
        self.scan(None, |id, row| {
            if let Some(at) = row.deadline() {
                if !self.deadlines.matches(&(at, id.to_owned()), row.version()) {
                    return Err(storage("Record TTL entry missing or stale"));
                }
                count += 1;
            }
            Ok(true)
        })?;
        if count != self.deadlines.len() {
            return Err(storage("record admission counts mismatch"));
        }
        Ok(())
    }

    pub(super) fn wire_length(&self) -> Result<usize, RecordError> {
        self.bytes
            .checked_add(self.keys_bytes)
            .and_then(|n| n.checked_add(2))
            .and_then(|n| n.checked_add(self.len().saturating_sub(1)))
            .ok_or_else(|| storage("record wire length overflow"))
    }
    pub(super) fn sealed_keys_bytes(&self) -> usize {
        self.sealed_keys_bytes
    }
    pub(super) fn same_values(&self, other: &Self) -> Result<bool, RecordError> {
        if self.len() != other.len()
            || self.bytes != other.bytes
            || self.live_ttl_count() != other.live_ttl_count()
        {
            return Ok(false);
        }
        let same_base = match (&self.base, &other.base) {
            (None, None) => true,
            (Some(base), Some(other_base)) => Arc::ptr_eq(base, other_base),
            _ => false,
        };
        if same_base && self.values.keys().eq(other.values.keys()) {
            for ((ka, a), (kb, b)) in self.values.iter().zip(&other.values) {
                if ka != kb || !a.same_value(b)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        let mut equal = true;
        self.scan(None, |id, row| {
            equal = match other.load(id)? {
                Some(other) => row.load(id)?.same_value(&other)?,
                None => false,
            };
            Ok(equal)
        })?;
        Ok(equal)
    }
    pub(super) fn len(&self) -> usize {
        self.count
    }
    pub(super) fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }
    #[cfg(test)]
    pub(super) fn get(&self, id: &str) -> Option<&Record> {
        self.values.get(id).map(|v| &**v)
    }
    #[cfg(test)]
    pub(super) fn values(&self) -> impl DoubleEndedIterator<Item = &Record> + ExactSizeIterator {
        self.values.values().map(|v| &**v)
    }
    pub(super) fn insert(
        &mut self,
        id: String,
        value: Record,
    ) -> Result<Option<Shared<Record>>, RecordError> {
        self.insert_shared(id, Shared::admit(value)?)
    }
    pub(super) fn insert_shared(
        &mut self,
        id: String,
        value: Shared<Record>,
    ) -> Result<Option<Shared<Record>>, RecordError> {
        let previous = self.find(&id)?.map(|row| row.account(&id)).transpose()?;
        let ttl = PreparedTtl::for_record(&value)?;
        self.replace_prepared(id, value, previous, ttl)
    }
    pub(super) fn replace_prepared(
        &mut self,
        id: String,
        value: Shared<Record>,
        previous: Option<PreviousRow>,
        prepared_ttl: Option<PreparedTtl>,
    ) -> Result<Option<Shared<Record>>, RecordError> {
        if id != value.record_id {
            return Err(storage("record key differs from its identity"));
        }
        let bytes = self
            .bytes
            .checked_sub(previous.map_or(0, |row| row.bytes))
            .and_then(|n| n.checked_add(value.wire_bytes()))
            .ok_or_else(|| storage("record replacement overflow"))?;
        if prepared_ttl.as_ref().map(PreparedTtl::metadata)
            != ttl::live_deadline(&value).map(|at| (at, id.as_str(), value.version))
        {
            return Err(storage("Record TTL differs from its prepared entry"));
        }
        let id = if let Some((at, version)) =
            previous.and_then(|row| row.deadline.map(|at| (at, row.version)))
        {
            let key = (at, id);
            if !self.deadlines.matches(&key, version) {
                return Err(storage("previous Record TTL differs from its entry"));
            }
            self.deadlines.remove(&key);
            key.1
        } else {
            id
        };
        if previous.is_none() {
            self.keys_bytes = self
                .keys_bytes
                .checked_add(Self::key_bytes(&id)?)
                .ok_or(RecordError::Busy)?;
            self.count = self.count.checked_add(1).ok_or(RecordError::Busy)?;
        }
        let previous = self.values.insert(id, value);
        self.bytes = bytes;
        if let Some(prepared) = prepared_ttl {
            self.deadlines.insert(prepared);
        }
        Ok(previous)
    }
    #[cfg(test)]
    pub(super) fn edit(
        &mut self,
        id: &str,
        growth: usize,
        change: impl FnOnce(&mut Record) -> Result<(), RecordError>,
    ) -> Result<(), RecordError> {
        let old = self
            .get(id)
            .ok_or_else(|| storage("missing editable record"))?;
        let bytes = checkpoint_codec::json_length(old)?
            .checked_add(growth)
            .and_then(|n| (n as u64).checked_mul(32))
            .ok_or_else(|| storage("record copy growth overflow"))?;
        let mut lease = reserve_memory(bytes).map_err(memory_error)?;
        let mut value = old.clone();
        change(&mut value)?;
        self.insert_shared(id.to_owned(), Shared::from_admitted(value, &mut lease)?)?;
        Ok(())
    }
}
impl Serialize for Records {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        if self.base.is_none() {
            return self.values.serialize(s);
        }
        let mut map = s.serialize_map(Some(self.len()))?;
        self.scan(None, |id, row| {
            map.serialize_entry(id, &row.load(id)?).map_err(storage)?;
            Ok(true)
        })
        .map_err(serde::ser::Error::custom)?;
        map.end()
    }
}
impl<'de> Deserialize<'de> for Records {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = BTreeMap::<String, Record>::deserialize(d)?;
        let mut result = Self::default();
        for (k, v) in raw {
            result.insert(k, v).map_err(serde::de::Error::custom)?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod admission_count_tests {
    use super::*;

    fn check(records: &Records) {
        records.validate_counts().unwrap();
        assert_eq!(
            records.live_ttl_count(),
            records.values().filter(|r| temporal::live_ttl(r)).count()
        );
    }

    #[test]
    fn admission_counts_follow_replacement_edit_clone_and_legacy_wire() {
        let mut state = State::default();
        for i in 0..12 {
            let mut request = super::super::tests::write(
                &format!("counts-{i}"),
                &format!("id-{}", i % 4),
                "alpha",
                ExpectedVersion::Any,
            );
            if i < 4 {
                request.temporal = Some(TemporalMetadata {
                    expire_at_ms: Some(1000),
                    ..Default::default()
                });
            }
            if i >= 8 {
                request.document = None;
            }
            let version = state
                .records
                .get(&request.record_id)
                .map_or(1, |r| r.version + 1);
            let applied = Applied {
                receipt: WriteReceipt {
                    seq: i + 1,
                    version,
                    record_id: request.record_id.clone(),
                },
                request,
                observed_epoch: 1,
                observed_time_ms: 1,
            };
            apply(&mut state, &applied).unwrap();
            check(&state.records);
            let clone = state.records.clone();
            check(&clone);
            let wire = serde_json::to_value(&clone).unwrap();
            assert_eq!(wire.as_object().unwrap().len(), clone.len());
            assert!(wire.get("counts").is_none());
            let recovered: Records = serde_json::from_value(wire).unwrap();
            check(&recovered);
            assert!(recovered.same_values(&clone).unwrap());
        }
        assert_eq!(state.records.live_ttl_count(), 0);
        state
            .records
            .edit("id-0", 0, |r| {
                r.document = None;
                Ok(())
            })
            .unwrap();
        check(&state.records);
        let before = serde_json::to_value(&state.records).unwrap();
        assert!(
            state
                .records
                .edit("id-0", 0, |r| {
                    r.document = None;
                    Err(storage("injected edit failure"))
                })
                .is_err()
        );
        check(&state.records);
        assert_eq!(before, serde_json::to_value(&state.records).unwrap());
    }

    #[test]
    fn ttl_index_tracks_same_version_updates_extension_delete_and_restore() {
        let mut request =
            super::super::tests::write("deadline", "id", "alpha", ExpectedVersion::Any);
        request.temporal = Some(TemporalMetadata {
            expire_at_ms: Some(1000),
            ..Default::default()
        });
        let applied = Applied {
            receipt: WriteReceipt {
                seq: 1,
                version: 1,
                record_id: "id".into(),
            },
            request,
            observed_epoch: 1,
            observed_time_ms: 1,
        };
        let mut state = State::default();
        apply(&mut state, &applied).unwrap();
        let records = &mut state.records;
        assert!(!records.may_have_due_ttl(999));

        // Deadline replacement is needed even when the technical version stays the same.
        records
            .edit("id", 0, |row| {
                row.temporal.expire_at_ms = Some(100);
                Ok(())
            })
            .unwrap();
        check(records);
        let mut due = Vec::new();
        records
            .visit_due_ttl(100, |id, version| {
                due.push((id.to_owned(), version));
                Ok(())
            })
            .unwrap();
        assert_eq!(due, [("id".to_owned(), 1)]);

        records
            .edit("id", 32, |row| {
                row.temporal.expire_at_ms = Some(2000);
                Ok(())
            })
            .unwrap();
        assert!(!records.may_have_due_ttl(1999));
        let mut rebuilt: Records =
            serde_json::from_slice(&serde_json::to_vec(records).unwrap()).unwrap();
        assert!(records.same_values(&rebuilt).unwrap());
        check(&rebuilt);
        let mut n = 0;
        rebuilt
            .visit_due_ttl(2000, |_, _| {
                n += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(n, 1);

        assert!(
            records
                .edit("id", 0, |row| {
                    row.temporal.expire_at_ms = Some(1);
                    Err(storage("rollback"))
                })
                .is_err()
        );
        assert!(!records.may_have_due_ttl(1999));
        assert!(records.may_have_due_ttl(2000));
        records
            .edit("id", 0, |row| {
                row.document = None;
                Ok(())
            })
            .unwrap();
        check(records);
        assert!(!records.may_have_due_ttl(u64::MAX));

        let key = (2000, "id".to_owned());
        rebuilt.deadlines.remove(&key);
        assert!(rebuilt.validate_counts().is_err());
        rebuilt
            .deadlines
            .insert(PreparedTtl::new(key.0, &key.1, 2).unwrap());
        assert!(rebuilt.validate_counts().is_err());
        assert!(matches!(
            rebuilt.visit_due_ttl(2000, |_, _| panic!("stale callback")),
            Err(RecordError::Storage(_))
        ));
    }

    #[test]
    fn restored_admission_counts_reject_corrupt_derived_cache() {
        let mut records = Records::default();
        records
            .deadlines
            .insert(PreparedTtl::new(100, "missing", 1).unwrap());
        assert!(records.validate_counts().is_err());
        // Wire input carries only records; untrusted cached statistics cannot survive reload.
        let restored: Records =
            serde_json::from_slice(&serde_json::to_vec(&records).unwrap()).unwrap();
        check(&restored);
    }
}
