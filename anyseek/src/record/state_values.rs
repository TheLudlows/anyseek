//! Immutable retained values. Containers copy keys/pointers, never record bodies
//! or historical requests. Mutations construct admitted replacements explicitly.
use super::state_memory::Shared;
use super::*;
use crate::storage::build_budget::reserve_memory;

impl<T: Serialize> Shared<T> {
    pub(super) fn wire_bytes(&self) -> usize {
        (self.charged_bytes() / 32) as usize
    }
}
#[derive(Debug, Clone)]
pub(super) struct Values<T> {
    values: Vec<Shared<T>>,
    bytes: usize,
}
impl<T> Default for Values<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            bytes: 0,
        }
    }
}
impl<T: Serialize> Values<T> {
    pub(super) fn wire_length(&self)->Result<usize,RecordError> {
        self.bytes.checked_add(2).and_then(|n|n.checked_add(self.len().saturating_sub(1)))
            .ok_or_else(||storage("history wire length overflow"))
    }
    pub(super) fn same_values(&self,other:&Self)->Result<bool,RecordError> where T:state_memory::ProofValue {
        if self.len()!=other.len() || self.bytes!=other.bytes {return Ok(false);}
        for (a,b) in self.values.iter().zip(&other.values) {if !a.same_value(b)? {return Ok(false);}}
        Ok(true)
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
    pub(super) fn last(&self) -> Option<&T> {
        self.values.last().map(|v| &**v)
    }
    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + ExactSizeIterator {
        self.values.iter().map(|v| &**v)
    }
    pub(super) fn push(&mut self, value: T) -> Result<(), RecordError> {
        self.push_shared(Shared::admit(value)?)
    }
    pub(super) fn push_shared(&mut self, value: Shared<T>) -> Result<(), RecordError> {
        let bytes = self
            .bytes
            .checked_add(value.wire_bytes())
            .ok_or_else(|| storage("values size overflow"))?;
        self.values.push(value);
        self.bytes = bytes;
        Ok(())
    }
    pub(super) fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        let bytes = &mut self.bytes;
        self.values.retain(|value| {
            if keep(value) { true } else { *bytes -= value.wire_bytes(); false }
        });
    }
    #[cfg(test)]
    pub(super) fn edit(
        &mut self,
        index: usize,
        growth: usize,
        change: impl FnOnce(&mut T) -> Result<(), RecordError>,
    ) -> Result<(), RecordError>
    where
        T: Clone,
    {
        let old = &self.values[index];
        let mut lease = reserve_memory(
            (old.wire_bytes()
                .checked_add(growth)
                .ok_or_else(|| storage("value growth overflow"))? as u64)
                .checked_mul(32)
                .ok_or_else(|| storage("value growth overflow"))?,
        )
        .map_err(storage)?;
        let mut value = (**old).clone();
        change(&mut value)?;
        let next = Shared::from_admitted(value, &mut lease)?;
        let bytes = self
            .bytes
            .checked_sub(old.wire_bytes())
            .and_then(|n| n.checked_add(next.wire_bytes()))
            .ok_or_else(|| storage("value replacement overflow"))?;
        self.values[index] = next;
        self.bytes = bytes;
        Ok(())
    }
}
impl<T> std::ops::Index<usize> for Values<T> {
    type Output = T;
    fn index(&self, index: usize) -> &T {
        &self.values[index]
    }
}
impl<'a, T> IntoIterator for &'a Values<T> {
    type Item = &'a T;
    type IntoIter = std::iter::Map<std::slice::Iter<'a, Shared<T>>, fn(&'a Shared<T>) -> &'a T>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter().map(|v| &**v)
    }
}
impl<T: Serialize> Serialize for Values<T> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.values.serialize(s)
    }
}
impl<'de, T: Serialize + Deserialize<'de>> Deserialize<'de> for Values<T> {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = Vec::<T>::deserialize(d)?;
        let mut result = Self::default();
        for v in raw {
            result.push(v).map_err(serde::de::Error::custom)?;
        }
        Ok(result)
    }
}
// Derived admission statistics are never serialized or trusted from a checkpoint.
// Every replacement (including edit and recovery inserts) passes insert_shared.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct AdmissionCounts {
    ttl: usize,
    sources: usize,
    typed: usize,
}
impl AdmissionCounts {
    fn of(record: &Record) -> Self {
        Self { ttl: usize::from(temporal::live_ttl(record)),
            sources: usize::from(evidence::retained_revision(record)),
            typed: usize::from(record.system.is_some() && record.document.is_some()) }
    }
    fn replacing(self, old: Self, next: Self) -> Result<Self, RecordError> {
        let update = |value: usize, before: usize, after: usize| value.checked_sub(before)
            .and_then(|n| n.checked_add(after)).ok_or_else(|| storage("record admission count overflow"));
        Ok(Self { ttl: update(self.ttl,old.ttl,next.ttl)?,
            sources: update(self.sources,old.sources,next.sources)?,
            typed: update(self.typed,old.typed,next.typed)? })
    }
}
#[derive(Debug, Clone, Default)]
pub(super) struct Records {
    counts: AdmissionCounts,
    // Conservative lower bound, not authority. Removal/extension may leave it
    // early until the next candidate scan; insertion must never leave it late.
    ttl_deadline_lower_bound: Option<u64>,
    values: BTreeMap<String, Shared<Record>>,
    bytes: usize,
}
impl Records {
    pub(super) fn visit_due_ttl(&mut self, evaluation: u64, mut visit: impl FnMut(&Record)) -> bool {
        if self.ttl_deadline_lower_bound.is_none_or(|deadline| deadline > evaluation) { return false; }
        let mut earliest = None;
        for record in self.values.values() {
            if !temporal::live_ttl(record) { continue; }
            let deadline = record.temporal.expire_at_ms.expect("live TTL deadline");
            earliest = Some(earliest.map_or(deadline, |old: u64| old.min(deadline)));
            if deadline <= evaluation { visit(record); }
        }
        self.ttl_deadline_lower_bound = earliest;
        true
    }
    pub(super) fn live_ttl_count(&self) -> usize { self.counts.ttl }
    pub(super) fn retained_source_count(&self) -> usize { self.counts.sources }
    pub(super) fn live_typed_count(&self) -> usize { self.counts.typed }
    pub(super) fn validate_counts(&self) -> Result<(), RecordError> {
        let mut counts = AdmissionCounts::default();
        let mut earliest = None;
        for record in self.values() {
            if temporal::live_ttl(record) {
                let deadline = record.temporal.expire_at_ms.unwrap();
                earliest = Some(earliest.map_or(deadline, |old: u64| old.min(deadline)));
            }
            counts = counts.replacing(AdmissionCounts::default(), AdmissionCounts::of(record))?;
        }
        if earliest.is_some_and(|deadline| self.ttl_deadline_lower_bound.is_none_or(|bound| bound > deadline)) {
            return Err(storage("record TTL deadline lower bound invalid"));
        }
        if counts != self.counts { return Err(storage("record admission counts mismatch")); }
        Ok(())
    }
    pub(super) fn wire_length(&self)->Result<usize,RecordError> {
        let mut bytes=self.bytes.checked_add(2).and_then(|n|n.checked_add(self.len().saturating_sub(1)))
            .ok_or_else(||storage("record wire length overflow"))?;
        for key in self.values.keys() {
            bytes=bytes.checked_add(checkpoint_codec::json_length(key)?).and_then(|n|n.checked_add(1))
                .ok_or_else(||storage("record key length overflow"))?;
        }
        Ok(bytes)
    }
    pub(super) fn same_values(&self,other:&Self)->Result<bool,RecordError> {
        if self.len()!=other.len() || self.bytes!=other.bytes || self.counts!=other.counts {return Ok(false);}
        for ((ka,a),(kb,b)) in self.values.iter().zip(&other.values) {
            if ka!=kb || !a.same_value(b)? {return Ok(false);}
        }
        Ok(true)
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
    pub(super) fn get(&self, id: &str) -> Option<&Record> {
        self.values.get(id).map(|v| &**v)
    }
    pub(super) fn shared(&self, id: &str) -> Option<Shared<Record>> {
        self.values.get(id).cloned()
    }
    pub(super) fn contains_key(&self, id: &str) -> bool {
        self.values.contains_key(id)
    }
    pub(super) fn keys(&self) -> impl DoubleEndedIterator<Item = &String> + ExactSizeIterator {
        self.values.keys()
    }
    pub(super) fn values(&self) -> impl DoubleEndedIterator<Item = &Record> + ExactSizeIterator {
        self.values.values().map(|v| &**v)
    }
    pub(super) fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&String, &Record)> + ExactSizeIterator {
        self.values.iter().map(|(k, v)| (k, &**v))
    }
    pub(super) fn range<R: std::ops::RangeBounds<String>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = (&String, &Record)> {
        self.values.range(range).map(|(k, v)| (k, &**v))
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
        let old = self.values.get(&id).map_or(0, Shared::wire_bytes);
        let bytes = self
            .bytes
            .checked_sub(old)
            .and_then(|n| n.checked_add(value.wire_bytes()))
            .ok_or_else(|| storage("record replacement overflow"))?;
        let counts = self.counts.replacing(
            self.get(&id).map(AdmissionCounts::of).unwrap_or_default(), AdmissionCounts::of(&value))?;
        let next_deadline = temporal::live_ttl(&value).then_some(value.temporal.expire_at_ms).flatten();
        let deadline = match (self.ttl_deadline_lower_bound, next_deadline) {
            (Some(old), Some(next)) => Some(old.min(next)),
            (old, next) => old.or(next),
        };
        let previous = self.values.insert(id, value);
        self.bytes = bytes;
        self.counts = counts;
        self.ttl_deadline_lower_bound = deadline;
        Ok(previous)
    }
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
        let mut lease = reserve_memory(bytes).map_err(storage)?;
        let mut value = old.clone();
        change(&mut value)?;
        self.insert_shared(id.to_owned(), Shared::from_admitted(value, &mut lease)?)?;
        Ok(())
    }
}
impl std::ops::Index<&str> for Records {
    type Output = Record;
    fn index(&self, id: &str) -> &Record {
        self.get(id).expect("record identity")
    }
}
impl std::ops::Index<&String> for Records {
    type Output = Record;
    fn index(&self, id: &String) -> &Record {
        &self[id.as_str()]
    }
}
impl<'a> IntoIterator for &'a Records {
    type Item = (&'a String, &'a Record);
    type IntoIter = std::iter::Map<
        std::collections::btree_map::Iter<'a, String, Shared<Record>>,
        fn((&'a String, &'a Shared<Record>)) -> (&'a String, &'a Record),
    >;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter().map(|(k, v)| (k, &**v))
    }
}
impl Serialize for Records {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.values.serialize(s)
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
        assert_eq!(records.live_ttl_count(), records.values().filter(|r| temporal::live_ttl(r)).count());
        assert_eq!(records.retained_source_count(), records.values().filter(|r| evidence::retained_revision(r)).count());
        assert_eq!(records.live_typed_count(), records.values().filter(|r| r.system.is_some() && r.document.is_some()).count());
    }

    #[test]
    fn admission_counts_follow_replacement_edit_clone_and_legacy_wire() {
        let mut state = State::default();
        for i in 0..12 {
            let mut request = super::super::tests::write(&format!("counts-{i}"), &format!("id-{}", i % 4), "alpha", ExpectedVersion::Any);
            if i % 2 == 0 { request.system = Some(EvidenceMetadata { kind: EvidenceKind::Source, relation: None, evidence_refs: vec![] }); }
            if i < 4 { request.temporal = Some(TemporalMetadata { expire_at_ms: Some(1000), ..Default::default() }); }
            if i >= 8 { request.document = None; }
            let version = state.records.get(&request.record_id).map_or(1, |r| r.version + 1);
            let applied = Applied { receipt: WriteReceipt { seq: i + 1, version, record_id: request.record_id.clone() },
                request, observed_epoch: 1, observed_time_ms: 1 };
            apply(&mut state, &applied).unwrap();
            check(&state.records);
            let clone = state.records.clone(); check(&clone);
            let wire = serde_json::to_value(&clone).unwrap();
            assert_eq!(wire.as_object().unwrap().len(), clone.len());
            assert!(wire.get("counts").is_none());
            let recovered: Records = serde_json::from_value(wire).unwrap();
            check(&recovered); assert!(recovered.same_values(&clone).unwrap());
        }
        assert_eq!(state.records.live_ttl_count(), 0);
        state.records.edit("id-0", 0, |r| { r.document = None; Ok(()) }).unwrap();
        check(&state.records);
        let before = serde_json::to_value(&state.records).unwrap();
        assert!(state.records.edit("id-0", 0, |r| { r.document = None; Err(storage("injected edit failure")) }).is_err());
        check(&state.records); assert_eq!(before, serde_json::to_value(&state.records).unwrap());
    }

    #[test]
    fn ttl_deadline_bound_skips_scan_and_tracks_early_update_extension_delete_restore() {
        let mut state = State::default();
        let mut request = super::super::tests::write("deadline", "id", "alpha", ExpectedVersion::Any);
        request.temporal = Some(TemporalMetadata { expire_at_ms: Some(1000), ..Default::default() });
        let applied = Applied { receipt: WriteReceipt { seq: 1, version: 1, record_id: "id".into() },
            request, observed_epoch: 1, observed_time_ms: 1 };
        apply(&mut state, &applied).unwrap();
        let records = &mut state.records;
        assert!(!records.visit_due_ttl(999, |_| panic!("early callback")));
        // Same-version changes still lower the bound, independent of identity version.
        records.edit("id", 0, |r| { r.temporal.expire_at_ms = Some(100); Ok(()) }).unwrap();
        check(records);
        let mut due = Vec::new();
        assert!(records.visit_due_ttl(100, |r| due.push(r.record_id.clone())));
        assert_eq!(due, ["id"]);
        records.edit("id", 32, |r| { r.temporal.expire_at_ms = Some(2000); Ok(()) }).unwrap();
        assert_eq!(records.ttl_deadline_lower_bound, Some(100));
        assert!(records.visit_due_ttl(100, |_| panic!("extended record expired")));
        assert_eq!(records.ttl_deadline_lower_bound, Some(2000));
        assert!(!records.visit_due_ttl(1999, |_| panic!("unexpected scan")));
        let mut jumped = 0;
        assert!(records.visit_due_ttl(10000, |_| jumped += 1));
        assert_eq!(jumped, 1);
        let before = records.ttl_deadline_lower_bound;
        assert!(records.edit("id", 0, |r| { r.temporal.expire_at_ms = Some(1); Err(storage("rollback")) }).is_err());
        assert_eq!(before, records.ttl_deadline_lower_bound);
        records.edit("id", 0, |r| { r.document = None; Ok(()) }).unwrap();
        check(records);
        let restored: Records = serde_json::from_slice(&serde_json::to_vec(records).unwrap()).unwrap();
        assert_eq!(restored.ttl_deadline_lower_bound, None);
        assert!(records.same_values(&restored).unwrap());
        assert!(records.visit_due_ttl(2000, |_| panic!("deleted record expired")));
        assert!(!records.visit_due_ttl(u64::MAX, |_| panic!("empty scan")));
        let mut restored = Records::default();
        // Invalid late/absent watermarks are detected by the recovery oracle.
        restored.insert("id".into(), mutation::projected_record(&State::default(), &applied,
            applied.request.document.clone(), applied.request.temporal.clone().unwrap())).unwrap();
        restored.ttl_deadline_lower_bound = Some(1001);
        assert!(restored.validate_counts().is_err());
        restored.ttl_deadline_lower_bound = None;
        assert!(restored.validate_counts().is_err());
        let mut rebuilt: Records = serde_json::from_slice(&serde_json::to_vec(&restored).unwrap()).unwrap();
        check(&rebuilt);
        let mut n = 0; assert!(rebuilt.visit_due_ttl(1000, |_| n += 1)); assert_eq!(n, 1);
    }

    #[test]
    fn restored_admission_counts_reject_corrupt_derived_cache() {
        let mut records = Records::default();
        records.counts.ttl = 1;
        assert!(records.validate_counts().is_err());
        // Wire input carries only records; untrusted cached statistics cannot survive reload.
        let restored: Records = serde_json::from_slice(&serde_json::to_vec(&records).unwrap()).unwrap();
        check(&restored);
    }
}
