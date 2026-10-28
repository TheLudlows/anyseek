//! Immutable retained values. Containers copy keys/pointers, never record bodies
//! or historical requests. Mutations construct admitted replacements explicitly.
use super::state_memory::Shared;
use super::*;
use crate::storage::build_budget::reserve_memory;

impl<T: Serialize> Shared<T> {
    pub(super) fn admit(value: T) -> Result<Self, RecordError> {
        let bytes = checkpoint_codec::json_length(&value)? as u64;
        let mut lease = reserve_memory(
            bytes
                .checked_mul(32)
                .ok_or_else(|| storage("value size overflow"))?,
        )
        .map_err(storage)?;
        Self::from_admitted(value, &mut lease)
    }
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
    pub(super) fn binary_search_by_key<B: Ord>(
        &self,
        key: &B,
        mut f: impl FnMut(&T) -> B,
    ) -> Result<usize, usize> {
        self.values.binary_search_by_key(key, |v| f(v))
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
#[derive(Debug, Clone, Default)]
pub(super) struct Records {
    values: BTreeMap<String, Shared<Record>>,
    bytes: usize,
}
impl Records {
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
        if self.len()!=other.len() || self.bytes!=other.bytes {return Ok(false);}
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
        let previous = self.values.insert(id, value);
        self.bytes = bytes;
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
