//! Private mutable manifest container: every mutable access invalidates admission
//! statistics before handing out a reference. The cache is never authority.
use super::*;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Counts {
    pub members: usize,
    pub controls: usize,
    pub ttl: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct Generations {
    values: BTreeMap<String, generation::Manifest>,
    #[serde(skip)]
    counts: OnceLock<Counts>,
}
impl Generations {
    fn scanned_counts(&self) -> Counts {
        let mut result = Counts::default();
        for stage in self.values.values().filter_map(|g| g.staged.as_ref()) {
            result.members += stage.members.len();
            result.controls += if stage.sealed { 1 } else { 2 };
            result.ttl += stage.chunks.iter().filter(|c|
                c.temporal.as_ref().is_some_and(|t| t.expire_at_ms.is_some())).count();
        }
        result
    }
    pub(super) fn counts(&self) -> &Counts {
        self.counts.get_or_init(|| self.scanned_counts())
    }
    pub(super) fn validate_counts(&self) -> Result<(), RecordError> {
        let scanned = self.scanned_counts();
        if self.counts.get().is_some_and(|cached| *cached != scanned) {
            return Err(storage("generation admission counts mismatch"));
        }
        Ok(())
    }
    pub(super) fn insert(&mut self, key: String, value: generation::Manifest) -> Option<generation::Manifest> {
        self.counts.take();
        self.values.insert(key, value)
    }
    pub(super) fn get_mut(&mut self, key: &str) -> Option<&mut generation::Manifest> {
        self.counts.take();
        self.values.get_mut(key)
    }
}
// Deliberately no DerefMut: entry/values_mut/index_mut cannot bypass invalidation.
impl std::ops::Deref for Generations {
    type Target = BTreeMap<String, generation::Manifest>;
    fn deref(&self) -> &Self::Target { &self.values }
}
impl<'a> IntoIterator for &'a Generations {
    type Item = (&'a String, &'a generation::Manifest);
    type IntoIter = std::collections::btree_map::Iter<'a, String, generation::Manifest>;
    fn into_iter(self) -> Self::IntoIter { self.values.iter() }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn manifest() -> generation::Manifest {
        serde_json::from_value(serde_json::json!({
             "version":1, "used":["g"], "active":null, "active_members":[],
            "staged":{"deadline_ms":1000,"scope":"scope","id":"g",
                "source":{"record_id":"s","version":1},
                "chunks":[{"record_id":"a","document":{},"temporal":{"expire_at_ms":100},"evidence_refs":[]}],
                "members":["a"],"sealed":false}
        })).unwrap()
    }
    #[test]
    fn controlled_mutation_invalidates_counts_and_wire_remains_legacy_map() {
        let mut values = Generations::default();
        assert_eq!(*values.counts(), Counts::default());
        values.insert("doc".into(), manifest());
        assert!(values.counts.get().is_none());
        assert_eq!(*values.counts(), Counts { members:1, controls:2, ttl:1 });
        values.validate_counts().unwrap();
        let before = serde_json::to_vec(&values).unwrap();
        let mut cloned = values.clone();
        cloned.get_mut("doc").unwrap().staged.as_mut().unwrap().sealed = true;
        assert!(cloned.counts.get().is_none());
        assert_eq!(cloned.counts().controls,1);
        assert_eq!(values.counts().controls,2);
        // Missing mutable access and rejected caller operations may evict a cache,
        // but cannot change wire authority or produce a stale cached result.
        assert!(values.get_mut("absent").is_none());
        assert!(values.counts.get().is_none());
        assert_eq!(before,serde_json::to_vec(&values).unwrap());
        assert_eq!(values.counts().controls,2);
        let raw: BTreeMap<String,generation::Manifest> = serde_json::from_slice(&before).unwrap();
        let restored: Generations = serde_json::from_slice(&serde_json::to_vec(&raw).unwrap()).unwrap();
        assert!(restored.counts.get().is_none());
        restored.validate_counts().unwrap();
        assert_eq!(*restored.counts(),*values.counts());
        assert_eq!(before,serde_json::to_vec(&restored).unwrap());
        // Replacement must invalidate an already initialized cache too.
        let mut replacement = manifest(); replacement.staged = None;
        assert!(values.insert("doc".into(),replacement).is_some());
        assert_eq!(*values.counts(),Counts::default());
        values.validate_counts().unwrap();
    }
    #[test]
    fn recovery_oracle_rejects_stale_cache_without_trusting_it() {
        let mut values = Generations::default();
        values.insert("doc".into(),manifest());
        values.counts.set(Counts::default()).unwrap();
        assert!(values.validate_counts().is_err());
        let restored: Generations = serde_json::from_slice(&serde_json::to_vec(&values).unwrap()).unwrap();
        restored.validate_counts().unwrap();
        assert_eq!(restored.counts().ttl,1);
    }
}
