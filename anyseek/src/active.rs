//! Exact live ordinal membership for Record vector/metadata components.
//! Lexical corpus statistics live exclusively in the native fixed-searcher view.
use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
};
/// Append-only invalidation independent of vector writer/read guards. An old
/// or queued physical row can never become live again after retirement.
#[derive(Default)]
pub(crate) struct Retired {
    rows: RwLock<BTreeSet<usize>>,
    shared: RwLock<std::collections::BTreeMap<usize, TokenRef>>,
}
pub(crate) struct LiveToken {
    live: std::sync::atomic::AtomicBool,
    _memory: crate::storage::build_budget::MemoryLease,
}
impl LiveToken {
    pub(crate) fn is_live(&self) -> bool { self.live.load(std::sync::atomic::Ordering::Acquire) }
}
struct TokenRef {
    token: Arc<LiveToken>,
    _memory: crate::storage::build_budget::MemoryLease,
}
impl Retired {
    pub(crate) fn contains(&self, id: usize) -> bool {
        self.rows.read().map(|rows| rows.contains(&id)).unwrap_or(true)
            || self.shared.read().map(|rows| rows.get(&id).is_some_and(|row| !row.token.is_live())).unwrap_or(true)
    }
    pub(crate) fn insert(&self, id: usize) -> Result<(), String> {
        // Same lock order as token(): retirement cannot race token creation.
        let mut rows = self.rows.write().map_err(|_| "retirement mask poisoned")?;
        let shared = self.shared.read().map_err(|_| "retirement token map poisoned")?;
        rows.insert(id);
        if let Some(row) = shared.get(&id) {
            row.token.live.store(false, std::sync::atomic::Ordering::Release);
        }
        Ok(())
    }
    pub(crate) fn token(&self, id: usize) -> Result<Arc<LiveToken>, String> {
        let rows = self.rows.read().map_err(|_| "retirement mask poisoned")?;
        let mut shared = self.shared.write().map_err(|_| "retirement token map poisoned")?;
        if let Some(row) = shared.get(&id) { return Ok(row.token.clone()); }
        let memory = crate::storage::build_budget::reserve_memory(128).map_err(|error| error.to_string())?;
        let link = crate::storage::build_budget::reserve_memory(64).map_err(|error| error.to_string())?;
        let token = Arc::new(LiveToken { live: std::sync::atomic::AtomicBool::new(!rows.contains(&id)), _memory: memory });
        shared.insert(id, TokenRef { token: token.clone(), _memory: link });
        Ok(token)
    }
    pub(crate) fn share(&self, id: usize, token: Arc<LiveToken>) -> Result<(), String> {
        let memory = crate::storage::build_budget::reserve_memory(64).map_err(|error| error.to_string())?;
        self.shared.write().map_err(|_| "retirement token map poisoned")?
            .insert(id, TokenRef { token, _memory: memory });
        Ok(())
    }
}
fn default_retired() -> Option<Arc<Retired>> { Some(Arc::new(Retired::default())) }
#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActiveStatistics {
    pub keys: Vec<String>,
    /// Logical body version and indexing revision; independent of row position.
    pub revisions: Vec<(u64, u64)>,
    /// Fingerprints preserve query identity across cache rebuilds.
    pub fingerprints: Vec<[u8; 32]>,
    live: BTreeSet<usize>,
    #[serde(skip, default = "default_retired")]
    pub(crate) retired: Option<Arc<Retired>>,
}
impl Default for ActiveStatistics {
    fn default() -> Self {
        Self { keys: Vec::new(), revisions: Vec::new(), fingerprints: Vec::new(), live: BTreeSet::new(), retired: default_retired() }
    }
}
impl ActiveStatistics {
    pub(crate) fn clone_local(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            revisions: self.revisions.clone(),
            fingerprints: self.fingerprints.clone(),
            live: self
                .live
                .iter()
                .copied()
                .filter(|id| self.is_live(*id))
                .collect(),
            retired: None,
        }
    }
    pub(crate) fn append_identity(&mut self, key: String, version: u64, revision: u64, fingerprint: [u8; 32]) {
        self.keys.push(key);
        self.revisions.push((version, revision));
        self.fingerprints.push(fingerprint);
    }
    pub(crate) fn is_live(&self, id: usize) -> bool {
        self.live.contains(&id) && !self.retired.as_ref().is_some_and(|mask| mask.contains(id))
    }
    pub(crate) fn publish(&mut self, id: usize) {
        assert!(id < self.keys.len());
        assert!(self.live.insert(id));
    }
    pub(crate) fn retire(&mut self, id: usize) {
        self.live.remove(&id);
    }
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.revisions.len() != self.keys.len() {
            return Err("component identity cardinality mismatch".into());
        }
        if self.fingerprints.len() != self.keys.len() {
            return Err("component fingerprint cardinality mismatch".into());
        }
        if self.live.iter().any(|id| *id >= self.keys.len()) {
            return Err("active ordinal outside component".into());
        }
        let mut live_keys = BTreeSet::new();
        if self
            .live
            .iter()
            .filter(|id| self.is_live(**id))
            .any(|id| !live_keys.insert(&self.keys[*id]))
        {
            return Err("duplicate active identity".into());
        }
        Ok(())
    }
}
impl serde::Serialize for ActiveStatistics {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeSeq, SerializeStruct};
        struct Live<'a>(&'a ActiveStatistics);
        impl serde::Serialize for Live<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(None)?;
                for id in &self.0.live {
                    if self.0.is_live(*id) {
                        seq.serialize_element(id)?;
                    }
                }
                seq.end()
            }
        }
        let mut state = serializer.serialize_struct("ActiveStatistics", 4)?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("live", &Live(self))?;
        state.serialize_field("revisions", &self.revisions)?;
        state.serialize_field("fingerprints", &self.fingerprints)?;
        state.end()
    }
}
/// Hash all indexed input, including temporal/facet fields and vector patches.
/// The top-level field order is normalized without copying values.
pub(crate) fn fingerprint(document: &crate::index::Document) -> Result<[u8; 32], String> {
    use sha2::{Digest, Sha256};
    struct Sink(Sha256);
    impl std::io::Write for Sink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let sorted = document.iter().collect::<std::collections::BTreeMap<_, _>>();
    let mut sink = Sink(Sha256::new());
    serde_json::to_writer(&mut sink, &sorted).map_err(|error| error.to_string())?;
    Ok(sink.0.finalize().into())
}

pub(crate) fn rank_with_keys(
    keys: Option<&[String]>,
    a: f32,
    a_id: usize,
    b: f32,
    b_id: usize,
) -> std::cmp::Ordering {
    crate::min_heap::score_ordering(a, b).then_with(|| match keys {
        Some(keys) => keys[b_id].cmp(&keys[a_id]),
        None => b_id.cmp(&a_id),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn copied_membership_and_retirement_do_not_change_an_old_view() {
        let mut current = ActiveStatistics::default();
        current.append_identity("a".into(), 1, 1, [0; 32]);
        current.publish(0);
        let old = current.clone_local();
        current.retire(0);
        assert!(old.is_live(0));
        assert!(!current.is_live(0));
        current.append_identity("a".into(), 2, 2, [1; 32]);
        current.publish(1);
        assert!(current.validate().is_ok());
    }
}
