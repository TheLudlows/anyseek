//! Exact live ordinal membership for Record vector/metadata components.
//! Lexical corpus statistics live exclusively in the native fixed-searcher view.
use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
};
/// Append-only invalidation independent of vector writer/read guards. An old
/// or queued physical row can never become live again after retirement.
#[derive(Debug, Default)]
pub(crate) struct Retired(RwLock<BTreeSet<usize>>);
impl Retired {
    pub(crate) fn contains(&self, id: usize) -> bool {
        self.0.read().map(|rows| rows.contains(&id)).unwrap_or(true)
    }
    pub(crate) fn insert(&self, id: usize) -> Result<(), String> {
        self.0
            .write()
            .map_err(|_| "retirement mask poisoned")?
            .insert(id);
        Ok(())
    }
}
#[derive(Clone, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActiveStatistics {
    pub keys: Vec<String>,
    live: BTreeSet<usize>,
    #[serde(skip)]
    pub(crate) retired: Option<Arc<Retired>>,
}
impl ActiveStatistics {
    pub(crate) fn clone_local(&self) -> Self {
        Self {
            keys: self.keys.clone(),
            live: self
                .live
                .iter()
                .copied()
                .filter(|id| self.is_live(*id))
                .collect(),
            retired: None,
        }
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
    pub(crate) fn validate_sparse(&self, _fields: usize) -> Result<(), String> {
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
        let mut state = serializer.serialize_struct("ActiveStatistics", 2)?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("live", &Live(self))?;
        state.end()
    }
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
        current.keys.push("a".into());
        current.publish(0);
        let old = current.clone_local();
        current.retire(0);
        assert!(old.is_live(0));
        assert!(!current.is_live(0));
        current.keys.push("a".into());
        current.publish(1);
        assert!(current.validate_sparse(0).is_ok());
    }
}
