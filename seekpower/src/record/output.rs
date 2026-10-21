//! Data hiding barrier independent of a fixed read view and catalog authority.
use super::RecordError;
use std::sync::{Arc, RwLock};

#[derive(Default)]
pub(super) struct OutputState {
    pub revision: u64,
    pub fenced: bool,
}
/// A bounded response may use an old complete read view, but may not cross a
/// source deletion/failure boundary. Combine with authorization at handoff.
#[derive(Clone)]
pub struct DataOutputPermit {
    states: Vec<(Arc<RwLock<OutputState>>, u64)>,
}
impl DataOutputPermit {
    pub(super) fn new(state: Arc<RwLock<OutputState>>, revision: u64) -> Self {
        Self {
            states: vec![(state, revision)],
        }
    }
    /// Combine already captured views for bounded collection listings. This
    /// aggregates output barriers, not data snapshots or cross-collection writes.
    pub fn combine(permits: Vec<Self>) -> Self {
        let mut states = std::collections::BTreeMap::new();
        for (state, revision) in permits.into_iter().flat_map(|p| p.states) {
            let entry = states
                .entry(Arc::as_ptr(&state) as usize)
                .or_insert((state, revision));
            entry.1 = entry.1.min(revision);
        }
        Self {
            states: states.into_values().collect(),
        }
    }
    /// Check and hand off without an intervening await. The callback must not
    /// reenter the engine or acquire an earlier catalog/partition lock.
    pub fn deliver<T>(&self, handoff: impl FnOnce() -> T) -> Result<T, RecordError> {
        let guards = self
            .states
            .iter()
            .map(|(state, _)| state.read().map_err(|_| RecordError::RecoveryRequired))
            .collect::<Result<Vec<_>, _>>()?;
        for (guard, (_, revision)) in guards.iter().zip(&self.states) {
            if guard.fenced || guard.revision != *revision {
                return Err(RecordError::RecoveryRequired);
            }
        }
        Ok(handoff())
    }
}
