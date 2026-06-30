use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use seekpower_core::Index;
use seekpower_error::SeekPowerError;
use seekpower_storage::KvStore;

/// Shared state held by the axum router.
pub type SharedState = Arc<State>;

pub struct State {
    pub db: KvStore,
    pub indexes: Mutex<HashMap<String, Arc<Index>>>,
}

impl State {
    pub fn new(db: KvStore) -> Self {
        Self {
            db,
            indexes: Mutex::new(HashMap::new()),
        }
    }
}

/// Get an index by name from the shared state.
pub fn get_index(state: &SharedState, name: &str) -> Result<Arc<Index>, SeekPowerError> {
    let guard = state
        .indexes
        .lock()
        .map_err(|_| SeekPowerError::StorageError("state mutex poisoned".into()))?;
    guard
        .get(name)
        .cloned()
        .ok_or_else(|| SeekPowerError::IndexNotLoaded(format!("index '{}' not loaded", name)))
}

/// Get the first available index (for backwards compatibility with single-index API).
pub fn get_any_index(state: &SharedState) -> Result<Arc<Index>, SeekPowerError> {
    let guard = state
        .indexes
        .lock()
        .map_err(|_| SeekPowerError::StorageError("state mutex poisoned".into()))?;
    guard
        .values()
        .next()
        .cloned()
        .ok_or_else(|| SeekPowerError::IndexNotLoaded("no index loaded".into()))
}