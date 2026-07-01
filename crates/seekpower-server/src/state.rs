use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use seekpower_core::Index;
use seekpower_error::SeekPowerError;
use seekpower_storage::KvStore;

/// Shared state held by the axum router.
pub type SharedState = Arc<State>;

pub struct State {
    pub db: KvStore,
    pub next_index_id: std::sync::atomic::AtomicU32,
    pub indexes: Mutex<HashMap<String, Arc<Index>>>,
}

impl State {
    pub fn new(db: KvStore) -> Self {
        Self {
            db,
            next_index_id: std::sync::atomic::AtomicU32::new(1),
            indexes: Mutex::new(HashMap::new()),
        }
    }

    /// Initialize the next_index_id by scanning the registry for existing indexes.
    pub async fn init_from_registry(&self) -> Result<(), SeekPowerError> {
        let prefix = b"__indexes__:";
        let entries = self.db.scan_prefix(prefix).await
            .map_err(|e| SeekPowerError::StorageError(format!("Failed to scan registry: {}", e)))?;

        let mut max_id: u32 = 0;
        for (_key, value) in entries {
            if value.len() == 4 {
                let id_bytes: [u8; 4] = value.try_into()
                    .map_err(|_| SeekPowerError::StorageError("Invalid index_id in registry".into()))?;
                let id = u32::from_be_bytes(id_bytes);
                if id > max_id {
                    max_id = id;
                }
            }
        }
        self.next_index_id.store(max_id + 1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
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