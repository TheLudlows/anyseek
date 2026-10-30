//! Lock order: global snapshot barrier -> collection barrier -> catalog metadata.
//! No barrier wait may hold the metadata mutex. Owned guards belong to accepted
//! work, never to the HTTP waiter. Identity gates disappear after their last user.
use super::{CatalogCommand, CatalogError, identity};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

#[derive(Default)]
pub(super) struct Execution {
    pub(super) global: Arc<RwLock<()>>,
    collections: Mutex<BTreeMap<String, Weak<RwLock<()>>>>,
}
pub(super) struct Read {
    _collection: OwnedRwLockReadGuard<()>,
    _global: OwnedRwLockReadGuard<()>,
}
enum Global {
    Read { _guard: OwnedRwLockReadGuard<()> },
    Write { _guard: OwnedRwLockWriteGuard<()> },
}
pub(super) struct Control {
    _collection: Option<OwnedRwLockWriteGuard<()>>,
    _global: Global,
}
impl Execution {
    pub(super) fn collection(&self, id: &str) -> Result<Arc<RwLock<()>>, CatalogError> {
        identity(id)?;
        let mut gates = self
            .collections
            .lock()
            .map_err(|_| CatalogError::RecoveryRequired)?;
        gates.retain(|_, weak| weak.strong_count() != 0);
        if let Some(gate) = gates.get(id).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        // Every user first owns one of the catalog's 64 admission slots; each
        // command uses at most one collection gate. Keep a hard defensive bound.
        if gates.len() >= 64 {
            return Err(CatalogError::Busy);
        }
        let gate = Arc::new(RwLock::new(()));
        gates.insert(id.to_owned(), Arc::downgrade(&gate));
        Ok(gate)
    }
    pub(super) async fn read(&self, id: &str) -> Result<Read, CatalogError> {
        let global = self.global.clone().read_owned().await;
        let collection = self.collection(id)?.read_owned().await;
        Ok(Read {
            _collection: collection,
            _global: global,
        })
    }
    pub(super) async fn control(&self, command: &CatalogCommand) -> Result<Control, CatalogError> {
        if matches!(command, CatalogCommand::RetireBackup { .. }) {
            return Ok(Control {
                _collection: None,
                _global: Global::Write {
                    _guard: self.freeze().await,
                },
            });
        }
        let global = self.global.clone().read_owned().await;
        let id = command.collection_scope();
        let collection = match id {
            // An invalid identity cannot name any retained collection. Leave
            // its rejection to the authority, AFTER original operation lookup,
            // so a reused operation ID still reports Conflict rather than a
            // new request's validation error. It must not allocate a gate.
            Some(id) => Some(self.collection(&id)?.write_owned().await),
            None => None,
        };
        Ok(Control {
            _collection: collection,
            _global: Global::Read { _guard: global },
        })
    }
    pub(super) async fn freeze(&self) -> OwnedRwLockWriteGuard<()> {
        self.global.clone().write_owned().await
    }
}
