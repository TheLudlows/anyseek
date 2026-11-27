//! In-memory ownership of open collection generations.
//!
//! A slot stays reserved until its owner has stopped and its resources have
//! been dropped. Catalog authorization and cold-open singleflight live outside
//! this table. Keys are the immutable scopes of Catalog collection entries.
use crate::storage::build_budget::shared_memory_usage;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    io,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    time::Instant,
};

pub(crate) struct Residency<T> {
    entries: Mutex<BTreeMap<String, Slot<T>>>,
    changed: watch::Sender<()>,
    close_slot: Arc<Semaphore>,
    budget: u64,
}

enum Slot<T> {
    Open(Resident<T>),
    Retiring(Option<Arc<T>>),
}

struct Resident<T> {
    value: T,
    activity: Arc<Mutex<Instant>>,
}

pub(crate) enum Lookup<T> {
    Missing,
    Retiring,
    Open { value: T, pin: Pin },
}

/// The pin can outlive a request or its cloned partition. A response can keep
/// this token through transport handoff without keeping a partition alive after
/// a separate Catalog deletion. Only a resident lookup can create a new pin.
pub(crate) struct Pin {
    activity: Option<Arc<Mutex<Instant>>>,
    changed: watch::Sender<()>,
}

impl Clone for Pin {
    fn clone(&self) -> Self {
        Self {
            activity: self.activity.clone(),
            changed: self.changed.clone(),
        }
    }
}

impl Drop for Pin {
    fn drop(&mut self) {
        let activity = self.activity.take().expect("pin released once");
        *lock(&activity) = Instant::now();
        drop(activity);
        self.changed.send_modify(|_| {});
    }
}

/// A claimed eviction is irreversible. Callers run shutdown in an owned task,
/// then release the slot only after dropping the successfully stopped value.
/// Abandoning the claim keeps the old owner quarantined in the table.
pub(crate) struct Retirement<T> {
    table: Arc<Residency<T>>,
    scope: String,
    value: Arc<T>,
    close_slot: OwnedSemaphorePermit,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

impl<T: Clone> Residency<T> {
    pub(crate) fn new(budget: u64) -> io::Result<Self> {
        let (_, shared_limit) = shared_memory_usage()?;
        if budget == 0 || budget > shared_limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid resident memory budget",
            ));
        }
        Ok(Self {
            entries: Mutex::new(BTreeMap::new()),
            changed: watch::channel(()).0,
            close_slot: Arc::new(Semaphore::new(1)),
            budget,
        })
    }

    /// Use bytes from the process's existing reservations, including pinned
    /// records and other admitted work. Never credit an estimated eviction.
    pub(crate) fn has_room(&self, requested: u64) -> bool {
        shared_memory_usage().is_ok_and(|(used, _)| {
            used.checked_add(requested)
                .is_some_and(|total| total <= self.budget)
        })
    }

    pub(crate) fn changes(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    /// Subscribe before checking pressure. Some global reservations and native
    /// owners are independent of this table, so also recheck periodically.
    pub(crate) async fn wait_for_change(
        changes: &mut watch::Receiver<()>,
        deadline: Instant,
    ) -> bool {
        let retry = Instant::now() + Duration::from_millis(25);
        tokio::select! {
            changed = changes.changed() => changed.is_ok() && Instant::now() < deadline,
            _ = tokio::time::sleep_until(retry.min(deadline)) => Instant::now() < deadline,
        }
    }

    pub(crate) fn lookup(&self, scope: &str) -> Lookup<T> {
        match lock(&self.entries).get(scope) {
            Some(Slot::Open(resident)) => Lookup::Open {
                value: resident.value.clone(),
                pin: self.pin(resident),
            },
            Some(Slot::Retiring(_)) => Lookup::Retiring,
            None => Lookup::Missing,
        }
    }

    /// Called under the Catalog's generation installation fence after the
    /// recovery owner has opened the partition. Return a request pin in the
    /// same step that publishes the value.
    pub(crate) fn install(&self, scope: String, value: T) -> Result<Pin, T> {
        let mut entries = lock(&self.entries);
        let Entry::Vacant(entry) = entries.entry(scope) else {
            return Err(value);
        };
        let resident = Resident {
            value,
            activity: Arc::new(Mutex::new(Instant::now())),
        };
        let pin = self.pin(&resident);
        entry.insert(Slot::Open(resident));
        self.changed.send_modify(|_| {});
        Ok(pin)
    }

    fn pin(&self, resident: &Resident<T>) -> Pin {
        Pin {
            activity: Some(resident.activity.clone()),
            changed: self.changed.clone(),
        }
    }

    /// Check engine-owned work while holding the same lock as pin acquisition.
    /// The predicate must use nonblocking checks and must not reenter the table.
    pub(crate) fn claim_cold(
        self: &Arc<Self>,
        requested: u64,
        idle: impl Fn(&T) -> bool,
    ) -> Option<Retirement<T>> {
        let mut entries = lock(&self.entries);
        if self.has_room(requested) {
            return None;
        }
        // Recheck the actual charge after one close, before spending another
        // resident generation to satisfy the same pressure.
        let close_slot = self.close_slot.clone().try_acquire_owned().ok()?;
        let scope = entries
            .iter()
            .filter_map(|(scope, slot)| match slot {
                Slot::Open(resident)
                    if Arc::strong_count(&resident.activity) == 1 && idle(&resident.value) =>
                {
                    Some((scope, *lock(&resident.activity)))
                }
                _ => None,
            })
            .min_by_key(|(_, activity)| *activity)?
            .0
            .clone();
        let slot = entries.get_mut(&scope).expect("locked resident table");
        let Slot::Open(resident) = std::mem::replace(slot, Slot::Retiring(None)) else {
            unreachable!("selected a resident")
        };
        let value = Arc::new(resident.value);
        *slot = Slot::Retiring(Some(value.clone()));
        Some(Retirement {
            table: self.clone(),
            scope,
            value,
            close_slot,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::build_budget::{MemoryLease, reserve_memory};

    fn table() -> Arc<Residency<Arc<MemoryLease>>> {
        let (used, _) = shared_memory_usage().unwrap();
        Arc::new(Residency::new(used + 64).unwrap())
    }

    #[test]
    fn coldest_idle_generation_releases_real_bytes_only_after_retirement() {
        let table = table();
        let old = "tenant/logs/0000000000000001";
        let new = "tenant/logs/0000000000000002";
        let old_pin = table
            .install(old.into(), Arc::new(reserve_memory(32).unwrap()))
            .ok()
            .unwrap();
        let new_pin = table
            .install(new.into(), Arc::new(reserve_memory(32).unwrap()))
            .ok()
            .unwrap();
        let output = old_pin.clone();
        drop(old_pin);
        assert!(table.has_room(0));
        assert!(table.claim_cold(1, |_| true).is_none());
        drop(output);
        drop(new_pin);

        let retirement = table.claim_cold(1, |_| true).unwrap();
        assert_eq!(retirement.value().bytes(), 32);
        assert!(matches!(table.lookup(old), Lookup::Retiring));
        assert!(table.claim_cold(1, |_| true).is_none());
        let Lookup::Open { value, pin } = table.lookup(new) else {
            panic!("new generation must stay open")
        };
        assert_eq!(value.bytes(), 32);
        assert!(!table.has_room(1));
        assert!(
            table
                .install(old.into(), Arc::new(reserve_memory(0).unwrap()))
                .is_err()
        );
        retirement.finish();
        assert!(table.has_room(32));
        assert!(matches!(table.lookup(old), Lookup::Missing));
        drop((value, pin));
    }

    #[tokio::test]
    async fn release_notifies_waiters_and_active_owners_have_a_deadline() {
        let table = table();
        let pin = table
            .install(
                "tenant/active/1".into(),
                Arc::new(reserve_memory(64).unwrap()),
            )
            .ok()
            .unwrap();
        let mut changes = table.changes();
        let deadline = Instant::now() + Duration::from_millis(5);
        assert!(table.claim_cold(1, |_| true).is_none());
        assert!(!Residency::<Arc<MemoryLease>>::wait_for_change(&mut changes, deadline).await);

        let deadline = Instant::now() + Duration::from_secs(1);
        drop(pin);
        tokio::time::timeout_at(deadline, changes.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(
            table.claim_cold(1, |_| false).is_none(),
            "engine activity vetoes eviction"
        );
        table.claim_cold(1, |_| true).unwrap().finish();
        assert!(table.has_room(64));
    }

    #[test]
    fn abandoned_shutdown_preserves_the_generation_barrier_and_charge() {
        let table = table();
        let scope = "tenant/logs/1";
        drop(
            table
                .install(scope.into(), Arc::new(reserve_memory(64).unwrap()))
                .ok()
                .unwrap(),
        );
        drop(table.claim_cold(1, |_| true).unwrap());
        assert!(matches!(table.lookup(scope), Lookup::Retiring));
        assert!(!table.has_room(1));
        assert!(table.claim_cold(1, |_| true).is_none());
    }
}

impl<T> Retirement<T> {
    pub(crate) fn value(&self) -> &T {
        &self.value
    }

    /// Release after shutdown has joined the native workers. The retiring
    /// barrier covers the actual destruction of the last stored value.
    pub(crate) fn finish(self) {
        let Self {
            table,
            scope,
            value,
            close_slot,
        } = self;
        let retained = match lock(&table.entries).get_mut(&scope) {
            Some(Slot::Retiring(retained)) => retained.take(),
            _ => unreachable!("retirement owns its slot"),
        };
        drop(retained);
        drop(value);
        lock(&table.entries).remove(&scope);
        drop(close_slot);
        table.changed.send_modify(|_| {});
    }
}
