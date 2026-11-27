//! Admit cold HTTP collections against the shared explicit-memory ledger.
use super::*;
use std::time::Duration;
use tokio::{sync::OwnedRwLockReadGuard, time::Instant};

pub(super) fn pressure_deadline(deadline: Instant) -> Instant {
    deadline.min(Instant::now() + Duration::from_secs(2))
}

pub(super) struct Output {
    pub(super) bytes: Bytes,
    pub(super) _data: DataOutputPermit,
}

impl AsRef<[u8]> for Output {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

pub(super) fn table(http: bool) -> Result<Residency<RecordPartition>, String> {
    let (_, shared) =
        crate::storage::build_budget::shared_memory_usage().map_err(|e| e.to_string())?;
    let budget = if http {
        match std::env::var("ANYSEEK_RESIDENT_MEMORY_BYTES") {
            Ok(value) => value
                .parse()
                .map_err(|_| "invalid ANYSEEK_RESIDENT_MEMORY_BYTES")?,
            Err(std::env::VarError::NotPresent) => shared - shared / 4,
            Err(_) => return Err("invalid ANYSEEK_RESIDENT_MEMORY_BYTES".into()),
        }
    } else {
        shared
    };
    Residency::new(budget).map_err(|e| e.to_string())
}

impl Service {
    pub(super) fn collection_activity(&self) -> Option<anyseek::resources::Activity> {
        if anyseek::resources::Lifecycle::current()
            .is_some_and(|current| Arc::ptr_eq(&current, &self.lifecycle))
        {
            anyseek::resources::Activity::current()
        } else {
            self.lifecycle.enter()
        }
    }

    pub(super) fn residency_deadline(&self) -> Instant {
        REQUEST_DEADLINE
            .try_with(|deadline| *deadline)
            .unwrap_or_else(|_| Instant::now() + self.request_timeout)
    }

    pub(super) async fn open_gate(
        &self,
        deadline: Instant,
    ) -> Result<Arc<OwnedRwLockReadGuard<()>>, Error> {
        tokio::time::timeout_at(deadline, self.cold.disk.clone().read_owned())
            .await
            .map(Arc::new)
            .map_err(|_| RecordError::Busy.into())
    }

    pub(super) async fn wait_for_resident_room(
        self: &Arc<Self>,
        disk: &Arc<OwnedRwLockReadGuard<()>>,
        requested: u64,
        deadline: Instant,
    ) -> Result<(), Error> {
        if self.master_verifier.is_none() {
            return Ok(());
        }
        let deadline = pressure_deadline(deadline);
        let mut changes = self.records.changes();
        while !self.records.has_room(requested) {
            if Instant::now() >= deadline {
                return Err(RecordError::Busy.into());
            }
            self.retire_one(disk, requested);
            if !Residency::<RecordPartition>::wait_for_change(&mut changes, deadline).await {
                return Err(RecordError::Busy.into());
            }
        }
        Ok(())
    }

    pub(super) fn evict_over_budget(self: &Arc<Self>) {
        if self.master_verifier.is_some() && !self.records.has_room(0) {
            if let Ok(disk) = self.cold.disk.clone().try_read_owned() {
                self.retire_one(&Arc::new(disk), 0);
            }
        }
    }

    fn retire_one(self: &Arc<Self>, disk: &Arc<OwnedRwLockReadGuard<()>>, requested: u64) {
        // Acquire ownership before claiming, so shutdown cannot strand a close
        // between removal from request lookups and starting the native join.
        let Some(activity) = self.collection_activity() else {
            return;
        };
        let Some(retirement) = self
            .records
            .claim_cold(requested, RecordPartition::idle_for_residency)
        else {
            return;
        };
        let disk = disk.clone();
        tokio::spawn(activity.run(async move {
            // Hold the physical open/backup gate across native shutdown and
            // destruction. No Catalog or service write lock is taken here.
            let _disk = disk;
            match retirement.value().finish_service_shutdown().await {
                Ok(()) => {
                    if let Err(error) =
                        tokio::task::spawn_blocking(move || retirement.finish()).await
                    {
                        log::warn!("record retirement destruction unfinished: {error}");
                    }
                }
                Err(error) => log::warn!("record retirement shutdown unfinished: {error}"),
            }
        }));
    }
}
