//! Generation-scoped recovery for existing HTTP collections.
use super::*;
use crate::resources::residency::Lookup;
use anyseek::resources::Lane;
use tokio::sync::watch;
use tokio::time::Instant;

#[cfg(test)]
mod tests;

type OpenResult = Option<Result<(), Error>>;

enum Next {
    Ready(CollectionRecord),
    Retiring,
    Opening(watch::Receiver<OpenResult>),
}

#[derive(Default)]
pub(super) struct Registry {
    pending: std::sync::Mutex<BTreeMap<String, watch::Receiver<OpenResult>>>,
    // Whole-service backup can open cold partitions privately. Exclude lazy
    // opens while it captures residents and opens its remaining inputs.
    pub(super) disk: Arc<RwLock<()>>,
    #[cfg(test)]
    pub(super) test: tests::Hooks,
}

struct Flight {
    service: Arc<Service>,
    key: String,
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.service
            .cold
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

impl Service {
    pub(super) fn recovery_scope(&self, entry: &CollectionEntry) -> Result<ResourceScope, Error> {
        self.resources
            .admit(
                format!("collection:{}", entry.scope()),
                Lane::Recovery,
                (MAX_BODY + MAX_RESPONSE) as u64,
            )
            .map_err(|error| Error(StatusCode::TOO_MANY_REQUESTS, "busy", error.to_string()))
    }

    pub(super) async fn record(
        self: &Arc<Self>,
        tenant: &str,
        id: &str,
    ) -> Result<CollectionRecord, Error> {
        let entry = self.catalog.collection_entry(tenant, id).await?;
        if ROUTED_COLLECTION
            .try_with(|route| {
                route.as_ref().is_some_and(|route| {
                    route.tenant == tenant
                        && route.id == id
                        && route.generation != Some(entry.generation)
                })
            })
            .unwrap_or(false)
        {
            return Err(CatalogError::Unauthorized.into());
        }
        self.record_entry(entry).await
    }

    pub(super) async fn record_entry(
        self: &Arc<Self>,
        entry: CollectionEntry,
    ) -> Result<CollectionRecord, Error> {
        let key = entry.scope();
        let generation = entry.generation;
        let queued = Instant::now();
        let retirement_deadline = residency::pressure_deadline(self.residency_deadline());
        let mut changes = self.records.changes();
        loop {
            let next = self
                .records
                .with_lookup(&key, |lookup| -> Result<Next, Error> {
                    match lookup {
                        Lookup::Open { value, pin } => {
                            return Ok(Next::Ready(CollectionRecord {
                                generation,
                                partition: value,
                                residency: pin,
                            }));
                        }
                        Lookup::Retiring => return Ok(Next::Retiring),
                        Lookup::Missing => {}
                    }
                    // Singleflight registration shares the resident lookup gate.
                    let mut pending = self.cold.pending.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(completion) = pending.get(&key) {
                        return Ok(Next::Opening(completion.clone()));
                    }
                    let activity = self.collection_activity().ok_or_else(|| {
                        Error(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "shutting_down",
                            "service is shutting down".into(),
                        )
                    })?;
                    let scope = self.recovery_scope(&entry)?;
                    let (done, completion) = watch::channel(None);
                    pending.insert(key.clone(), completion.clone());
                    let flight = Flight {
                        service: self.clone(),
                        key: key.clone(),
                    };
                    let entry = entry.clone();
                    tokio::spawn(activity.run(async move {
                        let result = scope.run(flight.open(entry, queued)).await;
                        drop(flight);
                        done.send_replace(Some(result));
                    }));
                    Ok(Next::Opening(completion))
                })?;
            match next {
                Next::Ready(record) => return Ok(record),
                Next::Retiring => {
                    #[cfg(test)]
                    self.cold.test.retirement_waiters.increment();
                    if !Residency::<RecordPartition>::wait_for_change(
                        &mut changes,
                        retirement_deadline,
                    )
                    .await
                    {
                        return Err(RecordError::Busy.into());
                    }
                }
                Next::Opening(mut completion) => {
                    #[cfg(test)]
                    self.cold.test.waiters.increment();
                    let result = completion
                        .wait_for(Option::is_some)
                        .await
                        .map_err(|_| storage("collection opener exited without a result"))?
                        .as_ref()
                        .unwrap()
                        .clone();
                    log::debug!(
                        "cold collection wait generation={generation} elapsed_ms={}",
                        queued.elapsed().as_millis()
                    );
                    result?;
                    // Completion carries no ownership. Reacquire the same
                    // generation, including when retirement won the handoff.
                }
            }
        }
    }

    // Management holds the control gate for unfinished intents. Register
    // before activation so a data request cannot start a second physical open
    // between Catalog activation and the resident insertion.
    pub(super) async fn finish_collection(
        self: &Arc<Self>,
        entry: CollectionEntry,
    ) -> Result<(), Error> {
        if entry.deleted {
            return Ok(());
        }
        if entry.active {
            self.record_entry(entry).await?;
            return Ok(());
        }
        let key = entry.scope();
        let (done, completion) = watch::channel(None);
        let flight = {
            let mut pending = self.cold.pending.lock().unwrap_or_else(|e| e.into_inner());
            if pending.contains_key(&key) {
                return Err(CatalogError::Busy.into());
            }
            pending.insert(key.clone(), completion);
            Flight {
                service: self.clone(),
                key,
            }
        };
        let result = async {
            let deadline = self.residency_deadline();
            let disk = self.open_gate(deadline).await?;
            self.wait_for_resident_room(&disk, 1, deadline).await?;
            let partition = Box::pin(self.open_partition(&entry)).await?;
            self.wait_for_resident_room(&disk, 0, deadline).await?;
            self.catalog
                .execute(CatalogRequest {
                    operation_id: internal_id(format!("sys:activate:{}", entry.directory())),
                    command: CatalogCommand::ActivateCollection {
                        tenant_id: entry.tenant_id,
                        collection_id: entry.collection_id,
                    },
                })
                .await?;
            self.records
                .install(flight.key.clone(), partition)
                .map_err(|_| RecordError::Busy)?;
            Ok(())
        }
        .await;
        drop(flight);
        done.send_replace(Some(result.clone()));
        result
    }
}

impl Flight {
    async fn open(&self, entry: CollectionEntry, queued: Instant) -> Result<(), Error> {
        let service = &self.service;
        // The recovery owner has its own budget deadline, independent of any
        // one cancelled or already-near-deadline request waiting for it.
        let deadline = queued + service.request_timeout;
        let disk = service.open_gate(deadline).await?;
        let generation = entry.generation;
        let current = service
            .catalog
            .collection_entry(&entry.tenant_id, &entry.collection_id)
            .await?;
        if current.generation != generation {
            return Err(CatalogError::Unauthorized.into());
        }
        service.wait_for_resident_room(&disk, 1, deadline).await?;
        let opened = Instant::now();
        log::debug!(
            "cold collection opening generation={generation} queued_ms={}",
            queued.elapsed().as_millis()
        );
        #[cfg(test)]
        service.cold.test.attempts.increment();
        let partition = Box::pin(service.open_partition(&current)).await;
        log::debug!(
            "cold collection open complete generation={generation} open_ms={} ok={}",
            opened.elapsed().as_millis(),
            partition.is_ok(),
        );
        #[cfg(test)]
        if partition.is_ok() {
            service.cold.test.before_install.wait().await;
        }
        let partition = partition?;
        service.wait_for_resident_room(&disk, 0, deadline).await?;
        let owner = service.clone();
        let key = self.key.clone();
        service
            .catalog
            .authorized_generation(
                entry.tenant_id,
                entry.collection_id,
                generation,
                async move |_| {
                    if owner.master_verifier.is_some() && !owner.records.has_room(0) {
                        return Err(RecordError::Busy.into());
                    }
                    owner
                        .records
                        .install(key, partition)
                        .map_err(|_| RecordError::Busy)?;
                    #[cfg(test)]
                    owner.cold.test.installs.increment();
                    log::debug!("cold collection installed generation={generation}");
                    Ok(())
                },
            )
            .await?
    }
}
