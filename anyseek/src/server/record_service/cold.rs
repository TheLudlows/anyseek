//! Generation-scoped recovery for existing HTTP collections.
use super::*;
use anyseek::resources::Lane;
use tokio::sync::watch;
use tokio::time::Instant;

#[cfg(test)]
mod tests;

type OpenResult = Option<Result<(), Error>>;

#[derive(Default)]
pub(super) struct Registry {
    pending: std::sync::Mutex<BTreeMap<String, watch::Receiver<OpenResult>>>,
    // Whole-service backup can open cold partitions privately. Exclude lazy
    // opens while it captures residents and opens its remaining inputs.
    pub(super) disk: RwLock<()>,
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
        let mut completion = {
            // Acquire the generation handle under the resident lock. There
            // cannot be a gap between a miss and registration of the flight.
            let records = self.records.read().await;
            if let Some(partition) = records.get(&key) {
                return Ok(CollectionRecord {
                    generation,
                    partition: partition.clone(),
                });
            }
            let mut pending = self.cold.pending.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(completion) = pending.get(&key) {
                completion.clone()
            } else {
                // A request already admitted by this service can still start
                // a required child during shutdown. The opener retains that
                // ticket, independently of the request's resource scope.
                let activity = if anyseek::resources::Lifecycle::current()
                    .is_some_and(|current| Arc::ptr_eq(&current, &self.lifecycle))
                {
                    anyseek::resources::Activity::current()
                } else {
                    self.lifecycle.enter()
                };
                let activity = activity.ok_or_else(|| {
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
                tokio::spawn(activity.run(async move {
                    let result = scope.run(flight.open(entry, queued)).await;
                    drop(flight);
                    done.send_replace(Some(result));
                }));
                completion
            }
        };
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
        let records = self.records.read().await;
        let partition = records
            .get(&key)
            .cloned()
            .ok_or(CatalogError::Unauthorized)?;
        Ok(CollectionRecord {
            generation,
            partition,
        })
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
            let partition = Box::pin(self.open_partition(&entry)).await?;
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
                .write()
                .await
                .insert(flight.key.clone(), partition);
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
        let _disk = service.cold.disk.read().await;
        let generation = entry.generation;
        let current = service
            .catalog
            .collection_entry(&entry.tenant_id, &entry.collection_id)
            .await?;
        if current.generation != generation {
            return Err(CatalogError::Unauthorized.into());
        }
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
        let owner = service.clone();
        let key = self.key.clone();
        service
            .catalog
            .authorized_generation(
                entry.tenant_id,
                entry.collection_id,
                generation,
                async move |_| {
                    owner.records.write().await.insert(key, partition?);
                    #[cfg(test)]
                    owner.cold.test.installs.increment();
                    log::debug!("cold collection installed generation={generation}");
                    Ok(())
                },
            )
            .await?
    }
}
