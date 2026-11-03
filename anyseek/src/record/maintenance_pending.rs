//! Bounded admission retries, using the existing worker semaphore's FIFO wakeup.
//! Waiting never owns a partition or build/reader pin. Accepted jobs still do.
use super::*;
use std::{sync::LazyLock, time::Duration};

const MAX_PENDING: usize = 64;
const WAIT_LIMIT: Duration = Duration::from_secs(300);
const RECHECK: Duration = Duration::from_millis(100);
static PENDING: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(MAX_PENDING)));

impl RecordPartition {
    pub(super) fn defer_maintenance(&self) -> bool {
        let Ok(slot)=PENDING.clone().try_acquire_owned() else {
            log::warn!("record_maintenance event=retry_capacity_exhausted action=retry_checkpoint");
            return false;
        };
        // Every member is weak: an idle waiter must not keep an old incarnation
        // open, prevent its owner lock from being released, or reopen its path.
        let inner=Arc::downgrade(&self.inner);
        let writer=Arc::downgrade(&self.writer);
        let committed_queue=Arc::downgrade(&self.committed_queue);
        let admission=Arc::downgrade(&self.admission);
        let output=Arc::downgrade(&self.output);
        let indexes=Arc::downgrade(&self.indexes);
        let upgrade=move || Some(RecordPartition {
            inner:inner.upgrade()?,writer:writer.upgrade()?,
            committed_queue:committed_queue.upgrade()?,admission:admission.upgrade()?,
            output:output.upgrade()?,indexes:indexes.upgrade()?,
        });
        crate::resources::spawn_background(async move {
            let _slot=slot;
            let started=Instant::now();
            let mut attempts=0u64;
            log::info!("record_maintenance event=admission_deferred");
            let retry=async {
                loop {
                    // Keep this acquire future across liveness checks so a hot
                    // collection cannot repeatedly jump ahead of this waiter.
                    let worker=indexer::WORKERS.clone().acquire_owned();
                    tokio::pin!(worker);
                    let worker=loop {
                        tokio::select! {
                            result=&mut worker=>break result.map_err(|_|RecordError::Busy)?,
                            _=tokio::time::sleep(RECHECK)=>{
                                if upgrade().is_none(){return Ok::<_,RecordError>(());}
                            }
                        }
                    };
                    let Some(partition)=upgrade() else {return Ok(());};
                    let mut inner=partition.inner.lock().await;
                    if !inner.healthy || inner.maintenance_requested.is_none() {
                        inner.maintenance_scheduled=false;
                        inner.maintenance_pending=false;
                        return Ok(());
                    }
                    if inner.background.is_some() {
                        drop(inner);drop(partition);drop(worker);
                        tokio::time::sleep(RECHECK).await;
                        continue;
                    }
                    attempts+=1;
                    let job=crate::resources::lifecycle::automatic(|| if inner.maintenance_requested==Some(cache_merge::CAUSE) {
                        partition.start_cache_merge_with_worker(WAIT_LIMIT,worker)
                    }else{partition.start_automatic_with_worker(worker)});
                    let Some(job)=job else {
                        inner.maintenance_scheduled=false;
                        inner.maintenance_pending=false;
                        return Ok(());
                    };
                    match job {
                        Ok(job)=>{
                            inner.maintenance_pending=false;
                            inner.maintenance_scheduled=true;
                            log::info!("record_maintenance event=retry_started age_ms={} attempts={attempts}",started.elapsed().as_millis());
                            drop(job);
                            return Ok(());
                        }
                        Err(RecordError::Busy)=>{
                            log::debug!("record_maintenance event=admission_backoff age_ms={} attempts={attempts}",started.elapsed().as_millis());
                        }
                        Err(error)=>return Err(error),
                    }
                    drop(inner);
                    drop(partition);
                    tokio::time::sleep(RECHECK).await;
                }
            };
            let outcome = tokio::select! {
                result = tokio::time::timeout(WAIT_LIMIT,retry) => result,
                _ = crate::resources::lifecycle::stopping() => {
                    if let Some(partition)=upgrade() {
                        let mut inner=partition.inner.lock().await;
                        inner.maintenance_scheduled=false;
                        inner.maintenance_pending=false;
                    }
                    return;
                }
            };
            if !matches!(outcome,Ok(Ok(()))) {
                if let Some(partition)=upgrade() {
                    let mut inner=partition.inner.lock().await;
                    inner.maintenance_scheduled=false;
                    inner.maintenance_pending=false;
                }
                // Keep the intent; no unbounded automatic retry of execution
                // failures or permanently unavailable resource admission.
                log::warn!("record_maintenance event=retry_stopped age_ms={} attempts={attempts} action=retry_checkpoint",started.elapsed().as_millis());
            }
        });
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_stops_deferred_maintenance_without_releasing_an_active_owner() {
        let root=tempfile::tempdir().unwrap();
        let path=root.path().join("records");
        let p=RecordPartition::create(&path,crate::record::tests::config()).await.unwrap();
        let workers=indexer::WORKERS.clone().acquire_many_owned(2).await.unwrap();
        let lifecycle=Arc::new(crate::resources::Lifecycle::default());
        let activity=lifecycle.enter().unwrap();
        p.inner.lock().await.request_checkpoint("shutdown-test");
        activity.run(p.kick_maintenance()).await;
        assert!(p.inner.lock().await.maintenance_pending);
        lifecycle.stop();
        tokio::time::timeout(Duration::from_secs(5),lifecycle.drained()).await.unwrap();
        let inner=p.inner.lock().await;
        assert!(!inner.maintenance_pending);
        assert!(!inner.maintenance_scheduled);
        assert!(inner.maintenance_requested.is_some());
        drop(inner);
        drop(workers);
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        let reopened=RecordPartition::open(&path,crate::record::tests::config().max_journal_bytes).await.unwrap();
        drop(reopened);
    }

    #[tokio::test]
    async fn pending_admission_is_bounded_deduplicated_and_does_not_retain_incarnations() {
        let root=tempfile::tempdir().unwrap();
        let path=root.path().join("old");
        let p=RecordPartition::create(&path,crate::record::tests::config()).await.unwrap();
        let workers=indexer::WORKERS.clone().acquire_many_owned(2).await.unwrap();
        let slots=PENDING.clone().acquire_many_owned(MAX_PENDING as u32).await.unwrap();
        p.inner.lock().await.request_checkpoint("test_pending_limit");
        p.kick_maintenance().await;
        assert!(!p.inner.lock().await.maintenance_scheduled);
        assert!(p.inner.lock().await.maintenance_requested.is_some());
        drop(slots);
        p.kick_maintenance().await;
        for _ in 0..32 {p.kick_maintenance().await;}
        assert_eq!(PENDING.available_permits(),MAX_PENDING-1);
        let weak=Arc::downgrade(&p.inner);
        drop(p);
        tokio::time::timeout(Duration::from_secs(2),async {
            while weak.upgrade().is_some(){tokio::time::sleep(RECHECK).await;}
        }).await.expect("waiting must not retain the owner lock");
        // Removal and a new incarnation at the same location cannot be reached
        // by the old weak waiter, even after worker capacity becomes available.
        while !RecordPartition::reclaim_deleted_files(&path,64).await.unwrap() {}
        let replacement=RecordPartition::create(&path,crate::record::tests::config()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2),async {
            while PENDING.available_permits()!=MAX_PENDING {tokio::time::sleep(RECHECK).await;}
        }).await.unwrap();
        drop(workers);
        assert!(replacement.inner.lock().await.anchor.is_none());
        assert!(replacement.inner.lock().await.maintenance_requested.is_none());

        // Collection admission refusal is retryable too; it must not be logged
        // as an execution failure or consume a worker while backing off.
        let admission=replacement.admission.clone().acquire_many_owned(16).await.unwrap();
        replacement.inner.lock().await.request_checkpoint("test_admission");
        replacement.kick_maintenance().await;
        tokio::time::sleep(RECHECK*2).await;
        assert!(replacement.inner.lock().await.maintenance_pending);
        drop(admission);
        crate::record::tests::wait_maintenance_idle(&replacement).await;
        assert!(replacement.inner.lock().await.maintenance_requested.is_none());
    }
}
