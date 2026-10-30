//! Two-phase disposal of a deleted incarnation. Receipts retain their existing
//! epoch fence; physical cleanup never replays a catalog command.
use super::*;

impl Catalog {
    /// Round-robin selection without copying every collection configuration.
    pub async fn next_deleted_collection(&self, after: u64) -> Result<Option<CollectionEntry>, CatalogError> {
        let inner = self.inner.lock().await;
        if !inner.is_healthy() { return Err(CatalogError::RecoveryRequired); }
        Ok(inner.state.collections.values().filter(|entry| entry.deleted)
            .min_by_key(|entry| (entry.generation <= after, entry.generation)).cloned())
    }

    /// Reclaim one exact deleted incarnation under its incarnation and snapshot
    /// barriers. The caller owns the service root and performs bounded, retryable
    /// cleanup; `true` means all physical resources have been released. Keepalive
    /// guards remain owned through the final metadata commit, even on cancellation.
    pub async fn reclaim_deleted<F, Fut, Owner>(
        &self, entry: CollectionEntry, keepalive: Owner, cleanup: F,
    ) -> Result<bool, CatalogError>
    where
        F: FnOnce(CollectionEntry) -> Fut + Send + 'static,
        Fut: Future<Output = Result<bool, String>> + Send + 'static,
        Owner: Send + 'static,
    {
        let admission = self.admission.clone().try_acquire_owned().map_err(|_| CatalogError::Busy)?;
        let owner = self.inner.clone();
        let execution = self.execution.clone();
        crate::resources::spawn(async move {
            let _admission = admission;
            let _keepalive = keepalive;
            let id = entry.scope();
            let _snapshot = execution.global.clone().read_owned().await;
            let _incarnation = execution.collection(&id)?.write_owned().await;
            let mut inner = owner.lock().await;
            if !inner.is_healthy() { return Err(CatalogError::RecoveryRequired); }
            let Some(current) = inner.state.collections.get(&id) else { return Ok(true); };
            if !current.deleted || current.active { return Err(CatalogError::Conflict("collection is not deleted".into())); }
            if inner.state.backups.iter().any(|(id, backup)|
                !inner.state.retired_backups.contains(id) && backup.tenant_id == entry.tenant_id
                    && backup.collection_id == entry.collection_id && backup.generation == entry.generation)
            {
                log::debug!("collection reclamation blocked by backup: {id}");
                return Ok(false);
            }
            if !current.reclaiming {
                let mut next = inner.state.clone();
                next.collections.get_mut(&id).unwrap().reclaiming = true;
                inner.replace_reclamation(next)?;
            }
            let entry = inner.state.collections[&id].clone();
            drop(inner);
            if !cleanup(entry).await.map_err(storage)? { return Ok(false); }
            let mut inner = owner.lock().await;
            if !inner.is_healthy() { return Err(CatalogError::RecoveryRequired); }
            let mut next = inner.state.clone();
            next.collections.remove(&id);
            inner.replace_reclamation(next)?;
            log::info!("collection reclamation complete: {id}");
            Ok(true)
        }).await.map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?
    }
}

impl Inner {
    fn replace_reclamation(&mut self, mut next: State) -> Result<(), CatalogError> {
        next.epoch_watermark = epoch().max(next.epoch_watermark);
        next.operations.retain(|op| op.request.operation_id.epoch.saturating_add(1) >= next.epoch_watermark);
        sqlite::validate_state(&next)?;
        self.healthy = false;
        self.store.persist(&self.state, &next)?;
        self.state = next;
        self.healthy = true;
        Ok(())
    }
}
