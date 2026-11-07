//! Incrementally apply a durable decision to all components of the read view.
//!
//! The context contains only a mutable read view, never publication authority.
//! Live callers hold the partition guard and fence output; private background
//! replay callers discard their candidate on error. Neither context can append
//! WAL, publish a certificate, or reopen output by itself.
use super::*;

impl replay::Context<'_> {
    pub(super) async fn publish_prepared(&mut self,applied:&Applied,snapshot_bytes:usize,prepared:Option<state_memory::Prepared>)->Result<(),RecordError>{
        self.publish_prepared_with_native(applied,snapshot_bytes,prepared,true).await
    }
    // Cold recovery has no readers and publishes the bounded native suffix at
    // rotation boundaries and before returning the recovered partition.
    pub(super) async fn publish_prepared_with_native(&mut self,applied:&Applied,snapshot_bytes:usize,prepared:Option<state_memory::Prepared>,publish_native:bool)->Result<(),RecordError>{
        let receipt = &applied.receipt;
        // Prepared owns the complete canonical document. Borrow it instead of
        // making an additional enrichment copy during index publication.
        let mut prepared = prepared.ok_or_else(|| {
            log::error!("Record prepared ownership missing at sequence {}", receipt.seq);
            RecordError::DurablePending(receipt.seq)
        })?;
        let document = prepared.record.document.as_ref();
        let old_location = self.locations.get(&receipt.record_id).copied();
        if let Some(document) = document {
            // Admit the full temporal/index input before cloning it. Five
            // bounded synthetic time fields add at most 8 KiB at the same 32x
            // logical expansion. Queue projections and analysis own other leases.
            let (_input, _scope) = (|| -> Result<_, RecordError> {
                let bytes = build_budget::document_estimate(document, &[])?
                    .checked_add(8192).ok_or_else(|| storage("index input estimate overflow"))?;
                let input = crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
                let scope = crate::resources::Scope::current()
                    .map(|scope| scope.bytes(bytes)).transpose().map_err(storage)?;
                Ok((input, scope))
            })().map_err(|error| {
                log::error!("Record input admission failed at sequence {}: {:?}", receipt.seq, error);
                RecordError::DurablePending(receipt.seq)
            })?;
            let id=self.index.append(receipt.record_id.clone(),receipt.version,receipt.seq,temporal::indexed(document, &temporal::retained(self.state.records.get(&receipt.record_id), &applied.request, applied.observed_time_ms).map_err(|_| RecordError::DurablePending(receipt.seq))?, if applied.request.vector_patch.is_some() { self.state.records[&receipt.record_id].ingested_at_ms } else { applied.observed_time_ms }),prepared.lexical.take(),prepared.component.take(),publish_native).await.map_err(|error| { log::error!("Record component publication failed at sequence {}: {:?}", receipt.seq, error); RecordError::DurablePending(receipt.seq) })?;
            #[cfg(test)]
            if self.fail_publication_stage == 1 {
                return Err(RecordError::DurablePending(receipt.seq));
            }
            if self.ordinals.len()<=id {self.ordinals.resize(id+1,String::new());}
            self.ordinals[id]=receipt.record_id.clone();
            self.locations.insert(receipt.record_id.clone(), id);
        } else {
            self.locations.remove(&receipt.record_id);
        }
        if let Some(old_id) = old_location {
            self.index.retire(old_id).await.map_err(|error| { log::error!("Record component publication failed at sequence {}: {:?}", receipt.seq, error); RecordError::DurablePending(receipt.seq) })?;
        }
        #[cfg(test)]
        if self.fail_publication_stage == 2 {
            return Err(RecordError::DurablePending(receipt.seq));
        }
        apply_prepared(&mut *self.state, applied, Some(prepared)).map_err(|_| RecordError::DurablePending(receipt.seq))?;
        *self.snapshot_bytes = snapshot_bytes;
        // Still serialize every Record/operation independently of incremental
        // snapshot_after arithmetic. Counting needs neither a clone nor decode.
        #[cfg(test)]
        assert_eq!(*self.snapshot_bytes,checkpoint_codec::full_length(self.config,self.state)?);
        Ok(())
    }
}

impl Inner {
    pub(super) async fn publish_prepared(&mut self,applied:&Applied,bytes:usize,prepared:Option<state_memory::Prepared>)->Result<(),RecordError>{
        self.replay_context().publish_prepared(applied,bytes,prepared).await
    }
}
