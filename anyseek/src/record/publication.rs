//! Incrementally apply a durable decision to all components of the read view.
//!
//! The context contains only a mutable read view, never publication authority.
//! Live callers hold the partition guard and fence output; private background
//! replay callers discard their candidate on error. Neither context can append
//! WAL, publish a certificate, or reopen output by itself.
use super::*;

impl replay::Context<'_> {
    pub(super) async fn publish_prepared(&mut self,applied:&Applied,snapshot_bytes:usize,prepared:Option<state_memory::Prepared>)->Result<(),RecordError>{
        let receipt = &applied.receipt;
        let document = enrichment::document(self.state.records.get(&receipt.record_id), &applied.request)?;
        let hiding = applied.request.vector_patch.is_none() && (self
                .state
                .records
                .get(&receipt.record_id)
                .is_some_and(|r| r.system.is_some()) || self.graph_cache.references(&receipt.record_id));
        let mut affected = if hiding {
            evidence::descendants(&self.state.records, &receipt.record_id)
                .map_err(|_| RecordError::DurablePending(receipt.seq))?
        } else {
            std::collections::BTreeSet::new()
        };
        // Updating the source replaces its own ordinal but keeps the new source visible.
        if document.is_some() { affected.remove(&receipt.record_id); }
        let old_location = self.locations.get(&receipt.record_id).copied();
        if let Some(document) = &document {
            let id=self.index.append(receipt.record_id.clone(),receipt.version,receipt.seq,temporal::indexed(document, &temporal::retained(self.state.records.get(&receipt.record_id), &applied.request, applied.observed_time_ms).map_err(|_| RecordError::DurablePending(receipt.seq))?, if applied.request.vector_patch.is_some() { self.state.records[&receipt.record_id].ingested_at_ms } else { applied.observed_time_ms })).await.map_err(|error| { log::error!("Record component publication failed at sequence {}: {:?}", receipt.seq, error); RecordError::DurablePending(receipt.seq) })?;
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
        for id in affected {
            if let Some(ordinal) = self.locations.remove(&id) {
                self.index.retire(ordinal).await.map_err(|error| { log::error!("Record component publication failed at sequence {}: {:?}", receipt.seq, error); RecordError::DurablePending(receipt.seq) })?;
            }
        }
        #[cfg(test)]
        if self.fail_publication_stage == 2 {
            return Err(RecordError::DurablePending(receipt.seq));
        }
        apply_prepared(&mut *self.state, applied, prepared).map_err(|_| RecordError::DurablePending(receipt.seq))?;
        self.graph_cache.insert(&self.state.records[&receipt.record_id]).map_err(|_| RecordError::DurablePending(receipt.seq))?;
        *self.snapshot_bytes = snapshot_bytes;
        #[cfg(test)]
        assert_eq!(
            snapshot_bytes,
            encode_entry(&Entry::Checkpoint {
                config: self.config.clone(),
                state: self.state.try_clone()?
            })
            .unwrap()
            .len()
        );
        Ok(())
    }
}

impl Inner {
    pub(super) async fn publish_prepared(&mut self,applied:&Applied,bytes:usize,prepared:Option<state_memory::Prepared>)->Result<(),RecordError>{
        self.replay_context().publish_prepared(applied,bytes,prepared).await
    }
}
