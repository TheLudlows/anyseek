//! Incrementally apply a durable decision to all components of the read view.
//!
//! The context contains only a mutable read view, never publication authority.
//! Live callers hold the partition guard and fence output; private background
//! replay callers discard their candidate on error. Neither context can append
//! WAL, publish a certificate, or reopen output by itself.
use super::*;

impl replay::Context<'_> {
    pub(super) async fn publish(
        &mut self,
        applied: &Applied,
        snapshot_bytes: usize,
    ) -> Result<(), RecordError> {
        let prepared=if applied.request.generation.is_none(){Some(state_memory::Prepared::new(&*self.state,applied)?)}else{None};
        self.publish_prepared(applied,snapshot_bytes,prepared,None).await
    }
    pub(super) async fn publish_prepared(&mut self,applied:&Applied,snapshot_bytes:usize,prepared:Option<state_memory::Prepared>,candidate:Option<State>)->Result<(),RecordError>{
        if applied.request.generation.is_some() { return self.publish_generation(applied, snapshot_bytes,candidate).await; }
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

impl replay::Context<'_> {
    async fn publish_generation(&mut self, applied: &Applied, snapshot_bytes: usize, candidate: Option<State>) -> Result<(), RecordError> {
        let growth=checkpoint_codec::json_length(applied)?.checked_mul(3).ok_or_else(|| storage("generation state growth overflow"))?;
        let next = if let Some(candidate)=candidate { candidate } else {
            let mut next=self.state.try_clone_with_growth(growth)?;
            apply(&mut next,applied).map_err(|_|RecordError::DurablePending(applied.receipt.seq))?;next
        };
        if matches!(applied.request.generation, Some(GenerationCommand::Publish { .. })) {
            let visible = evidence::visibility(&next.records).map_err(|_| RecordError::DurablePending(applied.receipt.seq))?;
            let retired: Vec<_> = self.locations.keys().filter(|id| visible.get(*id) != Some(&true)).cloned().collect();
            for id in retired {
                if let Some(ordinal) = self.locations.remove(&id) {
                    self.index.retire(ordinal).await.map_err(|error| { log::error!("Record generation publication failed at sequence {}: {:?}", applied.receipt.seq, error); RecordError::DurablePending(applied.receipt.seq) })?;
                }
            }
            for (id, record) in next.records.iter().filter(|(id, _)| !self.state.records.contains_key(*id)) {
                if visible.get(id) != Some(&true) { return Err(RecordError::DurablePending(applied.receipt.seq)); }
                let ordinal=self.index.append(id.clone(),record.version,applied.receipt.seq,temporal::indexed(record.document.as_ref().unwrap(), &record.temporal, record.ingested_at_ms)).await.map_err(|error| { log::error!("Record generation publication failed at sequence {}: {:?}", applied.receipt.seq, error); RecordError::DurablePending(applied.receipt.seq) })?;
                if self.ordinals.len()<=ordinal {self.ordinals.resize(ordinal+1,String::new());}
                self.ordinals[ordinal]=id.clone();self.locations.insert(id.clone(),ordinal);
                #[cfg(test)]
                if self.fail_publication_stage == 1 { return Err(RecordError::DurablePending(applied.receipt.seq)); }
            }
        }
        *self.state = next; *self.snapshot_bytes = snapshot_bytes;
        #[cfg(test)]
        assert_eq!(snapshot_bytes, encode_entry(&Entry::Checkpoint { config:self.config.clone(), state:self.state.try_clone()? }).unwrap().len());
        Ok(())
    }
}

impl Inner {
    pub(super) async fn publish(&mut self,applied:&Applied,bytes:usize)->Result<(),RecordError>{
        self.replay_context().publish(applied,bytes).await
    }
    pub(super) async fn publish_prepared(&mut self,applied:&Applied,bytes:usize,prepared:Option<state_memory::Prepared>,candidate:Option<State>)->Result<(),RecordError>{
        self.replay_context().publish_prepared(applied,bytes,prepared,candidate).await
    }
}
