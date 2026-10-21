//! Incrementally apply a durable decision to all components of the read view.
//!
//! Called only by the owned executor with the exclusive partition guard and
//! healthy=false. A failed component leaves the partition fenced. The executor
//! alone may reopen visibility after every component and authority agree.
use super::*;

impl Inner {
    pub(super) async fn publish(
        &mut self,
        applied: &Applied,
        snapshot_bytes: usize,
    ) -> Result<(), RecordError> {
        if applied.request.generation.is_some() { return self.publish_generation(applied, snapshot_bytes).await; }
        let receipt = &applied.receipt;
        let document = enrichment::document(self.state.records.get(&receipt.record_id), &applied.request)?;
        let hiding = applied.request.vector_patch.is_none() && self
                .state
                .records
                .get(&receipt.record_id)
                .is_some_and(|r| r.system.is_some());
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
            let id = self.ordinals.len();
            let shard = self.index.read().await.shard_vec[0].clone();
            shard
                .write()
                .await
                .active
                .as_mut()
                .unwrap()
                .keys
                .push(receipt.record_id.clone());
            self.index
                .index_documents(vec![document.clone()])
                .await
                .map_err(|_| RecordError::DurablePending(receipt.seq))?;
            #[cfg(test)]
            if self.fail_publication_stage == 1 {
                return Err(RecordError::DurablePending(receipt.seq));
            }
            self.ordinals.push(receipt.record_id.clone());
            self.locations.insert(receipt.record_id.clone(), id);
        } else {
            self.locations.remove(&receipt.record_id);
        }
        if let Some(old_id) = old_location {
            let shard = self.index.read().await.shard_vec[0].clone();
            let mut shard = shard.write().await;
            shard.delete_hashset.insert(old_id);
            shard.active.as_mut().unwrap().retire(old_id);
            shard.invalidate_frequentword_results();
        }
        for id in affected {
            if let Some(ordinal) = self.locations.remove(&id) {
                let shard = self.index.read().await.shard_vec[0].clone();
                let mut shard = shard.write().await;
                shard.delete_hashset.insert(ordinal);
                shard.active.as_mut().unwrap().retire(ordinal);
                shard.invalidate_frequentword_results();
            }
        }
        #[cfg(test)]
        if self.fail_publication_stage == 2 {
            return Err(RecordError::DurablePending(receipt.seq));
        }
        apply(&mut self.state, applied).map_err(|_| RecordError::DurablePending(receipt.seq))?;
        self.snapshot_bytes = snapshot_bytes;
        #[cfg(test)]
        assert_eq!(
            snapshot_bytes,
            encode_entry(&Entry::Checkpoint {
                config: self.config.clone(),
                state: self.state.clone()
            })
            .unwrap()
            .len()
        );
        Ok(())
    }
}

impl Inner {
    async fn publish_generation(&mut self, applied: &Applied, snapshot_bytes: usize) -> Result<(), RecordError> {
        let mut next = self.state.clone();
        apply(&mut next, applied).map_err(|_| RecordError::DurablePending(applied.receipt.seq))?;
        if matches!(applied.request.generation, Some(GenerationCommand::Publish { .. })) {
            let visible = evidence::visibility(&next.records).map_err(|_| RecordError::DurablePending(applied.receipt.seq))?;
            let retired: Vec<_> = self.locations.keys().filter(|id| visible.get(*id) != Some(&true)).cloned().collect();
            for id in retired {
                if let Some(ordinal) = self.locations.remove(&id) {
                    let shard = self.index.read().await.shard_vec[0].clone(); let mut shard = shard.write().await;
                    shard.delete_hashset.insert(ordinal); shard.active.as_mut().unwrap().retire(ordinal); shard.invalidate_frequentword_results();
                }
            }
            for (id, record) in next.records.iter().filter(|(id, _)| !self.state.records.contains_key(*id)) {
                if visible.get(id) != Some(&true) { return Err(RecordError::DurablePending(applied.receipt.seq)); }
                let ordinal = self.ordinals.len();
                self.index.read().await.shard_vec[0].write().await.active.as_mut().unwrap().keys.push(id.clone());
                self.index.index_documents(vec![record.document.clone().unwrap()]).await.map_err(|_| RecordError::DurablePending(applied.receipt.seq))?;
                self.ordinals.push(id.clone()); self.locations.insert(id.clone(), ordinal);
                #[cfg(test)]
                if self.fail_publication_stage == 1 { return Err(RecordError::DurablePending(applied.receipt.seq)); }
            }
        }
        self.state = next; self.snapshot_bytes = snapshot_bytes;
        #[cfg(test)]
        assert_eq!(snapshot_bytes, encode_entry(&Entry::Checkpoint { config:self.config.clone(), state:self.state.clone() }).unwrap().len());
        Ok(())
    }
}
