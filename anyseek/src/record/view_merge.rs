//! Derived component replacement. Record authority and its WAL are unchanged.
use super::*;
use std::collections::BTreeSet;

pub(crate) struct MergeSources {
    pub components: Vec<crate::component::VectorSegmentArc>,
    pub natives: Vec<Arc<lexical::runtime::Runtime>>,
    pub keys: BTreeSet<String>,
    rows: BTreeMap<String, MergeRow>,
    _memory: crate::storage::build_budget::MemoryLease,
}
struct MergeRow {
    component: crate::component::VectorSegmentArc,
    local: usize,
    version: u64,
    fingerprint: [u8; 32],
    token: Arc<crate::active::LiveToken>,
}
impl RetrievalView {
    /// Capture at most two frozen components and a bounded set of live identities.
    /// Their native runtimes pin source files until the owned merge task ends.
    pub(in crate::record) async fn capture_merge(&self, max_rows: usize) -> Result<Option<MergeSources>, RecordError> {
        let root = self.query.read().await;
        if root.segments.len() < 3 || root.record_lexical.len() != root.segments.len() { return Ok(None); }
        let tail = self.tail.read().await.segments[0].clone();
        if !Arc::ptr_eq(&root.segments[0], &tail) { return Err(storage("merge mutable tail topology mismatch")); }
        let mut choices = Vec::new();
        for (slot, component) in root.segments.iter().enumerate().skip(1) {
            let shard = component.read().await;
            if shard.failed { return Err(RecordError::RecoveryRequired); }
            let active = shard.active.as_ref().ok_or_else(|| storage("merge statistics missing"))?;
            if active.keys.len() <= max_rows && active.retired.is_some() {
                choices.push((active.keys.len(), slot));
            }
        }
        choices.sort_unstable();
        if choices.len() < 2 || choices[0].0.checked_add(choices[1].0).is_none_or(|rows| rows > max_rows) {
            return Ok(None);
        }
        let slots = [choices[0].1, choices[1].1];
        let mut bytes = 4096u64;
        for slot in slots {
            let shard = root.segments[slot].read().await;
            for key in &shard.active.as_ref().unwrap().keys {
                bytes = bytes.checked_add(key.len() as u64 * 8 + 512).ok_or(RecordError::Busy)?;
            }
        }
        let memory = crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
        let mut keys = BTreeSet::new();
        let mut rows = BTreeMap::new();
        for slot in slots {
            let component = &root.segments[slot];
            let shard = component.read().await;
            let active = shard.active.as_ref().unwrap();
            for (local, key) in active.keys.iter().enumerate().filter(|(i, _)| active.is_live(*i)) {
                if !keys.insert(key.clone()) { return Err(storage("duplicate live merge identity")); }
                rows.insert(key.clone(), MergeRow {
                    component: component.clone(), local,
                    version: active.revisions[local].0,
                    fingerprint: active.fingerprints[local],
                    token: active.retired.as_ref().unwrap().token(local).map_err(storage)?,
                });
            }
        }
        Ok(Some(MergeSources {
            components: slots.iter().map(|&slot| root.segments[slot].clone()).collect(),
            natives: slots.iter().map(|&slot| root.record_lexical[slot].clone()).collect(),
            keys, rows, _memory: memory,
        }))
    }

    /// Called under the Record guard. All fallible preparation precedes the
    /// topology swap. The mutable tail and its queued local ordinals are kept.
    pub(in crate::record) async fn install_merge(
        &mut self, sources: &MergeSources, merged: VectorIndexArc,
        ordinals: &mut Vec<String>, locations: &mut BTreeMap<String, usize>,
        max_address_slots: usize,
    ) -> Result<bool, RecordError> {
        if Arc::ptr_eq(&self.query, &merged) || Arc::ptr_eq(&self.tail, &merged) {
            return Err(storage("merge replacement aliases live root"));
        }
        let mut root = self.query.write().await;
        let old_count = root.segments.len();
        let slots = sources.components.iter().map(|source|
            root.segments.iter().position(|current| Arc::ptr_eq(source, current)))
            .collect::<Option<Vec<_>>>();
        let Some(mut slots) = slots else { return Ok(false); };
        slots.sort_unstable(); slots.dedup();
        if slots.len() != 2 || slots[0] == 0 { return Ok(false); }
        let replacement = merged.read().await;
        if replacement.segments.len() != 1 || replacement.record_lexical.len() != 1
            || serde_json::to_value(&replacement.schema_map).map_err(storage)? != serde_json::to_value(&root.schema_map).map_err(storage)? {
            return Err(storage("invalid merge replacement topology/schema"));
        }
        let candidate = replacement.segments[0].clone();
        if root.segments.iter().any(|source| Arc::ptr_eq(source, &candidate)) { return Err(storage("merge replacement aliases source")); }
        let native = replacement.record_lexical[0].clone();
        let mut shard = candidate.write().await;
        if shard.failed { return Err(RecordError::RecoveryRequired); }
        let active = shard.active.as_mut().ok_or_else(|| storage("merge replacement statistics missing"))?;
        if active.keys.len() != sources.keys.len() {
            return Err(storage("merge replacement identity population mismatch"));
        }
        let address_bytes = (ordinals.len() as u64).checked_mul(128)
            .and_then(|bytes| bytes.checked_add(active.keys.len() as u64 * old_count as u64 * 128))
            .and_then(|bytes| bytes.checked_add(locations.len() as u64 * 256))
            .ok_or(RecordError::Busy)?;
        let address_bytes = ordinals.iter().chain(&active.keys).try_fold(address_bytes, |bytes, key|
            bytes.checked_add(key.len() as u64 * 8).ok_or(RecordError::Busy))?;
        let address_memory = crate::storage::build_budget::reserve_memory(address_bytes).map_err(storage)?;
        let address_scope = crate::resources::Scope::current().map(|scope| scope.bytes(address_bytes))
            .transpose().map_err(storage)?;
        let mask = active.retired.get_or_insert_with(Default::default).clone();
        let mut replacement_locations = BTreeMap::new();
        for local in 0..active.keys.len() {
            let key = &active.keys[local];
            let source = sources.rows.get(key).ok_or_else(|| storage("merge replacement has unexpected identity"))?;
            if active.revisions[local].0 != source.version || active.fingerprints[local] != source.fingerprint {
                return Err(storage("merge replacement changed source value"));
            }
            mask.share(local, source.token.clone()).map_err(storage)?;
            let current = locations.get(key).is_some_and(|ordinal|
                ordinal / old_count == source.local && Arc::ptr_eq(&root.segments[ordinal % old_count], &source.component));
            if current && source.token.is_live() {
                if replacement_locations.insert(key.clone(), local).is_some() { return Err(storage("duplicate replacement identity")); }
            } else {
                active.retire(local);
            }
        }
        let replacement_keys = active.keys.clone();
        // Rebuild delete metadata as well as live statistics; vector candidate
        // selection must exclude retired rows before its local Top-K.
        shard.delete_hashset = (0..replacement_keys.len()).filter(|id| !shard.active.as_ref().unwrap().is_live(*id)).collect();
        let next_count = old_count - 1;
        let mut next_shards = Vec::with_capacity(next_count);
        let mut next_native = Vec::with_capacity(next_count);
        let mut remap_slots = vec![None; old_count];
        let mut replacement_slot = 0;
        for slot in 0..old_count {
            if slot == slots[0] {
                replacement_slot = next_shards.len();
                next_shards.push(candidate.clone()); next_native.push(native.clone());
            }
            if slots.contains(&slot) { continue; }
            remap_slots[slot] = Some(next_shards.len());
            next_shards.push(root.segments[slot].clone()); next_native.push(root.record_lexical[slot].clone());
        }
        let remap = |local: usize, slot: usize| local.checked_mul(next_count).and_then(|n| n.checked_add(slot))
            .filter(|n| *n < max_address_slots).ok_or_else(|| storage("merge address budget exceeded"));
        let mut mapped = Vec::new();
        let mut put = |address: usize, key: &String| -> Result<(), RecordError> {
            if mapped.len() <= address {
                mapped.try_reserve(address + 1 - mapped.len()).map_err(storage)?;
                mapped.resize(address + 1, String::new());
            }
            mapped[address] = key.clone(); Ok(())
        };
        for (address, key) in ordinals.iter().enumerate().filter(|(_, key)| !key.is_empty()) {
            if let Some(slot) = remap_slots[address % old_count] { put(remap(address / old_count, slot)?, key)?; }
        }
        for (local, key) in replacement_keys.iter().enumerate() { put(remap(local, replacement_slot)?, key)?; }
        let mut next_locations = BTreeMap::new();
        for (key, &address) in locations.iter() {
            let next = if let Some(slot) = remap_slots[address % old_count] {
                remap(address / old_count, slot)?
            } else {
                remap(*replacement_locations.get(key).ok_or_else(|| storage("merge lost current live identity"))?, replacement_slot)?
            };
            next_locations.insert(key.clone(), next);
        }
        drop(shard);
        let mut masks = Vec::with_capacity(next_count);
        for component in &next_shards {
            let shard = component.read().await;
            masks.push(shard.active.as_ref().and_then(|active| active.retired.clone()).ok_or_else(|| storage("merge retirement mask missing"))?);
        }
        // Source packages are now replaceable derived caches. Only references
        // actually removed from the topology are dropped; original readers keep
        // their native package pins, and authority-selected packages remain roots.
        let removed = sources.natives.iter().map(|native| native.package_path()).collect::<Vec<_>>();
        self.pending_seals.retain(|package| !removed.iter().any(|path|
            path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name == format!("segment-{}", package.id))));
        root.segments = next_shards; root.record_lexical = next_native;
        self.retired = masks;
        *ordinals = mapped; *locations = next_locations;
        self.detached_cache = true;
        self.merge_address_memory = Some(address_memory);
        self.merge_address_scope = address_scope;
        Ok(true)
    }

    /// A detached view seals only its disposable tail, without inventing a new
    /// Record certificate or retaining a second authoritative file inventory.
    pub(in crate::record) async fn seal_cache_into_successor(
        &mut self, new_tail: VectorIndexArc, ordinals: &mut Vec<String>,
        locations: &mut BTreeMap<String, usize>, budget: RotationBudget,
    ) -> Result<(), RecordError> {
        self.drain_indexes().await?;
        self.tail.commit().await.map_err(storage)?;
        self.tail.read().await.record_lexical[0].seal().await?;
        self.rotate_tail(new_tail, ordinals, locations, budget).await?;
        self.detached_cache = true;
        Ok(())
    }
}

#[cfg(test)]
#[path = "view_merge_tests.rs"]
mod tests;
