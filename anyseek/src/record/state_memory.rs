//! State residency is owned independently of component/writer admission.
//! Logical JSON expansion is conservative; this is not an RSS/allocator limit.
use super::*;
use crate::storage::build_budget::{reserve_memory, MemoryLease};

pub(super) struct Charge {
    bytes: u64,
    lease: MemoryLease,
}
impl std::fmt::Debug for Charge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateCharge")
            .field("bytes", &self.bytes)
            .finish()
    }
}
impl State {
    pub(super) fn admit_bytes(&mut self, bytes: usize) -> Result<(), RecordError> {
        let needed = (bytes as u64)
            .checked_mul(32)
            .ok_or_else(|| storage("state residency estimate overflow"))?;
        if let Some(charge) = self.residency.as_mut().and_then(Arc::get_mut) {
            if needed > charge.bytes {
                charge.lease.grow(needed - charge.bytes).map_err(storage)?;
                charge.bytes = needed;
            }
        } else {
            self.residency = Some(Arc::new(Charge {
                bytes: needed,
                lease: reserve_memory(needed).map_err(storage)?,
            }));
        }
        Ok(())
    }
    pub(super) fn attach_residency(&mut self, bytes: u64, lease: MemoryLease) {
        self.residency = Some(Arc::new(Charge { bytes, lease }));
    }
    /// Admission must succeed before any deep copy. Clones do not share a lease
    /// for separately allocated maps, history documents or generation chunks.
    pub(super) fn try_clone(&self) -> Result<Self, RecordError> {
        self.try_clone_with_growth(0)
    }
    pub(super) fn try_clone_with_growth(&self, growth: usize) -> Result<Self, RecordError> {
        let bytes = checkpoint_codec::json_length(self)?
            .checked_add(growth)
            .and_then(|n| (n as u64).checked_mul(32))
            .ok_or_else(|| storage("state clone estimate overflow"))?;
        let residency = Some(Arc::new(Charge {
            bytes,
            lease: reserve_memory(bytes).map_err(storage)?,
        }));
        Ok(Self {
            lineage: self.lineage.clone(),
            feed: self.feed.clone(),
            sealed_for: self.sealed_for.clone(),
            takeover_proof: self.takeover_proof.clone(),
            independent_snapshot: self.independent_snapshot,
            seq: self.seq,
            epoch_watermark: self.epoch_watermark,
            time_watermark_ms: self.time_watermark_ms,
            expirations: self.expirations.clone(),
            lease_expirations: self.lease_expirations.clone(),
            private_cleanup_pending: self.private_cleanup_pending,
            evidence_quarantined: self.evidence_quarantined,
            records: self.records.clone(),
            source_revisions: self.source_revisions.clone(),
            generations: self.generations.clone(),
            operations: self.operations.clone(),
            operation_lookup: self.operation_lookup.clone(),
            residency,
        })
    }
}
