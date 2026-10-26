//! State residency is owned independently of component/writer admission.
//! Logical JSON expansion is conservative; this is not an RSS/allocator limit.
use super::*;
use crate::storage::build_budget::{reserve_memory, MemoryLease};

/// An immutable value and its already admitted residency have one lifetime.
/// Serialization exposes only the value, preserving the authenticated format.
#[derive(Debug)]
pub(super) struct Shared<T>(Arc<ValueOwner<T>>);
struct ValueOwner<T> { value: T, _lease: MemoryLease }
impl<T: std::fmt::Debug> std::fmt::Debug for ValueOwner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueOwner").field("value", &self.value).field("bytes", &self._lease.bytes()).finish()
    }
}
impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}
impl<T> std::ops::Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.0.value }
}
impl<T: Serialize> Serialize for Shared<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.value.serialize(serializer)
    }
}
impl<T: Serialize> Shared<T> {
    pub(super) fn from_admitted(value: T, admitted: &mut MemoryLease) -> Result<Self, RecordError> {
        let bytes = (checkpoint_codec::json_length(&value)? as u64).checked_mul(32)
            .ok_or_else(|| storage("shared state value estimate overflow"))?;
        let lease = admitted.split_off(bytes).map_err(storage)?;
        Ok(Self(Arc::new(ValueOwner { value, _lease: lease })))
    }
    /// Reserve transfer precedes the actual deep clone, including on rejection.
    pub(super) fn copy_from_admitted(value: &T, admitted: &mut MemoryLease) -> Result<Self, RecordError>
    where T: Clone {
        let bytes = (checkpoint_codec::json_length(value)? as u64).checked_mul(32)
            .ok_or_else(|| storage("shared state value estimate overflow"))?;
        let lease = admitted.split_off(bytes).map_err(storage)?;
        Ok(Self(Arc::new(ValueOwner { value: value.clone(), _lease: lease })))
    }
    // Test-only non-growing mutation probe. Production values are immutable;
    // unique Arc ownership alone never authorizes an allocation or growth.
    #[cfg(test)]
    fn unique_mut(&mut self) -> Result<&mut T, RecordError> {
        Arc::get_mut(&mut self.0).map(|owned| &mut owned.value)
            .ok_or_else(|| storage("shared state mutation requires admitted private copy"))
    }
}

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

#[cfg(test)]
mod ownership_tests {
    use super::*;
    #[test]
    fn immutable_value_preserves_wire_and_outlives_live_owner() {
        let value = serde_json::json!({"body":"繁體 alpha", "history":[1,2,3]});
        let wire = serde_json::to_vec(&value).unwrap();
        let mut admitted = reserve_memory(wire.len() as u64 * 32).unwrap();
        let mut live = Shared::from_admitted(value, &mut admitted).unwrap();
        assert_eq!(admitted.bytes(), 0);
        let old = live.clone();
        assert!(live.unique_mut().is_err(), "must not allocate implicit COW");
        assert_eq!(serde_json::to_vec(&live).unwrap(), wire);
        let owner = Arc::downgrade(&live.0);
        drop(live); assert!(owner.upgrade().is_some());
        assert_eq!(serde_json::to_vec(&old).unwrap(), wire);
        drop(old); assert!(owner.upgrade().is_none());
    }
    #[test]
    fn rejected_cow_does_not_clone_or_change_snapshot() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COPIES: AtomicUsize = AtomicUsize::new(0);
        #[derive(Serialize, Debug)]
        struct Tracked { body: String }
        impl Clone for Tracked {
            fn clone(&self) -> Self { COPIES.fetch_add(1, Ordering::SeqCst); Self { body: self.body.clone() } }
        }
        let value = Tracked { body: "retained original".into() };
        let size = checkpoint_codec::json_length(&value).unwrap() as u64 * 32;
        let mut first = reserve_memory(size).unwrap();
        let old = Shared::from_admitted(value, &mut first).unwrap();
        let wire = serde_json::to_vec(&old).unwrap();
        let mut short = reserve_memory(size - 1).unwrap();
        assert!(Shared::copy_from_admitted(&*old, &mut short).is_err());
        assert_eq!(COPIES.load(Ordering::SeqCst), 0, "rejection precedes allocation");
        let mut gross = reserve_memory(size).unwrap();
        let mut next = Shared::copy_from_admitted(&*old, &mut gross).unwrap();
        assert_eq!(COPIES.load(Ordering::SeqCst), 1);
        next.unique_mut().unwrap().body.replace_range(.., "changed content!");
        assert_eq!(serde_json::to_vec(&old).unwrap(), wire);
        assert_ne!(serde_json::to_vec(&next).unwrap(), wire);
    }
    #[test]
    fn value_transfer_rejects_before_changing_admitted_balance() {
        let value = serde_json::json!({"body":"same sized replacement"});
        let bytes = checkpoint_codec::json_length(&value).unwrap() as u64 * 32;
        let mut admitted = reserve_memory(bytes - 1).unwrap();
        assert!(Shared::from_admitted(value, &mut admitted).is_err());
        assert_eq!(admitted.bytes(), bytes - 1);
    }
}
