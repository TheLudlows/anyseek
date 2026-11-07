//! One owner for the process and collection limits of an explicit allocation.
use super::{Busy, Lease, Scope, Usage};
use crate::storage::build_budget::{MemoryLease, reserve_memory};

pub(crate) struct MemoryReservation {
    memory: MemoryLease,
    scoped: Option<Lease>,
}

#[derive(Debug)]
pub(crate) enum MemoryError {
    Process(std::io::Error),
    Scoped(Busy),
}
impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Process(error) => error.fmt(f),
            Self::Scoped(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for MemoryError {}

impl MemoryReservation {
    /// The caller selects the allocation's lifetime/class explicitly. On either
    /// refusal no reservation escapes; allocation happens only after success.
    pub(crate) fn new(bytes: u64, scope: Option<&Scope>) -> Result<Self, MemoryError> {
        let memory = reserve_memory(bytes).map_err(MemoryError::Process)?;
        let scoped = scope.map(|s| s.bytes(bytes)).transpose().map_err(MemoryError::Scoped)?;
        Ok(Self { memory, scoped })
    }

    pub(crate) fn bytes(&self) -> u64 { self.memory.bytes() }

    /// Reserve the increment in both ledgers before changing this owner. A
    /// failed second admission drops the first increment automatically.
    pub(crate) fn grow(&mut self, bytes: u64) -> Result<(), MemoryError> {
        let added = self.scoped.as_ref().map(|lease| {
            lease.amount.bytes.checked_add(bytes)
                .ok_or_else(|| Busy::new("class=byte_overflow"))?;
            lease.governor.reserve(&lease.subject, lease.lane, Usage { bytes, ..Usage::default() })
        }).transpose().map_err(MemoryError::Scoped)?;
        self.memory.grow(bytes).map_err(MemoryError::Process)?;
        if let (Some(lease), Some(mut added)) = (&mut self.scoped, added) {
            lease.amount.bytes += bytes;
            added.amount.bytes = 0;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{Governor, Lane};
    use std::sync::Arc;

    #[test]
    fn failed_growth_releases_both_incremental_reservations() {
        let mut governor = Governor::default();
        let cap = &mut governor.limits[Lane::Background.index()];
        cap.bytes = 64;
        cap.subject_bytes = 64;
        let governor = Arc::new(governor);
        let scope = governor.admit("memory".into(), Lane::Background, 0).unwrap();
        let mut owner = MemoryReservation::new(16, Some(&scope)).unwrap();
        let held = scope.bytes(48).unwrap();
        assert!(matches!(owner.grow(1), Err(MemoryError::Scoped(_))));
        assert_eq!(owner.bytes(), 16);
        drop(held);

        // Refuse the process admission after the scoped increment was acquired.
        let (result, refused) = crate::storage::build_budget::refuse_shared_at_for_test(
            0, || owner.grow(32));
        assert!(refused);
        assert!(matches!(result, Err(MemoryError::Process(_))));
        assert_eq!(owner.bytes(), 16);
        assert_eq!(governor.ledger.lock().unwrap().total[Lane::Background.index()].bytes, 16);

        owner.grow(48).unwrap();
        assert_eq!(owner.bytes(), 64);
        drop(owner);
        let all = MemoryReservation::new(64, Some(&scope)).unwrap();
        assert_eq!(all.bytes(), 64);
    }
}
