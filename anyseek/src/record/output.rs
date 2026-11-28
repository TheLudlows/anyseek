//! Data hiding barrier independent of a fixed read view and catalog authority.
use super::RecordError;
use std::sync::{Arc, RwLock, Weak};

#[derive(Default)]
pub(super) struct OutputState {
    pub revision: u64,
    pub fenced: bool,
    pub(super) point: Option<super::point::PointView>,
    pub(super) point_pending: bool,
}
/// A bounded response may use an old complete read view, but may not cross a
/// failed authority publication boundary. Combine with authorization at handoff.
#[derive(Clone)]
pub struct DataOutputPermit {
    states: Vec<(Arc<RwLock<OutputState>>, u64, bool)>,
    // Only transport permits opt into waiting. Internal callers already holding
    // Inner must continue using synchronous deliver. Weak owners preserve the
    // rule that a saved response cannot keep a retired partition alive.
    publications: Vec<Weak<tokio::sync::Mutex<super::Inner>>>,
    point_publication: Option<Weak<tokio::sync::Mutex<super::Inner>>>,
    residency: Vec<crate::resources::residency::Pin>,
    time_fences: Vec<ReadTimeFence>,
}

#[derive(Clone)]
pub(crate) struct ReadTimeFence {
    pub(super) clock: Arc<dyn super::temporal::Clock>,
    pub(super) observed: super::temporal::ObservedTime,
    pub(super) certified_floor: u64,
    pub(super) deadline: tokio::time::Instant,
    pub(super) expires: Option<u64>,
    pub(super) monotonic_expiry: Option<tokio::time::Instant>,
}

impl ReadTimeFence {
    pub(super) fn check(&self) -> Result<(), RecordError> {
        let now = tokio::time::Instant::now();
        if now >= self.deadline || self.monotonic_expiry.is_some_and(|expiry| now >= expiry) {
            return Err(RecordError::Busy);
        }
        if let Some(expiry) = self.expires {
            if self.observed.get() >= expiry {
                return Err(RecordError::Busy);
            }
            let time = super::temporal::checked_time(self.clock.as_ref(), self.certified_floor)?;
            if self.observed.advance(time) >= expiry {
                return Err(RecordError::Busy);
            }
        }
        Ok(())
    }
}
impl DataOutputPermit {
    pub(super) fn new(state: Arc<RwLock<OutputState>>, revision: u64) -> Self {
        Self {
            states: vec![(state, revision, false)],
            publications: Vec::new(),
            point_publication: None,
            residency: Vec::new(),
            time_fences: Vec::new(),
        }
    }
    /// Combine already captured views for bounded collection listings. This
    /// aggregates output barriers, not data snapshots or cross-collection writes.
    pub fn combine(permits: Vec<Self>) -> Self {
        let mut states = std::collections::BTreeMap::new();
        let mut publications = std::collections::BTreeMap::new();
        let mut residency = Vec::new();
        let mut time_fences = Vec::new();
        for permit in permits {
            residency.extend(permit.residency);
            time_fences.extend(permit.time_fences);
            for owner in permit.publications.into_iter().chain(permit.point_publication) {
                publications.insert(owner.as_ptr() as usize, owner);
            }
            for (state, revision, point) in permit.states {
                let entry = states
                    .entry(Arc::as_ptr(&state) as usize)
                    .or_insert((state, revision, point));
                entry.1 = entry.1.min(revision);
                entry.2 |= point;
            }
        }
        Self {
            states: states.into_values().collect(),
            publications: publications.into_values().collect(),
            point_publication: None,
            residency,
            time_fences,
        }
    }

    pub(super) fn for_point(mut self, owner: Weak<tokio::sync::Mutex<super::Inner>>) -> Self {
        self.point_publication = Some(owner);
        self.states[0].2 = true;
        self
    }

    pub(crate) fn with_residency(mut self, pin: crate::resources::residency::Pin) -> Self {
        self.residency.push(pin);
        self
    }

    pub(crate) fn with_time_fence(mut self, time: ReadTimeFence) -> Self {
        self.time_fences.push(time);
        self
    }

    /// Receipt and progress responses keep residency without adding a new
    /// publication fence to their existing contracts.
    pub(crate) fn residency_only(pin: crate::resources::residency::Pin) -> Self {
        Self { states: Vec::new(), publications: Vec::new(), point_publication: None, residency: vec![pin], time_fences: Vec::new() }
    }
    /// Wait for an in-progress publication before checking the unchanged failure
    /// barrier and handing off. No lock is retained after the synchronous callback.
    /// Do not call from inside a partition operation or reenter it in the callback.
    pub async fn deliver_after_publication<T>(&self, handoff: impl FnOnce() -> T) -> Result<T, RecordError> {
        if let Some(owner) = &self.point_publication {
            // Exact point capture is independent of write preparation. The
            // same health/owner fence still runs at every handoff.
            let mut handoff = Some(handoff);
            return match self.deliver(|| handoff.take().unwrap()()) {
                Err(RecordError::RecoveryRequired) => {
                    let owner = owner.upgrade().ok_or(RecordError::RecoveryRequired)?;
                    let _publication = owner.lock_owned().await;
                    self.deliver(handoff.take().unwrap())
                }
                result => result,
            };
        }
        let mut guards = Vec::with_capacity(self.publications.len());
        for owner in &self.publications {
            let owner = owner.upgrade().ok_or(RecordError::RecoveryRequired)?;
            guards.push(owner.lock_owned().await);
        }
        self.deliver(handoff)
    }
    /// Check and hand off without an intervening await. The callback must not
    /// reenter the engine or acquire an earlier catalog/partition lock.
    pub fn deliver<T>(&self, handoff: impl FnOnce() -> T) -> Result<T, RecordError> {
        let guards = self
            .states
            .iter()
            .map(|(state, _, _)| state.read().map_err(|_| RecordError::RecoveryRequired))
            .collect::<Result<Vec<_>, _>>()?;
        for (guard, (_, revision, point)) in guards.iter().zip(&self.states) {
            if guard.fenced || (*point && guard.point_pending) || guard.revision != *revision {
                return Err(RecordError::RecoveryRequired);
            }
        }
        for fence in &self.time_fences { fence.check()?; }
        Ok(handoff())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{tests::{config, write}, ExpectedVersion, RecordPartition};
    use std::time::Duration;

    #[tokio::test]
    async fn transport_output_waits_for_publication_and_cancellation_does_not_cancel_writer() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let output = p.output_permit_after_publication().await.unwrap();
        let pause = root.join(".test-lexical-consumer-pause");
        std::fs::write(&pause, b"pause").unwrap();
        let owner = p.clone();
        let writer = tokio::spawn(async move {
            owner.write_batch(&[
                write("a", "a", "alpha", ExpectedVersion::Absent),
                write("b", "b", "alpha", ExpectedVersion::Absent),
            ]).await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !root.join(".test-lexical-consumer-started").exists() {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        // Reproduce the original fast-failure path while a healthy writer owns
        // publication. Neither capture nor delivery may expose data in this gap.
        assert!(matches!(p.output_permit(), Err(RecordError::RecoveryRequired)));
        assert_eq!(output.deliver(|| ()), Err(RecordError::RecoveryRequired));
        assert!(tokio::time::timeout(Duration::from_millis(20), p.output_permit_after_publication()).await.is_err());
        assert!(tokio::time::timeout(Duration::from_millis(20), output.deliver_after_publication(|| panic!("partial publication escaped"))).await.is_err());
        assert!(!writer.is_finished());
        std::fs::remove_file(pause).unwrap();
        assert!(writer.await.unwrap().unwrap().stop.is_none());
        let current = p.output_permit_after_publication().await.unwrap();
        let combined = DataOutputPermit::combine(vec![output, current]);
        assert_eq!(combined.deliver_after_publication(|| 7).await, Ok(7));
        assert_eq!(p.committed_progress().await.committed_seq, Some(2));
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        assert_eq!(combined.deliver_after_publication(|| ()).await, Err(RecordError::RecoveryRequired));
    }

    #[tokio::test]
    async fn transport_output_rejects_failed_publication_and_old_permit_after_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let output = p.output_permit_after_publication().await.unwrap();
        std::fs::write(root.join(".test-lexical-consumer-fail"), b"fail").unwrap();
        let writes = [write("a", "a", "alpha", ExpectedVersion::Absent),
            write("b", "b", "alpha", ExpectedVersion::Absent)];
        assert!(matches!(p.write_batch(&writes).await.unwrap().stop, Some(RecordError::DurablePending(2))));
        assert!(matches!(p.output_permit_after_publication().await, Err(RecordError::RecoveryRequired)));
        assert_eq!(output.deliver_after_publication(|| ()).await, Err(RecordError::RecoveryRequired));
        p.finish_service_shutdown().await.unwrap();
        drop(p);
        std::fs::remove_file(root.join(".test-lexical-consumer-fail")).unwrap();
        let recovered = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert!(recovered.write_batch(&writes).await.unwrap().stop.is_none());
        assert_eq!(recovered.committed_progress().await.committed_seq, Some(2));
        assert_eq!(recovered.search("alpha".into(), None, crate::search::SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 2);
        assert_eq!(output.deliver_after_publication(|| ()).await, Err(RecordError::RecoveryRequired));
        recovered.output_permit_after_publication().await.unwrap().deliver_after_publication(|| ()).await.unwrap();
        recovered.finish_service_shutdown().await.unwrap();
    }
}

impl super::RecordPartition {
    /// Capture a transport barrier after the current publication completes.
    /// A failed publication still rejects capture and all subsequent handoffs.
    pub async fn output_permit_after_publication(&self) -> Result<DataOutputPermit, RecordError> {
        let _inner = self.inner.lock().await;
        let mut permit = self.output_permit()?;
        permit.publications.push(Arc::downgrade(&self.inner));
        Ok(permit)
    }
}
