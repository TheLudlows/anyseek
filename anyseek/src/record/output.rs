//! Data hiding barrier independent of a fixed read view and catalog authority.
use super::RecordError;
use std::sync::{Arc, RwLock, Weak};

#[derive(Default)]
pub(super) struct OutputState {
    pub revision: u64,
    pub fenced: bool,
}
/// A bounded response may use an old complete read view, but may not cross a
/// failed authority publication boundary. Combine with authorization at handoff.
#[derive(Clone)]
pub struct DataOutputPermit {
    states: Vec<(Arc<RwLock<OutputState>>, u64)>,
    // Only transport permits opt into waiting. Internal callers already holding
    // Inner must continue using synchronous deliver. Weak owners preserve the
    // rule that a saved response cannot keep a retired partition alive.
    publications: Vec<Weak<tokio::sync::Mutex<super::Inner>>>,
}
impl DataOutputPermit {
    pub(super) fn new(state: Arc<RwLock<OutputState>>, revision: u64) -> Self {
        Self {
            states: vec![(state, revision)],
            publications: Vec::new(),
        }
    }
    /// Combine already captured views for bounded collection listings. This
    /// aggregates output barriers, not data snapshots or cross-collection writes.
    pub fn combine(permits: Vec<Self>) -> Self {
        let mut states = std::collections::BTreeMap::new();
        let mut publications = std::collections::BTreeMap::new();
        for permit in permits {
            for owner in permit.publications {
                publications.insert(owner.as_ptr() as usize, owner);
            }
            for (state, revision) in permit.states {
                let entry = states
                    .entry(Arc::as_ptr(&state) as usize)
                    .or_insert((state, revision));
                entry.1 = entry.1.min(revision);
            }
        }
        Self {
            states: states.into_values().collect(),
            publications: publications.into_values().collect(),
        }
    }
    /// Wait for an in-progress publication before checking the unchanged failure
    /// barrier and handing off. No lock is retained after the synchronous callback.
    /// Do not call from inside a partition operation or reenter it in the callback.
    pub async fn deliver_after_publication<T>(&self, handoff: impl FnOnce() -> T) -> Result<T, RecordError> {
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
            .map(|(state, _)| state.read().map_err(|_| RecordError::RecoveryRequired))
            .collect::<Result<Vec<_>, _>>()?;
        for (guard, (_, revision)) in guards.iter().zip(&self.states) {
            if guard.fenced || guard.revision != *revision {
                return Err(RecordError::RecoveryRequired);
            }
        }
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
