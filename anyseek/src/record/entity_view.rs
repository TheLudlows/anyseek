//! Pinned storage candidates for embedded owners of an isolated partition.
use super::*;
use crate::storage::{
    build_budget::{MemoryLease, reserve_memory},
    disk_records::entity,
};

const MAX_TAIL_ROWS: usize = 128;
const MAX_TAIL_BYTES: usize = 2 * 1024 * 1024;
const MAX_TAIL_SEQUENCE: u64 = 4096;

/// Direction of one edge relative to the requested anchor entity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntityDirection {
    /// Edges declared from the anchor.
    Out,
    /// Edges declared to the anchor.
    In,
}

impl EntityDirection {
    pub(crate) fn endpoints<'a>(self, from: &'a str, to: &'a str) -> (&'a str, &'a str) {
        match self {
            Self::Out => (from, to),
            Self::In => (to, from),
        }
    }
}

/// A derived edge declaration. Its canonical version is relative to the pinned cut.
#[derive(Debug, PartialEq, Eq)]
pub struct EntityCandidate {
    /// Other endpoint of the matched edge.
    pub neighbor: String,
    /// Record declaring the edge.
    pub record_id: String,
    /// Canonical body version of the declaration.
    pub version: u64,
}

/// Ordered edge declarations from one exact range.
pub struct EntityCandidates {
    candidates: Vec<EntityCandidate>,
    _memory: MemoryLease,
}

impl EntityCandidates {
    /// Borrow the results in neighbor ID and then Record ID order.
    pub fn as_slice(&self) -> &[EntityCandidate] {
        &self.candidates
    }
}

/// An immutable storage snapshot for a trusted embedded partition owner.
/// The selected files and suffix remain pinned for the lifetime of this view.
/// Later writes do not change it. Consumers must separately prove seed and
/// target eligibility and revalidate authority before delivering live results.
pub struct EntityAdjacencyView {
    snapshot: Arc<Snapshot>,
    admission: Arc<tokio::sync::Semaphore>,
}

struct Snapshot {
    cut: u64,
    evaluation: u64,
    base: Arc<state_rows::Base>,
    tail: Vec<state_memory::Shared<Record>>,
    max_visits: usize,
    barrier: DataOutputPermit,
    _memory: MemoryLease,
    _scope: Option<crate::resources::Lease>,
}

impl RecordPartition {
    /// Capture the current local Record cut and pin its optional adjacency sidecar.
    /// A missing sidecar or oversized suffix requires an explicit new build.
    pub async fn capture_entity_adjacency(&self) -> Result<EntityAdjacencyView, RecordError> {
        let _permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let mut inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        if inner.remote.is_some() || inner.archive.is_some() {
            return Err(RecordError::Rejected(
                "entity adjacency requires local-only storage without archive".into(),
            ));
        }
        let evaluation = inner.observe_time()?;
        let cut = inner.state.seq;
        let records = &inner.state.records;
        let base = records.entity_base().cloned().ok_or_else(|| {
            RecordError::Rejected("entity adjacency has no selected sidecar".into())
        })?;
        let base_cut = base.entity.as_ref().expect("selected sidecar").cut();
        let certified = inner
            .anchor
            .as_ref()
            .ok_or(RecordError::RecoveryRequired)?
            .checkpoint_state()?;
        if !certified
            .records
            .entity_base()
            .is_some_and(|selected| Arc::ptr_eq(selected, &base))
            || base_cut > certified.seq
            || certified.seq > cut
        {
            return Err(RecordError::RecoveryRequired);
        }
        let mut recent = records.recent_records();
        let count = recent.len();
        if count > MAX_TAIL_ROWS || cut - base_cut > MAX_TAIL_SEQUENCE {
            return Err(RecordError::Rejected(
                "entity adjacency tail exceeds its row or sequence budget".into(),
            ));
        }
        let bytes = recent.try_fold(0usize, |sum, row| sum.checked_add(row.wire_bytes()));
        let Some(bytes) = bytes.filter(|&bytes| bytes <= MAX_TAIL_BYTES) else {
            return Err(RecordError::Rejected(
                "entity adjacency tail exceeds its byte budget".into(),
            ));
        };
        let buffers = (1024 + count * size_of::<state_memory::Shared<Record>>()) as u64;
        let memory = reserve_memory(buffers).map_err(memory_error)?;
        let scope = crate::resources::Scope::current()
            .map(|scope| scope.bytes(buffers + bytes as u64))
            .transpose()
            .map_err(|_| RecordError::Busy)?;
        Ok(EntityAdjacencyView {
            snapshot: Arc::new(Snapshot {
                cut,
                evaluation,
                base,
                tail: records.recent_records().cloned().collect(),
                max_visits: entity::MAX_VISITS.min(inner.config.max_candidates),
                barrier: self.output_permit()?,
                _memory: memory,
                _scope: scope,
            }),
            admission: self.admission.clone(),
        })
    }
}

impl EntityAdjacencyView {
    /// Canonical Record sequence of the base plus captured overlay.
    pub fn visible_seq(&self) -> u64 {
        self.snapshot.cut
    }

    /// Read declarations matching one entity ID, edge type and direction.
    /// All matching postings examined, including shadowed or expired rows,
    /// count toward the hard visit limit. An overflow returns no partial result.
    pub async fn candidates(
        &self,
        direction: EntityDirection,
        anchor: &str,
        kind: &str,
        visit_limit: usize,
    ) -> Result<EntityCandidates, RecordError> {
        if !(1..=self.snapshot.max_visits).contains(&visit_limit) {
            return Err(RecordError::Rejected(
                "invalid entity adjacency visit limit".into(),
            ));
        }
        let range =
            entity::Range::new(direction, anchor, kind).map_err(state_rows::entity_error)?;
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let snapshot = self.snapshot.clone();
        crate::resources::spawn_blocking(move || {
            let _permit = permit;
            snapshot.read(&range, visit_limit)
        })
        .await
        .map_err(storage)?
    }
}

impl Snapshot {
    fn read(
        &self,
        range: &entity::Range,
        mut remaining: usize,
    ) -> Result<EntityCandidates, RecordError> {
        // Owned candidate strings, sort slots and per-record tail key workspace.
        let _scratch = reserve_memory(256 * 1024).map_err(memory_error)?;
        let memory = reserve_memory(remaining as u64 * 4096).map_err(memory_error)?;
        let mut output = Vec::new();
        let mut charge = || {
            remaining = remaining.checked_sub(1).ok_or_else(|| {
                RecordError::Rejected("entity adjacency visit budget exhausted".into())
            })?;
            Ok::<_, RecordError>(())
        };
        let adjacency = self.base.entity.as_ref().expect("selected sidecar");
        adjacency.visit(range, state_rows::entity_error, |key, reference| {
            charge()?;
            let (neighbor, record_id) = range.identity(key).map_err(state_rows::entity_error)?;
            if self
                .tail
                .binary_search_by(|row| row.record_id.as_str().cmp(&record_id))
                .is_err()
            {
                let row = adjacency
                    .locate(&self.base.checkpoint, &record_id, reference)
                    .map_err(state_rows::entity_error)?;
                if row.metadata.visible(self.evaluation) {
                    output.push((
                        key.to_owned(),
                        EntityCandidate {
                            neighbor,
                            record_id,
                            version: row.metadata.version,
                        },
                    ));
                }
            }
            Ok(())
        })?;
        for row in &self.tail {
            let Some(document) = &row.document else {
                continue;
            };
            for key in range
                .tail_keys(&row.record_id, document)
                .map_err(state_rows::entity_error)?
            {
                charge()?;
                if temporal::visible(row, self.evaluation) {
                    let (neighbor, record_id) =
                        range.identity(&key).map_err(state_rows::entity_error)?;
                    output.push((
                        key,
                        EntityCandidate {
                            neighbor,
                            record_id,
                            version: row.version,
                        },
                    ));
                }
            }
        }
        output.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        self.barrier.deliver(|| EntityCandidates {
            candidates: output.into_iter().map(|(_, candidate)| candidate).collect(),
            _memory: memory,
        })
    }
}

#[cfg(test)]
mod tests;
