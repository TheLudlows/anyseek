//! Pinned storage candidates for embedded owners of an isolated partition.
use super::*;
use crate::search::QueryWork;
use crate::storage::{
    build_budget::{MemoryLease, reserve_memory},
    disk_records::entity,
};
use tokio::time::{Instant, timeout_at};

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

/// Visible canonical revision declaring membership in an entity.
#[derive(Debug, PartialEq, Eq)]
pub struct EntityMember {
    /// Stable Record identity.
    pub record_id: String,
    /// Body version at the captured Record cut.
    pub version: u64,
}

/// One neighbor, its declarations and visible member revisions at the same cut.
pub struct EntityNeighborCandidates {
    /// Neighbor entity ID.
    pub entity_id: String,
    /// Edges linking the requested anchor and neighbor in the requested direction.
    pub declarations: Vec<EntityCandidate>,
    /// Canonical member revisions of this neighbor in Record ID order.
    pub members: Vec<EntityMember>,
}

/// Bounded discovery for an already authorized embedded owner of the pinned view.
/// These candidates require final publication revalidation before live delivery.
pub struct EntityCandidatePaths {
    /// Canonical member revisions of the anchor in Record ID order.
    pub seeds: Vec<EntityMember>,
    /// Neighbors with members, ordered by entity ID.
    pub neighbors: Vec<EntityNeighborCandidates>,
    _memory: MemoryLease,
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

    /// Discover seed and target revisions using exact member ranges at this cut.
    /// The visit limit covers all edge and member ranges together. The same
    /// deadline and work and positioned-read budgets apply across discovery.
    /// An edge-only sidecar needs rebuilding before it can supply members.
    pub async fn candidate_paths(
        &self,
        direction: EntityDirection,
        anchor: &str,
        kind: &str,
        visit_limit: usize,
        deadline: Instant,
    ) -> Result<EntityCandidatePaths, RecordError> {
        let work = Arc::new(QueryWork::new(65_536, 2_048, 16 * 1024 * 1024, deadline));
        self.candidate_paths_with_work(direction, anchor, kind, visit_limit, deadline, work)
            .await
    }

    pub(super) async fn candidate_paths_with_work(
        &self,
        direction: EntityDirection,
        anchor: &str,
        kind: &str,
        visit_limit: usize,
        deadline: Instant,
        work: Arc<QueryWork>,
    ) -> Result<EntityCandidatePaths, RecordError> {
        if !(1..=self.snapshot.max_visits).contains(&visit_limit) {
            return Err(RecordError::Rejected(
                "invalid entity adjacency visit limit".into(),
            ));
        }
        let edges =
            entity::Range::new(direction, anchor, kind).map_err(state_rows::entity_error)?;
        let seeds = entity::MemberRange::new(anchor).map_err(state_rows::entity_error)?;
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let snapshot = self.snapshot.clone();
        let anchor = anchor.to_owned();
        let task = crate::resources::spawn_blocking(move || {
            let _permit = permit;
            work.clone()
                .with_reads(|| snapshot.paths(&anchor, &seeds, &edges, visit_limit, &work))
        });
        timeout_at(deadline, task)
            .await
            .map_err(|_| RecordError::Busy)?
            .map_err(storage)?
    }
}

struct RangeWork<'a> {
    remaining: usize,
    work: Option<&'a QueryWork>,
}

impl RangeWork<'_> {
    fn take(&mut self) -> Result<(), RecordError> {
        self.remaining = self.remaining.checked_sub(1).ok_or_else(|| {
            RecordError::Rejected("entity adjacency visit budget exhausted".into())
        })?;
        Ok(())
    }
}

fn examine(work: Option<&QueryWork>) -> Result<(), RecordError> {
    work.map_or(Ok(()), |work| work.charge(1))
}

impl Snapshot {
    fn projection(&self) -> &entity::Projection {
        self.base.entity.as_ref().expect("selected sidecar")
    }

    fn shadowed(&self, id: &str) -> bool {
        self.tail
            .binary_search_by(|row| row.record_id.as_str().cmp(id))
            .is_ok()
    }

    fn read(&self, range: &entity::Range, limit: usize) -> Result<EntityCandidates, RecordError> {
        // Owned candidate strings, sort slots and per-record tail key workspace.
        let _scratch = reserve_memory(256 * 1024).map_err(memory_error)?;
        let memory = reserve_memory(limit as u64 * 4096).map_err(memory_error)?;
        let candidates = self.edges(
            range,
            &mut RangeWork {
                remaining: limit,
                work: None,
            },
        )?;
        self.barrier.deliver(|| EntityCandidates {
            candidates,
            _memory: memory,
        })
    }

    fn edges(
        &self,
        range: &entity::Range,
        budget: &mut RangeWork<'_>,
    ) -> Result<Vec<EntityCandidate>, RecordError> {
        let work = budget.work;
        let mut output = Vec::new();
        let adjacency = self.projection();
        adjacency.visit(
            &range.bounds,
            state_rows::entity_error,
            || examine(work),
            |key, reference| {
                budget.take()?;
                let (neighbor, record_id) =
                    range.identity(key).map_err(state_rows::entity_error)?;
                if !self.shadowed(&record_id) {
                    let row = adjacency
                        .locate(&self.base.checkpoint, &record_id, reference)
                        .map_err(state_rows::entity_error)?;
                    if row.metadata.visible(self.evaluation) {
                        output.push(EntityCandidate {
                            neighbor,
                            record_id,
                            version: row.metadata.version,
                        });
                    }
                }
                Ok(())
            },
        )?;
        for row in &self.tail {
            examine(work)?;
            let Some(document) = &row.document else {
                continue;
            };
            for key in range
                .tail_keys(&row.record_id, document, || {
                    examine(work).map_err(std::io::Error::other)
                })
                .map_err(state_rows::entity_error)?
            {
                budget.take()?;
                if temporal::visible(row, self.evaluation) {
                    let (neighbor, record_id) =
                        range.identity(&key).map_err(state_rows::entity_error)?;
                    output.push(EntityCandidate {
                        neighbor,
                        record_id,
                        version: row.version,
                    });
                }
            }
        }
        output
            .sort_unstable_by(|a, b| (&a.neighbor, &a.record_id).cmp(&(&b.neighbor, &b.record_id)));
        Ok(output)
    }

    fn members(
        &self,
        range: &entity::MemberRange,
        budget: &mut RangeWork<'_>,
    ) -> Result<Vec<EntityMember>, RecordError> {
        let work = budget.work;
        let check = || examine(work).map_err(std::io::Error::other);
        let mut output = Vec::new();
        let adjacency = self.projection();
        adjacency.visit(
            &range.bounds,
            state_rows::entity_error,
            || examine(work),
            |key, reference| {
                budget.take()?;
                let record_id = range.record_id(key).map_err(state_rows::entity_error)?;
                if self.shadowed(&record_id) {
                    return Ok(());
                }
                let row = adjacency
                    .locate(&self.base.checkpoint, &record_id, reference)
                    .map_err(state_rows::entity_error)?;
                if !row.metadata.visible(self.evaluation) {
                    return Ok(());
                }
                let version = row.metadata.version;
                let matched = state_rows::decode(&record_id, row, |record| {
                    let document = record
                        .document
                        .as_ref()
                        .ok_or_else(|| storage("missing entity member body"))?;
                    range
                        .matches(&record_id, document, check)
                        .map_err(state_rows::entity_error)
                })?;
                if !matched {
                    return Err(storage(
                        "entity member posting disagrees with its canonical Record",
                    ));
                }
                output.push(EntityMember { record_id, version });
                Ok(())
            },
        )?;
        for row in &self.tail {
            examine(work)?;
            let Some(document) = &row.document else {
                continue;
            };
            if range
                .matches(&row.record_id, document, check)
                .map_err(state_rows::entity_error)?
            {
                budget.take()?;
                if temporal::visible(row, self.evaluation) {
                    output.push(EntityMember {
                        record_id: row.record_id.clone(),
                        version: row.version,
                    });
                }
            }
        }
        output.sort_unstable_by(|a, b| a.record_id.cmp(&b.record_id));
        Ok(output)
    }

    fn paths(
        &self,
        anchor: &str,
        seeds: &entity::MemberRange,
        edges: &entity::Range,
        limit: usize,
        work: &QueryWork,
    ) -> Result<EntityCandidatePaths, RecordError> {
        work.charge(0)?;
        if !self.projection().has_members() {
            return Err(RecordError::Rejected(
                "entity membership requires rebuilding the selected sidecar".into(),
            ));
        }
        let _scratch = reserve_memory(256 * 1024).map_err(memory_error)?;
        let memory = reserve_memory(limit as u64 * 4096).map_err(memory_error)?;
        let mut budget = RangeWork {
            remaining: limit,
            work: Some(work),
        };
        let seeds = self.members(seeds, &mut budget)?;
        let mut neighbors = Vec::new();
        if !seeds.is_empty() {
            let mut ranges = BTreeMap::<String, Vec<EntityCandidate>>::new();
            for edge in self.edges(edges, &mut budget)? {
                work.charge(1)?;
                if edge.neighbor != anchor {
                    ranges.entry(edge.neighbor.clone()).or_default().push(edge);
                }
            }
            for (entity_id, declarations) in ranges {
                work.charge(1)?;
                let range =
                    entity::MemberRange::new(&entity_id).map_err(state_rows::entity_error)?;
                let members = self.members(&range, &mut budget)?;
                if !members.is_empty() {
                    neighbors.push(EntityNeighborCandidates {
                        entity_id,
                        declarations,
                        members,
                    });
                }
            }
        }
        work.charge(0)?;
        self.barrier.deliver(|| EntityCandidatePaths {
            seeds,
            neighbors,
            _memory: memory,
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod member_tests;
