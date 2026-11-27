//! Pinned storage candidates for embedded owners of an isolated partition.
use super::*;
use crate::search::QueryWork;
use crate::storage::{
    build_budget::{MemoryLease, reserve_memory},
    disk_records::entity,
};
use std::sync::Weak;
use tokio::time::{Instant, timeout_at};

const MAX_TAIL_ROWS: usize = 128;
const MAX_TAIL_BYTES: usize = 2 * 1024 * 1024;
const MAX_TAIL_SEQUENCE: u64 = 4096;

fn path_work(deadline: Instant) -> Arc<QueryWork> {
    Arc::new(QueryWork::new(65_536, 2_048, 16 * 1024 * 1024, deadline))
}

/// Direction of one edge relative to the requested anchor entity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, PartialEq, Eq, Serialize, utoipa::ToSchema)]
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
#[derive(Debug, PartialEq, Eq, Serialize, utoipa::ToSchema)]
pub struct EntityMember {
    /// Stable Record identity.
    pub record_id: String,
    /// Body version at the captured Record cut.
    pub version: u64,
}

/// One neighbor, its declarations and visible member revisions at the same cut.
#[derive(Serialize, utoipa::ToSchema)]
pub struct EntityNeighborCandidates {
    entity_id: String,
    declarations: Vec<EntityCandidate>,
    members: Vec<EntityMember>,
}

impl EntityNeighborCandidates {
    /// Neighbor entity ID.
    pub fn entity_id(&self) -> &str {
        &self.entity_id
    }

    /// Edges linking the requested anchor and neighbor in the requested direction.
    pub fn declarations(&self) -> &[EntityCandidate] {
        &self.declarations
    }

    /// Canonical member revisions of this neighbor in Record ID order.
    pub fn members(&self) -> &[EntityMember] {
        &self.members
    }
}

/// Bounded discovery for an already authorized embedded owner of the pinned view.
/// These candidates require final publication revalidation before live delivery.
/// The candidate storage remains borrowed from the result that holds its memory lease.
///
/// ```compile_fail,E0616
/// use anyseek::record::EntityCandidatePaths;
///
/// fn detach_candidates(mut result: EntityCandidatePaths) {
///     let neighbors = std::mem::take(&mut result.neighbors);
///     drop(result);
///     assert!(!neighbors.is_empty());
/// }
/// ```
#[derive(Serialize, utoipa::ToSchema)]
pub struct EntityCandidatePaths {
    visible_seq: u64,
    seeds: Vec<EntityMember>,
    neighbors: Vec<EntityNeighborCandidates>,
    #[serde(skip)]
    expires_at_ms: Option<u64>,
    #[serde(skip)]
    _memory: MemoryLease,
}

impl EntityCandidatePaths {
    /// The canonical cut of the returned identities and versions.
    pub fn visible_seq(&self) -> u64 {
        self.visible_seq
    }

    /// Canonical member revisions of the anchor in Record ID order.
    pub fn seeds(&self) -> &[EntityMember] {
        &self.seeds
    }

    /// Neighbors with members, ordered by entity ID.
    pub fn neighbors(&self) -> &[EntityNeighborCandidates] {
        &self.neighbors
    }
}

/// An immutable storage snapshot for a trusted embedded partition owner.
/// The selected files and suffix remain pinned for the lifetime of this view.
/// Later writes do not change it. Live delivery also requires the current
/// publication and authorization checks provided by the local SDK.
pub struct EntityAdjacencyView {
    snapshot: Arc<Snapshot>,
    admission: Arc<tokio::sync::Semaphore>,
}

struct Snapshot {
    publication: EntityPublication,
    evaluation: u64,
    observed_at: Instant,
    base: Arc<state_rows::Base>,
    tail: Vec<state_memory::Shared<Record>>,
    max_visits: usize,
    barrier: DataOutputPermit,
    _memory: MemoryLease,
    _scope: Option<crate::resources::Lease>,
}

#[derive(Clone)]
struct EntityPublication {
    cut: u64,
    group_revision: u64,
    selected_owner: String,
    base: Weak<state_rows::Base>,
}

impl EntityPublication {
    fn check(&self, inner: &Inner) -> Result<(), RecordError> {
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        if inner.state.seq != self.cut
            || inner.state.group_publication_revision() != self.group_revision
            || inner
                .anchor
                .as_ref()
                .is_none_or(|anchor| anchor.selected_owner() != self.selected_owner)
            || !inner
                .state
                .records
                .entity_base()
                .is_some_and(|base| std::ptr::eq(self.base.as_ptr(), Arc::as_ptr(base)))
        {
            return Err(RecordError::Busy);
        }
        Ok(())
    }
}

/// Recheck an entity publication without retaining an old partition or its base.
pub(crate) struct EntityFrameOutput {
    publication: EntityPublication,
    owner: Weak<Mutex<Inner>>,
    barrier: DataOutputPermit,
    time: output::ReadTimeFence,
}

pub(crate) struct EntityFrameGuard<'a> {
    output: &'a EntityFrameOutput,
    inner: tokio::sync::OwnedMutexGuard<Inner>,
}

impl EntityFrameOutput {
    pub(crate) fn try_pin(&self) -> Result<Option<EntityFrameGuard<'_>>, RecordError> {
        let owner = self.owner.upgrade().ok_or(RecordError::RecoveryRequired)?;
        let Ok(inner) = owner.try_lock_owned() else {
            return Ok(None);
        };
        Ok(Some(EntityFrameGuard {
            output: self,
            inner,
        }))
    }

    pub(crate) async fn pin(&self) -> Result<EntityFrameGuard<'_>, RecordError> {
        let owner = self.owner.upgrade().ok_or(RecordError::RecoveryRequired)?;
        let inner = timeout_at(self.time.deadline, owner.lock_owned())
            .await
            .map_err(|_| RecordError::Busy)?;
        Ok(EntityFrameGuard {
            output: self,
            inner,
        })
    }
}

impl EntityFrameGuard<'_> {
    pub(crate) fn deliver<T>(&self, handoff: impl FnOnce() -> T) -> Result<T, RecordError> {
        self.output.publication.check(&self.inner)?;
        self.output.barrier.deliver(|| {
            self.output.time.check()?;
            Ok(handoff())
        })?
    }
}

/// Holds the complete leased result and the exact request work until handoff.
pub(crate) struct PreparedEntityPaths {
    view: EntityAdjacencyView,
    owner: Weak<Mutex<Inner>>,
    paths: EntityCandidatePaths,
    deadline: Instant,
    work: Arc<QueryWork>,
}

impl RecordPartition {
    pub(crate) fn validate_entity_path_keys(anchor: &str, kind: &str) -> Result<(), RecordError> {
        entity::Range::validate(anchor, kind).map_err(state_rows::entity_error)
    }

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
        let observed_at = Instant::now();
        let cut = inner.state.seq;
        let records = &inner.state.records;
        let base = records.entity_base().cloned().ok_or_else(|| {
            RecordError::Rejected("entity adjacency has no selected sidecar".into())
        })?;
        let base_cut = base.entity.as_ref().expect("selected sidecar").cut();
        let anchor = inner.anchor.as_ref().ok_or(RecordError::RecoveryRequired)?;
        let certified = anchor.checkpoint_state()?;
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
                publication: EntityPublication {
                    cut,
                    group_revision: inner.state.group_publication_revision(),
                    selected_owner: anchor.selected_owner().to_owned(),
                    base: Arc::downgrade(&base),
                },
                evaluation,
                observed_at,
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

    pub(crate) async fn prepare_entity_paths(
        &self,
        direction: EntityDirection,
        anchor: &str,
        kind: &str,
        visit_limit: usize,
        deadline: Instant,
    ) -> Result<PreparedEntityPaths, RecordError> {
        let view = timeout_at(deadline, self.capture_entity_adjacency())
            .await
            .map_err(|_| RecordError::Busy)??;
        let work = path_work(deadline);
        let paths = view
            .candidate_paths_with_work(direction, anchor, kind, visit_limit, deadline, work.clone())
            .await?;
        Ok(PreparedEntityPaths {
            view,
            owner: Arc::downgrade(&self.inner),
            paths,
            deadline,
            work,
        })
    }
}

impl PreparedEntityPaths {
    /// The collection owner calls this inside its current authorization guard.
    pub(crate) async fn deliver(
        self,
        p: &RecordPartition,
    ) -> Result<(EntityCandidatePaths, EntityFrameOutput), RecordError> {
        if !self.owner.ptr_eq(&Arc::downgrade(&p.inner)) {
            return Err(RecordError::Busy);
        }
        let mut inner = timeout_at(self.deadline, p.inner.lock())
            .await
            .map_err(|_| RecordError::Busy)?;
        let snapshot = &self.view.snapshot;
        snapshot.publication.check(&inner)?;
        // Versions and canonical claims are immutable at the checked cut.
        // Bill each role before moving the complete result across the barrier.
        let roles = self.paths.seeds.len()
            + self
                .paths
                .neighbors
                .iter()
                .map(|neighbor| neighbor.declarations.len() + neighbor.members.len())
                .sum::<usize>();
        snapshot.barrier.deliver(|| {
            self.work.charge(roles)?;
            let evaluation = inner.observe_time()?;
            let expires = self.paths.expires_at_ms;
            let monotonic_expiry = expires.and_then(|expiry| {
                snapshot
                    .observed_at
                    .checked_add(std::time::Duration::from_millis(
                        expiry.saturating_sub(snapshot.evaluation),
                    ))
            });
            if expires.is_some_and(|expiry| evaluation >= expiry)
                || monotonic_expiry.is_some_and(|expiry| Instant::now() >= expiry)
            {
                return Err(RecordError::Busy);
            }
            self.work.charge(0)?;
            Ok((
                self.paths,
                EntityFrameOutput {
                    publication: snapshot.publication.clone(),
                    owner: self.owner,
                    barrier: snapshot.barrier.clone(),
                    time: output::ReadTimeFence {
                        clock: inner.clock.clone(),
                        observed: inner.observed_time.clone(),
                        certified_floor: inner.state.time_watermark_ms,
                        deadline: self.deadline,
                        expires,
                        monotonic_expiry,
                    },
                },
            ))
        })?
    }
}

impl EntityAdjacencyView {
    /// Canonical Record sequence of the base plus captured overlay.
    pub fn visible_seq(&self) -> u64 {
        self.snapshot.publication.cut
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
        let work = path_work(deadline);
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

struct EdgeEvidence {
    candidate: EntityCandidate,
    expires_at_ms: Option<u64>,
}

fn include_expiry(earliest: &mut Option<u64>, expiry: Option<u64>) {
    *earliest = (*earliest).into_iter().chain(expiry).min();
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
            candidates: candidates.into_iter().map(|edge| edge.candidate).collect(),
            _memory: memory,
        })
    }

    fn edges(
        &self,
        range: &entity::Range,
        budget: &mut RangeWork<'_>,
    ) -> Result<Vec<EdgeEvidence>, RecordError> {
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
                        let version = row.metadata.version;
                        let expires_at_ms = row.metadata.temporal.expire_at_ms;
                        // Candidate paths prove the body claim. The lower-level
                        // declaration-only read relies on the authenticated key.
                        if let Some(work) = work {
                            let matched = state_rows::decode(&record_id, row, |record| {
                                let document = record
                                    .document
                                    .as_ref()
                                    .ok_or_else(|| storage("missing entity declaration body"))?;
                                range
                                    .matches_neighbor(&record_id, document, &neighbor, || {
                                        work.charge(1).map_err(std::io::Error::other)
                                    })
                                    .map_err(state_rows::entity_error)
                            })?;
                            if !matched {
                                return Err(storage(
                                    "entity edge posting disagrees with its canonical Record",
                                ));
                            }
                        }
                        output.push(EdgeEvidence {
                            candidate: EntityCandidate {
                                neighbor,
                                record_id,
                                version,
                            },
                            expires_at_ms,
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
                    output.push(EdgeEvidence {
                        candidate: EntityCandidate {
                            neighbor,
                            record_id,
                            version: row.version,
                        },
                        expires_at_ms: row.temporal.expire_at_ms,
                    });
                }
            }
        }
        output.sort_unstable_by(|a, b| {
            let (a, b) = (&a.candidate, &b.candidate);
            (&a.neighbor, &a.record_id).cmp(&(&b.neighbor, &b.record_id))
        });
        Ok(output)
    }

    fn members(
        &self,
        range: &entity::MemberRange,
        budget: &mut RangeWork<'_>,
        earliest: &mut Option<u64>,
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
                let expiry = row.metadata.temporal.expire_at_ms;
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
                include_expiry(earliest, expiry);
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
                    include_expiry(earliest, row.temporal.expire_at_ms);
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
        let mut expires_at_ms = None;
        let seeds = self.members(seeds, &mut budget, &mut expires_at_ms)?;
        let mut neighbors = Vec::new();
        if !seeds.is_empty() {
            let mut ranges = BTreeMap::<String, Vec<EdgeEvidence>>::new();
            for edge in self.edges(edges, &mut budget)? {
                work.charge(1)?;
                if edge.candidate.neighbor != anchor {
                    ranges
                        .entry(edge.candidate.neighbor.clone())
                        .or_default()
                        .push(edge);
                }
            }
            for (entity_id, declarations) in ranges {
                work.charge(1)?;
                let range =
                    entity::MemberRange::new(&entity_id).map_err(state_rows::entity_error)?;
                let members = self.members(&range, &mut budget, &mut expires_at_ms)?;
                if !members.is_empty() {
                    let declarations = declarations
                        .into_iter()
                        .map(|edge| {
                            include_expiry(&mut expires_at_ms, edge.expires_at_ms);
                            edge.candidate
                        })
                        .collect();
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
            visible_seq: self.publication.cut,
            seeds,
            neighbors,
            expires_at_ms,
            _memory: memory,
        })
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod member_tests;
