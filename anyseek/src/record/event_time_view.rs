//! Native consumer of one certified event-time base and its canonical suffix.
//! The suffix is the existing materialized Record overlay. It already covers
//! every selected manifest delta and the committed WAL after that manifest.
use super::*;
use crate::search::{ChannelExplanation, QueryWork, rrf_contribution};
use crate::storage::{
    build_budget::{MemoryLease, reserve_memory},
    disk_records::event_time,
};
use std::{cmp::Reverse, ops::Range, sync::atomic::Ordering};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::{Instant as Deadline, timeout_at},
};

const MAX_OVERLAY_ROWS: usize = 128;
const MAX_OVERLAY_BYTES: usize = 2 * 1024 * 1024;
const MAX_SUFFIX_SEQUENCES: u64 = 4096;
const MAX_BASE_VISITS: usize = 4096;
const MAX_RESULTS: usize = 128;
const MAX_RESULT_ID_BYTES: usize = 8 * 1024 * 1024;
const MAX_BODY_BYTES: u64 = 16 * 1024 * 1024;
const MAX_FACETS: usize = 32;
const MAX_FACET_TERMS: usize = 128;
const MIXED_WORK: usize = 65_536;
const MIXED_RANGES: usize = 2_048;
const MIXED_READ_BYTES: usize = 16 * 1024 * 1024;

pub(crate) struct TimeVectorQuery {
    pub window: TimeRange,
    pub temporal: Option<TemporalFilter>,
    pub vector: Embedding,
    pub facets: Vec<FacetFilter>,
    pub fields: Vec<String>,
    pub offset: usize,
    pub length: usize,
}

pub(crate) struct TimeMixedQuery {
    pub window: TimeRange,
    pub temporal: Option<TemporalFilter>,
    pub lexical: Option<String>,
    pub vector: Option<Embedding>,
    pub facets: Vec<FacetFilter>,
    pub fields: Vec<String>,
    pub offset: usize,
    pub length: usize,
    pub rrf: crate::search::TimeRrfOptions,
}

impl From<TimeVectorQuery> for TimeMixedQuery {
    fn from(query: TimeVectorQuery) -> Self {
        Self {
            window: query.window,
            temporal: query.temporal,
            lexical: None,
            vector: Some(query.vector),
            facets: query.facets,
            fields: query.fields,
            offset: query.offset,
            length: query.length,
            rrf: Default::default(),
        }
    }
}

enum RetrievalLane {
    Lexical(String),
    Vector(Embedding),
}

/// Ranked time and retrieval hits from one authorized Record cut.
#[derive(Serialize)]
pub struct TimeVectorResult {
    /// Common Record cut.
    pub visible_seq: u64,
    /// Trusted capture time shared by the participating channels.
    pub evaluation_time_ms: u64,
    /// A bounded union does not provide an exact total.
    pub count_total: Option<usize>,
    /// Page of fused hits, with per-channel rank and score evidence.
    pub hits: Vec<RecordHit>,
    #[serde(skip)]
    pub(crate) delivery_time: output::ReadTimeFence,
}
impl std::fmt::Debug for TimeVectorResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeVectorResult")
            .field("visible_seq", &self.visible_seq)
            .field("evaluation_time_ms", &self.evaluation_time_ms)
            .field("hit_count", &self.hits.len())
            .finish()
    }
}

/// Index publications required by a consumer of this Record view. Native
/// lexical membership and facets currently use the vector/metadata component.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexDependencies {
    None,
    VectorMetadata,
    LexicalWithVectorMetadata,
}

pub(crate) struct Window {
    pub(crate) cut: u64,
    pub(crate) evaluation_time_ms: u64,
    pub(crate) hits: Vec<Hit>,
    delivery_time: output::ReadTimeFence,
    _memory: MemoryLease,
}

pub(crate) struct Hit {
    pub(crate) id: String,
    pub(crate) event_time_ms: i64,
    pub(crate) version: u64,
    expires_at_ms: Option<u64>,
    record: Option<QueryRecord>,
    _id: MemoryLease,
}

/// A bounded, latest-first event-time window at one published Record cut.
#[derive(Serialize)]
pub struct EventTimeResult {
    /// Canonical Record sequence shared by all hits.
    pub visible_seq: u64,
    /// Trusted UTC milliseconds at which visibility was evaluated.
    pub evaluation_time_ms: u64,
    /// Exact matches ordered by descending event time, then ascending record ID.
    pub hits: Vec<QueryRecord>,
    #[serde(skip)]
    pub(crate) delivery_time: output::ReadTimeFence,
    #[serde(skip_serializing)]
    _memory: MemoryLease,
}

impl std::fmt::Debug for EventTimeResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventTimeResult")
            .field("visible_seq", &self.visible_seq)
            .field("evaluation_time_ms", &self.evaluation_time_ms)
            .field("hit_count", &self.hits.len())
            .finish()
    }
}

#[derive(Default)]
struct Selection {
    temporal: Vec<FacetFilter>,
    facets: Vec<FacetFilter>,
    fields: Option<Vec<String>>,
}

impl Selection {
    fn matches_facets(
        &self,
        document: Option<&Document>,
        deadline: Deadline,
        work: Option<&QueryWork>,
    ) -> Result<bool, RecordError> {
        if self.facets.is_empty() {
            return Ok(true);
        }
        let Some(document) = document else {
            return Ok(false);
        };
        crate::metadata_filter::matches_document(document, &self.facets, |cost| match work {
            Some(work) => work.charge(cost),
            None if Deadline::now() >= deadline => Err(RecordError::Busy),
            None => Ok(()),
        })
    }
}

struct View {
    #[cfg(test)]
    root: PathBuf,
    cut: u64,
    evaluation: u64,
    clock: Arc<dyn temporal::Clock>,
    observed_time: temporal::ObservedTime,
    certified_floor: u64,
    observed_at: Deadline,
    anchor: String,
    group_revision: u64,
    base: Arc<state_rows::Base>,
    dependencies: IndexDependencies,
    retrieval: Option<query::Handle>,
    // Stable ID order from Records, used even for tombstones or moves outside
    // the requested window. Owning only the changed rows bounds the pin.
    suffix: Vec<state_memory::Shared<Record>>,
    max_base_visits: usize,
    barrier: DataOutputPermit,
    _memory: MemoryLease,
    _scope: Option<crate::resources::Lease>,
    _admission: OwnedSemaphorePermit,
}

pub(crate) enum EventTimeError {
    Unavailable(&'static str),
    Budget(&'static str),
    Record(RecordError),
}

impl From<RecordError> for EventTimeError {
    fn from(error: RecordError) -> Self {
        Self::Record(error)
    }
}

impl EventTimeError {
    #[cfg(test)]
    pub(super) fn record_error(self) -> RecordError {
        match self {
            Self::Unavailable(reason) | Self::Budget(reason) => {
                RecordError::Rejected(format!("event-time view unavailable: {reason}"))
            }
            Self::Record(error) => error,
        }
    }
}

fn published_cut(
    partition: &RecordPartition,
    inner: &Inner,
    dependencies: IndexDependencies,
) -> Result<u64, RecordError> {
    if !inner.healthy {
        return Err(RecordError::RecoveryRequired);
    }
    let cut = inner.state.seq;
    if dependencies == IndexDependencies::None {
        return Ok(cut);
    }
    // Index publication failures still fence the shared index pipeline.
    if partition.indexes.error().is_some() {
        return Err(RecordError::RecoveryRequired);
    }
    // Even lexical-only retrieval uses vector/metadata membership. A pure
    // vector consumer has no reciprocal dependency on the lexical cursor.
    if partition.indexes.vector.load(Ordering::Acquire) != cut
        || (dependencies == IndexDependencies::LexicalWithVectorMetadata
            && partition.indexes.lexical.load(Ordering::Acquire) != cut)
    {
        return Err(RecordError::Busy);
    }
    Ok(cut)
}

fn check_facets(facets: &[FacetFilter]) -> Result<(), EventTimeError> {
    if facets.len() > MAX_FACETS
        || facets.iter().any(|predicate| match predicate {
            FacetFilter::String { filter, .. } | FacetFilter::StringSet { filter, .. } => {
                filter.len() > MAX_FACET_TERMS
                    || filter
                        .iter()
                        .try_fold(0usize, |sum, term| sum.checked_add(term.len()))
                        .is_none_or(|bytes| bytes > 64 * 1024)
            }
            _ => false,
        })
    {
        return Err(EventTimeError::Budget(
            "event-time facet filter exceeds its budget",
        ));
    }
    Ok(())
}

struct FusedHit {
    version: u64,
    score: f32,
    channels: Vec<ChannelExplanation>,
}

fn add_channel(
    fused: &mut BTreeMap<String, FusedHit>,
    id: String,
    version: u64,
    channel: &str,
    rank_basis: &str,
    rank: usize,
    raw_score: Option<f32>,
    constant: f32,
    weight: f32,
) -> Result<(), RecordError> {
    if weight == 0.0 {
        return Ok(());
    }
    let hit = fused.entry(id).or_insert_with(|| FusedHit {
        version,
        score: 0.0,
        channels: Vec::new(),
    });
    if hit.version != version || hit.channels.iter().any(|prior| prior.channel == channel) {
        return Err(RecordError::Busy);
    }
    let contribution = rrf_contribution(weight, constant, rank);
    hit.score += contribution;
    hit.channels.push(ChannelExplanation {
        channel: channel.into(),
        rank,
        rank_basis: rank_basis.into(),
        raw_score,
        rrf_contribution: Some(contribution),
    });
    Ok(())
}

impl RecordPartition {
    pub(crate) async fn query_time_vector(
        &self,
        request: TimeVectorQuery,
        deadline: Deadline,
    ) -> Result<TimeVectorResult, EventTimeError> {
        self.query_time_mixed(request.into(), deadline).await
    }

    pub(crate) async fn query_time_mixed(
        &self,
        request: TimeMixedQuery,
        deadline: Deadline,
    ) -> Result<TimeVectorResult, EventTimeError> {
        let work = Arc::new(QueryWork::new(
            MIXED_WORK,
            MIXED_RANGES,
            MIXED_READ_BYTES,
            deadline,
        ));
        self.query_time_mixed_with_work(request, deadline, work)
            .await
    }

    pub(super) async fn query_time_mixed_with_work(
        &self,
        request: TimeMixedQuery,
        deadline: Deadline,
        work: Arc<QueryWork>,
    ) -> Result<TimeVectorResult, EventTimeError> {
        let TimeMixedQuery {
            window,
            temporal,
            lexical,
            vector,
            facets,
            fields,
            offset,
            length,
            rrf,
        } = request;
        let lanes = 1 + usize::from(lexical.is_some()) + usize::from(vector.is_some());
        if lanes == 1 {
            return Err(
                RecordError::Rejected("mixed query requires a retrieval channel".into()).into(),
            );
        }
        rrf.validate(lexical.is_some(), vector.is_some())
            .map_err(RecordError::Rejected)?;
        let limit = offset
            .checked_add(length)
            .filter(|&window| length > 0 && window <= MAX_RESULTS / lanes)
            .ok_or(EventTimeError::Budget(
                "mixed query requires a positive window and at most 128 total channel candidates",
            ))?;
        let (window, _) = temporal::event_window(TemporalFilter {
            event: Some(window),
            ..TemporalFilter::default()
        })?;
        let temporal = temporal
            .map(temporal::filters)
            .transpose()?
            .unwrap_or_default();
        check_facets(&facets)?;
        let mut retrieval_filters = facets.clone();
        retrieval_filters.extend(temporal.iter().cloned());
        let dependencies = if lexical.is_some() {
            IndexDependencies::LexicalWithVectorMetadata
        } else {
            IndexDependencies::VectorMetadata
        };
        let selection = Selection {
            temporal,
            facets,
            fields: None,
        };
        let (mut view, time) = self
            .capture_and_read(
                window,
                limit,
                dependencies,
                deadline,
                selection,
                Some(work.clone()),
                lanes,
            )
            .await?;
        let mut ranks = Vec::new();
        for (input, channel, weight) in [
            (
                lexical.map(RetrievalLane::Lexical),
                "lexical",
                rrf.keyword_weight,
            ),
            (
                vector.map(RetrievalLane::Vector),
                "vector",
                rrf.vector_weight,
            ),
        ] {
            if let Some(input) = input {
                let (next, candidates) = self
                    .read_retrieval_candidates(
                        view,
                        input,
                        retrieval_filters.clone(),
                        limit,
                        deadline,
                        work.clone(),
                    )
                    .await?;
                view = next;
                ranks.push((candidates, channel, weight));
            }
        }
        let expiry = time
            .hits
            .iter()
            .filter_map(|hit| hit.expires_at_ms)
            .chain(
                ranks
                    .iter()
                    .flat_map(|(hits, _, _)| hits.iter().filter_map(|hit| hit.expiry)),
            )
            .min();
        let mut fused = BTreeMap::new();
        for (rank, hit) in time.hits.iter().enumerate() {
            add_channel(
                &mut fused,
                hit.id.clone(),
                hit.version,
                "time",
                "window_time",
                rank,
                None,
                rrf.constant,
                rrf.time_weight,
            )?;
        }
        for (hits, channel, weight) in ranks {
            for (rank, hit) in hits.into_iter().enumerate() {
                add_channel(
                    &mut fused,
                    hit.id,
                    hit.version,
                    channel,
                    "score",
                    rank,
                    Some(hit.score),
                    rrf.constant,
                    weight,
                )?;
            }
        }
        let mut ranked: Vec<_> = fused.into_iter().collect();
        ranked.sort_unstable_by(|(a, left), (b, right)| {
            right.score.total_cmp(&left.score).then_with(|| a.cmp(b))
        });
        let inner = timeout_at(deadline, self.inner.lock())
            .await
            .map_err(|_| RecordError::Busy)?;
        view.recheck(self, &inner)?;
        let winners = ranked
            .into_iter()
            .skip(offset)
            .take(length)
            .map(|(id, hit)| {
                work.charge(1)?;
                let source = inner.state.records.source(&id).ok_or(RecordError::Busy)?;
                Ok::<_, RecordError>((id, hit, source))
            })
            .collect::<Result<Vec<_>, _>>()?;
        drop(inner);
        let worker = crate::resources::spawn_blocking(move || {
            work.clone().with_reads(|| {
                let hits = winners
                    .into_iter()
                    .map(|(id, hit, source)| {
                        work.charge(0)?;
                        let row = source.find(&id)?.ok_or(RecordError::Busy)?;
                        if !row.visible(view.evaluation) || row.version() != hit.version {
                            return Err(RecordError::Busy);
                        }
                        let record = row.load(&id)?;
                        let record = QueryRecord::copy(&record, Some(&fields))?;
                        Ok(RecordHit {
                            channels: hit.channels,
                            record,
                            score: hit.score,
                        })
                    })
                    .collect::<Result<Vec<_>, RecordError>>()?;
                let output = TimeVectorResult {
                    visible_seq: view.cut,
                    evaluation_time_ms: view.evaluation,
                    count_total: None,
                    hits,
                    delivery_time: view.time_fence(deadline, expiry),
                };
                Ok::<_, RecordError>((view, output))
            })
        });
        let (view, output) = timeout_at(deadline, worker)
            .await
            .map_err(|_| RecordError::Busy)?
            .map_err(storage)??;
        self.deliver_view(view, output, deadline, expiry).await
    }

    pub(crate) async fn query_event_time(
        &self,
        temporal: TemporalFilter,
        facets: Vec<FacetFilter>,
        fields: Vec<String>,
        limit: usize,
        deadline: Deadline,
    ) -> Result<EventTimeResult, EventTimeError> {
        let (window, temporal) = temporal::event_window(temporal)?;
        check_facets(&facets)?;
        let selection = Selection {
            temporal,
            facets,
            fields: Some(fields),
        };
        let window = self
            .read_window(window, limit, IndexDependencies::None, deadline, selection)
            .await?;
        Ok(EventTimeResult {
            visible_seq: window.cut,
            evaluation_time_ms: window.evaluation_time_ms,
            delivery_time: window.delivery_time,
            hits: window
                .hits
                .into_iter()
                .map(|hit| {
                    hit.record
                        .ok_or_else(|| storage("missing event-time output record"))
                })
                .collect::<Result<_, _>>()?,
            _memory: window._memory,
        })
    }

    /// Select one exact common cut for this native consumer. Callers supply
    /// the existing request deadline.
    #[cfg(test)]
    pub(crate) async fn read_event_time_window(
        &self,
        window: Range<i64>,
        limit: usize,
        dependencies: IndexDependencies,
        deadline: Deadline,
    ) -> Result<Window, RecordError> {
        self.read_window(window, limit, dependencies, deadline, Selection::default())
            .await
            .map_err(EventTimeError::record_error)
    }

    #[cfg(test)]
    pub(crate) async fn probe_event_time_vector_cut(
        &self,
        window: Range<i64>,
        limit: usize,
        work_limit: usize,
        facets: Vec<FacetFilter>,
        deadline: Deadline,
    ) -> Result<(Window, Vec<String>, usize), RecordError> {
        let work = Arc::new(QueryWork::new(
            work_limit,
            MIXED_RANGES,
            MIXED_READ_BYTES,
            deadline,
        ));
        let (view, output) = self
            .capture_and_read(
                window,
                limit,
                IndexDependencies::VectorMetadata,
                deadline,
                Selection {
                    facets: facets.clone(),
                    ..Selection::default()
                },
                Some(work.clone()),
                2,
            )
            .await
            .map_err(EventTimeError::record_error)?;
        let (view, candidates) = self
            .read_retrieval_candidates(
                view,
                RetrievalLane::Vector(Embedding::F32(vec![1.0, 0.0])),
                facets,
                limit,
                deadline,
                work.clone(),
            )
            .await?;
        let expiry = candidates
            .iter()
            .filter_map(|candidate| candidate.expiry)
            .min();
        let mut ids: Vec<_> = candidates
            .into_iter()
            .map(|candidate| candidate.id)
            .collect();
        ids.sort_unstable();
        let window = self
            .deliver_window(view, output, deadline, expiry)
            .await
            .map_err(EventTimeError::record_error)?;
        Ok((window, ids, work_limit - work.remaining()))
    }

    // Exercise real native postings at the common cut without exposing the
    // lexical scorer before its logical work has a complete budget.
    #[cfg(test)]
    pub(super) async fn probe_event_time_lexical_reads(
        &self,
        window: Range<i64>,
        text: String,
        facets: Vec<FacetFilter>,
        deadline: Deadline,
        work: Arc<QueryWork>,
    ) -> Result<(Window, crate::search::ResultObject, (usize, usize)), RecordError> {
        let (view, output) = self
            .capture_and_read(
                window,
                1,
                IndexDependencies::LexicalWithVectorMetadata,
                deadline,
                Selection {
                    facets: facets.clone(),
                    ..Selection::default()
                },
                Some(work.clone()),
                2,
            )
            .await
            .map_err(EventTimeError::record_error)?;
        let after_time = work.remaining_io();
        let (view, scored) = Self::score_retrieval_lane(
            view,
            RetrievalLane::Lexical(text),
            facets,
            1,
            deadline,
            work,
        )
        .await?;
        let window = self
            .deliver_window(view, output, deadline, None)
            .await
            .map_err(EventTimeError::record_error)?;
        Ok((window, scored, after_time))
    }

    async fn score_retrieval_lane(
        view: View,
        lane: RetrievalLane,
        facets: Vec<FacetFilter>,
        limit: usize,
        deadline: Deadline,
        work: Arc<QueryWork>,
    ) -> Result<(View, crate::search::ResultObject), RecordError> {
        let runtime = tokio::runtime::Handle::current();
        let scorer_work = work.clone();
        let worker = crate::resources::spawn_blocking(move || {
            #[cfg(test)]
            query::blocking_pause(
                &view.root,
                match &lane {
                    RetrievalLane::Vector(_) => ".test-event-time-vector-worker-pause",
                    RetrievalLane::Lexical(_) => ".test-event-time-lexical-worker-pause",
                },
            )?;
            let result = scorer_work.clone().with_reads(|| {
                runtime.block_on(async {
                    let mut filters = facets;
                    filters.push(temporal::expiry_filter(view.evaluation));
                    let index = &view.retrieval.as_ref().unwrap().index;
                    timeout_at(deadline, async move {
                        match lane {
                            RetrievalLane::Lexical(text) => {
                                lexical::runtime_query::search(
                                    index,
                                    text,
                                    None,
                                    SearchMode::Lexical,
                                    QueryType::Union,
                                    0,
                                    limit,
                                    ResultType::TopkCount,
                                    vec![],
                                    filters,
                                    vec![],
                                    lexical::Cut {
                                        sequence: view.cut,
                                        evaluation_time_ms: view.evaluation,
                                    },
                                )
                                .await
                            }
                            RetrievalLane::Vector(vector) => {
                                crate::search::search_with_counts(
                                    index,
                                    String::new(),
                                    Some(vector),
                                    SearchMode::Vector {
                                        similarity_threshold: None,
                                        ann_mode: crate::vector_similarity::AnnMode::All,
                                    },
                                    0,
                                    limit,
                                    ResultType::TopkCount,
                                    true,
                                    vec![],
                                    filters,
                                    vec![],
                                    &mut Vec::new(),
                                    Some(scorer_work),
                                )
                                .await
                            }
                        }
                    })
                    .await
                    .map_err(|_| RecordError::Busy)?
                })
            });
            result.map(|result| (view, result))
        });
        timeout_at(deadline, worker)
            .await
            .map_err(|_| RecordError::Busy)?
            .map_err(storage)?
    }

    async fn read_retrieval_candidates(
        &self,
        view: View,
        lane: RetrievalLane,
        facets: Vec<FacetFilter>,
        limit: usize,
        deadline: Deadline,
        work: Arc<QueryWork>,
    ) -> Result<(View, Vec<query::RetrievalEvidence>), RecordError> {
        let (view, scored) =
            Self::score_retrieval_lane(view, lane, facets, limit, deadline, work.clone()).await?;
        let hits = scored.results;
        let candidates = timeout_at(deadline, view.retrieval.as_ref().unwrap().candidates(hits))
            .await
            .map_err(|_| RecordError::Busy)??;
        let inner = timeout_at(deadline, self.inner.lock())
            .await
            .map_err(|_| RecordError::Busy)?;
        view.recheck(self, &inner)?;
        let mut pinned = Vec::with_capacity(candidates.len());
        for (rank, candidate) in candidates.into_iter().enumerate() {
            work.charge(1)?;
            pinned.push(
                timeout_at(deadline, candidate.pin(rank, &inner))
                    .await
                    .map_err(|_| RecordError::Busy)??
                    .ok_or(RecordError::Busy)?,
            );
        }
        drop(inner);
        let worker = crate::resources::spawn_blocking(move || {
            work.clone().with_reads(|| {
                let mut evidence = Vec::with_capacity(pinned.len());
                for candidate in pinned {
                    work.charge(0)?;
                    evidence.push(candidate.strict_evidence(view.evaluation)?);
                }
                Ok::<_, RecordError>((view, evidence))
            })
        });
        timeout_at(deadline, worker)
            .await
            .map_err(|_| RecordError::Busy)?
            .map_err(storage)?
    }

    async fn read_window(
        &self,
        window: Range<i64>,
        limit: usize,
        dependencies: IndexDependencies,
        deadline: Deadline,
        selection: Selection,
    ) -> Result<Window, EventTimeError> {
        let (view, output) = self
            .capture_and_read(window, limit, dependencies, deadline, selection, None, 1)
            .await?;
        self.deliver_window(view, output, deadline, None).await
    }

    async fn capture_and_read(
        &self,
        window: Range<i64>,
        limit: usize,
        dependencies: IndexDependencies,
        deadline: Deadline,
        selection: Selection,
        work: Option<Arc<crate::search::QueryWork>>,
        lanes: usize,
    ) -> Result<(View, Window), EventTimeError> {
        if window.is_empty() || limit == 0 || limit > MAX_RESULTS {
            return Err(
                RecordError::Rejected("invalid event-time window or result limit".into()).into(),
            );
        }
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let mut inner = timeout_at(deadline, self.inner.lock())
            .await
            .map_err(|_| RecordError::Busy)?;
        if !selection.facets.is_empty() {
            crate::metadata_filter::validate_record_filters(
                &inner.config.schema,
                &selection.facets,
            )
            .map_err(RecordError::Rejected)?;
        }
        if work.is_some() && (inner.archive.is_some() || inner.remote.is_some()) {
            return Err(EventTimeError::Unavailable(
                "mixed event-time queries require local storage without archive",
            ));
        }
        if limit > inner.config.max_candidates / lanes {
            return Err(EventTimeError::Budget(
                "combined candidate window exceeds the collection budget",
            ));
        }
        let view = timeout_at(
            deadline,
            View::capture(self, &mut inner, admission, limit, dependencies),
        )
        .await
        .map_err(|_| RecordError::Busy)??;
        drop(inner);

        let task = crate::resources::spawn_blocking(move || {
            let output = match work {
                Some(work) => work
                    .clone()
                    .with_reads(|| view.read(window, limit, deadline, &selection, Some(&work)))?,
                None => view.read(window, limit, deadline, &selection, None)?,
            };
            Ok::<_, EventTimeError>((view, output))
        });
        timeout_at(deadline, task)
            .await
            .map_err(|_| RecordError::Busy)?
            .map_err(storage)?
    }

    async fn deliver_window(
        &self,
        view: View,
        output: Window,
        deadline: Deadline,
        additional_expiry: Option<u64>,
    ) -> Result<Window, EventTimeError> {
        let expiry = additional_expiry
            .into_iter()
            .chain(output.hits.iter().filter_map(|hit| hit.expires_at_ms))
            .min();
        self.deliver_view(view, output, deadline, expiry).await
    }

    async fn deliver_view<T>(
        &self,
        view: View,
        output: T,
        deadline: Deadline,
        expiry: Option<u64>,
    ) -> Result<T, EventTimeError> {
        #[cfg(test)]
        pause_delivery(&self.inner, deadline).await;
        let mut inner = timeout_at(deadline, self.inner.lock())
            .await
            .map_err(|_| RecordError::Busy)?;
        let evaluation = inner.observe_time()?;
        if Deadline::now() >= deadline {
            return Err(RecordError::Busy.into());
        }
        view.recheck(self, &inner)?;
        if expiry.is_some_and(|expiry| expiry <= evaluation) {
            return Err(RecordError::Busy.into());
        }
        Ok(view.barrier.deliver(|| output)?)
    }
}

impl View {
    async fn capture(
        partition: &RecordPartition,
        inner: &mut Inner,
        admission: OwnedSemaphorePermit,
        limit: usize,
        dependencies: IndexDependencies,
    ) -> Result<Self, EventTimeError> {
        let evaluation = inner.observe_time()?;
        let observed_at = Deadline::now();
        let cut = published_cut(partition, inner, dependencies)?;
        let records = &inner.state.records;
        let base = records
            .event_time_base()
            .ok_or(EventTimeError::Unavailable(
                "no projection in the selected base",
            ))?;
        let anchor = inner
            .anchor
            .as_ref()
            .ok_or(EventTimeError::Unavailable("missing certified manifest"))?;
        let certified = anchor
            .checkpoint_state()
            .map_err(|_| EventTimeError::Unavailable("missing certified cut"))?;
        // Recovery and checkpoint publication install one shared base both in
        // the authenticated State and its post-certificate overlay.
        if !certified
            .records
            .event_time_base()
            .is_some_and(|pinned| Arc::ptr_eq(pinned, base))
        {
            return Err(EventTimeError::Unavailable(
                "the current overlay is not attached to the certified base",
            ));
        }
        let base_cut = base.event_time.as_ref().expect("selected projection").cut();
        if base_cut > certified.seq || certified.seq > cut {
            return Err(EventTimeError::Unavailable(
                "base, manifest, and published cuts are inconsistent",
            ));
        }
        if cut - base_cut > MAX_SUFFIX_SEQUENCES {
            return Err(EventTimeError::Budget("sequence suffix exceeds its budget"));
        }
        if limit > inner.config.max_candidates {
            return Err(EventTimeError::Budget(
                "event-time limit exceeds the collection candidate budget",
            ));
        }
        let recent = records.recent_records();
        if recent.len() > MAX_OVERLAY_ROWS {
            return Err(EventTimeError::Budget(
                "record suffix exceeds its row budget",
            ));
        }
        let overlay_bytes = recent
            .map(|row| row.wire_bytes())
            .try_fold(0usize, |sum, n| {
                sum.checked_add(n)
                    .filter(|bytes| *bytes <= MAX_OVERLAY_BYTES)
                    .ok_or(EventTimeError::Budget(
                        "record suffix exceeds its byte budget",
                    ))
            })?;
        let count = records.recent_records().len();
        let buffers =
            1024 + count * (size_of::<state_memory::Shared<Record>>() + size_of::<TailHit<'_>>());
        let memory = reserve_memory(buffers as u64).map_err(memory_error)?;
        let scope = crate::resources::Scope::current()
            .map(|scope| scope.bytes((buffers + overlay_bytes) as u64))
            .transpose()
            .map_err(|_| RecordError::Busy)?;
        // Capture the ordered component handles under the same Record guard
        // as the authenticated base and suffix. Their existing owners pin
        // the files even if a concurrent compaction retires the view.
        let retrieval = if dependencies == IndexDependencies::None {
            None
        } else {
            Some(inner.index.capture_query(limit).await?)
        };
        let view = Self {
            #[cfg(test)]
            root: inner.journal.root().to_owned(),
            cut,
            evaluation,
            clock: inner.clock.clone(),
            observed_time: inner.observed_time.clone(),
            certified_floor: inner.state.time_watermark_ms,
            observed_at,
            anchor: anchor.selected_owner().to_owned(),
            group_revision: inner.state.group_publication_revision(),
            base: base.clone(),
            dependencies,
            retrieval,
            suffix: records.recent_records().cloned().collect(),
            max_base_visits: MAX_BASE_VISITS.min(inner.config.max_candidates),
            barrier: partition.output_permit()?,
            _memory: memory,
            _scope: scope,
            _admission: admission,
        };
        view.recheck(partition, inner)?;
        Ok(view)
    }

    fn recheck(&self, partition: &RecordPartition, inner: &Inner) -> Result<(), RecordError> {
        if published_cut(partition, inner, self.dependencies)? != self.cut
            || inner.state.group_publication_revision() != self.group_revision
            || inner
                .anchor
                .as_ref()
                .is_none_or(|anchor| anchor.selected_owner() != self.anchor)
            || !inner
                .state
                .records
                .event_time_base()
                .is_some_and(|base| Arc::ptr_eq(base, &self.base))
        {
            return Err(RecordError::Busy);
        }
        if let Some(retrieval) = &self.retrieval
            && !inner.index.matches_query_topology(retrieval)
        {
            return Err(RecordError::Busy);
        }
        Ok(())
    }

    fn time_fence(&self, deadline: Deadline, expires: Option<u64>) -> output::ReadTimeFence {
        output::ReadTimeFence {
            clock: self.clock.clone(),
            observed: self.observed_time.clone(),
            certified_floor: self.certified_floor,
            deadline,
            expires,
            monotonic_expiry: expires.and_then(|expiry| {
                self.observed_at
                    .checked_add(std::time::Duration::from_millis(
                        expiry.saturating_sub(self.evaluation),
                    ))
            }),
        }
    }

    fn read(
        &self,
        window: Range<i64>,
        limit: usize,
        deadline: Deadline,
        selection: &Selection,
        work: Option<&crate::search::QueryWork>,
    ) -> Result<Window, EventTimeError> {
        let mut suffix_hits = Vec::with_capacity(self.suffix.len());
        for row in &self.suffix {
            if let Some(work) = work {
                work.charge(1)?;
            }
            if Deadline::now() >= deadline {
                return Err(RecordError::Busy.into());
            }
            if let Some(time) = row
                .temporal
                .event_time_ms
                .filter(|time| window.contains(time))
                && temporal::visible(row, self.evaluation)
                && temporal::matches_indexed_time(
                    &selection.temporal,
                    &row.temporal,
                    row.ingested_at_ms,
                )
                && selection.matches_facets(row.document.as_ref(), deadline, work)?
            {
                suffix_hits.push(TailHit { time, row });
            }
        }
        suffix_hits.sort_unstable_by(|a, b| a.rank().cmp(&b.rank()));
        let memory = reserve_memory((2 * limit * size_of::<Hit>()) as u64).map_err(memory_error)?;
        let mut output = OutputBudget::default();
        let mut base_hits = Vec::with_capacity(limit);
        let mut examined = 0;
        self.base
            .event_time
            .as_ref()
            .expect("selected projection")
            .visit(
                window,
                |error| EventTimeError::from(state_rows::read_error(error)),
                |id, time, reference| {
                    if let Some(work) = work {
                        work.charge(1)?;
                    }
                    if Deadline::now() >= deadline {
                        return Err(RecordError::Busy.into());
                    }
                    if examined >= self.max_base_visits {
                        return Err(EventTimeError::Budget("base candidate budget exhausted"));
                    }
                    examined += 1;
                    if self
                        .suffix
                        .binary_search_by(|row| row.record_id.as_str().cmp(id))
                        .is_ok()
                    {
                        return Ok(true);
                    }
                    let row = event_time::locate(
                        &self.base.checkpoint,
                        id,
                        time,
                        reference,
                        self.evaluation,
                    )
                    .map_err(state_rows::read_error)?;
                    if row.metadata.visible(self.evaluation)
                        && temporal::matches_indexed_time(
                            &selection.temporal,
                            &row.metadata.temporal,
                            row.metadata.ingested_at_ms,
                        )
                        && let Some(hit) =
                            output.sealed_hit(id, time, row, selection, deadline, work)?
                    {
                        base_hits.push(hit);
                    }
                    Ok(base_hits.len() < limit)
                },
            )?;

        let mut base = base_hits.into_iter().peekable();
        let mut tail = suffix_hits.into_iter().peekable();
        let mut hits = Vec::with_capacity(limit);
        while hits.len() < limit {
            match (base.peek(), tail.peek()) {
                (None, None) => break,
                (Some(disk), Some(recent)) if recent.rank() < disk.rank() => {
                    hits.push(tail.next().unwrap().copy(&mut output, selection)?);
                }
                (Some(_), _) => hits.push(base.next().unwrap()),
                (None, Some(_)) => hits.push(tail.next().unwrap().copy(&mut output, selection)?),
            }
        }
        Ok(Window {
            cut: self.cut,
            evaluation_time_ms: self.evaluation,
            delivery_time: self.time_fence(
                deadline,
                hits.iter().filter_map(|hit| hit.expires_at_ms).min(),
            ),
            hits,
            _memory: memory,
        })
    }
}

impl Hit {
    fn rank(&self) -> (Reverse<i64>, &str) {
        (Reverse(self.event_time_ms), &self.id)
    }
}

struct TailHit<'a> {
    time: i64,
    row: &'a Record,
}

impl TailHit<'_> {
    fn rank(&self) -> (Reverse<i64>, &str) {
        (Reverse(self.time), &self.row.record_id)
    }

    fn copy(self, output: &mut OutputBudget, selection: &Selection) -> Result<Hit, EventTimeError> {
        output.hit(
            &self.row.record_id,
            self.time,
            self.row.version,
            self.row.temporal.expire_at_ms,
            selection
                .fields
                .as_ref()
                .map(|fields| QueryRecord::copy(self.row, Some(fields)))
                .transpose()?,
        )
    }
}

#[derive(Default)]
struct OutputBudget {
    bytes: usize,
    body_bytes: u64,
}

impl OutputBudget {
    fn sealed_hit(
        &mut self,
        id: &str,
        time: i64,
        row: crate::storage::disk_records::Located<'_>,
        selection: &Selection,
        deadline: Deadline,
        work: Option<&QueryWork>,
    ) -> Result<Option<Hit>, EventTimeError> {
        let version = row.metadata.version;
        let expiry = row.metadata.temporal.expire_at_ms;
        if selection.facets.is_empty() && selection.fields.is_none() {
            return self.hit(id, time, version, expiry, None).map(Some);
        }
        let payload = row
            .sealed_payload()
            .ok_or_else(|| storage("missing live event-time payload"))?;
        self.body_bytes = self
            .body_bytes
            .checked_add(payload.range.length)
            .filter(|bytes| *bytes <= MAX_BODY_BYTES)
            .ok_or(EventTimeError::Budget("candidate payload budget exhausted"))?;
        let output = state_rows::decode(id, row, |record| {
            if !selection.matches_facets(record.document.as_ref(), deadline, work)? {
                return Ok(None);
            }
            let output = selection
                .fields
                .as_ref()
                .map(|fields| QueryRecord::copy(&record, Some(fields)))
                .transpose()?;
            Ok(Some(output))
        })?;
        output
            .map(|record| self.hit(id, time, version, expiry, record))
            .transpose()
    }

    fn hit(
        &mut self,
        id: &str,
        time: i64,
        version: u64,
        expires_at_ms: Option<u64>,
        record: Option<QueryRecord>,
    ) -> Result<Hit, EventTimeError> {
        self.bytes = self
            .bytes
            .checked_add(id.len())
            .filter(|sum| *sum <= MAX_RESULT_ID_BYTES)
            .ok_or(EventTimeError::Budget("result identity budget exhausted"))?;
        let memory = reserve_memory(id.len() as u64).map_err(memory_error)?;
        Ok(Hit {
            id: id.to_owned(),
            event_time_ms: time,
            version,
            expires_at_ms,
            record,
            _id: memory,
        })
    }
}

// Hold the native result after releasing its read worker so the tests can
// publish another real checkpoint at the same sequence before delivery.
#[cfg(test)]
async fn pause_delivery(inner: &Mutex<Inner>, deadline: Deadline) {
    let root = inner.lock().await.journal.root().to_owned();
    let gate = root.join(".test-event-time-delivery-pause");
    if gate.exists() {
        std::fs::write(root.join(".test-event-time-delivery-started"), b"ready").unwrap();
        while gate.exists() && Deadline::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
