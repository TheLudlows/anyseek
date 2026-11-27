//! Native consumer of one certified event-time base and its canonical suffix.
//! The suffix is the existing materialized Record overlay. It already covers
//! every selected manifest delta and the committed WAL after that manifest.
use super::*;
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

struct View {
    cut: u64,
    evaluation: u64,
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
    fn record_error(self) -> RecordError {
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

impl RecordPartition {
    pub(crate) async fn query_event_time(
        &self,
        temporal: TemporalFilter,
        facets: Vec<FacetFilter>,
        fields: Vec<String>,
        limit: usize,
        deadline: Deadline,
    ) -> Result<EventTimeResult, EventTimeError> {
        let (window, temporal) = temporal::event_window(temporal)?;
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

    // Exercise vector scoring on exactly the handles shared with the time
    // window. The probe returns identities only, without a mixed API.
    #[cfg(test)]
    pub(crate) async fn probe_event_time_vector_cut(
        &self,
        window: Range<i64>,
        limit: usize,
        deadline: Deadline,
    ) -> Result<(Window, Vec<String>), RecordError> {
        let (view, output) = self
            .capture_and_read(
                window,
                limit,
                IndexDependencies::VectorMetadata,
                deadline,
                Selection::default(),
            )
            .await
            .map_err(EventTimeError::record_error)?;
        let retrieval = view.retrieval.as_ref().unwrap();
        let result = timeout_at(
            deadline,
            lexical::runtime_query::search(
                &retrieval.index,
                String::new(),
                Some(Embedding::F32(vec![1.0, 0.0])),
                SearchMode::Vector {
                    similarity_threshold: None,
                    ann_mode: crate::vector_similarity::AnnMode::All,
                },
                QueryType::Union,
                0,
                limit,
                ResultType::TopkCount,
                vec![],
                vec![temporal::expiry_filter(view.evaluation)],
                vec![],
                lexical::Cut {
                    sequence: view.cut,
                    evaluation_time_ms: view.evaluation,
                },
            ),
        )
        .await
        .map_err(|_| RecordError::Busy)??;
        let mut ids = timeout_at(deadline, retrieval.candidate_ids(result.results))
            .await
            .map_err(|_| RecordError::Busy)??;
        ids.sort_unstable();
        let window = self
            .deliver_window(view, output, deadline)
            .await
            .map_err(EventTimeError::record_error)?;
        Ok((window, ids))
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
            .capture_and_read(window, limit, dependencies, deadline, selection)
            .await?;
        self.deliver_window(view, output, deadline).await
    }

    async fn capture_and_read(
        &self,
        window: Range<i64>,
        limit: usize,
        dependencies: IndexDependencies,
        deadline: Deadline,
        selection: Selection,
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
        let view = timeout_at(
            deadline,
            View::capture(self, &mut inner, admission, limit, dependencies),
        )
        .await
        .map_err(|_| RecordError::Busy)??;
        drop(inner);

        let task = crate::resources::spawn_blocking(move || {
            let output = view.read(window, limit, deadline, &selection)?;
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
    ) -> Result<Window, EventTimeError> {
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
        if output
            .hits
            .iter()
            .any(|hit| hit.expires_at_ms.is_some_and(|expiry| expiry <= evaluation))
        {
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
            cut,
            evaluation,
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

    fn read(
        &self,
        window: Range<i64>,
        limit: usize,
        deadline: Deadline,
        selection: &Selection,
    ) -> Result<Window, EventTimeError> {
        let mut suffix_hits = Vec::with_capacity(self.suffix.len());
        for row in &self.suffix {
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
                && (selection.facets.is_empty()
                    || row.document.as_ref().is_some_and(|doc| {
                        crate::metadata_filter::matches_document(doc, &selection.facets)
                    }))
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
                        && let Some(hit) = output.sealed_hit(id, time, row, selection)?
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
            if !selection.facets.is_empty()
                && !record.document.as_ref().is_some_and(|doc| {
                    crate::metadata_filter::matches_document(doc, &selection.facets)
                })
            {
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
