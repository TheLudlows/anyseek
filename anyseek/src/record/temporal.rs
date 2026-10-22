//! Trusted evaluation time and durable, irreversible TTL transitions. Expiration
//! is an owned partition command, never a read-time filter with stale statistics.
use super::*;
use std::collections::BTreeSet;

/// UTC Unix milliseconds. Business time does not implicitly create a TTL.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TemporalMetadata {
    /// Caller-supplied event time, distinct from ingestion time and sequence.
    pub event_time_ms: Option<i64>,
    /// Inclusive business-valid start.
    pub valid_from_ms: Option<i64>,
    /// Exclusive business-valid end.
    pub valid_to_ms: Option<i64>,
    /// Irreversible expiration at evaluation_time >= this deadline.
    pub expire_at_ms: Option<u64>,
}
const MAX_TIME: u64 = 253_402_300_799_999;
const MIN_TIME: i64 = -62_135_596_800_000;
const CLOCK_TOLERANCE: u64 = 300_000;
/// Replaceable trusted clock; production uses wall time checked against monotonic elapsed time.
pub(super) trait Clock: Send + Sync {
    fn now_ms(&self) -> Result<u64, RecordError>;
}
struct SystemClock {
    wall: u64,
    start: std::time::Instant,
}
fn wall_ms() -> Result<u64, RecordError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(storage)?
            .as_millis(),
    )
    .map_err(storage)
}
impl Clock for SystemClock {
    fn now_ms(&self) -> Result<u64, RecordError> {
        #[cfg(feature = "fault-injection")]
        if let Ok(path) = std::env::var("ANYSEEK_TEST_CLOCK_FILE") {
            return std::fs::read_to_string(path)
                .map_err(storage)?
                .trim()
                .parse()
                .map_err(storage);
        }
        let now = wall_ms()?;
        let expected = self
            .wall
            .saturating_add(self.start.elapsed().as_millis().min(u64::MAX as u128) as u64);
        if now.abs_diff(expected) > CLOCK_TOLERANCE {
            return Err(RecordError::ClockUncertain);
        }
        Ok(now)
    }
}
pub(super) fn clock() -> Result<Arc<dyn Clock>, RecordError> {
    Ok(Arc::new(SystemClock {
        wall: wall_ms()?,
        start: std::time::Instant::now(),
    }))
}
pub(super) fn validate(value: &TemporalMetadata, evaluation: u64) -> Result<(), RecordError> {
    for time in [value.event_time_ms, value.valid_from_ms, value.valid_to_ms]
        .into_iter()
        .flatten()
    {
        if !(MIN_TIME..=MAX_TIME as i64).contains(&time) {
            return Err(RecordError::Rejected(
                "time outside UTC years 1..9999 in milliseconds".into(),
            ));
        }
    }
    if value
        .valid_from_ms
        .zip(value.valid_to_ms)
        .is_some_and(|(a, b)| a >= b)
    {
        return Err(RecordError::Rejected(
            "valid interval must be nonempty [from,to)".into(),
        ));
    }
    if value
        .expire_at_ms
        .is_some_and(|t| t <= evaluation || t > MAX_TIME)
    {
        return Err(RecordError::Rejected(
            "TTL must be strictly after trusted evaluation time".into(),
        ));
    }
    Ok(())
}
pub(super) fn retained(
    previous: Option<&Record>,
    request: &RecordWrite,
    evaluation: u64,
) -> Result<TemporalMetadata, RecordError> {
    if previous.is_some_and(|r| r.expired_at_ms.is_some()) {
        return Err(RecordError::Conflict(
            "expired identity cannot be revived; use a new identity".into(),
        ));
    }
    if request.temporal.is_some()
        && (request.document.is_none()
            || request.vector_patch.is_some()
            || request.generation.is_some())
    {
        return Err(RecordError::Rejected(
            "temporal metadata requires a complete body write".into(),
        ));
    }
    let value = request
        .temporal
        .clone()
        .or_else(|| previous.map(|r| r.temporal.clone()))
        .unwrap_or_default();
    if value.expire_at_ms.is_some()
        && (request.record_id.len() > 256 || request.routing_key.len() > 256)
    {
        return Err(RecordError::Rejected(
            "TTL identity exceeds reserved expiration budget".into(),
        ));
    }
    if request.document.is_some() || request.vector_patch.is_some() {
        validate(&value, evaluation)?;
    }
    Ok(value)
}
pub(super) fn protected(record: &Record) -> bool {
    record.system.is_some()
        || record.temporal.expire_at_ms.is_some()
        || record.expired_at_ms.is_some()
}
pub(super) fn live_ttl(record: &Record) -> bool {
    record.document.is_some() && record.temporal.expire_at_ms.is_some()
}
pub(super) fn reserve_bytes(state: &State) -> usize {
    (state.records.values().filter(|r| live_ttl(r)).count() + generation::staged_ttl_count(state))
        * 8192
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ExpiredTarget {
    record_id: String,
    previous_version: u64,
}
/// A single committed expiration cause; future change feed can project this exact seq.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Expiration {
    pub(super) seq: u64,
    pub(super) evaluation_time_ms: u64,
    targets: Vec<ExpiredTarget>,
}
pub(super) fn apply(state: &mut State, event: &Expiration) -> Result<(), RecordError> {
    if event.seq != state.seq + 1
        || event.evaluation_time_ms < state.time_watermark_ms
        || event.targets.is_empty()
    {
        return Err(storage("inconsistent expiration command"));
    }
    let mut seen = BTreeSet::new();
    for target in &event.targets {
        let record = state
            .records
            .get_mut(&target.record_id)
            .ok_or_else(|| storage("expired identity absent"))?;
        if !seen.insert(&target.record_id)
            || record.version != target.previous_version
            || !live_ttl(record)
            || record.expired_at_ms.is_some()
            || record.temporal.expire_at_ms.unwrap() > event.evaluation_time_ms
        {
            return Err(storage("inconsistent expiration target"));
        }
        if evidence::retained_revision(record) {
            state.source_revisions.push(record.clone());
        }
        record.version = record
            .version
            .checked_add(1)
            .ok_or_else(|| storage("version exhausted"))?;
        record.document = None;
        record.expired_at_ms = Some(event.evaluation_time_ms);
    }
    state.seq = event.seq;
    state.time_watermark_ms = event.evaluation_time_ms;
    state.expirations.push(event.clone());
    Ok(())
}
impl Inner {
    pub(super) async fn advance_time(&mut self) -> Result<(), RecordError> {
        if !self.healthy || self.state.sealed_for.is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        let now = self.clock.now_ms()?;
        if now > MAX_TIME || now.saturating_add(CLOCK_TOLERANCE) < self.state.time_watermark_ms {
            return Err(RecordError::ClockUncertain);
        }
        let evaluation = now
            .max(self.evaluation_time_ms)
            .max(self.state.time_watermark_ms);
        // Archived authority is immutable. Protected bodies remain quarantined;
        // only certified takeover creates a live owner that may expire them.
        if self.state.independent_snapshot {
            self.evaluation_time_ms = evaluation;
            return Ok(());
        }
        self.expire_staging(evaluation).await?;
        self.cleanup_private().await?;
        self.trim_feed(evaluation)?;
        let targets: Vec<_> = self
            .state
            .records
            .values()
            .filter(|r| live_ttl(r) && r.temporal.expire_at_ms.unwrap() <= evaluation)
            .map(|r| ExpiredTarget {
                record_id: r.record_id.clone(),
                previous_version: r.version,
            })
            .collect();
        if targets.is_empty() {
            self.evaluation_time_ms = evaluation;
            return Ok(());
        }
        let event = Expiration {
            seq: self
                .state
                .seq
                .checked_add(1)
                .ok_or_else(|| storage("sequence exhausted"))?,
            evaluation_time_ms: evaluation,
            targets,
        };
        let mut candidate = self.state.clone();
        apply(&mut candidate, &event)?;
        let snapshot_bytes = encode_entry(&Entry::Checkpoint {
            config: self.config.clone(),
            state: candidate.clone(),
        })?
        .len();
        let feed_point = change_feed::pending_expiry(&self.state,event.seq,event.evaluation_time_ms)?;
        self.feed_cache.validate(&self.config.change_feed,Some(&feed_point),change_feed::reserve(&candidate))?;
        let reserve = generation::reserve_bytes(&candidate);
        PartitionJournal::validate_initial(snapshot_bytes + reserve, self.config.max_journal_bytes)
            .map_err(storage)?;
        let bytes = encode_entry(&Entry::Expired(event.clone()))?;
        self.journal
            .validate_append_with_reserve(bytes.len(), reserve)
            .map_err(storage)?;
        self.healthy = false;
        self.output
            .write()
            .map_err(|_| RecordError::RecoveryRequired)?
            .fenced = true;
        self.journal
            .append(&bytes)
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
        #[cfg(feature = "fault-injection")]
        match std::env::var("ANYSEEK_TEMPORAL_FAULT").ok().as_deref() {
            Some("after_append_unknown") => {
                return Err(RecordError::OutcomeUnknown(
                    "injected expiration append".into(),
                ));
            }
            Some("after_durable") => return Err(RecordError::DurablePending(event.seq)),
            Some("crash_after_durable") => std::process::exit(86),
            _ => {}
        }
        #[cfg(test)]
        if self.fail_after_durable {
            return Err(RecordError::DurablePending(event.seq));
        }
        let visible = evidence::visibility(&candidate.records)
            .map_err(|_| RecordError::DurablePending(event.seq))?;
        let retired: Vec<_> = self
            .locations
            .keys()
            .filter(|id| visible.get(*id) != Some(&true))
            .cloned()
            .collect();
        for id in retired {
            let ordinal = self.locations.remove(&id).unwrap();
            self.index.retire(ordinal).await.map_err(|_|RecordError::DurablePending(event.seq))?;
        }
        self.state = candidate;
        self.feed_cache.push(Some(feed_point));
        self.snapshot_bytes = snapshot_bytes;
        self.evaluation_time_ms = evaluation;
        self.output
            .write()
            .map_err(|_| RecordError::DurablePending(event.seq))?
            .fenced = false;
        self.healthy = true;
        #[cfg(feature = "fault-injection")]
        if std::env::var("ANYSEEK_TEMPORAL_FAULT").ok().as_deref() == Some("crash_after_visible")
        {
            std::process::exit(86);
        }
        Ok(())
    }
}
impl RecordPartition {
    /// Drain due expirations durably before issuing a new data view. Cancellation
    /// leaves the owned executor running. Existing fixed-time views may finish.
    pub async fn advance_time(&self) -> Result<(), RecordError> {
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let owner = self.inner.clone();
        crate::INDEX_RUNTIME
            .spawn(async move {
                let _permit = admission;
                owner.lock().await.advance_time().await
            })
            .await
            .map_err(storage)?
    }
}

/// A half-open UTC millisecond range. Missing endpoints are unbounded.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeRange {
    /// Inclusive lower boundary.
    pub from_ms: Option<i64>,
    /// Exclusive upper boundary.
    pub to_ms: Option<i64>,
}
/// Predicates ANDed before retrieval; business time never changes TTL evaluation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TemporalFilter {
    /// Records without event time do not match this predicate.
    pub event: Option<TimeRange>,
    /// Server-assigned body ingestion time.
    pub ingested: Option<TimeRange>,
    /// Point within the business-valid half-open interval; absent bounds are unbounded.
    pub valid_at_ms: Option<i64>,
}
const PREFIX: &str = "__seekpower_time_";
const FIELDS: [&str; 4] = [
    "__seekpower_time_event",
    "__seekpower_time_ingested",
    "__seekpower_time_from",
    "__seekpower_time_to",
];
pub(super) fn reserved(name: &str) -> bool {
    name.starts_with(PREFIX)
}
pub(super) fn schema(config: &RecordConfig) -> Vec<SchemaField> {
    let mut schema = config.schema.clone();
    for field in FIELDS {
        schema.push(serde_json::from_value(serde_json::json!({"field":field,"field_type":"I64","store":false,"index_lexical":false,"facet":true})).expect("internal time schema"));
    }
    schema
}
pub(super) fn indexed(document: &Document, metadata: &TemporalMetadata, ingested: u64) -> Document {
    let mut document = document.clone();
    for (field, value) in FIELDS.into_iter().zip([
        metadata.event_time_ms.unwrap_or(MIN_TIME - 1),
        ingested as i64,
        metadata.valid_from_ms.unwrap_or(MIN_TIME),
        metadata.valid_to_ms.unwrap_or(MAX_TIME as i64 + 1),
    ]) {
        document.insert(field.into(), value.into());
    }
    document
}
pub(super) fn filters(filter: TemporalFilter) -> Result<Vec<FacetFilter>, RecordError> {
    let mut filters = Vec::new();
    for (field, range) in [(FIELDS[0], filter.event), (FIELDS[1], filter.ingested)] {
        if let Some(range) = range {
            let from = range.from_ms.unwrap_or(MIN_TIME);
            let to = range.to_ms.unwrap_or(MAX_TIME as i64 + 1);
            if from < MIN_TIME || to > MAX_TIME as i64 + 1 || from >= to {
                return Err(RecordError::Rejected(
                    "time range must be nonempty [from_ms,to_ms) within supported UTC range".into(),
                ));
            }
            filters.push(FacetFilter::I64 {
                field: field.into(),
                filter: from..to,
            });
        }
    }
    if let Some(at) = filter.valid_at_ms {
        if !(MIN_TIME..=MAX_TIME as i64).contains(&at) {
            return Err(RecordError::Rejected(
                "valid_at_ms outside supported UTC range".into(),
            ));
        }
        filters.push(FacetFilter::I64 {
            field: FIELDS[2].into(),
            filter: MIN_TIME..at + 1,
        });
        filters.push(FacetFilter::I64 {
            field: FIELDS[3].into(),
            filter: at + 1..MAX_TIME as i64 + 2,
        });
    }
    Ok(filters)
}

pub(super) fn validate_state(state: &State) -> Result<(), RecordError> {
    // V7 retains every applied command and expiration cause. A checkpoint must
    // account for each sequence exactly once, including the internal TTL writes.
    let mut commits = BTreeMap::new();
    for (seq, time) in state
        .operations
        .iter()
        .map(|op| (op.receipt.seq, op.observed_time_ms))
        .chain(state.lease_expirations.iter().map(|event|(event.seq,event.evaluation_time_ms)))
        .chain(
            state
                .expirations
                .iter()
                .map(|event| (event.seq, event.evaluation_time_ms)),
        )
    {
        if seq == 0 || commits.insert(seq, time).is_some() {
            return Err(storage("duplicate committed time sequence"));
        }
    }
    if commits.len() as u64 != state.seq {
        return Err(storage("incomplete committed time sequence"));
    }
    let mut previous_time = 0;
    for (position, (seq, time)) in commits.into_iter().enumerate() {
        if seq != position as u64 + 1 || time < previous_time || time > state.time_watermark_ms {
            return Err(storage("invalid committed time ordering"));
        }
        previous_time = time;
    }
    if state.time_watermark_ms > MAX_TIME {
        return Err(storage("invalid persisted time watermark"));
    }
    let mut expired = BTreeSet::new();
    let mut last_seq = 0;
    let mut last_time = 0;
    for event in &state.expirations {
        if event.seq <= last_seq
            || event.seq > state.seq
            || event.evaluation_time_ms < last_time
            || event.evaluation_time_ms > state.time_watermark_ms
            || event.targets.is_empty()
        {
            return Err(storage("inconsistent expiration history"));
        }
        last_seq = event.seq;
        last_time = event.evaluation_time_ms;
        for target in &event.targets {
            let record = state
                .records
                .get(&target.record_id)
                .ok_or_else(|| storage("orphan expiration history"))?;
            if !expired.insert(&target.record_id)
                || record.document.is_some()
                || record.expired_at_ms != Some(event.evaluation_time_ms)
                || target.previous_version.checked_add(1) != Some(record.version)
                || record
                    .temporal
                    .expire_at_ms
                    .is_none_or(|t| t > event.evaluation_time_ms)
            {
                return Err(storage(
                    "expiration history does not match terminal tombstone",
                ));
            }
        }
    }
    for record in state.records.values().chain(state.source_revisions.iter()) {
        validate(&record.temporal, 0).map_err(storage)?;
        if record.ingested_at_ms > state.time_watermark_ms
            || record
                .expired_at_ms
                .is_some_and(|t| t > state.time_watermark_ms)
        {
            return Err(storage("record time exceeds committed watermark"));
        }
    }
    for record in state.records.values() {
        if record.expired_at_ms.is_some() != expired.contains(&record.record_id) {
            return Err(storage("missing terminal expiration history"));
        }
        if record.temporal.expire_at_ms.is_some()
            && (record.record_id.len() > 256 || record.routing_key.len() > 256)
        {
            return Err(storage("TTL identity exceeds reservation"));
        }
    }
    if state
        .operations
        .iter()
        .any(|op| op.observed_time_ms > state.time_watermark_ms)
    {
        return Err(storage("operation time exceeds committed watermark"));
    }
    Ok(())
}
