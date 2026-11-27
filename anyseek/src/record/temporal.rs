//! Fixed evaluation time, retrieval deadlines and background expiry reclamation.
//! TTL is not irreversible erasure; bodies may remain in logs and backups.
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
    /// Excluded from retrieval at evaluation_time >= this deadline; reclaimed later.
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
    if request.temporal.is_some()
        && (request.document.is_none()
            || request.vector_patch.is_some())
    {
        return Err(RecordError::Rejected(
            "temporal metadata requires a complete body write".into(),
        ));
    }
    let value = request
        .temporal
        .clone()
        .or_else(|| previous.filter(|r| visible(r,evaluation)).map(|r| r.temporal.clone()))
        .unwrap_or_default();
    if value.expire_at_ms.is_some()
        && request.record_id.len() > 256
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
pub(super) fn visible(record: &Record, evaluation: u64) -> bool {
    record.document.is_some() && record.temporal.expire_at_ms.is_none_or(|deadline| deadline > evaluation)
}
pub(super) fn live_ttl(record: &Record) -> bool {
    record.document.is_some() && record.temporal.expire_at_ms.is_some()
}
pub(super) fn reserve_bytes(state: &State) -> usize {
    state.records.live_ttl_count() * 8192
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct ExpiredTarget {
    record_id: String,
    previous_version: u64,
}
/// A single committed expiration cause; replay validates this exact sequence.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Expiration {
    pub(super) seq: u64,
    pub(super) evaluation_time_ms: u64,
    targets: Vec<ExpiredTarget>,
}
pub(super) struct PreparedExpiration {
    replacements: Vec<(state_values::PreviousRow, state_memory::Shared<Record>)>,
    pub(super) snapshot_bytes: usize,
    pub(super) reserve_bytes: usize,
    _temporary: crate::storage::build_budget::MemoryLease,
}

impl PreparedExpiration {
    pub(super) fn prepare(
        state: &State,
        event: &Expiration,
        before: usize,
    ) -> Result<Self, RecordError> {
        if state.seq.checked_add(1) != Some(event.seq)
            || event.evaluation_time_ms < state.time_watermark_ms
            || event.targets.is_empty()
        {
            return Err(storage("inconsistent expiration command"));
        }
        let temporary = crate::storage::build_budget::reserve_memory(
            (event.targets.len() as u64).checked_mul(256).ok_or(RecordError::Busy)?,
        ).map_err(memory_error)?;
        let mut seen = BTreeSet::new();
        let mut replacements = Vec::with_capacity(event.targets.len());
        let mut snapshot_bytes = before
            .checked_add(checkpoint_codec::json_length(event)?)
            .and_then(|size| size.checked_add(usize::from(!state.expirations.is_empty())))
            .and_then(|size| size.checked_add(event.seq.to_string().len() - state.seq.to_string().len()))
            .and_then(|size| size.checked_add(
                event.evaluation_time_ms.to_string().len() - state.time_watermark_ms.to_string().len(),
            ))
            .ok_or_else(|| storage("expiration wire size overflow"))?;
        for target in &event.targets {
            let record = state.records.find(&target.record_id)?
                .ok_or_else(|| storage("expired identity absent"))?;
            if !seen.insert(&target.record_id)
                || record.version() != target.previous_version
                || !record.live()
                || record.expired_at_ms().is_some()
                || record.temporal().expire_at_ms.is_none_or(|end| end > event.evaluation_time_ms)
            {
                return Err(storage("inconsistent expiration target"));
            }
            let previous = record.account(&target.record_id)?;
            let tombstone = state_memory::Shared::admit(Record {
                record_id: target.record_id.clone(),
                version: record.version().checked_add(1).ok_or_else(|| storage("version exhausted"))?,
                document: None,
                temporal: record.temporal().clone(),
                ingested_at_ms: record.ingested_at_ms(),
                expired_at_ms: Some(event.evaluation_time_ms),
            })?;
            snapshot_bytes = snapshot_bytes.checked_sub(previous.bytes)
                .and_then(|size| size.checked_add(tombstone.wire_bytes()))
                .ok_or_else(|| storage("expiration record size overflow"))?;
            replacements.push((previous, tombstone));
        }
        let reserve_bytes = state.records.live_ttl_count().checked_sub(replacements.len())
            .and_then(|count| count.checked_mul(8192))
            .ok_or_else(|| storage("expiration TTL budget overflow"))?;
        Ok(Self { replacements, snapshot_bytes, reserve_bytes, _temporary: temporary })
    }

    pub(super) fn admit(&self, state: &mut State) -> Result<(), RecordError> {
        let values = self.replacements.iter().try_fold(state.value_bytes()?, |bytes, (old, row)| {
            bytes.checked_sub(old.bytes).and_then(|bytes| bytes.checked_add(row.wire_bytes()))
                .ok_or_else(|| storage("expiration value size overflow"))
        })?;
        let metadata = self.snapshot_bytes.checked_sub(values)
            .and_then(|bytes| bytes.checked_sub(state.records.sealed_keys_bytes()))
            .and_then(|bytes| bytes.checked_add(state.lookup_bytes))
            .ok_or_else(|| storage("expiration container size overflow"))?;
        state.admit_containers(metadata)?;
        state.expirations.try_reserve(1).map_err(|_| RecordError::Busy)?;
        Ok(())
    }

    pub(super) fn install(self, state: &mut State, event: Expiration) -> Result<(), RecordError> {
        for (previous, record) in self.replacements {
            state.records.replace_prepared(record.record_id.clone(), record, Some(previous), None)?;
        }
        state.seq = event.seq;
        state.time_watermark_ms = event.evaluation_time_ms;
        state.expirations.push(event);
        Ok(())
    }
}

pub(super) fn apply(state: &mut State, event: &Expiration) -> Result<(), RecordError> {
    let prepared = PreparedExpiration::prepare(state, event, state_proof::length(state)?)?;
    prepared.admit(state)?;
    prepared.install(state, event.clone())
}

impl replay::Context<'_> {
    pub(super) async fn retire_expired(&mut self, event: &Expiration) -> Result<(), RecordError> {
        for target in &event.targets {
            if let Some(ordinal) = self.locations.remove(&target.record_id) {
                self.index.retire(ordinal).await?;
            }
        }
        Ok(())
    }
}
pub(super) fn checked_time(clock: &dyn Clock, floor: u64) -> Result<u64, RecordError> {
    let now = clock.now_ms()?;
    if now > MAX_TIME || now.saturating_add(CLOCK_TOLERANCE) < floor {
        return Err(RecordError::ClockUncertain);
    }
    Ok(now.max(floor))
}

/// Process-local high water shared by Record decisions and response delivery.
/// Individual operations still use the evaluation captured under Inner.
#[derive(Clone, Default)]
pub(super) struct ObservedTime(Arc<std::sync::atomic::AtomicU64>);

impl ObservedTime {
    pub(super) fn get(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(super) fn advance(&self, evaluation: u64) -> u64 {
        self.0.fetch_max(evaluation, std::sync::atomic::Ordering::AcqRel).max(evaluation)
    }
}

impl Inner {
    fn trusted_evaluation_time(&self) -> Result<u64, RecordError> {
        if !self.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        checked_time(self.clock.as_ref(), self.state.time_watermark_ms)
    }

    /// Observation is transient and never edits canonical State, WAL or indexes.
    pub(super) fn observe_time(&mut self) -> Result<u64, RecordError> {
        let evaluation = self.observed_time.advance(self.trusted_evaluation_time()?);
        self.evaluation_time_ms=evaluation;
        Ok(evaluation)
    }
    /// Called only by an admitted maintenance worker holding writer then Inner.
    pub(super) async fn expire_due(&mut self) -> Result<(), RecordError> {
        let evaluation=self.observe_time()?;
        let mut targets = Vec::new();
        self.state.records.visit_due_ttl(evaluation, |id, version| {
            targets.push(ExpiredTarget {record_id: id.to_owned(), previous_version: version});
            Ok(())
        })?;
        if targets.is_empty() {
            self.evaluation_time_ms = evaluation;
            return Ok(());
        }
        // Keep the existing record-ID order in the durable expiration frame.
        targets.sort_unstable_by(|a, b| a.record_id.cmp(&b.record_id));
        let event = Expiration {
            seq: self
                .state
                .seq
                .checked_add(1)
                .ok_or_else(|| storage("sequence exhausted"))?,
            evaluation_time_ms: evaluation,
            targets,
        };
        let prepared = PreparedExpiration::prepare(&self.state, &event, self.snapshot_bytes)?;
        let snapshot_bytes = prepared.snapshot_bytes;
        let reserve = prepared.reserve_bytes;
        checkpoint_codec::validate_size(snapshot_bytes, reserve, self.config.max_journal_bytes)
            .map_err(storage)?;
        let bytes = encode_entry(&Entry::Expired(event.clone()))?;
        self.ensure_journal_room(bytes.len(),reserve).await?;
        self.ensure_remote_tail_room(event.seq,&format!("system:expired:{}",event.seq),&bytes).await?;
        self.validate_journal_room(bytes.len(),reserve)?;
        prepared.admit(&mut self.state)?;
        self.healthy = false;
        self.output
            .write()
            .map_err(|_| RecordError::RecoveryRequired)?
            .fenced = true;
        if let Some(remote) = self.remote.as_mut() {
            remote.commit_entry(event.seq, format!("system:expired:{}", event.seq), &bytes)
                .await.map_err(|error|RecordError::OutcomeUnknown(error.to_string()))?;
        }
        self.append_operation(&bytes,event.seq).await?;
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
        self.replay_context().retire_expired(&event).await
            .map_err(|_| RecordError::DurablePending(event.seq))?;
        let seq = event.seq;
        prepared.install(&mut self.state, event)
            .map_err(|_| RecordError::DurablePending(seq))?;
        #[cfg(test)]
        checkpoint_codec::assert_full_length(&self.config, &self.state, snapshot_bytes).await?;
        self.snapshot_bytes = snapshot_bytes;
        self.evaluation_time_ms = evaluation;
        self.output
            .write()
            .map_err(|_| RecordError::DurablePending(seq))?
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
    /// Observe this process's calibrated time. Does not persist expiry or history.
    /// After restart the persisted write/maintenance watermark is the lower bound;
    /// an uncollected body can become visible again after a clock rollback.
    pub async fn advance_time(&self) -> Result<(), RecordError> {
        let _permit=self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        self.inner.lock().await.observe_time()?;
        Ok(())
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
const FIELDS: [&str; 5] = [
    "__seekpower_time_event",
    "__seekpower_time_ingested",
    "__seekpower_time_from",
    "__seekpower_time_to",
    "__seekpower_time_expire",
];
pub(super) fn reserved(name: &str) -> bool {
    name.starts_with(PREFIX)
}
pub(super) fn schema(config: &RecordConfig) -> Vec<SchemaField> {
    let mut schema = config.schema.clone();
    for field in FIELDS {
        schema.push(serde_json::from_value(serde_json::json!({"field":field,"field_type":"Int","store":false,"index_lexical":false,"facet":true})).expect("internal time schema"));
    }
    schema
}
fn indexed_values(metadata:&TemporalMetadata,ingested:u64)->[i64;5] {
    [metadata.event_time_ms.unwrap_or(MIN_TIME-1),ingested as i64,
        metadata.valid_from_ms.unwrap_or(MIN_TIME),metadata.valid_to_ms.unwrap_or(MAX_TIME as i64+1),
        metadata.expire_at_ms.map_or(MAX_TIME as i64+1,|at|at as i64)]
}
pub(super) fn indexed(document: &Document, metadata: &TemporalMetadata, ingested: u64) -> Document {
    let mut document = document.clone();
    for (field,value) in FIELDS.into_iter().zip(indexed_values(metadata,ingested)) {
        document.insert(field.into(), value.into());
    }
    document
}
/// Exact wire length of the indexed document/projection without cloning either
/// map or its values. The counter serializes borrowed fields and five stack
/// integers; temporal overrides share the same value source as `indexed`.
pub(super) fn indexed_length(document:&Document,metadata:&TemporalMetadata,ingested:u64,
    selected:impl Fn(&str)->bool)->Result<usize,RecordError>{
    struct Indexed<'a,F>{document:&'a Document,values:[i64;5],selected:F}
    impl<F:Fn(&str)->bool> Serialize for Indexed<'_,F> {
        fn serialize<S:serde::Serializer>(&self,serializer:S)->Result<S::Ok,S::Error>{
            use serde::ser::SerializeMap;
            let mut map=serializer.serialize_map(None)?;
            for (field,value) in self.document {
                if !FIELDS.contains(&field.as_str()) && (self.selected)(field) {map.serialize_entry(field,value)?;}
            }
            for (field,value) in FIELDS.into_iter().zip(self.values) {
                if (self.selected)(field) {map.serialize_entry(field,&value)?;}
            }
            map.end()
        }
    }
    checkpoint_codec::json_length(&Indexed{document,values:indexed_values(metadata,ingested),selected})
}
/// Applied before channel Top-K/counting, including count-only and browse.
pub(super) fn expiry_filter(evaluation:u64)->FacetFilter {
    FacetFilter::Int {field:FIELDS[4].into(),filter:(evaluation as i64+1)..(MAX_TIME as i64+2)}
}
pub(super) fn filters(filter: TemporalFilter) -> Result<Vec<FacetFilter>, RecordError> {
    let mut filters = Vec::new();
    for (field, range) in [(FIELDS[0], filter.event), (FIELDS[1], filter.ingested)] {
        if let Some(range) = range {
            filters.push(FacetFilter::Int {
                field: field.into(),
                filter: checked_range(range)?,
            });
        }
    }
    if let Some(at) = filter.valid_at_ms {
        if !(MIN_TIME..=MAX_TIME as i64).contains(&at) {
            return Err(RecordError::Rejected(
                "valid_at_ms outside supported UTC range".into(),
            ));
        }
        filters.push(FacetFilter::Int {
            field: FIELDS[2].into(),
            filter: MIN_TIME..at + 1,
        });
        filters.push(FacetFilter::Int {
            field: FIELDS[3].into(),
            filter: at + 1..MAX_TIME as i64 + 2,
        });
    }
    Ok(filters)
}

fn checked_range(range: TimeRange) -> Result<std::ops::Range<i64>, RecordError> {
    let from = range.from_ms.unwrap_or(MIN_TIME);
    let to = range.to_ms.unwrap_or(MAX_TIME as i64 + 1);
    if from < MIN_TIME || to > MAX_TIME as i64 + 1 || from >= to {
        return Err(RecordError::Rejected(
            "time range must be nonempty [from_ms,to_ms) within supported UTC range".into(),
        ));
    }
    Ok(from..to)
}

pub(super) fn event_window(
    mut request: TemporalFilter,
) -> Result<(std::ops::Range<i64>, Vec<FacetFilter>), RecordError> {
    let event = request.event.take()
        .filter(|range| range.from_ms.is_some() && range.to_ms.is_some())
        .ok_or_else(|| RecordError::Rejected("event-time query requires both event range endpoints".into()))?;
    Ok((checked_range(event)?, filters(request)?))
}

pub(super) fn matches_indexed_time(
    predicates: &[FacetFilter], metadata: &TemporalMetadata, ingested_at_ms: u64,
) -> bool {
    let values = indexed_values(metadata, ingested_at_ms);
    predicates.iter().all(|predicate| {
        let FacetFilter::Int { field, filter } = predicate else { return false; };
        FIELDS.iter().position(|name| field == name)
            .is_some_and(|slot| filter.contains(&values[slot]))
    })
}

/// Authenticated checkpoint summary of a fully validated committed-time prefix.
/// Expiration commands remain separately retained and checked.
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RetiredPrefix {pub seq:u64,pub time_ms:u64}

pub(super) fn validate_state(state: &State) -> Result<(), RecordError> {
    if state.operations.iter().zip(state.operations.iter().skip(1))
        .any(|(a,b)|a.receipt.seq>=b.receipt.seq) {
        return Err(storage("unordered retained committed history"));
    }
    // Images without a retired prefix retain every command/cause. A GC checkpoint may summarize a
    // verified prefix; its remaining suffix still accounts for every sequence
    // exactly once, including internal TTL writes.
    let mut commits = BTreeMap::new();
    for (seq, time) in state
        .operations
        .iter()
        .map(|op| (op.receipt.seq, op.observed_time_ms))
        .chain(
            state
                .expirations
                .iter()
                .map(|event| (event.seq, event.evaluation_time_ms)),
        )
        .chain(state.groups.iter().flat_map(|g|g.receipts.iter()).map(|r|(r.receipt.seq,r.observed_time_ms)))
    {
        if seq == 0 || commits.insert(seq, time).is_some() {
            return Err(storage("duplicate committed time sequence"));
        }
    }
    let (floor,mut previous_time)=if let Some(prefix)=&state.retired_operation_prefix {
        if prefix.seq==0 || prefix.seq>state.history.floor || prefix.seq>state.seq
            || prefix.time_ms>state.time_watermark_ms {return Err(storage("invalid retired operation time prefix"));}
        let mut retained_time=0;
        for (_,time) in commits.range(..=prefix.seq) {
            if *time<retained_time || *time>prefix.time_ms {return Err(storage("invalid retained prefix time ordering"));}
            retained_time=*time;
        }
        (prefix.seq,prefix.time_ms)
    } else {(0,0)};
    let suffix=commits.range((std::ops::Bound::Excluded(floor),std::ops::Bound::Unbounded));
    if suffix.clone().count() as u64 != state.seq-floor {
        return Err(storage("incomplete committed time sequence"));
    }
    for (position, (&seq, &time)) in suffix.enumerate() {
        if seq != floor + position as u64 + 1 || time < previous_time || time > state.time_watermark_ms {
            return Err(storage("invalid committed time ordering"));
        }
        previous_time = time;
    }
    if state.time_watermark_ms > MAX_TIME {
        return Err(storage("invalid persisted time watermark"));
    }
    let mut expired = BTreeMap::new();
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
                .find(&target.record_id)?
                .ok_or_else(|| storage("orphan expiration history"))?;
            let version=target.previous_version.checked_add(1).ok_or_else(||storage("expiration version overflow"))?;
            if record.version() < version || expired.get(target.record_id.as_str()).is_some_and(|previous| *previous>=version) {
                return Err(storage("expiration history version regression"));
            }
            expired.insert(target.record_id.as_str(),version);
            if record.version()==version && (record.live()
                || record.expired_at_ms()!=Some(event.evaluation_time_ms)
                || record.temporal().expire_at_ms.is_none_or(|t| t>event.evaluation_time_ms)) {
                return Err(storage("expiration tombstone differs from its current revision"));
            }
        }
    }
    state.records.scan(None, |id, row| {
        validate(row.temporal(), 0).map_err(storage)?;
        if row.ingested_at_ms() > state.time_watermark_ms
            || row.expired_at_ms().is_some_and(|time| time > state.time_watermark_ms)
        {
            return Err(storage("record time exceeds committed watermark"));
        }
        if row.expired_at_ms().is_some() != expired.get(id).is_some_and(|version| *version == row.version()) {
            return Err(storage("missing current expiration history"));
        }
        if row.temporal().expire_at_ms.is_some() && id.len() > 256 {
            return Err(storage("TTL identity exceeds reservation"));
        }
        Ok(true)
    })?;
    if state
        .operations
        .iter()
        .any(|op| op.observed_time_ms > state.time_watermark_ms)
    {
        return Err(storage("operation time exceeds committed watermark"));
    }
    Ok(())
}
