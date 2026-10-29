//! A bounded projection of the sole committed Record journal. The retained
//! operation/expiration ledgers remain authority; feed indexes are rebuildable.
use super::*;
use std::collections::VecDeque;

const MIN_RETENTION_MS: u64 = 7 * 86_400_000;
pub(super) const EVENT_RESERVE: usize = 8192;
/// Immutable feed resource policy. Exhaustion applies backpressure, never eviction.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct FeedConfig {
    /// At least seven days in the trusted commit evaluation timeline.
    pub retention_ms: u64,
    /// Event count including reserved future invalidations.
    pub max_events: usize,
    /// Serialized event bytes including reserved future invalidations.
    pub max_bytes: usize,
}
impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            retention_ms: MIN_RETENTION_MS,
            max_events: 100_000,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}
pub(super) fn validate_config(config: &FeedConfig) -> Result<(), RecordError> {
    if config.retention_ms < MIN_RETENTION_MS
        || config.retention_ms > 365 * 86_400_000
        || config.max_events == 0
        || config.max_events > 100_000
        || config.max_bytes < 1024
        || config.max_bytes > 64 * 1024 * 1024
    {
        return Err(RecordError::Rejected(
            "invalid feed retention or resource policy".into(),
        ));
    }
    Ok(())
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct FeedState {
    pub scope: String,
    pub floor: u64,
}
/// One event per externally relevant command; staging and abort are private.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    /// A body was durably created or replaced.
    RecordChanged,
    /// A stable identity was tombstoned.
    RecordDeleted,
    /// A source identity was created.
    SourceCreated,
    /// A new source revision invalidated old dependents.
    SourceUpdated,
    /// Source deletion invalidated its dependency closure.
    SourceDeleted,
    /// Missing vector fields became ready at the same body version.
    VectorReady,
    /// An entire document generation became visible.
    GenerationPublished,
    /// An aggregate expiration commit invalidated records and dependents.
    TtlExpired,
}
/// Content-free invalidation/refresh notice, never a historical document reader.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeEvent {
    /// Owner feed scope; changes on independent copy/restore.
    pub scope: String,
    /// Original partition provenance, not a shared write lease.
    pub partition_id: String,
    /// Authoritative command sequence. Gaps may contain private staging commands.
    pub seq: u64,
    /// Reserved for multiple notices per command; currently always zero.
    pub event_index: u32,
    /// Trusted evaluation time assigned to the committed command.
    pub committed_at_ms: u64,
    /// Refresh or invalidation cause.
    pub kind: ChangeKind,
    /// Stable identity, if the command refers to one externally visible identity.
    pub record_id: Option<String>,
    /// Resulting record/document-control version.
    pub version: Option<u64>,
    /// Original retry identity; TTL internal commits have none.
    pub operation_id: Option<OperationId>,
    /// Whole-generation publication identity; never uncommitted staging content.
    pub generation_id: Option<String>,
    /// Consumer must rebuild its effective view; this notice does not enumerate a closure.
    pub rescan_required: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    scope: String,
    seq: u64,
}
/// At-least-once page. Persist next only after applying the page idempotently.
#[derive(Clone, Debug, Serialize)]
pub struct ChangePage {
    /// Ordered notices after the supplied cursor.
    pub events: Vec<ChangeEvent>,
    /// Save only after applying this page idempotently.
    pub next: String,
    /// Last discarded event sequence; older consumers must bootstrap.
    pub retention_floor: u64,
    /// Authoritative cut captured for this response.
    pub visible_seq: u64,
    /// Fixed trusted UTC Unix milliseconds for this view.
    pub evaluation_time_ms: u64,
}
#[derive(Clone, Debug)]
pub(super) struct Point {
    pub seq: u64,
    pub at: u64,
    bytes: usize,
}
#[derive(Default)]
pub(super) struct Cache {
    points: VecDeque<Point>,
    bytes: usize,
}
/// Durable advancement of the logical retained prefix, never a physical WAL offset.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Trim {
    previous: u64,
    pub(super) floor: u64,
    at: u64,
    // Old WAL entries retain their exact public-point validation rule.
    #[serde(default,skip_serializing_if="std::ops::Not::not")]
    complete_time_prefix: bool,
}
fn projected(state: &State, applied: &Applied) -> Option<ChangeEvent> {
    let request = &applied.request;
    let kind = if request.vector_patch.is_some() {
        ChangeKind::VectorReady
    } else if let Some(command) = &request.generation {
        match command {
            GenerationCommand::Publish { .. } => ChangeKind::GenerationPublished,
            _ => return None,
        }
    } else {
        let system = request.system.as_ref().or_else(|| {
            state
                .records
                .get(&request.record_id)
                .and_then(|r| r.system.as_ref())
        });
        match (
            system.map(|s| &s.kind),
            request.document.is_some(),
            applied.receipt.version,
        ) {
            (Some(EvidenceKind::Source), false, _) => ChangeKind::SourceDeleted,
            (Some(EvidenceKind::Source), true, 1) => ChangeKind::SourceCreated,
            (Some(EvidenceKind::Source), true, _) => ChangeKind::SourceUpdated,
            (_, false, _) => ChangeKind::RecordDeleted,
            _ => ChangeKind::RecordChanged,
        }
    };
    let rescan_required = applied.graph_invalidation || matches!(
        kind,
        ChangeKind::SourceUpdated | ChangeKind::SourceDeleted | ChangeKind::GenerationPublished
    ) || (request.document.is_none()
        && request.vector_patch.is_none()
        && state
            .records
            .get(&request.record_id)
            .is_some_and(|r| r.system.is_some()));
    Some(ChangeEvent {
        scope: state.feed.scope.clone(),
        partition_id: state.lineage.clone(),
        seq: applied.receipt.seq,
        event_index: 0,
        committed_at_ms: applied.observed_time_ms,
        kind,
        record_id: Some(request.record_id.clone()),
        version: Some(applied.receipt.version),
        operation_id: Some(request.operation_id.clone()),
        generation_id: match &request.generation {
            Some(GenerationCommand::Publish { generation_id }) => Some(generation_id.clone()),
            _ => None,
        },
        rescan_required,
    })
}
fn expired(state: &State, seq: u64, at: u64) -> ChangeEvent {
    ChangeEvent {
        scope: state.feed.scope.clone(),
        partition_id: state.lineage.clone(),
        seq,
        event_index: 0,
        committed_at_ms: at,
        kind: ChangeKind::TtlExpired,
        record_id: None,
        version: None,
        operation_id: None,
        generation_id: None,
        rescan_required: true,
    }
}
fn point(event: &ChangeEvent) -> Result<Point, RecordError> {
    Ok(Point {
        seq: event.seq,
        at: event.committed_at_ms,
        bytes: serde_json::to_vec(event).map_err(storage)?.len(),
    })
}
pub(super) fn pending(state: &State, applied: &Applied) -> Result<Option<Point>, RecordError> {
    projected(state, applied).as_ref().map(point).transpose()
}
pub(super) fn pending_expiry(state: &State, seq: u64, at: u64) -> Result<Point, RecordError> {
    point(&expired(state, seq, at))
}
pub(super) fn reserve(state: &State) -> usize {
    generation::reserve_slots(state)
        + state.records.live_ttl_count()
        + generation::staged_ttl_count(state)
}
impl Cache {
    pub(super) fn rebuild(state: &State, config: &FeedConfig) -> Result<Self, RecordError> {
        if state.feed.scope.len() != 32
            || !state.feed.scope.bytes().all(|b| b.is_ascii_hexdigit())
            || state.feed.floor > state.seq
        {
            return Err(storage("invalid feed identity or floor"));
        }
        let mut points = Vec::new();
        for applied in &state.operations {
            if applied.receipt.seq > state.feed.floor {
                if let Some(point) = pending(state, applied)? {
                    points.push(point);
                }
            }
        }
        for event in &state.expirations {
            if event.seq > state.feed.floor {
                points.push(pending_expiry(state, event.seq, event.evaluation_time_ms)?);
            }
        }
        points.sort_by_key(|p| p.seq);
        let mut cache = Self::default();
        for point in points {
            cache.push(Some(point));
        }
        cache
            .validate(config, None, reserve(state))
            .map_err(storage)?;
        Ok(cache)
    }
    pub(super) fn push(&mut self, point: Option<Point>) {
        if let Some(point) = point {
            self.bytes += point.bytes;
            self.points.push_back(point);
        }
    }
    pub(super) fn validate(
        &self,
        config: &FeedConfig,
        point: Option<&Point>,
        reserved: usize,
    ) -> Result<(), RecordError> {
        if self
            .points
            .len()
            .saturating_add(usize::from(point.is_some()))
            .saturating_add(reserved)
            > config.max_events
            || self
                .bytes
                .saturating_add(point.map_or(0, |p| p.bytes))
                .saturating_add(reserved.saturating_mul(EVENT_RESERVE))
                > config.max_bytes
        {
            return Err(RecordError::FeedFull);
        }
        Ok(())
    }
    fn floor(&self, at: u64, retention: u64) -> Option<u64> {
        let cutoff = at.checked_sub(retention)?;
        self.points
            .iter()
            .take_while(|p| p.at <= cutoff)
            .last()
            .map(|p| p.seq)
    }
    fn trim(&mut self, floor: u64) {
        while self.points.front().is_some_and(|p| p.seq <= floor) {
            self.bytes -= self.points.pop_front().unwrap().bytes;
        }
    }
}
/// Private generation commands have no public Feed point, but are still
/// committed positions. Source state has a monotonic authenticated time chain;
/// include those positions so a private-only stream can advance retention.
fn committed_floor(state:&State,at:u64,retention:u64)->Option<u64> {
    let cutoff=at.checked_sub(retention)?;
    fn last_before(len:usize,mut entry:impl FnMut(usize)->(u64,u64),cutoff:u64)->Option<u64> {
        let (mut low,mut high)=(0,len);
        while low<high {
            let middle=low+(high-low)/2;
            if entry(middle).1<=cutoff {low=middle+1;} else {high=middle;}
        }
        low.checked_sub(1).map(|i|entry(i).0)
    }
    // Each authoritative sequence is independently ordered by committed time.
    // Upper bounds include every equal-time commit without a full locked scan.
    [last_before(state.operations.len(),|i|(state.operations[i].receipt.seq,state.operations[i].observed_time_ms),cutoff),
        last_before(state.expirations.len(),|i|(state.expirations[i].seq,state.expirations[i].evaluation_time_ms),cutoff),
        last_before(state.lease_expirations.len(),|i|(state.lease_expirations[i].seq,state.lease_expirations[i].evaluation_time_ms),cutoff),
        state.retired_operation_prefix.as_ref().filter(|prefix|prefix.time_ms<=cutoff).map(|prefix|prefix.seq)]
        .into_iter().flatten().filter(|seq|*seq>state.feed.floor).max()
}
pub(super) fn apply_trim(
    state: &mut State,
    config: &FeedConfig,
    trim: &Trim,
) -> Result<(), RecordError> {
    let mut cache = Cache::rebuild(state, config)?;
    let expected=if trim.complete_time_prefix {committed_floor(state,trim.at,config.retention_ms)}
        else {cache.floor(trim.at,config.retention_ms)};
    if trim.previous != state.feed.floor
        || trim.at < state.time_watermark_ms
        || expected != Some(trim.floor)
    {
        return Err(storage("invalid feed retention transition"));
    }
    cache.trim(trim.floor);
    state.feed.floor = trim.floor;
    state.time_watermark_ms = trim.at;
    Ok(())
}
impl Inner {
    pub(super) async fn trim_feed(&mut self, at: u64) -> Result<(), RecordError> {
        let Some(floor) = committed_floor(&self.state,at,self.config.change_feed.retention_ms)
        else {
            return Ok(());
        };
        let trim = Trim {
            previous: self.state.feed.floor,
            floor,
            at,
            complete_time_prefix: true,
        };
        #[cfg(feature="s3-object-store")]
        if self.remote.is_some() {
            self.maintenance_trim.get_or_insert(trim);
            return Err(self.request_checkpoint("remote_feed_trim"));
        }
        let bytes = encode_entry(&Entry::FeedTrim(trim.clone()))?;
        // Maintenance never consumes deletion/TTL emergency capacity. A full
        // physical WAL can be compacted through the existing checkpoint route.
        if self
            .journal
            .validate_append_with_reserve(bytes.len(), generation::reserve_bytes(&self.state))
            .is_err()
        {
            return Ok(());
        }
        let mut candidate = self.state.try_clone()?;
        candidate.feed.floor = floor;
        candidate.time_watermark_ms = at;
        let snapshot_bytes = checkpoint_codec::length(&self.config, &candidate)?;
        if checkpoint_codec::validate_size(
            snapshot_bytes, generation::reserve_bytes(&self.state),
            self.config.max_journal_bytes,
        )
        .is_err()
        {
            return Ok(());
        }
        self.healthy = false;
        self.output
            .write()
            .map_err(|_| RecordError::RecoveryRequired)?
            .fenced = true;
        self.append_operation(&bytes,self.state.seq).await?;
        #[cfg(feature = "fault-injection")]
        match std::env::var("ANYSEEK_FEED_TRIM_FAULT").ok().as_deref() {
            Some("after_append_unknown") => {
                return Err(RecordError::OutcomeUnknown("feed trim append".into()));
            }
            Some("after_durable") => return Err(RecordError::DurablePending(self.state.seq)),
            Some("crash_after_durable") => std::process::exit(90),
            _ => {}
        }
        self.state.feed.floor = floor;
        self.state.time_watermark_ms = at;
        self.snapshot_bytes = snapshot_bytes;
        self.feed_cache.trim(floor);
        self.output
            .write()
            .map_err(|_| RecordError::DurablePending(self.state.seq))?
            .fenced = false;
        self.healthy = true;
        Ok(())
    }
}
fn encode<T: Serialize>(value: &T) -> Result<String, RecordError> {
    Ok(serde_json::to_vec(value)
        .map_err(storage)?
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
fn decode<T: serde::de::DeserializeOwned>(token: &str) -> Result<T, RecordError> {
    if token.len() > 1024
        || !token.len().is_multiple_of(2)
        || !token.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(RecordError::Rejected(
            "invalid change cursor encoding".into(),
        ));
    }
    let bytes = (0..token.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&token[i..i + 2], 16).map_err(storage))
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::from_slice(&bytes)
        .map_err(|_| RecordError::Rejected("invalid change cursor".into()))
}
fn cursor(state: &State, seq: u64) -> Result<String, RecordError> {
    encode(&Cursor {
        version: 1,
        scope: state.feed.scope.clone(),
        seq,
    })
}
impl RecordPartition {
    /// Ordered metadata-only notices. The opaque cursor is not authorization.
    pub async fn changes(
        &self,
        after: Option<String>,
        limit: usize,
    ) -> Result<ChangePage, RecordError> {
        self.advance_time().await?;
        let _admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        if limit == 0 || limit > 1000 {
            return Err(RecordError::Rejected(
                "feed page limit must be 1..1000".into(),
            ));
        }
        let after = if let Some(token) = after {
            let token: Cursor = decode(&token)?;
            if token.version != 1 || token.scope != inner.state.feed.scope {
                return Err(RecordError::CursorExpired(inner.state.feed.floor));
            }
            token.seq
        } else {
            0
        };
        if after < inner.state.feed.floor {
            return Err(RecordError::CursorExpired(inner.state.feed.floor));
        }
        if after > inner.state.seq {
            return Err(RecordError::Rejected(
                "feed cursor is ahead of authority".into(),
            ));
        }
        let mut rows = inner.feed_cache.points.iter().filter(|p| p.seq > after);
        let mut events = Vec::new();
        for point in rows.by_ref().take(limit) {
            let event = match inner
                .state
                .operations
                .binary_search_by_key(&point.seq, |a| a.receipt.seq)
            {
                Ok(i) => projected(&inner.state, &inner.state.operations[i])
                    .ok_or_else(|| storage("private command in feed cache"))?,
                Err(_) => {
                    let i = inner
                        .state
                        .expirations
                        .binary_search_by_key(&point.seq, |e| e.seq)
                        .map_err(storage)?;
                    let e = &inner.state.expirations[i];
                    expired(&inner.state, e.seq, e.evaluation_time_ms)
                }
            };
            events.push(event);
        }
        let end = if rows.next().is_some() {
            events.last().unwrap().seq
        } else {
            inner.state.seq
        };
        Ok(ChangePage {
            events,
            next: cursor(&inner.state, end)?,
            retention_floor: inner.state.feed.floor,
            visible_seq: inner.state.seq,
            evaluation_time_ms: inner.evaluation_time_ms,
        })
    }
}

const BOOTSTRAP_LEASE_MS: u64 = 300_000;
const PAGE_BYTES: usize = 4 * 1024 * 1024;
pub(super) struct Bootstrap {
    id: String,
    seq: u64,
    at: u64,
    expires: u64,
    records: Vec<Record>,
    permit: DataOutputPermit,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BootstrapCursor {
    version: u8,
    scope: String,
    id: String,
    index: usize,
}
/// One page of a pinned effective snapshot, with an atomic snapshot-to-feed cut.
#[derive(Clone, Debug, Serialize)]
pub struct BootstrapPage {
    /// Effective records in one bounded snapshot page.
    pub records: Vec<Record>,
    /// Opaque next-page token, or None after the complete snapshot.
    pub next: Option<String>,
    /// Feed cut; consume only after the entire snapshot has been installed.
    pub continuation: String,
    /// Authoritative cut captured for this response.
    pub visible_seq: u64,
    /// Fixed trusted UTC Unix milliseconds for this view.
    pub evaluation_time_ms: u64,
    /// Snapshot lease deadline in trusted UTC Unix milliseconds.
    pub expires_at_ms: u64,
}
impl RecordPartition {
    /// One bounded five-minute pin per partition. Retry the same request_id and
    /// cursor after response loss. Restart/expiry/hiding invalidates the pin.
    pub async fn bootstrap(
        &self,
        request_id: String,
        after: Option<String>,
        limit: usize,
    ) -> Result<BootstrapPage, RecordError> {
        if request_id.len() != 32
            || !request_id.bytes().all(|b| b.is_ascii_hexdigit())
            || limit == 0
            || limit > 1000
        {
            return Err(RecordError::Rejected(
                "bootstrap requires 32-hex request_id and limit 1..1000".into(),
            ));
        }
        self.advance_time().await?;
        let _admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let mut inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        let at = inner.evaluation_time_ms;
        inner.refresh_bootstrap(at);
        if !request_id.starts_with(&inner.bootstrap_epoch) {
            return Err(RecordError::BootstrapExpired);
        }
        let index = if let Some(token) = after {
            let token: BootstrapCursor = decode(&token)?;
            if token.version != 1
                || token.scope != inner.state.feed.scope
                || token.id != request_id
                || inner.bootstrap.as_ref().is_none_or(|p| p.id != request_id)
            {
                return Err(RecordError::BootstrapExpired);
            }
            token.index
        } else {
            if inner.bootstrap.as_ref().is_some_and(|p| p.id != request_id) {
                return Err(RecordError::Busy);
            }
            if inner.bootstrap.is_none() {
                let mut records = Vec::new();
                let mut bytes = 0usize;
                for record in inner
                    .state
                    .records
                    .values()
                    .filter(|r| inner.locations.contains_key(&r.record_id))
                {
                    bytes = bytes
                        .checked_add(serde_json::to_vec(record).map_err(storage)?.len())
                        .ok_or_else(|| {
                            RecordError::Rejected("bootstrap byte budget exceeded".into())
                        })?;
                    if bytes as u64 > inner.config.max_journal_bytes {
                        return Err(RecordError::Rejected(
                            "bootstrap byte budget exceeded".into(),
                        ));
                    }
                    records.push(record.clone());
                }
                let permit = DataOutputPermit::new(
                    inner.output.clone(),
                    inner
                        .output
                        .read()
                        .map_err(|_| RecordError::RecoveryRequired)?
                        .revision,
                );
                inner.bootstrap = Some(Bootstrap {
                    id: request_id.clone(),
                    seq: inner.state.seq,
                    at,
                    expires: at.saturating_add(BOOTSTRAP_LEASE_MS),
                    records,
                    permit,
                });
            }
            0
        };
        let pin = inner
            .bootstrap
            .as_ref()
            .ok_or(RecordError::BootstrapExpired)?;
        pin.permit
            .deliver(|| ())
            .map_err(|_| RecordError::BootstrapExpired)?;
        if index > pin.records.len() {
            return Err(RecordError::Rejected(
                "bootstrap cursor outside snapshot".into(),
            ));
        }
        let mut bytes = 0;
        let mut records = Vec::new();
        for record in pin.records.iter().skip(index).take(limit) {
            let size = serde_json::to_vec(record).map_err(storage)?.len();
            if !records.is_empty() && bytes + size > PAGE_BYTES {
                break;
            }
            bytes += size;
            records.push(record.clone());
        }
        let end = index + records.len();
        let next = if end < pin.records.len() {
            Some(encode(&BootstrapCursor {
                version: 1,
                scope: inner.state.feed.scope.clone(),
                id: request_id,
                index: end,
            })?)
        } else {
            None
        };
        Ok(BootstrapPage {
            records,
            next,
            continuation: cursor(&inner.state, pin.seq)?,
            visible_seq: pin.seq,
            evaluation_time_ms: pin.at,
            expires_at_ms: pin.expires,
        })
    }
    /// Release a completed pin explicitly; the retained feed is unaffected.
    pub async fn release_bootstrap(&self, request_id: &str) -> Result<(), RecordError> {
        let mut inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        if inner.bootstrap.as_ref().is_some_and(|p| p.id == request_id) {
            inner.bootstrap = None;
            inner.bootstrap_epoch = format!("{:016x}", rand::random::<u64>());
        }
        Ok(())
    }
}

impl Inner {
    fn refresh_bootstrap(&mut self, at: u64) {
        if self.bootstrap.as_ref().is_some_and(|pin| {
            pin.expires <= at
                || pin.seq < self.state.feed.floor
                || pin.permit.deliver(|| ()).is_err()
        }) {
            self.bootstrap = None;
            self.bootstrap_epoch = format!("{:016x}", rand::random::<u64>());
        }
    }
}
impl RecordPartition {
    /// Prefix for a new bootstrap request_id: append sixteen random hex digits.
    /// Restart, release, expiry and hiding rotate this identity, so a lost first
    /// response cannot silently become a different snapshot on retry.
    pub async fn bootstrap_identity(&self) -> Result<String, RecordError> {
        self.advance_time().await?;
        let mut inner = self.inner.lock().await;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        let at = inner.evaluation_time_ms;
        inner.refresh_bootstrap(at);
        Ok(inner.bootstrap_epoch.clone())
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use crate::record::tests::{config,source_write,generation_write,stage_generation,write};

    fn apply_at(state:&mut State,settings:&RecordConfig,mut request:RecordWrite,at:u64) {
        request.operation_id.epoch=at/86_400_000;
        let mutation::PreparedWrite::Append {applied,candidate,..}=mutation::prepare_at(
            state,settings,checkpoint_codec::length(settings,state).unwrap(),request,at/86_400_000,at).unwrap()
            else {panic!("new operation")};
        if let Some(candidate)=candidate {*state=candidate;} else {apply(state,&applied).unwrap();}
    }

    #[tokio::test]
    async fn private_renewals_advance_retention_without_dropping_recent_public_changes() {
        let root=tempfile::tempdir().unwrap();
        let mut settings=config();settings.max_operations=2048;settings.max_journal_bytes=4*1024*1024;
        let initial=RecordPartition::create(&root.path().join("initial"),settings.clone()).await.unwrap();
        let mut state=initial.inner.lock().await.state.try_clone().unwrap();
        drop(initial);
        let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let beginning=now-8*86_400_000;
        apply_at(&mut state,&settings,source_write("source","s",&[]),beginning);
        apply_at(&mut state,&settings,generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["chunk"])),beginning);
        // Keep one upload genuinely alive across the retention period. Every
        // renewal is within its 30-minute lease; no fabricated long lease.
        for i in 1..=768u64 {
            apply_at(&mut state,&settings,generation_write(&format!("renew-{i}"),ExpectedVersion::Match(i),
                GenerationCommand::Renew {generation_id:"g".into()}),beginning+i*900_000);
        }
        let last_private=state.seq;
        apply_at(&mut state,&settings,write("recent-public","recent","beta",ExpectedVersion::Absent),now);
        let recent_seq=state.seq;
        validate_restored_state(&settings,&state).unwrap();
        assert_eq!(committed_floor(&state,beginning+settings.change_feed.retention_ms,
            settings.change_feed.retention_ms),Some(2),"upper bound includes both equal-time source and private stage");
        let safe=committed_floor(&state,now,settings.change_feed.retention_ms).unwrap();
        assert!(safe>1 && safe<last_private);
        // The previous algorithm stopped at the sole old public source event.
        assert_eq!(Cache::rebuild(&state,&settings.change_feed).unwrap().floor(now,settings.change_feed.retention_ms),Some(1));
        let old_wire=format!(r#"{{"previous":0,"floor":1,"at":{now}}}"#);
        let old:Trim=serde_json::from_str(&old_wire).unwrap();
        assert_eq!(serde_json::to_string(&old).unwrap(),old_wire);
        let mut old_replayed=state.try_clone().unwrap();
        apply_trim(&mut old_replayed,&settings.change_feed,&old).unwrap();
        let mut bad=Trim {previous:0,floor:recent_seq,at:now,complete_time_prefix:true};
        assert!(apply_trim(&mut state.try_clone().unwrap(),&settings.change_feed,&bad).is_err());
        bad.floor=safe;
        let mut replayed=state.try_clone().unwrap();
        apply_trim(&mut replayed,&settings.change_feed,&bad).unwrap();
        let path=root.path().join("live");
        let bytes=encode_entry(&Entry::Checkpoint {config:settings.clone(),state}).unwrap();
        drop(PartitionJournal::create(&path,settings.max_journal_bytes,&bytes).unwrap());
        let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        let before={let mut inner=p.inner.lock().await;
            inner.trim_feed(now).await.unwrap();
            assert_eq!(inner.state.feed.floor,safe);
            inner.state.operations.len()};
        assert!(matches!(p.write(write("trigger","trigger","gamma",ExpectedVersion::Absent)).await,Err(RecordError::Busy)));
        crate::record::tests::wait_maintenance_idle(&p).await;
        let inner=p.inner.lock().await;
        assert!(inner.state.operations.len()<before);
        assert!(inner.state.operations.iter().any(|op|op.receipt.seq==recent_seq));
        assert!(inner.state.operations.iter().any(|op|op.request.operation_id.nonce=="stage"));
        assert!(inner.state.operations.iter().any(|op|op.request.operation_id.nonce=="renew-768"));
        let from_floor=cursor(&inner.state,safe).unwrap();
        let stale=cursor(&inner.state,safe-1).unwrap();
        validate_restored_state(&settings,&inner.state).unwrap();
        drop(inner);drop(p);
        let recovered=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        assert!(matches!(recovered.changes(Some(stale),100).await,Err(RecordError::CursorExpired(_))));
        let page=recovered.changes(Some(from_floor),100).await.unwrap();
        assert_eq!(page.events.len(),1);
        assert_eq!(page.events[0].seq,recent_seq);
        assert_eq!(page.events[0].record_id.as_deref(),Some("recent"));
    }
}
