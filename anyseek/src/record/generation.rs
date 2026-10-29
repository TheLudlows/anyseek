//! Bounded whole-document staging and publication in the partition's one WAL.
//! Staged bodies live only in the manifest; publication moves them into Records.
use super::*;
use std::collections::BTreeSet;

/// Permanent provenance of a published member, retained after generation retirement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GenerationMember {
    /// Document control identity.
    pub document_id: String,
    /// Immutable generation identity within that document.
    pub generation_id: String,
}
/// One member in a complete staging request. Published IDs are never reused.
/// Abort releases unexposed staged member IDs, but never its generation ID.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationChunk {
    /// Collection-unique member identity.
    pub record_id: String,
    /// Caller-produced body with optional vectors.
    pub document: Document,
    /// Optional UTC millisecond metadata, validated again at Publish.
    #[serde(default)]
    pub temporal: Option<TemporalMetadata>,
    /// Additional exact evidence; the manifest source is inherited automatically.
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
}
/// Whole-manifest commands. Stage is durable but never searchable.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum GenerationCommand {
    /// Reserve a fixed complete member list; payload arrives in bounded batches.
    Begin { /// Never-reused generation identity.
        generation_id:String, /// Exact source identity/version and route.
        source:EvidenceRef, /// Ordered complete membership.
        members:Vec<String> },
    /// Append previously absent declared members; never searchable until publication.
    StageBatch { /// Active upload identity.
        generation_id:String, /// Bounded batch, each member sent exactly once.
        chunks:Vec<GenerationChunk> },
    /// Verify complete membership and canonical manifest digest before publication.
    Seal { /// Active upload identity.
        generation_id:String, /// SHA-256 of the canonical manifest representation.
        manifest_sha256:String },
    /// Persist one complete manifest; an existing staging slot must first finish.
    Stage {
        /// Never-reused identity within this document.
        generation_id: String,
        /// Exact source revision in this collection and routing group.
        source: EvidenceRef,
        /// Declared complete member IDs, checked against chunks without duplicates.
        members: Vec<String>,
        /// Complete bounded payload, not a partial upload.
        chunks: Vec<GenerationChunk>,
    },
    /// Atomically replace the document's active generation with its staged one.
    Publish { /// Staged identity to publish.
        generation_id: String },
    /// Extend an unexpired staging lease; expected Match is the fencing version.
    Renew { /// Staged generation identity.
        generation_id: String },
    /// Discard staged bodies; the generation ID remains spent.
    Abort { /// Staged identity to discard.
        generation_id: String },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Staged {
    pub(super) deadline_ms: u64,
    pub(super) scope: String,
    pub(super) id: String,
    pub(super) source: EvidenceRef,
    pub(super) chunks: Vec<GenerationChunk>,
    pub(super) members: Vec<String>,
    pub(super) sealed: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Manifest {
    pub(super) routing_key: String,
    pub(super) version: u64,
    pub(super) used: BTreeSet<String>,
    pub(super) active: Option<String>,
    pub(super) active_members: Vec<String>,
    pub(super) staged: Option<Staged>,
}
impl Manifest { pub(super) fn was_used(&self,id:&str)->bool {self.used.contains(id)} }
/// Protected metadata only; staged or retired bodies are never returned here.
#[derive(Clone, Debug, Serialize)]
pub struct GenerationStatus {
    /// Stable document control identity (not an ordinary Record).
    pub record_id: String,
    /// Control version used by subsequent Stage/Publish/Abort commands.
    pub version: u64,
    /// Last published generation, possibly hidden by source invalidation.
    pub active_generation: Option<String>,
    /// Currently visible members, from the same read view.
    pub visible_members: usize,
    /// Durable, not-yet-published generation.
    pub staged_generation: Option<String>,
    /// Number of complete staged bodies.
    pub staged_members: usize,
    /// Declared total membership, reserved before any batch is accepted.
    pub expected_members: usize,
    /// Only sealed complete uploads may publish.
    pub sealed: bool,
    /// Canonical digest of the currently received manifest; compare at Seal.
    pub manifest_sha256: Option<String>,
    /// Trusted UTC millisecond deadline; equality is expired.
    pub lease_expires_at_ms: Option<u64>,
    /// Current control version required for renewal/publication.
    pub lease_fence: Option<u64>,
    /// A recovered owner must renew before publishing another owner's staging.
    pub lease_requires_renewal: bool,
    /// Logical cleanup is durable; old WAL retirement may need a retry.
    pub private_cleanup_pending: bool,
    /// Common partition commit position.
    pub visible_seq: u64,
}
fn rejected(s: &str) -> RecordError { RecordError::Rejected(s.into()) }
fn conflict(s: &str) -> RecordError { RecordError::Conflict(s.into()) }
fn bounded(s: &str) -> bool { !s.is_empty() && s.len() <= 256 }
pub(super) fn reserved(state: &State) -> usize {
    state.generations.counts().members
}
pub(super) fn reserve_slots(state: &State) -> usize {
    state.records.live_typed_count()
        + reserved(state) + state.generations.counts().controls
}
pub(super) fn staged_ttl_count(state:&State)->usize {
    state.generations.counts().ttl
}
pub(super) fn reserve_bytes(state: &State) -> usize {
    // Staging also reserves the move-to-Record envelope and publish operation;
    // this is additional to each member's eventual typed deletion reserve.
    reserve_slots(state) * 8192 + reserved(state) * 32768 + temporal::reserve_bytes(state)
}
pub(super) fn occupied(state: &State, id: &str) -> bool {
    state.records.contains_key(id) || state.generations.contains_key(id)
        || state.generations.values().filter_map(|g| g.staged.as_ref()).any(|s| s.members.iter().any(|c| c == id))
}
fn member(stage: &Staged, chunk: &GenerationChunk, route: &str, document_id: &str, ingestion:u64) -> Record {
    let mut refs = vec![stage.source.clone()];
    refs.extend(chunk.evidence_refs.iter().filter(|r| **r != stage.source).cloned());
    Record { record_id: chunk.record_id.clone(), routing_key: route.into(), version: 1,
        temporal:chunk.temporal.clone().unwrap_or_default(), ingested_at_ms:ingestion, expired_at_ms:None,
        document: Some(chunk.document.clone()), system: Some(EvidenceMetadata { kind: EvidenceKind::Derived, evidence_refs: refs, relation:None }), retired_generation: false, generation_member: Some(GenerationMember { document_id:document_id.into(), generation_id:stage.id.clone() }) }
}
pub(super) fn validate_stage(state: &State, config: &RecordConfig, stage: &Staged, request: &RecordWrite, evaluation:u64) -> Result<(), RecordError> {
    if state.independent_snapshot && stage.chunks.iter().any(|c| c.temporal.as_ref().is_some_and(|t| t.expire_at_ms.is_some())) { return Err(rejected("TTL generation requires a certified live owner")); }
    let source = state.records.get(&stage.source.record_id).ok_or_else(|| conflict("generation source absent"))?;
    if source.version != stage.source.version || source.routing_key != stage.source.routing_key
        || source.routing_key != request.routing_key
        || !source.system.as_ref().is_some_and(|s| s.kind == EvidenceKind::Source)
        || evidence::status(&state.records, &source.record_id)? != EvidenceVisibility::Visible {
        return Err(conflict("generation requires current visible source in its routing group"));
    }
    for chunk in &stage.chunks {
        let record = member(stage, chunk, &request.routing_key, &request.record_id, evaluation);
        let write = RecordWrite { operation_id: request.operation_id.clone(), record_id: record.record_id,
            routing_key: record.routing_key, expected: ExpectedVersion::Absent, document: record.document,
            system: record.system, vector_patch: None, generation: None, temporal:chunk.temporal.clone() };
        evidence::validate(&state.records, &write)?;
        temporal::validate(&chunk.temporal.clone().unwrap_or_default(),evaluation)?;
        let body = write.document.as_ref().unwrap();
        validate_document_depth(body)?;
        crate::index::validate_field_values(config.schema.iter(), body).map_err(rejected_owned)?;
        crate::vector::validate_vectors_in_schema(&config.index.inference, config.schema.iter(), body).map_err(rejected_owned)?;
        if serde_json::to_vec(&write).map_err(storage)?.len() > config.max_record_bytes { return Err(rejected("generation member exceeds record byte budget")); }
    }
    Ok(())
}
fn rejected_owned(s: String) -> RecordError { RecordError::Rejected(s) }
pub(super) fn prepare(state: &State, config: &RecordConfig, request: RecordWrite, epoch: u64, evaluation:u64) -> Result<mutation::PreparedWrite, RecordError> {
    if state.evidence_quarantined { return Err(rejected("generation authority quarantined")); }
    if request.document.is_some() || request.system.is_some() || request.vector_patch.is_some() || request.temporal.is_some()
        || !bounded(&request.record_id) || !bounded(&request.routing_key) || request.operation_id.nonce.len() > 128 {
        return Err(rejected("invalid generation command envelope"));
    }
    if serde_json::to_vec(&request).map_err(storage)?.len() > config.max_record_bytes { return Err(rejected("generation request byte budget exceeded")); }
    let old = state.generations.get(&request.record_id);
    if state.records.contains_key(&request.record_id) || (old.is_none() && occupied(state, &request.record_id)) {
        return Err(conflict("generation control identity already used"));
    }
    if !match request.expected { ExpectedVersion::Absent => old.is_none(), ExpectedVersion::Match(v) => old.is_some_and(|g| g.version == v), ExpectedVersion::Any => false }
        || old.is_some_and(|g| g.routing_key != request.routing_key) { return Err(conflict("generation control version or route mismatch")); }
    match request.generation.as_ref().unwrap() {
        GenerationCommand::Stage { generation_id, source, members, chunks } => {
            if !bounded(generation_id) || chunks.is_empty() || chunks.len() > 100
                || old.is_some_and(|g| g.staged.is_some() || g.used.contains(generation_id)) { return Err(conflict("staging occupied, spent generation ID, or invalid member count")); }
            let declared: BTreeSet<_> = members.iter().collect(); let actual: BTreeSet<_> = chunks.iter().map(|c| &c.record_id).collect();
            if declared.len() != members.len() || actual.len() != chunks.len() || declared != actual
                || chunks.iter().any(|c| !bounded(&c.record_id) || c.record_id == request.record_id || occupied(state, &c.record_id)) {
                return Err(rejected("generation member manifest incomplete, duplicate or occupied"));
            }
            validate_stage(state, config, &Staged { deadline_ms:lease::deadline(evaluation)?, scope:state.history.scope.clone(), id:generation_id.clone(), source:source.clone(), chunks:chunks.clone(),members:members.clone(),sealed:true }, &request, evaluation)?;
        }
        GenerationCommand::Begin{..}|GenerationCommand::StageBatch{..}|GenerationCommand::Seal{..} => {
            upload::validate_request(state,config,&request,evaluation)?;
        }
        GenerationCommand::Publish { generation_id } | GenerationCommand::Abort { generation_id } | GenerationCommand::Renew { generation_id } => {
            let stage = old.and_then(|g| g.staged.as_ref()).filter(|s| &s.id == generation_id).ok_or_else(|| conflict("staged generation mismatch"))?;
            if stage.deadline_ms<=evaluation{return Err(conflict("staging lease expired"));}
            if matches!(request.generation,Some(GenerationCommand::Renew{..})){lease::deadline(evaluation)?;}
            if matches!(request.generation, Some(GenerationCommand::Publish { .. })) {
                if !stage.sealed{return Err(conflict("generation must be sealed before publication"));}
                if stage.scope!=state.history.scope{return Err(conflict("restored staging requires renewed fencing lease"));}
                validate_stage(state, config, stage, &request, evaluation)?;
            }
        }
    }
    let receipt = WriteReceipt { staged_only: matches!(request.generation, Some(GenerationCommand::Stage { .. }|GenerationCommand::Begin{..}|GenerationCommand::StageBatch{..}|GenerationCommand::Seal{..}|GenerationCommand::Renew { .. })), seq: state.seq.checked_add(1).ok_or_else(|| storage("sequence exhausted"))?,
        version: old.map_or(Some(1), |g| g.version.checked_add(1)).ok_or_else(|| storage("generation version exhausted"))?, record_id:request.record_id.clone() };
    let applied = Applied { request_digest:None, legacy_graph_invalidation:false, request, receipt, observed_epoch:epoch, observed_time_ms:evaluation };
    let growth=checkpoint_codec::json_length(&applied)?.checked_mul(3).ok_or_else(|| storage("generation state growth overflow"))?;
    let mut candidate = state.try_clone_with_growth(growth)?; super::apply(&mut candidate, &applied)?;
    validate_state(&candidate, config)?;
    if matches!(applied.request.generation, Some(GenerationCommand::Publish { .. })) {
        let visible = evidence::visibility(&candidate.records)?;
        if candidate.generations[&applied.request.record_id].active_members.iter().any(|id| visible.get(id) != Some(&true)) {
            return Err(conflict("replacement generation would invalidate its own evidence"));
        }
    }
    let reserve_bytes = reserve_bytes(&candidate);
    if candidate.operations.len() + reserve_slots(&candidate) > config.max_operations { return Err(rejected("generation operation reserve exceeded")); }
    let snapshot_bytes = checkpoint_codec::length(config, &candidate)?;
    checkpoint_codec::validate_size(snapshot_bytes, reserve_bytes, config.max_journal_bytes).map_err(|_| rejected("generation checkpoint reserve exceeded"))?;
    let bytes = encode_entry(&Entry::Applied(applied.clone()))?;
    Ok(mutation::PreparedWrite::Append { applied, bytes, snapshot_bytes, reserve_bytes, candidate: Some(candidate), planning: Vec::new() })
}
pub(super) fn apply(state: &mut State, applied: &Applied) -> Result<(), RecordError> {
    let request = &applied.request;
    let old = state.generations.get(&request.record_id);
    if applied.receipt.version != old.map_or(1, |g| g.version.saturating_add(1)) { return Err(storage("inconsistent generation version")); }
    let _manifest_copy=crate::storage::build_budget::reserve_memory(
        (old.map(checkpoint_codec::json_length).transpose()?.unwrap_or(0) as u64).checked_mul(32).ok_or_else(||storage("generation metadata copy overflow"))?
    ).map_err(storage)?;
    let mut manifest = old.cloned().unwrap_or(Manifest { routing_key:request.routing_key.clone(), version:0, used:BTreeSet::new(), active:None, active_members:vec![], staged:None });
    match request.generation.as_ref().unwrap() {
        GenerationCommand::Stage { generation_id, source, chunks, members } => {
            if manifest.staged.is_some() || !manifest.used.insert(generation_id.clone()) { return Err(storage("inconsistent generation staging")); }
            manifest.staged = Some(Staged { deadline_ms:lease::deadline(applied.observed_time_ms)?, scope:state.history.scope.clone(), id:generation_id.clone(), source:source.clone(), chunks:chunks.clone(),members:members.clone(),sealed:true });
        }
        GenerationCommand::Begin{..}|GenerationCommand::StageBatch{..}|GenerationCommand::Seal{..} => {
            upload::apply(&mut manifest,request,applied.observed_time_ms,&state.history.scope)?;
        }
        GenerationCommand::Publish { generation_id } => {
            let stage = manifest.staged.take().filter(|s| &s.id == generation_id).ok_or_else(|| storage("missing published stage"))?;
            for id in &manifest.active_members { state.records.edit(id, 0, |record| {record.retired_generation=true;Ok(())})?; }
            manifest.active_members.clear();
            for chunk in &stage.chunks {
                if state.records.contains_key(&chunk.record_id) { return Err(storage("published member identity reused")); }
                state.records.insert(chunk.record_id.clone(), member(&stage, chunk, &manifest.routing_key, &request.record_id, applied.observed_time_ms))?;
                manifest.active_members.push(chunk.record_id.clone());
            }
            manifest.active = Some(stage.id);
        }
        GenerationCommand::Renew { generation_id } => {
            let stage=manifest.staged.as_mut().filter(|s|&s.id==generation_id).ok_or_else(||storage("missing renewed stage"))?;
            if stage.deadline_ms<=applied.observed_time_ms{return Err(storage("renewed expired lease"));}
            stage.deadline_ms=lease::deadline(applied.observed_time_ms)?;stage.scope=state.history.scope.clone();
        }
        GenerationCommand::Abort { generation_id } => {
            if manifest.staged.take().is_none_or(|s| &s.id != generation_id) { return Err(storage("missing aborted stage")); }
            lease::redact(state,&request.record_id,generation_id)?;
        }
    }
    manifest.version = applied.receipt.version;
    state.generations.insert(request.record_id.clone(), manifest);
    Ok(())
}
pub(super) fn validate_state(state: &State, config: &RecordConfig) -> Result<(), RecordError> {
    state.generations.validate_counts()?;
    if state.records.len() + state.generations.len() + reserved(state) > config.max_records
        || state.generations.values().map(|g| g.used.len()).sum::<usize>() > config.max_records {
        return Err(rejected("generation retained identity budget exceeded"));
    }
    for record in state.records.values() {
        if let Some(member) = &record.generation_member {
            let g = state.generations.get(&member.document_id).ok_or_else(|| storage("orphan generation member"))?;
            if !g.used.contains(&member.generation_id) || g.routing_key != record.routing_key
                || !record.system.as_ref().is_some_and(|s| s.kind == EvidenceKind::Derived)
                || record.retired_generation == (g.active.as_ref() == Some(&member.generation_id))
                || g.active_members.contains(&record.record_id) != (g.active.as_ref() == Some(&member.generation_id)) {
                return Err(storage("inconsistent generation member provenance"));
            }
        } else if record.retired_generation { return Err(storage("retired record lacks generation provenance")); }
    }
    let mut staged = BTreeSet::new(); let mut active = BTreeSet::new();
    for (id, g) in &state.generations {
        if state.records.contains_key(id) || g.version == 0 || g.active.as_ref().is_some_and(|a| !g.used.contains(a)) { return Err(storage("invalid generation manifest")); }
        for member in &g.active_members {
            if !active.insert(member) || state.records.get(member).is_none_or(|r| r.generation_member.as_ref().is_none_or(|m| &m.document_id != id || Some(&m.generation_id) != g.active.as_ref()) || r.retired_generation || r.routing_key != g.routing_key || !r.system.as_ref().is_some_and(|s| s.kind == EvidenceKind::Derived)) { return Err(storage("invalid active generation member")); }
        }
        if let Some(s) = &g.staged {
            upload::validate_history(state,id,g)?;
            if s.scope.is_empty()||s.deadline_ms==0{return Err(storage("invalid staging lease"));}
            locator::validate_retained(state,&s.source)?;
            for reference in s.chunks.iter().flat_map(|c|c.evidence_refs.iter()) {locator::validate_retained(state,reference)?;}
            let source = state.records.get(&s.source.record_id).ok_or_else(|| storage("staged source missing"))?;
            if s.source.version == 0 || s.source.version > source.version || source.routing_key != s.source.routing_key || source.routing_key != g.routing_key
                || !source.system.as_ref().is_some_and(|s| s.kind == EvidenceKind::Source) { return Err(storage("invalid staged source provenance")); }
            if !g.used.contains(&s.id) || upload::validate_manifest(s).is_err() { return Err(storage("invalid staged generation")); }
            for member in &s.members {if !staged.insert(member) || state.records.contains_key(member) || state.generations.contains_key(member){return Err(storage("invalid staged member reservation"));}}
            for c in &s.chunks { if let Some(metadata)=&c.temporal { temporal::validate(metadata,0).map_err(storage)?; } if !s.members.contains(&c.record_id) { return Err(storage("invalid staged member reservation")); } }
        }
    }
    Ok(())
}
impl RecordPartition {
    /// Read protected manifest metadata without exposing staged bodies.
    pub async fn generation_status(&self, id: &str) -> Result<Option<GenerationStatus>, RecordError> {
        self.advance_time().await?;
        let _permit = self.admission.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let inner = self.inner.lock().await;
        if !inner.healthy { return Err(RecordError::RecoveryRequired); }
        let Some(g) = inner.state.generations.get(id) else { return Ok(None); };
        Ok(Some(GenerationStatus { record_id:id.into(), version:g.version, active_generation:g.active.clone(),
            visible_members:g.active_members.iter().filter(|id| inner.locations.contains_key(*id)).count(),
            lease_expires_at_ms:g.staged.as_ref().map(|s|s.deadline_ms), lease_fence:g.staged.as_ref().map(|_|g.version), lease_requires_renewal:g.staged.as_ref().is_some_and(|s|s.scope!=inner.state.history.scope), private_cleanup_pending:inner.state.private_cleanup_pending || inner.maintenance_gc_pending,
            expected_members:g.staged.as_ref().map_or(0,|s|s.members.len()),sealed:g.staged.as_ref().is_some_and(|s|s.sealed),manifest_sha256:g.staged.as_ref().map(upload::manifest_digest).transpose()?,
            staged_generation:g.staged.as_ref().map(|s| s.id.clone()), staged_members:g.staged.as_ref().map_or(0, |s| s.chunks.len()), visible_seq:inner.state.seq }))
    }
}
