//! Validate a Record command without touching authority or derived indexes.
//!
//! This is the single live-write planner. It runs under the partition executor's
//! guard; a plan must never be retained across another state transition. Recovery
//! consumes Applied decisions, not this admission path (wall time and current
//! retry windows must not reinterpret previously durable commands).
use super::*;

pub(super) enum PreparedWrite {
    Replay(WriteReceipt),
    Append {
        applied: Applied,
        bytes: Vec<u8>,
        snapshot_bytes: usize,
        reserve_bytes: usize,
        candidate: Option<State>,
        planning: Vec<crate::storage::build_budget::MemoryLease>,
    },
}

#[cfg(test)]
pub(super) fn prepare(
    state: &State,
    config: &RecordConfig,
    current_snapshot_bytes: usize,
    request: RecordWrite,
    wall_epoch: u64,
) -> Result<PreparedWrite, RecordError> {
    prepare_at(state,config,current_snapshot_bytes,request,wall_epoch,state.time_watermark_ms)
}
pub(super) fn prepare_at(state:&State, config:&RecordConfig, current_snapshot_bytes:usize, request:RecordWrite, wall_epoch:u64, evaluation_time_ms:u64)->Result<PreparedWrite,RecordError> {
    let epoch = wall_epoch.max(state.epoch_watermark);
    if request.operation_id.epoch > epoch || request.operation_id.epoch.saturating_add(1) < epoch {
        return Err(RecordError::OperationExpired);
    }
    if request.record_id.is_empty()
        || request.routing_key.is_empty()
        || request.operation_id.nonce.is_empty()
    {
        return Err(RecordError::Rejected("empty identity".into()));
    }
    if let Some(document) = &request.document {
        validate_document_depth(document)?;
        crate::index::validate_field_values(config.schema.iter(), document).map_err(RecordError::Rejected)?;
    }
    // Own temporary planner copies before cloning request/prior document data.
    // The caller's incoming request and the retained old value are separate owners.
    let request_bytes=checkpoint_codec::json_length(&request)? as u64;
    let _request_plan=crate::storage::build_budget::reserve_memory(request_bytes.checked_mul(32).ok_or_else(||storage("planner request estimate overflow"))?).map_err(storage)?;
    // serde_json::Value equality ignores object insertion order; the
    // whole semantic request is compared, not a collision-prone digest.
    if let Some(old) = state
        .operation_lookup
        .get(&request.operation_id)
        .map(|&position| &state.operations[position])
    {
        let comparison=(checkpoint_codec::json_length(&old.request)? as u64).checked_mul(32).ok_or_else(||storage("retry comparison estimate overflow"))?;
        let _comparison=crate::storage::build_budget::reserve_memory(comparison).map_err(storage)?;
        return if lease::same_request(old,&request)?
        {
            Ok(PreparedWrite::Replay(old.receipt.clone()))
        } else {
            Err(RecordError::Conflict(
                "operation identity reused with another request".into(),
            ))
        };
    }
    // Replay compares the retained request; it must not reserve the current
    // record body, which may have grown since that operation was committed.
    let prior_bytes=state.records.get(&request.record_id).map(checkpoint_codec::json_length).transpose()?.unwrap_or(0) as u64;
    let _prior_plan=crate::storage::build_budget::reserve_memory(prior_bytes.checked_mul(32).ok_or_else(||storage("planner prior estimate overflow"))?).map_err(storage)?;
    if request.generation.is_some() {
        let mut prepared=generation::prepare(state,config,request,epoch,evaluation_time_ms)?;
        if let PreparedWrite::Append{planning,..}=&mut prepared {planning.extend([_request_plan,_prior_plan]);}
        return Ok(prepared);
    }
    if state.generations.contains_key(&request.record_id)
        || (!state.records.contains_key(&request.record_id) && generation::occupied(state, &request.record_id)) {
        return Err(RecordError::Conflict("identity reserved by generation".into()));
    }
    if state.evidence_quarantined
        && (request.system.is_some()
            || request.temporal.as_ref().is_some_and(|t| t.expire_at_ms.is_some())
            || state
                .records
                .get(&request.record_id)
                .is_some_and(temporal::protected))
    {
        return Err(RecordError::Rejected(
            "restored evidence quarantined; deletion-history reconciliation not yet supported"
                .into(),
        ));
    }
    if request.vector_patch.is_some() {
        enrichment::validate(state, config, &request)?;
    } else {
        evidence::validate(&state.records, &request)?;
    }
    let temporal = temporal::retained(state.records.get(&request.record_id), &request, evaluation_time_ms)?;
    if state.independent_snapshot && temporal.expire_at_ms.is_some() { return Err(RecordError::Rejected("TTL requires a certified live owner".into())); }
    let next_document = enrichment::document(state.records.get(&request.record_id), &request)?;
    if let Some(document) = &next_document {
        validate_document_depth(document)?;
        crate::index::validate_field_values(config.schema.iter(), document).map_err(RecordError::Rejected)?;
        if serde_json::to_vec(document).map_err(storage)?.len() > config.max_record_bytes {
            return Err(RecordError::Rejected("enriched record byte budget exceeded".into()));
        }
    }
    // One fixed bounded delete command/receipt per live typed identity. This
    // reserve is retained even when an ancestor already hides that identity.
    let deleting_typed = next_document.is_none()
        && state
            .records
            .get(&request.record_id)
            .is_some_and(|r| r.system.is_some());
    let creating_typed =
        request.system.is_some() && !state.records.contains_key(&request.record_id);
    let reserved_slots = generation::reserve_slots(state) + usize::from(creating_typed) - usize::from(deleting_typed);
    // 8 KiB covers JSON escaping of bounded IDs/nonce, envelope, receipt, and
    // journal frame overhead. No evidence edges are copied into delete requests.
    let previous_ttl=state.records.get(&request.record_id).is_some_and(temporal::live_ttl);
    let next_ttl=next_document.is_some() && temporal.expire_at_ms.is_some();
    let reserve_bytes = reserved_slots * 8192 + generation::reserved(state) * 32768
        + temporal::reserve_bytes(state) + usize::from(next_ttl)*8192 - usize::from(previous_ttl)*8192;
    if serde_json::to_vec(&request).map_err(storage)?.len() > config.max_record_bytes
        || state
            .operations
            .len()
            .saturating_add(1)
            .saturating_add(reserved_slots)
            > config.max_operations
    {
        return Err(RecordError::Rejected(
            "record/operation budget exceeded".into(),
        ));
    }
    let previous = state.records.get(&request.record_id);
    let live_sources = state.records.retained_source_count();
    let adding_history = request.vector_patch.is_none() && previous.is_some_and(evidence::retained_revision);
    let new_source = previous.is_none() && request.system.as_ref().is_some_and(|s| s.kind == EvidenceKind::Source);
    let deleting_source = adding_history && next_document.is_none();
    if state.source_revisions.len() + usize::from(adding_history) + live_sources
        + usize::from(new_source) - usize::from(deleting_source) > config.max_records {
        return Err(RecordError::Rejected("source revision budget reserves final deletion slots".into()));
    }

    if previous.is_some_and(|r| r.routing_key != request.routing_key) {
        return Err(RecordError::Conflict("routing key cannot change".into()));
    }
    let matches = match request.expected {
        ExpectedVersion::Any => true,
        ExpectedVersion::Absent => previous.is_none_or(|r| r.document.is_none()),
        ExpectedVersion::Match(v) => previous.is_some_and(|r| r.version == v),
    };
    if !matches {
        return Err(RecordError::Conflict("version precondition failed".into()));
    }
    if previous.is_none() && state.records.len() + state.generations.len() + generation::reserved(state) >= config.max_records {
        return Err(RecordError::Rejected("identity budget exceeded".into()));
    }
    if let Some(document) = &next_document {
        crate::vector::validate_vectors_in_schema(
            &config.index.inference,
            config.schema.iter(),
            document,
        )
        .map_err(RecordError::Rejected)?;
    }
    let receipt = WriteReceipt {
        staged_only: false,
        seq: state
            .seq
            .checked_add(1)
            .ok_or_else(|| storage("sequence exhausted"))?,
        version: previous
            .map_or(Some(1), |r| if request.vector_patch.is_some() { Some(r.version) } else { r.version.checked_add(1) })
            .ok_or_else(|| storage("version exhausted"))?,
        record_id: request.record_id.clone(),
    };
    if next_ttl && receipt.version == u64::MAX { return Err(RecordError::Rejected("TTL requires a reserved tombstone version".into())); }
    let applied = Applied { request_digest:None,
        legacy_graph_invalidation:false,
        request,
        receipt: receipt.clone(),
        observed_epoch: epoch,
        observed_time_ms: evaluation_time_ms,
    };
    let next_record = projected_record(state, &applied, next_document, temporal);
    let snapshot_bytes = snapshot_after(state, current_snapshot_bytes, &applied, &next_record)?;
    if checkpoint_codec::validate_size(snapshot_bytes, reserve_bytes, config.max_journal_bytes)
        .is_err()
    {
        return Err(RecordError::Rejected(
            "complete checkpoint budget exceeded".into(),
        ));
    }
    let bytes = encode_entry(&Entry::Applied(applied.clone()))?;
    Ok(PreparedWrite::Append {
        applied,
        bytes,
        snapshot_bytes,
        reserve_bytes,
        candidate: None,
        planning: vec![_request_plan,_prior_plan],
    })
}

// Shared arithmetic only: recovery never re-runs admission, wall-clock policy,
// expected-version planning or operation-ID generation for a durable decision.
pub(super) fn projected_record(state: &State, applied: &Applied, document: Option<Document>, temporal: TemporalMetadata) -> Record {
    let previous = state.records.get(&applied.request.record_id);
    Record {
        record_id: applied.request.record_id.clone(),
        routing_key: applied.request.routing_key.clone(),
        version: applied.receipt.version,
        document,
        temporal, ingested_at_ms: if applied.request.document.is_none() { previous.map_or(applied.observed_time_ms, |r| r.ingested_at_ms) } else { applied.observed_time_ms }, expired_at_ms: None,
        retired_generation: previous.is_some_and(|r| r.retired_generation),
        generation_member: previous.and_then(|r| r.generation_member.clone()),
        system: applied
            .request
            .system
            .clone()
            .or_else(|| previous.and_then(|p| p.system.clone())),
    }
}

pub(super) fn snapshot_after(state: &State, before: usize, applied: &Applied, next: &Record) -> Result<usize, RecordError> {
    let old = state.records.get(&next.record_id);
    let old_bytes = old.map(checkpoint_codec::json_length).transpose()?.unwrap_or(0);
    let history = if applied.request.vector_patch.is_none() && old.is_some_and(evidence::retained_revision) {
        old_bytes.checked_add(usize::from(!state.source_revisions.is_empty())).ok_or_else(||storage("snapshot history size overflow"))?
    } else { 0 };
    let key = if old.is_none() { checkpoint_codec::json_length(&next.record_id)?.checked_add(1+usize::from(!state.records.is_empty())).ok_or_else(||storage("snapshot key size overflow"))? } else { 0 };
    let delta = |new:u64, old:u64| new.to_string().len().checked_sub(old.to_string().len()).ok_or_else(||storage("snapshot scalar regressed"));
    let additions = [history,key,checkpoint_codec::json_length(next)?,checkpoint_codec::json_length(applied)?,usize::from(!state.operations.is_empty()),
        delta(applied.receipt.seq,state.seq)?,delta(applied.observed_epoch.max(state.epoch_watermark),state.epoch_watermark)?,delta(applied.observed_time_ms.max(state.time_watermark_ms),state.time_watermark_ms)?];
    additions.into_iter().try_fold(before.checked_sub(old_bytes).ok_or_else(||storage("snapshot size underflow"))?, |sum,n|sum.checked_add(n).ok_or_else(||storage("snapshot size overflow")))
}

#[cfg(test)]
pub(super) fn replay_checkpoint_size(state: &State, before: usize, applied: &Applied) -> Result<usize, RecordError> {
    if applied.request.generation.is_some() || state.seq.checked_add(1)!=Some(applied.receipt.seq) || applied.receipt.record_id!=applied.request.record_id || applied.observed_time_ms<state.time_watermark_ms {
        return Err(storage("invalid ordinary replay size transition"));
    }
    let previous=state.records.get(&applied.request.record_id);
    let document=enrichment::document(previous,&applied.request)?;
    let temporal=temporal::retained(previous,&applied.request,applied.observed_time_ms)?;
    snapshot_after(state,before,applied,&projected_record(state,applied,document,temporal))
}


#[cfg(test)]
mod replay_size_tests {
    #[test]
    fn durable_counter_retains_higher_epoch_watermark() {
        let config=super::super::tests::config();let mut state=State::default();
        let mut before=checkpoint_codec::length(&config,&state).unwrap();
        for (i,epoch) in [100,99].into_iter().enumerate() {
            let mut request=super::super::tests::write(&format!("epoch-{i}"),"r","alpha",ExpectedVersion::Any);
            request.operation_id.epoch=epoch;
            let applied=Applied{request_digest:None,legacy_graph_invalidation:false,
                receipt:WriteReceipt{staged_only:false,seq:i as u64+1,version:i as u64+1,record_id:"r".into()},
                request,observed_epoch:epoch,observed_time_ms:99+i as u64};
            let bytes=replay_checkpoint_size(&state,before,&applied).unwrap();
            apply(&mut state,&applied).unwrap();
            assert_eq!(bytes,encode_entry(&Entry::Checkpoint{config:config.clone(),state:state.clone()}).unwrap().len());
            assert_eq!(state.epoch_watermark,100);before=bytes;
        }
        assert_eq!(state.operations.len(),2);
    }

    use super::*;
    #[test]
    fn durable_size_matches_full_wire_with_history_tombstones_and_digit_boundaries() {
        let mut config=super::super::tests::config();config.max_records=512;config.max_operations=512;config.max_record_bytes=1024*1024;config.max_journal_bytes=16*1024*1024;
        let mut state=State::default();let mut before=checkpoint_codec::length(&config,&state).unwrap();
        for i in 0..104 {
            let id=if i<100{format!("id-\"{i}繁")}else{"id-\"0繁".into()};
            let mut request=super::super::tests::write(&format!("op-{i}"),&id,"alpha",ExpectedVersion::Any);
            request.operation_id.epoch=9+i/10;
            request.system=Some(EvidenceMetadata{kind:EvidenceKind::Source,relation:None,evidence_refs:vec![]});
            if i==0 {request.document.as_mut().unwrap().insert("opaque".into(),serde_json::json!("large\n繁體".repeat(8192)));}
            if i==102 {request.document=None;}
            let previous=state.records.get(&id);
            let applied=Applied{request_digest:None,legacy_graph_invalidation:false,receipt:WriteReceipt{staged_only:false,seq:state.seq+1,version:previous.map_or(1,|r|r.version+1),record_id:id},request,observed_epoch:9+i/10,observed_time_ms:99+i};
            let bytes=replay_checkpoint_size(&state,before,&applied).unwrap();
            apply(&mut state,&applied).unwrap();
            let wire=encode_entry(&Entry::Checkpoint{config:config.clone(),state:state.clone()}).unwrap();
            assert_eq!(bytes,wire.len(),"operation {i}");before=bytes;
        }
        assert_eq!(state.operations.len(),104);assert_eq!(state.source_revisions.len(),3);
        assert_eq!(state.source_revisions[0].document.as_ref().unwrap()["opaque"].as_str().unwrap().len(),"large\n繁體".repeat(8192).len());
    }
    #[test]
    fn malformed_replay_sequence_and_size_fail_closed() {
        let mut state=State::default();let request=super::super::tests::write("op","id","alpha",ExpectedVersion::Any);
        let mut applied=Applied{request_digest:None,legacy_graph_invalidation:false,receipt:WriteReceipt{staged_only:false,seq:1,version:1,record_id:"id".into()},request,observed_epoch:1,observed_time_ms:1};
        apply(&mut state,&applied).unwrap();assert!(replay_checkpoint_size(&state,0,&applied).is_err());
        applied.receipt.seq=2;applied.receipt.version=2;applied.request.operation_id=OperationId::new("next".into());assert!(replay_checkpoint_size(&state,0,&applied).is_err());
    }
}
