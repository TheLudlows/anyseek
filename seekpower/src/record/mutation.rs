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
    },
}

pub(super) fn prepare(
    state: &State,
    config: &RecordConfig,
    current_snapshot_bytes: usize,
    request: RecordWrite,
    wall_epoch: u64,
) -> Result<PreparedWrite, RecordError> {
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
    }
    // serde_json::Value equality ignores object insertion order; the
    // whole semantic request is compared, not a collision-prone digest.
    if let Some(old) = state
        .operation_lookup
        .get(&request.operation_id)
        .map(|&position| &state.operations[position])
    {
        return if serde_json::to_value(&old.request).map_err(storage)?
            == serde_json::to_value(&request).map_err(storage)?
        {
            Ok(PreparedWrite::Replay(old.receipt.clone()))
        } else {
            Err(RecordError::Conflict(
                "operation identity reused with another request".into(),
            ))
        };
    }
    if state.evidence_quarantined
        && (request.system.is_some()
            || state
                .records
                .get(&request.record_id)
                .is_some_and(|r| r.system.is_some()))
    {
        return Err(RecordError::Rejected(
            "restored evidence quarantined; deletion-history reconciliation not yet supported"
                .into(),
        ));
    }
    evidence::validate(&state.records, &request)?;
    // One fixed bounded delete command/receipt per live typed identity. This
    // reserve is retained even when an ancestor already hides that identity.
    let typed_live = state
        .records
        .values()
        .filter(|r| r.system.is_some() && r.document.is_some())
        .count();
    let deleting_typed = request.document.is_none()
        && state
            .records
            .get(&request.record_id)
            .is_some_and(|r| r.system.is_some());
    let creating_typed =
        request.system.is_some() && !state.records.contains_key(&request.record_id);
    let reserved_slots = typed_live + usize::from(creating_typed) - usize::from(deleting_typed);
    // 8 KiB covers JSON escaping of bounded IDs/nonce, envelope, receipt, and
    // journal frame overhead. No evidence edges are copied into delete requests.
    let reserve_bytes = reserved_slots * 8192;
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
    let live_sources = state.records.values().filter(|r| evidence::retained_revision(r)).count();
    let adding_history = previous.is_some_and(evidence::retained_revision);
    let new_source = previous.is_none() && request.system.as_ref().is_some_and(|s| s.kind == EvidenceKind::Source);
    let deleting_source = adding_history && request.document.is_none();
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
    if previous.is_none() && state.records.len() >= config.max_records {
        return Err(RecordError::Rejected("identity budget exceeded".into()));
    }
    if let Some(document) = &request.document {
        crate::vector::validate_vectors_in_schema(
            &config.index.inference,
            config.schema.iter(),
            document,
        )
        .map_err(RecordError::Rejected)?;
    }
    let receipt = WriteReceipt {
        seq: state
            .seq
            .checked_add(1)
            .ok_or_else(|| storage("sequence exhausted"))?,
        version: previous
            .map_or(Some(1), |r| r.version.checked_add(1))
            .ok_or_else(|| storage("version exhausted"))?,
        record_id: request.record_id.clone(),
    };
    let applied = Applied {
        request,
        receipt: receipt.clone(),
        observed_epoch: epoch,
    };
    let next_record = Record {
        record_id: applied.request.record_id.clone(),
        routing_key: applied.request.routing_key.clone(),
        version: receipt.version,
        document: applied.request.document.clone(),
        system: applied
            .request
            .system
            .clone()
            .or_else(|| previous.and_then(|p| p.system.clone())),
    };
    // Exact serialized checkpoint accounting, touching only this
    // operation and record. No complete state clone/serialization.
    let old_record = state.records.get(&next_record.record_id);
    let old_size = old_record
        .map(serde_json::to_vec)
        .transpose()
        .map_err(storage)?
        .map_or(0, |bytes| bytes.len());
    let map_overhead = if old_record.is_none() {
        serde_json::to_vec(&next_record.record_id)
            .map_err(storage)?
            .len()
            + 1
            + usize::from(!state.records.is_empty())
    } else {
        0
    };
    let history_bytes = if adding_history {
        old_size + usize::from(!state.source_revisions.is_empty())
    } else { 0 };
    let snapshot_bytes = current_snapshot_bytes + history_bytes - old_size
        + serde_json::to_vec(&next_record).map_err(storage)?.len()
        + map_overhead
        + serde_json::to_vec(&applied).map_err(storage)?.len()
        + usize::from(!state.operations.is_empty())
        + receipt.seq.to_string().len()
        - state.seq.to_string().len()
        + epoch.to_string().len()
        - state.epoch_watermark.to_string().len();
    if PartitionJournal::validate_initial(snapshot_bytes + reserve_bytes, config.max_journal_bytes)
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
    })
}
