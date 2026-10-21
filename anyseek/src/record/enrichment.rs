//! Append-only vector enrichment of an exact body revision. Both admission and
//! replay compose the same authoritative document; no second vector journal.
use super::*;

pub(super) fn document(previous: Option<&Record>, request: &RecordWrite) -> Result<Option<Document>, RecordError> {
    let Some(patch) = &request.vector_patch else { return Ok(request.document.clone()); };
    if request.document.is_some() || request.system.is_some() || patch.is_empty() {
        return Err(RecordError::Rejected("vector patch requires nonempty fields and no document/system".into()));
    }
    let previous = previous.ok_or_else(|| RecordError::Conflict("vector target absent".into()))?;
    if !matches!(request.expected, ExpectedVersion::Match(v) if v == previous.version) {
        return Err(RecordError::Conflict("vector patch requires exact body version".into()));
    }
    let mut document = previous.document.clone().ok_or_else(|| RecordError::Conflict("vector target deleted".into()))?;
    for (field, value) in patch {
        if document.contains_key(field) { return Err(RecordError::Conflict("vector field already populated".into())); }
        document.insert(field.clone(), value.clone());
    }
    Ok(Some(document))
}

pub(super) fn validate(state: &State, config: &RecordConfig, request: &RecordWrite) -> Result<(), RecordError> {
    let Some(patch) = &request.vector_patch else { return Ok(()); };
    if !matches!(evidence::status(&state.records, &request.record_id)?, EvidenceVisibility::Visible) {
        return Err(RecordError::Conflict("vector target not visible".into()));
    }
    for field in patch.keys() {
        if !config.schema.iter().any(|f| f.index_vector && &f.field == field) {
            return Err(RecordError::Rejected("vector patch contains non-vector field".into()));
        }
    }
    Ok(())
}
