//! Technical visibility of the current record, without business dependencies.
use super::*;

/// Current record state. Historical source and dependency states belong to applications.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all="snake_case")]
pub enum RecordVisibility {
    /// Current body participates in retrieval.
    Visible,
    /// Current identity is a deletion tombstone.
    Deleted,
    /// Current body has expired.
    Expired,
}
pub(super) fn status(record: &Record, evaluation:u64) -> RecordVisibility {
    if record.expired_at_ms.is_some() || record.document.is_some() && !temporal::visible(record,evaluation) { RecordVisibility::Expired }
    else if record.document.is_some() { RecordVisibility::Visible }
    else { RecordVisibility::Deleted }
}
pub(super) fn visible_records(records: &state_values::Records) -> Result<BTreeMap<String, bool>, RecordError> {
    Ok(records.iter().map(|(id,r)| (id.clone(),r.document.is_some())).collect())
}
