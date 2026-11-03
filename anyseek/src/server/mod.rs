//! HTTP contracts for the AnySeek service.
pub mod api_schema;
pub mod api_types;

/// Service bridge: apply the HTTP stored-field allowlist before result copies.
/// The core query methods continue returning complete records.
#[doc(hidden)]
pub async fn query_records(
    partition: &crate::record::RecordPartition,
    query: crate::record::RecordQuery,
    fields: Vec<String>,
) -> Result<crate::record::RecordSearch, crate::record::RecordError> {
    partition.query_output(query, None, Some(fields)).await
}
