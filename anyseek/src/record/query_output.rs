//! Query copies are admitted before allocation and retain their own byte leases.
use super::*;

/// Immutable query record. Clones share both the selected data and its admission.
#[derive(Clone)]
pub struct QueryRecord(Arc<Owner>);
struct Owner {
    record: Record,
    _memory: crate::storage::build_budget::MemoryLease,
    _scope: Option<crate::resources::Lease>,
}
impl std::ops::Deref for QueryRecord {
    type Target = Record;
    fn deref(&self) -> &Record { &self.0.record }
}
impl std::fmt::Debug for QueryRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.record.fmt(f) }
}
impl Serialize for QueryRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.record.serialize(serializer)
    }
}
impl QueryRecord {
    // None preserves the internal complete-record contract. Some contains the
    // exact output allowlist (including an empty allowlist); never search_fields.
    pub(super) fn copy(record: &Record, fields: Option<&[String]>) -> Result<Self, RecordError> {
        let selected = |name: &str| fields.is_none_or(|fields| fields.iter().any(|f| f == name));
        let document = record.document.as_ref().map(|doc| doc.iter()
            .filter(|(name, _)| selected(name))
            .collect::<BTreeMap<_, _>>());
        // Count a borrowed view. The existing 32x logical JSON allowance covers
        // Value/string containers; fixed overhead covers field names and the row.
        // This is explicit buffer admission, not an allocator/RSS hard limit.
        let bytes = (checkpoint_codec::json_length(&(&record.record_id, record.version,
            &document, &record.temporal, record.ingested_at_ms, record.expired_at_ms,
            &record.system))? as u64).checked_mul(32).and_then(|n| n.checked_add(1024))
            .ok_or(RecordError::Busy)?;
        let memory = crate::storage::build_budget::reserve_memory(bytes).map_err(|_| RecordError::Busy)?;
        let scope = crate::resources::Scope::current().map(|s| s.bytes(bytes))
            .transpose().map_err(|_| RecordError::Busy)?;
        let record = Record {
            record_id: record.record_id.clone(), version: record.version,
            document: record.document.as_ref().map(|doc| doc.iter()
                .filter(|(name, _)| selected(name)).map(|(k,v)| (k.clone(),v.clone())).collect()),
            temporal: record.temporal.clone(), ingested_at_ms: record.ingested_at_ms,
            expired_at_ms: record.expired_at_ms, system: record.system.clone(),
        };
        Ok(Self(Arc::new(Owner { record, _memory: memory, _scope: scope })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn selected_copies_and_shared_owners_keep_only_their_actual_charge() {
        let record = Record { record_id:"r".into(), version:1,
            document:Some(serde_json::from_value(serde_json::json!({
                "title":"small", "rank":i64::MAX, "body":"x".repeat(256 * 1024),
                "vector":vec![1.0; 4096]
            })).unwrap()), temporal:Default::default(), ingested_at_ms:0,
            expired_at_ms:None, system:None };
        let scope = Arc::new(crate::resources::Governor::default())
            .admit("result-owner".into(), crate::resources::Lane::Foreground, 0).unwrap();
        scope.clone().run(async {
            let projected = QueryRecord::copy(&record, Some(&["title".into(),"rank".into()])).unwrap();
            let bytes = projected.0._memory.bytes();
            assert!(bytes < 8192);
            let encoded = serde_json::to_string(&projected).unwrap();
            assert!(encoded.contains("\"rank\":9223372036854775807"));
            assert!(!encoded.contains("vector"));
            let remaining = scope.bytes(512 * 1024 * 1024 - bytes).unwrap();
            assert!(matches!(QueryRecord::copy(&record, None), Err(RecordError::Busy)));
            let copy = projected.clone();
            assert!(Arc::ptr_eq(&copy.0, &projected.0));
            drop(projected);
            assert!(scope.bytes(1).is_err(), "clone retains the original payload lease");
            drop(copy);
            assert!(scope.bytes(bytes).is_ok(), "last owner refunds admission");
            drop(remaining);
            let complete = QueryRecord::copy(&record, None).unwrap();
            assert!(complete.0._memory.bytes() > 8 * 1024 * 1024);
            assert_eq!(complete.document.as_ref().unwrap().len(),4);
            assert!(QueryRecord::copy(&record, Some(&[])).unwrap().document.as_ref().unwrap().is_empty());
        }).await;
    }
}
