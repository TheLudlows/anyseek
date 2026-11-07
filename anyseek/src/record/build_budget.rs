//! Conservative logical admission. Deliberately not allocator/RSS accounting.
use super::*;
use crate::storage::build_budget::{MemoryLease, reserve_memory};
pub(crate) fn document_estimate(
    document: &Document,
    schema: &[SchemaField],
) -> Result<u64, RecordError> {
    struct Count(u64);
    impl std::io::Write for Count {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(b.len() as u64)
                .ok_or_else(|| std::io::Error::other("build estimate overflow"))?;
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    fn text_tokens(s: &str) -> u64 {
        let mut count = 0;
        let mut word = false;
        for c in s.chars() {
            if !c.is_ascii() {
                count += 1;
                word = false;
            } else if c.is_ascii_alphanumeric() {
                if !word {
                    count += 1;
                }
                word = true;
            } else {
                word = false;
            }
        }
        count.max(s.split_whitespace().count() as u64)
    }
    fn tokens(v: &serde_json::Value) -> u64 {
        match v {
            serde_json::Value::String(s) => text_tokens(s),
            serde_json::Value::Array(a) => a.iter().map(tokens).sum(),
            serde_json::Value::Object(o) => o.iter().map(|(k, v)| text_tokens(k) + tokens(v)).sum(),
            _ => 1,
        }
    }
    let fields = schema
        .iter()
        .filter(|f| f.index_lexical || f.index_vector)
        .count() as u64;
    let mut bytes = Count(0);
    serde_json::to_writer(&mut bytes, document).map_err(storage)?;
    let terms = schema
        .iter()
        .filter(|f| f.index_lexical)
        .filter_map(|f| document.get(&f.field))
        .map(tokens)
        .sum::<u64>();
    // Includes normalized text, transient JSON copies, vector/centroid
    // copies, and at most unigram/bigram/trigram logical term metadata.
    // Unicode/OpenCC expansion is conservatively included here;
    // HashMap buckets and allocator/realloc overhead remain excluded.
    let n = bytes
        .0
        .checked_mul(32)
        .and_then(|n| {
            terms
                .checked_mul(3 * (256 + fields * 96))
                .and_then(|t| n.checked_add(t))
        })
        .ok_or_else(|| storage("build estimate overflow"))?;
    Ok(n)
}
/// Match the synchronous native input without cloning the temporal document.
/// Synthetic temporal fields are numeric, non-lexical metadata; token charges
/// stay unchanged while the exact serialized-input contribution is replaced.
pub(super) fn indexed_document_estimate(document:&Document,schema:&[SchemaField],metadata:&TemporalMetadata,ingested:u64)->Result<u64,RecordError>{
    let raw=checkpoint_codec::json_length(document)? as u64;
    let indexed=temporal::indexed_length(document,metadata,ingested,|_|true)? as u64;
    document_estimate(document,schema)?.checked_sub(raw.checked_mul(32).ok_or_else(||storage("native input estimate overflow"))?)
        .and_then(|n|n.checked_add(indexed.checked_mul(32)?)).ok_or_else(||storage("native indexed input estimate overflow"))
}

/// Retained vector/facet projection only. Canonical bodies belong to State/Payload;
/// the full input Document has a distinct short-lived lease in component::append.
/// 1024 bytes per row cover row metadata (<=64 x9 bytes including presence markers),
/// active-mask/address bookkeeping and conservative container overhead. Variable identity bytes
/// are admitted separately; ordinary Record identities have no256-byte cap.
pub(crate) fn identity_estimate(id: &str) -> Result<u64,RecordError> {
    (id.len() as u64).checked_mul(3).ok_or_else(||storage("component identity estimate overflow"))
}

pub(crate) fn component_estimate<'a>(document: &Document, fields: impl Iterator<Item=&'a str>) -> Result<u64,RecordError> {
    let selected=fields.filter_map(|name|document.get(name).map(|value|(name,value))).collect::<BTreeMap<_,_>>();
    (checkpoint_codec::json_length(&selected)? as u64).checked_mul(32).and_then(|n|n.checked_add(1024))
        .ok_or_else(||storage("component retained estimate overflow"))
}

pub(super) fn admit<'a>(
    config: &RecordConfig,
    records: impl Iterator<Item = &'a Record>,
) -> Result<MemoryLease, RecordError> {
    // Retain only component metadata/document capacity. The retired posting
    // builder's 64 MiB scratch buffer no longer exists. Native writer buffers,
    // analyzer tables and reader metadata own separate admission leases; charging
    // the removed scratch buffer for every idle collection exhausts shared memory.
    let mut estimate = config.schema.len() as u64 * 65536 * 32;
    // The mature Chinese dictionary has one process-owned admission, not a
    // per-component allocation. Document and component estimates remain local.
    // Match component::append: this component owns no lexical postings or
    // token buffers. Native analysis/queued documents have their own leases.
    let component_schema = retrieval_schema(config);
    for record in records {
        if let Some(document) = &record.document {
            estimate = estimate
                .checked_add(component_estimate(document, component_schema.iter().filter(|f|f.index_vector||f.facet).map(|f|f.field.as_str()))?)
                .and_then(|n|n.checked_add(identity_estimate(&record.record_id).ok()?))
                .ok_or_else(|| storage("build estimate overflow"))?;
        }
    }
    reserve_memory(estimate).map_err(storage)
}

/// Admit the complete business operation, including its index updates,
/// before WAL append. Native commits cannot split the operation's authority cut.
pub(super) fn write_estimate(
    state: &State,
    config: &RecordConfig,
    applied: &Applied,
) -> Result<(u64, usize), RecordError> {
    write_estimate_for(state.records.get(&applied.request.record_id), config, applied)
}
pub(super) fn write_estimate_for(previous: Option<&Record>, config: &RecordConfig, applied: &Applied) -> Result<(u64, usize), RecordError> {
    let estimate = |doc: &Document, id: &str| {
        document_estimate(doc, &config.schema)?
            .checked_add(65536)
            .and_then(|n|n.checked_add(identity_estimate(id).ok()?))
            .ok_or_else(|| storage("write estimate overflow"))
    };
    match enrichment::document(
            previous, &applied.request,
        )? {
            Some(document) => Ok((estimate(&document,&applied.request.record_id)?,1)),
            None => Ok((0,0)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_temporal_estimate_matches_complete_input_at_numeric_boundaries() {
        let settings=crate::record::tests::config();
        let document:Document=serde_json::from_value(serde_json::json!({"body":"alpha ".repeat(50_000),"vector":[2.0,0.0]})).unwrap();
        let raw=document_estimate(&document,&settings.schema).unwrap();
        for metadata in [TemporalMetadata::default(),TemporalMetadata{
            event_time_ms:Some(i64::MIN),valid_from_ms:Some(i64::MIN),valid_to_ms:Some(i64::MAX),expire_at_ms:Some(u64::MAX),
        }] {
            for ingested in [0,999,1_000,u64::MAX] {
                let indexed=temporal::indexed(&document,&metadata,ingested);
                let exact=document_estimate(&indexed,&settings.schema).unwrap();
                assert!(raw<exact,"the old raw-document reservation undercharged synchronous publication");
                assert_eq!(indexed_document_estimate(&document,&settings.schema,&metadata,ingested).unwrap(),exact);
            }
        }
    }

    #[test]
    fn nonindexed_body_is_transient_while_vectors_and_facets_stay_charged() {
        let small:Document=serde_json::from_value(serde_json::json!({"body":"a","vector":[1.0,2.0],"tag":["one","two"]})).unwrap();
        let mut large=small.clone();large.insert("body".into(),serde_json::json!("x".repeat(256*1024)));
        let fields=["vector","tag"];
        assert_eq!(component_estimate(&small,fields.into_iter()).unwrap(),component_estimate(&large,fields.into_iter()).unwrap());
        assert!(document_estimate(&large,&[]).unwrap()>document_estimate(&small,&[]).unwrap()+8*1024*1024-1024);
        assert!(component_estimate(&large,["vector","tag","body"].into_iter()).unwrap()>component_estimate(&large,fields.into_iter()).unwrap()+8*1024*1024-1024);
        assert_eq!(component_estimate(&small,["tag","tag"].into_iter()).unwrap(),component_estimate(&small,["tag"].into_iter()).unwrap());
    }
}
