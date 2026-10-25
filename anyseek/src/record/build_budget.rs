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
    // Unicode/OpenCC expansion and ZH matrices are overestimated here;
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
    for record in records {
        if let Some(document) = &record.document {
            estimate = estimate
                .checked_add(document_estimate(document, &config.schema)?)
                .ok_or_else(|| storage("build estimate overflow"))?;
        }
    }
    reserve_memory(estimate).map_err(storage)
}

/// Admit the complete business operation, including all generation members,
/// before WAL append. Native commits cannot split the operation's authority cut.
pub(super) fn write_estimate(
    state: &State,
    config: &RecordConfig,
    applied: &Applied,
) -> Result<(u64, usize), RecordError> {
    let estimate = |doc: &Document| {
        document_estimate(doc, &config.schema)?
            .checked_add(65536)
            .ok_or_else(|| storage("write estimate overflow"))
    };
    match &applied.request.generation {
        Some(GenerationCommand::Publish { .. }) => {
            let chunks=&state.generations[&applied.request.record_id]
                .staged.as_ref().ok_or_else(|| storage("missing staged generation"))?.chunks;
            let bytes=chunks.iter().try_fold(0u64, |sum, c| {
                sum.checked_add(estimate(&c.document)?)
                    .ok_or_else(|| storage("generation estimate overflow"))
            })?;
            Ok((bytes,chunks.len()))
        },
        Some(_) => Ok((0,0)),
        None => match enrichment::document(
            state.records.get(&applied.request.record_id), &applied.request,
        )? {
            Some(document) => Ok((estimate(&document)?,1)),
            None => Ok((0,0)),
        },
    }
}
