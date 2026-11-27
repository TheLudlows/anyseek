//! Sealed record rows selected by the authenticated State base descriptor.
//! Operation history remains in the existing state frame stream.
use super::*;
use crate::storage::{
    build_budget::{File, reserve_memory},
    disk_records::{Checkpoint, Limits, Located, Root, primary, rows},
    payload_pool::{PoolBuilder, PoolReader},
};
use std::collections::BTreeSet;

const SEGMENT: u64 = 0;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    primary: primary::Descriptor,
    count: u64,
    generation: [u8; 16],
    committed_cut: u64,
    objects: BTreeSet<String>,
}

fn identity(lineage: &str) -> Result<[u8; 16], RecordError> {
    if lineage.len() != 32 {
        return Err(storage("invalid disk record lineage"));
    }
    Ok(u128::from_str_radix(lineage, 16)
        .map_err(storage)?
        .to_be_bytes())
}

fn fingerprint(
    document: &Document,
    temporal: &TemporalMetadata,
    ingested_at_ms: u64,
) -> Result<[u8; 32], RecordError> {
    crate::active::fingerprint(&temporal::indexed(document, temporal, ingested_at_ms))
        .map_err(storage)
}

impl Descriptor {
    pub(super) fn write(
        root: &Path,
        state: &State,
        config: &RecordConfig,
        pool: &mut PoolBuilder,
    ) -> Result<Self, RecordError> {
        let generation = identity(&state.lineage)?;
        let mut keys = primary::Writer::create(root, config.max_record_bytes).map_err(storage)?;
        let mut rows = rows::SegmentWriter::create(root, SEGMENT).map_err(storage)?;
        let mut objects = BTreeSet::new();
        for (id, record) in &state.records {
            let (payload, fingerprint) = if let Some(document) = &record.document {
                let bytes = checkpoint_codec::json_length(document)?;
                let estimate = (bytes as u64).checked_mul(32).ok_or(RecordError::Busy)?;
                let _temporary = reserve_memory(estimate).map_err(memory_error)?;
                let raw = snapshot_json::to_vec(document).map_err(storage)?;
                let reference = pool.intern(&raw).map_err(storage)?;
                let object = identity(&reference.object)?;
                let range = pool.payload_ref(&reference).map_err(storage)?;
                objects.insert(reference.object);
                (
                    Some(rows::PayloadAddress { object, range }),
                    Some(fingerprint(
                        document,
                        &record.temporal,
                        record.ingested_at_ms,
                    )?),
                )
            } else {
                (None, None)
            };
            let metadata = rows::Metadata {
                version: record.version,
                ingested_at_ms: record.ingested_at_ms,
                temporal: record.temporal.clone(),
                expired_at_ms: record.expired_at_ms,
                fingerprint,
            };
            let row = rows
                .push(id, &rows::Row { metadata, payload })
                .map_err(storage)?;
            keys.push(id, row).map_err(storage)?;
        }
        Ok(Self {
            primary: keys.finish().map_err(storage)?,
            count: rows.finish().map_err(storage)?,
            generation,
            committed_cut: state.seq,
            objects,
        })
    }

    pub(super) fn visit(
        &self,
        root: &Path,
        lineage: &str,
        committed_cut: u64,
        config: &RecordConfig,
        pool: &PoolReader,
        mut accept: impl FnMut(String, Record) -> Result<(), RecordError>,
    ) -> Result<(), RecordError> {
        if self.generation != identity(lineage)? || self.committed_cut != committed_cut {
            return Err(storage("disk records differ from their selected State cut"));
        }
        let payloads = self
            .objects
            .iter()
            .map(|id| Ok((id.clone(), pool.extent(id).map_err(storage)?)))
            .collect::<Result<BTreeMap<_, _>, RecordError>>()?;
        let selected = Root {
            generation: self.generation,
            committed_cut: self.committed_cut,
            primary: self.primary.clone(),
            segments: BTreeMap::from([(SEGMENT, self.count)]),
            payloads,
        };
        let limits = Limits {
            page_cache_bytes: 64 * 1024,
            max_record_bytes: config.max_record_bytes,
            max_files: self.objects.len().checked_add(3).ok_or(RecordError::Busy)?,
        };
        let checkpoint = Checkpoint::open(root, pool.root(), selected, limits, File::read_exact_at)
            .map_err(storage)?;
        let mut seen = 0u64;
        checkpoint.visit_sealed(storage, |id, row| {
            seen = seen
                .checked_add(1)
                .ok_or_else(|| storage("disk record count overflow"))?;
            if seen > self.count {
                return Err(storage("disk record count exceeded"));
            }
            self.decode(id, row, &mut accept)
        })?;
        if seen != self.count {
            return Err(storage("disk record count mismatch"));
        }
        Ok(())
    }

    fn decode(
        &self,
        id: &str,
        row: Located<'_>,
        accept: &mut impl FnMut(String, Record) -> Result<(), RecordError>,
    ) -> Result<(), RecordError> {
        let output = |document| Record {
            record_id: id.to_owned(),
            version: row.metadata.version,
            document,
            temporal: row.metadata.temporal.clone(),
            ingested_at_ms: row.metadata.ingested_at_ms,
            expired_at_ms: row.metadata.expired_at_ms,
        };
        let found = row
            .with_canonical_body(|bytes| {
                let estimate = (bytes.len() as u64)
                    .checked_mul(32)
                    .ok_or(RecordError::Busy)?;
                let _decode = reserve_memory(estimate).map_err(memory_error)?;
                let document = snapshot_json::from_slice(bytes).map_err(storage)?;
                let current = fingerprint(
                    &document,
                    &row.metadata.temporal,
                    row.metadata.ingested_at_ms,
                )?;
                if row.metadata.fingerprint != Some(current) {
                    return Err(storage("disk record retrieval fingerprint mismatch"));
                }
                accept(id.to_owned(), output(Some(document)))
            })
            .map_err(storage)?;
        match found {
            Some(result) => result,
            None => accept(id.to_owned(), output(None)),
        }
    }
}
