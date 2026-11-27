//! Sealed record rows selected by the authenticated State base descriptor.
//! Operation history remains in the existing state frame stream.
use super::*;
use crate::storage::{
    build_budget::{File, reserve_memory},
    disk_records::{Checkpoint, Limits, Located, Root, event_time, primary, rows},
    payload_pool::{PoolBuilder, PoolReader},
};
use std::collections::BTreeSet;

const SEGMENT: u64 = 0;

pub(super) fn read_error(error: std::io::Error) -> RecordError {
    if error
        .get_ref()
        .is_some_and(|cause| cause.is::<crate::resources::Busy>())
    {
        RecordError::Busy
    } else {
        memory_error(error)
    }
}

fn projection_error(error: std::io::Error) -> RecordError {
    if error
        .get_ref()
        .is_some_and(|cause| cause.is::<event_time::BuildLimit>())
    {
        RecordError::Rejected(error.to_string())
    } else {
        read_error(error)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    primary: primary::Descriptor,
    count: u64,
    generation: [u8; 16],
    committed_cut: u64,
    objects: BTreeSet<String>,
}

/// A selected immutable row directory and the GC pins for its physical files.
pub(super) struct Base {
    pub(super) checkpoint: Arc<Checkpoint>,
    pub(super) event_time: Option<Arc<event_time::Projection>>,
    count: u64,
    _pin: Arc<package_pins::Pin>,
}
impl std::fmt::Debug for Base {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedRows")
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
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
        project: Option<(&str, event_time::BuildLimits)>,
    ) -> Result<Self, RecordError> {
        let generation = identity(&state.lineage)?;
        let mut projection = project
            .map(|(_, limits)| event_time::Builder::new(root, config.max_record_bytes, limits))
            .transpose()
            .map_err(projection_error)?;
        let mut keys = primary::Writer::create(root, config.max_record_bytes).map_err(storage)?;
        let mut rows = rows::SegmentWriter::create(root, SEGMENT).map_err(storage)?;
        let mut objects = BTreeSet::new();
        state.records.scan(None, |id, row| {
            let record = row.load(id)?;
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
            if let Some(projection) = &mut projection
                && record.document.is_some()
                && let Some(at) = record.temporal.event_time_ms
            {
                projection.push(at, id, row).map_err(projection_error)?;
            }
            Ok(true)
        })?;
        let descriptor = Self {
            primary: keys.finish().map_err(storage)?,
            count: rows.finish().map_err(storage)?,
            generation,
            committed_cut: state.seq,
            objects,
        };
        if let Some((projection, (owner, _))) = projection.zip(project) {
            projection.finish(descriptor.event_time_binding(owner)?).map_err(projection_error)?;
        }
        Ok(descriptor)
    }

    pub(super) fn event_time_binding(&self, owner: &str) -> Result<event_time::Binding, RecordError> {
        Ok(event_time::Binding {
            owner: owner.to_owned(),
            lineage: self.generation,
            cut: self.committed_cut,
            rows_sha256: checkpoint_codec::json_digest(self)?,
        })
    }

    pub(super) fn open(
        &self,
        root: &Path,
        lineage: &str,
        committed_cut: u64,
        config: &RecordConfig,
        pool: &PoolReader,
        event_time: Option<Arc<event_time::Projection>>,
    ) -> Result<Base, RecordError> {
        if self.generation != identity(lineage)? || self.committed_cut != committed_cut {
            return Err(storage("disk records differ from their selected State cut"));
        }
        if event_time.as_ref().is_some_and(|projection| projection.cut() != committed_cut) {
            return Err(storage("event-time projection differs from its selected Record cut"));
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
        let mut paths = Vec::with_capacity(self.objects.len() + 1);
        paths.push(root.to_owned());
        paths.extend(
            self.objects
                .iter()
                .map(|id| pool.root().join(format!("payload-{id}"))),
        );
        let pin = package_pins::Pin::acquire(&paths)?;
        let checkpoint = Checkpoint::open(root, pool.root(), selected, limits, File::read_exact_at)
            .map_err(storage)?;
        Ok(Base {
            checkpoint,
            event_time,
            count: self.count,
            _pin: pin,
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
        let base = self.open(root, lineage, committed_cut, config, pool, None)?;
        base.visit(|id, row| decode(id, row, |record| accept(id.to_owned(), record)))
    }
}

impl Base {
    pub(super) fn visit(
        &self,
        mut accept: impl FnMut(&str, Located<'_>) -> Result<(), RecordError>,
    ) -> Result<(), RecordError> {
        let mut seen = 0u64;
        self.checkpoint.visit_sealed(read_error, |id, row| {
            seen = seen
                .checked_add(1)
                .ok_or_else(|| storage("disk record count overflow"))?;
            if seen > self.count {
                return Err(storage("disk record count exceeded"));
            }
            accept(id, row)
        })?;
        if seen != self.count {
            return Err(storage("disk record count mismatch"));
        }
        Ok(())
    }
}

pub(super) fn output(id: &str, row: &Located<'_>, document: Option<Document>) -> Record {
    Record {
        record_id: id.to_owned(),
        version: row.metadata.version,
        document,
        temporal: row.metadata.temporal.clone(),
        ingested_at_ms: row.metadata.ingested_at_ms,
        expired_at_ms: row.metadata.expired_at_ms,
    }
}

pub(super) fn decode<T>(
    id: &str,
    row: Located<'_>,
    accept: impl FnOnce(Record) -> Result<T, RecordError>,
) -> Result<T, RecordError> {
    if !row.metadata.is_live() {
        return accept(output(id, &row, None));
    }
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
            accept(output(id, &row, Some(document)))
        })
        .map_err(read_error)?;
    found.ok_or_else(|| storage("disk record missing its selected payload"))?
}
