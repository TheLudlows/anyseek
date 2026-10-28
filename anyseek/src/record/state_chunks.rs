//! Bounded metadata frames; a frame is never a publication authority.
//! Payload references are rewritten only at enumerated engine-owned positions.
use super::*;
use crate::storage::{
    build_budget::reserve_memory,
    payload_pool::{PoolBuilder, PoolReader},
};
use serde_json::Value;
use std::io::{Read, Write};

const FRAME: usize = 16 * 1024 * 1024;
const CHUNK: usize = 4 * 1024 * 1024;
const MAX_CHUNKS: usize = 256;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    format: u32,
    config: RecordConfig,
    chunks: Vec<u64>,
    entries: u64,
    expanded: u64,
    digest: [u8; 32],
    #[serde(skip)]
    _build_lease: Option<Arc<crate::storage::build_budget::MemoryLease>>,
}
/// One ordered journal suffix. Its parent identity is part of the enclosing
/// authenticated anchor, not a discoverable or independently selectable head.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Delta {
    parent: String,
    frames: Descriptor,
}
impl Delta {
    pub(super) fn parent(&self) -> &str { &self.parent }
    pub(super) fn config(&self) -> &RecordConfig { self.frames.config() }

    /// Build only from the selected healthy WAL cut. Same-sequence maintenance
    /// entries remain in physical order. The old certificate must match exactly;
    /// successful callbacks are not a license to publish a partial suffix.
    pub(super) fn write(
        root: &Path, config: &RecordConfig, pool: &mut PoolBuilder,
        journal: &mut PartitionJournal, parent: &anchor::Reference, identity: String,
    ) -> Result<Option<Self>, RecordError> {
        Self::write_from(root,config,pool,parent,identity,|accept|journal.visit_committed(accept))
    }
    pub(super) fn write_captured(root:&Path,config:&RecordConfig,pool:&mut PoolBuilder,
        frames:&[Vec<u8>],parent:&anchor::Reference,identity:String)->Result<Option<Self>,RecordError> {
        Self::write_from(root,config,pool,parent,identity,|accept|{
            for frame in frames {
                // The live journal visitor owns this scratch lease while its
                // callback decodes. Captured frames retain only raw bytes.
                let _decode=reserve_memory((frame.len() as u64).checked_mul(32)
                    .ok_or_else(||std::io::Error::other("captured delta decode overflow"))?)?;
                accept(frame)?;
            }Ok(())
        })
    }
    fn write_from(root:&Path,config:&RecordConfig,pool:&mut PoolBuilder,parent:&anchor::Reference,identity:String,
        visit:impl FnOnce(&mut dyn FnMut(&[u8])->std::io::Result<()>)->std::io::Result<()>)->Result<Option<Self>,RecordError> {
        let mut writer = Writer { root, config, pool, file: None, chunks: Vec::new(), entries: 0, expanded: 0,
            metadata:descriptor_admission(config)?, };
        let mut digest = PieceDigest::new();
        let mut first = true;
        let mut failure = None;
        let visited = visit(&mut |bytes| {
            let result = (|| -> Result<(), RecordError> {
                let entry: Entry = serde_json::from_slice(bytes).map_err(storage)?;
                if first {
                    first = false;
                    let Entry::Anchor(selected) = entry else { return Err(invalid("delta requires an anchor certificate")); };
                    if checkpoint_codec::json_digest(&selected)? != checkpoint_codec::json_digest(parent)? {
                        return Err(invalid("delta parent certificate mismatch"));
                    }
                    return Ok(());
                }
                // Wrap Applied in the exact engine-owned payload positions used
                // by full State frames. User JSON remains opaque to the visitor.
                match &entry {
                    Entry::Applied(applied) => {
                        let piece = BTreeMap::from([("operations", [applied])]);
                        digest.push(&piece)?;
                        writer.push(&piece)?;
                    }
                    Entry::Expired(_) | Entry::FeedTrim(_) | Entry::LeaseExpired(_) => {
                        let piece = BTreeMap::from([("event", &entry)]);
                        digest.push(&piece)?;
                        writer.push(&piece)?;
                    }
                    Entry::Anchor(_) | Entry::Checkpoint { .. } => return Err(invalid("authority inside delta suffix")),
                }
                Ok(())
            })();
            if let Err(error) = result {
                failure = Some(error);
                return Err(std::io::Error::other("delta preparation rejected"));
            }
            Ok(())
        });
        if let Some(error) = failure { return Err(error); }
        visited.map_err(storage)?;
        if first { return Err(invalid("missing delta parent certificate")); }
        if writer.entries == 0 { return Ok(None); }
        if let Some(mut file) = writer.file.take() {
            file.flush().map_err(storage)?;
            file.sync_all().map_err(storage)?;
        }
        let (hash, entries, expanded) = digest.finish();
        if entries != writer.entries || expanded != writer.expanded { return Err(invalid("delta preparation count mismatch")); }
        Ok(Some(Self { parent: identity, frames: Descriptor {
            format: 2, config: config.clone(), chunks: writer.chunks,
            entries, expanded, digest: hash, _build_lease:Some(Arc::new(writer.metadata)),
        }}))
    }

    /// Only a private recovery/candidate State may be mutated by accept. Check
    /// the final digest before installing that State or publishing a certificate.
    pub(super) fn visit(
        &self, root: &Path, pool: &PoolReader, authority: u64,
        mut accept: impl FnMut(Entry) -> Result<(), RecordError>,
    ) -> Result<(), RecordError> {
        if self.frames.format != 2 { return Err(invalid("unsupported delta format")); }
        let mut actual = PieceDigest::new();
        self.frames.visit_pieces(root, pool, authority, None, |wire, _, bytes| {
            actual.value(&wire, bytes)?;
            let Value::Object(mut object) = wire else { return Err(invalid("invalid delta entry")); };
            if object.len() != 1 { return Err(invalid("invalid delta entry")); }
            let entry = if let Some(mut operations) = object.remove("operations") {
                let values = operations.as_array_mut().filter(|v| v.len() == 1)
                    .ok_or_else(|| invalid("invalid delta operation count"))?;
                Entry::Applied(serde_json::from_value(values.pop().unwrap()).map_err(storage)?)
            } else {
                let entry: Entry = serde_json::from_value(object.remove("event")
                    .ok_or_else(|| invalid("missing delta event"))?).map_err(storage)?;
                if !matches!(entry, Entry::Expired(_) | Entry::FeedTrim(_) | Entry::LeaseExpired(_)) {
                    return Err(invalid("invalid delta maintenance entry"));
                }
                entry
            };
            accept(entry)?;
            Ok(0)
        })?;
        if actual.finish() != (self.frames.digest, self.frames.entries, self.frames.expanded) {
            return Err(invalid("delta content or order mismatch"));
        }
        Ok(())
    }
}
// The reservation is returned with the state, rather than released at the end
// of decoding. The caller must retain it for that state's complete lifetime.
pub(super) struct Decoded {
    pub state: State,
}

fn filename(root: &Path, slot: usize) -> PathBuf {
    root.join(format!("state-{slot:04x}"))
}
fn expanded_limit(limit: u64) -> Result<u64, RecordError> {
    limit
        .checked_mul(2)
        .and_then(|n| n.checked_add(65536))
        .ok_or_else(|| storage("state expansion limit overflow"))
}

fn invalid(message: &str) -> RecordError {
    storage(message)
}

struct Writer<'a> {
    root: &'a Path,
    config: &'a RecordConfig,
    pool: &'a mut PoolBuilder,
    file: Option<crate::storage::build_budget::File>,
    chunks: Vec<u64>,
    entries: u64,
    expanded: u64,
    metadata: crate::storage::build_budget::MemoryLease,
}
fn descriptor_admission(config:&RecordConfig)->Result<crate::storage::build_budget::MemoryLease,RecordError> {
    let bytes=checkpoint_codec::json_length(config)?.checked_add(MAX_CHUNKS*16+512)
        .and_then(|n|(n as u64).checked_mul(32)).ok_or_else(||invalid("descriptor metadata overflow"))?;
    reserve_memory(bytes).map_err(storage)
}
impl Writer<'_> {
    fn push(&mut self, raw: &impl Serialize) -> Result<(), RecordError> {
        let expanded = checkpoint_codec::json_length(raw)?;
        if expanded > FRAME {
            return Err(invalid("state metadata entry exceeds frame budget"));
        }
        self.expanded = self
            .expanded
            .checked_add(expanded as u64)
            .filter(|n| *n <= expanded_limit(self.config.max_journal_bytes).unwrap_or(0))
            .ok_or_else(|| invalid("state metadata expansion budget exceeded"))?;
        // Admit before building a temporary JSON tree. The complete State is
        // never converted to Value. This lease lasts through the actual write.
        let _temporary = reserve_memory(
            (expanded as u64)
                .checked_mul(32)
                .ok_or_else(|| invalid("state entry estimate overflow"))?,
        )
        .map_err(storage)?;
        let mut wire = serde_json::to_value(raw).map_err(storage)?;
        segment_state::visit(&mut wire, |body| {
            let bytes = serde_json::to_vec(body).map_err(storage)?;
            if bytes.len() > self.config.max_record_bytes {
                return Err(invalid("payload body budget exceeded"));
            }
            *body = serde_json::json!({"$payload": self.pool.intern(&bytes).map_err(storage)?});
            Ok(())
        })?;
        let bytes = serde_json::to_vec(&wire).map_err(storage)?;
        if bytes.len() > FRAME {
            return Err(invalid("referenced state entry exceeds frame budget"));
        }
        let framed = bytes.len() as u64 + 16;
        if self.file.is_none()
            || self
                .chunks
                .last()
                .is_some_and(|n| *n != 0 && *n + framed > CHUNK as u64)
        {
            if let Some(mut file) = self.file.take() {
                file.flush().map_err(storage)?;
                file.sync_all().map_err(storage)?;
            }
            if self.chunks.len() == MAX_CHUNKS {
                return Err(invalid("state chunk count exceeded"));
            }
            self.file = Some(
                crate::storage::build_budget::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(filename(self.root, self.chunks.len()))
                    .map_err(storage)?,
            );
            self.chunks.push(0);
        }
        let file = self.file.as_mut().unwrap();
        file.write_all(&(bytes.len() as u64).to_le_bytes())
            .map_err(storage)?;
        file.write_all(&(expanded as u64).to_le_bytes())
            .map_err(storage)?;
        file.write_all(&bytes).map_err(storage)?;
        *self.chunks.last_mut().unwrap() += framed;
        self.entries += 1;
        Ok(())
    }
}
trait Pieces {
    fn push(&mut self, raw: &impl Serialize) -> Result<(), RecordError>;
}
impl Pieces for Writer<'_> {
    fn push(&mut self, raw: &impl Serialize) -> Result<(), RecordError> { Writer::push(self, raw) }
}
fn visit_state(state: &State, sink: &mut impl Pieces) -> Result<(), RecordError> {
        // An empty-body header retains all scalar identity and authorization state.
        // Large history and staging collections are emitted separately below.
        let header = State {
            lineage: state.lineage.clone(),
            feed: state.feed.clone(),
            sealed_for: state.sealed_for.clone(),
            takeover_proof: state.takeover_proof.clone(),
            independent_snapshot: state.independent_snapshot,
            seq: state.seq,
            epoch_watermark: state.epoch_watermark,
            time_watermark_ms: state.time_watermark_ms,
            retired_operation_prefix: state.retired_operation_prefix.clone(),
            private_cleanup_pending: state.private_cleanup_pending,
            evidence_quarantined: state.evidence_quarantined,
            ..State::default()
        };
        sink.push(&BTreeMap::from([("header", &header)]))?;
        for (id, record) in &state.records {
            sink.push(&BTreeMap::from([(
                "records",
                BTreeMap::from([(id, record)]),
            )]))?;
        }
        for record in &state.source_revisions {
            sink.push(&BTreeMap::from([("source_revisions", [record])]))?;
        }
        for operation in &state.operations {
            sink.push(&BTreeMap::from([("operations", [operation])]))?;
        }
        for (id, manifest) in &state.generations {
            #[derive(Serialize)]
            struct StagedMetadata<'a> {
                deadline_ms: u64,
                scope: &'a str,
                id: &'a str,
                source: &'a EvidenceRef,
                chunks: [(); 0],
                members: &'a [String],
                sealed: bool,
            }
            #[derive(Serialize)]
            struct GenerationMetadata<'a> {
                routing_key: &'a str,
                version: u64,
                used: &'a std::collections::BTreeSet<String>,
                active: &'a Option<String>,
                active_members: &'a [String],
                staged: Option<StagedMetadata<'a>>,
            }
            let staged = manifest.staged.as_ref().map(|s| StagedMetadata {
                deadline_ms: s.deadline_ms,
                scope: &s.scope,
                id: &s.id,
                source: &s.source,
                chunks: [],
                members: &s.members,
                sealed: s.sealed,
            });
            let metadata = GenerationMetadata {
                routing_key: &manifest.routing_key,
                version: manifest.version,
                used: &manifest.used,
                active: &manifest.active,
                active_members: &manifest.active_members,
                staged,
            };
            sink.push(&BTreeMap::from([(
                "generations",
                BTreeMap::from([(id, metadata)]),
            )]))?;
            if let Some(staged) = &manifest.staged {
                #[derive(Serialize)]
                struct Piece<'a> {
                    generation_chunk: &'a str,
                    generations: BTreeMap<
                        &'a str,
                        BTreeMap<&'static str, BTreeMap<&'static str, [&'a GenerationChunk; 1]>>,
                    >,
                }
                for chunk in &staged.chunks {
                    sink.push(&Piece {
                        generation_chunk: id,
                        generations: BTreeMap::from([(
                            id.as_str(),
                            BTreeMap::from([("staged", BTreeMap::from([("chunks", [chunk])]))]),
                        )]),
                    })?;
                }
            }
        }
        for event in &state.expirations {
            sink.push(&BTreeMap::from([("expirations", [event])]))?;
        }
        for event in &state.lease_expirations {
            sink.push(&BTreeMap::from([("lease_expirations", [event])]))?;
        }
    Ok(())
}
struct PieceDigest { hash: sha2::Sha256, entries: u64, expanded: u64 }
impl PieceDigest {
    fn new() -> Self { use sha2::Digest; Self { hash: sha2::Sha256::new(), entries: 0, expanded: 0 } }
    fn value(&mut self, value: &Value, bytes: u64) -> Result<(), RecordError> {
        use sha2::Digest;
        self.hash.update(bytes.to_le_bytes());
        self.hash.update(checkpoint_codec::json_digest(value)?);
        self.entries = self.entries.checked_add(1).ok_or_else(|| invalid("piece count overflow"))?;
        self.expanded = self.expanded.checked_add(bytes).ok_or_else(|| invalid("piece size overflow"))?;
        Ok(())
    }
    fn finish(self) -> ([u8;32],u64,u64) { use sha2::Digest; (self.hash.finalize().into(),self.entries,self.expanded) }
}
impl Pieces for PieceDigest {
    fn push(&mut self, raw: &impl Serialize) -> Result<(), RecordError> {
        let bytes=checkpoint_codec::json_length(raw)? as u64;
        if bytes > FRAME as u64 { return Err(invalid("state metadata entry exceeds frame budget")); }
        let _lease=reserve_memory(bytes.checked_mul(32).ok_or_else(||invalid("state entry estimate overflow"))?).map_err(storage)?;
        self.value(&serde_json::to_value(raw).map_err(storage)?,bytes)
    }
}
impl Descriptor {
    pub(super) fn config(&self) -> &RecordConfig {
        &self.config
    }
    pub(super) fn write(
        root: &Path,
        config: &RecordConfig,
        state: &State,
        pool: &mut PoolBuilder,
    ) -> Result<Self, RecordError> {
        validate_config(config)?;
        Self::write_encoded(root, config, &checkpoint_codec::StateEncoding::new(state)?, pool)
    }
    pub(super) fn write_encoded(root: &Path, config: &RecordConfig,
        encoded: &checkpoint_codec::StateEncoding<'_>, pool: &mut PoolBuilder,
    ) -> Result<Self, RecordError> {
        let state = encoded.state();
        validate_config(config)?;
        if encoded.length() as u64 > config.max_journal_bytes {
            return Err(invalid("complete state byte budget exceeded"));
        }
        let mut writer = Writer {
            root,
            config,
            pool,
            file: None,
            chunks: Vec::new(),
            entries: 0,
            expanded: 0,
            metadata:descriptor_admission(config)?,
        };
        visit_state(state, &mut writer)?;
        if let Some(mut file) = writer.file.take() {
            file.flush().map_err(storage)?;
            file.sync_all().map_err(storage)?;
        }
        Ok(Self {
            format: 1,
            config: config.clone(),
            chunks: writer.chunks,
            entries: writer.entries,
            expanded: writer.expanded,
            digest: encoded.digest(),
            _build_lease:Some(Arc::new(writer.metadata)),
        })
    }
    fn visit_pieces(&self, root:&Path, pool:&PoolReader, authority:u64,
        mut residency:Option<&mut crate::storage::build_budget::MemoryLease>,
        mut accept:impl FnMut(Value,u64,u64)->Result<u64,RecordError>) -> Result<(),RecordError> {
        validate_config(&self.config)?;
        let limit = authority.min(self.config.max_journal_bytes);
        if !matches!(self.format, 1 | 2)
            || self.chunks.is_empty()
            || self.chunks.len() > MAX_CHUNKS
            || self.expanded > expanded_limit(limit)?
        {
            return Err(invalid("invalid chunked state descriptor"));
        }
        let mut entries = 0u64;
        let mut expanded_total = 0u64;
        for (slot, expected) in self.chunks.iter().enumerate() {
            if *expected == 0 || *expected > FRAME as u64 + 16 {
                return Err(invalid("invalid state chunk size"));
            }
            let path = filename(root, slot);
            if std::fs::symlink_metadata(&path)
                .map_err(storage)?
                .file_type()
                .is_symlink()
            {
                return Err(invalid("state chunk symlink rejected"));
            }
            let mut file = std::fs::File::open(path).map_err(storage)?;
            if file.metadata().map_err(storage)?.len() != *expected {
                return Err(invalid("state chunk length mismatch"));
            }
            let mut remaining = *expected;
            while remaining != 0 {
                if remaining < 16 {
                    return Err(invalid("truncated state frame header"));
                }
                let mut header = [0u8; 16];
                file.read_exact(&mut header).map_err(storage)?;
                let bytes = u64::from_le_bytes(header[..8].try_into().unwrap());
                let expanded = u64::from_le_bytes(header[8..].try_into().unwrap());
                if bytes > FRAME as u64 || expanded > FRAME as u64 || bytes > remaining - 16 {
                    return Err(invalid("invalid state frame length"));
                }
                expanded_total = expanded_total
                    .checked_add(expanded)
                    .filter(|n| *n <= self.expanded && *n <= expanded_limit(limit).unwrap_or(0))
                    .ok_or_else(|| invalid("state expansion budget exceeded"))?;
                // Admission precedes buffer allocation and deserialization, and
                // decoded state keeps the reservation after this function returns.
                let charge=expanded.checked_mul(32).ok_or_else(||invalid("state residency overflow"))?;
                let mut frame = reserve_memory(charge).map_err(storage)?;
                let _temporary = reserve_memory(
                    bytes
                        .checked_mul(32)
                        .ok_or_else(|| invalid("state entry estimate overflow"))?,
                )
                .map_err(storage)?;
                let mut buffer = vec![0; bytes as usize];
                file.read_exact(&mut buffer).map_err(storage)?;
                let mut wire: Value = serde_json::from_slice(&buffer).map_err(storage)?;
                let mut body_bytes = 0usize;
                segment_state::visit(&mut wire, |body| {
                    let object = body
                        .as_object()
                        .filter(|o| o.len() == 1)
                        .ok_or_else(|| invalid("invalid state payload reference"))?;
                    let reference = serde_json::from_value(
                        object
                            .get("$payload")
                            .ok_or_else(|| invalid("missing state payload reference"))?
                            .clone(),
                    )
                    .map_err(storage)?;
                    // Keep cumulative expansion within the frame's admitted size
                    // before allocating or parsing each referenced body.
                    let available = (expanded as usize)
                        .checked_sub(body_bytes)
                        .ok_or_else(|| invalid("state body expansion exceeded"))?;
                    let raw = pool
                        .read_limited(&reference, available.min(self.config.max_record_bytes))
                        .map_err(storage)?;
                    body_bytes = body_bytes
                        .checked_add(raw.len())
                        .filter(|n| *n <= expanded as usize)
                        .ok_or_else(|| invalid("state body expansion exceeded"))?;
                    *body = serde_json::from_slice(&raw).map_err(storage)?;
                    Ok(())
                })?;
                if checkpoint_codec::json_length(&wire)? != expanded as usize {
                    return Err(invalid("state frame expansion mismatch"));
                }
                let owned = accept(wire, entries, expanded)?;
                if let Some(metadata)=residency.as_mut() {
                    // Newly decoded immutable values already own their leases.
                    // Retain only this frame's key/container/header overhead;
                    // no complete-history allocation or aggregate single lease.
                    drop(frame.split_off(owned).map_err(storage)?);
                    metadata.merge(frame).map_err(storage)?;
                }
                entries = entries
                    .checked_add(1)
                    .filter(|n| *n <= self.entries)
                    .ok_or_else(|| invalid("state entry count exceeded"))?;
                remaining -= bytes + 16;
            }
        }
        if entries != self.entries || expanded_total != self.expanded { return Err(invalid("state closure mismatch")); }
        Ok(())
    }
    // Verify the complete authenticated wire stream against the guard-pinned
    // State, retaining only one decoded frame rather than a second full history.
    pub(super) fn verify_expected(&self,root:&Path,pool:&PoolReader,authority:u64,state:&State)->Result<(),RecordError> {
        if self.format != 1 { return Err(invalid("unsupported complete state format")); }
        self.verify_encoded(root, pool, authority, &checkpoint_codec::StateEncoding::new(state)?)
    }
    pub(super) fn verify_encoded(&self, root: &Path, pool: &PoolReader, authority: u64,
        encoded: &checkpoint_codec::StateEncoding<'_>,
    ) -> Result<(), RecordError> {
        let state = encoded.state();
        if self.format != 1 { return Err(invalid("unsupported complete state format")); }
        // Recoverability is checked per bounded frame/value, with the complete
        // residency charged to the shared pool by its actual owners.
        if encoded.digest() != self.digest || encoded.length() as u64 > authority.min(self.config.max_journal_bytes) {
            return Err(invalid("expected state closure mismatch"));
        }
        validate_restored_state(&self.config,state)?;
        if state.operation_lookup.len()!=state.operations.len() || state.operations.iter().enumerate().any(|(i,op)|state.operation_lookup.get(&op.request.operation_id)!=Some(&i)) || state.records.iter().any(|(id,r)|id!=&r.record_id) {
            return Err(invalid("expected state identity mismatch"));
        }
        let mut expected=PieceDigest::new(); visit_state(state,&mut expected)?;
        let mut actual=PieceDigest::new();
        self.visit_pieces(root,pool,authority,None,|wire,_,bytes|{actual.value(&wire,bytes)?;Ok(0)})?;
        if actual.finish()!=expected.finish() { return Err(invalid("state piece order or content mismatch")); }
        Ok(())
    }
    pub(super) fn read(
        &self,
        root: &Path,
        pool: &PoolReader,
        authority: u64,
    ) -> Result<Decoded, RecordError> {
        if self.format != 1 { return Err(invalid("unsupported complete state format")); }
        let mut state=None;
        let mut residency=reserve_memory(0).map_err(storage)?;
        self.visit_pieces(root,pool,authority,Some(&mut residency),|wire,ordinal,_| {
            let before=state.as_ref().map(State::value_bytes).transpose()?.unwrap_or(0);
            apply_piece(&mut state,wire,ordinal,&self.config)?;
            let after=state.as_ref().map(State::value_bytes).transpose()?.unwrap_or(0);
            after.checked_sub(before).and_then(|n|(n as u64).checked_mul(32)).ok_or_else(||invalid("decoded value ownership overflow"))
        })?;
        let limit=authority.min(self.config.max_journal_bytes);
        let mut state = state.ok_or_else(|| invalid("missing state header"))?;
        if checkpoint_codec::json_digest(&state)? != self.digest
        {
            return Err(invalid("state closure mismatch"));
        }
        if checkpoint_codec::json_length(&state)? as u64 > limit {
            return Err(invalid("complete state byte budget exceeded"));
        }
        state.attach_residency(residency.bytes(), residency);
        restore_operation_lookup(&mut state)?;
        validate_restored_state(&self.config, &state)?;
        state.admit_bytes(checkpoint_codec::json_length(&state)?)?;
        Ok(Decoded { state })
    }
}
fn apply_piece(
    state: &mut Option<State>,
    mut wire: Value,
    ordinal: u64,
    config: &RecordConfig,
) -> Result<(), RecordError> {
    let object = wire
        .as_object_mut()
        .ok_or_else(|| invalid("invalid state piece"))?;
    if ordinal == 0 {
        if object.len() != 1 {
            return Err(invalid("invalid state header"));
        }
        let header: State = serde_json::from_value(
            object
                .remove("header")
                .ok_or_else(|| invalid("missing state header"))?,
        )
        .map_err(storage)?;
        if !header.records.is_empty()
            || !header.source_revisions.is_empty()
            || !header.operations.is_empty()
            || !header.generations.is_empty()
            || !header.expirations.is_empty()
            || !header.lease_expirations.is_empty()
        {
            return Err(invalid("nonempty state header collections"));
        }
        *state = Some(header);
        return Ok(());
    }
    let state = state
        .as_mut()
        .ok_or_else(|| invalid("missing state header"))?;
    if let Some(id) = object.remove("generation_chunk") {
        if object.len() != 1 {
            return Err(invalid("invalid generation chunk piece"));
        }
        let id = id
            .as_str()
            .ok_or_else(|| invalid("invalid generation identity"))?;
        let generations = object
            .remove("generations")
            .ok_or_else(|| invalid("missing generation chunk"))?;
        let mut groups: BTreeMap<String, Value> =
            serde_json::from_value(generations).map_err(storage)?;
        if groups.len() != 1 {
            return Err(invalid("invalid generation chunk group"));
        }
        let mut group = groups
            .remove(id)
            .ok_or_else(|| invalid("generation chunk identity mismatch"))?;
        let chunks = group
            .get_mut("staged")
            .and_then(|s| s.get_mut("chunks"))
            .ok_or_else(|| invalid("missing staged chunk"))?
            .take();
        let mut chunks: Vec<GenerationChunk> = serde_json::from_value(chunks).map_err(storage)?;
        if chunks.len() != 1 {
            return Err(invalid("invalid generation chunk count"));
        }
        state
            .generations
            .get_mut(id)
            .and_then(|g| g.staged.as_mut())
            .ok_or_else(|| invalid("unknown staged generation"))?
            .chunks
            .push(chunks.remove(0));
        return Ok(());
    }
    if object.len() != 1 {
        return Err(invalid("invalid state piece fields"));
    }
    let (kind, value) = object.iter_mut().next().unwrap();
    let exceeded = match kind.as_str() {
        "records" => state.records.len() >= config.max_records,
        "source_revisions" => state.source_revisions.len() >= config.max_records,
        "operations" => state.operations.len() >= config.max_operations,
        "generations" => state.generations.len() >= config.max_records,
        "expirations" => state.expirations.len() >= config.max_operations,
        "lease_expirations" => state.lease_expirations.len() >= config.max_operations,
        _ => false,
    };
    if exceeded {
        return Err(invalid("state entry count exceeds configured capacity"));
    }
    macro_rules! one {
        ($ty:ty) => {{
            let mut values: Vec<$ty> = serde_json::from_value(value.take()).map_err(storage)?;
            if values.len() != 1 {
                return Err(invalid("invalid state piece cardinality"));
            }
            values.remove(0)
        }};
    }
    match kind.as_str() {
        "records" => {
            let records: BTreeMap<String, Record> =
                serde_json::from_value(value.take()).map_err(storage)?;
            if records.len() != 1 {
                return Err(invalid("invalid record piece cardinality"));
            }
            for (id, record) in records {
                if id != record.record_id || state.records.insert(id, record)?.is_some() {
                    return Err(invalid("duplicate or mismatched state record"));
                }
            }
        }
        "source_revisions" => state.source_revisions.push(one!(Record))?,
        "operations" => state.operations.push(one!(Applied))?,
        "generations" => {
            let values: BTreeMap<String, generation::Manifest> =
                serde_json::from_value(value.take()).map_err(storage)?;
            if values.len() != 1 {
                return Err(invalid("invalid generation piece cardinality"));
            }
            for (id, generation) in values {
                if generation
                    .staged
                    .as_ref()
                    .is_some_and(|s| !s.chunks.is_empty())
                    || state.generations.insert(id, generation).is_some()
                {
                    return Err(invalid("duplicate or nonempty generation metadata"));
                }
            }
        }
        "expirations" => state.expirations.push(one!(temporal::Expiration)),
        "lease_expirations" => state.lease_expirations.push(one!(lease::Expiration)),
        _ => return Err(invalid("unknown state piece")),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_value_recovery_exceeds_single_total_and_late_failure_releases_all() {
        const FLAG:&str="ANYSEEK_BOUNDED_VALUE_RECOVERY_CHILD";
        if std::env::var_os(FLAG).is_none() {
            let output=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","record::state_chunks::tests::bounded_value_recovery_exceeds_single_total_and_late_failure_releases_all","--nocapture"])
                .env(FLAG,"1").env("ANYSEEK_BUILD_MEMORY_BYTES",(64u64*1024*1024).to_string())
                .env("ANYSEEK_BUILD_SHARED_MEMORY_BYTES",(512u64*1024*1024).to_string()).output().unwrap();
            assert!(output.status.success(),"{}\n{}",String::from_utf8_lossy(&output.stdout),String::from_utf8_lossy(&output.stderr));return;
        }
        let root=tempfile::tempdir().unwrap();let mut config=super::super::tests::config();
        config.max_records=256;config.max_operations=256;config.max_record_bytes=16*1024;config.max_journal_bytes=16*1024*1024;
        let mut state=State{lineage:"0".repeat(32),feed:change_feed::FeedState{scope:"1".repeat(32),floor:0},..State::default()};
        state.admit_bytes(checkpoint_codec::json_length(&state).unwrap()).unwrap();
        let mut snapshot=checkpoint_codec::length(&config,&state).unwrap();
        for i in 0..128 {
            let request=super::super::tests::write(&format!("op-{i}"),&format!("id-{i}"),&"x".repeat(8192),ExpectedVersion::Absent);
            let receipt=WriteReceipt{staged_only:false,seq:i+1,version:1,record_id:request.record_id.clone()};
            let applied=Applied{request_digest:None,graph_invalidation:false,observed_epoch:request.operation_id.epoch,observed_time_ms:0,request,receipt};
            let prepared=state_memory::Prepared::new(&state,&applied).unwrap();
            snapshot=mutation::snapshot_after(&state,snapshot,&applied,&prepared.record).unwrap();
            prepared.admit_containers(&mut state,snapshot).unwrap();apply_prepared(&mut state,&applied,Some(prepared)).unwrap();
        }
        assert!(checkpoint_codec::json_length(&state).unwrap()*32>64*1024*1024);
        let captured=state.try_clone().unwrap();
        let directory=root.path().join("chunks");let (mut descriptor,pool)=prepare(&directory,&config,&state);
        assert!(descriptor.entries>256);descriptor.verify_expected(&directory,&pool,config.max_journal_bytes,&captured).unwrap();
        let before=crate::storage::build_budget::used_memory_for_test();
        descriptor.digest[0]^=1;
        assert!(descriptor.read(&directory,&pool,config.max_journal_bytes).is_err());
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before,"late failure releases all decoded values and metadata");
        let directory=root.path().join("valid");let (descriptor,pool)=prepare(&directory,&config,&state);
        let restored=descriptor.read(&directory,&pool,config.max_journal_bytes).unwrap();
        assert_eq!(restored.state.operations.len(),128);assert_eq!(restored.state.operation_lookup.len(),128);
        assert_eq!(checkpoint_codec::json_digest(&restored.state).unwrap(),checkpoint_codec::json_digest(&captured).unwrap());
    }

    use crate::storage::payload_pool::{Budget, Closure};

    async fn fixture(root: &Path) -> (RecordConfig, State) {
        let config = super::super::tests::config();
        let partition = RecordPartition::create(&root.join("partition"), config.clone())
            .await
            .unwrap();
        let mut first = super::super::tests::source_write("source", "s", &[]);
        first.document.as_mut().unwrap().insert(
            "opaque".into(),
            serde_json::json!({"$payload":123,"document":"unchanged", "float":1.23456789}),
        );
        partition.write(first).await.unwrap();
        partition
            .write(super::super::tests::generation_write(
                "stage",
                ExpectedVersion::Absent,
                super::super::tests::stage_generation("g", &["a", "b"]),
            ))
            .await
            .unwrap();
        let mut revision = super::super::tests::source_write("revision", "s", &[]);
        revision.expected = ExpectedVersion::Match(1);
        revision.document.as_mut().unwrap().insert(
            "body".into(),
            serde_json::json!("繁體 unicode \\\" revised"),
        );
        partition.write(revision).await.unwrap();
        let state = partition.inner.lock().await.state.try_clone().unwrap();
        (config, state)
    }
    fn prepare(root: &Path, config: &RecordConfig, state: &State) -> (Descriptor, PoolReader) {
        std::fs::create_dir(root).unwrap();
        let budget = Budget {
            bytes: config.max_journal_bytes,
            objects: 64,
            payloads: 10000,
            body_bytes: config.max_record_bytes,
        };
        let mut pool = PoolBuilder::begin(
            root,
            Closure::default(),
            budget,
            format!("{:032x}", rand::random::<u128>()),
        )
        .unwrap();
        let descriptor = Descriptor::write(root, config, state, &mut pool).unwrap();
        let closure = pool.finish().unwrap();
        let reader = PoolReader::open(root, closure, budget).unwrap();
        (descriptor, reader)
    }
    #[tokio::test]
    async fn chunks_preserve_full_history_staging_opaque_json_and_owned_residency() {
        let root = tempfile::tempdir().unwrap();
        let (config, state) = fixture(root.path()).await;
        let directory = root.path().join("chunks");
        let (descriptor, pool) = prepare(&directory, &config, &state);
        assert!(descriptor.entries > state.records.len() as u64 + state.operations.len() as u64);
        // The persisted metadata must not retain either original or staged bodies.
        for slot in 0..descriptor.chunks.len() {
            let bytes = std::fs::read(filename(&directory, slot)).unwrap();
            assert!(!bytes.windows(b"unchanged".len()).any(|w| w == b"unchanged"));
        }
        let owner=Arc::as_ptr(state.residency.as_ref().unwrap());
        descriptor.verify_expected(&directory,&pool,config.max_journal_bytes,&state).unwrap();
        assert_eq!(owner,Arc::as_ptr(state.residency.as_ref().unwrap()));
        let decoded = descriptor
            .read(&directory, &pool, config.max_journal_bytes)
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&decoded.state).unwrap(),
            serde_json::to_vec(&state).unwrap()
        );
        assert_eq!(decoded.state.operation_lookup.len(), state.operations.len());
        let owner = Arc::downgrade(decoded.state.residency.as_ref().unwrap());
        let copy = decoded.state.try_clone().unwrap();
        assert!(!Arc::ptr_eq(
            decoded.state.residency.as_ref().unwrap(),
            copy.residency.as_ref().unwrap()
        ));
        drop(decoded);
        assert!(owner.upgrade().is_none());
        assert_eq!(
            copy.generations["doc"]
                .staged
                .as_ref()
                .unwrap()
                .chunks
                .len(),
            2
        );
        assert_eq!(copy.source_revisions.len(), 1);
    }
    #[tokio::test]
    async fn chunks_reject_truncation_forged_expansion_and_frame_length_before_decode() {
        let root = tempfile::tempdir().unwrap();
        let (config, state) = fixture(root.path()).await;
        for scenario in ["truncated", "expansion", "length", "count", "digest"] {
            let directory = root.path().join(scenario);
            let (mut descriptor, pool) = prepare(&directory, &config, &state);
            let file = filename(&directory, 0);
            let mut bytes = std::fs::read(&file).unwrap();
            match scenario {
                "truncated" => {
                    bytes.pop();
                }
                "expansion" => bytes[8..16].copy_from_slice(&1u64.to_le_bytes()),
                "length" => bytes[..8].copy_from_slice(&u64::MAX.to_le_bytes()),
                "count" => descriptor.entries += 1,
                "digest" => descriptor.digest[0] ^= 1,
                _ => unreachable!(),
            }
            std::fs::write(file, bytes).unwrap();
            assert!(descriptor.verify_expected(&directory,&pool,config.max_journal_bytes,&state).is_err(), "stream {scenario}");
            assert!(
                descriptor
                    .read(&directory, &pool, config.max_journal_bytes)
                    .is_err(),
                "{scenario}"
            );
        }
    }
    #[tokio::test]
    async fn verified_stream_rejects_reordering_duplicate_identity_and_changed_expected_state() {
        let root=tempfile::tempdir().unwrap();
        let (config,state)=fixture(root.path()).await;
        for scenario in ["order","duplicate","expected","lookup","recovery_budget"] {
            let directory=root.path().join(scenario);
            let (mut descriptor,pool)=prepare(&directory,&config,&state);
            let mut expected=state.try_clone().unwrap();
            if scenario=="expected" { expected.time_watermark_ms+=1; }
            else if scenario=="lookup" { expected.operation_lookup.clear(); }
            else if scenario=="recovery_budget" { descriptor.expanded=8*1024*1024*1024; }
            else {
                let path=filename(&directory,0);let bytes=std::fs::read(&path).unwrap();
                let mut frames=Vec::new();let mut offset=0;
                while offset<bytes.len() {
                    let len=u64::from_le_bytes(bytes[offset..offset+8].try_into().unwrap()) as usize+16;
                    frames.push(bytes[offset..offset+len].to_vec());offset+=len;
                }
                assert!(frames.len()>3);
                if scenario=="order" {frames.swap(1,2);} else {frames[2]=frames[1].clone();}
                let changed=frames.concat();descriptor.chunks[0]=changed.len() as u64;
                // Match descriptor sizes so rejection must inspect content/order.
                descriptor.expanded=frames.iter().map(|f|u64::from_le_bytes(f[8..16].try_into().unwrap())).sum();
                std::fs::write(path,changed).unwrap();
            }
            assert!(descriptor.verify_expected(&directory,&pool,config.max_journal_bytes,&expected).is_err(),"{scenario}");
            assert_eq!(state.operations.len(),3);
        }
    }

    #[test]
    fn writer_crosses_real_chunk_boundary_and_bounds_one_large_frame() {
        let root = tempfile::tempdir().unwrap();
        let mut config = super::super::tests::config();
        config.max_journal_bytes = 64 * 1024 * 1024;
        let budget = Budget {
            bytes: config.max_journal_bytes,
            objects: 64,
            payloads: 100,
            body_bytes: config.max_record_bytes,
        };
        let mut pool = PoolBuilder::begin(
            root.path(),
            Closure::default(),
            budget,
            format!("{:032x}", rand::random::<u128>()),
        )
        .unwrap();
        let mut writer = Writer {
            root: root.path(),
            config: &config,
            pool: &mut pool,
            file: None,
            chunks: Vec::new(),
            entries: 0,
            expanded: 0,
            metadata:descriptor_admission(&config).unwrap(),
        };
        let data = "x".repeat(CHUNK / 2);
        writer.push(&BTreeMap::from([("padding", &data)])).unwrap();
        writer.push(&BTreeMap::from([("padding", &data)])).unwrap();
        assert_eq!(
            writer.chunks.len(),
            2,
            "framing overhead crosses the real 4MiB boundary"
        );
        let large = "y".repeat(CHUNK + 1024);
        writer.push(&BTreeMap::from([("padding", &large)])).unwrap();
        assert_eq!(writer.chunks.len(), 3);
        assert!(writer.chunks[2] > CHUNK as u64 && writer.chunks[2] <= FRAME as u64 + 16);
        let previous = writer.chunks.clone();
        assert!(writer.push(&"z".repeat(FRAME)).is_err());
        assert_eq!(
            writer.chunks, previous,
            "oversize rejection must not write a partial frame"
        );
    }

    #[test]
    fn actual_state_budget_rejection_preserves_existing_owner_and_contents() {
        let mut state = State {
            seq: 17,
            ..State::default()
        };
        state.admit_bytes(1024).unwrap();
        let identity = Arc::as_ptr(state.residency.as_ref().unwrap());
        // Configuration validation caps the shared pool at8GiB; this request
        // must reach the real reservation rejection without allocating that size.
        let error = state
            .admit_bytes(256 * 1024 * 1024 + 1)
            .unwrap_err()
            .to_string();
        assert!(error.contains("class=single_growth"), "{error}");
        assert_eq!(state.seq, 17);
        assert_eq!(identity, Arc::as_ptr(state.residency.as_ref().unwrap()));
        assert!(state
            .try_clone_with_growth(256 * 1024 * 1024 + 1)
            .unwrap_err()
            .to_string()
            .contains("class=single"));
        let owner = Arc::downgrade(state.residency.as_ref().unwrap());
        drop(state);
        assert!(owner.upgrade().is_none());
    }

    #[test]
    fn state_copy_growth_overflow_rejects_without_mutating_original() {
        let state = State::default();
        assert!(state.try_clone_with_growth(usize::MAX).is_err());
        assert!(state.residency.is_none());
        assert_eq!(state.seq, 0);
    }
}
