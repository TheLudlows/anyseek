//! A flat, certificate-selected State closure. Blocks never select authority.
use super::*;
use crate::storage::payload_pool::PoolReader;

pub(super) const MAX_DELTAS: usize = 32;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Block {
    pub owner: String,
    pub delta: state_chunks::Delta,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Image {
    pub base_owner: String,
    pub base: state_chunks::Descriptor,
    pub deltas: Vec<Block>,
    pub digest: [u8; 32],
}

impl Image {
    pub(super) fn config(&self) -> &RecordConfig { self.base.config() }
    pub(super) fn owners(&self) -> Result<Vec<&str>, RecordError> {
        if self.deltas.len() > MAX_DELTAS { return Err(storage("state delta count exceeded")); }
        let mut owners = vec![self.base_owner.as_str()];
        for block in &self.deltas {
            if block.delta.parent() != *owners.last().unwrap() || owners.contains(&block.owner.as_str()) {
                return Err(storage("state delta parent/order mismatch"));
            }
            owners.push(block.owner.as_str());
        }
        Ok(owners)
    }
    pub(super) fn validate_result(&self, state: &State, authority: u64) -> Result<(), RecordError> {
        self.validate_encoded(&checkpoint_codec::StateEncoding::new(state)?, authority)
    }
    pub(super) fn validate_encoded(&self, encoded: &checkpoint_codec::StateEncoding<'_>, authority: u64) -> Result<(), RecordError> {
        let state = encoded.state();
        if encoded.digest() != self.digest {
            return Err(storage("incremental state digest mismatch"));
        }
        if encoded.length() as u64 > authority.min(self.config().max_journal_bytes) {
            return Err(storage("incremental state byte budget exceeded"));
        }
        validate_restored_state(self.config(), state)?;
        if state.operation_lookup.len() != state.operations.len()
            || state.operations.iter().enumerate().any(|(i, op)| state.operation_lookup.get(&op.request.operation_id) != Some(&i))
            || state.records.iter().any(|(id, record)| id != &record.record_id)
        {
            return Err(storage("incremental state identity mismatch"));
        }
        Ok(())
    }
    pub(super) fn read(
        &self, pool: &PoolReader, authority: u64,
        mut directory: impl FnMut(&str) -> Result<PathBuf, RecordError>,
    ) -> Result<State, RecordError> {
        self.owners()?;
        let decoded = self.base.read(&directory(&self.base_owner)?, pool, authority)?;
        let mut state = decoded.state;
        for block in &self.deltas {
            Self::apply_block(&block.delta, &directory(&block.owner)?, pool, authority, self.config(), &mut state)?;
        }
        self.validate_result(&state, authority)?;
        Ok(state)
    }
    pub(super) fn apply_block(
        delta: &state_chunks::Delta, path: &Path, pool: &PoolReader,
        authority: u64, config: &RecordConfig, state: &mut State,
    ) -> Result<(), RecordError> {
        if checkpoint_codec::json_digest(delta.config())? != checkpoint_codec::json_digest(config)? {
            return Err(storage("delta analyzer/config identity mismatch"));
        }
        let mut admitted_bytes = checkpoint_codec::json_length(state)?;
        delta.visit(path, pool, authority, |entry| {
            // Pre-admit metadata before replay. Actual immutable record/history
            // values obtain their own full leases in the existing apply path.
            let growth = checkpoint_codec::json_length(&entry)?.checked_mul(3)
                .ok_or_else(|| storage("delta replay growth overflow"))?;
            admitted_bytes = admitted_bytes.checked_add(growth)
                .ok_or_else(|| storage("delta replay size overflow"))?;
            state.admit_bytes(admitted_bytes)?;
            match entry {
                Entry::Applied(applied) => apply(state, &applied),
                Entry::Expired(event) => temporal::apply(state, &event),
                Entry::FeedTrim(event) => change_feed::apply_trim(state, &config.change_feed, &event),
                Entry::LeaseExpired(event) => lease::apply(state, &event),
                Entry::Anchor(_) | Entry::Checkpoint { .. } => Err(storage("authority in state delta")),
            }
        })
    }
}
