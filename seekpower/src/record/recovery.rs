//! Persistent owner sealing. This module does not activate restored snapshots.
//! A lineage identifies provenance, not a distributed ownership lease. Callers
//! must own the service root and authorize recovery through its live catalog.
use super::*;
use sha2::{Digest, Sha256};

/// Immutable binding for a future same-service takeover; no arbitrary path.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SealRequest {
    /// SHA-256 identity of the authorized catalog takeover intent.
    pub intent_id: String,
    /// Intended new collection identity, never a filesystem path.
    pub target_collection_id: String,
    /// SHA-256 of the complete immutable backup input.
    pub backup_digest: String,
}
/// Durable final Record waterline and exact takeover binding. A certificate is
/// not a standalone authorization to activate a new owner or trust an archive.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealCertificate {
    /// Original partition lineage, retained through independent snapshots.
    pub lineage: String,
    /// Last committed record operation sequence before the permanent seal.
    pub final_seq: u64,
    /// Hash of the complete pre-seal authority, including operation receipts.
    pub final_state_digest: String,
    /// Only this intent may resume the seal operation.
    pub request: SealRequest,
}
/// Trusted recovery inspection, not a public document-reading bypass.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecoveryIdentity {
    /// Provenance only; does not prove that another copy is not running.
    pub lineage: String,
    /// Current Record operation sequence.
    pub seq: u64,
    /// Complete authority size for bounded trusted backup planning.
    pub checkpoint_bytes: usize,
    /// Digest of configuration and complete authoritative state.
    pub digest: String,
    /// Independent restored evidence cannot act as current deletion authority.
    pub quarantined: bool,
    /// Every independent snapshot is non-authoritative, including empty ones.
    pub independent_snapshot: bool,
    /// Persistent terminal seal, if any.
    pub seal: Option<SealCertificate>,
}
fn digest(config: &RecordConfig, state: &State) -> Result<String, RecordError> {
    let payload = encode_entry(&Entry::Checkpoint { config: config.clone(), state: state.clone() })?;
    Ok(Sha256::digest(payload).iter().map(|b| format!("{b:02x}")).collect())
}
fn content_digest(config: &RecordConfig, state: &State) -> Result<String, RecordError> {
    let mut content = state.clone();
    content.sealed_for = None;
    content.takeover_proof = None;
    digest(config, &content)
}
fn hash_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
impl RecordPartition {
    /// Inspect a healthy owner's identity without clearing any restore/seal gate.
    /// The service must authenticate its trusted management caller separately.
    pub async fn recovery_identity(&self) -> Result<RecoveryIdentity, RecordError> {
        let inner = self.inner.lock().await;
        if !inner.healthy { return Err(RecordError::RecoveryRequired); }
        Ok(RecoveryIdentity {
            lineage: inner.state.lineage.clone(), seq: inner.state.seq, checkpoint_bytes: inner.snapshot_bytes,
            digest: digest(&inner.config, &inner.state)?,
            quarantined: inner.state.evidence_quarantined,
            independent_snapshot: inner.state.independent_snapshot,
            seal: inner.state.sealed_for.clone(),
        })
    }

    /// Permanently stop ordinary reads/writes and output on this owner. Accepted
    /// work is cancellation-safe. Retry the identical request after unknown
    /// outcomes; opening the source never removes a durable seal. This does not
    /// create or activate a successor, and has no unseal escape hatch.
    pub async fn seal_for_takeover(&self, request: SealRequest) -> Result<SealCertificate, RecordError> {
        let admission = self.admission.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let owner = self.inner.clone();
        crate::INDEX_RUNTIME.spawn(async move {
            let _admission = admission;
            let mut inner = owner.lock().await;
            if !inner.healthy { return Err(RecordError::RecoveryRequired); }
            if !hash_id(&request.intent_id) || !hash_id(&request.backup_digest)
                || request.target_collection_id.is_empty() || request.target_collection_id.len() > 256 {
                return Err(RecordError::Rejected("invalid takeover binding".into()));
            }
            if inner.state.independent_snapshot || inner.state.evidence_quarantined {
                return Err(RecordError::Rejected("independent snapshot is not current deletion authority".into()));
            }
            if let Some(seal) = &inner.state.sealed_for {
                return if seal.request == request { Ok(seal.clone()) }
                    else { Err(RecordError::Conflict("partition sealed for a different takeover".into())) };
            }
            let certificate = SealCertificate {
                lineage: inner.state.lineage.clone(), final_seq: inner.state.seq,
                final_state_digest: content_digest(&inner.config, &inner.state)?, request,
            };
            let mut next = inner.state.clone();
            next.sealed_for = Some(certificate.clone());
            let payload = encode_entry(&Entry::Checkpoint { config: inner.config.clone(), state: next.clone() })?;
            PartitionJournal::validate_initial(payload.len(), inner.config.max_journal_bytes)
                .map_err(|e| RecordError::Rejected(e.to_string()))?;
            // No await between closing the output gate and publishing the full
            // authority. A failed publication remains fenced until recovery.
            inner.healthy = false;
            inner.output.write().map_err(|_| RecordError::RecoveryRequired)?.fenced = true;
            inner.journal.checkpoint(&payload).map_err(creation_error)?;
            #[cfg(test)]
            if inner.fail_after_durable { return Err(RecordError::DurablePending(next.seq)); }
            inner.state = next;
            inner.snapshot_bytes = payload.len();
            inner.healthy = true;
            // Output stays fenced permanently, including after future compaction.
            Ok(certificate)
        }).await.map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?
    }
}

impl RecordPartition {
    /// Build a new successor from the complete final sealed authority, preserving
    /// all acknowledged operations. The backup is an exact provenance input, not
    /// a rollback point. The trusted catalog must keep the target inaccessible
    /// until certification and its final activation command have completed.
    pub async fn create_takeover_successor(
        &self, backup: &Self, target: &Path, request: SealRequest,
    ) -> Result<(), RecordError> {
        if Arc::ptr_eq(&self.inner, &backup.inner) {
            return Err(RecordError::Rejected("backup must be an independent snapshot".into()));
        }
        let source = self.inner.clone();
        let backup = backup.inner.clone();
        let target = target.to_owned();
        crate::INDEX_RUNTIME.spawn(async move {
            let original = source.lock().await;
            let snapshot = backup.lock().await;
            if !original.healthy || !snapshot.healthy { return Err(RecordError::RecoveryRequired); }
            let seal = original.state.sealed_for.as_ref()
                .ok_or_else(|| RecordError::Conflict("source not sealed".into()))?;
            if seal.request != request || original.state.independent_snapshot
                || original.state.evidence_quarantined || !snapshot.state.independent_snapshot
                || snapshot.state.lineage != original.state.lineage
                || snapshot.state.seq > seal.final_seq
                || digest(&snapshot.config, &snapshot.state)? != request.backup_digest
                || content_digest(&original.config, &original.state)? != seal.final_state_digest {
                return Err(RecordError::Conflict("untrusted or mismatched takeover authority".into()));
            }
            let mut state = original.state.clone();
            state.sealed_for = None;
            state.takeover_proof = Some(seal.clone());
            let bytes = encode_entry(&Entry::Checkpoint { config: original.config.clone(), state })?;
            PartitionJournal::validate_initial(bytes.len(), original.config.max_journal_bytes)
                .map_err(|e| RecordError::Rejected(e.to_string()))?;
            drop(PartitionJournal::create(&target, original.config.max_journal_bytes, &bytes).map_err(creation_error)?);
            Ok(())
        }).await.map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?
    }

    /// Verify a freshly rebuilt successor before catalog activation. This is
    /// deliberately strict: later user writes make this preparation check fail.
    pub async fn certify_takeover_successor(&self, expected: &SealCertificate) -> Result<(), RecordError> {
        let inner = self.inner.lock().await;
        if !inner.healthy { return Err(RecordError::RecoveryRequired); }
        if inner.state.sealed_for.is_some() || inner.state.independent_snapshot
            || inner.state.evidence_quarantined || inner.state.takeover_proof.as_ref() != Some(expected)
            || inner.state.lineage != expected.lineage || inner.state.seq != expected.final_seq
            || content_digest(&inner.config, &inner.state)? != expected.final_state_digest {
            return Err(RecordError::Conflict("successor is not the certified final state".into()));
        }
        Ok(())
    }
}

impl RecordPartition {
    /// Called under the live catalog's authorization guard before its irreversible
    /// Prepare. That guard excludes service writes until source grants are closed.
    pub async fn validate_takeover_capacity(&self, request: &SealRequest, lineage: &str) -> Result<(), RecordError> {
        let inner = self.inner.lock().await;
        if !inner.healthy { return Err(RecordError::RecoveryRequired); }
        if inner.state.lineage != lineage || inner.state.independent_snapshot || inner.state.evidence_quarantined
            || inner.state.sealed_for.is_some() || !hash_id(&request.intent_id) || !hash_id(&request.backup_digest)
            || request.target_collection_id.is_empty() || request.target_collection_id.len() > 256 {
            return Err(RecordError::Rejected("source is not eligible for takeover".into()));
        }
        let seal = SealCertificate { lineage: lineage.into(), final_seq: inner.state.seq,
            final_state_digest: content_digest(&inner.config, &inner.state)?, request: request.clone() };
        let mut next = inner.state.clone();
        next.sealed_for = Some(seal.clone());
        for target in [false, true] {
            if target { next.sealed_for = None; next.takeover_proof = Some(seal.clone()); }
            let bytes = encode_entry(&Entry::Checkpoint { config: inner.config.clone(), state: next.clone() })?;
            PartitionJournal::validate_initial(bytes.len(), inner.config.max_journal_bytes)
                .map_err(|e| RecordError::Rejected(e.to_string()))?;
        }
        Ok(())
    }
}
