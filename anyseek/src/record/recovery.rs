//! Trusted recovery inspection, not a public document-reading bypass.
//! A lineage identifies provenance, not a distributed ownership lease. Callers
//! must own the service root and authorize recovery through its live catalog.
use super::*;

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
}
fn digest(config: &RecordConfig, state: &State) -> Result<String, RecordError> {
    Ok(checkpoint_codec::digest(config, state)?.iter().map(|b| format!("{b:02x}")).collect())
}
impl RecordPartition {
    /// Inspect a healthy owner's identity without clearing any restore gate.
    /// The service must authenticate its trusted management caller separately.
    pub async fn recovery_identity(&self) -> Result<RecoveryIdentity, RecordError> {
        let inner = self.inner.lock().await;
        if !inner.healthy { return Err(RecordError::RecoveryRequired); }
        Ok(RecoveryIdentity {
            lineage: inner.state.lineage.clone(), seq: inner.state.seq, checkpoint_bytes: inner.snapshot_bytes,
            digest: digest(&inner.config, &inner.state)?,
            quarantined: inner.state.evidence_quarantined,
            independent_snapshot: inner.state.independent_snapshot,
        })
    }
}
