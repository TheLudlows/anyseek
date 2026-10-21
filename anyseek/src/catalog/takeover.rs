//! Catalog owns visibility/authorization transitions; Record owns final content.
use super::*;
use crate::record::{SealCertificate, SealRequest};

/// Retained takeover lifecycle, not an expiring operation lookup cache.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TakeoverIntent {
    /// Current source in the same exclusively owned service root.
    pub source_collection_id: String,
    /// Immutable provenance verified before closing source authorization.
    pub lineage: String,
    /// Original client request identity, including its retry epoch.
    pub operation_id: OperationId,
    /// Record seal binding; the target ID is included explicitly.
    pub binding: SealRequest,
    /// Set only by the final atomic catalog transition.
    pub completed: Option<SealCertificate>,
}
pub(super) fn apply(state: &mut State, event: &Applied) -> Result<(), CatalogError> {
    match &event.request.command {
        CatalogCommand::PrepareTakeover { collection_id, source_collection_id, owner_key,
            config, backup_id, lineage, binding } => {
            identity(collection_id)?;
            identity(owner_key)?;
            identity(source_collection_id)?;
            if binding.intent_id.len() != 64 || binding.backup_digest.len() != 64
                || !binding.intent_id.bytes().all(|b| b.is_ascii_hexdigit())
                || !binding.backup_digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(CatalogError::Rejected("invalid binding digest".into()));
            }
            if state.collections.contains_key(collection_id) || collection_id == source_collection_id {
                return Err(CatalogError::Conflict("takeover target identity retained".into()));
            }
            if !state.keys.get(owner_key).is_some_and(|k| k.active) { return Err(CatalogError::Unauthorized); }
            if backup_id.len() != 64 || !backup_id.bytes().all(|b| b.is_ascii_hexdigit())
                || state.retired_backups.contains(backup_id) || binding.target_collection_id != *collection_id
                || lineage.len() != 32 {
                return Err(CatalogError::Rejected("invalid takeover provenance".into()));
            }
            let source = state.collections.get_mut(source_collection_id)
                .ok_or_else(|| CatalogError::Conflict("source absent".into()))?;
            if !source.active || source.deleted || source.takeover_target.is_some()
                || serde_json::to_value(&source.config).map_err(storage)? != serde_json::to_value(config).map_err(storage)? {
                return Err(CatalogError::Conflict("source unavailable or configuration mismatch".into()));
            }
            let directory = event.receipt.directory.clone().ok_or_else(|| storage("missing takeover directory"))?;
            if directory != format!("collection-{:016x}", event.receipt.seq) { return Err(storage("invalid takeover directory")); }
            source.active = false;
            source.takeover_target = Some(collection_id.clone());
            state.grants.remove(source_collection_id);
            state.collections.insert(collection_id.clone(), CollectionEntry {
                collection_id: collection_id.clone(), directory, owner_key: owner_key.clone(),
                config: config.clone(), active: false, initialized: false, deleted: false,
                restore_from: Some(backup_id.clone()), takeover_target: None,
                takeover: Some(TakeoverIntent { source_collection_id: source_collection_id.clone(),
                    lineage: lineage.clone(), operation_id: event.request.operation_id.clone(),
                    binding: binding.clone(), completed: None }),
            });
        }
        CatalogCommand::FinalizeTakeover { collection_id, certificate } => {
            let target = state.collections.get(collection_id)
                .ok_or_else(|| CatalogError::Conflict("takeover absent".into()))?;
            let intent = target.takeover.as_ref().ok_or_else(|| CatalogError::Conflict("not a takeover target".into()))?;
            if certificate.final_state_digest.len() != 64 || !certificate.final_state_digest.bytes().all(|b| b.is_ascii_hexdigit())
                || target.deleted || target.active || intent.completed.is_some()
                || intent.lineage != certificate.lineage || intent.binding != certificate.request {
                return Err(CatalogError::Conflict("takeover certificate mismatch".into()));
            }
            let source_id = intent.source_collection_id.clone();
            let owner = target.owner_key.clone();
            let source = state.collections.get_mut(&source_id).ok_or_else(|| storage("source intent missing"))?;
            if source.active || source.deleted || source.takeover_target.as_ref() != Some(collection_id) {
                return Err(CatalogError::Conflict("source not exclusively reserved".into()));
            }
            source.deleted = true;
            state.grants.remove(&source_id);
            let target = state.collections.get_mut(collection_id).unwrap();
            target.active = true;
            target.initialized = true;
            target.takeover.as_mut().unwrap().completed = Some(certificate.clone());
            if state.keys.get(&owner).is_some_and(|k| k.active) {
                state.grants.entry(collection_id.clone()).or_default().insert(owner, event.receipt.seq);
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

pub(super) fn pending_count(state: &State) -> usize {
    state.collections.values().filter(|c| c.takeover.as_ref().is_some_and(|t| t.completed.is_none())).count()
}
