//! Bounded multipart generation protocol. Payload batches are journal decisions,
//! not an independent store. Seal/Publish never repeat received bodies in WAL.
use super::*;
use generation::{Manifest, Staged};
use std::collections::BTreeSet;
const MAX_MEMBERS: usize = 100;
const MAX_BYTES: usize = 4 * 1024 * 1024;
fn reject(s: &str) -> RecordError {
    RecordError::Rejected(s.into())
}
fn conflict(s: &str) -> RecordError {
    RecordError::Conflict(s.into())
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 256
}
pub(super) fn manifest_digest(stage: &Staged) -> Result<String, RecordError> {
    // Membership order is caller-declared; batch arrival order is not semantic.
    let chunks: BTreeMap<_, _> = stage.chunks.iter().map(|c| (&c.record_id, c)).collect();
    lease::digest(
        &serde_json::json!({"source":stage.source,"members":stage.members,"chunks":chunks}),
    )
}
pub(super) fn validate_manifest(stage: &Staged) -> Result<(), RecordError> {
    let declared: BTreeSet<_> = stage.members.iter().collect();
    let received: BTreeSet<_> = stage.chunks.iter().map(|c| &c.record_id).collect();
    if declared.is_empty()
        || declared.len() > MAX_MEMBERS
        || declared.len() != stage.members.len()
        || stage.members.iter().any(|id| !valid_id(id))
        || received.len() != stage.chunks.len()
        || !received.is_subset(&declared)
        || stage.sealed && received != declared
    {
        return Err(reject("invalid upload membership or completeness"));
    }
    if serde_json::to_vec(&stage.chunks).map_err(storage)?.len() > MAX_BYTES {
        return Err(reject("generation total payload exceeds 4 MiB"));
    }
    Ok(())
}
pub(super) fn validate_request(
    state: &State,
    config: &RecordConfig,
    request: &RecordWrite,
    evaluation: u64,
) -> Result<(), RecordError> {
    match request.generation.as_ref().unwrap() {
        GenerationCommand::Begin {
            generation_id,
            source,
            members,
        } => {
            if !valid_id(generation_id)
                || state
                    .generations
                    .get(&request.record_id)
                    .is_some_and(|m| m.staged.is_some() || m.was_used(generation_id))
            {
                return Err(conflict("staging occupied or generation identity spent"));
            }
            if members
                .iter()
                .any(|id| id == &request.record_id || generation::occupied(state, id))
            {
                return Err(conflict("upload member already reserved"));
            }
            let stage = Staged {
                deadline_ms: lease::deadline(evaluation)?,
                scope: state.history.scope.clone(),
                id: generation_id.clone(),
                source: source.clone(),
                members: members.clone(),
                chunks: vec![],
                sealed: false,
            };
            validate_manifest(&stage)?;
            generation::validate_stage(state, config, &stage, request, evaluation)?;
        }
        GenerationCommand::StageBatch { generation_id, .. }
        | GenerationCommand::Seal { generation_id, .. } => {
            let old = state
                .generations
                .get(&request.record_id)
                .ok_or_else(|| conflict("upload absent"))?;
            let stage = old
                .staged
                .as_ref()
                .filter(|s| &s.id == generation_id)
                .ok_or_else(|| conflict("upload identity mismatch"))?;
            if stage.deadline_ms <= evaluation || stage.scope != state.history.scope {
                return Err(conflict("upload requires unexpired renewed owner lease"));
            }
            if stage.sealed {
                return Err(conflict("sealed upload cannot change"));
            }
            let mut candidate = old.clone();
            apply(&mut candidate, request, evaluation, &state.history.scope)?;
            let stage = candidate.staged.as_ref().unwrap();
            validate_manifest(stage)?;
            generation::validate_stage(state, config, stage, request, evaluation)?;
        }
        _ => return Err(reject("not a multipart command")),
    }
    Ok(())
}
pub(super) fn apply(
    manifest: &mut Manifest,
    request: &RecordWrite,
    now: u64,
    scope: &str,
) -> Result<(), RecordError> {
    match request.generation.as_ref().unwrap() {
        GenerationCommand::Begin {
            generation_id,
            source,
            members,
        } => {
            if manifest.staged.is_some() || !manifest.used.insert(generation_id.clone()) {
                return Err(conflict("upload identity reused"));
            }
            manifest.staged = Some(Staged {
                deadline_ms: lease::deadline(now)?,
                scope: scope.into(),
                id: generation_id.clone(),
                source: source.clone(),
                members: members.clone(),
                chunks: vec![],
                sealed: false,
            });
        }
        GenerationCommand::StageBatch {
            generation_id,
            chunks,
        } => {
            let stage = manifest
                .staged
                .as_mut()
                .filter(|s| &s.id == generation_id && !s.sealed)
                .ok_or_else(|| conflict("upload absent or sealed"))?;
            if chunks.is_empty() || chunks.len() > MAX_MEMBERS {
                return Err(reject("empty or oversized upload batch"));
            }
            stage.chunks.extend(chunks.iter().cloned());
            // Canonical publication/record order cannot depend on upload order.
            stage.chunks.sort_by_key(|c| {
                stage
                    .members
                    .iter()
                    .position(|id| id == &c.record_id)
                    .unwrap_or(usize::MAX)
            });
            validate_manifest(stage)?;
            stage.deadline_ms = lease::deadline(now)?;
        }
        GenerationCommand::Seal {
            generation_id,
            manifest_sha256,
        } => {
            let stage = manifest
                .staged
                .as_mut()
                .filter(|s| &s.id == generation_id && !s.sealed)
                .ok_or_else(|| conflict("upload absent or sealed"))?;
            stage.sealed = true;
            validate_manifest(stage)?;
            if manifest_digest(stage)? != *manifest_sha256 {
                return Err(conflict("upload manifest digest mismatch"));
            }
            stage.deadline_ms = lease::deadline(now)?;
        }
        _ => return Err(reject("not a multipart command")),
    }
    Ok(())
}

// Checkpoint fields are not independent authority for sealing or member order.
// Reconstruct the live private generation from its retained control decisions.
pub(super) fn validate_history(
    state: &State,
    id: &str,
    manifest: &Manifest,
) -> Result<(), RecordError> {
    let Some(stage) = &manifest.staged else {
        return Ok(());
    };
    let mut rebuilt: Option<Staged> = None;
    for op in state
        .operations
        .iter()
        .filter(|op| op.request.record_id == id)
    {
        let Some(command) = &op.request.generation else {
            continue;
        };
        let generation_id = match command {
            GenerationCommand::Stage { generation_id, .. }
            | GenerationCommand::Begin { generation_id, .. }
            | GenerationCommand::StageBatch { generation_id, .. }
            | GenerationCommand::Seal { generation_id, .. }
            | GenerationCommand::Publish { generation_id }
            | GenerationCommand::Abort { generation_id }
            | GenerationCommand::Renew { generation_id } => generation_id,
        };
        if generation_id != &stage.id {
            continue;
        }
        if op.request_digest.is_some() {
            return Err(storage("live upload contains redacted decision"));
        }
        match command {
            GenerationCommand::Stage {
                source, members, ..
            }
            | GenerationCommand::Begin {
                source, members, ..
            } => {
                if rebuilt.is_some() {
                    return Err(storage("duplicate upload origin"));
                }
                let (chunks, sealed) = match command {
                    GenerationCommand::Stage { chunks, .. } => (chunks.clone(), true),
                    _ => (vec![], false),
                };
                rebuilt = Some(Staged {
                    deadline_ms: lease::deadline(op.observed_time_ms)?,
                    scope: stage.scope.clone(),
                    id: stage.id.clone(),
                    source: source.clone(),
                    members: members.clone(),
                    chunks,
                    sealed,
                });
            }
            GenerationCommand::StageBatch { .. } | GenerationCommand::Seal { .. } => {
                let mut holder = Manifest {
                    version: 0,
                    used: BTreeSet::new(),
                    active: None,
                    active_members: vec![],
                    staged: rebuilt.take(),
                };
                apply(&mut holder, &op.request, op.observed_time_ms, &stage.scope)
                    .map_err(storage)?;
                rebuilt = holder.staged;
            }
            GenerationCommand::Renew { .. } => {
                let value = rebuilt
                    .as_mut()
                    .ok_or_else(|| storage("renew before upload origin"))?;
                value.deadline_ms = lease::deadline(op.observed_time_ms)?;
            }
            _ => return Err(storage("terminal upload still staged")),
        }
    }
    let rebuilt = rebuilt.ok_or_else(|| storage("upload origin absent"))?;
    if serde_json::to_value(&rebuilt).map_err(storage)?
        != serde_json::to_value(stage).map_err(storage)?
    {
        return Err(storage("staging differs from committed upload decisions"));
    }
    Ok(())
}
