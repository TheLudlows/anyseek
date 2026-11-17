//! Catalog commands and receipts share the native certified WAL. Checkpoints
//! contain control state only; Record bodies and search indexes live elsewhere.
use super::*;
use crate::storage::journal::CreateError;
use std::path::PathBuf;

const FORMAT: u32 = 6;
// One bounded checkpoint plus a bounded command tail. The native frame limit
// also applies to each checkpoint/command independently.
const JOURNAL_BUDGET: u64 = BUDGET * 2;

#[derive(Serialize, Deserialize)]
enum Entry {
    Checkpoint { format: u32, state: State },
    Applied(Applied),
}

pub(super) struct Store {
    journal: PartitionJournal,
    seq: u64,
}

fn checkpoint_bytes(state: &State) -> Result<Vec<u8>, CatalogError> {
    for tenant in &state.tenants {
        validate_name(tenant)?;
    }
    for key in state.keys.values() {
        if key
            .tenant_id
            .as_ref()
            .is_some_and(|tenant| !state.tenants.contains(tenant))
        {
            return Err(storage("catalog key tenant missing"));
        }
    }
    let mut current = std::collections::BTreeSet::new();
    for (id, entry) in &state.collections {
        let name = scope(&entry.tenant_id, &entry.collection_id)?;
        if entry.scope() != *id
            || entry.generation == 0
            || entry.generation > state.seq
            || !state.tenants.contains(&entry.tenant_id)
            || (!entry.deleted && !current.insert(name))
        {
            return Err(storage("catalog collection identity mismatch"));
        }
    }
    let bytes = serde_json::to_vec(&Entry::Checkpoint {
        format: FORMAT,
        state: state.clone(),
    })
    .map_err(storage)?;
    // Validate the actual recovery representation, including serde's depth
    // limit, before accepting metadata which could not subsequently reopen.
    let _: Entry = serde_json::from_slice(&bytes)
        .map_err(|e| CatalogError::Rejected(format!("unrecoverable catalog metadata: {e}")))?;
    if bytes.len() as u64 > BUDGET {
        return Err(CatalogError::Rejected(
            "catalog metadata budget exceeded".into(),
        ));
    }
    PartitionJournal::validate_initial(bytes.len(), JOURNAL_BUDGET)
        .map_err(|e| CatalogError::Rejected(e.to_string()))?;
    Ok(bytes)
}

pub(super) fn validate_state(state: &State) -> Result<(), CatalogError> {
    checkpoint_bytes(state).map(|_| ())
}

fn publication(error: CreateError) -> CatalogError {
    match error {
        CreateError::NotPublished(error) => storage(error),
        CreateError::PublicationUnknown(error) => CatalogError::OutcomeUnknown(error.to_string()),
    }
}

impl Store {
    fn create(root: &Path, state: &State) -> Result<Self, CatalogError> {
        let bytes = checkpoint_bytes(state)?;
        let journal =
            PartitionJournal::create(root, JOURNAL_BUDGET, &bytes).map_err(publication)?;
        Ok(Self {
            journal,
            seq: state.seq,
        })
    }

    pub(super) fn open(root: &Path) -> Result<(Self, State), CatalogError> {
        let (journal, entries) = PartitionJournal::open(root, JOURNAL_BUDGET).map_err(storage)?;
        let mut entries = entries.into_iter();
        let first = entries
            .next()
            .ok_or_else(|| storage("missing catalog checkpoint"))?;
        let mut state = match serde_json::from_slice(&first).map_err(storage)? {
            Entry::Checkpoint {
                format: FORMAT,
                state,
            } => state,
            _ => return Err(storage("unsupported catalog journal format")),
        };
        for bytes in entries {
            let Entry::Applied(event) = serde_json::from_slice(&bytes).map_err(storage)? else {
                return Err(storage("unexpected catalog checkpoint in command tail"));
            };
            apply(&mut state, &event).map_err(storage)?;
        }
        validate_state(&state).map_err(storage)?;
        Ok((
            Self {
                journal,
                seq: state.seq,
            },
            state,
        ))
    }

    pub(super) fn append(&mut self, old: &State, event: &Applied) -> Result<(), CatalogError> {
        if self.seq != old.seq || self.seq.checked_add(1) != Some(event.receipt.seq) {
            return Err(CatalogError::RecoveryRequired);
        }
        let bytes = serde_json::to_vec(&Entry::Applied(event.clone())).map_err(storage)?;
        // Space pressure checkpoints the preceding committed state. A failed
        // command cannot become visible merely because its checkpoint fitted.
        if self.journal.checkpoint_can_make_room(bytes.len(), 0) {
            self.checkpoint(old)?;
        }
        self.journal
            .validate_append_with_reserve(bytes.len(), 0)
            .map_err(|e| CatalogError::Rejected(e.to_string()))?;
        #[cfg(test)]
        crash("before_commit");
        self.journal
            .append(&bytes)
            .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?;
        #[cfg(test)]
        crash("after_commit");
        self.seq = event.receipt.seq;
        Ok(())
    }

    pub(super) fn checkpoint(&mut self, state: &State) -> Result<(), CatalogError> {
        if self.seq != state.seq {
            return Err(CatalogError::RecoveryRequired);
        }
        let bytes = checkpoint_bytes(state)?;
        self.journal.checkpoint(&bytes).map_err(publication)
    }
}

fn pending(root: &Path) -> Result<PathBuf, CatalogError> {
    let name = root
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| storage("invalid catalog root"))?;
    Ok(root.with_file_name(format!(".catalog-create-{name}")))
}

pub(super) fn resume(root: &Path) -> Result<(), CatalogError> {
    let stage = pending(root)?;
    if !stage.exists() {
        return Ok(());
    }
    if root.exists() {
        return Err(storage("ambiguous catalog initialization roots"));
    }
    let (store, _) = Store::open(&stage)?;
    drop(store);
    crate::storage::partition::publish_bundle(&stage, root)
        .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?;
    initial_fault("root_published");
    Ok(())
}

pub(super) fn create(root: &Path, state: &State) -> Result<Store, CatalogError> {
    if root.exists() {
        return Err(CatalogError::Conflict("catalog root exists".into()));
    }
    let stage = pending(root)?;
    drop(Store::create(&stage, state)?);
    // Preserve the existing initialization fault boundary: now this means the
    // private native journal's publication certificate is committed.
    initial_fault("metadata_committed");
    crate::storage::partition::publish_bundle(&stage, root)
        .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?;
    initial_fault("root_published");
    Store::open(root).map(|(store, _)| store)
}

fn initial_fault(stage: &str) {
    #[cfg(feature = "fault-injection")]
    if std::env::var("ANYSEEK_CATALOG_INITIAL_FAULT").as_deref() == Ok(stage) {
        std::process::exit(87);
    }
    #[cfg(not(feature = "fault-injection"))]
    let _ = stage;
}

#[cfg(test)]
fn crash(stage: &str) {
    if std::env::var("ANYSEEK_CATALOG_CRASH").as_deref() == Ok(stage) {
        std::process::exit(73);
    }
}

#[cfg(test)]
mod tests;
