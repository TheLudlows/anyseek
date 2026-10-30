//! SQLite tenant identity and collection lifecycle authority. Data directories
//! are independent of identities; revoking a key never deletes a collection.
//! The HTTP service integration is tracked in docs/record-model.md.
use crate::{
    record::{OperationId, RecordConfig},
    storage::partition::PartitionJournal,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Semaphore};

mod execution;
mod service_snapshot;
mod sqlite;
#[cfg(test)]
mod tests;
pub use service_snapshot::ServiceSnapshot;

const BUDGET: u64 = 16 * 1024 * 1024;
const MAX_OPERATIONS: usize = 10_000;

/// Admission, authority and persistence outcomes; errors never imply a revoke succeeded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogError {
    /// Admission refused before operation lookup; prior attempts may have succeeded.
    Busy,
    /// Request not accepted; resource or validation failure.
    Rejected(String),
    /// Identity, lifecycle or retry semantic conflict.
    Conflict(String),
    /// Authentication or tenant ownership check failed.
    Unauthorized,
    /// Operation epoch lies outside the supported retry window.
    OperationExpired,
    /// Durability or application is uncertain; recover and query the same ID.
    OutcomeUnknown(String),
    /// Authority is fenced until recovery.
    RecoveryRequired,
    /// Opening or reading persistent authority failed.
    Storage(String),
}
impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CatalogError {}
fn storage(e: impl std::fmt::Display) -> CatalogError {
    CatalogError::Storage(e.to_string())
}
fn epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        / 86400
}

/// Reserved authenticated platform identity, outside the tenant ID alphabet.
pub const MASTER: &str = "@master";

/// Validate a tenant/collection path component without normalization.
pub fn validate_name(value: &str) -> Result<(), CatalogError> {
    if !(1..=63).contains(&value.len())
        || !value.as_bytes()[0].is_ascii_lowercase()
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
    {
        return Err(CatalogError::Rejected(
            "name must match ^[a-z][a-z0-9_-]{0,62}$".into(),
        ));
    }
    Ok(())
}
fn scope(tenant: &str, collection: &str) -> Result<String, CatalogError> {
    validate_name(tenant)?;
    validate_name(collection)?;
    Ok(format!("{tenant}/{collection}"))
}
fn identity(value: &str) -> Result<(), CatalogError> {
    if value.is_empty() || value.len() > 256 {
        return Err(CatalogError::Rejected(
            "identity length must be 1..256 bytes".into(),
        ));
    }
    Ok(())
}

/// Trusted management/lifecycle commands. HTTP enforces the caller boundary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CatalogCommand {
    /// Create an immutable tenant identity.
    AddTenant { tenant_id: String },
    /// Bind a new credential verifier to one existing tenant.
    AddKey {
        /// Stable credential ID; never reused after revocation.
        key_id: String,
        /// Immutable tenant identity.
        tenant_id: String,
        /// SHA-256 verifier; raw credentials are not persisted.
        verifier: String,
    },
    /// Disable a credential without deleting tenant data.
    RevokeKey { key_id: String },
    /// Persist a collection initialization intent before opening its data engine.
    PrepareCollection {
        /// Immutable tenant identity.
        tenant_id: String,
        /// Name unique within its tenant.
        collection_id: String,
        /// Immutable Record configuration.
        config: RecordConfig,
    },
    /// Restore into a previously unused collection identity.
    PrepareRestore {
        /// Immutable tenant identity.
        tenant_id: String,
        /// Name unique within its tenant.
        collection_id: String,
        /// Immutable Record configuration.
        config: RecordConfig,
        /// Stable backup identity without path components.
        backup_id: String,
    },
    /// Publish completed physical initialization.
    ActivateCollection {
        /// Immutable tenant identity.
        tenant_id: String,
        /// Name unique within its tenant.
        collection_id: String,
    },
    /// Retain backup ownership before creating its physical archive.
    RegisterBackup {
        /// Immutable tenant identity.
        tenant_id: String,
        /// Name unique within its tenant.
        collection_id: String,
        /// Stable backup identity without path components.
        backup_id: String,
    },
    /// Retire a backup after excluding active restores.
    RetireBackup { backup_id: String },
    /// Logically delete a collection; physical reclamation is separate.
    DeleteCollection {
        /// Immutable tenant identity.
        tenant_id: String,
        /// Name unique within its tenant.
        collection_id: String,
    },
}
impl CatalogCommand {
    fn tenant(&self) -> &str {
        match self {
            Self::PrepareCollection { tenant_id, .. }
            | Self::PrepareRestore { tenant_id, .. }
            | Self::ActivateCollection { tenant_id, .. }
            | Self::DeleteCollection { tenant_id, .. }
            | Self::RegisterBackup { tenant_id, .. } => tenant_id,
            _ => MASTER,
        }
    }
    fn collection_scope(&self) -> Option<String> {
        match self {
            Self::PrepareCollection {
                tenant_id,
                collection_id,
                ..
            }
            | Self::PrepareRestore {
                tenant_id,
                collection_id,
                ..
            }
            | Self::ActivateCollection {
                tenant_id,
                collection_id,
            }
            | Self::DeleteCollection {
                tenant_id,
                collection_id,
            } => scope(tenant_id, collection_id).ok(),
            _ => None,
        }
    }
}
/// Idempotent control-plane request; its scope comes from the command.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CatalogRequest {
    /// Original request identity retained across retries.
    pub operation_id: OperationId,
    /// Management or lifecycle command.
    pub command: CatalogCommand,
}
/// Durable control-plane outcome.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogReceipt {
    /// Durable catalog sequence.
    pub seq: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Key {
    /// SHA-256 verifier; raw credentials are not persisted.
    verifier: String,
    /// Physical initialization is complete and visible.
    active: bool,
    /// Immutable tenant identity.
    tenant_id: Option<String>,
}
/// Tenant-owned physical initialization/lifecycle metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CollectionEntry {
    /// Immutable tenant identity.
    pub tenant_id: String,
    /// Name unique within its tenant.
    pub collection_id: String,
    /// Internal lifecycle identity, allocated by the catalog sequence.
    pub generation: u64,
    /// Immutable Record configuration.
    pub config: RecordConfig,
    /// Physical initialization is complete and visible.
    pub active: bool,
    /// Physical initialization occurred, including before deletion.
    pub initialized: bool,
    /// Logically deleted; physical reclamation is separate.
    pub deleted: bool,
    /// Source backup identity for a retained restore intent.
    pub restore_from: Option<String>,
}
impl CollectionEntry {
    /// Tenant-qualified cache/backup identity, distinct from the public name.
    pub fn scope(&self) -> String {
        format!(
            "{}/{}/{:016x}",
            self.tenant_id, self.collection_id, self.generation
        )
    }
    /// Local data location derived directly from tenant/name and internal generation.
    pub fn directory(&self) -> String {
        format!(
            "collections/{}/{}/{:016x}",
            self.tenant_id, self.collection_id, self.generation
        )
    }
    /// Durably establish the name hierarchy before publishing a child partition.
    pub fn prepare_parent(&self, root: &Path) -> Result<(), CatalogError> {
        validate_name(&self.tenant_id)?;
        validate_name(&self.collection_id)?;
        let parent = root.join(self.directory()).parent().unwrap().to_path_buf();
        std::fs::create_dir_all(&parent).map_err(storage)?;
        #[cfg(unix)]
        for path in parent.ancestors().take_while(|path| path.starts_with(root)) {
            std::fs::File::open(path)
                .and_then(|f| f.sync_all())
                .map_err(storage)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BackupEntry {
    tenant_id: String,
    collection_id: String,
    generation: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Applied {
    request: CatalogRequest,
    receipt: CatalogReceipt,
    observed_epoch: u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct State {
    seq: u64,
    epoch_watermark: u64,
    tenants: std::collections::BTreeSet<String>,
    keys: BTreeMap<String, Key>,
    collections: BTreeMap<String, CollectionEntry>,
    backups: BTreeMap<String, BackupEntry>,
    operations: Vec<Applied>,
    retired_backups: std::collections::BTreeSet<String>,
    historical_operations: Vec<(String, OperationId)>,
    service_snapshot: Option<(String, bool)>,
    authority_history: Vec<serde_json::Value>,
}
impl State {
    // Generations sort in allocation order within one validated tenant/name.
    fn current_key(&self, id: &str) -> Option<String> {
        self.collections
            .range(format!("{id}/")..format!("{id}0"))
            .next_back()
            .map(|(key, _)| key.clone())
    }
    fn collection(&self, id: &str) -> Option<&CollectionEntry> {
        self.current_key(id)
            .and_then(|key| self.collections.get(&key))
    }
    fn collection_mut(&mut self, id: &str) -> Option<&mut CollectionEntry> {
        self.current_key(id)
            .and_then(|key| self.collections.get_mut(&key))
    }
}
struct Inner {
    state: State,
    healthy: bool,
    failed: Arc<AtomicBool>,
    store: sqlite::Store,
    #[cfg(test)]
    operation_limit: usize,
    #[cfg(test)]
    fail_after_durable: bool,
}
impl Inner {
    fn is_healthy(&self) -> bool {
        self.healthy && !self.failed.load(Ordering::Acquire)
    }
    fn compact(&mut self, now: u64) -> Result<(), CatalogError> {
        let mut next = self.state.clone();
        next.epoch_watermark = now.max(next.epoch_watermark);
        next.operations
            .retain(|op| op.request.operation_id.epoch.saturating_add(1) >= next.epoch_watermark);
        self.healthy = false;
        match self.store.persist(&self.state, &next) {
            Ok(()) => {
                self.state = next;
                self.healthy = true;
                Ok(())
            }
            Err(error @ (CatalogError::OutcomeUnknown(_) | CatalogError::RecoveryRequired)) => {
                Err(error)
            }
            Err(error) => {
                self.healthy = true;
                Err(error)
            }
        }
    }
}
struct CollectionWork {
    failed: Arc<AtomicBool>,
    complete: bool,
}
impl Drop for CollectionWork {
    fn drop(&mut self) {
        if !self.complete {
            self.failed.store(true, Ordering::Release);
        }
    }
}
/// Bounded tenant metadata authority with owned collection lifecycle work.
#[derive(Clone)]
pub struct Catalog {
    inner: Arc<Mutex<Inner>>,
    admission: Arc<Semaphore>,
    execution: Arc<execution::Execution>,
}
fn backup_id(id: &str) -> Result<(), CatalogError> {
    if id.len() != 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(CatalogError::Rejected("invalid backup identity".into()));
    }
    Ok(())
}
fn apply(state: &mut State, event: &Applied) -> Result<(), CatalogError> {
    if event.receipt.seq
        != state
            .seq
            .checked_add(1)
            .ok_or_else(|| storage("sequence exhausted"))?
    {
        return Err(storage("catalog sequence mismatch"));
    }
    match &event.request.command {
        CatalogCommand::AddTenant { tenant_id } => {
            validate_name(tenant_id)?;
            if !state.tenants.insert(tenant_id.clone()) {
                return Err(CatalogError::Conflict("tenant exists".into()));
            }
        }
        CatalogCommand::AddKey {
            key_id,
            tenant_id,
            verifier,
        } => {
            identity(key_id)?;
            validate_name(tenant_id)?;
            if !state.tenants.contains(tenant_id) {
                return Err(CatalogError::Rejected("tenant not found".into()));
            }
            if key_id == "master"
                || verifier.len() != 64
                || !verifier.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(CatalogError::Rejected(
                    "reserved key ID or invalid SHA-256 verifier".into(),
                ));
            }
            if state.keys.contains_key(key_id)
                || state
                    .keys
                    .values()
                    .any(|k| k.verifier.eq_ignore_ascii_case(verifier))
            {
                return Err(CatalogError::Conflict(
                    "key ID or verifier already retained".into(),
                ));
            }
            state.keys.insert(
                key_id.clone(),
                Key {
                    verifier: verifier.to_ascii_lowercase(),
                    active: true,
                    tenant_id: Some(tenant_id.clone()),
                },
            );
        }
        CatalogCommand::RevokeKey { key_id } => {
            if key_id == "master" {
                return Err(CatalogError::Rejected("master cannot be revoked".into()));
            }
            state
                .keys
                .get_mut(key_id)
                .ok_or(CatalogError::Unauthorized)?
                .active = false;
        }
        CatalogCommand::PrepareCollection {
            tenant_id,
            collection_id,
            config,
        }
        | CatalogCommand::PrepareRestore {
            tenant_id,
            collection_id,
            config,
            ..
        } => {
            let id = scope(tenant_id, collection_id)?;
            if !state.tenants.contains(tenant_id) {
                return Err(CatalogError::Unauthorized);
            }
            crate::record::validate_config(config)
                .map_err(|e| CatalogError::Rejected(e.to_string()))?;
            if state.collection(&id).is_some_and(|e| !e.deleted) {
                return Err(CatalogError::Conflict("collection name occupied".into()));
            }
            let restore_from = if let CatalogCommand::PrepareRestore { backup_id: id, .. } =
                &event.request.command
            {
                backup_id(id)?;
                if state.retired_backups.contains(id)
                    || !state
                        .backups
                        .get(id)
                        .is_some_and(|b| b.tenant_id == *tenant_id)
                {
                    return Err(CatalogError::Unauthorized);
                }
                Some(id.clone())
            } else {
                None
            };
            state.collections.insert(
                format!("{id}/{:016x}", event.receipt.seq),
                CollectionEntry {
                    tenant_id: tenant_id.clone(),
                    collection_id: collection_id.clone(),
                    generation: event.receipt.seq,
                    config: config.clone(),
                    active: false,
                    initialized: false,
                    deleted: false,
                    restore_from,
                },
            );
        }
        CatalogCommand::ActivateCollection {
            tenant_id,
            collection_id,
        } => {
            let entry = state
                .collection_mut(&scope(tenant_id, collection_id)?)
                .ok_or_else(|| CatalogError::Conflict("collection not prepared".into()))?;
            if entry.active || entry.deleted {
                return Err(CatalogError::Conflict(
                    "collection already active or deleted".into(),
                ));
            }
            entry.active = true;
            entry.initialized = true;
        }
        CatalogCommand::DeleteCollection {
            tenant_id,
            collection_id,
        } => {
            let entry = state
                .collection_mut(&scope(tenant_id, collection_id)?)
                .ok_or_else(|| CatalogError::Conflict("collection not found".into()))?;
            entry.active = false;
            entry.deleted = true;
        }
        CatalogCommand::RegisterBackup {
            tenant_id,
            collection_id,
            backup_id: id,
        } => {
            backup_id(id)?;
            if !state
                .collection(&scope(tenant_id, collection_id)?)
                .is_some_and(|e| e.active && !e.deleted)
            {
                return Err(CatalogError::Unauthorized);
            }
            if state.retired_backups.contains(id) {
                return Err(CatalogError::Conflict("backup retired".into()));
            }
            let generation = state
                .collection(&scope(tenant_id, collection_id)?)
                .unwrap()
                .generation;
            if state.backups.get(id).is_some_and(|b| {
                b.tenant_id != *tenant_id
                    || b.collection_id != *collection_id
                    || b.generation != generation
            }) {
                return Err(CatalogError::Conflict("backup identity reused".into()));
            }
            state.backups.insert(
                id.clone(),
                BackupEntry {
                    tenant_id: tenant_id.clone(),
                    collection_id: collection_id.clone(),
                    generation,
                },
            );
        }
        CatalogCommand::RetireBackup { backup_id: id } => {
            backup_id(id)?;
            if state
                .collections
                .values()
                .any(|e| !e.active && !e.deleted && e.restore_from.as_ref() == Some(id))
            {
                return Err(CatalogError::Conflict(
                    "backup pinned by unfinished restore".into(),
                ));
            }
            state.retired_backups.insert(id.clone());
        }
    }
    state.seq = event.receipt.seq;
    state.epoch_watermark = state.epoch_watermark.max(event.observed_epoch);
    state.operations.push(event.clone());
    Ok(())
}
impl Catalog {
    /// Resume a durably prepared, unserved initial database.
    pub async fn resume_initialization(path: &Path) -> Result<(), CatalogError> {
        sqlite::resume(path)
    }
    /// Create an empty SQLite catalog in a new root.
    pub async fn create(path: &Path) -> Result<Self, CatalogError> {
        Self::create_state(path, State::default()).await
    }
    /// Initialize the platform credential without a tenant data identity.
    pub async fn create_with_key(
        path: &Path,
        key_id: String,
        verifier: String,
    ) -> Result<Self, CatalogError> {
        if key_id != "master"
            || verifier.len() != 64
            || !verifier.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(CatalogError::Rejected("invalid platform credential".into()));
        }
        let mut state = State::default();
        state.keys.insert(
            key_id,
            Key {
                verifier: verifier.to_ascii_lowercase(),
                active: true,
                tenant_id: None,
            },
        );
        Self::create_state(path, state).await
    }
    async fn create_state(path: &Path, state: State) -> Result<Self, CatalogError> {
        let path = path.to_owned();
        crate::resources::spawn(async move {
            let store = sqlite::create(&path, &state)?;
            Ok(Self::from_parts(state, store))
        })
        .await
        .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?
    }
    /// Open a valid existing SQLite catalog; never replace missing metadata.
    pub async fn open(path: &Path) -> Result<Self, CatalogError> {
        let path = path.to_owned();
        crate::resources::spawn(async move {
            let (store, state) = sqlite::Store::open(&path)?;
            Ok(Self::from_parts(state, store))
        })
        .await
        .map_err(storage)?
    }
    fn from_parts(state: State, store: sqlite::Store) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                state,
                healthy: true,
                failed: Arc::new(AtomicBool::new(false)),
                store,
                #[cfg(test)]
                operation_limit: MAX_OPERATIONS,
                #[cfg(test)]
                fail_after_durable: false,
            })),
            admission: Arc::new(Semaphore::new(64)),
            execution: Arc::new(execution::Execution::default()),
        }
    }
    /// Persist a trusted command and its tenant-scoped retry receipt atomically.
    pub async fn execute(&self, request: CatalogRequest) -> Result<CatalogReceipt, CatalogError> {
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| CatalogError::Busy)?;
        let owner = self.inner.clone();
        let execution = self.execution.clone();
        crate::resources::spawn(async move {
            let _admission = admission;
            let _execution = execution.control(&request.command).await?;
            let mut inner = owner.lock().await;
            if !inner.is_healthy() {
                return Err(CatalogError::RecoveryRequired);
            }
            let now = epoch().max(inner.state.epoch_watermark);
            let id = &request.operation_id;
            if id.epoch > now || id.epoch.saturating_add(1) < now {
                return Err(CatalogError::OperationExpired);
            }
            identity(&id.nonce)?;
            let tenant = request.command.tenant();
            if inner
                .state
                .historical_operations
                .contains(&(tenant.to_owned(), id.clone()))
            {
                return Err(CatalogError::Conflict(
                    "operation belongs to pre-restore authority".into(),
                ));
            }
            if let Some(old) =
                inner.state.operations.iter().find(|op| {
                    op.request.command.tenant() == tenant && op.request.operation_id == *id
                })
            {
                return if serde_json::to_value(&old.request).map_err(storage)?
                    == serde_json::to_value(&request).map_err(storage)?
                {
                    Ok(old.receipt.clone())
                } else {
                    Err(CatalogError::Conflict("operation ID reused".into()))
                };
            }
            #[cfg(test)]
            let limit = inner.operation_limit;
            #[cfg(not(test))]
            let limit = MAX_OPERATIONS;
            if inner.state.operations.len() >= limit {
                inner.compact(now)?;
                if inner.state.operations.len() >= limit {
                    return Err(CatalogError::Rejected(
                        "catalog operation budget exhausted".into(),
                    ));
                }
            }
            let seq = inner
                .state
                .seq
                .checked_add(1)
                .ok_or_else(|| storage("sequence exhausted"))?;
            let event = Applied {
                request,
                receipt: CatalogReceipt { seq },
                observed_epoch: now,
            };
            let mut next = inner.state.clone();
            apply(&mut next, &event)?;
            sqlite::validate_state(&next)?;
            inner.healthy = false;
            {
                let Inner { store, state, .. } = &mut *inner;
                store.persist(state, &next)?;
            }
            #[cfg(test)]
            if inner.fail_after_durable {
                return Err(CatalogError::OutcomeUnknown(
                    "injected after durability".into(),
                ));
            }
            inner.state = next;
            inner.healthy = true;
            Ok(event.receipt)
        })
        .await
        .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?
    }
    /// Resolve the authenticated tenant once. Revocation affects subsequent authentication.
    pub async fn authenticate(&self, verifier: &str) -> Result<String, CatalogError> {
        let inner = self.inner.lock().await;
        if !inner.is_healthy() {
            return Err(CatalogError::RecoveryRequired);
        }
        inner
            .state
            .keys
            .values()
            .find(|k| k.active && k.verifier == verifier)
            .map(|k| k.tenant_id.clone().unwrap_or_else(|| MASTER.into()))
            .ok_or(CatalogError::Unauthorized)
    }
    /// List tenant IDs for the authenticated management layer.
    pub async fn tenants(&self) -> Result<Vec<String>, CatalogError> {
        let inner = self.inner.lock().await;
        if !inner.is_healthy() {
            return Err(CatalogError::RecoveryRequired);
        }
        Ok(inner.state.tenants.iter().cloned().collect())
    }
    /// Inventory of logically retired backups.
    pub async fn retired_backups(
        &self,
    ) -> Result<std::collections::BTreeSet<String>, CatalogError> {
        let inner = self.inner.lock().await;
        if !inner.is_healthy() {
            return Err(CatalogError::RecoveryRequired);
        }
        Ok(inner.state.retired_backups.clone())
    }
    /// Check backup tenant ownership, including retained completed restores.
    pub async fn require_backup(&self, tenant: &str, id: &str) -> Result<(), CatalogError> {
        validate_name(tenant)?;
        let inner = self.inner.lock().await;
        if !inner.is_healthy() {
            return Err(CatalogError::RecoveryRequired);
        }
        if !inner
            .state
            .backups
            .get(id)
            .is_some_and(|b| b.tenant_id == tenant)
        {
            return Err(CatalogError::Unauthorized);
        }
        Ok(())
    }
    /// Privileged lifecycle inventory including prepared and deleted collections.
    pub async fn collections(&self) -> Result<Vec<CollectionEntry>, CatalogError> {
        let inner = self.inner.lock().await;
        if !inner.is_healthy() {
            return Err(CatalogError::RecoveryRequired);
        }
        Ok(inner.state.collections.values().cloned().collect())
    }
    /// List active collections belonging to an already authenticated tenant.
    pub async fn listing(&self, tenant: &str) -> Result<Vec<CollectionEntry>, CatalogError> {
        validate_name(tenant)?;
        let inner = self.inner.lock().await;
        if !inner.is_healthy() {
            return Err(CatalogError::RecoveryRequired);
        }
        if !inner.state.tenants.contains(tenant) {
            return Err(CatalogError::Unauthorized);
        }
        Ok(inner
            .state
            .collections
            .values()
            .filter(|e| e.tenant_id == tenant && e.active && !e.deleted)
            .cloned()
            .collect())
    }
    /// Work already authenticated by the service is scoped to its tenant and
    /// ordered against collection deletion and complete-service snapshots.
    pub async fn authorized<T, F, Fut>(
        &self,
        tenant: String,
        collection: String,
        work: F,
    ) -> Result<T, CatalogError>
    where
        T: Send + 'static,
        F: FnOnce(CollectionEntry) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.authorized_inner(tenant, collection, None, work).await
    }
    /// Execute only against the captured incarnation, never a same-name replacement.
    pub async fn authorized_generation<T, F, Fut>(
        &self,
        tenant: String,
        collection: String,
        generation: u64,
        work: F,
    ) -> Result<T, CatalogError>
    where
        T: Send + 'static,
        F: FnOnce(CollectionEntry) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.authorized_inner(tenant, collection, Some(generation), work)
            .await
    }
    async fn authorized_inner<T, F, Fut>(
        &self,
        tenant: String,
        collection: String,
        generation: Option<u64>,
        work: F,
    ) -> Result<T, CatalogError>
    where
        T: Send + 'static,
        F: FnOnce(CollectionEntry) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        let id = scope(&tenant, &collection)?;
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| CatalogError::Busy)?;
        let owner = self.inner.clone();
        let execution = self.execution.clone();
        crate::resources::spawn(async move {
            let _admission = admission;
            let _execution = execution.read(&id).await?;
            let inner = owner.lock().await;
            if !inner.is_healthy() {
                return Err(CatalogError::RecoveryRequired);
            }
            let entry = inner
                .state
                .collection(&id)
                .filter(|e| e.active && !e.deleted && generation.is_none_or(|g| g == e.generation))
                .ok_or(CatalogError::Unauthorized)?
                .clone();
            let mut guard = CollectionWork {
                failed: inner.failed.clone(),
                complete: false,
            };
            drop(inner);
            let result = work(entry).await;
            guard.complete = true;
            Ok(result)
        })
        .await
        .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?
    }
    /// Prune expired management receipts without changing collection ownership.
    pub async fn compact(&self) -> Result<(), CatalogError> {
        let _admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| CatalogError::Busy)?;
        let _execution = self.execution.freeze().await;
        let mut inner = self.inner.lock().await;
        if !inner.is_healthy() {
            return Err(CatalogError::RecoveryRequired);
        }
        let now = epoch().max(inner.state.epoch_watermark);
        inner.compact(now)
    }
    /// Look up a platform management receipt.
    pub async fn operation(
        &self,
        id: &OperationId,
    ) -> Result<Option<CatalogReceipt>, CatalogError> {
        self.operation_for(MASTER, id).await
    }
    /// Look up a control receipt within its trusted tenant scope.
    pub async fn operation_for(
        &self,
        tenant: &str,
        id: &OperationId,
    ) -> Result<Option<CatalogReceipt>, CatalogError> {
        let inner = self.inner.lock().await;
        if !inner.is_healthy() {
            return Err(CatalogError::RecoveryRequired);
        }
        let now = epoch().max(inner.state.epoch_watermark);
        if id.epoch > now || id.epoch.saturating_add(1) < now {
            return Err(CatalogError::OperationExpired);
        }
        if inner
            .state
            .historical_operations
            .contains(&(tenant.into(), id.clone()))
        {
            return Err(CatalogError::Conflict(
                "operation belongs to pre-restore authority".into(),
            ));
        }
        Ok(inner
            .state
            .operations
            .iter()
            .find(|op| op.request.command.tenant() == tenant && op.request.operation_id == *id)
            .map(|op| op.receipt.clone()))
    }
}
