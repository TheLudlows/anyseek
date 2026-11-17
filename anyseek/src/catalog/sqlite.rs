//! Catalog metadata in the service's SQLite database. No Record payloads live here.
use super::*;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::{
    fs::{File, OpenOptions as FileOptions},
    path::PathBuf,
    time::Duration,
};

pub(super) const DATABASE: &str = "metadata.sqlite";

pub(super) struct Store {
    connection: Connection,
    // A second Catalog must not retain an independent stale in-memory view.
    _owner: File,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    seq: u64,
    epoch_watermark: u64,
    retired_backups: std::collections::BTreeSet<String>,
    historical_operations: Vec<(String, OperationId)>,
    service_snapshot: Option<(String, bool)>,
}
impl Metadata {
    fn from_state(state: &State) -> Self {
        Self {
            seq: state.seq,
            epoch_watermark: state.epoch_watermark,
            retired_backups: state.retired_backups.clone(),
            historical_operations: state.historical_operations.clone(),
            service_snapshot: state.service_snapshot.clone(),
        }
    }
}
impl Store {
    fn connect(root: &Path, create: bool) -> Result<Self, CatalogError> {
        let owner = FileOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(root.join("owner"))
            .map_err(storage)?;
        owner.try_lock().map_err(storage)?;
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | if create {
                OpenFlags::SQLITE_OPEN_CREATE
            } else {
                OpenFlags::empty()
            };
        let connection =
            Connection::open_with_flags(root.join(DATABASE), flags).map_err(storage)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(storage)?;
        if create {
            connection
                .pragma_update(None, "journal_mode", "WAL")
                .map_err(storage)?;
        } else {
            let mode: String = connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(storage)?;
            if !mode.eq_ignore_ascii_case("wal") {
                return Err(storage("catalog requires existing WAL mode"));
            }
        }
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(storage)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(storage)?;
        Ok(Self {
            connection,
            _owner: owner,
        })
    }
    pub(super) fn create(root: &Path, state: &State) -> Result<Self, CatalogError> {
        validate_state(state)?;
        let mut store = Self::connect(root, true)?;
        store.connection.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE catalog_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), format INTEGER NOT NULL, metadata TEXT NOT NULL);
             CREATE TABLE catalog_collections (tenant_id TEXT NOT NULL, collection_id TEXT NOT NULL, generation INTEGER NOT NULL, metadata TEXT NOT NULL, PRIMARY KEY(tenant_id,collection_id,generation));
             CREATE UNIQUE INDEX catalog_current_collection ON catalog_collections(tenant_id,collection_id) WHERE json_extract(metadata,'$.deleted')=0;
             CREATE TABLE catalog_backups (backup_id TEXT PRIMARY KEY, metadata TEXT NOT NULL);
             CREATE TABLE catalog_operations (scope TEXT NOT NULL, epoch INTEGER NOT NULL, nonce TEXT NOT NULL, metadata TEXT NOT NULL, PRIMARY KEY(scope,epoch,nonce));
             COMMIT;"
        ).map_err(storage)?;
        store.persist(&State::default(), state)?;
        Ok(store)
    }
    pub(super) fn open(root: &Path) -> Result<(Self, State), CatalogError> {
        let store = Self::connect(root, false)?;
        let (format, metadata): (u32, String) = store
            .connection
            .query_row(
                "SELECT format,metadata FROM catalog_meta WHERE singleton=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(storage)?;
        if format != 7 {
            return Err(storage("unsupported catalog SQLite format"));
        }
        let metadata: Metadata = serde_json::from_str(&metadata).map_err(storage)?;
        let mut state = State {
            seq: metadata.seq,
            epoch_watermark: metadata.epoch_watermark,
            retired_backups: metadata.retired_backups,
            historical_operations: metadata.historical_operations,
            service_snapshot: metadata.service_snapshot,
            ..State::default()
        };
        for (id, value) in store
            .rows("SELECT tenant_id || '/' || collection_id || '/' || printf('%016x',generation),metadata FROM catalog_collections ORDER BY tenant_id,collection_id,generation")?
        {
            let entry: CollectionEntry = serde_json::from_str(&value).map_err(storage)?;
            scope(&entry.tenant_id, &entry.collection_id)?;
            if entry.scope() != id || entry.generation == 0 || entry.generation > state.seq {
                return Err(storage("catalog collection identity mismatch"));
            }
            state.collections.insert(id, entry);
        }
        for (id, value) in
            store.rows("SELECT backup_id,metadata FROM catalog_backups ORDER BY backup_id")?
        {
            state
                .backups
                .insert(id, serde_json::from_str(&value).map_err(storage)?);
        }
        let mut statement = store
            .connection
            .prepare("SELECT metadata FROM catalog_operations ORDER BY epoch,nonce")
            .map_err(storage)?;
        for row in statement
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(storage)?
        {
            state
                .operations
                .push(serde_json::from_str(&row.map_err(storage)?).map_err(storage)?);
        }
        drop(statement);
        state.operations.sort_by_key(|op| op.receipt.seq);
        validate_state(&state)?;
        Ok((store, state))
    }
    fn rows(&self, sql: &str) -> Result<Vec<(String, String)>, CatalogError> {
        self.connection
            .prepare(sql)
            .map_err(storage)?
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(storage)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(storage)
    }
    pub(super) fn persist(&mut self, old: &State, next: &State) -> Result<(), CatalogError> {
        let transaction = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(storage)?;
        let current: Option<u64> = transaction
            .query_row(
                "SELECT json_extract(metadata,'$.seq') FROM catalog_meta WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        if current.is_some_and(|seq| seq != old.seq) {
            return Err(CatalogError::RecoveryRequired);
        }
        // Only changed catalog rows are written. Record authority connections
        // share this database but their tables are never replaced or cleared.
        for (id, collection) in &old.collections {
            if !next.collections.contains_key(id) {
                transaction.execute("DELETE FROM catalog_collections WHERE tenant_id=?1 AND collection_id=?2 AND generation=?3",
                    params![collection.tenant_id,collection.collection_id,collection.generation]).map_err(storage)?;
            }
        }
        for (id, collection) in &next.collections {
            let json = serde_json::to_string(collection).map_err(storage)?;
            if old
                .collections
                .get(id)
                .map(serde_json::to_string)
                .transpose()
                .map_err(storage)?
                .as_ref()
                != Some(&json)
            {
                transaction.execute("INSERT INTO catalog_collections VALUES(?1,?2,?3,?4) ON CONFLICT(tenant_id,collection_id,generation) DO UPDATE SET metadata=excluded.metadata", params![collection.tenant_id,collection.collection_id,collection.generation,json]).map_err(storage)?;
            }
        }
        for (id, backup) in &next.backups {
            if !old.backups.contains_key(id) {
                transaction
                    .execute(
                        "INSERT INTO catalog_backups VALUES(?1,?2)",
                        params![id, serde_json::to_string(backup).map_err(storage)?],
                    )
                    .map_err(storage)?;
            }
        }
        transaction
            .execute(
                "DELETE FROM catalog_operations WHERE epoch < ?1",
                [next.epoch_watermark.saturating_sub(1)],
            )
            .map_err(storage)?;
        let old_seq = old.seq;
        for operation in &next.operations {
            if operation.receipt.seq > old_seq || current.is_none() {
                transaction
                    .execute(
                        "INSERT INTO catalog_operations VALUES(?1,?2,?3,?4)",
                        params![
                            operation.request.command.tenant(),
                            operation.request.operation_id.epoch,
                            operation.request.operation_id.nonce,
                            serde_json::to_string(operation).map_err(storage)?
                        ],
                    )
                    .map_err(storage)?;
            }
        }
        transaction.execute("INSERT INTO catalog_meta VALUES(1,7,?1) ON CONFLICT(singleton) DO UPDATE SET metadata=excluded.metadata", [serde_json::to_string(&Metadata::from_state(next)).map_err(storage)?]).map_err(storage)?;
        #[cfg(test)]
        crash("before_commit");
        transaction
            .commit()
            .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?;
        #[cfg(test)]
        crash("after_commit");
        Ok(())
    }
}
pub(super) fn validate_state(state: &State) -> Result<(), CatalogError> {
    let bytes = serde_json::to_vec(state).map_err(storage)?;
    let _: State = serde_json::from_slice(&bytes)
        .map_err(|e| CatalogError::Rejected(format!("unrecoverable catalog metadata: {e}")))?;
    if bytes.len() as u64 > BUDGET {
        return Err(CatalogError::Rejected(
            "catalog metadata budget exceeded".into(),
        ));
    }
    Ok(())
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
    std::fs::create_dir(&stage).map_err(storage)?;
    drop(Store::create(&stage, state)?);
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
