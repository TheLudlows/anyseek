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
    authority_history: Vec<serde_json::Value>,
}
impl Metadata {
    fn from_state(state: &State) -> Self {
        Self {
            seq: state.seq,
            epoch_watermark: state.epoch_watermark,
            retired_backups: state.retired_backups.clone(),
            historical_operations: state.historical_operations.clone(),
            service_snapshot: state.service_snapshot.clone(),
            authority_history: state.authority_history.clone(),
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
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(storage)?;
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
        let mut store = Self::connect(root, true)?;
        store.connection.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE catalog_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), format INTEGER NOT NULL, metadata TEXT NOT NULL);
             CREATE TABLE catalog_tenants (tenant_id TEXT PRIMARY KEY);
             CREATE TABLE catalog_keys (key_id TEXT PRIMARY KEY, tenant_id TEXT REFERENCES catalog_tenants(tenant_id), metadata TEXT NOT NULL);
             CREATE TABLE catalog_collections (tenant_id TEXT NOT NULL REFERENCES catalog_tenants(tenant_id), collection_id TEXT NOT NULL, generation INTEGER NOT NULL, metadata TEXT NOT NULL, PRIMARY KEY(tenant_id,collection_id,generation));
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
        if format != 3 {
            return Err(storage("unsupported catalog SQLite format"));
        }
        let metadata: Metadata = serde_json::from_str(&metadata).map_err(storage)?;
        let mut state = State {
            seq: metadata.seq,
            epoch_watermark: metadata.epoch_watermark,
            retired_backups: metadata.retired_backups,
            historical_operations: metadata.historical_operations,
            service_snapshot: metadata.service_snapshot,
            authority_history: metadata.authority_history,
            ..State::default()
        };
        for (id, _) in
            store.rows("SELECT tenant_id,tenant_id FROM catalog_tenants ORDER BY tenant_id")?
        {
            validate_name(&id)?;
            state.tenants.insert(id);
        }
        for (id, value) in store.rows("SELECT key_id,metadata FROM catalog_keys ORDER BY key_id")? {
            state
                .keys
                .insert(id, serde_json::from_str(&value).map_err(storage)?);
        }
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
        validate_state(next)?;
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
        for tenant in next.tenants.difference(&old.tenants) {
            transaction
                .execute("INSERT INTO catalog_tenants VALUES(?1)", [tenant])
                .map_err(storage)?;
        }
        for (id, key) in &next.keys {
            let json = serde_json::to_string(key).map_err(storage)?;
            if old
                .keys
                .get(id)
                .map(serde_json::to_string)
                .transpose()
                .map_err(storage)?
                .as_ref()
                != Some(&json)
            {
                transaction.execute("INSERT INTO catalog_keys VALUES(?1,?2,?3) ON CONFLICT(key_id) DO UPDATE SET metadata=excluded.metadata", params![id,key.tenant_id,json]).map_err(storage)?;
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
        transaction.execute("INSERT INTO catalog_meta VALUES(1,3,?1) ON CONFLICT(singleton) DO UPDATE SET metadata=excluded.metadata", [serde_json::to_string(&Metadata::from_state(next)).map_err(storage)?]).map_err(storage)?;
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
    validate_state(state)?;
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
mod tests {
    use super::*;
    use crate::catalog::tests::{config, request};

    #[tokio::test]
    async fn sqlite_catalog_reopens_without_a_catalog_journal_and_excludes_second_owner() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("service");
        let catalog = Catalog::create_with_key(&root, "master".into(), "a".repeat(64))
            .await
            .unwrap();
        catalog
            .execute(request(
                "tenant",
                CatalogCommand::AddTenant {
                    tenant_id: "tenant".into(),
                },
            ))
            .await
            .unwrap();
        catalog
            .execute(request(
                "prepare",
                CatalogCommand::PrepareCollection {
                    collection_id: "documents".into(),
                    tenant_id: "tenant".into(),
                    config: config(),
                },
            ))
            .await
            .unwrap();
        assert!(root.join(DATABASE).exists());
        assert!(!root.join("generation-0000000000000000").exists());
        assert!(Catalog::open(&root).await.is_err());
        drop(catalog);
        let catalog = Catalog::open(&root).await.unwrap();
        assert_eq!(catalog.authenticate(&"a".repeat(64)).await.unwrap(), MASTER);
        assert_eq!(
            catalog.collections().await.unwrap()[0].collection_id,
            "documents"
        );
    }

    #[tokio::test]
    async fn sqlite_catalog_rolls_back_rows_and_receipt_together_on_statement_failure() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("service");
        let catalog = Catalog::create_with_key(&root, "master".into(), "a".repeat(64))
            .await
            .unwrap();
        catalog
            .execute(request(
                "tenant",
                CatalogCommand::AddTenant {
                    tenant_id: "tenant".into(),
                },
            ))
            .await
            .unwrap();
        let db = Connection::open(root.join(DATABASE)).unwrap();
        db.execute_batch("CREATE TRIGGER refuse_meta BEFORE UPDATE ON catalog_meta BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        assert!(catalog.compact().await.is_err());
        assert_eq!(catalog.authenticate(&"a".repeat(64)).await.unwrap(), MASTER);
        let change = request(
            "new-key",
            CatalogCommand::AddKey {
                key_id: "new".into(),
                tenant_id: "tenant".into(),
                verifier: "b".repeat(64),
            },
        );
        assert!(catalog.execute(change.clone()).await.is_err());
        drop(catalog);
        db.execute_batch("DROP TRIGGER refuse_meta").unwrap();
        let catalog = Catalog::open(&root).await.unwrap();
        assert!(catalog.authenticate(&"b".repeat(64)).await.is_err());
        assert_eq!(catalog.operation(&change.operation_id).await.unwrap(), None);
        catalog.execute(change).await.unwrap();
        assert_eq!(
            catalog.authenticate(&"b".repeat(64)).await.unwrap(),
            "tenant"
        );
    }

    #[tokio::test]
    async fn sqlite_catalog_resumes_committed_private_initialization() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("service");
        let stage = pending(&root).unwrap();
        std::fs::create_dir(&stage).unwrap();
        drop(Store::create(&stage, &State::default()).unwrap());
        Catalog::resume_initialization(&root).await.unwrap();
        assert!(!stage.exists());
        assert!(root.join(DATABASE).exists());
        assert!(Catalog::open(&root).await.is_ok());
    }

    #[tokio::test]
    async fn sqlite_catalog_missing_or_corrupt_database_never_becomes_empty_catalog() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("service");
        drop(
            Catalog::create_with_key(&root, "master".into(), "a".repeat(64))
                .await
                .unwrap(),
        );
        std::fs::remove_file(root.join(DATABASE)).unwrap();
        assert!(Catalog::open(&root).await.is_err());
        assert!(!root.join(DATABASE).exists());
        std::fs::write(root.join(DATABASE), b"invalid SQLite").unwrap();
        assert!(Catalog::open(&root).await.is_err());
        assert_eq!(
            std::fs::read(root.join(DATABASE)).unwrap(),
            b"invalid SQLite"
        );
    }

    #[cfg(feature = "s3-object-store")]
    #[tokio::test]
    async fn sqlite_catalog_transactions_leave_shared_record_authority_tables_intact() {
        use crate::storage::sqlite_authority::SqliteAuthority;
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("service");
        let catalog = Catalog::create_with_key(&root, "master".into(), "a".repeat(64))
            .await
            .unwrap();
        catalog
            .execute(request(
                "tenant",
                CatalogCommand::AddTenant {
                    tenant_id: "tenant".into(),
                },
            ))
            .await
            .unwrap();
        let mut authority =
            SqliteAuthority::open(&root.join(DATABASE), Duration::from_secs(5)).unwrap();
        let original = authority.create_partition("p0", 7).unwrap();
        catalog
            .execute(request(
                "new-key",
                CatalogCommand::AddKey {
                    key_id: "new".into(),
                    tenant_id: "tenant".into(),
                    verifier: "b".repeat(64),
                },
            ))
            .await
            .unwrap();
        catalog.compact().await.unwrap();
        assert_eq!(authority.read_head("p0").unwrap(), Some(original.clone()));
        drop(authority);
        drop(catalog);
        let catalog = Catalog::open(&root).await.unwrap();
        let authority =
            SqliteAuthority::open(&root.join(DATABASE), Duration::from_secs(5)).unwrap();
        assert_eq!(authority.read_head("p0").unwrap(), Some(original));
        assert_eq!(
            catalog.authenticate(&"b".repeat(64)).await.unwrap(),
            "tenant"
        );
    }
}
