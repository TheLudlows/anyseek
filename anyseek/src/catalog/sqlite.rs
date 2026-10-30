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
    historical_operations: Vec<OperationId>,
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
             CREATE TABLE catalog_keys (key_id TEXT PRIMARY KEY, metadata TEXT NOT NULL);
             CREATE TABLE catalog_collections (collection_id TEXT PRIMARY KEY, metadata TEXT NOT NULL);
             CREATE TABLE catalog_grants (collection_id TEXT NOT NULL, key_id TEXT NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY(collection_id,key_id));
             CREATE TABLE catalog_operations (epoch INTEGER NOT NULL, nonce TEXT NOT NULL, metadata TEXT NOT NULL, PRIMARY KEY(epoch,nonce));
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
        if format != 1 {
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
        for (id, value) in store.rows("SELECT key_id,metadata FROM catalog_keys ORDER BY key_id")? {
            state
                .keys
                .insert(id, serde_json::from_str(&value).map_err(storage)?);
        }
        for (id, value) in store
            .rows("SELECT collection_id,metadata FROM catalog_collections ORDER BY collection_id")?
        {
            let entry: CollectionEntry = serde_json::from_str(&value).map_err(storage)?;
            if entry.collection_id != id {
                return Err(storage("catalog collection identity mismatch"));
            }
            state.collections.insert(id, entry);
        }
        let mut statement = store.connection.prepare("SELECT collection_id,key_id,revision FROM catalog_grants ORDER BY collection_id,key_id").map_err(storage)?;
        for row in statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, u64>(2)?,
                ))
            })
            .map_err(storage)?
        {
            let (collection, key, revision) = row.map_err(storage)?;
            state
                .grants
                .entry(collection)
                .or_default()
                .insert(key, revision);
        }
        drop(statement);
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
                transaction.execute("INSERT INTO catalog_keys VALUES(?1,?2) ON CONFLICT(key_id) DO UPDATE SET metadata=excluded.metadata", params![id,json]).map_err(storage)?;
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
                transaction.execute("INSERT INTO catalog_collections VALUES(?1,?2) ON CONFLICT(collection_id) DO UPDATE SET metadata=excluded.metadata", params![id,json]).map_err(storage)?;
            }
        }
        for (collection, grants) in &next.grants {
            for (key, revision) in grants {
                if old.grants.get(collection).and_then(|g| g.get(key)) != Some(revision) {
                    transaction.execute("INSERT INTO catalog_grants VALUES(?1,?2,?3) ON CONFLICT(collection_id,key_id) DO UPDATE SET revision=excluded.revision", params![collection,key,revision]).map_err(storage)?;
                }
            }
        }
        for (collection, grants) in &old.grants {
            for key in grants.keys() {
                if !next
                    .grants
                    .get(collection)
                    .is_some_and(|g| g.contains_key(key))
                {
                    transaction
                        .execute(
                            "DELETE FROM catalog_grants WHERE collection_id=?1 AND key_id=?2",
                            params![collection, key],
                        )
                        .map_err(storage)?;
                }
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
                        "INSERT INTO catalog_operations VALUES(?1,?2,?3)",
                        params![
                            operation.request.operation_id.epoch,
                            operation.request.operation_id.nonce,
                            serde_json::to_string(operation).map_err(storage)?
                        ],
                    )
                    .map_err(storage)?;
            }
        }
        transaction.execute("INSERT INTO catalog_meta VALUES(1,1,?1) ON CONFLICT(singleton) DO UPDATE SET metadata=excluded.metadata", [serde_json::to_string(&Metadata::from_state(next)).map_err(storage)?]).map_err(storage)?;
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
        .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))
}
pub(super) fn create(root: &Path, state: &State) -> Result<Store, CatalogError> {
    validate_state(state)?;
    if root.exists() {
        return Err(CatalogError::Conflict("catalog root exists".into()));
    }
    let stage = pending(root)?;
    std::fs::create_dir(&stage).map_err(storage)?;
    drop(Store::create(&stage, state)?);
    crate::storage::partition::publish_bundle(&stage, root)
        .map_err(|e| CatalogError::OutcomeUnknown(e.to_string()))?;
    Store::open(root).map(|(store, _)| store)
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
                "prepare",
                CatalogCommand::PrepareCollection {
                    collection_id: "documents".into(),
                    owner_key: "master".into(),
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
        assert_eq!(
            catalog.authenticate(&"a".repeat(64)).await.unwrap(),
            "master"
        );
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
        let db = Connection::open(root.join(DATABASE)).unwrap();
        db.execute_batch("CREATE TRIGGER refuse_meta BEFORE UPDATE ON catalog_meta BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
        assert!(catalog.compact().await.is_err());
        assert_eq!(
            catalog.authenticate(&"a".repeat(64)).await.unwrap(),
            "master"
        );
        let change = request(
            "new-key",
            CatalogCommand::AddKey {
                key_id: "new".into(),
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
        assert_eq!(catalog.authenticate(&"b".repeat(64)).await.unwrap(), "new");
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
        let mut authority =
            SqliteAuthority::open(&root.join(DATABASE), Duration::from_secs(5)).unwrap();
        let original = authority.create_partition("p0", 7).unwrap();
        catalog
            .execute(request(
                "new-key",
                CatalogCommand::AddKey {
                    key_id: "new".into(),
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
        assert_eq!(catalog.authenticate(&"b".repeat(64)).await.unwrap(), "new");
    }
}
