use super::*;
use crate::catalog::tests::{config, request};

fn prepare() -> CatalogRequest {
    request(
        "prepare",
        CatalogCommand::PrepareCollection {
            tenant_id: "tenant".into(),
            collection_id: "documents".into(),
            config: config(),
        },
    )
}

#[tokio::test]
async fn sqlite_catalog_reopens_without_a_catalog_journal_and_excludes_second_owner() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = Catalog::create(&root).await.unwrap();
    let receipt = catalog.execute(prepare()).await.unwrap();
    assert!(root.join(DATABASE).exists());
    assert!(!root.join("generation-0000000000000000").exists());
    assert!(Catalog::open(&root).await.is_err());
    drop(catalog);
    let catalog = Catalog::open(&root).await.unwrap();
    assert_eq!(catalog.execute(prepare()).await.unwrap(), receipt);
    assert_eq!(catalog.collections().await.unwrap()[0].tenant_id, "tenant");
}

#[tokio::test]
async fn sqlite_catalog_rolls_back_rows_and_receipt_together_on_statement_failure() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = Catalog::create(&root).await.unwrap();
    let db = Connection::open(root.join(DATABASE)).unwrap();
    db.execute_batch("CREATE TRIGGER refuse_meta BEFORE UPDATE ON catalog_meta BEGIN SELECT RAISE(ABORT,'injected'); END;").unwrap();
    assert!(catalog.compact().await.is_err());
    assert!(catalog.collections().await.unwrap().is_empty());
    let change = prepare();
    assert!(catalog.execute(change.clone()).await.is_err());
    drop(catalog);
    db.execute_batch("DROP TRIGGER refuse_meta").unwrap();
    let catalog = Catalog::open(&root).await.unwrap();
    assert!(catalog.collections().await.unwrap().is_empty());
    assert_eq!(
        catalog
            .operation_for("tenant", &change.operation_id)
            .await
            .unwrap(),
        None
    );
    let receipt = catalog.execute(change.clone()).await.unwrap();
    assert_eq!(
        catalog
            .operation_for("tenant", &change.operation_id)
            .await
            .unwrap(),
        Some(receipt)
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
    drop(Catalog::create(&root).await.unwrap());
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

#[tokio::test]
async fn sqlite_catalog_transactions_leave_shared_record_authority_tables_intact() {
    use crate::storage::sqlite_authority::SqliteAuthority;
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = Catalog::create(&root).await.unwrap();
    let mut authority =
        SqliteAuthority::open(&root.join(DATABASE), Duration::from_secs(5)).unwrap();
    let original = authority.create_partition("p0", 7).unwrap();
    let receipt = catalog.execute(prepare()).await.unwrap();
    catalog.compact().await.unwrap();
    assert_eq!(authority.read_head("p0").unwrap(), Some(original.clone()));
    drop(authority);
    drop(catalog);
    let catalog = Catalog::open(&root).await.unwrap();
    let authority = SqliteAuthority::open(&root.join(DATABASE), Duration::from_secs(5)).unwrap();
    assert_eq!(authority.read_head("p0").unwrap(), Some(original));
    assert_eq!(catalog.execute(prepare()).await.unwrap(), receipt);
}

#[tokio::test]
async fn catalog_rejects_legacy_formats_without_conversion_or_replacement() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    drop(Catalog::create(&root).await.unwrap());
    let db = Connection::open(root.join(DATABASE)).unwrap();
    db.execute("UPDATE catalog_meta SET format=5", []).unwrap();
    assert!(Catalog::open(&root).await.is_err());
    assert!(Catalog::create(&root).await.is_err());
    assert_eq!(
        db.query_row::<u32, _, _>("SELECT format FROM catalog_meta", [], |r| r.get(0))
            .unwrap(),
        5
    );
    let native = parent.path().join("native");
    drop(PartitionJournal::create(&native, BUDGET, b"legacy-native-catalog").unwrap());
    assert!(Catalog::open(&native).await.is_err());
    assert!(Catalog::create(&native).await.is_err());
    assert!(!native.join(DATABASE).exists());
    let (_, entries) = PartitionJournal::open(&native, BUDGET).unwrap();
    assert_eq!(entries, vec![b"legacy-native-catalog".to_vec()]);
}

#[tokio::test]
async fn catalog_persists_only_collection_backup_and_receipt_metadata_without_credentials() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = Catalog::create(&root).await.unwrap();
    catalog.execute(prepare()).await.unwrap();
    catalog
        .execute(request(
            "activate",
            CatalogCommand::ActivateCollection {
                tenant_id: "tenant".into(),
                collection_id: "documents".into(),
            },
        ))
        .await
        .unwrap();
    let backup = "f".repeat(64);
    catalog
        .execute(request(
            "backup",
            CatalogCommand::RegisterBackup {
                tenant_id: "tenant".into(),
                collection_id: "documents".into(),
                backup_id: backup.clone(),
            },
        ))
        .await
        .unwrap();
    let db = Connection::open(root.join(DATABASE)).unwrap();
    let tables = db
        .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        tables,
        [
            "catalog_backups",
            "catalog_collections",
            "catalog_meta",
            "catalog_operations"
        ]
    );
    assert_eq!(
        db.query_row::<String, _, _>(
            "SELECT json_extract(metadata,'$.tenant_id') FROM catalog_backups WHERE backup_id=?1",
            [&backup],
            |r| r.get(0)
        )
        .unwrap(),
        "tenant"
    );
    let metadata: String = db
        .query_row("SELECT metadata FROM catalog_meta", [], |r| r.get(0))
        .unwrap();
    assert!(!metadata.contains("verifier"));
    assert!(!metadata.contains("keys"));
    assert_eq!(
        db.query_row::<u64, _, _>("SELECT count(*) FROM catalog_operations", [], |r| r.get(0))
            .unwrap(),
        3
    );
    drop(catalog);
    let catalog = Catalog::open(&root).await.unwrap();
    catalog.require_backup("tenant", &backup).await.unwrap();
    assert!(catalog.require_backup("other", &backup).await.is_err());
}
