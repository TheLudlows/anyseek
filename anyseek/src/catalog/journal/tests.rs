use super::*;
use crate::catalog::tests::{config, request};

#[tokio::test]
async fn catalog_reopens_without_sqlite_and_excludes_second_owner() {
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
    let change = request(
        "prepare",
        CatalogCommand::PrepareCollection {
            collection_id: "documents".into(),
            tenant_id: "tenant".into(),
            config: config(),
        },
    );
    let receipt = catalog.execute(change.clone()).await.unwrap();
    assert!(!root.join("metadata.sqlite").exists());
    assert!(root.join("generation-0000000000000000/wal").exists());
    assert!(Catalog::open(&root).await.is_err());
    drop(catalog);
    let catalog = Catalog::open(&root).await.unwrap();
    assert_eq!(catalog.authenticate(&"a".repeat(64)).await.unwrap(), MASTER);
    assert_eq!(
        catalog.collections().await.unwrap()[0].collection_id,
        "documents"
    );
    assert_eq!(catalog.execute(change).await.unwrap(), receipt);
    assert!(!root.join("metadata.sqlite").exists());
}

#[tokio::test]
async fn uncertain_checkpoint_fences_catalog_and_preserves_keys_and_receipts() {
    for stage in ["before", "after"] {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("service");
        let catalog = Catalog::create_with_key(&root, "master".into(), "a".repeat(64))
            .await
            .unwrap();
        let change = request(
            "tenant",
            CatalogCommand::AddTenant {
                tenant_id: "tenant".into(),
            },
        );
        let receipt = catalog.execute(change.clone()).await.unwrap();
        let fault = root.join(format!(".test-checkpoint-{stage}-certificate"));
        std::fs::write(&fault, b"fault").unwrap();
        assert!(matches!(
            catalog.compact().await,
            Err(CatalogError::OutcomeUnknown(_))
        ));
        assert_eq!(
            catalog.authenticate(&"a".repeat(64)).await.unwrap_err(),
            CatalogError::RecoveryRequired
        );
        drop(catalog);
        std::fs::remove_file(fault).unwrap();
        let catalog = Catalog::open(&root).await.unwrap();
        assert_eq!(catalog.authenticate(&"a".repeat(64)).await.unwrap(), MASTER);
        assert_eq!(catalog.execute(change).await.unwrap(), receipt);
    }
}

#[tokio::test]
async fn catalog_resumes_certified_private_initialization() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let stage = pending(&root).unwrap();
    drop(Store::create(&stage, &State::default()).unwrap());
    Catalog::resume_initialization(&root).await.unwrap();
    assert!(!stage.exists());
    assert!(!root.join("metadata.sqlite").exists());
    assert!(Catalog::open(&root).await.is_ok());
}

#[tokio::test]
async fn missing_or_corrupt_catalog_journal_never_becomes_empty_catalog() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    drop(
        Catalog::create_with_key(&root, "master".into(), "a".repeat(64))
            .await
            .unwrap(),
    );
    let wal = root.join("generation-0000000000000000/wal");
    std::fs::remove_file(&wal).unwrap();
    assert!(Catalog::open(&root).await.is_err());
    assert!(!wal.exists());
    std::fs::write(&wal, b"invalid journal").unwrap();
    assert!(Catalog::open(&root).await.is_err());
    assert_eq!(std::fs::read(wal).unwrap(), b"invalid journal");
    assert!(!root.join("metadata.sqlite").exists());
}

#[tokio::test]
async fn legacy_sqlite_catalog_is_rejected_without_conversion() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("owner"), b"").unwrap();
    let db = root.join("metadata.sqlite");
    std::fs::write(&db, b"legacy catalog").unwrap();
    assert!(Catalog::open(&root).await.is_err());
    assert!(Catalog::create(&root).await.is_err());
    assert_eq!(std::fs::read(db).unwrap(), b"legacy catalog");
    assert!(!root.join("generation-0000000000000000").exists());
}

#[tokio::test]
async fn catalog_commands_do_not_use_or_lock_s3_authority_database() {
    use crate::storage::sqlite_authority::SqliteAuthority;
    use std::time::Duration;
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = Catalog::create_with_key(&root, "master".into(), "a".repeat(64))
        .await
        .unwrap();
    let db = root.join("metadata.sqlite");
    let mut authority = SqliteAuthority::open(&db, Duration::from_secs(5)).unwrap();
    let head = authority.create_partition("p0", 7).unwrap();
    let connection = rusqlite::Connection::open(&db).unwrap();
    connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let change = request(
        "tenant",
        CatalogCommand::AddTenant {
            tenant_id: "tenant".into(),
        },
    );
    let receipt = catalog.execute(change.clone()).await.unwrap();
    catalog.compact().await.unwrap();
    assert_eq!(
        connection
            .query_row::<u64, _, _>(
                "SELECT count(*) FROM sqlite_master WHERE name LIKE 'catalog_%'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        0
    );
    connection.execute_batch("ROLLBACK").unwrap();
    assert_eq!(authority.read_head("p0").unwrap(), Some(head));
    drop(catalog);
    let catalog = Catalog::open(&root).await.unwrap();
    assert_eq!(catalog.execute(change).await.unwrap(), receipt);
}

#[tokio::test]
async fn catalog_tail_rotates_into_native_checkpoint_without_losing_receipts() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = Catalog::create(&root).await.unwrap();
    let mut requests = Vec::new();
    for number in 0..100 {
        let change = request(
            &format!("op{number:04}"),
            CatalogCommand::AddTenant {
                tenant_id: format!("tenant{number:04}"),
            },
        );
        let receipt = catalog.execute(change.clone()).await.unwrap();
        requests.push((change, receipt));
    }
    {
        let mut inner = catalog.inner.lock().await;
        let budget = inner.store.journal.physical_bytes() + 1;
        inner.store.journal.restrict_budget(budget).unwrap();
    }
    catalog
        .execute(request(
            "rotate",
            CatalogCommand::AddTenant {
                tenant_id: "last".into(),
            },
        ))
        .await
        .unwrap();
    assert!(root.join("generation-0000000000000001").exists());
    assert!(!root.join("generation-0000000000000000").exists());
    drop(catalog);
    let catalog = Catalog::open(&root).await.unwrap();
    for (change, receipt) in requests {
        assert_eq!(catalog.execute(change).await.unwrap(), receipt);
    }
    assert!(!root.join("metadata.sqlite").exists());
}
