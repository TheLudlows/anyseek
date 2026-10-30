use super::*;
use serde_json::json;
use std::time::Duration;

pub(super) fn request(nonce: &str, command: CatalogCommand) -> CatalogRequest {
    CatalogRequest {
        operation_id: OperationId::new(nonce.into()),
        command,
    }
}
pub(super) fn config() -> RecordConfig {
    serde_json::from_value(json!({"index":{"id":0,"name":"shared","lexical_similarity":"Bm25f","tokenizer":"TantivySimpleV1","stop_words":"None","frequent_words":"None","ngram_indexing":0,"access_type":"Mmap","clustering":"None","inference":"None"},"schema":[{"field":"body","field_type":"Text","store":false,"index_lexical":true}],"embedding_space_id":null,"max_records":32,"max_record_bytes":4096,"max_operations":100,"max_journal_bytes":1048576,"max_candidates":100})).unwrap()
}
async fn setup(path: &Path) -> Catalog {
    let catalog = Catalog::create_with_key(path, "master".into(), "a".repeat(64))
        .await
        .unwrap();
    for (tenant, key, digest) in [("alpha", "a", "b"), ("beta", "b", "c")] {
        catalog
            .execute(request(
                &format!("tenant-{tenant}"),
                CatalogCommand::AddTenant {
                    tenant_id: tenant.into(),
                },
            ))
            .await
            .unwrap();
        catalog
            .execute(request(
                &format!("key-{key}"),
                CatalogCommand::AddKey {
                    key_id: key.into(),
                    tenant_id: tenant.into(),
                    verifier: digest.repeat(64),
                },
            ))
            .await
            .unwrap();
    }
    catalog
}
async fn create(catalog: &Catalog, tenant: &str, id: &str) -> CatalogReceipt {
    let receipt = catalog
        .execute(request(
            &format!("create-{id}"),
            CatalogCommand::PrepareCollection {
                tenant_id: tenant.into(),
                collection_id: id.into(),
                config: config(),
            },
        ))
        .await
        .unwrap();
    catalog
        .execute(request(
            &format!("activate-{id}"),
            CatalogCommand::ActivateCollection {
                tenant_id: tenant.into(),
                collection_id: id.into(),
            },
        ))
        .await
        .unwrap();
    receipt
}
#[test]
fn tenant_and_collection_names_reject_invalid_path_components() {
    for value in [
        "a",
        "documents",
        "knowledge-base",
        "user_memory",
        &"a".repeat(63),
    ] {
        validate_name(value).unwrap();
    }
    for value in [
        "",
        "A",
        "0name",
        "../x",
        "a/b",
        "a\\b",
        "a.b",
        "a b",
        "中文",
        &"a".repeat(64),
    ] {
        assert!(validate_name(value).is_err(), "{value}");
    }
}
#[tokio::test]
async fn tenant_keys_same_name_collections_and_scoped_retries_survive_reopen() {
    let parent = tempfile::tempdir().unwrap();
    let path = parent.path().join("service");
    let catalog = setup(&path).await;
    let alpha = create(&catalog, "alpha", "documents").await;
    let beta = create(&catalog, "beta", "documents").await;
    assert_ne!(alpha.seq, beta.seq);
    assert_eq!(catalog.authenticate(&"a".repeat(64)).await.unwrap(), MASTER);
    assert_eq!(
        catalog.authenticate(&"b".repeat(64)).await.unwrap(),
        "alpha"
    );
    assert_eq!(catalog.authenticate(&"c".repeat(64)).await.unwrap(), "beta");
    assert_eq!(catalog.listing("alpha").await.unwrap().len(), 1);
    assert_eq!(
        catalog
            .authorized("alpha".into(), "documents".into(), |e| async move {
                e.tenant_id
            })
            .await
            .unwrap(),
        "alpha"
    );
    assert!(
        catalog
            .authorized("alpha".into(), "missing".into(), |_| async {})
            .await
            .is_err()
    );
    assert!(catalog.listing(MASTER).await.is_err());
    let retry = request(
        "create-documents",
        CatalogCommand::PrepareCollection {
            tenant_id: "alpha".into(),
            collection_id: "documents".into(),
            config: config(),
        },
    );
    assert_eq!(catalog.execute(retry.clone()).await.unwrap(), alpha);
    let mut different = retry.clone();
    different.command = CatalogCommand::DeleteCollection {
        tenant_id: "alpha".into(),
        collection_id: "documents".into(),
    };
    assert!(matches!(
        catalog.execute(different).await,
        Err(CatalogError::Conflict(_))
    ));
    drop(catalog);
    let catalog = Catalog::open(&path).await.unwrap();
    assert_eq!(catalog.execute(retry).await.unwrap(), alpha);
    assert_eq!(catalog.listing("beta").await.unwrap()[0].tenant_id, "beta");
    let db = rusqlite::Connection::open(path.join("metadata.sqlite")).unwrap();
    assert_eq!(
        db.query_row::<u64, _, _>(
            "SELECT count(*) FROM catalog_collections WHERE collection_id='documents'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query_row::<u64, _, _>(
            "SELECT count(*) FROM sqlite_master WHERE name='catalog_grants'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
}
#[tokio::test]
async fn revoke_rejects_new_authentication_without_waiting_for_owned_work_or_deleting_data() {
    let parent = tempfile::tempdir().unwrap();
    let path = parent.path().join("service");
    let catalog = setup(&path).await;
    create(&catalog, "alpha", "documents").await;
    catalog
        .execute(request(
            "second-key",
            CatalogCommand::AddKey {
                key_id: "second".into(),
                tenant_id: "alpha".into(),
                verifier: "d".repeat(64),
            },
        ))
        .await
        .unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let owner = catalog.clone();
    let caller = tokio::spawn(async move {
        owner
            .authorized("alpha".into(), "documents".into(), |_| async move {
                entered.send(()).unwrap();
                wait.await.unwrap();
                42
            })
            .await
    });
    ready.await.unwrap();
    caller.abort();
    let _ = caller.await;
    tokio::time::timeout(
        Duration::from_secs(2),
        catalog.execute(request(
            "revoke",
            CatalogCommand::RevokeKey { key_id: "a".into() },
        )),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(catalog.authenticate(&"b".repeat(64)).await.is_err());
    assert_eq!(
        catalog.authenticate(&"d".repeat(64)).await.unwrap(),
        "alpha"
    );
    assert_eq!(catalog.listing("alpha").await.unwrap().len(), 1);
    release.send(()).unwrap();
    // Lifecycle work remains owned after the HTTP waiter is cancelled.
    let _barrier = catalog.execution.freeze().await;
}
#[tokio::test]
async fn tenant_backup_ownership_and_restore_intents_survive_restart() {
    let parent = tempfile::tempdir().unwrap();
    let path = parent.path().join("service");
    let catalog = setup(&path).await;
    create(&catalog, "alpha", "documents").await;
    let id = "f".repeat(64);
    catalog
        .execute(request(
            "backup",
            CatalogCommand::RegisterBackup {
                tenant_id: "alpha".into(),
                collection_id: "documents".into(),
                backup_id: id.clone(),
            },
        ))
        .await
        .unwrap();
    catalog.require_backup("alpha", &id).await.unwrap();
    assert!(catalog.require_backup("beta", &id).await.is_err());
    let restore = |tenant: &str| {
        request(
            "restore",
            CatalogCommand::PrepareRestore {
                tenant_id: tenant.into(),
                collection_id: "restored".into(),
                config: config(),
                backup_id: id.clone(),
            },
        )
    };
    assert!(catalog.execute(restore("beta")).await.is_err());
    catalog.execute(restore("alpha")).await.unwrap();
    assert!(matches!(
        catalog
            .execute(request(
                "retire",
                CatalogCommand::RetireBackup {
                    backup_id: id.clone()
                }
            ))
            .await,
        Err(CatalogError::Conflict(_))
    ));
    drop(catalog);
    let catalog = Catalog::open(&path).await.unwrap();
    catalog.require_backup("alpha", &id).await.unwrap();
    assert!(catalog.require_backup("beta", &id).await.is_err());
}
#[tokio::test]
async fn invalid_configuration_is_rejected_without_poisoning_catalog() {
    let parent = tempfile::tempdir().unwrap();
    let path = parent.path().join("service");
    let catalog = setup(&path).await;
    for boost in [f32::NAN, f32::INFINITY, -1.0] {
        let mut config = config();
        config.schema[0].boost = boost;
        assert!(
            catalog
                .execute(request(
                    "invalid",
                    CatalogCommand::PrepareCollection {
                        tenant_id: "alpha".into(),
                        collection_id: "invalid".into(),
                        config
                    }
                ))
                .await
                .is_err()
        );
    }
    assert_eq!(
        catalog.authenticate(&"b".repeat(64)).await.unwrap(),
        "alpha"
    );
    drop(catalog);
    assert!(
        Catalog::open(&path)
            .await
            .unwrap()
            .collections()
            .await
            .unwrap()
            .is_empty()
    );
}
#[tokio::test]
async fn complete_service_restore_retains_tenants_but_disables_historical_keys() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = setup(&root).await;
    create(&catalog, "alpha", "documents").await;
    let entry = catalog.listing("alpha").await.unwrap().pop().unwrap();
    entry.prepare_parent(&root).unwrap();
    let partition = crate::record::RecordPartition::create(&root.join(entry.directory()), config())
        .await
        .unwrap();
    let archive = parent.path().join("archive");
    catalog
        .backup_service(
            root.clone(),
            archive.clone(),
            "backup".into(),
            BTreeMap::from([(entry.scope(), partition.clone())]),
        )
        .await
        .unwrap();
    let restored = parent.path().join("restored");
    Catalog::restore_service(archive, restored.clone(), "restore".into(), "e".repeat(64))
        .await
        .unwrap();
    let catalog = Catalog::open(&restored).await.unwrap();
    catalog.require_service_completion(&restored).await.unwrap();
    assert_eq!(catalog.tenants().await.unwrap(), vec!["alpha", "beta"]);
    assert!(catalog.authenticate(&"b".repeat(64)).await.is_err());
    assert!(catalog.authenticate(&"c".repeat(64)).await.is_err());
    assert_eq!(catalog.authenticate(&"e".repeat(64)).await.unwrap(), MASTER);
    catalog
        .execute(request(
            "fresh-key",
            CatalogCommand::AddKey {
                tenant_id: "alpha".into(),
                key_id: "fresh".into(),
                verifier: "f".repeat(64),
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        catalog.authenticate(&"f".repeat(64)).await.unwrap(),
        "alpha"
    );
    assert_eq!(catalog.listing("alpha").await.unwrap().len(), 1);
}
#[tokio::test]
async fn receipt_pruning_retains_tenant_ownership_and_rejects_expired_retry() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = setup(&root).await;
    create(&catalog, "alpha", "documents").await;
    let id = OperationId::new("create-documents".into());
    catalog.inner.lock().await.compact(id.epoch + 2).unwrap();
    drop(catalog);
    let catalog = Catalog::open(&root).await.unwrap();
    assert_eq!(
        catalog.operation_for("alpha", &id).await.unwrap_err(),
        CatalogError::OperationExpired
    );
    assert_eq!(catalog.listing("alpha").await.unwrap().len(), 1);
}
#[tokio::test]
async fn panicking_owned_collection_work_fences_new_operations() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = setup(&root).await;
    create(&catalog, "alpha", "documents").await;
    let outcome = catalog
        .authorized("alpha".into(), "documents".into(), |_| async {
            panic!("injected");
        })
        .await;
    assert!(matches!(outcome, Err(CatalogError::OutcomeUnknown(_))));
    assert_eq!(
        catalog.authenticate(&"b".repeat(64)).await.unwrap_err(),
        CatalogError::RecoveryRequired
    );
}
#[tokio::test]
#[ignore = "subprocess worker invoked by sqlite_process_exit_preserves_key_and_receipt_atomicity"]
async fn sqlite_crash_worker() {
    let root = std::path::PathBuf::from(std::env::var_os("ANYSEEK_CATALOG_CHILD_PATH").unwrap());
    let catalog = Catalog::open(&root).await.unwrap();
    unsafe {
        std::env::set_var(
            "ANYSEEK_CATALOG_CRASH",
            std::env::var("ANYSEEK_CATALOG_CHILD_STAGE").unwrap(),
        );
    }
    let _ = catalog
        .execute(request(
            "crash-revoke",
            CatalogCommand::RevokeKey { key_id: "a".into() },
        ))
        .await;
    panic!("boundary not reached");
}
#[tokio::test]
async fn sqlite_process_exit_preserves_key_and_receipt_atomicity() {
    for stage in ["before_commit", "after_commit"] {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("service");
        drop(setup(&root).await);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "catalog::tests::sqlite_crash_worker",
                "--ignored",
            ])
            .env("ANYSEEK_CATALOG_CHILD_PATH", &root)
            .env("ANYSEEK_CATALOG_CHILD_STAGE", stage)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(73),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let catalog = Catalog::open(&root).await.unwrap();
        let receipt = catalog
            .operation(&OperationId::new("crash-revoke".into()))
            .await
            .unwrap();
        assert_eq!(
            catalog.authenticate(&"b".repeat(64)).await.is_ok(),
            receipt.is_none()
        );
        if stage == "after_commit" {
            assert!(receipt.is_some());
        }
    }
}

#[tokio::test]
async fn complete_service_snapshot_drains_owned_lifecycle_work_and_excludes_queued_writes() {
    use crate::record::{RecordPartition, RecordWrite};
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = setup(&root).await;
    let mut records = BTreeMap::new();
    for tenant in ["alpha", "beta"] {
        let receipt = create(&catalog, tenant, "documents").await;
        let entry = catalog.listing(tenant).await.unwrap().remove(0);
        entry.prepare_parent(&root).unwrap();
        let partition = RecordPartition::create(&root.join(entry.directory()), config())
            .await
            .unwrap();
        records.insert(
            format!("{tenant}/documents/{:016x}", receipt.seq),
            partition,
        );
    }
    let write = |nonce: &str| {
        serde_json::from_value::<RecordWrite>(json!({"operation_id":OperationId::new(nonce.into()),"record_id":"r","routing_key":"r","expected":"Absent","document":{"body":"alpha"}})).unwrap()
    };
    let alpha = catalog.listing("alpha").await.unwrap().remove(0);
    let beta = catalog.listing("beta").await.unwrap().remove(0);
    let first = records[&alpha.scope()].clone();
    let command = write("owned");
    let second = records[&beta.scope()].clone();
    let queued_command = write("queued");
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let owner = catalog.clone();
    let caller = tokio::spawn(async move {
        owner
            .authorized("alpha".into(), "documents".into(), |_| async move {
                entered.send(()).unwrap();
                wait.await.unwrap();
                first.write(command).await
            })
            .await
    });
    ready.await.unwrap();
    caller.abort();
    let _ = caller.await;
    let owner = catalog.clone();
    let archive = parent.path().join("archive");
    let snapshot = tokio::spawn(async move {
        owner
            .backup_service(root, archive, "cut".into(), records)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while catalog.execution.global.clone().try_read_owned().is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let owner = catalog.clone();
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let queued = tokio::spawn(async move {
        owner
            .authorized("beta".into(), "documents".into(), |_| async move {
                flag.store(true, Ordering::Release);
                second.write(queued_command).await
            })
            .await
    });
    assert!(!snapshot.is_finished());
    assert!(!ran.load(Ordering::Acquire));
    release.send(()).unwrap();
    let receipt = snapshot.await.unwrap().unwrap();
    assert_eq!(receipt.record_seqs[&alpha.scope()], 1);
    assert_eq!(receipt.record_seqs[&beta.scope()], 0);
    queued.await.unwrap().unwrap().unwrap();
    assert!(ran.load(Ordering::Acquire));
}

#[tokio::test]
async fn collection_recreation_keeps_generations_receipts_and_backups_separate() {
    use crate::record::{RecordPartition, RecordWrite};
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("service");
    let catalog = setup(&root).await;
    let first = create(&catalog, "alpha", "documents").await;
    let old = catalog.listing("alpha").await.unwrap().remove(0);
    old.prepare_parent(&root).unwrap();
    let partition = RecordPartition::create(&root.join(old.directory()), config())
        .await
        .unwrap();
    let write: RecordWrite = serde_json::from_value(json!({
        "operation_id":OperationId::new("same-write".into()), "record_id":"r", "routing_key":"r",
        "expected":"Absent", "document":{"body":"old"}
    }))
    .unwrap();
    partition.write(write.clone()).await.unwrap();
    let backup_id = "e".repeat(64);
    catalog
        .execute(request(
            "old-backup",
            CatalogCommand::RegisterBackup {
                tenant_id: "alpha".into(),
                collection_id: "documents".into(),
                backup_id: backup_id.clone(),
            },
        ))
        .await
        .unwrap();
    let delete = request(
        "delete",
        CatalogCommand::DeleteCollection {
            tenant_id: "alpha".into(),
            collection_id: "documents".into(),
        },
    );
    catalog.execute(delete.clone()).await.unwrap();
    let second = catalog
        .execute(request(
            "recreate",
            CatalogCommand::PrepareCollection {
                tenant_id: "alpha".into(),
                collection_id: "documents".into(),
                config: config(),
            },
        ))
        .await
        .unwrap();
    catalog
        .execute(request(
            "reactivate",
            CatalogCommand::ActivateCollection {
                tenant_id: "alpha".into(),
                collection_id: "documents".into(),
            },
        ))
        .await
        .unwrap();
    let new = catalog.listing("alpha").await.unwrap().remove(0);
    assert_ne!(old.generation, new.generation);
    assert_ne!(old.directory(), new.directory());
    assert_eq!(new.generation, second.seq);
    // A captured old handle cannot enter work under the replacement's guard.
    assert!(matches!(
        catalog
            .authorized_generation(
                "alpha".into(),
                "documents".into(),
                old.generation,
                |_| async { panic!("stale work ran") }
            )
            .await,
        Err(CatalogError::Unauthorized)
    ));
    let current = RecordPartition::create(&root.join(&new.directory()), config())
        .await
        .unwrap();
    assert!(
        current
            .operation(&write.operation_id)
            .await
            .unwrap()
            .is_none()
    );
    let mut fresh = write.clone();
    fresh.document = Some(serde_json::from_value(json!({"body":"new"})).unwrap());
    current.write(fresh).await.unwrap();
    assert_eq!(
        partition.get("r").await.unwrap().unwrap().document.unwrap()["body"],
        "old"
    );
    assert_eq!(
        current.get("r").await.unwrap().unwrap().document.unwrap()["body"],
        "new"
    );
    assert!(
        catalog
            .execute(request(
                "reuse-backup",
                CatalogCommand::RegisterBackup {
                    tenant_id: "alpha".into(),
                    collection_id: "documents".into(),
                    backup_id: backup_id.clone(),
                }
            ))
            .await
            .is_err()
    );
    // Old control retries return their original outcome without mutating the new lifecycle.
    assert_eq!(
        catalog
            .execute(request(
                "create-documents",
                CatalogCommand::PrepareCollection {
                    tenant_id: "alpha".into(),
                    collection_id: "documents".into(),
                    config: config(),
                }
            ))
            .await
            .unwrap(),
        first
    );
    catalog.execute(delete).await.unwrap();
    assert_eq!(
        catalog.listing("alpha").await.unwrap()[0].generation,
        new.generation
    );
    catalog.require_backup("alpha", &backup_id).await.unwrap();
    drop(catalog);
    let catalog = Catalog::open(&root).await.unwrap();
    assert_eq!(catalog.collections().await.unwrap().len(), 2);
    assert_eq!(
        catalog.listing("alpha").await.unwrap()[0].generation,
        new.generation
    );
    // Complete-service snapshots retain both physical lifecycles and certify distinct cuts.
    let archive = parent.path().join("archive");
    let receipt = catalog
        .backup_service(
            root,
            archive.clone(),
            "generations".into(),
            BTreeMap::from([(old.scope(), partition), (new.scope(), current)]),
        )
        .await
        .unwrap();
    assert_eq!(receipt.record_seqs.len(), 2);
}
