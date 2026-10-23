use super::*;
use super::tests::{root, config, request, setup, prepare, activate};
use crate::record::{RecordPartition, RecordWrite};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

async fn other(catalog: &Catalog) {
    catalog.execute(request("prepare-other", CatalogCommand::PrepareCollection {
        collection_id: "other".into(), owner_key: "a".into(), config: config(),
    })).await.unwrap();
    catalog.execute(request("activate-other", CatalogCommand::ActivateCollection {
        collection_id: "other".into(),
    })).await.unwrap();
}
async fn writer_waits(gate: Arc<tokio::sync::RwLock<()>>) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while gate.clone().try_read_owned().is_ok() { tokio::task::yield_now().await; }
    }).await.unwrap();
}
#[tokio::test]
async fn catalog_execution_scoped_revoke_and_cancel_do_not_block_other_collection() {
    let path = root(); let catalog = setup(&path).await;
    prepare(&catalog).await; activate(&catalog).await; other(&catalog).await;
    let (_, old) = catalog.authorized("b".into(), "shared".into(), |_| async {}).await.unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let owner = catalog.clone();
    let caller = tokio::spawn(async move {
        owner.authorized("b".into(), "shared".into(), |_| async move {
            entered.send(()).unwrap(); wait.await.unwrap();
        }).await
    });
    ready.await.unwrap(); caller.abort(); let _ = caller.await;
    let gate = catalog.execution.collection("shared").unwrap();
    let owner = catalog.clone();
    let revoke = tokio::spawn(async move {
        owner.execute(request("revoke-scoped", CatalogCommand::RevokeGrant {
            collection_id: "shared".into(), key_id: "b".into(),
        })).await
    });
    writer_waits(gate).await;
    let ran = Arc::new(AtomicBool::new(false)); let flag = ran.clone();
    let owner = catalog.clone();
    let queued = tokio::spawn(async move {
        owner.authorized("b".into(), "shared".into(), |_| async move { flag.store(true, Ordering::SeqCst); }).await
    });
    // A's accepted work and pending revoke cannot retain the metadata mutex.
    assert_eq!(tokio::time::timeout(Duration::from_secs(5), catalog.authenticate(&"a".repeat(64))).await.unwrap().unwrap(), "a");
    tokio::time::timeout(Duration::from_secs(5), catalog.authorized("a".into(), "other".into(), |_| async {})).await.unwrap().unwrap();
    assert!(!revoke.is_finished());
    release.send(()).unwrap(); revoke.await.unwrap().unwrap();
    assert!(matches!(queued.await.unwrap(), Err(CatalogError::Unauthorized)));
    assert!(!ran.load(Ordering::SeqCst)); assert!(old.deliver(|| ()).is_err());
    drop(catalog); std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn catalog_execution_key_revoke_drains_every_collection() {
    let path = root(); let catalog = setup(&path).await;
    prepare(&catalog).await; activate(&catalog).await; other(&catalog).await;
    let mut releases = Vec::new(); let mut tasks = Vec::new(); let mut permits = Vec::new();
    for id in ["shared", "other"] {
        permits.push(catalog.authorized("a".into(), id.into(), |_| async {}).await.unwrap().1);
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        let owner = catalog.clone(); let id = id.to_owned();
        tasks.push(tokio::spawn(async move {
            owner.authorized("a".into(), id, |_| async move { entered.send(()).unwrap(); wait.await.unwrap(); }).await
        }));
        ready.await.unwrap(); releases.push(release);
    }
    let owner = catalog.clone();
    let revoke = tokio::spawn(async move { owner.execute(request("revoke-all", CatalogCommand::RevokeKey {key_id:"a".into()})).await });
    writer_waits(catalog.execution.global.clone()).await;
    releases.remove(0).send(()).unwrap(); tasks.remove(0).await.unwrap().unwrap();
    assert!(!revoke.is_finished());
    releases.remove(0).send(()).unwrap(); tasks.remove(0).await.unwrap().unwrap();
    revoke.await.unwrap().unwrap();
    for permit in permits { assert!(matches!(permit.deliver(|| ()), Err(CatalogError::Unauthorized))); }
    assert!(catalog.authorized("a".into(), "other".into(), |_| async {}).await.is_err());
    drop(catalog); std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn catalog_execution_panic_cannot_be_reopened_by_concurrent_publication() {
    let path = root(); let catalog = setup(&path).await; prepare(&catalog).await; activate(&catalog).await;
    let (_, permit) = catalog.authorized("a".into(), "shared".into(), |_| async {}).await.unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let owner = catalog.clone();
    let caller = tokio::spawn(async move {
        owner.authorized("a".into(), "shared".into(), |_| async move {
            entered.send(()).unwrap(); wait.await.unwrap(); panic!("failure while metadata publication is in progress");
        }).await
    });
    ready.await.unwrap();
    let mut inner = tokio::time::timeout(Duration::from_secs(5), catalog.inner.lock()).await.unwrap();
    inner.fence(); // A different collection's publication/compaction has closed output.
    release.send(()).unwrap();
    assert!(matches!(caller.await.unwrap(), Err(CatalogError::OutcomeUnknown(_))));
    assert!(matches!(inner.publish_output(), Err(CatalogError::RecoveryRequired)));
    assert!(permit.deliver(|| ()).is_err()); drop(inner);
    assert_eq!(catalog.authenticate(&"a".repeat(64)).await.unwrap_err(), CatalogError::RecoveryRequired);
    drop(catalog); std::fs::remove_dir_all(path).unwrap();
}

fn write(nonce: &str) -> RecordWrite {
    serde_json::from_value(json!({"operation_id":OperationId::new(nonce.into()), "record_id":nonce,
        "routing_key":"r", "expected":"Absent", "document":{"body":"alpha"}})).unwrap()
}
#[tokio::test]
async fn catalog_execution_service_backup_freezes_the_complete_accepted_cut() {
    let path = root(); let catalog = setup(&path).await;
    let prepared = prepare(&catalog).await;
    let directory = prepared.directory.unwrap();
    let p = RecordPartition::create(&path.join("catalog").join(&directory), config()).await.unwrap();
    activate(&catalog).await;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    let owner = catalog.clone(); let data = p.clone();
    let first = tokio::spawn(async move {
        owner.authorized("a".into(), "shared".into(), |_| async move {
            entered.send(()).unwrap(); wait.await.unwrap(); data.write(write("first")).await
        }).await
    });
    ready.await.unwrap();
    let owner = catalog.clone(); let source = path.join("catalog"); let archive = path.join("archive");
    let target = archive.clone(); let records = BTreeMap::from([("shared".into(), p.clone())]);
    let backup = tokio::spawn(async move { owner.backup_service(source, target, "fixed-cut".into(), records).await });
    writer_waits(catalog.execution.global.clone()).await;
    let owner = catalog.clone(); let data = p.clone();
    let late = tokio::spawn(async move {
        owner.authorized("a".into(), "shared".into(), |_| async move { data.write(write("late")).await }).await
    });
    assert!(!backup.is_finished()); release.send(()).unwrap();
    first.await.unwrap().unwrap().0.unwrap();
    let receipt = backup.await.unwrap().unwrap();
    assert_eq!(receipt.record_seqs["shared"], 1);
    late.await.unwrap().unwrap().0.unwrap();
    assert_eq!(p.recovery_identity().await.unwrap().seq, 2);
    let archived = RecordPartition::open(&archive.join(directory), config().max_journal_bytes).await.unwrap();
    assert!(archived.get("first").await.unwrap().is_some()); assert!(archived.get("late").await.unwrap().is_none());
    drop(archived); drop(p); drop(catalog); std::fs::remove_dir_all(path).unwrap();
}
