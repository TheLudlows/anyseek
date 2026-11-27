use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::oneshot;
use tower::ServiceExt;

const WAIT: Duration = Duration::from_secs(15);

#[derive(Default)]
pub(in crate::server::record_service) struct Counter(AtomicUsize);

impl Counter {
    pub(in crate::server::record_service) fn increment(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    async fn until(&self, value: usize) {
        tokio::time::timeout(WAIT, async {
            while self.get() < value {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("test milestone did not occur");
    }
}

type Pause = (oneshot::Sender<()>, oneshot::Receiver<()>);

#[derive(Default)]
pub(in crate::server::record_service) struct PauseNext(std::sync::Mutex<Option<Pause>>);

impl PauseNext {
    pub(in crate::server::record_service) fn pause_next(
        &self,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (arrived, reached) = oneshot::channel();
        let (release, resumed) = oneshot::channel();
        assert!(self.0.lock().unwrap().replace((arrived, resumed)).is_none());
        (reached, release)
    }

    pub(in crate::server::record_service) async fn wait(&self) {
        let pause = self.0.lock().unwrap().take();
        if let Some((arrived, resumed)) = pause {
            let _ = arrived.send(());
            let _ = resumed.await;
        }
    }
}

#[derive(Default)]
pub(in crate::server::record_service) struct Hooks {
    pub(in crate::server::record_service) preflight: PauseNext,
    pub(in crate::server::record_service) before_install: PauseNext,
    pub(in crate::server::record_service) backup_snapshot: PauseNext,
    pub(in crate::server::record_service) backup_waiters: Counter,
    pub(in crate::server::record_service) attempts: Counter,
    pub(in crate::server::record_service) installs: Counter,
    pub(in crate::server::record_service) waiters: Counter,
}

async fn reached(milestone: oneshot::Receiver<()>) {
    tokio::time::timeout(WAIT, milestone)
        .await
        .unwrap()
        .unwrap();
}

async fn call(
    app: Router,
    headers: HeaderMap,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut request = axum::http::Request::builder().method(method).uri(uri);
    request.headers_mut().unwrap().extend(headers);
    let response = app
        .oneshot(
            request
                .body(Body::from(
                    body.map(|body| body.to_string()).unwrap_or_default(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let raw = axum::body::to_bytes(response.into_body(), MAX_RESPONSE)
        .await
        .unwrap();
    (status, serde_json::from_slice(&raw).unwrap())
}

async fn reopen(root: &FsPath) -> Arc<Service> {
    Service::open_http(root, 200.try_into().unwrap(), "anyseek-unit-test-secret")
        .await
        .unwrap()
}

fn create(id: &str) -> Value {
    json!({
        "operation_id": OperationId::new(format!("create-{id}")),
        "collection_id": id,
        "config": {"index_name": "test", "schema": [{"field": "body", "field_type": "Text"}]}
    })
}

async fn write_fixture(app: &Router, headers: &HeaderMap) {
    let write = json!({
        "operation_id": OperationId::new("write-fixture".into()),
        "record_id": "fixture",
        "expected": "Absent",
        "document": {"body": "cold text"}
    });
    let (status, body) = call(
        app.clone(),
        headers.clone(),
        "POST",
        "/api/v1/index/c/batch",
        Some(json!({"writes": [write]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn http_cold_start_shares_one_open_and_lists_live_statistics() {
    let (initial, root, headers) = super::super::tests::setup().await;
    let app = router(initial.clone());
    write_fixture(&app, &headers).await;
    assert_eq!(
        call(
            app.clone(),
            headers.clone(),
            "POST",
            "/api/v1/index",
            Some(create("other"))
        )
        .await
        .0,
        StatusCode::OK
    );
    drop(app);
    initial.shutdown().wait().await.unwrap();

    let service = reopen(&root).await;
    let hooks = &service.cold.test;
    let app = router(service.clone());
    assert_eq!(
        call(app.clone(), HeaderMap::new(), "GET", "/api/v1/live", None)
            .await
            .0,
        StatusCode::OK
    );
    let mut invalid = headers.clone();
    invalid.insert("apikey", STANDARD.encode([0; 32]).parse().unwrap());
    assert_eq!(
        call(app.clone(), invalid, "GET", "/api/v1/index/c", None)
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    assert!(service.records.read().await.is_empty());
    assert_eq!(hooks.attempts.get(), 0);

    let (opened, install) = hooks.before_install.pause_next();
    let leader = tokio::spawn(call(
        app.clone(),
        headers.clone(),
        "GET",
        "/api/v1/index/c/doc/fixture",
        None,
    ));
    reached(opened).await;
    let peer = tokio::spawn(call(
        app.clone(),
        headers.clone(),
        "GET",
        "/api/v1/index/c/doc/fixture",
        None,
    ));
    hooks.waiters.until(2).await;
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
    assert_eq!(hooks.attempts.get(), 1);
    assert_eq!(hooks.installs.get(), 0);
    install.send(()).unwrap();
    let (status, body) = tokio::time::timeout(WAIT, peer).await.unwrap().unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["document"]["body"], "cold text");
    assert_eq!(hooks.installs.get(), 1);

    let (status, listing) = call(app.clone(), headers, "GET", "/api/v1/index", None).await;
    assert_eq!(status, StatusCode::OK, "{listing}");
    assert_eq!(listing.as_array().unwrap().len(), 2);
    let listed = listing
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "c")
        .unwrap();
    assert_eq!(listed["statistics"]["active_records"], 1);
    assert_eq!(
        hooks.attempts.get(),
        2,
        "only the other collection was cold during listing"
    );
    assert_eq!(hooks.installs.get(), 2);
    assert_eq!(service.records.read().await.len(), 2);
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn http_cold_failure_retries_without_installing_a_deleted_generation() {
    let (initial, root, headers) = super::super::tests::setup().await;
    let entry = initial
        .catalog
        .collection_entry("tenant", "c")
        .await
        .unwrap();
    initial.shutdown().wait().await.unwrap();
    let service = reopen(&root).await;
    let app = router(service.clone());
    let original = root.join(entry.directory());
    let aside = original.with_extension("unavailable");
    std::fs::rename(&original, &aside).unwrap();
    let (status, failed) = call(app.clone(), headers.clone(), "GET", "/api/v1/index/c", None).await;
    assert_eq!(
        (status, &failed["code"]),
        (StatusCode::SERVICE_UNAVAILABLE, &json!("storage_error"))
    );
    assert!(service.records.read().await.is_empty());
    std::fs::rename(aside, original).unwrap();
    assert_eq!(
        call(app.clone(), headers.clone(), "GET", "/api/v1/index/c", None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(service.cold.test.attempts.get(), 2);
    assert_eq!(service.cold.test.installs.get(), 1);
    drop(app);
    service.shutdown().wait().await.unwrap();

    let service = reopen(&root).await;
    let app = router(service.clone());
    let hooks = &service.cold.test;
    let (opened, install) = hooks.before_install.pause_next();
    let old = tokio::spawn(call(
        app.clone(),
        headers.clone(),
        "GET",
        "/api/v1/index/c",
        None,
    ));
    reached(opened).await;
    assert_eq!(hooks.attempts.get(), 1);
    let delete = json!({"operation_id": OperationId::new("delete-opening".into())});
    assert_eq!(
        call(
            app.clone(),
            headers.clone(),
            "DELETE",
            "/api/v1/index/c",
            Some(delete)
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            app.clone(),
            headers.clone(),
            "POST",
            "/api/v1/index",
            Some(create("c"))
        )
        .await
        .0,
        StatusCode::OK
    );
    let replacement = service
        .catalog
        .collection_entry("tenant", "c")
        .await
        .unwrap();
    assert_ne!(replacement.generation, entry.generation);
    install.send(()).unwrap();
    let (status, body) = tokio::time::timeout(WAIT, old).await.unwrap().unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(hooks.installs.get(), 0);
    assert_eq!(
        call(app.clone(), headers, "GET", "/api/v1/index/c", None)
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        service
            .records
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        [replacement.scope()]
    );
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn http_shutdown_drains_an_abandoned_cold_open() {
    let (initial, root, headers) = super::super::tests::setup().await;
    initial.shutdown().wait().await.unwrap();
    let service = reopen(&root).await;
    let app = router(service.clone());
    let (opened, install) = service.cold.test.before_install.pause_next();
    let leader = tokio::spawn(call(app.clone(), headers, "GET", "/api/v1/index/c", None));
    reached(opened).await;
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
    let mut shutdown = service.shutdown();
    assert_eq!(
        call(app.clone(), HeaderMap::new(), "GET", "/api/v1/live", None)
            .await
            .1["code"],
        "shutting_down"
    );
    drop(app);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), shutdown.wait())
            .await
            .is_err()
    );
    install.send(()).unwrap();
    tokio::time::timeout(WAIT, shutdown.wait())
        .await
        .unwrap()
        .unwrap();
    reopen(&root).await.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn http_cold_open_and_service_backup_serialize_file_owners_in_both_orders() {
    for backup_first in [false, true] {
        let (initial, root, headers) = super::super::tests::setup().await;
        let app = router(initial.clone());
        write_fixture(&app, &headers).await;
        drop(app);
        initial.shutdown().wait().await.unwrap();
        let service = reopen(&root).await;
        let app = router(service.clone());
        let hooks = &service.cold.test;
        let (opened, install) = hooks.before_install.pause_next();
        let (mut copied, freeze) = hooks.backup_snapshot.pause_next();
        let backup = call(
            app.clone(),
            headers.clone(),
            "POST",
            "/api/v1/service-backups",
            Some(json!({"operation_id": OperationId::new("backup-cold".into())})),
        );
        let read = call(
            app.clone(),
            headers.clone(),
            "GET",
            "/api/v1/index/c/doc/fixture",
            None,
        );
        let read_task;
        let backup_task;
        if backup_first {
            backup_task = tokio::spawn(backup);
            reached(copied).await;
            assert!(service.records.read().await.is_empty());
            read_task = tokio::spawn(read);
            hooks.waiters.until(1).await;
            assert_eq!(hooks.attempts.get(), 0);
            freeze.send(()).unwrap();
            reached(opened).await;
            install.send(()).unwrap();
        } else {
            read_task = tokio::spawn(read);
            reached(opened).await;
            backup_task = tokio::spawn(backup);
            hooks.backup_waiters.until(1).await;
            assert!(matches!(
                copied.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            install.send(()).unwrap();
            reached(copied).await;
            freeze.send(()).unwrap();
        }
        let (status, found) = tokio::time::timeout(WAIT, read_task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, StatusCode::OK, "{found}");
        assert_eq!(found["document"]["body"], "cold text");
        let (status, backup) = tokio::time::timeout(WAIT, backup_task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, StatusCode::OK, "{backup}");
        assert_eq!(hooks.attempts.get(), 1);
        assert_eq!(hooks.installs.get(), 1);
        assert_eq!(service.records.read().await.len(), 1);

        let archive = root.join(format!(
            "service-backup-{}",
            backup["backup_id"].as_str().unwrap()
        ));
        let restored = root.join("verified-backup");
        let snapshot =
            Catalog::restore_service(archive, restored.clone(), "verify-backup-cut".into())
                .await
                .unwrap();
        let entry = service
            .catalog
            .collection_entry("tenant", "c")
            .await
            .unwrap();
        assert_eq!(snapshot.record_seqs.get(&entry.scope()), Some(&1));
        let restored = reopen(&restored).await;
        assert!(restored.records.read().await.is_empty());
        let result = call(
            router(restored.clone()),
            headers,
            "GET",
            "/api/v1/index/c/doc/fixture",
            None,
        )
        .await;
        assert_eq!(result.0, StatusCode::OK, "{}", result.1);
        assert_eq!(result.1["document"]["body"], "cold text");
        restored.shutdown().wait().await.unwrap();
        drop(app);
        service.shutdown().wait().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
