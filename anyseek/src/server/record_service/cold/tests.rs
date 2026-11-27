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
    pub(in crate::server::record_service) event_time_admission: PauseNext,
    pub(in crate::server::record_service) preflight: PauseNext,
    pub(in crate::server::record_service) before_install: PauseNext,
    pub(in crate::server::record_service) backup_snapshot: PauseNext,
    pub(in crate::server::record_service) before_retire: PauseNext,
    pub(in crate::server::record_service) backup_waiters: Counter,
    pub(in crate::server::record_service) attempts: Counter,
    pub(in crate::server::record_service) installs: Counter,
    pub(in crate::server::record_service) waiters: Counter,
    pub(in crate::server::record_service) retirement_waiters: Counter,
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

async fn tcp_get(address: std::net::SocketAddr, headers: &HeaderMap, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::time::timeout(WAIT, async {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\napikey: {}\r\nX-AnySeek-Tenant: {}\r\n\r\n",
            headers["apikey"].to_str().unwrap(), headers[TENANT_HEADER].to_str().unwrap());
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }).await.unwrap()
}

#[tokio::test]
async fn residency_pressure_keeps_frames_and_reopens_original_record() {
    residency_pressure_keeps_frame(FrameSource::Record).await;
}

#[tokio::test]
async fn event_time_checkpoint_receipt_frame_pins_built_generation() {
    residency_pressure_keeps_frame(FrameSource::EventTimeCheckpoint).await;
}

enum FrameSource {
    Record,
    EventTimeCheckpoint,
}

async fn residency_pressure_keeps_frame(source: FrameSource) {
    use crate::storage::build_budget::{reserve_memory, shared_memory_usage};
    use http_body::Body as _;

    let (initial, root, headers) = super::super::tests::setup().await;
    let app = router(initial.clone());
    let operation = OperationId::new("eviction-write".into());
    let original = "Original retained body ".repeat(1024);
    let (status, result) = call(app.clone(), headers.clone(), "POST", "/api/v1/index/c/batch",
        Some(json!({"writes":[{"operation_id":operation,"record_id":"fixture","expected":"Absent","document":{"body":original}}]}))).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    let receipt = result["results"][0]["receipt"].clone();
    assert_eq!(receipt["version"], 1);
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
    let app = router(service.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let network_app = app.clone();
    let transport = tokio::spawn(async move {
        axum::serve(listener, network_app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let scope = service
        .catalog
        .collection_entry("tenant", "c")
        .await
        .unwrap()
        .scope();
    let request = match source {
        FrameSource::Record => {
            let mut request = axum::http::Request::builder().uri("/api/v1/index/c/doc/fixture");
            request.headers_mut().unwrap().extend(headers.clone());
            request.body(Body::empty()).unwrap()
        }
        FrameSource::EventTimeCheckpoint => {
            let mut request = axum::http::Request::builder()
                .method("POST")
                .uri("/api/v1/event-time-checkpoints");
            request
                .headers_mut()
                .unwrap()
                .extend(super::super::tests::master_headers());
            request
                .body(Body::from(r#"{"tenant_id":"tenant","collection_id":"c"}"#))
                .unwrap()
        }
    };
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let bytes = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    drop(body);
    let before: Value = serde_json::from_slice(&bytes).unwrap();
    match source {
        FrameSource::Record => assert_eq!(before["document"]["body"], original),
        FrameSource::EventTimeCheckpoint => assert_eq!(before, json!({"indexed_seq":1})),
    }
    let (used, shared) = shared_memory_usage().unwrap();
    service.records.set_budget_for_test(used);
    let pressure = reserve_memory(1024).unwrap();
    let refusal = tcp_get(address, &headers, "/api/v1/index/other").await;
    assert!(refusal.starts_with("HTTP/1.1 429 "), "{refusal}");
    assert!(refusal.contains("\"code\":\"busy\""), "{refusal}");
    assert!(matches!(
        service.records.lookup(&scope),
        Lookup::Open { .. }
    ));
    drop(bytes);

    // The same pressure now permits a safe close and a physical open of the
    // other collection. The original scope remains durably active in Catalog.
    let other = tcp_get(address, &headers, "/api/v1/index/other").await;
    assert!(other.starts_with("HTTP/1.1 200 "), "{other}");
    assert!(matches!(service.records.lookup(&scope), Lookup::Missing));
    assert!(
        service
            .catalog
            .collection_entry("tenant", "c")
            .await
            .unwrap()
            .active
    );
    drop(pressure);
    service.records.set_budget_for_test(shared);
    let (status, after) = call(
        app.clone(),
        headers.clone(),
        "GET",
        "/api/v1/index/c/doc/fixture",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{after}");
    assert_eq!(after["document"]["body"], original);
    if matches!(source, FrameSource::Record) {
        assert_eq!(after, before);
    }
    let operation_uri = format!(
        "/api/v1/index/c/operation/{}/{}",
        operation.epoch, operation.nonce
    );
    let (status, result) = call(app.clone(), headers, "GET", &operation_uri, None).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["receipt"], receipt);
    stop.send(()).unwrap();
    tokio::time::timeout(WAIT, transport)
        .await
        .unwrap()
        .unwrap();
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn residency_pressure_preserves_whole_service_backup_capture() {
    use crate::storage::build_budget::{reserve_memory, shared_memory_usage};

    let (service, root, headers) = super::super::tests::setup().await;
    let app = router(service.clone());
    write_fixture(&app, &headers).await;
    let scope = service
        .catalog
        .collection_entry("tenant", "c")
        .await
        .unwrap()
        .scope();
    let (captured, release) = service.cold.test.backup_snapshot.pause_next();
    let mut master = HeaderMap::new();
    master.insert("apikey", headers["apikey"].clone());
    let backup = tokio::spawn(call(
        app.clone(),
        master,
        "POST",
        "/api/v1/service-backups",
        Some(json!({"operation_id": OperationId::new("resident-backup".into())})),
    ));
    reached(captured).await;
    let (used, shared) = shared_memory_usage().unwrap();
    service.records.set_budget_for_test(used);
    let pressure = reserve_memory(1024).unwrap();
    // Test the production periodic path while the backup owns the disk gate.
    service.evict_over_budget();
    assert!(matches!(
        service.records.lookup(&scope),
        Lookup::Open { .. }
    ));
    assert!(!service.records.has_room(0));
    service.records.set_budget_for_test(shared);
    drop(pressure);
    release.send(()).unwrap();
    let (status, body) = tokio::time::timeout(WAIT, backup).await.unwrap().unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["snapshot"]["record_seqs"][&scope], 1);
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn concurrent_cold_reads_wait_for_retirement_then_share_the_reopen() {
    use crate::storage::build_budget::{reserve_memory, shared_memory_usage};

    let (service, root, headers) = super::super::tests::setup().await;
    let app = router(service.clone());
    write_fixture(&app, &headers).await;
    let scope = service
        .catalog
        .collection_entry("tenant", "c")
        .await
        .unwrap()
        .scope();
    let (entered, release) = service.cold.test.before_retire.pause_next();
    let (used, shared) = shared_memory_usage().unwrap();
    service.records.set_budget_for_test(used);
    let pressure = reserve_memory(1024).unwrap();
    service.evict_over_budget();
    reached(entered).await;
    assert!(matches!(service.records.lookup(&scope), Lookup::Retiring));

    let path = "/api/v1/index/c/doc/fixture";
    let first = tokio::spawn(call(app.clone(), headers.clone(), "GET", path, None));
    let second = tokio::spawn(call(app.clone(), headers, "GET", path, None));
    service.cold.test.retirement_waiters.until(2).await;
    assert_eq!(service.cold.test.attempts.get(), 0);
    service.records.set_budget_for_test(shared);
    drop(pressure);
    release.send(()).unwrap();

    for task in [first, second] {
        let (status, body) = tokio::time::timeout(WAIT, task).await.unwrap().unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["document"]["body"], "cold text");
    }
    assert_eq!(service.cold.test.attempts.get(), 1);
    assert_eq!(service.cold.test.installs.get(), 1);
    assert!(matches!(
        service.records.lookup(&scope),
        Lookup::Open { .. }
    ));
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
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
    assert!(service.records.is_empty());
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
    assert_eq!(service.records.len(), 2);
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
    assert!(service.records.is_empty());
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
            .snapshot()
            .unwrap()
            .into_keys()
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
    let (reading, admitted) = oneshot::channel();
    let (send_body, body_ready) = oneshot::channel();
    let batch = json!({"writes": [{
        "operation_id": OperationId::new("admitted-before-stop".into()),
        "record_id": "fixture",
        "expected": "Absent",
        "document": {"body": "accepted"}
    }]})
    .to_string();
    let body = Body::from_stream(futures::stream::once(async move {
        reading.send(()).unwrap();
        body_ready.await.unwrap();
        Ok::<_, std::io::Error>(Bytes::from(batch))
    }));
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/api/v1/index/c/batch");
    request.headers_mut().unwrap().extend(headers);
    let leader = tokio::spawn(app.clone().oneshot(request.body(body).unwrap()));
    reached(admitted).await;
    assert_eq!(service.cold.test.attempts.get(), 0);
    let mut shutdown = service.shutdown();
    assert_eq!(
        call(app.clone(), HeaderMap::new(), "GET", "/api/v1/live", None)
            .await
            .1["code"],
        "shutting_down"
    );
    send_body.send(()).unwrap();
    reached(opened).await;
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
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
            assert!(service.records.is_empty());
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
        assert_eq!(service.records.len(), 1);

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
        assert!(restored.records.is_empty());
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
