use super::{
    super::{
        EVENT_TIME_CHECKPOINT_PATH,
        tests::{master_headers, namespace_api, namespace_response},
    },
    *,
};
use http_body::Body as _;
use std::{num::NonZeroUsize, time::Duration};
use tower::ServiceExt;

const QUERY: &str = "/api/v1/index/graph/entity/query";
const GROUP: &str = "/api/v1/index/graph/group/replace";
const WAIT: Duration = Duration::from_secs(25);

async fn open(root: &FsPath) -> Arc<Service> {
    Service::open_http(
        root,
        NonZeroUsize::new(200).unwrap(),
        "anyseek-unit-test-secret",
    )
    .await
    .unwrap()
}

async fn create(app: &Router, tenant: &str, nonce: &str) {
    let (status, payload) = namespace_api(
        app,
        "POST",
        "/api/v1/index",
        tenant,
        Some(json!({
            "operation_id": OperationId::new(nonce.into()),
            "collection_id": "graph",
            "config": {"index_name":"graph","tokenizer":"TantivySimpleV1","schema":[
                {"field":"body","field_type":"Text","store":true,"index_lexical":true},
                {"field":"entity_ids","field_type":"StringSet","store":true},
                {"field":"relations","field_type":"Json","store":true}
            ]}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{payload}");
}

async fn replace(app: &Router, tenant: &str, previous: u64, records: Value) {
    let request = json!({
        "operation_id": OperationId::new(format!("graph-{previous}")),
        "group_id": "graph", "expected_group_version": previous, "records": records
    });
    let deadline = Instant::now() + WAIT;
    loop {
        let (status, payload) =
            namespace_api(app, "POST", GROUP, tenant, Some(request.clone())).await;
        if status != StatusCode::TOO_MANY_REQUESTS || Instant::now() >= deadline {
            assert_eq!(status, StatusCode::OK, "{payload}");
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn graph(neighbor: &str, expiry: Option<u64>) -> Value {
    let mut target = json!({"record_id":"target","temporal":{"event_time_ms":25},"document":{"body":"private source","entity_ids":[neighbor]}});
    if let Some(expiry) = expiry {
        target["temporal"]["expire_at_ms"] = json!(expiry);
    }
    json!([
        {"record_id":"seed","document":{"body":"private source","entity_ids":["app:A"]}},
        {"record_id":"edge","document":{"body":"private source","relations":{"edges":[
            {"from":"app:A","type":"link","to":neighbor}
        ]}}},
        target
    ])
}

async fn build(app: &Router, tenant: &str, checkpoint: &str) {
    build_at(app, tenant, checkpoint, 1).await;
}

async fn build_at(app: &Router, tenant: &str, checkpoint: &str, indexed_seq: u64) {
    let deadline = Instant::now() + WAIT;
    loop {
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(checkpoint)
                    .header("apikey", master_headers()["apikey"].clone())
                    .body(Body::from(
                        json!({"tenant_id":tenant,"collection_id":"graph"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let (status, payload) = decode(response).await;
        if status != StatusCode::TOO_MANY_REQUESTS || Instant::now() >= deadline {
            assert_eq!(status, StatusCode::OK, "{payload}");
            assert_eq!(payload, json!({"indexed_seq":indexed_seq}));
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn decode(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let error = response.headers().get(ERROR_CODE_HEADER).cloned();
    let bytes = axum::body::to_bytes(response.into_body(), MAX_RESPONSE)
        .await
        .unwrap();
    let payload: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        error.as_ref().and_then(|value| value.to_str().ok()),
        payload["code"].as_str()
    );
    (status, payload)
}

async fn query(
    app: &Router,
    tenant: &str,
    direction: &str,
    anchor: &str,
    visit_limit: usize,
) -> Response {
    namespace_response(
        app,
        "POST",
        QUERY,
        tenant,
        Some(json!({
            "direction":direction,"anchor":anchor,"kind":"link","visit_limit":visit_limit
        })),
    )
    .await
}

async fn path(app: &Router, tenant: &str, direction: &str, anchor: &str, neighbor: &str) {
    let (status, payload) = decode(query(app, tenant, direction, anchor, 3).await).await;
    assert_eq!(status, StatusCode::OK, "{payload}");
    let (seed, target) = if direction == "out" {
        ("seed", "target")
    } else {
        ("target", "seed")
    };
    assert_eq!(
        payload,
        json!({
            "visible_seq":1,
            "seeds":[{"record_id":seed,"version":1}],
            "neighbors":[{
                "entity_id":neighbor,
                "declarations":[{"neighbor":neighbor,"record_id":"edge","version":1}],
                "members":[{"record_id":target,"version":1}]
            }]
        })
    );
}

async fn closes_without_data(response: Response, case: &str) {
    if response.status() != StatusCode::OK {
        panic!(
            "{case}: expected prepared headers, got {:?}",
            decode(response).await
        );
    }
    let mut body = response.into_body();
    let next = timeout_at(
        Instant::now() + WAIT,
        std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)),
    )
    .await
    .expect("stale body must terminate");
    assert!(
        matches!(next, Some(Err(_))),
        "{case}: stale response emitted a frame: {next:?}"
    );
    assert!(
        std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .is_none()
    );
}

#[tokio::test]
async fn http_entity_round_trip_is_scoped_opt_in_and_bounded() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("service");
    let service = open(&root).await;
    let app = router(service.clone());

    let denied = namespace_response(
        &app,
        "POST",
        CHECKPOINT_PATH,
        "tenant-a",
        Some(json!({"tenant_id":"tenant-b","collection_id":"graph"})),
    )
    .await;
    let (status, payload) = decode(denied).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(payload["code"], "unauthorized");

    for (tenant, neighbor) in [("tenant-a", "app:B"), ("tenant-b", "app:C")] {
        create(&app, tenant, "create").await;
        replace(&app, tenant, 0, graph(neighbor, None)).await;
        let (status, unavailable) = decode(query(&app, tenant, "out", "app:A", 3).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(unavailable["code"], "rejected");
        build(&app, tenant, CHECKPOINT_PATH).await;
        path(&app, tenant, "out", "app:A", neighbor).await;
        path(&app, tenant, "in", neighbor, "app:A").await;
    }
    let stale = query(&app, "tenant-a", "out", "app:A", 3).await;
    build(&app, "tenant-a", EVENT_TIME_CHECKPOINT_PATH).await;
    closes_without_data(stale, "same-sequence root change").await;
    path(&app, "tenant-a", "out", "app:A", "app:B").await;
    let (status, window) = namespace_api(
        &app,
        "POST",
        "/api/v1/index/graph/event-time/query",
        "tenant-a",
        Some(json!({"temporal":{"event":{"from_ms":24,"to_ms":26}},"length":3})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{window}");
    assert_eq!(window["visible_seq"], 1);
    assert_eq!(window["hits"].as_array().unwrap().len(), 1);
    assert_eq!(window["hits"][0]["record_id"], "target");
    let (status, denied) = decode(query(&app, "tenant-other", "out", "app:A", 3).await).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(denied["code"], "unauthorized");
    let (status, hidden) = decode(query(&app, "tenant-other", "out", "bad key", 3).await).await;
    assert_eq!((status, hidden), (StatusCode::FORBIDDEN, denied));
    for limit in [0, 2, 257] {
        let (status, rejected) = decode(query(&app, "tenant-a", "out", "app:A", limit).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(rejected["code"], "rejected");
        assert_eq!(rejected.as_object().unwrap().len(), 2);
    }
    let (status, invalid) = decode(query(&app, "tenant-a", "out", "bad key", 3).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid["code"], "rejected");

    replace(&app, "tenant-a", 1, json!([])).await;
    drop(app);
    service.shutdown().wait().await.unwrap();
    let service = open(&root).await;
    let app = router(service.clone());
    let (status, cleared) = decode(query(&app, "tenant-a", "out", "app:A", 3).await).await;
    assert_eq!(status, StatusCode::OK, "{cleared}");
    assert_eq!(cleared, json!({"visible_seq":2,"seeds":[],"neighbors":[]}));
    path(&app, "tenant-b", "in", "app:C", "app:A").await;
    drop(app);
    service.shutdown().wait().await.unwrap();
}

#[tokio::test]
async fn http_entity_body_rechecks_withdrawal_generation_and_ttl() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("service");
    let service = open(&root).await;
    let app = router(service.clone());

    create(&app, "tenant", "create").await;
    replace(&app, "tenant", 0, graph("app:B", None)).await;
    build(&app, "tenant", CHECKPOINT_PATH).await;

    let (reached, release) = service.cold.test.entity_handoff.pause_next();
    let client = app.clone();
    let pending = tokio::spawn(async move { query(&client, "tenant", "out", "app:A", 3).await });
    timeout_at(Instant::now() + WAIT, reached)
        .await
        .unwrap()
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(service.request_timeout).await;
    let timed_out = timeout_at(Instant::now() + WAIT, pending)
        .await
        .unwrap()
        .unwrap();
    tokio::time::resume();
    drop(release);
    let (status, payload) = decode(timed_out).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(payload["code"], "busy");

    let stale = query(&app, "tenant", "out", "app:A", 3).await;
    replace(&app, "tenant", 1, json!([])).await;
    closes_without_data(stale, "withdrawal").await;

    replace(&app, "tenant", 2, graph("app:B", None)).await;
    // Keep the old owner healthy and resident so only retirement invalidates this frame.
    let old = service.record("tenant", "graph").await.unwrap();
    let retired = query(&app, "tenant", "out", "app:A", 32).await;
    let (status, payload) = namespace_api(
        &app,
        "DELETE",
        "/api/v1/index/graph",
        "tenant",
        Some(json!({"operation_id":OperationId::new("retire".into())})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{payload}");
    create(&app, "tenant", "replacement").await;
    assert!(old.output_permit().is_ok());
    closes_without_data(retired, "retirement").await;
    drop(old);

    let record = service.record("tenant", "graph").await.unwrap();
    let (clock, now) = crate::record::tests::test_clock(&record).await;
    let expiry = now + 120_000;
    replace(&app, "tenant", 0, graph("app:B", Some(expiry))).await;
    build(&app, "tenant", CHECKPOINT_PATH).await;
    path(&app, "tenant", "out", "app:A", "app:B").await;
    let expiring = query(&app, "tenant", "out", "app:A", 3).await;
    clock.set(expiry);
    closes_without_data(expiring, "TTL").await;

    drop(record);
    drop(app);
    service.shutdown().wait().await.unwrap();
}

#[test]
fn http_entity_openapi_describes_admission_and_payload() {
    let doc = openapi_document();
    let query = &doc["paths"][QUERY_PATH]["post"];
    let schema = &doc["components"]["schemas"]["EntityCandidatePaths"];
    assert_eq!(
        query["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/EntityPathRequest"
    );
    assert_eq!(
        query["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/EntityCandidatePaths"
    );
    assert_eq!(
        query["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["name"]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case(TENANT_HEADER)))
            .count(),
        1
    );
    assert!(
        doc["paths"][CHECKPOINT_PATH]["post"]["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| !p["name"]
                .as_str()
                .is_some_and(|name| name.eq_ignore_ascii_case(TENANT_HEADER)))
    );
    assert!(schema["properties"].get("expires_at_ms").is_none());
    assert!(schema["properties"].get("_memory").is_none());
}


#[path = "fusion_http_tests.rs"]
mod fusion;
