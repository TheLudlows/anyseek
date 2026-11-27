use super::{
    tests::{master_headers, namespace_api},
    *,
};
use std::{
    num::NonZeroUsize,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

const QUERY: &str = "/api/v1/index/window/event-time/query";
const BUILD: &str = "/api/v1/event-time-checkpoints";
const BATCH: &str = "/api/v1/index/window/batch";

async fn open(root: &FsPath) -> Arc<Service> {
    Service::open_http(
        root,
        NonZeroUsize::new(200).unwrap(),
        "anyseek-unit-test-secret",
    )
    .await
    .unwrap()
}

async fn build(app: &Router, headers: HeaderMap) -> (StatusCode, Value) {
    let mut request = axum::http::Request::builder().method("POST").uri(BUILD);
    *request.headers_mut().unwrap() = headers;
    let response = app
        .clone()
        .oneshot(
            request
                .body(Body::from(
                    r#"{"tenant_id":"tenant","collection_id":"window"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), MAX_RESPONSE)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn row(id: &str, at: i64, rank: Option<i64>, expires: Option<u64>) -> Value {
    json!({"operation_id":OperationId::new(id.into()),"record_id":id,"expected":"Absent",
        "temporal":{"event_time_ms":at,"valid_from_ms":100,"valid_to_ms":200,"expire_at_ms":expires},
        "document":{"body":"match","private":"do not return","rank":rank,"tags":["even","green"]}})
}

fn bounded_request() -> Value {
    json!({"temporal":{"event":{"from_ms":150,"to_ms":151},"valid_at_ms":199},"length":4,
        "fields":["body","private"],
        "filter":[{"Int":{"field":"rank","filter":{"start":2,"end":8}}},
                  {"StringSet":{"field":"tags","filter":["blue","green"]}}]})
}

#[tokio::test]
async fn event_time_http_authority_reopen_and_work_limits() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let service = open(&root).await;
    let app = router(service.clone());
    let (status, create) = namespace_api(
        &app,
        "POST",
        "/api/v1/index",
        "tenant",
        Some(json!({
            "operation_id":OperationId::new("create-window".into()),"collection_id":"window",
            "config":{"index_name":"window","schema":[
                {"field":"body","field_type":"Text","store":true,"index_lexical":true},
                {"field":"private","field_type":"Text","store":false},
                {"field":"rank","field_type":"Int","filterable":true},
                {"field":"tags","field_type":"StringSet","filterable":true}
            ]}
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{create}");

    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 1800;
    let (status, body) = namespace_api(&app, "POST", BATCH, "tenant", Some(json!({"writes":[
        row("below",149,Some(3),None),row("edge",151,Some(3),None),row("rank-miss",150,Some(9),None),
        row("null",150,None,None),row("retained",150,Some(3),None),row("expires",150,Some(3),Some(expiry))
    ]}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, error) =
        namespace_api(&app, "POST", QUERY, "tenant", Some(bounded_request())).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(error["code"], "event_time_unavailable");
    assert!(error["message"].as_str().unwrap().contains("no projection"));
    assert_eq!(
        namespace_api(&app, "POST", QUERY, "outside", Some(bounded_request()))
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    let mut tenant_headers = master_headers();
    tenant_headers.insert(TENANT_HEADER, "tenant".parse().unwrap());
    assert_eq!(build(&app, tenant_headers).await.0, StatusCode::FORBIDDEN);
    assert_eq!(
        build(&app, HeaderMap::new()).await.0,
        StatusCode::UNAUTHORIZED
    );
    let (status, result) = build(&app, master_headers()).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["indexed_seq"], 6);
    for (name, value) in [
        ("query", json!("match")),
        ("query_vector", json!([1, 0])),
        ("rrf", json!({})),
    ] {
        let mut request = bounded_request();
        request[name] = value;
        assert_eq!(
            namespace_api(&app, "POST", QUERY, "tenant", Some(request))
                .await
                .0,
            StatusCode::BAD_REQUEST
        );
    }
    let mut empty = bounded_request();
    empty["temporal"]["event"]["to_ms"] = json!(150);
    assert_eq!(
        namespace_api(&app, "POST", QUERY, "tenant", Some(empty))
            .await
            .0,
        StatusCode::BAD_REQUEST
    );

    tokio::time::sleep(Duration::from_millis(
        expiry.saturating_sub(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        ) + 10,
    ))
    .await;
    let (status, result) =
        namespace_api(&app, "POST", QUERY, "tenant", Some(bounded_request())).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["visible_seq"], 6);
    assert_eq!(result["hits"].as_array().unwrap().len(), 1);
    assert_eq!(result["hits"][0]["record_id"], "retained");
    assert_eq!(result["hits"][0]["document"], json!({"body":"match"}));
    let mut request = bounded_request();
    let ingested = result["hits"][0]["ingested_at_ms"].as_i64().unwrap();
    request["temporal"]["ingested"] = json!({"from_ms":ingested,"to_ms":ingested+1});
    let selected = namespace_api(&app, "POST", QUERY, "tenant", Some(request.clone())).await;
    assert_eq!(selected.0, StatusCode::OK, "{:?}", selected.1);
    assert_eq!(selected.1["hits"], result["hits"]);
    request["temporal"]["valid_at_ms"] = json!(200);
    let expired_validity = namespace_api(&app, "POST", QUERY, "tenant", Some(request)).await;
    assert_eq!(
        expired_validity.0,
        StatusCode::OK,
        "{:?}",
        expired_validity.1
    );
    assert_eq!(expired_validity.1["hits"], json!([]));
    let ordinary = namespace_api(
        &app,
        "POST",
        "/api/v1/index/window/query",
        "tenant",
        Some(json!({"query":"match","length":6})),
    )
    .await;
    assert_eq!(ordinary.0, StatusCode::OK, "{:?}", ordinary.1);
    assert_eq!(ordinary.1["statistics"]["active_records"], 5);

    let selected = root.join(
        service
            .catalog
            .collection_entry("tenant", "window")
            .await
            .unwrap()
            .directory(),
    );
    let pages = std::fs::read_dir(selected)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("event-time/primary-pages"))
        .find(|path| path.exists())
        .unwrap();
    drop(app);
    service.shutdown().wait().await.unwrap();

    let bytes = std::fs::read(&pages).unwrap();
    for (damaged, valid) in [
        (None, false),
        (
            Some({
                let mut changed = bytes.clone();
                changed[20] ^= 1;
                changed
            }),
            false,
        ),
        (Some(bytes), true),
    ] {
        match damaged.as_ref() {
            Some(contents) => std::fs::write(&pages, contents).unwrap(),
            None => std::fs::remove_file(&pages).unwrap(),
        }
        let service = open(&root).await;
        let app = router(service.clone());
        let (status, replayed) =
            namespace_api(&app, "POST", QUERY, "tenant", Some(bounded_request())).await;
        if valid {
            assert_eq!(status, StatusCode::OK, "{replayed}");
            assert_eq!(replayed["visible_seq"], 6);
            assert_eq!(replayed["hits"], result["hits"]);
            for chunk in (0..129).collect::<Vec<_>>().chunks(50) {
                let rows: Vec<_> = chunk
                    .iter()
                    .map(|i| row(&format!("tail-{i}"), 250, Some(3), None))
                    .collect();
                let (status, written) =
                    namespace_api(&app, "POST", BATCH, "tenant", Some(json!({"writes":rows})))
                        .await;
                assert_eq!(status, StatusCode::OK, "{written}");
            }
            let (status, exhausted) =
                namespace_api(&app, "POST", QUERY, "tenant", Some(bounded_request())).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{exhausted}");
            assert_eq!(exhausted["code"], "event_time_budget");
            assert!(
                exhausted["message"]
                    .as_str()
                    .unwrap()
                    .contains("row budget")
            );
            let (status, refreshed) = build(&app, master_headers()).await;
            assert_eq!(status, StatusCode::OK, "{refreshed}");
            assert!(refreshed["indexed_seq"].as_u64().unwrap() >= 135);
            let current =
                namespace_api(&app, "POST", QUERY, "tenant", Some(bounded_request())).await;
            assert_eq!(current.0, StatusCode::OK, "{:?}", current.1);
            assert_eq!(current.1["hits"], result["hits"]);
            let compacted =
                namespace_api(&app, "PATCH", "/api/v1/index/window", "tenant", None).await;
            assert_eq!(compacted.0, StatusCode::OK, "{:?}", compacted.1);
            let removed =
                namespace_api(&app, "POST", QUERY, "tenant", Some(bounded_request())).await;
            assert_eq!(removed.0, StatusCode::BAD_REQUEST, "{:?}", removed.1);
            assert_eq!(removed.1["code"], "event_time_unavailable");
        } else {
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{replayed}");
            assert_eq!(replayed["code"], "storage_error");
        }
        drop(app);
        service.shutdown().wait().await.unwrap();
    }

    // The ordinary checkpoint selected a base without the projection. Reopen
    // that actual certificate before rebuilding through the same admin route.
    let service = open(&root).await;
    let app = router(service.clone());
    let missing = namespace_api(&app, "POST", QUERY, "tenant", Some(bounded_request())).await;
    assert_eq!(missing.0, StatusCode::BAD_REQUEST, "{:?}", missing.1);
    assert_eq!(missing.1["code"], "event_time_unavailable");
    let (status, checkpoint) = build(&app, master_headers()).await;
    assert_eq!(status, StatusCode::OK, "{checkpoint}");
    let recovered = namespace_api(&app, "POST", QUERY, "tenant", Some(bounded_request())).await;
    assert_eq!(recovered.0, StatusCode::OK, "{:?}", recovered.1);
    assert_eq!(recovered.1["hits"], result["hits"]);
    drop(app);
    service.shutdown().wait().await.unwrap();
}

#[tokio::test]
async fn event_time_http_original_deadline_returns_busy() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (mut service, path, headers) = super::tests::setup().await;
    Arc::get_mut(&mut service).unwrap().request_timeout = Duration::from_millis(75);
    let app = router(service.clone());
    let entered = Arc::new(AtomicBool::new(false));
    let reached = entered.clone();
    let pending = futures::stream::poll_fn(move |_| {
        reached.store(true, Ordering::Release);
        Poll::Pending::<Option<Result<Bytes, std::io::Error>>>
    });
    let mut request = axum::http::Request::builder()
        .method("POST")
        .uri("/api/v1/index/c/event-time/query");
    *request.headers_mut().unwrap() = headers;
    let response = app
        .clone()
        .oneshot(request.body(Body::from_stream(pending)).unwrap())
        .await
        .unwrap();
    assert!(
        entered.load(Ordering::Acquire),
        "exercise the handler deadline after admission"
    );
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()["x-anyseek-error-code"], "busy");
    drop(response);
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}
