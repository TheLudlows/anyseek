use super::{
    tests::{master_headers, namespace_api, namespace_response},
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

async fn build(app: &Router, headers: HeaderMap, collection: &str) -> (StatusCode, Value) {
    let mut request = axum::http::Request::builder().method("POST").uri(BUILD);
    *request.headers_mut().unwrap() = headers;
    let response = app
        .clone()
        .oneshot(
            request
                .body(Body::from(
                    json!({"tenant_id":"tenant","collection_id":collection}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    decode_response(response).await
}

async fn decode_response(response: Response) -> (StatusCode, Value) {
    let status = response.status();
    let code = response.headers().get(ERROR_CODE_HEADER).cloned();
    let bytes = axum::body::to_bytes(response.into_body(), MAX_RESPONSE)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    if status.is_success() {
        assert!(code.is_none(), "success must not carry an error header");
    } else {
        assert_eq!(
            code.as_ref().and_then(|value| value.to_str().ok()),
            body["code"].as_str()
        );
        assert!(code.is_some(), "response is missing its error code header");
    }
    (status, body)
}

async fn window(app: &Router, request: Value) -> (StatusCode, Value) {
    decode_response(namespace_response(app, "POST", QUERY, "tenant", Some(request)).await).await
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

#[test]
fn event_time_vector_openapi_keeps_ordinary_and_mixed_query_shapes() {
    let doc = openapi_document();
    let query = &doc["paths"]["/api/v1/index/{id}/query"]["post"];
    let input = &query["requestBody"]["content"]["application/json"]["schema"]["oneOf"];
    assert_eq!(input[0]["$ref"], "#/components/schemas/SearchRequestObject");
    assert_eq!(input[1]["$ref"], "#/components/schemas/TimeVectorRequest");
    assert_eq!(input[2]["$ref"], "#/components/schemas/TimeTextRequest");
    let output = &query["responses"]["200"]["content"]["application/json"]["schema"]["oneOf"];
    assert_eq!(
        output[0]["properties"]["diagnostics"]["$ref"],
        "#/components/schemas/QueryDiagnostics"
    );
    assert_eq!(output[1]["$ref"], "#/components/schemas/TimeVectorResult");
    let channels = &doc["components"]["schemas"]["TimeVectorResult"]["properties"]["hits"]
        ["items"]["properties"]["channels"];
    assert_eq!(channels["maxItems"], 3);
    assert_eq!(channels["items"]["properties"]["channel"]["enum"], json!(["time", "lexical", "vector"]));
    assert_eq!(
        doc["components"]["schemas"]["TimeVectorRequest"]["properties"]["all"]["const"],
        true
    );
    assert!(
        doc["components"]["schemas"]["EventTimeRequest"]["properties"]
            .get("recall")
            .is_none()
    );
}

async fn create_mixed_collection(app: &Router) {
    let (status, created) = namespace_api(app, "POST", "/api/v1/index", "tenant", Some(json!({
        "operation_id":OperationId::new("create-mixed".into()),"collection_id":"window",
        "embedding_space_id":"fixture", "config":{"index_name":"window","vector":{"dimensions":2},"schema":[
            {"field":"body","field_type":"Text","store":true}, {"field":"private","field_type":"Text","store":false},
            {"field":"vector","field_type":"Json","index_vector":true},
            {"field":"tags","field_type":"StringSet","filterable":true}
        ]}
    }))).await;
    assert_eq!(status, StatusCode::OK, "{created}");
}

const MIXED_QUERY: &str = "/api/v1/index/window/query";

fn mixed_request() -> Value {
    json!({"recall":{"time":{"from_ms":18,"to_ms":22}},"query_vector":[1,0],"all":true,"length":2})
}

fn mixed_row(nonce: &str, id: &str, expiry: Option<u64>) -> Value {
    json!({"operation_id":OperationId::new(nonce.into()),"record_id":id,"expected":"Absent",
        "temporal":{"event_time_ms":20,"expire_at_ms":expiry},"document":{"body":id,"vector":[1,0]}})
}

async fn write_mixed(app: &Router, rows: Vec<Value>) {
    let (status, body) =
        namespace_api(app, "POST", BATCH, "tenant", Some(json!({"writes":rows}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for result in body["results"].as_array().unwrap() {
        assert_eq!(result["code"], "applied", "{body}");
    }
}

async fn wait_marker(marker: &FsPath) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn event_time_public_handoffs_keep_point_observed_expiry_after_rollback() {
    for mixed in [true, false] {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("catalog");
        let service = open(&root).await;
        let app = router(service.clone());
        let sdk = Arc::new(local::LocalEngine::from_test_service(service.clone()));
        create_mixed_collection(&app).await;
        let p = service.record("tenant", "window").await.unwrap();
        let (clock, now) = anyseek::record::tests::test_clock(&p).await;
        let expiry = now + 60_000;
        write_mixed(&app, vec![mixed_row("expire", "expires", Some(expiry))]).await;
        assert_eq!(
            build(&app, master_headers(), "window").await,
            (StatusCode::OK, json!({"indexed_seq":1}))
        );
        let (path, request) = if mixed {
            (MIXED_QUERY, mixed_request())
        } else {
            (
                QUERY,
                json!({"temporal":{"event":{"from_ms":18,"to_ms":22}}}),
            )
        };
        let response =
            namespace_response(&app, "POST", path, "tenant", Some(request.clone())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let gate = root.join(".test-event-read-handoff-pause");
        std::fs::write(&gate, b"pause").unwrap();
        let reader = sdk.clone();
        let pending = tokio::spawn(async move {
            if mixed {
                reader
                    .query_time_vector("tenant", "window", serde_json::from_value(request).unwrap())
                    .await
                    .map(|_| ())
            } else {
                reader
                    .query_event_time("tenant", "window", serde_json::from_value(request).unwrap())
                    .await
                    .map(|_| ())
            }
        });
        wait_marker(&root.join(".test-event-read-handoff-started")).await;
        clock.set(expiry + 1000);
        assert!(
            sdk.get("tenant", "window", "expires")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            p.committed_progress().await.committed_seq,
            Some(1),
            "point observation does not persist expiry"
        );
        clock.set(expiry - 7000);
        std::fs::remove_file(gate).unwrap();
        let mut body = response.into_body();
        use http_body::Body as _;
        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        assert!(
            matches!(frame, Some(Err(_))),
            "the prepared HTTP frame must not expose the expired row"
        );
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none()
        );
        assert_eq!(pending.await.unwrap().unwrap_err().code(), "busy");
        drop(body);
        drop(p);
        drop(sdk);
        drop(app);
        service.shutdown().wait().await.unwrap();
    }
}

#[tokio::test]
async fn event_time_vector_stringset_work_counts_tail_and_sealed_tags() {
    let tmp = tempfile::tempdir().unwrap();
    let service = open(&tmp.path().join("catalog")).await;
    let app = router(service.clone());
    create_mixed_collection(&app).await;
    let mut row = mixed_row("small", "tagged", None);
    row["document"]["tags"] = json!(["present"]);
    write_mixed(&app, vec![row.clone()]).await;
    assert_eq!(
        build(&app, master_headers(), "window").await.0,
        StatusCode::OK
    );
    let mut request = mixed_request();
    let filter: Vec<_> = (0..128).map(|n| format!("missing-{n:04}")).collect();
    request["filter"] = json!([{"StringSet":{"field":"tags","filter":filter}}]);
    let (status, result) =
        namespace_api(&app, "POST", MIXED_QUERY, "tenant", Some(request.clone())).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["hits"], json!([]));
    row["operation_id"] = json!(OperationId::new("large".into()));
    row["expected"] = json!({"Match":1});
    let tags: Vec<_> = (0..4096).map(|n| format!("present-{n:04}")).collect();
    row["document"]["tags"] = json!(tags);
    write_mixed(&app, vec![row]).await;
    for sealed in [false, true] {
        if sealed {
            assert_eq!(
                build(&app, master_headers(), "window").await.0,
                StatusCode::OK
            );
        }
        let (status, error) = decode_response(
            namespace_response(&app, "POST", MIXED_QUERY, "tenant", Some(request.clone())).await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "sealed={sealed} {error}");
        assert!(
            error["message"]
                .as_str()
                .unwrap()
                .contains("shared query work budget exhausted"),
            "{error}"
        );
        assert!(error.get("hits").is_none());
    }
    drop(app);
    service.shutdown().wait().await.unwrap();
}

#[tokio::test]
async fn event_time_vector_publication_timeout_remains_a_read_error() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    let mut service = open(&root).await;
    Arc::get_mut(&mut service).unwrap().request_timeout = Duration::from_millis(300);
    let app = router(service.clone());
    create_mixed_collection(&app).await;
    let sdk = local::LocalEngine::from_test_service(service.clone());
    let entry = service
        .catalog
        .collection_entry("tenant", "window")
        .await
        .unwrap();
    let partition = root.join(entry.directory());
    let gate = partition.join(".test-lexical-consumer-pause");
    std::fs::write(&gate, b"pause").unwrap();
    let owner = service.record("tenant", "window").await.unwrap();
    let writer = tokio::spawn(async move {
        owner
            .write_batch(
                &serde_json::from_value::<Vec<RecordWrite>>(json!([
                    mixed_row("publish-a", "a", None),
                    mixed_row("publish-b", "b", None)
                ]))
                .unwrap(),
            )
            .await
    });
    wait_marker(&partition.join(".test-lexical-consumer-started")).await;
    let (status, error) = decode_response(
        namespace_response(&app, "POST", MIXED_QUERY, "tenant", Some(mixed_request())).await,
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{error}");
    assert_eq!(error["code"], "busy");
    assert!(!error["message"].as_str().unwrap().contains("operation"));
    assert!(error.get("hits").is_none());
    let error = sdk
        .query_time_vector(
            "tenant",
            "window",
            serde_json::from_value(mixed_request()).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "busy");
    assert!(!error.to_string().contains("operation"));
    assert!(!writer.is_finished());
    std::fs::remove_file(gate).unwrap();
    assert!(writer.await.unwrap().unwrap().stop.is_none());
    drop(sdk);
    drop(app);
    service.shutdown().wait().await.unwrap();
}

#[tokio::test]
async fn event_time_vector_http_respects_authorization_and_publication() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    let service = open(&root).await;
    let app = router(service.clone());
    create_mixed_collection(&app).await;
    let dated = |id, time, vector: Value| {
        json!({"operation_id":OperationId::new(String::from(id)),"record_id":id,
        "expected":"Absent","temporal":{"event_time_ms":time},"document":{"body":id,"private":"hidden","vector":vector}})
    };
    let (status, written) = namespace_api(
        &app,
        "POST",
        BATCH,
        "tenant",
        Some(json!({"writes":[
            dated("both", 20, json!([1,0])), dated("later", 100, json!([0,1]))
        ]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{written}");
    assert_eq!(written["results"][1]["code"], "applied");

    let path = "/api/v1/index/window/query";
    let query = json!({"recall":{"time":{"from_ms":18,"to_ms":22}},"query_vector":[1,0],"all":true,
        "length":2,"fields":["body","private"]});
    let routes = &app;
    let call = |tenant, request| async move {
        decode_response(namespace_response(routes, "POST", path, tenant, Some(request)).await).await
    };
    let (status, missing) = call("tenant", query.clone()).await;
    assert_eq!(
        (status, missing["code"].as_str()),
        (StatusCode::BAD_REQUEST, Some("event_time_unavailable"))
    );
    assert_eq!(
        build(&app, master_headers(), "window").await,
        (StatusCode::OK, json!({"indexed_seq":2}))
    );
    let (status, body) = call("tenant", query.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["visible_seq"], 2);
    assert!(body["count_total"].is_null());
    assert_eq!(
        body["hits"][0]["record"]["document"],
        json!({"body":"both"})
    );
    assert_eq!(body["hits"][0]["channels"][0]["channel"], "time");
    assert_eq!(body["hits"][0]["channels"][1]["channel"], "vector");
    assert_eq!(body["hits"][1]["record"]["record_id"], "later");
    assert_eq!(
        call("outside", query.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    for (field, value) in [
        ("nprobe", json!(1)),
        ("all", json!(false)),
        ("count", json!(true)),
    ] {
        let mut unsupported = query.clone();
        unsupported[field] = value;
        assert_eq!(call("tenant", unsupported).await.0, StatusCode::BAD_REQUEST);
    }
    let mut budget = query.clone();
    budget["offset"] = json!(64);
    let (_, rejected) = call("tenant", budget).await;
    assert_eq!(rejected["code"], "event_time_budget");

    let entry = service
        .catalog
        .collection_entry("tenant", "window")
        .await
        .unwrap();
    let gate = root
        .join(entry.directory())
        .join(".test-vector-consumer-pause");
    std::fs::write(&gate, b"pause").unwrap();
    let (status, written) = namespace_api(
        &app,
        "POST",
        "/api/v1/index/window/ingest",
        "tenant",
        Some(json!({"records":[dated("tail", 21, json!([1,0]))]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{written}");
    assert_eq!(written["results"][0]["receipt"]["seq"], 3);
    let busy = call("tenant", query).await;
    std::fs::remove_file(gate).unwrap();
    assert_eq!(
        (busy.0, busy.1["code"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("busy"))
    );
    drop(app);
    service.shutdown().wait().await.unwrap();
}

#[tokio::test]
async fn event_time_text_http_and_sdk_fuse_one_cut_and_reject_excess_work() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    let service = open(&root).await;
    let app = router(service.clone());
    let sdk = local::LocalEngine::from_test_service(service.clone());
    create_mixed_collection(&app).await;
    let mut rows = Vec::new();
    for (id, time, body, vector) in [
        ("both", 20, "match", Some(json!([1, 0]))),
        ("time", 21, "other", None),
        ("lexical", 90, "match", None),
        ("vector", 91, "other", Some(json!([0, 1]))),
    ] {
        let mut row = mixed_row(id, id, None);
        row["temporal"]["event_time_ms"] = json!(time);
        row["document"] = json!({"body":body,"private":"hidden","tags":["broad",id]});
        if let Some(vector) = vector {
            row["document"]["vector"] = vector;
        }
        rows.push(row);
    }
    for n in 0..12 {
        let id = format!("padding-{n}");
        let mut row = mixed_row(&id, &id, None);
        row["temporal"]["event_time_ms"] = json!(100);
        row["document"] = json!({"body":"other","tags":["broad",id]});
        rows.push(row);
    }
    write_mixed(&app, rows).await;
    assert_eq!(
        build(&app, master_headers(), "window").await,
        (StatusCode::OK, json!({"indexed_seq":16}))
    );
    let routes = &app;
    let call = |tenant, request| async move {
        decode_response(
            namespace_response(routes, "POST", MIXED_QUERY, tenant, Some(request)).await,
        )
        .await
    };
    let text = json!({"recall":{"time":{"from_ms":18,"to_ms":22}},"query":"match",
        "length":4,"fields":["body","private"]});
    let mut triple = text.clone();
    triple["query_vector"] = json!([1, 0]);
    triple["all"] = json!(true);
    for (request, expected) in [(text.clone(), 3), (triple.clone(), 4)] {
        let (status, body) = call("tenant", request.clone()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["visible_seq"], 16);
        assert!(body["count_total"].is_null());
        let hits = body["hits"].as_array().unwrap();
        assert_eq!(hits.len(), expected);
        assert_eq!(hits[0]["record"]["record_id"], "both");
        let channels = |id: &str| {
            hits.iter()
                .find(|hit| hit["record"]["record_id"] == id)
                .unwrap()["channels"]
                .as_array()
                .unwrap()
                .iter()
                .map(|channel| channel["channel"].as_str().unwrap())
                .collect::<Vec<_>>()
        };
        let expected_both = if expected == 3 {
            vec!["time", "lexical"]
        } else {
            vec!["time", "lexical", "vector"]
        };
        assert_eq!(channels("both"), expected_both);
        assert_eq!(channels("time"), ["time"]);
        assert_eq!(channels("lexical"), ["lexical"]);
        if expected == 4 {
            assert_eq!(channels("vector"), ["vector"]);
        }
        assert_eq!(hits[0]["record"]["document"], json!({"body":"match"}));
        let embedded = sdk
            .query_time_text("tenant", "window", serde_json::from_value(request).unwrap())
            .await
            .unwrap();
        let embedded: Value =
            serde_json::from_str(&serde_json::to_string(&embedded).unwrap()).unwrap();
        assert_eq!(embedded["hits"], body["hits"]);
    }
    assert_eq!(
        call("outside", triple.clone()).await.0,
        StatusCode::FORBIDDEN
    );
    assert!(
        sdk.query_time_text(
            "outside",
            "window",
            serde_json::from_value(triple.clone()).unwrap()
        )
        .await
        .is_err()
    );

    let mut empty = text.clone();
    empty["recall"]["time"] = json!({"from_ms":0,"to_ms":1});
    empty["query"] = json!("unmatched");
    let (status, body) = call("tenant", empty).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["hits"], json!([]));

    let maximum =
        json!({"constant":1,"time_weight":1.2e38,"keyword_weight":1.2e38,"vector_weight":1.2e38});
    let mut finite = text.clone();
    finite["rrf"] = maximum.clone();
    assert_eq!(call("tenant", finite).await.0, StatusCode::OK);
    let mut overflow = triple.clone();
    overflow["rrf"] = maximum;
    let mut over_budget = text.clone();
    over_budget["filter"] = json!([{"StringSet":{"field":"tags","filter":vec!["broad";128]}}]);
    let mut unsupported = triple;
    unsupported["query"] = json!("\"match other\"");
    for (request, message) in [
        (overflow, "maximum fused score"),
        (over_budget, "query work budget exhausted"),
        (unsupported, "single positive analyzed term"),
    ] {
        let (status, body) = call("tenant", request.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body["message"].as_str().unwrap().contains(message),
            "{body}"
        );
        assert!(body.get("hits").is_none());
        assert!(
            sdk.query_time_text("tenant", "window", serde_json::from_value(request).unwrap())
                .await
                .is_err()
        );
    }

    let entry = service
        .catalog
        .collection_entry("tenant", "window")
        .await
        .unwrap();
    let partition = root.join(entry.directory());
    let gate = partition.join(".test-lexical-consumer-pause");
    std::fs::write(&gate, b"pause").unwrap();
    let (status, ingested) = namespace_api(
        &app,
        "POST",
        "/api/v1/index/window/ingest",
        "tenant",
        Some(json!({"records":[mixed_row("publish", "tail", None)]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ingested}");
    assert_eq!(ingested["results"][0]["receipt"]["seq"], 17);
    wait_marker(&partition.join(".test-lexical-consumer-started")).await;
    let busy = call("tenant", text.clone()).await;
    let embedded = sdk
        .query_time_text("tenant", "window", serde_json::from_value(text).unwrap())
        .await;
    std::fs::remove_file(gate).unwrap();
    assert_eq!(busy.0, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(busy.1["code"], "busy");
    assert!(busy.1.get("hits").is_none());
    assert_eq!(embedded.unwrap_err().code(), "busy");
    drop(sdk);
    drop(app);
    service.shutdown().wait().await.unwrap();
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

    let (status, error) = window(&app, bounded_request()).await;
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
    assert_eq!(
        build(&app, tenant_headers, "window").await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        build(&app, HeaderMap::new(), "window").await.0,
        StatusCode::UNAUTHORIZED
    );
    let (status, result) = build(&app, master_headers(), "window").await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["indexed_seq"], 6);
    for (name, value) in [
        ("query", json!("match")),
        ("query_vector", json!([1, 0])),
        ("rrf", json!({})),
    ] {
        let mut request = bounded_request();
        request[name] = value;
        assert_eq!(window(&app, request).await.0, StatusCode::BAD_REQUEST);
    }
    let mut empty = bounded_request();
    empty["temporal"]["event"]["to_ms"] = json!(150);
    assert_eq!(window(&app, empty).await.0, StatusCode::BAD_REQUEST);

    tokio::time::sleep(Duration::from_millis(
        expiry.saturating_sub(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        ) + 10,
    ))
    .await;
    let (status, result) = window(&app, bounded_request()).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["visible_seq"], 6);
    assert_eq!(result["hits"].as_array().unwrap().len(), 1);
    assert_eq!(result["hits"][0]["record_id"], "retained");
    assert_eq!(result["hits"][0]["document"], json!({"body":"match"}));
    let mut request = bounded_request();
    let ingested = result["hits"][0]["ingested_at_ms"].as_i64().unwrap();
    request["temporal"]["ingested"] = json!({"from_ms":ingested,"to_ms":ingested+1});
    let selected = window(&app, request.clone()).await;
    assert_eq!(selected.0, StatusCode::OK, "{:?}", selected.1);
    assert_eq!(selected.1["hits"], result["hits"]);
    request["temporal"]["valid_at_ms"] = json!(200);
    let expired_validity = window(&app, request).await;
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
        let (status, replayed) = window(&app, bounded_request()).await;
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
            let (status, exhausted) = window(&app, bounded_request()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{exhausted}");
            assert_eq!(exhausted["code"], "event_time_budget");
            assert!(
                exhausted["message"]
                    .as_str()
                    .unwrap()
                    .contains("row budget")
            );
            let (status, refreshed) = build(&app, master_headers(), "window").await;
            assert_eq!(status, StatusCode::OK, "{refreshed}");
            assert!(refreshed["indexed_seq"].as_u64().unwrap() >= 135);
            let current = window(&app, bounded_request()).await;
            assert_eq!(current.0, StatusCode::OK, "{:?}", current.1);
            assert_eq!(current.1["hits"], result["hits"]);
            let compacted =
                namespace_api(&app, "PATCH", "/api/v1/index/window", "tenant", None).await;
            assert_eq!(compacted.0, StatusCode::OK, "{:?}", compacted.1);
            let removed = window(&app, bounded_request()).await;
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
    let missing = window(&app, bounded_request()).await;
    assert_eq!(missing.0, StatusCode::BAD_REQUEST, "{:?}", missing.1);
    assert_eq!(missing.1["code"], "event_time_unavailable");
    let (status, checkpoint) = build(&app, master_headers(), "window").await;
    assert_eq!(status, StatusCode::OK, "{checkpoint}");
    let recovered = window(&app, bounded_request()).await;
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

#[tokio::test]
async fn event_time_http_query_busy_keeps_its_error_header() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (service, root, headers) = super::tests::setup().await;
    let app = router(service.clone());
    let entry = service
        .catalog
        .collection_entry("tenant", "c")
        .await
        .unwrap();
    let partition = root.join(entry.directory());
    let (status, written) = namespace_api(
        &app,
        "POST",
        "/api/v1/index/c/batch",
        "tenant",
        Some(json!({"writes":[row("dated", 150, None, None)]})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{written}");
    assert_eq!(
        build(&app, master_headers(), "c").await,
        (StatusCode::OK, json!({"indexed_seq":1}))
    );

    let gate = partition.join(".test-event-time-delivery-pause");
    std::fs::write(&gate, b"pause").unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let network_app = app.clone();
    let transport = tokio::spawn(async move {
        axum::serve(listener, network_app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let query = tokio::spawn(async move {
        let body = r#"{"temporal":{"event":{"from_ms":150,"to_ms":151}}}"#;
        let request = format!(
            "POST /api/v1/index/c/event-time/query HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\napikey: {}\r\nX-AnySeek-Tenant: tenant\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            headers["apikey"].to_str().unwrap(),
            body.len()
        );
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    });
    let started = partition.join(".test-event-time-delivery-started");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    // Replace the selected base at the same sequence between capture and delivery.
    assert_eq!(
        build(&app, master_headers(), "c").await,
        (StatusCode::OK, json!({"indexed_seq":1}))
    );
    std::fs::remove_file(gate).unwrap();
    let response = tokio::time::timeout(Duration::from_secs(5), query)
        .await
        .unwrap()
        .unwrap();
    let (headers, body) = response.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("HTTP/1.1 429 "), "{response}");
    assert!(
        headers
            .lines()
            .any(|header| header == "x-anyseek-error-code: busy"),
        "{response}"
    );
    assert!(
        body.contains(r#""code":"busy""#) && body.contains(r#""message":"Busy""#),
        "{response}"
    );
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), transport)
        .await
        .unwrap()
        .unwrap();
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[derive(Clone)]
enum CheckpointClient {
    Http(Router),
    Sdk(Arc<local::LocalEngine>),
}

impl CheckpointClient {
    async fn checkpoint(&self) -> Result<u64, (String, String)> {
        match self {
            Self::Http(app) => {
                let (status, body) = build(app, master_headers(), "c").await;
                if status == StatusCode::OK {
                    return Ok(body["indexed_seq"].as_u64().unwrap());
                }
                let code = body["code"].as_str().unwrap();
                assert_eq!(
                    status,
                    match code {
                        "busy" => StatusCode::TOO_MANY_REQUESTS,
                        "outcome_unknown" => StatusCode::SERVICE_UNAVAILABLE,
                        _ => panic!("unexpected checkpoint error {body}"),
                    }
                );
                Err((code.into(), body["message"].as_str().unwrap().into()))
            }
            Self::Sdk(engine) => engine
                .build_event_time_projection("tenant", "c")
                .await
                .map_err(|error| (error.code().into(), error.to_string())),
        }
    }

    async fn current_cut(&self) -> Result<u64, String> {
        let request = json!({"temporal":{"event":{"from_ms":150,"to_ms":151}}});
        match self {
            Self::Http(app) => {
                let (status, body) = decode_response(
                    namespace_response(
                        app,
                        "POST",
                        "/api/v1/index/c/event-time/query",
                        "tenant",
                        Some(request),
                    )
                    .await,
                )
                .await;
                if status == StatusCode::OK {
                    Ok(body["visible_seq"].as_u64().unwrap())
                } else {
                    Err(body["code"].as_str().unwrap().into())
                }
            }
            Self::Sdk(engine) => engine
                .query_event_time("tenant", "c", serde_json::from_value(request).unwrap())
                .await
                .map(|result| result.visible_seq)
                .map_err(|error| error.code().into()),
        }
    }
}

#[tokio::test]
async fn event_time_checkpoint_wait_distinguishes_http_and_sdk_admission() {
    for use_sdk in [false, true] {
        let (mut service, root, _) = super::tests::setup().await;
        Arc::get_mut(&mut service).unwrap().request_timeout = Duration::from_secs(2);
        let app = router(service.clone());
        let client = if use_sdk {
            CheckpointClient::Sdk(Arc::new(local::LocalEngine::from_test_service(
                service.clone(),
            )))
        } else {
            CheckpointClient::Http(app.clone())
        };

        let partition = root.join(
            service
                .catalog
                .collection_entry("tenant", "c")
                .await
                .unwrap()
                .directory(),
        );
        let (status, body) = namespace_api(
            &app,
            "POST",
            "/api/v1/index/c/batch",
            "tenant",
            Some(json!({"writes":[row("old",150,None,None)]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        // Catalog work owns its task, even before the Record worker is admitted.
        let (reached, release) = service.cold.test.event_time_admission.pause_next();
        let caller = client.clone();
        let request = tokio::spawn(async move { caller.checkpoint().await });
        tokio::time::timeout(Duration::from_secs(5), reached)
            .await
            .unwrap()
            .unwrap();
        let (code, message) = request.await.unwrap().unwrap_err();
        assert_eq!(code, "busy");
        assert!(!message.contains("operation identity"), "{message}");
        assert!(
            message.contains("Query the current event-time projection"),
            "{message}"
        );
        if !use_sdk {
            assert!(message.contains("before checkpoint admission"), "{message}");
        }
        release.send(()).unwrap();
        assert_eq!(
            client.current_cut().await,
            Err("event_time_unavailable".into())
        );

        // Only this new explicit invocation installs the first projection.
        assert_eq!(client.checkpoint().await, Ok(1));
        let previous = std::fs::read_dir(&partition)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| path.join("event-time/descriptor").exists())
            .unwrap();
        let (status, body) = namespace_api(
            &app,
            "POST",
            "/api/v1/index/c/batch",
            "tenant",
            Some(json!({"writes":[row("recent",151,None,None)]})),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        // Hold private image construction after the actual worker was admitted.
        // The live authority and old projection remain independently readable.
        let gate = partition.join(".test-anchor-image-io-pause");
        std::fs::write(&gate, b"pause").unwrap();
        let caller = client.clone();
        let request = tokio::spawn(async move { caller.checkpoint().await });
        let started = partition.join(".test-anchor-image-io-started");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !started.exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let (code, message) = request.await.unwrap().unwrap_err();
        assert_eq!(code, "outcome_unknown");
        assert!(!message.contains("operation identity"), "{message}");
        assert!(message.contains("The build may continue"), "{message}");
        assert!(
            message.contains("Query the current event-time projection first"),
            "{message}"
        );
        assert!(
            message.contains("does not confirm this refresh"),
            "{message}"
        );
        if !use_sdk {
            assert!(message.contains("after checkpoint admission"), "{message}");
        }
        assert_eq!(client.current_cut().await, Ok(2));
        assert!(previous.join("event-time/descriptor").exists());

        std::fs::remove_file(gate).unwrap();
        drop(app);
        tokio::time::timeout(Duration::from_secs(10), async move {
            match client {
                CheckpointClient::Sdk(engine) => {
                    drop(service);
                    engine.shutdown().await.unwrap();
                }
                CheckpointClient::Http(app) => {
                    drop(app);
                    service.shutdown().wait().await.unwrap();
                }
            }
        })
        .await
        .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
