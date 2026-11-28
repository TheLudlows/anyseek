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

tokio::task_local! { static PUBLICATION_WAIT: Arc<tokio::sync::Notify>; }

pub(super) async fn observe_publication_wait<T>(publication: impl Future<Output = T>) -> T {
    tokio::pin!(publication);
    match futures::poll!(&mut publication) {
        Poll::Ready(result) => result,
        Poll::Pending => {
            let _ = PUBLICATION_WAIT.try_with(|reached| reached.notify_one());
            publication.await
        }
    }
}

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
    assert_eq!(input[3]["$ref"], "#/components/schemas/TimeAnnRequest");
    assert_eq!(input[4]["$ref"], "#/components/schemas/TimeEntityRequest");
    let schemas = &doc["components"]["schemas"];
    assert_eq!(schemas["TimeEntityRecall"]["properties"]["entity"]["$ref"], "#/components/schemas/EntityPathRequest");
    assert_eq!(schemas["TimeEntityRequest"]["properties"]["all"]["const"], true);
    assert!(schemas["TimeEntityRequest"]["properties"].get("nprobe").is_none());
    assert_eq!(schemas["TimeEntityRrfOptions"]["properties"]["entity_weight"]["type"], "number");
    let ann = &doc["components"]["schemas"]["TimeAnnRequest"];
    assert_eq!(ann["properties"]["nprobe"]["minimum"], 1);
    assert_eq!(ann["properties"]["nprobe"]["maximum"], MAX_MIXED_NPROBE);
    assert_eq!(ann["additionalProperties"], false);
    assert!(ann["properties"].get("all").is_none());
    let output = &query["responses"]["200"]["content"]["application/json"]["schema"]["oneOf"];
    assert_eq!(
        output[0]["properties"]["diagnostics"]["$ref"],
        "#/components/schemas/QueryDiagnostics"
    );
    assert_eq!(output[1]["$ref"], "#/components/schemas/TimeVectorResult");
    let channels = &doc["components"]["schemas"]["TimeVectorResult"]["properties"]["hits"]
        ["items"]["properties"]["channels"];
    assert_eq!(channels["maxItems"], 4);
    assert_eq!(channels["items"]["properties"]["channel"]["enum"], json!(["time", "lexical", "vector", "relation"]));
    assert_eq!(
        channels["items"]["properties"]["rank_basis"]["enum"],
        json!(["window_time", "score", "stable_adjacency"])
    );
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

fn ann_request() -> Value {
    let mut request = mixed_request();
    request.as_object_mut().unwrap().remove("all");
    request["nprobe"] = json!(1);
    request
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

async fn create_ann_fixture_collection(service: &Arc<Service>, id: &str, vector: Value) {
    let config = serde_json::from_value(json!({"index_name":id,"vector":vector,"schema":[
        {"field":"body","field_type":"Text","store":true},
        {"field":"private","field_type":"Text","store":false},
        {"field":"vector","field_type":"Json","index_vector":true},
        {"field":"tags","field_type":"StringSet","filterable":true}
    ]}))
    .unwrap();
    let mut config = record_config(config, Some("ann-fixture".into())).unwrap();
    // The public create route uses Auto. Use the same catalog creation and
    // storage lifecycle with two deterministic medoids for the oracle.
    config.index.clustering = Clustering::Fixed(2);
    let receipt = service
        .catalog
        .execute(CatalogRequest {
            operation_id: OperationId::new(format!("ann-create-{id}")),
            command: CatalogCommand::PrepareCollection {
                collection_id: id.into(),
                tenant_id: "tenant".into(),
                config,
            },
        })
        .await
        .unwrap();
    let entry = service
        .catalog
        .collections()
        .await
        .unwrap()
        .into_iter()
        .find(|entry| {
            entry.collection_id == id
                && entry.tenant_id == "tenant"
                && entry.generation == receipt.seq
        })
        .unwrap();
    service.finish_collection(entry).await.unwrap();
}

async fn request_mixed(
    app: &Router,
    id: &str,
    tenant: &str,
    request: Value,
) -> (StatusCode, Value) {
    decode_response(
        namespace_response(
            app,
            "POST",
            &format!("/api/v1/index/{id}/query"),
            tenant,
            Some(request),
        )
        .await,
    )
    .await
}

#[tokio::test]
async fn event_time_ann_opt_in_shares_a_cut_through_http_sdk_and_reopen() {
    const DIMENSIONS: usize = 256;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let service = open(&root).await;
    let app = router(service.clone());
    let sdk = local::LocalEngine::from_test_service(service.clone());
    create_ann_fixture_collection(&service, "window", json!({"dimensions":DIMENSIONS})).await;
    let embedding = |x: f32, y: f32| {
        let mut values = vec![0.; DIMENSIONS];
        values[0] = x;
        values[1] = y;
        json!(values)
    };
    let mut request = ann_request();
    request["query_vector"] = embedding(1., 0.);
    request["fields"] = json!(["body", "private"]);
    let (_, missing) = request_mixed(&app, "window", "tenant", request.clone()).await;
    assert_eq!(missing["code"], "event_time_unavailable");
    let partition = service.record("tenant", "window").await.unwrap();
    for segment in 0..2 {
        for start in [0, 60] {
            let rows = (start..start + 60).map(|ordinal| {
                let id = format!("s{segment}-r{ordinal:03}");
                let winner = ordinal < 24;
                let mut row = mixed_row(&id, &id, None);
                let group = if winner { "winner" } else { "loser" };
                // Both needles are closer to the loser medoid than to the winner.
                // For the tilted query they outscore any winner's ordinary member.
                let vector = match (segment, ordinal) {
                    (0, 118) => embedding(0.45, 0.89302856),
                    (0, 119) => embedding(0.35, 0.9367497),
                    _ if winner => embedding(1., 0.),
                    _ => embedding(0., 1.),
                };
                row["temporal"]["event_time_ms"] = json!(match (segment, ordinal) {
                    (0, 0) => 20,
                    (0, 118) => 300,
                    _ => 100,
                });
                row["document"] = json!({"body": group,"private":"hidden","tags":["broad",group,id],
                    "vector":vector});
                row
            }).collect();
            write_mixed(&app, rows).await;
        }
        if segment == 0 {
            assert_eq!(
                build(&app, master_headers(), "window").await,
                (StatusCode::OK, json!({"indexed_seq":120}))
            );
        } else {
            assert_eq!(
                anyseek::record::tests::seal_test_cache_tail(&partition).await,
                2
            );
        }
    }
    let (status, response) = request_mixed(&app, "window", "tenant", request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{response}");
    assert_eq!(response["visible_seq"], 240);
    assert!(response["count_total"].is_null());
    assert_eq!(
        response["hits"][0]["record"]["document"],
        json!({"body":"winner"})
    );
    assert_eq!(response["hits"][0]["record"]["record_id"], "s0-r000");
    assert_eq!(response["hits"][1]["record"]["record_id"], "s0-r001");
    assert_eq!(response["hits"][0]["channels"][1]["raw_score"], 1.0);
    let embed =
        |value| sdk.query_time_ann("tenant", "window", serde_json::from_value(value).unwrap());
    let embedded = embed(request.clone()).await.unwrap();
    let embedded: Value = serde_json::from_str(&serde_json::to_string(&embedded).unwrap()).unwrap();
    assert_eq!(embedded["visible_seq"], response["visible_seq"]);
    assert_eq!(embedded["hits"], response["hits"]);

    let tags = |values: Vec<&str>| json!([{"StringSet":{"field":"tags","filter":values}}]);
    let mut exact = request.clone();
    exact.as_object_mut().unwrap().remove("nprobe");
    exact["all"] = json!(true);
    exact["filter"] = tags(vec!["winner"]);
    let (status, exact_winners) = request_mixed(&app, "window", "tenant", exact.clone()).await;
    assert_eq!(status, StatusCode::OK, "{exact_winners}");
    assert_eq!(exact_winners["hits"], response["hits"]);
    let mut losers = request.clone();
    losers["filter"] = tags(vec!["loser"]);
    let (status, pruned) = request_mixed(&app, "window", "tenant", losers.clone()).await;
    assert_eq!(status, StatusCode::OK, "{pruned}");
    assert_eq!(pruned["hits"], json!([]));
    let mut all_clusters = losers.clone();
    all_clusters["nprobe"] = json!(2);
    let (status, probed) = request_mixed(&app, "window", "tenant", all_clusters).await;
    assert_eq!(status, StatusCode::OK, "{probed}");
    exact["filter"] = tags(vec!["loser"]);
    let (status, exact_losers) = request_mixed(&app, "window", "tenant", exact).await;
    assert_eq!(status, StatusCode::OK, "{exact_losers}");
    assert_eq!(probed["hits"], exact_losers["hits"]);
    assert_eq!(probed["hits"].as_array().unwrap().len(), 2);
    losers["nprobe"] = json!(MAX_MIXED_NPROBE);
    let (status, capped) = request_mixed(&app, "window", "tenant", losers).await;
    assert_eq!(status, StatusCode::OK, "{capped}");
    assert_eq!(capped["hits"], exact_losers["hits"]);

    // Compare only vector-lane ranks first, without a matching time window.
    // The tilted query prefers the winning medoid (0.8 vs 0.6), while
    // two members of the other cluster have a cosine greater than 0.8.
    let mut approximate = request.clone();
    approximate["query_vector"] = embedding(0.8, 0.6);
    approximate["recall"]["time"] = json!({"from_ms":500,"to_ms":501});
    let mut exhaustive = approximate.clone();
    exhaustive.as_object_mut().unwrap().remove("nprobe");
    exhaustive["all"] = json!(true);
    let (status, exact_top) = request_mixed(&app, "window", "tenant", exhaustive.clone()).await;
    assert_eq!(status, StatusCode::OK, "{exact_top}");
    let (status, ann_top) = request_mixed(&app, "window", "tenant", approximate.clone()).await;
    assert_eq!(status, StatusCode::OK, "{ann_top}");
    assert_eq!(exact_top["visible_seq"], ann_top["visible_seq"]);
    for (body, ids) in [
        (&exact_top, ["s0-r118", "s0-r119"]),
        (&ann_top, ["s0-r000", "s0-r001"]),
    ] {
        for (rank, id) in ids.into_iter().enumerate() {
            let hit = &body["hits"][rank];
            assert_eq!(hit["record"]["record_id"], id, "{body}");
            let channels = hit["channels"].as_array().unwrap();
            assert_eq!(channels.len(), 1);
            assert_eq!(channels[0]["channel"], "vector");
            assert_eq!(channels[0]["rank"], rank);
        }
    }

    let approximate_vector_only = approximate.clone();
    // Now one omitted vector hit can only reach the page through time.
    approximate["recall"]["time"] = json!({"from_ms":300,"to_ms":301});
    approximate["length"] = json!(3);
    approximate["rrf"] = json!({"constant":1,"time_weight":2});
    let (status, rescued) = request_mixed(&app, "window", "tenant", approximate.clone()).await;
    assert_eq!(status, StatusCode::OK, "{rescued}");
    assert_eq!(rescued["visible_seq"], ann_top["visible_seq"]);
    let hits = rescued["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0]["record"]["record_id"], "s0-r118");
    let rescued_channels = hits[0]["channels"].as_array().unwrap();
    assert_eq!(rescued_channels.len(), 1);
    assert_eq!(rescued_channels[0]["channel"], "time");
    assert_eq!(rescued_channels[0]["rank"], 0);
    assert!(rescued_channels[0]["raw_score"].is_null());
    assert_eq!(rescued_channels[0]["rrf_contribution"], 2.0);
    assert_eq!(hits[0]["score"], 2.0);
    assert!(hits.iter().all(|hit| hit["record"]["record_id"] != "s0-r119"));
    assert_eq!(hits[1]["record"]["record_id"], "s0-r000");
    assert_eq!(hits[2]["record"]["record_id"], "s0-r001");
    let rescued_sdk = embed(approximate).await.unwrap();
    let rescued_sdk: Value =
        serde_json::from_str(&serde_json::to_string(&rescued_sdk).unwrap()).unwrap();
    assert_eq!(rescued_sdk["hits"], rescued["hits"]);

    let mut triple = request.clone();
    triple["query"] = json!("winner");
    let (status, triple_http) = request_mixed(&app, "window", "tenant", triple.clone()).await;
    assert_eq!(status, StatusCode::OK, "{triple_http}");
    assert_eq!(triple_http["visible_seq"], 240);
    let triple_sdk = embed(triple).await.unwrap();
    let channels = triple_sdk.hits[0]
        .channels
        .iter()
        .map(|hit| hit.channel.as_str())
        .collect::<Vec<_>>();
    assert_eq!(channels, ["time", "lexical", "vector"]);
    let triple_sdk: Value =
        serde_json::from_str(&serde_json::to_string(&triple_sdk).unwrap()).unwrap();
    assert_eq!(triple_sdk["hits"], triple_http["hits"]);

    let mut expensive = request.clone();
    expensive["filter"] = tags(vec!["broad"; 128]);
    let (status, refused) = request_mixed(&app, "window", "tenant", expensive.clone()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refused}");
    assert!(
        refused["message"]
            .as_str()
            .unwrap()
            .contains("shared query work budget exhausted")
    );
    assert!(refused.get("hits").is_none());
    assert_eq!(embed(expensive).await.unwrap_err().code(), "rejected");

    assert_eq!(
        request_mixed(&app, "window", "outside", request.clone())
            .await
            .0,
        StatusCode::FORBIDDEN
    );
    assert!(
        sdk.query_time_ann(
            "outside",
            "window",
            serde_json::from_value(request.clone()).unwrap()
        )
        .await
        .is_err()
    );
    drop(partition);
    drop(sdk);
    drop(app);
    service.shutdown().wait().await.unwrap();

    let service = open(&root).await;
    let sdk = local::LocalEngine::from_test_service(service.clone());
    let reopened = sdk
        .query_time_ann(
            "tenant",
            "window",
            serde_json::from_value(request.clone()).unwrap(),
        )
        .await
        .unwrap();
    let reopened: Value = serde_json::from_str(&serde_json::to_string(&reopened).unwrap()).unwrap();
    assert_eq!(reopened["visible_seq"], 240);
    assert_eq!(reopened["hits"], response["hits"]);

    // Exercise the same protected response over an actual loopback connection.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let app = router(service.clone());
    let network_app = app.clone();
    let server = tokio::spawn(async move {
        axum::serve(listener, network_app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = request.to_string();
    let request = format!(
        "POST {MIXED_QUERY} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\napikey: {}\r\nX-AnySeek-Tenant: tenant\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        master_headers()["apikey"].to_str().unwrap(),
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut network = String::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut network))
        .await.unwrap().unwrap();
    let (headers, body) = network.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("HTTP/1.1 200 "), "{headers}");
    assert!(!headers.contains(ERROR_CODE_HEADER), "{headers}");
    let body = if headers.lines().any(|line| line == "transfer-encoding: chunked") {
        // The protected response encodes the JSON as one guarded data frame.
        let (length, remaining) = body.split_once("\r\n").unwrap();
        let length = usize::from_str_radix(length, 16).unwrap();
        assert_eq!(&remaining[length..], "\r\n0\r\n\r\n");
        &remaining[..length]
    } else {
        body
    };
    let network: Value = serde_json::from_str(body).unwrap();
    assert_eq!(network["visible_seq"], 240);
    assert_eq!(network["hits"], response["hits"]);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server).await.unwrap().unwrap();

    // A real full compaction replaces the selected root. Rebuild the projection
    // against that root and compare exact ranks as the logical control.
    let (status, compacted) =
        namespace_api(&app, "PATCH", "/api/v1/index/window", "tenant", None).await;
    assert_eq!(status, StatusCode::OK, "{compacted}");
    assert_eq!(
        build(&app, master_headers(), "window").await,
        (StatusCode::OK, json!({"indexed_seq":240}))
    );
    let (status, exact_after) = request_mixed(&app, "window", "tenant", exhaustive).await;
    assert_eq!(status, StatusCode::OK, "{exact_after}");
    assert_eq!(exact_after["visible_seq"], exact_top["visible_seq"]);
    assert_eq!(exact_after["hits"], exact_top["hits"]);
    let (status, ann_after) =
        request_mixed(&app, "window", "tenant", approximate_vector_only).await;
    assert_eq!(status, StatusCode::OK, "{ann_after}");
    assert_eq!(ann_after["visible_seq"], exact_after["visible_seq"]);
    let ann_ids = |body: &Value| {
        body["hits"].as_array().unwrap().iter()
            .map(|hit| hit["record"]["record_id"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    eprintln!(
        "ANN top-2 before compaction: {:?}; after compaction: {:?}; exact: {:?}",
        ann_ids(&ann_top),
        ann_ids(&ann_after),
        ann_ids(&exact_after),
    );

    drop(sdk);
    drop(app);
    service.shutdown().wait().await.unwrap();
}

#[tokio::test]
async fn event_time_ann_rejects_ambiguous_requests_and_unsupported_layouts() {
    let temp = tempfile::tempdir().unwrap();
    let service = open(&temp.path().join("catalog")).await;
    let app = router(service.clone());
    let sdk = local::LocalEngine::from_test_service(service.clone());
    create_mixed_collection(&app).await;
    for (key, value) in [
        ("all", json!(true)),
        ("all", json!(false)),
        ("all", json!(null)),
        ("nprobe", json!(0)),
        ("nprobe", json!(MAX_MIXED_NPROBE + 1)),
        ("nprobe", json!(1.5)),
        ("nprobe", json!("1")),
        ("nprobe", json!(null)),
        ("query", json!(null)),
        ("count", json!(true)),
    ] {
        let mut request = ann_request();
        request[key] = value;
        let (status, error) = request_mixed(&app, "window", "tenant", request).await;
        assert_eq!(
            (status, error["code"].as_str()),
            (StatusCode::BAD_REQUEST, Some("rejected")),
            "{key}: {error}"
        );
        assert!(error.get("hits").is_none());
    }
    for nprobe in [0, MAX_MIXED_NPROBE + 1] {
        let mut request: TimeAnnRequest = serde_json::from_value(ann_request()).unwrap();
        request.nprobe = nprobe;
        assert_eq!(
            sdk.query_time_ann("tenant", "window", request)
                .await
                .unwrap_err()
                .code(),
            "rejected"
        );
    }
    for (id, vector) in [
        ("dot", json!({"dimensions":2,"metric":"Dot"})),
        ("euclidean", json!({"dimensions":2,"metric":"Euclidean"})),
        (
            "quantized",
            json!({"dimensions":2,"quantization":"ScalarQuantizationI8"}),
        ),
        ("i8", json!({"dimensions":2,"precision":"I8"})),
    ] {
        create_ann_fixture_collection(&service, id, vector).await;
        let (status, error) = request_mixed(&app, id, "tenant", ann_request()).await;
        assert_eq!(
            (status, error["code"].as_str()),
            (StatusCode::BAD_REQUEST, Some("rejected")),
            "{id}: {error}"
        );
        assert!(
            error["message"]
                .as_str()
                .unwrap()
                .contains("unquantized F32 cosine")
        );
        assert_eq!(
            sdk.query_time_ann("tenant", id, serde_json::from_value(ann_request()).unwrap())
                .await
                .unwrap_err()
                .code(),
            "rejected"
        );
    }
    drop(sdk);
    drop(app);
    service.shutdown().wait().await.unwrap();
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
async fn event_time_lexical_snapshot_stays_at_the_selected_cut() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let service = open(&root).await;
    let app = router(service.clone());
    let sdk = Arc::new(local::LocalEngine::from_test_service(service.clone()));
    create_mixed_collection(&app).await;

    let request = json!({"recall":{"time":{"from_ms":0,"to_ms":1}},"query":"sentinel","length":4});
    let mut row = mixed_row("original", "row", None);
    row["document"]["body"] = json!("sentinel original");
    write_mixed(&app, vec![row]).await;
    assert_eq!(
        build(&app, master_headers(), "window").await,
        (StatusCode::OK, json!({"indexed_seq":1}))
    );
    let partition = service.record("tenant", "window").await.unwrap();
    let generation = service.catalog.collection_entry("tenant", "window").await.unwrap();
    let record_dir = root.join(generation.directory());
    let gate = record_dir.join(".test-event-time-lexical-worker-pause");
    let entered = record_dir.join(".test-event-time-lexical-worker-pause-entered");

    for (use_sdk, version) in [(false, 2), (true, 3)] {
        std::fs::write(&gate, b"pause").unwrap();
        let request_copy = request.clone();
        let pending = if use_sdk {
            let reader = sdk.clone();
            tokio::spawn(async move {
                reader
                    .query_time_text("tenant", "window", serde_json::from_value(request_copy).unwrap())
                    .await
                    .unwrap_err()
                    .code()
                    .to_owned()
            })
        } else {
            let router = app.clone();
            tokio::spawn(async move {
                let (status, body) = request_mixed(&router, "window", "tenant", request_copy).await;
                assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
                assert!(body.get("hits").is_none(), "{body}");
                body["code"].as_str().unwrap().to_owned()
            })
        };
        wait_marker(&entered).await;
        let mut update = mixed_row(&format!("patch-{version}"), "row", None);
        update["expected"] = json!({"Match":version - 1});
        update["document"]["body"] = json!(format!("sentinel version {version}"));
        write_mixed(&app, vec![update]).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let progress = partition.committed_progress().await;
                assert!(!progress.recovery_required);
                if progress.committed_seq == Some(version) && progress.lexical_seq == version
                    && progress.vector_seq == version
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.unwrap();
        std::fs::remove_file(&gate).unwrap();
        std::fs::remove_file(&entered).unwrap();
        assert_eq!(pending.await.unwrap(), "busy", "sdk={use_sdk}, cut={version}");

        let fresh = if use_sdk {
            let output = sdk
                .query_time_text("tenant", "window", serde_json::from_value(request.clone()).unwrap())
                .await
                .unwrap();
            serde_json::to_value(output).unwrap()
        } else {
            let (status, body) = request_mixed(&app, "window", "tenant", request.clone()).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            body
        };
        assert_eq!(fresh["visible_seq"], version);
        assert_eq!(fresh["hits"].as_array().unwrap().len(), 1);
        assert_eq!(fresh["hits"][0]["record"]["record_id"], "row");
        assert_eq!(fresh["hits"][0]["record"]["version"], version);
    }
    assert_eq!(request_mixed(&app, "window", "outside", request).await.0, StatusCode::FORBIDDEN);
    drop(partition);
    drop(sdk);
    drop(app);
    service.shutdown().wait().await.unwrap();
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
    for ann in [false, true] {
        check_vector_publication_timeout(ann).await;
    }
}

async fn check_vector_publication_timeout(ann: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    let service = open(&root).await;
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

    let request = if ann { ann_request() } else { mixed_request() };
    let http_wait = Arc::new(tokio::sync::Notify::new());
    let http_read = PUBLICATION_WAIT.scope(
        http_wait.clone(),
        namespace_response(&app, "POST", MIXED_QUERY, "tenant", Some(request.clone())),
    );
    let sdk_wait = Arc::new(tokio::sync::Notify::new());
    let sdk_read = PUBLICATION_WAIT.scope(sdk_wait.clone(), async {
        if ann {
            sdk.query_time_ann("tenant", "window", serde_json::from_value(request).unwrap())
                .await
        } else {
            sdk.query_time_vector("tenant", "window", serde_json::from_value(request).unwrap())
                .await
        }
    });
    let expire_reads = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(http_wait.notified(), sdk_wait.notified());
        })
        .await
        .expect("HTTP and SDK must both reach the publication lock");

        // Setup uses the normal service deadline. Advance only after both
        // queries are blocked in the actual publication wait.
        tokio::time::pause();
        tokio::time::advance(service.request_timeout).await;
        tokio::time::resume();
    };
    let (response, sdk_result, ()) = tokio::join!(http_read, sdk_read, expire_reads);
    let (status, error) = decode_response(response).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{error}");
    assert_eq!(error["code"], "busy");
    assert!(!error["message"].as_str().unwrap().contains("operation"));
    assert!(error.get("operation_id").is_none());
    assert!(error.get("hits").is_none());
    let error = sdk_result.unwrap_err();
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
        ("lexical", 90, "other", None),
        ("vector", 91, "other", Some(json!([0, 1]))),
    ] {
        let mut row = mixed_row(id, id, None);
        row["temporal"]["event_time_ms"] = json!(time);
        row["document"] = json!({"body":body,"private":"hidden","tags":["broad",id]});
        if matches!(id, "both" | "lexical") {
            row["document"]["private"] = json!("match");
        }
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
    let mut vector_budget = mixed_request();
    vector_budget["filter"] = over_budget["filter"].clone();
    let (status, error) = call("tenant", vector_budget.clone()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("query work budget exhausted")
    );
    assert!(error.get("hits").is_none());
    let error = sdk
        .query_time_vector(
            "tenant",
            "window",
            serde_json::from_value(vector_budget).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "rejected");
    let mut unsupported = triple;
    unsupported["query"] = json!("\"match other\"");
    let mut punctuation = text.clone();
    punctuation["query"] = json!("!!!");
    let mut only_exclusion = text.clone();
    only_exclusion["query"] = json!("-match");
    only_exclusion["rrf"] = json!({"keyword_weight":0});
    for (request, message) in [
        (overflow, "maximum fused score"),
        (over_budget, "query work budget exhausted"),
        (unsupported, "single positive analyzed term"),
        (punctuation, "single positive analyzed term"),
        (only_exclusion, "single positive analyzed term"),
    ] {
        let (status, body) = call("tenant", request.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            body["message"].as_str().unwrap().contains(message),
            "{body}"
        );
        assert!(body.get("hits").is_none());
        let error = sdk
            .query_time_text("tenant", "window", serde_json::from_value(request).unwrap())
            .await
            .unwrap_err();
        assert_eq!(error.code(), "rejected");
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
async fn event_time_zero_weight_candidates_do_not_fence_http_or_sdk_delivery() {
    // Cover the time-side and retrieval-side evidence independently.
    for (zero_time, ann) in [(false, false), (true, false), (false, true)] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("catalog");
        let service = open(&root).await;
        let app = router(service.clone());
        let sdk = Arc::new(local::LocalEngine::from_test_service(service.clone()));
        create_mixed_collection(&app).await;
        let partition = service.record("tenant", "window").await.unwrap();
        let (clock, now) = anyseek::record::tests::test_clock(&partition).await;
        let expiry = now + 60_000;
        let mut keep = mixed_row("keep", "keep", None);
        let mut expires = mixed_row("expires", "expires", Some(expiry));
        keep["document"] = json!({"body":"match"});
        expires["document"]["body"] = json!("other");
        if zero_time {
            keep["temporal"]["event_time_ms"] = json!(100);
            keep["document"]["vector"] = json!([1, 0]);
            expires["document"]
                .as_object_mut()
                .unwrap()
                .remove("vector");
        } else {
            expires["temporal"]["event_time_ms"] = json!(100);
        }
        write_mixed(&app, vec![keep, expires]).await;
        assert_eq!(
            build(&app, master_headers(), "window").await,
            (StatusCode::OK, json!({"indexed_seq":2}))
        );

        let muted = if zero_time {
            "time_weight"
        } else {
            "vector_weight"
        };
        let mut positive = if ann { ann_request() } else { mixed_request() };
        positive["query"] = json!("match");
        positive["length"] = json!(1);
        positive["rrf"] =
            json!({"constant":2,"time_weight":2,"keyword_weight":3,"vector_weight":2});
        positive["rrf"][muted] = json!(0.25);
        let mut zero = positive.clone();
        zero["rrf"][muted] = json!(0);
        let routes = &app;
        let before = |request| async move {
            namespace_response(routes, "POST", MIXED_QUERY, "tenant", Some(request)).await
        };
        let (status, body) = decode_response(before(positive.clone()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["hits"][0]["record"]["record_id"], "keep");
        assert_eq!(body["hits"][0]["score"], 2.5);
        // The expiring, positively weighted candidate does not fit on the
        // first page, but its rank evidence is still part of delivery.
        let positive_http = before(positive.clone()).await;
        let zero_http = before(zero.clone()).await;
        assert_eq!(positive_http.status(), StatusCode::OK);
        assert_eq!(zero_http.status(), StatusCode::OK);
        let gate = root.join(".test-event-read-handoff-pause");
        let entered = root.join(".test-event-read-handoff-started");
        std::fs::write(&gate, b"pause").unwrap();
        let start = |request| {
            let reader = sdk.clone();
            tokio::spawn(async move {
                if ann {
                    reader
                        .query_time_ann(
                            "tenant",
                            "window",
                            serde_json::from_value(request).unwrap(),
                        )
                        .await
                } else {
                    reader
                        .query_time_text(
                            "tenant",
                            "window",
                            serde_json::from_value(request).unwrap(),
                        )
                        .await
                }
            })
        };
        let zero_sdk = start(zero);
        wait_marker(&entered).await;
        std::fs::remove_file(&entered).unwrap();
        let positive_sdk = start(positive);
        wait_marker(&entered).await;
        clock.set(expiry + 1000);
        assert!(
            sdk.get("tenant", "window", "expires")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(partition.committed_progress().await.committed_seq, Some(2));
        clock.set(expiry - 7000);
        std::fs::remove_file(gate).unwrap();

        let (status, body) = decode_response(zero_http).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["hits"].as_array().unwrap().len(), 1);
        let hit = &body["hits"][0];
        assert_eq!(hit["record"]["record_id"], "keep");
        assert_eq!(hit["score"], 2.5);
        let explanations: Vec<_> = hit["channels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                (
                    item["channel"].as_str().unwrap(),
                    item["rrf_contribution"].as_f64().unwrap(),
                )
            })
            .collect();
        let expected = if zero_time {
            [("lexical", 1.5), ("vector", 1.0)]
        } else {
            [("time", 1.0), ("lexical", 1.5)]
        };
        assert_eq!(explanations, expected);
        let embedded = zero_sdk.await.unwrap().unwrap();
        assert_eq!(embedded.hits[0].record.record_id, "keep");
        assert_eq!(embedded.hits[0].score, 2.5);
        assert_eq!(positive_sdk.await.unwrap().unwrap_err().code(), "busy");

        let mut body = positive_http.into_body();
        use http_body::Body as _;
        assert!(matches!(
            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await,
            Some(Err(_))
        ));
        assert!(
            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none()
        );
        drop(body);
        drop(partition);
        drop(sdk);
        drop(app);
        service.shutdown().wait().await.unwrap();
    }
}

#[tokio::test]
async fn event_time_http_authority_reopen_and_work_limits() {
    async fn open_for_projection_damage(root: &FsPath) -> (Arc<Service>, Router) {
        let service = open(root).await;
        let app = router(service.clone());
        // Logical expiry is exercised by the query. Keep the periodic sweep
        // from replacing the selected projection while the test damages it.
        let sweep = service.reclamation.lock().unwrap().take().unwrap();
        sweep.abort();
        assert!(sweep.await.unwrap_err().is_cancelled());
        (service, app)
    }

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let (service, app) = open_for_projection_damage(&root).await;
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
        let (service, app) = open_for_projection_damage(&root).await;
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
    let (service, app) = open_for_projection_damage(&root).await;
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
