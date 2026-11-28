use super::*;
use crate::server::record_service::local::LocalEngine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MIX: &str = "/api/v1/index/graph/query";
const TENANT: &str = "tenant";
const PROOF_ROLES: [&str; 3] = ["seed", "edge", "e17-purge-reset"];

fn document(id: &str, body: &str) -> Value {
    let tags = PROOF_ROLES
        .into_iter()
        .filter(|role| *role != id)
        .map(|role| format!("except-{role}"))
        .chain(["allowed".to_owned()])
        .collect::<Vec<_>>();
    json!({"body":body,"private":"hidden","tags":tags})
}

fn declaration(first_edge: bool, expiry: Option<(&str, u64)>) -> Value {
    let mut doc = document("edge", "declare");
    let mut edges = vec![json!({"from":"app:A","type":"resolves","to":"app:C"})];
    if first_edge {
        edges.push(json!({"from":"app:A","type":"resolves","to":"app:B"}));
    }
    doc["relations"] = json!({"edges":edges});
    let mut row = json!({"record_id":"edge","document":doc,"temporal":{"valid_from_ms":30}});
    apply_expiry(&mut row, expiry);
    json!([row])
}

fn apply_expiry(row: &mut Value, expiry: Option<(&str, u64)>) {
    if let Some((role, expires)) = expiry
        && row["record_id"] == role
    {
        row["temporal"]["expire_at_ms"] = json!(expires);
    }
}

async fn initialize(app: &Router) {
    let (status, body) = namespace_api(
        app, "POST", "/api/v1/index", TENANT,
        Some(json!({
            "operation_id":OperationId::new("four-create".into()),"collection_id":"graph",
            "config":{"index_name":"graph","tokenizer":"TantivySimpleV1","vector":{"dimensions":2},"schema":[
                {"field":"body","field_type":"Text","store":true,"index_lexical":true},
                {"field":"private","field_type":"Text","store":false},
                {"field":"vector","field_type":"Json","index_vector":true},
                {"field":"entity_ids","field_type":"StringSet","store":true},
                {"field":"relations","field_type":"Json","store":true},
                {"field":"tags","field_type":"StringSet","filterable":true}
            ]}
        })),
    ).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

async fn populate(app: &Router, expiry: Option<(&str, u64)>, wide_seed: bool) -> u64 {
    replace(app, TENANT, 0, declaration(true, expiry)).await;
    let rows = [
        ("seed", "identity", json!({"entity_ids":["app:A"]}), None),
        (
            "e17-purge-reset",
            "purge",
            json!({"entity_ids":["app:B"]}),
            None,
        ),
        ("text-only", "compressor", json!({}), None),
        ("vector-only", "manual", json!({"vector":[1,0]}), None),
        ("time-only", "shift", json!({}), Some(25)),
        (
            "a-shared",
            "compressor",
            json!({"vector":[1,0],"entity_ids":["app:B","app:C"]}),
            Some(24),
        ),
    ];
    let writes: Vec<_> = rows
        .into_iter()
        .map(|(id, body, extra, event)| {
            let mut doc = document(id, body);
            doc.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            if wide_seed && id == "seed" {
                let tags = doc["tags"].as_array_mut().unwrap();
                let allowed = tags.pop().unwrap();
                tags.extend((0..768).map(|index| json!(format!("padding-{index}"))));
                tags.push(allowed);
            }
            let mut row = json!({
                "operation_id":OperationId::new(id.into()),"record_id":id,"expected":"Absent",
                "document":doc,
            });
            if let Some(event) = event {
                row["temporal"] = json!({"event_time_ms":event});
            }
            apply_expiry(&mut row, expiry);
            row
        })
        .collect();
    let (status, body) = namespace_api(
        app,
        "POST",
        "/api/v1/index/graph/batch",
        TENANT,
        Some(json!({"writes":writes})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["results"]
            .as_array()
            .unwrap()
            .iter()
            .all(|result| result["code"] == "applied"),
        "{body}"
    );
    1 + writes.len() as u64
}

fn request() -> Value {
    json!({
        "recall":{"time":{"from_ms":24,"to_ms":26},
            "entity":{"direction":"out","anchor":"app:A","kind":"resolves","visit_limit":32}},
        "query":"compressor","query_vector":[1,0],"all":true,"length":8,"fields":["body","private"],
        "rrf":{"constant":1.0}
    })
}

async fn http(app: &Router, tenant: &str, request: Value) -> Response {
    namespace_response(app, "POST", MIX, tenant, Some(request)).await
}

async fn embedded(sdk: &LocalEngine, request: Value) -> Result<Value, crate::sdk::LocalError> {
    let result = sdk
        .query_time_entity(TENANT, "graph", serde_json::from_value(request).unwrap())
        .await?;
    Ok(serde_json::from_str(&serde_json::to_string(&result).unwrap()).unwrap())
}

fn hits_by_id(body: &Value) -> BTreeMap<&str, &Value> {
    body["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| (hit["record"]["record_id"].as_str().unwrap(), hit))
        .collect()
}

fn verify_ranks(body: &Value) {
    let hits = hits_by_id(body);
    for (id, lanes) in [
        ("e17-purge-reset", vec!["relation"]),
        ("text-only", vec!["lexical"]),
        ("time-only", vec!["time"]),
        ("vector-only", vec!["vector"]),
        ("a-shared", vec!["time", "lexical", "vector", "relation"]),
    ] {
        let hit = hits[id];
        let channels = hit["channels"].as_array().unwrap();
        let names: Vec<_> = channels
            .iter()
            .map(|lane| lane["channel"].as_str().unwrap())
            .collect();
        assert_eq!(names, lanes, "{hit}");
        let mut sum = 0.0f32;
        for lane in channels {
            let value = 1.0 / (1.0 + lane["rank"].as_u64().unwrap() as f32);
            assert_eq!(lane["rrf_contribution"].as_f64().unwrap() as f32, value);
            sum += value;
        }
        assert_eq!(hit["score"].as_f64().unwrap() as f32, sum);
        assert!(hit["record"]["document"].get("private").is_none());
    }
    assert_eq!(hits.len(), 5);
    assert_eq!(hits["a-shared"]["channels"][3]["rank"], 0);
    assert_eq!(hits["e17-purge-reset"]["channels"][0]["rank"], 1);
    assert_eq!(
        hits["e17-purge-reset"]["channels"][0]["rank_basis"],
        "stable_adjacency"
    );
    assert!(hits["e17-purge-reset"]["channels"][0]["raw_score"].is_null());
}

async fn round_trip_tcp(app: Router, request: &Value) -> Value {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let body = request.to_string();
    let raw = format!(
        "POST {MIX} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\napikey: {}\r\nX-AnySeek-Tenant: {TENANT}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        master_headers()["apikey"].to_str().unwrap(),
        body.len()
    );
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(raw.as_bytes()).await.unwrap();
    let mut response = String::new();
    timeout_at(Instant::now() + WAIT, stream.read_to_string(&mut response))
        .await
        .unwrap()
        .unwrap();
    let (headers, wire_body) = response.split_once("\r\n\r\n").unwrap();
    assert!(headers.starts_with("HTTP/1.1 200 "), "{headers}");
    assert!(!headers.contains(ERROR_CODE_HEADER), "{headers}");
    let body = if headers
        .lines()
        .any(|line| line == "transfer-encoding: chunked")
    {
        let (length, bytes) = wire_body.split_once("\r\n").unwrap();
        let length = usize::from_str_radix(length, 16).unwrap();
        assert_eq!(&bytes[length..], "\r\n0\r\n\r\n");
        &bytes[..length]
    } else {
        wire_body
    };
    stop.send(()).unwrap();
    timeout_at(Instant::now() + WAIT, server)
        .await
        .unwrap()
        .unwrap();
    serde_json::from_str(body).unwrap()
}

#[tokio::test]
async fn four_channel_http_sdk_prove_same_cut_dedup_filters_and_withdrawal() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("catalog");
    let service = open(&root).await;
    let app = router(service.clone());
    let sdk = LocalEngine::from_test_service(service.clone());
    initialize(&app).await;
    let cut = populate(&app, None, true).await;
    build_at(&app, TENANT, EVENT_TIME_CHECKPOINT_PATH, cut).await;
    let (status, missing) = decode(http(&app, TENANT, request()).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{missing}");
    assert_eq!(missing["code"], "event_time_unavailable");
    build_at(&app, TENANT, CHECKPOINT_PATH, cut).await;

    let (status, body) = decode(http(&app, TENANT, request()).await).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["visible_seq"], cut);
    assert!(body["count_total"].is_null());
    verify_ranks(&body);
    assert_eq!(
        embedded(&sdk, request()).await.unwrap()["hits"],
        body["hits"]
    );
    assert_eq!(
        round_trip_tcp(app.clone(), &request()).await["hits"],
        body["hits"]
    );

    // Common predicates must qualify the proof as well as the target.
    for role in PROOF_ROLES {
        let mut filtered = request();
        filtered["filter"] =
            json!([{"StringSet":{"field":"tags","filter":[format!("except-{role}")]}}]);
        let (status, matches) = decode(http(&app, TENANT, filtered).await).await;
        assert_eq!(status, StatusCode::OK, "{matches}");
        let hits = hits_by_id(&matches);
        assert!(!hits.contains_key("e17-purge-reset"), "{matches}");
        for id in ["text-only", "time-only", "vector-only", "a-shared"] {
            assert!(hits.contains_key(id), "{matches}");
        }
        let shared = hits["a-shared"]["channels"].as_array().unwrap();
        assert_eq!(
            shared.iter().any(|lane| lane["channel"] == "relation"),
            role == "e17-purge-reset"
        );
    }

    assert_eq!(
        decode(http(&app, "other-tenant", request()).await).await.0,
        StatusCode::FORBIDDEN
    );
    let mut three = request();
    three["recall"].as_object_mut().unwrap().remove("entity");
    let (status, control) = decode(http(&app, TENANT, three).await).await;
    assert_eq!(status, StatusCode::OK, "{control}");
    assert!(!hits_by_id(&control).contains_key("e17-purge-reset"));

    let mut valid_at = request();
    valid_at["temporal"] = json!({"valid_at_ms":29});
    let early = embedded(&sdk, valid_at.clone()).await.unwrap();
    assert!(!hits_by_id(&early).contains_key("e17-purge-reset"));
    assert_eq!(
        hits_by_id(&early)["a-shared"]["channels"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    valid_at["temporal"]["valid_at_ms"] = json!(30);
    assert_eq!(
        embedded(&sdk, valid_at).await.unwrap()["hits"],
        body["hits"]
    );

    let mut budgeted = request();
    let needles = (0..127)
        .map(|index| format!("missing-{index}"))
        .chain(["allowed".into()])
        .collect::<Vec<_>>();
    budgeted["filter"] = json!([{"StringSet":{"field":"tags","filter":needles}}]);
    let mut budget_control = budgeted.clone();
    budget_control["recall"]
        .as_object_mut()
        .unwrap()
        .remove("entity");
    let (status, control) = decode(http(&app, TENANT, budget_control).await).await;
    assert_eq!(status, StatusCode::OK, "{control}");
    let (status, error) = decode(http(&app, TENANT, budgeted.clone()).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("shared query work budget exhausted"),
        "{error}"
    );
    assert!(error.get("hits").is_none());
    assert_eq!(
        embedded(&sdk, budgeted).await.unwrap_err().code(),
        "rejected"
    );

    let old_root = http(&app, TENANT, request()).await;
    build_at(&app, TENANT, EVENT_TIME_CHECKPOINT_PATH, cut).await;
    closes_without_data(old_root, "same-sequence dual projection replacement").await;
    assert_eq!(
        embedded(&sdk, request()).await.unwrap()["hits"],
        body["hits"]
    );

    let stale = http(&app, TENANT, request()).await;
    replace(&app, TENANT, 1, declaration(false, None)).await;
    closes_without_data(stale, "edge withdrawal at delivery").await;
    let (status, changed) = decode(http(&app, TENANT, request()).await).await;
    assert_eq!(status, StatusCode::OK, "{changed}");
    assert_eq!(changed["visible_seq"], cut + 1);
    assert_eq!(
        embedded(&sdk, request()).await.unwrap()["hits"],
        changed["hits"]
    );
    let mut original = hits_by_id(&body);
    original.remove("e17-purge-reset").unwrap();
    assert_eq!(hits_by_id(&changed), original);
    let (status, target) = namespace_api(
        &app,
        "GET",
        "/api/v1/index/graph/doc/e17-purge-reset",
        TENANT,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{target}");

    for (field, value, code) in [
        ("nprobe", json!(1), "rejected"),
        ("all", json!(false), "rejected"),
        ("length", json!(33), "event_time_budget"),
        (
            "rrf",
            json!({"constant":1,"time_weight":1e38,"keyword_weight":1e38,"vector_weight":1e38,"entity_weight":1e38}),
            "rejected",
        ),
    ] {
        let mut invalid = request();
        invalid[field] = value;
        let (status, error) = decode(http(&app, TENANT, invalid.clone()).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert_eq!(error["code"], code);
        assert!(error.get("hits").is_none());
        if field != "nprobe" {
            assert_eq!(embedded(&sdk, invalid).await.unwrap_err().code(), code);
        }
    }
    let mut restricted = request();
    restricted["recall"]["entity"]["visit_limit"] = json!(2);
    let (status, error) = decode(http(&app, TENANT, restricted.clone()).await).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert!(
        error["message"]
            .as_str()
            .unwrap()
            .contains("visit budget exhausted")
    );
    assert!(error.get("hits").is_none());
    assert_eq!(
        embedded(&sdk, restricted).await.unwrap_err().code(),
        "rejected"
    );

    drop(sdk);
    drop(app);
    service.shutdown().wait().await.unwrap();

    let service = open(&root).await;
    let app = router(service.clone());
    let (status, reopened) = decode(http(&app, TENANT, request()).await).await;
    assert_eq!(status, StatusCode::OK, "{reopened}");
    assert_eq!(reopened["visible_seq"], cut + 1);
    assert_eq!(reopened["hits"], changed["hits"]);
    let sdk = LocalEngine::from_test_service(service.clone());
    assert_eq!(
        embedded(&sdk, request()).await.unwrap()["hits"],
        reopened["hits"]
    );
    drop(sdk);
    drop(app);
    service.shutdown().wait().await.unwrap();
}

#[tokio::test]
async fn four_channel_ttl_fences_each_lane_and_off_page_proof() {
    for role in [
        "time-only",
        "text-only",
        "vector-only",
        "seed",
        "edge",
        "e17-purge-reset",
    ] {
        let temp = tempfile::tempdir().unwrap();
        let service = open(&temp.path().join("catalog")).await;
        let app = router(service.clone());
        let sdk = Arc::new(LocalEngine::from_test_service(service.clone()));
        initialize(&app).await;
        let partition = service.record(TENANT, "graph").await.unwrap();
        let (clock, now) = crate::record::tests::test_clock(&partition).await;
        let expiry = now + 120_000;
        let cut = populate(&app, Some((role, expiry)), false).await;
        build_at(&app, TENANT, EVENT_TIME_CHECKPOINT_PATH, cut).await;
        build_at(&app, TENANT, CHECKPOINT_PATH, cut).await;

        // This window ranks two candidates per lane while keeping the unique
        // relation hit outside the returned page. Its evidence still matters.
        let mut paged = request();
        paged["length"] = json!(2);
        let preview = embedded(&sdk, paged.clone()).await.unwrap();
        assert!(
            !hits_by_id(&preview).contains_key("e17-purge-reset"),
            "{preview}"
        );
        let response = http(&app, TENANT, paged.clone()).await;
        assert_eq!(response.status(), StatusCode::OK, "{role}");
        let zero_weight = if PROOF_ROLES.contains(&role) {
            let mut zero = paged.clone();
            zero["rrf"]["entity_weight"] = json!(0);
            Some(http(&app, TENANT, zero).await)
        } else {
            None
        };
        let gate = service.root.join(".test-event-read-handoff-pause");
        let started = service.root.join(".test-event-read-handoff-started");
        std::fs::write(&gate, b"pause").unwrap();
        let local = sdk.clone();
        let pending = tokio::spawn(async move { embedded(&local, paged).await });
        timeout_at(Instant::now() + WAIT, async {
            while !started.exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        clock.set(expiry);
        std::fs::remove_file(gate).unwrap();
        closes_without_data(response, role).await;
        assert_eq!(pending.await.unwrap().unwrap_err().code(), "busy", "{role}");
        if let Some(response) = zero_weight {
            let (status, body) = decode(response).await;
            assert_eq!(status, StatusCode::OK, "{body}");
            assert!(hits_by_id(&body).values().all(|hit| {
                hit["channels"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|lane| lane["channel"] != "relation")
            }));
        }
        drop(partition);
        drop(sdk);
        drop(app);
        service.shutdown().wait().await.unwrap();
    }
}
