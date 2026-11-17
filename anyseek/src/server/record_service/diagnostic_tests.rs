use super::*;
use tower::ServiceExt;

async fn call(
    app: &Router,
    headers: &HeaderMap,
    method: &str,
    path: &str,
    body: Value,
) -> (StatusCode, HeaderMap, Value) {
    let mut request = axum::http::Request::builder().method(method).uri(path);
    *request.headers_mut().unwrap() = headers.clone();
    request = request
        .header("content-type", "application/json")
        .header("x-anyseek-request-id", "untrusted-client-value");
    let response = app
        .clone()
        .oneshot(
            request
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let value = serde_json::from_slice(
        &axum::body::to_bytes(response.into_body(), MAX_RESPONSE)
            .await
            .unwrap(),
    )
    .unwrap();
    (status, headers, value)
}

#[tokio::test]
async fn diagnostics_correlate_requests_without_exposing_input_or_claiming_snapshot() {
    let (service, path, headers) = tests::setup().await;
    let app = router(service.clone());
    let (_, _, info) = call(&app, &headers, "GET", "/api/v1/index/c", Value::Null).await;
    assert_eq!(info["analysis"]["format"], "native-package-analysis:v1");
    let (status, response_headers, query) = call(
        &app,
        &headers,
        "POST",
        "/api/v1/index/c/query",
        json!({"query":"PRIVATE QUERY"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let observations = &query["diagnostics"];
    assert_eq!(observations["format_version"], 2);
    assert!(
        observations
            .get("embedding_space_sha256")
            .unwrap()
            .is_null()
    );
    let id = response_headers["x-anyseek-request-id"].to_str().unwrap();
    assert_eq!(id.len(), 32);
    assert_ne!(id, "untrusted-client-value");
    assert_eq!(observations["request_id"], id);
    assert_eq!(
        observations["analysis_fingerprint"],
        info["analysis"]["fingerprint"]
    );
    assert_eq!(observations["mode"], "lexical");
    assert_eq!(
        observations["index_progress"]["sampling"],
        "after_query_not_atomic_with_result"
    );
    assert!(!observations.to_string().contains("PRIVATE QUERY"));
    let (_, next_headers, browse) =
        call(&app, &headers, "POST", "/api/v1/index/c/query", json!({})).await;
    assert_ne!(
        next_headers["x-anyseek-request-id"],
        response_headers["x-anyseek-request-id"]
    );
    assert_eq!(browse["diagnostics"]["mode"], "browse");
    let (status, errors, _) = call(
        &app,
        &HeaderMap::new(),
        "POST",
        "/api/v1/index/c/query",
        json!({}),
    )
    .await;
    assert!(!status.is_success());
    assert_eq!(errors["x-anyseek-request-id"].as_bytes().len(), 32);
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn query_diagnostics_distinguish_equal_dimension_embedding_spaces() {
    let (service, path, headers) = tests::setup().await;
    let app = router(service.clone());
    let mut observed = Vec::new();
    for (id, space) in [
        ("space-a", "PRIVATE SPACE A"),
        ("space-b", "PRIVATE SPACE B"),
    ] {
        let (status, _, _) = call(&app, &headers, "POST", "/api/v1/index", json!({
            "operation_id":OperationId::new(id.into()), "collection_id":id,
            "embedding_space_id":space, "config":{"index_name":"same-analysis",
                "schema":[{"field":"body","field_type":"Text"},
                    {"field":"vector","field_type":"Json","index_lexical":false,"index_vector":true}],
                "vector":{"dimensions":2}}
        })).await;
        assert!(status.is_success());
        let (status, _, result) = call(
            &app,
            &headers,
            "POST",
            &format!("/api/v1/index/{id}/query"),
            json!({"query_vector":[1.0,0.0],"all":true}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let diagnostic = result["diagnostics"].clone();
        let digest: String = Sha256::digest(space.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(diagnostic["embedding_space_sha256"], digest);
        assert_eq!(diagnostic["mode"], "vector");
        assert!(!diagnostic.to_string().contains("PRIVATE SPACE"));
        observed.push(diagnostic);
    }
    assert_eq!(
        observed[0]["analysis_fingerprint"],
        observed[1]["analysis_fingerprint"]
    );
    assert_ne!(
        observed[0]["embedding_space_sha256"],
        observed[1]["embedding_space_sha256"]
    );
    drop(app);
    service.shutdown().wait().await.unwrap();
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn analysis_digest_tracks_configuration_and_redacts_custom_dictionaries() {
    let request = || {
        serde_json::from_value::<CreateIndexRequest>(json!({"index_name":"test","schema":[]}))
            .unwrap()
    };
    let mut config = record_config(request(), None, CapacityProfile::default()).unwrap();
    let first = config.analysis_contract().unwrap();
    assert_eq!(first.fingerprint.len(), 64);
    config.index.stop_words = anyseek::index::StopwordType::Custom {
        terms: vec!["PRIVATE DICTIONARY".into()],
    };
    let changed = config.analysis_contract().unwrap();
    assert_ne!(first.fingerprint, changed.fingerprint);
    assert_ne!(first.stop_words_fingerprint, changed.stop_words_fingerprint);
    assert_eq!(changed.stop_words, "Custom");
    assert!(
        !serde_json::to_string(&changed)
            .unwrap()
            .contains("PRIVATE DICTIONARY")
    );
    assert_eq!(
        changed.fingerprint,
        config.analysis_contract().unwrap().fingerprint
    );
}
