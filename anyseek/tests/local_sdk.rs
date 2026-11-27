//! Local SDK contract through its public Rust API.
use anyseek::sdk::{LocalEngine, LocalErrorKind, OperationId};
use serde_json::{Value, json};

mod entity_one_hop;

async fn group_view(engine: &LocalEngine, query_term: &str, ids: &[&str], seq: u64) {
    for query in [
        json!({"query":query_term,"length":5,"count":true}),
        json!({"query_vector":[1,0],"all":true,"length":5,"count":true}),
    ] {
        let result = engine
            .query("1-tenant", "policy", serde_json::from_value(query).unwrap())
            .await
            .unwrap();
        assert_eq!(result.count_total(), Some(ids.len()));
        let mut found: Vec<_> = result
            .hits()
            .iter()
            .map(|hit| hit.record.record_id.as_str())
            .collect();
        found.sort_unstable();
        assert_eq!(found, ids);
        let response = serde_json::to_value(result).unwrap();
        let progress = &response["diagnostics"]["index_progress"];
        for name in ["committed_seq", "lexical_seq", "vector_seq"] {
            assert_eq!(progress[name], seq, "{progress}");
        }
    }
}

#[tokio::test]
async fn local_sdk_owns_publishes_isolates_and_recovers() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("catalog");
    let engine = LocalEngine::open(&root, true).await.unwrap();
    assert!(LocalEngine::open(&root, false).await.is_err());
    engine
        .create_collection(
            "1-tenant",
            "policy",
            OperationId::new("create-policy".into()),
            serde_json::from_value(json!({
                "index_name":"policy","tokenizer":"TantivySimpleV1","vector":{"dimensions":2},
                "schema":[
                    {"field":"terms","field_type":"Text","store":true,"index_lexical":true},
                    {"field":"vector","field_type":"Json","index_vector":true},
                ]
            }))
            .unwrap(),
            Some("fixture-f32-cosine".into()),
        )
        .await
        .unwrap();
    assert_eq!(
        engine
            .collection_info("2-tenant", "policy")
            .await
            .unwrap_err()
            .kind(),
        LocalErrorKind::NotFound
    );
    let group = |nonce: &str, version: u64, ids: &[&str], terms: &str| {
        let records: Vec<Value> = ids
            .iter()
            .map(|id| json!({"record_id":id,"document":{"terms":terms,"vector":[1,0]}}))
            .collect();
        serde_json::from_value::<anyseek::record::GroupReplace>(json!({
            "operation_id":OperationId::new(nonce.into()),
            "group_id":"policy","expected_group_version":version,"records":records,
        }))
        .unwrap()
    };

    let initial = group("publish-policy", 0, &["p1", "p2"], "connection timeout");
    let first = engine
        .replace_group("1-tenant", "policy", initial.clone())
        .await
        .unwrap();
    assert_eq!(
        (first.seq, first.group_version, first.record_count),
        (1, 1, 2)
    );
    group_view(&engine, "connection", &["p1", "p2"], 1).await;

    let replacement = group("replace-policy", 1, &["p1", "p3"], "subscription timeout");
    // Discard the committed outcome and recover using the original operation ID.
    let _ = engine
        .replace_group("1-tenant", "policy", replacement.clone())
        .await
        .unwrap();
    assert_eq!(
        engine
            .group_version("1-tenant", "policy", "policy")
            .await
            .unwrap(),
        Some(2)
    );
    engine.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
    assert!(engine.version().is_err());

    // Exercise the production checkpoint of this isolated SDK collection.
    // The catalog must then cold-open the new root instead of rebuilding
    // from the initial journal alone.
    let collection_root = root.join("collections").join("1-tenant").join("policy");
    let generations: Vec<_> = std::fs::read_dir(collection_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    let [generation] = generations.as_slice() else {
        panic!("expected one isolated collection generation");
    };
    let partition = anyseek::record::RecordPartition::open(generation, 256 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(partition.compact_journal().await.unwrap(), 2);
    partition.finish_service_shutdown().await.unwrap();
    drop(partition);
    assert!(std::fs::read_dir(generation).unwrap().any(|entry| {
        let path = entry.unwrap().path();
        path.join("primary-pages").is_file() && path.join("records-0000000000000000").is_file()
    }));

    let reopened = LocalEngine::open(&root, false).await.unwrap();
    let record = reopened
        .get("1-tenant", "policy", "p1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.version, 2);
    assert_eq!(record.document.unwrap()["terms"], "subscription timeout");
    assert!(
        reopened
            .get("1-tenant", "policy", "p2")
            .await
            .unwrap()
            .is_none()
    );
    let second = reopened
        .replace_group("1-tenant", "policy", replacement.clone())
        .await
        .unwrap();
    assert_eq!(
        (second.seq, second.group_version, second.record_count),
        (2, 2, 2)
    );
    assert_eq!(
        reopened
            .replace_group("1-tenant", "policy", replacement)
            .await
            .unwrap(),
        second
    );
    assert_eq!(
        reopened
            .replace_group("1-tenant", "policy", initial)
            .await
            .unwrap(),
        first
    );
    group_view(&reopened, "subscription", &["p1", "p3"], 2).await;
    let stale = reopened
        .query(
            "1-tenant",
            "policy",
            serde_json::from_value(json!({"query":"connection","count":true})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stale.count_total(), Some(0));

    let withdrawal = group("withdraw-policy", 2, &[], "");
    let third = reopened
        .replace_group("1-tenant", "policy", withdrawal.clone())
        .await
        .unwrap();
    assert_eq!(
        (third.seq, third.group_version, third.record_count),
        (3, 3, 0)
    );
    group_view(&reopened, "subscription", &[], 3).await;
    reopened.shutdown().await.unwrap();
    let reopened = LocalEngine::open(&root, false).await.unwrap();
    assert!(
        reopened
            .get("1-tenant", "policy", "p1")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reopened
            .replace_group("1-tenant", "policy", withdrawal)
            .await
            .unwrap(),
        third
    );
    assert_eq!(
        reopened
            .group_version("1-tenant", "policy", "policy")
            .await
            .unwrap(),
        Some(3)
    );
    group_view(&reopened, "subscription", &[], 3).await;
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_sdk_event_time_projection_shares_filters_and_survives_reopen() {
    use anyseek::sdk::EventTimeRequest;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("catalog");
    let engine = LocalEngine::open(&root, true).await.unwrap();
    let tenant = "tenant";
    let collection = "window";
    engine
        .create_collection(
            tenant,
            collection,
            OperationId::new("create-window".into()),
            serde_json::from_value(json!({"index_name":"window","schema":[
                {"field":"body","field_type":"Text","store":true,"index_lexical":true},
                {"field":"private","field_type":"Text","store":false},
                {"field":"label","field_type":"String","filterable":true}
            ]}))
            .unwrap(),
            None,
        )
        .await
        .unwrap();
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 1600;
    let row = |id: &str, time: i64, label: &str, expiry: Option<u64>| {
        json!({
            "record_id":id,"document":{"body":"alpha","private":"hidden","label":label},
            "temporal":{"event_time_ms":time,"expire_at_ms":expiry}
        })
    };
    let group = |id: &str, records: Vec<Value>| {
        serde_json::from_value(json!({
        "operation_id":OperationId::new(id.into()),"group_id":id,"expected_group_version":0,"records":records
    })).unwrap()
    };
    engine
        .replace_group(
            tenant,
            collection,
            group(
                "base",
                vec![
                    row("earlier", 9, "take", None),
                    row("later", 11, "take", None),
                    row("excluded", 10, "skip", None),
                    row("matched", 10, "take", None),
                    row("expired", 10, "take", Some(expiry)),
                ],
            ),
        )
        .await
        .unwrap();
    let request = json!({"temporal":{"event":{"from_ms":10,"to_ms":11}},"length":4,
        "fields":["body","private"], "filter":[{"String":{"field":"label","filter":["take"]}}]});
    let parse = || serde_json::from_value::<EventTimeRequest>(request.clone()).unwrap();
    let mut invalid = request.clone();
    invalid["query_vector"] = json!([1, 0]);
    assert!(serde_json::from_value::<EventTimeRequest>(invalid).is_err());
    assert_eq!(
        engine
            .query_event_time("other", collection, parse())
            .await
            .unwrap_err()
            .kind(),
        LocalErrorKind::NotFound
    );
    let missing = engine
        .query_event_time(tenant, collection, parse())
        .await
        .unwrap_err();
    assert_eq!(missing.kind(), LocalErrorKind::Rejected);
    assert_eq!(missing.code(), "event_time_unavailable");
    let ordinary = || serde_json::from_value(json!({"query":"alpha","length":10})).unwrap();
    assert!(
        !engine
            .query(tenant, collection, ordinary())
            .await
            .unwrap()
            .hits()
            .is_empty()
    );
    assert_eq!(
        engine
            .build_event_time_projection(tenant, collection)
            .await
            .unwrap(),
        1
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
    let result = engine
        .query_event_time(tenant, collection, parse())
        .await
        .unwrap();
    assert_eq!(result.visible_seq, 1);
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].record_id, "matched");
    assert_eq!(result.hits[0].document.as_ref().unwrap().len(), 1);
    assert_eq!(result.hits[0].document.as_ref().unwrap()["body"], "alpha");
    engine.shutdown().await.unwrap();

    let engine = LocalEngine::open(&root, false).await.unwrap();
    let result = engine
        .query_event_time(tenant, collection, parse())
        .await
        .unwrap();
    assert_eq!(result.visible_seq, 1);
    assert_eq!(result.hits[0].record_id, "matched");
    engine
        .replace_group(
            tenant,
            collection,
            group(
                "overlay",
                vec![
                    row("a-overlay", 10, "take", None),
                    row("suppressed-overlay", 10, "skip", None),
                ],
            ),
        )
        .await
        .unwrap();
    let updated = engine
        .query_event_time(tenant, collection, parse())
        .await
        .unwrap();
    assert_eq!(updated.visible_seq, 2);
    assert_eq!(
        updated
            .hits
            .iter()
            .map(|row| row.record_id.as_str())
            .collect::<Vec<_>>(),
        ["a-overlay", "matched"]
    );
    for (i, chunk) in (0..129).collect::<Vec<_>>().chunks(43).enumerate() {
        let rows = chunk
            .iter()
            .map(|n| row(&format!("r-{n}"), 20, "take", None))
            .collect();
        engine
            .replace_group(tenant, collection, group(&format!("tail-{i}"), rows))
            .await
            .unwrap();
    }
    let exhausted = engine
        .query_event_time(tenant, collection, parse())
        .await
        .unwrap_err();
    assert_eq!(exhausted.kind(), LocalErrorKind::Rejected);
    assert_eq!(exhausted.code(), "event_time_budget");
    assert!(
        !engine
            .query(tenant, collection, ordinary())
            .await
            .unwrap()
            .hits()
            .is_empty()
    );
    assert_eq!(
        engine
            .build_event_time_projection(tenant, collection)
            .await
            .unwrap(),
        5
    );
    let refreshed = engine
        .query_event_time(tenant, collection, parse())
        .await
        .unwrap();
    assert_eq!(refreshed.visible_seq, 5);
    assert_eq!(
        refreshed
            .hits
            .iter()
            .map(|row| row.record_id.as_str())
            .collect::<Vec<_>>(),
        ["a-overlay", "matched"]
    );
    engine.shutdown().await.unwrap();

    // An ordinary full checkpoint is supported by the underlying Record
    // partition. Close the SDK owner before using that native entry point.
    let catalog = anyseek::catalog::Catalog::open(&root).await.unwrap();
    let entry = catalog.collection_entry(tenant, collection).await.unwrap();
    let partition = anyseek::record::RecordPartition::open(
        &root.join(entry.directory()),
        entry.config.max_journal_bytes,
    )
    .await
    .unwrap();
    assert_eq!(
        partition
            .index_committed(Duration::from_secs(20))
            .await
            .unwrap(),
        5
    );
    partition.finish_service_shutdown().await.unwrap();
    drop(partition);
    drop(catalog);

    let engine = LocalEngine::open(&root, false).await.unwrap();
    let missing = engine
        .query_event_time(tenant, collection, parse())
        .await
        .unwrap_err();
    assert_eq!(missing.code(), "event_time_unavailable");
    assert_eq!(
        engine
            .build_event_time_projection(tenant, collection)
            .await
            .unwrap(),
        5
    );
    engine.shutdown().await.unwrap();
    let engine = LocalEngine::open(&root, false).await.unwrap();
    let replayed = engine
        .query_event_time(tenant, collection, parse())
        .await
        .unwrap();
    assert_eq!(replayed.visible_seq, refreshed.visible_seq);
    assert_eq!(
        replayed
            .hits
            .iter()
            .map(|row| row.record_id.as_str())
            .collect::<Vec<_>>(),
        ["a-overlay", "matched"]
    );
    engine.shutdown().await.unwrap();
}
