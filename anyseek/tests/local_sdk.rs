//! Local SDK contract through its public Rust API.
use anyseek::sdk::{LocalEngine, LocalErrorKind, OperationId};
use serde_json::json;

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
            serde_json::from_value(json!({"index_name":"policy","tokenizer":"TantivySimpleV1",
            "schema":[{"field":"terms","field_type":"Text","store":true,"index_lexical":true}]}))
            .unwrap(),
            None,
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
    let operation_id = OperationId::new("publish-policy".into());
    let group = json!({"operation_id":operation_id,"group_id":"policy","expected_group_version":0,
        "records":[{"record_id":"p1","document":{"terms":"connection timeout"}}]});
    for _ in 0..2 {
        let receipt = engine
            .replace_group(
                "1-tenant",
                "policy",
                serde_json::from_value(group.clone()).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(receipt.group_version, 1);
    }
    assert_eq!(
        engine
            .group_version("1-tenant", "policy", "policy")
            .await
            .unwrap(),
        Some(1)
    );
    engine.shutdown().await.unwrap();
    engine.shutdown().await.unwrap();
    assert!(engine.version().is_err());
    let reopened = LocalEngine::open(&root, false).await.unwrap();
    let result = reopened
        .query(
            "1-tenant",
            "policy",
            serde_json::from_value(json!({"query":"connection","length":5,"count":true})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(result.count_total(), Some(1));
    assert_eq!(result.hits()[0].record.record_id, "p1");
    reopened.shutdown().await.unwrap();
}
