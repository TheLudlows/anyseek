//! Public SDK coverage for one bounded time and exact-vector read view.
use anyseek::sdk::{LocalEngine, LocalErrorKind, OperationId, TimeVectorRequest};
use serde_json::json;

#[tokio::test]
async fn local_time_vector_fuses_one_cut_and_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("catalog");
    let mut engine = LocalEngine::open(&root, true).await.unwrap();
    engine
        .create_collection(
            "tenant",
            "policy",
            OperationId::new("create".into()),
            serde_json::from_value(
                json!({"index_name":"policy","vector":{"dimensions":2},"schema":[
                    {"field":"body","field_type":"Text","store":true},
                    {"field":"private","field_type":"Text","store":false},
                    {"field":"rank","field_type":"Int","filterable":true},
                    {"field":"vector","field_type":"Json","index_vector":true}
                ]}),
            )
            .unwrap(),
            Some("fixture-two-dimensions".into()),
        )
        .await
        .unwrap();
    engine.replace_group("tenant", "policy", serde_json::from_value(json!({
        "operation_id":OperationId::new("publish".into()),"group_id":"fixture","expected_group_version":0,
        "records":[
            {"record_id":"both","temporal":{"event_time_ms":20},"document":{"body":"both","private":"hidden","rank":1,"vector":[1,0]}},
            {"record_id":"time","temporal":{"event_time_ms":19},"document":{"body":"time","rank":1}},
            {"record_id":"vector","temporal":{"event_time_ms":100},"document":{"body":"vector","rank":1,"vector":[0,1]}},
            {"record_id":"excluded","temporal":{"event_time_ms":21},"document":{"body":"excluded","rank":9,"vector":[1,0]}}
        ]
    })).unwrap()).await.unwrap();
    let input = |from, to, offset, length| {
        serde_json::from_value::<TimeVectorRequest>(json!({
            "recall":{"time":{"from_ms":from,"to_ms":to}},"query_vector":[1,0],"all":true,
            "offset":offset,"length":length,"fields":["body","private","vector"],
            "filter":[{"Int":{"field":"rank","filter":{"start":1,"end":2}}}]
        }))
        .unwrap()
    };
    assert_eq!(
        engine
            .query_time_vector("tenant", "policy", input(18, 22, 0, 3))
            .await
            .unwrap_err()
            .code(),
        "event_time_unavailable"
    );
    assert_eq!(
        engine
            .build_event_time_projection("tenant", "policy")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        engine
            .query_time_vector("other", "policy", input(18, 22, 0, 3))
            .await
            .unwrap_err()
            .kind(),
        LocalErrorKind::NotFound
    );
    assert_eq!(
        engine
            .query_time_vector("tenant", "policy", input(18, 22, 64, 1))
            .await
            .unwrap_err()
            .code(),
        "event_time_budget"
    );
    for reopen in [false, true] {
        let result = engine
            .query_time_vector("tenant", "policy", input(18, 22, 0, 3))
            .await
            .unwrap();
        assert_eq!(result.visible_seq, 1);
        assert_eq!(result.count_total, None);
        assert_eq!(
            result
                .hits
                .iter()
                .map(|hit| hit.record.record_id.as_str())
                .collect::<Vec<_>>(),
            ["both", "time", "vector"]
        );
        assert_eq!(
            result.hits[0]
                .channels
                .iter()
                .map(|lane| (lane.channel.as_str(), lane.rank))
                .collect::<Vec<_>>(),
            [("time", 0), ("vector", 0)]
        );
        assert!((result.hits[0].score - 2.0 / 0.6).abs() < 1e-6);
        assert_eq!(result.hits[1].channels[0].rank, 1);
        assert_eq!(result.hits[2].channels[0].rank, 1);
        assert_eq!(
            serde_json::to_value(result.hits[0].record.document.as_ref().unwrap()).unwrap(),
            json!({"body":"both"})
        );
        let page = engine
            .query_time_vector("tenant", "policy", input(18, 22, 1, 2))
            .await
            .unwrap();
        assert_eq!(
            page.hits
                .iter()
                .map(|hit| hit.record.record_id.as_str())
                .collect::<Vec<_>>(),
            ["time", "vector"]
        );
        let empty_time = engine
            .query_time_vector("tenant", "policy", input(200, 201, 0, 3))
            .await
            .unwrap();
        assert_eq!(empty_time.hits.len(), 2);
        assert!(
            empty_time
                .hits
                .iter()
                .all(|hit| hit.channels.len() == 1 && hit.channels[0].channel == "vector")
        );
        engine.shutdown().await.unwrap();
        if !reopen {
            engine = LocalEngine::open(&root, false).await.unwrap();
        }
    }
}
