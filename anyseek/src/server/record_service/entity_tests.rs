use super::*;
use crate::sdk::{EntityCandidatePaths, EntityDirection, LocalEngine, LocalErrorKind};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(25);

async fn create(engine: &LocalEngine, tenant: &str, id: &str, nonce: &str) {
    engine
        .create_collection(
            tenant,
            id,
            OperationId::new(nonce.into()),
            serde_json::from_value(json!({
                "index_name":id,"tokenizer":"TantivySimpleV1",
                "schema":[
                    {"field":"body","field_type":"Text","store":true,"index_lexical":true},
                    {"field":"entity_ids","field_type":"StringSet","store":true},
                    {"field":"relations","field_type":"Json","store":true}
                ]
            }))
            .unwrap(),
            None,
        )
        .await
        .unwrap();
}

async fn write_graph(
    engine: &LocalEngine,
    tenant: &str,
    id: &str,
    target: &str,
    expiry: Option<(usize, u64)>,
) {
    let mut records = [
        json!({"record_id":"seed","document":{"body":"alpha","entity_ids":["app:A"]}}),
        json!({"record_id":"edge","document":{"body":"alpha","relations":{"edges":[
            {"from":"app:A","type":"link","to":target}
        ]}}}),
        json!({"record_id":"target","document":{"body":"alpha","entity_ids":[target]}}),
    ];
    if let Some((index, deadline)) = expiry {
        records[index]["temporal"] = json!({"expire_at_ms":deadline});
    }
    replace(engine, tenant, id, 0, &records).await;
}

async fn replace(engine: &LocalEngine, tenant: &str, id: &str, previous: u64, records: &[Value]) {
    let request = serde_json::from_value::<GroupReplace>(json!({
        "operation_id":OperationId::new(format!("entity-{id}-{previous}")),
        "group_id":"graph","expected_group_version":previous,"records":records
    }))
    .unwrap();
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match engine.replace_group(tenant, id, request.clone()).await {
            Ok(_) => return,
            Err(error) if error.code() == "busy" && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("replace {tenant}/{id}: {error:?}"),
        }
    }
}

async fn build(engine: &LocalEngine, tenant: &str, id: &str) {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match engine.build_entity_adjacency(tenant, id).await {
            Ok(cut) => {
                assert_eq!(cut, 1);
                return;
            }
            Err(error) if error.code() == "busy" && tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("build {tenant}/{id}: {error:?}"),
        }
    }
}

fn assert_path(paths: &EntityCandidatePaths, seed: &str, neighbor: &str, target: &str) {
    let [seed_row] = paths.seeds() else {
        panic!("expected one seed")
    };
    assert_eq!((seed_row.record_id.as_str(), seed_row.version), (seed, 1));
    let [entry] = paths.neighbors() else {
        panic!("expected one neighbor")
    };
    assert_eq!(entry.entity_id(), neighbor);
    let [edge] = entry.declarations() else {
        panic!("expected one edge")
    };
    assert_eq!((edge.record_id.as_str(), edge.version), ("edge", 1));
    let [target_row] = entry.members() else {
        panic!("expected one target")
    };
    assert_eq!(
        (target_row.record_id.as_str(), target_row.version),
        (target, 1)
    );
}

#[tokio::test]
async fn local_entity_sdk_discovers_isolated_paths_after_reopen() {
    use EntityDirection::{In, Out};
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("service");
    let engine = LocalEngine::open(&root, true).await.unwrap();
    for (tenant, target) in [("tenant-a", "app:B"), ("tenant-b", "app:C")] {
        create(&engine, tenant, "graph", tenant).await;
        write_graph(&engine, tenant, "graph", target, None).await;
        if tenant == "tenant-a" {
            let error = engine
                .query_entity_paths(tenant, "graph", Out, "app:A", "link", 32)
                .await
                .err()
                .unwrap();
            assert_eq!(error.kind(), LocalErrorKind::Rejected);
        }
        build(&engine, tenant, "graph").await;
    }
    engine.shutdown().await.unwrap();
    drop(engine);
    let engine = LocalEngine::open(&root, false).await.unwrap();
    for (tenant, target) in [("tenant-a", "app:B"), ("tenant-b", "app:C")] {
        let forward = engine
            .query_entity_paths(tenant, "graph", Out, "app:A", "link", 32)
            .await
            .expect("forward discovery");
        assert_path(&forward, "seed", target, "target");
        let reverse = engine
            .query_entity_paths(tenant, "graph", In, target, "link", 32)
            .await
            .expect("reverse discovery");
        assert_path(&reverse, "target", "app:A", "seed");
    }
    let anchor = format!("n:{}", "é".repeat(127));
    let kind = "é".repeat(64);
    let empty = engine
        .query_entity_paths("tenant-a", "graph", Out, &anchor, &kind, 32)
        .await
        .unwrap();
    assert!(empty.seeds().is_empty() && empty.neighbors().is_empty());

    let long_anchor = format!("{anchor}x");
    let long_kind = format!("{kind}x");
    for (anchor, kind, limit) in [
        (long_anchor.as_str(), "link", 32),
        ("app:A", long_kind.as_str(), 32),
        ("", "link", 32),
        ("app:", "link", 32),
        ("app:A", "", 32),
        ("app:A", "bad kind", 32),
        ("app:A", "link", 0),
    ] {
        let error = engine
            .query_entity_paths("tenant-a", "graph", Out, anchor, kind, limit)
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind(), LocalErrorKind::Rejected);
    }

    let absent = engine
        .query_entity_paths("other", "graph", Out, "app:A", "link", 32)
        .await
        .err()
        .unwrap();
    let invalid = engine
        .query_entity_paths("other", "graph", Out, &long_anchor, &long_kind, 32)
        .await
        .err()
        .unwrap();
    assert_eq!(absent.kind(), LocalErrorKind::NotFound);
    assert_eq!(
        (invalid.kind(), invalid.code()),
        (absent.kind(), absent.code())
    );
    engine.shutdown().await.unwrap();
    drop(engine);

    let mut service = Service::open(&root, 200.try_into().unwrap(), None, false)
        .await
        .unwrap();
    Arc::get_mut(&mut service).unwrap().request_timeout = Duration::ZERO;
    let engine = LocalEngine::from_test_service(service);
    tokio::time::pause();
    for (tenant, anchor) in [
        ("tenant-a", "app:A"),
        ("tenant-a", long_anchor.as_str()),
        ("other", "app:A"),
    ] {
        let expired = engine
            .query_entity_paths(tenant, "graph", Out, anchor, "link", 32)
            .await
            .err()
            .unwrap();
        assert_eq!(expired.code(), "busy");
    }
    tokio::time::resume();
    engine.shutdown().await.unwrap();
}

async fn fixture(root: &FsPath) -> (Arc<Service>, Arc<LocalEngine>) {
    let service = Service::open(root, 200.try_into().unwrap(), None, true)
        .await
        .unwrap();
    let engine = Arc::new(LocalEngine::from_test_service(service.clone()));
    (service, engine)
}

async fn pause_query(
    service: &Service,
    engine: &Arc<LocalEngine>,
    id: &str,
) -> (
    tokio::task::JoinHandle<Result<EntityCandidatePaths, crate::sdk::LocalError>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (reached, resume) = service.cold.test.entity_handoff.pause_next();
    let engine = engine.clone();
    let id = id.to_owned();
    let query = tokio::spawn(async move {
        engine
            .query_entity_paths("tenant", &id, EntityDirection::Out, "app:A", "link", 32)
            .await
    });
    tokio::time::timeout(WAIT, reached).await.unwrap().unwrap();
    (query, resume)
}

#[tokio::test]
async fn local_entity_handoff_refuses_replaced_group_and_catalog_generation() {
    let temporary = tempfile::tempdir().unwrap();
    let (service, engine) = fixture(&temporary.path().join("service")).await;
    create(&engine, "tenant", "graph", "create").await;
    write_graph(&engine, "tenant", "graph", "app:B", None).await;
    build(&engine, "tenant", "graph").await;

    let (pending, resume) = pause_query(&service, &engine, "graph").await;
    tokio::time::pause();
    tokio::time::advance(service.request_timeout).await;
    let expired = tokio::time::timeout(WAIT, pending)
        .await
        .expect("the query must time out while paused at handoff")
        .unwrap()
        .err()
        .unwrap();
    tokio::time::resume();
    drop(resume);
    assert_eq!(expired.code(), "busy");

    let (pending, resume) = pause_query(&service, &engine, "graph").await;
    replace(&engine, "tenant", "graph", 1, &[]).await;
    resume.send(()).unwrap();
    assert_eq!(pending.await.unwrap().err().unwrap().code(), "busy");

    let (pending, resume) = pause_query(&service, &engine, "graph").await;
    service
        .catalog
        .execute(CatalogRequest {
            operation_id: OperationId::new("delete-old".into()),
            command: CatalogCommand::DeleteCollection {
                tenant_id: "tenant".into(),
                collection_id: "graph".into(),
            },
        })
        .await
        .unwrap();
    create(&engine, "tenant", "graph", "create-replacement").await;
    resume.send(()).unwrap();
    assert_eq!(
        pending.await.unwrap().err().unwrap().kind(),
        LocalErrorKind::NotFound
    );
    drop(service);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_entity_handoff_accepts_no_role_at_its_expiry_boundary() {
    let temporary = tempfile::tempdir().unwrap();
    let (service, engine) = fixture(&temporary.path().join("service")).await;
    for (index, id) in ["seed", "edge", "target"].into_iter().enumerate() {
        create(&engine, "tenant", id, id).await;
        let resident = service.record("tenant", id).await.unwrap();
        let (clock, now) = crate::record::tests::test_clock(&resident).await;
        let expiry = now + 20_000;
        write_graph(&engine, "tenant", id, "app:B", Some((index, expiry))).await;
        build(&engine, "tenant", id).await;

        let (pending, resume) = pause_query(&service, &engine, id).await;
        clock.set(expiry);
        resume.send(()).unwrap();
        assert_eq!(pending.await.unwrap().err().unwrap().code(), "busy", "{id}");
    }
    drop(service);
    engine.shutdown().await.unwrap();
}
