use super::*;
use serde_json::{Value, json};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(45);

fn config() -> RecordConfig {
    let mut config = crate::record::tests::config();
    config.schema.extend(
        serde_json::from_value::<Vec<SchemaField>>(json!([
            {"field":"entity_ids","field_type":"StringSet","index_lexical":false,"store":true},
            {"field":"relations","field_type":"Json","index_lexical":false,"store":true},
        ]))
        .unwrap(),
    );
    config.max_candidates = 512;
    config.max_records = 4096;
    config.max_journal_bytes = 128 * 1024 * 1024;
    config
}

fn member(id: &str, entities: &[&str]) -> Value {
    json!({"record_id":id,"document":{"body":"alpha","entity_ids":entities}})
}

fn declaration(id: &str, from: &str, to: &str) -> Value {
    json!({"record_id":id,"document":{"body":"alpha","relations":{"edges":[{
        "from":from,"type":"link","to":to
    }]}}})
}

async fn replace(p: &RecordPartition, group: &str, version: u64, rows: Vec<Value>) -> u64 {
    let request = serde_json::from_value::<GroupReplace>(json!({
        "operation_id":OperationId::new(format!("{group}-{version}")),"group_id":group,
        "expected_group_version":version,"records":rows,
    }))
    .unwrap();
    let until = Instant::now() + WAIT;
    loop {
        match p.replace_group(request.clone()).await {
            Ok(receipt) => return receipt.seq,
            Err(RecordError::Busy) if Instant::now() < until => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(error) => panic!("group {group}@{version}: {error:?}"),
        }
    }
}

async fn build(p: &RecordPartition) -> u64 {
    let until = Instant::now() + WAIT;
    loop {
        match p.build_entity_adjacency(WAIT).await {
            Ok(cut) => return cut,
            Err(RecordError::Busy) if Instant::now() < until => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(error) => panic!("entity adjacency build: {error:?}"),
        }
    }
}

async fn base(p: &RecordPartition) {
    replace(p, "seeds", 0, vec![member("seed", &["app:A", "app:A"])]).await;
    replace(p, "targets", 0, vec![member("target", &["app:B"])]).await;
    replace(
        p,
        "edges",
        0,
        vec![
            declaration("first", "app:A", "app:B"),
            declaration("second", "app:A", "app:B"),
            declaration("loop", "app:A", "app:A"),
            declaration("two-hop", "app:B", "app:C"),
            declaration("lookalike", "app:AA", "app:BB"),
            member("two-hop-member", &["app:C"]),
            member("other-prefix", &["app:AA"]),
        ],
    )
    .await;
}

async fn paths(
    view: &EntityAdjacencyView,
    direction: EntityDirection,
    anchor: &str,
) -> EntityCandidatePaths {
    view.candidate_paths(direction, anchor, "link", 32, Instant::now() + WAIT)
        .await
        .unwrap()
}

fn expect(output: &EntityCandidatePaths, seed: &str, entity: &str, target: &str, version: u64) {
    assert_eq!(
        output.seeds(),
        [EntityMember {
            record_id: seed.into(),
            version: 1
        }]
    );
    let [neighbor] = output.neighbors() else {
        panic!("expected one neighbor")
    };
    assert_eq!(neighbor.entity_id(), entity);
    assert_eq!(
        neighbor
            .declarations()
            .iter()
            .map(|edge| edge.record_id.as_str())
            .collect::<Vec<_>>(),
        ["first", "second"]
    );
    assert_eq!(
        neighbor.members(),
        [EntityMember {
            record_id: target.into(),
            version
        }]
    );
}

#[tokio::test]
async fn entity_members_discover_both_directions_and_shadow_after_reopen() {
    use EntityDirection::{In, Out};
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("records");
    let cap = config().max_journal_bytes;
    let p = RecordPartition::create(&root, config()).await.unwrap();
    base(&p).await;
    assert_eq!(build(&p).await, 3);
    let old = p.capture_entity_adjacency().await.unwrap();
    expect(
        &paths(&old, Out, "app:A").await,
        "seed",
        "app:B",
        "target",
        1,
    );
    expect(
        &paths(&old, In, "app:B").await,
        "target",
        "app:A",
        "seed",
        1,
    );
    assert_eq!(
        replace(
            &p,
            "targets",
            1,
            vec![member("target", &["app:B", "app:other"])]
        )
        .await,
        4
    );
    expect(
        &paths(&p.capture_entity_adjacency().await.unwrap(), Out, "app:A").await,
        "seed",
        "app:B",
        "target",
        2,
    );
    expect(
        &paths(&old, Out, "app:A").await,
        "seed",
        "app:B",
        "target",
        1,
    );
    drop(old);
    assert_eq!(replace(&p, "targets", 2, vec![]).await, 5);
    assert_eq!(p.compact_journal().await.unwrap(), 5);
    p.finish_service_shutdown().await.unwrap();
    drop(p);
    let p = RecordPartition::open(&root, cap).await.unwrap();
    let view = p.capture_entity_adjacency().await.unwrap();
    assert_eq!(view.visible_seq(), 5);
    let forward = paths(&view, Out, "app:A").await;
    assert_eq!(forward.seeds().len(), 1);
    assert!(forward.neighbors().is_empty());
    let reverse = paths(&view, In, "app:B").await;
    assert!(reverse.seeds().is_empty() && reverse.neighbors().is_empty());
    drop(view);

    let (clock, now) = crate::record::tests::test_clock(&p).await;
    let expiry = now + 5_000;
    let mut expiring = member("expiring", &["app:B"]);
    expiring["temporal"] = json!({"expire_at_ms":expiry});
    assert_eq!(replace(&p, "expiring", 0, vec![expiring]).await, 6);
    expect(
        &paths(&p.capture_entity_adjacency().await.unwrap(), Out, "app:A").await,
        "seed",
        "app:B",
        "expiring",
        1,
    );
    assert_eq!(build(&p).await, 6);
    clock.set(expiry);
    let view = p.capture_entity_adjacency().await.unwrap();
    assert!(paths(&view, Out, "app:A").await.neighbors().is_empty());
    assert!(paths(&view, In, "app:B").await.seeds().is_empty());
    p.finish_service_shutdown().await.unwrap();
}

#[tokio::test]
async fn entity_members_sparse_ranges_do_not_visit_unrelated_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut logical = Vec::new();
    let mut reads = Vec::new();
    for n in [64, 512, 2048] {
        let p = RecordPartition::create(&dir.path().join(format!("records-{n}")), config())
            .await
            .unwrap();
        base(&p).await;
        for offset in (0..n).step_by(64) {
            let rows = (offset..offset + 64)
                .map(|id| member(&format!("noise-{id:05}"), &[&format!("noise:{id:05}")]))
                .collect();
            replace(&p, &format!("noise-{offset}"), 0, rows).await;
        }
        build(&p).await;
        let view = p.capture_entity_adjacency().await.unwrap();
        let deadline = Instant::now() + WAIT;
        let work = Arc::new(QueryWork::new(65_536, 2_048, 16 * 1024 * 1024, deadline));
        let result = view
            .candidate_paths_with_work(
                EntityDirection::Out,
                "app:A",
                "link",
                32,
                deadline,
                work.clone(),
            )
            .await
            .unwrap();
        expect(&result, "seed", "app:B", "target", 1);
        let (r, b) = work.remaining_io();
        let usage = 65_536 - work.remaining();
        logical.push(usage);
        reads.push(2_048 - r);
        eprintln!(
            "ENTITY-MEMBERS unrelated={n} logical={usage} positioned_reads={} bytes={}",
            2_048 - r,
            16 * 1024 * 1024 - b
        );
        p.finish_service_shutdown().await.unwrap();
    }
    assert!(
        logical.iter().all(|count| *count == logical[0]),
        "{logical:?}"
    );
    assert!(reads[2] <= reads[0] + 16, "{reads:?}");
}

#[tokio::test]
async fn entity_members_share_limits_between_seed_edges_and_targets() {
    let dir = tempfile::tempdir().unwrap();
    let p = RecordPartition::create(&dir.path().join("records"), config())
        .await
        .unwrap();
    base(&p).await;
    // The two seed/target ranges and three matching edges each fit separately,
    // but discovery must finish under a single visit limit.
    build(&p).await;
    let view = p.capture_entity_adjacency().await.unwrap();
    let deadline = Instant::now() + WAIT;
    assert!(
        matches!(view.candidate_paths(EntityDirection::Out,"app:A","link",4,deadline).await,
        Err(RecordError::Rejected(reason)) if reason.contains("visit budget"))
    );
    expect(
        &view
            .candidate_paths(EntityDirection::Out, "app:A", "link", 5, deadline)
            .await
            .unwrap(),
        "seed",
        "app:B",
        "target",
        1,
    );
    let normal = Arc::new(QueryWork::new(65_536, 2_048, 16 * 1024 * 1024, deadline));
    view.candidate_paths_with_work(
        EntityDirection::Out,
        "app:A",
        "link",
        32,
        deadline,
        normal.clone(),
    )
    .await
    .unwrap();
    let (remaining_reads, remaining_bytes) = normal.remaining_io();
    let limited = Arc::new(QueryWork::new(
        65_536 - normal.remaining() - 1,
        2_048,
        16 * 1024 * 1024,
        deadline,
    ));
    assert!(
        matches!(view.candidate_paths_with_work(EntityDirection::Out,"app:A","link",32,deadline,limited).await,
        Err(RecordError::Rejected(reason)) if reason.contains("work budget"))
    );
    assert!(remaining_reads < 2_048 && remaining_bytes < 16 * 1024 * 1024);
    for (reads, bytes) in [(0, 16 * 1024 * 1024), (2_048, 0)] {
        let limited = Arc::new(QueryWork::new(65_536, reads, bytes, deadline));
        assert!(
            matches!(view.candidate_paths_with_work(EntityDirection::Out,"app:A","link",32,deadline,limited).await,
            Err(RecordError::Rejected(reason)) if reason.contains("positioned read budget"))
        );
    }
    drop(view);
    for start in (0..257).step_by(64) {
        let rows = (start..(start + 64).min(257))
            .map(|id| member(&format!("hot-{id:03}"), &["app:B"]))
            .collect();
        replace(&p, &format!("hot-{start}"), 0, rows).await;
    }
    build(&p).await;
    let view = p.capture_entity_adjacency().await.unwrap();
    assert!(
        matches!(view.candidate_paths(EntityDirection::Out,"app:A","link",256,Instant::now()+WAIT).await,
        Err(RecordError::Rejected(reason)) if reason.contains("visit budget"))
    );
    p.finish_service_shutdown().await.unwrap();
}

#[tokio::test]
async fn entity_delivery_reuses_budget_and_refuses_a_new_root_at_the_same_cut() {
    let dir = tempfile::tempdir().unwrap();
    let p = RecordPartition::create(&dir.path().join("records"), config())
        .await
        .unwrap();
    base(&p).await;
    let cut = build(&p).await;
    let prepare = || {
        p.prepare_entity_paths(
            EntityDirection::Out,
            "app:A",
            "link",
            32,
            Instant::now() + WAIT,
        )
    };
    let selected = prepare().await.unwrap();
    let work = selected.work.clone();
    let before = work.remaining();
    let io = work.remaining_io();
    let result = selected.deliver(&p).await.unwrap();
    expect(&result, "seed", "app:B", "target", 1);
    assert_eq!(work.remaining(), before - 4);
    assert_eq!(work.remaining_io(), io);

    let limited = prepare().await.unwrap();
    limited.work.charge(limited.work.remaining() - 3).unwrap();
    assert!(matches!(limited.deliver(&p).await,
        Err(RecordError::Rejected(reason)) if reason.contains("work budget")));

    let stale = prepare().await.unwrap();
    assert_eq!(build(&p).await, cut);
    assert!(matches!(stale.deliver(&p).await, Err(RecordError::Busy)));
    p.finish_service_shutdown().await.unwrap();
}
