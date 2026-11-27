use super::*;
use serde_json::{Value, json};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test]
async fn entity_adjacency_build_preserves_selected_event_time_projection() {
    let directory = tempfile::tempdir().unwrap();
    let p = RecordPartition::create(&directory.path().join("records"), settings())
        .await
        .unwrap();
    let mut dated = row("ab", vec![edge("app:A", "link", "app:B")]);
    dated["temporal"] = json!({"event_time_ms":25});
    replace(&p, "dated", 0, vec![dated]).await;
    assert_eq!(
        p.start_event_time_checkpoint(DEADLINE)
            .unwrap()
            .finish()
            .await
            .unwrap(),
        1
    );
    assert_eq!(p.build_entity_adjacency(DEADLINE).await.unwrap(), 1);
    expect(
        &p.capture_entity_adjacency().await.unwrap(),
        EntityDirection::Out,
        "app:A",
        &[("app:B", "ab", 1)],
    )
    .await;
    let inner = p.inner.lock().await;
    let base = inner.state.records.event_time_base().unwrap();
    assert_eq!(base.event_time.as_ref().unwrap().cut(), 1);
    assert!(Arc::ptr_eq(
        base,
        inner.state.records.entity_base().unwrap()
    ));
    drop(inner);
    p.finish_service_shutdown().await.unwrap();
}

fn settings() -> RecordConfig {
    let mut config = super::super::tests::config();
    config.schema.push(
        serde_json::from_value(json!({
            "field":"relations","field_type":"Json","store":true,"index_lexical":false
        }))
        .unwrap(),
    );
    config.max_candidates = 512;
    config
}

fn edge(from: &str, kind: &str, to: &str) -> Value {
    json!({"from":from,"type":kind,"to":to})
}

fn row(id: &str, edges: Vec<Value>) -> Value {
    json!({"record_id":id,"document":{"body":"alpha","relations":{"edges":edges}}})
}

async fn replace(p: &RecordPartition, group: &str, previous: u64, rows: Vec<Value>) -> u64 {
    let request = serde_json::from_value(json!({
        "operation_id":OperationId::new(format!("{group}-{previous}")),
        "group_id":group,"expected_group_version":previous,"records":rows
    }))
    .unwrap();
    p.replace_group(request).await.unwrap().seq
}

async fn expect(
    view: &EntityAdjacencyView,
    direction: EntityDirection,
    anchor: &str,
    expected: &[(&str, &str, u64)],
) {
    let candidates = view.candidates(direction, anchor, "link", 8).await.unwrap();
    let observed: Vec<_> = candidates
        .as_slice()
        .iter()
        .map(|candidate| {
            (
                candidate.neighbor.as_str(),
                candidate.record_id.as_str(),
                candidate.version,
            )
        })
        .collect();
    assert_eq!(observed, expected);
}

#[tokio::test]
async fn entity_adjacency_reopens_shadows_and_pins_authenticated_root() {
    use EntityDirection::{In, Out};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("records");
    let config = settings();
    let p = RecordPartition::create(&root, config.clone())
        .await
        .unwrap();
    assert_eq!(
        replace(
            &p,
            "ab",
            0,
            vec![row("ab", vec![edge("app:A", "link", "app:B"); 2])]
        )
        .await,
        1
    );
    assert_eq!(
        replace(
            &p,
            "other",
            0,
            vec![
                row("zb", vec![edge("app:Z", "link", "app:B")]),
                row(
                    "bounds",
                    vec![
                        edge("app:AA", "link", "app:BB"),
                        edge("app:A", "linker", "app:B")
                    ]
                ),
            ]
        )
        .await,
        2
    );
    assert_eq!(p.index_committed(DEADLINE).await.unwrap(), 2);
    assert!(matches!(
        p.capture_entity_adjacency().await,
        Err(RecordError::Rejected(_))
    ));
    assert_eq!(p.build_entity_adjacency(DEADLINE).await.unwrap(), 2);
    let old_root = {
        let inner = p.inner.lock().await;
        assert!(inner.state.records.event_time_base().is_none());
        root.join(format!(
            "anchor-{}",
            inner.anchor.as_ref().unwrap().identity()
        ))
    };
    let old = p.capture_entity_adjacency().await.unwrap();
    assert_eq!(old.visible_seq(), 2);
    expect(&old, Out, "app:A", &[("app:B", "ab", 1)]).await;
    expect(&old, In, "app:B", &[("app:A", "ab", 1), ("app:Z", "zb", 1)]).await;
    p.finish_service_shutdown().await.unwrap();
    drop(p);
    assert!(matches!(
        old.candidates(Out, "app:A", "link", 8).await,
        Err(RecordError::RecoveryRequired)
    ));
    drop(old);

    let p = RecordPartition::open(&root, config.max_journal_bytes)
        .await
        .unwrap();
    let old = p.capture_entity_adjacency().await.unwrap();
    expect(&old, In, "app:B", &[("app:A", "ab", 1), ("app:Z", "zb", 1)]).await;
    assert_eq!(
        replace(
            &p,
            "ab",
            1,
            vec![row("ab", vec![edge("app:A", "link", "app:C")])]
        )
        .await,
        3
    );
    let after_update = p.capture_entity_adjacency().await.unwrap();
    assert_eq!(after_update.visible_seq(), 3);
    expect(&after_update, Out, "app:A", &[("app:C", "ab", 2)]).await;
    expect(&after_update, In, "app:B", &[("app:Z", "zb", 1)]).await;
    expect(&after_update, In, "app:C", &[("app:A", "ab", 2)]).await;
    drop(after_update);
    assert_eq!(replace(&p, "ab", 2, vec![]).await, 4);
    assert_eq!(p.compact_journal().await.unwrap(), 4);
    expect(
        &p.capture_entity_adjacency().await.unwrap(),
        Out,
        "app:A",
        &[],
    )
    .await;
    expect(&old, Out, "app:A", &[("app:B", "ab", 1)]).await;
    drop(old);
    p.finish_service_shutdown().await.unwrap();
    drop(p);

    let p = RecordPartition::open(&root, config.max_journal_bytes)
        .await
        .unwrap();
    let delta = p.capture_entity_adjacency().await.unwrap();
    assert_eq!(delta.visible_seq(), 4);
    expect(&delta, Out, "app:A", &[]).await;
    expect(&delta, In, "app:B", &[("app:Z", "zb", 1)]).await;
    assert_eq!(p.build_entity_adjacency(DEADLINE).await.unwrap(), 4);
    assert!(old_root.is_dir());
    expect(&delta, In, "app:B", &[("app:Z", "zb", 1)]).await;
    drop(delta);
    p.inner.lock().await.reclaim_anchor_objects().await.unwrap();
    assert!(!old_root.exists());

    let selected = {
        let inner = p.inner.lock().await;
        root.join(format!(
            "anchor-{}",
            inner.anchor.as_ref().unwrap().identity()
        ))
    };
    let view = p.capture_entity_adjacency().await.unwrap();
    let pages = selected.join(entity::MEMBERS[2]);
    let mut bytes = std::fs::read(&pages).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(&pages, bytes).unwrap();
    assert!(matches!(
        view.candidates(In, "app:B", "link", 8).await,
        Err(RecordError::Storage(_))
    ));
    drop(view);
    p.finish_service_shutdown().await.unwrap();
    drop(p);
    assert!(matches!(
        RecordPartition::open(&root, config.max_journal_bytes).await,
        Err(RecordError::Storage(_))
    ));
}

#[tokio::test]
async fn entity_adjacency_rejects_hot_fanout_without_a_partial_range() {
    let directory = tempfile::tempdir().unwrap();
    let p = RecordPartition::create(&directory.path().join("records"), settings())
        .await
        .unwrap();
    let rows = (0..5)
        .map(|chunk| {
            row(
                &format!("claim-{chunk}"),
                (chunk * 64..((chunk + 1) * 64).min(257))
                    .map(|id| edge(&format!("app:F{id:03}"), "link", "app:hot"))
                    .collect(),
            )
        })
        .collect();
    replace(&p, "hot", 0, rows).await;
    p.build_entity_adjacency(DEADLINE).await.unwrap();
    let view = p.capture_entity_adjacency().await.unwrap();
    assert!(
        matches!(view.candidates(EntityDirection::In,"app:hot","link",256).await,
        Err(RecordError::Rejected(reason)) if reason.contains("visit budget"))
    );
    assert!(
        matches!(view.candidates(EntityDirection::In,"app:hot","link",257).await,
        Err(RecordError::Rejected(reason)) if reason.contains("visit limit"))
    );
    expect(
        &view,
        EntityDirection::Out,
        "app:F000",
        &[("app:hot", "claim-0", 1)],
    )
    .await;
    p.output.write().unwrap().fenced = true;
    assert!(matches!(
        view.candidates(EntityDirection::Out, "app:F000", "link", 8)
            .await,
        Err(RecordError::RecoveryRequired)
    ));
    p.finish_service_shutdown().await.unwrap();
}

#[tokio::test]
async fn entity_adjacency_invalid_declaration_keeps_canonical_write_and_selected_sidecar() {
    let directory = tempfile::tempdir().unwrap();
    let config = settings();
    let p = RecordPartition::create(&directory.path().join("records"), config)
        .await
        .unwrap();
    replace(
        &p,
        "valid",
        0,
        vec![row("valid", vec![edge("app:A", "link", "app:B")])],
    )
    .await;
    assert_eq!(p.build_entity_adjacency(DEADLINE).await.unwrap(), 1);
    let old = p.capture_entity_adjacency().await.unwrap();
    assert_eq!(
        replace(
            &p,
            "invalid",
            0,
            vec![row("invalid", vec![edge("no_namespace", "link", "app:B")])]
        )
        .await,
        2
    );
    assert!(matches!(
        p.build_entity_adjacency(DEADLINE).await,
        Err(RecordError::Rejected(_))
    ));
    assert_eq!(p.get("invalid").await.unwrap().unwrap().version, 1);
    expect(
        &old,
        EntityDirection::Out,
        "app:A",
        &[("app:B", "valid", 1)],
    )
    .await;
    let current = p.capture_entity_adjacency().await.unwrap();
    assert_eq!(current.visible_seq(), 2);
    assert!(matches!(
        current
            .candidates(EntityDirection::Out, "app:A", "link", 8)
            .await,
        Err(RecordError::Rejected(_))
    ));
    p.finish_service_shutdown().await.unwrap();
}
