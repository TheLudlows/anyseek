use super::super::tests::{config, query, write};
use super::*;

const DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);

async fn build(partition: &RecordPartition) -> u64 {
    partition
        .start_event_time_checkpoint(DEADLINE)
        .unwrap()
        .finish()
        .await
        .unwrap()
}

fn dated(operation: &str, id: &str, time: i64, expected: ExpectedVersion) -> RecordWrite {
    let mut request = write(operation, id, "alpha", expected);
    request.temporal = Some(TemporalMetadata {
        event_time_ms: Some(time),
        ..Default::default()
    });
    request
}

fn projected(
    base: &Path,
    selected: &Reference,
    manifest: &Manifest,
    time: i64,
    id: &str,
) -> Option<crate::storage::disk_records::RowRef> {
    let inventory = if manifest.state.base_owner == selected.id {
        &selected.files
    } else {
        &selected.state_blocks[0].files
    };
    let descriptor = Reference::event_time_descriptor(inventory)
        .unwrap()
        .unwrap();
    let rows = manifest.state.base.record_rows().unwrap();
    let binding = rows.event_time_binding(&manifest.state.base_owner).unwrap();
    event_time::lookup_for_test(
        base,
        &binding,
        manifest.state.config().max_record_bytes,
        descriptor.bytes,
        descriptor.sha256,
        time,
        id,
    )
    .unwrap()
}

#[tokio::test]
async fn event_time_checkpoint_reopens_legacy_and_pins_the_selected_base() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let settings = config();
    let p = RecordPartition::create(&root, settings.clone())
        .await
        .unwrap();
    p.write(dated("new", "item", -100, ExpectedVersion::Absent))
        .await
        .unwrap();
    p.write(dated("gone", "gone", 90, ExpectedVersion::Absent))
        .await
        .unwrap();
    p.write(write(
        "undated",
        "undated",
        "alpha",
        ExpectedVersion::Absent,
    ))
    .await
    .unwrap();
    p.index_committed(DEADLINE).await.unwrap();
    {
        let inner = p.inner.lock().await;
        let legacy = inner.anchor.as_ref().unwrap();
        assert!(
            Reference::event_time_descriptor(&legacy.files)
                .unwrap()
                .is_none()
        );
    }
    drop(p);
    let p = RecordPartition::open(&root, settings.max_journal_bytes)
        .await
        .unwrap();
    assert_eq!(query(&p, SearchMode::Lexical).await.count_total, 3);
    let mut delete = write("delete", "gone", "", ExpectedVersion::Match(1));
    delete.document = None;
    delete.temporal = None;
    p.write(delete).await.unwrap();
    assert_eq!(build(&p).await, 4);

    let selected_bytes = serde_json::to_vec(p.inner.lock().await.anchor.as_ref().unwrap()).unwrap();
    let selected: Reference = serde_json::from_slice(&selected_bytes).unwrap();
    let (path, manifest, _pool, _lease) = selected
        .authenticated_manifest(&root, settings.max_journal_bytes)
        .unwrap();
    let row = projected(&path, &selected, &manifest, -100, "item").unwrap();
    assert!(projected(&path, &selected, &manifest, 90, "gone").is_none());
    assert!(projected(&path, &selected, &manifest, 0, "undated").is_none());
    drop(p);
    let p = RecordPartition::open(&root, settings.max_journal_bytes)
        .await
        .unwrap();
    let old = p.inner.lock().await.state.records.clone();
    let old_source = old.source("item").unwrap();
    let old_row = old_source.find("item").unwrap().unwrap();
    let state_values::CurrentRow::Sealed(old_row) = old_row else {
        panic!("expected certified row")
    };
    assert_eq!(old_row.address, Some(row.address));

    p.write(dated("update", "item", 200, ExpectedVersion::Match(1)))
        .await
        .unwrap();
    assert_eq!(p.compact_journal().await.unwrap(), 5);
    drop(p);
    let p = RecordPartition::open(&root, settings.max_journal_bytes)
        .await
        .unwrap();
    {
        let inner = p.inner.lock().await;
        let incremental = inner.anchor.as_ref().unwrap();
        let (_, image, _, _) = incremental
            .authenticated_manifest(&root, settings.max_journal_bytes)
            .unwrap();
        assert_eq!(image.state.base_owner, selected.id);
        assert_eq!(image.state.deltas.len(), 1);
        assert!(
            Reference::event_time_descriptor(&incremental.files)
                .unwrap()
                .is_none()
        );
        assert!(projected(&path, incremental, &image, 200, "item").is_none());
    }

    assert_eq!(build(&p).await, 5);
    let now = p.inner.lock().await.anchor.clone().unwrap();
    let (current_path, current, _, _) = now
        .authenticated_manifest(&root, settings.max_journal_bytes)
        .unwrap();
    assert!(projected(&current_path, &now, &current, 200, "item").is_some());
    assert!(projected(&current_path, &now, &current, -100, "item").is_none());
    assert_eq!(
        projected(&path, &selected, &manifest, -100, "item"),
        Some(row)
    );
    assert_eq!(
        p.get("item").await.unwrap().unwrap().temporal.event_time_ms,
        Some(200)
    );
    assert_eq!(query(&p, SearchMode::Lexical).await.count_total, 2);

    drop(old_source);
    drop(old);
    p.inner.lock().await.reclaim_anchor_objects().await.unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn event_time_checkpoint_rejects_wrong_binding_and_corrupt_selected_page() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let settings = config();
    let p = RecordPartition::create(&root, settings.clone())
        .await
        .unwrap();
    p.write(dated("first", "one", 0, ExpectedVersion::Absent))
        .await
        .unwrap();
    build(&p).await;
    let old = p.inner.lock().await.anchor.clone().unwrap();
    let old_descriptor = std::fs::read(
        directory(&root, &old.id)
            .unwrap()
            .join(event_time::MEMBERS[0]),
    )
    .unwrap();

    build(&p).await;
    let selected_bytes = serde_json::to_vec(p.inner.lock().await.anchor.as_ref().unwrap()).unwrap();
    let mut selected: Reference = serde_json::from_slice(&selected_bytes).unwrap();
    let path = directory(&root, &selected.id).unwrap();
    let descriptor_path = path.join(event_time::MEMBERS[0]);
    let good = std::fs::read(&descriptor_path).unwrap();
    std::fs::write(&descriptor_path, old_descriptor).unwrap();
    // Recomputing only the fixture inventory exercises semantic binding after
    // file hashes pass. The actual journal certificate is left untouched.
    Arc::get_mut(&mut selected.metadata).unwrap().data.files =
        files(&path, closure_limit(settings.max_journal_bytes).unwrap()).unwrap();
    let Err(error) = selected.load_local(&root, settings.max_journal_bytes).await else {
        panic!("accepted an event-time descriptor from another owner")
    };
    assert!(
        error
            .to_string()
            .contains("differs from its selected State base"),
        "{error}"
    );
    std::fs::write(&descriptor_path, good).unwrap();

    let pages_path = path.join(event_time::MEMBERS[2]);
    let mut pages = std::fs::read(&pages_path).unwrap();
    pages[20] ^= 1;
    std::fs::write(&pages_path, pages).unwrap();
    Arc::get_mut(&mut selected.metadata).unwrap().data.files =
        files(&path, closure_limit(settings.max_journal_bytes).unwrap()).unwrap();
    let Err(error) = selected.load_local(&root, settings.max_journal_bytes).await else {
        panic!("accepted a bad event-time root page")
    };
    assert!(
        error.to_string().contains("corrupt primary page"),
        "{error}"
    );

    drop(p);
    let Err(error) = RecordPartition::open(&root, settings.max_journal_bytes).await else {
        panic!("reopened a damaged selected package")
    };
    assert!(
        error.to_string().contains("closure checksum mismatch"),
        "{error}"
    );
}
