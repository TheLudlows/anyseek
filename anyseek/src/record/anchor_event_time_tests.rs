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
    manifest: &Manifest,
    time: i64,
    id: &str,
) -> Option<crate::storage::disk_records::RowRef> {
    let mut found = None;
    manifest
        .state
        .event_time
        .as_ref()
        .unwrap()
        .visit(
            time..time + 1,
            state_rows::read_error,
            |candidate, _, row| {
                if candidate == id {
                    found = Some(row);
                }
                Ok(found.is_none())
            },
        )
        .unwrap();
    found
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
    let row = projected(&manifest, -100, "item").unwrap();
    assert!(projected(&manifest, 90, "gone").is_none());
    assert!(projected(&manifest, 0, "undated").is_none());
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
        assert!(projected(&image, 200, "item").is_none());
    }

    assert_eq!(build(&p).await, 5);
    let now = p.inner.lock().await.anchor.clone().unwrap();
    let (_, current, _, _) = now
        .authenticated_manifest(&root, settings.max_journal_bytes)
        .unwrap();
    assert!(projected(&current, 200, "item").is_some());
    assert!(projected(&current, -100, "item").is_none());
    assert!(path.exists());
    assert_eq!(projected(&manifest, -100, "item"), Some(row));
    assert_eq!(
        p.get("item").await.unwrap().unwrap().temporal.event_time_ms,
        Some(200)
    );
    assert_eq!(query(&p, SearchMode::Lexical).await.count_total, 2);

    drop(manifest);
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

use super::super::event_time_view::Window;
use std::{
    ops::Range,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::time::Instant as NativeDeadline;

fn native_deadline() -> NativeDeadline {
    NativeDeadline::now() + DEADLINE
}

async fn read_window(
    partition: &RecordPartition,
    range: Range<i64>,
    limit: usize,
) -> Result<Window, RecordError> {
    partition
        .read_event_time_window(range, limit, native_deadline())
        .await
}

fn hit_ids(window: &Window) -> Vec<&str> {
    window.hits.iter().map(|hit| hit.id.as_str()).collect()
}

struct EventClock(AtomicU64);

impl temporal::Clock for EventClock {
    fn now_ms(&self) -> Result<u64, RecordError> {
        Ok(self.0.load(Ordering::Acquire))
    }
}

#[tokio::test]
async fn event_time_window_joins_all_manifest_deltas_and_wal_tail_then_reopens() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let settings = config();
    let p = RecordPartition::create(&root, settings.clone())
        .await
        .unwrap();
    let now = p.inner.lock().await.observe_time().unwrap();
    let clock = Arc::new(EventClock(AtomicU64::new(now)));
    p.inner.lock().await.clock = clock.clone();
    for (id, time) in [("base", 100), ("move", 180), ("gone", 170), ("tie-z", 120)] {
        p.write(dated(id, id, time, ExpectedVersion::Absent))
            .await
            .unwrap();
    }
    let mut ttl = dated("ttl", "ttl", 160, ExpectedVersion::Absent);
    ttl.temporal.as_mut().unwrap().expire_at_ms = Some(now + 2_000);
    p.write(ttl).await.unwrap();
    assert_eq!(build(&p).await, 5);
    assert_eq!(
        hit_ids(&read_window(&p, 120..170, 4).await.unwrap()),
        ["ttl", "tie-z"]
    );

    p.write(dated(
        "move-updated",
        "move",
        201,
        ExpectedVersion::Match(1),
    ))
    .await
    .unwrap();
    p.write(dated(
        "delta-one",
        "delta-one",
        150,
        ExpectedVersion::Absent,
    ))
    .await
    .unwrap();
    assert_eq!(p.compact_journal().await.unwrap(), 7);
    let mut delete = write("delete-gone", "gone", "", ExpectedVersion::Match(1));
    delete.document = None;
    p.write(delete).await.unwrap();
    p.write(dated(
        "delta-two",
        "delta-two",
        120,
        ExpectedVersion::Absent,
    ))
    .await
    .unwrap();
    assert_eq!(p.compact_journal().await.unwrap(), 9);
    {
        let inner = p.inner.lock().await;
        let (_, selected, _, _) = inner
            .anchor
            .as_ref()
            .unwrap()
            .authenticated_manifest(&root, settings.max_journal_bytes)
            .unwrap();
        assert_eq!(selected.state.deltas.len(), 2);
    }

    p.write(dated(
        "tail-move",
        "delta-one",
        220,
        ExpectedVersion::Match(1),
    ))
    .await
    .unwrap();
    p.write(dated("tail-new", "a-tail", 120, ExpectedVersion::Absent))
        .await
        .unwrap();
    let before = read_window(&p, 0..200, 10).await.unwrap();
    assert_eq!(before.cut, 11);
    assert_eq!(before.evaluation_time_ms, now);
    assert_eq!(
        hit_ids(&before),
        ["ttl", "a-tail", "delta-two", "tie-z", "base"]
    );
    assert_eq!(
        hit_ids(&read_window(&p, 0..200, 2).await.unwrap()),
        ["ttl", "a-tail"]
    );
    let moved = read_window(&p, 200..300, 4).await.unwrap();
    assert_eq!(
        moved
            .hits
            .iter()
            .map(|hit| (hit.id.as_str(), hit.event_time_ms, hit.version))
            .collect::<Vec<_>>(),
        [("delta-one", 220, 2), ("move", 201, 2)]
    );

    let gate = root.join(".test-event-time-delivery-pause");
    std::fs::write(&gate, b"pause").unwrap();
    let reader = p.clone();
    let pending = tokio::spawn(async move { read_window(&reader, 0..200, 10).await });
    wait_file(&root.join(".test-event-time-delivery-started")).await;
    clock.0.store(now + 2_000, Ordering::Release);
    std::fs::remove_file(gate).unwrap();
    assert!(matches!(pending.await.unwrap(), Err(RecordError::Busy)));
    let expected = ["a-tail", "delta-two", "tie-z", "base"];
    assert_eq!(
        hit_ids(&read_window(&p, 0..200, 10).await.unwrap()),
        expected
    );
    assert!(p.get("ttl").await.unwrap().is_none());
    p.finish_service_shutdown().await.unwrap();
    drop(p);
    let p = RecordPartition::open(&root, settings.max_journal_bytes)
        .await
        .unwrap();
    p.inner.lock().await.clock = clock;
    let replayed = read_window(&p, 0..200, 10).await.unwrap();
    assert_eq!(replayed.cut, 11);
    assert_eq!(hit_ids(&replayed), expected);
    assert_eq!(build(&p).await, 11);
    assert_eq!(
        hit_ids(&read_window(&p, 0..200, 10).await.unwrap()),
        expected
    );
    assert!(p.get("gone").await.unwrap().is_none());
}

async fn wait_file(path: &Path) {
    tokio::time::timeout(DEADLINE, async {
        while !path.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn event_time_window_refuses_cursor_lag_and_replaced_publication_at_same_sequence() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    p.write(dated("first", "base", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    assert!(matches!(read_window(&p, 0..30, 2).await,
        Err(RecordError::Rejected(reason)) if reason.contains("no projection")));
    assert_eq!(query(&p, SearchMode::Lexical).await.count_total, 1);
    assert_eq!(build(&p).await, 1);

    {
        let _publication = p.inner.lock().await;
        let deadline = NativeDeadline::now() + std::time::Duration::from_millis(5);
        assert!(matches!(
            p.read_event_time_window(0..30, 2, deadline).await,
            Err(RecordError::Busy)
        ));
    }

    let lane_stop = root.join(".test-lexical-consumer-pause");
    std::fs::write(&lane_stop, b"pause").unwrap();
    let batch = p
        .commit_writes(vec![dated("later", "tail", 27, ExpectedVersion::Absent)])
        .await
        .unwrap();
    assert!(matches!(
        batch.results.as_slice(),
        [CommittedItem::Committed(_)]
    ));
    wait_file(&root.join(".test-lexical-consumer-started")).await;
    let progress = p.committed_progress().await;
    assert_eq!(progress.committed_seq, Some(2));
    assert_eq!(progress.lexical_seq, 1);
    assert!(matches!(
        read_window(&p, 0..30, 2).await,
        Err(RecordError::Busy)
    ));
    std::fs::remove_file(lane_stop).unwrap();
    p.inner.lock().await.index.drain_indexes().await.unwrap();
    assert_eq!(
        hit_ids(&read_window(&p, 0..30, 2).await.unwrap()),
        ["tail", "base"]
    );

    let delivery = root.join(".test-event-time-delivery-pause");
    std::fs::write(&delivery, b"pause").unwrap();
    let reader = p.clone();
    let pending = tokio::spawn(async move { read_window(&reader, 0..30, 2).await });
    wait_file(&root.join(".test-event-time-delivery-started")).await;
    assert_eq!(build(&p).await, 2);
    std::fs::remove_file(delivery).unwrap();
    assert!(matches!(pending.await.unwrap(), Err(RecordError::Busy)));
    assert_eq!(
        hit_ids(&read_window(&p, 0..30, 2).await.unwrap()),
        ["tail", "base"]
    );
}

#[tokio::test]
async fn event_time_window_refuses_oversize_suffix_without_changing_ordinary_reads() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let mut settings = config();
    settings.max_records = 256;
    settings.max_operations = 256;
    settings.max_journal_bytes = 4 * 1024 * 1024;
    settings.max_candidates = 10;
    let p = RecordPartition::create(&root, settings).await.unwrap();
    p.write(dated("base", "base", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    assert_eq!(build(&p).await, 1);
    for i in 0..129 {
        let id = format!("tail-{i:03}");
        p.write(dated(&id, &id, 1000 + i, ExpectedVersion::Absent))
            .await
            .unwrap();
    }
    assert!(matches!(read_window(&p, 0..30, 2).await,
        Err(RecordError::Rejected(reason)) if reason.contains("row budget")));
    assert_eq!(query(&p, SearchMode::Lexical).await.count_total, 130);
    assert_eq!(build(&p).await, 130);
    assert_eq!(hit_ids(&read_window(&p, 0..30, 2).await.unwrap()), ["base"]);
    for i in 119..128 {
        let id = format!("tail-{i:03}");
        p.write(dated(
            &format!("move-{id}"),
            &id,
            2000,
            ExpectedVersion::Match(1),
        ))
        .await
        .unwrap();
    }
    assert_eq!(
        hit_ids(&read_window(&p, 1000..1200, 1).await.unwrap()),
        ["tail-128"]
    );
    assert!(matches!(read_window(&p, 1000..1200, 2).await,
        Err(RecordError::Rejected(reason)) if reason.contains("base candidate budget")));
}
