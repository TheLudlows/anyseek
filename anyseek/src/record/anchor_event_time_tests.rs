use super::super::tests::{config, query, write};
use super::*;
use crate::{search::QueryWork, vector_similarity::AnnMode};

const DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);
const ANN_DIMENSIONS: usize = 256;
const ANN_ROWS: usize = 120;
const ANN_WORK: usize = 65_536;
const ANN_RANGES: usize = 2048;
const ANN_BYTES: usize = 16 * 1024 * 1024;

fn ann_embedding(axis: usize) -> Vec<f32> {
    let mut vector = vec![0.; ANN_DIMENSIONS];
    vector[axis] = 1.;
    vector
}

async fn ann_fixture(
    root: &Path,
    levels: usize,
    medoids: usize,
    winners: usize,
    expiry: Option<u64>,
) -> RecordPartition {
    let mut settings = config();
    settings.index.clustering = crate::index::Clustering::Fixed(medoids);
    settings.index.inference = serde_json::from_value(serde_json::json!({"External":{
        "dimensions":ANN_DIMENSIONS,"precision":"F32","quantization":"None","similarity":"Cosine"
    }}))
    .unwrap();
    settings.max_records = 256;
    settings.max_operations = 512;
    settings.max_journal_bytes = 16 * 1024 * 1024;
    settings.schema.push(
        serde_json::from_value(serde_json::json!({
            "field":"priority","field_type":"Int","store":false,"facet":true,"index_lexical":false
        }))
        .unwrap(),
    );
    let p = RecordPartition::create(root, settings).await.unwrap();
    for level in 0..levels {
        for row in 0..ANN_ROWS {
            let id = format!("s{level}-r{row:03}");
            let time = if level == 0 && row == 1 { 25 } else { 100 };
            let mut record = dated(&id, &id, time, ExpectedVersion::Absent);
            if level == 0 && row == 0 {
                record.temporal.as_mut().unwrap().expire_at_ms = expiry;
            }
            let axis = if row < winners {
                0
            } else {
                1 + (row - winners) % (medoids - 1)
            };
            let doc = record.document.as_mut().unwrap();
            doc.insert("vector".into(), serde_json::json!(ann_embedding(axis)));
            doc.insert("priority".into(), 1.into());
            p.write(record).await.unwrap();
        }
        if level == 0 {
            assert_eq!(build(&p).await, ANN_ROWS as u64);
        } else {
            // A new full projection would rebuild both populations into one
            // component. Seal the committed suffix through the real cache
            // rotation so both clustered vector files belong to the view.
            assert_eq!(super::super::tests::seal_test_cache_tail(&p).await, level + 1);
        }
    }
    p
}

async fn run_ann_probe(
    p: &RecordPartition,
    mode: AnnMode,
    facets: Vec<FacetFilter>,
    limits: (usize, usize, usize),
) -> Result<
    (
        Window,
        Vec<super::super::query::RetrievalEvidence>,
        Arc<QueryWork>,
    ),
    RecordError,
> {
    let deadline = native_deadline();
    let work = Arc::new(QueryWork::new(limits.0, limits.1, limits.2, deadline));
    let (window, evidence) = p
        .probe_metered_vector_cut(
            25..26,
            2,
            Embedding::F32(ann_embedding(0)),
            mode,
            facets,
            deadline,
            work.clone(),
        )
        .await?;
    Ok((window, evidence, work))
}

#[tokio::test]
async fn metered_ann_separates_discovery_probes_members_and_positioned_io() {
    let temp = tempfile::tempdir().unwrap();
    let full = (ANN_WORK, ANN_RANGES, ANN_BYTES);
    let reject_all = FacetFilter::Int {
        field: "priority".into(),
        filter: 9..10,
    };
    for (name, levels, medoids, winners, reject) in [
        ("base", 1, 2, 24, false),
        ("medoids", 1, 4, 24, false),
        ("levels", 2, 2, 24, false),
        ("filtered-base", 1, 2, 24, true),
        ("filtered-wide", 1, 2, 72, true),
    ] {
        let p = ann_fixture(&temp.path().join(name), levels, medoids, winners, None).await;
        let components = p.inner.lock().await.index.components().await;
        let mut frozen = 0;
        for component in components {
            frozen += usize::from(component.read().await.vector_levels > 0);
        }
        assert_eq!(frozen, levels, "{name}: frozen vector components");
        let facets = if reject {
            vec![reject_all.clone()]
        } else {
            vec![]
        };
        let (exact_window, exact, exact_work) =
            run_ann_probe(&p, AnnMode::All, facets.clone(), full)
                .await
                .unwrap();
        let (window, approximate, work) =
            run_ann_probe(&p, AnnMode::Nprobe(1), facets.clone(), full)
                .await
                .unwrap();
        assert_eq!(window.cut, (levels * ANN_ROWS) as u64);
        assert_eq!(window.cut, exact_window.cut);
        assert_eq!(
            hit_ids(&window),
            if reject { vec![] } else { vec!["s0-r001"] }
        );
        let keys = |hits: &[super::super::query::RetrievalEvidence]| {
            hits.iter()
                .map(|hit| (hit.id.clone(), hit.version, hit.score.to_bits()))
                .collect::<Vec<_>>()
        };
        assert_eq!(keys(&approximate), keys(&exact));
        assert_eq!(approximate.len(), if reject { 0 } else { 2 });
        let visits = &work.vector_visits;
        let exact_visits = &exact_work.vector_visits;
        assert_eq!(visits.levels.load(Ordering::Relaxed), levels, "{name}");
        assert_eq!(
            visits.header_clusters.load(Ordering::Relaxed),
            medoids * levels,
            "{name}"
        );
        assert_eq!(
            visits.medoids.load(Ordering::Relaxed),
            medoids * levels,
            "{name}"
        );
        assert_eq!(visits.clusters.load(Ordering::Relaxed), levels, "{name}");
        assert_eq!(
            visits.sealed_members.load(Ordering::Relaxed),
            winners * levels,
            "{name}"
        );
        assert_eq!(visits.tail_members.load(Ordering::Relaxed), 0, "{name}");
        assert_eq!(exact_visits.medoids.load(Ordering::Relaxed), 0);
        assert_eq!(
            exact_visits.sealed_members.load(Ordering::Relaxed),
            ANN_ROWS * levels
        );
        let vector_io = |work: &QueryWork| {
            (
                work.vector_visits.vector_ranges.load(Ordering::Relaxed),
                work.vector_visits.vector_bytes.load(Ordering::Relaxed),
            )
        };
        let remaining = work.remaining_io();
        let used = (
            ANN_WORK - work.remaining(),
            ANN_RANGES - remaining.0,
            ANN_BYTES - remaining.1,
        );
        assert!(vector_io(&work).0 >= levels && vector_io(&work).1 >= 65_536 * levels);
        assert!(used.1 >= vector_io(&work).0 && used.2 >= vector_io(&work).1);
        eprintln!(
            "{name}: levels={levels} headers={} medoids={} probes={levels} members={} candidates={} ANN work/ranges/bytes={used:?} vector file={:?}, exact vector file={:?}",
            medoids * levels,
            medoids * levels,
            winners * levels,
            approximate.len(),
            vector_io(&work),
            vector_io(&exact_work)
        );
        if name == "base" {
            let ordinary = p
                .search(
                    String::new(),
                    Some(Embedding::F32(ann_embedding(0))),
                    SearchMode::Vector {
                        similarity_threshold: None,
                        ann_mode: AnnMode::Nprobe(1),
                    },
                    vec![],
                    2,
                )
                .await
                .unwrap();
            let ordinary_keys: Vec<_> = ordinary
                .hits
                .iter()
                .map(|hit| {
                    (
                        hit.record.record_id.clone(),
                        hit.record.version,
                        hit.score.to_bits(),
                    )
                })
                .collect();
            assert_eq!(ordinary_keys, keys(&approximate));
            let before_selection =
                ANN_WORK - visits.remaining_after_medoids.load(Ordering::Relaxed);
            let deadline = native_deadline();
            let cap = Arc::new(QueryWork::new(
                before_selection,
                ANN_RANGES,
                ANN_BYTES,
                deadline,
            ));
            let result = p
                .probe_metered_vector_cut(
                    25..26,
                    2,
                    Embedding::F32(ann_embedding(0)),
                    AnnMode::Nprobe(1),
                    facets.clone(),
                    deadline,
                    cap.clone(),
                )
                .await;
            assert!(
                matches!(result, Err(RecordError::Rejected(reason)) if reason == "shared query work budget exhausted")
            );
            assert_eq!(cap.vector_visits.medoids.load(Ordering::Relaxed), medoids);
            assert_eq!(cap.vector_visits.clusters.load(Ordering::Relaxed), 0);
            assert_eq!(cap.vector_visits.sealed_members.load(Ordering::Relaxed), 0);
        }
        if name == "levels" {
            for (budget, message) in [
                (
                    (used.0 - 1, used.1, used.2),
                    "shared query work budget exhausted",
                ),
                (
                    (used.0, used.1 - 1, used.2),
                    "shared query positioned read budget exhausted",
                ),
                (
                    (used.0, used.1, used.2 - 1),
                    "shared query positioned read budget exhausted",
                ),
            ] {
                assert!(
                    matches!(run_ann_probe(&p, AnnMode::Nprobe(1), facets.clone(), budget).await,
                    Err(RecordError::Rejected(reason)) if reason == message),
                    "short budget {budget:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn metered_ann_uses_canonical_versions_expiry_and_worker_admission() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let expiry = super::super::unix_time_ms_i64() as u64 + 120_000;
    let p = ann_fixture(&root, 2, 2, 24, Some(expiry)).await;
    let (clock, _) = super::super::tests::test_clock(&p).await;
    let full = (ANN_WORK, ANN_RANGES, ANN_BYTES);
    let replacement = |operation: &str, version| {
        let mut row = dated(operation, "s0-r001", 25, ExpectedVersion::Match(version));
        let doc = row.document.as_mut().unwrap();
        doc.insert("vector".into(), serde_json::json!(ann_embedding(0)));
        doc.insert("priority".into(), 1.into());
        row
    };
    p.write(replacement("first-revision", 1)).await.unwrap();
    let (window, initial, initial_work) = run_ann_probe(&p, AnnMode::Nprobe(1), vec![], full)
        .await
        .unwrap();
    assert_eq!(window.cut, 241);
    assert_eq!(
        initial
            .iter()
            .map(|h| (h.id.as_str(), h.version))
            .collect::<Vec<_>>(),
        [("s0-r000", 1), ("s0-r001", 2)]
    );
    assert_eq!(
        initial_work
            .vector_visits
            .tail_members
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(initial[0].expiry, Some(expiry));

    let deliver = root.join(".test-event-time-delivery-pause");
    let delivered = root.join(".test-event-time-delivery-started");
    for expire in [true, false] {
        std::fs::write(&deliver, b"pause").unwrap();
        let reader = p.clone();
        let pending =
            tokio::spawn(
                async move { run_ann_probe(&reader, AnnMode::Nprobe(1), vec![], full).await },
            );
        wait_file(&delivered).await;
        if expire {
            clock.set(expiry + 1000);
            assert!(p.get("s0-r000").await.unwrap().is_none());
            clock.set(expiry - 7000);
        } else {
            p.write(replacement("second-revision", 2)).await.unwrap();
        }
        std::fs::remove_file(&deliver).unwrap();
        std::fs::remove_file(&delivered).unwrap();
        assert!(matches!(pending.await.unwrap(), Err(RecordError::Busy)));
    }
    let (window, current, _) = run_ann_probe(&p, AnnMode::Nprobe(1), vec![], full)
        .await
        .unwrap();
    let (_, exact, _) = run_ann_probe(&p, AnnMode::All, vec![], full).await.unwrap();
    let keys = |hits: &[super::super::query::RetrievalEvidence]| {
        hits.iter()
            .map(|h| (h.id.clone(), h.version, h.score.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&current), keys(&exact));
    assert_eq!(current[0].id, "s0-r001");
    assert_eq!(current[0].version, 3);
    assert_eq!(window.cut, 242);
    assert!(current.iter().all(|h| h.id != "s0-r000"));

    let permits = p.admission.available_permits();
    let gate = root.join(".test-event-time-vector-worker-pause");
    std::fs::write(&gate, b"hold").unwrap();
    let deadline = NativeDeadline::now() + std::time::Duration::from_secs(3);
    let work = Arc::new(QueryWork::new(ANN_WORK, ANN_RANGES, ANN_BYTES, deadline));
    let reader = p.clone();
    let worker_work = work.clone();
    let pending = tokio::spawn(async move {
        reader
            .probe_metered_vector_cut(
                25..26,
                2,
                Embedding::F32(ann_embedding(0)),
                AnnMode::Nprobe(1),
                vec![],
                deadline,
                worker_work,
            )
            .await
    });
    wait_file(&root.join(".test-event-time-vector-worker-pause-entered")).await;
    let before = work.remaining_io();
    assert!(matches!(pending.await.unwrap(), Err(RecordError::Busy)));
    let timed_out = std::time::Instant::now();
    assert_eq!(p.admission.available_permits(), permits - 1);
    std::fs::remove_file(gate).unwrap();
    tokio::time::timeout_at(native_deadline(), async {
        while p.admission.available_permits() != permits {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(work.remaining_io(), before);
    assert_eq!(work.vector_visits.medoids.load(Ordering::Relaxed), 0);
    eprintln!(
        "ANN timeout to worker permit release after removing the fixture pause: {:?}",
        timed_out.elapsed()
    );
}

#[tokio::test]
async fn metered_ann_binds_read_budget_and_refuses_count_only() {
    let temp = tempfile::tempdir().unwrap();
    let p = ann_fixture(&temp.path().join("records"), 1, 2, 24, None).await;
    let handle = p.inner.lock().await.index.capture_query(2).await.unwrap();
    let runtime = tokio::runtime::Handle::current();
    let call = |work, ambient: Option<Arc<QueryWork>>, result_type, length| {
        let index = handle.index.clone();
        let runtime = runtime.clone();
        crate::resources::spawn_blocking(move || {
            let run = || {
                runtime.block_on(async move {
                    let mut counts = vec![usize::MAX];
                    let result = crate::search::search_with_counts(
                        &index,
                        String::new(),
                        Some(Embedding::F32(ann_embedding(0))),
                        SearchMode::Vector {
                            similarity_threshold: None,
                            ann_mode: AnnMode::Nprobe(1),
                        },
                        0,
                        length,
                        result_type,
                        true,
                        vec![],
                        vec![],
                        vec![],
                        &mut counts,
                        work,
                    )
                    .await;
                    (result, counts)
                })
            };
            match ambient {
                Some(work) => work.with_reads(run),
                None => run(),
            }
        })
    };
    let work = |ranges, bytes| Arc::new(QueryWork::new(ANN_WORK, ranges, bytes, native_deadline()));
    for (ranges, bytes) in [(0, ANN_BYTES), (ANN_RANGES, 0)] {
        let denied = work(ranges, bytes);
        let (result, counts) = call(Some(denied.clone()), None, ResultType::TopkCount, 2)
            .await
            .unwrap();
        assert!(matches!(result, Err(RecordError::Rejected(reason))
            if reason == "shared query positioned read budget exhausted"));
        assert_eq!(
            denied.vector_visits.vector_ranges.load(Ordering::Relaxed),
            0
        );
        assert_eq!(counts, [usize::MAX]);
    }

    let supplied = work(0, 0);
    let conflicting = work(ANN_RANGES, ANN_BYTES);
    let (result, counts) = call(
        Some(supplied.clone()),
        Some(conflicting.clone()),
        ResultType::TopkCount,
        2,
    )
    .await
    .unwrap();
    assert!(matches!(result, Err(RecordError::Rejected(reason))
        if reason == "vector search read budget does not match the supplied query budget"));
    assert_eq!(supplied.remaining_io(), (0, 0));
    assert_eq!(conflicting.remaining_io(), (ANN_RANGES, ANN_BYTES));
    assert_eq!(supplied.remaining(), ANN_WORK);
    assert_eq!(counts, [usize::MAX]);

    let control = work(ANN_RANGES, ANN_BYTES);
    let (result, counts) = call(Some(control.clone()), None, ResultType::TopkCount, 2)
        .await
        .unwrap();
    let result = result.unwrap();
    assert_eq!(result.results.len(), 2);
    assert!(counts.len() > 1);
    let remaining = control.remaining_io();
    let consumed = (ANN_RANGES - remaining.0, ANN_BYTES - remaining.1);
    assert!(consumed.0 > 0 && consumed.1 > 0);
    assert_eq!(
        consumed,
        (
            control.vector_visits.vector_ranges.load(Ordering::Relaxed),
            control.vector_visits.vector_bytes.load(Ordering::Relaxed),
        )
    );

    let same = work(ANN_RANGES, ANN_BYTES);
    let (already_bound, already_bound_counts) = call(
        Some(same.clone()),
        Some(same.clone()),
        ResultType::TopkCount,
        2,
    )
    .await
    .unwrap();
    let keys = |scored: &crate::search::ResultObject| {
        scored
            .results
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&already_bound.unwrap()), keys(&result));
    assert_eq!(same.remaining_io(), remaining);
    assert_eq!(already_bound_counts, counts);

    let count = work(ANN_RANGES, ANN_BYTES);
    let (denied, denied_counts) = call(Some(count.clone()), None, ResultType::Count, 0)
        .await
        .unwrap();
    assert!(matches!(denied, Err(RecordError::Rejected(reason))
        if reason == "metered ANN does not support count-only search"));
    assert_eq!(count.remaining_io(), (ANN_RANGES, ANN_BYTES));
    assert_eq!(count.remaining(), ANN_WORK);
    assert_eq!(denied_counts, [usize::MAX]);

    let (legacy, legacy_counts) = call(None, None, ResultType::Count, 2).await.unwrap();
    let legacy = legacy.unwrap();
    assert!(legacy.results.is_empty());
    assert_eq!(legacy.result_count_total, result.result_count_total);
    assert_eq!(legacy_counts, counts);
    eprintln!(
        "self-bound TopkCount on frozen ANN: {} reads, {} bytes, {} vector candidates",
        consumed.0,
        consumed.1,
        result.results.len()
    );
}

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
    let old_source = old.source("item").unwrap().unwrap();
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

use super::super::event_time_view::{IndexDependencies, Window};
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
        .read_event_time_window(range, limit, IndexDependencies::None, native_deadline())
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
    p.inner.lock().await.set_clock(clock.clone());
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
    p.inner.lock().await.set_clock(clock);
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
async fn event_time_window_requires_all_retrieval_dependencies_at_the_cut() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    p.write(dated("first", "base", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    assert_eq!(build(&p).await, 1);

    let lexical_gate = root.join(".test-lexical-consumer-pause");
    let vector_gate = root.join(".test-vector-consumer-pause");
    let lexical_started = root.join(".test-lexical-consumer-started");
    let vector_started = root.join(".test-vector-consumer-started");
    for (cut, id, lexical_first, resume, paused) in [
        (2, "first-tail", true, &lexical_gate, &vector_gate),
        (3, "second-tail", false, &vector_gate, &lexical_gate),
    ] {
        std::fs::write(&lexical_gate, b"pause").unwrap();
        std::fs::write(&vector_gate, b"pause").unwrap();
        let batch = p
            .commit_writes(vec![dated(id, id, 25 + cut, ExpectedVersion::Absent)])
            .await
            .unwrap();
        assert!(
            matches!(batch.results.as_slice(), [CommittedItem::Committed(receipt)] if receipt.seq == cut as u64)
        );
        wait_file(&lexical_started).await;
        wait_file(&vector_started).await;
        let cut = cut as u64;
        let progress = p.committed_progress().await;
        assert_eq!(progress.committed_seq, Some(cut));
        assert_eq!(
            (progress.lexical_seq, progress.vector_seq),
            (cut - 1, cut - 1)
        );
        let deadline = native_deadline();
        let pure = p
            .read_event_time_window(0..30, 1, IndexDependencies::None, deadline)
            .await
            .unwrap();
        assert_eq!(pure.cut, cut);
        assert_eq!(hit_ids(&pure), [id]);
        for dependencies in [
            IndexDependencies::VectorMetadata,
            IndexDependencies::LexicalWithVectorMetadata,
        ] {
            assert!(matches!(
                p.read_event_time_window(0..30, 1, dependencies, deadline)
                    .await,
                Err(RecordError::Busy)
            ));
        }

        std::fs::remove_file(resume).unwrap();
        tokio::time::timeout_at(deadline, async {
            loop {
                let progress = p.committed_progress().await;
                let (published, paused_seq) = if lexical_first {
                    (progress.lexical_seq, progress.vector_seq)
                } else {
                    (progress.vector_seq, progress.lexical_seq)
                };
                assert_eq!(paused_seq, cut - 1);
                if published == cut {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        assert!(matches!(
            p.read_event_time_window(
                0..30,
                1,
                IndexDependencies::LexicalWithVectorMetadata,
                deadline
            )
            .await,
            Err(RecordError::Busy)
        ));
        if lexical_first {
            assert!(matches!(
                p.read_event_time_window(0..30, 1, IndexDependencies::VectorMetadata, deadline)
                    .await,
                Err(RecordError::Busy)
            ));
        } else {
            let (window, vector, _) = p
                .probe_event_time_vector_cut(0..30, 3, 4096, vec![], deadline)
                .await
                .unwrap();
            assert_eq!(window.cut, cut);
            assert_eq!(hit_ids(&window)[0], id);
            assert_eq!(vector, ["base", "first-tail", "second-tail"]);
        }

        std::fs::remove_file(paused).unwrap();
        p.inner.lock().await.index.drain_indexes().await.unwrap();
        let aligned = p
            .read_event_time_window(
                0..30,
                1,
                IndexDependencies::LexicalWithVectorMetadata,
                deadline,
            )
            .await
            .unwrap();
        assert_eq!(aligned.cut, cut);
        assert_eq!(hit_ids(&aligned), [id]);
        std::fs::remove_file(&lexical_started).unwrap();
        std::fs::remove_file(&vector_started).unwrap();
    }
}

#[tokio::test]
async fn event_time_lexical_reads_bill_real_postings_to_the_shared_meter() {
    use crate::search::QueryWork;

    let temp = tempfile::tempdir().unwrap();
    let p = RecordPartition::create(&temp.path().join("records"), config())
        .await
        .unwrap();
    p.write(dated("time", "time", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    p.write(dated("other", "other", 37, ExpectedVersion::Absent))
        .await
        .unwrap();
    assert_eq!(build(&p).await, 2);

    let search = |work, deadline| {
        p.probe_event_time_lexical_reads(25..26, "alpha".into(), vec![], deadline, work)
    };
    let deadline = native_deadline();
    search(
        Arc::new(QueryWork::new(4096, 2048, 16 * 1024 * 1024, deadline)),
        deadline,
    )
    .await
    .unwrap();
    let deadline = native_deadline();
    let work = Arc::new(QueryWork::new(4096, 2048, 16 * 1024 * 1024, deadline));
    let (window, scored, after_time) = search(work.clone(), deadline).await.unwrap();
    assert_eq!(window.cut, 2);
    assert_eq!(hit_ids(&window), ["time"]);
    assert_eq!(scored.result_count_total, 2); // Lexical candidates are independent of the time window.
    let remaining = work.remaining_io();
    let staged = (after_time.0 - remaining.0, after_time.1 - remaining.1);
    assert!(
        staged.0 > 0 && staged.1 > 0,
        "missing physical postings charges: {staged:?}"
    );
    let used = (2048 - remaining.0, 16 * 1024 * 1024 - remaining.1);

    for (reads, bytes) in [(used.0 - 1, used.1), (used.0, used.1 - 1)] {
        let deadline = native_deadline();
        let work = Arc::new(QueryWork::new(4096, reads, bytes, deadline));
        let result = search(work, deadline).await;
        assert!(
            matches!(&result, Err(RecordError::Rejected(message))
            if message == "shared query positioned read budget exhausted"),
            "expected typed rejection with {reads} ranges and {bytes} bytes (full request {used:?}), got {:?}",
            result.err()
        );
    }

    let deadline = native_deadline();
    let exact = Arc::new(QueryWork::new(4096, used.0, used.1, deadline));
    let (_, scored, _) = search(exact.clone(), deadline).await.unwrap();
    assert_eq!(scored.result_count_total, 2);
    assert_eq!(exact.remaining_io(), (0, 0));
}

#[tokio::test]
async fn event_time_lexical_statistics_meter_dictionary_and_physical_revisions_separately() {
    use crate::search::QueryWork;

    const VISITS: usize = 4096;
    const RANGES: usize = 2048;
    const BYTES: usize = 16 * 1024 * 1024;
    let temp = tempfile::tempdir().unwrap();
    let filter = FacetFilter::Int {
        field: "priority".into(),
        filter: 9..10,
    };
    for two_fields in [false, true] {
        let mut settings = config();
        settings.schema.push(serde_json::from_value(serde_json::json!({
            "field":"priority", "field_type":"Int", "store":false, "facet":true, "index_lexical":false
        })).unwrap());
        if two_fields {
            settings.schema.push(
                serde_json::from_value(serde_json::json!({
                    "field":"title", "field_type":"Text", "store":false, "index_lexical":true
                }))
                .unwrap(),
            );
        }
        let root = temp.path().join(if two_fields {
            "two-fields"
        } else {
            "one-field"
        });
        let p = RecordPartition::create(&root, settings).await.unwrap();
        let request = |operation: &str, id: &str, time, priority, expected| {
            let mut row = dated(operation, id, time, expected);
            let body = row.document.as_mut().unwrap();
            body.insert("priority".into(), serde_json::json!(priority));
            if two_fields {
                body.insert("title".into(), "alpha".into());
            }
            row
        };
        p.write(request(
            "qualified",
            "qualified",
            25,
            9,
            ExpectedVersion::Absent,
        ))
        .await
        .unwrap();
        for n in 0..11 {
            let id = format!("rejected-{n}");
            p.write(request(
                &id,
                &id,
                36,
                if n == 0 && two_fields { 9 } else { 1 },
                ExpectedVersion::Absent,
            ))
            .await
            .unwrap();
        }
        assert_eq!(build(&p).await, 12);
        if two_fields {
            // The retired revision would pass the facet, and its postings
            // remain in the frozen package while the update lives in the tail.
            p.write(request(
                "update",
                "rejected-0",
                36,
                1,
                ExpectedVersion::Match(1),
            ))
            .await
            .unwrap();
        }

        let ordinary = p
            .search(
                "alpha".into(),
                None,
                SearchMode::Lexical,
                vec![filter.clone()],
                1,
            )
            .await
            .unwrap();
        assert_eq!(ordinary.hits.len(), 1);
        assert_eq!(ordinary.hits[0].record.record_id, "qualified");
        let search = |work, deadline| {
            p.probe_event_time_lexical_reads(
                25..26,
                "alpha".into(),
                vec![filter.clone()],
                deadline,
                work,
            )
        };
        let deadline = native_deadline();
        let work = Arc::new(QueryWork::new(VISITS, RANGES, BYTES, deadline));
        let (window, scored, after_time) = search(work.clone(), deadline).await.unwrap();
        assert_eq!(hit_ids(&window), ["qualified"]);
        assert_eq!(scored.result_count_total, 1);
        assert_eq!(scored.results[0].score, ordinary.hits[0].score);
        let usage = work
            .take_lexical_statistics()
            .expect("real scorer used corpus statistics");
        if two_fields {
            assert!(
                usage.postings >= 24,
                "two fields, twelve frozen revisions: {usage:?}"
            );
            assert!(
                usage.ranges >= 2 && usage.bytes > 0,
                "missing postings ranges: {usage:?}"
            );
        } else {
            assert_eq!(
                usage.postings, 0,
                "the dictionary DF shortcut must not visit postings"
            );
        }
        let total = VISITS - work.remaining();
        eprintln!("two_fields={two_fields} stats={usage:?}, total_logical={total}");
        // The time channel would have returned a row. A budget which cannot
        // finish statistics must fail the entire native request.
        assert!(after_time.0 < RANGES && usage.visits > 0);
        let deadline = native_deadline();
        let before_statistics = VISITS - usage.initial_visits;
        let short = Arc::new(QueryWork::new(
            before_statistics + usage.visits - 1,
            RANGES,
            BYTES,
            deadline,
        ));
        assert!(
            matches!(search(short.clone(), deadline).await, Err(RecordError::Rejected(message))
            if message == "shared query work budget exhausted")
        );
        assert!(
            short.take_lexical_statistics().is_none(),
            "the low budget reached beyond statistics"
        );
        if two_fields {
            let ranges = RANGES - usage.initial_ranges + usage.ranges;
            let bytes = BYTES - usage.initial_bytes + usage.bytes;
            for (ranges, bytes) in [(ranges - 1, BYTES), (RANGES, bytes - 1)] {
                let deadline = native_deadline();
                let short = Arc::new(QueryWork::new(VISITS, ranges, bytes, deadline));
                assert!(matches!(search(short.clone(), deadline).await, Err(RecordError::Rejected(message))
                    if message == "shared query positioned read budget exhausted"));
                assert!(short.take_lexical_statistics().is_none());
            }
        }
    }
}

#[tokio::test]
async fn event_time_lexical_work_depends_on_postings_and_facet_expansion() {
    use crate::search::QueryWork;

    let temp = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.max_records = 240;
    settings.max_operations = 512;
    settings.schema.push(serde_json::from_value(serde_json::json!({
        "field":"tags", "field_type":"StringSet", "store":false, "facet":true, "index_lexical":false
    })).unwrap());
    let p = RecordPartition::create(&temp.path().join("records"), settings)
        .await
        .unwrap();
    let long_body = format!("rare common {}alpha beta", "alpha zeta ".repeat(125));
    for n in 0..192 {
        let id = format!("row-{n}");
        let mut row = dated(
            &id,
            &id,
            if n == 0 { 25 } else { 36 },
            ExpectedVersion::Absent,
        );
        let doc = row.document.as_mut().unwrap();
        if n == 0 {
            doc.insert("body".into(), long_body.clone().into());
            doc.insert(
                "tags".into(),
                serde_json::json!(["tight", "broad", "tag-0"]),
            );
        } else {
            doc.insert("body".into(), "common".into());
            doc.insert(
                "tags".into(),
                serde_json::json!(["broad", format!("tag-{n}")]),
            );
        }
        p.write(row).await.unwrap();
    }
    assert_eq!(build(&p).await, 192);

    let filter = |tag: &str| FacetFilter::StringSet {
        field: "tags".into(),
        filter: vec![tag.into()],
    };
    let score = |text: &str, tag, visits| {
        let deadline = native_deadline();
        let work = Arc::new(QueryWork::new(visits, 2048, 16 * 1024 * 1024, deadline));
        let run = p.probe_event_time_lexical_reads(
            25..26,
            text.into(),
            vec![filter(tag)],
            deadline,
            work.clone(),
        );
        async move {
            run.await
                .map(|(window, scored, _)| (window, scored, visits - work.remaining()))
        }
    };

    let full = 32_768;
    let (window, rare, rare_work) = score("rare", "tight", full).await.unwrap();
    assert_eq!(hit_ids(&window), ["row-0"]);
    assert_eq!(rare.result_count_total, 1);
    for (word, tag) in [("common", "tight"), ("rare", "broad")] {
        let (window, result, used) = score(word, tag, full).await.unwrap();
        assert_eq!(hit_ids(&window), ["row-0"]);
        assert_eq!(result.result_count_total, 1);
        let ordinary = p
            .search(word.into(), None, SearchMode::Lexical, vec![filter(tag)], 1)
            .await
            .unwrap();
        assert_eq!(ordinary.hits[0].record.record_id, "row-0");
        assert_eq!(result.results[0].score, ordinary.hits[0].score);
        assert!(
            used > rare_work + 64,
            "{word}/{tag} took {used} vs {rare_work}"
        );
        eprintln!("one visible match with {word}/{tag} cost {used} work units");
        assert!(
            matches!(score(word, tag, rare_work).await, Err(RecordError::Rejected(message))
            if message == "shared query work budget exhausted")
        );
    }
    eprintln!("one visible match with rare/tight cost {rare_work} work units");
    for text in ["\"alpha beta\"", "+rare +common", "rare -common"] {
        assert!(
            matches!(score(text, "tight", full).await, Err(RecordError::Rejected(message))
            if message == "bounded lexical recall supports a single positive analyzed term")
        );
    }
    let phrase = p
        .search(
            "\"alpha beta\"".into(),
            None,
            SearchMode::Lexical,
            vec![filter("tight")],
            1,
        )
        .await
        .unwrap();
    assert_eq!(phrase.hits[0].record.record_id, "row-0");
}

#[tokio::test]
async fn event_time_lexical_cut_refuses_legacy_identity_fallback() {
    use crate::search::QueryWork;

    let temp = tempfile::tempdir().unwrap();
    let p = RecordPartition::create(&temp.path().join("records"), config())
        .await
        .unwrap();
    p.write(dated("time", "time", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    build(&p).await;
    let components = p.inner.lock().await.index.components().await;
    let mut changed = None;
    for component in components {
        let mut shard = component.write().await;
        let active = shard.active.as_mut().unwrap();
        if active.keys.is_empty() {
            continue;
        }
        active.keys.push("unpaired".into());
        active.revisions.push((1, 1));
        changed = Some(component.clone());
        break;
    }
    let component = changed.expect("checkpoint has a nonempty frozen component");
    // Correct cursors and an absent query term still cannot prove this pair.
    let deadline = native_deadline();
    let work = Arc::new(QueryWork::new(4096, 2048, 16 * 1024 * 1024, deadline));
    let result = p
        .probe_event_time_lexical_reads(25..26, "absent".into(), vec![], deadline, work)
        .await;
    {
        let mut shard = component.write().await;
        let active = shard.active.as_mut().unwrap();
        active.keys.pop();
        active.revisions.pop();
    }
    assert!(matches!(result, Err(RecordError::Busy)));
    let deadline = native_deadline();
    let work = Arc::new(QueryWork::new(4096, 2048, 16 * 1024 * 1024, deadline));
    let (window, scored, _) = p
        .probe_event_time_lexical_reads(25..26, "absent".into(), vec![], deadline, work)
        .await
        .unwrap();
    assert_eq!(hit_ids(&window), ["time"]);
    assert!(scored.results.is_empty());
}

#[tokio::test]
async fn event_time_lexical_worker_keeps_admission_until_cutoff_is_observed() {
    use crate::search::QueryWork;

    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    p.write(dated("time", "time", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    build(&p).await;

    let available = p.admission.available_permits();
    let pause = root.join(".test-event-time-lexical-worker-pause");
    let entered = root.join(".test-event-time-lexical-worker-pause-entered");
    std::fs::write(&pause, b"hold").unwrap();
    let owner = p.clone();
    let deadline = NativeDeadline::now() + std::time::Duration::from_secs(2);
    let work = Arc::new(QueryWork::new(4096, 2048, 16 * 1024 * 1024, deadline));
    let worker_work = work.clone();
    let waiting = tokio::spawn(async move {
        owner
            .probe_event_time_lexical_reads(25..26, "alpha".into(), vec![], deadline, worker_work)
            .await
    });
    wait_file(&entered).await;
    let after_time = work.remaining_io();
    assert!(matches!(waiting.await.unwrap(), Err(RecordError::Busy)));
    assert_eq!(p.admission.available_permits(), available - 1);
    std::fs::remove_file(pause).unwrap();
    tokio::time::timeout(DEADLINE, async {
        while p.admission.available_permits() != available {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(work.remaining_io(), after_time);
}

#[tokio::test]
async fn event_time_text_three_lane_holds_admission_through_the_last_worker() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    p.write(dated("matching", "matching", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    build(&p).await;
    let request = || TimeMixedQuery {
        window: TimeRange {
            from_ms: Some(25),
            to_ms: Some(26),
        },
        temporal: None,
        lexical: Some("alpha".into()),
        vector: Some((Embedding::F32(vec![1.0, 0.0]), AnnMode::All)),
        entity: None,
        facets: vec![],
        fields: vec![],
        offset: 0,
        length: 1,
        rrf: Default::default(),
    };

    let baseline = p.admission.available_permits();
    let lexical = root.join(".test-event-time-lexical-worker-pause");
    let vector = root.join(".test-event-time-vector-worker-pause");
    for gate in [&lexical, &vector] {
        std::fs::write(gate, b"hold").unwrap();
    }
    let owner = p.clone();
    let deadline = NativeDeadline::now() + std::time::Duration::from_secs(4);
    let waiting = tokio::spawn(async move { owner.query_time_mixed(request(), deadline).await });
    let lexical_entered = root.join(".test-event-time-lexical-worker-pause-entered");
    let vector_entered = root.join(".test-event-time-vector-worker-pause-entered");
    wait_file(&lexical_entered).await;
    assert!(!vector_entered.exists());
    assert_eq!(p.admission.available_permits(), baseline - 1);
    std::fs::remove_file(lexical).unwrap();
    wait_file(&vector_entered).await;
    assert!(matches!(
        waiting.await.unwrap(),
        Err(EventTimeError::Record(RecordError::Busy))
    ));
    assert_eq!(p.admission.available_permits(), baseline - 1);
    std::fs::remove_file(vector).unwrap();
    tokio::time::timeout(DEADLINE, async {
        while p.admission.available_permits() != baseline {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let output = p
        .query_time_mixed(request(), native_deadline())
        .await
        .map_err(EventTimeError::record_error)
        .unwrap();
    assert_eq!(output.hits[0].channels.len(), 3);
}

#[tokio::test]
async fn event_time_window_refuses_replaced_publication_at_same_sequence() {
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
            p.read_event_time_window(
                0..30,
                2,
                IndexDependencies::LexicalWithVectorMetadata,
                deadline
            )
            .await,
            Err(RecordError::Busy)
        ));
    }

    let delivery = root.join(".test-event-time-delivery-pause");
    std::fs::write(&delivery, b"pause").unwrap();
    let reader = p.clone();
    let pending = tokio::spawn(async move {
        reader
            .read_event_time_window(
                0..30,
                2,
                IndexDependencies::LexicalWithVectorMetadata,
                native_deadline(),
            )
            .await
    });
    wait_file(&root.join(".test-event-time-delivery-started")).await;
    assert_eq!(build(&p).await, 1);
    std::fs::remove_file(delivery).unwrap();
    assert!(matches!(pending.await.unwrap(), Err(RecordError::Busy)));
    assert_eq!(hit_ids(&read_window(&p, 0..30, 2).await.unwrap()), ["base"]);
}

#[tokio::test]
async fn event_time_vector_cut_refuses_topology_handoff_and_group_publication() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    let group = |version, id, time| {
        let row = dated(id, id, time, ExpectedVersion::Absent);
        GroupReplace {
            operation_id: row.operation_id,
            group_id: "group".into(),
            expected_group_version: version,
            records: vec![GroupRecord {
                record_id: row.record_id,
                document: row.document.unwrap(),
                temporal: row.temporal,
            }],
        }
    };
    p.write(dated("base", "base", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    assert_eq!(p.replace_group(group(0, "old", 24)).await.unwrap().seq, 2);
    assert_eq!(build(&p).await, 2);
    p.write(dated("tail", "tail", 26, ExpectedVersion::Absent))
        .await
        .unwrap();

    let gate = root.join(".test-event-time-delivery-pause");
    let started = root.join(".test-event-time-delivery-started");
    for handoff in [true, false] {
        std::fs::write(&gate, b"pause").unwrap();
        let reader = p.clone();
        let pending = tokio::spawn(async move {
            reader
                .probe_event_time_vector_cut(0..30, 3, 4096, vec![], native_deadline())
                .await
        });
        wait_file(&started).await;
        if handoff {
            // Compaction can rotate the live tail before selecting a new
            // certificate. The Record cut and root alone miss that handoff.
            let mut inner = p.inner.lock().await;
            let owner = inner.anchor.as_ref().unwrap().selected_owner().to_owned();
            let components = inner.index.component_count().await;
            inner.stage_stable_tail().await.unwrap();
            assert_eq!(inner.state.seq, 3);
            assert_eq!(inner.anchor.as_ref().unwrap().selected_owner(), owner);
            assert_eq!(inner.index.component_count().await, components + 1);
        } else {
            assert_eq!(p.replace_group(group(1, "new", 27)).await.unwrap().seq, 4);
        }
        std::fs::remove_file(&gate).unwrap();
        std::fs::remove_file(&started).unwrap();
        assert!(matches!(pending.await.unwrap(), Err(RecordError::Busy)));
    }

    assert_eq!(build(&p).await, 4);
    let (window, vector, _) = p
        .probe_event_time_vector_cut(0..30, 3, 4096, vec![], native_deadline())
        .await
        .unwrap();
    assert_eq!(window.cut, 4);
    assert_eq!(hit_ids(&window), ["new", "tail", "base"]);
    assert_eq!(vector, ["base", "new", "tail"]);
}

#[tokio::test]
async fn event_time_vector_charges_positioned_ranges_and_bytes_through_delivery() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    p.write(dated("base", "base", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    assert_eq!(build(&p).await, 1);
    let request = || TimeVectorQuery {
        window: TimeRange {
            from_ms: Some(20),
            to_ms: Some(26),
        },
        temporal: None,
        vector: Embedding::F32(vec![1., 0.]),
        facets: vec![],
        fields: vec!["body".into()],
        offset: 0,
        length: 1,
    };
    let deadline = native_deadline();
    let initial = (128, 128 * 1024);
    let work = Arc::new(crate::search::QueryWork::new(
        4096, initial.0, initial.1, deadline,
    ));
    let output = p
        .query_time_mixed_with_work(request().into(), deadline, work.clone())
        .await
        .map_err(|error| error.record_error())
        .unwrap();
    assert_eq!(output.hits.len(), 1);
    assert_eq!(output.hits[0].channels.len(), 2);
    let remaining = work.remaining_io();
    let measured = (initial.0 - remaining.0, initial.1 - remaining.1);
    assert!(
        measured.0 > 3 && measured.1 > 4096,
        "positioned projection, Record and vector reads {measured:?}"
    );
    eprintln!(
        "time, vector and selected-record readback: {} range requests / {} bytes",
        measured.0, measured.1
    );
    for (reads, bytes) in [(measured.0 - 1, initial.1), (initial.0, measured.1 - 1)] {
        let deadline = native_deadline();
        let work = Arc::new(crate::search::QueryWork::new(4096, reads, bytes, deadline));
        assert!(
            matches!(p.query_time_mixed_with_work(request().into(), deadline, work).await,
            Err(EventTimeError::Record(RecordError::Rejected(message))) if message.contains("positioned read budget"))
        );
    }
}

#[tokio::test]
async fn event_time_vector_budget_counts_filtered_work_across_selected_shards() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let mut settings = config();
    settings.schema.push(serde_json::from_value(serde_json::json!({
        "field":"priority", "field_type":"Int", "store":false, "facet":true, "index_lexical":false
    })).unwrap());
    let p = RecordPartition::create(&root, settings).await.unwrap();
    let filter = FacetFilter::Int {
        field: "priority".into(),
        filter: 9..10,
    };
    // Three time visits, two shard entries, two input predicates and eight
    // workspace slots per shard, two frozen range steps, and six compiled
    // filter slots per vector visit.
    let combined_cost = 3 + 2 * (1 + 2 + 8) + 2 + 4 * (1 + 6);
    let short_budget = combined_cost - 1;
    for (id, time) in [("a", 25), ("b", 26), ("c", 27), ("d", 28)] {
        let mut row = dated(id, id, time, ExpectedVersion::Absent);
        row.document
            .as_mut()
            .unwrap()
            .insert("priority".into(), 1.into());
        p.write(row).await.unwrap();
        if id == "b" {
            assert_eq!(build(&p).await, 2);
            let (window, vector, single) = p
                .probe_event_time_vector_cut(
                    25..26,
                    1,
                    short_budget,
                    vec![filter.clone()],
                    native_deadline(),
                )
                .await
                .unwrap();
            assert!(window.hits.is_empty() && vector.is_empty());
            assert_eq!(single, combined_cost - 2 - 2 * (1 + 6));
        }
    }
    let inner = p.inner.lock().await;
    let components = inner.index.components().await;
    let mut nonempty = 0;
    for component in &components {
        nonempty += usize::from(component.read().await.indexed_vector_count != 0);
    }
    assert_eq!(
        nonempty, 2,
        "the fixture must cross a frozen shard and the live tail"
    );
    drop(inner);

    let legacy = p
        .search(
            String::new(),
            Some(Embedding::F32(vec![1.0, 0.0])),
            SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: crate::vector_similarity::AnnMode::All,
            },
            vec![filter.clone()],
            1,
        )
        .await
        .unwrap();
    assert!(legacy.hits.is_empty());

    let (window, vector, spent) = p
        .probe_event_time_vector_cut(25..26, 1, 4096, vec![filter.clone()], native_deadline())
        .await
        .unwrap();
    assert!(window.hits.is_empty() && vector.is_empty());
    assert_eq!(spent, combined_cost);
    assert!(
        matches!(p.probe_event_time_vector_cut(25..26, 1, short_budget, vec![filter.clone()], native_deadline()).await,
        Err(RecordError::Rejected(reason)) if reason == "shared query work budget exhausted")
    );
    let (window, vector, exact) = p
        .probe_event_time_vector_cut(25..26, 1, spent, vec![filter.clone()], native_deadline())
        .await
        .unwrap();
    assert!(window.hits.is_empty() && vector.is_empty());
    assert_eq!(exact, spent);
    eprintln!("four filtered vectors across two nonempty shards: {spent} shared work units");

    let permits = p.admission.available_permits();
    let held = components[0].write().await;
    let deadline = NativeDeadline::now() + std::time::Duration::from_millis(25);
    assert!(matches!(
        p.probe_event_time_vector_cut(25..26, 1, spent, vec![filter.clone()], deadline)
            .await,
        Err(RecordError::Busy)
    ));
    drop(held);
    let (_, _, after_deadline) = p
        .probe_event_time_vector_cut(25..26, 1, spent, vec![filter], native_deadline())
        .await
        .unwrap();
    assert_eq!(after_deadline, spent);

    // The caller timeout can precede the first worker's final release.
    // Wait for that worker before measuring the next request's admission.
    tokio::time::timeout_at(native_deadline(), async {
        while p.admission.available_permits() != permits {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let worker_gate = root.join(".test-event-time-vector-worker-pause");
    std::fs::write(&worker_gate, b"pause").unwrap();
    let reader = p.clone();
    let pending = tokio::spawn(async move {
        let deadline = NativeDeadline::now() + std::time::Duration::from_millis(500);
        reader
            .probe_event_time_vector_cut(25..26, 1, spent, vec![], deadline)
            .await
    });
    wait_file(&root.join(".test-event-time-vector-worker-pause-entered")).await;
    assert_eq!(p.admission.available_permits(), permits - 1);
    assert!(matches!(pending.await.unwrap(), Err(RecordError::Busy)));
    assert_eq!(
        p.admission.available_permits(),
        permits - 1,
        "a timed-out waiter cannot release the worker's admission"
    );
    std::fs::remove_file(worker_gate).unwrap();
    tokio::time::timeout_at(native_deadline(), async {
        while p.admission.available_permits() != permits {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn event_time_vector_meters_filter_width_and_fusion_before_delivery() {
    use crate::search::QueryWork;

    let temp = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.schema.push(serde_json::from_value(serde_json::json!({
        "field":"tags", "field_type":"StringSet", "store":false, "facet":true, "index_lexical":false
    })).unwrap());
    let p = RecordPartition::create(&temp.path().join("records"), settings)
        .await
        .unwrap();
    let tags: Vec<_> = (0..24).map(|n| format!("tag-{n:02}")).collect();
    for (n, tag) in tags.iter().enumerate() {
        let id = format!("row-{n:02}");
        let mut row = dated(
            &id,
            &id,
            if n == 0 { 25 } else { 50 },
            ExpectedVersion::Absent,
        );
        let doc = row.document.as_mut().unwrap();
        doc.insert("tags".into(), serde_json::json!([tag]));
        doc.insert(
            "vector".into(),
            if n == 23 {
                serde_json::json!([1, 0])
            } else {
                serde_json::json!([0, 1])
            },
        );
        p.write(row).await.unwrap();
    }
    assert_eq!(build(&p).await, 24);
    let (handle, evaluation) = {
        let inner = p.inner.lock().await;
        (
            inner.index.capture_query(1).await.unwrap(),
            inner.evaluation_time_ms,
        )
    };
    let filter = |width| FacetFilter::StringSet {
        field: "tags".into(),
        filter: tags[..width].to_vec(),
    };
    let mut vector_work = Vec::new();
    let mut setup_work = Vec::new();
    for width in [1, 24] {
        let deadline = native_deadline();
        let filters = vec![filter(width), temporal::expiry_filter(evaluation)];
        let setup = QueryWork::new(4096, 2048, 16 * 1024 * 1024, deadline);
        for segment in &handle.index.read().await.segments {
            let shard = segment.vector.read().await;
            crate::metadata_filter::metered_workspace_bytes(&shard, &filters, &setup).unwrap();
        }
        setup_work.push(4096 - setup.remaining());
        let work = Arc::new(QueryWork::new(4096, 2048, 16 * 1024 * 1024, deadline));
        let result = crate::search::search_with_counts(
            &handle.index,
            String::new(),
            Some(Embedding::F32(vec![1.0, 0.0])),
            SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: crate::vector_similarity::AnnMode::All,
            },
            0,
            1,
            ResultType::TopkCount,
            true,
            vec![],
            filters,
            vec![],
            &mut Vec::new(),
            Some(work.clone()),
        )
        .await
        .unwrap();
        assert_eq!(result.results.len(), 1);
        vector_work.push(4096 - work.remaining());
    }
    // The 24 deduplicated compiled values take four extra search levels
    // compared with one value, for every vector, after removing setup work.
    assert_eq!(
        vector_work[1] - vector_work[0] - (setup_work[1] - setup_work[0]),
        24 * 4
    );
    eprintln!("24 vectors, facet width 1/24: {vector_work:?}, setup: {setup_work:?}");
    drop(handle);

    let request = |width| TimeVectorQuery {
        window: TimeRange {
            from_ms: Some(25),
            to_ms: Some(26),
        },
        temporal: None,
        vector: Embedding::F32(vec![1.0, 0.0]),
        facets: vec![filter(width)],
        fields: vec![],
        offset: 0,
        length: 1,
    };
    let run = |width, visits| {
        let deadline = native_deadline();
        let work = Arc::new(QueryWork::new(visits, 2048, 16 * 1024 * 1024, deadline));
        let future = p.query_time_mixed_with_work(request(width).into(), deadline, work.clone());
        async move {
            future
                .await
                .map(|output| (output, visits - work.remaining()))
        }
    };
    let (narrow, spent_narrow) = run(1, 4096)
        .await
        .map_err(EventTimeError::record_error)
        .unwrap();
    assert_eq!(narrow.hits[0].record.record_id, "row-00");
    let (window, vector, work_before_fusion) = p
        .probe_event_time_vector_cut(25..26, 1, 4096, vec![filter(24)], native_deadline())
        .await
        .unwrap();
    assert_eq!(hit_ids(&window), ["row-00"]);
    assert_eq!(vector, ["row-23"]);
    assert!(spent_narrow < work_before_fusion);
    // Two insertions (4 + 5 units) and materialization (2 units), followed
    // by one ordering comparison. Refusal in either phase yields no page.
    for budget in [spent_narrow, work_before_fusion, work_before_fusion + 11] {
        assert!(
            matches!(run(24, budget).await, Err(EventTimeError::Record(RecordError::Rejected(message)))
            if message == "shared query work budget exhausted")
        );
    }
    let (wide, spent_wide) = run(24, work_before_fusion + 13)
        .await
        .map_err(EventTimeError::record_error)
        .unwrap();
    assert_eq!(spent_wide, work_before_fusion + 13);
    assert_eq!(wide.hits[0].record.record_id, "row-00"); // Equal lane ranks break ties by ID.
    assert_eq!(wide.hits[0].channels[0].channel, "time");
}

#[tokio::test]
async fn event_time_vector_cut_rejects_expiry_outside_the_time_window_and_version_changes() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    let now = p.inner.lock().await.observe_time().unwrap();
    let clock = Arc::new(EventClock(AtomicU64::new(now)));
    p.inner.lock().await.set_clock(clock.clone());
    p.write(dated("base", "base", 25, ExpectedVersion::Absent))
        .await
        .unwrap();
    let mut vector_only = dated("expiry", "outside", 300, ExpectedVersion::Absent);
    vector_only.temporal.as_mut().unwrap().expire_at_ms = Some(now + 2000);
    p.write(vector_only).await.unwrap();
    assert_eq!(build(&p).await, 2);
    let request = || TimeVectorQuery {
        window: TimeRange {
            from_ms: Some(25),
            to_ms: Some(26),
        },
        temporal: None,
        vector: Embedding::F32(vec![1., 0.]),
        facets: vec![],
        fields: vec![],
        offset: 0,
        length: 2,
    };
    let output = p
        .query_time_vector(request(), native_deadline())
        .await
        .map_err(|error| error.record_error())
        .unwrap();
    assert_eq!(output.hits[0].record.record_id, "base");
    assert_eq!(output.hits[1].record.record_id, "outside");
    assert_eq!(output.hits[1].channels.len(), 1);
    assert_eq!(output.hits[1].channels[0].channel, "vector");
    let transport = p
        .output_permit()
        .unwrap()
        .with_time_fence(output.delivery_time);

    let gate = root.join(".test-event-time-delivery-pause");
    let started = root.join(".test-event-time-delivery-started");
    for expiry in [true, false] {
        std::fs::write(&gate, b"pause").unwrap();
        let reader = p.clone();
        let pending =
            tokio::spawn(
                async move { reader.query_time_vector(request(), native_deadline()).await },
            );
        wait_file(&started).await;
        if expiry {
            clock.0.store(now + 2000, Ordering::Release);
            assert_eq!(p.committed_progress().await.committed_seq, Some(2));
            assert!(matches!(transport.deliver(|| ()), Err(RecordError::Busy)));
        } else {
            p.write(dated("replace", "base", 25, ExpectedVersion::Match(1)))
                .await
                .unwrap();
        }
        std::fs::remove_file(&gate).unwrap();
        std::fs::remove_file(&started).unwrap();
        assert!(matches!(
            pending.await.unwrap(),
            Err(EventTimeError::Record(RecordError::Busy))
        ));
    }
    let output = p
        .query_time_vector(request(), native_deadline())
        .await
        .map_err(|error| error.record_error())
        .unwrap();
    assert_eq!(output.hits.len(), 1);
    assert_eq!(output.hits[0].record.record_id, "base");
    assert_eq!(output.hits[0].record.version, 2);
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

#[tokio::test]
async fn four_channel_read_budget_and_both_watermarks_share_the_certified_cut() {
    let temp = tempfile::tempdir().unwrap();
    let mut settings = config();
    settings.schema.extend(
        serde_json::from_value::<Vec<crate::index::SchemaField>>(serde_json::json!([
            {"field":"entity_ids","field_type":"StringSet","store":true,"index_lexical":false},
            {"field":"relations","field_type":"Json","store":true,"index_lexical":false}
        ]))
        .unwrap(),
    );
    let p = RecordPartition::create(&temp.path().join("records"), settings)
        .await
        .unwrap();
    for (id, body, field, value) in [
        (
            "text-vector-time",
            "alpha",
            "vector",
            serde_json::json!([1, 0]),
        ),
        (
            "seed",
            "identity",
            "entity_ids",
            serde_json::json!(["app:A"]),
        ),
        (
            "edge",
            "declaration",
            "relations",
            serde_json::json!({"edges":[
                {"from":"app:A","type":"link","to":"app:B"}
            ]}),
        ),
        (
            "relation-only",
            "manual",
            "entity_ids",
            serde_json::json!(["app:B"]),
        ),
    ] {
        let mut row = write(id, id, body, ExpectedVersion::Absent);
        let doc = row.document.as_mut().unwrap();
        doc.shift_remove("vector");
        doc.insert(field.into(), value);
        if id == "text-vector-time" {
            row.temporal = Some(TemporalMetadata {
                event_time_ms: Some(25),
                ..Default::default()
            });
        }
        p.write(row).await.unwrap();
    }
    assert_eq!(build(&p).await, 4);
    assert_eq!(p.build_entity_adjacency(DEADLINE).await.unwrap(), 4);
    for id in ["tail-a", "tail-b", "tail-c"] {
        let mut row = write(id, id, "ordinary", ExpectedVersion::Absent);
        row.document.as_mut().unwrap().shift_remove("vector");
        p.write(row).await.unwrap();
    }
    assert_eq!(p.inner.lock().await.state.records.recent_count(), 3);
    let request = |entity| TimeMixedQuery {
        window: TimeRange {
            from_ms: Some(25),
            to_ms: Some(26),
        },
        temporal: None,
        lexical: Some("alpha".into()),
        vector: Some((Embedding::F32(vec![1., 0.]), AnnMode::All)),
        entity: if entity {
            Some((
                crate::record::EntityRecall {
                    direction: crate::record::EntityDirection::Out,
                    anchor: "app:A".into(),
                    kind: "link".into(),
                    visit_limit: 16,
                },
                1.0,
            ))
        } else {
            None
        },
        facets: vec![],
        fields: vec![],
        offset: 0,
        length: 4,
        rrf: Default::default(),
    };
    let run = |entity, ranges, bytes| {
        let deadline = native_deadline();
        let work = Arc::new(QueryWork::new(65_536, ranges, bytes, deadline));
        let plan = request(entity);
        let partition = &p;
        async move {
            let result = partition
                .query_time_mixed_with_work(plan, deadline, work.clone())
                .await;
            (result, work)
        }
    };
    let (three, three_work) = run(false, ANN_RANGES, ANN_BYTES).await;
    let three = three.unwrap_or_else(|error| panic!("{}", error.record_error()));
    let (four, four_work) = run(true, ANN_RANGES, ANN_BYTES).await;
    let mut four = four.unwrap_or_else(|error| panic!("{}", error.record_error()));
    assert_eq!(three.visible_seq, four.visible_seq);
    assert_eq!(three.hits.len(), 1);
    assert_eq!(four.hits.len(), 2);
    let spent = |work: &QueryWork| {
        let (ranges, bytes) = work.remaining_io();
        (
            65_536 - work.remaining(),
            ANN_RANGES - ranges,
            ANN_BYTES - bytes,
        )
    };
    let before = spent(&three_work);
    let after = spent(&four_work);
    assert!(after.0 > before.0 && after.1 > before.1 && after.2 > before.2);
    eprintln!("three channels work/ranges/bytes={before:?}, four channels={after:?}");
    for (ranges, bytes) in [(before.1, ANN_BYTES), (ANN_RANGES, before.2)] {
        let (error, _) = run(true, ranges, bytes).await;
        let error = error.unwrap_err().record_error();
        assert!(
            matches!(&error, RecordError::Rejected(reason) if reason.contains("positioned read budget")),
            "{error}"
        );
    }

    let output = four.entity_frame.take().unwrap();
    for cursor in [&p.indexes.vector, &p.indexes.lexical] {
        let guard = output.pin().await.unwrap();
        guard.deliver(|| ()).unwrap();
        cursor.store(four.visible_seq - 1, std::sync::atomic::Ordering::Release);
        assert!(matches!(guard.deliver(|| ()), Err(RecordError::Busy)));
        cursor.store(four.visible_seq, std::sync::atomic::Ordering::Release);
    }
}
