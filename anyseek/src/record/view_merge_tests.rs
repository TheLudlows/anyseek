use super::*;
use crate::record::tests::{config, source_write, write};

async fn freeze(p: &RecordPartition) {
    let mut inner = p.inner.lock().await;
    let root = inner.journal.root().to_owned();
    let target = root.join(format!("cache-{:032x}", rand::random::<u128>()));
    let (tail, _) = build_at(&root, &target, &inner.config, &State::default()).await.unwrap();
    let Inner { index, ordinals, locations, .. } = &mut *inner;
    index.seal_cache_into_successor(tail, ordinals, locations,
        RotationBudget { max_segments: 16, max_address_slots: 100_000 }).await.unwrap();
}
async fn prepare(p: &RecordPartition) -> (MergeSources, IndexArc) {
    let (sources, state, settings, root) = {
        let inner = p.inner.lock().await;
        (inner.index.capture_merge(16384).await.unwrap().unwrap(), inner.state.try_clone().unwrap(),
            inner.config.clone(), inner.journal.root().to_owned())
    };
    let target = root.join(format!("cache-{:032x}", rand::random::<u128>()));
    let (candidate, _) = build_at_selected(&root, &target, &settings, &state, Some(&sources.keys)).await.unwrap();
    candidate.read().await.record_lexical[0].seal().await.unwrap();
    (sources, candidate)
}
async fn install(p: &RecordPartition, sources: &MergeSources, candidate: IndexArc) {
    let mut inner = p.inner.lock().await;
    let Inner { index, ordinals, locations, .. } = &mut *inner;
    assert!(index.install_merge(sources, candidate, ordinals, locations, 100_000).await.unwrap());
}
async fn entered(root: &Path, gate: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !root.join(format!("{gate}-entered")).exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.unwrap();
}
async fn seeded(root: &Path) -> RecordPartition {
    let p = RecordPartition::create(root, config()).await.unwrap();
    for id in ["a", "b"] { p.write(write(id, id, "alpha", ExpectedVersion::Absent)).await.unwrap(); }
    freeze(&p).await;
    for id in ["c", "d"] { p.write(write(id, id, "alpha", ExpectedVersion::Absent)).await.unwrap(); }
    freeze(&p).await;
    p
}
async fn update_delete(p: &RecordPartition) {
    p.write(write("update-a", "a", "beta", ExpectedVersion::Match(1))).await.unwrap();
    let mut delete = write("delete-b", "b", "", ExpectedVersion::Match(1));
    delete.document = None;
    p.write(delete).await.unwrap();
}

#[tokio::test]
async fn merge_install_discards_concurrent_updates_and_deletes() {
    let temporary = tempfile::tempdir().unwrap();
    let p = seeded(&temporary.path().join("records")).await;
    let (sources, candidate) = prepare(&p).await;
    update_delete(&p).await;
    let seq = p.inner.lock().await.state.seq;
    install(&p, &sources, candidate).await;
    assert_eq!(p.inner.lock().await.state.seq, seq, "cache merge is not a Record commit");
    assert_eq!(p.inner.lock().await.index.components().await.len(), 2);
    let found = p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap();
    assert_eq!(found.hits.iter().map(|hit| hit.record.record_id.as_str()).collect::<BTreeSet<_>>(), BTreeSet::from(["c", "d"]));
    assert_eq!(p.search("beta".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 1);
}

#[tokio::test]
async fn old_reader_retirement_survives_merge_rotation_and_second_merge() {
    for gate in [".test-query-scoring-pause", ".test-query-candidates-pause"] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = seeded(&root).await;
        std::fs::write(root.join(gate), b"pause").unwrap();
        let held = { let p = p.clone(); tokio::spawn(async move {
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await
        }) };
        entered(&root, gate).await;
        let (sources, candidate) = prepare(&p).await;
        install(&p, &sources, candidate).await;
        // A new rotation and another merge must keep the exact same liveness
        // tokens. There is no weak alias chain that can lose an older reader.
        p.write(write("e", "e", "alpha", ExpectedVersion::Absent)).await.unwrap();
        freeze(&p).await;
        let (next, candidate) = prepare(&p).await;
        install(&p, &next, candidate).await;
        update_delete(&p).await;
        std::fs::remove_file(root.join(gate)).unwrap();
        let found = held.await.unwrap().unwrap();
        let expected = if gate == ".test-query-scoring-pause" {
            // The captured handle intentionally shares its then-mutable tail;
            // e was appended to that tail before scoring resumed.
            BTreeSet::from(["c", "d", "e"])
        } else { BTreeSet::from(["c", "d"]) };
        assert_eq!(found.hits.iter().map(|hit| hit.record.record_id.as_str()).collect::<BTreeSet<_>>(), expected);
        if gate == ".test-query-scoring-pause" { assert_eq!(found.count_total, 3, "retire before old-reader Top-K"); }
        assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 3);
    }
}

#[tokio::test]
async fn merge_keeps_unconsumed_tail_addresses_and_live_tokens() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("records");
    let p = seeded(&root).await;

    let (sources, candidate) = prepare(&p).await;
    std::fs::write(root.join(".test-vector-consumer-pause"), b"pause").unwrap();
    p.commit_writes(vec![write("queued", "queued", "alpha", ExpectedVersion::Absent)]).await.unwrap();
    {
        let inner = p.inner.lock().await;
        let components = inner.index.components().await;
        assert_eq!(components[0].read().await.indexed_doc_count, 0);
        assert!(inner.locations.contains_key("queued"));
    }
    install(&p, &sources, candidate).await;
    std::fs::remove_file(root.join(".test-vector-consumer-pause")).unwrap();
    p.inner.lock().await.index.drain_indexes().await.unwrap();
    let found = p.search("".into(), Some(Embedding::F32(vec![1.0, 0.0])), SearchMode::Vector {
        similarity_threshold: None, ann_mode: crate::vector_similarity::AnnMode::All,
    }, vec![], 10).await.unwrap();
    assert_eq!(found.hits.len(), 5);
    assert!(found.hits.iter().any(|hit| hit.record.record_id == "queued"));
    assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 5);
}

#[tokio::test]
async fn source_cascade_after_merge_fences_old_output_and_masks_new_queries() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    p.write(source_write("s", "s", &[])).await.unwrap();
    p.write(source_write("d", "d", &[("s", 1)])).await.unwrap();
    freeze(&p).await;
    p.write(source_write("i", "i", &[("d", 1)])).await.unwrap();
    p.write(write("ordinary", "ordinary", "evidence original", ExpectedVersion::Absent)).await.unwrap();
    freeze(&p).await;
    let gate = ".test-query-candidates-pause";
    std::fs::write(root.join(gate), b"pause").unwrap();
    let held = { let p = p.clone(); tokio::spawn(async move {
        p.search("evidence".into(), None, SearchMode::Lexical, vec![], 10).await
    }) };
    entered(&root, gate).await;
    let (sources, candidate) = prepare(&p).await;
    install(&p, &sources, candidate).await;
    let mut delete = write("delete-source", "s", "", ExpectedVersion::Match(1)); delete.document = None;
    p.write(delete).await.unwrap();
    std::fs::remove_file(root.join(gate)).unwrap();
    assert!(matches!(held.await.unwrap(), Err(RecordError::RecoveryRequired)));
    let found = p.search("evidence".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap();
    assert_eq!(found.hits.len(), 1);
    assert_eq!(found.hits[0].record.record_id, "ordinary");
}

#[tokio::test]
async fn merge_rejection_keeps_addresses_and_reader_pins_protect_retired_files() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("records");
    let p = seeded(&root).await;
    let held = p.inner.lock().await.index.capture_query(10).await.unwrap();
    let (sources, candidate) = prepare(&p).await;
    let paths = sources.natives.iter().map(|native| native.package_path().to_owned()).collect::<Vec<_>>();
    {
        let mut inner = p.inner.lock().await;
        let old_ordinals = inner.ordinals.clone();
        let old_locations = inner.locations.clone();
        let Inner { index, ordinals, locations, .. } = &mut *inner;
        assert!(index.install_merge(&sources, candidate.clone(), ordinals, locations, 1).await.is_err());
        assert_eq!(*ordinals, old_ordinals);
        assert_eq!(*locations, old_locations);
        assert_eq!(index.components().await.len(), 3);
    }
    install(&p, &sources, candidate).await;
    drop(sources);
    p.inner.lock().await.reclaim_anchor_objects().await.unwrap();
    assert!(paths.iter().all(|path| path.exists()), "captured reader pins removed source caches");
    drop(held);
    p.inner.lock().await.reclaim_anchor_objects().await.unwrap();
    assert!(paths.iter().all(|path| !path.exists()), "unselected unpinned source caches were not reclaimed");
    assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 4);
}

#[tokio::test]
async fn same_version_vector_patch_retires_premerge_reader_row() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    let mut initial = write("a", "a", "alpha", ExpectedVersion::Absent);
    initial.document.as_mut().unwrap().shift_remove("vector");
    p.write(initial).await.unwrap(); freeze(&p).await;
    p.write(write("b", "b", "alpha", ExpectedVersion::Absent)).await.unwrap(); freeze(&p).await;
    let gate = ".test-query-scoring-pause";
    std::fs::write(root.join(gate), b"pause").unwrap();
    let held = { let p = p.clone(); tokio::spawn(async move {
        p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await
    }) };
    entered(&root, gate).await;
    let (sources, candidate) = prepare(&p).await; install(&p, &sources, candidate).await;
    let mut patch = write("patch", "a", "", ExpectedVersion::Match(1));
    patch.document = None;
    patch.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector": [1.0, 0.0]})).unwrap());
    assert_eq!(p.write(patch).await.unwrap().version, 1);
    for source in &sources.components {
        let shard = source.read().await;
        let active = shard.active.as_ref().unwrap();
        for (local, key) in active.keys.iter().enumerate().filter(|(_, key)| key.as_str() == "a") {
            assert!(!active.is_live(local), "same-version patch must retire the old reader's physical row");
            let _ = key;
        }
    }
    std::fs::remove_file(root.join(gate)).unwrap();
    let found = held.await.unwrap().unwrap();
    // The then-mutable tail is shared, so the newly appended patch is eligible;
    // only its original source row must be masked before Top-K.
    assert_eq!(found.count_total, 2);
    assert_eq!(found.hits.len(), 2);
    assert!(found.hits.iter().find(|hit| hit.record.record_id == "a").unwrap()
        .record.document.as_ref().unwrap().contains_key("vector"));
    assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().count_total, 2);
}

#[tokio::test]
async fn detached_merge_keeps_unrelated_authority_corruption_fail_closed() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("records");
    let settings = config();
    let p = RecordPartition::create(&root, settings.clone()).await.unwrap();
    for id in ["a", "b", "c"] { p.write(write(id, id, "alpha", ExpectedVersion::Absent)).await.unwrap(); }
    p.compact_journal().await.unwrap();
    let authority = p.inner.lock().await.anchor.clone().unwrap();
    let selected = authority.retained_packages()[0].clone();
    let selected_path = selected.path(&root).unwrap();
    p.write(write("d", "d", "alpha", ExpectedVersion::Absent)).await.unwrap(); freeze(&p).await;
    p.write(write("e", "e", "alpha", ExpectedVersion::Absent)).await.unwrap(); freeze(&p).await;
    let (sources, candidate) = prepare(&p).await;
    assert!(sources.natives.iter().all(|native| native.package_path() != selected_path));
    install(&p, &sources, candidate).await;
    assert!(p.inner.lock().await.index.detached_cache);
    let file = selected_path.join("schema.json");
    let original = std::fs::read(&file).unwrap();
    let mut changed = original.clone(); changed.push(b' ');
    std::fs::write(&file, changed).unwrap();
    assert!(p.compact_journal().await.is_err(), "detached view must not bypass full source authentication");
    assert_eq!(p.inner.lock().await.anchor.as_ref().unwrap().identity(), authority.identity());
    drop(sources); drop(p);
    assert!(RecordPartition::open(&root, settings.max_journal_bytes).await.is_err(), "cold recovery must authenticate non-participating selected packages");
    // Only this test's temporary fixture is restored, allowing an independent
    // positive reopen oracle instead of leaving a corruption-only assertion.
    std::fs::write(&file, original).unwrap();
    let recovered = RecordPartition::open(&root, settings.max_journal_bytes).await.unwrap();
    assert_eq!(recovered.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().count_total, 5);
}
