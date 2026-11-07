use super::*;
use crate::component::Commit;

// Fixtures without pre-WAL credits; service inputs use the admitted routes.
impl Input {
    fn admitted(
        native: &lexical::runtime::Runtime,
        identity: lexical::Identity,
        document: &Document,
    ) -> Result<Self, RecordError> {
        Self::admitted_visible(native,identity,document,None)
    }
    fn vector(
        config: &RecordConfig,
        identity: lexical::Identity,
        ordinal: usize,
        document: &Document,
    ) -> Result<Self, RecordError> {
        Self::vector_with_credit(config,identity,ordinal,document,None)
    }
}

async fn fixture() -> (tempfile::TempDir, VectorIndexArc, RecordConfig, Engine, crate::resources::Scope) {
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::record::tests::config();
    config.schema.push(serde_json::from_value(serde_json::json!({
        "field":"number", "field_type":"Int", "facet":true, "store":false, "index_lexical":false
    })).unwrap());
    let (index, _) = build_at(root.path(), &root.path().join("cache"), &config, &State::default()).await.unwrap();
    let native = index.read().await.segments[0].lexical.clone();
    let engine = Engine::Vector { index:index.clone(), _native:native };
    let scope = Arc::new(crate::resources::Governor::default())
        .admit("vector-contention".into(),crate::resources::Lane::Background,0).unwrap();
    (root,index,config,engine,scope)
}
fn input(config:&RecordConfig) -> Input {
    Input::vector(config, lexical::Identity {id:"row".into(),version:1,revision:1},0,
        &serde_json::from_value(serde_json::json!({"vector":[2.0,0.0],"number":42})).unwrap()).unwrap()
}
#[tokio::test(flavor="current_thread")]
async fn vector_io_waits_before_identity_and_metadata_growth() {
    let (_root,index,config,engine,scope) = fixture().await;
    let component = index.read().await.segments[0].vector.clone();
    // Force capacity growth with one row rather than a 65K-row workload.
    component.write().await.facets_file_mmap = memmap2::MmapOptions::new().len(1).map_anon().unwrap();
    let held = scope.io().unwrap(); let cursors = Arc::new(Cursors::default());
    let queued = scope.clone().run(async { input(&config) }).await;
    let bytes = queued._memory.bytes();
    let remaining = scope.bytes(2048 * 1024 * 1024 - bytes).unwrap();
    assert!(scope.bytes(1).is_err());
    let mut pending = Box::pin(scope.clone().run(engine.append(queued,&cursors)));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut pending).await.is_err());
    assert!(component.try_read().is_err(), "guard acquired before I/O wait");
    drop(pending); // Cancellation before admission cannot change identity or mmap.
    assert!(scope.bytes(bytes).is_ok(), "cancelled input releases its retained lease");
    drop(remaining);
    { let shard = component.read().await;
      assert_eq!(shard.active.as_ref().unwrap().keys.len(),0);
      assert_eq!(shard.indexed_doc_count,0); assert_eq!(shard.facets_file_mmap.len(),1); assert!(!shard.failed); }
    assert!(cursors.error().is_none()); drop(held);
    tokio::time::timeout(std::time::Duration::from_secs(5),scope.clone().run(engine.append(input(&config),&cursors))).await.unwrap().unwrap();
    let shard = component.read().await;
    assert_eq!(shard.indexed_doc_count,1); assert_eq!(shard.active.as_ref().unwrap().keys.len(),1);
    assert!(shard.facets_file_mmap.len()>1); assert!(!shard.failed); assert!(cursors.error().is_none());
    assert!(scope.io().is_ok());
}
#[tokio::test(flavor="current_thread")]
async fn component_commit_waits_then_finishes_vector_and_descriptor_under_one_loan() {
    let (_root,index,config,engine,scope) = fixture().await;
    let cursors = Arc::new(Cursors::default()); engine.append(input(&config),&cursors).await.unwrap();
    let component = index.read().await.segments[0].vector.clone();
    let path = std::path::PathBuf::from(index.read().await.index_path_string.clone());
    let before = std::fs::read(path.join("component.json")).unwrap();
    let held = scope.io().unwrap();
    let mut pending = Box::pin(scope.clone().run(index.commit()));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut pending).await.is_err());
    assert_eq!(before,std::fs::read(path.join("component.json")).unwrap());
    drop(held);
    tokio::time::timeout(std::time::Duration::from_secs(5),pending).await.unwrap().unwrap();
    assert_eq!(component.read().await.committed_doc_count,1);
    index.read().await.segments[0].lexical.seal().await.unwrap();
    let reopened = crate::component::open_index(&path, &config).await.unwrap();
    assert_eq!(reopened.read().await.segments[0].vector.read().await.indexed_doc_count,1);
    assert!(scope.io().is_ok());
}
#[tokio::test(flavor="current_thread")]
async fn vector_real_metadata_io_failure_still_fences_component_and_cursor() {
    let (_root,index,config,engine,scope) = fixture().await;
    let component = index.read().await.segments[0].vector.clone();
    { let mut shard = component.write().await;
      shard.facets_file_mmap = memmap2::MmapOptions::new().len(1).map_anon().unwrap();
      shard.readonly_metadata_for_test(); }
    let cursors = Arc::new(Cursors::default());
    let queued = scope.clone().run(async { input(&config) }).await;
    let bytes = queued._memory.bytes();
    let remaining = scope.bytes(2048 * 1024 * 1024 - bytes).unwrap();
    assert!(scope.clone().run(engine.append(queued,&cursors)).await.is_err());
    assert!(scope.bytes(bytes).is_ok(), "failed consumer releases its input");
    drop(remaining);
    assert!(component.read().await.failed); assert!(cursors.error().is_some());
    assert_eq!(cursors.vector.load(Ordering::Acquire),0);
    assert!(scope.io().is_ok());
}

#[tokio::test(flavor="current_thread")]
async fn component_real_vector_write_failure_still_fences_after_partial_commit() {
    let (_root,index,config,engine,scope) = fixture().await;
    let cursors = Arc::new(Cursors::default()); engine.append(input(&config),&cursors).await.unwrap();
    let component = index.read().await.segments[0].vector.clone();
    let path = std::path::PathBuf::from(index.read().await.index_path_string.clone());
    component.write().await.vector_file = crate::storage::build_budget::File::open(path.join("vectors.bin")).unwrap();
    assert!(scope.clone().run(index.commit()).await.is_err());
    assert!(component.read().await.failed);
    assert!(scope.io().is_ok());
}

#[tokio::test(flavor="current_thread")]
async fn component_guard_owner_can_finish_io_while_consumer_waits_for_guard() {
    let (_root,index,config,engine,scope) = fixture().await;
    let component = index.read().await.segments[0].vector.clone();
    let mut shard = component.write().await;
    let cursors = Arc::new(Cursors::default()); let mut queued = input(&config); queued.ordinal = 1;
    let mut waiting = Box::pin(scope.clone().run(engine.append(queued,&cursors)));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut waiting).await.is_err());
    // A consumer waiting for this guard must not own the sole I/O permit.
    assert!(scope.io().is_ok());
    let document: Document = serde_json::from_value(serde_json::json!({"vector":[2.0,0.0],"number":7})).unwrap();
    shard.active.as_mut().unwrap().append_identity("first".into(),1,1,crate::active::fingerprint(&document).unwrap());
    tokio::time::timeout(std::time::Duration::from_secs(5),scope.clone().run(shard.append(document))).await.unwrap().unwrap();
    drop(shard);
    tokio::time::timeout(std::time::Duration::from_secs(5),waiting).await.unwrap().unwrap();
    assert_eq!(component.read().await.indexed_doc_count,2);
    assert!(cursors.error().is_none()); assert!(scope.io().is_ok());
}

#[tokio::test(flavor="current_thread")]
async fn successor_component_creation_waits_before_touching_candidate_and_keeps_old_writer() {
    let (root,index,config,engine,scope) = fixture().await;
    let candidate = root.path().join("successor");
    let schema = retrieval_schema(&config);
    let held = scope.io().unwrap();
    let mut create = Box::pin(scope.clone().run(crate::component::create_index(
        &candidate,config.index.clone(),&schema,&config)));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut create).await.is_err());
    assert!(!candidate.exists(), "no partial candidate on transient I/O contention");
    drop(held);
    let successor = tokio::time::timeout(std::time::Duration::from_secs(5),create).await.unwrap().unwrap();
    assert_eq!(successor.read().await.segments[0].vector.read().await.indexed_doc_count,0);
    assert!(candidate.join("schema.json").exists());
    assert!(candidate.join("vectors.bin").exists());
    assert!(candidate.join("metadata.bin").exists());
    let cursors = Arc::new(Cursors::default());
    scope.clone().run(engine.append(input(&config),&cursors)).await.unwrap();
    scope.clone().run(index.commit()).await.unwrap();
    assert_eq!(index.read().await.segments[0].vector.read().await.indexed_doc_count,1);
    assert!(cursors.error().is_none()); assert!(scope.io().is_ok());
}

#[tokio::test(flavor="current_thread")]
async fn queued_projection_ignores_unindexed_body_and_rolls_back_partial_admission() {
    let (_root,index,config,_engine,scope) = fixture().await;
    let native = index.read().await.segments[0].lexical.clone();
    scope.clone().run(async {
        let small: Document = serde_json::from_value(serde_json::json!({
            "body":"alpha", "vector":[2.0,0.0], "number":42, "payload":"a"
        })).unwrap();
        let mut large = small.clone();
        large.insert("payload".into(), serde_json::json!("x".repeat(256 * 1024)));
        let identity = || lexical::Identity { id:"row".into(), version:1, revision:1 };
        let lexical = Input::admitted(&native, identity(), &small).unwrap();
        let vector = Input::vector(&config, identity(), 0, &small).unwrap();
        let large_lexical = Input::admitted(&native, identity(), &large).unwrap();
        let large_vector = Input::vector(&config, identity(), 0, &large).unwrap();
        assert_eq!(lexical.document, large_lexical.document);
        assert_eq!(vector.document, large_vector.document);
        assert_eq!(lexical._memory.bytes(), large_lexical._memory.bytes());
        assert_eq!(vector._memory.bytes(), large_vector._memory.bytes());
        assert!(build_budget::document_estimate(&large, &[]).unwrap()
            > build_budget::document_estimate(&small, &[]).unwrap() + 8 * 1024 * 1024 - 1024);
        let bytes = vector._memory.bytes();
        drop((lexical, vector, large_lexical, large_vector));
        let remaining = scope.bytes(2048 * 1024 * 1024 - bytes).unwrap();
        let partial = (|| -> Result<Vec<Input>, RecordError> {
            let first = Input::vector(&config, identity(), 0, &large)?;
            let second = Input::admitted(&native, identity(), &large)?;
            Ok(vec![first, second])
        })();
        assert!(partial.is_err());
        assert!(scope.bytes(bytes).is_ok(), "failed second projection drops the first");
        drop(remaining);
    }).await;
}

#[tokio::test]
async fn consumer_fault_matrix_keeps_authority_but_rejects_all_search_channels() {
    use crate::record::tests::{config, write};
    use crate::vector_similarity::AnnMode;
    for lane in ["lexical", "vector"] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let marker = root.join(format!(".test-{lane}-consumer-fail"));
        std::fs::write(&marker, b"fail").unwrap();
        let request = write("fault-matrix", "row", "alpha", ExpectedVersion::Absent);
        let batch = p.commit_writes(vec![request.clone()]).await.unwrap();
        assert!(matches!(&batch.results[..], [CommittedItem::Committed(r)] if r.seq == 1));
        assert!(p.inner.lock().await.index.drain_indexes().await.is_err());
        let progress = p.committed_progress().await;
        assert_eq!(progress.committed_seq, Some(1));
        assert!(!progress.recovery_required);
        assert!(progress.index_error.is_some());
        assert_eq!(if lane == "lexical" { progress.lexical_seq } else { progress.vector_seq }, 0);
        assert!(p.operation(&request.operation_id).await.unwrap().is_some());
        assert!(p.get("row").await.unwrap().is_some());
        for mode in [SearchMode::Lexical,
            SearchMode::Vector { similarity_threshold: None, ann_mode: AnnMode::All },
            SearchMode::Hybrid { similarity_threshold: None, ann_mode: AnnMode::All, rrf: Default::default() }] {
            assert!(matches!(p.search("alpha".into(), Some(Embedding::F32(vec![1.0, 0.0])), mode, vec![], 10).await,
                Err(RecordError::RecoveryRequired)));
        }
        std::fs::remove_file(marker).unwrap();
        let owner = Arc::downgrade(&p.inner);
        p.finish_service_shutdown().await.unwrap();drop(p);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while owner.strong_count() != 0 { tokio::task::yield_now().await; }
        }).await.unwrap();
        let reopened = RecordPartition::open(&root, config().max_journal_bytes).await.unwrap();
        assert_eq!(reopened.write(request).await.unwrap().seq, 1);
        assert_eq!(reopened.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 1);
    }
}

#[tokio::test(flavor="current_thread")]
async fn component_credits_transfer_under_full_memory_and_cancel_without_leaks() {
    let (_root,index,config,engine,scope)=fixture().await;
    let component=index.read().await.segments[0].vector.clone();
    let document:Document=serde_json::from_value(serde_json::json!({"vector":[2.0,0.0],"number":42})).unwrap();
    let before=crate::storage::build_budget::used_memory_for_test();
    let first={let shard=component.read().await;ComponentCredit::new(&shard,&document,"cancelled").unwrap()};
    let second={let shard=component.read().await;ComponentCredit::new(&shard,&document,"row").unwrap()};
    let retained=second.identity.bytes()+second.append.retained.bytes();
    let initial=component.read().await.admission.bytes().unwrap();
    assert!(component.read().await.admission.pending()>retained);
    drop(first);
    assert_eq!(component.read().await.admission.pending(),retained);
    let queued=Input::vector_with_credit(&config,lexical::Identity{id:"row".into(),version:1,revision:1},0,&document,Some(second)).unwrap();
    let held=crate::storage::build_budget::fill_memory_for_test();
    let cursors=Arc::new(Cursors::default());
    scope.clone().run(engine.append(queued,&cursors)).await.unwrap();
    {
        let shard=component.read().await;
        assert_eq!(shard.indexed_doc_count,1);assert!(!shard.failed);
        assert_eq!(shard.admission.pending(),0);
        assert_eq!(shard.admission.bytes().unwrap(),initial+retained);
    }
    drop(held);
    assert_eq!(crate::storage::build_budget::used_memory_for_test(),before+retained);

    // A dropped plan waiting for the real I/O gate must refund all three
    // credits and its pending aggregate; no component mutation has started.
    let before_cancel=crate::storage::build_budget::used_memory_for_test();
    let credit={let shard=component.read().await;ComponentCredit::new(&shard,&document,"cancel").unwrap()};
    let queued=Input::vector_with_credit(&config,lexical::Identity{id:"cancel".into(),version:1,revision:2},1,&document,Some(credit)).unwrap();
    let io=scope.io().unwrap();
    let mut waiting=Box::pin(scope.clone().run(engine.append(queued,&cursors)));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut waiting).await.is_err());
    drop(waiting);drop(io);
    assert_eq!(component.read().await.admission.pending(),0);
    assert_eq!(component.read().await.indexed_doc_count,1);
    assert_eq!(crate::storage::build_budget::used_memory_for_test(),before_cancel);
}

#[tokio::test(flavor="current_thread")]
async fn component_credit_checks_pending_aggregate_before_admission_and_keeps_io_failures_fatal() {
    let (_root,index,config,engine,scope)=fixture().await;
    let component=index.read().await.segments[0].vector.clone();
    let document:Document=serde_json::from_value(serde_json::json!({"vector":[2.0,0.0],"number":42})).unwrap();
    let before=crate::storage::build_budget::used_memory_for_test();
    {
        let shard=component.read().await;
        // Exercise the single-owner boundary without allocating a 1GiB test
        // corpus. Simulate already queued retained bytes, then restore the ledger.
        let cap=std::env::var("ANYSEEK_BUILD_MEMORY_BYTES").ok().map(|s|s.parse::<u64>().unwrap())
            .unwrap_or(crate::storage::build_budget::BUILD_MEMORY);
        shard.admission.set_pending_for_test(cap);
        assert!(matches!(ComponentCredit::new(&shard,&document,"a"),Err(RecordError::Storage(_))));
        assert_eq!(shard.admission.pending(),cap);
        shard.admission.set_pending_for_test(0);
    }
    assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
    let credit={let shard=component.read().await;ComponentCredit::new(&shard,&document,"row").unwrap()};
    {
        let mut shard=component.write().await;
        shard.facets_file_mmap=memmap2::MmapOptions::new().len(1).map_anon().unwrap();
        shard.readonly_metadata_for_test();
    }
    let queued=Input::vector_with_credit(&config,lexical::Identity{id:"row".into(),version:1,revision:1},0,&document,Some(credit)).unwrap();
    let cursors=Arc::new(Cursors::default());
    let error=scope.run(engine.append(queued,&cursors)).await.unwrap_err();
    assert!(!matches!(error,RecordError::Busy));
    assert!(component.read().await.failed);assert!(cursors.error().is_some());
    assert_eq!(component.read().await.admission.pending(),0);
}

#[test]
fn borrowed_temporal_component_length_matches_materialized_projection() {
    let document:Document=serde_json::from_value(serde_json::json!({
        "body":"quoted \"text\" / 中文", "vector":[1.0,-2.5],"number":-42,
        "payload":{"nested":["x",null,true]}, "__seekpower_time_from":123
    })).unwrap();
    for metadata in [temporal::TemporalMetadata::default(),temporal::TemporalMetadata{
        event_time_ms:Some(-62_135_596_800_000),valid_from_ms:Some(-123),
        valid_to_ms:Some(253_402_300_799_999),expire_at_ms:Some(42),
    }] {
        for ingested in [0,253_402_300_799_999] {
            for projection in [false,true] {
                let selected=|name:&str|!projection || name=="vector" || name=="number" || temporal::reserved(name);
                let mut materialized=temporal::indexed(&document,&metadata,ingested);
                materialized.retain(|name,_|selected(name));
                let before=crate::storage::build_budget::used_memory_for_test();
                assert_eq!(temporal::indexed_length(&document,&metadata,ingested,selected).unwrap(),
                    checkpoint_codec::json_length(&materialized).unwrap());
                assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
            }
        }
    }
}

#[tokio::test(flavor="current_thread")]
async fn borrowed_component_admission_refusal_refunds_partial_credits() {
    let (_root,index,_config,_engine,_scope)=fixture().await;
    let component=index.read().await.segments[0].vector.clone();
    let document:Document=serde_json::from_value(serde_json::json!({"vector":[2.0,0.0],"number":42,"payload":"unindexed"})).unwrap();
    let metadata=temporal::TemporalMetadata::default();
    let shard=component.read().await;
    let bytes=temporal::indexed_length(&document,&metadata,123,|name|
        shard.indexed_schema_vec.iter().any(|field|field.field==name)||shard.facets.iter().any(|field|field.name==name)).unwrap() as u64;
    let input=bytes*32;let retained=input+1024;
    let original_owner=shard.admission.bytes().unwrap();
    let original_pending=shard.admission.pending();
    let held=crate::storage::build_budget::fill_memory_except_for_test(build_budget::identity_estimate("refused").unwrap());
    let before=crate::storage::build_budget::used_memory_for_test();
    assert!(matches!(ComponentCredit::from_estimates(&shard.admission,input,retained,"refused"),Err(RecordError::Busy)));
    assert_eq!(crate::storage::build_budget::used_memory_for_test(),before,"identity admission preceding projection refusal must refund");
    assert_eq!(shard.admission.pending(),original_pending);
    assert_eq!(shard.admission.bytes().unwrap(),original_owner);
    drop(held);
    let before=crate::storage::build_budget::used_memory_for_test();
    let admitted=ComponentCredit::from_estimates(&shard.admission,input,retained,"retry").unwrap();
    assert!(shard.admission.pending()>original_pending);
    drop(admitted);
    assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
    assert_eq!(shard.admission.pending(),original_pending);
}

#[tokio::test(flavor="current_thread")]
async fn component_admission_without_shard_read_while_real_io_waits() {
    let (_root,index,config,engine,scope)=fixture().await;
    let component=index.read().await.segments[0].vector.clone();
    let admission=index.read().await.segments[0].admission.clone();
    let view=view::RetrievalView::single(index.clone());
    let record=Record{record_id:"next".into(),version:1,
        document:Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0],"number":42,"payload":"not projected"})).unwrap()),
        temporal:Default::default(),ingested_at_ms:123,expired_at_ms:None};
    let queued=input(&config);let cursors=Arc::new(Cursors::default());
    let io=scope.io().unwrap();
    let mut waiting=Box::pin(scope.clone().run(engine.append(queued,&cursors)));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut waiting).await.is_err());
    assert!(component.try_read().is_err(),"actual vector writer holds shard across I/O admission");
    let before=crate::storage::build_budget::used_memory_for_test();let initial=admission.bytes().unwrap();
    let credit=tokio::time::timeout(std::time::Duration::from_secs(1),view.reserve_component(&record,&config)).await
        .expect("planner must not wait for shard writer").unwrap();
    assert_eq!(admission.pending(),credit.identity.bytes()+credit.append.retained.bytes());
    assert_eq!(admission.bytes().unwrap(),initial);
    drop(credit);assert_eq!(admission.pending(),0);
    assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
    drop(waiting);drop(io);
    assert!(!component.read().await.failed);assert!(cursors.error().is_none());
    let credit=view.reserve_component(&record,&config).await.unwrap();drop(credit);
}

#[tokio::test(flavor="current_thread")]
async fn component_ledger_old_tail_shutdown_and_typed_metadata_pressure() {
    let (root,index,config,_engine,_scope)=fixture().await;
    let admission=index.read().await.segments[0].admission.clone();
    let mut view=view::RetrievalView::single(index.clone());
    let (next,_)=build_at(root.path(),&root.path().join("successor"),&config,&State::default()).await.unwrap();
    let successor=next.read().await.segments[0].admission.clone();
    let mut credit=ComponentCredit::from_estimates(&admission,128,256,"old").unwrap();
    view.rotate_tail(next,&mut Vec::new(),&mut std::collections::BTreeMap::new(),
        view::RotationBudget{max_segments:4,max_address_slots:100}).await.unwrap();
    assert!(ComponentCredit::from_estimates(&admission,1,1,"late").is_err());
    // Already admitted old-generation work may finish, never on the successor.
    credit.append.pending.transfer(&admission,credit.identity).unwrap();
    credit.append.pending.transfer(&admission,credit.append.retained).unwrap();
    drop(credit.append.input);drop(credit.append.pending);assert_eq!(admission.pending(),0);
    assert!(ComponentCredit::from_estimates(&successor,128,256,"new").is_ok());
    view.shutdown_workers().await.unwrap();
    assert!(ComponentCredit::from_estimates(&successor,128,256,"closed").is_err());

    // Allow descriptor parsing, but leave too little shared budget for the new
    // immutable schema ledger. Refusal stays typed Busy and refunds parsing.
    let path=std::path::PathBuf::from(&index.read().await.index_path_string);
    let descriptor=std::fs::read(path.join("component.json")).unwrap();
    let estimate=descriptor.len() as u64*8+4096;
    let pressure=crate::storage::build_budget::fill_memory_except_for_test(estimate+1);
    let before=crate::storage::build_budget::used_memory_for_test();
    assert!(matches!(crate::component::open_index(&path,&config).await,Err(RecordError::Busy)));
    assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);drop(pressure);
    assert_eq!(std::fs::read(path.join("component.json")).unwrap(),descriptor);
    let reopened=crate::component::open_index(&path,&config).await.unwrap();drop(reopened);
    let pressure=crate::storage::build_budget::fill_memory_for_test();
    assert!(matches!(crate::component::create_index(&root.path().join("refused"),config.index.clone(),
        &retrieval_schema(&config),&config).await,Err(RecordError::Busy)));
    assert!(!root.path().join("refused").exists());drop(pressure);
    let created=crate::component::create_index(&root.path().join("refused"),config.index.clone(),
        &retrieval_schema(&config),&config).await.unwrap();drop(created);
}
