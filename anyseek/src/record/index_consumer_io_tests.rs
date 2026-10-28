use super::*;
use crate::component::Commit;

async fn fixture() -> (tempfile::TempDir, IndexArc, RecordConfig, Engine, crate::resources::Scope) {
    let root = tempfile::tempdir().unwrap();
    let mut config = crate::record::tests::config();
    config.schema.push(serde_json::from_value(serde_json::json!({
        "field":"number", "field_type":"U64", "facet":true, "store":false, "index_lexical":false
    })).unwrap());
    let (index, _) = build_at(root.path(), &root.path().join("cache"), &config, &State::default()).await.unwrap();
    let native = index.read().await.record_lexical[0].clone();
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
    let component = index.read().await.shard_vec[0].clone();
    // Force capacity growth with one row rather than a 65K-row workload.
    component.write().await.facets_file_mmap = memmap2::MmapOptions::new().len(1).map_anon().unwrap();
    let held = scope.io().unwrap(); let cursors = Cursors::default();
    let mut pending = Box::pin(scope.clone().run(engine.append(input(&config),&cursors)));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut pending).await.is_err());
    assert!(component.try_read().is_err(), "guard acquired before I/O wait");
    drop(pending); // Cancellation before admission cannot change identity or mmap.
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
    let cursors = Cursors::default(); engine.append(input(&config),&cursors).await.unwrap();
    let component = index.read().await.shard_vec[0].clone();
    let path = std::path::PathBuf::from(index.read().await.index_path_string.clone());
    let before = std::fs::read(path.join("component.json")).unwrap();
    let held = scope.io().unwrap();
    let mut pending = Box::pin(scope.clone().run(index.commit()));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut pending).await.is_err());
    assert_eq!(before,std::fs::read(path.join("component.json")).unwrap());
    drop(held);
    tokio::time::timeout(std::time::Duration::from_secs(5),pending).await.unwrap().unwrap();
    assert_eq!(component.read().await.committed_doc_count,1);
    let reopened = crate::component::open_index(&path).await.unwrap();
    assert_eq!(reopened.read().await.shard_vec[0].read().await.indexed_doc_count,1);
    assert!(scope.io().is_ok());
}
#[tokio::test(flavor="current_thread")]
async fn vector_real_metadata_io_failure_still_fences_component_and_cursor() {
    let (_root,index,config,engine,scope) = fixture().await;
    let component = index.read().await.shard_vec[0].clone();
    { let mut shard = component.write().await;
      shard.facets_file_mmap = memmap2::MmapOptions::new().len(1).map_anon().unwrap();
      shard.readonly_metadata_for_test(); }
    let cursors = Cursors::default();
    assert!(scope.clone().run(engine.append(input(&config),&cursors)).await.is_err());
    assert!(component.read().await.failed); assert!(cursors.error().is_some());
    assert_eq!(cursors.vector.load(Ordering::Acquire),0);
    assert!(scope.io().is_ok());
}

#[tokio::test(flavor="current_thread")]
async fn component_real_vector_write_failure_still_fences_after_partial_commit() {
    let (_root,index,config,engine,scope) = fixture().await;
    let cursors = Cursors::default(); engine.append(input(&config),&cursors).await.unwrap();
    let component = index.read().await.shard_vec[0].clone();
    let path = std::path::PathBuf::from(index.read().await.index_path_string.clone());
    component.write().await.vector_file = crate::storage::build_budget::File::open(path.join("vectors.bin")).unwrap();
    assert!(scope.clone().run(index.commit()).await.is_err());
    assert!(component.read().await.failed);
    assert!(scope.io().is_ok());
}

#[tokio::test(flavor="current_thread")]
async fn component_guard_owner_can_finish_io_while_consumer_waits_for_guard() {
    let (_root,index,config,engine,scope) = fixture().await;
    let component = index.read().await.shard_vec[0].clone();
    let mut shard = component.write().await;
    let cursors = Cursors::default(); let mut queued = input(&config); queued.ordinal = 1;
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
        &candidate,config.index.clone(),&schema,Some(1))));
    assert!(tokio::time::timeout(std::time::Duration::from_millis(20),&mut create).await.is_err());
    assert!(!candidate.exists(), "no partial candidate on transient I/O contention");
    drop(held);
    let successor = tokio::time::timeout(std::time::Duration::from_secs(5),create).await.unwrap().unwrap();
    assert_eq!(successor.read().await.shard_vec[0].read().await.indexed_doc_count,0);
    assert!(candidate.join("schema.json").exists());
    assert!(candidate.join("vectors.bin").exists());
    assert!(candidate.join("metadata.bin").exists());
    let cursors = Cursors::default();
    scope.clone().run(engine.append(input(&config),&cursors)).await.unwrap();
    scope.clone().run(index.commit()).await.unwrap();
    assert_eq!(index.read().await.shard_vec[0].read().await.indexed_doc_count,1);
    assert!(cursors.error().is_none()); assert!(scope.io().is_ok());
}
