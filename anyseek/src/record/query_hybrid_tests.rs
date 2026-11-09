use std::collections::BTreeSet;
use super::*;
use crate::record::tests::{config, write};
use crate::search::{ResultSort, SortOrder};
use std::time::Duration;

fn query(sort: Vec<ResultSort>) -> RecordQuery {
    RecordQuery { temporal: None, text: "alpha".into(), vector: Some(Embedding::F32(vec![1.0,0.0])),
        mode: SearchMode::Hybrid { similarity_threshold: None, ann_mode: crate::vector_similarity::AnnMode::All, rrf: Default::default() },
        query_type: QueryType::Union, offset: 0, length: 10, result_type: ResultType::TopkCount,
        fields: vec![], filters: vec![], sort }
}
async fn entered(root: &Path, gate: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !root.join(format!("{gate}-entered")).exists() { tokio::time::sleep(Duration::from_millis(2)).await; }
    }).await.unwrap();
}

#[tokio::test]
async fn hybrid_capture_releases_default_id_score_but_keeps_facet_guards() {
    for field in ["", "_id", "_score", "rank"] {
        let dir = tempfile::tempdir().unwrap(); let root = dir.path().join("records");
        let mut settings = config();
        settings.schema.push(serde_json::from_value(serde_json::json!({"field":"rank","field_type":"Int","store":false,"index_lexical":false,"facet":true})).unwrap());
        let p = RecordPartition::create(&root, settings).await.unwrap();
        let mut request = write("seed","a","alpha",ExpectedVersion::Absent);
        request.document.as_mut().unwrap().insert("rank".into(),serde_json::json!(1));
        p.write(request).await.unwrap();
        let components = p.inner.lock().await.index.components().await;
        let sort = if field.is_empty() { vec![] } else { vec![ResultSort {field:field.into(),order:SortOrder::Ascending}] };
        let gate = ".test-hybrid-capture-pause";
        std::fs::write(root.join(gate), b"pause").unwrap();
        let task = { let p=p.clone(); tokio::spawn(async move { p.query(query(sort)).await }) };
        entered(&root,gate).await;
        let writable = components.iter().all(|component| component.try_write().is_ok());
        std::fs::remove_file(root.join(gate)).unwrap();
        let result = task.await.unwrap().unwrap();
        assert_eq!(result.hits.len(),1);
        p.finish_service_shutdown().await.unwrap();
        assert_eq!(writable,field!="rank","component ownership for {field}");
    }
}

#[tokio::test]
async fn hybrid_vector_only_retirement_between_phases_never_relabels_old_scores() {
    let dir=tempfile::tempdir().unwrap();let root=dir.path().join("records");
    let p=RecordPartition::create(&root,config()).await.unwrap();
    for (id,body) in [("a","beta"),("b","beta"),("c","alpha")] {
        p.write(write(id,id,body,ExpectedVersion::Absent)).await.unwrap();
    }
    let gate=".test-hybrid-vector-phase-pause";
    std::fs::write(root.join(gate),b"pause").unwrap();
    let task={let p=p.clone();tokio::spawn(async move {p.query(query(vec![])).await})};
    entered(&root,gate).await;
    let mut delete=write("delete","b","",ExpectedVersion::Match(1));delete.document=None;
    // No lexical hit for a in either version. Its old vector score must not be
    // attached to a's new body after the vector/live capture boundary.
    p.write(write("update","a","gamma",ExpectedVersion::Match(1))).await.unwrap();
    p.write(delete).await.unwrap();
    std::fs::remove_file(root.join(gate)).unwrap();
    let found=task.await.unwrap().unwrap();
    assert_eq!(found.hits.iter().map(|h|h.record.record_id.as_str()).collect::<Vec<_>>(),vec!["c"]);
    assert_eq!(found.hits[0].record.version,1);
    assert_eq!(found.hits[0].channels.iter().map(|c|c.channel.as_str()).collect::<BTreeSet<_>>(),BTreeSet::from(["lexical","vector"]));
    assert_eq!(found.count_total,3,"counts retain the vector phase observation");
    assert_eq!(p.get("a").await.unwrap().unwrap().version,2);
    p.finish_service_shutdown().await.unwrap();
}

#[tokio::test]
async fn hybrid_capture_keeps_vector_only_identities_with_lagging_native_and_count() {
    for count in [false,true] {
        let dir=tempfile::tempdir().unwrap();let root=dir.path().join("records");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        for (id,body) in [("a","beta"),("b","beta"),("c","alpha")] {
            p.write(write(id,id,body,ExpectedVersion::Absent)).await.unwrap();
        }
        let handle=p.inner.lock().await.index.capture_query(10).await.unwrap();
        let expected_fingerprints = {
            let root=handle.index.read().await;let shard=root.segments[0].vector.read().await;
            let active=shard.active.as_ref().unwrap();
            active.keys.iter().cloned().zip(active.fingerprints.iter().copied())
                .collect::<std::collections::BTreeMap<_,_>>()
        };
        let native=lexical::runtime::Runtime::create(&dir.path().join("lag"),&config()).unwrap();
        native.append(lexical::Identity{id:"c".into(),version:1,revision:3},
            &write("unused","c","alpha",ExpectedVersion::Absent).document.unwrap(),true).await.unwrap();
        native.seal().await.unwrap();
        handle.index.write().await.segments[0].lexical=native;
        let gate=".test-hybrid-vector-phase-pause";std::fs::write(root.join(gate),b"pause").unwrap();
        let task=tokio::spawn(async move {
            let q=query(vec![]);
            let result=lexical::runtime_query::search(&handle.index,q.text,q.vector,q.mode,q.query_type,0,10,
                if count {ResultType::Count}else{ResultType::TopkCount},vec![],vec![],vec![],
                lexical::Cut{sequence:u64::MAX,evaluation_time_ms:u64::MAX}).await;
            (handle,result)
        });
        entered(&root,gate).await;
        p.write(write("update","a","gamma",ExpectedVersion::Match(1))).await.unwrap();
        let mut delete=write("delete","b","",ExpectedVersion::Match(1));delete.document=None;
        p.write(delete).await.unwrap();
        std::fs::remove_file(root.join(gate)).unwrap();
        let (handle,result)=task.await.unwrap();let result=result.unwrap();
        assert_eq!(result.result_count_total,3);
        // Even after scoring returns, later writes cannot rebind old addresses.
        p.write(write("after-score","c","gamma",ExpectedVersion::Match(1))).await.unwrap();
        let captured=handle.candidates(result.results.clone()).await.unwrap();
        for candidate in &captured {
            assert_eq!(candidate.fingerprint,expected_fingerprints[&candidate.identity.id]);
        }
        assert_eq!(p.get("c").await.unwrap().unwrap().version,2);
        if count {assert!(captured.is_empty());assert!(result.results.is_empty());}
        else {
            let identities=captured.iter().map(|c|(c.identity.id.as_str(),c.identity.version,c.identity.revision)).collect::<BTreeSet<_>>();
            assert_eq!(identities,BTreeSet::from([("a",1,1),("b",1,2),("c",1,3)]));
            for candidate in &captured {
                let channels=&result.explanations[&candidate.hit.doc_id];
                assert!(channels.iter().any(|c|c.channel=="vector"));
                assert_eq!(channels.iter().any(|c|c.channel=="lexical"),candidate.identity.id=="c");
            }
        }
        drop(handle);p.finish_service_shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn hybrid_cancelled_capture_retains_scope_activity_and_refunds_budget() {
    use crate::resources::{Governor,Lane,Lifecycle};
    let dir=tempfile::tempdir().unwrap();let root=dir.path().join("records");
    let p=RecordPartition::create(&root,config()).await.unwrap();
    p.write(write("seed","a","alpha",ExpectedVersion::Absent)).await.unwrap();
    let permits=p.admission.available_permits();
    let scope=Arc::new(Governor::default()).admit("hybrid-cancel".into(),Lane::Foreground,0).unwrap();
    let lifecycle=Arc::new(Lifecycle::default());
    let full=512*1024*1024;
    let occupied=scope.bytes(full).unwrap();
    assert!(matches!(scope.clone().run(p.query(query(vec![]))).await,Err(RecordError::Busy)));
    drop(occupied);
    let gate=".test-hybrid-capture-pause";std::fs::write(root.join(gate),b"pause").unwrap();
    let caller={let p=p.clone();let context=scope.clone();tokio::spawn(lifecycle.enter().unwrap().run(context.run(async move {p.query(query(vec![])).await})))};
    entered(&root,gate).await;
    assert!(scope.bytes(full).is_err(),"query reservations must survive capture");
    lifecycle.stop();caller.abort();assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(p.admission.available_permits(),permits-1);
    assert!(tokio::time::timeout(Duration::from_millis(20),lifecycle.drained()).await.is_err());
    assert!(scope.bytes(full).is_err(),"cancelled caller cannot refund active worker");
    // Accepted writes/consumers can progress while the cancelled query still owns its pins.
    tokio::time::timeout(Duration::from_secs(10),p.write(write("next","b","alpha",ExpectedVersion::Absent))).await.unwrap().unwrap();
    std::fs::remove_file(root.join(gate)).unwrap();
    tokio::time::timeout(Duration::from_secs(5),lifecycle.drained()).await.unwrap();
    assert_eq!(p.admission.available_permits(),permits);
    drop(scope.bytes(full).expect("all query scope bytes refunded after owner drains"));
    assert_eq!(Arc::strong_count(&p.inner),1);
    p.finish_service_shutdown().await.unwrap();drop(p);
    let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
    assert_eq!(p.query(query(vec![])).await.unwrap().hits.len(),2);
    p.finish_service_shutdown().await.unwrap();
}

#[tokio::test]
async fn hybrid_detached_capture_still_rechecks_ttl_and_failed_components() {
    use std::sync::atomic::{AtomicU64,Ordering};
    struct Clock(AtomicU64);
    impl temporal::Clock for Clock {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0.load(Ordering::SeqCst))}}
    for fail in [false,true] {
        let dir=tempfile::tempdir().unwrap();let root=dir.path().join("records");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        let clock=Arc::new(Clock(AtomicU64::new(1000)));p.inner.lock().await.clock=clock.clone();
        let mut request=write("seed","a","alpha",ExpectedVersion::Absent);
        request.temporal=Some(TemporalMetadata{expire_at_ms:Some(1100),..Default::default()});
        p.write(request).await.unwrap();
        let components=p.inner.lock().await.index.components().await;
        let gate=".test-hybrid-capture-pause";std::fs::write(root.join(gate),b"pause").unwrap();
        let task={let p=p.clone();tokio::spawn(async move {p.query(query(vec![])).await})};
        entered(&root,gate).await;
        if fail {components[0].write().await.failed=true;} else {clock.0.store(1100,Ordering::SeqCst);}
        std::fs::remove_file(root.join(gate)).unwrap();
        let result=task.await.unwrap();
        if fail {assert!(matches!(result,Err(RecordError::RecoveryRequired)));components[0].write().await.failed=false;}
        else {assert!(result.unwrap().hits.is_empty());assert_eq!(p.inner.lock().await.state.seq,1);}
        p.finish_service_shutdown().await.unwrap();
    }
}
