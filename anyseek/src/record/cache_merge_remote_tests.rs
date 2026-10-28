//! In-memory object storage with real SQLite authority, not real-cloud evidence.
use super::*;
use crate::record::tests::{config,write,wait_maintenance_idle};
use crate::storage::{s3_compatible::S3CompatibleStore,sqlite_authority::SqliteAuthority};
use object_store::memory::InMemory;

#[tokio::test]
async fn remote_long_suffix_merges_caches_without_replacing_anchor_and_cold_recovers() {
    let temporary=tempfile::tempdir().unwrap();
    let root=temporary.path().join("warm-cache");
    let authority_path=temporary.path().join("authority.sqlite");
    let mut settings=config();
    settings.max_records=16;
    settings.max_operations=4096;
    settings.max_journal_bytes=16*1024*1024;
    let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
    let backend=Arc::new(InMemory::new());
    let remote=RemotePersistence {
        store:S3CompatibleStore::from_store(backend,16*1024*1024),
        authority:Mutex::new(SqliteAuthority::open(&authority_path,Duration::from_secs(1)).unwrap()),
        partition_id:"long-suffix".into(),owner_epoch:1,
        max_tail_blocks:4096,max_tail_bytes:16*1024*1024,max_object_bytes:16*1024*1024,
        _owner:None,
    };
    {
        let mut inner=p.inner.lock().await;
        let initial=encode_entry(&Entry::Checkpoint {config:inner.config.clone(),state:inner.state.clone()}).unwrap();
        let mut authority=remote.authority.lock().await;
        authority.create_partition("long-suffix",1).unwrap();
        remote.store.publish_bootstrap(&mut authority,"long-suffix",1,&initial,unix_time_ms_i64()).await.unwrap();
        drop(authority);
        inner.remote=Some(remote);
    }
    assert_eq!(p.compact_journal().await.unwrap(),0);
    let (anchor,manifest)={
        let inner=p.inner.lock().await;
        let remote=inner.remote.as_ref().unwrap();
        let head=remote.authority.lock().await.read_head("long-suffix").unwrap().unwrap();
        (inner.anchor.as_ref().unwrap().identity().to_owned(),head.manifest)
    };
    let pipeline=IngestPipeline::open(p.clone()).await.unwrap();
    let first=write("update-0","same","alpha",ExpectedVersion::Any);
    let mut last=first.clone();
    for offset in (0..2305).step_by(64) {
        let batch=(offset..(offset+64).min(2305)).map(|i| {
            write(&format!("update-{i}"),"same",if i==2304 {"terminal"} else {"alpha"},ExpectedVersion::Any)
        }).collect::<Vec<_>>();
        last=batch.last().unwrap().clone();
        let accepted=pipeline.submit(batch.clone()).await.unwrap();
        assert_eq!(accepted.len(),batch.len());
        let progress=pipeline.finish().await.unwrap();
        assert_eq!(progress.pending,0);
        assert!(progress.lexical_seq>=progress.materialized_seq && progress.vector_seq>=progress.materialized_seq);
        for request in batch {
            let status=pipeline.operation(&request.operation_id).await.unwrap();
            assert!(status.error.is_none(),"accepted command must actually succeed: {:?}",status.error);
            assert!(status.receipt.is_some());
        }
    }
    wait_maintenance_idle(&p).await;
    let first_receipt=pipeline.operation(&first.operation_id).await.unwrap().receipt.unwrap();
    let last_receipt=pipeline.operation(&last.operation_id).await.unwrap().receipt.unwrap();
    assert_eq!(first_receipt.version,1);
    assert_eq!(last_receipt.version,2305);
    {
        let inner=p.inner.lock().await;
        assert_eq!(inner.anchor.as_ref().unwrap().identity(),anchor);
        assert!(inner.index.detached_cache,"the suffix must exercise independent cache maintenance");
        assert!(inner.index.components().await.len()<=capacity::segments(&settings)+1);
        let remote=inner.remote.as_ref().unwrap();
        let head=remote.authority.lock().await.read_head("long-suffix").unwrap().unwrap();
        assert_eq!(head.manifest,manifest,"cache maintenance must not publish a new authority checkpoint");
        assert_eq!(head.log_base_seq,0);
        assert_eq!(head.committed_seq,2305);
    }
    pipeline.finish().await.unwrap();
    drop(pipeline);
    let remote=p.inner.lock().await.remote.take().unwrap();
    drop(p);
    // Keep the entire old cache untouched: recovery receives an empty new path.
    let cold=temporary.path().join("cold-cache");
    assert!(!cold.exists());
    let recovered=RecordPartition::open_s3_with_remote(&cold,settings.max_journal_bytes,remote).await.unwrap();
    assert_eq!(recovered.get("same").await.unwrap().unwrap().version,2305);
    assert_eq!(recovered.search("terminal".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,1);
    assert_eq!(recovered.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,0);
    assert_eq!(recovered.write(first).await.unwrap(),first_receipt);
    assert_eq!(recovered.write(last).await.unwrap(),last_receipt);
    assert_eq!(recovered.inner.lock().await.anchor.as_ref().unwrap().identity(),anchor);
}
