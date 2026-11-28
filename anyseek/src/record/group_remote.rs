//! A single group WAL object is selected only after its upload-time basis is rechecked.
use super::*;
#[cfg(test)]
use std::time::Duration;

impl write::RemoteUpload {
    pub(super) async fn upload_group(self, seq:u64, operation_id:String, payload:&[u8]) -> Result<write::UploadedBatch,RecordError> {
        use sha2::{Digest,Sha256};
        #[cfg(test)] let path=self.path.clone();
        let uploaded=self.upload_entries(vec![crate::storage::object_log::ObjectLogEntry {
            seq,operation_id,request_sha256:Sha256::digest(payload).into(),payload:payload.to_vec(),
        }]).await?;
        #[cfg(test)] {
            let gate=path.with_extension("group-after-upload-pause");
            if gate.exists() {
                std::fs::write(gate.with_extension("started"),b"uploaded, not selected").map_err(storage)?;
                let until=Instant::now()+Duration::from_secs(20);
                while gate.exists() {
                    if Instant::now()>=until {return Err(RecordError::Busy);}
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
        Ok(uploaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{s3_compatible::S3CompatibleStore,sqlite_authority::SqliteAuthority};
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicU64,Ordering};
    use crate::record::tests::{config,write};
    struct Clock(AtomicU64);
    impl temporal::Clock for Clock {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0.load(Ordering::SeqCst))}}
    fn remote(store:Arc<InMemory>,db:&Path,id:&str)->RemotePersistence {
        RemotePersistence {store:S3CompatibleStore::from_store(store,4*1024*1024),
            authority:Mutex::new(SqliteAuthority::open(db,Duration::from_secs(1)).unwrap()),
            partition_id:id.into(),owner_epoch:1,max_tail_blocks:10000,max_tail_bytes:16*1024*1024,
            max_object_bytes:4*1024*1024,_owner:None}
    }
    async fn source(root:&Path,db:&Path)->(RecordPartition,Arc<InMemory>,Arc<Clock>) {
        let p=RecordPartition::create(root,config()).await.unwrap();
        let store=Arc::new(InMemory::new());let r=remote(store.clone(),db,"groups");
        let mut inner=p.inner.lock().await;
        let bytes=encode_entry(&Entry::Checkpoint{config:inner.config.clone(),state:inner.state.try_clone().unwrap()}).unwrap();
        let mut a=r.authority.lock().await;a.create_partition("groups",1).unwrap();
        r.store.publish_bootstrap(&mut a,"groups",1,&bytes,unix_time_ms_i64()).await.unwrap();drop(a);
        let clock=Arc::new(Clock(AtomicU64::new(unix_time_ms_i64() as u64)));inner.set_clock(clock.clone());inner.remote=Some(r);drop(inner);
        (p,store,clock)
    }
    fn request(nonce:&str,version:u64,ids:&[&str])->GroupReplace {
        GroupReplace{operation_id:OperationId::new(nonce.into()),group_id:"g".into(),expected_group_version:version,
            records:ids.iter().map(|id|GroupRecord{record_id:(*id).into(),document:write("unused",id,"alpha",ExpectedVersion::Any).document.unwrap(),temporal:None}).collect()}
    }
    async fn cold(p:RecordPartition,path:&Path,clock:Arc<Clock>)->RecordPartition {
        let r=p.inner.lock().await.remote.take().unwrap();p.finish_service_shutdown().await.unwrap();drop(p);
        let p=RecordPartition::open_s3_with_remote(path,config().max_journal_bytes,r).await.unwrap();p.inner.lock().await.set_clock(clock);p
    }
    #[tokio::test]
    async fn group_remote_one_sequence_cache_failure_and_cold_receipt() {
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("warm");let db=temp.path().join("authority.sqlite");
        let (p,_,clock)=source(&root,&db).await;let first=request("first",0,&["a","b","c"]);
        let receipt=p.replace_group(first.clone()).await.unwrap();assert_eq!(receipt.seq,1);
        let next=request("next",1,&["a","d"]);std::fs::write(root.join(".test-cache-append-failure"),b"fail").unwrap();
        assert!(matches!(p.replace_group(next.clone()).await,Err(RecordError::DurablePending(2))));
        assert!(matches!(p.get("a").await,Err(RecordError::RecoveryRequired)));
        {let inner=p.inner.lock().await;let r=inner.remote.as_ref().unwrap();let h=r.authority.lock().await.read_head("groups").unwrap().unwrap();
            assert_eq!(h.committed_seq,2);let entries=crate::storage::object_log::load_committed_entries_after(&r.store,&h,0,100,4*1024*1024).await.unwrap();
            assert_eq!(entries.len(),2);assert_eq!(entries[0].seq,1);assert_eq!(entries[1].seq,2);}
        let p=cold(p,&temp.path().join("cold"),clock).await;
        assert_eq!(p.replace_group(first).await.unwrap(),receipt);assert_eq!(p.replace_group(next).await.unwrap().seq,2);
        assert_eq!(p.group_version("g").await.unwrap(),Some(2));assert!(p.get("b").await.unwrap().is_none());assert!(p.get("d").await.unwrap().is_some());
        p.finish_service_shutdown().await.unwrap();
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn group_upload_deadline_or_clock_rejection_never_selects_object() {
        for rollback in [false,true] {
            let temp=tempfile::tempdir().unwrap();let root=temp.path().join("warm");let db=temp.path().join("authority.sqlite");
            let (p,_,clock)=source(&root,&db).await;let now=clock.0.load(Ordering::SeqCst);
            p.replace_group(request("base",0,&["a"])).await.unwrap();
            let mut r=request("timed",1,&["b"]);r.records[0].temporal=Some(TemporalMetadata{expire_at_ms:Some(now+1000),..Default::default()});
            let gate=db.with_extension("group-after-upload-pause");std::fs::write(&gate,b"pause").unwrap();
            let owner=p.clone();let task=tokio::spawn(async move{owner.replace_group(r).await});
            tokio::time::timeout(Duration::from_secs(10),async{while !gate.with_extension("started").exists(){tokio::task::yield_now().await;}}).await.unwrap();
            tokio::time::timeout(Duration::from_secs(2),async {
                assert!(p.get("a").await.unwrap().is_some());
                assert!(p.get("b").await.unwrap().is_none());
                assert_eq!(crate::record::tests::query(&p,SearchMode::Lexical).await.hits.len(),1);
                let inner=p.inner.lock().await;
                assert_eq!(inner.state.seq,1);
                let remote=inner.remote.as_ref().unwrap();
                assert_eq!(remote.authority.lock().await.read_head("groups").unwrap().unwrap().committed_seq,1);
            }).await.expect("reads and shared authority must remain available during upload");
            clock.0.store(if rollback{now-300_001}else{now+1000},Ordering::SeqCst);std::fs::remove_file(gate).unwrap();
            let result=task.await.unwrap();if rollback{assert!(matches!(result,Err(RecordError::ClockUncertain)));}else{assert!(matches!(result,Err(RecordError::Busy)));}
            {let inner=p.inner.lock().await;assert_eq!(inner.state.seq,1);let r=inner.remote.as_ref().unwrap();assert_eq!(r.authority.lock().await.read_head("groups").unwrap().unwrap().committed_seq,1);}
            clock.0.store(now+1000,Ordering::SeqCst);let p=cold(p,&temp.path().join("cold"),clock).await;
            assert_eq!(p.group_version("g").await.unwrap(),Some(1));assert!(p.get("a").await.unwrap().is_some());
                assert!(p.get("b").await.unwrap().is_none());p.finish_service_shutdown().await.unwrap();
        }
    }
    #[tokio::test]
    async fn group_backup_restores_fixed_version_and_independent_retry() {
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("source");let db=temp.path().join("authority.sqlite");
        let (p,store,clock)=source(&root,&db).await;let first=request("first",0,&["a","b"]);let receipt=p.replace_group(first.clone()).await.unwrap();
        p.compact_journal().await.unwrap();let (bundle,seq,_)=p.create_s3_backup("group-cut",100,1024*1024).await.unwrap();assert_eq!(seq,1);
        p.replace_group(request("next",1,&["c"])).await.unwrap();
        let restored=RecordPartition::restore_s3_with_remote(&temp.path().join("restore"),config(),remote(store,&temp.path().join("restore.sqlite"),"restored"),&bundle,1024*1024,10000,16*1024*1024).await.unwrap();
        restored.inner.lock().await.set_clock(clock);assert_eq!(restored.group_version("g").await.unwrap(),Some(1));
        assert_eq!(restored.replace_group(first).await.unwrap(),receipt);assert!(restored.get("a").await.unwrap().is_some());assert!(restored.get("c").await.unwrap().is_none());
        restored.finish_service_shutdown().await.unwrap();p.finish_service_shutdown().await.unwrap();
    }
}
