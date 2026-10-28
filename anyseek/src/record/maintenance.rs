//! Numeric, attempt-local maintenance diagnostics. Never log records, queries,
//! operation IDs, object paths, credentials, or serialized State.
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SealFallback {
    PrivateCleanup,
    MissingCapture,
    PackageBound,
    UnstableTail,
}

pub(super) fn seal_fallback(
    private_cleanup: bool, captured: bool, selected: usize,
    pending: usize, limit: usize, stable: bool,
) -> Option<SealFallback> {
    if private_cleanup { return Some(SealFallback::PrivateCleanup); }
    if !captured { return Some(SealFallback::MissingCapture); }
    if selected.checked_add(pending).and_then(|n|n.checked_add(1)).is_none_or(|n|n>=limit) {
        return Some(SealFallback::PackageBound);
    }
    if !stable { return Some(SealFallback::UnstableTail); }
    None
}

/// Exit timing includes errors/cancellation: it is not a success counter.
/// Nested phases overlap and must not be summed into total elapsed time.
pub(super) struct Phase {
    name: &'static str,
    seq: u64,
    started: Instant,
}
impl Phase {
    pub(super) fn new(name: &'static str, seq: u64) -> Self {
        #[cfg(test)] tests::trace_timings();
        log::debug!("record_maintenance phase={name} seq={seq} event=enter");
        Self {name,seq,started:Instant::now()}
    }
}
impl Drop for Phase {
    fn drop(&mut self) {
        log::info!("record_maintenance phase={} seq={} event=exit elapsed_us={}",
            self.name,self.seq,self.started.elapsed().as_micros());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Timings;
    impl log::Log for Timings {
        fn enabled(&self,metadata:&log::Metadata<'_>)->bool {metadata.level()<=log::Level::Info}
        fn log(&self,record:&log::Record<'_>) {
            if self.enabled(record.metadata()) && record.args().to_string().starts_with("record_maintenance") {
                // Test evidence is an opt-in file, never library terminal output.
                use std::io::Write;
                if let Some(path)=std::env::var_os("ANYSEEK_TEST_PHASE_TIMINGS") {
                    if let Ok(mut file)=std::fs::OpenOptions::new().create(true).append(true).open(path) {
                        let _=writeln!(file,"{}",record.args());
                    }
                }
            }
        }
        fn flush(&self) {}
    }
    pub(super) fn trace_timings() {
        static LOGGER:Timings=Timings;
        if std::env::var_os("ANYSEEK_TEST_PHASE_TIMINGS").is_some() && log::set_logger(&LOGGER).is_ok() {log::set_max_level(log::LevelFilter::Info);}
    }
    #[test]
    fn seal_bound_counts_successor_and_rejects_overflow() {
        assert_eq!(seal_fallback(false,true,9,5,16,true),None);
        assert_eq!(seal_fallback(false,true,9,6,16,true),Some(SealFallback::PackageBound));
        assert_eq!(seal_fallback(false,true,usize::MAX,1,16,true),Some(SealFallback::PackageBound));
    }
    #[tokio::test]
    async fn native_hard_pressure_returns_busy_before_accepting_and_worker_makes_progress() {
        trace_timings();
        use super::super::tests::{config,path,write};
        let root=path();let mut settings=config();settings.max_records=256;settings.max_operations=2048;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        for i in 0..1024 {
            let expected=if i==0 {ExpectedVersion::Absent}else{ExpectedVersion::Match(i)};
            p.write(write(&format!("op-{i}"),"existing","alpha",expected)).await.unwrap();
        }
        std::fs::write(root.join(".test-cache-merge-pause"),b"pause").unwrap();
        p.write(write("op-1024","existing","alpha",ExpectedVersion::Match(1024))).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !root.join(".test-cache-merge-started").exists(){tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();
        let mut last=None;
        for i in 1025..1152 {
            let request=write(&format!("op-{i}"),"existing","alpha",ExpectedVersion::Match(i));
            let receipt=p.write(request.clone()).await.unwrap();last=Some((request,receipt));
        }
        let request=write("blocked","new","beta",ExpectedVersion::Absent);
        assert!(matches!(p.write(request.clone()).await,Err(RecordError::Busy)));
        {let inner=p.inner.lock().await;assert_eq!(inner.state.seq,1152);assert!(inner.anchor.is_none());}
        assert!(tokio::time::timeout(std::time::Duration::from_secs(2),p.get("new")).await.unwrap().unwrap().is_none());
        let found=tokio::time::timeout(std::time::Duration::from_secs(2),p.search("alpha".into(),None,SearchMode::Lexical,vec![],1)).await.unwrap().unwrap();
        assert_eq!(found.hits.len(),1);
        let (old,receipt)=last.unwrap();assert_eq!(p.write(old).await.unwrap(),receipt);
        assert!(matches!(tokio::time::timeout(std::time::Duration::from_secs(2),p.write(request.clone())).await.unwrap(),Err(RecordError::Busy)));
        std::fs::remove_file(root.join(".test-cache-merge-pause")).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(20),async {
            while p.inner.lock().await.maintenance_scheduled {tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();
        assert!(p.inner.lock().await.maintenance_requested.is_none());
        let receipt=p.write(request.clone()).await.unwrap();assert_eq!(receipt.seq,1153);
        // This newly rotated spare tail may schedule a second owned merge.
        super::super::tests::wait_maintenance_idle(&p).await;
        drop(p);let p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.write(request).await.unwrap(),receipt);assert!(p.get("new").await.unwrap().is_some());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn private_erasure_intent_is_not_installed_before_background_certificate() {
        use super::super::tests::{config,path,source_write,generation_write,stage_generation};
        struct Clock(std::sync::atomic::AtomicU64);
        impl temporal::Clock for Clock {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0.load(std::sync::atomic::Ordering::SeqCst))}}
        for fail_gc in [false,true] {
        let root=path();let settings=config();let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let clock=Arc::new(Clock(std::sync::atomic::AtomicU64::new(now)));p.inner.lock().await.clock=clock.clone();
        p.write(source_write("source","s",&[])).await.unwrap();
        let stage=generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["private"]));
        let receipt=p.write(stage.clone()).await.unwrap();
        std::fs::write(root.join(".test-native-rebuild-pause"),b"pause").unwrap();
        clock.0.store(now+1_800_001,std::sync::atomic::Ordering::SeqCst);
        assert!(matches!(p.advance_time().await,Err(RecordError::Busy)));
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !root.join(".test-native-rebuild-started").exists(){tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();
        {let inner=tokio::time::timeout(std::time::Duration::from_secs(2),p.inner.lock()).await.unwrap();
            assert!(inner.state.private_cleanup_pending);assert_eq!(inner.state.lease_expirations.len(),1);}
        if fail_gc {std::fs::write(root.join(".test-cleanup-failure"),b"retain old private generation").unwrap();}
        std::fs::remove_file(root.join(".test-native-rebuild-pause")).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(20),async {
            while p.inner.lock().await.maintenance_scheduled {tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();
        if fail_gc {
            {let inner=p.inner.lock().await;assert!(!inner.state.private_cleanup_pending);assert!(inner.maintenance_gc_pending);assert!(inner.maintenance_requested.is_some());}
            std::fs::remove_file(root.join(".test-cleanup-failure")).unwrap();
            assert!(matches!(p.advance_time().await,Err(RecordError::Busy)));
            tokio::time::timeout(std::time::Duration::from_secs(20),async {
                while p.inner.lock().await.maintenance_scheduled {tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
            }).await.unwrap();
        }
        {let inner=p.inner.lock().await;assert!(!inner.state.private_cleanup_pending);assert!(!inner.maintenance_gc_pending);assert!(inner.maintenance_requested.is_none());}
        assert_eq!(p.write(stage.clone()).await.unwrap(),receipt);
        drop(p);let p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();p.inner.lock().await.clock=clock;
        assert_eq!(p.write(stage).await.unwrap(),receipt);assert!(p.get("private").await.unwrap().is_none());
        drop(p);std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn seal_reason_preserves_private_and_capture_precedence() {
        assert_eq!(seal_fallback(true,false,16,0,16,false),Some(SealFallback::PrivateCleanup));
        assert_eq!(seal_fallback(false,false,16,0,16,false),Some(SealFallback::MissingCapture));
        assert_eq!(seal_fallback(false,true,1,0,16,false),Some(SealFallback::UnstableTail));
    }

    #[test]
    fn receipt_gc_preserves_feed_and_retry_window_and_persists_rollback_floor() {
        let settings=super::super::tests::config();
        let mut state=State::default();
        for (nonce,epoch) in [("expired",100),("feed",100),("recent",101)] {
            let mut request=super::super::tests::write(nonce,nonce,"alpha",ExpectedVersion::Absent);
            request.operation_id.epoch=epoch;
            let mutation::PreparedWrite::Append {applied,..}=mutation::prepare_at(
                &state,&settings,checkpoint_codec::length(&settings,&state).unwrap(),request,epoch,epoch).unwrap()
                else {panic!("new operation")};
            apply(&mut state,&applied).unwrap();
        }
        state.feed.floor=1;
        let before=state.operations.bytes();
        collect_operations(&mut state,102).unwrap();
        assert_eq!(state.operations.len(),2);
        assert!(state.operations.bytes()<before);
        assert_eq!(state.operations[0].request.operation_id.nonce,"feed");
        assert_eq!(state.operations[1].request.operation_id.nonce,"recent");
        assert_eq!(state.epoch_watermark,102);
        let encoded=encode_entry(&Entry::Checkpoint {config:settings.clone(),state}).unwrap();
        let Entry::Checkpoint {mut state,..}=serde_json::from_slice(&encoded).unwrap() else {panic!("checkpoint")};
        restore_operation_lookup(&mut state).unwrap();
        let mut expired=super::super::tests::write("expired","expired","alpha",ExpectedVersion::Absent);
        expired.operation_id.epoch=100;
        assert!(matches!(mutation::prepare(&state,&settings,encoded.len(),expired,100),Err(RecordError::OperationExpired)));
        let mut recent=super::super::tests::write("recent","recent","alpha",ExpectedVersion::Absent);
        recent.operation_id.epoch=101;
        assert!(matches!(mutation::prepare(&state,&settings,encoded.len(),recent,100),Ok(mutation::PreparedWrite::Replay(_))));
        assert_eq!(checkpoint_codec::length(&settings,&state).unwrap(),encoded.len());
        assert_eq!(state_proof::length(&state).unwrap(),checkpoint_codec::json_length(&state).unwrap());
        let mut invalid=state.try_clone().unwrap();
        invalid.retired_operation_prefix.as_mut().unwrap().seq=2;
        assert!(temporal::validate_state(&invalid).is_err());
        let mut invalid=state.try_clone().unwrap();
        invalid.retired_operation_prefix.as_mut().unwrap().time_ms=102;
        assert!(temporal::validate_state(&invalid).is_err());
        let mut invalid=state.try_clone().unwrap();
        invalid.operations.retain(|op|op.receipt.seq!=2);
        assert!(temporal::validate_state(&invalid).is_err());
        let mut invalid=state.try_clone().unwrap();
        invalid.operations.edit(0,0,|op|{op.observed_time_ms=99;Ok(())}).unwrap();
        assert!(temporal::validate_state(&invalid).is_err());
        let mut changed=state.try_clone().unwrap();
        changed.retired_operation_prefix.as_mut().unwrap().time_ms=101;
        assert_eq!(state_proof::length(&state).unwrap(),state_proof::length(&changed).unwrap());
        assert!(state_proof::verify(&settings,&state,&changed).is_err());
        let mut unordered=state.try_clone().unwrap();
        let mut reversed=state_values::Values::default();
        for operation in state.operations.iter().rev(){reversed.push(operation.clone()).unwrap();}
        unordered.operations=reversed;
        assert!(temporal::validate_state(&unordered).is_err());
    }

    #[tokio::test]
    async fn receipt_gc_retains_generation_lease_provenance() {
        use super::super::tests::{config,source_write,generation_write,stage_generation};
        let root=tempfile::tempdir().unwrap();
        let p=RecordPartition::create(&root.path().join("partition"),config()).await.unwrap();
        p.write(source_write("source","s",&[])).await.unwrap();
        p.write(generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["private"]))).await.unwrap();
        let mut inner=p.inner.lock().await;
        let expiry=inner.state.time_watermark_ms+1_800_001;
        inner.expire_staging(expiry).await.unwrap();
        let mut state=inner.state.try_clone().unwrap();
        state.feed.floor=state.seq;
        collect_operations(&mut state,current_epoch()+2).unwrap();
        assert_eq!(state.operations.len(),1);
        assert!(state.operations[0].request.generation.is_some());
        assert!(state.operations[0].request_digest.is_some());
        assert_eq!(state.lease_expirations.len(),1);
        lease::validate_state(&state).unwrap();
        validate_restored_state(&inner.config,&state).unwrap();
    }

    #[tokio::test]
    async fn receipt_gc_checkpoint_recovery_resolves_publication_failures() {
        use super::super::tests::{config,write,wait_maintenance_idle};
        for fault in [None,Some(".test-checkpoint-before-certificate"),Some(".test-checkpoint-after-certificate")] {
            let root=tempfile::tempdir().unwrap();
            let bootstrap=RecordPartition::create(&root.path().join("bootstrap"),config()).await.unwrap();
            let (settings,mut state)={let inner=bootstrap.inner.lock().await;(inner.config.clone(),inner.state.try_clone().unwrap())};
            drop(bootstrap);
            let epoch=current_epoch();
            let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
            let mut old=write("old","old","alpha",ExpectedVersion::Absent);old.operation_id.epoch=epoch-2;
            let recent=write("recent","recent","alpha",ExpectedVersion::Absent);
            let mut receipt=None;
            for (request,at) in [(old.clone(),now-8*86_400_000),(recent.clone(),now)] {
                let mutation::PreparedWrite::Append {applied,..}=mutation::prepare_at(&state,&settings,
                    checkpoint_codec::length(&settings,&state).unwrap(),request.clone(),request.operation_id.epoch,at).unwrap()
                    else {panic!("new operation")};
                receipt=Some(applied.receipt.clone());apply(&mut state,&applied).unwrap();
            }
            state.feed.floor=1;
            let path=root.path().join("partition");
            let bytes=encode_entry(&Entry::Checkpoint {config:settings.clone(),state}).unwrap();
            drop(PartitionJournal::create(&path,settings.max_journal_bytes,&bytes).unwrap());
            let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
            std::fs::write(path.join(".test-indexer-pause"),b"pause").unwrap();
            if let Some(fault)=fault {std::fs::write(path.join(fault),b"fault").unwrap();}
            assert!(matches!(p.write(write("next","next","beta",ExpectedVersion::Absent)).await,Err(RecordError::Busy)));
            tokio::time::timeout(std::time::Duration::from_secs(5),async {
                while !path.join(".test-indexer-started").exists(){tokio::task::yield_now().await;}
            }).await.unwrap();
            assert_eq!(p.inner.lock().await.state.operations.len(),2,"capture must not trim live authority");
            assert_eq!(p.write(recent.clone()).await.unwrap(),receipt.clone().unwrap());
            std::fs::remove_file(path.join(".test-indexer-pause")).unwrap();
            wait_maintenance_idle(&p).await;
            if let Some(fault)=fault {std::fs::remove_file(path.join(fault)).unwrap();}
            drop(p);
            let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
            assert_eq!(p.inner.lock().await.state.operations.len(),if fault==Some(".test-checkpoint-before-certificate"){2}else{1});
            assert!(matches!(p.write(old).await,Err(RecordError::OperationExpired)));
            assert_eq!(p.write(recent).await.unwrap(),receipt.unwrap());
            assert!(p.get("old").await.unwrap().is_some(),"receipt GC never removes authoritative bodies");
        }
    }

    #[cfg(feature="s3-object-store")]
    #[tokio::test]
    async fn receipt_gc_remote_checkpoint_cold_recovery_preserves_time_prefix() {
        use super::super::tests::{config,write,wait_maintenance_idle};
        use crate::storage::{s3_compatible::S3CompatibleStore,sqlite_authority::SqliteAuthority};
        let temporary=tempfile::tempdir().unwrap();
        let database=temporary.path().join("authority.sqlite");
        let settings=config();
        let objects=Arc::new(object_store::memory::InMemory::new());
        let p=RecordPartition::create(&temporary.path().join("records"),settings.clone()).await.unwrap();
        let store=S3CompatibleStore::from_store(objects.clone(),1024*1024);
        let mut authority=SqliteAuthority::open(&database,std::time::Duration::from_secs(1)).unwrap();
        authority.create_partition("source",1).unwrap();
        let bootstrap={let inner=p.inner.lock().await;encode_entry(&Entry::Checkpoint {config:inner.config.clone(),state:inner.state.try_clone().unwrap()}).unwrap()};
        store.publish_bootstrap(&mut authority,"source",1,&bootstrap,unix_time_ms_i64()).await.unwrap();
        p.inner.lock().await.remote=Some(RemotePersistence {store,authority:Mutex::new(authority),partition_id:"source".into(),owner_epoch:1,
            max_tail_blocks:128,max_tail_bytes:1024*1024,max_object_bytes:1024*1024,_owner:None});
        let epoch=current_epoch();
        let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let mut old=write("old","old","alpha",ExpectedVersion::Absent);old.operation_id.epoch=epoch-2;
        let recent=write("recent","recent","alpha",ExpectedVersion::Absent);
        let mut receipt=None;
        // Construct previously durable decisions with their original trusted
        // clocks. Recovery must accept these even though admission has expired.
        for (request,at) in [(old.clone(),now-8*86_400_000),(recent.clone(),now)] {
            let mut inner=p.inner.lock().await;
            let mutation::PreparedWrite::Append {applied,bytes,..}=mutation::prepare_at(&inner.state,&inner.config,
                inner.snapshot_bytes,request.clone(),request.operation_id.epoch,at).unwrap() else {panic!("new operation")};
            receipt=Some(applied.receipt.clone());
            inner.remote.as_mut().unwrap().commit_entry(applied.receipt.seq,
                format!("{}:{}",request.operation_id.epoch,request.operation_id.nonce),&bytes).await.unwrap();
            inner.append_operation(&bytes,applied.receipt.seq).unwrap();
            inner.replay_context().replay(Entry::Applied(applied)).await.unwrap();
            inner.feed_cache=change_feed::Cache::rebuild(&inner.state,&inner.config.change_feed).unwrap();
        }
        assert!(matches!(p.write(write("next","next","beta",ExpectedVersion::Absent)).await,Err(RecordError::Busy)));
        wait_maintenance_idle(&p).await;
        assert_eq!(p.inner.lock().await.state.operations.len(),1);
        drop(p);
        let remote=RemotePersistence {store:S3CompatibleStore::from_store(objects,1024*1024),
            authority:Mutex::new(SqliteAuthority::open(&database,std::time::Duration::from_secs(1)).unwrap()),
            partition_id:"source".into(),owner_epoch:1,max_tail_blocks:128,max_tail_bytes:1024*1024,max_object_bytes:1024*1024,_owner:None};
        let recovered=RecordPartition::open_s3_with_remote(&temporary.path().join("empty-cache"),settings.max_journal_bytes,remote).await.unwrap();
        {let inner=recovered.inner.lock().await;assert_eq!(inner.state.operations.len(),1);assert_eq!(inner.state.retired_operation_prefix.as_ref().unwrap().seq,1);}
        assert!(matches!(recovered.write(old).await,Err(RecordError::OperationExpired)));
        assert_eq!(recovered.write(recent).await.unwrap(),receipt.unwrap());
        assert_eq!(recovered.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,2);
    }
}

use super::*;
/// Captured committed frames own independent raw-buffer admission. The source
/// journal is never read by the background worker through a second cursor.
pub(super) struct SourceProof {
    anchor:Option<anchor::Reference>,
    pending:Vec<segment::Package>,
    frames:Vec<Vec<u8>>,
    _memory:crate::storage::build_budget::MemoryLease,
}
impl SourceProof {
    pub(super) fn capture(inner:&mut Inner)->Result<Self,RecordError> {
        let _phase=Phase::new("source_frames_capture_locked",inner.state.seq);
        let mut memory=crate::storage::build_budget::reserve_memory(0).map_err(storage)?;
        let mut frames=Vec::new();
        inner.journal.visit_committed(|bytes|{
            let charge=bytes.len().checked_add(2*std::mem::size_of::<Vec<u8>>()).ok_or_else(||std::io::Error::other("source frame inventory overflow"))?;
            memory.grow(charge as u64)?;
            frames.push(bytes.to_vec());Ok(())
        }).map_err(storage)?;
        memory.grow((inner.index.pending_seals.len() as u64).checked_mul((2*std::mem::size_of::<segment::Package>()) as u64).ok_or_else(||storage("source package inventory overflow"))?).map_err(storage)?;
        Ok(Self{anchor:inner.anchor.clone(),pending:inner.index.pending_seals.clone(),frames,_memory:memory})
    }
    #[cfg(feature="s3-object-store")]
    pub(super) async fn verify_remote(&self,remote:&remote_checkpoint::RemoteBuild,root:&Path,config:&RecordConfig)->Result<(),RecordError> {
        remote.verify_source(root,self.anchor.as_ref(),&self.frames,config.max_journal_bytes).await
    }
    pub(super) fn verify(&self,root:&Path,config:&RecordConfig,expected:&State)->Result<(),RecordError> {
        let _phase=Phase::new("background_source_proof",expected.seq);
        if let Some(anchor)=&self.anchor {return anchor.verify_captured_suffix(root,config,expected,&self.frames,&self.pending);}
        let mut replayed=None;
        let mut admitted=0usize;
        for frame in &self.frames {
            let _decode=crate::storage::build_budget::reserve_memory((frame.len() as u64).checked_mul(32).ok_or_else(||storage("source proof decode overflow"))?).map_err(storage)?;
            let entry:Entry=serde_json::from_slice(frame).map_err(storage)?;
            if replayed.is_none() {
                let Entry::Checkpoint{config:source,mut state}=entry else{return Err(storage("missing captured bootstrap authority"));};
                if checkpoint_codec::json_digest(&source)?!=checkpoint_codec::json_digest(config)?{return Err(storage("captured bootstrap configuration mismatch"));}
                admitted=checkpoint_codec::json_length(&state)?;
                state.admit_bytes(admitted)?;
                restore_operation_lookup(&mut state)?;
                state.admit_bytes(checkpoint_codec::json_length(&state)?)?;
                replayed=Some(state);continue;
            }
            let state=replayed.as_mut().unwrap();
            let growth=checkpoint_codec::json_length(&entry)?.checked_mul(3).ok_or_else(||storage("source proof growth overflow"))?;
            admitted=admitted.checked_add(growth).ok_or_else(||storage("source proof size overflow"))?;
            state.admit_bytes(admitted)?;
            match entry {
                Entry::Applied(op)=>apply(state,&op)?,
                Entry::Expired(event)=>temporal::apply(state,&event)?,
                Entry::LeaseExpired(event)=>lease::apply(state,&event)?,
                Entry::FeedTrim(event)=>change_feed::apply_trim(state,&config.change_feed,&event)?,
                _=>return Err(storage("unexpected captured source authority")),
            }
        }
        state_proof::verify(config,expected,&replayed.ok_or_else(||storage("empty captured source journal"))?)
    }
}
impl Inner {
    pub(super) fn request_checkpoint(&mut self,cause:&'static str)->RecordError {
        self.maintenance_requested.get_or_insert(cause);
        log::info!("record_maintenance event=backpressure cause={} seq={}",cause,self.state.seq);
        RecordError::Busy
    }
}
impl RecordPartition {
    pub(super) async fn kick_maintenance(&self) {
        let mut inner=self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some() || inner.maintenance_requested.is_none()
            || inner.background.is_some() || inner.maintenance_scheduled {return;}
        inner.maintenance_scheduled=true;
        let job=if inner.maintenance_requested==Some(cache_merge::CAUSE) {
            self.start_cache_merge_job(std::time::Duration::from_secs(300))
        } else {self.start_index_job(std::time::Duration::from_secs(300),true)};
        match job {
            Ok(job)=>drop(job),
            Err(error)=>{inner.maintenance_scheduled=false;log::debug!("record_maintenance event=admission_deferred error={error}");}
        }
    }
}

/// A same-sequence transition is proved against the authenticated source cut,
/// then installed with the new certificate, never applied to live State early.
pub(super) struct Intent {
    private_cleanup:bool,
    trim:Option<change_feed::Trim>,
    operations:Option<OperationGc>,
}
impl Intent {
    pub(super) fn capture(inner:&Inner,automatic:bool)->Result<Self,RecordError> {
        Ok(Self{private_cleanup:automatic&&inner.state.private_cleanup_pending,
            trim:if automatic {inner.maintenance_trim.clone()}else{None},
            operations:if automatic {Some(OperationGc::capture(&inner.state,current_epoch(),
                inner.maintenance_trim.as_ref().map_or(inner.state.feed.floor,|trim|trim.floor))?)}else{None}})
    }
    pub(super) fn apply(&self,state:&mut State,config:&RecordConfig)->Result<(),RecordError> {
        if self.private_cleanup {state.private_cleanup_pending=false;}
        if let Some(trim)=&self.trim {change_feed::apply_trim(state,&config.change_feed,trim)?;}
        if let Some(operations)=&self.operations {operations.apply(state)?;}
        Ok(())
    }
    pub(super) fn rewrites_payloads(&self)->bool {self.private_cleanup}
    pub(super) fn trims_feed(&self)->bool {self.trim.is_some()}
}

fn expired_operation(op:&Applied,feed_floor:u64,epoch:u64)->bool {
    // Feed rows retain their source until its authenticated floor advances.
    op.receipt.seq<=feed_floor
        && op.request.operation_id.epoch.saturating_add(1)<epoch
}
pub(super) fn has_expired_operations(state:&State,epoch:u64)->Result<bool,RecordError> {
    let epoch=epoch.max(state.epoch_watermark);
    if !state.operations.iter().take_while(|op|op.receipt.seq<=state.feed.floor)
        .any(|op|expired_operation(op,state.feed.floor,epoch)) {return Ok(false);}
    let _memory=crate::storage::build_budget::reserve_memory((state.operations.len() as u64)
        .checked_mul(512).ok_or_else(||storage("receipt GC protection overflow"))?).map_err(storage)?;
    let protected=lease::protected_operation_sequences(state);
    Ok(state.operations.iter().take_while(|op|op.receipt.seq<=state.feed.floor)
        .any(|op|expired_operation(op,state.feed.floor,epoch)&&!protected.contains(&op.receipt.seq)))
}
struct OperationGc {
    epoch:u64,
    selected:std::collections::BTreeSet<u64>,
    prefix:Option<temporal::RetiredPrefix>,
    _memory:crate::storage::build_budget::MemoryLease,
}
impl OperationGc {
    fn capture(state:&State,epoch:u64,floor:u64)->Result<Self,RecordError> {
        let epoch=epoch.max(state.epoch_watermark);
        let memory=crate::storage::build_budget::reserve_memory((state.operations.len() as u64)
            .checked_mul(512).ok_or_else(||storage("receipt GC inventory overflow"))?).map_err(storage)?;
        let protected=lease::protected_operation_sequences(state);
        let selected:std::collections::BTreeSet<_>=state.operations.iter()
            .filter(|op|expired_operation(op,floor,epoch)&&!protected.contains(&op.receipt.seq))
            .map(|op|op.receipt.seq).collect();
        let prefix=state.operations.iter().filter(|op|selected.contains(&op.receipt.seq))
            .max_by_key(|op|op.receipt.seq).map(|last|temporal::RetiredPrefix {seq:last.receipt.seq,time_ms:last.observed_time_ms});
        Ok(Self{epoch,selected,prefix,_memory:memory})
    }
    fn apply(&self,state:&mut State)->Result<(),RecordError> {
    let Some(prefix)=&self.prefix else {return Ok(());};
    // SourceProof validated the full source cut before this transition. Retain
    // its compressed committed-time boundary inside the authenticated image.
    temporal::validate_state(state)?;
    if state.retired_operation_prefix.as_ref().is_none_or(|old|old.seq<prefix.seq) {
        state.retired_operation_prefix=Some(prefix.clone());
    }
    state.operations.retain(|op|!self.selected.contains(&op.receipt.seq));
    // The fixed floor travels in the selected checkpoint. Recovery never uses
    // its own wall clock; rollback cannot admit a forgotten operation again.
    state.epoch_watermark=state.epoch_watermark.max(self.epoch);
    restore_operation_lookup(state)
    }
}
#[cfg(test)]
pub(super) fn collect_operations(state:&mut State,epoch:u64)->Result<(),RecordError> {
    OperationGc::capture(state,epoch,state.feed.floor)?.apply(state)
}

#[cfg(test)]
#[path = "maintenance_gc_tests.rs"]
mod gc_tests;
