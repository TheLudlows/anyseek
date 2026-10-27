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
    #[test]
    fn seal_bound_counts_successor_and_rejects_overflow() {
        assert_eq!(seal_fallback(false,true,9,5,16,true),None);
        assert_eq!(seal_fallback(false,true,9,6,16,true),Some(SealFallback::PackageBound));
        assert_eq!(seal_fallback(false,true,usize::MAX,1,16,true),Some(SealFallback::PackageBound));
    }
    #[tokio::test]
    async fn native_hard_pressure_returns_busy_before_accepting_and_worker_makes_progress() {
        use super::super::tests::{config,path,write};
        let root=path();let mut settings=config();settings.max_records=256;settings.max_operations=512;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let mut last=None;
        for i in 0..128 {let request=write(&format!("op-{i}"),&format!("id-{i}"),"alpha",ExpectedVersion::Absent);let receipt=p.write(request.clone()).await.unwrap();last=Some((request,receipt));}
        std::fs::write(root.join(".test-native-rebuild-pause"),b"pause").unwrap();
        let request=write("blocked","new","beta",ExpectedVersion::Absent);
        assert!(matches!(p.write(request.clone()).await,Err(RecordError::Busy)));
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !root.join(".test-native-rebuild-started").exists(){tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();
        assert_eq!(p.inner.lock().await.state.operations.len(),128);
        assert!(tokio::time::timeout(std::time::Duration::from_secs(2),p.get("new")).await.unwrap().unwrap().is_none());
        let found=tokio::time::timeout(std::time::Duration::from_secs(2),p.search("alpha".into(),None,SearchMode::Lexical,vec![],1)).await.unwrap().unwrap();
        assert_eq!(found.hits.len(),1);
        let (old,receipt)=last.unwrap();assert_eq!(p.write(old).await.unwrap(),receipt);
        assert!(matches!(tokio::time::timeout(std::time::Duration::from_secs(2),p.write(request.clone())).await.unwrap(),Err(RecordError::Busy)));
        std::fs::remove_file(root.join(".test-native-rebuild-pause")).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(20),async {
            while p.inner.lock().await.maintenance_scheduled {tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();
        assert!(p.inner.lock().await.maintenance_requested.is_none());
        {
            let mut inner=p.inner.lock().await;
            let oversized=inner.config.max_journal_bytes as usize-64;
            assert!(inner.journal.checkpoint_can_make_room(oversized,0));
            assert!(matches!(inner.ensure_journal_room(oversized,0).await,Err(RecordError::Rejected(_))));
            assert!(inner.maintenance_requested.is_none());
        }
        let receipt=p.write(request.clone()).await.unwrap();assert_eq!(receipt.seq,129);
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
        match self.start_index_job(std::time::Duration::from_secs(300),true) {
            Ok(job)=>drop(job),
            Err(error)=>{inner.maintenance_scheduled=false;log::debug!("record_maintenance event=admission_deferred error={error}");}
        }
    }
}

/// A same-sequence transition is proved against the authenticated source cut,
/// then installed with the new certificate, never applied to live State early.
#[derive(Clone)]
pub(super) struct Intent {
    private_cleanup:bool,
    trim:Option<change_feed::Trim>,
}
impl Intent {
    pub(super) fn capture(inner:&Inner,automatic:bool)->Self {
        Self{private_cleanup:automatic&&inner.state.private_cleanup_pending,
            trim:if automatic {inner.maintenance_trim.clone()}else{None}}
    }
    pub(super) fn apply(&self,state:&mut State,config:&RecordConfig)->Result<(),RecordError> {
        if self.private_cleanup {state.private_cleanup_pending=false;}
        if let Some(trim)=&self.trim {change_feed::apply_trim(state,&config.change_feed,trim)?;}
        Ok(())
    }
    pub(super) fn rewrites_payloads(&self)->bool {self.private_cleanup}
    pub(super) fn trims_feed(&self)->bool {self.trim.is_some()}
}
