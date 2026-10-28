//! Bounded owned index jobs. Snapshot and candidate construction are separated
//! from the short, fenced publication phase. Dropping a caller does not cancel
//! accepted work or release the partition's reader/build protection early.
use super::*;
use futures::FutureExt;
use std::{sync::LazyLock, time::{Duration, Instant}};

pub(super) static WORKERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(2)));
const MAX_DEADLINE: Duration = Duration::from_secs(300);

fn crash_boundary(point:&str) {
    #[cfg(test)]
    if std::env::var("ANYSEEK_INDEXER_TEST_CRASH").as_deref()==Ok(point) { std::process::exit(93); }
    #[cfg(feature="fault-injection")]
    if std::env::var("ANYSEEK_INDEXER_FAULT").as_deref()==Ok(point) { std::process::exit(93); }
    let _=point;
}

pub(super) struct Tail {
    pub(super) id: u128,
    pub entries: Vec<Vec<u8>>,
    pub bytes:u64,
    pub max_bytes:u64,
    pub overflowed:bool,
    // Counts committed append identities, including same-sequence maintenance.
    pub revision:u64,
}
struct Plan {
    root: PathBuf, config:RecordConfig, snapshot:State,
    prior:crate::storage::payload_pool::Closure, base:Vec<u8>,
    disk:crate::storage::build_budget::DiskScope,
    memory:crate::storage::build_budget::MemoryLease,
    resources: crate::resources::Lease,
    source:Option<maintenance::SourceProof>,
    intent:maintenance::Intent,
    #[cfg(feature="s3-object-store")]
    remote:Option<remote_checkpoint::RemoteBuild>,
}
struct Candidate {
    state: State,
    index: view::RetrievalView,
    ordinals: Vec<String>,
    locations: BTreeMap<String, usize>,
    graph: graph::Cache,
    snapshot_bytes: usize,
}
impl Candidate {
    fn swap(&mut self, inner: &mut Inner) {
        std::mem::swap(&mut self.state, &mut inner.state);
        std::mem::swap(&mut self.index, &mut inner.index);
        std::mem::swap(&mut self.ordinals, &mut inner.ordinals);
        std::mem::swap(&mut self.locations, &mut inner.locations);
        std::mem::swap(&mut self.graph, &mut inner.graph_cache);
        std::mem::swap(&mut self.snapshot_bytes, &mut inner.snapshot_bytes);
    }
}
/// An accepted cancellation-safe maintenance job. Dropping this handle leaves
/// its bounded owned worker running; `finish` waits for the indexed cut.
pub struct IndexJob {
    pub(super) handle: tokio::task::JoinHandle<Result<u64,RecordError>>,
}
impl IndexJob {
    /// Wait for publication. Output still requires the service authorization permit.
    pub async fn finish(self) -> Result<u64,RecordError> {
        self.handle.await.map_err(|e|RecordError::OutcomeUnknown(e.to_string()))?
    }
}
impl RecordPartition {
    /// Index a fixed committed cut while allowing foreground reads and writes.
    /// At most two jobs run process-wide and one per partition; excess requests
    /// return Busy without spawning queued tasks. The maximum deadline is 5 min.
    /// The returned sequence is the indexed cut; visible state may be newer.
    pub async fn index_committed(&self, timeout: Duration) -> Result<u64, RecordError> {
        self.start_index_committed(timeout)?.finish().await
    }

    /// Admit and start a job without holding a service authorization lock during
    /// construction. The caller checks authorization for admission and for output.
    pub fn start_index_committed(&self, timeout: Duration) -> Result<IndexJob, RecordError> {
        let job=self.start_index_job(timeout,false)?;
        let partition=self.clone();
        Ok(IndexJob{handle:crate::resources::spawn_background(async move {
            let result=job.finish().await;
            partition.kick_maintenance().await;
            result
        })})
    }
    pub(super) fn start_index_job(&self,timeout:Duration,automatic:bool)->Result<IndexJob,RecordError> {
        if timeout.is_zero() || timeout > MAX_DEADLINE {
            return Err(RecordError::Rejected("index build deadline must be within 1ns..=300s".into()));
        }
        let worker = WORKERS.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let admission = self.admission.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let owner = self.inner.clone();
        let recovery = owner.clone();
        let cursors = self.indexes.clone();
        let handle = crate::resources::spawn_background(async move {
          let outcome = std::panic::AssertUnwindSafe(async move {
            let _worker = worker;
            let _admission = admission;
            let started = Instant::now();
            let id = rand::random::<u128>();
            let plan = {
                let mut inner = owner.lock().await;
                let _capture_phase=maintenance::Phase::new("background_capture_locked",inner.state.seq);
                if !inner.healthy || inner.state.sealed_for.is_some() { return Err(RecordError::RecoveryRequired); }
                if inner.background.is_some() { return Err(RecordError::Busy); }
                inner.index.drain_indexes().await?;
                if !automatic {inner.advance_time().await?;}
                inner.reclaim_anchor_objects().await?;
                let estimate = (inner.snapshot_bytes as u64).checked_mul(4)
                    .ok_or_else(||storage("index snapshot estimate overflow"))?;
                let tail_bytes=inner.config.max_journal_bytes.min(crate::storage::build_budget::EXPLICIT_BUFFERS);
                let resources = crate::resources::io_scope(crate::resources::Lane::Background)
                    .bytes(estimate.checked_add(tail_bytes).ok_or(RecordError::Busy)?)
                    .map_err(|_|RecordError::Busy)?;
                // State::try_clone owns the complete snapshot's 32x residency.
                // The former generic 4x snapshot reservation covered that same
                // allocation before State had ownership; no whole JSON buffer is
                // built here. Keep the distinct raw-tail lease and Scope quota.
                let memory = crate::storage::build_budget::reserve_memory(tail_bytes).map_err(storage)?;
                let root = inner.journal.root().to_path_buf();
                let limit = inner.config.max_journal_bytes.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024))
                    .ok_or_else(||storage("index disk budget overflow"))?;
                let disk = crate::storage::build_budget::DiskScope::begin(&root,limit).map_err(storage)?;
                let source=if automatic {Some(maintenance::SourceProof::capture(&mut inner)?)}else{None};
                // Complete fallible local capture before acquiring the remote pin.
                let snapshot={let _phase=maintenance::Phase::new("snapshot_clone_locked",inner.state.seq);
                    inner.state.try_clone().map_err(|e|storage(format!("stage=index_snapshot_clone {e}")))?};
                let base=serde_json::to_vec(&inner.anchor).map_err(storage)?;
                let intent=maintenance::Intent::capture(&inner,automatic)?;
                #[cfg(feature="s3-object-store")]
                let remote = match inner.remote.as_mut() {
                    Some(remote) => Some(remote.begin_background_build(timeout).await?),
                    None => None,
                };
                #[cfg(feature="s3-object-store")]
                if remote.as_ref().is_some_and(|r|r.build.cut_seq!=inner.state.seq) {
                    if let (Some(active),Some(captured))=(inner.remote.as_mut(),remote.as_ref()) {
                        if let Err(error)=active.authority.lock().await.cancel_index_build(&captured.build) {
                            log::warn!("index build pin cleanup pending: {error}");
                        }
                    }
                    inner.healthy=false;
                    inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
                    return Err(RecordError::RecoveryRequired);
                }
                let result = Plan {source,intent,root,config:inner.config.clone(),snapshot,prior:inner.payloads.clone(),
                    base,disk,memory,resources,
                    #[cfg(feature="s3-object-store")] remote};
                inner.background = Some(Tail{id,entries:Vec::new(),bytes:0,max_bytes:tail_bytes,overflowed:false,revision:0});
                result
            };
            let Plan{source,intent,root,config,mut snapshot,prior,base,disk,memory,resources:_resources,
                #[cfg(feature="s3-object-store")] remote} = plan;
            let cut = snapshot.seq;
            #[cfg(any(test,feature="fault-injection"))]
            pause(&root,timeout).await;
            let result = async {
                if started.elapsed() >= timeout { return Err(RecordError::Busy); }
                if let Some(source)=source {
                    source.verify(&root,&config,&snapshot)?;
                    #[cfg(feature="s3-object-store")]
                    if let Some(remote)=&remote {
                        source.verify_remote(remote,&root,&config).await.map_err(|error| {
                            if matches!(error,RecordError::Busy){error}else{
                                log::warn!("background remote source authentication failed: {error}");
                                RecordError::RecoveryRequired
                            }
                        })?;
                    }
                }
                intent.apply(&mut snapshot,&config)?;
                let prior=if intent.rewrites_payloads(){Default::default()}else{prior};
                let reference = anchor::Reference::prepare(&root,&config,&snapshot,prior,None,None,true,None).await?;
                // The pinned cut already owns complete history. Verify the
                // authenticated package against it and transfer that same owner
                // into the candidate instead of decoding a duplicate State.
                let loaded = reference.load_captured(&root, config.clone(), snapshot).await?;
                let payloads = loaded.payloads;
                let mut index = view::RetrievalView::single(loaded.index);
                let mut ordinals = loaded.ordinals;
                let mut locations = loaded.locations;
                let cache = root.join(format!("segment-{:032x}",rand::random::<u128>()));
                let (tail,_) = build_at(&root,&cache,&config,&State::default()).await?;
                index.rotate_tail(tail,&mut ordinals,&mut locations,view::RotationBudget{
                    max_segments:64,max_address_slots:config.max_records.checked_add(config.max_operations)
                        .and_then(|n|n.checked_mul(64)).ok_or_else(||storage("index address budget overflow"))?,
                }).await?;
                let mut candidate = Candidate {
                    graph:graph::Cache::rebuild(&loaded.state)?,
                    snapshot_bytes:checkpoint_codec::length(&config, &loaded.state)?,
                    state:loaded.state,index,ordinals,locations,
                };
                crash_boundary("prepared");
                #[cfg(feature="s3-object-store")]
                let manifest = match &remote {
                    Some(remote) => {
                        let remaining=timeout.checked_sub(started.elapsed()).ok_or(RecordError::Busy)?;
                        Some(tokio::time::timeout(remaining,remote.upload(&root,&reference,&payloads,config.max_journal_bytes))
                            .await.map_err(|_|RecordError::Busy)??)
                    },
                    None => None,
                };
                // Never swap speculative state into the live partition. Capture
                // a bounded suffix and comparison cut, then replay and prove it
                // without the foreground lock. A versioned recheck closes the gap.
                let mut consumed=0usize;
                let mut rounds=0usize;
                let (mut inner,installed_feed)=loop {
                    rounds+=1;
                    if rounds>8 || started.elapsed()>=timeout {return Err(RecordError::Busy);}
                    let (raw,mut expected,revision,entry_count,entry_bytes,_raw,_decoded)={
                        let inner=owner.lock().await;
                        let _capture_phase=maintenance::Phase::new("catchup_capture_locked",inner.state.seq);
                        if !inner.healthy || inner.state.sealed_for.is_some(){return Err(RecordError::RecoveryRequired);}
                        let tail=inner.background.as_ref().filter(|t|t.id==id && !t.overflowed).ok_or(RecordError::Busy)?;
                        if serde_json::to_vec(&inner.anchor).map_err(storage)?!=base || consumed>tail.entries.len(){return Err(RecordError::Busy);}
                        let suffix=&tail.entries[consumed..];
                        let raw_bytes=suffix.iter().try_fold(0u64,|n,b|n.checked_add(b.len() as u64)
                            .and_then(|n|n.checked_add((2*std::mem::size_of::<Vec<u8>>()) as u64)))
                            .ok_or_else(||storage("index raw catch-up estimate overflow"))?;
                        let raw_lease=crate::storage::build_budget::reserve_memory(raw_bytes).map_err(storage)?;
                        let decoded_lease=crate::storage::build_budget::reserve_memory(raw_bytes.checked_mul(32)
                            .ok_or_else(||storage("index decoded tail estimate overflow"))?).map_err(storage)?;
                        (suffix.to_vec(),inner.state.try_clone()?,tail.revision,tail.entries.len(),tail.bytes,raw_lease,decoded_lease)
                    };
                    {
                        let _phase=maintenance::Phase::new("background_replay_and_proof",expected.seq);
                        let mut context=replay::Context {root:&root,config:&config,state:&mut candidate.state,
                            index:&mut candidate.index,ordinals:&mut candidate.ordinals,locations:&mut candidate.locations,
                            snapshot_bytes:&mut candidate.snapshot_bytes,graph_cache:&mut candidate.graph,
                            #[cfg(test)] fail_publication_stage:0};
                        for bytes in raw {context.replay(serde_json::from_slice(&bytes).map_err(storage)?).await?;}
                        intent.apply(&mut expected,&config)?;
                        state_proof::verify(&config,&expected,&candidate.state)?;
                    }
                    let installed_feed=change_feed::Cache::rebuild(&candidate.state,&config.change_feed)?;
                    consumed=entry_count;
                    #[cfg(test)]
                    pause_catchup(&root,timeout).await;
                    let inner=owner.lock().await;
                    let _check_phase=maintenance::Phase::new("candidate_recheck_locked",inner.state.seq);
                    if !inner.healthy || inner.state.sealed_for.is_some(){return Err(RecordError::RecoveryRequired);}
                    if started.elapsed()>=timeout || serde_json::to_vec(&inner.anchor).map_err(storage)?!=base {return Err(RecordError::Busy);}
                    let tail=inner.background.as_ref().filter(|t|t.id==id && !t.overflowed).ok_or(RecordError::Busy)?;
                    if tail.revision!=revision || tail.entries.len()!=entry_count || tail.bytes!=entry_bytes {continue;}
                    break (inner,installed_feed);
                };
                let _install_phase=maintenance::Phase::new("candidate_install_locked",inner.state.seq);
                let certificate = encode_entry(&Entry::Anchor(reference.clone()))?;
                let suffix = &inner.background.as_ref().unwrap().entries;
                // Bound the entire certificate+suffix before any authority publication.
                let bytes = suffix.iter().try_fold(certificate.len() as u64 + 128,|n,b|
                    n.checked_add(b.len() as u64 + 24).ok_or_else(||storage("index tail size overflow")))?;
                if bytes > config.max_journal_bytes { return Err(RecordError::Busy); }
                inner.index.drain_indexes().await?;
                inner.healthy = false;
                let fenced = {
                    let mut output = inner.output.write().map_err(|_|RecordError::RecoveryRequired)?;
                    let old = output.fenced; output.fenced = true; old
                };
                #[cfg(feature="s3-object-store")]
                if let (Some(remote),Some(manifest)) = (&remote,manifest) {
                    let expected_seq=inner.state.seq;
                    let active = inner.remote.as_mut().ok_or(RecordError::RecoveryRequired)?;
                    let published = {let _phase=maintenance::Phase::new("background_remote_cas_locked",expected_seq);
                        remote.publish(active,manifest,&root,expected_seq).await?};
                    if !published {
                        inner.healthy = true;
                        inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced = fenced;
                        return Err(RecordError::Busy);
                    }
                }
                crash_boundary("authority");
                // Borrow fields separately: the complete suffix is durably staged
                // before the new local generation acquires its certificate.
                let journal_result = {
                    let _phase=maintenance::Phase::new("background_journal_install_locked",inner.state.seq);
                    let Inner{journal,background,..} = &mut *inner;
                    journal.checkpoint_with_tail(&certificate,&background.as_ref().unwrap().entries)
                };
                if let Err(error) = journal_result {
                    #[cfg(feature="s3-object-store")]
                    if remote.is_some() { return Err(RecordError::DurablePending(inner.state.seq)); }
                    if matches!(error,CreateError::NotPublished(_)) {
                        inner.healthy = true;
                        inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced = fenced;
                    }
                    return Err(creation_error(error));
                }
                crash_boundary("cache");
                #[cfg(test)]
                if root.join(".test-anchor-post-certificate-gc").exists() {
                    std::fs::write(root.join(".test-anchor-gc-failure"),b"injected retirement failure").map_err(storage)?;
                }
                candidate.swap(&mut inner);
                cursors.immediate(inner.state.seq);
                inner.feed_cache=installed_feed;
                inner.anchor = Some(reference);
                inner.payloads = payloads;
                inner.healthy = true;
                inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced = fenced;
                #[cfg(feature="fault-injection")]
                if intent.trims_feed() {
                    match std::env::var("ANYSEEK_FEED_TRIM_FAULT").ok().as_deref() {
                        Some("after_append_unknown"|"after_durable")=>{
                            inner.healthy=false;
                            inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
                            return Err(RecordError::DurablePending(inner.state.seq));
                        },
                        Some("crash_after_durable")=>std::process::exit(90),
                        _=>{},
                    }
                }
                Ok(cut)
            }.await;
            // Drop construction accounting before rescanning/removing orphans.
            drop(disk); drop(memory);
            let mut inner = owner.lock().await;
            if matches!(&result,Err(RecordError::RecoveryRequired)) {
                inner.healthy=false;
                inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
            }
            if inner.background.as_ref().is_some_and(|t|t.id==id) { inner.background = None; }
            #[cfg(feature="s3-object-store")]
            if let Some(remote) = &remote {
                let current=if let Some(active)=inner.remote.as_ref() {
                    active.authority.lock().await.read_head(&remote.build.partition_id)
                        .ok().flatten()
                } else {None};
                if current.as_ref().is_none_or(|head|head.owner_epoch!=remote.build.owner_epoch || head.committed_seq!=inner.state.seq) {
                    inner.healthy=false;
                    inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
                }
                if let Some(active) = inner.remote.as_mut() {
                    if let Err(error) = active.authority.lock().await.cancel_index_build(&remote.build) {
                        log::warn!("index build pin cleanup pending: {error}");
                    }
                }
            }
            if inner.healthy {
                match inner.reclaim_anchor_objects().await {
                    Ok(())=>inner.maintenance_gc_pending=false,
                    Err(error)=>{
                        if intent.rewrites_payloads() || inner.maintenance_gc_pending {
                            inner.maintenance_gc_pending=true;
                            inner.maintenance_requested=Some("private_cleanup_gc");
                        }
                        log::warn!("index build cleanup pending: {error}");
                    },
                }
            }
            result
          }).catch_unwind().await;
          let result=match outcome {
            Ok(result) if !matches!(&result,Err(RecordError::RecoveryRequired)) => result,
            // This also covers ownership rejection during initial admission,
            // before a Plan/background pin exists and normal cleanup is reached.
            _ => {
                let mut inner = recovery.lock().await;
                inner.healthy = false;
                if let Ok(mut output) = inner.output.write() { output.fenced = true; }
                Err(RecordError::RecoveryRequired)
            }
          };
          if automatic {
              let mut inner=recovery.lock().await;
              let outcome=match &result {Ok(_)=>"installed",Err(RecordError::Busy)=>"busy",Err(RecordError::RecoveryRequired)=>"recovery_required",Err(RecordError::DurablePending(_))=>"durable_pending",Err(RecordError::OutcomeUnknown(_))=>"outcome_unknown",Err(_)=>"failed"};
              log::info!("record_maintenance event=worker_finished outcome={} seq={}",outcome,inner.state.seq);
              inner.maintenance_scheduled=false;
              if result.is_ok(){if !inner.maintenance_gc_pending {inner.maintenance_requested=None;}inner.maintenance_trim=None;inner.maintenance_compacted_bytes=Some(inner.journal.physical_bytes());}
          }
          result
        });
        Ok(IndexJob{handle})
    }
}

#[cfg(any(test,feature="fault-injection"))]
async fn pause(root:&Path,timeout:Duration) {
    let mut gate=None;
    #[cfg(test)]
    if root.join(".test-indexer-pause").exists() {
        gate=Some((root.join(".test-indexer-pause"),root.join(".test-indexer-started")));
    }
    #[cfg(feature="fault-injection")]
    if let Ok(path)=std::env::var("ANYSEEK_INDEXER_PAUSE_FILE") {
        let path=PathBuf::from(path);let started=path.with_extension("started");
        gate=Some((path,started));
    }
    let _=root;
    if let Some((path,started))=gate {
        if path.exists() {
            std::fs::write(started,b"snapshot fixed").expect("write indexer test witness");
            let until=Instant::now()+timeout;
            while path.exists() && Instant::now()<until { tokio::time::sleep(Duration::from_millis(5)).await; }
        }
    }
}

#[cfg(test)]
async fn pause_catchup(root:&Path,timeout:Duration) {
    let gate=root.join(".test-indexer-catchup-pause");
    if gate.exists() {
        std::fs::write(root.join(".test-indexer-catchup-started"),b"candidate proved outside lock").unwrap();
        let until=Instant::now()+timeout;
        while gate.exists() && Instant::now()<until {tokio::time::sleep(Duration::from_millis(5)).await;}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::tests::{config,path,write,source_write,query};
    async fn wait_started(root:&Path) {
        tokio::time::timeout(Duration::from_secs(5),async {
            while !root.join(".test-indexer-started").exists() { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.unwrap();
    }
    #[tokio::test]
    async fn catchup_proof_releases_live_lock_and_rechecks_new_suffix() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(root.join(".test-indexer-catchup-pause"),b"pause").unwrap();
        let job=p.start_index_committed(Duration::from_secs(20)).unwrap();
        tokio::time::timeout(Duration::from_secs(5),async {
            while !root.join(".test-indexer-catchup-started").exists(){tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.unwrap();
        let request=write("concurrent","b","beta",ExpectedVersion::Absent);
        let receipt=tokio::time::timeout(Duration::from_secs(2),p.write(request.clone())).await.unwrap().unwrap();
        assert_eq!(receipt.seq,2);
        assert!(tokio::time::timeout(Duration::from_secs(2),p.get("b")).await.unwrap().unwrap().is_some());
        std::fs::remove_file(root.join(".test-indexer-catchup-pause")).unwrap();
        assert_eq!(job.finish().await.unwrap(),1);
        assert_eq!(p.write(request.clone()).await.unwrap(),receipt);
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.write(request).await.unwrap(),receipt);assert!(p.get("b").await.unwrap().is_some());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn catchup_install_rechecks_same_sequence_feed_trim() {
        struct Fixed(u64);
        impl temporal::Clock for Fixed {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0)}}
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.inner.lock().await.clock=Arc::new(Fixed(1_000));
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(root.join(".test-indexer-catchup-pause"),b"pause").unwrap();
        let job=p.start_index_committed(Duration::from_secs(20)).unwrap();
        tokio::time::timeout(Duration::from_secs(5),async {
            while !root.join(".test-indexer-catchup-started").exists(){tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.unwrap();
        let seq=p.inner.lock().await.state.seq;
        p.inner.lock().await.clock=Arc::new(Fixed(1_001+config().change_feed.retention_ms));
        p.advance_time().await.unwrap();
        {let inner=p.inner.lock().await;assert_eq!(inner.state.seq,seq);assert_eq!(inner.state.feed.floor,seq);assert_eq!(inner.background.as_ref().unwrap().revision,1);}
        std::fs::remove_file(root.join(".test-indexer-catchup-pause")).unwrap();
        assert_eq!(job.finish().await.unwrap(),seq);
        assert_eq!(p.inner.lock().await.state.feed.floor,seq);
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.inner.lock().await.state.feed.floor,seq);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn background_index_cut_keeps_concurrent_writes_deletes_receipts_and_output_barrier() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(source_write("source","s",&[])).await.unwrap();
        p.write(source_write("derived","d",&[("s",1)])).await.unwrap();
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        let output=p.output_permit().unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_index_committed(Duration::from_secs(20)).unwrap();wait_started(&root).await;
        assert!(matches!(p.compact_journal().await,Err(RecordError::Busy)));
        assert!(matches!(p.index_committed(Duration::from_secs(1)).await,Err(RecordError::Busy)));
        let update=write("second","a","alpha revised",ExpectedVersion::Match(1));
        let receipt=p.write(update.clone()).await.unwrap();
        let mut delete=write("delete-source","s","",ExpectedVersion::Match(1));delete.document=None;
        p.write(delete).await.unwrap();
        assert!(p.get("d").await.unwrap().is_none());
        assert!(output.deliver(||()).is_err());
        p.write(write("third","b","alpha beta",ExpectedVersion::Absent)).await.unwrap();
        let before=query(&p,SearchMode::Lexical).await;
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        assert_eq!(job.finish().await.unwrap(),3);
        let after=query(&p,SearchMode::Lexical).await;
        assert_eq!(before.visible_seq,after.visible_seq);
        assert_eq!(before.count_total,after.count_total);
        assert_eq!(p.write(update.clone()).await.unwrap(),receipt);
        assert!(p.get("d").await.unwrap().is_none());
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.write(update).await.unwrap(),receipt);
        assert_eq!(query(&p,SearchMode::Lexical).await.count_total,after.count_total);
        assert!(p.get("d").await.unwrap().is_none());
        // Following incremental foreground flush must match the installed view.
        p.compact_journal().await.unwrap();drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn expired_background_build_leaves_current_view_and_can_retry() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        let request=write("first","a","alpha",ExpectedVersion::Absent);let receipt=p.write(request.clone()).await.unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_index_committed(Duration::from_millis(10)).unwrap();wait_started(&root).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        p.write(write("second","b","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        assert!(matches!(job.finish().await,Err(RecordError::Busy)));
        assert!(p.inner.lock().await.healthy);assert!(p.inner.lock().await.background.is_none());
        assert_eq!(p.write(request).await.unwrap(),receipt);
        assert_eq!(p.index_committed(Duration::from_secs(20)).await.unwrap(),2);
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert!(p.get("b").await.unwrap().is_some());drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn dropped_job_handle_keeps_owner_and_finishes_accepted_work() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_index_committed(Duration::from_secs(20)).unwrap();wait_started(&root).await;
        // Observe actual task completion without retaining its JoinHandle or
        // cancelling accepted work. Clearing background precedes the final
        // recovery-owner drop and therefore cannot prove the file lock is free.
        let completion=job.handle.abort_handle();drop(job);
        p.write(write("second","b","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        tokio::time::timeout(Duration::from_secs(20),async {
            while !completion.is_finished() { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.unwrap();
        assert!(p.inner.lock().await.background.is_none());
        assert_eq!(p.inner.lock().await.state.seq,2);assert!(p.inner.lock().await.anchor.is_some());
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert!(p.get("b").await.unwrap().is_some());drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    #[ignore = "invoked by background index process-crash parent"]
    async fn background_index_crash_worker() {
        let root=PathBuf::from(std::env::var("ANYSEEK_INDEXER_CRASH_ROOT").unwrap());
        let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_index_committed(Duration::from_secs(30)).unwrap();wait_started(&root).await;
        p.write(write("second","b","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        job.finish().await.unwrap();
        panic!("crash boundary was not reached");
    }
    #[tokio::test]
    async fn background_index_process_crashes_keep_acknowledged_concurrent_suffix() {
        for point in ["prepared","authority","cache"] {
            let root=path();
            let status=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","record::indexer::tests::background_index_crash_worker","--ignored","--nocapture"])
                .env("ANYSEEK_INDEXER_CRASH_ROOT",&root).env("ANYSEEK_INDEXER_TEST_CRASH",point)
                .status().unwrap();
            assert_eq!(status.code(),Some(93),"{point}");
            let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
            assert!(p.get("a").await.unwrap().is_some(),"{point}");
            assert!(p.get("b").await.unwrap().is_some(),"{point}");
            let receipt=p.write(write("second","b","alpha",ExpectedVersion::Absent)).await.unwrap();
            assert_eq!(receipt.seq,2);
            assert_eq!(p.index_committed(Duration::from_secs(30)).await.unwrap(),2);
            drop(p);std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn background_tail_budget_abandons_build_without_losing_accepted_write() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_index_committed(Duration::from_secs(20)).unwrap();wait_started(&root).await;
        p.inner.lock().await.background.as_mut().unwrap().max_bytes=1;
        let request=write("second","b","alpha",ExpectedVersion::Absent);
        let receipt=p.write(request.clone()).await.unwrap();
        assert!(p.inner.lock().await.background.as_ref().unwrap().overflowed);
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        assert!(matches!(job.finish().await,Err(RecordError::Busy)));
        assert_eq!(p.write(request.clone()).await.unwrap(),receipt);
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.write(request).await.unwrap(),receipt);assert!(p.get("b").await.unwrap().is_some());
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }

}
