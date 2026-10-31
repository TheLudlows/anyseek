//! Bounded owned index jobs. Snapshot and candidate construction are separated
//! from the short, fenced publication phase. Dropping a caller does not cancel
//! accepted work or release the partition's reader/build protection early.
use super::*;
use futures::FutureExt;
use std::{sync::LazyLock, time::{Duration, Instant}};

pub(super) static WORKERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(2)));
const MAX_DEADLINE: Duration = Duration::from_secs(300);

#[derive(Clone,Copy,Eq,PartialEq)]
enum JobMode { VectorIndex, Automatic, Compact }

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
    source:Option<maintenance::SourceCapture>,
    native:Option<anchor::NativeReuse>,
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
        self.start_owned_index_job(timeout,if automatic {JobMode::Automatic}else{JobMode::VectorIndex})
    }
    pub(super) fn start_compact_job(&self)->Result<IndexJob,RecordError> {
        self.start_owned_index_job(MAX_DEADLINE,JobMode::Compact)
    }
    fn start_owned_index_job(&self,timeout:Duration,mode:JobMode)->Result<IndexJob,RecordError> {
        let automatic=mode==JobMode::Automatic;
        if timeout.is_zero() || timeout > MAX_DEADLINE {
            return Err(RecordError::Rejected("index build deadline must be within 1ns..=300s".into()));
        }
        let worker = WORKERS.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let admission = self.admission.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let writer = self.writer.clone();
        let recovery_writer = writer.clone();
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
                let _writer = writer.lock().await;
                let mut inner = owner.lock().await;
                let _capture_phase=maintenance::Phase::new("background_capture_locked",inner.state.seq);
                if !inner.healthy { return Err(RecordError::RecoveryRequired); }
                if inner.background.is_some() { return Err(RecordError::Busy); }
                inner.index.drain_indexes().await?;
                if mode==JobMode::VectorIndex {inner.advance_time().await?;}
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
                let native=if mode==JobMode::Compact {
                    if let Some(admission)=anchor::NativeReuse::admit(&inner).await? {
                        if inner.anchor.as_ref().is_some_and(|previous|
                            previous.retained_packages().len()+inner.index.pending_seals.len()+1<capacity::segments(&inner.config))
                            && inner.index.stable_tail_path(&root).await.is_some()
                            && inner.index.has_tail_rows().await? {
                            let _phase=maintenance::Phase::new("compact_tail_handoff_locked",inner.state.seq);
                            inner.stage_stable_tail().await?;
                        }
                        Some(anchor::NativeReuse::capture(&inner,admission).await?)
                    }else{None}
                }else{None};
                let source=if mode!=JobMode::VectorIndex {Some(maintenance::SourceCapture::capture(&inner)?)}else{None};
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
                let result = Plan {source,native,intent,root,config:inner.config.clone(),snapshot,prior:inner.payloads.clone(),
                    base,disk,memory,resources,
                    #[cfg(feature="s3-object-store")] remote};
                inner.background = Some(Tail{id,entries:Vec::new(),bytes:0,max_bytes:tail_bytes,overflowed:false,revision:0});
                result
            };
            let Plan{source,native,intent,root,config,mut snapshot,prior,base,disk,memory,resources:_resources,
                #[cfg(feature="s3-object-store")] remote} = plan;
            let cut = snapshot.seq;
            #[cfg(any(test,feature="fault-injection"))]
            pause(&root,timeout).await;
            let result = async {
                if started.elapsed() >= timeout { return Err(RecordError::Busy); }
                let source=if let Some(source)=source {
                    let source=source.collect(&owner,id,started+timeout).await?;
                    source.verify(&root,&config,&snapshot).map_err(|error| {
                        if mode==JobMode::Compact && !matches!(error,RecordError::Busy) {
                            log::warn!("compact source authentication failed: {error}");
                            RecordError::RecoveryRequired
                        }else{error}
                    })?;
                    #[cfg(feature="s3-object-store")]
                    if let Some(remote)=&remote {
                        source.verify_remote(remote,&root,&config).await.map_err(|error| {
                            if matches!(error,RecordError::Busy){error}else{
                                log::warn!("background remote source authentication failed: {error}");
                                RecordError::RecoveryRequired
                            }
                        })?;
                    }
                    Some(source)
                }else{None};
                let source=if mode==JobMode::Compact {source}else{None};
                intent.apply(&mut snapshot,&config)?;
                let reference = if mode==JobMode::Compact {
                    let source=source.as_ref().ok_or(RecordError::RecoveryRequired)?;
                    anchor::Reference::prepare_captured(&root,&config,&snapshot,prior,source.anchor(),source.frames(),native.as_ref()).await?
                }else{anchor::Reference::prepare(&root,&config,&snapshot,prior,None,None,true,None).await?};
                drop(source);
                drop(native);
                #[cfg(test)]
                if mode==JobMode::Compact && root.join(".test-anchor-reject-prepared").exists() {
                    return Err(storage("injected candidate rejection before certificate"));
                }
                // The pinned cut already owns complete history. Verify the
                // authenticated package against it and transfer that same owner
                // into the candidate instead of decoding a duplicate State.
                let loaded = reference.load_captured(&root, config.clone(), snapshot).await?;
                #[cfg(test)]
                if mode==JobMode::Compact && root.join(".test-anchor-reject-loaded").exists() {
                    return Err(storage("injected loaded candidate rejection before certificate"));
                }
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
                let (_writer,mut inner)=loop {
                    rounds+=1;
                    if rounds>8 || started.elapsed()>=timeout {return Err(RecordError::Busy);}
                    let (raw,mut expected,revision,entry_count,entry_bytes,_raw,_decoded)={
                        let inner=owner.lock().await;
                        let _capture_phase=maintenance::Phase::new("catchup_capture_locked",inner.state.seq);
                        if !inner.healthy {return Err(RecordError::RecoveryRequired);}
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
                    history::validate_state(&candidate.state)?;
                    consumed=entry_count;
                    #[cfg(test)]
                    pause_catchup(&root,timeout).await;
                    let writer_guard=writer.lock().await;
                    let inner=owner.lock().await;
                    let _check_phase=maintenance::Phase::new("candidate_recheck_locked",inner.state.seq);
                    if !inner.healthy {return Err(RecordError::RecoveryRequired);}
                    if started.elapsed()>=timeout || serde_json::to_vec(&inner.anchor).map_err(storage)?!=base {return Err(RecordError::Busy);}
                    let tail=inner.background.as_ref().filter(|t|t.id==id && !t.overflowed).ok_or(RecordError::Busy)?;
                    if tail.revision!=revision || tail.entries.len()!=entry_count || tail.bytes!=entry_bytes {continue;}
                    break (writer_guard,inner);
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
                    #[cfg(test)] pause_journal_install(&root).await;
                    let permit = match crate::resources::Scope::current() {
                        Some(scope) => scope.journal_io().await.map(Some)
                            .map_err(|error| CreateError::NotPublished(std::io::Error::other(error))),
                        None => Ok(None),
                    };
                    let Inner{journal,background,..} = &mut *inner;
                    let mut install = || journal.checkpoint_with_tail(&certificate,&background.as_ref().unwrap().entries);
                    match permit {
                        Ok(Some(permit)) => permit.run(install),
                        Ok(None) => install(),
                        Err(error) => Err(error),
                    }
                };
                #[cfg(test)]
                let journal_result=if mode==JobMode::Compact && root.join(".test-checkpoint-before-certificate").exists() {
                    journal_result.map_err(|error|match error {
                        CreateError::NotPublished(error)=>CreateError::PublicationUnknown(error),error=>error,
                    })
                }else{journal_result};
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
                inner.anchor = Some(reference);
                inner.payloads = payloads;
                inner.healthy = true;
                inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced = fenced;
                #[cfg(feature="fault-injection")]
                if intent.trims_history() {
                    match std::env::var("ANYSEEK_HISTORY_TRIM_FAULT").ok().as_deref() {
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
            let _writer = writer.lock().await;
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
                if mode==JobMode::Compact && result.is_ok() && inner.healthy {
                    let _phase=maintenance::Phase::new("compact_remote_gc_locked",inner.state.seq);
                    if let Some(active)=inner.remote.as_mut() {
                        if let Err(error)=active.collect_objects().await {
                            log::warn!("remote object collection pending: {error}");
                        }
                    }
                }
            }
            if inner.healthy {
                match inner.reclaim_anchor_objects().await {
                    Ok(())=>{},
                    Err(error)=>{
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
                let _writer = recovery_writer.lock().await;
                let mut inner = recovery.lock().await;
                inner.healthy = false;
                if let Ok(mut output) = inner.output.write() { output.fenced = true; }
                Err(RecordError::RecoveryRequired)
            }
          };
          if automatic {
              let _writer = recovery_writer.lock().await;
              let mut inner=recovery.lock().await;
              let outcome=match &result {Ok(_)=>"installed",Err(RecordError::Busy)=>"busy",Err(RecordError::RecoveryRequired)=>"recovery_required",Err(RecordError::DurablePending(_))=>"durable_pending",Err(RecordError::OutcomeUnknown(_))=>"outcome_unknown",Err(_)=>"failed"};
              log::info!("record_maintenance event=worker_finished outcome={} seq={}",outcome,inner.state.seq);
              inner.maintenance_scheduled=false;
              if result.is_ok(){inner.maintenance_requested=None;inner.maintenance_trim=None;inner.maintenance_compacted_bytes=Some(inner.journal.physical_bytes());}
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
pub(super) async fn pause_journal_install(root: &Path) {
    let gate = root.join(".test-journal-io-pause");
    if gate.exists() {
        std::fs::write(root.join(".test-journal-io-started"), b"ready").unwrap();
        while gate.exists() { tokio::time::sleep(Duration::from_millis(1)).await; }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::tests::{config,path,write,source_write,query};
    #[tokio::test(flavor = "current_thread")]
    async fn compact_native_probe_releases_io_before_waiting_for_shard_statistics() {
        use std::{future::Future, io::Write, task::Poll};
        let temp = tempfile::tempdir().unwrap(); let root = temp.path().join("records");
        let scope = Arc::new(crate::resources::Governor::default())
            .admit("native-probe".into(), crate::resources::Lane::Background, 0).unwrap();
        // Keep setup workers outside the probe governor: the deliberate first
        // poll must stop at the held shard lock, not an unrelated setup I/O.
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(write("first", "a", "alpha", ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let inner = p.inner.lock().await;
        let shard = inner.index.test_index().read().await.segments[0].clone();
        let guard = shard.write().await;
        {
        let admission = scope.clone().run(anchor::NativeReuse::admit(&inner));
        tokio::pin!(admission);
        // Poll exactly through the I/O probe to the contested shard lock; no
        // timing assumption or background scheduling is needed for this edge.
        assert!(std::future::poll_fn(|cx| Poll::Ready(admission.as_mut().poll(cx).is_pending())).await);
        let permit = tokio::time::timeout(Duration::from_secs(5), scope.journal_io()).await.unwrap().unwrap();
        permit.run(|| scope.clone().run_sync(|| {
            let mut file = crate::storage::build_budget::File::create(temp.path().join("guard-owner-io")).unwrap();
            file.write_all(b"guard owner completes its I/O").unwrap();
        }));
        drop(guard);
        assert!(admission.await.unwrap().is_some());
        }
        // Saturation already present at entry still refuses before any handoff.
        let held = crate::resources::hold_io_for_test(&scope).await;
        assert!(matches!(scope.clone().run(anchor::NativeReuse::admit(&inner)).await, Err(RecordError::Busy)));
        assert!(inner.healthy);
        drop(held); drop(inner);
        scope.run(p.write(write("second", "b", "alpha", ExpectedVersion::Absent))).await.unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn checkpoint_io_loan_waits_after_native_work_and_preserves_suffix() {
        for background in [false, true] {
            let temp = tempfile::tempdir().unwrap(); let root = temp.path().join("records");
            let scope = Arc::new(crate::resources::Governor::default())
                .admit("checkpoint-io".into(), crate::resources::Lane::Background, 0).unwrap();
            let p = scope.clone().run(RecordPartition::create(&root, config())).await.unwrap();
            let first = write("first", "a", "alpha", ExpectedVersion::Absent);
            let receipt = scope.clone().run(p.write(first.clone())).await.unwrap();
            std::fs::write(root.join(".test-journal-io-pause"), b"pause").unwrap();
            if background { std::fs::write(root.join(".test-indexer-pause"), b"pause").unwrap(); }
            let owner = p.clone();
            let task = tokio::spawn(scope.clone().run(async move {
                if background { owner.compact_journal().await }
                else { owner.inner.lock().await.publish_anchor().await }
            }));
            let mut second = None;
            if background {
                wait_started(&root).await;
                let request = write("suffix", "b", "alpha", ExpectedVersion::Absent);
                let receipt = scope.clone().run(p.write(request.clone())).await.unwrap();
                second = Some((request, receipt));
                std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while !root.join(".test-journal-io-started").exists() { tokio::time::sleep(Duration::from_millis(1)).await; }
            }).await.unwrap();
            let held = crate::resources::hold_io_for_test(&scope).await;
            std::fs::remove_file(root.join(".test-journal-io-pause")).unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(!task.is_finished(), "generation must wait before touching its first file");
            drop(held);
            tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap().unwrap();
            assert_eq!(scope.clone().run(p.write(first.clone())).await.unwrap(), receipt);
            drop(p);
            let p = scope.clone().run(RecordPartition::open(&root, config().max_journal_bytes)).await.unwrap();
            assert_eq!(scope.clone().run(p.write(first)).await.unwrap(), receipt);
            if let Some((request, receipt)) = second {
                assert_eq!(scope.clone().run(p.write(request)).await.unwrap(), receipt);
            }
            assert_eq!(query(&p, SearchMode::Lexical).await.hits.len(), if background { 2 } else { 1 });
        }
    }
    async fn wait_started(root:&Path) {
        tokio::time::timeout(Duration::from_secs(5),async {
            while !root.join(".test-indexer-started").exists() { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.unwrap();
    }
    #[tokio::test]
    async fn compact_native_capture_isolated_from_same_version_patch_and_concurrent_delete() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        let mut initial=write("a","a","alpha",ExpectedVersion::Absent);
        initial.document.as_mut().unwrap().shift_remove("vector");
        p.write(initial).await.unwrap();p.write(write("b","b","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let selected=p.inner.lock().await.anchor.as_ref().unwrap().retained_packages()[0].id.clone();
        p.write(write("c","c","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_compact_job().unwrap();wait_started(&root).await;
        let mut patch=write("patch","a","",ExpectedVersion::Match(1));patch.document=None;
        patch.vector_patch=Some(serde_json::from_value(serde_json::json!({"vector":[1.0,0.0]})).unwrap());
        let receipt=p.write(patch.clone()).await.unwrap();assert_eq!(receipt.version,1);
        let mut delete=write("delete","b","",ExpectedVersion::Match(1));delete.document=None;p.write(delete).await.unwrap();
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();job.finish().await.unwrap();
        assert!(p.inner.lock().await.anchor.as_ref().unwrap().retained_packages().iter().any(|package|package.id==selected));
        for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All}] {
            let result=query(&p,mode).await;assert_eq!(result.count_total,2);assert!(result.hits.iter().all(|hit|hit.record.record_id!="b"));
        }
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.write(patch).await.unwrap(),receipt);assert!(p.get("b").await.unwrap().is_none());
        assert_eq!(query(&p,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All}).await.count_total,2);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn compact_native_capture_budget_refuses_before_tail_handoff() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("a","a","alpha",ExpectedVersion::Absent)).await.unwrap();p.compact_journal().await.unwrap();
        p.write(write("b","b","alpha",ExpectedVersion::Absent)).await.unwrap();
        let tail=p.inner.lock().await.index.stable_tail_path(&root).await.unwrap();
        std::fs::write(root.join(".test-compact-native-capture-budget"),b"reject admission").unwrap();
        assert!(matches!(p.compact_journal().await,Err(RecordError::Busy)));
        {let inner=p.inner.lock().await;assert!(inner.healthy);assert!(inner.background.is_none());assert!(inner.index.pending_seals.is_empty());assert_eq!(inner.index.stable_tail_path(&root).await.unwrap(),tail);}
        p.write(write("c","c","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::remove_file(root.join(".test-compact-native-capture-budget")).unwrap();p.compact_journal().await.unwrap();
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();assert_eq!(query(&p,SearchMode::Lexical).await.count_total,3);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn compact_native_reuse_rejects_corrupt_selected_package() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("a","a","alpha",ExpectedVersion::Absent)).await.unwrap();p.compact_journal().await.unwrap();
        let before=p.inner.lock().await.anchor.clone().unwrap();
        let path=before.retained_packages()[0].path(&root).unwrap().join("schema.json");
        let original=std::fs::read(&path).unwrap();std::fs::write(&path,b"corrupt").unwrap();
        assert!(matches!(p.compact_journal().await,Err(RecordError::RecoveryRequired)));
        {let inner=p.inner.lock().await;assert!(!inner.healthy);assert_eq!(inner.anchor.as_ref().unwrap().identity(),before.identity());}
        drop(p);assert!(RecordPartition::open(&root,config().max_journal_bytes).await.is_err());
        std::fs::write(&path,original).unwrap();let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert!(p.get("a").await.unwrap().is_some());drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn compact_owned_job_preserves_time_receipts_and_concurrent_same_seq_trim() {
        struct Fixed(u64);
        impl temporal::Clock for Fixed {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0)}}
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.inner.lock().await.clock=Arc::new(Fixed(1_000));
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.inner.lock().await.clock=Arc::new(Fixed(1_001+config().history.retention_ms));
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_compact_job().unwrap();wait_started(&root).await;
        {let inner=p.inner.lock().await;assert_eq!(inner.state.time_watermark_ms,1_000);assert_eq!(inner.state.history.floor,0);}
        let request=write("second","b","beta",ExpectedVersion::Absent);
        let receipt=tokio::time::timeout(Duration::from_secs(2),p.write(request.clone())).await.unwrap().unwrap();
        {let inner=p.inner.lock().await;assert_eq!(inner.state.history.floor,1);assert_eq!(inner.background.as_ref().unwrap().revision,2);}
        let completion=job.handle.abort_handle();drop(job);drop(p);
        assert!(RecordPartition::open(&root,config().max_journal_bytes).await.is_err());
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        tokio::time::timeout(Duration::from_secs(30),async {
            while !completion.is_finished(){tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.unwrap();
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        {let inner=p.inner.lock().await;assert_eq!(inner.state.operations.len(),2);assert_eq!(inner.state.history.floor,1);assert!(inner.anchor.is_some());}
        assert_eq!(p.write(request).await.unwrap(),receipt);drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn compact_owned_job_faults_preserve_source_and_recover_unknown_publication() {
        for gate in [".test-anchor-reject-prepared",".test-anchor-reject-loaded",".test-checkpoint-before-certificate",".test-checkpoint-after-certificate"] {
            let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
            let request=write("first","a","alpha",ExpectedVersion::Absent);let receipt=p.write(request.clone()).await.unwrap();
            std::fs::write(root.join(gate),b"fault").unwrap();
            let result=p.start_compact_job().unwrap().finish().await;
            let unknown=gate.starts_with(".test-checkpoint-");
            assert!(result.is_err());assert_eq!(matches!(result,Err(RecordError::OutcomeUnknown(_))),unknown,"{gate}: {result:?}");
            {let inner=p.inner.lock().await;assert_eq!(inner.healthy,!unknown);assert!(inner.anchor.is_none());assert!(inner.background.is_none());}
            std::fs::remove_file(root.join(gate)).unwrap();drop(p);
            let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
            assert_eq!(p.write(request).await.unwrap(),receipt);assert!(p.get("a").await.unwrap().is_some());
            p.start_compact_job().unwrap().finish().await.unwrap();drop(p);std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[tokio::test]
    async fn compact_owned_job_deadline_refuses_before_authority_without_fencing() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        let request=write("first","a","alpha",ExpectedVersion::Absent);let receipt=p.write(request.clone()).await.unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_owned_index_job(Duration::from_millis(10),JobMode::Compact).unwrap();
        assert!(matches!(job.finish().await,Err(RecordError::Busy)));
        {let inner=p.inner.lock().await;assert!(inner.healthy);assert!(inner.anchor.is_none());assert!(inner.background.is_none());}
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        assert_eq!(p.write(request).await.unwrap(),receipt);
        p.compact_journal().await.unwrap();drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn chunked_source_proof_keeps_append_same_sequence_trim_and_cancelled_owner() {
        struct Fixed(u64);
        impl temporal::Clock for Fixed {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0)}}
        let root=path();let mut settings=config();settings.max_record_bytes=256*1024;settings.max_journal_bytes=8*1024*1024;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        p.inner.lock().await.clock=Arc::new(Fixed(1_000));
        p.write(write("first","a",&"alpha ".repeat(15000),ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(root.join(".test-source-chunk-pause"),b"pause").unwrap();
        let job=p.start_index_job(Duration::from_secs(30),true).unwrap();
        tokio::time::timeout(Duration::from_secs(10),async {
            while !root.join(".test-source-chunk-started").exists(){tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.unwrap();
        let request=write("second","b","beta",ExpectedVersion::Absent);
        let receipt=tokio::time::timeout(Duration::from_secs(2),p.write(request.clone())).await.unwrap().unwrap();
        p.inner.lock().await.clock=Arc::new(Fixed(1_001+settings.history.retention_ms));
        p.advance_time().await.unwrap();
        {let inner=p.inner.lock().await;assert_eq!(inner.state.seq,2);assert_eq!(inner.state.history.floor,2);assert_eq!(inner.background.as_ref().unwrap().revision,2);}
        let completion=job.handle.abort_handle();drop(job);drop(p);
        assert!(RecordPartition::open(&root,settings.max_journal_bytes).await.is_err());
        std::fs::remove_file(root.join(".test-source-chunk-pause")).unwrap();
        tokio::time::timeout(Duration::from_secs(30),async {
            while !completion.is_finished(){tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.unwrap();
        let p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        {let inner=p.inner.lock().await;assert!(inner.anchor.is_some());assert_eq!(inner.state.seq,2);assert_eq!(inner.state.history.floor,2);}
        assert_eq!(p.write(request).await.unwrap(),receipt);assert!(p.get("a").await.unwrap().is_some());
        drop(p);std::fs::remove_dir_all(root).unwrap();
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
    async fn catchup_install_rechecks_same_sequence_history_trim() {
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
        p.inner.lock().await.clock=Arc::new(Fixed(1_001+config().history.retention_ms));
        p.advance_time().await.unwrap();
        {let inner=p.inner.lock().await;assert_eq!(inner.state.seq,seq);assert_eq!(inner.state.history.floor,seq);assert_eq!(inner.background.as_ref().unwrap().revision,1);}
        std::fs::remove_file(root.join(".test-indexer-catchup-pause")).unwrap();
        assert_eq!(job.finish().await.unwrap(),seq);
        assert_eq!(p.inner.lock().await.state.history.floor,seq);
        drop(p);let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.inner.lock().await.state.history.floor,seq);
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
