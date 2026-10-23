//! Bounded owned index jobs. Snapshot and candidate construction are separated
//! from the short, fenced publication phase. Dropping a caller does not cancel
//! accepted work or release the partition's reader/build protection early.
use super::*;
use futures::FutureExt;
use std::{sync::LazyLock, time::{Duration, Instant}};

static WORKERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(2)));
const MAX_DEADLINE: Duration = Duration::from_secs(300);

fn crash_boundary(point:&str) {
    #[cfg(test)]
    if std::env::var("ANYSEEK_INDEXER_TEST_CRASH").as_deref()==Ok(point) { std::process::exit(93); }
    #[cfg(feature="fault-injection")]
    if std::env::var("ANYSEEK_INDEXER_FAULT").as_deref()==Ok(point) { std::process::exit(93); }
    let _=point;
}

pub(super) struct Tail {
    id: u128,
    pub entries: Vec<Vec<u8>>,
    pub bytes:u64,
    pub max_bytes:u64,
    pub overflowed:bool,
}
struct Plan {
    root: PathBuf, config:RecordConfig, snapshot:State,
    prior:crate::storage::payload_pool::Closure, base:Vec<u8>,
    disk:crate::storage::build_budget::DiskScope,
    memory:(crate::storage::build_budget::MemoryLease,crate::storage::build_budget::MemoryLease),
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
    handle: tokio::task::JoinHandle<Result<u64,RecordError>>,
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
        if timeout.is_zero() || timeout > MAX_DEADLINE {
            return Err(RecordError::Rejected("index build deadline must be within 1ns..=300s".into()));
        }
        let worker = WORKERS.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let admission = self.admission.clone().try_acquire_owned().map_err(|_| RecordError::Busy)?;
        let owner = self.inner.clone();
        let recovery = owner.clone();
        let handle = crate::INDEX_RUNTIME.spawn(async move {
          let outcome = std::panic::AssertUnwindSafe(async move {
            let _worker = worker;
            let _admission = admission;
            let started = Instant::now();
            let id = rand::random::<u128>();
            let plan = {
                let mut inner = owner.lock().await;
                if !inner.healthy || inner.state.sealed_for.is_some() { return Err(RecordError::RecoveryRequired); }
                if inner.background.is_some() { return Err(RecordError::Busy); }
                inner.advance_time().await?;
                inner.reclaim_anchor_objects().await?;
                let estimate = (inner.snapshot_bytes as u64).checked_mul(4)
                    .ok_or_else(||storage("index snapshot estimate overflow"))?;
                let tail_bytes=inner.config.max_journal_bytes.min(crate::storage::build_budget::EXPLICIT_BUFFERS);
                let memory = (crate::storage::build_budget::reserve_memory(estimate).map_err(storage)?,
                    crate::storage::build_budget::reserve_memory(tail_bytes).map_err(storage)?);
                let root = inner.journal.root().to_path_buf();
                let limit = inner.config.max_journal_bytes.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024))
                    .ok_or_else(||storage("index disk budget overflow"))?;
                let disk = crate::storage::build_budget::DiskScope::begin(&root,limit).map_err(storage)?;
                #[cfg(feature="s3-object-store")]
                let remote = match inner.remote.as_mut() {
                    Some(remote) => Some(remote.begin_background_build(timeout).await?),
                    None => None,
                };
                #[cfg(feature="s3-object-store")]
                if remote.as_ref().is_some_and(|r|r.build.cut_seq!=inner.state.seq) {
                    return Err(RecordError::RecoveryRequired);
                }
                let result = Plan {root,config:inner.config.clone(),snapshot:inner.state.clone(),prior:inner.payloads.clone(),
                    base:serde_json::to_vec(&inner.anchor).map_err(storage)?,disk,memory,
                    #[cfg(feature="s3-object-store")] remote};
                inner.background = Some(Tail{id,entries:Vec::new(),bytes:0,max_bytes:tail_bytes,overflowed:false});
                result
            };
            let Plan{root,config,snapshot,prior,base,disk,memory,
                #[cfg(feature="s3-object-store")] remote} = plan;
            let cut = snapshot.seq;
            #[cfg(any(test,feature="fault-injection"))]
            pause(&root,timeout).await;
            let result = async {
                if started.elapsed() >= timeout { return Err(RecordError::Busy); }
                let reference = anchor::Reference::prepare(&root,&config,&snapshot,prior,None,None,true).await?;
                let loaded = reference.load(&root,config.max_journal_bytes).await?;
                let payloads = loaded.payloads;
                let mut index = view::RetrievalView::single(loaded.index);
                let mut ordinals = loaded.ordinals;
                let mut locations = loaded.locations;
                let cache = root.join(format!("cache-{:032x}",rand::random::<u128>()));
                let (tail,_) = build_at(&root,&cache,&config,&State::default()).await?;
                index.rotate_tail(tail,&mut ordinals,&mut locations,view::RotationBudget{
                    max_segments:64,max_address_slots:config.max_records.checked_add(config.max_operations)
                        .and_then(|n|n.checked_mul(64)).ok_or_else(||storage("index address budget overflow"))?,
                }).await?;
                let mut candidate = Candidate {
                    graph:graph::Cache::rebuild(&loaded.state)?,
                    snapshot_bytes:encode_entry(&Entry::Checkpoint{config:config.clone(),state:loaded.state.clone()})?.len(),
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
                let mut inner = owner.lock().await;
                if !inner.healthy || inner.state.sealed_for.is_some() { return Err(RecordError::RecoveryRequired); }
                if started.elapsed() >= timeout || inner.background.as_ref().is_none_or(|t|t.id!=id || t.overflowed)
                    || serde_json::to_vec(&inner.anchor).map_err(storage)? != base { return Err(RecordError::Busy); }
                let entries = &inner.background.as_ref().unwrap().entries;
                let entries: Vec<Entry> = entries.iter().map(|b|serde_json::from_slice(b).map_err(storage)).collect::<Result<_,_>>()?;
                // Candidate catch-up cannot publish or append WAL. Restore the
                // current view even when replay fails; output is not yet fenced.
                // A panic during speculative replay must leave the partition
                // fenced before dropping its guard, even before the catch handler
                // can reacquire it on another runtime worker.
                inner.healthy=false;
                let replay_fenced={
                    let mut output=inner.output.write().map_err(|_|RecordError::RecoveryRequired)?;
                    let previous=output.fenced;output.fenced=true;previous
                };
                candidate.swap(&mut inner);
                let replay = async {
                    for entry in entries { inner.replay_anchor_tail(entry).await?; }
                    if serde_json::to_vec(&inner.state).map_err(storage)? != serde_json::to_vec(&candidate.state).map_err(storage)? {
                        return Err(storage("index catch-up did not reproduce current authoritative state"));
                    }
                    Ok(())
                }.await;
                candidate.swap(&mut inner);
                inner.healthy=true;
                inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=replay_fenced;
                replay?;
                if started.elapsed() >= timeout { return Err(RecordError::Busy); }
                let certificate = encode_entry(&Entry::Anchor(reference.clone()))?;
                let suffix = &inner.background.as_ref().unwrap().entries;
                // Bound the entire certificate+suffix before any authority publication.
                let bytes = suffix.iter().try_fold(certificate.len() as u64 + 128,|n,b|
                    n.checked_add(b.len() as u64 + 24).ok_or_else(||storage("index tail size overflow")))?;
                if bytes > config.max_journal_bytes { return Err(RecordError::Busy); }
                inner.healthy = false;
                let fenced = {
                    let mut output = inner.output.write().map_err(|_|RecordError::RecoveryRequired)?;
                    let old = output.fenced; output.fenced = true; old
                };
                #[cfg(feature="s3-object-store")]
                if let (Some(remote),Some(manifest)) = (&remote,manifest) {
                    let expected_seq=inner.state.seq;
                    let active = inner.remote.as_mut().ok_or(RecordError::RecoveryRequired)?;
                    let published = remote.publish(active,manifest,&root,expected_seq).await?;
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
                candidate.swap(&mut inner);
                inner.anchor = Some(reference);
                inner.payloads = payloads;
                inner.healthy = true;
                inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced = fenced;
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
                if let Err(error) = inner.reclaim_anchor_objects().await { log::warn!("index build cleanup pending: {error}"); }
            }
            result
          }).catch_unwind().await;
          match outcome {
            Ok(result) => result,
            Err(_) => {
                let mut inner = recovery.lock().await;
                inner.healthy = false;
                if let Ok(mut output) = inner.output.write() { output.fenced = true; }
                Err(RecordError::RecoveryRequired)
            }
          }
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
mod tests {
    use super::*;
    use super::super::tests::{config,path,write,source_write,query};
    async fn wait_started(root:&Path) {
        tokio::time::timeout(Duration::from_secs(5),async {
            while !root.join(".test-indexer-started").exists() { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.unwrap();
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
        let job=p.start_index_committed(Duration::from_secs(20)).unwrap();wait_started(&root).await;drop(job);
        p.write(write("second","b","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        tokio::time::timeout(Duration::from_secs(20),async {
            while p.inner.lock().await.background.is_some() { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.unwrap();
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
