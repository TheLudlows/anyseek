//! Disposable, bounded two-component compaction. The Record WAL/certificate is
//! untouched: a failed candidate can be discarded and recovery rebuilds caches.
use super::*;
use futures::FutureExt;
use std::time::{Duration,Instant};

pub(super) const CAUSE:&str="derived_cache_merge";
pub(super) const MAX_ROWS:usize=2*capacity::PACKAGE_RECORDS;
const MAX_INPUT_BYTES:u64=32*1024*1024;
const MAX_BUILD_BYTES:u64=256*1024*1024;

pub(super) struct Snapshot {
    seq:u64,
    records:Vec<state_memory::Shared<Record>>,
    _memory:crate::storage::build_budget::MemoryLease,
}
impl Snapshot {
    pub(super) fn capture(state:&State,_config:&RecordConfig,sources:&view::MergeSources)->Result<Self,RecordError> {
        let mut input=0u64;
        for key in &sources.keys {
            let record=state.records.shared(key).ok_or_else(||storage("cache merge source record missing"))?;
            if record.document.is_none(){return Err(storage("cache merge source body absent"));}
            input=input.checked_add(record.wire_bytes() as u64)
                .filter(|n|*n<=MAX_INPUT_BYTES).ok_or(RecordError::Busy)?;
        }
        let memory=crate::storage::build_budget::reserve_memory((sources.keys.len() as u64).checked_mul(128)
            .ok_or_else(||storage("cache snapshot inventory overflow"))?).map_err(storage)?;
        let records=sources.keys.iter().map(|key|state.records.shared(key)
            .ok_or_else(||storage("cache merge source owner missing"))).collect::<Result<Vec<_>,_>>()?;
        Ok(Self{seq:state.seq,records,_memory:memory})
    }
}

pub(super) fn address_limit(config:&RecordConfig)->Result<usize,RecordError> {
    config.max_records.checked_add(config.max_operations).and_then(|n|n.checked_mul(64))
        .ok_or_else(||storage("cache merge address budget overflow"))
}
pub(super) async fn build_candidate(root:&Path,config:&RecordConfig,snapshot:&Snapshot)
    ->Result<IndexArc,RecordError> {
    let build_limit=MAX_BUILD_BYTES;
    #[cfg(test)]
    let build_limit=std::fs::read_to_string(root.join(".test-cache-merge-build-budget")).ok()
        .and_then(|value|value.parse::<u64>().ok()).unwrap_or(build_limit);
    let mut estimate=0u64;
    for record in &snapshot.records {
        estimate=estimate.checked_add(build_budget::document_estimate(record.document.as_ref().unwrap(),&config.schema)?)
            .filter(|n|*n<=build_limit).ok_or(RecordError::Busy)?;
    }
    let path=root.join(format!("cache-{:032x}",rand::random::<u128>()));
    let (candidate,_)=build_records_at(root,&path,config,snapshot.seq,snapshot.records.iter().map(|record|&**record).collect()).await?;
    candidate.commit().await.map_err(storage)?;
    candidate.read().await.record_lexical[0].seal().await?;
    Ok(candidate)
}

/// Recovery cannot make an optional compaction heuristic an admission rule for
/// already committed records. Rebuild bounded private components from canonical
/// state when no optional pair fits; no checkpoint/certificate is published.
pub(super) async fn rebuild_recovery_view(root:&Path,config:&RecordConfig,state:&State)
    ->Result<(view::RetrievalView,Vec<String>,BTreeMap<String,usize>),RecordError> {
    let inventory_bytes=state.records.keys().try_fold(0u64,|bytes,key|bytes.checked_add(512+key.len() as u64*8)
        .ok_or_else(||storage("recovery cache inventory overflow")))?;
    let inventory=crate::storage::build_budget::reserve_memory(inventory_bytes).map_err(storage)?;
    let inventory_scope=crate::resources::Scope::current().map(|scope|scope.bytes(inventory_bytes)).transpose().map_err(storage)?;
    let visible=evidence::visibility(&state.records)?;
    let records=state.records.values().filter(|record|visible.get(&record.record_id)==Some(&true)
        && !(state.evidence_quarantined&&temporal::protected(record))).collect::<Vec<_>>();
    let mut indexes=Vec::new();
    for chunk in records.chunks(capacity::PACKAGE_RECORDS) {
        if indexes.len()>=capacity::segments(config){return Err(storage("recovery component population exceeded"));}
        let path=root.join(format!("cache-{:032x}",rand::random::<u128>()));
        let (index,_)=build_records_at(root,&path,config,state.seq,chunk.to_vec()).await?;
        index.commit().await.map_err(storage)?;
        index.read().await.record_lexical[0].seal().await?;
        indexes.push(index);
    }
    if indexes.is_empty() {
        let path=root.join(format!("cache-{:032x}",rand::random::<u128>()));
        let (index,_)=build_records_at(root,&path,config,state.seq,Vec::new()).await?;
        index.commit().await.map_err(storage)?;
        index.read().await.record_lexical[0].seal().await?;indexes.push(index);
    }
    let count=indexes.len();let mut shards=Vec::new();let mut natives=Vec::new();
    let mut ordinals=Vec::new();let mut locations=BTreeMap::new();
    for (slot,index) in indexes.iter().enumerate() {
        let index=index.read().await;
        let component=index.shard_vec[0].clone();
        {let mut shard=component.write().await;shard.meta.id=slot as u64;
            let active=shard.active.as_mut().ok_or_else(||storage("recovery statistics missing"))?;
            active.retired.get_or_insert_with(Default::default);
            for (local,key) in active.keys.iter().enumerate() {
                let address=local.checked_mul(count).and_then(|n|n.checked_add(slot)).ok_or_else(||storage("recovery address overflow"))?;
                if ordinals.len()<=address{ordinals.resize(address+1,String::new());}
                ordinals[address]=key.clone();locations.insert(key.clone(),address);
            }
        }
        shards.push(component);natives.push(index.record_lexical[0].clone());
    }
    let index=indexes[0].clone();
    {let mut root=index.write().await;root.shard_vec=shards;root.record_lexical=natives;root.shard_number=count;}
    let mut view=view::RetrievalView::single(index);view.detached_cache=true;
    view.retain_address_budget(inventory,inventory_scope);
    Ok((view,ordinals,locations))
}

pub(super) async fn reclaim_recovery_caches(root:&Path,index:&view::RetrievalView)->Result<(),RecordError> {
    let keep=index.storage_paths().await;
    for entry in std::fs::read_dir(root).map_err(storage)? {
        let entry=entry.map_err(storage)?;let path=entry.path();
        let name=entry.file_name();let Some(name)=name.to_str()else{continue;};
        let Some(id)=name.strip_prefix("cache-")else{continue;};
        if id.len()!=32 || !id.bytes().all(|b|b.is_ascii_hexdigit()) || keep.contains(&path){continue;}
        package_pins::retire_if_unpinned(&path,|| {
            if !entry.file_type().map_err(storage)?.is_dir(){return Err(storage("derived recovery cache is not a directory"));}
            std::fs::remove_dir_all(&path).map_err(storage)
        })?;
    }
    Ok(())
}

impl RecordPartition {
    pub(super) fn start_cache_merge_job(&self,timeout:Duration)->Result<IndexJob,RecordError> {
        if timeout.is_zero() || timeout>Duration::from_secs(300) {return Err(RecordError::Rejected("invalid cache merge deadline".into()));}
        let worker=indexer::WORKERS.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let admission=self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let writer=self.writer.clone();
        let owner=self.inner.clone();
        let id=rand::random::<u128>();
        let handle=crate::resources::spawn_background(async move {
            let _worker=worker;let _admission=admission;
            let started=Instant::now();
            let outcome=std::panic::AssertUnwindSafe(async {
                let (root,config,snapshot,sources,base,selected,_disk,_resources)={
                    let _writer=writer.lock().await;
                    let mut inner=owner.lock().await;
                    let _phase=maintenance::Phase::new("cache_merge_capture_locked",inner.state.seq);
                    if !inner.healthy {return Err(RecordError::RecoveryRequired);}
                    if inner.background.is_some(){return Err(RecordError::Busy);}
                    let Some(sources)=inner.index.capture_merge(MAX_ROWS).await? else {
                        inner.maintenance_requested=Some("cache_merge_unavailable");
                        return Err(RecordError::Busy);
                    };
                    let root=inner.journal.root().to_path_buf();
                    let limit=inner.config.max_journal_bytes.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024))
                        .ok_or_else(||storage("cache merge disk budget overflow"))?;
                    let disk=crate::storage::build_budget::DiskScope::begin_concurrent(&root,limit).map_err(storage)?;
                    let resources=crate::resources::io_scope(crate::resources::Lane::Background)
                        .bytes((sources.keys.len() as u64).checked_mul(128).ok_or(RecordError::Busy)?).map_err(|_|RecordError::Busy)?;
                    let snapshot=Snapshot::capture(&inner.state,&inner.config,&sources)?;
                    let config=inner.config.clone();
                    let base=serde_json::to_vec(&inner.anchor).map_err(storage)?;
                    let selected=inner.anchor.as_ref().map(|anchor|anchor.retained_packages().iter()
                        .filter(|package|sources.natives.iter().any(|native|package.path(&root).is_ok_and(|path|path==native.package_path())))
                        .cloned().collect::<Vec<_>>()).unwrap_or_default();
                    // A cache build uses the existing owner/GC barrier without
                    // copying a WAL suffix. Live updates propagate retirement
                    // through source tokens and are reconciled during install.
                    inner.background=Some(indexer::Tail{id,entries:Vec::new(),bytes:0,max_bytes:0,overflowed:true,revision:0});
                    if inner.maintenance_requested==Some(CAUSE){inner.maintenance_requested=None;}
                    (root,config,snapshot,sources,base,selected,disk,resources)
                };
                #[cfg(test)]
                pause(&root,timeout).await;
                if started.elapsed()>=timeout{return Err(RecordError::Busy);}
                // Verify participating authority packages outside the guard.
                // Untouched packages/WAL remain authenticated by checkpoint
                // and cold recovery; cache compaction cannot certify over them.
                let limit=config.max_journal_bytes.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024)).ok_or(RecordError::Busy)?;
                for package in &selected {
                    package.verify(&root,limit).map_err(|_|RecordError::RecoveryRequired)?;
                }
                let candidate={let _phase=maintenance::Phase::new("cache_merge_build_unlocked",snapshot.seq);
                    build_candidate(&root,&config,&snapshot).await?};
                #[cfg(test)]
                if root.join(".test-cache-merge-fail-before-install").exists(){return Err(storage("injected private cache candidate failure"));}
                let cut=snapshot.seq;
                {
                    let _writer=writer.lock().await;
                    let mut inner=owner.lock().await;
                    let _phase=maintenance::Phase::new("cache_merge_install_locked",inner.state.seq);
                    if !inner.healthy {return Err(RecordError::RecoveryRequired);}
                    if started.elapsed()>=timeout || inner.background.as_ref().is_none_or(|tail|tail.id!=id)
                        || serde_json::to_vec(&inner.anchor).map_err(storage)?!=base {return Err(RecordError::Busy);}
                    let Inner{index,ordinals,locations,..}=&mut *inner;
                    if !index.install_merge(&sources,candidate,ordinals,locations,address_limit(&config)?).await? {
                        return Err(RecordError::Busy);
                    }
                    // Never advance consumers: mutable-tail work may still lag.
                }
                // Source handles and snapshot drop after the publication guard.
                Ok(cut)
            }).catch_unwind().await;
            let result=match outcome {Ok(result)=>result,Err(_)=>Err(storage("cache merge candidate worker panicked"))};
            let _writer=writer.lock().await;
            let mut inner=owner.lock().await;
            let _cleanup_phase=maintenance::Phase::new("cache_merge_cleanup_locked",inner.state.seq);
            if inner.background.as_ref().is_some_and(|tail|tail.id==id){inner.background=None;}
            inner.maintenance_scheduled=false;
            if matches!(result,Err(RecordError::RecoveryRequired)) {
                inner.healthy=false;
                inner.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
            }
            if result.is_err() && inner.maintenance_requested.is_none()
                && inner.index.components().await.len()>=capacity::segments(&inner.config)+1 {
                inner.maintenance_requested=Some("cache_merge_retry_checkpoint");
            }
            if inner.healthy {
                if let Err(error)=inner.reclaim_anchor_objects().await {log::warn!("cache merge retirement deferred: {error}");}
            }
            result
        });
        Ok(IndexJob{handle})
    }
}

#[cfg(test)]
async fn pause(root:&Path,timeout:Duration) {
    let gate=root.join(".test-cache-merge-pause");
    if gate.exists() {
        std::fs::write(root.join(".test-cache-merge-started"),b"captured").unwrap();
        let until=Instant::now()+timeout;
        while gate.exists() && Instant::now()<until {tokio::time::sleep(Duration::from_millis(5)).await;}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config,write};
    async fn rotate(partition:&RecordPartition) {
        let mut inner=partition.inner.lock().await;
        let root=inner.journal.root().to_owned();
        let (tail,_)=build_at(&root,&root.join(format!("cache-{:032x}",rand::random::<u128>())),&inner.config,&State::default()).await.unwrap();
        let budget=view::RotationBudget{max_segments:16,max_address_slots:address_limit(&inner.config).unwrap()};
        let Inner{index,ordinals,locations,..}=&mut *inner;
        index.seal_cache_into_successor(tail,ordinals,locations,budget).await.unwrap();
    }
    async fn fixture(path:&Path)->RecordPartition {
        let p=RecordPartition::create(path,config()).await.unwrap();
        p.write(write("a","a","alpha",ExpectedVersion::Absent)).await.unwrap();rotate(&p).await;
        p.write(write("b","b","alpha",ExpectedVersion::Absent)).await.unwrap();rotate(&p).await;
        p.write(write("c","c","alpha",ExpectedVersion::Absent)).await.unwrap();p
    }
    async fn started(root:&Path) {
        tokio::time::timeout(Duration::from_secs(5),async {
            while !root.join(".test-cache-merge-started").exists(){tokio::task::yield_now().await;}
        }).await.unwrap();
    }
    #[tokio::test]
    async fn derived_merge_catches_concurrent_update_delete_and_recovers_without_certificate() {
        let temporary=tempfile::tempdir().unwrap();let root=temporary.path().join("records");
        let p=fixture(&root).await;
        let held={let inner=p.inner.lock().await;inner.index.test_index().read().await.record_lexical[1].clone()};
        let held=held.snapshot().await.unwrap();
        std::fs::write(root.join(".test-cache-merge-pause"),b"pause").unwrap();
        let job=p.start_cache_merge_job(Duration::from_secs(20)).unwrap();started(&root).await;
        p.write(write("update","a","beta",ExpectedVersion::Match(1))).await.unwrap();
        let mut deleted=write("delete","b","",ExpectedVersion::Match(1));deleted.document=None;
        p.write(deleted).await.unwrap();
        p.write(write("d","d","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::remove_file(root.join(".test-cache-merge-pause")).unwrap();
        assert_eq!(job.finish().await.unwrap(),3);
        {let inner=p.inner.lock().await;assert!(inner.anchor.is_none());assert_eq!(inner.state.seq,6);assert!(inner.index.detached_cache);assert_eq!(inner.index.components().await.len(),2);}
        assert_eq!(held.identities.len(),1);
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,2);
        assert_eq!(p.get("a").await.unwrap().unwrap().version,2);
        drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert!(p.get("b").await.unwrap().is_none());
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,2);
        assert_eq!(p.search("beta".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,1);
    }
    #[tokio::test]
    async fn derived_candidate_failure_preserves_live_view_and_recovery() {
        let temporary=tempfile::tempdir().unwrap();let root=temporary.path().join("records");
        let p=fixture(&root).await;
        std::fs::write(root.join(".test-cache-merge-fail-before-install"),b"failure").unwrap();
        assert!(p.start_cache_merge_job(Duration::from_secs(20)).unwrap().finish().await.is_err());
        {let inner=p.inner.lock().await;assert!(inner.healthy);assert!(inner.background.is_none());assert!(inner.anchor.is_none());assert_eq!(inner.index.components().await.len(),3);}
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,3);
        drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,3);
    }
    #[tokio::test]
    async fn abandoned_cache_job_retains_owner_and_management_returns_busy() {
        let temporary=tempfile::tempdir().unwrap();let root=temporary.path().join("records");
        let p=fixture(&root).await;
        let pipeline=IngestPipeline::open(p.clone()).await.unwrap();
        std::fs::write(root.join(".test-cache-merge-pause"),b"pause").unwrap();
        let job=p.start_cache_merge_job(Duration::from_secs(20)).unwrap();started(&root).await;
        pipeline.finish().await.unwrap();
        assert!(matches!(pipeline.require_drained().await,Err(RecordError::Busy)));
        drop(job);drop(pipeline);drop(p);
        assert!(RecordPartition::open(&root,config().max_journal_bytes).await.is_err());
        std::fs::remove_file(root.join(".test-cache-merge-pause")).unwrap();
        let p=tokio::time::timeout(Duration::from_secs(10),async {
            loop {if let Ok(p)=RecordPartition::open(&root,config().max_journal_bytes).await {break p;}
                tokio::time::sleep(Duration::from_millis(5)).await;}
        }).await.unwrap();
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,3);
    }
    #[tokio::test]
    async fn repeated_updates_beyond_sixteen_tails_recover_from_original_wal() {
        let temporary=tempfile::tempdir().unwrap();let root=temporary.path().join("records");
        let mut settings=config();settings.max_records=16;settings.max_operations=4096;settings.max_journal_bytes=16*1024*1024;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        p.compact_journal().await.unwrap();
        let certificate=p.inner.lock().await.anchor.as_ref().unwrap().identity().to_owned();
        let pipeline=IngestPipeline::open(p.clone()).await.unwrap();
        for start in (0..2305).step_by(64) {
            let batch=(start..(start+64).min(2305)).map(|i|write(&format!("op-{i}"),"same",if i==2304{"terminal"}else{"alpha"},ExpectedVersion::Any)).collect();
            pipeline.submit(batch).await.unwrap();pipeline.finish().await.unwrap();
        }
        crate::record::tests::wait_maintenance_idle(&p).await;
        {let inner=p.inner.lock().await;assert_eq!(inner.anchor.as_ref().unwrap().identity(),certificate,"cache merges must not replace the checkpoint");assert!(inner.index.components().await.len()<=capacity::segments(&settings)+1);}
        drop(pipeline);drop(p);
        let p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.get("same").await.unwrap().unwrap().version,2305);
        assert_eq!(p.search("terminal".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,1);
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,0);
    }
    #[tokio::test]
    async fn recovery_rebuilds_private_cache_when_optional_merge_budget_is_exhausted() {
        let temporary=tempfile::tempdir().unwrap();let root=temporary.path().join("records");
        let mut settings=config();settings.max_records=64;settings.max_record_bytes=16384;settings.max_journal_bytes=4*1024*1024;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        p.compact_journal().await.unwrap();
        let certificate=p.inner.lock().await.anchor.as_ref().unwrap().identity().to_owned();
        for id in ["a","b","c"] {
            p.write(write(id,id,&"alpha ".repeat(1000),ExpectedVersion::Absent)).await.unwrap();
            rotate(&p).await;
        }
        // A real, lowered optional analysis quota exercises the same Busy path
        // as large committed input exceeding the production 256 MiB heuristic.
        std::fs::write(root.join(".test-cache-merge-build-budget"),b"512").unwrap();
        let mut padded=0;
        for _ in 0..3 {
            while p.inner.lock().await.index.components().await.len()<capacity::segments(&settings)+1 {
                let id=format!("pad-{padded}");padded+=1;
                p.write(write(&id,&id,&"alpha ".repeat(1000),ExpectedVersion::Absent)).await.unwrap();rotate(&p).await;
            }
            let mut inner=p.inner.lock().await;
            let before=inner.journal.physical_bytes();
            inner.replay_context().stage_stable_tail().await.unwrap();
            assert_eq!(inner.journal.physical_bytes(),before);
            assert_eq!(inner.anchor.as_ref().unwrap().identity(),certificate);
            assert_eq!(inner.index.components().await.len(),2,"budget pressure must use full private cache fallback");
            let current=inner.index.storage_paths().await;
            let caches=std::fs::read_dir(&root).unwrap().filter_map(Result::ok)
                .filter(|entry|entry.file_name().to_string_lossy().starts_with("cache-")).count();
            assert!(caches<=current.len(),"repeated private rebuilds must retire obsolete caches");
        }
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,3+padded);
        drop(p);
        let p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,3+padded);
    }
}

#[cfg(all(test,feature="s3-object-store"))]
#[path="cache_merge_remote_tests.rs"]
mod remote_tests;
