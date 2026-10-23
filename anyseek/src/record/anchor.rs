//! A partition certificate selects this immutable state/index closure. Directory
//! discovery is never a second head or a reason to adopt an unselected object.
use super::*;
use crate::storage::payload_pool::{Budget,Closure,PoolBuilder,PoolReader};
use segment::{files,write_json,read_bounded,FileRef};

#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Reference {id:String,files:Vec<FileRef>,segments:Vec<Frozen>}
type Frozen = segment::Package;
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {format:u32,seq:u64,state:segment_state::ReferencedState,payloads:Closure}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Statistics {segments:Vec<crate::active::ActiveStatistics>}
pub(super) struct Loaded {
    pub config:RecordConfig,pub state:State,pub index:IndexArc,
    pub ordinals:Vec<String>,pub locations:BTreeMap<String,usize>,pub payloads:Closure,
}
fn valid(id:&str)->bool{id.len()==32&&id.bytes().all(|b|b.is_ascii_digit()||(b'a'..=b'f').contains(&b))}
fn directory(root:&Path,id:&str)->Result<PathBuf,RecordError>{
    if !valid(id){return Err(storage("invalid anchor identity"));}
    Ok(root.join(format!("anchor-{id}")))
}
fn budget(config:&RecordConfig)->Result<Budget,RecordError>{
    Ok(Budget{bytes:config.max_journal_bytes,objects:64,payloads:config.max_records.checked_add(config.max_operations).and_then(|n|n.checked_mul(102)).ok_or_else(||storage("anchor payload count overflow"))?,body_bytes:config.max_record_bytes})
}
// This is an acceptance bound, not yet the final build-time allocator quota.
fn closure_limit(authority:u64)->Result<u64,RecordError>{authority.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024)).ok_or_else(||storage("anchor closure budget overflow"))}
use segment::sync_tree;
impl Frozen {
    async fn prepare(root:&Path,config:&RecordConfig,state:&State,selected:Option<&std::collections::BTreeSet<String>>)->Result<(Self,crate::active::ActiveStatistics),RecordError>{
        let id=format!("{:032x}",rand::random::<u128>());
        let prepared=root.join(format!(".segment-prepare-{id}"));
        let (index,_)=build_at_selected(root,&prepared,config,state,selected).await?;
        let active=index.read().await.shard_vec[0].read().await.active.as_ref().ok_or_else(||storage("missing frozen statistics"))?.clone_local();
        drop(index);
        let reference=Self::seal(root,&prepared,id,closure_limit(config.max_journal_bytes)?)?;
        Ok((reference,active))
    }
    async fn load(&self,root:&Path,config:&RecordConfig,active:crate::active::ActiveStatistics)->Result<IndexArc,RecordError>{
        let path=self.verify(root,closure_limit(config.max_journal_bytes)?)?;
        active.validate_sparse(retrieval_schema(config).iter().filter(|f|f.index_lexical||f.index_vector).count()).map_err(storage)?;
        let schema:Vec<SchemaField>=serde_json::from_slice(&read_bounded(&path.join("schema.json"),config.max_journal_bytes)?).map_err(storage)?;
        if serde_json::to_vec(&schema).map_err(storage)?!=serde_json::to_vec(&retrieval_schema(config)).map_err(storage)?{return Err(storage("frozen segment schema mismatch"));}
        let mapped=self.files.iter().try_fold(0u64,|n,f|n.checked_add(f.bytes)).ok_or_else(||storage("loaded segment estimate overflow"))?;
        let estimate=mapped.checked_mul(2).ok_or_else(||storage("loaded segment estimate overflow"))?;
        let mut reservation=build_budget::admit(config,std::iter::empty())?;
        reservation.grow(estimate).map_err(storage)?;
        let index=crate::index::open_index(&path).await.map_err(storage)?;detach(&index).await;
        index.write().await.record_lexical.push(lexical::runtime::Runtime::open(&path.join("native"),config)?);
        index.read().await.shard_vec[0].write().await.build_reservation=Some(reservation);
        {
            let index=index.read().await;
            if index.shard_number!=1||index.shard_vec.len()!=1||!index.stored_field_names.is_empty(){return Err(storage("invalid frozen topology"));}
            let mut shard=index.shard_vec[0].write().await;
            if shard.indexed_doc_count!=active.keys.len(){return Err(storage("frozen cardinality mismatch"));}
            shard.delete_hashset=(0..active.keys.len()).filter(|id|!active.is_live(*id)).collect();
            shard.active=Some(active);
        }
        Ok(index)
    }
}
impl Reference {
    #[cfg(feature="s3-object-store")]
    pub(super) fn identity(&self)->&str {&self.id}
    #[cfg(feature="s3-object-store")]
    pub(super) fn remote_files(&self,payloads:&Closure)->Result<Vec<(String,u64,[u8;32])>,RecordError>{
        let mut result=Vec::new();
        for file in &self.files {result.push((format!("anchor-{}/{}",self.id,file.path),file.bytes,file.sha256));}
        for segment in &self.segments {for file in &segment.files {
            result.push((format!("segment-{}/{}",segment.id,file.path),file.bytes,file.sha256));
        }}
        for (id,descriptor) in &payloads.objects {result.push((format!("payload-{id}"),descriptor.bytes,descriptor.sha256));}
        if result.len()>10_000 {return Err(storage("checkpoint file count exceeded"));}
        Ok(result)
    }
    pub(super) async fn prepare(root:&Path,config:&RecordConfig,state:&State,prior:Closure,previous:Option<&Reference>,view:Option<&view::RetrievalView>,compact:bool)->Result<Self,RecordError>{
        let id=format!("{:032x}",rand::random::<u128>());
        let prepared=root.join(format!(".anchor-prepare-{id}"));
        std::fs::create_dir(&prepared).map_err(storage)?;
        let mut pool=PoolBuilder::begin(root,prior,budget(config)?,format!("{:032x}",rand::random::<u128>())).map_err(storage)?;
        let referenced=segment_state::ReferencedState::write(config,state,&mut pool)?;
        let payloads=pool.finish().map_err(storage)?;
        write_json(&prepared.join("manifest"),&Manifest{format:2,seq:state.seq,state:referenced,payloads})?;
        let mut segments=Vec::new();let mut statistics=Vec::new();
        if !compact && previous.is_some_and(|p|p.segments.len()<8) {
            let previous=previous.unwrap();
            let local=view.ok_or_else(||storage("missing incremental input view"))?.local_statistics().await?;
            if local.len()!=previous.segments.len()+1{return Err(storage("selected segment/view mismatch"));}
            let mut local=local.into_iter();let tail=local.next().unwrap();
            for (segment,active) in previous.segments.iter().zip(local) {
                if (0..active.keys.len()).any(|id|active.is_live(id)) {segments.push(segment.clone());statistics.push(active);}
            }
            let keys=tail.keys.iter().enumerate().filter(|(id,_)|tail.is_live(*id)).map(|(_,key)|key.clone()).collect::<std::collections::BTreeSet<_>>();
            if !keys.is_empty() {
                let (segment,active)=Frozen::prepare(root,config,state,Some(&keys)).await?;
                segments.push(segment);statistics.push(active);
            }
        } else {
            let (segment,active)=Frozen::prepare(root,config,state,None).await?;
            segments.push(segment);statistics.push(active);
        }
        // Keep one empty physical segment so the query engine retains schema.
        if segments.is_empty(){let (segment,active)=Frozen::prepare(root,config,state,None).await?;segments.push(segment);statistics.push(active);}
        write_json(&prepared.join("statistics"),&Statistics{segments:statistics})?;
        let members=files(&prepared,closure_limit(config.max_journal_bytes)?)?;
        sync_tree(&prepared)?;
        crate::storage::partition::publish_bundle(&prepared,&directory(root,&id)?).map_err(storage)?;
        Ok(Self{id,files:members,segments})
    }
    pub(super) async fn load(&self,root:&Path,authority_budget:u64)->Result<Loaded,RecordError>{
        if self.segments.is_empty()||self.segments.len()>8{return Err(storage("invalid selected segment count"));}
        let mut ids=std::collections::BTreeSet::new();
        if self.segments.iter().any(|s|!ids.insert(&s.id)){return Err(storage("duplicate selected segment"));}
        let path=directory(root,&self.id)?;
        if serde_json::to_vec(&files(&path,closure_limit(authority_budget)?)?).map_err(storage)?!=serde_json::to_vec(&self.files).map_err(storage)?{return Err(storage("anchor closure checksum mismatch"));}
        let manifest:Manifest=serde_json::from_slice(&read_bounded(&path.join("manifest"),authority_budget)?).map_err(storage)?;
        if manifest.format!=2{return Err(storage("unsupported anchor format"));}
        validate_config(manifest.state.config())?;
        let mut limits=budget(manifest.state.config())?;limits.bytes=limits.bytes.min(authority_budget);
        let pool=PoolReader::open(root,manifest.payloads.clone(),limits).map_err(storage)?;
        let (config,state)=manifest.state.read(&pool,authority_budget)?;
        if state.seq!=manifest.seq{return Err(storage("anchor sequence mismatch"));}
        let statistics:Statistics=serde_json::from_slice(&read_bounded(&path.join("statistics"),closure_limit(authority_budget)?)?).map_err(storage)?;
        if statistics.segments.len()!=self.segments.len(){return Err(storage("segment statistics closure mismatch"));}
        let visible=evidence::visibility(&state.records)?;
        let expected:std::collections::BTreeSet<_>=state.records.values().filter(|r|visible.get(&r.record_id)==Some(&true)&&!(state.evidence_quarantined&&temporal::protected(r))).map(|r|r.record_id.clone()).collect();
        let n=self.segments.len();let mut locations=BTreeMap::new();let mut ordinals=Vec::new();let mut live=std::collections::BTreeSet::new();let mut indexes=Vec::new();
        let address_budget=config.max_records.checked_add(config.max_operations).and_then(|n|n.checked_mul(64)).ok_or_else(||storage("address budget overflow"))?;
        for (slot,(segment,active)) in self.segments.iter().zip(statistics.segments).enumerate(){
            for (id,key) in active.keys.iter().enumerate(){
                let address=id.checked_mul(n).and_then(|v|v.checked_add(slot)).filter(|v|*v<address_budget).ok_or_else(||storage("segment address budget exceeded"))?;
                if ordinals.len()<=address{ordinals.try_reserve(address+1-ordinals.len()).map_err(storage)?;ordinals.resize(address+1,String::new());}
                ordinals[address]=key.clone();
                if active.is_live(id) {
                    if !live.insert(key.clone()){return Err(storage("duplicate live segment identity"));}
                    locations.insert(key.clone(),address);
                }
            }
            indexes.push(segment.load(root,&config,active).await?);
        }
        if live!=expected{return Err(storage("anchor visible identity mismatch"));}
        let index=indexes[0].clone();
        let mut shards=Vec::new();for component in &indexes{shards.push(component.read().await.shard_vec[0].clone());}
        let mut native=Vec::new();for component in &indexes{native.extend(component.read().await.record_lexical.iter().cloned());}
        {let mut root=index.write().await;root.shard_vec=shards;root.shard_number=n;root.record_lexical=native;}
        Ok(Loaded{config,state,index,ordinals,locations,payloads:manifest.payloads})
    }
}

impl Inner {
    /// Replay certified tail decisions into the selected index view. Never
    /// append WAL or generate another operation/feed identity during recovery.
    pub(super) async fn replay_anchor_tail(&mut self,entry:Entry)->Result<(),RecordError>{
        match entry {
            Entry::Applied(applied)=>{
                let mut next=self.state.clone();apply(&mut next,&applied)?;
                let bytes=encode_entry(&Entry::Checkpoint{config:self.config.clone(),state:next})?.len();
                self.publish(&applied,bytes).await?;
            },
            Entry::Expired(event)=>{
                let mut next=self.state.clone();temporal::apply(&mut next,&event)?;
                let visible=evidence::visibility(&next.records)?;
                let retired:Vec<_>=self.locations.keys().filter(|id|visible.get(*id)!=Some(&true)).cloned().collect();
                for id in retired {let ordinal=self.locations.remove(&id).unwrap();self.index.retire(ordinal).await?;}
                self.state=next;
            },
            Entry::LeaseExpired(event)=>lease::apply(&mut self.state,&event)?,
            Entry::FeedTrim(event)=>change_feed::apply_trim(&mut self.state,&self.config.change_feed,&event)?,
            Entry::Checkpoint{..}|Entry::Anchor(_)=>return Err(storage("unexpected anchor in operation stream")),
        }
        self.graph_cache=graph::Cache::rebuild(&self.state)?;
        self.snapshot_bytes=encode_entry(&Entry::Checkpoint{config:self.config.clone(),state:self.state.clone()})?.len();
        Ok(())
    }
}

impl Inner {
    pub(super) async fn publish_anchor(&mut self)->Result<u64,RecordError>{
        self.publish_anchor_state(self.state.clone(),true).await
    }
    pub(super) async fn publish_anchor_state(&mut self,next:State,reuse:bool)->Result<u64,RecordError>{
        if self.background.is_some() { return Err(RecordError::Busy); }
        let result=self.prepare_and_publish_anchor(next,reuse).await;
        // The inner future has dropped every candidate view/handle before GC.
        // Unknown publication leaves healthy=false: never remove recovery
        // evidence on the basis of the old in-memory certificate in that case.
        if result.is_err() && self.healthy {
            if let Err(error)=self.reclaim_anchor_objects().await {
                log::warn!("unpublished anchor cleanup pending: {error}");
            }
        }
        result
    }
    async fn prepare_and_publish_anchor(&mut self,next:State,reuse:bool)->Result<u64,RecordError>{
        self.reclaim_anchor_objects().await?;
        let root=self.journal.root().to_path_buf();
        let _disk=crate::storage::build_budget::DiskScope::begin(&root,closure_limit(self.config.max_journal_bytes)?).map_err(storage)?;
        let snapshot_bytes=encode_entry(&Entry::Checkpoint{config:self.config.clone(),state:next.clone()})?.len();
        let prior=if reuse{self.payloads.clone()}else{Closure::default()};
        let reference=Reference::prepare(&root,&self.config,&next,prior,self.anchor.as_ref(),Some(&self.index),!reuse).await?;
        #[cfg(test)]
        if root.join(".test-anchor-reject-prepared").exists() {
            return Err(storage("injected candidate rejection before certificate"));
        }
        let loaded=reference.load(&root,self.config.max_journal_bytes).await?;
        let mut index=view::RetrievalView::single(loaded.index);
        let mut ordinals=loaded.ordinals;
        let mut locations=loaded.locations;
        let cache=root.join(format!("cache-{:032x}",rand::random::<u128>()));
        let (tail,_)=build_at(&root,&cache,&self.config,&State::default()).await?;
        index.rotate_tail(tail,&mut ordinals,&mut locations,view::RotationBudget{max_segments:64,max_address_slots:self.config.max_records.checked_add(self.config.max_operations).and_then(|n|n.checked_mul(64)).ok_or_else(||storage("view address budget overflow"))?}).await?;
        #[cfg(test)]
        if root.join(".test-anchor-reject-loaded").exists() {
            return Err(storage("injected loaded candidate rejection before certificate"));
        }
        let payload=encode_entry(&Entry::Anchor(reference.clone()))?;
        PartitionJournal::validate_initial(payload.len(),self.config.max_journal_bytes).map_err(storage)?;
        let fenced=self.output.read().map_err(|_|RecordError::RecoveryRequired)?.fenced;
        self.healthy=false;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
        #[cfg(feature="s3-object-store")]
        if let Some(remote)=self.remote.as_mut() {
            remote.publish_anchor(&root,&reference,&loaded.payloads,next.seq,self.config.max_journal_bytes,false).await?;
        }
        match self.journal.checkpoint(&payload){
            Ok(())=>{},
            Err(error@CreateError::NotPublished(_))=>{
                #[cfg(feature="s3-object-store")]
                if self.remote.is_some() {return Err(RecordError::DurablePending(next.seq));}
                self.healthy=true;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=fenced;
                return Err(creation_error(error));
            },
            Err(error)=>return Err(creation_error(error)),
        }
        // All fallible preparation precedes the authority certificate. The
        // partition guard excludes readers until the complete view is installed.
        self.index=index;self.ordinals=ordinals;self.locations=locations;
        self.anchor=Some(reference);self.payloads=loaded.payloads;self.state=next;self.snapshot_bytes=snapshot_bytes;
        self.healthy=true;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=fenced;
        #[cfg(test)]
        if root.join(".test-anchor-post-certificate-gc").exists() {
            std::fs::write(root.join(".test-anchor-gc-failure"), b"injected retirement failure").map_err(storage)?;
        }
        if let Err(error)=self.reclaim_anchor_objects().await {log::warn!("anchor certified; object retirement pending: {error}");}
        #[cfg(feature="s3-object-store")]
        if let Some(remote)=self.remote.as_mut() {
            if let Err(error)=remote.collect_objects().await {log::warn!("remote object collection pending: {error}");}
        }
        Ok(self.state.seq)
    }
    /// The owned partition guard is the local reader/build pin. Backups export
    /// independent inline authority under that same guard, never borrowed paths.
    pub(super) async fn reclaim_anchor_objects(&self)->Result<(),RecordError>{
        // The owned worker is a local build pin. Its candidate and mmap files
        // cannot be retired until it completes or is explicitly cancelled.
        if self.background.is_some() { return Ok(()); }
        self.journal.reclaim_obsolete().map_err(storage)?;
        let root=self.journal.root();
        #[cfg(test)]
        if root.join(".test-anchor-gc-failure").exists() {
            return Err(storage("injected anchor retirement failure"));
        }
        let caches=self.index.storage_paths().await;
        for entry in std::fs::read_dir(root).map_err(storage)?{
            let entry=entry.map_err(storage)?;let name=entry.file_name();let Some(name)=name.to_str() else{continue;};
            let remove=if let Some(id)=name.strip_prefix("anchor-") {
                valid(id)&&self.anchor.as_ref().is_none_or(|reference|reference.id!=id)
            }else if let Some(id)=name.strip_prefix("segment-") {
                valid(id)&&self.anchor.as_ref().is_none_or(|a|!a.segments.iter().any(|s|s.id==id))
            }else if let Some(id)=name.strip_prefix(".segment-prepare-") {valid(id)
            }else if let Some(id)=name.strip_prefix("payload-") {
                valid(id)&&!self.payloads.objects.contains_key(id)
            }else if let Some(id)=name.strip_prefix(".anchor-prepare-") {valid(id)}
            else if let Some(id)=name.strip_prefix(".payload-prepare-") {valid(id)}
            else if let Some(id)=name.strip_prefix("cache-") {valid(id)&&!caches.iter().any(|p|p==&entry.path())}
            else{false};
            if !remove{continue;}
            package_pins::retire_if_unpinned(&entry.path(), || {
            let kind=entry.file_type().map_err(storage)?;
            if kind.is_symlink(){return Err(storage("retirement symlink rejected"));}
            if kind.is_dir(){std::fs::remove_dir_all(entry.path()).map_err(storage)?;}
            else if kind.is_file(){std::fs::remove_file(entry.path()).map_err(storage)?;}
            else{return Err(storage("retirement special file rejected"));}
                Ok(())
            })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::tests::{path,config,write,query};
    #[tokio::test]
    async fn resource_admission_rejects_before_wal_and_retries_after_space_returns() {
        let root=path();let mut settings=config();settings.max_record_bytes=1024*1024;settings.max_journal_bytes=8*1024*1024;
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let request=write("oversized-analysis","a",&"a ".repeat(300_000),ExpectedVersion::Absent);
        assert!(matches!(p.write(request).await,Err(RecordError::Rejected(message)) if message.contains("build admission")));
        assert_eq!(p.inner.lock().await.state.seq,0);
        let accepted=write("oversized-analysis","a","alpha",ExpectedVersion::Absent);
        p.write(accepted.clone()).await.unwrap();p.compact_journal().await.unwrap();
        let selected=serde_json::to_vec(p.inner.lock().await.anchor.as_ref().unwrap()).unwrap();
        // Retained evidence outside the orphan-name namespace must be counted,
        // not silently deleted to make the next publication fit.
        let retained=root.join("retained-budget-evidence");
        std::fs::File::create(&retained).unwrap().set_len(closure_limit(settings.max_journal_bytes).unwrap()+1).unwrap();
        assert!(p.compact_journal().await.is_err());
        assert_eq!(serde_json::to_vec(p.inner.lock().await.anchor.as_ref().unwrap()).unwrap(),selected);
        assert!(retained.exists());assert!(p.inner.lock().await.healthy);
        std::fs::remove_file(retained).unwrap();p.compact_journal().await.unwrap();
        drop(p);let p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.write(accepted).await.unwrap().seq,1);
        assert_eq!(query(&p,SearchMode::Lexical).await.hits.len(),1);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn rejected_prepared_anchor_reclaims_only_unselected_objects() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("one","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let selected=p.inner.lock().await.anchor.clone().unwrap();
        let selected_bytes=serde_json::to_vec(&selected).unwrap();
        p.write(write("two","b","alpha beta",ExpectedVersion::Absent)).await.unwrap();
        for gate in [".test-anchor-reject-prepared",".test-anchor-reject-loaded"] {
        std::fs::write(root.join(gate),b"reject").unwrap();
        for _ in 0..3 {
            assert!(p.compact_journal().await.is_err());
            let inner=p.inner.lock().await;
            assert!(inner.healthy);
            assert_eq!(serde_json::to_vec(inner.anchor.as_ref().unwrap()).unwrap(),selected_bytes);
            let names=std::fs::read_dir(&root).unwrap().map(|e|e.unwrap().file_name().to_string_lossy().into_owned()).collect::<Vec<_>>();
            assert!(!names.iter().any(|n|n.starts_with(".anchor-prepare-")||n.starts_with(".segment-prepare-")||n.starts_with(".payload-prepare-")));
            assert_eq!(names.iter().filter(|n|n.starts_with("anchor-")).count(),1);
            assert_eq!(names.iter().filter(|n|n.starts_with("segment-")).count(),selected.segments.len());
            assert_eq!(names.iter().filter(|n|n.starts_with("payload-")).count(),inner.payloads.objects.len());
        }
        std::fs::remove_file(root.join(gate)).unwrap();
        }
        let retry=write("three","c","alpha gamma",ExpectedVersion::Absent);
        p.write(retry.clone()).await.unwrap();
        p.compact_journal().await.unwrap();drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert!(p.write(retry).await.is_ok());
        assert_eq!(p.inner.lock().await.state.records.len(),3);
        drop(p);std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn successive_flush_reuses_segments_compacts_and_replays_tail() {
        async fn counted(p:&RecordPartition,mode:SearchMode)->RecordSearch {
            let vector=if mode==SearchMode::Lexical{None}else{Some(Embedding::F32(vec![1.0,0.0]))};
            p.query(RecordQuery{temporal:None,text:"alpha".into(),vector,mode,query_type:QueryType::Union,offset:0,length:32,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![]}).await.unwrap()
        }
        let root=path();let oracle_root=path();let settings=config();
        let mut p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let oracle=RecordPartition::create(&oracle_root,settings.clone()).await.unwrap();
        let initial=write("initial","a","alpha alpha",ExpectedVersion::Absent);
        p.write(initial.clone()).await.unwrap();oracle.write(initial).await.unwrap();
        p.compact_journal().await.unwrap();
        let first=p.inner.lock().await.anchor.clone().unwrap().segments[0].clone();
        let fingerprint=serde_json::to_vec(&first.files).unwrap();
        for round in 0..10 {
            let key=format!("b{round}");let request=write(&key,&key,"alpha omega",ExpectedVersion::Absent);
            p.write(request.clone()).await.unwrap();oracle.write(request).await.unwrap();
            if round==1 {
                let update=write("a2","a","alpha revised revised",ExpectedVersion::Match(1));
                p.write(update.clone()).await.unwrap();oracle.write(update).await.unwrap();
            }
            let seq=p.compact_journal().await.unwrap();
            let reference=p.inner.lock().await.anchor.clone().unwrap();
            assert!(reference.segments.len()<=8);
            if round==0 {
                assert_eq!(reference.segments.len(),2);
                assert_eq!(reference.segments[0].id,first.id);
                assert_eq!(serde_json::to_vec(&files(&first.path(&root).unwrap(),closure_limit(settings.max_journal_bytes).unwrap()).unwrap()).unwrap(),fingerprint);
            }
            for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All},SearchMode::Hybrid{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All}] {
                let got=counted(&p,mode.clone()).await;let want=counted(&oracle,mode).await;
                assert_eq!(got.visible_seq,seq);assert_eq!(got.count_total,want.count_total);
                let hits=|v:RecordSearch|v.hits.into_iter().map(|h|(h.record.record_id,h.record.version,h.score)).collect::<Vec<_>>();
                assert_eq!(hits(got),hits(want),"round {round}");
            }
            drop(p);p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
            assert_eq!(p.get("a").await.unwrap().unwrap().version,if round>=1{2}else{1});
        }
        let tail=write("after-flush","tail","alpha tail",ExpectedVersion::Absent);
        let receipt=p.write(tail.clone()).await.unwrap();drop(p);
        p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.write(tail).await.unwrap(),receipt);
        let inner=p.inner.lock().await;let selected=inner.anchor.as_ref().unwrap().segments.iter().map(|s|s.id.clone()).collect::<std::collections::BTreeSet<_>>();
        let actual=std::fs::read_dir(&root).unwrap().map(Result::unwrap).filter_map(|e|e.file_name().to_str().and_then(|s|s.strip_prefix("segment-")).map(str::to_owned)).collect::<std::collections::BTreeSet<_>>();
        assert_eq!(actual,selected);
        drop(inner);drop(p);drop(oracle);std::fs::remove_dir_all(root).unwrap();std::fs::remove_dir_all(oracle_root).unwrap();
    }

    #[tokio::test]
    async fn multisegment_source_cascade_and_corrupt_selected_object_fail_closed() {
        use super::super::tests::source_write;
        let root=path();let settings=config();let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        for mut request in [source_write("s","s",&[]),source_write("d","d",&[("s",1)]),source_write("e","e",&[("d",1)])] {
            if request.record_id=="d" {request.document.as_mut().unwrap().shift_remove("vector");}
            p.write(request).await.unwrap();p.compact_journal().await.unwrap();
        }
        assert_eq!(p.inner.lock().await.anchor.as_ref().unwrap().segments.len(),3);
        let mut patch=write("enrich","d","",ExpectedVersion::Match(1));patch.document=None;
        patch.vector_patch=Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        assert_eq!(p.write(patch.clone()).await.unwrap().version,1);
        assert_eq!(p.statistics().await.unwrap().physical_versions,4);
        let mut deletion=write("delete","s","",ExpectedVersion::Match(1));deletion.document=None;
        let receipt=p.write(deletion.clone()).await.unwrap();
        for id in ["s","d","e"]{assert!(p.get(id).await.unwrap().is_none());}
        let events=serde_json::to_vec(&p.changes(None,100).await.unwrap().events).unwrap();
        p.compact_journal().await.unwrap();
        assert_eq!(serde_json::to_vec(&p.changes(None,100).await.unwrap().events).unwrap(),events);
        drop(p);let p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        for id in ["s","d","e"]{assert!(p.get(id).await.unwrap().is_none());}
        assert_eq!(p.write(deletion).await.unwrap(),receipt);
        assert_eq!(p.statistics().await.unwrap().active_records,0);
        assert_eq!(p.write(patch).await.unwrap().version,1);
        for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All},SearchMode::Hybrid{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All}] {assert!(query(&p,mode).await.hits.is_empty());}
        let selected=p.inner.lock().await.anchor.as_ref().unwrap().segments[0].path(&root).unwrap();
        drop(p);
        std::fs::write(selected.join("unselected-extra"),b"corrupt certified closure").unwrap();
        assert!(RecordPartition::open(&root,settings.max_journal_bytes).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn rejected_index_file_creation_returns_error_and_drains_builders() {
        use crate::index::{INDEX_FILENAME,INDEX_FORMAT_VERSION_MAJOR,INDEX_FORMAT_VERSION_MINOR,DOCSTORE_FILENAME,VECTOR_FILENAME,SCHEMA_FILENAME};
        for blocked in [DOCSTORE_FILENAME,VECTOR_FILENAME,SCHEMA_FILENAME] {
            let root=path();let shard=root.join("shards/0");std::fs::create_dir_all(&shard).unwrap();
            let header=[INDEX_FORMAT_VERSION_MAJOR.to_le_bytes(),INDEX_FORMAT_VERSION_MINOR.to_le_bytes()].concat();
            std::fs::write(root.join(INDEX_FILENAME),&header).unwrap();std::fs::write(shard.join(INDEX_FILENAME),&header).unwrap();
            std::fs::create_dir(shard.join(blocked)).unwrap();
            let sibling=root.join("shards/1");std::fs::create_dir_all(&sibling).unwrap();
            std::fs::write(sibling.join(INDEX_FILENAME),&header).unwrap();
            std::fs::write(sibling.join(".test-builder-wait"),b"hold accepted sibling").unwrap();
            let target=root.clone();let cfg=config();
            let mut task=tokio::spawn(async move {create_index(&target,cfg.index,&retrieval_schema(&config()),11,true,Some(2)).await});
            tokio::time::timeout(std::time::Duration::from_secs(5),async {
                while !sibling.join(".test-builder-entered").exists(){tokio::task::yield_now().await;}
            }).await.unwrap();
            // A real gate holds the successful sibling after the failing
            // shard was accepted. The parent must not return while it is held.
            assert!(tokio::time::timeout(std::time::Duration::from_millis(25),&mut task).await.is_err());
            std::fs::remove_file(sibling.join(".test-builder-wait")).unwrap();
            let result=task.await;
            assert!(matches!(result,Ok(Err(_))),"{blocked}: builder must return an error, not panic");
            // The other accepted builder has completed before the error returns.
            assert!(sibling.join(SCHEMA_FILENAME).is_file());
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn empty_tail_does_not_preallocate_dictionary_buckets() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        let inner=p.inner.lock().await;let index=inner.index.test_index().read().await;
        for shard in &index.shard_vec {
            let shard=shard.read().await;
            assert_eq!(shard.segments_level0.iter().map(|s|s.segment.capacity()).sum::<usize>(),0);
        }
        drop(index);drop(inner);drop(p);std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn default_anchor_loads_index_and_replays_only_tail_with_pinned_quantization(){
        let root=path();let mut config=config();
        config.index.inference=serde_json::from_value(serde_json::json!({"External":{"dimensions":2,"precision":"F32","quantization":"ScalarQuantizationI8","similarity":"Euclidean"}})).unwrap();
        let partition=RecordPartition::create(&root,config.clone()).await.unwrap();
        for (id,vector) in [("a",vec![1.0,2.0]),("b",vec![0.123,-9.0])]{
            let mut request=write(id,id,"alpha",ExpectedVersion::Absent);request.document.as_mut().unwrap().insert("vector".into(),serde_json::json!(vector));partition.write(request).await.unwrap();
        }
        let mode=SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All};
        let before=query(&partition,mode.clone()).await;
        partition.compact_journal().await.unwrap();
        let after=query(&partition,mode.clone()).await;
        let scores=|r:RecordSearch|r.hits.into_iter().map(|h|(h.record.record_id,h.score)).collect::<Vec<_>>();
        assert_eq!(scores(before),scores(after));
        let reference={let inner=partition.inner.lock().await;inner.anchor.clone().unwrap()};
        let selected=directory(&root,&reference.id).unwrap();
        let files_before=serde_json::to_vec(&files(&selected,closure_limit(config.max_journal_bytes).unwrap()).unwrap()).unwrap();
        let update=write("a2","a","alpha revised",ExpectedVersion::Match(1));let receipt=partition.write(update.clone()).await.unwrap();
        let expected=scores(query(&partition,mode.clone()).await);
        drop(partition);
        let restored=RecordPartition::open(&root,config.max_journal_bytes).await.unwrap();
        {
            let inner=restored.inner.lock().await;
            let index=inner.index.test_index().read().await;
            assert_eq!(PathBuf::from(&index.index_path_string),reference.segments[0].path(&root).unwrap());
            assert_eq!(index.shard_number,2);
        }
        assert_eq!(scores(query(&restored,mode).await),expected);
        assert_eq!(restored.write(update).await.unwrap(),receipt);
        assert_eq!(serde_json::to_vec(&files(&selected,closure_limit(config.max_journal_bytes).unwrap()).unwrap()).unwrap(),files_before);
        drop(restored);std::fs::remove_dir_all(root).unwrap();
    }
}
