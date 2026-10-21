//! A partition certificate selects this immutable state/index closure. Directory
//! discovery is never a second head or a reason to adopt an unselected object.
use super::*;
use crate::storage::payload_pool::{Budget,Closure,PoolBuilder,PoolReader};
use segment::{files,write_json,read_bounded,FileRef};

#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Reference {id:String,files:Vec<FileRef>}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {format:u32,seq:u64,state:segment_state::ReferencedState,payloads:Closure}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Statistics {active:crate::active::ActiveStatistics}
pub(super) struct Loaded {
    pub config:RecordConfig,pub state:State,pub index:IndexArc,
    pub ordinals:Vec<String>,pub payloads:Closure,
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
impl Reference {
    pub(super) async fn prepare(root:&Path,config:&RecordConfig,state:&State,prior:Closure)->Result<Self,RecordError>{
        let id=format!("{:032x}",rand::random::<u128>());
        let prepared=root.join(format!(".anchor-prepare-{id}"));
        std::fs::create_dir(&prepared).map_err(storage)?;
        let mut pool=PoolBuilder::begin(root,prior,budget(config)?,format!("{:032x}",rand::random::<u128>())).map_err(storage)?;
        let referenced=segment_state::ReferencedState::write(config,state,&mut pool)?;
        let payloads=pool.finish().map_err(storage)?;
        write_json(&prepared.join("manifest"),&Manifest{format:1,seq:state.seq,state:referenced,payloads})?;
        let (index,_)=build_at(root,&prepared.join("search"),config,state).await?;
        let active=index.read().await.shard_vec[0].read().await.active.as_ref().ok_or_else(||storage("missing anchor statistics"))?.clone_local();
        write_json(&prepared.join("statistics"),&Statistics{active})?;
        drop(index);
        let members=files(&prepared,closure_limit(config.max_journal_bytes)?)?;
        // Flush every component before the containing directory is published.
        fn sync_tree(path:&Path)->Result<(),RecordError>{
            for entry in std::fs::read_dir(path).map_err(storage)?{
                let entry=entry.map_err(storage)?;
                if entry.file_type().map_err(storage)?.is_dir(){sync_tree(&entry.path())?;}
                else{std::fs::OpenOptions::new().write(true).open(entry.path()).map_err(storage)?.sync_all().map_err(storage)?;}
            }
            #[cfg(unix)] std::fs::File::open(path).map_err(storage)?.sync_all().map_err(storage)?;
            Ok(())
        }
        sync_tree(&prepared)?;
        crate::storage::partition::publish_bundle(&prepared,&directory(root,&id)?).map_err(storage)?;
        Ok(Self{id,files:members})
    }
    pub(super) async fn load(&self,root:&Path,authority_budget:u64)->Result<Loaded,RecordError>{
        let path=directory(root,&self.id)?;
        let expected=serde_json::to_vec(&self.files).map_err(storage)?;
        let actual=files(&path,closure_limit(authority_budget)?)?;
        if serde_json::to_vec(&actual).map_err(storage)?!=expected{return Err(storage("anchor closure checksum mismatch"));}
        let manifest:Manifest=serde_json::from_slice(&read_bounded(&path.join("manifest"),authority_budget)?).map_err(storage)?;
        if manifest.format!=1{return Err(storage("unsupported anchor format"));}
        validate_config(manifest.state.config())?;
        let mut limits=budget(manifest.state.config())?;limits.bytes=limits.bytes.min(authority_budget);
        let pool=PoolReader::open(root,manifest.payloads.clone(),limits).map_err(storage)?;
        let (config,state)=manifest.state.read(&pool,authority_budget)?;
        if state.seq!=manifest.seq{return Err(storage("anchor sequence mismatch"));}
        let statistics:Statistics=serde_json::from_slice(&read_bounded(&path.join("statistics"),closure_limit(authority_budget)?)?).map_err(storage)?;
        statistics.active.validate(retrieval_schema(&config).iter().filter(|f|f.index_lexical||f.index_vector).count()).map_err(storage)?;
        let visible=evidence::visibility(&state.records)?;
        let ordinals:Vec<_>=state.records.values().filter(|r|visible.get(&r.record_id)==Some(&true)&&!(state.evidence_quarantined&&temporal::protected(r))).map(|r|r.record_id.clone()).collect();
        if statistics.active.keys!=ordinals{return Err(storage("anchor visible identity mismatch"));}
        let schema:Vec<SchemaField>=serde_json::from_slice(&read_bounded(&path.join("search/schema.json"),authority_budget)?).map_err(storage)?;
        if serde_json::to_vec(&schema).map_err(storage)?!=serde_json::to_vec(&retrieval_schema(&config)).map_err(storage)?{return Err(storage("anchor schema mismatch"));}
        let index=crate::index::open_index(&path.join("search")).await.map_err(storage)?;detach(&index).await;
        {
            let root=index.read().await;
            if root.shard_number!=1||root.shard_vec.len()!=1||!root.stored_field_names.is_empty(){return Err(storage("invalid anchor index topology"));}
            let mut shard=root.shard_vec[0].write().await;
            if shard.indexed_doc_count!=ordinals.len(){return Err(storage("anchor index cardinality mismatch"));}
            shard.active=Some(statistics.active);
        }
        Ok(Loaded{config,state,index,ordinals,payloads:manifest.payloads})
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
        self.reclaim_anchor_objects().await?;
        let root=self.journal.root().to_path_buf();
        let snapshot_bytes=encode_entry(&Entry::Checkpoint{config:self.config.clone(),state:next.clone()})?.len();
        let prior=if reuse{self.payloads.clone()}else{Closure::default()};
        let reference=Reference::prepare(&root,&self.config,&next,prior).await?;
        let loaded=reference.load(&root,self.config.max_journal_bytes).await?;
        let mut index=view::RetrievalView::single(loaded.index);
        let mut ordinals=loaded.ordinals;
        let mut locations:BTreeMap<_,_>=ordinals.iter().enumerate().map(|(id,key)|(key.clone(),id)).collect();
        let cache=root.join(format!("cache-{:032x}",rand::random::<u128>()));
        let (tail,_)=build_at(&root,&cache,&self.config,&State::default()).await?;
        index.rotate_tail(tail,&mut ordinals,&mut locations,view::RotationBudget{max_segments:64,max_address_slots:self.config.max_records.checked_add(self.config.max_operations).and_then(|n|n.checked_mul(64)).ok_or_else(||storage("view address budget overflow"))?}).await?;
        let payload=encode_entry(&Entry::Anchor(reference.clone()))?;
        PartitionJournal::validate_initial(payload.len(),self.config.max_journal_bytes).map_err(storage)?;
        let fenced=self.output.read().map_err(|_|RecordError::RecoveryRequired)?.fenced;
        self.healthy=false;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
        match self.journal.checkpoint(&payload){
            Ok(())=>{},
            Err(error@CreateError::NotPublished(_))=>{
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
        Ok(self.state.seq)
    }
    /// The owned partition guard is the local reader/build pin. Backups export
    /// independent inline authority under that same guard, never borrowed paths.
    pub(super) async fn reclaim_anchor_objects(&self)->Result<(),RecordError>{
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
            }else if let Some(id)=name.strip_prefix("payload-") {
                valid(id)&&!self.payloads.objects.contains_key(id)
            }else if let Some(id)=name.strip_prefix(".anchor-prepare-") {valid(id)}
            else if let Some(id)=name.strip_prefix(".payload-prepare-") {valid(id)}
            else if let Some(id)=name.strip_prefix("cache-") {valid(id)&&!caches.iter().any(|p|p==&entry.path())}
            else{false};
            if !remove{continue;}
            let kind=entry.file_type().map_err(storage)?;
            if kind.is_symlink(){return Err(storage("retirement symlink rejected"));}
            if kind.is_dir(){std::fs::remove_dir_all(entry.path()).map_err(storage)?;}
            else if kind.is_file(){std::fs::remove_file(entry.path()).map_err(storage)?;}
            else{return Err(storage("retirement special file rejected"));}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::tests::{path,config,write,query};
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
            assert_eq!(PathBuf::from(&index.index_path_string),selected.join("search"));
            assert_eq!(index.shard_number,2);
        }
        assert_eq!(scores(query(&restored,mode).await),expected);
        assert_eq!(restored.write(update).await.unwrap(),receipt);
        assert_eq!(serde_json::to_vec(&files(&selected,closure_limit(config.max_journal_bytes).unwrap()).unwrap()).unwrap(),files_before);
        drop(restored);std::fs::remove_dir_all(root).unwrap();
    }
}
