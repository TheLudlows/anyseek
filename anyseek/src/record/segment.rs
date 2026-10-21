//! Prepared immutable segment closure. This module never publishes a head.
//! The caller must authenticate the returned descriptor with the partition
//! publication protocol before it can become an authoritative recovery input.
use super::*;
use sha2::{Digest,Sha256};
use std::io::{Read,Write};

#[derive(Serialize,Deserialize,Clone)]
#[serde(deny_unknown_fields)]
struct FileRef { path:String, bytes:u64, sha256:[u8;32] }
#[derive(Serialize,Deserialize,Clone)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor { format:u32, seq:u64, files:Vec<FileRef> }
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchState { active:crate::active::ActiveStatistics }

fn files(root:&Path,budget:u64)->Result<Vec<FileRef>,RecordError> {
    if std::fs::symlink_metadata(root).map_err(storage)?.file_type().is_symlink(){return Err(storage("segment root symlink rejected"));}
    fn walk(root:&Path,path:&Path,total:&mut u64,budget:u64,depth:usize,directories:&mut usize,result:&mut Vec<FileRef>)->Result<(),RecordError>{
        *directories+=1;
        if depth>4 || *directories>32{return Err(storage("segment directory budget exceeded"));}
        for entry in std::fs::read_dir(path).map_err(storage)? {
            let entry=entry.map_err(storage)?;let kind=entry.file_type().map_err(storage)?;
            if kind.is_symlink(){return Err(storage("segment symlink rejected"));}
            if kind.is_dir(){walk(root,&entry.path(),total,budget,depth+1,directories,result)?;continue;}
            if !kind.is_file(){return Err(storage("segment contains special file"));}
            if result.len()>=1024{return Err(storage("segment file count exceeded"));}
            let mut file=std::fs::File::open(entry.path()).map_err(storage)?;
            let bytes=file.metadata().map_err(storage)?.len();
            *total=total.checked_add(bytes).filter(|n|*n<=budget).ok_or_else(||storage("segment build budget exceeded"))?;
            let mut digest=Sha256::new();let mut buffer=[0u8;8192];
            loop {let n=file.read(&mut buffer).map_err(storage)?;if n==0{break;}digest.update(&buffer[..n]);}
            let path=entry.path().strip_prefix(root).map_err(storage)?.to_str().ok_or_else(||storage("invalid segment filename"))?.replace('\\',"/");
            result.push(FileRef{path,bytes,sha256:digest.finalize().into()});
        }
        Ok(())
    }
    let mut result=Vec::new();walk(root,root,&mut 0,budget,0,&mut 0,&mut result)?;result.sort_by(|a,b|a.path.cmp(&b.path));Ok(result)
}
fn write_json(path:&Path,value:&impl Serialize)->Result<(),RecordError>{
    let mut file=std::fs::OpenOptions::new().write(true).create_new(true).open(path).map_err(storage)?;
    serde_json::to_writer(&mut file,value).map_err(storage)?;file.flush().map_err(storage)?;file.sync_all().map_err(storage)
}
fn read_bounded(path:&Path,limit:u64)->Result<Vec<u8>,RecordError>{
    let file=std::fs::File::open(path).map_err(storage)?;
    if file.metadata().map_err(storage)?.len()>limit{return Err(storage("segment metadata budget exceeded"));}
    let mut bytes=Vec::new();file.take(limit.saturating_add(1)).read_to_end(&mut bytes).map_err(storage)?;
    if bytes.len() as u64>limit{return Err(storage("segment metadata budget exceeded"));}Ok(bytes)
}
impl Descriptor {
    pub(super) async fn prepare(path:&Path,config:&RecordConfig,state:&State,budget:u64)->Result<Self,RecordError>{
        std::fs::create_dir(path).map_err(storage)?;
        let prepared=segment_state::PreparedState::write(&path.join("payload"),config,state)?;
        write_json(&path.join("state"),&prepared)?;
        let (index,_)=build_at(path,&path.join("search"),config,state).await?;
        let active=index.read().await.shard_vec[0].read().await.active.as_ref().map(|active|active.clone_local()).ok_or_else(||storage("missing segment statistics"))?;
        write_json(&path.join("statistics"),&SearchState{active})?;
        // No mutable/mapped handle may outlive preparation. Publication and
        // retirement can then use the same discipline on Linux and Windows.
        drop(index);
        let refs=files(path,budget)?;
        for object in &refs {
            std::fs::OpenOptions::new().write(true).open(path.join(&object.path)).map_err(storage)?.sync_all().map_err(storage)?;
        }
        // Parent directory publication remains the caller's responsibility.
        #[cfg(unix)]
        fn sync_dirs(path:&Path)->Result<(),RecordError>{
            for entry in std::fs::read_dir(path).map_err(storage)? {let entry=entry.map_err(storage)?;if entry.file_type().map_err(storage)?.is_dir(){sync_dirs(&entry.path())?;}}
            std::fs::File::open(path).map_err(storage)?.sync_all().map_err(storage)
        }
        #[cfg(unix)] sync_dirs(path)?;
        Ok(Self{format:1,seq:state.seq,files:refs})
    }
    pub(super) async fn open(&self,path:&Path,budget:u64,authority_budget:u64)->Result<(RecordConfig,State,IndexArc,Vec<String>),RecordError>{
        if self.format!=1{return Err(storage("unsupported segment format"));}
        let actual=files(path,budget)?;
        if actual.len()!=self.files.len() || actual.iter().zip(&self.files).any(|(a,b)|a.path!=b.path || a.bytes!=b.bytes || a.sha256!=b.sha256){return Err(storage("segment closure mismatch"));}
        let encoded=read_bounded(&path.join("state"),authority_budget)?;
        let (config,state)=segment_state::PreparedState::decode(&encoded,authority_budget)?.read(&path.join("payload"),authority_budget)?;
        if state.seq!=self.seq{return Err(storage("segment sequence mismatch"));}
        let search:SearchState=serde_json::from_slice(&read_bounded(&path.join("statistics"),budget)?).map_err(storage)?;
        search.active.validate(retrieval_schema(&config).iter().filter(|field|field.index_lexical || field.index_vector).count()).map_err(storage)?;
        let visible=evidence::visibility(&state.records)?;
        let expected:Vec<_>=state.records.values().filter(|record|visible.get(&record.record_id)==Some(&true) && !(state.evidence_quarantined && temporal::protected(record))).map(|r|r.record_id.clone()).collect();
        if search.active.keys!=expected{return Err(storage("segment visibility mapping mismatch"));}
        let index=crate::index::open_index(&path.join("search")).await.map_err(storage)?;
        detach(&index).await;
        if index.read().await.shard_vec.len()!=1{return Err(storage("segment shard count mismatch"));}
        {
            let root=index.read().await;
            let shard=root.shard_vec[0].read().await;
            if shard.indexed_doc_count!=expected.len() || !root.stored_field_names.is_empty(){return Err(storage("segment search identity mismatch"));}
        }
        let stored_schema:Vec<SchemaField>=serde_json::from_slice(&read_bounded(&path.join("search/schema.json"),authority_budget)?).map_err(storage)?;
        if serde_json::to_vec(&stored_schema).map_err(storage)?!=serde_json::to_vec(&retrieval_schema(&config)).map_err(storage)? {return Err(storage("segment schema mismatch"));}
        index.read().await.shard_vec[0].write().await.active=Some(search.active);
        Ok((config,state,index,expected))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn prepared_segment_reopens_existing_postings_and_original_vectors() {
        use super::super::tests::{path,config,write,query};
        let root=path();let prepared=path();let mut settings=config();settings.schema.swap(0,1);
        let partition=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let mut request=write("op","a","alpha omega",ExpectedVersion::Absent);
        request.document.as_mut().unwrap().insert("vector".into(),serde_json::json!([7.0,0.0]));
        request.document.as_mut().unwrap().insert("unindexed".into(),serde_json::json!({"preserved":true}));
        let original=request.document.clone();let receipt=partition.write(request.clone()).await.unwrap();
        let lexical=||RecordQuery {temporal:None,text:"alpha".into(),vector:None,mode:SearchMode::Lexical,query_type:QueryType::Union,offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![]};
        let baseline=partition.query(lexical()).await.unwrap();
        let mut inner=partition.inner.lock().await;
        let descriptor=Descriptor::prepare(&prepared,&inner.config,&inner.state,16*settings.max_journal_bytes).await.unwrap();
        let (_,state,index,ordinals)=descriptor.open(&prepared,16*settings.max_journal_bytes,settings.max_journal_bytes).await.unwrap();
        assert_eq!(serde_json::to_vec(&state).unwrap(),serde_json::to_vec(&inner.state).unwrap());
        inner.index=view::RetrievalView::single(index);inner.ordinals=ordinals;inner.state=state;
        drop(inner);
        assert_eq!(partition.write(request.clone()).await.unwrap(),receipt);
        request.document.as_mut().unwrap().insert("body".into(),serde_json::json!("conflict"));
        assert!(matches!(partition.write(request).await,Err(RecordError::Conflict(_))));
        let result=partition.query(lexical()).await.unwrap();
        assert_eq!(result.count_total,baseline.count_total);
        assert_eq!(result.hits[0].score,baseline.hits[0].score);
        assert_eq!(result.hits[0].record.document,original);
        let vector=query(&partition,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All}).await;
        assert_eq!(vector.hits.len(),1);assert_eq!(vector.hits[0].record.document,original);
        // Opening/searching is read-only with respect to all selected files.
        assert!(files(&prepared,16*settings.max_journal_bytes).unwrap().iter().zip(&descriptor.files).all(|(a,b)|a.sha256==b.sha256));
        drop(partition);
        std::fs::write(prepared.join("unexpected"),b"not selected").unwrap();
        assert!(descriptor.open(&prepared,16*settings.max_journal_bytes,settings.max_journal_bytes).await.is_err());
        std::fs::remove_dir_all(root).unwrap();std::fs::remove_dir_all(prepared).unwrap();
    }
    #[tokio::test]
    async fn shared_segment_statistics_match_effective_corpus_after_mutations() {
        use super::super::tests::{path,config,write,query};
        let roots=[path(),path(),path()];
        let a=RecordPartition::create(&roots[0],config()).await.unwrap();
        let b=RecordPartition::create(&roots[1],config()).await.unwrap();
        let oracle=RecordPartition::create(&roots[2],config()).await.unwrap();
        for (partition,id,body) in [(&a,"a","alpha alpha"),(&a,"c","beta"),(&b,"b","alpha omega omega"),(&b,"d","alpha delta")] {
            let request=write(id,id,body,ExpectedVersion::Absent);
            partition.write(request.clone()).await.unwrap();oracle.write(request).await.unwrap();
        }
        let corpus=std::sync::Arc::new(std::sync::RwLock::new(crate::active::Corpus::default()));
        for partition in [&a,&b] {
            partition.inner.lock().await.index.test_index().read().await.shard_vec[0].write().await.active.as_mut().unwrap().attach(&corpus).unwrap();
        }
        async fn matches(a:&RecordPartition,b:&RecordPartition,oracle:&RecordPartition) {
            let mut scores=BTreeMap::new();
            for partition in [a,b] {for hit in query(partition,SearchMode::Lexical).await.hits {scores.insert(hit.record.record_id,hit.score);}}
            let expected:BTreeMap<_,_>=query(oracle,SearchMode::Lexical).await.hits.into_iter().map(|hit|(hit.record.record_id,hit.score)).collect();
            assert_eq!(scores,expected);
        }
        matches(&a,&b,&oracle).await;
        let update=write("revision","a","alpha alpha alpha omega",ExpectedVersion::Match(1));
        a.write(update.clone()).await.unwrap();oracle.write(update).await.unwrap();
        matches(&a,&b,&oracle).await;
        let mut deletion=write("delete","d","",ExpectedVersion::Match(1));deletion.document=None;
        b.write(deletion.clone()).await.unwrap();oracle.write(deletion).await.unwrap();
        matches(&a,&b,&oracle).await;
        let insert=write("new","e","alpha",ExpectedVersion::Absent);
        b.write(insert.clone()).await.unwrap();oracle.write(insert).await.unwrap();
        matches(&a,&b,&oracle).await;
        // Compose the actual root search engine from the two physical shards.
        // This exercises global channel ranking/RRF, rather than merging two
        // already-fused result lists in the test.
        let root_index=a.inner.lock().await.index.test_index().clone();
        let second=b.inner.lock().await.index.test_index().read().await.shard_vec[0].clone();
        second.write().await.meta.id=1;
        {let mut root=root_index.write().await;root.shard_vec.push(second);root.shard_number=2;}
        for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All},SearchMode::Hybrid{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All}] {
            for order in [vec![],vec![ResultSort{field:"_id".into(),order:crate::search::SortOrder::Ascending}],vec![ResultSort{field:"_id".into(),order:crate::search::SortOrder::Descending}]] {
              for (offset,length) in [(0,10),(0,1),(1,1)] {
                let vector=if mode==SearchMode::Lexical{None}else{Some(Embedding::F32(vec![1.0,0.0]))};
                let expected=oracle.query(RecordQuery{temporal:None,text:"alpha".into(),vector:vector.clone(),mode:mode.clone(),query_type:QueryType::Union,offset,length,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:order.clone()}).await.unwrap();
                let actual=root_index.search("alpha".into(),vector,QueryType::Union,mode.clone(),true,offset,length,ResultType::TopkCount,true,vec![],vec![],order.clone()).await;
                assert!(actual.error.is_none(),"{:?}",actual.error);
                let mut mapped=Vec::new();
                for hit in actual.results {
                    let root=root_index.read().await;let shard=root.shard_vec[hit.doc_id%2].read().await;
                    mapped.push((shard.active.as_ref().unwrap().keys[hit.doc_id/2].clone(),hit.score));
                }
                let wanted:Vec<_>=expected.hits.into_iter().map(|hit|(hit.record.record_id,hit.score)).collect();
                assert_eq!(mapped,wanted,"{mode:?} order={order:?} offset={offset} length={length}");
                assert_eq!(actual.result_count_total,expected.count_total);
              }
            }
        }
        drop(root_index);drop(a);drop(b);drop(oracle);for root in roots {std::fs::remove_dir_all(root).unwrap();}
    }

}
