//! Immutable Segment file inventory and bounded metadata I/O for selected anchors.
use super::*;
use sha2::{Digest,Sha256};
use std::io::{Read,Write};

/// Immutable file closure. This descriptor is meaningful only when selected
/// by the partition certificate (local) or SQLite head (remote).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Package {
    pub(super) id: String,
    pub(super) files: Inventory,
}
impl Package {
    pub(super) fn path(&self, root: &Path) -> Result<PathBuf, RecordError> {
        if self.id.len() != 32
            || !self
                .id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(storage("invalid immutable package identity"));
        }
        Ok(root.join(format!("segment-{}", self.id)))
    }
    /// Seal after all writers have drained. An identity-stable directory needs
    /// no rename. Neither path makes it authoritative; the Record certificate does.
    pub(super) fn seal(
        root: &Path,
        prepared: &Path,
        id: String,
        limit: u64,
    ) -> Result<Self, RecordError> {
        let package = Self {
            id,
            files: files(prepared, limit)?,
        };
        let target = package.path(root)?;
        sync_tree(prepared)?;
        if prepared != target {
            crate::storage::partition::publish_bundle(prepared, &target).map_err(storage)?;
        }
        // Directory presence is never authority, including in-place sealing.
        // The caller still authenticates and publishes the Record certificate.
        Ok(package)
    }
    pub(super) fn verify(&self, root: &Path, limit: u64) -> Result<PathBuf, RecordError> {
        let path = self.path(root)?;
        if files(&path, limit)?.as_slice() != self.files.as_slice()
        {
            return Err(storage("immutable package closure mismatch"));
        }
        Ok(path)
    }
}
/// Validate the complete closure before syncing, including symlink/special-file
/// rejection. This is shared by native and existing production package builders.
pub(super) fn sync_tree(path: &Path) -> Result<(), RecordError> {
    for entry in std::fs::read_dir(path).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        let kind = entry.file_type().map_err(storage)?;
        if kind.is_dir() {
            sync_tree(&entry.path())?;
        } else if kind.is_file() {
            std::fs::OpenOptions::new()
                .write(true)
                .open(entry.path())
                .map_err(storage)?
                .sync_all()
                .map_err(storage)?;
        } else {
            return Err(storage(
                "immutable package contains symlink or special file",
            ));
        }
    }
    #[cfg(unix)]
    std::fs::File::open(path)
        .map_err(storage)?
        .sync_all()
        .map_err(storage)?;
    Ok(())
}
#[derive(Debug,Serialize,Deserialize,Clone,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct FileRef { pub(super) path:String, pub(super) bytes:u64, pub(super) sha256:[u8;32] }
/// Immutable file metadata shares a residency owner across package/reference
/// clones. Filesystem enumeration admits each retained entry before allocating
/// its String/Vec slot; decoding admits the borrowed JSON before materializing.
#[derive(Clone)]
pub(super) struct Inventory(Arc<InventoryOwner>);
impl Inventory {
    #[cfg(test)]
    pub(super) fn charge_for_test(&self)->u64 {self.0._lease.bytes()}
}
struct InventoryOwner { values:Vec<FileRef>, _lease:crate::storage::build_budget::MemoryLease }
impl std::fmt::Debug for Inventory {
    fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result { std::fmt::Debug::fmt(&self.0.values,f) }
}
impl std::ops::Deref for Inventory {
    type Target=Vec<FileRef>;
    fn deref(&self)->&Self::Target { &self.0.values }
}
impl<'a> IntoIterator for &'a Inventory {
    type Item=&'a FileRef;
    type IntoIter=std::slice::Iter<'a,FileRef>;
    fn into_iter(self)->Self::IntoIter {self.0.values.iter()}
}
impl Serialize for Inventory {
    fn serialize<S:serde::Serializer>(&self,s:S)->Result<S::Ok,S::Error>{self.0.values.serialize(s)}
}
impl<'de> Deserialize<'de> for Inventory {
    fn deserialize<D:serde::Deserializer<'de>>(d:D)->Result<Self,D::Error>{
        let raw=<&serde_json::value::RawValue>::deserialize(d)?;
        if raw.get().len()>16*1024*1024{return Err(serde::de::Error::custom("inventory metadata too large"));}
        let bytes=(raw.get().len() as u64).checked_mul(32).and_then(|n|n.checked_add(128))
            .ok_or_else(||serde::de::Error::custom("inventory charge overflow"))?;
        let lease=crate::storage::build_budget::reserve_memory(bytes).map_err(serde::de::Error::custom)?;
        let values:Vec<FileRef>=serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
        if values.len()>1024{return Err(serde::de::Error::custom("inventory entry limit exceeded"));}
        Ok(Self(Arc::new(InventoryOwner{values,_lease:lease})))
    }
}
pub(super) fn files(root:&Path,budget:u64)->Result<Inventory,RecordError> {
    if std::fs::symlink_metadata(root).map_err(storage)?.file_type().is_symlink(){return Err(storage("segment root symlink rejected"));}
    fn walk(root:&Path,path:&Path,total:&mut u64,budget:u64,depth:usize,directories:&mut usize,result:&mut Vec<FileRef>,lease:&mut crate::storage::build_budget::MemoryLease)->Result<(),RecordError>{
        *directories+=1;
        if depth>4 || *directories>32{return Err(storage("segment directory budget exceeded"));}
        for entry in std::fs::read_dir(path).map_err(storage)? {
            let entry=entry.map_err(storage)?;let kind=entry.file_type().map_err(storage)?;
            if kind.is_symlink(){return Err(storage("segment symlink rejected"));}
            if kind.is_dir(){walk(root,&entry.path(),total,budget,depth+1,directories,result,lease)?;continue;}
            if !kind.is_file(){return Err(storage("segment contains special file"));}
            if result.len()>=1024{return Err(storage("segment file count exceeded"));}
            let mut file=std::fs::File::open(entry.path()).map_err(storage)?;
            let bytes=file.metadata().map_err(storage)?.len();
            *total=total.checked_add(bytes).filter(|n|*n<=budget).ok_or_else(||storage("segment build budget exceeded"))?;
            let mut digest=Sha256::new();let mut buffer=[0u8;8192];
            loop {let n=file.read(&mut buffer).map_err(storage)?;if n==0{break;}digest.update(&buffer[..n]);}
            let entry_path=entry.path();
            let relative=entry_path.strip_prefix(root).map_err(storage)?.to_str().ok_or_else(||storage("invalid segment filename"))?;
            let charge=(relative.len() as u64).checked_mul(6).and_then(|n|n.checked_add(512)).and_then(|n|n.checked_mul(32))
                .ok_or_else(||storage("inventory entry charge overflow"))?;
            lease.grow(charge).map_err(storage)?;
            let path=relative.replace('\\',"/");
            result.push(FileRef{path,bytes,sha256:digest.finalize().into()});
        }
        Ok(())
    }
    let mut lease=crate::storage::build_budget::reserve_memory(128).map_err(storage)?;
    let mut result=Vec::new();walk(root,root,&mut 0,budget,0,&mut 0,&mut result,&mut lease)?;
    result.sort_by(|a,b|a.path.cmp(&b.path));
    Ok(Inventory(Arc::new(InventoryOwner{values:result,_lease:lease})))
}
pub(super) fn write_json(path:&Path,value:&impl Serialize)->Result<(),RecordError>{
    let mut file=crate::storage::build_budget::OpenOptions::new().write(true).create_new(true).open(path).map_err(storage)?;
    serde_json::to_writer(&mut file,value).map_err(storage)?;file.flush().map_err(storage)?;file.sync_all().map_err(storage)
}
pub(super) fn read_bounded(path:&Path,limit:u64)->Result<Vec<u8>,RecordError>{
    let file=std::fs::File::open(path).map_err(storage)?;
    if file.metadata().map_err(storage)?.len()>limit{return Err(storage("segment metadata budget exceeded"));}
    let mut bytes=Vec::new();file.take(limit.saturating_add(1)).read_to_end(&mut bytes).map_err(storage)?;
    if bytes.len() as u64>limit{return Err(storage("segment metadata budget exceeded"));}Ok(bytes)
}
#[cfg(test)]
mod tests {
    use super::*;
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
        let update=write("revision","a","alpha alpha alpha omega",ExpectedVersion::Match(1));
        a.write(update.clone()).await.unwrap();oracle.write(update).await.unwrap();
        let mut deletion=write("delete","d","",ExpectedVersion::Match(1));deletion.document=None;
        b.write(deletion.clone()).await.unwrap();oracle.write(deletion).await.unwrap();
        let insert=write("new","e","alpha",ExpectedVersion::Absent);
        b.write(insert.clone()).await.unwrap();oracle.write(insert).await.unwrap();
        // Compose the actual root search engine from the two physical shards.
        // This exercises global channel ranking/RRF, rather than merging two
        // already-fused result lists in the test.
        let root_index=a.inner.lock().await.index.test_index().clone();
        let second=b.inner.lock().await.index.test_index().read().await.segments[0].clone();
        let native=b.inner.lock().await.index.test_index().read().await.record_lexical[0].clone();
        {let mut root=root_index.write().await;root.segments.push(second);root.record_lexical.push(native);}
        for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All},SearchMode::Hybrid{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All, rrf: Default::default() }] {
            for order in [vec![],vec![ResultSort{field:"_id".into(),order:crate::search::SortOrder::Ascending}],vec![ResultSort{field:"_id".into(),order:crate::search::SortOrder::Descending}]] {
              for (offset,length) in [(0,10),(0,1),(1,1)] {
                let vector=if mode==SearchMode::Lexical{None}else{Some(Embedding::F32(vec![1.0,0.0]))};
                let expected=oracle.query(RecordQuery{temporal:None,text:"alpha".into(),vector:vector.clone(),mode:mode.clone(),query_type:QueryType::Union,offset,length,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:order.clone()}).await.unwrap();
                let cut={let inner=oracle.inner.lock().await;lexical::Cut{sequence:inner.state.seq,evaluation_time_ms:inner.evaluation_time_ms}};
                let actual=lexical::runtime_query::search(&root_index,"alpha".into(),vector,mode.clone(),QueryType::Union,offset,length,ResultType::TopkCount,vec![],vec![],order.clone(),cut).await.unwrap();
                assert!(actual.error.is_none(),"{:?}",actual.error);
                let mut mapped=Vec::new();
                for hit in actual.results {
                    let root=root_index.read().await;let shard=root.segments[hit.doc_id%2].read().await;
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
