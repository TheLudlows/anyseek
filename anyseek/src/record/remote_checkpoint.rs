//! Object-first export of an existing authenticated anchor. The descriptor maps
//! immutable local files to bounded object chunks; it contains no payload copies.
//! SQLite alone selects the descriptor. Old graphs remain immutable for backups.
use super::*;
use crate::storage::sqlite_authority::ObjectRef;
use sha2::{Digest, Sha256};
use std::io::{Read, Write};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    format: u32,
    partition_id: String,
    seq: u64,
    anchor: anchor::Reference,
    files: Vec<File>,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct File {
    path: String,
    length: u64,
    sha256: [u8; 32],
    chunks: Vec<ObjectRef>,
}
/// Index-only roots bind a complete recovery checkpoint and a certified WAL cut.
/// They never contain a second copy of State or truncate the recovery log.
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexOnly {
    format:u32,partition_id:String,checkpoint:ObjectRef,checkpoint_seq:u64,
    indexed_seq:u64,wal_tip:Option<ObjectRef>,state_sha:[u8;32],config_sha:[u8;32],
    packages:Vec<segment::Package>,statistics:Vec<crate::active::ActiveStatistics>,files:Vec<File>,
}
struct Selected {
    checkpoint:Checkpoint,checkpoint_ref:ObjectRef,index:Option<IndexOnly>,
    _metadata:Vec<crate::storage::build_budget::MemoryLease>,
}
fn safe_path(root: &Path, relative: &str) -> Result<PathBuf, RecordError> {
    let parts = relative.split('/').collect::<Vec<_>>();
    let first = parts
        .first()
        .ok_or_else(|| storage("empty checkpoint path"))?;
    let identity = first
        .strip_prefix("anchor-")
        .or_else(|| first.strip_prefix("segment-"))
        .or_else(|| first.strip_prefix("payload-"));
    if !identity.is_some_and(|id| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()))
        || parts
            .iter()
            .any(|s| s.is_empty() || *s == "." || *s == ".." || s.contains('\\') || s.contains(':'))
        || parts.len() > 6
    {
        return Err(storage("invalid checkpoint path"));
    }
    let mut path = root.to_path_buf();
    for component in parts {
        path.push(component);
        if let Ok(meta) = std::fs::symlink_metadata(&path) {
            if meta.file_type().is_symlink() {
                return Err(storage("checkpoint symlink rejected"));
            }
        }
    }
    Ok(path)
}
// Reject an undersized object-count budget before uploading any part of a
// certified closure. Native packages have more files than the former format.
fn admit_checkpoint_chunks(files: &[(String, u64, [u8; 32])], object_bytes: usize, limit: usize) -> Result<(), RecordError> {
    if object_bytes == 0 { return Err(storage("zero checkpoint object budget")); }
    let required = files.iter().try_fold(0u64, |count, (_, length, _)| {
        count.checked_add(length.div_ceil(object_bytes as u64))
            .ok_or_else(|| storage("checkpoint chunk count overflow"))
    })?;
    if required > limit as u64 {
        return Err(storage(format!("checkpoint chunk budget exceeded: requires {required}, limit {limit}")));
    }
    Ok(())
}

// One admission rule for publication, fresh verification and cold recovery.
// The lease owns the temporary inventory; descriptors retain their own leases.
fn combined_files<'a>(root:&Path,base:&'a [File],index:&'a [File],budget:u64,chunk_limit:usize)
    ->Result<(BTreeMap<&'a str,&'a File>,crate::storage::build_budget::MemoryLease),RecordError> {
    if base.len()>10000 || index.len()>10000 {return Err(storage("file count exceeded"));}
    let count=base.len().checked_add(index.len()).ok_or_else(||storage("file count overflow"))?;
    let lease=crate::storage::build_budget::reserve_memory((count as u64).checked_mul(256).ok_or_else(||storage("inventory budget overflow"))?).map_err(|_|RecordError::Busy)?;
    let mut files=BTreeMap::new();
    for list in [base,index] {
        let mut seen=std::collections::BTreeSet::new();
        for file in list {
            if !seen.insert(file.path.as_str()) {return Err(storage("duplicate descriptor file"));}
            if let Some(old)=files.insert(file.path.as_str(),file) {
                if old!=file {return Err(storage("conflicting index/checkpoint file"));}
            }
        }
    }
    let max=budget.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024)).ok_or_else(||storage("closure byte budget overflow"))?;
    let mut bytes=0u64;let mut chunks=0usize;
    for file in files.values() {
        safe_path(root,&file.path)?;
        bytes=bytes.checked_add(file.length).ok_or_else(||storage("closure bytes overflow"))?;
        chunks=chunks.checked_add(file.chunks.len()).ok_or_else(||storage("closure chunks overflow"))?;
        if bytes>max || chunks>chunk_limit {return Err(RecordError::Rejected("combined checkpoint/index closure exceeds recovery budget".into()));}
        if file.chunks.iter().try_fold(0u64,|n,r|n.checked_add(r.length))!=Some(file.length) {
            return Err(storage("checkpoint file/chunk length mismatch"));
        }
    }
    Ok((files,lease))
}

impl RemotePersistence {
    async fn selected(&self,head:&crate::storage::sqlite_authority::PartitionHead,fresh:bool)->Result<Selected,RecordError> {
        Self::selected_from(&self.store,&self.partition_id,head,fresh).await
    }
    async fn selected_from(source:&crate::storage::s3_compatible::S3CompatibleStore,partition_id:&str,
        head:&crate::storage::sqlite_authority::PartitionHead,fresh:bool)->Result<Selected,RecordError> {
        #[derive(Deserialize)] struct Header{format:u32}
        let store=if fresh {source.uncached()}else{source.clone()};
        let selected=head.manifest.as_ref().ok_or(RecordError::RecoveryRequired)?;
        let bytes=store.get_verified(selected).await.map_err(remote_read_error)?;
        let charge=|n:usize|->Result<crate::storage::build_budget::MemoryLease,RecordError>{
            crate::storage::build_budget::reserve_memory((n as u64).checked_mul(32).ok_or_else(||storage("descriptor metadata overflow"))?).map_err(|_|RecordError::Busy)
        };
        let mut leases=vec![charge(bytes.len())?];
        let format:Header=serde_json::from_slice(&bytes).map_err(storage)?;
        let (checkpoint,checkpoint_ref,index)=match format.format {
            1=>(serde_json::from_slice::<Checkpoint>(&bytes).map_err(storage)?,selected.clone(),None),
            2=>{
                let index:IndexOnly=serde_json::from_slice(&bytes).map_err(storage)?;
                if index.partition_id!=partition_id || index.checkpoint_seq!=head.log_base_seq
                    || index.indexed_seq!=head.indexed_seq || index.indexed_seq<index.checkpoint_seq
                    || index.indexed_seq>head.committed_seq || index.packages.is_empty()
                    || index.packages.len()>capacity::MAX_SEGMENTS || index.statistics.len()!=index.packages.len()
                    || index.files.len()>10000 {return Err(storage("invalid index-only identity or watermarks"));}
                let base=store.get_verified(&index.checkpoint).await.map_err(remote_read_error)?;
                leases.push(charge(base.len())?);
                let checkpoint:Checkpoint=serde_json::from_slice(&base).map_err(storage)?;
                let reference=index.checkpoint.clone();
                (checkpoint,reference,Some(index))
            },
            _=>return Err(storage("unsupported remote descriptor format")),
        };
        if checkpoint.format!=1 || checkpoint.partition_id!=partition_id
            || checkpoint.seq!=head.log_base_seq || (index.is_none() && checkpoint.seq!=head.indexed_seq) {
            return Err(storage("invalid selected recovery checkpoint"));
        }
        Ok(Selected{checkpoint,checkpoint_ref,index,_metadata:leases})
    }
    async fn verify_index_cut(&self,root:&Path,index:&mut IndexOnly,
        head:&crate::storage::sqlite_authority::PartitionHead,committed_tip:Option<ObjectRef>,
        config:&RecordConfig,base:&State)->Result<(),RecordError> {
        if index.config_sha!=checkpoint_codec::json_digest(config)? || base.seq!=index.checkpoint_seq
            || committed_tip!=index.wal_tip {
            return Err(storage("index-only cut/config binding mismatch"));
        }
        let mut expected=BTreeMap::new();let mut ids=std::collections::BTreeSet::new();
        for package in &index.packages {
            if !ids.insert(&package.id) {return Err(storage("duplicate index package"));}
            for file in &package.files {
                if expected.insert(format!("segment-{}/{}",package.id,file.path),(file.bytes,file.sha256)).is_some() {
                    return Err(storage("duplicate index package file"));
                }
            }
        }
        let mut paths=std::collections::BTreeSet::new();
        if expected.len()!=index.files.len() || index.files.iter().any(|f|!paths.insert(&f.path) || expected.get(&f.path)!=Some(&(f.length,f.sha256))) {
            return Err(storage("index-only file closure mismatch"));
        }
        let mut cut=head.clone();cut.committed_seq=index.indexed_seq;cut.indexed_seq=index.checkpoint_seq;cut.wal_tip=index.wal_tip.clone();
        let tail=crate::storage::object_log::load_committed_entries_after(&self.store.uncached(),&cut,index.checkpoint_seq,self.max_tail_blocks,self.max_tail_bytes).await.map_err(storage)?;
        let mut state=base.try_clone()?;let mut admitted=state_proof::length(&state)?;
        for entry in tail {
            if <[u8;32]>::from(Sha256::digest(&entry.payload))!=entry.request_sha256 {return Err(storage("index-only WAL digest mismatch"));}
            let _decode=crate::storage::build_budget::reserve_memory((entry.payload.len() as u64).checked_mul(32).ok_or_else(||storage("index replay decode overflow"))?).map_err(storage)?;
            admitted=admitted.checked_add(entry.payload.len().checked_mul(3).ok_or_else(||storage("index replay size overflow"))?).ok_or_else(||storage("index replay size overflow"))?;
            state.admit_bytes(admitted)?;
            match serde_json::from_slice::<Entry>(&entry.payload).map_err(storage)? {
                Entry::Applied(op)=>apply(&mut state,&op)?,
                Entry::Expired(event)=>temporal::apply(&mut state,&event)?,
                Entry::LeaseExpired(event)=>lease::apply(&mut state,&event)?,
                _=>return Err(storage("unexpected authority or same-sequence event in index suffix")),
            }
        }
        validate_restored_state(config,&state)?;
        if state.seq!=index.indexed_seq || checkpoint_codec::json_digest(&state)?!=index.state_sha {
            return Err(storage("index-only replay State mismatch"));
        }
        anchor::Reference::verify_derived_parts(root,config,&state,&index.packages,std::mem::take(&mut index.statistics)).await
    }

    pub(super) async fn tail_needs_checkpoint(&self,seq:u64,operation_id:&str,payload:&[u8])->Result<bool,RecordError> {
        #[derive(Serialize)]
        struct Entry<'a>{seq:u64,operation_id:&'a str,request_sha256:[u8;32],payload:&'a [u8]}
        #[derive(Serialize)]
        struct Block<'a>{partition_id:&'a str,first_seq:u64,last_seq:u64,previous:Option<&'a ObjectRef>,entries:[Entry<'a>;1]}
        let authority=self.authority.lock().await;
        let head=authority.read_head(&self.partition_id).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if head.owner_epoch!=self.owner_epoch || head.committed_seq.checked_add(1)!=Some(seq) {return Err(RecordError::RecoveryRequired);}
        let mut block=Block{partition_id:&self.partition_id,first_seq:seq,last_seq:seq,previous:head.wal_tip.as_ref(),entries:[Entry{seq,operation_id,request_sha256:Sha256::digest(payload).into(),payload}]};
        let bytes=checkpoint_codec::json_length(&block)? as u64;
        let (count,total)=authority.retained_tail_size(&head).map_err(storage)?;
        if bytes<=self.max_object_bytes as u64 && count<self.max_tail_blocks as u64
            && total.checked_add(bytes).is_some_and(|n|n<=self.max_tail_bytes as u64) {return Ok(false);}
        block.previous=None;
        let fresh=checkpoint_codec::json_length(&block)?;
        if self.max_tail_blocks==0 || fresh>self.max_object_bytes || fresh>self.max_tail_bytes {
            return Err(RecordError::Rejected("remote WAL entry exceeds recovery budget".into()));
        }
        Ok(true)
    }
    /// Authenticate the existing remote authority without advancing its recovery
    /// baseline. Local pending packages are derived caches, never remote authority.
    pub(super) async fn verify_seal_inputs(&self, reference:&anchor::Reference, seq:u64,
        journal:&mut PartitionJournal, authority_budget:u64)->Result<(),RecordError> {
        let authority=self.authority.lock().await;
        let head=authority.read_head(&self.partition_id).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if head.owner_epoch!=self.owner_epoch || head.committed_seq!=seq {
            return Err(RecordError::RecoveryRequired);
        }
        let store=self.store.uncached();
        let selected=self.selected(&head,true).await?;
        let checkpoint=&selected.checkpoint;
        if checkpoint_codec::json_digest(&checkpoint.anchor)?!=checkpoint_codec::json_digest(reference)? {
            return Err(storage("remote seal authority mismatch"));
        }
        // Fresh backend reads preserve the publication-time corruption check.
        // No mtime, cached bytes or registered digest substitutes for this check.
        let (files,_inventory)=combined_files(journal.root(),&checkpoint.files,selected.index.as_ref().map_or(&[],|i|i.files.as_slice()),authority_budget,self.max_tail_blocks)?;
        let mut chunks=0usize;let mut total=0usize;
        let max_total=authority_budget.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024)).ok_or_else(||storage("remote seal budget overflow"))?;
        if checkpoint.files.len()>10_000 {return Err(storage("remote seal file count"));}
        for file in files.values() {
            let mut hash=Sha256::new();let mut length=0u64;
            for chunk in &file.chunks {
                chunks=chunks.checked_add(1).filter(|n|*n<=self.max_tail_blocks).ok_or_else(||storage("remote seal chunk budget"))?;
                let bytes=store.get_verified(chunk).await.map_err(remote_read_error)?;
                total=total.checked_add(bytes.len()).filter(|n|*n as u64<=max_total).ok_or_else(||storage("remote seal bytes overflow"))?;
                length=length.checked_add(bytes.len() as u64).ok_or_else(||storage("remote seal file overflow"))?;
                hash.update(&bytes);
            }
            let digest:[u8;32]=hash.finalize().into();
            if length!=file.length || digest!=file.sha256 {return Err(storage("remote seal file checksum"));}
        }
        let entries=crate::storage::object_log::load_committed_entries_after(&store,&head,head.log_base_seq,self.max_tail_blocks,self.max_tail_bytes).await.map_err(|e|match e {crate::storage::object_log::TailError::ObjectStore(e)=>remote_read_error(e),other=>storage(other)})?;
        let mut index=0usize;let mut first=true;
        journal.visit_committed(|bytes| {
            if first {first=false;return Ok(());}
            let Some(entry)=entries.get(index) else{return Err(std::io::Error::other("local suffix exceeds remote authority"));};
            if entry.payload!=bytes || entry.request_sha256!=<[u8;32]>::from(Sha256::digest(bytes)) {
                return Err(std::io::Error::other("remote/local suffix differs"));
            }
            index+=1;Ok(())
        }).map_err(storage)?;
        if first || index!=entries.len(){return Err(storage("local suffix omits remote commits"));}
        // Other SQLite connections may race while object reads await. Bind the
        // exact head version, including same-sequence maintenance and owner epoch.
        let after=authority.read_head(&self.partition_id).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if after!=head {return Err(RecordError::RecoveryRequired);}
        Ok(())
    }
    async fn upload_files(&self,authority:&mut crate::storage::sqlite_authority::SqliteAuthority,
        root:&Path,closure:Vec<(String,u64,[u8;32])>,authority_budget:u64,prior:&[&File])
        ->Result<(Vec<File>,crate::storage::build_budget::MemoryLease),RecordError> {
        admit_checkpoint_chunks(&closure,self.max_object_bytes,self.max_tail_blocks)?;
        let metadata=closure.iter().try_fold(0u64,|sum,(path,length,_)| {
            let chunks=length.div_ceil(self.max_object_bytes as u64);
            sum.checked_add(path.len() as u64).and_then(|n|n.checked_add(256))
                .and_then(|n|chunks.checked_mul(512).and_then(|c|n.checked_add(c)))
                .ok_or_else(||storage("upload inventory size overflow"))
        })?.checked_mul(32).ok_or_else(||storage("upload inventory charge overflow"))?;
        let lease=crate::storage::build_budget::reserve_memory(metadata).map_err(|_|RecordError::Busy)?;
        let attempt=rand::random::<u128>();
        let mut files = Vec::new();
        let mut total = 0u64;
        let mut chunk_count = 0usize;
        admit_checkpoint_chunks(&closure, self.max_object_bytes, self.max_tail_blocks)?;
        for (path, length, sha256) in closure {
            total = total
                .checked_add(length)
                .filter(|n| {
                    *n <= authority_budget
                        .saturating_mul(64)
                        .saturating_add(32 * 1024 * 1024)
                })
                .ok_or_else(|| storage("checkpoint upload budget exceeded"))?;
            let target = safe_path(root, &path)?;
            let mut input = std::fs::File::open(target).map_err(storage)?;
            let mut chunks = Vec::new();
            let mut hash = Sha256::new();
            let mut actual = 0u64;
            loop {
                let _buffer = crate::resources::io_scope(crate::resources::Lane::Background)
                    .bytes((self.max_object_bytes as u64).min(length.saturating_sub(actual)).saturating_mul(2))
                    .map_err(|_| RecordError::Busy)?;
                let mut bytes = Vec::new();
                {
                    let _io = crate::resources::io_scope(crate::resources::Lane::Background).io().map_err(|_|RecordError::Busy)?;
                    (&mut input)
                        .take(self.max_object_bytes as u64)
                        .read_to_end(&mut bytes)
                        .map_err(storage)?;
                }
                if bytes.is_empty() {
                    break;
                }
                hash.update(&bytes);
                actual = actual
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| storage("checkpoint length overflow"))?;
                if actual > length {
                    return Err(storage("checkpoint file grew during export"));
                }
                let digest = Sha256::digest(&bytes)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                let reference=prior.iter().find(|f|f.path==path && f.length==length && f.sha256==sha256)
                    .and_then(|f|f.chunks.get(chunks.len()));
                let object=if let Some(reference)=reference {
                    if reference.length!=bytes.len() as u64 || reference.sha256!=<[u8;32]>::from(Sha256::digest(&bytes)) {
                        return Err(storage("reused chunk boundary mismatch"));
                    }
                    let verified=self.store.uncached().get_verified(reference).await.map_err(remote_read_error)?;
                    if verified!=bytes {return Err(storage("reused chunk content mismatch"));}
                    reference.clone()
                } else {
                    // Unselected failed attempts may be retired by GC. A fresh
                    // attempt never tries to resurrect their immutable keys.
                    let key=format!("partitions/{}/files/{attempt:032x}/{}/{:08}-{digest}",self.partition_id,path,chunks.len());
                    self.store.put_and_register(authority,&key,&bytes,unix_time_ms_i64()).await.map_err(storage)?
                };
                chunks.push(object);
                chunk_count=chunk_count.checked_add(1).ok_or_else(||storage("checkpoint chunk count overflow"))?;
                if chunk_count > self.max_tail_blocks {
                    return Err(storage("checkpoint chunk budget exceeded"));
                }
            }
            let actual_hash: [u8; 32] = hash.finalize().into();
            if actual != length || actual_hash != sha256 {
                return Err(storage("checkpoint export checksum mismatch"));
            }
            files.push(File {
                path,
                length,
                sha256,
                chunks,
            });
        }
        Ok((files,lease))
    }

    pub(super) async fn publish_derived_index(&mut self,root:&Path,config:&RecordConfig,state:&State,
        packages:Vec<segment::Package>,statistics:Vec<crate::active::ActiveStatistics>)->Result<(),RecordError> {
        let mut authority=self.authority.lock().await;
        let head=authority.read_head(&self.partition_id).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if head.owner_epoch!=self.owner_epoch || head.committed_seq!=state.seq {return Err(RecordError::RecoveryRequired);}
        let selected=self.selected(&head,true).await?;
        anchor::Reference::verify_derived_parts(root,config,state,&packages,statistics.clone()).await?;
        let count=packages.iter().try_fold(0usize,|n,p|n.checked_add(p.files.len()).ok_or_else(||storage("index file count overflow")))?;
        let _paths=crate::storage::build_budget::reserve_memory((count as u64).checked_mul(512*32).ok_or_else(||storage("index path budget overflow"))?).map_err(|_|RecordError::Busy)?;
        let closure=packages.iter().flat_map(|p|p.files.iter().map(move|f|(format!("segment-{}/{}",p.id,f.path),f.bytes,f.sha256))).collect();
        let prior=selected.index.iter().flat_map(|i|i.files.iter()).chain(selected.checkpoint.files.iter()).collect::<Vec<_>>();
        let (files,_export)=self.upload_files(&mut authority,root,closure,config.max_journal_bytes,&prior).await?;
        let (_joint,_joint_lease)=combined_files(root,&selected.checkpoint.files,&files,config.max_journal_bytes,self.max_tail_blocks)?;
        drop(_joint);
        let index=IndexOnly{format:2,partition_id:self.partition_id.clone(),checkpoint:selected.checkpoint_ref,
            checkpoint_seq:head.log_base_seq,indexed_seq:state.seq,wal_tip:head.wal_tip.clone(),
            state_sha:checkpoint_codec::json_digest(state)?,config_sha:checkpoint_codec::json_digest(config)?,packages,statistics,files};
        let length=checkpoint_codec::json_length(&index)?;
        if length>self.max_object_bytes {return Err(RecordError::Rejected("index-only descriptor exceeds object budget".into()));}
        let _encoded=crate::storage::build_budget::reserve_memory((length as u64).checked_mul(2).ok_or_else(||storage("index descriptor overflow"))?).map_err(|_|RecordError::Busy)?;
        let bytes=serde_json::to_vec(&index).map_err(storage)?;
        let key=format!("partitions/{}/indexes/{:032x}",self.partition_id,rand::random::<u128>());
        let object=self.store.put_and_register(&mut authority,&key,&bytes,unix_time_ms_i64()).await.map_err(storage)?;
        authority.register_object_edge(&object,&index.checkpoint).map_err(storage)?;
        if let Some(tip)=&index.wal_tip {authority.register_object_edge(&object,tip).map_err(storage)?;}
        for file in &index.files {for child in &file.chunks {authority.register_object_edge(&object,child).map_err(storage)?;}}
        #[cfg(test)] if root.join(".test-index-only-before-cas").exists(){return Err(storage("injected index-only rejection before CAS"));}
        let selected=authority.publish_index(&self.partition_id,head.head_version,self.owner_epoch,state.seq,object)
            .map_err(|e|RecordError::OutcomeUnknown(e.to_string()))?.ok_or(RecordError::RecoveryRequired)?;
        if selected.log_base_seq!=head.log_base_seq || selected.wal_tip!=head.wal_tip || selected.log_anchor!=head.log_anchor {
            return Err(RecordError::RecoveryRequired);
        }
        #[cfg(test)] if root.join(".test-index-only-after-cas").exists(){return Err(RecordError::OutcomeUnknown("injected index-only response loss".into()));}
        Ok(())
    }

    pub(super) async fn publish_anchor(
        &mut self,
        root: &Path,
        reference: &anchor::Reference,
        payloads: &crate::storage::payload_pool::Closure,
        seq: u64,
        authority_budget: u64,
        restoring: bool,
    ) -> Result<(), RecordError> {
        let mut authority = self.authority.lock().await;
        let head = authority
            .read_head(&self.partition_id)
            .map_err(storage)?
            .ok_or(RecordError::RecoveryRequired)?;
        if head.owner_epoch != self.owner_epoch || (!restoring && head.committed_seq != seq) {
            return Err(RecordError::RecoveryRequired);
        }
        let closure=reference.remote_files(payloads)?;
        let selected=if head.manifest.is_some(){Some(self.selected(&head,true).await?)}else{None};
        let prior=selected.iter().flat_map(|s|s.index.iter().flat_map(|i|i.files.iter()).chain(s.checkpoint.files.iter())).collect::<Vec<_>>();
        let (files,_export_metadata)=self.upload_files(&mut authority,root,closure,authority_budget,&prior).await?;
        let checkpoint = Checkpoint {
            format: 1,
            partition_id: self.partition_id.clone(),
            seq,
            anchor: reference.clone(),
            files,
        };
        let bytes = serde_json::to_vec(&checkpoint).map_err(storage)?;
        let key = format!(
            "partitions/{}/checkpoints/{}",
            self.partition_id,
            reference.identity()
        );
        let manifest = self
            .store
            .put_and_register(&mut authority, &key, &bytes, unix_time_ms_i64())
            .await
            .map_err(storage)?;
        for file in &checkpoint.files {
            for child in &file.chunks {
                authority
                    .register_object_edge(&manifest, child)
                    .map_err(storage)?;
            }
        }
        #[cfg(test)]
        if root.join(".test-remote-before-checkpoint").exists() {
            return Err(storage("injected before remote checkpoint publication"));
        }
        #[cfg(feature = "fault-injection")]
        let fault_partition = std::env::var("ANYSEEK_REMOTE_CHECKPOINT_PARTITION")
            .map(|id| id == self.partition_id).unwrap_or(true);
        #[cfg(feature = "fault-injection")]
        if fault_partition && std::env::var("ANYSEEK_REMOTE_CHECKPOINT_FAULT").as_deref() == Ok("before_publish") {
            std::process::exit(91);
        }
        // This CAS also certifies same-sequence maintenance such as feed trimming.
        // Never discard predecessor edges: retained backups still traverse them.
        let published = if restoring {
            authority.publish_restored_checkpoint(
                &self.partition_id,
                head.head_version,
                self.owner_epoch,
                seq,
                manifest,
            )
        } else {
            authority.publish_checkpoint(
                &self.partition_id,
                head.head_version,
                self.owner_epoch,
                seq,
                manifest,
            )
        };
        published
            .map_err(storage)?
            .ok_or(RecordError::RecoveryRequired)?;
        #[cfg(feature = "fault-injection")]
        if fault_partition && std::env::var("ANYSEEK_REMOTE_CHECKPOINT_FAULT").as_deref() == Ok("after_publish") {
            std::process::exit(91);
        }
        #[cfg(test)]
        if root.join(".test-remote-after-checkpoint").exists() {
            return Err(RecordError::DurablePending(seq));
        }
        Ok(())
    }

    /// Materialize only the authority-selected checkpoint. Called with the local
    /// journal owner lock held and without mapped index handles. An interrupted
    /// download leaves private temp files, never a selected partial anchor.
    pub(super) async fn materialize_checkpoint(
        &mut self,
        root: &Path,
        budget: u64,
    ) -> Result<Option<Vec<u8>>, RecordError> {
        let authority = self.authority.lock().await;
        let head = authority
            .read_head(&self.partition_id)
            .map_err(storage)?
            .ok_or(RecordError::RecoveryRequired)?;
        if head.owner_epoch != self.owner_epoch {
            return Err(RecordError::RecoveryRequired);
        }
        if head.manifest.is_none(){return Ok(None);}
        let mut selected=self.selected(&head,false).await?;
        let checkpoint=&selected.checkpoint;
        let (files,_inventory)=combined_files(root,&checkpoint.files,selected.index.as_ref().map_or(&[],|i|i.files.as_slice()),budget,self.max_tail_blocks)?;
        for file in files.values() {
            let target = safe_path(root, &file.path)?;
            std::fs::create_dir_all(
                target
                    .parent()
                    .ok_or_else(|| storage("checkpoint parent missing"))?,
            )
            .map_err(storage)?;
            let temporary = root.join(format!(".remote-download-{:032x}", rand::random::<u128>()));
            let result = async {
                let mut output = std::fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&temporary)
                    .map_err(storage)?;
                let mut hash = Sha256::new();
                for chunk in &file.chunks {
                    let bytes = self.store.get_verified(chunk).await.map_err(storage)?;
                    hash.update(&bytes);
                    output.write_all(&bytes).map_err(storage)?;
                }
                let actual: [u8; 32] = hash.finalize().into();
                if actual != file.sha256 {
                    return Err(storage("checkpoint download checksum mismatch"));
                }
                output.sync_all().map_err(storage)?;
                drop(output);
                // Local files are discardable cache. The owner lock excludes readers.
                if target.exists() {
                    std::fs::remove_file(&target).map_err(storage)?;
                }
                std::fs::rename(&temporary, &target).map_err(storage)
            }
            .await;
            if result.is_err() {
                let _ = std::fs::remove_file(&temporary);
            }
            result?;
        }
        // Verify the complete existing anchor/index closure before publishing even
        // the rebuildable local certificate. This uses the normal Segment readers.
        let loaded = checkpoint.anchor.load(root, budget).await?;
        if loaded.state.seq != checkpoint.seq {
            return Err(storage("checkpoint state sequence mismatch"));
        }
        drop(files);
        if let Some(index)=selected.index.as_mut() {
            let committed_tip=authority.committed_wal_at(&self.partition_id,index.indexed_seq).map_err(storage)?;
            self.verify_index_cut(root,index,&head,committed_tip,&loaded.config,&loaded.state).await?;
        }
        let payload=encode_entry(&Entry::Anchor(checkpoint.anchor.clone()))?;
        drop(loaded);
        let after=authority.read_head(&self.partition_id).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if after!=head {return Err(RecordError::RecoveryRequired);}
        Ok(Some(payload))
    }

    /// Only this single owner invokes GC under the partition guard, after all
    /// uploads/builds have completed. Persistent pins protect external consumers.
    pub(super) async fn collect_objects(&mut self) -> Result<usize, RecordError> {
        let mut authority = self.authority.lock().await;
        let head = authority
            .read_head(&self.partition_id)
            .map_err(storage)?
            .ok_or(RecordError::RecoveryRequired)?;
        if head.owner_epoch != self.owner_epoch {
            return Err(RecordError::RecoveryRequired);
        }
        for object in authority.unreferenced_objects().map_err(storage)? {
            authority
                .retire_object(&object.key, head.head_version, unix_time_ms_i64())
                .map_err(storage)?;
        }
        let objects = authority
            .collectible_objects(unix_time_ms_i64())
            .map_err(storage)?;
        for object in &objects {
            self.store
                .delete_retired(&mut authority, &object.key, unix_time_ms_i64())
                .await
                .map_err(storage)?;
        }
        // Tombstones are retried too: an ambiguous old PUT may arrive after a
        // prior DELETE. It can never be registered or published again.
        Ok(objects.len())
    }
}

impl RecordPartition {
    /// Restore into a new object namespace. Borrowing another authority's object
    /// graph would let either GC delete the other owner's live data; instead copy
    /// verified canonical files and publish a new complete checkpoint before use.
    pub(super) async fn restore_s3_with_remote(
        path: &Path,
        config: RecordConfig,
        mut target: RemotePersistence,
        bundle_json: &str,
        max_snapshot_bytes: usize,
        max_objects: usize,
        max_total_object_bytes: usize,
    ) -> Result<Self, RecordError> {
        use crate::storage::{s3_compatible::BackupBundle, sqlite_authority::SqliteAuthority};
        let bundle: BackupBundle = serde_json::from_str(bundle_json).map_err(storage)?;
        target
            .authority
            .lock()
            .await
            .bind_restore(bundle.descriptor_ref.sha256)
            .map_err(storage)?;
        let existing = target
            .authority
            .lock()
            .await
            .read_head(&target.partition_id)
            .map_err(storage)?;
        if existing.as_ref().is_some_and(|h| h.manifest.is_some()) {
            return Self::open_s3_with_remote(path, config.max_journal_bytes, target).await;
        }
        // Use a disposable import authority for closure validation/replay only.
        // It never runs GC, advances the source head, or serves requests.
        let source_path = target
            .authority
            .lock()
            .await
            .database_path()
            .with_extension("restore-source.sqlite");
        let mut source_db = SqliteAuthority::open(&source_path, std::time::Duration::from_secs(5))
            .map_err(storage)?;
        let snapshot = target
            .store
            .restore_backup_bundle(
                &mut source_db,
                &bundle.descriptor_ref,
                max_snapshot_bytes,
                max_objects,
                max_total_object_bytes,
            )
            .await
            .map_err(storage)?;
        if snapshot.heads.len() != 1 {
            return Err(storage("collection restore requires one partition"));
        }
        let source_id = snapshot.heads[0].partition_id.clone();
        if source_id == target.partition_id {
            return Err(RecordError::Rejected(
                "restore requires a new partition/object namespace".into(),
            ));
        }
        let source = RemotePersistence {
            store: target.store.clone(),
            authority: Mutex::new(source_db),
            partition_id: source_id,
            owner_epoch: snapshot.heads[0].owner_epoch,
            max_tail_blocks: target.max_tail_blocks,
            max_tail_bytes: target.max_tail_bytes,
            max_object_bytes: target.max_object_bytes,
            _owner: None,
        };
        if path.exists() {
            let mut cache = PartitionJournal::open(path, config.max_journal_bytes)
                .map_err(storage)?
                .0;
            let bootstrap = source
                .store
                .get_verified(
                    snapshot.heads[0]
                        .bootstrap
                        .as_ref()
                        .ok_or(RecordError::RecoveryRequired)?,
                )
                .await
                .map_err(storage)?;
            cache.checkpoint(&bootstrap).map_err(creation_error)?;
        }
        let recovered = Self::open_s3_with_remote(path, config.max_journal_bytes, source).await?;
        {
            let mut inner = recovered.inner.lock().await;
            // Drop the disposable source adapter BEFORE producing any maintenance.
            // All data/receipt identities survive; old deletion history is untrusted.
            inner.remote = None;
            let mut next = inner.state.try_clone()?;
            next.independent_snapshot = true;
            next.evidence_quarantined |= next.records.values().any(temporal::protected);
            next.feed.scope = Sha256::digest(format!("restore:{}", target.partition_id))
                .iter()
                .take(16)
                .map(|b| format!("{b:02x}"))
                .collect();
            inner.publish_anchor_state(next, false).await?;
            let mut initial = State::default();
            initial.lineage = inner.state.lineage.clone();
            initial.feed = change_feed::FeedState {
                scope: inner.state.feed.scope.clone(),
                floor: 0,
            };
            let bootstrap = encode_entry(&Entry::Checkpoint {
                config: inner.config.clone(),
                state: initial,
            })?;
            {
                let mut db = target.authority.lock().await;
                if db
                    .read_head(&target.partition_id)
                    .map_err(storage)?
                    .is_none()
                {
                    db.create_partition(&target.partition_id, target.owner_epoch)
                        .map_err(storage)?;
                }
                let h = db
                    .read_head(&target.partition_id)
                    .map_err(storage)?
                    .ok_or(RecordError::RecoveryRequired)?;
                if h.bootstrap.is_none() {
                    target
                        .store
                        .publish_bootstrap(
                            &mut db,
                            &target.partition_id,
                            target.owner_epoch,
                            &bootstrap,
                            unix_time_ms_i64(),
                        )
                        .await
                        .map_err(storage)?;
                }
            }
            target
                .publish_anchor(
                    inner.journal.root(),
                    inner.anchor.as_ref().ok_or(RecordError::RecoveryRequired)?,
                    &inner.payloads,
                    inner.state.seq,
                    inner.config.max_journal_bytes,
                    true,
                )
                .await?;
            inner.remote = Some(target);
            inner.feed_cache =
                change_feed::Cache::rebuild(&inner.state, &inner.config.change_feed)?;
        }
        for file in [
            &source_path,
            &source_path.with_extension("sqlite-wal"),
            &source_path.with_extension("sqlite-shm"),
        ] {
            let _ = std::fs::remove_file(file);
        }
        // A process can restart at every earlier boundary. The target head is the
        // durable completion receipt; retries never import/rewind it again.
        Ok(recovered)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn checkpoint_object_admission_counts_every_native_file_and_partial_chunk() {
        let files = vec![("a".into(), 0, [0; 32]), ("b".into(), 4, [0; 32]), ("c".into(), 5, [0; 32])];
        assert!(super::admit_checkpoint_chunks(&files, 4, 3).is_ok());
        assert!(super::admit_checkpoint_chunks(&files, 4, 2).is_err());
        assert!(super::admit_checkpoint_chunks(&files, 0, 100).is_err());
    }

    use super::super::tests::{config, path, query, write, source_write};
    use super::*;
    use crate::storage::{s3_compatible::S3CompatibleStore, sqlite_authority::SqliteAuthority};
    use object_store::memory::InMemory;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Clock(AtomicU64);
    impl temporal::Clock for Clock {
        fn now_ms(&self) -> Result<u64, RecordError> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }
    fn remote(store: Arc<InMemory>, db: &Path, id: &str) -> RemotePersistence {
        RemotePersistence {
            store: S3CompatibleStore::from_store(store, 1024 * 1024),
            authority: Mutex::new(
                SqliteAuthority::open(db, std::time::Duration::from_secs(1)).unwrap(),
            ),
            partition_id: id.into(),
            owner_epoch: 1,
            max_tail_blocks: 10000,
            max_tail_bytes: 16 * 1024 * 1024,
            max_object_bytes: 1024 * 1024,
            _owner: None,
        }
    }
    async fn source() -> (RecordPartition, Arc<InMemory>, PathBuf, PathBuf, Arc<Clock>) {
        source_config(config()).await
    }
    async fn source_config(settings:RecordConfig) -> (RecordPartition, Arc<InMemory>, PathBuf, PathBuf, Arc<Clock>) {
        let root = path();
        let db = path();
        let store = Arc::new(InMemory::new());
        let p = RecordPartition::create(&root, settings).await.unwrap();
        let remote = remote(store.clone(), &db, "source");
        let mut inner = p.inner.lock().await;
        let initial = encode_entry(&Entry::Checkpoint {
            config: inner.config.clone(),
            state: inner.state.clone(),
        })
        .unwrap();
        let mut authority = remote.authority.lock().await;
        authority.create_partition("source", 1).unwrap();
        remote
            .store
            .publish_bootstrap(&mut authority, "source", 1, &initial, unix_time_ms_i64())
            .await
            .unwrap();
        drop(authority);
        let clock = Arc::new(Clock(AtomicU64::new(unix_time_ms_i64() as u64)));
        inner.clock = clock.clone();
        inner.remote = Some(remote);
        drop(inner);
        (p, store, root, db, clock)
    }
    #[test]
    fn joint_closure_admits_once_and_rejects_duplicate_or_over_budget() {
        let root=path();
        let file=|name:&str,n:u64|File{path:format!("segment-{:032x}/{name}",1),length:n,sha256:[0;32],chunks:vec![ObjectRef{key:name.into(),sha256:[0;32],length:n}]};
        let base=vec![file("a",1)];let index=vec![file("a",1),file("b",1)];
        assert_eq!(combined_files(&root,&base,&index,1,2).unwrap().0.len(),2);
        assert!(matches!(combined_files(&root,&base,&index,1,1),Err(RecordError::Rejected(_))));
        assert!(combined_files(&root,&base,&[file("a",1),file("a",1)],1,3).is_err());
        assert!(combined_files(&root,&base,&[file("a",2)],1,3).is_err());
        let limit=32*1024*1024+64;
        assert!(combined_files(&root,&[],&[file("large",limit)],1,1).is_ok());
        assert!(matches!(combined_files(&root,&[],&[file("large",limit+1)],1,1),Err(RecordError::Rejected(_))));
    }
    #[tokio::test]
    async fn index_only_cas_faults_preserve_selected_cut_and_receipts() {
        for after in [false,true] {
            let (p,_,root,_,clock)=source().await;
            p.write(write("seed","seed","alpha",ExpectedVersion::Absent)).await.unwrap();
            p.compact_journal().await.unwrap();
            let request=write("tail","tail","alpha tail",ExpectedVersion::Absent);
            let receipt=p.write(request.clone()).await.unwrap();
            let fault=if after {".test-index-only-after-cas"}else{".test-index-only-before-cas"};
            std::fs::write(root.join(fault),b"fail").unwrap();
            {
                let mut inner=p.inner.lock().await;
                assert!(inner.maintain_index_tail().await.is_err());
                assert_eq!(inner.healthy,!after);
                let remote=inner.remote.as_ref().unwrap();
                let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();
                assert_eq!(head.log_base_seq,1);assert_eq!(head.committed_seq,2);
                assert_eq!(head.indexed_seq,if after {2}else{1});
            }
            std::fs::remove_file(root.join(fault)).unwrap();
            if !after {
                p.collect_s3_garbage().await.unwrap();
                p.inner.lock().await.publish_remote_index_tail().await.unwrap();
                assert_eq!(p.write(request.clone()).await.unwrap(),receipt);
            }
            let p=cold(p,&root,clock).await;
            assert_eq!(p.write(request).await.unwrap(),receipt);
            assert_eq!(p.statistics().await.unwrap().visible_seq,2);
        }
    }
    #[tokio::test]
    async fn index_only_backup_gc_and_same_sequence_checkpoint_preserve_history() {
        let (p,store,root,_,clock)=source().await;
        p.write(write("seed","seed","alpha seed",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let request=write("tail","tail","alpha tail",ExpectedVersion::Absent);
        let receipt=p.write(request.clone()).await.unwrap();
        p.inner.lock().await.maintain_index_tail().await.unwrap();
        {
            let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();
            let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();
            assert_eq!((head.log_base_seq,head.indexed_seq,head.committed_seq),(1,2,2));
        }
        let expected=scores(&p).await;
        let (bundle,seq,_)=p.create_s3_backup("index-cut",100,1024*1024).await.unwrap();
        assert_eq!(seq,2);
        {
            let mut inner=p.inner.lock().await;
            let future=clock.0.load(Ordering::SeqCst)+inner.config.change_feed.retention_ms+1;
            clock.0.store(future,Ordering::SeqCst);inner.trim_feed(future).await.unwrap();
            assert!(inner.state.feed.floor>0);
            let remote=inner.remote.as_ref().unwrap();let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();
            assert_eq!((head.log_base_seq,head.indexed_seq,head.committed_seq),(2,2,2));
            assert!(head.wal_tip.is_none());
        }
        p.write(write("later","later","alpha later",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        p.collect_s3_garbage().await.unwrap();
        let target=path();let db=path();
        let restored=RecordPartition::restore_s3_with_remote(&target,config(),remote(store,&db,"restored"),&bundle,1024*1024,10000,16*1024*1024).await.unwrap();
        restored.inner.lock().await.clock=clock.clone();
        assert_eq!(scores(&restored).await,expected);
        assert_eq!(restored.write(request).await.unwrap(),receipt);
        p.release_s3_backup_pins("index-cut").await.unwrap();
        p.collect_s3_garbage().await.unwrap();
        let restored=cold(restored,&target,clock.clone()).await;
        assert_eq!(scores(&restored).await,expected);
        let p=cold(p,&root,clock).await;
        assert_eq!(p.statistics().await.unwrap().visible_seq,3);
    }
    #[tokio::test]
    async fn background_maintenance_rejects_corrupt_selected_remote_inputs() {
        use object_store::ObjectStore;
        for damaged in ["manifest","payload","wal"] {
            let (p,backend,_root,_db,_clock)=source().await;
            p.write(write("seed","seed","alpha",ExpectedVersion::Absent)).await.unwrap();
            p.compact_journal().await.unwrap();
            p.write(write("tail","tail","beta",ExpectedVersion::Absent)).await.unwrap();
            let (before,target)={let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();
                let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();
                let target=match damaged {
                    "manifest"=>head.manifest.as_ref().unwrap().clone(),
                    "wal"=>head.wal_tip.as_ref().unwrap().clone(),
                    _=>remote.selected(&head,true).await.unwrap().checkpoint.files.iter()
                        .find(|file|file.path.starts_with("payload-")).unwrap().chunks[0].clone(),
                };(head,target)
            };
            backend.put(&object_store::path::Path::from(target.key),object_store::PutPayload::from_static(b"corrupt")).await.unwrap();
            p.inner.lock().await.request_checkpoint("test_remote_source");p.kick_maintenance().await;
            tokio::time::timeout(std::time::Duration::from_secs(20),async {
                while p.inner.lock().await.maintenance_scheduled {tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
            }).await.unwrap();
            let inner=p.inner.lock().await;assert!(!inner.healthy,"{damaged}");assert!(inner.output.read().unwrap().fenced);
            assert_eq!(inner.state.seq,2);assert!(inner.maintenance_requested.is_some());
            let remote=inner.remote.as_ref().unwrap();
            let after=remote.authority.lock().await.read_head("source").unwrap().unwrap();
            assert_eq!(after,before,"{damaged}");
        }
    }
    #[tokio::test]
    async fn background_same_sequence_feed_trim_installs_and_cold_recovers() {
        let (p,_,root,_,clock)=source().await;
        let request=write("seed","seed","alpha",ExpectedVersion::Absent);
        let receipt=p.write(request.clone()).await.unwrap();
        let future=clock.0.load(Ordering::SeqCst)+p.inner.lock().await.config.change_feed.retention_ms+1;
        clock.0.store(future,Ordering::SeqCst);
        assert!(matches!(p.advance_time().await,Err(RecordError::Busy)));
        tokio::time::timeout(std::time::Duration::from_secs(20),async {
            while p.inner.lock().await.maintenance_scheduled {tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
        }).await.unwrap();
        {let inner=p.inner.lock().await;
            assert_eq!(inner.state.seq,receipt.seq);assert_eq!(inner.state.feed.floor,receipt.seq);
            assert!(inner.maintenance_trim.is_none());assert!(inner.maintenance_requested.is_none());
            let remote=inner.remote.as_ref().unwrap();let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();
            assert_eq!((head.log_base_seq,head.indexed_seq,head.committed_seq),(1,1,1));assert!(head.wal_tip.is_none());
        }
        let p=cold(p,&root,clock).await;
        assert_eq!(p.inner.lock().await.state.feed.floor,receipt.seq);
        assert_eq!(p.write(request).await.unwrap(),receipt);
    }
    #[tokio::test]
    async fn resource_refused_write_keeps_head_receipt_and_retry_classification() {
        use crate::resources::{Governor, Lane};
        let (p,_,_,_,_) = source().await;
        let governor = Arc::new(Governor::default());
        let scope = governor.admit("a".into(), Lane::Foreground, 0).unwrap();
        let request = write("resource-refused", "a", "alpha", ExpectedVersion::Absent);
        let first = scope.io().unwrap(); let second = scope.io().unwrap();
        assert!(matches!(scope.clone().run(p.write(request.clone())).await, Err(RecordError::Busy)));
        assert_eq!(p.statistics().await.unwrap().visible_seq, 0);
        assert!(p.operation(&request.operation_id).await.unwrap().is_none());
        {
            let inner = p.inner.lock().await;
            let remote = inner.remote.as_ref().unwrap();
            assert_eq!(remote.authority.lock().await.read_head("source").unwrap().unwrap().committed_seq, 0);
        }
        drop(first);drop(second);
        let receipt = scope.clone().run(p.write(request.clone())).await.unwrap();
        assert_eq!(receipt.seq, 1);
        assert_eq!(scope.run(p.write(request)).await.unwrap(), receipt);
    }
    #[tokio::test]
    async fn remote_seal_keeps_checkpoint_and_cold_replays_pending_packages() {
        let mut settings=config();settings.max_records=256;settings.max_operations=256;
        let (p,_store,root,_db,clock)=source_config(settings).await;
        p.write(write("seed","seed","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let before={let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();head};
        let mut receipts=Vec::new();
        for i in 0..129 {let request=write(&format!("op{i}"),&format!("r{i}"),"alpha",ExpectedVersion::Absent);let receipt=p.write(request.clone()).await.unwrap();receipts.push((request,receipt));}
        {let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();
            assert_eq!(head.indexed_seq,129);assert_eq!(head.log_base_seq,before.log_base_seq);assert_ne!(head.manifest,before.manifest);
            assert_eq!(head.committed_seq,130);assert!(!inner.index.pending_seals.is_empty());
            let selected=remote.selected(&head,true).await.unwrap();assert_eq!(selected.checkpoint_ref,before.manifest.clone().unwrap());assert!(selected.index.is_some());}
        let expected=scores(&p).await;
        let p=cold(p,&root,clock).await;
        assert_eq!(scores(&p).await,expected);
        for (request,receipt) in receipts {assert_eq!(p.write(request).await.unwrap(),receipt);}
    }

    #[tokio::test]
    async fn remote_tail_budget_checkpoints_before_commit_and_rejects_oversize() {
        let (p,_store,root,_db,clock)=source().await;
        p.inner.lock().await.remote.as_mut().unwrap().max_tail_blocks=64;
        p.write(write("seed","seed","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        // Keep checkpoint chunk budget viable while reducing the independent
        // WAL byte budget to less than two ordinary operation envelopes.
        let request=write("first","a","alpha",ExpectedVersion::Absent);
        let receipt=p.write(request.clone()).await.unwrap();
        let first_bytes={let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();let authority=remote.authority.lock().await;let head=authority.read_head("source").unwrap().unwrap();authority.retained_tail_size(&head).unwrap().1};
        p.inner.lock().await.remote.as_mut().unwrap().max_tail_bytes=first_bytes as usize+200;
        p.write(write("second","b","alpha",ExpectedVersion::Absent)).await.unwrap();
        {let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();let authority=remote.authority.lock().await;let head=authority.read_head("source").unwrap().unwrap();assert_eq!(head.log_base_seq,2);assert_eq!(head.committed_seq,3);assert_eq!(authority.retained_tail_size(&head).unwrap().0,1);}
        let p=cold(p,&root,clock).await;
        assert_eq!(p.write(request).await.unwrap(),receipt);
        let before=p.statistics().await.unwrap().visible_seq;
        p.inner.lock().await.remote.as_mut().unwrap().max_tail_bytes=1;
        assert!(matches!(p.write(write("too-big","c","alpha",ExpectedVersion::Absent)).await,Err(RecordError::Rejected(_))));
        assert_eq!(p.statistics().await.unwrap().visible_seq,before);
        assert!(p.inner.lock().await.healthy);
    }

    #[tokio::test]
    async fn remote_seal_resource_refusal_keeps_writer_and_corruption_fences() {
        use crate::resources::{Governor,Lane};
        let (p,backend,_root,_db,_clock)=source().await;
        p.write(write("seed","seed","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        p.write(write("tail","tail","alpha",ExpectedVersion::Absent)).await.unwrap();
        let governor=Arc::new(Governor::default());let scope=governor.admit("seal".into(),Lane::Foreground,0).unwrap();
        let one=scope.io().unwrap();let two=scope.io().unwrap();
        let result=scope.run(async {p.inner.lock().await.maintain_index_tail().await}).await;
        assert!(matches!(result,Err(RecordError::Busy)));assert!(p.inner.lock().await.healthy);
        drop(one);drop(two);
        p.write(write("after-busy","third","alpha",ExpectedVersion::Absent)).await.unwrap();
        let tip={let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();head.wal_tip.unwrap()};
        use object_store::ObjectStore;
        backend.put(&object_store::path::Path::from(tip.key),object_store::PutPayload::from_static(b"corrupt")).await.unwrap();
        assert!(p.inner.lock().await.maintain_index_tail().await.is_err());
        assert!(!p.inner.lock().await.healthy);
    }

    #[tokio::test]
    async fn remote_tail_count_pressure_preserves_all_receipts() {
        let (p,_store,root,_db,clock)=source().await;
        p.inner.lock().await.remote.as_mut().unwrap().max_tail_blocks=64;
        let mut receipts=Vec::new();
        for i in 0..65 {let request=write(&format!("op{i}"),"same","alpha",if i==0 {ExpectedVersion::Absent}else{ExpectedVersion::Match(i)});let receipt=p.write(request.clone()).await.unwrap();receipts.push((request,receipt));}
        {let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();let authority=remote.authority.lock().await;let head=authority.read_head("source").unwrap().unwrap();assert_eq!(head.committed_seq,65);assert_eq!(head.log_base_seq,64);assert_eq!(authority.retained_tail_size(&head).unwrap().0,1);}
        let p=cold(p,&root,clock).await;
        for (request,receipt) in receipts {assert_eq!(p.write(request).await.unwrap(),receipt);}
    }

    async fn cold(p: RecordPartition, root: &Path, clock: Arc<Clock>) -> RecordPartition {
        let remote = p.inner.lock().await.remote.take().unwrap();
        drop(p);
        std::fs::remove_dir_all(root).unwrap();
        let p = RecordPartition::open_s3_with_remote(root, config().max_journal_bytes, remote)
            .await
            .unwrap();
        p.inner.lock().await.clock = clock;
        p
    }
    async fn scores(p: &RecordPartition) -> Vec<serde_json::Value> {
        let mut results = Vec::new();
        for mode in [
            SearchMode::Lexical,
            SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: crate::vector_similarity::AnnMode::All,
            },
            SearchMode::Hybrid {
                similarity_threshold: None,
                ann_mode: crate::vector_similarity::AnnMode::All,
            },
        ] {
            let vector = if mode == SearchMode::Lexical {
                None
            } else {
                Some(Embedding::F32(vec![1.0, 0.0]))
            };
            let result = p
                .query(RecordQuery {
                    temporal: None,
                    text: "alpha".into(),
                    vector,
                    mode,
                    query_type: QueryType::Union,
                    offset: 0,
                    length: 10,
                    result_type: ResultType::TopkCount,
                    fields: vec![],
                    filters: vec![],
                    sort: vec![],
                })
                .await
                .unwrap();
            results.push(serde_json::json!({"count":result.count_total,"hits":result.hits.into_iter()
                .map(|h|serde_json::json!([h.record.record_id,h.record.version,h.record.document,h.score])).collect::<Vec<_>>() }));
        }
        results
    }
    #[tokio::test]
    async fn cache_corruption_and_loss_preserve_checkpoint_tail_and_hidden_evidence() {
        let (p,_,root,db,clock) = source().await;
        p.write(source_write("source","s",&[])).await.unwrap();
        p.write(source_write("derived","d",&[("s",1)])).await.unwrap();
        p.compact_journal().await.unwrap();
        let request = write("tail","a","alpha tail",ExpectedVersion::Absent);
        let receipt = p.write(request.clone()).await.unwrap();
        let mut deletion = write("delete-source","s","",ExpectedVersion::Match(1)); deletion.document = None;
        p.write(deletion).await.unwrap();
        let expected = scores(&p).await;
        let cache_root = db.with_extension("object-cache");
        {
            let mut inner = p.inner.lock().await;
            let remote = inner.remote.as_mut().unwrap();
            remote.store = remote.store.clone().with_cache(crate::storage::verified_cache::VerifiedCache::new(
                cache_root.clone(), &["fixture", "bucket"], 16 * 1024 * 1024,
            ));
        }
        let p = cold(p, &root, clock.clone()).await;
        assert_eq!(scores(&p).await, expected);
        let mut damaged = 0;
        for row in std::fs::read_dir(&cache_root).unwrap() {
            let row = row.unwrap();
            if row.file_name().to_string_lossy().starts_with("block-") {
                std::fs::write(row.path(), b"corrupted cached block").unwrap(); damaged += 1;
            }
        }
        assert!(damaged > 0);
        let p = cold(p, &root, clock.clone()).await;
        assert_eq!(scores(&p).await, expected);
        assert!(p.get("d").await.unwrap().is_none());
        assert_eq!(p.write(request.clone()).await.unwrap(), receipt);
        std::fs::remove_dir_all(&cache_root).unwrap();
        let p = cold(p, &root, clock).await;
        assert_eq!(scores(&p).await, expected);
        assert!(p.get("d").await.unwrap().is_none());
        assert_eq!(p.write(request).await.unwrap(), receipt);
        drop(p);
        let _ = std::fs::remove_dir_all(cache_root);
    }

    #[tokio::test]
    async fn background_index_keeps_new_commits_and_hidden_evidence_after_cache_loss() {
        let (p,_,root,_,clock)=source().await;
        p.write(source_write("source","s",&[])).await.unwrap();
        p.write(source_write("derived","d",&[("s",1)])).await.unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_index_committed(std::time::Duration::from_secs(30)).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !root.join(".test-indexer-started").exists() { tokio::time::sleep(std::time::Duration::from_millis(5)).await; }
        }).await.unwrap();
        let request=write("concurrent","a","alpha revised",ExpectedVersion::Absent);
        let receipt=p.write(request.clone()).await.unwrap();
        let mut deletion=write("delete-source","s","",ExpectedVersion::Match(1));deletion.document=None;
        p.write(deletion).await.unwrap();
        let expected=scores(&p).await;
        assert!(matches!(p.collect_s3_garbage().await,Err(RecordError::Busy)));
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        assert_eq!(job.finish().await.unwrap(),2);
        {
            let inner=p.inner.lock().await;let remote=inner.remote.as_ref().unwrap();
            let head=remote.authority.lock().await.read_head("source").unwrap().unwrap();
            assert_eq!((head.indexed_seq,head.committed_seq,head.log_base_seq),(2,4,2));
            assert!(head.wal_tip.is_some());
        }
        p.collect_s3_garbage().await.unwrap();
        let p=cold(p,&root,clock).await;
        assert_eq!(scores(&p).await,expected);
        assert!(p.get("d").await.unwrap().is_none());
        assert_eq!(p.write(request).await.unwrap(),receipt);
        p.compact_journal().await.unwrap();
    }
    #[tokio::test]
    async fn background_index_owner_rejection_during_admission_also_closes_output() {
        let (p,_,_,db,_)=source().await;
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        let output=p.output_permit().unwrap();
        let connection=rusqlite::Connection::open(db).unwrap();
        connection.execute("UPDATE partition_heads SET owner_epoch=2 WHERE partition_id='source'",[]).unwrap();
        drop(connection);
        assert!(matches!(p.index_committed(std::time::Duration::from_secs(30)).await,Err(RecordError::RecoveryRequired)));
        assert!(p.get("a").await.is_err());assert!(output.deliver(||()).is_err());
    }

    #[tokio::test]
    async fn background_index_old_owner_cannot_reopen_output_after_rejected_publication() {
        let (p,_,root,db,_)=source().await;
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        let output=p.output_permit().unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_index_committed(std::time::Duration::from_secs(30)).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !root.join(".test-indexer-started").exists() { tokio::time::sleep(std::time::Duration::from_millis(5)).await; }
        }).await.unwrap();
        // Exercise the authority fence even if an obsolete worker is still alive.
        let connection=rusqlite::Connection::open(db).unwrap();
        connection.execute("UPDATE partition_heads SET owner_epoch=2 WHERE partition_id='source'",[]).unwrap();
        drop(connection);
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        assert!(matches!(job.finish().await,Err(RecordError::RecoveryRequired)));
        assert!(p.get("a").await.is_err());assert!(output.deliver(||()).is_err());
    }

    #[tokio::test]
    async fn expired_remote_index_build_cleans_pins_without_advancing_manifest() {
        let (p,_,root,_,clock)=source().await;
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(root.join(".test-indexer-pause"),b"pause").unwrap();
        let job=p.start_index_committed(std::time::Duration::from_millis(10)).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !root.join(".test-indexer-started").exists() { tokio::time::sleep(std::time::Duration::from_millis(5)).await; }
        }).await.unwrap();
        p.write(write("second","b","alpha",ExpectedVersion::Absent)).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        std::fs::remove_file(root.join(".test-indexer-pause")).unwrap();
        assert!(matches!(job.finish().await,Err(RecordError::Busy)));
        let p=cold(p,&root,clock).await;
        assert!(p.get("a").await.unwrap().is_some());assert!(p.get("b").await.unwrap().is_some());
        assert_eq!(p.index_committed(std::time::Duration::from_secs(30)).await.unwrap(),2);
    }

    #[tokio::test]
    async fn remote_checkpoint_gc_and_cold_tail_keep_all_query_views() {
        let (p, _, root, _, clock) = source().await;
        let original = write("a", "a", "alpha old", ExpectedVersion::Absent);
        let receipt = p.write(original.clone()).await.unwrap();
        p.write(write("b", "b", "alpha beta", ExpectedVersion::Absent))
            .await
            .unwrap();
        let old = {
            let inner = p.inner.lock().await;
            let r = inner.remote.as_ref().unwrap();
            r.authority
                .lock()
                .await
                .read_head("source")
                .unwrap()
                .unwrap()
                .wal_tip
                .unwrap()
        };
        p.compact_journal().await.unwrap();
        {
            let inner = p.inner.lock().await;
            let r = inner.remote.as_ref().unwrap();
            let h = r
                .authority
                .lock()
                .await
                .read_head("source")
                .unwrap()
                .unwrap();
            assert_eq!(h.indexed_seq, 2);
            assert_eq!(h.log_base_seq, 2);
            assert!(h.wal_tip.is_none());
            assert!(r.store.get_verified(&old).await.is_err());
        }
        p.write(write(
            "update",
            "a",
            "alpha latest",
            ExpectedVersion::Match(1),
        ))
        .await
        .unwrap();
        let mut deletion = write("delete", "b", "", ExpectedVersion::Match(1));
        deletion.document = None;
        p.write(deletion).await.unwrap();
        p.write(write("c", "c", "alpha tail", ExpectedVersion::Absent))
            .await
            .unwrap();
        let expected = scores(&p).await;
        let p = cold(p, &root, clock).await;
        assert_eq!(scores(&p).await, expected);
        assert!(p.get("b").await.unwrap().is_none());
        assert_eq!(p.write(original).await.unwrap(), receipt);
        p.compact_journal().await.unwrap();
        assert_eq!(scores(&p).await, expected);
    }
    #[tokio::test]
    async fn remote_feed_floor_survives_cache_loss_and_reclaims_covered_wal() {
        let (p, _, root, _, clock) = source().await;
        p.write(write("first", "a", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        let page = p.changes(None, 10).await.unwrap();
        let cursor = page.next;
        let (seq, floor) = {
            let mut inner = p.inner.lock().await;
            let future = clock.0.load(Ordering::SeqCst) + inner.config.change_feed.retention_ms + 1;
            clock.0.store(future, Ordering::SeqCst);
            inner.trim_feed(future).await.unwrap();
            assert!(inner.state.feed.floor > 0);
            let r = inner.remote.as_ref().unwrap();
            let h = r
                .authority
                .lock()
                .await
                .read_head("source")
                .unwrap()
                .unwrap();
            assert_eq!(h.log_base_seq, inner.state.seq);
            assert!(h.wal_tip.is_none());
            (inner.state.seq, inner.state.feed.floor)
        };
        let p = cold(p, &root, clock).await;
        assert_eq!(p.inner.lock().await.state.feed.floor, floor);
        assert_eq!(p.statistics().await.unwrap().visible_seq, seq);
        // A cursor at the retained floor remains valid; an earlier one is explicit expiry.
        let _ = p.changes(Some(cursor), 10).await;
        p.write(write("next", "b", "alpha new", ExpectedVersion::Absent))
            .await
            .unwrap();
        assert_eq!(p.statistics().await.unwrap().visible_seq, seq + 1);
    }
    #[tokio::test]
    async fn remote_checkpoint_publication_interruptions_recover_only_selected_roots() {
        for boundary in [
            ".test-remote-before-checkpoint",
            ".test-remote-after-checkpoint",
        ] {
            let (p, _, root, _, clock) = source().await;
            p.write(write("a", "a", "alpha durable", ExpectedVersion::Absent))
                .await
                .unwrap();
            std::fs::write(root.join(boundary), b"fail").unwrap();
            assert!(p.compact_journal().await.is_err());
            let r = p.inner.lock().await.remote.take().unwrap();
            let h = r
                .authority
                .lock()
                .await
                .read_head("source")
                .unwrap()
                .unwrap();
            assert_eq!(h.manifest.is_some(), boundary.ends_with("after-checkpoint"));
            drop(p);
            std::fs::remove_dir_all(&root).unwrap();
            let p = RecordPartition::open_s3_with_remote(&root, config().max_journal_bytes, r)
                .await
                .unwrap();
            p.inner.lock().await.clock = clock;
            assert_eq!(
                p.get("a").await.unwrap().unwrap().document.unwrap()["body"],
                "alpha durable"
            );
            p.compact_journal().await.unwrap();
            assert_eq!(query(&p, SearchMode::Lexical).await.hits.len(), 1);
        }
    }
    #[tokio::test]
    async fn remote_backup_retry_restore_isolation_and_gc_are_complete() {
        let (p, store, root, db, clock) = source().await;
        let request = write("first", "a", "alpha saved", ExpectedVersion::Absent);
        let receipt = p.write(request.clone()).await.unwrap();
        p.compact_journal().await.unwrap();
        let fail = db.with_extension("fail-backup-after-snapshot");
        std::fs::write(&fail, b"fail").unwrap();
        assert!(p.create_s3_backup("cut", 100, 1024 * 1024).await.is_err());
        p.write(write(
            "update",
            "a",
            "alpha changed",
            ExpectedVersion::Match(1),
        ))
        .await
        .unwrap();
        p.compact_journal().await.unwrap();
        std::fs::remove_file(fail).unwrap();
        let (bundle, seq, _) = p.create_s3_backup("cut", 999, 1024 * 1024).await.unwrap();
        assert_eq!(seq, receipt.seq);
        assert_eq!(
            p.create_s3_backup("cut", 1000, 1024 * 1024)
                .await
                .unwrap()
                .0,
            bundle
        );
        let restored_root = path();
        let restored_db = path();
        let restored = RecordPartition::restore_s3_with_remote(
            &restored_root,
            config(),
            remote(store.clone(), &restored_db, "restored"),
            &bundle,
            1024 * 1024,
            10000,
            16 * 1024 * 1024,
        )
        .await
        .unwrap();
        restored.inner.lock().await.clock = clock.clone();
        assert_eq!(
            restored.get("a").await.unwrap().unwrap().document.unwrap()["body"],
            "alpha saved"
        );
        assert_eq!(restored.write(request).await.unwrap(), receipt);
        assert!(
            restored
                .recovery_identity()
                .await
                .unwrap()
                .independent_snapshot
        );
        let expected = scores(&restored).await;
        p.release_s3_backup_pins("cut").await.unwrap();
        p.collect_s3_garbage().await.unwrap();
        let restored = cold(restored, &restored_root, clock.clone()).await;
        assert_eq!(scores(&restored).await, expected);
        restored.compact_journal().await.unwrap();
        let p = cold(p, &root, clock).await;
        assert_eq!(
            p.get("a").await.unwrap().unwrap().document.unwrap()["body"],
            "alpha changed"
        );
        // Retrying restore after activation does not rewind subsequent target writes.
        restored
            .write(write("new", "b", "alpha target", ExpectedVersion::Absent))
            .await
            .unwrap();
        drop(restored);
        let restored = RecordPartition::restore_s3_with_remote(
            &restored_root,
            config(),
            remote(store, &restored_db, "restored"),
            &bundle,
            1024 * 1024,
            10000,
            16 * 1024 * 1024,
        )
        .await
        .unwrap();
        assert!(restored.get("b").await.unwrap().is_some());
    }
    #[tokio::test]
    async fn sqlite_owner_lock_is_independent_of_partition_cache() {
        let db=path();
        let mut settings=S3CompatibleRecordConfig {
            authority_path:db.clone(),partition_id:"exclusive".into(),owner_epoch:1,
            endpoint:"http://127.0.0.1:9".into(),bucket:"test".into(),region:"us-east-1".into(),
            access_key_id:"fixture".into(),secret_access_key:"fixture-secret".into(),allow_http:true,
            max_object_bytes:1024*1024,max_tail_blocks:100,max_tail_bytes:16*1024*1024,
        };
        let first=RemotePersistence::open(&settings).unwrap();
        assert!(RemotePersistence::open(&settings).is_err());
        settings.partition_id="another-cache".into();
        assert!(RemotePersistence::open(&settings).is_err(),"a different cache/partition cannot bypass the authority owner");
        drop(first);
        settings.partition_id="exclusive".into();
        drop(RemotePersistence::open(&settings).unwrap());
        for file in [db.clone(),db.with_extension("sqlite-wal"),db.with_extension("sqlite-shm"),db.with_extension("sqlite.owner")] {let _=std::fs::remove_file(file);}
    }

    #[tokio::test]
    async fn confirmed_remote_commit_and_expiry_survive_cache_append_failure() {
        for expiration in [false,true] {
            let (p,_,root,_,clock)=source().await;
            let mut request=write("cache-fault","a","alpha",ExpectedVersion::Absent);
            let now=clock.0.load(Ordering::SeqCst);
            if expiration {
                request.temporal=Some(TemporalMetadata {expire_at_ms:Some(now+10),..Default::default()});
                p.write(request.clone()).await.unwrap();
                clock.0.store(now+20,Ordering::SeqCst);
            }
            let permit=p.output_permit().unwrap();
            std::fs::write(root.join(".test-cache-append-failure"),b"fail cache").unwrap();
            let failed=if expiration {p.statistics().await.map(|_|())} else {p.write(request.clone()).await.map(|_|())};
            assert_eq!(failed,Err(RecordError::DurablePending(if expiration {2} else {1})));
            assert!(permit.deliver(||()).is_err());
            assert!(p.output_permit().is_err());
            let p=cold(p,&root,clock).await;
            assert_eq!(p.write(request).await.unwrap().seq,1,"original receipt is retained");
            if expiration {
                assert!(p.get("a").await.unwrap().is_none());
                assert_eq!(p.statistics().await.unwrap().visible_seq,2);
            } else {assert!(p.get("a").await.unwrap().is_some());}
        }
    }

    #[tokio::test]
    async fn remote_staging_expiration_survives_cache_append_failure() {
        use super::super::tests::{source_write,generation_write,stage_generation};
        let (p,_,root,_,clock)=source().await;
        p.write(source_write("source","s",&[])).await.unwrap();
        let stage=generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["a","b"]));
        let receipt=p.write(stage.clone()).await.unwrap();
        clock.0.fetch_add(1_800_001,Ordering::SeqCst);
        std::fs::write(root.join(".test-cache-append-failure"),b"fail cache").unwrap();
        assert!(matches!(p.generation_status("doc").await,Err(RecordError::DurablePending(3))));
        assert!(p.output_permit().is_err());
        let p=cold(p,&root,clock).await;
        assert_eq!(p.generation_status("doc").await.unwrap().unwrap().staged_members,0);
        assert_eq!(p.write(stage).await.unwrap(),receipt);
        assert!(p.get("a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn malformed_bootstrap_never_publishes_a_local_cache() {
        for wrong_seq in [false,true] {
            let root=path();let db=path();let store=Arc::new(InMemory::new());
            let remote=remote(store,&db,"bad-bootstrap");
            let mut settings=config();let mut state=State::default();
            if wrong_seq {state.seq=1;} else {settings.max_journal_bytes+=1;}
            let bytes=encode_entry(&Entry::Checkpoint {config:settings,state}).unwrap();
            {
                let mut authority=remote.authority.lock().await;
                authority.create_partition("bad-bootstrap",1).unwrap();
                remote.store.publish_bootstrap(&mut authority,"bad-bootstrap",1,&bytes,10).await.unwrap();
            }
            assert!(matches!(RecordPartition::open_s3_with_remote(&root,config().max_journal_bytes,remote).await,Err(RecordError::Rejected(_))));
            assert!(!root.exists(),"unusable bootstrap must not create a local authority-looking cache");
            for file in [db.clone(),db.with_extension("sqlite-wal"),db.with_extension("sqlite-shm")] {let _=std::fs::remove_file(file);}
        }
    }

    #[tokio::test]
    async fn terminal_seal_uses_remote_authority_across_publication_failures() {
        for stage in ["before", "after"] {
            let (p, _, root, _, clock) = source().await;
            p.write(write("a", "a", "alpha", ExpectedVersion::Absent)).await.unwrap();
            let request = SealRequest { intent_id: "a".repeat(64), target_collection_id: "next".into(), backup_digest: "b".repeat(64) };
            let permit = p.output_permit().unwrap();
            std::fs::write(root.join(format!(".test-remote-{stage}-checkpoint")), b"fault").unwrap();
            assert!(p.seal_for_takeover(request.clone()).await.is_err());
            assert!(permit.deliver(|| ()).is_err());
            assert!(p.output_permit().is_err());
            let p = cold(p, &root, clock.clone()).await;
            let identity = p.recovery_identity().await.unwrap();
            assert_eq!(identity.seq, 1);
            assert_eq!(identity.seal.is_some(), stage == "after");
            let seal = p.seal_for_takeover(request.clone()).await.unwrap();
            let p = cold(p, &root, clock).await;
            assert_eq!(p.recovery_identity().await.unwrap().seal, Some(seal.clone()));
            assert_eq!(p.seal_for_takeover(request).await.unwrap(), seal);
            assert!(p.write(write("late", "late", "alpha", ExpectedVersion::Absent)).await.is_err());
            assert!(p.output_permit().is_err());
        }
    }

    #[tokio::test]
    async fn missing_checkpoint_member_fails_closed_and_bootstrap_is_not_collectible() {
        use object_store::{ObjectStore, path::Path as ObjectPath};
        let (p, store, root, _, _) = source().await;
        p.write(write("a", "a", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        p.compact_journal().await.unwrap();
        let r = p.inner.lock().await.remote.take().unwrap();
        let h = r
            .authority
            .lock()
            .await
            .read_head("source")
            .unwrap()
            .unwrap();
        assert!(
            !r.authority
                .lock()
                .await
                .retire_object(&h.bootstrap.unwrap().key, 100, 100)
                .unwrap()
        );
        let bytes = r.store.get_verified(&h.manifest.unwrap()).await.unwrap();
        let c: Checkpoint = serde_json::from_slice(&bytes).unwrap();
        store
            .delete(&ObjectPath::from(
                c.files
                    .iter()
                    .flat_map(|f| &f.chunks)
                    .next()
                    .unwrap()
                    .key
                    .as_str(),
            ))
            .await
            .unwrap();
        drop(p);
        std::fs::remove_dir_all(&root).unwrap();
        assert!(
            RecordPartition::open_s3_with_remote(&root, config().max_journal_bytes, r)
                .await
                .is_err()
        );
    }
}

impl RecordPartition {
    /// Export a retained joint backup into a standalone local archive for the
    /// complete-service snapshot workflow. The caller retains the original backup
    /// pin until completion and owns the private replay directory.
    pub async fn materialize_s3_backup_archive(
        destination: &Path,
        config: RecordConfig,
        remote_config: S3CompatibleRecordConfig,
        bundle_json: &str,
    ) -> Result<(), RecordError> {
        use crate::storage::s3_compatible::BackupBundle;
        let bundle: BackupBundle = serde_json::from_str(bundle_json).map_err(storage)?;
        let mut remote = RemotePersistence::open(&remote_config)?;
        let snapshot = {
            let mut db = remote.authority.lock().await;
            remote
                .store
                .restore_backup_bundle(
                    &mut db,
                    &bundle.descriptor_ref,
                    256 * 1024 * 1024,
                    1_000_000,
                    10 * 1024 * 1024 * 1024usize,
                )
                .await
                .map_err(storage)?
        };
        if snapshot.heads.len() != 1 {
            return Err(storage("backup must contain one collection"));
        }
        remote.partition_id = snapshot.heads[0].partition_id.clone();
        remote.owner_epoch = snapshot.heads[0].owner_epoch;
        let replay = destination.with_extension("replay");
        let p = Self::open_s3_with_remote(&replay, config.max_journal_bytes, remote).await?;
        // Snapshot must not run wall-clock maintenance on historical authority.
        p.inner.lock().await.remote = None;
        {
            let mut inner = p.inner.lock().await;
            inner.state.independent_snapshot = true;
        }
        let scope = Sha256::digest(format!("backup-archive:{:?}", bundle.descriptor_ref.sha256))
            .iter().take(16).map(|b| format!("{b:02x}")).collect();
        p.checkpoint_with_scope(destination, Some(scope)).await?;
        drop(p);
        let _ = std::fs::remove_dir_all(replay);
        Ok(())
    }

    /// Activate a verified complete-service restore in a fresh S3 namespace.
    /// The service's completion certificate is the restore identity; arbitrary
    /// local caches must never be adopted through this management-only entry point.
    pub async fn adopt_s3_snapshot(
        path: &Path,
        config: RecordConfig,
        remote_config: S3CompatibleRecordConfig,
        restore_identity: &str,
    ) -> Result<Self, RecordError> {
        Self::adopt_s3_authority(path, config, remote_config, restore_identity, None).await
    }

    /// Publish a catalog-authorized successor in its own S3 namespace. Its
    /// complete final authority must match the source's permanent seal; an
    /// independent backup cannot be promoted through this entry point.
    pub async fn adopt_s3_takeover_successor(
        path: &Path,
        config: RecordConfig,
        remote_config: S3CompatibleRecordConfig,
        seal: &SealCertificate,
    ) -> Result<Self, RecordError> {
        Self::adopt_s3_authority(path, config, remote_config, &seal.request.intent_id, Some(seal)).await
    }

    async fn adopt_s3_authority(
        path: &Path,
        config: RecordConfig,
        remote_config: S3CompatibleRecordConfig,
        restore_identity: &str,
        seal: Option<&SealCertificate>,
    ) -> Result<Self, RecordError> {
        let mut remote = RemotePersistence::open(&remote_config)?;
        let domain = if seal.is_some() { "takeover" } else { "service" };
        let binding: [u8; 32] = Sha256::digest(format!(
            "{domain}:{restore_identity}:{}",
            remote.partition_id
        ))
        .into();
        remote
            .authority
            .lock()
            .await
            .bind_restore(binding)
            .map_err(storage)?;
        let head = remote
            .authority
            .lock()
            .await
            .read_head(&remote.partition_id)
            .map_err(storage)?;
        if head.is_some_and(|h| h.manifest.is_some()) {
            let p = Self::open_s3_with_remote(path, config.max_journal_bytes, remote).await?;
            if let Some(seal) = seal { p.certify_takeover_successor(seal).await?; }
            return Ok(p);
        }
        let p = Self::open(path, config.max_journal_bytes).await?;
        if let Some(seal) = seal { p.certify_takeover_successor(seal).await?; }
        {
            let mut inner = p.inner.lock().await;
            if seal.is_none() && !inner.state.independent_snapshot {
                return Err(RecordError::Rejected(
                    "only certified independent snapshots may be adopted".into(),
                ));
            }
            let bootstrap = remote
                .authority
                .lock()
                .await
                .read_head(&remote.partition_id)
                .map_err(storage)?
                .and_then(|h| h.bootstrap);
            if let Some(reference) = bootstrap {
                let bytes = remote
                    .store
                    .get_verified(&reference)
                    .await
                    .map_err(storage)?;
                match serde_json::from_slice::<Entry>(&bytes).map_err(storage)? {
                    Entry::Checkpoint { state, .. }
                        if state.seq == 0 && state.lineage == inner.state.lineage =>
                    {
                        if seal.is_some() && state.feed.scope != inner.state.feed.scope {
                            return Err(RecordError::RecoveryRequired);
                        }
                        inner.state.feed.scope = state.feed.scope;
                    }
                    _ => return Err(RecordError::RecoveryRequired),
                }
            }
            inner.publish_anchor().await?;
            let mut initial = State::default();
            initial.lineage = inner.state.lineage.clone();
            initial.feed = change_feed::FeedState {
                scope: inner.state.feed.scope.clone(),
                floor: 0,
            };
            let bytes = encode_entry(&Entry::Checkpoint {
                config: inner.config.clone(),
                state: initial,
            })?;
            {
                let mut db = remote.authority.lock().await;
                if db
                    .read_head(&remote.partition_id)
                    .map_err(storage)?
                    .is_none()
                {
                    db.create_partition(&remote.partition_id, remote.owner_epoch)
                        .map_err(storage)?;
                }
                let h = db
                    .read_head(&remote.partition_id)
                    .map_err(storage)?
                    .ok_or(RecordError::RecoveryRequired)?;
                if h.bootstrap.is_none() {
                    remote
                        .store
                        .publish_bootstrap(
                            &mut db,
                            &remote.partition_id,
                            remote.owner_epoch,
                            &bytes,
                            unix_time_ms_i64(),
                        )
                        .await
                        .map_err(storage)?;
                }
            }
            remote
                .publish_anchor(
                    inner.journal.root(),
                    inner.anchor.as_ref().ok_or(RecordError::RecoveryRequired)?,
                    &inner.payloads,
                    inner.state.seq,
                    inner.config.max_journal_bytes,
                    true,
                )
                .await?;
            inner.remote = Some(remote);
        }
        Ok(p)
    }
}

/// Independent SQLite connection keeps long object I/O outside both the Record
/// guard and its foreground authority mutex. All connections use the same DB.
pub(super) struct RemoteBuild {
    source_head:crate::storage::sqlite_authority::PartitionHead,
    max_tail_bytes:usize,
    pub build: crate::storage::sqlite_authority::IndexBuild,
    store: crate::storage::s3_compatible::S3CompatibleStore,
    authority_path: PathBuf,
    partition_id: String,
    max_object_bytes: usize,
    max_tail_blocks: usize,
}
impl RemotePersistence {
    pub(super) async fn begin_background_build(&mut self, timeout: std::time::Duration) -> Result<RemoteBuild,RecordError> {
        let mut authority = self.authority.lock().await;
        let head=authority.read_head(&self.partition_id).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if head.owner_epoch!=self.owner_epoch { return Err(RecordError::RecoveryRequired); }
        let now = unix_time_ms_i64();
        let deadline = now.checked_add(timeout.as_millis().max(1).try_into().map_err(storage)?)
            .ok_or_else(||storage("build deadline overflow"))?;
        let build = authority.begin_index_build(&self.partition_id,self.owner_epoch,now,deadline).map_err(storage)?
            .ok_or(RecordError::Busy)?;
        Ok(RemoteBuild{source_head:head,max_tail_bytes:self.max_tail_bytes,build,store:self.store.clone().background(),authority_path:authority.database_path().to_path_buf(),
            partition_id:self.partition_id.clone(),max_object_bytes:self.max_object_bytes,max_tail_blocks:self.max_tail_blocks})
    }
}
impl RemoteBuild {
    pub(super) async fn verify_source(&self,root:&Path,reference:Option<&anchor::Reference>,
        frames:&[Vec<u8>],authority_budget:u64)->Result<(),RecordError> {
        let head=&self.source_head;
        if head.owner_epoch!=self.build.owner_epoch || head.committed_seq!=self.build.cut_seq {
            return Err(RecordError::RecoveryRequired);
        }
        let store=self.store.uncached();
        if head.manifest.is_some() {
            let selected=RemotePersistence::selected_from(&store,&self.partition_id,head,true).await?;
            let reference=reference.ok_or_else(||storage("remote source lacks local anchor"))?;
            if checkpoint_codec::json_digest(&selected.checkpoint.anchor)?!=checkpoint_codec::json_digest(reference)? {
                return Err(storage("remote source authority mismatch"));
            }
            let (files,_inventory)=combined_files(root,&selected.checkpoint.files,selected.index.as_ref().map_or(&[],|i|i.files.as_slice()),authority_budget,self.max_tail_blocks)?;
            let max_total=authority_budget.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024)).ok_or_else(||storage("remote source budget overflow"))?;
            let mut count=0usize;let mut total=0u64;
            for file in files.values() {
                let mut hash=Sha256::new();let mut length=0u64;
                for chunk in &file.chunks {
                    count=count.checked_add(1).filter(|n|*n<=self.max_tail_blocks).ok_or_else(||storage("remote source chunk budget"))?;
                    let bytes=store.get_verified(chunk).await.map_err(remote_read_error)?;
                    total=total.checked_add(bytes.len() as u64).filter(|n|*n<=max_total).ok_or_else(||storage("remote source byte budget"))?;
                    length=length.checked_add(bytes.len() as u64).ok_or_else(||storage("remote source file overflow"))?;
                    hash.update(&bytes);
                }
                if length!=file.length || <[u8;32]>::from(hash.finalize())!=file.sha256 {return Err(storage("remote source file checksum"));}
            }
        } else if reference.is_some() {return Err(storage("local anchor absent from remote authority"));}
        let entries=crate::storage::object_log::load_committed_entries_after(&store,head,head.log_base_seq,self.max_tail_blocks,self.max_tail_bytes).await
            .map_err(|e|match e {crate::storage::object_log::TailError::ObjectStore(e)=>remote_read_error(e),other=>storage(other)})?;
        if frames.is_empty() || entries.len()!=frames.len()-1 {return Err(storage("remote/local source suffix length mismatch"));}
        for (entry,bytes) in entries.iter().zip(frames.iter().skip(1)) {
            if entry.payload!=*bytes || entry.request_sha256!=<[u8;32]>::from(Sha256::digest(bytes)) {return Err(storage("remote/local source suffix differs"));}
        }
        // Appends after the pinned cut are legal and proved by catch-up. A
        // different owner or selected authority is not a permissible suffix.
        let authority=crate::storage::sqlite_authority::SqliteAuthority::open(&self.authority_path,std::time::Duration::from_secs(5)).map_err(storage)?;
        let current=authority.read_head(&self.partition_id).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if current.owner_epoch!=head.owner_epoch || current.manifest!=head.manifest || current.indexed_seq!=head.indexed_seq || current.committed_seq<head.committed_seq {
            return Err(RecordError::RecoveryRequired);
        }
        Ok(())
    }
    pub(super) async fn publish(&self,active:&mut RemotePersistence,manifest:ObjectRef,root:&Path,expected_seq:u64) -> Result<bool,RecordError> {
        #[cfg(test)]
        if root.join(".test-remote-before-checkpoint").exists() { return Err(storage("injected before remote checkpoint publication")); }
        #[cfg(feature="fault-injection")]
        let fault_partition=std::env::var("ANYSEEK_REMOTE_CHECKPOINT_PARTITION").map(|id|id==self.partition_id).unwrap_or(true);
        #[cfg(feature="fault-injection")]
        if fault_partition && std::env::var("ANYSEEK_REMOTE_CHECKPOINT_FAULT").as_deref()==Ok("before_publish") { std::process::exit(91); }
        let published=active.authority.lock().await.publish_index_build(&self.build,manifest,unix_time_ms_i64()).map_err(storage)?;
        let Some(head)=published else {
            let head=active.authority.lock().await.read_head(&self.partition_id).map_err(storage)?
                .ok_or(RecordError::RecoveryRequired)?;
            if head.owner_epoch!=self.build.owner_epoch || head.committed_seq!=expected_seq
                || head.manifest!=self.build.base_manifest || head.indexed_seq!=self.build.base_indexed_seq {
                return Err(RecordError::RecoveryRequired);
            }
            return Ok(false);
        };
        if head.committed_seq!=expected_seq { return Err(RecordError::RecoveryRequired); }
        #[cfg(feature="fault-injection")]
        if fault_partition && std::env::var("ANYSEEK_REMOTE_CHECKPOINT_FAULT").as_deref()==Ok("after_publish") { std::process::exit(91); }
        #[cfg(test)]
        if root.join(".test-remote-after-checkpoint").exists() { return Err(RecordError::DurablePending(head.committed_seq)); }
        let _=(root,head);
        Ok(true)
    }
    pub(super) async fn upload(&self, root:&Path, reference:&anchor::Reference,
        payloads:&crate::storage::payload_pool::Closure, authority_budget:u64) -> Result<ObjectRef,RecordError> {
        let mut authority = crate::storage::sqlite_authority::SqliteAuthority::open(&self.authority_path,std::time::Duration::from_secs(5)).map_err(storage)?;
        let mut files = Vec::new();
        let mut total = 0u64;
        let mut chunk_count = 0usize;
        let closure = reference.remote_files(payloads)?;
        admit_checkpoint_chunks(&closure, self.max_object_bytes, self.max_tail_blocks)?;
        for (path, length, sha256) in closure {
            total = total
                .checked_add(length)
                .filter(|n| {
                    *n <= authority_budget
                        .saturating_mul(64)
                        .saturating_add(32 * 1024 * 1024)
                })
                .ok_or_else(|| storage("checkpoint upload budget exceeded"))?;
            let target = safe_path(root, &path)?;
            let mut input = std::fs::File::open(target).map_err(storage)?;
            let mut chunks = Vec::new();
            let mut hash = Sha256::new();
            let mut actual = 0u64;
            loop {
                let _buffer = crate::resources::io_scope(crate::resources::Lane::Background)
                    .bytes((self.max_object_bytes as u64).min(length.saturating_sub(actual)).saturating_mul(2))
                    .map_err(|_| RecordError::Busy)?;
                let mut bytes = Vec::new();
                {
                    let _io = crate::resources::io_scope(crate::resources::Lane::Background).io().map_err(|_|RecordError::Busy)?;
                    (&mut input)
                        .take(self.max_object_bytes as u64)
                        .read_to_end(&mut bytes)
                        .map_err(storage)?;
                }
                if bytes.is_empty() {
                    break;
                }
                hash.update(&bytes);
                actual = actual
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| storage("checkpoint length overflow"))?;
                if actual > length {
                    return Err(storage("checkpoint file grew during export"));
                }
                let digest = Sha256::digest(&bytes)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>();
                let key = format!(
                    "partitions/{}/files/build-{}/{}/{:08}-{digest}",
                    self.partition_id,
                    self.build.build_epoch,
                    path,
                    chunks.len()
                );
                let planned=ObjectRef{key:key.clone(),sha256:Sha256::digest(&bytes).into(),length:bytes.len() as u64};
                authority.reserve_index_build_output(&self.build,&planned,unix_time_ms_i64()).map_err(storage)?;
                let object = self.store.put_and_register(&mut authority,&key,&bytes,unix_time_ms_i64()).await.map_err(storage)?;
                chunks.push(object);
                chunk_count=chunk_count.checked_add(1).ok_or_else(||storage("checkpoint chunk count overflow"))?;
                if chunk_count > self.max_tail_blocks {
                    return Err(storage("checkpoint chunk budget exceeded"));
                }
            }
            let actual_hash: [u8; 32] = hash.finalize().into();
            if actual != length || actual_hash != sha256 {
                return Err(storage("checkpoint export checksum mismatch"));
            }
            files.push(File {
                path,
                length,
                sha256,
                chunks,
            });
        }
        let checkpoint = Checkpoint {
            format: 1,
            partition_id: self.partition_id.clone(),
            seq: self.build.cut_seq,
            anchor: reference.clone(),
            files,
        };
        let bytes = serde_json::to_vec(&checkpoint).map_err(storage)?;
        let key = format!(
            "partitions/{}/checkpoints/{}",
            self.partition_id,
            reference.identity()
        );
        let planned=ObjectRef{key:key.clone(),sha256:Sha256::digest(&bytes).into(),length:bytes.len() as u64};
        authority.reserve_index_build_output(&self.build,&planned,unix_time_ms_i64()).map_err(storage)?;
        let manifest = self
            .store
            .put_and_register(&mut authority, &key, &bytes, unix_time_ms_i64())
            .await
            .map_err(storage)?;
        for file in &checkpoint.files {
            for child in &file.chunks {
                authority
                    .register_object_edge(&manifest, child)
                    .map_err(storage)?;
            }
        }

        Ok(manifest)
    }
}

impl Inner {
    pub(super) async fn ensure_remote_tail_room(&mut self,seq:u64,operation_id:&str,payload:&[u8])->Result<(),RecordError> {
        let Some(remote)=self.remote.as_ref() else{return Ok(())};
        if !remote.tail_needs_checkpoint(seq,operation_id,payload).await? {return Ok(());}
        log::info!("record_maintenance cause=remote_tail_room seq={} incoming_bytes={}",self.state.seq,payload.len());
        Err(self.request_checkpoint("remote_tail_room"))
    }
}

fn remote_read_error(error:object_store::Error)->RecordError {
    if crate::storage::s3_compatible::is_resource_error(&error) {RecordError::Busy}else{storage(error)}
}

impl Inner {
    pub(super) async fn publish_remote_index_tail(&mut self)->Result<(),RecordError> {
        if self.remote.is_none(){return Ok(());}
        let bytes=self.index.statistics_wire_length().await?;
        let _metadata=crate::storage::build_budget::reserve_memory((bytes as u64).checked_mul(64).and_then(|n|n.checked_add(1024*1024)).ok_or_else(||storage("index statistics budget overflow"))?).map_err(|_|RecordError::Busy)?;
        let mut statistics=self.index.local_statistics().await?;
        if statistics.first().is_none_or(|s|!s.keys.is_empty()){return Err(storage("index seal successor is not empty"));}
        statistics.remove(0);
        let anchor=self.anchor.as_ref().ok_or(RecordError::RecoveryRequired)?;
        let packages=self.index.pending_seals.iter().chain(anchor.retained_packages()).cloned().collect();
        let fenced=self.output.read().map_err(|_|RecordError::RecoveryRequired)?.fenced;
        self.healthy=false;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
        let result=self.remote.as_mut().unwrap().publish_derived_index(self.journal.root(),&self.config,&self.state,packages,statistics).await;
        if !matches!(&result,Err(RecordError::OutcomeUnknown(_)|RecordError::RecoveryRequired)) {
            self.healthy=true;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=fenced;
        }
        result
    }
}
