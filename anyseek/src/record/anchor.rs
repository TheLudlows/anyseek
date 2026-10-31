//! A partition certificate selects this immutable state/index closure. Directory
//! discovery is never a second head or a reason to adopt an unselected object.
use super::*;
use crate::storage::payload_pool::{Budget,Closure,PoolBuilder,PoolReader};
use segment::{files,write_json,read_bounded,Inventory};

/// Clones share immutable inventory and its residency lease. Runtime proof
/// owners are not serialized and never replace on-disk authentication.
#[derive(Debug,Clone)]
pub(super) struct Reference {
    metadata:Arc<ReferenceOwner>,
    captured:Arc<std::sync::OnceLock<State>>,
    previous_capture:Option<Arc<std::sync::OnceLock<State>>>,
}
#[derive(Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReferenceData {
    id:String, files:Inventory, segments:Vec<Frozen>, state_blocks:Vec<StateFiles>,
}
struct ReferenceOwner { data:ReferenceData, _lease:crate::storage::build_budget::MemoryLease }
impl std::fmt::Debug for ReferenceOwner {
    fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result { std::fmt::Debug::fmt(&self.data,f) }
}
impl std::ops::Deref for Reference {
    type Target=ReferenceData;
    fn deref(&self)->&Self::Target { &self.metadata.data }
}
impl Serialize for Reference {
    fn serialize<S:serde::Serializer>(&self,s:S)->Result<S::Ok,S::Error> { self.metadata.data.serialize(s) }
}
impl<'de> Deserialize<'de> for Reference {
    fn deserialize<D:serde::Deserializer<'de>>(d:D)->Result<Self,D::Error> {
        // Persistent entries are decoded from bounded JSON slices. Borrow the
        // raw representation first: no Vec/String inventory is materialized
        // before its complete independent residency has been admitted.
        let raw=<&serde_json::value::RawValue>::deserialize(d)?;
        let charge=metadata_charge(raw.get().len()).and_then(|n|n.checked_add(65536).ok_or_else(||storage("reference scratch overflow"))).map_err(serde::de::Error::custom)?;
        let mut lease=crate::storage::build_budget::reserve_memory(charge).map_err(serde::de::Error::custom)?;
        let data:ReferenceData=serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
        if !valid(&data.id) || data.segments.iter().any(|s|!valid(&s.id)) || data.state_blocks.iter().any(|b|!valid(&b.id)) {
            return Err(serde::de::Error::custom("invalid reference metadata identity"));
        }
        let needed=reference_shell_charge(data.segments.len(),data.state_blocks.len()).map_err(serde::de::Error::custom)?;
        let owned=lease.split_off(needed).map_err(serde::de::Error::custom)?;
        Ok(Self {metadata:Arc::new(ReferenceOwner{data,_lease:owned}),captured:Arc::default(),previous_capture:None})
    }
}
fn reference_shell_charge(segments:usize,blocks:usize)->Result<u64,RecordError> {
    if segments>capacity::MAX_SEGMENTS || blocks>anchor_image::MAX_DELTAS {return Err(storage("reference metadata count exceeded"));}
    // IDs are validated as 32-byte hex before selected paths can be used.
    // Include Vec slots, transient reallocations, Arc owner/header and IDs.
    let bytes=segments.checked_add(blocks).and_then(|n|n.checked_mul(256)).and_then(|n|n.checked_add(512))
        .ok_or_else(||storage("reference shell overflow"))?;
    metadata_charge(bytes)
}
fn metadata_charge(bytes:usize)->Result<u64,RecordError> {
    if bytes>16*1024*1024 {return Err(storage("anchor metadata frame budget exceeded"));}
    (bytes as u64).checked_mul(32).ok_or_else(||storage("anchor metadata charge overflow"))
}
#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct StateFiles { id:String, files:Inventory }
type Frozen = segment::Package;
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {format:u32,seq:u64,state:anchor_image::Image,payloads:Closure}
/// Raw bytes and decoded metadata have separate owners. Both reservations
/// precede their allocations; the decoded owner survives every use of Image.
fn read_manifest(path:&Path,limit:u64)->Result<(Manifest,crate::storage::build_budget::MemoryLease),RecordError> {
    use std::io::Read;
    let mut file=crate::storage::build_budget::OpenOptions::new().read(true).open(path).map_err(storage)?;
    let length=file.metadata().map_err(storage)?.len();
    if length>limit {return Err(storage("anchor manifest byte budget exceeded"));}
    let length=usize::try_from(length).map_err(storage)?;
    let parsed=crate::storage::build_budget::reserve_memory(metadata_charge(length)?).map_err(storage)?;
    let _raw=crate::storage::build_budget::reserve_memory(length as u64).map_err(storage)?;
    let mut bytes=vec![0;length];
    file.read_exact(&mut bytes).map_err(storage)?;
    if file.read(&mut [0u8;1]).map_err(storage)?!=0 {return Err(storage("anchor manifest grew during read"));}
    let manifest=serde_json::from_slice(&bytes).map_err(storage)?;
    Ok((manifest,parsed))
}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Statistics {segments:Vec<crate::active::ActiveStatistics>}
/// Membership is frozen at the same cut as State. No mutable retirement token
/// escapes capture; immutable packages remain protected by the owned build.
pub(super) struct NativeReuse {
    pending:Vec<Frozen>,
    local:Vec<crate::active::ActiveStatistics>,
    _admission:NativeAdmission,
}
pub(super) struct NativeAdmission {
    _memory:crate::storage::build_budget::MemoryLease,
    _resources:crate::resources::Lease,
}
impl NativeReuse {
    pub(super) async fn admit(inner:&Inner)->Result<Option<NativeAdmission>,RecordError> {
        let _phase=maintenance::Phase::new("compact_native_admission_locked",inner.state.seq);
        if inner.index.detached_cache || inner.anchor.as_ref().is_none_or(|previous|
            previous.segments.len()+inner.index.pending_seals.len()>=capacity::segments(&inner.config)) {return Ok(None);}
        // Refuse known I/O saturation before preparing or sealing a successor.
        // Actual handoff I/O retains its own admission and failure fencing.
        let probe=crate::resources::io_scope(crate::resources::Lane::Background).io().map_err(|_|RecordError::Busy)?;
        // Statistics await shard locks. Their writer may be awaiting I/O, so a
        // preflight probe must never retain capacity across that lock boundary.
        drop(probe);
        let bytes=(inner.index.statistics_wire_length().await? as u64).checked_mul(64)
            .and_then(|n|n.checked_add(1024*1024)).ok_or_else(||storage("native capture metadata overflow"))?;
        #[cfg(test)]
        if inner.journal.root().join(".test-compact-native-capture-budget").exists() {return Err(RecordError::Busy);}
        let memory=crate::storage::build_budget::reserve_memory(bytes).map_err(|_|RecordError::Busy)?;
        let resources=crate::resources::io_scope(crate::resources::Lane::Background).bytes(bytes).map_err(|_|RecordError::Busy)?;
        Ok(Some(NativeAdmission{_memory:memory,_resources:resources}))
    }
    pub(super) async fn capture(inner:&Inner,admission:NativeAdmission)->Result<Self,RecordError> {
        let _phase=maintenance::Phase::new("compact_native_membership_capture_locked",inner.state.seq);
        let local=inner.index.local_statistics().await?;
        let previous=inner.anchor.as_ref().ok_or_else(||storage("missing native capture predecessor"))?;
        if inner.index.detached_cache || local.len()!=previous.segments.len()+inner.index.pending_seals.len()+1
            || local.iter().any(|active|active.retired.is_some()) {return Err(storage("native capture topology mismatch"));}
        Ok(Self{pending:inner.index.pending_seals.clone(),local,_admission:admission})
    }
}
pub(super) struct Loaded {
    pub config:RecordConfig,pub state:State,pub index:VectorIndexArc,
    pub ordinals:Vec<String>,pub locations:BTreeMap<String,usize>,pub payloads:Closure,
}
struct VerifiedView { index:VectorIndexArc, ordinals:Vec<String>, locations:BTreeMap<String,usize>, payloads:Closure }
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
        // Stable identity from creation: no live mapped directory is renamed.
        // Until the Record certificate selects it, this remains a private orphan.
        let prepared=root.join(format!("segment-{id}"));
        let (index,_)=build_at_selected(root,&prepared,config,state,selected).await?;
        #[cfg(test)]
        if root.join(".test-native-rebuild-pause").exists() {
            std::fs::write(root.join(".test-native-rebuild-started"),b"built-private-native").map_err(storage)?;
            tokio::time::timeout(std::time::Duration::from_secs(20),async {
                while root.join(".test-native-rebuild-pause").exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            }).await.map_err(|_|RecordError::Busy)?;
        }
        let active=index.read().await.segments[0].read().await.active.as_ref().ok_or_else(||storage("missing frozen statistics"))?.clone_local();
        let native=index.read().await.record_lexical[0].clone();
        native.seal().await?;
        drop(native);
        drop(index);
        let reference=Self::seal(root,&prepared,id,closure_limit(config.max_journal_bytes)?)?;
        Ok((reference,active))
    }
    async fn prepare_chunks(root:&Path,config:&RecordConfig,state:&State,selected:Option<&std::collections::BTreeSet<String>>,segments:&mut Vec<Self>,statistics:&mut Vec<crate::active::ActiveStatistics>)->Result<(),RecordError>{
        // At most one bounded identity batch is materialized; the full State is
        // borrowed. Each package releases its writer before the next is built.
        let max_id=state.records.keys().map(String::len).max().unwrap_or(0);
        let bytes=(capacity::PACKAGE_RECORDS as u64).checked_mul(max_id as u64+256)
            .ok_or_else(||storage("package identity estimate overflow"))?;
        let _keys=crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
        let mut keys=std::collections::BTreeSet::new();
        for key in state.records.keys().filter(|key|selected.is_none_or(|s|s.contains(*key))) {
            keys.insert(key.clone());
            if keys.len()==capacity::PACKAGE_RECORDS {
                if segments.len()>=capacity::segments(config){return Err(storage("selected package budget exceeded"));}
                let (segment,active)=Self::prepare(root,config,state,Some(&keys)).await?;
                segments.push(segment);statistics.push(active);keys.clear();
            }
        }
        if !keys.is_empty() {
            if segments.len()>=capacity::segments(config){return Err(storage("selected package budget exceeded"));}
            let (segment,active)=Self::prepare(root,config,state,Some(&keys)).await?;
            segments.push(segment);statistics.push(active);
        }
        Ok(())
    }
    async fn load(&self,root:&Path,config:&RecordConfig,mut active:crate::active::ActiveStatistics)->Result<VectorIndexArc,RecordError>{
        let path=self.verify(root,closure_limit(config.max_journal_bytes)?)?;
        active.validate().map_err(storage)?;
        let schema:Vec<SchemaField>=serde_json::from_slice(&read_bounded(&path.join("schema.json"),config.max_journal_bytes)?).map_err(storage)?;
        if serde_json::to_vec(&schema).map_err(storage)?!=serde_json::to_vec(&retrieval_schema(config)).map_err(storage)?{return Err(storage("frozen segment schema mismatch"));}
        let mapped=self.files.iter().try_fold(0u64,|n,f|n.checked_add(f.bytes)).ok_or_else(||storage("loaded segment estimate overflow"))?;
        let estimate=mapped.checked_mul(2).ok_or_else(||storage("loaded segment estimate overflow"))?;
        let mut reservation=build_budget::admit(config,std::iter::empty()).map_err(|e|storage(format!("stage=frozen_component_base package={} {e}",self.id)))?;
        reservation.grow(estimate).map_err(storage)?;
        let index=crate::index::open_index(&path).await.map_err(storage)?;
        index.write().await.record_lexical.push(lexical::runtime::Runtime::open_owned(&path.join("native"),config).await?);
        index.read().await.segments[0].write().await.build_reservation=Some(reservation);
        {
            let index=index.read().await;
            if index.segments.len()!=1||!index.stored_field_names.is_empty(){return Err(storage("invalid frozen topology"));}
            let mut shard=index.segments[0].write().await;
            if shard.indexed_doc_count!=active.keys.len(){return Err(storage("frozen cardinality mismatch"));}
            shard.delete_hashset=(0..active.keys.len()).filter(|id|!active.is_live(*id)).collect();
            active.retired.get_or_insert_with(Default::default);
            shard.active=Some(active);
        }
        Ok(index)
    }
}
impl Reference {
    #[cfg(any(test,feature="s3-object-store"))]
    pub(super) fn identity(&self)->&str {&self.id}
    pub(super) fn retained_packages(&self)->&[segment::Package] {&self.segments}
    #[cfg(feature="s3-object-store")]
    pub(super) fn remote_files(&self,payloads:&Closure)->Result<Vec<(String,u64,[u8;32])>,RecordError>{
        let mut result=Vec::new();
        for file in &self.files {result.push((format!("anchor-{}/{}",self.id,file.path),file.bytes,file.sha256));}
        for block in &self.state_blocks {for file in &block.files {
            result.push((format!("anchor-{}/{}",block.id,file.path),file.bytes,file.sha256));
        }}
        for segment in &self.segments {for file in &segment.files {
            result.push((format!("segment-{}/{}",segment.id,file.path),file.bytes,file.sha256));
        }}
        for (id,descriptor) in &payloads.objects {result.push((format!("payload-{id}"),descriptor.bytes,descriptor.sha256));}
        if result.len()>10_000 {return Err(storage("checkpoint file count exceeded"));}
        Ok(result)
    }
    pub(super) async fn prepare(root:&Path,config:&RecordConfig,state:&State,prior:Closure,previous:Option<&Reference>,view:Option<&view::RetrievalView>,compact:bool,journal:Option<&mut PartitionJournal>)->Result<Self,RecordError>{
        let encoded={let _phase=maintenance::Phase::new("state_measure_and_digest",state.seq);checkpoint_codec::StateEncoding::new(state)?};
        Self::prepare_encoded(root,config,&encoded,prior,previous,view,compact,journal,None,None).await
    }
    pub(super) async fn prepare_captured(root:&Path,config:&RecordConfig,state:&State,prior:Closure,
        previous:Option<&Reference>,frames:&[Vec<u8>],native:Option<&NativeReuse>)->Result<Self,RecordError> {
        let encoded=checkpoint_codec::StateEncoding::new(state)?;
        Self::prepare_encoded(root,config,&encoded,prior,previous,None,false,None,Some(frames),native).await
    }
    async fn prepare_encoded(root:&Path,config:&RecordConfig,encoded:&checkpoint_codec::StateEncoding<'_>,prior:Closure,previous:Option<&Reference>,view:Option<&view::RetrievalView>,compact:bool,journal:Option<&mut PartitionJournal>,captured:Option<&[Vec<u8>]>,native:Option<&NativeReuse>)->Result<Self,RecordError>{
        let state=encoded.state();
        let _phase=maintenance::Phase::new("anchor_prepare",state.seq);
        let id=format!("{:032x}",rand::random::<u128>());
        let prepared=root.join(format!(".anchor-prepare-{id}"));
        std::fs::create_dir(&prepared).map_err(storage)?;
        let payload_budget = budget(config)?;
        // Reuse is an optimization, not a lifetime limit on further writes.
        // At the bounded object ceiling rebuild the complete referenced payload
        // set privately. Old selected objects remain protected by the existing
        // publication/GC protocol and DiskScope charges both generations.
        let prior_objects=prior.objects.len();
        let pool_compaction = prior_objects == payload_budget.objects;
        let prior = if pool_compaction {
            Closure::default()
        } else { prior };
        let mut pool=PoolBuilder::begin(root,prior,payload_budget,format!("{:032x}",rand::random::<u128>())).map_err(storage)?;
        // Inventory values have their own shared owners. Reserve the bounded
        // shell arrays/id copies before preparing or cloning any of them.
        let mut metadata_lease=crate::storage::build_budget::reserve_memory(
            reference_shell_charge(capacity::segments(config),anchor_image::MAX_DELTAS)?
        ).map_err(storage)?;
        let mut state_blocks=Vec::new();
        let mut previous_capture=None;
        let mut incremental=None;
        let mut image_lease=None;
        let mut base_reason=if compact {"forced_full_base"} else if pool_compaction {"payload_pool_bound"}
            else {"missing_predecessor_or_journal"};
        if !compact && !pool_compaction {
            if let Some(previous)=previous.filter(|_|journal.is_some() || captured.is_some()) {
                base_reason="missing_captured_proof";
                if previous.captured.get().is_some() {
                    let (_,manifest,_,mut manifest_lease)=previous.authenticated_manifest(root,config.max_journal_bytes)?;
                    base_reason="delta_count_bound";
                    if manifest.state.deltas.len()<anchor_image::MAX_DELTAS {
                        base_reason="empty_journal_suffix";
                        let delta=if let Some(frames)=captured {
                            state_chunks::Delta::write_captured(&prepared,config,&mut pool,frames,previous,previous.id.clone())?
                        }else{state_chunks::Delta::write(&prepared,config,&mut pool,journal.unwrap(),previous,previous.id.clone())?};
                        if let Some(delta)=delta {
                            let growth=checkpoint_codec::json_length(&delta)?
                                .checked_add(id.len()+128)
                                .and_then(|n|n.checked_add((manifest.state.deltas.len()+1)*std::mem::size_of::<anchor_image::Block>()))
                                .ok_or_else(||storage("delta metadata growth overflow"))?;
                            manifest_lease.grow(metadata_charge(growth)?).map_err(storage)?;
                            let mut image=manifest.state;
                            image.deltas.push(anchor_image::Block{owner:id.clone(),delta});
                            image.digest=encoded.digest();
                            image.owners()?;
                            state_blocks=previous.state_blocks.clone();
                            state_blocks.push(StateFiles{id:previous.id.clone(),files:previous.files.clone()});
                            previous_capture=Some(previous.captured.clone());
                            incremental=Some(image);
                            base_reason="incremental";
                            image_lease=Some(manifest_lease);
                        }
                    }
                }
            }
        }
        log::info!("record_maintenance phase=state_image seq={} reason={} incremental={} compact={} payload_compaction={} prior_objects={} prior_deltas={}",
            state.seq,base_reason,incremental.is_some(),compact,pool_compaction,
            prior_objects,previous.map_or(0,|p|p.state_blocks.len()));
        let referenced=match incremental {
            Some(image)=>{pool.retain_prior_closure();image},
            None=>anchor_image::Image {
                base_owner:id.clone(),
                base:state_chunks::Descriptor::write_encoded(&prepared,config,encoded,&mut pool)?,
                deltas:Vec::new(),digest:encoded.digest(),
            },
        };
        let payloads=pool.finish().map_err(storage)?;
        let manifest=Manifest{format:7,seq:state.seq,state:referenced,payloads};
        // Chain count is not a byte bound: repeated descriptors/long references
        // may exhaust a small configured authority before MAX_DELTAS. Measure
        // without constructing an encoded Vec or writing an oversized manifest.
        if !checkpoint_codec::json_fits(&manifest,config.max_journal_bytes.min(16*1024*1024))? {
            if !manifest.state.deltas.is_empty() {
                log::info!("record_maintenance cause=manifest_byte_bound seq={} deltas={} byte_limit={}",state.seq,manifest.state.deltas.len(),config.max_journal_bytes.min(16*1024*1024));
                // The failed candidate is private. Reuse its bounded Payload
                // closure (including all old dependencies), but write a new full
                // base once. No journal/certificate/owner has changed. Existing
                // post-attempt GC retires the abandoned preparation, never a
                // selected or pinned object. None prevents recursive delta use.
                let Manifest{payloads,state:old_image,..}=manifest;
                drop(old_image);
                drop(image_lease);
                drop(metadata_lease);
                drop(state_blocks);
                drop(previous_capture);
                return Box::pin(Self::prepare_encoded(root,config,encoded,payloads,previous,view,compact,None,None,native)).await;
            }
            return Err(storage("complete anchor manifest byte budget exceeded"));
        }
        write_json(&prepared.join("manifest"),&manifest)?;
        drop(manifest);
        drop(image_lease);
        let mut segments=Vec::new();let mut statistics=Vec::new();
        let pending_count=native.map_or_else(||view.map_or(0,|v|v.pending_seals.len()),|capture|capture.pending.len());
        let reuse_native=!compact && (native.is_some() || view.is_some_and(|view|!view.detached_cache))
            && previous.is_some_and(|p|p.segments.len()+pending_count<capacity::segments(config));
        log::info!("record_maintenance phase=native_packages seq={} reuse={} selected={} pending={} package_limit={}",
            state.seq,reuse_native,previous.map_or(0,|p|p.segments.len()),pending_count,capacity::segments(config));
        let native_phase=maintenance::Phase::new("native_prepare",state.seq);
        if reuse_native {
            let previous=previous.unwrap();
            let local=if let Some(capture)=native {capture.local.clone()}else{view.ok_or_else(||storage("missing incremental input view"))?.local_statistics().await?};
            let pending=if let Some(capture)=native {&capture.pending}else{&view.unwrap().pending_seals};
            if local.len()!=previous.segments.len()+pending.len()+1{return Err(storage("selected segment/view mismatch"));}
            let mut local=local.into_iter();let tail=local.next().unwrap();
            for (segment,active) in pending.iter().chain(previous.segments.iter()).zip(local) {
                if (0..active.keys.len()).any(|id|active.is_live(id)) {segments.push(segment.clone());statistics.push(active);}
            }
            let keys=tail.keys.iter().enumerate().filter(|(id,_)|tail.is_live(*id)).map(|(_,key)|key.clone()).collect::<std::collections::BTreeSet<_>>();
            if !keys.is_empty() {
                Frozen::prepare_chunks(root,config,state,Some(&keys),&mut segments,&mut statistics).await?;
            }
        } else {
            Frozen::prepare_chunks(root,config,state,None,&mut segments,&mut statistics).await?;
        }
        // Keep one empty physical segment so the query engine retains schema.
        if segments.is_empty(){let (segment,active)=Frozen::prepare(root,config,state,None).await?;segments.push(segment);statistics.push(active);}
        drop(native_phase);
        write_json(&prepared.join("statistics"),&Statistics{segments:statistics})?;
        let members=files(&prepared,closure_limit(config.max_journal_bytes)?)?;
        sync_tree(&prepared)?;
        crate::storage::partition::publish_bundle(&prepared,&directory(root,&id)?).map_err(storage)?;
        let data=ReferenceData{id,files:members,segments,state_blocks};
        let needed=reference_shell_charge(data.segments.len(),data.state_blocks.len())?;
        let owned=metadata_lease.split_off(needed).map_err(storage)?;
        Ok(Self{metadata:Arc::new(ReferenceOwner{data,_lease:owned}),captured:Arc::default(),previous_capture})
    }
    /// Call only after this selected closure and the journal have authenticated,
    /// under the exclusive journal owner. Reclaim crash-left derived packages
    /// before creating another recovery tail, without touching selected/pinned data.
    pub(super) fn reclaim_unselected_segments(&self,root:&Path)->Result<(),RecordError>{
        for entry in std::fs::read_dir(root).map_err(storage)? {
            let entry=entry.map_err(storage)?;let name=entry.file_name();let Some(name)=name.to_str() else{continue;};
            let Some(id)=name.strip_prefix("segment-") else{continue;};
            if !valid(id) || self.segments.iter().any(|s|s.id==id){continue;}
            package_pins::retire_if_unpinned(&entry.path(),||{
                if !entry.file_type().map_err(storage)?.is_dir(){return Err(storage("derived segment is not a directory"));}
                std::fs::remove_dir_all(entry.path()).map_err(storage)
            })?;
        }
        Ok(())
    }
    pub(super) fn verify_captured_suffix(&self,root:&Path,config:&RecordConfig,live:&State,frames:&[Vec<u8>],pending:&[Frozen])->Result<(),RecordError>{
        self.verify_suffix_with(root,config,live,pending,|accept|{for frame in frames {accept(frame)?;}Ok(())})
    }
    fn verify_suffix_with(&self,root:&Path,config:&RecordConfig,live:&State,pending:&[Frozen],
        visit:impl FnOnce(&mut dyn FnMut(&[u8])->std::io::Result<()>)->std::io::Result<()>)->Result<(),RecordError>{
        let _phase=maintenance::Phase::new("live_suffix_authentication_and_proof",live.seq);
        let captured=self.captured.get().ok_or_else(||storage("missing certified State proof owner"))?;
        // captured is set only after full load/manifest validation and cannot
        // mutate. Reauthenticated inventory binds the exact originally validated
        // manifest/base/deltas, not a sequence-keyed mutable cache.
        let (_path,manifest,_pool,_metadata)=self.authenticated_manifest(root,config.max_journal_bytes)?;
        if manifest.seq!=captured.seq || checkpoint_codec::json_digest(manifest.state.config())?!=checkpoint_codec::json_digest(config)? {
            return Err(storage("selected recovery proof identity mismatch"));
        }
        for package in self.segments.iter().chain(pending) {package.verify(root,closure_limit(config.max_journal_bytes)?)?;}
        if state_proof::length(live)? as u64>config.max_journal_bytes {return Err(storage("live State proof exceeds authority budget"));}
        let mut replayed=captured.try_clone()?;
        let mut admitted=state_proof::length(&replayed)?;
        let expected=checkpoint_codec::json_digest(self)?;
        let mut first=true;
        visit(&mut |bytes| {
            let result=(||->Result<(),RecordError>{
                let size=(bytes.len() as u64).checked_mul(32).ok_or_else(||storage("WAL proof decode overflow"))?;
                let _decode=crate::storage::build_budget::reserve_memory(size).map_err(storage)?;
                let entry:Entry=serde_json::from_slice(bytes).map_err(storage)?;
                if first {
                    first=false;
                    return match entry {Entry::Anchor(reference) if checkpoint_codec::json_digest(&reference)?==expected=>Ok(()),_=>Err(storage("WAL proof authority mismatch"))};
                }
                let growth=checkpoint_codec::json_length(&entry)?.checked_mul(3).ok_or_else(||storage("WAL proof growth overflow"))?;
                admitted=admitted.checked_add(growth).ok_or_else(||storage("WAL proof size overflow"))?;
                replayed.admit_bytes(admitted)?;
                match entry {
                    Entry::Applied(op)=>apply(&mut replayed,&op),
                    Entry::Expired(event)=>temporal::apply(&mut replayed,&event),
                    Entry::HistoryTrim(event)=>history::apply_trim(&mut replayed,&config.history,&event),
                    Entry::Anchor(_)|Entry::Checkpoint{..}=>Err(storage("authority in WAL suffix")),
                }
            })();result.map_err(|e|std::io::Error::other(e.to_string()))
        }).map_err(storage)?;
        if first{return Err(storage("missing WAL proof authority"));}
        state_proof::verify(config,live,&replayed)
    }
    fn authenticated_manifest(&self,root:&Path,authority_budget:u64)->Result<(PathBuf,Manifest,PoolReader,crate::storage::build_budget::MemoryLease),RecordError>{
        if self.segments.is_empty()||self.segments.len()>capacity::MAX_SEGMENTS{return Err(storage("invalid selected segment count"));}
        let mut ids=std::collections::BTreeSet::new();
        if self.segments.iter().any(|s|!ids.insert(&s.id)){return Err(storage("duplicate selected segment"));}
        let path=directory(root,&self.id)?;
        if files(&path,closure_limit(authority_budget)?)?.as_slice()!=self.files.as_slice(){return Err(storage("anchor closure checksum mismatch"));}
        let (manifest,mut manifest_lease)=read_manifest(&path.join("manifest"),authority_budget)?;
        if manifest.format!=7{return Err(storage("unsupported anchor format"));}
        let owners=manifest.state.owners()?;
        if owners.last()!=Some(&self.id.as_str()) || self.state_blocks.len()+1!=owners.len() {
            return Err(storage("state block closure count or head mismatch"));
        }
        for (block,owner) in self.state_blocks.iter().zip(&owners) {
            if block.id!=*owner || block.id==self.id {
                return Err(storage("state block closure order mismatch"));
            }
            let path=directory(root,&block.id)?;
            if files(&path,closure_limit(authority_budget)?)?.as_slice()!=block.files.as_slice() {
                return Err(storage("state block closure checksum mismatch"));
            }
        }
        validate_config(manifest.state.config())?;
        if self.segments.len()>capacity::segments(manifest.state.config()){return Err(storage("configured selected segment count exceeded"));}
        let mut limits=budget(manifest.state.config())?;limits.bytes=limits.bytes.min(authority_budget);
        // PoolReader retains a separate copy of the closure while Manifest is
        // still used. Admit that copy before clone, not under the State lease.
        manifest_lease.grow(metadata_charge(checkpoint_codec::json_length(&manifest.payloads)?)?).map_err(storage)?;
        let pool=PoolReader::open(root,manifest.payloads.clone(),limits).map_err(storage)?;
        Ok((path,manifest,pool,manifest_lease))
    }
    // Only for a service archive whose exact journal/closure was already fully
    // opened and validated. Recheck every bound byte without reopening readers.
    pub(super) fn archive_state(&self,root:&Path,authority_budget:u64)->Result<(RecordConfig,State,crate::storage::build_budget::MemoryLease),RecordError>{
        let (_path,manifest,pool,_lease)=self.authenticated_manifest(root,authority_budget)?;
        for segment in &self.segments {segment.verify(root,closure_limit(authority_budget)?)?;}
        let config_owner=crate::storage::build_budget::reserve_memory(metadata_charge(checkpoint_codec::json_length(manifest.state.config())?)?).map_err(storage)?;
        let config=manifest.state.config().clone();
        let state=manifest.state.read(&pool,authority_budget,|id|directory(root,id))?;
        if state.seq!=manifest.seq{return Err(storage("archive anchor sequence mismatch"));}
        Ok((config,state,config_owner))
    }
    pub(super) async fn load(&self,root:&Path,authority_budget:u64)->Result<Loaded,RecordError>{
        let (path,manifest,pool,_manifest_lease)=self.authenticated_manifest(root,authority_budget)?;
        let config=manifest.state.config().clone();
        let state=manifest.state.read(&pool,authority_budget,|id|directory(root,id))?;
        if state.seq!=manifest.seq{return Err(storage("anchor sequence mismatch"));}
        let view=self.load_components(root,&path,authority_budget,&config,&state,manifest.payloads).await?;
        if self.captured.get().is_none() { let _=self.captured.set(state.try_clone()?); }
        Ok(Loaded{config,state,index:view.index,ordinals:view.ordinals,locations:view.locations,payloads:view.payloads})
    }
    // Authenticate the sealed package against the owned captured cut. Its State
    // stays alive throughout verification and becomes the candidate; do not
    // deserialize a second complete history from the same package.
    pub(super) async fn load_captured(&self, root: &Path, config: RecordConfig, state: State) -> Result<Loaded, RecordError> {
        let view = self.load_verified(root, &config, &state).await?;
        Ok(Loaded { config, state, index: view.index, ordinals: view.ordinals,
            locations: view.locations, payloads: view.payloads })
    }
    async fn load_verified(&self,root:&Path,config:&RecordConfig,state:&State)->Result<VerifiedView,RecordError>{
        self.load_verified_encoded(root,config,&checkpoint_codec::StateEncoding::new(state)?).await
    }
    async fn load_verified_encoded(&self,root:&Path,config:&RecordConfig,encoded:&checkpoint_codec::StateEncoding<'_>)->Result<VerifiedView,RecordError>{
        let state=encoded.state();
        let _phase=maintenance::Phase::new("candidate_authentication_and_load",state.seq);
        let (path,manifest,pool,_manifest_lease)=self.authenticated_manifest(root,config.max_journal_bytes)?;
        if manifest.seq!=state.seq || serde_json::to_vec(manifest.state.config()).map_err(storage)?!=serde_json::to_vec(config).map_err(storage)? {
            return Err(storage("expected anchor identity mismatch"));
        }
        if manifest.state.deltas.is_empty() {
            manifest.state.base.verify_encoded(&path,&pool,config.max_journal_bytes,encoded)?;
        } else if let Some(captured)=self.captured.get() {
            state_proof::verify(config,state,captured)?;
        } else {
            let previous=self.previous_capture.as_ref().and_then(|c|c.get())
                .ok_or_else(||storage("missing certified predecessor owner"))?;
            let mut replayed=previous.try_clone()?;
            let last=manifest.state.deltas.last().unwrap();
            anchor_image::Image::apply_block(&last.delta,&path,&pool,config.max_journal_bytes,config,&mut replayed)?;
            state_proof::verify(config,state,&replayed)?;
        }
        manifest.state.validate_encoded(encoded,config.max_journal_bytes)?;
        let view=self.load_components(root,&path,config.max_journal_bytes,config,state,manifest.payloads).await?;
        if self.captured.get().is_none() { let _=self.captured.set(state.try_clone()?); }
        Ok(view)
    }
    async fn load_components(&self,root:&Path,path:&Path,authority_budget:u64,config:&RecordConfig,state:&State,payloads:Closure)->Result<VerifiedView,RecordError>{
        let statistics:Statistics=serde_json::from_slice(&read_bounded(&path.join("statistics"),closure_limit(authority_budget)?)?).map_err(storage)?;
        Self::load_parts(root,config,state,payloads,&self.segments,statistics.segments).await
    }
    #[cfg(feature="s3-object-store")]
    pub(super) async fn verify_derived_parts(root:&Path,config:&RecordConfig,state:&State,
        segments:&[segment::Package],statistics:Vec<crate::active::ActiveStatistics>)->Result<(),RecordError> {
        let verified=Self::load_parts(root,config,state,Closure::default(),segments,statistics).await?;
        drop(verified);Ok(())
    }
    async fn load_parts(root:&Path,config:&RecordConfig,state:&State,payloads:Closure,
        segments:&[segment::Package],statistics:Vec<crate::active::ActiveStatistics>)->Result<VerifiedView,RecordError>{
        if statistics.len()!=segments.len(){return Err(storage("segment statistics closure mismatch"));}
        let visible=evidence::visibility(&state.records)?;
        let expected:std::collections::BTreeSet<_>=state.records.values().filter(|r|visible.get(&r.record_id)==Some(&true)&&!(state.evidence_quarantined&&temporal::protected(r))).map(|r|r.record_id.clone()).collect();
        let n=segments.len();let mut locations=BTreeMap::new();let mut ordinals=Vec::new();let mut live=std::collections::BTreeSet::new();let mut indexes=Vec::new();
        let address_budget=config.max_records.checked_add(config.max_operations).and_then(|n|n.checked_mul(64)).ok_or_else(||storage("address budget overflow"))?;
        for (slot,(segment,active)) in segments.iter().zip(statistics).enumerate(){
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
        let mut shards=Vec::new();for component in &indexes{shards.push(component.read().await.segments[0].clone());}
        let mut native=Vec::new();for component in &indexes{native.extend(component.read().await.record_lexical.iter().cloned());}
        {let mut root=index.write().await;root.segments=shards;root.record_lexical=native;}
        Ok(VerifiedView{index,ordinals,locations,payloads})
    }
}

// A private independent root has no live query view to rotate. Build and fully
// authenticate its selected package, then return the bounded authority frame.
// The caller owns its journal and performs the unchanged checkpoint/root publish.
pub(super) async fn snapshot_payload(root:&Path,config:&RecordConfig,state:&State)
    ->Result<(Vec<u8>,crate::storage::build_budget::MemoryLease,crate::storage::build_budget::DiskScope),RecordError>{
    let disk=crate::storage::build_budget::DiskScope::begin(root,closure_limit(config.max_journal_bytes)?).map_err(storage)?;
    let encoded=checkpoint_codec::StateEncoding::new(state)?;
    let reference=Reference::prepare_encoded(root,config,&encoded,Closure::default(),None,None,true,None,None,None).await?;
    let verified=reference.load_verified_encoded(root,config,&encoded).await?;
    drop(verified); // Close all mappings before later Windows root publication.
    let estimate=checkpoint_codec::json_length(&reference)?.checked_add(64).ok_or_else(||storage("snapshot authority overflow"))?;
    let lease=crate::storage::build_budget::reserve_memory(metadata_charge(estimate)?).map_err(storage)?;
    let payload=encode_entry(&Entry::Anchor(reference))?;
    PartitionJournal::validate_initial(payload.len(),config.max_journal_bytes).map_err(storage)?;
    Ok((payload,lease,disk))
}

impl Inner {
    pub(super) async fn replay_anchor_tail(&mut self,entry:Entry)->Result<(),RecordError>{
        self.replay_context().replay(entry).await
    }
}

impl Inner {
    #[cfg(any(test,feature="s3-object-store"))]
    pub(super) async fn publish_anchor(&mut self)->Result<u64,RecordError>{
        self.publish_anchor_candidate(None,true).await
    }
    pub(super) async fn publish_anchor_state(&mut self,next:State,reuse:bool)->Result<u64,RecordError>{
        self.publish_anchor_candidate(Some(next),reuse).await
    }
    async fn publish_anchor_candidate(&mut self,next:Option<State>,reuse:bool)->Result<u64,RecordError>{
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
    pub(super) async fn stage_stable_tail(&mut self)->Result<(),RecordError> {
        let root=self.journal.root().to_path_buf();
        let target=root.join(format!("segment-{:032x}",rand::random::<u128>()));
        let (successor,_)=build_at(&root,&target,&self.config,&State::default()).await?;
        successor.read().await.record_lexical[0].admit_writer().await?;
        let budget=view::RotationBudget{max_segments:capacity::segments(&self.config).checked_add(1).ok_or_else(||storage("recovery segment limit overflow"))?,max_address_slots:self.config.max_records.checked_add(self.config.max_operations).and_then(|n|n.checked_mul(64)).ok_or_else(||storage("view address budget overflow"))?};
        // Fence before the first potentially destructive seal. A successful
        // private handoff restores availability even if certificate preparation
        // later rejects; an uncertain/failed handoff requires authoritative reopen.
        let fenced=self.output.read().map_err(|_|RecordError::RecoveryRequired)?.fenced;
        self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
        self.healthy=false;
        self.index.seal_into_successor(&root,successor,&mut self.ordinals,&mut self.locations,budget,closure_limit(self.config.max_journal_bytes)?).await?;
        self.healthy=true;
        self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=fenced;
        Ok(())
    }
    /// Local derived index sealing does not advance or replace journal authority.
    /// Recovery reconstructs pending packages from the complete committed suffix.
    pub(super) async fn maintain_index_tail(&mut self)->Result<(),RecordError> {
        self.index.drain_batch_prefix(self.state.seq).await?;
        if self.background.is_some(){return Err(RecordError::Busy);}
        // The certificate continues selecting its own immutable packages. The
        // live derived view has an independent finite component allowance plus
        // one successor, so foreground writes can proceed during a merge.
        let limit=capacity::segments(&self.config)+1;
        if self.index.components().await.len()>=limit {
            return Err(self.request_checkpoint(cache_merge::CAUSE));
        }
        let root=self.journal.root().to_path_buf();
        let _disk=crate::storage::build_budget::DiskScope::begin(&root,closure_limit(self.config.max_journal_bytes)?).map_err(storage)?;
        let target=root.join(format!("cache-{:032x}",rand::random::<u128>()));
        let (successor,_)=build_at(&root,&target,&self.config,&State::default()).await?;
        successor.read().await.record_lexical[0].admit_writer().await?;
        let budget=view::RotationBudget{max_segments:limit,max_address_slots:cache_merge::address_limit(&self.config)?};
        let fenced=self.output.read().map_err(|_|RecordError::RecoveryRequired)?.fenced;
        self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
        self.healthy=false;
        self.index.seal_cache_into_successor(successor,&mut self.ordinals,&mut self.locations,budget).await?;
        self.healthy=true;
        self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=fenced;
        if self.index.components().await.len()>=limit && self.maintenance_requested.is_none() {
            self.maintenance_requested=Some(cache_merge::CAUSE);
        }
        Ok(())
    }
    pub(super) async fn ensure_journal_room(&mut self,bytes:usize,reserve:usize)->Result<(),RecordError> {
        if self.journal.checkpoint_can_make_room(bytes,reserve){
            // The last background checkpoint already compacted this physical
            // cut. With no intervening append another rebuild cannot help.
            if self.maintenance_compacted_bytes==Some(self.journal.physical_bytes()) {
                return self.validate_journal_room(bytes,reserve);
            }
            log::info!("record_maintenance cause=journal_room seq={} incoming_bytes={} reserved_bytes={}",self.state.seq,bytes,reserve);
            return Err(self.request_checkpoint("journal_room"));
        }
        self.validate_journal_room(bytes,reserve)
    }
    pub(super) fn validate_journal_room(&self,bytes:usize,reserve:usize)->Result<(),RecordError> {
        self.journal.validate_append_with_reserve(bytes,reserve).map_err(|e|{
            if e.kind()==std::io::ErrorKind::InvalidInput {RecordError::Rejected(e.to_string())}else{storage(e)}
        })
    }
    async fn prepare_and_publish_anchor(&mut self,candidate:Option<State>,reuse:bool)->Result<u64,RecordError>{
        self.index.drain_batch_prefix(self.state.seq).await?;
        let _phase=maintenance::Phase::new("full_checkpoint",self.state.seq);
        #[cfg(feature="s3-object-store")]
        if let Some(remote)=self.remote.as_ref() {
            if let Err(error)=remote.verify_seal_inputs(self.anchor.as_ref(),self.state.seq,&mut self.journal,self.config.max_journal_bytes).await {
                if matches!(error,RecordError::Busy){return Err(error);}
                self.healthy=false;
                self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
                return Err(error);
            }
        }
        self.reclaim_anchor_objects().await?;
        let root=self.journal.root().to_path_buf();
        let _disk=crate::storage::build_budget::DiskScope::begin(&root,closure_limit(self.config.max_journal_bytes)?).map_err(storage)?;
        if self.index.detached_cache {
            // A disposable cache no longer has the certificate's topology.
            // Full authority replacement must still authenticate its original
            // selected closure and WAL, never certify over hidden corruption.
            let source=maintenance::SourceProof::capture(self)?;
            if let Err(error)=source.verify(&root,&self.config,&self.state) {
                if matches!(error,RecordError::Busy){return Err(error);}
                self.healthy=false;
                self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
                return Err(RecordError::RecoveryRequired);
            }
        }
        // Pre-admit a separate writer before making the current tail immutable.
        // Explicit replacement/compaction remains on the authoritative rebuild path.
        if candidate.is_none() && reuse && !self.index.detached_cache && self.anchor.as_ref().is_some_and(|a|
            a.segments.len()+self.index.pending_seals.len()+1<capacity::segments(&self.config))
            && self.index.stable_tail_path(&root).await.is_some()
            && self.index.local_statistics().await?.first().is_some_and(|s|!s.keys.is_empty()) {
            self.stage_stable_tail().await?;
        }
        // The partition guard pins the live State. Ordinary compaction borrows
        // it instead of cloning all retained history. Explicit replacement
        // candidates (snapshot/restore) still own their independent State.
        let next=candidate.as_ref().unwrap_or(&self.state);
        let next_seq=next.seq;
        let encoded={let _phase=maintenance::Phase::new("state_measure_and_digest",next.seq);checkpoint_codec::StateEncoding::new(next)?};
        let snapshot_bytes=encoded.checkpoint_length(&self.config)?;
        let prior=if reuse{self.payloads.clone()}else{Closure::default()};
        let mut reference=Reference::prepare_encoded(&root,&self.config,&encoded,prior,self.anchor.as_ref(),Some(&self.index),!reuse,if candidate.is_none() && reuse {Some(&mut self.journal)}else{None},None,None).await?;
        #[cfg(test)]
        if root.join(".test-anchor-reject-prepared").exists() {
            return Err(storage("injected candidate rejection before certificate"));
        }
        // Verify every persisted frame against the pinned State without a
        // second full-history allocation; no authority changes before this succeeds.
        let loaded=reference.load_verified_encoded(&root,&self.config,&encoded).await?;
        // The newly verified shared cut replaces the predecessor proof owner;
        // do not retain an extra old State through subsequent publications.
        reference.previous_capture=None;
        let mut index=view::RetrievalView::single(loaded.index);
        let mut ordinals=loaded.ordinals;
        let mut locations=loaded.locations;
        let cache=root.join(format!("segment-{:032x}",rand::random::<u128>()));
        let (tail,_)=build_at(&root,&cache,&self.config,&State::default()).await.map_err(|e|storage(format!("stage=anchor_empty_tail {e}")))?;
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
            remote.publish_anchor(&root,&reference,&loaded.payloads,next_seq,self.config.max_journal_bytes,false).await?;
        }
        let journal_phase=maintenance::Phase::new("journal_certificate_install",next_seq);
        #[cfg(test)] indexer::pause_journal_install(&root).await;
        // Remote publication and native sealing have finished. The local
        // generation, all witnesses and PUBLISHED share one synchronous loan.
        let permit = match crate::resources::Scope::current() {
            Some(scope) => scope.journal_io().await.map(Some)
                .map_err(|error| CreateError::NotPublished(std::io::Error::other(error))),
            None => Ok(None),
        };
        let journal_result = match permit {
            Ok(Some(permit)) => permit.run(|| self.journal.checkpoint(&payload)),
            Ok(None) => self.journal.checkpoint(&payload),
            Err(error) => Err(error),
        };
        drop(journal_phase);
        match journal_result{
            Ok(())=>{},
            Err(error@CreateError::NotPublished(_))=>{
                #[cfg(feature="s3-object-store")]
                if self.remote.is_some() {return Err(RecordError::DurablePending(next_seq));}
                self.healthy=true;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=fenced;
                return Err(creation_error(error));
            },
            Err(error)=>return Err(creation_error(error)),
        }
        // All fallible preparation precedes the authority certificate. The
        // partition guard excludes readers until the complete view is installed.
        self.index=index;self.ordinals=ordinals;self.locations=locations;
        self.anchor=Some(reference);self.payloads=loaded.payloads;if let Some(candidate)=candidate {self.state=candidate;}self.snapshot_bytes=snapshot_bytes;
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
        let _phase=maintenance::Phase::new("gc_reclaim",self.state.seq);
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
                valid(id)&&self.anchor.as_ref().is_none_or(|reference|reference.id!=id&&!reference.state_blocks.iter().any(|b|b.id==id))
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
    #[tokio::test]
    async fn incomplete_identities_are_rejected_on_package_open() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(write("a", "a", "alpha", ExpectedVersion::Absent)).await.unwrap();
        let inner = p.inner.lock().await;
        let (package, active) = segment::Package::prepare(&root, &inner.config, &inner.state, None).await.unwrap();
        for field in ["revisions", "fingerprints"] {
            let mut missing = serde_json::to_value(&active).unwrap();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<crate::active::ActiveStatistics>(missing).is_err(), "{field}");
            let mut truncated = active.clone();
            if field == "revisions" { truncated.revisions.clear(); }
            else { truncated.fingerprints.clear(); }
            assert!(package.load(&root, &inner.config, truncated).await.is_err(), "{field}");
        }
        let mut null = serde_json::to_value(&active).unwrap();
        null["fingerprints"][0] = serde_json::Value::Null;
        assert!(serde_json::from_value::<crate::active::ActiveStatistics>(null).is_err());
        package.load(&root, &inner.config, active).await.unwrap();
    }

    #[tokio::test]
    async fn manifest_writer_and_reader_agree_at_exact_byte_boundary() {
        let root=tempfile::tempdir().unwrap();
        let path=root.path().join("partition");
        let p=RecordPartition::create(&path,config()).await.unwrap();
        p.write(write("first","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let reference=p.inner.lock().await.anchor.clone().unwrap();
        let file=directory(&path,&reference.id).unwrap().join("manifest");
        let bytes=std::fs::metadata(&file).unwrap().len();
        let (manifest,_owner)=read_manifest(&file,bytes).unwrap();
        assert_eq!(checkpoint_codec::json_length(&manifest).unwrap() as u64,bytes);
        assert!(checkpoint_codec::json_fits(&manifest,bytes).unwrap());
        assert!(!checkpoint_codec::json_fits(&manifest,bytes-1).unwrap());
        assert!(read_manifest(&file,bytes-1).is_err());
    }
    #[test]
    fn reference_metadata_pre_admission_shared_owner_and_failure_release() {
        const FLAG:&str="ANYSEEK_ANCHOR_METADATA_CHILD";
        if std::env::var_os(FLAG).is_none() {
            let output=std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact","record::anchor::tests::reference_metadata_pre_admission_shared_owner_and_failure_release","--nocapture"])
                .env(FLAG,"1").env("ANYSEEK_BUILD_MEMORY_BYTES",(64u64*1024*1024).to_string())
                .env("ANYSEEK_BUILD_SHARED_MEMORY_BYTES",(128u64*1024*1024).to_string()).output().unwrap();
            assert!(output.status.success(),"{}\n{}",String::from_utf8_lossy(&output.stdout),String::from_utf8_lossy(&output.stderr));return;
        }
        let _fill=[crate::storage::build_budget::reserve_memory(64*1024*1024).unwrap(),
            crate::storage::build_budget::reserve_memory(64*1024*1024-131072).unwrap()];
        let before=crate::storage::build_budget::used_memory_for_test();
        let raw=r#"{"id":"00000000000000000000000000000001","files":[],"segments":[],"state_blocks":[]}"#;
        let reference:Reference=serde_json::from_str(raw).unwrap();
        let charged=crate::storage::build_budget::used_memory_for_test();
        assert_eq!(charged-before,reference.metadata._lease.bytes()+reference.files.charge_for_test());
        let second=reference.clone();
        assert!(Arc::ptr_eq(&reference.metadata,&second.metadata));
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),charged);
        drop(reference);
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),charged);
        drop(second);
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
        assert!(serde_json::from_str::<Reference>(r#"{"id":0}"#).is_err());
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
        let oversized=format!(r#"{{"unknown":"{}"}}"#,"x".repeat(8192));
        let error=serde_json::from_str::<Reference>(&oversized).unwrap_err().to_string();
        assert!(error.contains("class=shared"),"admission must precede field decoding: {error}");
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
        assert!(metadata_charge(usize::MAX).is_err());
        let root=tempfile::tempdir().unwrap();
        let path=root.path().join("manifest");
        std::fs::write(&path," ".repeat(8192)).unwrap();
        assert!(read_manifest(&path,16384).err().unwrap().to_string().contains("class=shared"));
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
    }
    #[tokio::test]
    async fn incremental_state_same_sequence_maintenance_and_independent_backup() {
        struct FixedClock(u64);
        impl temporal::Clock for FixedClock {
            fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0)}
        }
        let root=tempfile::tempdir().unwrap();
        let path=root.path().join("partition");
        let backup=root.path().join("backup");
        let settings=config();
        let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
        p.write(write("first","a","base-body",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        p.write(write("second","b","delta-body",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let (sequence,floor,time)={
            let mut inner=p.inner.lock().await;
            let seq=inner.state.seq;
            let future=inner.state.time_watermark_ms+inner.config.history.retention_ms+1;
            inner.clock=Arc::new(FixedClock(future));
            inner.trim_history(future).await.unwrap();
            inner.maintain_index_tail().await.unwrap();
            assert_eq!(inner.state.seq,seq);
            assert!(inner.state.history.floor>0);
            (seq,inner.state.history.floor,inner.state.time_watermark_ms)
        };
        p.compact_journal().await.unwrap();
        assert_eq!(p.inner.lock().await.anchor.as_ref().unwrap().state_blocks.len(),2);
        p.checkpoint(&backup).await.unwrap();
        drop(p);
        let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        {
            let inner=p.inner.lock().await;
            assert_eq!((inner.state.seq,inner.state.history.floor,inner.state.time_watermark_ms),(sequence,floor,time));
            assert_eq!(inner.state.operations.len(),2);
        }
        drop(p);
        std::fs::remove_dir_all(&path).unwrap();
        let copy=RecordPartition::open(&backup,settings.max_journal_bytes).await.unwrap();
        let inner=copy.inner.lock().await;
        assert!(inner.anchor.as_ref().unwrap().state_blocks.is_empty(),"independent backup must materialize its own full base");
        assert_eq!(inner.state.seq,sequence);
        assert_eq!(inner.state.operations.len(),2);
        assert_eq!(inner.state.records["a"].document.as_ref().unwrap()["body"],serde_json::json!("base-body"));
        assert_eq!(inner.state.records["b"].document.as_ref().unwrap()["body"],serde_json::json!("delta-body"));
    }
    #[tokio::test]
    async fn incremental_state_closure_survives_rejection_gc_and_reopen() {
        let root=tempfile::tempdir().unwrap();
        let path=root.path().join("partition");
        let settings=config();
        let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
        let mut receipts=Vec::new();
        for i in 0..2 {
            let request=write(&format!("op-{i}"),&format!("id-{i}"),&format!("alpha distinct-body-{i}"),ExpectedVersion::Absent);
            receipts.push((request.clone(),p.write(request).await.unwrap()));
            p.compact_journal().await.unwrap();
        }
        let reference=p.inner.lock().await.anchor.clone().unwrap();
        assert_eq!(reference.state_blocks.len(),1);
        let base=directory(&path,&reference.state_blocks[0].id).unwrap();
        assert!(base.exists(),"selected base cannot be collected with its old certificate");
        let before=checkpoint_codec::json_digest(&reference).unwrap();
        let request=write("op-2","id-2","beta",ExpectedVersion::Absent);
        receipts.push((request.clone(),p.write(request).await.unwrap()));
        std::fs::write(path.join(".test-anchor-reject-loaded"),b"reject").unwrap();
        assert!(p.compact_journal().await.is_err());
        assert_eq!(checkpoint_codec::json_digest(p.inner.lock().await.anchor.as_ref().unwrap()).unwrap(),before);
        assert!(base.exists());
        std::fs::remove_file(path.join(".test-anchor-reject-loaded")).unwrap();
        p.compact_journal().await.unwrap();
        let digest=checkpoint_codec::json_digest(&p.inner.lock().await.state).unwrap();
        assert_eq!(p.inner.lock().await.anchor.as_ref().unwrap().state_blocks.len(),2);
        drop(reference);drop(p);
        let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        assert_eq!(checkpoint_codec::json_digest(&p.inner.lock().await.state).unwrap(),digest);
        for (request,receipt) in receipts {assert_eq!(p.write(request).await.unwrap(),receipt);}
        assert_eq!(p.inner.lock().await.state.operations.len(),3);
        drop(p);
        // The dependency inventory remains authenticated after its original
        // anchor stopped being the current business head.
        let file=base.join("state-0000");
        let mut bytes=std::fs::read(&file).unwrap();
        *bytes.last_mut().unwrap()^=1;
        std::fs::write(file,bytes).unwrap();
        assert!(RecordPartition::open(&path,settings.max_journal_bytes).await.is_err());
    }
    #[tokio::test]
    async fn captured_cut_retains_owner_and_rejects_mismatch_without_install() {
        use super::*;
        use super::super::tests::{config, write};
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("partition");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(write("first", "a", "alpha", ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let (reference, settings, snapshot) = {
            let inner = p.inner.lock().await;
            (inner.anchor.clone().unwrap(), inner.config.clone(), inner.state.try_clone().unwrap())
        };
        let owner = Arc::as_ptr(snapshot.residency.as_ref().unwrap());
        let original = checkpoint_codec::json_digest(&snapshot).unwrap();
        let loaded = reference.load_captured(&root, settings.clone(), snapshot).await.unwrap();
        assert_eq!(owner, Arc::as_ptr(loaded.state.residency.as_ref().unwrap()));
        assert_eq!(original, checkpoint_codec::json_digest(&loaded.state).unwrap());
        let mut invalid = loaded.state.try_clone().unwrap();
        invalid.time_watermark_ms += 1;
        let released = Arc::downgrade(invalid.residency.as_ref().unwrap());
        assert!(reference.load_captured(&root, settings, invalid).await.is_err());
        assert!(released.upgrade().is_none(), "failed candidate releases its State owner");
        assert_eq!(original, checkpoint_codec::json_digest(&p.inner.lock().await.state).unwrap());
        assert_eq!(p.statistics().await.unwrap().visible_seq, 1);
    }
    #[tokio::test]
    async fn anchor_tail_vector_patch_and_temporal_metadata_recover_exact_sizes_and_receipts() {
        for failure in [0,1,2] {
            let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");let settings=config();
            let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
            let mut initial=write("initial","source","alpha original",ExpectedVersion::Absent);
            initial.document.as_mut().unwrap().remove("vector");
            p.write(initial).await.unwrap();p.compact_journal().await.unwrap();
            let anchor=serde_json::to_vec(&p.inner.lock().await.anchor).unwrap();
            {let i=p.inner.lock().await;assert_eq!(i.snapshot_bytes,checkpoint_codec::length(&i.config,&i.state).unwrap(),"after compact");}
            let mut patch=write("patch","source","unused",ExpectedVersion::Match(1));patch.document=None;
            patch.vector_patch=Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
            let patch_receipt=p.write(patch.clone()).await.unwrap();assert_eq!(patch_receipt.version,1);
            {let i=p.inner.lock().await;assert_eq!(i.snapshot_bytes,checkpoint_codec::length(&i.config,&i.state).unwrap(),"after patch");}
            let mut timed=write("timed","timed","alpha temporal",ExpectedVersion::Absent);
            let metadata=TemporalMetadata{event_time_ms:Some(-1),valid_from_ms:Some(0),valid_to_ms:Some(1),expire_at_ms:Some(253_402_300_799_998)};
            timed.temporal=Some(metadata.clone());
            if failure==1 {p.inner.lock().await.fail_after_durable=true;}
            if failure==2 {p.inner.lock().await.fail_publication_stage=1;}
            let timed_receipt=WriteReceipt{seq:3,version:1,record_id:"timed".into()};
            let result=p.write(timed.clone()).await;
            if failure==0 {assert_eq!(result.unwrap(),timed_receipt);} else {assert!(matches!(result,Err(RecordError::DurablePending(3))));}
            assert_eq!(serde_json::to_vec(&p.inner.lock().await.anchor).unwrap(),anchor,"suffix must not be absorbed by another compact");
            drop(p);
            let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
            {
                let inner=p.inner.lock().await;
                assert_eq!(inner.state.seq,3);assert_eq!(inner.state.operations.len(),3);
                assert_eq!(inner.snapshot_bytes,checkpoint_codec::length(&inner.config,&inner.state).unwrap());
                assert_eq!(inner.state.records["timed"].temporal,metadata);
                assert_eq!(inner.state.records["source"].document.as_ref().unwrap()["body"],"alpha original");
                assert_eq!(inner.state.records["source"].document.as_ref().unwrap()["vector"],serde_json::json!([2.0,0.0]));
            }
            assert_eq!(p.write(patch).await.unwrap(),patch_receipt);
            assert_eq!(p.write(timed).await.unwrap(),timed_receipt);
            assert_eq!(p.inner.lock().await.state.seq,3);
            drop(p);
        }
    }

    use super::*;
    use super::super::tests::{path,config,write,query};
    #[tokio::test]
    async fn resource_admission_rejects_before_wal_and_retries_after_space_returns() {
        let root=path();let mut settings=config();settings.max_record_bytes=1024*1024;settings.max_journal_bytes=8*1024*1024;
        // Keep the document below the independent 1MiB record limit, while
        // enough lexical fields make its conservative analysis exceed 1GiB.
        for i in 0..12 {
            let mut field=settings.schema.iter().find(|f| f.field=="body").unwrap().clone();
            field.field=format!("budget-field-{i}");settings.schema.push(field);
        }
        let p=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let request=write("oversized-analysis","a",&"a ".repeat(300_000),ExpectedVersion::Absent);
        assert!(build_budget::document_estimate(request.document.as_ref().unwrap(), &settings.schema).unwrap() > crate::storage::build_budget::BUILD_MEMORY);
        let error = p.write(request).await.unwrap_err();
        assert!(matches!(&error, RecordError::Rejected(message) if message.contains("record build resource budget exceeded: class=single")), "{error:?}");
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
    async fn automatic_seal_leaves_checkpoint_unchanged_and_replays_complete_suffix() {
        let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");let mut settings=config();
        settings.max_records=256;settings.max_operations=512;settings.max_candidates=256;
        settings.max_journal_bytes=16*1024*1024;
        let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
        p.write(write("seed","seed","alpha",ExpectedVersion::Absent)).await.unwrap();p.compact_journal().await.unwrap();
        let selected=p.inner.lock().await.anchor.as_ref().unwrap().id.clone();
        let mut receipts=Vec::new();
        for n in 0..129 {let req=write(&format!("op-{n}"),&format!("r-{n}"),"alpha",ExpectedVersion::Absent);let receipt=p.write(req.clone()).await.unwrap();receipts.push((req,receipt));}
        {let inner=p.inner.lock().await;assert_eq!(inner.anchor.as_ref().unwrap().id,selected,"native rotation must not replace the State checkpoint");assert!(inner.index.detached_cache);assert_eq!(inner.index.components().await.len(),3);}
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],200).await.unwrap().count_total,130);
        drop(p);
        std::fs::write(path.join(".test-recovery-after-derived-seal"),b"fail").unwrap();
        let mut leftovers=None;
        for _ in 0..3 {
            assert!(RecordPartition::open(&path,settings.max_journal_bytes).await.is_err());
            let count=std::fs::read_dir(&path).unwrap().filter_map(Result::ok).filter(|e|{let name=e.file_name();let name=name.to_string_lossy();name.starts_with("segment-")||name.starts_with("cache-")}).count();
            if let Some(expected)=leftovers {assert_eq!(count,expected,"interrupted recovery cannot accumulate derived packages");} else {leftovers=Some(count);}
        }
        std::fs::remove_file(path.join(".test-recovery-after-derived-seal")).unwrap();
        let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.inner.lock().await.anchor.as_ref().unwrap().id,selected);
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],200).await.unwrap().count_total,130);
        for (req,receipt) in receipts {assert_eq!(p.write(req).await.unwrap(),receipt);}
        p.compact_journal().await.unwrap();assert_ne!(p.inner.lock().await.anchor.as_ref().unwrap().id,selected);
    }
    #[tokio::test]
    async fn derived_package_bound_merges_without_changing_checkpoint_or_dropping_records() {
        let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");let settings=config();
        let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
        p.write(write("seed","seed","alpha",ExpectedVersion::Absent)).await.unwrap();p.compact_journal().await.unwrap();
        let original=p.inner.lock().await.anchor.as_ref().unwrap().id.clone();
        let bound=capacity::segments(&settings);
        for n in 0..bound {
            super::super::tests::write_after_maintenance(&p,write(&format!("op-{n}"),&format!("r-{n}"),"alpha",ExpectedVersion::Absent)).await.unwrap();
            let result=p.inner.lock().await.maintain_index_tail().await;
            if !matches!(result,Err(RecordError::Busy)) {result.unwrap();}
            p.kick_maintenance().await;super::super::tests::wait_maintenance_idle(&p).await;
            let inner=p.inner.lock().await;
            assert!(inner.index.local_statistics().await.unwrap().len()<=bound+1);
            assert!(inner.anchor.as_ref().unwrap().segments.len()<=bound);
        }
        p.kick_maintenance().await;super::super::tests::wait_maintenance_idle(&p).await;
        assert_eq!(p.inner.lock().await.anchor.as_ref().unwrap().id,original);
        drop(p);let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        assert_eq!(query(&p,SearchMode::Lexical).await.hits.len(),bound+1);
        assert_eq!(p.inner.lock().await.state.operations.len(),bound+1);
    }
    #[tokio::test]
    async fn journal_pressure_checkpoints_before_append_and_preserves_unknown_outcome() {
        for unknown in [false,true] {
            let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");let settings=config();
            let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
            p.write(write("seed","seed","alpha",ExpectedVersion::Absent)).await.unwrap();p.compact_journal().await.unwrap();
            let before=p.inner.lock().await.anchor.as_ref().unwrap().id.clone();
            {let mut inner=p.inner.lock().await;
                assert!(matches!(inner.ensure_journal_room(usize::MAX,0).await,Err(RecordError::Rejected(_))));
                assert_eq!(inner.anchor.as_ref().unwrap().id,before);assert!(inner.healthy);
            }
            for n in 0..6 {p.write(write(&format!("op-{n}"),&format!("r-{n}"),&format!("alpha {}","x".repeat(3000)),ExpectedVersion::Absent)).await.unwrap();}
            if unknown {std::fs::write(path.join(".test-checkpoint-after-certificate"),b"fail").unwrap();}
            let size={let mut inner=p.inner.lock().await;
                let base=encode_entry(&Entry::Anchor(inner.anchor.clone().unwrap())).unwrap().len();
                let size=settings.max_journal_bytes as usize-base-16384;
                assert!(inner.journal.checkpoint_can_make_room(size,0));
                assert!(matches!(inner.ensure_journal_room(size,0).await,Err(RecordError::Busy)));
                assert!(inner.healthy);assert_eq!(inner.anchor.as_ref().unwrap().id,before);size
            };
            p.kick_maintenance().await;super::super::tests::wait_maintenance_idle(&p).await;
            {let inner=p.inner.lock().await;
                if unknown {assert!(!inner.healthy);assert!(inner.output.read().unwrap().fenced);}
                else {assert_ne!(inner.anchor.as_ref().unwrap().id,before);assert!(inner.journal.validate_append_with_reserve(size,0).is_ok());}
            }
            if unknown {std::fs::remove_file(path.join(".test-checkpoint-after-certificate")).unwrap();}
            drop(p);let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
            assert_eq!(query(&p,SearchMode::Lexical).await.hits.len(),7);
        }
    }
    #[tokio::test]
    async fn checkpoint_proof_rejects_old_closure_or_semantic_wal_damage_after_cache_rotation() {
        for target in ["payload","state","selected","wal"] {
            let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");let settings=config();
            let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
            for id in ["a","b"] {p.write(write(id,id,"alpha",ExpectedVersion::Absent)).await.unwrap();p.compact_journal().await.unwrap();}
            p.write(write("c","c","alpha",ExpectedVersion::Absent)).await.unwrap();
            let selected={
                let mut inner=p.inner.lock().await;inner.maintain_index_tail().await.unwrap();
                let selected=inner.anchor.as_ref().unwrap().id.clone();
                if target=="wal" {inner.journal.append(br#"{"RecordHistoryTrimV13":{"previous":0,"floor":999999,"at":0}}"#).unwrap();}
                else {
                    let file=match target {
                        "payload"=>path.join(format!("payload-{}",inner.payloads.objects.keys().next().unwrap())),
                        "state"=>{let block=&inner.anchor.as_ref().unwrap().state_blocks[0];directory(&path,&block.id).unwrap().join(&block.files[0].path)},
                        _=>{let package=&inner.anchor.as_ref().unwrap().segments[0];package.path(&path).unwrap().join(&package.files[0].path)},
                    };
                    use std::io::{Seek,Write};let mut file=std::fs::OpenOptions::new().write(true).open(file).unwrap();
                    file.seek(std::io::SeekFrom::End(0)).unwrap();file.write_all(b"corrupt").unwrap();file.sync_all().unwrap();
                }
                selected
            };
            assert!(p.compact_journal().await.is_err(),"{target}");
            assert_eq!(p.inner.lock().await.anchor.as_ref().unwrap().id,selected,"corrupt authority must not be replaced");
            drop(p);
            assert!(RecordPartition::open(&path,settings.max_journal_bytes).await.is_err(),"{target}");
        }
    }
    #[tokio::test]
    async fn direct_tail_seal_reuses_bytes_and_rejection_keeps_successor_writable() {
        let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");
        let settings=config();let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
        p.write(write("a","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        let tail=p.inner.lock().await.index.stable_tail_path(&path).await.unwrap();
        let b=write("b","b","alpha beta",ExpectedVersion::Absent);
        let receipt=p.write(b.clone()).await.unwrap();
        let held=p.inner.lock().await.index.test_index().read().await.record_lexical[0].snapshot().await.unwrap();
        std::fs::write(path.join(".test-anchor-reject-loaded"),b"reject").unwrap();
        assert!(p.compact_journal().await.is_err());
        let sealed={let inner=p.inner.lock().await;assert!(inner.healthy);
            assert_eq!(inner.index.pending_seals.len(),1);
            let package=inner.index.pending_seals[0].clone();assert_eq!(package.path(&path).unwrap(),tail);
            package.verify(&path,closure_limit(settings.max_journal_bytes).unwrap()).unwrap();package};
        assert_eq!(query(&p,SearchMode::Lexical).await.hits.len(),2);
        p.write(write("c","c","alpha gamma",ExpectedVersion::Absent)).await.unwrap();
        assert_eq!(query(&p,SearchMode::Lexical).await.hits.len(),3);
        assert_eq!(held.cut.sequence,receipt.seq,"held snapshot keeps the pre-successor cut");
        std::fs::remove_file(path.join(".test-anchor-reject-loaded")).unwrap();
        p.compact_journal().await.unwrap();
        {let inner=p.inner.lock().await;let chosen=inner.anchor.as_ref().unwrap().segments.iter().find(|s|s.id==sealed.id).expect("same native package identity, not rebuilt from bodies");assert_eq!(chosen.files.as_slice(),sealed.files.as_slice());assert!(inner.index.pending_seals.is_empty());}
        drop(p);let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.write(b).await.unwrap(),receipt);
        assert_eq!(query(&p,SearchMode::Lexical).await.hits.len(),3);
    }
    #[tokio::test]
    async fn pending_seals_keep_retired_versions_out_of_vector_and_field_queries() {
        let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");let settings=config();
        let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
        p.write(write("a1","a","alpha old",ExpectedVersion::Absent)).await.unwrap();p.compact_journal().await.unwrap();
        p.write(write("b1","b","alpha old",ExpectedVersion::Absent)).await.unwrap();
        std::fs::write(path.join(".test-anchor-reject-prepared"),b"reject").unwrap();
        assert!(p.compact_journal().await.is_err());
        let updated=write("b2","b","alpha current",ExpectedVersion::Match(1));let receipt=p.write(updated.clone()).await.unwrap();
        let mut deleted=write("a-delete","a","",ExpectedVersion::Match(1));deleted.document=None;p.write(deleted).await.unwrap();
        assert!(p.compact_journal().await.is_err());
        assert_eq!(p.inner.lock().await.index.pending_seals.len(),2);
        let check=async |p:&RecordPartition| {
            for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All}] {
                let result=query(p,mode).await;assert_eq!(result.hits.len(),1);
                assert_eq!(result.hits[0].record.record_id,"b");assert_eq!(result.hits[0].record.version,2);
            }
            let field_query=|text:&str| RecordQuery {temporal:None,text:text.into(),vector:None,mode:SearchMode::Lexical,query_type:QueryType::Union,offset:0,length:10,result_type:ResultType::Topk,fields:vec!["body".into()],filters:vec![],sort:vec![]};
            let result=p.query(field_query("current")).await.unwrap();
            assert_eq!(result.hits.len(),1);assert_eq!(result.hits[0].record.version,2);
            assert!(p.query(field_query("old")).await.unwrap().hits.is_empty());
        };
        check(&p).await;
        std::fs::remove_file(path.join(".test-anchor-reject-prepared")).unwrap();p.compact_journal().await.unwrap();
        check(&p).await;drop(p);let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        check(&p).await;assert_eq!(p.write(updated).await.unwrap(),receipt);
    }
    #[tokio::test]
    async fn direct_tail_seal_unknown_certificate_fences_and_reopens() {
        for gate in [".test-checkpoint-before-certificate",".test-checkpoint-after-certificate"] {
            let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");let settings=config();
            let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
            p.write(write("a","a","alpha",ExpectedVersion::Absent)).await.unwrap();p.compact_journal().await.unwrap();
            let request=write("b","b","alpha beta",ExpectedVersion::Absent);let receipt=p.write(request.clone()).await.unwrap();
            std::fs::write(path.join(gate),b"fail").unwrap();
            assert!(matches!(p.compact_journal().await,Err(RecordError::OutcomeUnknown(_))));
            assert!(!p.inner.lock().await.healthy);
            assert!(matches!(p.write(request.clone()).await,Err(RecordError::RecoveryRequired)));
            std::fs::remove_file(path.join(gate)).unwrap();drop(p);
            let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
            assert_eq!(p.write(request).await.unwrap(),receipt);
            assert_eq!(query(&p,SearchMode::Lexical).await.hits.len(),2);
        }
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
            let mut owned=selected.segments.iter().chain(inner.index.pending_seals.iter())
                .map(|s|format!("segment-{}",s.id)).collect::<std::collections::BTreeSet<_>>();
            if let Some(tail)=inner.index.stable_tail_path(&root).await {owned.insert(tail.file_name().unwrap().to_str().unwrap().to_owned());}
            assert_eq!(names.iter().filter(|n|n.starts_with("segment-")).cloned().collect::<std::collections::BTreeSet<_>>(),owned,"keep precisely selected or live-owned packages, never unowned candidates");
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
    async fn payload_object_ceiling_compacts_without_losing_receipts_or_authority() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("partition");
        let mut settings = config();
        settings.max_records = 128;
        settings.max_operations = 256;
        settings.max_candidates = 128;
        let mut p = RecordPartition::create(&path, settings.clone()).await.unwrap();
        let mut receipts = Vec::new();
        for i in 0..64 {
            let request = write(&format!("op-{i}"), &format!("r-{i}"), &format!("alpha unique{i}"), ExpectedVersion::Absent);
            receipts.push((request.clone(), p.write(request).await.unwrap()));
            p.compact_journal().await.unwrap();
            assert_eq!(p.inner.lock().await.anchor.as_ref().unwrap().state_blocks.len(),i%(anchor_image::MAX_DELTAS+1),"finite delta chain must materialize a new base at its bound");
        }
        assert_eq!(p.inner.lock().await.payloads.objects.len(), 64);
        let old_closure = p.inner.lock().await.payloads.clone();
        let old_paths = old_closure.objects.keys().map(|id| path.join(format!("payload-{id}"))).collect::<Vec<_>>();
        let old_pin = package_pins::Pin::acquire(&old_paths).unwrap();
        let old_reader = PoolReader::open(&path, old_closure.clone(), budget(&settings).unwrap()).unwrap();
        let old_reference = crate::storage::payload_pool::Reference {
            object: old_closure.objects.keys().next().unwrap().clone(), position: 0,
        };
        let old_body = old_reader.read(&old_reference).unwrap();
        let before = serde_json::to_vec(&p.inner.lock().await.anchor).unwrap();
        let request = write("op-64", "r-64", "alpha unique64", ExpectedVersion::Absent);
        receipts.push((request.clone(), p.write(request).await.unwrap()));
        for (gate, unknown, certified) in [
            (".test-anchor-reject-prepared", false, false),
            (".test-anchor-reject-loaded", false, false),
            (".test-checkpoint-before-certificate", true, false),
            (".test-checkpoint-after-certificate", true, true),
        ] {
            std::fs::write(path.join(gate), b"reject").unwrap();
            let error = p.compact_journal().await.unwrap_err();
            assert_eq!(matches!(error, RecordError::OutcomeUnknown(_)), unknown, "gate={gate} error={error:?}");
            assert_eq!(serde_json::to_vec(&p.inner.lock().await.anchor).unwrap(), before);
            assert_eq!(p.inner.lock().await.payloads.objects.len(), 64);
            assert_eq!(p.inner.lock().await.healthy, !unknown);
            assert!(old_paths.iter().all(|path| path.exists()));
            std::fs::remove_file(path.join(gate)).unwrap();
            if unknown {
                assert!(matches!(p.write(receipts[0].0.clone()).await, Err(RecordError::RecoveryRequired)), "unknown publication must fence even an idempotent retry");
                drop(p);
                p = RecordPartition::open(&path, settings.max_journal_bytes).await.unwrap();
            }
            let inner = p.inner.lock().await;
            assert_eq!(inner.payloads.objects.len(), if certified { 1 } else { 64 });
            assert_eq!(inner.state.records.len(), 65);
            if !certified { assert_eq!(serde_json::to_vec(&inner.anchor).unwrap(), before); }
            else { assert_ne!(serde_json::to_vec(&inner.anchor).unwrap(), before, "durable certificate must select the new closure after reopen"); }
            let names = std::fs::read_dir(&path).unwrap().map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned()).collect::<Vec<_>>();
            assert!(!names.iter().any(|name| name.starts_with(".anchor-prepare-") || name.starts_with(".segment-prepare-") || name.starts_with(".payload-prepare-")), "candidate orphans must be retired after definite rejection or recovery");
            let selected=inner.anchor.as_ref().unwrap();
            let expected=std::iter::once(format!("anchor-{}",selected.id))
                .chain(selected.state_blocks.iter().map(|block|format!("anchor-{}",block.id)))
                .collect::<std::collections::BTreeSet<_>>();
            let actual=names.iter().filter(|name|name.starts_with("anchor-")).cloned()
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(actual,expected,"retain precisely the selected base/delta closure, never orphan anchors");
            assert_eq!(names.iter().filter(|name| name.starts_with("payload-")).count(), if certified { 65 } else { 64 }, "retain only selected objects plus explicitly pinned old closure");
            drop(inner);
            for (request, receipt) in &receipts { assert_eq!(p.write(request.clone()).await.unwrap(), *receipt); }
            assert_eq!(old_reader.read(&old_reference).unwrap(), old_body);
        }
        p.compact_journal().await.unwrap();
        assert_eq!(p.inner.lock().await.payloads.objects.len(), 1);
        p.inner.lock().await.reclaim_anchor_objects().await.unwrap();
        assert!(old_paths.iter().all(|path| path.exists()), "old pinned closure must survive successful publication and GC");
        assert_eq!(old_reader.read(&old_reference).unwrap(), old_body);
        drop(old_reader);
        drop(old_pin);
        p.inner.lock().await.reclaim_anchor_objects().await.unwrap();
        assert!(old_paths.iter().all(|path| !path.exists()), "retired objects are reclaimed only after releasing readers/pins");
        drop(p);
        let p = RecordPartition::open(&path, settings.max_journal_bytes).await.unwrap();
        assert_eq!(p.inner.lock().await.state.records.len(), 65);
        for (request, receipt) in receipts { assert_eq!(p.write(request).await.unwrap(), receipt); }
        assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 100).await.unwrap().count_total, 65);
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
                assert!(reference.segments.iter().any(|segment| segment.id == first.id),
                    "the retained package may move when the new tail is prepended");
                assert_eq!(serde_json::to_vec(&files(&first.path(&root).unwrap(),closure_limit(settings.max_journal_bytes).unwrap()).unwrap()).unwrap(),fingerprint);
            }
            for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All},SearchMode::Hybrid{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All, rrf: Default::default() }] {
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
        let inner=p.inner.lock().await;let mut selected=inner.anchor.as_ref().unwrap().segments.iter().map(|s|s.id.clone()).collect::<std::collections::BTreeSet<_>>();
        // Recovery owns a mutable successor in addition to the certified frozen
        // closure. Account for that exact tail, while still rejecting orphans.
        let tail = inner.index.stable_tail_path(&root).await.unwrap();
        selected.insert(tail.file_name().unwrap().to_str().unwrap().strip_prefix("segment-").unwrap().to_owned());
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
        p.compact_journal().await.unwrap();
        drop(p);let p=RecordPartition::open(&root,settings.max_journal_bytes).await.unwrap();
        for id in ["s","d","e"]{assert!(p.get(id).await.unwrap().is_none());}
        assert_eq!(p.write(deletion).await.unwrap(),receipt);
        assert_eq!(p.statistics().await.unwrap().active_records,0);
        assert_eq!(p.write(patch).await.unwrap().version,1);
        for mode in [SearchMode::Lexical,SearchMode::Vector{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All},SearchMode::Hybrid{similarity_threshold:None,ann_mode:crate::vector_similarity::AnnMode::All, rrf: Default::default() }] {assert!(query(&p,mode).await.hits.is_empty());}
        let selected=p.inner.lock().await.anchor.as_ref().unwrap().segments[0].path(&root).unwrap();
        drop(p);
        std::fs::write(selected.join("unselected-extra"),b"corrupt certified closure").unwrap();
        assert!(RecordPartition::open(&root,settings.max_journal_bytes).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn component_creation_rejects_existing_directory_without_overwriting_files() {
        for filename in ["vectors.bin", "metadata.bin", "schema.json", "component.json"] {
            let root=path();std::fs::create_dir(&root).unwrap();
            std::fs::write(root.join(filename),b"existing data").unwrap();
            let cfg=config();
            assert!(create_index(&root,cfg.index,&retrieval_schema(&config())).await.is_err());
            assert_eq!(std::fs::read(root.join(filename)).unwrap(),b"existing data");
            assert_eq!(std::fs::read_dir(&root).unwrap().count(),1);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[tokio::test]
    async fn empty_tail_does_not_preallocate_vector_rows() {
        let root=path();let p=RecordPartition::create(&root,config()).await.unwrap();
        let inner=p.inner.lock().await;let index=inner.index.test_index().read().await;
        for shard in &index.segments {
            let shard=shard.read().await;
            assert_eq!(shard.block_vector_buffer.capacity(),0);
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
            assert_eq!(index.segments.len(),2);
        }
        assert_eq!(scores(query(&restored,mode).await),expected);
        assert_eq!(restored.write(update).await.unwrap(),receipt);
        assert_eq!(serde_json::to_vec(&files(&selected,closure_limit(config.max_journal_bytes).unwrap()).unwrap()).unwrap(),files_before);
        drop(restored);std::fs::remove_dir_all(root).unwrap();
    }
}
