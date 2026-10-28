//! Bounded private staging lifetime and the evidence retained by receipt GC.
use super::*;
use sha2::{Digest,Sha256};
use std::collections::BTreeSet;
const LEASE_MS:u64=30*60*1000;
pub(super) fn deadline(now:u64)->Result<u64,RecordError>{now.checked_add(LEASE_MS).filter(|t|*t<=253_402_300_799_999).ok_or_else(||RecordError::Rejected("staging lease time overflow".into()))}
pub(super) fn digest<T:Serialize>(request:&T)->Result<String,RecordError>{
    // Value equality ignores object ordering. Canonicalize recursively before
    // replacing an aborted private payload by its collision-resistant identity.
    fn sort(value:&mut serde_json::Value){match value{serde_json::Value::Object(map)=>{for v in map.values_mut(){sort(v);}map.sort_keys();},serde_json::Value::Array(a)=>for v in a{sort(v);},_=>{}}}
    let mut value=serde_json::to_value(request).map_err(storage)?;sort(&mut value);
    Ok(Sha256::digest(serde_json::to_vec(&value).map_err(storage)?).iter().map(|b|format!("{b:02x}")).collect())
}
pub(super) fn same_request(old:&Applied,request:&RecordWrite)->Result<bool,RecordError>{
    if let Some(hash)=&old.request_digest {Ok(hash==&digest(request)?)}else{Ok(serde_json::to_value(&old.request).map_err(storage)?==serde_json::to_value(request).map_err(storage)?)}
}
pub(super) fn redact(state:&mut State,id:&str,generation:&str)->Result<(),RecordError>{
    for index in 0..state.operations.len() {
        let op=&state.operations[index];
        if op.request.record_id!=id || !matches!(&op.request.generation,Some(GenerationCommand::Stage{generation_id,..}|GenerationCommand::StageBatch{generation_id,..}) if generation_id==generation){continue;}
        state.operations.edit(index,256,|op| {
        if op.request_digest.is_none(){op.request_digest=Some(digest(&op.request)?);}
        match &mut op.request.generation{Some(GenerationCommand::Stage{chunks,members,..})=>{chunks.clear();members.clear();},Some(GenerationCommand::StageBatch{chunks,..})=>chunks.clear(),_=>{}}
        Ok(())})?;
        state.private_cleanup_pending=true;
    }Ok(())
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub(super) struct Target {record_id:String,generation_id:String,previous_version:u64,deadline_ms:u64}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub(super) struct Expiration {pub seq:u64,pub evaluation_time_ms:u64,targets:Vec<Target>}
pub(super) fn apply(state:&mut State,event:&Expiration)->Result<(),RecordError>{
    if event.seq!=state.seq+1 || event.evaluation_time_ms<state.time_watermark_ms || event.targets.is_empty(){return Err(storage("invalid staging expiration"));}
    let mut seen=std::collections::BTreeSet::new();
    for target in &event.targets {
        let manifest=state.generations.get_mut(&target.record_id).ok_or_else(||storage("expired staging absent"))?;
        let stage=manifest.staged.as_ref().ok_or_else(||storage("expired staging absent"))?;
        if !seen.insert(&target.record_id)||manifest.version!=target.previous_version||stage.id!=target.generation_id||stage.deadline_ms!=target.deadline_ms||stage.deadline_ms>event.evaluation_time_ms{return Err(storage("invalid staging expiry fence"));}
        manifest.staged=None;manifest.version=manifest.version.checked_add(1).ok_or_else(||storage("lease fence exhausted"))?;
        redact(state,&target.record_id,&target.generation_id)?;
    }
    state.seq=event.seq;state.time_watermark_ms=event.evaluation_time_ms;state.lease_expirations.push(event.clone());Ok(())
}
/// Business evidence that cannot be retired with an expired retry receipt.
/// The caller freezes this set with its authenticated maintenance source cut;
/// it must not recompute it against a later catch-up suffix.
pub(super) fn protected_operation_sequences(state:&State)->BTreeSet<u64> {
    let expiry_versions:BTreeSet<_>=state.lease_expirations.iter()
        .flat_map(|event|event.targets.iter().map(|target|(target.record_id.as_str(),target.previous_version)))
        .collect();
    let mut redacted=BTreeSet::new();
    for op in &state.operations {
        if op.request_digest.is_some() {
            if let Some(GenerationCommand::Stage {generation_id,..}|GenerationCommand::StageBatch {generation_id,..})=&op.request.generation {
                redacted.insert((op.request.record_id.as_str(),generation_id.as_str()));
            }
        }
    }
    let mut protected=BTreeSet::new();
    let mut latest_renew=BTreeMap::new();
    for op in &state.operations {
        let Some(command)=&op.request.generation else {continue;};
        let generation_id=match command {
            GenerationCommand::Stage {generation_id,..}|GenerationCommand::Begin {generation_id,..}
            |GenerationCommand::StageBatch {generation_id,..}|GenerationCommand::Seal {generation_id,..}
            |GenerationCommand::Renew {generation_id}|GenerationCommand::Publish {generation_id}
            |GenerationCommand::Abort {generation_id}=>generation_id,
        };
        if expiry_versions.contains(&(op.request.record_id.as_str(),op.receipt.version)) {
            protected.insert(op.receipt.seq);
        }
        if state.generations.get(&op.request.record_id).and_then(|g|g.staged.as_ref())
            .is_some_and(|stage|stage.id==*generation_id) {
            if matches!(command,GenerationCommand::Renew {..}) {
                // Rebuilding the private upload needs only the last renewal's
                // deadline. Origins, batches and seals remain exact decisions.
                latest_renew.entry(op.request.record_id.as_str())
                    .and_modify(|seq:&mut u64|*seq=(*seq).max(op.receipt.seq)).or_insert(op.receipt.seq);
            } else {
                protected.insert(op.receipt.seq);
            }
        }
        if matches!(command,GenerationCommand::Abort {..})
            && redacted.contains(&(op.request.record_id.as_str(),generation_id.as_str())) {
            protected.insert(op.receipt.seq);
        }
    }
    protected.extend(latest_renew.into_values());
    protected
}
pub(super) fn validate_state(state:&State)->Result<(),RecordError>{
    let mut retired=std::collections::BTreeSet::new();
    let controls:BTreeMap<_,_>=state.operations.iter().filter(|op|op.request.generation.is_some()).map(|op|((op.request.record_id.as_str(),op.receipt.version),op)).collect();
    for event in &state.lease_expirations {
        if event.targets.is_empty()||event.evaluation_time_ms>state.time_watermark_ms{return Err(storage("invalid lease expiration history"));}
        for t in &event.targets {
            let m=state.generations.get(&t.record_id).ok_or_else(||storage("orphan lease history"))?;
            let previous=controls.get(&(t.record_id.as_str(),t.previous_version)).ok_or_else(||storage("missing expiring lease receipt"))?;
            if previous.receipt.seq>=event.seq || deadline(previous.observed_time_ms)?!=t.deadline_ms || !matches!(&previous.request.generation,Some(GenerationCommand::Stage{generation_id,..}|GenerationCommand::Begin{generation_id,..}|GenerationCommand::StageBatch{generation_id,..}|GenerationCommand::Seal{generation_id,..}|GenerationCommand::Renew{generation_id}) if generation_id==&t.generation_id){return Err(storage("expired lease provenance mismatch"));}
            if !m.was_used(&t.generation_id)||!retired.insert((&t.record_id,&t.generation_id))||t.deadline_ms>event.evaluation_time_ms||m.version<=t.previous_version||m.staged.as_ref().is_some_and(|s|s.id==t.generation_id){return Err(storage("lease history mismatch"));}
        }
    }
    for op in &state.operations {
        if let Some(hash)=&op.request_digest {
            if hash.len()!=64||!hash.bytes().all(|b|b.is_ascii_hexdigit()){return Err(storage("invalid retained request digest"));}
            let (generation_id,empty)=match &op.request.generation{Some(GenerationCommand::Stage{generation_id,chunks,members,..})=>(generation_id,chunks.is_empty()&&members.is_empty()),Some(GenerationCommand::StageBatch{generation_id,chunks})=>(generation_id,chunks.is_empty()),_=>return Err(storage("only retired private stages can be redacted"))};
            let terminal=state.operations.iter().any(|a|a.receipt.seq>op.receipt.seq&&a.request.record_id==op.request.record_id&&matches!(&a.request.generation,Some(GenerationCommand::Abort{generation_id:id}) if id==generation_id)) || state.lease_expirations.iter().any(|e|e.seq>op.receipt.seq&&e.targets.iter().any(|t|t.record_id==op.request.record_id&&t.generation_id==*generation_id));
            if !terminal||!empty||state.generations.get(&op.request.record_id).is_none_or(|m|!m.was_used(generation_id)||m.staged.as_ref().is_some_and(|s|&s.id==generation_id))||state.records.values().any(|r|r.generation_member.as_ref().is_some_and(|m|m.document_id==op.request.record_id&&m.generation_id==*generation_id)){return Err(storage("redacted stage still reachable"));}
        }
    }Ok(())
}
impl Inner {
    pub(super) async fn expire_staging(&mut self,evaluation:u64)->Result<(),RecordError>{
        let targets:Vec<_>=self.state.generations.iter().filter_map(|(id,g)|g.staged.as_ref().filter(|s|s.deadline_ms<=evaluation).map(|s|Target{record_id:id.clone(),generation_id:s.id.clone(),previous_version:g.version,deadline_ms:s.deadline_ms})).collect();
        if targets.is_empty(){return Ok(());}
        let event=Expiration{seq:self.state.seq.checked_add(1).ok_or_else(||storage("sequence exhausted"))?,evaluation_time_ms:evaluation,targets};
        let mut candidate=self.state.try_clone_with_growth(checkpoint_codec::json_length(&event)?)?;apply(&mut candidate,&event)?;
        let snapshot_bytes=checkpoint_codec::length(&self.config, &candidate)?;
        let reserve=generation::reserve_bytes(&candidate);
        checkpoint_codec::validate_size(snapshot_bytes,reserve,self.config.max_journal_bytes).map_err(storage)?;
        let bytes=encode_entry(&Entry::LeaseExpired(event.clone()))?;
        self.ensure_journal_room(bytes.len(),reserve).await?;
        #[cfg(feature="s3-object-store")]
        self.ensure_remote_tail_room(event.seq,&format!("system:lease-expired:{}",event.seq),&bytes).await?;
        self.validate_journal_room(bytes.len(),reserve)?;
        self.healthy=false;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
        #[cfg(feature = "s3-object-store")]
        if let Some(remote) = self.remote.as_mut() {
            remote.commit_entry(event.seq, format!("system:lease-expired:{}", event.seq), &bytes)
                .await.map_err(|error|RecordError::OutcomeUnknown(error.to_string()))?;
        }
        self.append_operation(&bytes,event.seq)?;
        #[cfg(feature="fault-injection")]
        match std::env::var("ANYSEEK_LEASE_FAULT").ok().as_deref(){Some("after_append_unknown")=>return Err(RecordError::OutcomeUnknown("injected lease append".into())),Some("after_durable")=>return Err(RecordError::DurablePending(event.seq)),Some("crash_after_durable")=>std::process::exit(86),_=>{}}
        #[cfg(test)] if self.fail_after_durable{return Err(RecordError::DurablePending(event.seq));}
        self.state=candidate;self.snapshot_bytes=snapshot_bytes;self.healthy=true;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=false;
        #[cfg(feature="fault-injection")] if std::env::var("ANYSEEK_LEASE_FAULT").ok().as_deref()==Some("crash_after_visible"){std::process::exit(86);}
        Ok(())
    }
    pub(super) async fn cleanup_private(&mut self)->Result<(),RecordError>{
        if !self.state.private_cleanup_pending && !self.maintenance_gc_pending{return Ok(());}
        // Keep the erasure obligation until a fully authenticated replacement
        // is installed. The owned worker rewrites payloads outside the live lock.
        Err(self.request_checkpoint("private_cleanup"))
    }
}

#[cfg(test)]
mod gc_tests {
    use super::*;
    use crate::record::tests::{config,source_write,generation_write,stage_generation};

    async fn staged() -> (tempfile::TempDir,RecordPartition) {
        let root=tempfile::tempdir().unwrap();
        let p=RecordPartition::create(&root.path().join("live"),config()).await.unwrap();
        p.write(source_write("source","s",&[])).await.unwrap();
        p.write(generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&["chunk"]))).await.unwrap();
        (root,p)
    }
    async fn reopen(root:&Path,state:State) -> RecordPartition {
        let settings=config();
        validate_restored_state(&settings,&state).unwrap();
        let bytes=encode_entry(&Entry::Checkpoint {config:settings.clone(),state}).unwrap();
        drop(PartitionJournal::create(root,settings.max_journal_bytes,&bytes).unwrap());
        RecordPartition::open(root,settings.max_journal_bytes).await.unwrap()
    }

    #[tokio::test]
    async fn generation_gc_retires_redundant_renewals_preserves_live_upload_and_epoch_fence() {
        let (root,p)=staged().await;
        for version in 1..=3 {
            p.write(generation_write(&format!("renew-{version}"),ExpectedVersion::Match(version),
                GenerationCommand::Renew {generation_id:"g".into()})).await.unwrap();
        }
        let mut state=p.inner.lock().await.state.try_clone().unwrap();
        let epoch=current_epoch()+2;
        state.feed.floor=state.seq;
        maintenance::collect_operations(&mut state,epoch).unwrap();
        assert_eq!(state.operations.len(),2,"only upload origin and latest renewal are evidence");
        assert_eq!(state.operations[0].request.operation_id.nonce,"stage");
        assert_eq!(state.operations[1].request.operation_id.nonce,"renew-3");
        upload::validate_history(&state,"doc",&state.generations["doc"]).unwrap();
        let recovered=reopen(&root.path().join("recovered"),state).await;
        let old=generation_write("renew-1",ExpectedVersion::Match(1),GenerationCommand::Renew {generation_id:"g".into()});
        assert!(matches!(recovered.write(old).await,Err(RecordError::OperationExpired)));
        let mut next=generation_write("next",ExpectedVersion::Match(4),GenerationCommand::Renew {generation_id:"g".into()});
        next.operation_id.epoch=epoch;
        let receipt=recovered.write(next.clone()).await.unwrap();
        assert_eq!(receipt.version,5);
        assert_eq!(recovered.write(next).await.unwrap(),receipt);
    }

    #[tokio::test]
    async fn generation_gc_reclaims_closed_controls_but_preserves_exact_spent_identity() {
        let (root,p)=staged().await;
        p.write(generation_write("publish",ExpectedVersion::Match(1),GenerationCommand::Publish {generation_id:"g".into()})).await.unwrap();
        let mut state=p.inner.lock().await.state.try_clone().unwrap();
        state.feed.floor=state.seq;
        let epoch=current_epoch()+2;
        maintenance::collect_operations(&mut state,epoch).unwrap();
        assert!(state.operations.is_empty());
        assert!(state.generations["doc"].was_used("g"));
        let recovered=reopen(&root.path().join("recovered"),state).await;
        assert!(recovered.get("chunk").await.unwrap().is_some());
        let mut reused=generation_write("reuse",ExpectedVersion::Match(2),stage_generation("g",&["fresh"]));
        reused.operation_id.epoch=epoch;
        assert!(matches!(recovered.write(reused).await,Err(RecordError::Conflict(_))));
    }

    #[tokio::test]
    async fn generation_gc_preserves_expiry_and_redaction_provenance_until_unneeded() {
        for expires in [false,true] {
            let (root,p)=staged().await;
            if expires {
                let mut inner=p.inner.lock().await;
                let at=inner.state.time_watermark_ms+LEASE_MS+1;
                inner.expire_staging(at).await.unwrap();
            } else {
                p.write(generation_write("abort",ExpectedVersion::Match(1),GenerationCommand::Abort {generation_id:"g".into()})).await.unwrap();
            }
            let mut state=p.inner.lock().await.state.try_clone().unwrap();
            state.feed.floor=state.seq;
            maintenance::collect_operations(&mut state,current_epoch()+2).unwrap();
            assert_eq!(state.operations.len(),1);
            if expires {
                assert!(matches!(state.operations[0].request.generation,Some(GenerationCommand::Stage {..})));
                assert!(state.operations[0].request_digest.is_some());
                assert_eq!(state.lease_expirations.len(),1);
                let mut corrupted=state.try_clone().unwrap();
                corrupted.operations.retain(|_|false);
                assert!(validate_state(&corrupted).is_err(),"expiry cannot lose its prior-version proof");
            } else {
                assert!(matches!(state.operations[0].request.generation,Some(GenerationCommand::Abort {..})));
                maintenance::collect_operations(&mut state,current_epoch()+2).unwrap();
                assert!(state.operations.is_empty(),"terminal proof can retire after all redacted receipts retire");
            }
            let recovered=reopen(&root.path().join("recovered"),state).await;
            let inner=recovered.inner.lock().await;
            assert!(inner.state.generations["doc"].staged.is_none());
            assert!(inner.state.generations["doc"].was_used("g"));
            validate_state(&inner.state).unwrap();
        }
    }
}
