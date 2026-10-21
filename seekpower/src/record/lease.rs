//! Bounded private staging lifetime; not general version or receipt GC.
use super::*;
use sha2::{Digest,Sha256};
const LEASE_MS:u64=30*60*1000;
pub(super) fn deadline(now:u64)->Result<u64,RecordError>{now.checked_add(LEASE_MS).filter(|t|*t<=253_402_300_799_999).ok_or_else(||RecordError::Rejected("staging lease time overflow".into()))}
fn digest(request:&RecordWrite)->Result<String,RecordError>{
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
    for op in &mut state.operations {
        if op.request.record_id!=id || !matches!(&op.request.generation,Some(GenerationCommand::Stage{generation_id,..}) if generation_id==generation){continue;}
        if op.request_digest.is_none(){op.request_digest=Some(digest(&op.request)?);}
        if let Some(GenerationCommand::Stage{chunks,members,..})=&mut op.request.generation{chunks.clear();members.clear();}
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
pub(super) fn validate_state(state:&State)->Result<(),RecordError>{
    let mut retired=std::collections::BTreeSet::new();
    let controls:BTreeMap<_,_>=state.operations.iter().filter(|op|op.request.generation.is_some()).map(|op|((op.request.record_id.as_str(),op.receipt.version),op)).collect();
    for event in &state.lease_expirations {
        if event.targets.is_empty()||event.evaluation_time_ms>state.time_watermark_ms{return Err(storage("invalid lease expiration history"));}
        for t in &event.targets {
            let m=state.generations.get(&t.record_id).ok_or_else(||storage("orphan lease history"))?;
            let previous=controls.get(&(t.record_id.as_str(),t.previous_version)).ok_or_else(||storage("missing expiring lease receipt"))?;
            if previous.receipt.seq>=event.seq || deadline(previous.observed_time_ms)?!=t.deadline_ms || !matches!(&previous.request.generation,Some(GenerationCommand::Stage{generation_id,..}|GenerationCommand::Renew{generation_id}) if generation_id==&t.generation_id){return Err(storage("expired lease provenance mismatch"));}
            if !m.was_used(&t.generation_id)||!retired.insert((&t.record_id,&t.generation_id))||t.deadline_ms>event.evaluation_time_ms||m.version<=t.previous_version||m.staged.as_ref().is_some_and(|s|s.id==t.generation_id){return Err(storage("lease history mismatch"));}
        }
    }
    for op in &state.operations {
        if let Some(hash)=&op.request_digest {
            if hash.len()!=64||!hash.bytes().all(|b|b.is_ascii_hexdigit()){return Err(storage("invalid retained request digest"));}
            let Some(GenerationCommand::Stage{generation_id,chunks,members,..})=&op.request.generation else{return Err(storage("only retired private stages can be redacted"));};
            let terminal=state.operations.iter().any(|a|a.receipt.seq>op.receipt.seq&&a.request.record_id==op.request.record_id&&matches!(&a.request.generation,Some(GenerationCommand::Abort{generation_id:id}) if id==generation_id)) || state.lease_expirations.iter().any(|e|e.seq>op.receipt.seq&&e.targets.iter().any(|t|t.record_id==op.request.record_id&&t.generation_id==*generation_id));
            if !terminal||!chunks.is_empty()||!members.is_empty()||state.generations.get(&op.request.record_id).is_none_or(|m|!m.was_used(generation_id)||m.staged.as_ref().is_some_and(|s|&s.id==generation_id))||state.records.values().any(|r|r.generation_member.as_ref().is_some_and(|m|m.document_id==op.request.record_id&&m.generation_id==*generation_id)){return Err(storage("redacted stage still reachable"));}
        }
    }Ok(())
}
impl Inner {
    pub(super) async fn expire_staging(&mut self,evaluation:u64)->Result<(),RecordError>{
        let targets:Vec<_>=self.state.generations.iter().filter_map(|(id,g)|g.staged.as_ref().filter(|s|s.deadline_ms<=evaluation).map(|s|Target{record_id:id.clone(),generation_id:s.id.clone(),previous_version:g.version,deadline_ms:s.deadline_ms})).collect();
        if targets.is_empty(){return Ok(());}
        let event=Expiration{seq:self.state.seq.checked_add(1).ok_or_else(||storage("sequence exhausted"))?,evaluation_time_ms:evaluation,targets};
        let mut candidate=self.state.clone();apply(&mut candidate,&event)?;
        let snapshot_bytes=encode_entry(&Entry::Checkpoint{config:self.config.clone(),state:candidate.clone()})?.len();
        let reserve=generation::reserve_bytes(&candidate);
        PartitionJournal::validate_initial(snapshot_bytes+reserve,self.config.max_journal_bytes).map_err(storage)?;
        let bytes=encode_entry(&Entry::LeaseExpired(event.clone()))?;
        self.journal.validate_append_with_reserve(bytes.len(),reserve).map_err(storage)?;
        self.healthy=false;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;
        self.journal.append(&bytes).map_err(|e|RecordError::OutcomeUnknown(e.to_string()))?;
        #[cfg(feature="fault-injection")]
        match std::env::var("SEEKPOWER_LEASE_FAULT").ok().as_deref(){Some("after_append_unknown")=>return Err(RecordError::OutcomeUnknown("injected lease append".into())),Some("after_durable")=>return Err(RecordError::DurablePending(event.seq)),Some("crash_after_durable")=>std::process::exit(86),_=>{}}
        #[cfg(test)] if self.fail_after_durable{return Err(RecordError::DurablePending(event.seq));}
        self.state=candidate;self.snapshot_bytes=snapshot_bytes;self.healthy=true;self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=false;
        #[cfg(feature="fault-injection")] if std::env::var("SEEKPOWER_LEASE_FAULT").ok().as_deref()==Some("crash_after_visible"){std::process::exit(86);}
        Ok(())
    }
    pub(super) fn cleanup_private(&mut self)->Result<(),RecordError>{
        if !self.state.private_cleanup_pending{return Ok(());}
        let mut candidate=self.state.clone();candidate.private_cleanup_pending=false;
        let bytes=encode_entry(&Entry::Checkpoint{config:self.config.clone(),state:candidate.clone()})?;
        self.healthy=false;
        match self.journal.checkpoint(&bytes){
            Ok(())=>{
                // checkpoint success only confirms publication. Windows/open
                // files or I/O failure can still leave the old private WAL.
                candidate.private_cleanup_pending=self.journal.reclaim_obsolete().is_err();
                self.snapshot_bytes=encode_entry(&Entry::Checkpoint{config:self.config.clone(),state:candidate.clone()})?.len();
                self.state=candidate;self.healthy=true;Ok(())
            },
            Err(CreateError::NotPublished(_))=>{self.healthy=true;Ok(())},
            Err(e)=>{self.output.write().map_err(|_|RecordError::RecoveryRequired)?.fenced=true;Err(creation_error(e))}
        }
    }
}
