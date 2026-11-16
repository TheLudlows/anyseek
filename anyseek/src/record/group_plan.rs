//! Pure bounded group planning. No journal, index, or live-state mutation occurs here.
use super::*;
use std::collections::BTreeSet;

/// One complete member body; chunking and embedding remain the caller's work.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupRecord {
    pub record_id: String,
    pub document: Document,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal: Option<TemporalMetadata>,
}
/// Replace all members under one optimistic version and one retry identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupReplace {
    pub operation_id: OperationId,
    pub group_id: String,
    pub expected_group_version: u64,
    pub records: Vec<GroupRecord>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GroupReceipt {
    pub seq: u64,
    pub group_id: String,
    pub group_version: u64,
    pub record_count: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head { pub version:u64, pub members:BTreeSet<String> }
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Decision {
    pub operation_id:OperationId,
    pub request_digest:[u8;32],
    pub members:BTreeSet<String>,
    pub receipt:GroupReceipt,
    pub records:Vec<Record>,
    pub observed_epoch:u64,
    pub observed_time_ms:u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StoredReceipt {
    pub operation_id:OperationId,
    pub request_digest:[u8;32],
    pub receipt:GroupReceipt,
    pub observed_epoch:u64,
    pub observed_time_ms:u64,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Groups {
    pub heads:BTreeMap<String,Head>,
    // Keep retired membership until a group explicitly reuses the identity.
    pub owners:BTreeMap<String,String>,
    pub receipts:Vec<StoredReceipt>,
    pub publication_seq:u64,
}
pub(super) enum Plan { Replay(GroupReceipt), Replace(Decision) }
pub(super) const MAX_MEMBERS:usize=64;
const MAX_BYTES:usize=4*1024*1024;

pub(super) fn prepare(state:&State, groups:&Groups, config:&RecordConfig, request:GroupReplace,
    wall_epoch:u64, now:u64)->Result<Plan,RecordError> {
    let epoch=wall_epoch.max(state.epoch_watermark);
    if request.operation_id.epoch>epoch || request.operation_id.epoch.saturating_add(1)<epoch {
        return Err(RecordError::OperationExpired);
    }
    if request.group_id.is_empty() || request.group_id.len()>256 || request.operation_id.nonce.is_empty() {
        return Err(RecordError::Rejected("invalid group or operation identity".into()));
    }
    if request.records.len()>MAX_MEMBERS || checkpoint_codec::json_length(&request)?>MAX_BYTES {
        return Err(RecordError::Rejected("group request budget exceeded".into()));
    }
    if state.operation_lookup.contains_key(&request.operation_id) {
        return Err(RecordError::Conflict("operation identity belongs to an ordinary write".into()));
    }
    if let Some(old)=groups.receipts.iter().find(|d|d.operation_id==request.operation_id) {
        return if old.request_digest==request_digest(&request)? {
            Ok(Plan::Replay(old.receipt.clone()))
        } else {Err(RecordError::Conflict("operation identity reused with another group request".into()))};
    }
    let head=groups.heads.get(&request.group_id);
    let version=head.map_or(0,|h|h.version);
    if request.expected_group_version!=version {return Err(RecordError::Conflict("group version precondition failed".into()));}
    if groups.receipts.len().saturating_add(state.operations.len())>=config.max_operations {
        return Err(RecordError::Rejected("operation budget exceeded".into()));
    }
    if head.is_none() && groups.heads.len()>=config.max_records {
        return Err(RecordError::Rejected("group identity budget exceeded".into()));
    }
    let mut members=BTreeSet::new();
    for member in &request.records {
        if member.record_id.is_empty() || !members.insert(member.record_id.clone()) {
            return Err(RecordError::Rejected("empty or duplicate group member".into()));
        }
        match groups.owners.get(&member.record_id) {
            Some(owner) if owner!=&request.group_id=>return Err(RecordError::Conflict("record belongs to another group".into())),
            None if state.records.get(&member.record_id).is_some()=>return Err(RecordError::Conflict("record belongs to independent writes".into())),
            _=>{}
        }
    }
    let removed:Vec<_>=head.into_iter().flat_map(|h|h.members.iter()).filter(|id|!members.contains(*id)).cloned().collect();
    if removed.len()+request.records.len()>MAX_MEMBERS {
        return Err(RecordError::Rejected("expanded group change budget exceeded".into()));
    }
    let receipt=GroupReceipt {seq:state.seq.checked_add(1).ok_or_else(||storage("sequence exhausted"))?,
        group_id:request.group_id.clone(), group_version:version.checked_add(1).ok_or_else(||storage("group version exhausted"))?,record_count:request.records.len()};
    let mut records=Vec::new();
    let mut count=state.records.len();
    let mut ttl=state.records.live_ttl_count();
    let mut snapshot=checkpoint_codec::length(config,state)?;
    let writes=request.records.iter().map(|r|(r.record_id.clone(),Some(r.document.clone()),r.temporal.clone()))
        .chain(removed.into_iter().map(|id|(id,None,None)));
    for (id,document,temporal) in writes {
        let previous=state.records.get(&id);
        let write=RecordWrite{operation_id:request.operation_id.clone(),record_id:id,expected:ExpectedVersion::Any,document,vector_patch:None,temporal};
        let input=mutation::Input{previous,operation:None,seq:state.seq,epoch_watermark:epoch,time_watermark_ms:state.time_watermark_ms,
            record_count:count,operation_count:state.operations.len()+groups.receipts.len(),ttl_count:ttl};
        let mutation::PreparedWrite::Append{applied,snapshot_bytes,..}=mutation::prepare_input(&input,config,snapshot,write,epoch,now)? else {return Err(storage("unexpected member replay"));};
        let time=temporal::retained(previous,&applied.request,now)?;
        let record=mutation::projected_record(previous,&applied,applied.request.document.clone(),time);
        ttl=ttl-usize::from(previous.is_some_and(temporal::live_ttl))+usize::from(temporal::live_ttl(&record));
        count+=usize::from(previous.is_none());snapshot=snapshot_bytes;records.push(record);
    }
    Ok(Plan::Replace(Decision{request_digest:request_digest(&request)?,operation_id:request.operation_id,members,receipt,records,observed_epoch:epoch,observed_time_ms:now}))
}

fn request_digest(request:&GroupReplace)->Result<[u8;32],RecordError>{
    fn sorted(value:serde_json::Value)->serde_json::Value {
        match value {
            serde_json::Value::Object(map)=>serde_json::Value::Object(map.into_iter().map(|(k,v)|(k,sorted(v))).collect::<BTreeMap<_,_>>().into_iter().collect()),
            serde_json::Value::Array(values)=>serde_json::Value::Array(values.into_iter().map(sorted).collect()),
            other=>other,
        }
    }
    checkpoint_codec::json_digest(&sorted(serde_json::to_value(request).map_err(storage)?))
}
impl Decision {
    fn stored(&self)->StoredReceipt {StoredReceipt{operation_id:self.operation_id.clone(),request_digest:self.request_digest,receipt:self.receipt.clone(),observed_epoch:self.observed_epoch,observed_time_ms:self.observed_time_ms}}
}
impl State {
    pub(super) fn group_publication_revision(&self)->u64 {self.groups.as_ref().map_or(0,|g|g.publication_seq)}
}

/// Apply an already durable whole-group decision. Recovery never reinterprets TTL/admission.
pub(super) fn apply(state:&mut State,d:&Decision)->Result<(),RecordError>{
    if state.seq.checked_add(1)!=Some(d.receipt.seq) || d.observed_time_ms<state.time_watermark_ms
        || d.records.len()>MAX_MEMBERS || d.members.len()!=d.receipt.record_count
        || d.operation_id.nonce.is_empty() || d.receipt.group_id.is_empty() || d.receipt.group_id.len()>256
        || d.operation_id.epoch>d.observed_epoch || d.operation_id.epoch.saturating_add(1)<d.observed_epoch {
        return Err(storage("invalid group decision sequence or bounds"));
    }
    let empty=Groups::default();let groups=state.groups.as_ref().unwrap_or(&empty);
    if groups.receipts.iter().any(|r|r.operation_id==d.operation_id) || state.operation_lookup.contains_key(&d.operation_id)
        || groups.heads.get(&d.receipt.group_id).map_or(0,|h|h.version).checked_add(1)!=Some(d.receipt.group_version) {
        return Err(storage("invalid group decision version or retry identity"));
    }
    let old=groups.heads.get(&d.receipt.group_id).map(|h|&h.members);
    let expected:BTreeSet<_>=old.into_iter().flatten().chain(d.members.iter()).cloned().collect();
    let actual:BTreeSet<_>=d.records.iter().map(|r|r.record_id.clone()).collect();
    if actual!=expected || actual.len()!=d.records.len() {return Err(storage("group decision omits or duplicates members"));}
    for r in &d.records {
        if state.records.get(&r.record_id).map_or(Some(1),|old|old.version.checked_add(1))!=Some(r.version)
            || r.document.is_some()!=d.members.contains(&r.record_id) || r.expired_at_ms.is_some()
            || r.ingested_at_ms!=if r.document.is_some(){d.observed_time_ms}else{state.records.get(&r.record_id).map_or(d.observed_time_ms,|r|r.ingested_at_ms)}
            || groups.owners.get(&r.record_id).is_some_and(|g|g!=&d.receipt.group_id)
            || (groups.owners.get(&r.record_id).is_none() && state.records.get(&r.record_id).is_some()) {
            return Err(storage("inconsistent group member decision"));
        }
    }
    state.admit_replay_frame(checkpoint_codec::json_length(d)?)?;
    for r in &d.records {state.records.insert(r.record_id.clone(),r.clone())?;}
    let groups=state.groups.get_or_insert_with(Groups::default);
    for r in &d.records {groups.owners.insert(r.record_id.clone(),d.receipt.group_id.clone());}
    groups.heads.insert(d.receipt.group_id.clone(),Head{version:d.receipt.group_version,members:d.members.clone()});
    groups.receipts.push(d.stored());groups.publication_seq=d.receipt.seq;
    state.seq=d.receipt.seq;state.epoch_watermark=state.epoch_watermark.max(d.observed_epoch);state.time_watermark_ms=d.observed_time_ms;
    Ok(())
}
pub(super) fn validate(state:&State,config:&RecordConfig)->Result<(),RecordError>{
    let Some(g)=&state.groups else{return Ok(());};
    if g.publication_seq>state.seq || g.heads.len()>config.max_records || g.owners.len()>config.max_records
        || g.receipts.len().saturating_add(state.operations.len())>config.max_operations {return Err(storage("group metadata bounds exceeded"));}
    let mut ids=BTreeSet::new();let mut seq=0;
    for r in &g.receipts {
        if r.receipt.seq<=seq || r.receipt.seq>state.seq || r.observed_time_ms>state.time_watermark_ms
            || !ids.insert(&r.operation_id) || state.operation_lookup.contains_key(&r.operation_id)
            || g.heads.get(&r.receipt.group_id).is_none_or(|h|h.version<r.receipt.group_version) {return Err(storage("invalid group receipt history"));}
        seq=r.receipt.seq;
    }
    if seq>g.publication_seq || (seq<g.publication_seq && state.retired_operation_prefix.as_ref().is_none_or(|p|p.seq<g.publication_seq)) {return Err(storage("group publication watermark mismatch"));}
    for (id,head) in &g.heads {
        if id.is_empty() || head.version==0 || head.members.len()>MAX_MEMBERS {return Err(storage("invalid group head"));}
        if let Some(last)=g.receipts.iter().rev().find(|r|r.receipt.group_id==*id)
            .filter(|r|state.retired_operation_prefix.as_ref().is_none_or(|p|r.receipt.seq>p.seq)) {
            if last.receipt.group_version!=head.version || last.receipt.record_count!=head.members.len(){return Err(storage("group head differs from latest receipt"));}
        }
        for member in &head.members {if g.owners.get(member)!=Some(id) || state.records.get(member).is_none_or(|r|r.document.is_none()&&r.expired_at_ms.is_none()){return Err(storage("orphan group member"));}}
    }
    for (id,owner) in &g.owners {if !g.heads.contains_key(owner) || state.records.get(id).is_none(){return Err(storage("orphan group ownership"));}
        if !g.heads[owner].members.contains(id)&&state.records[id].document.is_some(){return Err(storage("removed group member has a body"));}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(id:&str,version:u64,ids:&[&str])->GroupReplace {
        GroupReplace{operation_id:OperationId::new(id.into()),group_id:"g".into(),expected_group_version:version,
            records:ids.iter().map(|id|GroupRecord{record_id:(*id).into(),document:super::super::tests::write("unused",id,"alpha",ExpectedVersion::Any).document.unwrap(),temporal:None}).collect()}
    }
    fn plan(state:&State,groups:&Groups,r:GroupReplace)->Result<Plan,RecordError>{prepare(state,groups,&super::super::tests::config(),r,current_epoch(),1)}
    fn install(state:&mut State,groups:&mut Groups,d:Decision){
        for r in &d.records {state.records.insert(r.record_id.clone(),r.clone()).unwrap();groups.owners.insert(r.record_id.clone(),d.receipt.group_id.clone());}
        groups.heads.insert(d.receipt.group_id.clone(),Head{version:d.receipt.group_version,members:d.members.clone()});
        state.seq=d.receipt.seq;groups.publication_seq=state.seq;groups.receipts.push(d.stored());
    }
    #[test]
    fn replace_clear_replay_and_membership_are_whole_decisions(){
        let mut s=State::default();let mut g=Groups::default();let first=request("first",0,&["a","b","c"]);
        let Plan::Replace(d)=plan(&s,&g,first.clone()).unwrap() else {panic!()};let receipt=d.receipt.clone();install(&mut s,&mut g,d);
        let Plan::Replace(d)=plan(&s,&g,request("next",1,&["a","d"])).unwrap() else {panic!()};
        assert_eq!(d.records.len(),4);assert_eq!(d.receipt.seq,2);assert_eq!(d.records.iter().filter(|r|r.document.is_none()).count(),2);install(&mut s,&mut g,d);
        assert!(matches!(plan(&s,&g,first),Ok(Plan::Replay(r)) if r==receipt));
        let Plan::Replace(d)=plan(&s,&g,request("clear",2,&[])).unwrap() else {panic!()};assert_eq!(d.records.len(),2);install(&mut s,&mut g,d);
        assert_eq!(g.heads["g"].version,3);assert!(g.heads["g"].members.is_empty());
        assert!(matches!(plan(&s,&g,request("reuse",3,&["b"])),Ok(Plan::Replace(_))));
        let mut other=request("other",0,&["b"]);other.group_id="other".into();assert!(matches!(plan(&s,&g,other),Err(RecordError::Conflict(_))));
    }
    #[test]
    fn invalid_suffix_stale_version_and_retry_conflict_never_change_state(){
        let s=State::default();let g=Groups::default();
        let mut r=request("invalid",0,&["a","b"]);r.records[1].document.insert("vector".into(),serde_json::json!([1]));
        assert!(plan(&s,&g,r).is_err());assert_eq!(s.seq,0);assert!(s.records.is_empty());
        assert!(matches!(plan(&s,&g,request("stale",1,&["fresh"])),Err(RecordError::Conflict(_))));
        let mut s=s;let mut g=g;let Plan::Replace(d)=plan(&s,&g,request("same",0,&["a"])).unwrap() else {panic!()};install(&mut s,&mut g,d);
        assert!(matches!(plan(&s,&g,request("same",0,&["b"])),Err(RecordError::Conflict(_))));
        assert!(matches!(plan(&s,&g,request("duplicate",1,&["b","b"])),Err(RecordError::Rejected(_))));
    }
    #[test]
    fn expanded_work_counts_deletions_and_expired_bodies_do_not_inherit_time(){
        let mut s=State::default();let mut g=Groups::default();
        let mut r=request("first",0,&["a"]);r.records[0].temporal=Some(TemporalMetadata{expire_at_ms:Some(2),..Default::default()});
        let Plan::Replace(d)=plan(&s,&g,r).unwrap() else {panic!()};install(&mut s,&mut g,d);
        let Plan::Replace(d)=prepare(&s,&g,&super::super::tests::config(),request("next",1,&["a"]),current_epoch(),3).unwrap() else {panic!()};
        assert_eq!(d.records[0].temporal.expire_at_ms,None);
        let mut head=g.heads["g"].clone();head.members=(0..MAX_MEMBERS).map(|n|format!("old{n}")).collect();g.heads.insert("g".into(),head);
        assert!(matches!(plan(&s,&g,request("too-many",1,&["new"])),Err(RecordError::Rejected(_))));
    }
    #[test]
    fn group_receipt_gc_requires_both_boundaries_and_retains_group_history(){
        let mut state=State::default();let cfg=super::super::tests::config();let request=request("gc",0,&["a"]);
        let epoch=request.operation_id.epoch;
        let Plan::Replace(d)=plan(&state,&Groups::default(),request.clone()).unwrap() else{panic!()};
        apply(&mut state,&d).unwrap();
        maintenance::collect_operations(&mut state,epoch+2).unwrap();assert_eq!(state.groups.as_ref().unwrap().receipts.len(),1);
        state.history.floor=1;
        maintenance::collect_operations(&mut state,epoch+1).unwrap();assert_eq!(state.groups.as_ref().unwrap().receipts.len(),1);
        maintenance::collect_operations(&mut state,epoch+2).unwrap();
        let groups=state.groups.as_ref().unwrap();assert!(groups.receipts.is_empty());assert_eq!(groups.heads["g"].version,1);assert_eq!(groups.owners["a"],"g");assert_eq!(groups.publication_seq,1);
        validate(&state,&cfg).unwrap();temporal::validate_state(&state).unwrap();
        assert!(matches!(prepare(&state,groups,&cfg,request,epoch,1),Err(RecordError::OperationExpired)));
    }

}
