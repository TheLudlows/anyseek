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
    #[serde(skip)]
    pub(super) receipt_lookup: BTreeMap<OperationId, usize>,
    #[serde(skip)]
    pub(super) receipt_epochs: ReceiptEpochs,
}
impl Groups {
    pub(super) fn receipt(&self, id: &OperationId) -> Option<&StoredReceipt> {
        self.receipt_lookup.get(id).map(|&index| &self.receipts[index])
    }

    pub(super) fn has_expired_receipts(&self, floor: u64, epoch: u64) -> bool {
        self.receipt_epochs.has_expired(floor, epoch)
    }

    pub(super) fn receipt_indexes(&self) -> (&BTreeMap<OperationId, usize>, &ReceiptEpochs) {
        (&self.receipt_lookup, &self.receipt_epochs)
    }

    pub(super) fn rebuild_receipt_indexes(
        &mut self, ordinary: &BTreeMap<OperationId, usize>,
    ) -> Result<(), RecordError> {
        self.receipt_lookup.clear();
        self.receipt_epochs = ReceiptEpochs::default();
        for (position, receipt) in self.receipts.iter().enumerate() {
            let id = &receipt.operation_id;
            if ordinary.contains_key(id) || self.receipt_lookup.insert(id.clone(), position).is_some() {
                return Err(storage("duplicate checkpoint group operation identity"));
            }
            self.receipt_epochs.push(receipt.receipt.seq, id.epoch);
        }
        Ok(())
    }

    fn push_receipt(&mut self, receipt: StoredReceipt) {
        self.receipt_lookup.insert(receipt.operation_id.clone(), self.receipts.len());
        self.receipt_epochs.push(receipt.receipt.seq, receipt.operation_id.epoch);
        self.receipts.push(receipt);
    }
}
pub(super) enum Plan { Replay(GroupReceipt), Replace(Decision) }
pub(super) const MAX_MEMBERS:usize=64;
const MAX_BYTES:usize=4*1024*1024;

pub(super) fn prepare(state:&State, config:&RecordConfig, request:GroupReplace,
    wall_epoch:u64, now:u64)->Result<Plan,RecordError> {
    let empty = Groups::default();
    let groups = state.groups.as_ref().unwrap_or(&empty);
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
    if let Some(old)=groups.receipt(&request.operation_id) {
        return if old.request_digest==request_digest(&request)? {
            Ok(Plan::Replay(old.receipt.clone()))
        } else {Err(RecordError::Conflict("operation identity reused with another group request".into()))};
    }
    let head=groups.heads.get(&request.group_id);
    let version=head.map_or(0,|h|h.version);
    if request.expected_group_version!=version {return Err(RecordError::Conflict("group version precondition failed".into()));}
    let mut members=BTreeSet::new();
    for member in &request.records {
        if member.record_id.is_empty() || !members.insert(member.record_id.clone()) {
            return Err(RecordError::Rejected("empty or duplicate group member".into()));
        }
        match groups.owners.get(&member.record_id) {
            Some(owner) if owner!=&request.group_id=>return Err(RecordError::Conflict("record belongs to another group".into())),
            None if state.records.find(&member.record_id)?.is_some()=>return Err(RecordError::Conflict("record belongs to independent writes".into())),
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
    let mut ttl=state.records.live_ttl_count();
    let writes=request.records.iter().map(|r|(r.record_id.clone(),Some(r.document.clone()),r.temporal.clone()))
        .chain(removed.into_iter().map(|id|(id,None,None)));
    for (id,document,temporal) in writes {
        let previous=state.records.load(&id)?;
        let previous=previous.as_deref();
        let write=RecordWrite{operation_id:request.operation_id.clone(),record_id:id,expected:ExpectedVersion::Any,document,vector_patch:None,temporal};
        let input=mutation::Input{previous,operation:None,seq:state.seq,epoch_watermark:epoch,time_watermark_ms:state.time_watermark_ms,
            record_count:state.records.len(),operation_count:state.operations.len(),ttl_count:ttl};
        let _request_plan=mutation::reserve_request(&write)?;
        let mutation::PlannedRecord { record, .. } = mutation::prepare_record(&input,config,write,epoch,now)?;
        ttl=ttl-usize::from(previous.is_some_and(temporal::live_ttl))+usize::from(temporal::live_ttl(&record));
        records.push(record);
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
impl State {
    pub(super) fn group_publication_revision(&self)->u64 {self.groups.as_ref().map_or(0,|g|g.publication_seq)}
}

fn validate_decision(state: &State, d: &Decision) -> Result<(), RecordError> {
    if state.seq.checked_add(1)!=Some(d.receipt.seq) || d.observed_time_ms<state.time_watermark_ms
        || d.records.len()>MAX_MEMBERS || d.members.len()!=d.receipt.record_count
        || d.operation_id.nonce.is_empty() || d.receipt.group_id.is_empty() || d.receipt.group_id.len()>256
        || d.operation_id.epoch>d.observed_epoch || d.operation_id.epoch.saturating_add(1)<d.observed_epoch {
        return Err(storage("invalid group decision sequence or bounds"));
    }
    let empty=Groups::default();let groups=state.groups.as_ref().unwrap_or(&empty);
    if groups.receipt(&d.operation_id).is_some() || state.operation_lookup.contains_key(&d.operation_id)
        || groups.heads.get(&d.receipt.group_id).map_or(0,|h|h.version).checked_add(1)!=Some(d.receipt.group_version) {
        return Err(storage("invalid group decision version or retry identity"));
    }
    let old=groups.heads.get(&d.receipt.group_id).map(|h|&h.members);
    let expected:BTreeSet<_>=old.into_iter().flatten().chain(d.members.iter()).cloned().collect();
    let actual:BTreeSet<_>=d.records.iter().map(|r|r.record_id.clone()).collect();
    if actual!=expected || actual.len()!=d.records.len() {return Err(storage("group decision omits or duplicates members"));}
    for r in &d.records {
        let previous = state.records.find(&r.record_id)?;
        if previous.as_ref().map_or(Some(1),|old|old.version().checked_add(1))!=Some(r.version)
            || r.document.is_some()!=d.members.contains(&r.record_id) || r.expired_at_ms.is_some()
            || r.ingested_at_ms!=if r.document.is_some(){d.observed_time_ms}else{previous.as_ref().map_or(d.observed_time_ms, state_values::CurrentRow::ingested_at_ms)}
            || groups.owners.get(&r.record_id).is_some_and(|g|g!=&d.receipt.group_id)
            || (groups.owners.get(&r.record_id).is_none() && previous.is_some()) {
            return Err(storage("inconsistent group member decision"));
        }
    }
    Ok(())
}

pub(super) struct Prepared {
    pub(super) stored: StoredReceipt,
    pub(super) records: Vec<state_memory::Shared<Record>>,
    previous: Vec<Option<state_values::PreviousRow>>,
    pub(super) snapshot_bytes: usize,
    pub(super) reserve_bytes: usize,
    head: Head,
    lookup_bytes: usize,
    container_bytes: usize,
}

fn replace_bytes(total: usize, previous: usize, next: usize) -> Result<usize, RecordError> {
    total.checked_sub(previous).and_then(|n| n.checked_add(next))
        .ok_or_else(|| storage("group size accounting overflow"))
}

fn map_key_bytes(key: &str, has_entries: bool) -> Result<usize, RecordError> {
    checkpoint_codec::json_length(&key)?.checked_add(1 + usize::from(has_entries))
        .ok_or_else(|| storage("group key size overflow"))
}

impl Prepared {
    /// Admit only the changed values and metadata. The writer keeps the same
    /// basis until publication, and every byte delta describes the existing
    /// checkpoint representation, including the first group and map delimiters.
    pub(super) fn new(state: &State, decision: Decision, mut snapshot_bytes: usize) -> Result<Self, RecordError> {
        validate_decision(state, &decision)?;
        let Decision { operation_id, request_digest, members, receipt, records,
            observed_epoch, observed_time_ms } = decision;
        let empty = Groups::default();
        let groups = state.groups.as_ref().unwrap_or(&empty);
        if state.groups.is_none() {
            snapshot_bytes = replace_bytes(snapshot_bytes, 0,
                checkpoint_codec::json_length(&empty)? + b",\"groups\":".len())?;
        }

        let head = Head { version: receipt.group_version, members };
        let old_head = groups.heads.get(&receipt.group_id);
        let head_key = if old_head.is_none() {
            map_key_bytes(&receipt.group_id, !groups.heads.is_empty())?
        } else { 0 };
        snapshot_bytes = replace_bytes(snapshot_bytes,
            old_head.map(checkpoint_codec::json_length).transpose()?.unwrap_or(0),
            checkpoint_codec::json_length(&head)?.checked_add(head_key)
                .ok_or_else(|| storage("group head size overflow"))?)?;

        let stored = StoredReceipt { operation_id, request_digest, receipt, observed_epoch, observed_time_ms };
        let lookup_bytes = state.lookup_bytes.checked_add(operation_lookup_bytes(&stored.operation_id)?)
            .ok_or_else(|| storage("group lookup size overflow"))?;
        snapshot_bytes = replace_bytes(snapshot_bytes, 0, checkpoint_codec::json_length(&stored)?
            .checked_add(usize::from(!groups.receipts.is_empty()))
            .ok_or_else(|| storage("group receipt size overflow"))?)?;
        for (previous, next) in [
            (groups.publication_seq, stored.receipt.seq),
            (state.seq, stored.receipt.seq),
            (state.epoch_watermark, state.epoch_watermark.max(observed_epoch)),
            (state.time_watermark_ms, observed_time_ms),
        ] {
            snapshot_bytes = replace_bytes(snapshot_bytes,
                checkpoint_codec::json_length(&previous)?, checkpoint_codec::json_length(&next)?)?;
        }

        let mut values = state.value_bytes()?;
        let mut has_records = !state.records.is_empty();
        let mut has_owners = !groups.owners.is_empty();
        let mut ttl = state.records.live_ttl_count();
        let mut prepared = Vec::with_capacity(records.len());
        let mut replaced = Vec::with_capacity(records.len());
        let group_owner_bytes = checkpoint_codec::json_length(&stored.receipt.group_id)?;
        for record in records {
            let id = &record.record_id;
            let previous = state.records.find(id)?.map(|row| row.account(id)).transpose()?;
            let old = previous.map_or(0, |row| row.bytes);
            replaced.push(previous);
            if previous.is_none() {
                snapshot_bytes = replace_bytes(snapshot_bytes, 0, map_key_bytes(id, has_records)?)?;
                has_records = true;
            }
            if !groups.owners.contains_key(id) {
                let key = map_key_bytes(id, has_owners)?;
                snapshot_bytes = replace_bytes(snapshot_bytes, 0,
                    key.checked_add(group_owner_bytes).ok_or_else(|| storage("group owner size overflow"))?)?;
                has_owners = true;
            }
            ttl = replace_bytes(ttl, usize::from(previous.is_some_and(state_values::PreviousRow::has_ttl)),
                usize::from(temporal::live_ttl(&record)))?;
            let value = state_memory::Shared::admit(record)?;
            values = replace_bytes(values, old, value.wire_bytes())?;
            snapshot_bytes = replace_bytes(snapshot_bytes, old, value.wire_bytes())?;
            prepared.push(value);
        }
        let reserve_bytes = ttl.checked_mul(8192).ok_or_else(|| storage("group TTL budget overflow"))?;
        let container_bytes = replace_bytes(snapshot_bytes, values.checked_add(state.records.sealed_keys_bytes()).ok_or(RecordError::Busy)?, lookup_bytes)?;
        Ok(Self { stored, records: prepared, previous: replaced, snapshot_bytes, reserve_bytes, head, lookup_bytes, container_bytes })
    }

    pub(super) fn admit_containers(&self, state: &mut State) -> Result<(), RecordError> {
        state.admit_containers(self.container_bytes)
    }

    /// Install while holding the publication guard. No state read or response
    /// can observe a subset, and unexpected failure fences the live partition.
    pub(super) fn install(self, state: &mut State) -> Result<GroupReceipt, RecordError> {
        let Self { stored, records, previous, head, lookup_bytes, .. } = self;
        let receipt = stored.receipt.clone();
        let groups = state.groups.get_or_insert_with(Groups::default);
        for (record, previous) in records.into_iter().zip(previous) {
            let id = record.record_id.clone();
            if !groups.owners.contains_key(&id) {
                groups.owners.insert(id.clone(), receipt.group_id.clone());
            }
            state.records.replace_prepared(id, record, previous)?;
        }
        groups.heads.insert(receipt.group_id.clone(), head);
        groups.publication_seq = receipt.seq;
        state.seq = receipt.seq;
        state.epoch_watermark = state.epoch_watermark.max(stored.observed_epoch);
        state.time_watermark_ms = stored.observed_time_ms;
        state.lookup_bytes = lookup_bytes;
        groups.push_receipt(stored);
        Ok(receipt)
    }
}

/// Apply an already durable decision during checkpoint/WAL proof replay.
pub(super) fn apply(state: &mut State, decision: Decision, state_bytes: usize) -> Result<(), RecordError> {
    let prepared = Prepared::new(state, decision, state_bytes)?;
    prepared.admit_containers(state)?;
    prepared.install(state)?;
    Ok(())
}

pub(super) fn validate(state:&State)->Result<(),RecordError>{
    let Some(g)=&state.groups else{return Ok(());};
    if g.publication_seq>state.seq {return Err(storage("group publication watermark exceeds state sequence"));}
    let mut seq=0;let mut latest=BTreeMap::new();
    let mut receipt_epochs = ReceiptEpochs::default();
    for (position, r) in g.receipts.iter().enumerate() {
        if r.receipt.seq<=seq || r.receipt.seq>state.seq || r.observed_time_ms>state.time_watermark_ms
            || g.receipt_lookup.get(&r.operation_id) != Some(&position) || state.operation_lookup.contains_key(&r.operation_id)
            || g.heads.get(&r.receipt.group_id).is_none_or(|h|h.version<r.receipt.group_version) {return Err(storage("invalid group receipt history"));}
        seq=r.receipt.seq;latest.insert(&r.receipt.group_id,r);
        receipt_epochs.push(seq, r.operation_id.epoch);
    }
    if g.receipt_lookup.len() != g.receipts.len() || g.receipt_epochs != receipt_epochs {
        return Err(storage("invalid group receipt indexes"));
    }
    if seq>g.publication_seq || (seq<g.publication_seq && state.retired_operation_prefix.as_ref().is_none_or(|p|p.seq<g.publication_seq)) {return Err(storage("group publication watermark mismatch"));}
    for (id,head) in &g.heads {
        if id.is_empty() || head.version==0 || head.members.len()>MAX_MEMBERS {return Err(storage("invalid group head"));}
        if let Some(last)=latest.get(id)
            .filter(|r|state.retired_operation_prefix.as_ref().is_none_or(|p|r.receipt.seq>p.seq)) {
            if last.receipt.group_version!=head.version || last.receipt.record_count!=head.members.len(){return Err(storage("group head differs from latest receipt"));}
        }
        for member in &head.members {if g.owners.get(member)!=Some(id) || state.records.find(member)?.is_none_or(|r|!r.live()&&r.expired_at_ms().is_none()){return Err(storage("orphan group member"));}}
    }
    for (id,owner) in &g.owners {
        let row=state.records.find(id)?.ok_or_else(||storage("orphan group ownership"))?;
        let head=g.heads.get(owner).ok_or_else(||storage("orphan group ownership"))?;
        if !head.members.contains(id)&&row.live(){return Err(storage("removed group member has a body"));}
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
    fn plan(state: &State, request: GroupReplace) -> Result<Plan, RecordError> {
        prepare(state, &super::super::tests::config(), request, current_epoch(), 1)
    }
    fn apply_decision(state: &mut State, decision: Decision) {
        let bytes = state_proof::length(state).unwrap();
        apply(state, decision, bytes).unwrap();
    }
    #[tokio::test]
    async fn group_budget_counts_one_receipt_and_preserves_ordinary_wire_size() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = super::super::tests::config();
        // Sixteen 2 KB bodies fit once, but not twice, in this checkpoint.
        config.max_journal_bytes = 50 * 1024;
        config.max_operations = 2;
        let p = RecordPartition::create(&temp.path().join("records"), config.clone()).await.unwrap();
        let ids: Vec<_> = (0..16).map(|n| format!("member-{n}")).collect();
        let mut replacement = request("group", 0, &ids.iter().map(String::as_str).collect::<Vec<_>>());
        for member in &mut replacement.records {
            member.document.insert("body".into(), serde_json::json!("x".repeat(2000)));
        }
        let receipt = p.replace_group(replacement.clone()).await.unwrap();
        p.write(super::super::tests::write("ordinary", "independent", "beta", ExpectedVersion::Absent)).await.unwrap();
        assert_eq!(p.replace_group(replacement).await.unwrap(), receipt);
        p.write(super::super::tests::write("extra", "extra", "gamma", ExpectedVersion::Absent)).await.unwrap();
        let inner = p.inner.lock().await;
        assert_eq!(inner.snapshot_bytes, checkpoint_codec::full_length(&config, &inner.state).unwrap());
        assert_eq!(inner.state.operations.len(), 2);
        assert_eq!(inner.state.groups.as_ref().unwrap().receipts.len(), 1);
        drop(inner);
        assert_eq!(p.statistics().await.unwrap().retained_operations, 3);
        p.finish_service_shutdown().await.unwrap();
    }
    #[test]
    fn replace_clear_replay_and_membership_are_whole_decisions(){
        let mut s=State::default();let first=request("first",0,&["a","b","c"]);
        let Plan::Replace(d)=plan(&s,first.clone()).unwrap() else {panic!()};let receipt=d.receipt.clone();apply_decision(&mut s,d);
        let Plan::Replace(d)=plan(&s,request("next",1,&["a","d"])).unwrap() else {panic!()};
        assert_eq!(d.records.len(),4);assert_eq!(d.receipt.seq,2);assert_eq!(d.records.iter().filter(|r|r.document.is_none()).count(),2);apply_decision(&mut s,d);
        assert!(matches!(plan(&s,first),Ok(Plan::Replay(r)) if r==receipt));
        let Plan::Replace(d)=plan(&s,request("clear",2,&[])).unwrap() else {panic!()};assert_eq!(d.records.len(),2);apply_decision(&mut s,d);
        let group = &s.groups.as_ref().unwrap().heads["g"];
        assert_eq!(group.version, 3);
        assert!(group.members.is_empty());
        assert!(matches!(plan(&s,request("reuse",3,&["b"])),Ok(Plan::Replace(_))));
        let mut other=request("other",0,&["b"]);other.group_id="other".into();assert!(matches!(plan(&s,other),Err(RecordError::Conflict(_))));
    }
    #[test]
    fn invalid_suffix_stale_version_and_retry_conflict_never_change_state(){
        let mut s=State::default();
        let mut r=request("invalid",0,&["a","b"]);r.records[1].document.insert("vector".into(),serde_json::json!([1]));
        assert!(plan(&s,r).is_err());assert_eq!(s.seq,0);assert!(s.records.is_empty());
        assert!(matches!(plan(&s,request("stale",1,&["fresh"])),Err(RecordError::Conflict(_))));
        let Plan::Replace(d)=plan(&s,request("same",0,&["a"])).unwrap() else {panic!()};apply_decision(&mut s,d);
        assert!(matches!(plan(&s,request("same",0,&["b"])),Err(RecordError::Conflict(_))));
        assert!(matches!(plan(&s,request("duplicate",1,&["b","b"])),Err(RecordError::Rejected(_))));
    }
    #[test]
    fn expanded_work_counts_deletions_and_expired_bodies_do_not_inherit_time(){
        let mut s=State::default();
        let mut r=request("first",0,&["a"]);r.records[0].temporal=Some(TemporalMetadata{expire_at_ms:Some(2),..Default::default()});
        let Plan::Replace(d)=plan(&s,r).unwrap() else {panic!()};apply_decision(&mut s,d);
        let Plan::Replace(d)=prepare(&s,&super::super::tests::config(),request("next",1,&["a"]),current_epoch(),3).unwrap() else {panic!()};
        assert_eq!(d.records[0].temporal.expire_at_ms,None);
        let head=s.groups.as_mut().unwrap().heads.get_mut("g").unwrap();
        head.members=(0..MAX_MEMBERS).map(|n|format!("old{n}")).collect();
        assert!(matches!(plan(&s,request("too-many",1,&["new"])),Err(RecordError::Rejected(_))));
    }
    #[test]
    fn group_receipt_gc_requires_both_boundaries_and_retains_group_history(){
        let mut state=State::default();let cfg=super::super::tests::config();let request=request("gc",0,&["a"]);
        let epoch=request.operation_id.epoch;
        let Plan::Replace(d)=plan(&state,request.clone()).unwrap() else{panic!()};
        apply_decision(&mut state,d);
        maintenance::collect_operations(&mut state,epoch+2).unwrap();assert_eq!(state.groups.as_ref().unwrap().receipts.len(),1);
        state.history.floor=1;
        maintenance::collect_operations(&mut state,epoch+1).unwrap();assert_eq!(state.groups.as_ref().unwrap().receipts.len(),1);
        maintenance::collect_operations(&mut state,epoch+2).unwrap();
        let groups=state.groups.as_ref().unwrap();assert!(groups.receipts.is_empty());assert_eq!(groups.heads["g"].version,1);assert_eq!(groups.owners["a"],"g");assert_eq!(groups.publication_seq,1);
        validate(&state).unwrap();temporal::validate_state(&state).unwrap();
        assert!(matches!(prepare(&state,&cfg,request,epoch,1),Err(RecordError::OperationExpired)));
    }

    #[test]
    fn receipt_expiry_uses_commit_floor_even_when_older_epoch_arrives_later() {
        let mut state = State::default();
        let config = super::super::tests::config();
        let mut operations = Vec::new();
        for (version, epoch) in [40, 39, 41].into_iter().enumerate() {
            let mut group = request(&format!("epoch-{epoch}"), version as u64, &["a"]);
            group.operation_id.epoch = epoch;
            let Plan::Replace(decision) = prepare(&state, &config, group.clone(), epoch.max(40), version as u64 + 1).unwrap()
                else { panic!("new group operation") };
            operations.push(group.operation_id);
            apply_decision(&mut state, decision);
        }
        let mut ordinary = super::super::tests::write("previous-epoch", "independent", "alpha", ExpectedVersion::Absent);
        ordinary.operation_id.epoch = 40;
        let mutation::PreparedWrite::Append { applied, .. } = mutation::prepare_at(
            &state, &config, checkpoint_codec::length(&config, &state).unwrap(), ordinary.clone(), 41, 4,
        ).unwrap() else { panic!("new ordinary operation") };
        super::super::apply(&mut state, &applied).unwrap();

        for floor in 0..=state.seq {
            state.history.floor = floor;
            for wall_epoch in [40u64, 41, 42, 43] {
                let epoch = wall_epoch.max(state.epoch_watermark);
                let oracle = operations.iter().chain([&ordinary.operation_id]).zip(1..)
                    .any(|(id, seq)| seq <= floor && id.epoch.saturating_add(1) < epoch);
                assert_eq!(maintenance::has_expired_operations(&state, wall_epoch), oracle,
                    "history floor {floor}, epoch {wall_epoch}");
            }
        }
        state.history.floor = 2;
        maintenance::collect_operations(&mut state, 41).unwrap();
        let groups = state.groups.as_ref().unwrap();
        assert!(groups.receipt(&operations[0]).is_some());
        assert!(groups.receipt(&operations[1]).is_none());
        assert!(groups.receipt(&operations[2]).is_some());
        assert!(!maintenance::has_expired_operations(&state, 41));

        // Same serialized shape and order, with both indexes rebuilt at open.
        let mut reopened: State = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
        restore_operation_lookup(&mut reopened).unwrap();
        validate(&reopened).unwrap();
        state_proof::verify(&config, &state, &reopened).unwrap();
    }

    #[test]
    #[ignore = "explicit fixed-size group planning comparison"]
    fn group_planning_across_retained_history_sizes() {
        use std::{hint::black_box, time::Instant};

        let config = super::super::tests::config();
        let epoch = current_epoch();
        for count in [32, 8192] {
            let mut state = State::default();
            let mut bytes = checkpoint_codec::length(&config, &state).unwrap();
            let mut seed = |request| {
                let Plan::Replace(decision) = prepare(&state, &config, request, epoch, 9).unwrap()
                    else { panic!("new seeded group") };
                let prepared = Prepared::new(&state, decision, bytes).unwrap();
                prepared.admit_containers(&mut state).unwrap();
                bytes = prepared.snapshot_bytes;
                prepared.install(&mut state).unwrap();
            };
            seed(request("target", 0, &["target-a", "target-b"]));
            let mut last = None;
            for n in 0..count {
                let id = format!("history-{n:05}");
                let mut history = request(&id, 0, &[&id]);
                history.group_id = id;
                last = Some(history.clone());
                seed(history);
            }
            state.history.floor = state.seq;
            bytes = checkpoint_codec::length(&config, &state).unwrap();
            assert_eq!(state.records.len(), count + 2);
            assert_eq!(state.groups.as_ref().unwrap().receipts.len(), count + 1);

            let update = request("update", 1, &["target-a", "target-c"]);
            let last = last.unwrap();
            let iterations = 1000;
            let started = Instant::now();
            for _ in 0..iterations {
                assert!(!maintenance::has_expired_operations(&state, epoch));
                let Plan::Replace(decision) = prepare(&state, &config, update.clone(), epoch, 10).unwrap()
                    else { panic!("new measured group") };
                let entry = Entry::GroupApplied(decision);
                black_box(encode_entry(&entry).unwrap());
                let Entry::GroupApplied(decision) = entry
                    else { panic!("group WAL entry") };
                let prepared = Prepared::new(&state, decision, bytes).unwrap();
                assert_eq!(prepared.records.len(), 3);
                black_box(prepared);
            }
            let planning_us = started.elapsed().as_micros() / iterations;
            let started = Instant::now();
            for _ in 0..iterations {
                let replay = prepare(&state, &config, last.clone(), epoch, 10).unwrap();
                assert!(matches!(replay, Plan::Replay(receipt) if receipt.seq == state.seq));
            }
            let replay_ns = started.elapsed().as_nanos() / iterations;

            let started = Instant::now();
            let mut serialized = 0;
            for _ in 0..20 {
                let clone = state.try_clone().unwrap();
                serialized = checkpoint_codec::length(&config, &clone).unwrap();
                black_box(clone);
            }
            let clone_and_measure_us = started.elapsed().as_micros() / 20;

            let Plan::Replace(decision) = prepare(&state, &config, update, epoch, 10).unwrap()
                else { panic!("new group") };
            let prepared = Prepared::new(&state, decision, bytes).unwrap();
            let expected = prepared.snapshot_bytes;
            prepared.admit_containers(&mut state).unwrap();
            prepared.install(&mut state).unwrap();
            assert_eq!(checkpoint_codec::full_length(&config, &state).unwrap(), expected);
            assert_eq!(serialized, bytes);
            eprintln!("history={count} touched=3 plan_us={planning_us} replay_ns={replay_ns} clone_and_measure_us={clone_and_measure_us}");
        }
    }

}
