//! State residency is owned independently of component/writer admission.
//! Logical JSON expansion is conservative; this is not an RSS/allocator limit.
use super::*;
use crate::storage::build_budget::{reserve_memory, MemoryLease};

/// An immutable value and its already admitted residency have one lifetime.
/// Serialization exposes only the value, preserving the authenticated format.
#[derive(Debug)]
pub(super) struct Shared<T>(Arc<ValueOwner<T>>);
struct ValueOwner<T> { value: T, _lease: MemoryLease }
impl<T: std::fmt::Debug> std::fmt::Debug for ValueOwner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValueOwner").field("value", &self.value).field("bytes", &self._lease.bytes()).finish()
    }
}
impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self { Self(self.0.clone()) }
}
impl<T> std::ops::Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.0.value }
}
impl<T: Serialize> Serialize for Shared<T> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.value.serialize(serializer)
    }
}
pub(super) trait ProofValue: Serialize {}
impl ProofValue for Record {}
impl ProofValue for Applied {}

impl<T: Serialize> Shared<T> {
    fn take_parts(parts: &mut [MemoryLease], bytes: u64) -> Result<MemoryLease, RecordError> {
        crate::storage::build_budget::validate_single(bytes).map_err(storage)?;
        let total=parts.iter().try_fold(0u64,|n,p|n.checked_add(p.bytes()).ok_or_else(||storage("prepared balance overflow")))?;
        if bytes>total{return Err(storage("value exceeds prepared ownership"));}
        let mut value=reserve_memory(0).map_err(storage)?;let mut remaining=bytes;
        for part in parts {let n=remaining.min(part.bytes());value.merge(part.split_off(n).map_err(storage)?).map_err(storage)?;remaining-=n;if remaining==0{break;}}
        Ok(value)
    }
    fn from_parts(value:T, parts:&mut [MemoryLease])->Result<Self,RecordError>{
        let bytes=(checkpoint_codec::json_length(&value)? as u64).checked_mul(32).ok_or_else(||storage("shared value overflow"))?;
        Ok(Self(Arc::new(ValueOwner{value,_lease:Self::take_parts(parts,bytes)?})))
    }
    fn copy_from_parts(value:&T,parts:&mut [MemoryLease])->Result<Self,RecordError> where T:Clone {
        let bytes=(checkpoint_codec::json_length(value)? as u64).checked_mul(32).ok_or_else(||storage("shared value overflow"))?;
        let lease=Self::take_parts(parts,bytes)?;Ok(Self(Arc::new(ValueOwner{value:value.clone(),_lease:lease})))
    }
    pub(super) fn same_value(&self, other: &Self) -> Result<bool,RecordError> where T:ProofValue {
        Ok(Arc::ptr_eq(&self.0,&other.0) || (self.charged_bytes()==other.charged_bytes()
            && checkpoint_codec::json_digest(&**self)?==checkpoint_codec::json_digest(&**other)?))
    }
    pub(super) fn charged_bytes(&self) -> u64 { self.0._lease.bytes() }
    pub(super) fn from_admitted(value: T, admitted: &mut MemoryLease) -> Result<Self, RecordError> {
        let bytes = (checkpoint_codec::json_length(&value)? as u64).checked_mul(32)
            .ok_or_else(|| storage("shared state value estimate overflow"))?;
        let lease = admitted.split_off(bytes).map_err(storage)?;
        Ok(Self(Arc::new(ValueOwner { value, _lease: lease })))
    }
    /// Reserve transfer precedes the actual deep clone, including on rejection.
    pub(super) fn copy_from_admitted(value: &T, admitted: &mut MemoryLease) -> Result<Self, RecordError>
    where T: Clone {
        let bytes = (checkpoint_codec::json_length(value)? as u64).checked_mul(32)
            .ok_or_else(|| storage("shared state value estimate overflow"))?;
        let lease = admitted.split_off(bytes).map_err(storage)?;
        Ok(Self(Arc::new(ValueOwner { value: value.clone(), _lease: lease })))
    }
    // Test-only non-growing mutation probe. Production values are immutable;
    // unique Arc ownership alone never authorizes an allocation or growth.
    #[cfg(test)]
    fn unique_mut(&mut self) -> Result<&mut T, RecordError> {
        Arc::get_mut(&mut self.0).map(|owned| &mut owned.value)
            .ok_or_else(|| storage("shared state mutation requires admitted private copy"))
    }
}

pub(super) struct Charge {
    bytes: u64,
    lease: MemoryLease,
}
impl std::fmt::Debug for Charge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateCharge")
            .field("bytes", &self.bytes)
            .finish()
    }
}
impl State {
    pub(super) fn admit_bytes(&mut self, bytes: usize) -> Result<(), RecordError> {
        let needed = self.container_bytes(bytes)?;
        self.admit_containers(needed)
    }
    fn admit_containers(&mut self, bytes:usize)->Result<(),RecordError>{
        let needed=(bytes as u64).checked_mul(32).ok_or_else(||storage("state metadata estimate overflow"))?;
        if let Some(charge) = self.residency.as_mut().and_then(Arc::get_mut) {
            if needed > charge.bytes {
                charge.lease.grow(needed - charge.bytes).map_err(storage)?;
                charge.bytes = needed;
            }
        } else {
            self.residency = Some(Arc::new(Charge {
                bytes: needed,
                lease: reserve_memory(needed).map_err(storage)?,
            }));
        }
        Ok(())
    }
    pub(super) fn admit_lookup(&mut self, bytes: usize) -> Result<(), RecordError> {
        let metadata=self.container_bytes(state_proof::length(self)?)?
            .checked_sub(self.lookup_bytes).and_then(|n|n.checked_add(bytes))
            .ok_or_else(||storage("lookup metadata estimate overflow"))?;
        self.admit_containers(metadata)?;
        self.lookup_bytes=bytes;Ok(())
    }
    pub(super) fn attach_residency(&mut self, bytes: u64, lease: MemoryLease) {
        self.residency = Some(Arc::new(Charge { bytes, lease }));
    }
    pub(super) fn value_bytes(&self) -> Result<usize, RecordError> {
        self.records.bytes().checked_add(self.source_revisions.bytes()).and_then(|n|n.checked_add(self.operations.bytes())).ok_or_else(||storage("shared value total overflow"))
    }
    fn container_bytes(&self, total: usize) -> Result<usize, RecordError> {
        let values = self.records.bytes().checked_add(self.source_revisions.bytes())
            .and_then(|n|n.checked_add(self.operations.bytes())).ok_or_else(||storage("shared value total overflow"))?;
        total.checked_sub(values).and_then(|n|n.checked_add(self.lookup_bytes))
            .and_then(|n|n.checked_add(std::mem::size_of::<std::sync::OnceLock<generation_values::Counts>>()))
            .ok_or_else(||storage("state container size overflow"))
    }
    // Container keys, pointers, lookup and nonshared metadata are copied only
    // after admission. Each immutable value retains its own shared lease.
    pub(super) fn try_clone(&self) -> Result<Self, RecordError> {
        self.try_clone_with_growth(0)
    }
    pub(super) fn try_clone_with_growth(&self, growth: usize) -> Result<Self, RecordError> {
        let bytes = self.container_bytes(state_proof::length(self)?)?
            .checked_add(growth)
            .and_then(|n| (n as u64).checked_mul(32))
            .ok_or_else(|| storage("state clone estimate overflow"))?;
        let residency = Some(Arc::new(Charge {
            bytes,
            lease: reserve_memory(bytes).map_err(storage)?,
        }));
        Ok(Self {
            lineage: self.lineage.clone(),
            feed: self.feed.clone(),
            sealed_for: self.sealed_for.clone(),
            takeover_proof: self.takeover_proof.clone(),
            independent_snapshot: self.independent_snapshot,
            seq: self.seq,
            epoch_watermark: self.epoch_watermark,
            time_watermark_ms: self.time_watermark_ms,
            retired_operation_prefix: self.retired_operation_prefix.clone(),
            expirations: self.expirations.clone(),
            lease_expirations: self.lease_expirations.clone(),
            private_cleanup_pending: self.private_cleanup_pending,
            evidence_quarantined: self.evidence_quarantined,
            records: self.records.clone(),
            source_revisions: self.source_revisions.clone(),
            generations: self.generations.clone(),
            operations: self.operations.clone(),
            operation_lookup: self.operation_lookup.clone(),
            lookup_bytes: self.lookup_bytes,
            residency,
        })
    }
}

/// A gross ownership reservation prepared before the durable append. Replacing
/// a same-sized live value still allocates a complete new value while a snapshot
/// may retain the old one; no net-size subtraction is used for these owners.
pub(super) struct Prepared {
    pub record: Shared<Record>,
    pub operation: Shared<Applied>,
}
impl Prepared {
    pub(super) fn new(state: &State, applied: &Applied) -> Result<Self, RecordError> {
        let prior=state.records.get(&applied.request.record_id);
        let applied_bytes=checkpoint_codec::json_length(applied)?;
        let prior_bytes=prior.map(checkpoint_codec::json_length).transpose()?.unwrap_or(0);
        // Separate bounded allocations: new record projection, retained request
        // copy and small fixed record metadata. No giant aggregate single lease.
        let mut admission=[applied_bytes,applied_bytes,prior_bytes,1024].into_iter().map(|n|{
            reserve_memory((n as u64).checked_mul(32).ok_or_else(||storage("prepared estimate overflow"))?).map_err(storage)
        }).collect::<Result<Vec<_>,_>>()?;
        let document=enrichment::document(prior,&applied.request)?;
        let temporal=temporal::retained(prior,&applied.request,applied.observed_time_ms)?;
        let record=mutation::projected_record(state,applied,document,temporal);
        let record=Shared::from_parts(record,&mut admission)?;
        let operation=Shared::copy_from_parts(applied,&mut admission)?;
        Ok(Self{record,operation})
    }
    pub(super) fn admit_containers(&self,state:&mut State,total:usize)->Result<(),RecordError>{
        let old=state.records.get(&self.record.record_id);
        let old_bytes=old.map(checkpoint_codec::json_length).transpose()?.unwrap_or(0);
        let history=if self.operation.request.vector_patch.is_none() && old.is_some_and(evidence::retained_revision){old_bytes}else{0};
        let values=state.value_bytes()?.checked_sub(old_bytes).and_then(|n|n.checked_add(history))
            .and_then(|n|n.checked_add(self.record.wire_bytes())).and_then(|n|n.checked_add(self.operation.wire_bytes()))
            .ok_or_else(||storage("prepared retained value size overflow"))?;
        let lookup=state.lookup_bytes.checked_add(checkpoint_codec::json_length(&self.operation.request.operation_id)?).ok_or_else(||storage("prepared lookup size overflow"))?;
        let metadata=total.checked_sub(values).and_then(|n|n.checked_add(lookup)).ok_or_else(||storage("prepared metadata size overflow"))?;
        state.admit_containers(metadata)
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    // Exhausting the process-wide pool must not affect unrelated parallel tests.
    fn isolated_budget_case(name: &str) -> bool {
        const FLAG: &str = "ANYSEEK_STATE_OWNERSHIP_CHILD";
        if std::env::var(FLAG).ok().as_deref() == Some(name) { return false; }
        let full = format!("record::state_memory::ownership_tests::{name}");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &full, "--nocapture"])
            .env(FLAG, name).output().unwrap();
        assert!(output.status.success(), "{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        true
    }
    #[tokio::test]
    async fn retained_snapshot_same_size_write_is_gross_admitted_and_rejection_rolls_back() {
        if isolated_budget_case("retained_snapshot_same_size_write_is_gross_admitted_and_rejection_rolls_back") { return; }
        use super::super::tests::{config,write};
        let directory=tempfile::tempdir().unwrap();let root=directory.path().join("partition");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        let first=write("first","d","alpha",ExpectedVersion::Absent);
        let receipt=p.write(first).await.unwrap();
        let old=p.inner.lock().await.state.try_clone().unwrap();
        let value=old.records.shared("d").unwrap();
        let last=Arc::downgrade(&value.0);drop(value);
        let update=write("same-size","d","bravo",ExpectedVersion::Match(1));
        let full=crate::storage::build_budget::fill_memory_for_test();
        assert!(p.write(update.clone()).await.is_err());
        {
            let live=p.inner.lock().await;
            assert_eq!(live.state.seq,receipt.seq);
            assert_eq!(live.state.operations.len(),1);
            assert!(!live.state.operation_lookup.contains_key(&update.operation_id));
            assert_eq!(live.state.records["d"].document.as_ref().unwrap()["body"],"alpha");
        }
        drop(full);
        let second=p.write(update.clone()).await.unwrap();assert_eq!(second.version,2);
        assert_eq!(p.write(update).await.unwrap(),second);
        assert_eq!(old.records["d"].version,1);
        assert_eq!(old.records["d"].document.as_ref().unwrap()["body"],"alpha");
        assert_eq!(old.operations.len(),1);
        assert!(last.upgrade().is_some());drop(old);assert!(last.upgrade().is_none());
        p.compact_journal().await.unwrap();drop(p);
        let reopened=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        assert_eq!(reopened.operation(&second_operation(&reopened).await).await.unwrap(),Some(second));
        async fn second_operation(p:&RecordPartition)->OperationId {p.inner.lock().await.state.operations[1].request.operation_id.clone()}
    }
    #[tokio::test]
    async fn old_receipt_retry_does_not_reserve_unrelated_current_body() {
        if isolated_budget_case("old_receipt_retry_does_not_reserve_unrelated_current_body") { return; }
        use super::super::tests::{config,write};
        let directory=tempfile::tempdir().unwrap();let root=directory.path().join("partition");
        let mut settings=config();settings.max_record_bytes=128*1024;settings.max_journal_bytes=8*1024*1024;
        let p=RecordPartition::create(&root,settings).await.unwrap();
        let first=write("original","d","alpha",ExpectedVersion::Absent);
        let receipt=p.write(first.clone()).await.unwrap();
        p.write(write("larger-current","d",&"x".repeat(50*1024),ExpectedVersion::Match(1))).await.unwrap();
        let held=crate::storage::build_budget::fill_memory_except_for_test(64*1024);
        assert_eq!(p.write(first).await.unwrap(),receipt);
        drop(held);assert_eq!(p.statistics().await.unwrap().visible_seq,2);
    }
    #[tokio::test]
    async fn lookup_recovery_rejects_before_allocating_keys() {
        if isolated_budget_case("lookup_recovery_rejects_before_allocating_keys") { return; }
        use super::super::tests::{config,write};
        let directory=tempfile::tempdir().unwrap();let root=directory.path().join("partition");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("a-very-long-operation-identity","d","alpha",ExpectedVersion::Absent)).await.unwrap();
        let mut state=p.inner.lock().await.state.try_clone().unwrap();
        state.operation_lookup.clear();
        let reclaim=state.lookup_bytes as u64*32;
        let charge=Arc::get_mut(state.residency.as_mut().unwrap()).unwrap();
        drop(charge.lease.split_off(reclaim).unwrap());charge.bytes-=reclaim;state.lookup_bytes=0;
        let held=crate::storage::build_budget::fill_memory_for_test();
        assert!(restore_operation_lookup(&mut state).is_err());
        assert!(state.operation_lookup.is_empty());assert_eq!(state.lookup_bytes,0);
        drop(held);restore_operation_lookup(&mut state).unwrap();assert_eq!(state.operation_lookup.len(),1);
    }

    #[test]
    fn immutable_value_preserves_wire_and_outlives_live_owner() {
        let value = serde_json::json!({"body":"繁體 alpha", "history":[1,2,3]});
        let wire = serde_json::to_vec(&value).unwrap();
        let mut admitted = reserve_memory(wire.len() as u64 * 32).unwrap();
        let mut live = Shared::from_admitted(value, &mut admitted).unwrap();
        assert_eq!(admitted.bytes(), 0);
        let old = live.clone();
        assert!(live.unique_mut().is_err(), "must not allocate implicit COW");
        assert_eq!(serde_json::to_vec(&live).unwrap(), wire);
        let owner = Arc::downgrade(&live.0);
        drop(live); assert!(owner.upgrade().is_some());
        assert_eq!(serde_json::to_vec(&old).unwrap(), wire);
        drop(old); assert!(owner.upgrade().is_none());
    }
    #[test]
    fn rejected_cow_does_not_clone_or_change_snapshot() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COPIES: AtomicUsize = AtomicUsize::new(0);
        #[derive(Serialize, Debug)]
        struct Tracked { body: String }
        impl Clone for Tracked {
            fn clone(&self) -> Self { COPIES.fetch_add(1, Ordering::SeqCst); Self { body: self.body.clone() } }
        }
        let value = Tracked { body: "retained original".into() };
        let size = checkpoint_codec::json_length(&value).unwrap() as u64 * 32;
        let mut first = reserve_memory(size).unwrap();
        let old = Shared::from_admitted(value, &mut first).unwrap();
        let wire = serde_json::to_vec(&old).unwrap();
        let mut short = reserve_memory(size - 1).unwrap();
        assert!(Shared::copy_from_admitted(&*old, &mut short).is_err());
        assert_eq!(COPIES.load(Ordering::SeqCst), 0, "rejection precedes allocation");
        let mut gross = reserve_memory(size).unwrap();
        let mut next = Shared::copy_from_admitted(&*old, &mut gross).unwrap();
        assert_eq!(COPIES.load(Ordering::SeqCst), 1);
        next.unique_mut().unwrap().body.replace_range(.., "changed content!");
        assert_eq!(serde_json::to_vec(&old).unwrap(), wire);
        assert_ne!(serde_json::to_vec(&next).unwrap(), wire);
    }
    #[test]
    fn value_transfer_rejects_before_changing_admitted_balance() {
        let value = serde_json::json!({"body":"same sized replacement"});
        let bytes = checkpoint_codec::json_length(&value).unwrap() as u64 * 32;
        let mut admitted = reserve_memory(bytes - 1).unwrap();
        assert!(Shared::from_admitted(value, &mut admitted).is_err());
        assert_eq!(admitted.bytes(), bytes - 1);
    }
}
