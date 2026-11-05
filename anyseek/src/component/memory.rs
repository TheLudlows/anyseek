//! One segment's actual retained owner and outstanding pre-WAL credits.
//! Lock order is segment -> ledger. Admission/drop take only the ledger; no
//! ledger guard may cross an await, file operation, or acquire a segment lock.
use std::sync::{Arc, Mutex};
use std::io;
use crate::storage::build_budget::{self, MemoryLease};

pub(crate) struct Admission {
    fields: Vec<String>,
    _metadata: MemoryLease,
    state: Mutex<State>,
}
struct State {
    owner: Option<MemoryLease>,
    pending: u64,
    bulk: bool,
    failed: bool,
    closed: bool,
}
pub(crate) struct Pending { admission: Arc<Admission>, bytes: u64 }
pub(crate) struct Credits {
    pub identity: MemoryLease,
    pub retained: MemoryLease,
    pub input: MemoryLease,
    pub pending: Pending,
}
fn invalid(message: &str) -> io::Error { io::Error::other(message) }
impl Admission {
    pub fn new<'a>(fields:impl Iterator<Item=&'a str>+Clone)->io::Result<Arc<Self>> {
        // This duplicate immutable projection belongs to its own admitted owner,
        // including the Arc header, Mutex/state, Vec entries and string buffers.
        let mut bytes=(std::mem::size_of::<Self>()+2*std::mem::size_of::<usize>()) as u64;
        let mut count=0usize;
        for name in fields.clone() {
            count=count.checked_add(1).ok_or_else(||invalid("component schema overflow"))?;
            bytes=bytes.checked_add(std::mem::size_of::<String>() as u64).and_then(|n|n.checked_add(name.len() as u64))
                .ok_or_else(||invalid("component schema overflow"))?;
        }
        let metadata=build_budget::reserve_memory(bytes)?;
        let mut projection=Vec::with_capacity(count);
        projection.extend(fields.map(str::to_owned));
        Ok(Arc::new(Self { fields:projection, _metadata:metadata,
            state: Mutex::new(State { owner:None,pending:0,bulk:false,failed:false,closed:false }) }))
    }
    fn lock(&self) -> io::Result<std::sync::MutexGuard<'_,State>> {
        self.state.lock().map_err(|_|invalid("component admission ledger poisoned"))
    }
    pub fn includes(&self,name:&str)->bool { self.fields.iter().any(|field|field==name) }
    pub fn install(&self,owner:MemoryLease)->io::Result<()> {
        let mut state=self.lock()?;
        // Each Arc is a generation. Never replace a generation's retained owner.
        if state.owner.is_some() || state.pending!=0 || state.failed || state.closed {
            return Err(invalid("component owner already installed or unavailable"));
        }
        build_budget::validate_single(owner.bytes().checked_add(self._metadata.bytes()).ok_or_else(||invalid("component aggregate overflow"))?)?;
        state.owner=Some(owner);Ok(())
    }
    pub fn set_bulk(&self,bulk:bool)->io::Result<()> {
        let mut state=self.lock()?;
        if state.pending!=0 || state.failed || state.closed {return Err(invalid("component bulk mode with live credits or unavailable owner"));}
        state.bulk=bulk;Ok(())
    }
    pub fn bytes(&self)->io::Result<u64> {self.lock()?.owner.as_ref().map_or(0,MemoryLease::bytes).checked_add(self._metadata.bytes()).ok_or_else(||invalid("component aggregate overflow"))}
    pub fn can_grow(&self,bytes:u64)->bool {
        self.lock().is_ok_and(|s|!s.closed&&!s.failed&&s.owner.as_ref().is_some_and(|owner|
            owner.bytes().checked_add(self._metadata.bytes()).and_then(|n|n.checked_add(s.pending)).and_then(|n|n.checked_add(bytes)).is_some_and(|n|build_budget::validate_single(n).is_ok())))
    }
    /// Ordinary/replay growth still charges the same actual global lease. The
    /// pending sum is included even when this legacy path interleaves credits.
    pub fn grow(&self,bytes:u64,skip_bulk:bool)->io::Result<()> {
        let mut state=self.lock()?;
        if state.failed||state.closed {return Err(invalid("component admission failed or closed"));}
        if skip_bulk&&state.bulk {return Ok(());}
        let pending=state.pending;
        if let Some(owner)=&mut state.owner {
            let total=owner.bytes().checked_add(self._metadata.bytes()).and_then(|n|n.checked_add(pending)).and_then(|n|n.checked_add(bytes)).ok_or_else(||invalid("component aggregate overflow"))?;
            build_budget::validate_single(total)?;owner.grow(bytes)?;
        }
        Ok(())
    }
    pub fn reserve(self:&Arc<Self>,input:u64,retained:u64,identity:u64)->io::Result<Credits> {
        let mut state=self.lock()?;
        if state.failed||state.closed||state.bulk {return Err(invalid("component admission unavailable"));}
        let owner=state.owner.as_ref().ok_or_else(||invalid("component credit missing retained owner"))?;
        let bytes=identity.checked_add(retained).ok_or_else(||invalid("component credit overflow"))?;
        let next=state.pending.checked_add(bytes).ok_or_else(||invalid("component pending overflow"))?;
        build_budget::validate_single(owner.bytes().checked_add(self._metadata.bytes()).and_then(|n|n.checked_add(next)).ok_or_else(||invalid("component aggregate overflow"))?)?;
        let identity=build_budget::reserve_memory(identity)?;
        let retained=build_budget::reserve_memory(retained)?;
        let input=build_budget::reserve_memory(input)?;
        state.pending=next;
        Ok(Credits{identity,retained,input,pending:Pending{admission:self.clone(),bytes}})
    }
    pub fn fail(&self) {self.state.lock().unwrap_or_else(|e|e.into_inner()).failed=true;}
    pub fn close(&self) {self.state.lock().unwrap_or_else(|e|e.into_inner()).closed=true;}
    #[cfg(test)]
    pub fn pending(&self)->u64 {self.state.lock().unwrap().pending}
    #[cfg(test)]
    pub fn set_pending_for_test(&self,bytes:u64) {self.state.lock().unwrap().pending=bytes;}
}
impl Pending {
    /// Merge first, then debit pending in the same critical section. Closed old
    /// tails may finish already accepted work; failed or different tails cannot.
    pub fn transfer(&mut self,target:&Arc<Admission>,lease:MemoryLease)->io::Result<()> {
        if !Arc::ptr_eq(&self.admission,target) {return Err(invalid("component credit generation mismatch"));}
        let mut state=target.lock()?;
        let bytes=lease.bytes();
        if state.failed||state.bulk||bytes>self.bytes||bytes>state.pending {return Err(invalid("component credit transfer unavailable"));}
        state.owner.as_mut().ok_or_else(||invalid("component credit missing retained owner"))?.merge(lease)?;
        state.pending-=bytes;self.bytes-=bytes;Ok(())
    }
}
impl Drop for Pending {fn drop(&mut self){
    let mut state=self.admission.state.lock().unwrap_or_else(|e|e.into_inner());
    state.pending-=self.bytes;
}}

/// Once mutation begins, errors/unwind/cancellation must fence detached
/// admission too. A successful operation explicitly disarms this guard.
pub(crate) struct Mutation { admission:Arc<Admission>, complete:bool }
impl Admission {
    pub fn mutation(self:&Arc<Self>)->Mutation {Mutation{admission:self.clone(),complete:false}}
}
impl Mutation {pub fn complete(mut self){self.complete=true;}}
impl Drop for Mutation {fn drop(&mut self){if !self.complete {self.admission.fail();}}}

#[cfg(test)]
mod tests {
    use super::*;
    fn ledger()->Arc<Admission>{
        let ledger=Admission::new(["vector","number"].into_iter()).unwrap();
        ledger.install(build_budget::reserve_memory(1024).unwrap()).unwrap();ledger
    }
    #[test]
    fn ledger_partial_transfer_cancel_and_full_memory_conserve_owners(){
        let before=build_budget::used_memory_for_test();
        let ledger=ledger();let base=ledger.bytes().unwrap();
        let mut credit=ledger.reserve(128,256,64).unwrap();
        assert_eq!(ledger.pending(),320);
        let pressure=build_budget::fill_memory_for_test();
        credit.pending.transfer(&ledger,credit.identity).unwrap();
        assert_eq!(ledger.bytes().unwrap(),base+64);assert_eq!(ledger.pending(),256);
        assert_eq!(ledger.bytes().unwrap()+ledger.pending(),base+320);
        drop(credit.retained);drop(credit.input);drop(credit.pending);
        assert_eq!(ledger.pending(),0);assert_eq!(ledger.bytes().unwrap(),base+64);
        drop(pressure);drop(ledger);
        assert_eq!(build_budget::used_memory_for_test(),before);
    }
    #[test]
    fn ledger_generation_close_failure_and_install_are_checked(){
        let first=ledger();let other=ledger();let base=first.bytes().unwrap();
        let mut credit=first.reserve(128,256,64).unwrap();
        assert!(first.set_bulk(true).is_err());
        assert!(first.install(build_budget::reserve_memory(1).unwrap()).is_err());
        assert!(credit.pending.transfer(&other,credit.identity).is_err());
        assert_eq!(first.pending(),320);assert_eq!(first.bytes().unwrap(),base);
        drop(credit.retained);drop(credit.input);drop(credit.pending);
        assert_eq!(first.pending(),0);
        let mut credit=first.reserve(128,256,64).unwrap();
        first.close();assert!(first.reserve(1,1,1).is_err());assert!(first.grow(1,false).is_err());
        credit.pending.transfer(&first,credit.identity).unwrap();
        credit.pending.transfer(&first,credit.retained).unwrap();
        drop(credit.input);drop(credit.pending);assert_eq!(first.pending(),0);
        let mut credit=other.reserve(128,256,64).unwrap();
        let mutation=other.mutation();drop(mutation); // cancelled mutation fences detached admission
        assert!(other.reserve(1,1,1).is_err());
        assert!(credit.pending.transfer(&other,credit.identity).is_err());
        drop(credit.retained);drop(credit.input);drop(credit.pending);assert_eq!(other.pending(),0);
    }
    #[test]
    fn ledger_metadata_and_pending_count_toward_single_cap_and_bulk(){
        let ledger=ledger();let base=ledger.bytes().unwrap();
        let cap=std::env::var("ANYSEEK_BUILD_MEMORY_BYTES").ok().map(|s|s.parse::<u64>().unwrap()).unwrap_or(build_budget::BUILD_MEMORY);
        // Simulate a cap-sized outstanding suffix, without allocating a corpus.
        ledger.set_pending_for_test(cap-base);
        assert!(!ledger.can_grow(1));assert!(ledger.grow(1,false).is_err());
        assert!(ledger.reserve(1,1,1).is_err());assert!(ledger.set_bulk(true).is_err());
        ledger.set_pending_for_test(0);
        ledger.set_bulk(true).unwrap();assert!(ledger.reserve(1,1,1).is_err());
        ledger.grow(1024,true).unwrap();assert_eq!(ledger.bytes().unwrap(),base);
        ledger.set_bulk(false).unwrap();ledger.grow(1,true).unwrap();assert_eq!(ledger.bytes().unwrap(),base+1);
    }
    #[test]
    fn ledger_partial_transfer_keeps_next_admission_at_exact_cap(){
        let ledger=ledger();let base=ledger.bytes().unwrap();
        let cap=std::env::var("ANYSEEK_BUILD_MEMORY_BYTES").ok().map(|s|s.parse::<u64>().unwrap()).unwrap_or(build_budget::BUILD_MEMORY);
        let mut credit=ledger.reserve(128,256,64).unwrap();
        // Simulate other pending owners to expose double-counting after identity
        // transfer without allocating a cap-sized document corpus.
        ledger.set_pending_for_test(cap-base-1);
        credit.pending.transfer(&ledger,credit.identity).unwrap();
        assert_eq!(ledger.bytes().unwrap()+ledger.pending(),cap-1);
        let next=ledger.reserve(0,0,1).unwrap();
        assert_eq!(ledger.bytes().unwrap()+ledger.pending(),cap);
        assert!(ledger.reserve(0,0,1).is_err());drop(next);
        ledger.set_pending_for_test(256);
        credit.pending.transfer(&ledger,credit.retained).unwrap();
        drop(credit.input);drop(credit.pending);assert_eq!(ledger.pending(),0);
    }
    #[test]
    fn ledger_failed_metadata_admission_and_poison_refund(){
        let before=build_budget::used_memory_for_test();
        let pressure=build_budget::fill_memory_for_test();
        assert!(Admission::new(["field"].into_iter()).is_err());
        drop(pressure);assert_eq!(build_budget::used_memory_for_test(),before);
        let ledger=ledger();
        let _=std::panic::catch_unwind(std::panic::AssertUnwindSafe(||{let _guard=ledger.state.lock().unwrap();panic!("poison ledger");}));
        assert!(ledger.reserve(1,1,1).is_err());assert!(!ledger.can_grow(1));drop(ledger);
        assert_eq!(build_budget::used_memory_for_test(),before);
    }
}
