//! Immutable analyzed rows shared by the visible delta and its materializer.
use super::*;
use super::super::{Document, RecordConfig};
use crate::storage::build_budget::MemoryLease;

/// Both publication routes own all explicit native progress memory before WAL.
/// Fallback skips the retained analyzed visible overlay, not native admission.
pub(crate) enum Admission { Visible(Reservation), Fallback(Fallback) }
pub(crate) struct Fallback {
    pub(super) pending:Pending,
    pub(super) workspace:super::runtime::Escrow,
}

pub(crate) struct Reservation {
    pub bytes: u64,
    _memory: MemoryLease,
    _scope: Option<crate::resources::Lease>,
    pub(super) capacity:Option<Capacity>,
    pub(super) analysis_bytes:u64,
    pub(super) id_bytes:u64,
    pub(super) workspace:std::sync::Mutex<Option<super::runtime::Escrow>>,
    pub(super) pending:Option<Pending>,
}
impl Reservation {
    pub(crate) fn new(document: &Document, config: &RecordConfig, id: &str) -> Result<Self,RecordError> {
        let analysis_bytes=crate::record::build_budget::document_estimate(document,&config.schema)?;
        let bytes=analysis_bytes
            .checked_add(65536).and_then(|n|n.checked_add(id.len() as u64*3))
            .ok_or_else(||storage("visible lexical reservation overflow"))?;
        crate::storage::build_budget::validate_single(bytes).map_err(storage)?;
        let memory=crate::storage::build_budget::reserve_memory(bytes).map_err(crate::record::memory_error)?;
        let scope=crate::resources::Scope::current().map(|s|s.retained_io().bytes(bytes)).transpose().map_err(|error|if error.is_recoverable(){RecordError::Busy}else{storage(error)})?;
        Ok(Self{bytes,_memory:memory,_scope:scope,capacity:None,analysis_bytes,id_bytes:id.len() as u64,workspace:Default::default(),pending:None})
    }
}
pub(crate) struct Row {
    pub identity: Arc<Identity>,
    pub lengths: Vec<u64>,
    pub postings: Vec<BTreeMap<String,Vec<u32>>>,
    pub document: Document,
    pub reservation: Reservation,
}
impl Row {
    pub(super) async fn analyze(identity: Identity, document: &Document, config: &RecordConfig, analyzer: &super::query::Analyzer, reservation: Reservation)->Result<Arc<Self>,RecordError>{
        let mut lengths=Vec::new();let mut postings=Vec::new();let mut retained=Document::new();
        for field in config.schema.iter().filter(|f|f.index_lexical) {
            let text=match document.get(&field.field){
                None=>String::new(),
                Some(value) if field.field_type==crate::index::FieldType::Json && value.is_object()=>{let mut strings=Vec::new();crate::index::object_values_to_string_vec_recursive(value,&mut strings);strings.join(" ")},
                Some(value)=>value.as_str().map(str::to_owned).unwrap_or_else(||value.to_string()),
            };
            if let Some(value)=document.get(&field.field){retained.insert(field.field.clone(),value.clone());}
            let field=analyzer.field(&text).await;lengths.push(u64::from(field.length));
            let mut terms:BTreeMap<String,Vec<u32>>=BTreeMap::new();
            for token in field.tokens {terms.entry(token.text).or_default().push(token.position as u32);}
            postings.push(terms);
        }
        Ok(Arc::new(Self{identity:Arc::new(identity),lengths,postings,document:retained,reservation}))
    }
    pub(super) fn positions(&self,fields:&[Field],field:Field,term:&str)->&[u32]{
        fields.iter().position(|f|*f==field).and_then(|i|self.postings[i].get(term)).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// Pointer-only snapshot copies; potentially long record IDs remain shared.
#[derive(Clone,Debug,PartialEq,Eq)]
pub(in crate::record) struct Identities(pub Vec<Arc<Identity>>);
impl From<Vec<Identity>> for Identities {fn from(v:Vec<Identity>)->Self{Self(v.into_iter().map(Arc::new).collect())}}
impl Identities {
    pub fn len(&self)->usize{self.0.len()}
    pub fn iter(&self)->impl Iterator<Item=&Identity>{self.0.iter().map(Arc::as_ref)}
}
impl std::ops::Index<usize> for Identities{type Output=Identity;fn index(&self,n:usize)->&Identity{&self.0[n]}}

pub(super) struct Capacity {pub counter:Arc<std::sync::atomic::AtomicU64>,pub bytes:u64}
impl Drop for Capacity {fn drop(&mut self){self.counter.fetch_sub(self.bytes,std::sync::atomic::Ordering::AcqRel);}}

impl<const N:usize> PartialEq<[Identity;N]> for Identities {fn eq(&self,other:&[Identity;N])->bool{self.iter().eq(other.iter())}}

/// Independent from Live: releasing a cancelled plan or pinned row never reenters
/// the publication lock. Live -> pending is the only nested lock order.
#[derive(Default)]
pub(super) struct PendingCounts {pub rows:u64,pub id_bytes:u64}
pub(super) struct Pending {pub counts:Arc<std::sync::Mutex<PendingCounts>>,pub id_bytes:u64}
impl Drop for Pending {fn drop(&mut self){
    // Preserve accounting even if a prior panic poisoned this bookkeeping lock.
    let mut counts=self.counts.lock().unwrap_or_else(|error|error.into_inner());
    counts.rows-=1;counts.id_bytes-=self.id_bytes;
}}
