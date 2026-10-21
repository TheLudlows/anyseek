//! Published active-version statistics for authoritative record partitions.
//! Physical posting counters remain independent because they describe encoding.
use crate::{
    add_result::{B, K},
    index::{DOCUMENT_LENGTH_COMPRESSION, FieldLengthNormalization, Shard},
};
use ahash::AHashMap;

#[derive(Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct ActiveStatistics {
    pub keys: Vec<String>,
    #[serde(skip)]
    common: Option<std::sync::Arc<std::sync::RwLock<Corpus>>>,
    footprints: AHashMap<usize, Footprint>,
    df: AHashMap<u64, u32>,
    fields: Vec<(u64, u64)>,
    shared_sum: u64,
}
/// One live scoring context shared by all immutable segments and the tail.
/// Segment-local footprints remain necessary for deletion and reconstruction;
/// the global aggregate is never serialized into every segment.
#[derive(Default)]
pub(crate) struct Corpus {
    count: usize,
    df: AHashMap<u64,u32>,
    fields: Vec<(u64,u64)>,
    sum: u64,
}
impl Corpus {
    fn insert(&mut self,terms:&[u64],fields:&[(usize,u32)]) {
        self.count+=1;
        for term in terms {*self.df.entry(*term).or_default()+=1;}
        for &(field,length) in fields {
            if self.fields.len()<=field {self.fields.resize(field+1,(0,0));}
            self.fields[field].0+=length as u64;self.fields[field].1+=u64::from(length>0);self.sum+=length as u64;
        }
    }
    fn retire(&mut self,terms:&[u64],fields:&[(usize,u32)]) {
        self.count-=1;
        for term in terms {let n=self.df.get_mut(term).expect("shared active term");*n-=1;if *n==0{self.df.remove(term);}}
        for &(field,length) in fields {self.fields[field].0-=length as u64;self.fields[field].1-=u64::from(length>0);self.sum-=length as u64;}
    }
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Footprint {
    terms: Vec<u64>,
    fields: Vec<(usize, u32)>,
}
impl ActiveStatistics {
    /// Copy local persisted contributions without sharing a mutable view context.
    pub(crate) fn clone_local(&self)->Self {
        Self{keys:self.keys.clone(),common:None,footprints:self.footprints.clone(),df:self.df.clone(),fields:self.fields.clone(),shared_sum:self.shared_sum}
    }
    /// Explicit retirement under the partition publication guard; Drop never
    /// changes a published corpus behind an in-flight query.
    pub(crate) fn detach(&mut self)->Result<(),String>{
        if let Some(common)=self.common.as_ref(){
            let mut aggregate=common.write().map_err(|_|"shared statistics poisoned")?;
            for footprint in self.footprints.values(){aggregate.retire(&footprint.terms,&footprint.fields);}
        }
        self.common=None;Ok(())
    }
    pub(crate) fn attach(&mut self,common:&std::sync::Arc<std::sync::RwLock<Corpus>>)->Result<(),String>{
        if self.common.is_some(){return Err("statistics already attached".into());}
        let mut aggregate=common.write().map_err(|_|"shared statistics poisoned")?;
        for footprint in self.footprints.values(){aggregate.insert(&footprint.terms,&footprint.fields);}
        self.common=Some(common.clone());Ok(())
    }
    fn count(&self)->usize {self.common.as_ref().map_or(self.footprints.len(),|common|common.read().expect("shared statistics poisoned").count)}
    fn document_frequency(&self,term:u64)->u32 {match &self.common {
        Some(common)=>common.read().expect("shared statistics poisoned").df.get(&term).copied().unwrap_or(0),
        None=>self.df.get(&term).copied().unwrap_or(0),
    }}
    /// Validate a persisted footprint closure without re-tokenizing payloads.
    pub(crate) fn validate(&self,field_count:usize)->Result<(),String>{
        if self.footprints.len()!=self.keys.len(){return Err("incomplete active footprints".into());}
        let mut rebuilt=Self::default();
        for (&id,footprint) in &self.footprints {
            if id>=self.keys.len() || footprint.terms.windows(2).any(|v|v[0]>=v[1])
                || footprint.fields.iter().any(|(field,_)|*field>=field_count)
                || footprint.fields.iter().map(|(field,_)|*field).collect::<std::collections::BTreeSet<_>>().len()!=footprint.fields.len(){
                return Err("invalid active footprint".into());
            }
            rebuilt.insert(id,footprint.terms.clone(),footprint.fields.clone());
        }
        if self.df!=rebuilt.df || self.fields!=rebuilt.fields || self.shared_sum!=rebuilt.shared_sum {
            return Err("inconsistent active statistics".into());
        }
        Ok(())
    }
    pub fn insert(&mut self, id: usize, mut terms: Vec<u64>, fields: Vec<(usize, u32)>) {
        terms.sort_unstable();
        terms.dedup();
        for term in &terms {
            *self.df.entry(*term).or_default() += 1;
        }
        for &(field, length) in &fields {
            if self.fields.len() <= field {
                self.fields.resize(field + 1, (0, 0));
            }
            self.fields[field].0 += length as u64;
            self.fields[field].1 += u64::from(length > 0);
            self.shared_sum += length as u64;
        }
        if let Some(common)=&self.common {common.write().expect("shared statistics poisoned").insert(&terms,&fields);}
        assert!(
            self.footprints
                .insert(id, Footprint { terms, fields })
                .is_none()
        );
    }
    pub fn retire(&mut self, id: usize) {
        if let Some(old) = self.footprints.remove(&id) {
            if let Some(common)=&self.common {common.write().expect("shared statistics poisoned").retire(&old.terms,&old.fields);}
            for term in old.terms {
                let count = self.df.get_mut(&term).expect("active term");
                *count -= 1;
                if *count == 0 {
                    self.df.remove(&term);
                }
            }
            for (field, length) in old.fields {
                self.fields[field].0 -= length as u64;
                self.fields[field].1 -= u64::from(length > 0);
                self.shared_sum -= length as u64;
            }
        }
    }
    fn average(&self, field: usize, normalization: &FieldLengthNormalization) -> f32 {
        let global=self.common.as_ref().map(|common|common.read().expect("shared statistics poisoned"));
        let (sum, count) = if let Some(global)=global.as_ref() {
            if *normalization==FieldLengthNormalization::Shared {(global.sum,global.count as u64)}else{global.fields.get(field).copied().unwrap_or_default()}
        } else if *normalization == FieldLengthNormalization::Shared {
            (self.shared_sum, self.footprints.len() as u64)
        } else {
            self.fields.get(field).copied().unwrap_or_default()
        };
        if count == 0 || sum == 0 {
            1.0
        } else {
            sum as f32 / count as f32
        }
    }
}
pub(crate) fn rank_with_keys(
    keys: Option<&[String]>,
    a: f32,
    a_id: usize,
    b: f32,
    b_id: usize,
) -> std::cmp::Ordering {
    crate::min_heap::score_ordering(a, b).then_with(|| match keys {
        Some(keys) => keys[b_id].cmp(&keys[a_id]).then_with(|| b_id.cmp(&a_id)),
        None => b_id.cmp(&a_id),
    })
}
impl Shard {
    pub(crate) fn scoring_count(&self) -> usize {
        self.active
            .as_ref()
            .map_or(self.indexed_doc_count, |a| a.count())
    }
    pub(crate) fn scoring_df(&self, hash: u64, physical: u32) -> u32 {
        self.active
            .as_ref()
            .map_or(physical, |a| a.document_frequency(hash))
    }
    pub(crate) fn active_average(&self, field: usize) -> Option<f32> {
        self.active
            .as_ref()
            .map(|a| a.average(field, &self.meta.field_length_normalization))
    }
    pub(crate) fn active_component(&self, field: usize, code: u8) -> Option<f32> {
        self.active_average(field).map(|average| {
            K * (1.0 - B + B * (DOCUMENT_LENGTH_COMPRESSION[code as usize] as f32 / average))
        })
    }
    pub(crate) fn rank(&self, a: f32, a_id: usize, b: f32, b_id: usize) -> std::cmp::Ordering {
        rank_with_keys(
            self.active.as_ref().map(|a| a.keys.as_slice()),
            a,
            a_id,
            b,
            b_id,
        )
    }
}

#[cfg(test)]
mod shared_tests {
    use super::*;
    #[test]
    fn explicit_local_copy_and_retirement_do_not_alias_corpus() {
        let common=std::sync::Arc::new(std::sync::RwLock::new(Corpus::default()));
        let mut a=ActiveStatistics::default();a.keys.push("a".into());a.insert(0,vec![1],vec![(0,2)]);
        let mut b=ActiveStatistics::default();b.keys.push("b".into());b.insert(0,vec![1,2],vec![(0,4)]);
        a.attach(&common).unwrap();b.attach(&common).unwrap();
        assert_eq!(a.count(),2);assert_eq!(b.document_frequency(1),2);
        let mut copy=a.clone_local();copy.retire(0);
        assert_eq!(a.count(),2);assert_eq!(a.document_frequency(1),2);
        a.detach().unwrap();assert_eq!(b.count(),1);assert_eq!(b.document_frequency(1),1);
        assert_eq!(a.count(),1);
        a.detach().unwrap();assert_eq!(b.count(),1);
        a.attach(&common).unwrap();assert_eq!(b.count(),2);
        assert!(a.attach(&common).is_err());assert_eq!(b.count(),2);
    }
}
