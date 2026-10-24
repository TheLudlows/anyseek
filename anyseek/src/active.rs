//! Exact live ordinal membership for Record vector/metadata components.
//! Lexical corpus statistics live exclusively in the native fixed-searcher view.
use std::collections::BTreeSet;
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActiveStatistics {
    pub keys: Vec<String>,
    live: BTreeSet<usize>,
}
impl ActiveStatistics {
    pub(crate) fn clone_local(&self)->Self {self.clone()}
    pub(crate) fn is_live(&self,id:usize)->bool {self.live.contains(&id)}
    pub(crate) fn publish(&mut self,id:usize) {assert!(id<self.keys.len());assert!(self.live.insert(id));}
    pub(crate) fn retire(&mut self,id:usize) {self.live.remove(&id);}
    pub(crate) fn validate_sparse(&self,_fields:usize)->Result<(),String>{
        if self.live.iter().any(|id|*id>=self.keys.len()) {return Err("active ordinal outside component".into());}
        let mut live_keys=BTreeSet::new();
        if self.live.iter().any(|id|!live_keys.insert(&self.keys[*id])) {return Err("duplicate active identity".into());}
        Ok(())
    }
}
pub(crate) fn rank_with_keys(keys:Option<&[String]>,a:f32,a_id:usize,b:f32,b_id:usize)->std::cmp::Ordering {
    crate::min_heap::score_ordering(a,b).then_with(||match keys {Some(keys)=>keys[b_id].cmp(&keys[a_id]),None=>b_id.cmp(&a_id)})
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn copied_membership_and_retirement_do_not_change_an_old_view(){
        let mut current=ActiveStatistics::default();current.keys.push("a".into());current.publish(0);
        let old=current.clone_local();current.retire(0);assert!(old.is_live(0));assert!(!current.is_live(0));
        current.keys.push("a".into());current.publish(1);assert!(current.validate_sparse(0).is_ok());
    }
}
