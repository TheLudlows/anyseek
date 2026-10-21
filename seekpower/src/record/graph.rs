//! Explicit same-partition declarations. Record metadata is authority; entity
//! directories and adjacency contain only rebuildable stable identities.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

/// A directed declaration, separate from the edge's evidence_refs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RelationMetadata {
    /// Exact origin version in this partition.
    pub from: EvidenceRef,
    /// Exact destination version in this partition.
    pub to: EvidenceRef,
    /// Caller-declared relation type; no inference or ontology expansion.
    pub relation_type: String,
    /// Optional finite caller weight, not an automatic retrieval score.
    #[serde(default)]
    pub weight: Option<f64>,
}
pub(super) fn endpoints(system:&EvidenceMetadata)->impl Iterator<Item=&EvidenceRef> {
    system.relation.iter().flat_map(|r| [&r.from,&r.to])
}
pub(super) fn validate_metadata(system:&EvidenceMetadata)->Result<(),RecordError> {
    if (system.kind==EvidenceKind::Relation)!=system.relation.is_some() {
        return Err(RecordError::Rejected("relation metadata requires exactly relation kind".into()));
    }
    if let Some(edge)=&system.relation {
        if edge.relation_type.is_empty() || edge.relation_type.len()>128 || edge.weight.is_some_and(|w|!w.is_finite()) {
            return Err(RecordError::Rejected("invalid relation type or finite weight".into()));
        }
        for endpoint in [&edge.from,&edge.to] {
            if endpoint.locator.is_some() || endpoint.record_id.is_empty() || endpoint.record_id.len()>256 || endpoint.routing_key.is_empty() || endpoint.routing_key.len()>256 || endpoint.version==0 {
                return Err(RecordError::Rejected("invalid exact relation endpoint".into()));
            }
        }
    }
    Ok(())
}
pub(super) fn validate_new(records:&BTreeMap<String,Record>,system:&EvidenceMetadata)->Result<(),RecordError> {
    validate_metadata(system)?;
    if system.relation.is_none(){return Ok(());}
    for endpoint in endpoints(system) {
        let target=records.get(&endpoint.record_id).ok_or_else(||RecordError::Rejected("relation endpoint not found in this collection".into()))?;
        if target.version!=endpoint.version || target.routing_key!=endpoint.routing_key {
            return Err(RecordError::Conflict("relation endpoint version or route mismatch".into()));
        }
        if target.system.as_ref().is_some_and(|s|s.kind==EvidenceKind::Relation) || !evidence::endpoint_visible(records,&endpoint.record_id)? {
            return Err(RecordError::Rejected("relation requires visible non-relation endpoints".into()));
        }
    }
    Ok(())
}
#[derive(Default)]
pub(super) struct Cache {
    entities:BTreeSet<String>,
    outgoing:BTreeMap<String,BTreeSet<(String,String)>>,
    incoming:BTreeMap<String,BTreeSet<(String,String)>>,
}
impl Cache {
    pub(super) fn rebuild(state:&State)->Result<Self,RecordError> {
        let mut cache=Self::default();for record in state.records.values(){cache.insert(record)?;}Ok(cache)
    }
    // Metadata is immutable for each identity. Retain tombstoned entries so
    // candidate accounting is deterministic across rebuilds; visibility gates
    // every use. Existing max_records bounds these retained identities.
    pub(super) fn insert(&mut self,record:&Record)->Result<(),RecordError> {
        if let Some(system)=&record.system {
            validate_metadata(system)?;
            if system.kind==EvidenceKind::Entity {self.entities.insert(record.record_id.clone());}
            if let Some(edge)=&system.relation {
                self.outgoing.entry(edge.from.record_id.clone()).or_default().insert((edge.to.record_id.clone(),record.record_id.clone()));
                self.incoming.entry(edge.to.record_id.clone()).or_default().insert((edge.from.record_id.clone(),record.record_id.clone()));
            }
        }Ok(())
    }
    pub(super) fn references(&self,id:&str)->bool {self.outgoing.contains_key(id)||self.incoming.contains_key(id)}
}
/// Direction of an explicit single hop.
#[derive(Clone,Debug,Default,Serialize,Deserialize)]
#[serde(rename_all="snake_case")]
pub enum RelationDirection {
    /// Edges whose origin is the seed.
    Out,
    /// Edges whose destination is the seed.
    In,
    /// Either direction, deduplicating self loops.
    #[default] Both,
}
/// Bounded traversal request, not a graph language.
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NeighborRequest {
    /// Unique, exact visible seed identities in caller order.
    pub seeds:Vec<EvidenceRef>,
    /// Direction relative to each seed.
    #[serde(default)] pub direction:RelationDirection,
    /// Empty accepts all declared types.
    #[serde(default)] pub relation_types:Vec<String>,
    /// Per-seed visible edge-path bound, 1..128.
    pub per_seed:usize,
    /// Total examined adjacency candidates, at most collection max_candidates.
    pub max_candidates:usize,
    /// Only explicit opt-in permits a window rather than an exact answer.
    #[serde(default)] pub allow_partial:bool,
}
/// One visible edge path. Multiple explicit edges may reach the same neighbor.
#[derive(Clone,Debug,Serialize)]
pub struct NeighborHit {
    /// Visible seed identity from the request.
    pub seed_id:String,
    /// Current visible edge, with original metadata/body.
    pub edge:Record,
    /// Current visible endpoint record.
    pub neighbor:Record,
}
/// A common-view bounded expansion. Count is visible paths, not unique nodes.
#[derive(Clone,Debug,Serialize)]
pub struct NeighborPage {
    /// Ordered visible edge paths.
    pub hits:Vec<NeighborHit>,
    /// True only if the complete requested neighborhood was examined.
    pub complete:bool,
    /// None on exact results; otherwise the limiting resource.
    pub reason:Option<String>,
    /// Visible path count; relation is exact or window, never estimated.
    pub count:RelationCount,
    /// Common authoritative position.
    pub visible_seq:u64,
    /// Fixed trusted evaluation time in UTC Unix milliseconds.
    pub evaluation_time_ms:u64,
    /// Same effective corpus as all other retrieval channels.
    pub statistics:RecordStatistics,
}
/// Honest count interpretation for one-hop results.
#[derive(Clone,Debug,Serialize)]
pub struct RelationCount {
    /// Number of returned visible edge paths.
    pub value:usize,
    /// exact or window.
    pub relation:String,
}
impl RecordPartition {
    /// Expand only current exact visible seeds under a single partition view.
    pub async fn neighbors(&self,request:NeighborRequest)->Result<NeighborPage,RecordError> {
        self.advance_time().await?;
        let _admission=self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let inner=self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some(){return Err(RecordError::RecoveryRequired);}
        if request.seeds.is_empty() || request.seeds.len()>32 || !(1..=128).contains(&request.per_seed) || request.max_candidates==0 || request.max_candidates>inner.config.max_candidates || request.relation_types.len()>16 || request.relation_types.iter().any(|t|t.is_empty()||t.len()>128) {
            return Err(RecordError::Rejected("invalid one-hop resource bounds".into()));
        }
        let mut seen=BTreeSet::new();
        for seed in &request.seeds {
            if seed.locator.is_some() {return Err(RecordError::Rejected("relation seeds cannot contain locators".into()));}
            if !seen.insert(&seed.record_id) {return Err(RecordError::Rejected("duplicate relation seed".into()));}
            let record=inner.state.records.get(&seed.record_id).filter(|r|inner.locations.contains_key(&r.record_id));
            if record.is_none_or(|r|r.version!=seed.version || r.routing_key!=seed.routing_key || r.system.as_ref().is_some_and(|s|s.kind==EvidenceKind::Relation)) {
                return Err(RecordError::Conflict("seed is not a current visible non-relation version".into()));
            }
        }
        let mut hits=Vec::new();let mut scanned=0usize;let mut bytes=0usize;let mut reason=None;
        'seeds: for seed in &request.seeds {
            // Merge sorted adjacency lazily: a small candidate budget must not
            // first allocate or scan the complete neighborhood. Equal entries
            // are self loops and are consumed once.
            let mut outgoing=inner.graph_cache.outgoing.get(&seed.record_id).filter(|_|!matches!(request.direction,RelationDirection::In)).into_iter().flatten().peekable();
            let mut incoming=inner.graph_cache.incoming.get(&seed.record_id).filter(|_|!matches!(request.direction,RelationDirection::Out)).into_iter().flatten().peekable();
            let mut count=0;
            loop {
                let candidate=match (outgoing.peek(),incoming.peek()) {
                    (Some(a),Some(b)) => match a.cmp(b) {
                        std::cmp::Ordering::Less=>outgoing.next(),
                        std::cmp::Ordering::Greater=>incoming.next(),
                        std::cmp::Ordering::Equal=>{incoming.next();outgoing.next()}
                    },
                    (Some(_),None)=>outgoing.next(),
                    (None,Some(_))=>incoming.next(),
                    (None,None)=>None,
                };
                let Some((neighbor_id,edge_id))=candidate else {break;};
                if scanned==request.max_candidates {reason=Some("candidate_budget".into());break 'seeds;}scanned+=1;
                if !inner.locations.contains_key(edge_id)||!inner.locations.contains_key(neighbor_id){continue;}
                let edge=&inner.state.records[edge_id];let metadata=edge.system.as_ref().and_then(|s|s.relation.as_ref()).ok_or_else(||storage("invalid adjacency cache"))?;
                if !request.relation_types.is_empty()&&!request.relation_types.contains(&metadata.relation_type){continue;}
                if count==request.per_seed {reason=Some("per_seed_limit".into());break 'seeds;}
                let hit=NeighborHit{seed_id:seed.record_id.clone(),edge:edge.clone(),neighbor:inner.state.records[neighbor_id].clone()};
                let size=serde_json::to_vec(&hit).map_err(storage)?.len();
                if bytes.saturating_add(size)>4*1024*1024 {reason=Some("response_budget".into());break 'seeds;}
                bytes+=size;hits.push(hit);count+=1;
            }
        }
        if reason.is_some()&&!request.allow_partial {return Err(RecordError::Rejected("exact one-hop query exceeds budget; explicitly allow_partial or narrow query".into()));}
        Ok(NeighborPage{count:RelationCount{value:hits.len(),relation:if reason.is_none(){"exact"}else{"window"}.into()},hits,complete:reason.is_none(),reason,visible_seq:inner.state.seq,evaluation_time_ms:inner.evaluation_time_ms,statistics:inner.statistics()})
    }
    /// Stable-ID entity directory; a changed view invalidates ordinary paging.
    pub async fn entities(&self,cursor:Option<RecordCursor>,length:usize)->Result<RecordPage,RecordError> {
        self.advance_time().await?;
        let _admission=self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let inner=self.inner.lock().await;
        if !inner.healthy || inner.state.sealed_for.is_some(){return Err(RecordError::RecoveryRequired);}
        if length==0||length>1000{return Err(RecordError::Rejected("entity page length must be 1..1000".into()));}
        if cursor.as_ref().is_some_and(|c|c.visible_seq!=inner.state.seq){return Err(RecordError::Conflict("entity directory view changed".into()));}
        let mut rows=inner.graph_cache.entities.iter().filter(|id|cursor.as_ref().is_none_or(|c|id.as_str()>c.after.as_str())&&inner.locations.contains_key(*id));
        let mut records=Vec::new();let mut bytes=0usize;let mut more=false;
        for id in rows.by_ref().take(length+1) {
            let record=&inner.state.records[id];let size=serde_json::to_vec(record).map_err(storage)?.len();
            if records.len()==length || bytes.saturating_add(size)>4*1024*1024 {
                if records.is_empty(){return Err(RecordError::Rejected("entity exceeds page response budget".into()));}
                more=true;break;
            }
            bytes+=size;records.push(record.clone());
        }
        let next=if more{Some(RecordCursor{visible_seq:inner.state.seq,after:records.last().unwrap().record_id.clone()})}else{None};
        Ok(RecordPage{records,next,visible_seq:inner.state.seq,evaluation_time_ms:inner.evaluation_time_ms,statistics:inner.statistics()})
    }
}

pub(super) fn invalidates(state:&State,request:&RecordWrite)->bool {
    request.system.as_ref().or_else(||state.records.get(&request.record_id).and_then(|r|r.system.as_ref())).is_some_and(|s|matches!(s.kind,EvidenceKind::Entity|EvidenceKind::Relation))
    || (request.vector_patch.is_none() && state.records.values().any(|r|r.system.as_ref().is_some_and(|s|endpoints(s).any(|e|e.record_id==request.record_id))))
}
