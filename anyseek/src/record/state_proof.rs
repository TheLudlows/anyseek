//! Attempt-local equality proof, not a new persistent digest or authority.
//! Immutable shared values may prove equality by ownership identity. Independently
//! replayed values retain canonical SHA comparison; order and all metadata matter.
use super::*;
use serde::ser::SerializeStruct;

struct Metadata<'a>(&'a State);
impl Serialize for Metadata<'_> {
    fn serialize<S:serde::Serializer>(&self,serializer:S)->Result<S::Ok,S::Error> {
        // Exhaustive destructuring makes adding any State field a compile-time
        // obligation here. Exactly the three serde-skipped fields are excluded.
        let State {residency:_,lineage,feed,sealed_for,takeover_proof,
            independent_snapshot,seq,epoch_watermark,time_watermark_ms,retired_operation_prefix,expirations,
            lease_expirations,private_cleanup_pending,evidence_quarantined,
            records:_,source_revisions:_,generations,operations:_,
            operation_lookup:_,lookup_bytes:_}=self.0;
        let mut s=serializer.serialize_struct("State",16+usize::from(retired_operation_prefix.is_some()))?;
        macro_rules! field {($name:ident)=>{s.serialize_field(stringify!($name),$name)?};}
        field!(lineage);field!(feed);field!(sealed_for);field!(takeover_proof);
        field!(independent_snapshot);field!(seq);field!(epoch_watermark);field!(time_watermark_ms);
        if retired_operation_prefix.is_some() {field!(retired_operation_prefix);}
        field!(expirations);field!(lease_expirations);field!(private_cleanup_pending);field!(evidence_quarantined);
        s.serialize_field("records",&())?;s.serialize_field("source_revisions",&())?;
        field!(generations);s.serialize_field("operations",&())?;s.end()
    }
}

/// Exact current wire length, including escaped keys and every delimiter.
/// Shared value lengths were measured before admission and cannot mutate.
pub(super) fn length(state:&State)->Result<usize,RecordError> {
    let records=state.records.wire_length()?;
    let revisions=state.source_revisions.wire_length()?;
    let operations=state.operations.wire_length()?;
    checkpoint_codec::json_length(&Metadata(state))?.checked_sub(12)
        .and_then(|n|n.checked_add(records))
        .and_then(|n|n.checked_add(revisions))
        .and_then(|n|n.checked_add(operations))
        .ok_or_else(||storage("State proof length overflow"))
}
/// Both inputs must remain borrowed throughout comparison. There is no seq-only
/// cache and no inference from on-disk names, mtime, or file hashes.
pub(super) fn verify(config:&RecordConfig,expected:&State,replayed:&State)->Result<(),RecordError> {
    if length(expected)?!=length(replayed)?
        || checkpoint_codec::json_digest(&Metadata(expected))?!=checkpoint_codec::json_digest(&Metadata(replayed))?
        || !expected.records.same_values(&replayed.records)?
        || !expected.source_revisions.same_values(&replayed.source_revisions)?
        || !expected.operations.same_values(&replayed.operations)? {
        return Err(storage("replayed State differs from live State"));
    }
    validate_restored_state(config,replayed)?;
    for state in [expected,replayed] {
        let lookup_bytes=state.operations.iter().try_fold(0usize,|n,op|n.checked_add(checkpoint_codec::json_length(&op.request.operation_id)?).ok_or_else(||storage("lookup proof overflow")))?;
        if state.lookup_bytes!=lookup_bytes {return Err(storage("replayed State lookup accounting mismatch"));}
        if state.operation_lookup.len()!=state.operations.len()
            || state.operations.iter().enumerate().any(|(i,op)|state.operation_lookup.get(&op.request.operation_id)!=Some(&i))
            || state.records.iter().any(|(id,r)|id!=&r.record_id) {
            return Err(storage("replayed State identity mismatch"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config,write};
    #[tokio::test]
    async fn shared_proof_matches_wire_and_detects_independent_same_size_changes() {
        let root=tempfile::tempdir().unwrap();let settings=config();
        let p=RecordPartition::create(&root.path().join("partition"),settings.clone()).await.unwrap();
        for (id,body) in [("a\\\"中","alpha 中文"),("b","beta")]{
            p.write(write(id,id,body,ExpectedVersion::Absent)).await.unwrap();
        }
        let inner=p.inner.lock().await;let state=&inner.state;
        assert_eq!(length(state).unwrap(),checkpoint_codec::json_length(state).unwrap());
        let mut metadata_oracle=serde_json::to_value(state).unwrap();
        for name in ["records","source_revisions","operations"] {metadata_oracle[name]=serde_json::Value::Null;}
        assert_eq!(serde_json::to_value(Metadata(state)).unwrap(),metadata_oracle,"metadata projection must follow every State serde attribute");
        let shared=state.try_clone().unwrap();verify(&settings,state,&shared).unwrap();
        let bytes=serde_json::to_vec(state).unwrap();
        let mut independent:State=serde_json::from_slice(&bytes).unwrap();restore_operation_lookup(&mut independent).unwrap();
        verify(&settings,state,&independent).unwrap();
        assert_eq!(length(&independent).unwrap(),bytes.len());
        independent.records.edit("b",0,|r|{r.document.as_mut().unwrap().insert("body".into(),serde_json::json!("zeta"));Ok(())}).unwrap();
        assert_eq!(length(&independent).unwrap(),bytes.len(),"same-size mutation must not bypass proof");
        assert!(verify(&settings,state,&independent).is_err());
    }
    #[tokio::test]
    async fn proof_preserves_order_same_sequence_metadata_and_lookup() {
        let root=tempfile::tempdir().unwrap();let settings=config();
        let p=RecordPartition::create(&root.path().join("partition"),settings.clone()).await.unwrap();
        for id in ["a","b"] {p.write(write(id,id,"alpha",ExpectedVersion::Absent)).await.unwrap();}
        let inner=p.inner.lock().await;let state=&inner.state;
        let mut other=state.try_clone().unwrap();other.epoch_watermark=1;
        assert_eq!(length(&other).unwrap(),checkpoint_codec::json_length(&other).unwrap());
        assert!(verify(&settings,state,&other).is_err());
        let mut other=state.try_clone().unwrap();other.operation_lookup.clear();
        assert!(verify(&settings,state,&other).is_err());
        let mut other=state.try_clone().unwrap();let mut reversed=state_values::Values::default();
        for op in state.operations.iter().rev(){reversed.push(op.clone()).unwrap();}
        other.operations=reversed;assert!(verify(&settings,state,&other).is_err());
        // Length/ordered-value proof is independent of source-domain validity;
        // use a retained Record value without publishing fabricated source history.
        let mut history=state.try_clone().unwrap();history.source_revisions.push(state.records["a"].clone()).unwrap();
        assert_eq!(length(&history).unwrap(),checkpoint_codec::json_length(&history).unwrap());
        let mut changed=history.try_clone().unwrap();
        changed.source_revisions.edit(0,0,|r|{r.document.as_mut().unwrap().insert("body".into(),serde_json::json!("omega"));Ok(())}).unwrap();
        assert_eq!(length(&changed).unwrap(),checkpoint_codec::json_length(&changed).unwrap());
        assert_eq!(length(&history).unwrap(),length(&changed).unwrap());
        assert!(!history.source_revisions.same_values(&changed.source_revisions).unwrap());
        let empty=State::default();assert_eq!(length(&empty).unwrap(),checkpoint_codec::json_length(&empty).unwrap());
    }
}
