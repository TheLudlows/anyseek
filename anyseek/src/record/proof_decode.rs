//! Proof decoding keeps admission outside serde's untyped custom-error boundary.
//! The caller owns the normal frame scratch lease until decoding/replay completes.
//! Retained values acquire exactly the same independent charges as normal decode.
use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireState {
    lineage:String,
    history:history::HistoryState,
    independent_snapshot:bool,
    seq:u64,
    epoch_watermark:u64,
    time_watermark_ms:u64,
    #[serde(default)]
    retired_operation_prefix:Option<temporal::RetiredPrefix>,
    expirations:Vec<temporal::Expiration>,
    records:BTreeMap<String,Record>,
    operations:Vec<Applied>,
}
impl WireState {
    fn admit(self)->Result<State,RecordError>{
        let Self{lineage,history,independent_snapshot,seq,epoch_watermark,time_watermark_ms,
            retired_operation_prefix,expirations,records,operations}=self;
        let mut admitted_records=state_values::Records::default();
        for (id,record) in records {admitted_records.insert(id,record)?;}
        let mut admitted_operations=state_values::Values::default();
        for operation in operations {admitted_operations.push(operation)?;}
        // Exhaustive initializer: a new State field must be considered here.
        Ok(State{residency:None,lineage,history,independent_snapshot,seq,
            epoch_watermark,time_watermark_ms,retired_operation_prefix,expirations,
            records:admitted_records,operations:admitted_operations,
            operation_lookup:BTreeMap::new(),lookup_bytes:0})
    }
}
#[derive(Deserialize)]
enum Bootstrap {
    #[serde(rename="RecordCheckpointV19")]
    Checkpoint{config:RecordConfig,state:WireState},
}
pub(super) fn bootstrap(bytes:&[u8])->Result<(RecordConfig,State),RecordError>{
    let Bootstrap::Checkpoint{config,state}=serde_json::from_slice(bytes).map_err(storage)?;
    Ok((config,state.admit()?))
}
#[derive(Deserialize)]
enum Mutation {
    #[serde(rename="RecordAppliedV16")]
    Applied(Applied),
    #[serde(rename="RecordExpiredV13")]
    Expired(temporal::Expiration),
    #[serde(rename="RecordHistoryTrimV14")]
    HistoryTrim(history::Trim),
}
pub(super) fn mutation(bytes:&[u8])->Result<Entry,RecordError>{
    Ok(match serde_json::from_slice(bytes).map_err(storage)? {
        Mutation::Applied(op)=>Entry::Applied(op),
        Mutation::Expired(event)=>Entry::Expired(event),
        Mutation::HistoryTrim(event)=>Entry::HistoryTrim(event),
    })
}
#[derive(Deserialize)]
enum Certificate<'a> {
    #[serde(rename="RecordAnchorV5")]
    Anchor(#[serde(borrow)] &'a serde_json::value::RawValue),
}
pub(super) fn anchor(bytes:&[u8])->Result<anchor::Reference,RecordError>{
    let Certificate::Anchor(raw)=serde_json::from_slice(bytes).map_err(storage)?;
    anchor::Reference::decode_proof(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn parity(bytes:&[u8])->bool {
        let old=serde_json::from_slice::<Entry>(bytes);
        let new=bootstrap(bytes);
        match (old,new) {
            (Ok(Entry::Checkpoint{config:a,state:a_state}),Ok((b,b_state)))=>{
                assert_eq!(checkpoint_codec::json_digest(&a).unwrap(),checkpoint_codec::json_digest(&b).unwrap());
                assert_eq!(checkpoint_codec::json_digest(&a_state).unwrap(),checkpoint_codec::json_digest(&b_state).unwrap());true
            },
            (Err(_),Err(_))=>false,
            _=>panic!("proof bootstrap parser diverged from persisted Entry"),
        }
    }
    #[tokio::test]
    async fn proof_wire_parity_preserves_defaults_duplicates_unknowns_and_canonical_anchor() {
        use super::super::tests::{config,write};
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        p.write(write("op中","a\\\"中","alpha 中文",ExpectedVersion::Absent)).await.unwrap();
        let state=p.inner.lock().await.state.try_clone().unwrap();
        let bytes=encode_entry(&Entry::Checkpoint{config:config(),state}).unwrap();
        assert!(parity(&bytes));
        let mut value:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
        value["RecordCheckpointV19"]["state"].as_object_mut().unwrap().remove("retired_operation_prefix");
        assert!(parity(&serde_json::to_vec_pretty(&value).unwrap()));
        let missing_default=serde_json::to_vec(&value).unwrap();
        // Maps historically accept duplicate record keys with last value wins;
        // preserve that behavior rather than silently inventing stricter wire rules.
        let records=&value["RecordCheckpointV19"]["state"]["records"];
        let (id,record)=records.as_object().unwrap().iter().next().unwrap();
        let key=serde_json::to_string(id).unwrap();let first=serde_json::to_string(record).unwrap();
        let mut changed=record.clone();changed["document"]["body"]="last duplicate value".into();
        let duplicate=format!("{{{key}:{first},{key}:{}}}",serde_json::to_string(&changed).unwrap());
        let ordinary=serde_json::to_string(records).unwrap();
        let duplicate_wire=String::from_utf8(missing_default.clone()).unwrap().replacen(&format!("\"records\":{ordinary}"),&format!("\"records\":{duplicate}"),1);
        assert!(parity(duplicate_wire.as_bytes()));
        value["RecordCheckpointV19"]["state"]["unexpected"]=true.into();
        assert!(!parity(&serde_json::to_vec(&value).unwrap()));
        assert!(!parity(&bytes[..bytes.len()-1]));
        let text=String::from_utf8(missing_default).unwrap();
        assert!(!parity(text.replacen("\"seq\":1","\"seq\":1,\"seq\":1",1).as_bytes()));
        assert!(!parity(format!("{text} trailing").as_bytes()));
        assert!(mutation(&bytes).is_err());
        p.compact_journal().await.unwrap();
        let reference=p.inner.lock().await.anchor.clone().unwrap();
        let frame=encode_entry(&Entry::Anchor(reference.clone())).unwrap();
        let reordered=serde_json::to_vec_pretty(&serde_json::from_slice::<serde_json::Value>(&frame).unwrap()).unwrap();
        assert_eq!(checkpoint_codec::json_digest(&anchor(&reordered).unwrap()).unwrap(),checkpoint_codec::json_digest(&reference).unwrap());
        assert!(bootstrap(&frame).is_err());assert!(mutation(&frame).is_err());
        let mut altered:serde_json::Value=serde_json::from_slice(&frame).unwrap();
        altered["RecordAnchorV5"]["unexpected"]=true.into();
        assert!(anchor(&serde_json::to_vec(&altered).unwrap()).is_err());
        altered["RecordAnchorV5"].as_object_mut().unwrap().remove("unexpected");
        altered["RecordAnchorV5"]["id"]="invalid/path".into();
        assert!(anchor(&serde_json::to_vec(&altered).unwrap()).is_err());
        p.finish_service_shutdown().await.unwrap();
    }
}
