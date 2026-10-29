//! Canonical state references for authenticated payload pools. An anchor
//! selects the complete closure; isolated blocks never publish authority.
use super::*;
use serde_json::Value;

// Enumerate only engine-owned payload positions. Never recurse into user JSON:
// a user field called document/generation/$payload has no special meaning.
pub(super) fn visit(state: &mut Value, mut body: impl FnMut(&mut Value)->Result<(),RecordError>)->Result<(),RecordError> {
    fn record(value:&mut Value, body:&mut impl FnMut(&mut Value)->Result<(),RecordError>)->Result<(),RecordError>{
        if let Some(document)=value.get_mut("document") {if !document.is_null(){body(document)?;}}
        Ok(())
    }
    fn chunks(value:&mut Value, body:&mut impl FnMut(&mut Value)->Result<(),RecordError>)->Result<(),RecordError>{
        if let Some(chunks)=value.get_mut("chunks").and_then(Value::as_array_mut){for chunk in chunks {record(chunk,body)?;}}
        Ok(())
    }
    if let Some(records)=state.get_mut("records").and_then(Value::as_object_mut){for value in records.values_mut(){record(value,&mut body)?;}}
    if let Some(history)=state.get_mut("source_revisions").and_then(Value::as_array_mut){for value in history{record(value,&mut body)?;}}
    if let Some(operations)=state.get_mut("operations").and_then(Value::as_array_mut){for op in operations{
        let request=op.get_mut("request").ok_or_else(||storage("missing prepared request"))?;
        record(request,&mut body)?;
        if let Some(patch)=request.get_mut("vector_patch"){if !patch.is_null(){body(patch)?;}}
        if let Some(generation)=request.get_mut("generation"){chunks(generation,&mut body)?;}
    }}
    if let Some(generations)=state.get_mut("generations").and_then(Value::as_object_mut){for manifest in generations.values_mut(){
        if let Some(staged)=manifest.get_mut("staged"){chunks(staged,&mut body)?;}
    }}
    Ok(())
}
// Count encoded metadata without allocating another full JSON buffer.
#[cfg(test)]
fn encoded_length(value:&Value,limit:u64)->Result<u64,RecordError> {
    struct Counter {bytes:u64,limit:u64}
    impl std::io::Write for Counter {
        fn write(&mut self,bytes:&[u8])->std::io::Result<usize>{
            self.bytes=self.bytes.checked_add(bytes.len() as u64).filter(|n|*n<=self.limit)
                .ok_or_else(||std::io::Error::other("prepared state budget exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self)->std::io::Result<()>{Ok(())}
    }
    let mut counter=Counter{bytes:0,limit};
    serde_json::to_writer(&mut counter,value).map_err(storage)?;Ok(counter.bytes)
}
#[cfg(test)]
fn payload_limit(config:&RecordConfig)->Result<usize,RecordError>{
    config.max_records.checked_add(config.max_operations).and_then(|n|n.checked_mul(102)).ok_or_else(||storage("payload budget overflow"))
}
#[cfg(test)]
fn hydrate(config:RecordConfig,mut wire:Value,max_bytes:u64,mut read:impl FnMut(&Value)->Result<Vec<u8>,RecordError>)->Result<(RecordConfig,State),RecordError>{
    validate_config(&config)?;
    let max_bytes=max_bytes.min(config.max_journal_bytes);
    let max_payloads=payload_limit(&config)?;
    let mut expanded=encoded_length(&wire,max_bytes)?;
    let mut references=0usize;
    visit(&mut wire,|value|{
        references=references.checked_add(1).filter(|n|*n<=max_payloads).ok_or_else(||storage("payload reference budget exceeded"))?;
        let object=value.as_object().ok_or_else(||storage("invalid payload reference"))?;
        if object.len()!=1{return Err(storage("invalid payload reference fields"));}
        let bytes=read(object.get("$payload").ok_or_else(||storage("missing payload reference"))?)?;
        if bytes.len()>config.max_record_bytes{return Err(storage("payload body budget exceeded"));}
        expanded=expanded.checked_sub(encoded_length(value,max_bytes)?).and_then(|n|n.checked_add(bytes.len() as u64))
            .filter(|n|*n<=max_bytes).ok_or_else(||storage("expanded payload budget exceeded"))?;
        *value=serde_json::from_slice(&bytes).map_err(storage)?;Ok(())
    })?;
    let mut state=serde_json::from_value(wire).map_err(storage)?;
    restore_operation_lookup(&mut state)?;validate_restored_state(&config,&state)?;
    Ok((config,state))
}

/// State anchor with stable cross-object references. The authenticated manifest
/// supplies its payload closure; paths are never read from arbitrary user JSON.
#[cfg(test)]
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReferencedState {config:RecordConfig,state:Value}
#[cfg(test)]
impl ReferencedState {
    pub(super) fn write(config:&RecordConfig,state:&State,pool:&mut crate::storage::payload_pool::PoolBuilder)->Result<Self,RecordError>{
        validate_config(config)?;
        let mut wire=serde_json::to_value(state).map_err(storage)?;
        encoded_length(&wire,config.max_journal_bytes)?;
        visit(&mut wire,|value|{
            let bytes=serde_json::to_vec(value).map_err(storage)?;
            if bytes.len()>config.max_record_bytes{return Err(storage("payload body budget exceeded"));}
            *value=serde_json::json!({"$payload":pool.intern(&bytes).map_err(storage)?});Ok(())
        })?;
        encoded_length(&wire,config.max_journal_bytes)?;
        Ok(Self{config:config.clone(),state:wire})
    }
    pub(super) fn read(self,pool:&crate::storage::payload_pool::PoolReader,max_bytes:u64)->Result<(RecordConfig,State),RecordError>{
        let limit=self.config.max_record_bytes.min(usize::try_from(max_bytes.min(self.config.max_journal_bytes)).map_err(storage)?);
        hydrate(self.config,self.state,max_bytes,|value|{
            let reference=serde_json::from_value(value.clone()).map_err(storage)?;
            pool.read_limited(&reference,limit).map_err(storage)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::payload_pool::{Budget, Closure, PoolBuilder, PoolReader};

    fn prepare(root: &Path, config: &RecordConfig, state: &State) -> (ReferencedState, PoolReader, Closure, PathBuf) {
        let directory=root.join("private-payload");std::fs::create_dir(&directory).unwrap();
        let budget=Budget {bytes:config.max_journal_bytes,objects:4,payloads:payload_limit(config).unwrap(),body_bytes:config.max_record_bytes};
        let id=format!("{:032x}",rand::random::<u128>());
        let mut writer=PoolBuilder::begin(&directory,Closure::default(),budget,id.clone()).unwrap();
        let prepared=ReferencedState::write(config,state,&mut writer).unwrap();
        let closure=writer.finish().unwrap();
        let reader=PoolReader::open(&directory,closure.clone(),budget).unwrap();
        (prepared,reader,closure,directory.join(format!("payload-{id}")))
    }
    #[tokio::test]
    async fn successive_record_anchors_share_prior_request_and_source_payloads(){
        use crate::storage::payload_pool::{Budget,Closure,PoolBuilder,PoolReader};
        let root=super::super::tests::path();let config=super::super::tests::config();
        let partition=RecordPartition::create(&root,config.clone()).await.unwrap();
        let objects=root.join("objects");std::fs::create_dir(&objects).unwrap();
        let budget=Budget{bytes:config.max_journal_bytes,objects:4,payloads:100,body_bytes:config.max_record_bytes};
        partition.write(super::super::tests::source_write("initial","source",&[])).await.unwrap();
        let mut pool=PoolBuilder::begin(&objects,Closure::default(),budget,format!("{:032x}",1)).unwrap();
        let first={let inner=partition.inner.lock().await;ReferencedState::write(&inner.config,&inner.state,&mut pool).unwrap()};
        let closure=pool.finish().unwrap();assert_eq!(closure.objects.values().map(|d|d.payloads.len()).sum::<usize>(),1);
        let old=closure.objects.clone();
        let reader=PoolReader::open(&objects,closure.clone(),budget).unwrap();
        let narrow=ReferencedState{config:RecordConfig{max_record_bytes:1,..config.clone()},state:first.state.clone()};
        assert!(narrow.read(&reader,config.max_journal_bytes).unwrap_err().to_string().contains("payload read budget exceeded"));
        first.read(&reader,config.max_journal_bytes).unwrap();drop(reader);
        let mut update=super::super::tests::source_write("revision","source",&[]);update.expected=ExpectedVersion::Match(1);
        update.document.as_mut().unwrap().insert("body".into(),serde_json::json!("revised alpha"));
        partition.write(update).await.unwrap();
        let mut pool=PoolBuilder::begin(&objects,closure,budget,format!("{:032x}",2)).unwrap();
        let inner=partition.inner.lock().await;let expected=serde_json::to_vec(&inner.state).unwrap();
        let second=ReferencedState::write(&inner.config,&inner.state,&mut pool).unwrap();drop(inner);
        let closure=pool.finish().unwrap();assert_eq!(closure.objects.len(),2);
        for (id,descriptor) in old {assert_eq!(closure.objects[&id],descriptor);}
        assert_eq!(closure.objects.values().map(|d|d.payloads.len()).sum::<usize>(),2);
        let reader=PoolReader::open(&objects,closure,budget).unwrap();
        let encoded=serde_json::to_vec(&second).unwrap();
        let second:ReferencedState=serde_json::from_slice(&encoded).unwrap();
        let (_,state)=second.read(&reader,config.max_journal_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&state).unwrap(),expected);
        assert_eq!(state.operation_lookup.len(),2);
        drop(reader);drop(partition);std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn referenced_record_state_preserves_exact_requests_and_raw_payloads() {
        // Exercise the real write planner and authority, not a parallel record API.
        let root=super::super::tests::path();
        let config=super::super::tests::config();
        let partition=RecordPartition::create(&root,config.clone()).await.unwrap();
        let mut request=super::super::tests::write("payload", "a", "alpha", ExpectedVersion::Absent);
        request.document.as_mut().unwrap().insert("document".into(),serde_json::json!({"$payload":999}));
        partition.write(request).await.unwrap();
        let inner=partition.inner.lock().await;
        let expected=serde_json::to_vec(&inner.state).unwrap();
        let (prepared,pool,closure,_)=prepare(&root,&inner.config,&inner.state);
        // The same exact Document in current Record and retry request shares bytes.
        assert_eq!(closure.objects.values().map(|d|d.payloads.len()).sum::<usize>(),1);
        let encoded=serde_json::to_vec(&prepared).unwrap();
        let prepared=serde_json::from_slice::<ReferencedState>(&encoded).unwrap();
        let (_,state)=prepared.read(&pool,config.max_journal_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&state).unwrap(),expected);
        drop(pool);drop(inner);drop(partition);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn referenced_state_keeps_history_private_chunks_and_same_version_vector_patch() {
        use super::super::tests::{path,config,source_write,write,generation_write,stage_generation};
        let root=path(); let settings=config();
        let partition=RecordPartition::create(&root,settings.clone()).await.unwrap();
        let mut source=source_write("source","s",&[]);
        source.document.as_mut().unwrap().shift_remove("vector");
        let original=source.document.clone();
        partition.write(source.clone()).await.unwrap();
        let mut patch=write("patch","s","ignored",ExpectedVersion::Match(1));
        patch.document=None;
        patch.vector_patch=Some(serde_json::from_value(serde_json::json!({"vector":[7.0,0.0]})).unwrap());
        assert_eq!(partition.write(patch.clone()).await.unwrap().version,1);
        partition.write(generation_write("private",ExpectedVersion::Absent,stage_generation("g",&["a","b"]))).await.unwrap();
        let mut revision=write("revision","s","changed",ExpectedVersion::Match(1));
        revision.document.as_mut().unwrap().shift_remove("vector");
        partition.write(revision).await.unwrap();
        let inner=partition.inner.lock().await;
        assert_eq!(inner.state.source_revisions.len(),1);
        let expected=serde_json::to_vec(&inner.state).unwrap();
        let (prepared,pool,closure,_)=prepare(&root,&settings,&inner.state);
        let mut references=0; let mut wire=prepared.state.clone();
        visit(&mut wire,|_|{references+=1;Ok(())}).unwrap();
        assert!(closure.objects.values().map(|d|d.payloads.len()).sum::<usize>()<references);
        let (_,state)=prepared.read(&pool,settings.max_journal_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&state).unwrap(),expected);
        assert_eq!(state.operations[0].request.document,original);
        assert_eq!(state.source_revisions[0].document.as_ref().unwrap()["vector"],serde_json::json!([7.0,0.0]));
        assert_eq!(state.records["s"].version,2);
        // Neither the current source nor its historical revision can expose staged bodies.
        assert!(!state.records.contains_key("a"));
        assert_eq!(state.generations["doc"].staged.as_ref().unwrap().chunks.len(),2);
        drop(pool);drop(inner);drop(partition);std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn referenced_state_rejects_invalid_reference_and_truncated_payload() {
        use super::super::tests::{path,config,write};
        let root=path();let settings=config();
        let partition=RecordPartition::create(&root,settings.clone()).await.unwrap();
        partition.write(write("op","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        let inner=partition.inner.lock().await;
        let (prepared,pool,_,file)=prepare(&root,&settings,&inner.state);
        let encoded=serde_json::to_vec(&prepared).unwrap();
        let mut invalid:ReferencedState=serde_json::from_slice(&encoded).unwrap();
        invalid.state["records"]["a"]["document"]=serde_json::json!({"$payload":{"object":prepared.state["records"]["a"]["document"]["$payload"]["object"],"position":u64::MAX}});
        assert!(invalid.read(&pool,settings.max_journal_bytes).is_err());
        let mut invalid:ReferencedState=serde_json::from_slice(&encoded).unwrap();
        invalid.state["records"]["a"]["document"]=serde_json::json!({"$payload":0,"extra":true});
        assert!(invalid.read(&pool,settings.max_journal_bytes).is_err());
        std::fs::OpenOptions::new().write(true).open(&file).unwrap().set_len(16).unwrap();
        assert!(prepared.read(&pool,settings.max_journal_bytes).is_err());
        drop(pool);drop(inner);drop(partition);std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn referenced_state_limits_reference_expansion_before_decoding() {
        use super::super::tests::{path,config,write};
        let root=path();let mut settings=config();settings.max_record_bytes=4096;
        let partition=RecordPartition::create(&root,settings.clone()).await.unwrap();
        partition.write(write("op","a",&"x".repeat(3000),ExpectedVersion::Absent)).await.unwrap();
        let inner=partition.inner.lock().await;
        let (mut prepared,pool,closure,_)=prepare(&root,&settings,&inner.state);
        let record=prepared.state["records"]["a"].clone();
        for n in 0..20 {prepared.state["records"][format!("copy-{n}")]=record.clone();}
        // Physical bytes + metadata fit, but expanded repeated bodies do not.
        assert!(closure.objects.values().map(|d|d.bytes).sum::<u64>()<20_000);
        assert!(encoded_length(&prepared.state,20_000).is_ok());
        let error=prepared.read(&pool,20_000).err().unwrap();
        assert!(error.to_string().contains("expanded payload budget"),"{error}");
        drop(pool);drop(inner);drop(partition);std::fs::remove_dir_all(root).unwrap();
    }

}
