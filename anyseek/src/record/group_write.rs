//! A group has exactly one durable decision and one publication generation.
use super::*;
use futures::FutureExt;
impl RecordPartition {
    /// Return the retained group version, including an empty group's version.
    pub async fn group_version(&self,group_id:&str)->Result<Option<u64>,RecordError>{
        let inner=self.inner.lock().await;
        if !inner.healthy{return Err(RecordError::RecoveryRequired);}
        Ok(inner.state.groups.as_ref().and_then(|g|g.heads.get(group_id)).map(|h|h.version))
    }
    /// Atomically replace one bounded group. Cancellation never cancels accepted work.
    pub async fn replace_group(&self,request:GroupReplace)->Result<GroupReceipt,RecordError>{
        let bytes=checkpoint_codec::json_length(&request)?;
        if bytes>4*1024*1024 {return Err(RecordError::Rejected("group request byte budget exceeded".into()));}
        let memory=crate::storage::build_budget::reserve_memory((bytes as u64).checked_mul(32).ok_or(RecordError::Busy)?).map_err(memory_error)?;
        let permit=self.admission.clone().try_acquire_owned().map_err(|_|RecordError::Busy)?;
        let p=self.clone();
        crate::resources::spawn(async move{
            let _permit=permit;let _memory=memory;
            let _writer=p.writer.lock().await;
            let result=std::panic::AssertUnwindSafe(p.replace_group_owned(request)).catch_unwind().await
                .unwrap_or_else(|_|Err(RecordError::OutcomeUnknown("group writer unwound".into())));
            if matches!(&result,Err(RecordError::OutcomeUnknown(_)|RecordError::DurablePending(_)|RecordError::RecoveryRequired)) {
                let mut inner=p.inner.lock().await;inner.healthy=false;inner.index.abort_batch();
                if let Ok(mut output)=inner.output.write(){output.fenced=true;}
            }
            drop(_writer);p.kick_maintenance().await;
            result
        }).await.map_err(|e|RecordError::OutcomeUnknown(e.to_string()))?
    }
    async fn replace_group_owned(&self,request:GroupReplace)->Result<GroupReceipt,RecordError>{
        // Keep the guard through error handling; only the unselected object upload
        // releases it, while the owned task retains the writer and admission leases.
        let mut guard=Some(self.inner.lock().await);
        let result=std::panic::AssertUnwindSafe(self.replace_group_locked(&mut guard,request)).catch_unwind().await
            .unwrap_or_else(|_|Err(RecordError::OutcomeUnknown("group publication unwound".into())));
        if guard.is_none() {guard=Some(self.inner.lock().await);}
        let inner=guard.as_mut().unwrap();
        if matches!(&result,Err(RecordError::OutcomeUnknown(_)|RecordError::DurablePending(_)|RecordError::RecoveryRequired)) {
            inner.healthy=false;inner.index.abort_batch();
            if let Ok(mut output)=inner.output.write(){output.fenced=true;}
        }
        result
    }
    async fn replace_group_locked<'a>(&'a self,guard:&mut Option<tokio::sync::MutexGuard<'a,Inner>>,request:GroupReplace)->Result<GroupReceipt,RecordError>{
        let inner=guard.as_mut().unwrap();
        if !inner.healthy{return Err(RecordError::RecoveryRequired);}
        inner.observe_time()?;
        let replay=inner.state.groups.as_ref().is_some_and(|g|g.receipt(&request.operation_id).is_some());
        if !replay && maintenance::has_expired_operations(&inner.state,current_epoch()) {return Err(inner.request_checkpoint("expired_group_receipts"));}
        let decision=match group_plan::prepare(&inner.state,&inner.config,request,current_epoch(),inner.evaluation_time_ms)? {
            group_plan::Plan::Replay(r)=>return Ok(r),group_plan::Plan::Replace(d)=>d,
        };
        if inner.maintenance_requested.is_some(){return Err(RecordError::Busy);}
        let entry = Entry::GroupApplied(decision);
        let payload = encode_entry(&entry)?;
        let Entry::GroupApplied(decision) = entry else { unreachable!() };
        let prepared = group_plan::Prepared::new(&inner.state, decision, inner.snapshot_bytes)?;
        let seq = prepared.stored.receipt.seq;
        checkpoint_codec::validate_size(prepared.snapshot_bytes, prepared.reserve_bytes, inner.config.max_journal_bytes)
            .map_err(|_| RecordError::Rejected("group checkpoint budget exceeded".into()))?;
        inner.ensure_journal_room(payload.len(), prepared.reserve_bytes).await?;
        inner.index.attach_cursors(self.indexes.clone());inner.index.drain_indexes().await?;
        let (growth, updates) = group_index_growth(&prepared, &inner.config)?;
        if !inner.index.can_admit(growth,updates).await {inner.maintain_index_tail().await?;}
        if !inner.index.can_admit(growth,updates).await{return Err(RecordError::Busy);}
        let mut credits=Vec::new();
        for r in &prepared.records {
            let credit=if let Some(document)=&r.document {
                Some((lexical::memory::Admission::Visible(inner.index.reserve_visible(document,&inner.config,&r.record_id,inner.journal.owner_guard()).await?),inner.index.reserve_component(r,&inner.config).await?))
            }else{None};credits.push(credit);
        }
        // Recheck deadlines after every fallible/awaiting admission, before authority.
        inner.observe_time()?;
        if prepared.records.iter().any(|r|r.document.is_some()&&r.temporal.expire_at_ms.is_some_and(|t|t<=inner.evaluation_time_ms)) {return Err(RecordError::Rejected("group deadline elapsed before commit".into()));}
        let deadline=prepared.records.iter().flat_map(|r|[
            r.document.as_ref().and(r.temporal.expire_at_ms),
            inner.state.records.get(&r.record_id).filter(|r|temporal::visible(r,prepared.stored.observed_time_ms)).and_then(|r|r.temporal.expire_at_ms)
        ]).flatten().min();
        if deadline.is_some_and(|t|t<=inner.evaluation_time_ms){return Err(RecordError::Busy);}
        prepared.admit_containers(&mut inner.state)?;
        if let Some(remote)=inner.remote.as_ref() {
            let id=format!("{}:{}",prepared.stored.operation_id.epoch,prepared.stored.operation_id.nonce);
            if remote.batch_tail_needs_checkpoint(&[(seq,id.clone(),&payload)]).await? {return Err(inner.request_checkpoint("group_remote_tail_room"));}
            let upload=remote.prepare_upload(seq-1,inner.journal.root()).await?;
            drop(guard.take());
            let uploaded=upload.upload_group(seq,id,&payload).await;
            *guard=Some(self.inner.lock().await);
            let inner=guard.as_mut().unwrap();
            let uploaded=uploaded?;
            if !inner.healthy || inner.state.seq.checked_add(1)!=Some(seq) {return Err(RecordError::RecoveryRequired);}
            inner.observe_time()?;
            if deadline.is_some_and(|t|t<=inner.evaluation_time_ms){return Err(RecordError::Busy);}
            let clock=inner.clock.clone();
            let min_time_ms=inner.evaluation_time_ms.max(inner.state.time_watermark_ms);
            uploaded.publish_checked(inner.remote.as_mut().ok_or(RecordError::RecoveryRequired)?,move || {
                let now=temporal::checked_time(clock.as_ref(),min_time_ms)?;
                if deadline.is_some_and(|t|t<=now){return Err(RecordError::Busy);}
                Ok(())
            }).await?;
        }
        let inner=guard.as_mut().unwrap();
        inner.append_operation(&payload,seq).await?;
        #[cfg(test)] if std::env::var("ANYSEEK_GROUP_CRASH_ROOT").ok().as_deref()==inner.journal.root().to_str(){std::process::exit(72);}
        #[cfg(test)] if inner.fail_after_durable {return Err(RecordError::DurablePending(seq));}
        let publication=async{
            inner.index.begin_batch(self.indexes.clone(),Arc::downgrade(&self.inner)).await?;
            for (record,credit) in prepared.records.iter().zip(credits){
                let (native,component)=credit.map_or((None,None),|(a,b)|(Some(a),Some(b)));
                inner.replay_context().publish_group_member(record,seq,native,component,true).await?;
            }
            inner.index.finish_batch(seq,self.indexes.clone()).await?;
            inner.index.drain_indexes().await?;
            let snapshot_bytes = prepared.snapshot_bytes;
            let receipt = prepared.install(&mut inner.state)?;
            inner.snapshot_bytes = snapshot_bytes;
            self.indexes.immediate(seq);
            Ok::<_, RecordError>(receipt)
        }.await;
        publication.map_err(|_| RecordError::DurablePending(seq))
    }
}
fn group_index_growth(
    prepared: &group_plan::Prepared, config: &RecordConfig,
) -> Result<(u64, usize), RecordError> {
    prepared.records.iter().try_fold((0u64, 0usize), |(bytes, updates), record| {
        let (growth, count) = build_budget::document_write_estimate(config, &record.record_id, record.document.as_ref())?;
        Ok((bytes.checked_add(growth).ok_or(RecordError::Busy)?,
            updates.checked_add(count).ok_or(RecordError::Busy)?))
    })
}
impl replay::Context<'_>{
    pub(super) async fn publish_group_member(&mut self,r:&Record,seq:u64,native:Option<lexical::memory::Admission>,component:Option<index_consumers::ComponentCredit>,publish_native:bool)->Result<(),RecordError>{
        let old=self.locations.get(&r.record_id).copied();
        if let Some(document)=&r.document {
            let id=self.index.append(r.record_id.clone(),r.version,seq,temporal::indexed(document,&r.temporal,r.ingested_at_ms),native,component,publish_native).await?;
            self.publish_location(id, &r.record_id);
        }else{self.locations.remove(&r.record_id);}
        if let Some(old)=old{self.index.retire(old).await?;}
        #[cfg(test)] if self.fail_publication_stage!=0{return Err(RecordError::DurablePending(seq));}
        Ok(())
    }
    pub(super) async fn replay_group(&mut self,d:group_plan::Decision,publish_native:bool)->Result<(),RecordError>{
        let prepared = group_plan::Prepared::new(self.state, d, *self.snapshot_bytes)?;
        let seq = prepared.stored.receipt.seq;
        let (growth, updates) = group_index_growth(&prepared, self.config)?;
        if growth>0&&!self.index.can_admit(growth,updates).await {if !publish_native{self.index.flush_replayed_native().await?;}self.stage_stable_tail().await?;}
        if growth>0&&!self.index.can_admit(growth,updates).await{return Err(storage("durable group exceeds replay tail capacity"));}
        prepared.admit_containers(self.state)?;
        for record in &prepared.records {
            self.publish_group_member(record, seq, None, None, publish_native).await?;
        }
        let snapshot_bytes = prepared.snapshot_bytes;
        prepared.install(self.state)?;
        *self.snapshot_bytes = snapshot_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(op:&str,version:u64,ids:&[&str])->GroupReplace {
        GroupReplace{operation_id:OperationId::new(op.into()),group_id:"group".into(),expected_group_version:version,
            records:ids.iter().map(|id|GroupRecord{record_id:(*id).into(),document:super::super::tests::write("unused",id,"alpha",ExpectedVersion::Any).document.unwrap(),temporal:None}).collect()}
    }
    #[tokio::test]
    async fn local_whole_group_retry_checkpoint_and_reopen(){
        let temp=tempfile::tempdir().unwrap();let path=temp.path().join("records");let cfg=super::super::tests::config();
        let p=RecordPartition::create(&path,cfg.clone()).await.unwrap();
        let first=request("first",0,&["a","b","c"]);let receipt=p.replace_group(first.clone()).await.unwrap();assert_eq!(receipt.seq,1);
        let next=request("next",1,&["a","d"]);let newer=p.replace_group(next.clone()).await.unwrap();assert_eq!(newer.seq,2);
        assert_eq!(p.replace_group(first).await.unwrap(),receipt);
        assert!(matches!(p.write(super::super::tests::write("ordinary","a","bad",ExpectedVersion::Any)).await,Err(RecordError::Conflict(_))));
        assert!(p.get("b").await.unwrap().is_none());assert_eq!(p.group_version("group").await.unwrap(),Some(2));
        assert_eq!(p.statistics().await.unwrap().physical_versions, 5);
        p.compact_journal().await.unwrap();
        p.finish_service_shutdown().await.unwrap();drop(p);
        let p=RecordPartition::open(&path,cfg.max_journal_bytes).await.unwrap();
        assert_eq!(p.replace_group(next).await.unwrap(),newer);assert_eq!(p.group_version("group").await.unwrap(),Some(2));
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,2);
        let cleared=p.replace_group(request("clear",2,&[])).await.unwrap();assert_eq!(cleared.seq,3);assert_eq!(cleared.group_version,3);
        p.finish_service_shutdown().await.unwrap();drop(p);
        let p=RecordPartition::open(&path,cfg.max_journal_bytes).await.unwrap();assert_eq!(p.group_version("group").await.unwrap(),Some(3));
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,0);p.finish_service_shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn invalid_suffix_and_durable_failure_leave_no_half_group(){
        let temp=tempfile::tempdir().unwrap();let path=temp.path().join("records");let cfg=super::super::tests::config();
        let p=RecordPartition::create(&path,cfg.clone()).await.unwrap();let mut bad=request("bad",0,&["a","b"]);bad.records[1].document.insert("vector".into(),serde_json::json!([1]));
        assert!(p.replace_group(bad).await.is_err());assert_eq!(p.inner.lock().await.state.seq,0);
        let request=request("durable",0,&["a","b"]);p.inner.lock().await.fail_after_durable=true;
        assert_eq!(p.replace_group(request.clone()).await,Err(RecordError::DurablePending(1)));
        assert!(p.get("a").await.is_err());p.finish_service_shutdown().await.unwrap();drop(p);
        let p=RecordPartition::open(&path,cfg.max_journal_bytes).await.unwrap();assert_eq!(p.replace_group(request).await.unwrap().seq,1);
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,2);p.finish_service_shutdown().await.unwrap();
    }
    #[tokio::test]
    #[ignore = "invoked by group WAL crash parent"]
    async fn group_wal_crash_worker(){
        let root=PathBuf::from(std::env::var("ANYSEEK_GROUP_CRASH_ROOT").unwrap());
        let p=RecordPartition::create(&root,super::super::tests::config()).await.unwrap();
        let _=p.replace_group(request("crash",0,&["a","b"])).await;
        panic!("crash hook was not reached");
    }
    #[tokio::test]
    async fn group_wal_process_exit_recovers_single_decision(){
        let temp=tempfile::tempdir().unwrap();let path=temp.path().join("records");
        let output=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","record::group_write::tests::group_wal_crash_worker","--ignored","--nocapture"])
            .env("ANYSEEK_GROUP_CRASH_ROOT",&path).output().unwrap();
        assert_eq!(output.status.code(),Some(72),"{}",String::from_utf8_lossy(&output.stderr));
        let p=RecordPartition::open(&path,super::super::tests::config().max_journal_bytes).await.unwrap();
        assert_eq!(p.replace_group(request("crash",0,&["a","b"])).await.unwrap().seq,1);
        assert_eq!(p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,2);
        assert_eq!(p.statistics().await.unwrap().physical_versions, 2);
        p.finish_service_shutdown().await.unwrap();
    }

}
