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
        let replay=inner.state.groups.as_ref().is_some_and(|g|g.receipts.iter().any(|r|r.operation_id==request.operation_id));
        if !replay && maintenance::has_expired_operations(&inner.state,current_epoch())? {return Err(inner.request_checkpoint("expired_group_receipts"));}
        let empty=group_plan::Groups::default();
        let decision=match group_plan::prepare(&inner.state,inner.state.groups.as_ref().unwrap_or(&empty),&inner.config,request,current_epoch(),inner.evaluation_time_ms)? {
            group_plan::Plan::Replay(r)=>return Ok(r),group_plan::Plan::Replace(d)=>d,
        };
        if inner.maintenance_requested.is_some(){return Err(RecordError::Busy);}
        let mut candidate=inner.state.try_clone_with_growth(checkpoint_codec::json_length(&decision)?)?;
        group_plan::apply(&mut candidate,&decision)?;
        let snapshot=checkpoint_codec::length(&inner.config,&candidate)?;
        let reserve=mutation::reserve_bytes(&candidate);
        checkpoint_codec::validate_size(snapshot,reserve,inner.config.max_journal_bytes).map_err(|_|RecordError::Rejected("group checkpoint budget exceeded".into()))?;
        let payload=encode_entry(&Entry::GroupApplied(decision.clone()))?;
        inner.ensure_journal_room(payload.len(),reserve).await?;
        inner.index.attach_cursors(self.indexes.clone());inner.index.drain_indexes().await?;
        let mut growth=0u64;let mut updates=0usize;
        for r in &decision.records {let a=member_applied(&decision,r);let (g,u)=build_budget::write_estimate_for(inner.state.records.get(&r.record_id),&inner.config,&a)?;growth=growth.checked_add(g).ok_or(RecordError::Busy)?;updates=updates.checked_add(u).ok_or(RecordError::Busy)?;}
        if !inner.index.can_admit(growth,updates).await {inner.maintain_index_tail().await?;}
        if !inner.index.can_admit(growth,updates).await{return Err(RecordError::Busy);}
        let mut credits=Vec::new();
        for r in &decision.records {
            let credit=if let Some(document)=&r.document {
                Some((lexical::memory::Admission::Visible(inner.index.reserve_visible(document,&inner.config,&r.record_id,inner.journal.owner_guard()).await?),inner.index.reserve_component(r,&inner.config).await?))
            }else{None};credits.push(credit);
        }
        // Recheck deadlines after every fallible/awaiting admission, before authority.
        inner.observe_time()?;
        if decision.records.iter().any(|r|r.document.is_some()&&r.temporal.expire_at_ms.is_some_and(|t|t<=inner.evaluation_time_ms)) {return Err(RecordError::Rejected("group deadline elapsed before commit".into()));}
        let deadline=decision.records.iter().flat_map(|r|[
            r.document.as_ref().and(r.temporal.expire_at_ms),
            inner.state.records.get(&r.record_id).filter(|r|temporal::visible(r,decision.observed_time_ms)).and_then(|r|r.temporal.expire_at_ms)
        ]).flatten().min();
        if deadline.is_some_and(|t|t<=inner.evaluation_time_ms){return Err(RecordError::Busy);}
        if let Some(remote)=inner.remote.as_ref() {
            let id=format!("{}:{}",decision.operation_id.epoch,decision.operation_id.nonce);
            if remote.batch_tail_needs_checkpoint(&[(decision.receipt.seq,id.clone(),&payload)]).await? {return Err(inner.request_checkpoint("group_remote_tail_room"));}
            let upload=remote.prepare_upload(decision.receipt.seq-1,inner.journal.root()).await?;
            drop(guard.take());
            let uploaded=upload.upload_group(decision.receipt.seq,id,&payload).await;
            *guard=Some(self.inner.lock().await);
            let inner=guard.as_mut().unwrap();
            let uploaded=uploaded?;
            if !inner.healthy || inner.state.seq.checked_add(1)!=Some(decision.receipt.seq) {return Err(RecordError::RecoveryRequired);}
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
        inner.append_operation(&payload,decision.receipt.seq).await?;
        #[cfg(test)] if std::env::var("ANYSEEK_GROUP_CRASH_ROOT").ok().as_deref()==inner.journal.root().to_str(){std::process::exit(72);}
        #[cfg(test)] if inner.fail_after_durable {return Err(RecordError::DurablePending(decision.receipt.seq));}
        let publication=async{
            inner.index.begin_batch(self.indexes.clone(),Arc::downgrade(&self.inner)).await?;
            for (record,credit) in decision.records.iter().zip(credits){
                let (native,component)=credit.map_or((None,None),|(a,b)|(Some(a),Some(b)));
                inner.replay_context().publish_group_member(record,decision.receipt.seq,native,component,true).await?;
            }
            inner.index.finish_batch(decision.receipt.seq,self.indexes.clone()).await?;
            inner.index.drain_indexes().await?;
            Ok::<_,RecordError>(())
        }.await;
        if publication.is_err(){return Err(RecordError::DurablePending(decision.receipt.seq));}
        inner.state=candidate;inner.snapshot_bytes=snapshot;
        self.indexes.immediate(decision.receipt.seq);
        Ok(decision.receipt)
    }
}
fn member_applied(d:&group_plan::Decision,r:&Record)->Applied {
    Applied{request:RecordWrite{operation_id:d.operation_id.clone(),record_id:r.record_id.clone(),expected:ExpectedVersion::Any,
        document:r.document.clone(),vector_patch:None,temporal:r.document.as_ref().map(|_|r.temporal.clone())},
        receipt:WriteReceipt{seq:d.receipt.seq,version:r.version,record_id:r.record_id.clone()},observed_epoch:d.observed_epoch,observed_time_ms:d.observed_time_ms}
}
impl replay::Context<'_>{
    pub(super) async fn publish_group_member(&mut self,r:&Record,seq:u64,native:Option<lexical::memory::Admission>,component:Option<index_consumers::ComponentCredit>,publish_native:bool)->Result<(),RecordError>{
        let old=self.locations.get(&r.record_id).copied();
        if let Some(document)=&r.document {
            let id=self.index.append(r.record_id.clone(),r.version,seq,temporal::indexed(document,&r.temporal,r.ingested_at_ms),native,component,publish_native).await?;
            if self.ordinals.len()<=id {self.ordinals.resize(id+1,String::new());}
            self.ordinals[id]=r.record_id.clone();self.locations.insert(r.record_id.clone(),id);
        }else{self.locations.remove(&r.record_id);}
        if let Some(old)=old{self.index.retire(old).await?;}
        #[cfg(test)] if self.fail_publication_stage!=0{return Err(RecordError::DurablePending(seq));}
        Ok(())
    }
    pub(super) async fn replay_group(&mut self,d:group_plan::Decision,publish_native:bool)->Result<(),RecordError>{
        let mut next=self.state.try_clone_with_growth(checkpoint_codec::json_length(&d)?)?;
        group_plan::apply(&mut next,&d)?;
        let(mut growth,mut updates)=(0u64,0usize);
        for r in &d.records {let a=member_applied(&d,r);let(g,u)=build_budget::write_estimate_for(self.state.records.get(&r.record_id),self.config,&a)?;
            growth=growth.checked_add(g).ok_or_else(||storage("group replay growth overflow"))?;updates=updates.checked_add(u).ok_or_else(||storage("group replay update overflow"))?;}
        if growth>0&&!self.index.can_admit(growth,updates).await {if !publish_native{self.index.flush_replayed_native().await?;}self.stage_stable_tail().await?;}
        if growth>0&&!self.index.can_admit(growth,updates).await{return Err(storage("durable group exceeds replay tail capacity"));}
        for r in &d.records {self.publish_group_member(r,d.receipt.seq,None,None,publish_native).await?;}
        *self.state=next;*self.snapshot_bytes=checkpoint_codec::length(self.config,self.state)?;
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
        p.finish_service_shutdown().await.unwrap();
    }

}
