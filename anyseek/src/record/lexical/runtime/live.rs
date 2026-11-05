//! WAL-backed visible deltas. Native materialization never owns the foreground
//! publication mutex while doing file I/O. The enclosing Record owns authority.
use super::*;
use futures::FutureExt;
use std::sync::Weak;
use crate::record::{index_consumers::Cursors,lexical::memory::{Row,Reservation}};
#[derive(Default)]
pub(super) struct Live {
    pub base: Option<Arc<Snapshot>>,
    pub enabled: bool,
    pub running: bool,
    pub closing: bool,
    pub rows: Vec<Arc<Row>>,
    pub bytes: u64,
    pub error: Option<String>,
    pub cursors: Weak<Cursors>,
    pub pending:Arc<std::sync::Mutex<super::super::memory::PendingCounts>>,
    pub writer:Weak<super::super::directory::WriterReservation>,
}
impl Runtime {
    pub(crate) fn reserve_visible(&self,reservation:&mut Reservation)->Result<(),RecordError>{
        use std::sync::atomic::Ordering;
        self.visible_bytes.fetch_update(Ordering::AcqRel,Ordering::Acquire,|n|n.checked_add(reservation.bytes).filter(|n|*n<=crate::storage::build_budget::EXPLICIT_BUFFERS)).map_err(|_|RecordError::Busy)?;
        reservation.capacity=Some(crate::record::lexical::memory::Capacity{counter:self.visible_bytes.clone(),bytes:reservation.bytes});
        let (workspace,pending)=self.reserve_progress(reservation.analysis_bytes,reservation.id_bytes)?;
        reservation.pending=Some(pending);
        *reservation.workspace.get_mut().map_err(|_|storage("visible workspace poisoned"))?=Some(workspace);
        Ok(())
    }
    fn reserve_progress(&self,analysis_bytes:u64,identity_bytes:u64)->Result<(Escrow,super::super::memory::Pending),RecordError>{
        let mut state=self.live.lock().map_err(|_|RecordError::RecoveryRequired)?;
        if state.closing||state.error.is_some(){return Err(RecordError::RecoveryRequired);}
        let publication=self.published.read().map_err(|_|RecordError::RecoveryRequired)?;
        let base=state.base.as_ref().or(publication.as_ref()).ok_or_else(||storage("progress reservation missing native base"))?;
        let (base_count,base_bytes)=(base.identities.len() as u64,base.identity_id_bytes);
        let mut pending=state.pending.lock().map_err(|_|storage("visible pending accounting poisoned"))?;
        let count=base_count.checked_add(pending.rows).and_then(|n|n.checked_add(1)).ok_or_else(||storage("visible identity count overflow"))?;
        let id_bytes=base_bytes.checked_add(pending.id_bytes).and_then(|n|n.checked_add(identity_bytes)).ok_or_else(||storage("visible identity bytes overflow"))?;
        let identity_total=count.checked_mul(256).and_then(|n|n.checked_add(id_bytes)).ok_or_else(||storage("visible identity memory overflow"))?;
        crate::storage::build_budget::validate_single(identity_total).map_err(storage)?;
        let temporary=65536u64.checked_add(count.checked_mul(256).ok_or_else(||storage("visible manifest overflow"))?).and_then(|n|n.checked_add(id_bytes.checked_mul(8)?)).ok_or_else(||storage("visible manifest overflow"))?;
        let fields=self.config.schema.iter().filter(|field|field.index_lexical).count() as u64;
        let snapshot=65536u64.checked_add(count.checked_mul(128+fields*32).ok_or_else(||storage("visible snapshot overflow"))?).and_then(|n|n.checked_add(id_bytes)).ok_or_else(||storage("visible snapshot overflow"))?;
        let row=RowWorkspace{analysis:Charge::admit(analysis_bytes)?,identity:Charge::admit(256+identity_bytes)?};
        let publication=PublishWorkspace{temporary:Charge::admit(temporary)?,snapshot:Charge::admit(snapshot)?};
        let writer=match state.writer.upgrade(){Some(writer)=>writer,None=>self.directory.as_ref().ok_or_else(||storage("immutable progress reservation"))?.prepare_writer(WRITER_BYTES as u64)?};
        // Publish accounting only after every fallible admission. Cancellation
        // refunds the entire plan; no worker waits for later memory occupancy.
        pending.rows+=1;pending.id_bytes+=identity_bytes;drop(pending);
        state.writer=Arc::downgrade(&writer);
        Ok((Escrow{row,writer,publication},super::super::memory::Pending{counts:state.pending.clone(),id_bytes:identity_bytes}))
    }
    pub(crate) fn reserve_fallback(&self,document:&Document,id:&str)->Result<super::super::memory::Fallback,RecordError>{
        let analysis=crate::record::build_budget::document_estimate(document,&self.config.schema)?;
        self.reserve_fallback_estimated(analysis,id)
    }
    pub(crate) fn reserve_fallback_estimated(&self,analysis:u64,id:&str)->Result<super::super::memory::Fallback,RecordError>{
        let (workspace,pending)=self.reserve_progress(analysis,id.len() as u64)?;
        Ok(super::super::memory::Fallback{pending,workspace})
    }
    pub(crate) async fn append_fallback_admitted(self:&Arc<Self>,identity:Identity,document:&Document,
        admission:super::super::memory::Fallback,count_update:bool)->Result<(),RecordError>{
        self.join_visible().await?;
        #[cfg(feature="fault-injection")]
        if let Some(path)=std::env::var_os("ANYSEEK_FALLBACK_PAUSE_FILE").map(std::path::PathBuf::from) {
            std::fs::write(path.with_extension("started"),b"fallback accepted").map_err(storage)?;
            let deadline=std::time::Instant::now()+std::time::Duration::from_secs(20);
            while path.exists(){
                if std::time::Instant::now()>deadline{return Err(storage("fallback pause deadline"));}
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        // The blocking native task takes this owner, so cancellation of an
        // awaiting future cannot refund identity accounting before publication.
        self.append_inner(identity,document,true,count_update,None,Some(admission)).await
    }
    pub(crate) fn visible_enabled(&self)->bool{self.live.lock().map_or(true,|s|s.enabled)}
    pub(crate) fn visible_admits(&self,bytes:u64)->bool{self.live.lock().is_ok_and(|s|s.error.is_none()&&!s.closing&&(bytes>crate::storage::build_budget::EXPLICIT_BUFFERS || self.visible_bytes.load(std::sync::atomic::Ordering::Acquire).checked_add(bytes).is_some_and(|n|n<=crate::storage::build_budget::EXPLICIT_BUFFERS)))}
    pub(crate) fn visible_status(&self)->Result<(),RecordError>{
        if self.live.lock().map_err(|_|RecordError::RecoveryRequired)?.error.is_some(){Err(RecordError::RecoveryRequired)}else{Ok(())}
    }
    fn with_delta(base:&Arc<Snapshot>,rows:&[Arc<Row>])->Result<Arc<Snapshot>,RecordError>{
        let mut snapshot=(**base).clone();
        let mut identities=(*base.identities).clone();
        snapshot.overlay=Vec::with_capacity(rows.len());
        for row in rows {snapshot.identity_id_bytes=snapshot.identity_id_bytes.checked_add(row.identity.id.len() as u64).ok_or_else(||storage("visible identity bytes overflow"))?;let ordinal=identities.len();identities.0.push(row.identity.clone());snapshot.overlay.push((ordinal,row.clone()));snapshot.cut.sequence=snapshot.cut.sequence.max(row.identity.revision);}
        snapshot.identities=Arc::new(identities);Ok(Arc::new(snapshot))
    }
    pub(crate) async fn append_visible(self:&Arc<Self>,identity:Identity,document:&Document,reservation:Reservation,count_update:bool,cursors:Option<Arc<Cursors>>)->Result<(),RecordError>{
        if !self.has_snapshot()? {return Err(storage("accepted visible row missing pre-WAL base"));}
        let row=Row::analyze(identity,document,&self.config,&self.analyzer,reservation).await?;
        let start={
            let mut state=self.live.lock().map_err(|_|RecordError::RecoveryRequired)?;
            if state.closing||state.error.is_some(){return Err(RecordError::RecoveryRequired);}
            if state.rows.len()>=128 || state.bytes.checked_add(row.reservation.bytes).is_none_or(|n|n>crate::storage::build_budget::EXPLICIT_BUFFERS){return Err(RecordError::RecoveryRequired);}
            if let Some(cursors)=cursors{state.cursors=Arc::downgrade(&cursors);}
            let mut publication=self.published.write().map_err(|_|RecordError::RecoveryRequired)?;
            let previous=publication.as_ref().ok_or_else(||storage("missing visible base"))?;
            // The native base is retained independently from its visible suffix.
            let base=state.base.get_or_insert_with(||previous.clone()).clone();
            state.enabled=true;state.bytes+=row.reservation.bytes;state.rows.push(row);
            *publication=Some(Self::with_delta(&base,&state.rows)?);
            if count_update{self.reserve_update();}
            let start=!state.running;state.running=true;start
        };
        if start {
            let owner=self.clone();
            let scope=crate::resources::Scope::current().map(|s|s.retained_io());
            // Construct the tracked worker under the current accepted activity,
            // but retain no caller foreground admission entry.
            crate::INDEX_RUNTIME.spawn(crate::resources::lifecycle::track(async move {
                let work=async{std::panic::AssertUnwindSafe(owner.materialize()).catch_unwind().await};
                let result=match scope{Some(s)=>s.run(work).await,None=>work.await};
                let error=match result{Ok(Ok(()))=>None,Ok(Err(e))=>Some(format!("{e:?}")),Err(_)=>Some("visible materializer panicked".into())};
                if let Some(error)=error {
                    if let Ok(mut state)=owner.live.lock(){state.error=Some(error.clone());state.running=false;if let Some(c)=state.cursors.upgrade(){c.fail(error);}}
                    owner.live_done.notify_waiters();
                }
            }));
        }
        Ok(())
    }
    async fn materialize(self:&Arc<Self>)->Result<(),RecordError>{
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let rows={self.live.lock().map_err(|_|RecordError::RecoveryRequired)?.rows.clone()};
            if rows.is_empty(){return Err(storage("empty running materializer"));}
            #[cfg(feature="fault-injection")]
            if let Some(path)={
                let configured=std::env::var_os("ANYSEEK_VISIBLE_PAUSE_FILE").map(std::path::PathBuf::from);
                #[cfg(test)]
                let configured=configured.or_else(||{let p=self.package_path().join(".test-visible-pause");p.exists().then_some(p)});
                configured
            } {
                let path=path.as_path();let _=std::fs::write(path.with_extension("started"),b"materialize");
                let deadline=std::time::Instant::now()+std::time::Duration::from_secs(20);
                while path.exists(){if std::time::Instant::now()>deadline{return Err(storage("visible pause deadline"));}tokio::time::sleep(std::time::Duration::from_millis(5)).await;}
            }
            #[cfg(feature="fault-injection")]
            if std::env::var("ANYSEEK_VISIBLE_CRASH_STAGE").as_deref()==Ok("before_native"){std::process::abort();}
            let mut workspaces=Vec::with_capacity(rows.len());
            for row in &rows {
                workspaces.push(row.reservation.workspace.lock().map_err(|_|storage("visible workspace poisoned"))?.take().ok_or_else(||storage("visible workspace already consumed"))?);
            }
            // The last prepared row covers this whole prefix. Pending counters
            // also include cancelled-order gaps and pinned rows conservatively.
            let publication=workspaces.last_mut().ok_or_else(||storage("empty visible workspace"))?;
            self.directory.as_ref().ok_or_else(||storage("immutable visible prefix"))?.install_writer(publication.writer.clone())?;
            let mut publication=None;
            let mut row_work=Vec::with_capacity(workspaces.len());
            for workspace in workspaces {row_work.push(workspace.row);publication=Some(workspace.publication);}
            #[cfg(test)]
            {
                let gate=self.package_path().join(".test-visible-workspace-ready");
                if gate.exists(){
                    std::fs::write(gate.with_extension("started"),b"reserved").map_err(storage)?;
                    tokio::time::timeout(std::time::Duration::from_secs(10),async {
                        while gate.exists(){tokio::time::sleep(std::time::Duration::from_millis(1)).await;}
                    }).await.map_err(|_|storage("workspace ready test deadline"))?;
                }
            }
            for (row,workspace) in rows.iter().zip(row_work) {self.append_inner((*row.identity).clone(),&row.document,false,false,Some(workspace),None).await?;}
            let data=OwnedData{guard:self.data.clone().lock_owned().await,_owner:self.clone(),unacknowledged:false};
            let (_,snapshot)=self.blocking(data,move |owner,data|owner.commit_snapshot_locked(data,publication)).await?;
            #[cfg(feature="fault-injection")]
            if std::env::var("ANYSEEK_VISIBLE_CRASH_STAGE").as_deref()==Ok("before_handoff"){std::process::abort();}
            #[cfg(feature="fault-injection")]
            if std::env::var_os("ANYSEEK_VISIBLE_FAIL_BEFORE_HANDOFF").is_some(){return Err(storage("injected visible handoff failure"));}
            #[cfg(test)]
            if self.package_path().join(".test-visible-fail").exists(){return Err(storage("injected visible handoff failure"));}
            {
                let mut state=self.live.lock().map_err(|_|RecordError::RecoveryRequired)?;
                if state.rows.len()<rows.len() || !state.rows.iter().zip(&rows).all(|(a,b)|Arc::ptr_eq(a,b)){return Err(storage("visible materialization prefix changed"));}
                for row in state.rows.drain(..rows.len()).collect::<Vec<_>>(){state.bytes-=row.reservation.bytes;}
                state.base=Some(snapshot.clone());
                *self.published.write().map_err(|_|RecordError::RecoveryRequired)?=Some(Self::with_delta(&snapshot,&state.rows)?);
                #[cfg(feature="fault-injection")]
                if std::env::var("ANYSEEK_VISIBLE_CRASH_STAGE").as_deref()==Ok("after_handoff"){std::process::abort();}
                // Release the worker's copied prefix before waking drainers;
                // any remaining charge now belongs to a pinned query snapshot.
                drop(rows);
                if state.rows.is_empty(){state.running=false;self.live_done.notify_waiters();return Ok(());}
            }
        }
    }
    pub(crate) async fn append_fallback_queued(self:&Arc<Self>,identity:Identity,document:&Document)->Result<(),RecordError>{
        self.join_visible().await?;self.append_inner(identity,document,true,false,None,None).await
    }
    pub(crate) async fn join_visible(&self)->Result<(),RecordError>{
        loop {
            let notified=self.live_done.notified();tokio::pin!(notified);notified.as_mut().enable();
            {let state=self.live.lock().map_err(|_|RecordError::RecoveryRequired)?;if !state.running{return if state.error.is_some(){Err(RecordError::RecoveryRequired)}else{Ok(())};}}
            notified.await;
        }
    }
}

#[cfg(all(test,feature="fault-injection"))]
mod tests {
    use super::*;
    use crate::record::{RecordPartition,ExpectedVersion};
    use crate::record::tests::{config,write};
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn committed_delta_is_queryable_during_paused_materialization_and_replays() {
        let temp=tempfile::tempdir().unwrap();
        let root=temp.path().join("records");let p=RecordPartition::create(&root,config()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
        let a=write("delta-a","same","alpha",ExpectedVersion::Absent);
        let ra=tokio::time::timeout(std::time::Duration::from_secs(5),p.write(a.clone())).await.unwrap().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5),async{while !pause.with_extension("started").exists(){tokio::time::sleep(std::time::Duration::from_millis(1)).await;}}).await.unwrap();
        assert_eq!(p.search("alpha".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().hits.len(),1);
        let b=write("delta-b","same","beta",ExpectedVersion::Match(1));
        let rb=tokio::time::timeout(std::time::Duration::from_secs(5),p.write(b.clone())).await.unwrap().unwrap();
        assert!(p.search("alpha".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().hits.is_empty());
        assert_eq!(p.search("beta".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().hits.len(),1);
        assert_eq!(p.write(a.clone()).await.unwrap(),ra);assert_eq!(p.write(b.clone()).await.unwrap(),rb);
        let mut delete=write("delta-delete","same","unused",ExpectedVersion::Match(2));delete.document=None;
        let rd=p.write(delete.clone()).await.unwrap();
        assert!(p.search("beta".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().hits.is_empty());
        std::fs::remove_file(&pause).unwrap();p.finish_service_shutdown().await.unwrap();drop(native);drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        for (request,receipt) in [(a,ra),(b,rb),(delete,rd)]{assert_eq!(p.write(request).await.unwrap(),receipt);}
        assert!(p.search("beta".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().hits.is_empty());
        p.finish_service_shutdown().await.unwrap();
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn delta_scores_and_hybrid_order_equal_native_after_atomic_handoff() {
        use crate::{index::FieldLengthNormalization,search::SearchMode,vector::Embedding,vector_similarity::AnnMode};
        for normalization in [FieldLengthNormalization::Shared,FieldLengthNormalization::PerField] {
            let temp=tempfile::tempdir().unwrap();let mut cfg=config();cfg.index.field_length_normalization=normalization;
            cfg.schema[0].boost=1.7;
            cfg.schema.push(serde_json::from_value(serde_json::json!({"field":"title","field_type":"Text","store":false,"index_lexical":true,"boost":3.0})).unwrap());
            let p=RecordPartition::create(&temp.path().join("records"),cfg).await.unwrap();
            // Keep both a materialized base and a paused suffix in the same corpus.
            p.write(write("base","base","alpha",ExpectedVersion::Absent)).await.unwrap();
            let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();native.join_visible().await.unwrap();
            let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
            for (id,body,title) in [("a","alpha alpha".to_owned(),"beta"),("b",format!("alpha {}","beta ".repeat(130)),""),("c","gamma".to_owned(),"alpha beta")] {
                let mut r=write(id,id,&body,ExpectedVersion::Absent);r.document.as_mut().unwrap().insert("title".into(),title.into());p.write(r).await.unwrap();
            }
            let held=native.snapshot().await.unwrap();assert_eq!(held.overlay.len(),3);
            let mut before=Vec::new();
            for text in ["alpha beta","\"alpha beta\"","alpha -gamma","+alpha +beta"] {
                for mode in [SearchMode::Lexical,SearchMode::Hybrid{similarity_threshold:None,ann_mode:AnnMode::All,rrf:Default::default()}] {
                    let vector=if matches!(mode,SearchMode::Lexical){None}else{Some(Embedding::F32(vec![1.0,0.0]))};
                    let q=p.search(text.into(),vector.clone(),mode.clone(),vec![],10).await.unwrap();
                    before.push((text,vector,mode,q.hits.iter().map(|h|(h.record.record_id.clone(),h.score)).collect::<Vec<_>>()));
                }
            }
            std::fs::remove_file(&pause).unwrap();native.join_visible().await.unwrap();assert!(native.snapshot().await.unwrap().overlay.is_empty());
            // A pinned old query must retain its reservations after publication swaps.
            assert!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire)>0);
            for (text,vector,mode,expected) in before {let q=p.search(text.into(),vector,mode,vec![],10).await.unwrap();assert_eq!(expected,q.hits.iter().map(|h|(h.record.record_id.clone(),h.score)).collect::<Vec<_>>(),"{text}");}
            drop(held);assert_eq!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire),0);
            p.finish_service_shutdown().await.unwrap();
        }
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn query_finalize_budget_and_inner_join_do_not_cycle() {
        for release_inner in [true,false] {
            let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");
            let p=RecordPartition::create(&root,config()).await.unwrap();
            p.write(write("seed","seed","seed",ExpectedVersion::Absent)).await.unwrap();
            let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
            native.join_visible().await.unwrap(); // Writer capacity already admitted.
            let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
            let receipt=p.write(write("suffix","a","alpha",ExpectedVersion::Absent)).await.unwrap();
            let gate=root.join(".test-query-candidates-pause");std::fs::write(&gate,b"pause").unwrap();
            let owner=p.clone();let query=tokio::spawn(async move {
                owner.query_independent(crate::record::RecordQuery {
                    text:"no_such_term".into(),vector:None,mode:crate::search::SearchMode::Lexical,
                    query_type:crate::search::QueryType::Intersection,offset:0,length:100,
                    result_type:crate::search::ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![],temporal:None,
                },None).await
            });
            tokio::time::timeout(std::time::Duration::from_secs(5),async {
                while !root.join(".test-query-candidates-pause-entered").exists(){tokio::time::sleep(std::time::Duration::from_millis(1)).await;}
            }).await.unwrap();
            let inner=p.inner.lock().await;
            let held=crate::storage::build_budget::fill_memory_for_test();
            std::fs::remove_file(&gate).unwrap();std::fs::remove_file(&pause).unwrap();
            let start=std::time::Instant::now();
            let joined=if release_inner {
                let short=tokio::time::timeout(std::time::Duration::from_millis(100),inner.index.join_materializers()).await;
                if let Ok(result)=short {result.unwrap();}assert!(!query.is_finished());drop(inner);
                let result=query.await.unwrap();assert!(result.is_ok(),"releasing Inner must let query finish: {result:?}");
                tokio::time::timeout(std::time::Duration::from_secs(8),native.join_visible()).await.unwrap()
            } else {
                let result=tokio::time::timeout(std::time::Duration::from_secs(8),inner.index.join_materializers()).await.unwrap();
                assert!(!query.is_finished());drop(inner);
                let result_query=query.await.unwrap();
                eprintln!("held Inner query result={result_query:?}");result
            };
            let reason=native.live.lock().unwrap().error.clone();
            eprintln!("cycle diagnostic release_inner={release_inner} elapsed={:?} joined={joined:?} live_error={reason:?} receipt={receipt:?}",start.elapsed());
            drop(held);
            // A temporary query-owned budget must not poison acknowledged data.
            assert!(reason.is_none(),"Inner/query budget cycle permanently fenced materializer: {reason:?}");
            assert!(joined.is_ok());
            p.maintenance_tick().await;native.join_visible().await.unwrap();
            p.finish_service_shutdown().await.unwrap();
        }
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn hybrid_count_filters_nonmatching_delta_before_channel_accounting() {
        use crate::{search::SearchMode,vector::Embedding,vector_similarity::AnnMode};
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
        p.write(write("a","a","alpha",ExpectedVersion::Absent)).await.unwrap();
        p.write(write("b","b","beta",ExpectedVersion::Absent)).await.unwrap();
        assert_eq!(native.snapshot().await.unwrap().overlay.len(),2);
        // Neither document passes the vector threshold, so only the alpha
        // lexical match may contribute to the hybrid channel count.
        let mode=SearchMode::Hybrid{similarity_threshold:Some(0.9),ann_mode:AnnMode::All,rrf:Default::default()};
        let before=p.search("alpha".into(),Some(Embedding::F32(vec![0.0,1.0])),mode.clone(),vec![],10).await.unwrap();
        assert_eq!(before.hits.len(),1);assert_eq!(before.hits[0].record.record_id,"a");
        assert_eq!(before.count_total,1,"nonmatching visible rows must not inflate the hybrid count");
        std::fs::remove_file(&pause).unwrap();native.join_visible().await.unwrap();
        let after=p.search("alpha".into(),Some(Embedding::F32(vec![0.0,1.0])),mode.clone(),vec![],10).await.unwrap();
        assert_eq!(after.count_total,before.count_total);assert_eq!(after.hits[0].score,before.hits[0].score);
        p.finish_service_shutdown().await.unwrap();drop(native);drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();
        let reopened=p.search("alpha".into(),Some(Embedding::F32(vec![0.0,1.0])),mode,vec![],10).await.unwrap();
        assert_eq!(reopened.count_total,1);assert_eq!(reopened.hits[0].record.record_id,"a");
        p.finish_service_shutdown().await.unwrap();
    }
    #[ignore = "isolated process: set build single/shared budgets to67108864"]
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn small_shared_budget_pending_rows_do_not_starve_their_own_materializer() {
        assert_eq!(std::env::var("ANYSEEK_BUILD_SHARED_MEMORY_BYTES").unwrap(),"67108864");
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
        let mut receipts=Vec::new();let body="a ".repeat(1800);
        for n in 0..32 {
            let id=format!("row-{n}");
            match p.write(write(&id,&id,&body,ExpectedVersion::Absent)).await {
                Ok(receipt)=>receipts.push(receipt),Err(RecordError::Busy)=>break,Err(error)=>panic!("unexpected admission {error:?}"),
            }
        }
        assert!(receipts.len()>1);std::fs::remove_file(pause).unwrap();
        let mut joined=Err(RecordError::Busy);
        for attempt in 0..3 {
            p.maintenance_tick().await;
            joined=tokio::time::timeout(std::time::Duration::from_secs(5),native.join_visible()).await.unwrap();
            let (pending,error)={let state=native.live.lock().unwrap();(state.rows.len(),state.error.clone())};
            let count=native.data.lock().await.identities.len();
            eprintln!("self-held memory attempt={attempt} ACKs={} used={} join={joined:?} rows={pending} native={count} error={error:?}",receipts.len(),crate::storage::build_budget::used_memory_for_test());
            if joined.is_ok(){break;}
        }
        assert!(joined.is_ok(),"acknowledged rows must retain enough workspace to make progress without any external owner to release");
        p.finish_service_shutdown().await.unwrap();
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn prewal_escrow_cancellation_refunds_writer_and_partial_scope_admission() {
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");
        let scope=Arc::new(crate::resources::Governor::default()).admit("escrow-owner".into(),crate::resources::Lane::Background,0).unwrap();
        let p=scope.clone().run(RecordPartition::create(&root,config())).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        scope.clone().run(native.flush()).await.unwrap();
        let document=serde_json::from_str(r#"{"body":"alpha"}"#).unwrap();
        let before=crate::storage::build_budget::used_memory_for_test();
        let mut first=scope.clone().run_sync(||Reservation::new(&document,&config(),"same")).unwrap();
        scope.clone().run_sync(||native.reserve_visible(&mut first)).unwrap();
        let mut second=scope.clone().run_sync(||Reservation::new(&document,&config(),"same")).unwrap();
        scope.clone().run_sync(||native.reserve_visible(&mut second)).unwrap();
        let fallback=scope.clone().run_sync(||native.reserve_fallback(&document,"large-pending")).unwrap();
        let writer=native.live.lock().unwrap().writer.clone();
        assert_eq!(writer.strong_count(),3,"visible and fallback plans share one writer charge");
        assert!(Arc::ptr_eq(&fallback.workspace.writer,&writer.upgrade().unwrap()));
        assert_eq!(native.live.lock().unwrap().pending.lock().unwrap().rows,3);
        drop(first);assert_eq!(writer.strong_count(),2);
        drop(second);assert_eq!(writer.strong_count(),1);
        drop(fallback);assert!(writer.upgrade().is_none());
        assert_eq!(native.live.lock().unwrap().pending.lock().unwrap().rows,0);
        assert_eq!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire),0);
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);

        let mut refused=scope.clone().run_sync(||Reservation::new(&document,&config(),"rejected")).unwrap();
        let mut held=Vec::new();let mut bytes=1u64<<31;
        while bytes>0 {match scope.bytes(bytes){Ok(lease)=>held.push(lease),Err(_)=>bytes/=2}}
        let before_refusal=crate::storage::build_budget::used_memory_for_test();
        assert_eq!(scope.clone().run_sync(||native.reserve_visible(&mut refused)),Err(RecordError::Busy));
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before_refusal,"partial global charge must be refunded on subject refusal");
        assert_eq!(native.live.lock().unwrap().pending.lock().unwrap().rows,0);
        assert!(writer.upgrade().is_none());
        drop(refused);drop(held);
        assert_eq!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire),0);
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before);
        p.finish_service_shutdown().await.unwrap();
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn running_fallback_cancellation_keeps_workspace_until_native_task_exits() {
        let temp=tempfile::tempdir().unwrap();let package=temp.path().join("package");std::fs::create_dir(&package).unwrap();
        let scope=Arc::new(crate::resources::Governor::default()).admit("fallback-cancel".into(),crate::resources::Lane::Background,0).unwrap();
        // Establish the reusable tokenizer before measuring per-runtime refunds.
        let warm=scope.clone().run(Runtime::create_owned(&package.join("warm"),&config())).await.unwrap();
        warm.shutdown().await.unwrap();drop(warm);
        fn available(scope:&crate::resources::Scope)->u64 {
            let mut held=Vec::new();let mut size=1u64<<31;let mut total=0;
            while size>0 {match scope.bytes(size){Ok(lease)=>{held.push(lease);total+=size;},Err(_)=>size/=2}}
            total
        }
        let scope_before=available(&scope);
        let before=crate::storage::build_budget::used_memory_for_test();
        let native=scope.clone().run(Runtime::create_owned(&package.join("native"),&config())).await.unwrap();
        scope.clone().run(native.flush()).await.unwrap();
        let document:Document=serde_json::from_value(serde_json::json!({"body":"alpha alpha"})).unwrap();
        let admission=scope.clone().run_sync(||native.reserve_fallback(&document,"cancelled")).unwrap();
        let writer=Arc::downgrade(&admission.workspace.writer);
        let pending=native.live.lock().unwrap().pending.clone();
        let gate=package.join(".test-fallback-native-block");std::fs::write(&gate,b"pause").unwrap();
        let owner=native.clone();let run_scope=scope.clone();
        let task=tokio::spawn(async move {run_scope.run(owner.append_fallback_admitted(
            Identity{id:"cancelled".into(),version:1,revision:1},&document,admission,true)).await});
        tokio::time::timeout(std::time::Duration::from_secs(5),async {
            while !gate.with_extension("started").exists(){tokio::task::yield_now().await;}
        }).await.unwrap();
        assert!(native.data.try_lock().is_err(),"actual blocking task must own native Data");
        let occupied=crate::storage::build_budget::used_memory_for_test();
        task.abort();assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pending.lock().unwrap().rows,1,"waiter cancellation cannot refund the running identity");
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),occupied,"running task keeps its workspace");
        assert!(writer.upgrade().is_some());
        assert!(!crate::record::package_pins::retire_if_unpinned(&package,||panic!("running native pin lost")).unwrap());
        let mut scope_pressure=Vec::new();let mut size=1u64<<31;
        while size>0 {match scope.bytes(size){Ok(lease)=>scope_pressure.push(lease),Err(_)=>size/=2}}
        let pressure=crate::storage::build_budget::fill_memory_for_test();std::fs::remove_file(&gate).unwrap();
        {let data=tokio::time::timeout(std::time::Duration::from_secs(5),native.data.lock()).await.unwrap();
            assert!(data.write_state==WriteState::Failed,"cancelled native result fences writer reuse");
            assert_eq!(data.identities.len(),1);assert_eq!(data.pending_analysis_documents,0);}
        assert_eq!(pending.lock().unwrap().rows,0);
        assert_eq!(native.snapshot().await.unwrap().identities.len(),1,"owned publication completed under full memory");
        assert!(native.flush().await.is_err());drop(pressure);drop(scope_pressure);
        native.shutdown().await.unwrap();assert!(writer.upgrade().is_none());drop(native);
        assert!(crate::record::package_pins::retire_if_unpinned(&package,||Ok(())).unwrap());
        assert_eq!(crate::storage::build_budget::used_memory_for_test(),before,"runtime cancellation refunds all per-owner memory");
        assert_eq!(available(&scope),scope_before,"both governor and global credits must refund");
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn admitted_prefix_finishes_under_full_memory_with_old_snapshot_pinned() {
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
        let gate=native.package_path().join(".test-visible-workspace-ready");std::fs::write(&gate,b"pause").unwrap();
        for id in ["a","b","c"]{p.write(write(id,id,"alpha",ExpectedVersion::Absent)).await.unwrap();}
        let pinned=native.snapshot().await.unwrap();assert_eq!(pinned.overlay.len(),3);
        std::fs::remove_file(pause).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5),async{
            while !gate.with_extension("started").exists(){tokio::time::sleep(std::time::Duration::from_millis(1)).await;}
        }).await.unwrap();
        let held=crate::storage::build_budget::fill_memory_for_test();std::fs::remove_file(gate).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5),native.join_visible()).await.unwrap().unwrap();
        assert_eq!(native.data.lock().await.identities.len(),3);assert_eq!(native.snapshot().await.unwrap().overlay.len(),0);
        assert!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire)>0);drop(pinned);
        assert_eq!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire),0);drop(held);
        assert_eq!(p.search("alpha".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().count_total,3);
        p.finish_service_shutdown().await.unwrap();
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn acknowledged_rows_finish_under_pressure_with_each_old_prefix_pinned() {
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let pause=native.package_path().join(".test-visible-pause");
        let mut pinned=Vec::new();
        for n in 0..3 {
            std::fs::write(&pause,b"pause").unwrap();
            let id=format!("row-{n}");
            p.write(write(&id,&id,"alpha",ExpectedVersion::Absent)).await.unwrap();
            pinned.push(native.snapshot().await.unwrap());
            let visible=pinned.last().unwrap();
            assert_eq!(visible.identity_id_bytes,visible.identities.iter().map(|id|id.id.len() as u64).sum::<u64>());
            let held=crate::storage::build_budget::fill_memory_for_test();
            std::fs::remove_file(&pause).unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5),native.join_visible()).await.unwrap().unwrap();
            for snapshot in &pinned {for (_,row) in &snapshot.overlay {
                assert!(row.reservation.workspace.lock().unwrap().is_none(),"old pinned rows must not retain consumed native workspace");
            }}
            assert_eq!(native.data.lock().await.identities.len(),n+1);
            let published=native.snapshot().await.unwrap();
            assert_eq!(published.identity_id_bytes,published.identities.iter().map(|id|id.id.len() as u64).sum::<u64>());
            assert!(native.live.lock().unwrap().error.is_none());
            drop(held);
        }
        let writer=native.live.lock().unwrap().writer.clone();
        assert!(writer.upgrade().is_some(),"native directory owns the started writer");
        p.finish_service_shutdown().await.unwrap();
        assert!(writer.upgrade().is_none(),"joined writer charge must release even with old readers pinned");
        drop(pinned);
        assert_eq!(native.live.lock().unwrap().pending.lock().unwrap().rows,0);
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn acknowledged_delta_materialization_failure_fences_and_replays() {
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");let p=RecordPartition::create(&root,config()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
        std::fs::write(native.package_path().join(".test-visible-fail"),b"fail").unwrap();
        let request=write("ack","a","alpha",ExpectedVersion::Absent);let receipt=p.write(request.clone()).await.unwrap();
        assert_eq!(p.search("alpha".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().hits.len(),1);
        std::fs::remove_file(&pause).unwrap();assert!(native.join_visible().await.is_err());
        assert!(p.search("alpha".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.is_err());
        assert!(p.write(write("later","b","beta",ExpectedVersion::Absent)).await.is_err());
        assert!(p.finish_service_shutdown().await.is_err());drop(native);drop(p);
        let p=RecordPartition::open(&root,config().max_journal_bytes).await.unwrap();assert_eq!(p.write(request).await.unwrap(),receipt);
        assert_eq!(p.search("alpha".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().hits.len(),1);
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn paused_materializer_bounds_retained_rows_and_backpressures_before_wal() {
        let temp=tempfile::tempdir().unwrap();let mut cfg=config();cfg.max_record_bytes=128*1024;cfg.max_journal_bytes=8*1024*1024;
        let p=RecordPartition::create(&temp.path().join("records"),cfg.clone()).await.unwrap();
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();
        let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
        let body=(0..4000).map(|i|format!("word{i} ")).collect::<String>();
        let template=write("template","a",&body,ExpectedVersion::Absent);
        let estimate=crate::record::build_budget::document_estimate(template.document.as_ref().unwrap(),&cfg.schema).unwrap()+65536+3;
        let capacity=crate::storage::build_budget::EXPLICIT_BUFFERS;
        let count=(capacity/estimate) as usize;assert!((2..32).contains(&count));
        for i in 0..count {p.write(write(&format!("op{i}"),"a",&body,ExpectedVersion::Any)).await.unwrap();}
        assert!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire)<=capacity);
        let wal_bytes=||std::fs::read_dir(temp.path().join("records")).unwrap().map(|e|e.unwrap().path().join("wal")).filter_map(|p|std::fs::metadata(p).ok()).map(|m|m.len()).sum::<u64>();
        let before_wal=wal_bytes();assert!(before_wal>0);
        let owner=p.clone();let next=write("after-pressure","a",&body,ExpectedVersion::Any);
        let task=tokio::spawn(async move{owner.write(next).await});
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(!task.is_finished(),"rotation must wait for the paused bounded materializer");
        assert!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire)<=capacity);
        assert_eq!(wal_bytes(),before_wal);
        // The blocked write has not passed its WAL admission. Releasing the
        // worker permits the existing prefix to seal before the next decision.
        std::fs::remove_file(&pause).unwrap();
        let receipt=tokio::time::timeout(std::time::Duration::from_secs(20),task).await.unwrap().unwrap().unwrap();assert_eq!(receipt.seq,count as u64+1);
        assert_eq!(p.search("word3999".into(),None,crate::search::SearchMode::Lexical,vec![],10).await.unwrap().hits.len(),1);
        p.finish_service_shutdown().await.unwrap();
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn delta_ttl_updates_deletes_and_exact_sort_survive_handoff() {
        use crate::{record::{RecordQuery,TemporalMetadata,temporal},search::{SearchMode,QueryType,ResultType,ResultSort,SortOrder}};
        struct Clock(std::sync::atomic::AtomicU64);
        impl temporal::Clock for Clock {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0.load(std::sync::atomic::Ordering::SeqCst))}}
        let temp=tempfile::tempdir().unwrap();let root=temp.path().join("records");
        let mut cfg=config();cfg.schema.push(serde_json::from_value(serde_json::json!({"field":"rank","field_type":"Int","store":false,"facet":true,"index_lexical":false})).unwrap());
        let p=RecordPartition::create(&root,cfg.clone()).await.unwrap();
        let clock=Arc::new(Clock(std::sync::atomic::AtomicU64::new(1000)));p.inner.lock().await.clock=clock.clone();
        for id in ["base","updated","deleted"] {let mut r=write(id,id,"alpha old",ExpectedVersion::Absent);r.document.as_mut().unwrap().insert("rank".into(),3.into());p.write(r).await.unwrap();}
        let native=p.inner.lock().await.index.test_index().read().await.segments[0].lexical.clone();native.join_visible().await.unwrap();
        let pause=native.package_path().join(".test-visible-pause");std::fs::write(&pause,b"pause").unwrap();
        for i in 0..12 {let id=format!("delta-{i:02}");let mut r=write(&id,&id,if i%2==0 {"alpha beta"}else{"alpha gamma"},ExpectedVersion::Absent);r.document.as_mut().unwrap().insert("rank".into(),(i%3).into());if i%3==0{r.temporal=Some(TemporalMetadata{expire_at_ms:Some(1100),..Default::default()});}p.write(r).await.unwrap();}
        let mut update=write("update","updated","beta new",ExpectedVersion::Match(1));update.document.as_mut().unwrap().insert("rank".into(),0.into());p.write(update.clone()).await.unwrap();
        let mut delete=write("delete","deleted","unused",ExpectedVersion::Match(1));delete.document=None;p.write(delete.clone()).await.unwrap();
        clock.0.store(1100,std::sync::atomic::Ordering::SeqCst);
        let held=native.snapshot().await.unwrap();assert_eq!(held.overlay.len(),13);
        let query=|text:&str,field:&str,order:SortOrder|RecordQuery{temporal:None,text:text.into(),vector:None,mode:SearchMode::Lexical,query_type:QueryType::Union,offset:1,length:5,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![ResultSort{field:field.into(),order}]};
        let mut before=Vec::new();
        for text in ["alpha","beta","old","\"alpha beta\"","alpha -gamma",""] {
            for field in ["rank","_id","_score"] {
                for order in [SortOrder::Ascending,SortOrder::Descending] {
                    let result=p.query(query(text,field,order.clone())).await.unwrap();
                    assert!(result.hits.iter().all(|h|h.record.record_id!="deleted" && !["delta-00","delta-03","delta-06","delta-09"].contains(&h.record.record_id.as_str())));
                    before.push((text,field,order,result.count_total,result.hits.iter().map(|h|(h.record.record_id.clone(),h.score)).collect::<Vec<_>>()));
                }
            }
        }
        std::fs::remove_file(&pause).unwrap();native.join_visible().await.unwrap();
        for (text,field,order,count,hits) in &before {let result=p.query(query(text,field,order.clone())).await.unwrap();assert_eq!(*count,result.count_total);assert_eq!(*hits,result.hits.iter().map(|h|(h.record.record_id.clone(),h.score)).collect::<Vec<_>>());}
        assert!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire)>0);drop(held);assert_eq!(native.visible_bytes.load(std::sync::atomic::Ordering::Acquire),0);
        p.finish_service_shutdown().await.unwrap();drop(native);drop(p);
        let p=RecordPartition::open(&root,cfg.max_journal_bytes).await.unwrap();p.inner.lock().await.clock=clock;
        for (text,field,order,count,hits) in before {let result=p.query(query(text,field,order)).await.unwrap();assert_eq!(count,result.count_total);assert_eq!(hits,result.hits.iter().map(|h|(h.record.record_id.clone(),h.score)).collect::<Vec<_>>());}
        p.finish_service_shutdown().await.unwrap();
    }

}
