//! A private replay context cannot access journal authority, remote CAS, output
//! fences, or the background task slot. Live callers supply their guarded view;
//! background callers supply an independently owned candidate.
use super::*;

pub(super) struct Context<'a> {
    pub root:&'a Path,
    pub config:&'a RecordConfig,
    pub state:&'a mut State,
    pub index:&'a mut view::RetrievalView,
    pub ordinals:&'a mut Vec<String>,
    pub locations:&'a mut BTreeMap<String,usize>,
    pub snapshot_bytes:&'a mut usize,
    pub graph_cache:&'a mut graph::Cache,
    #[cfg(test)] pub fail_publication_stage:u8,
}
impl Inner {
    pub(super) fn replay_context(&mut self)->Context<'_> {
        Context {root:self.journal.root(),config:&self.config,state:&mut self.state,
            index:&mut self.index,ordinals:&mut self.ordinals,locations:&mut self.locations,
            snapshot_bytes:&mut self.snapshot_bytes,graph_cache:&mut self.graph_cache,
            #[cfg(test)] fail_publication_stage:self.fail_publication_stage}
    }
}
impl Context<'_> {
    async fn stage_stable_tail(&mut self)->Result<(),RecordError> {
        let target=self.root.join(format!("segment-{:032x}",rand::random::<u128>()));
        let (successor,_)=build_at(self.root,&target,self.config,&State::default()).await?;
        successor.read().await.record_lexical[0].admit_writer().await?;
        let budget=view::RotationBudget{max_segments:capacity::segments(self.config).checked_add(1).ok_or_else(||storage("recovery segment limit overflow"))?,
            max_address_slots:self.config.max_records.checked_add(self.config.max_operations).and_then(|n|n.checked_mul(64)).ok_or_else(||storage("view address budget overflow"))?};
        let limit=self.config.max_journal_bytes.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024)).ok_or_else(||storage("replay closure budget overflow"))?;
        self.index.seal_into_successor(self.root,successor,self.ordinals,self.locations,budget,limit).await
    }
}
impl Context<'_> {
    /// Replay certified tail decisions into the selected index view. Never
    /// append WAL or generate another operation/feed identity during recovery.
    pub(super) async fn replay(&mut self,entry:Entry)->Result<(),RecordError>{
        match entry {
            Entry::Applied(applied)=>{
                let (index_bytes,updates)=build_budget::write_estimate(&*self.state,&self.config,&applied)?;
                if index_bytes>0 && !self.index.can_admit(index_bytes,updates).await {
                    self.stage_stable_tail().await?;
                }
                let growth=checkpoint_codec::json_length(&applied)?.checked_mul(3).ok_or_else(||storage("tail state growth overflow"))?;
                if applied.request.generation.is_some() {
                    let mut next=self.state.try_clone_with_growth(growth)?;apply(&mut next,&applied)?;
                    let bytes=checkpoint_codec::length(&self.config, &next)?;
                    next.admit_bytes(bytes)?;
                    self.publish_prepared(&applied,bytes,None,Some(next)).await?;
                } else {
                    let prepared=state_memory::Prepared::new(&*self.state,&applied)?;
                    let bytes=mutation::snapshot_after(&*self.state,*self.snapshot_bytes,&applied,&prepared.record)?;
                    prepared.admit_containers(&mut *self.state,bytes)?;
                    self.publish_prepared(&applied,bytes,Some(prepared),None).await?;
                }
                *self.graph_cache=graph::Cache::rebuild(&*self.state)?;
                return Ok(());
            },
            Entry::Expired(event)=>{
                let mut next=self.state.try_clone_with_growth(checkpoint_codec::json_length(&event)?)?;temporal::apply(&mut next,&event)?;
                let visible=evidence::visibility(&next.records)?;
                let retired:Vec<_>=self.locations.keys().filter(|id|visible.get(*id)!=Some(&true)).cloned().collect();
                for id in retired {let ordinal=self.locations.remove(&id).unwrap();self.index.retire(ordinal).await?;}
                *self.state=next;
            },
            Entry::LeaseExpired(event)=>{
                let size=checkpoint_codec::json_length(&*self.state)?.checked_add(checkpoint_codec::json_length(&event)?).ok_or_else(||storage("lease state growth overflow"))?;
                self.state.admit_bytes(size)?;
                lease::apply(&mut *self.state,&event)?;
            },
            Entry::FeedTrim(event)=>change_feed::apply_trim(&mut *self.state,&self.config.change_feed,&event)?,
            Entry::Checkpoint{..}|Entry::Anchor(_)=>return Err(storage("unexpected anchor in operation stream")),
        }
        *self.graph_cache=graph::Cache::rebuild(&*self.state)?;
        *self.snapshot_bytes=checkpoint_codec::length(&self.config, &*self.state)?;
        Ok(())
    }
}
