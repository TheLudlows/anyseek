//! Authenticated history retention for receipt GC.
//! There is no external notification stream or cursor.
use super::*;

const MIN_RETENTION_MS: u64 = 2 * 86_400_000;
/// Receipt-history GC eligibility, in addition to the operation retry epoch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct HistoryRetention {
    /// Minimum authenticated age before history is eligible for receipt GC.
    /// Defaults to two days; the operation retry epoch is independently checked.
    /// This is an earliest eligible age, not a maximum cleanup delay.
    pub retention_ms: u64,
}
impl Default for HistoryRetention {
    fn default() -> Self {
        Self { retention_ms: MIN_RETENTION_MS }
    }
}
pub(super) fn validate_config(config: &HistoryRetention) -> Result<(), RecordError> {
    if config.retention_ms < MIN_RETENTION_MS || config.retention_ms > 365 * 86_400_000 {
        return Err(RecordError::Rejected("invalid history retention policy".into()));
    }
    Ok(())
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(super) struct HistoryState {
    // Owner scope distinguishes restored history authority.
    pub scope: String,
    // Authenticated GC eligibility boundary; not an index-consumer cursor.
    pub floor: u64,
}
pub(super) fn validate_state(state: &State) -> Result<(), RecordError> {
    if state.history.scope.len() != 32
        || !state.history.scope.bytes().all(|b| b.is_ascii_hexdigit())
        || state.history.floor > state.seq {
        return Err(storage("invalid history identity or GC boundary"));
    }
    Ok(())
}
/// Durable advancement of the logical retained prefix, never a physical WAL offset.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Trim {
    previous: u64,
    pub(super) floor: u64,
    at: u64,
}
/// Every command is a committed position.
/// Ordinary operations, TTL expirations and the retired prefix carry
/// authenticated committed times used to advance retention.
pub(super) fn committed_floor(state:&State,at:u64,retention:u64)->Option<u64> {
    let cutoff=at.checked_sub(retention)?;
    fn last_before(len:usize,mut entry:impl FnMut(usize)->(u64,u64),cutoff:u64)->Option<u64> {
        let (mut low,mut high)=(0,len);
        while low<high {
            let middle=low+(high-low)/2;
            if entry(middle).1<=cutoff {low=middle+1;} else {high=middle;}
        }
        low.checked_sub(1).map(|i|entry(i).0)
    }
    // Each authoritative sequence is independently ordered by committed time.
    // Upper bounds include every equal-time commit without a full locked scan.
    [last_before(state.operations.len(),|i|(state.operations[i].receipt.seq,state.operations[i].observed_time_ms),cutoff),
        last_before(state.expirations.len(),|i|(state.expirations[i].seq,state.expirations[i].evaluation_time_ms),cutoff),
        state.retired_operation_prefix.as_ref().filter(|prefix|prefix.time_ms<=cutoff).map(|prefix|prefix.seq)]
        .into_iter().flatten().filter(|seq|*seq>state.history.floor).max()
}
pub(super) fn apply_trim(
    state: &mut State,
    config: &HistoryRetention,
    trim: &Trim,
) -> Result<(), RecordError> {
    validate_state(state)?;
    let expected=committed_floor(state,trim.at,config.retention_ms);
    if trim.previous != state.history.floor
        || trim.at < state.time_watermark_ms
        || expected != Some(trim.floor)
    {
        return Err(storage("invalid history retention transition"));
    }
    state.history.floor = trim.floor;
    state.time_watermark_ms = trim.at;
    Ok(())
}
impl Inner {
    pub(super) async fn trim_history(&mut self, at: u64) -> Result<(), RecordError> {
        let Some(floor) = committed_floor(&self.state,at,self.config.history.retention_ms)
        else {
            return Ok(());
        };
        let trim = Trim {
            previous: self.state.history.floor,
            floor,
            at,
        };
        if self.remote.is_some() {
            self.maintenance_trim.get_or_insert(trim);
            return Err(self.request_checkpoint("remote_history_trim"));
        }
        let bytes = encode_entry(&Entry::HistoryTrim(trim.clone()))?;
        // Maintenance never consumes deletion/TTL emergency capacity. A full
        // physical WAL can be compacted through the existing checkpoint route.
        if self
            .journal
            .validate_append_with_reserve(bytes.len(), mutation::reserve_bytes(&self.state))
            .is_err()
        {
            return Ok(());
        }
        let mut candidate = self.state.try_clone()?;
        candidate.history.floor = floor;
        candidate.time_watermark_ms = at;
        let snapshot_bytes = checkpoint_codec::length(&self.config, &candidate)?;
        if checkpoint_codec::validate_size(
            snapshot_bytes, mutation::reserve_bytes(&self.state),
            self.config.max_journal_bytes,
        )
        .is_err()
        {
            return Ok(());
        }
        self.healthy = false;
        self.output
            .write()
            .map_err(|_| RecordError::RecoveryRequired)?
            .fenced = true;
        self.append_operation(&bytes,self.state.seq).await?;
        #[cfg(feature = "fault-injection")]
        match std::env::var("ANYSEEK_HISTORY_TRIM_FAULT").ok().as_deref() {
            Some("after_append_unknown") => {
                return Err(RecordError::OutcomeUnknown("history trim append".into()));
            }
            Some("after_durable") => return Err(RecordError::DurablePending(self.state.seq)),
            Some("crash_after_durable") => std::process::exit(90),
            _ => {}
        }
        self.state.history.floor = floor;
        self.state.time_watermark_ms = at;
        self.snapshot_bytes = snapshot_bytes;
        self.output
            .write()
            .map_err(|_| RecordError::DurablePending(self.state.seq))?
            .fenced = false;
        self.healthy = true;
        Ok(())
    }
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use crate::record::tests::{config,write};

    fn apply_at(state:&mut State,settings:&RecordConfig,mut request:RecordWrite,at:u64) {
        request.operation_id.epoch=at/86_400_000;
        let mutation::PreparedWrite::Append {applied,..}=mutation::prepare_at(
            state,settings,checkpoint_codec::length(settings,state).unwrap(),request,at/86_400_000,at).unwrap()
            else {panic!("new operation")};
        apply(state,&applied).unwrap();
    }

    #[test]
    fn two_day_retention_keeps_active_epochs_and_requires_authenticated_floor() {
        let day=86_400_000;
        let settings=config();assert_eq!(settings.history.retention_ms,2*day);
        assert!(validate_config(&HistoryRetention{retention_ms:2*day-1}).is_err());
        let seven=HistoryRetention{retention_ms:7*day};
        assert_eq!(serde_json::from_value::<HistoryRetention>(serde_json::to_value(&seven).unwrap()).unwrap(),seven);
        assert_eq!(serde_json::from_value::<HistoryRetention>(serde_json::json!({})).unwrap().retention_ms,2*day);
        let mut state=State::default();state.history.scope="0".repeat(32);
        for (name,epoch) in [("old",4),("previous",5),("current",6)] {
            apply_at(&mut state,&settings,write(name,name,"alpha",ExpectedVersion::Absent),epoch*day);
        }
        assert_eq!(committed_floor(&state,6*day-1,2*day),None);
        assert_eq!(committed_floor(&state,6*day,2*day),Some(1));
        maintenance::collect_operations(&mut state,6).unwrap();assert_eq!(state.operations.len(),3,"expired epoch alone is insufficient");
        // Advancing time may authenticate an older prefix without advancing the
        // retry epoch. Current/previous IDs must remain even if floor covers them.
        apply_trim(&mut state,&settings.history,&Trim{previous:0,floor:3,at:8*day}).unwrap();
        maintenance::collect_operations(&mut state,6).unwrap();
        assert_eq!(state.operations.iter().map(|op|op.request.operation_id.nonce.as_str()).collect::<Vec<_>>(),["previous","current"]);
        assert_eq!(state.records.len(),3,"receipt GC does not erase current bodies");
        let wire=encode_entry(&Entry::Checkpoint{config:settings.clone(),state}).unwrap();
        let Entry::Checkpoint{mut state,..}=serde_json::from_slice(&wire).unwrap() else{panic!("checkpoint")};
        restore_operation_lookup(&mut state).unwrap();
        let mut old=write("old","old","alpha",ExpectedVersion::Absent);old.operation_id.epoch=4;
        assert!(matches!(mutation::prepare(&state,&settings,wire.len(),old,4),Err(RecordError::OperationExpired)));
        let mut previous=write("previous","previous","alpha",ExpectedVersion::Absent);previous.operation_id.epoch=5;
        assert!(matches!(mutation::prepare(&state,&settings,wire.len(),previous,6),Ok(mutation::PreparedWrite::Replay(_))));
    }

    #[tokio::test]
    async fn ordinary_writes_advance_retention_without_dropping_recent_receipts() {
        let root=tempfile::tempdir().unwrap();
        let mut settings=config();settings.max_operations=2048;settings.max_journal_bytes=4*1024*1024;
        let initial=RecordPartition::create(&root.path().join("initial"),settings.clone()).await.unwrap();
        let mut state=initial.inner.lock().await.state.try_clone().unwrap();
        drop(initial);
        let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let beginning=now-8*86_400_000;
        apply_at(&mut state,&settings,write("source","s","evidence original",ExpectedVersion::Absent),beginning);
        apply_at(&mut state,&settings,write("initial", "ordinary", "alpha", ExpectedVersion::Absent),beginning);
        for i in 1..=768u64 {
            apply_at(&mut state,&settings,write(&format!("update-{i}"),"ordinary","alpha",ExpectedVersion::Match(i)),beginning+i*900_000);
        }
        let last_update=state.seq;
        apply_at(&mut state,&settings,write("recent-public","recent","beta",ExpectedVersion::Absent),now);
        let recent_seq=state.seq;
        validate_restored_state(&settings,&state).unwrap();
        assert_eq!(committed_floor(&state,beginning+settings.history.retention_ms,
            settings.history.retention_ms),Some(2),"upper bound includes both equal-time source and ordinary write");
        let safe=committed_floor(&state,now,settings.history.retention_ms).unwrap();
        assert!(safe>1 && safe<last_update);
        let mut bad=Trim {previous:0,floor:recent_seq,at:now};
        assert!(apply_trim(&mut state.try_clone().unwrap(),&settings.history,&bad).is_err());
        bad.floor=safe;
        let mut replayed=state.try_clone().unwrap();
        apply_trim(&mut replayed,&settings.history,&bad).unwrap();
        let path=root.path().join("live");
        let bytes=encode_entry(&Entry::Checkpoint {config:settings.clone(),state}).unwrap();
        drop(PartitionJournal::create(&path,settings.max_journal_bytes,&bytes).unwrap());
        let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        let before={let mut inner=p.inner.lock().await;
            inner.trim_history(now).await.unwrap();
            assert_eq!(inner.state.history.floor,safe);
            inner.state.operations.len()};
        assert!(matches!(p.write(write("trigger","trigger","gamma",ExpectedVersion::Absent)).await,Err(RecordError::Busy)));
        crate::record::tests::wait_maintenance_idle(&p).await;
        let inner=p.inner.lock().await;
        assert!(inner.state.operations.len()<before);
        assert!(inner.state.operations.iter().any(|op|op.receipt.seq==recent_seq));
        assert!(!inner.state.operations.iter().any(|op|op.request.operation_id.nonce=="initial"));
        assert!(inner.state.operations.iter().any(|op|op.request.operation_id.nonce=="update-768"));
        validate_restored_state(&settings,&inner.state).unwrap();
        drop(inner);drop(p);
        let recovered=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        let inner = recovered.inner.lock().await;
        assert_eq!(inner.state.history.floor, safe);
        assert!(inner.state.operations.iter().any(|op| op.receipt.seq == recent_seq));
    }
}
