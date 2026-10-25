//! Finite population bounds; these do not grant memory or candidate admission.
pub(super) const MAX_RECORDS: usize = 100_000;
pub(super) const PACKAGE_RECORDS: usize = 8192;
pub(super) const MAX_SEGMENTS: usize = 16;
pub(super) fn segments(config: &super::RecordConfig) -> usize {
    if config.max_records <= 10_000 { 8 } else { MAX_SEGMENTS }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finite_population_does_not_raise_candidate_or_memory_limits() {
        let mut config=super::super::tests::config();
        let candidates=config.max_candidates;
        config.max_records=MAX_RECORDS;
        super::super::validate_config(&config).unwrap();
        assert_eq!(config.max_candidates,candidates);
        assert!(MAX_RECORDS.div_ceil(PACKAGE_RECORDS)<segments(&config));
        config.max_records=MAX_RECORDS+1;
        assert!(super::super::validate_config(&config).is_err());
    }

    #[tokio::test]
    async fn population_above_one_package_recovers_all_identities_and_receipts() {
        use super::super::*;
        let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");
        let mut config=tests::config();config.max_records=MAX_RECORDS;config.max_operations=MAX_RECORDS;
        config.max_journal_bytes=256*1024*1024;config.max_candidates=10_000;
        config.index.inference=crate::vector::Inference::None;config.schema.retain(|f|f.index_lexical);
        config.embedding_space_id=None;
        let partition=RecordPartition::create(&path,config.clone()).await.unwrap();
        let mut state=partition.inner.lock().await.state.try_clone().unwrap();
        state.admit_bytes(10_001*2048).unwrap();
        let mut last=None;
        // Construct canonical Applied fixtures, not pretend this is HTTP load.
        // Real load is exercised separately by the bounded capacity harness.
        for i in 0..10_001 {
            let request=tests::write(&format!("op-{i}"),&format!("id-{i:05}"),if i==10_000{"alpha terminal"}else{"alpha"},ExpectedVersion::Absent);
            let receipt=WriteReceipt{staged_only:false,seq:i+1,version:1,record_id:request.record_id.clone()};
            let applied=Applied{request_digest:None,graph_invalidation:false,observed_epoch:request.operation_id.epoch,observed_time_ms:0,request,receipt};
            apply(&mut state,&applied).unwrap();last=Some(applied);
        }
        let last=last.unwrap();
        partition.inner.lock().await.publish_anchor_state(state,false).await.unwrap();
        assert_eq!(partition.inner.lock().await.index.local_statistics().await.unwrap().len(),3);
        let mut next=Some(partition);
        for _ in 0..2 {
            let p=match next.take(){Some(p)=>p,None=>RecordPartition::open(&path,config.max_journal_bytes).await.unwrap()};
            let result=p.query(RecordQuery{text:"alpha".into(),vector:None,mode:SearchMode::Lexical,query_type:QueryType::Intersection,offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
            assert_eq!(result.count_total,10_001);assert_eq!(result.hits.len(),10);
            assert_eq!(p.search("terminal".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().hits[0].record.record_id,"id-10000");
            assert_eq!(p.write(last.request.clone()).await.unwrap(),last.receipt);
        }
    }
}
