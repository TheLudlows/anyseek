//! Physical package sizing and retrieval address budgets.
pub(super) const PACKAGE_RECORDS: usize = 8192;

/// Compact at the expected full-build package count plus room for small tails.
pub(super) fn package_target(identities: usize) -> usize {
    (identities.div_ceil(PACKAGE_RECORDS) + 2).max(8)
}

/// Interleaved addresses can leave gaps, so bound the dense slot array by bytes.
pub(super) fn address_slots() -> Result<usize, super::RecordError> {
    let slot_bytes = size_of::<String>();
    let bytes = crate::storage::build_budget::single_memory_limit().map_err(super::storage)?;
    let slots = bytes / slot_bytes as u64;
    Ok(usize::try_from(slots).unwrap_or(usize::MAX).min(usize::MAX / slot_bytes))
}

#[cfg(test)]
#[path = "../../tests/common/scale.rs"]
mod scale;

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{Applied, ExpectedVersion, RecordConfig, WriteReceipt};

    fn seed_record(seq: u64) -> Applied {
        let mut request = super::super::tests::write(
            &format!("op-{seq}"), &format!("id-{seq:06}"), "alpha", ExpectedVersion::Absent,
        );
        request.document.as_mut().unwrap().retain(|field, _| field == "body");
        let receipt = WriteReceipt { seq, version: 1, record_id: request.record_id.clone() };
        Applied { observed_epoch: request.operation_id.epoch, observed_time_ms: 0, request, receipt }
    }

    fn lexical_config() -> RecordConfig {
        let mut config = super::super::tests::config();
        config.max_journal_bytes = 256 * 1024 * 1024;
        config.max_candidates = 10_000;
        config.index.inference = crate::vector::Inference::None;
        config.schema.retain(|field| field.index_lexical);
        config.embedding_space_id = None;
        config
    }

    #[test]
    fn package_target_grows_with_the_collection() {
        assert_eq!(package_target(10_000), 8);
        let identities = 200_000usize;
        assert!(package_target(identities) > identities.div_ceil(PACKAGE_RECORDS));
    }

    #[tokio::test]
    async fn collection_writes_queries_and_recovers_above_legacy_count_limits() {
        use super::super::*;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("partition");
        let legacy_limit = scale::choose(10_000, 100_000);
        let mut config = lexical_config();
        config.max_records = legacy_limit;
        config.max_operations = legacy_limit;
        let partition = RecordPartition::create(&path, config.clone()).await.unwrap();
        let mut state = partition.inner.lock().await.state.try_clone().unwrap();
        // Seed canonical state without an fsync for every fixture row. The next
        // write uses the regular write path, and reopening decodes the checkpoint and WAL.
        let seed_count = legacy_limit as u64 + 1;
        for seq in 1..=seed_count {
            apply(&mut state, &seed_record(seq)).unwrap();
        }
        state.admit_bytes(state_proof::length(&state).unwrap()).unwrap();
        partition.inner.lock().await.publish_anchor_state(state, false).await.unwrap();
        let last = tests::write("last", "last", "alpha terminal", ExpectedVersion::Absent);
        let receipt = partition.write(last.clone()).await.unwrap();
        assert_eq!(receipt.seq, seed_count + 1);

        let mut current = partition;
        for reopen in [false, true] {
            if reopen {
                current.finish_service_shutdown().await.unwrap();
                drop(current);
                current = RecordPartition::open(&path, config.max_journal_bytes).await.unwrap();
            }
            let result = current.query(RecordQuery {
                text: "alpha".into(), vector: None, mode: SearchMode::Lexical,
                query_type: QueryType::Intersection, offset: 0, length: 10,
                result_type: ResultType::TopkCount, fields: vec![], filters: vec![],
                sort: vec![], temporal: None,
            }).await.unwrap();
            assert_eq!(result.count_total as u64, seed_count + 1);
            assert_eq!(result.statistics.retained_identities as u64, seed_count + 1);
            assert_eq!(result.statistics.retained_operations as u64, seed_count + 1);
            assert_eq!(result.hits.len(), 10);
            let terminal = current.search("terminal".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap();
            assert_eq!(terminal.hits[0].record.record_id, "last");
            assert_eq!(current.write(last.clone()).await.unwrap(), receipt);
        }
        current.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn recovery_from_wal_builds_multiple_packages_and_adds_a_writable_tail() {
        use super::super::*;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("partition");
        let config = lexical_config();
        let initial = RecordPartition::create(&path, config.clone()).await.unwrap();
        let seed_count = PACKAGE_RECORDS + 1;
        let frames: Vec<_> = (1..=seed_count)
            .map(|seq| encode_entry(&Entry::Applied(seed_record(seq as u64))).unwrap())
            .collect();
        initial.inner.lock().await.journal
            .append_batch(&frames.iter().map(Vec::as_slice).collect::<Vec<_>>()).unwrap();
        initial.finish_service_shutdown().await.unwrap();
        drop(initial);

        let partition = RecordPartition::open(&path, config.max_journal_bytes).await.unwrap();
        assert_eq!(partition.inner.lock().await.index.components().await.len(), 3);
        partition.write(tests::write("new", "new", "alpha", ExpectedVersion::Absent)).await.unwrap();
        let result = partition.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap();
        assert_eq!(result.count_total, seed_count + 1);
        partition.finish_service_shutdown().await.unwrap();
    }
}
