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

    async fn seed_lexical_fixture(partition: &super::super::RecordPartition, rows: usize, matches: usize) {
        use super::super::*;
        const BATCH_ROWS: usize = 1024;
        const MAX_APPLIED_WIRE_BYTES: usize = 1024;
        let mut inner = partition.inner.lock().await;
        assert_eq!(inner.state.seq, 0, "fixture requires a fresh collection");
        let observed_time_ms = inner.observe_time().unwrap();
        let mut state = inner.state.try_clone().unwrap();
        for ordinal in 0..rows {
            if ordinal % BATCH_ROWS == 0 {
                let batch_rows = (rows - ordinal).min(BATCH_ROWS);
                state.admit_replay_frame(batch_rows * MAX_APPLIED_WIRE_BYTES).unwrap();
            }
            let id = format!("r{ordinal:06}");
            let mut body = "common ".repeat(1 + ordinal % 5);
            if ordinal < matches {
                body.push_str("rareterm");
            }
            let mut request = tests::write(&format!("seed-{ordinal}"), &id, &body, ExpectedVersion::Absent);
            request.document.as_mut().unwrap().insert("priority".into(), serde_json::json!(ordinal));
            let receipt = WriteReceipt { seq: ordinal as u64 + 1, version: 1, record_id: id };
            let applied = Applied {
                observed_epoch: request.operation_id.epoch,
                observed_time_ms,
                request,
                receipt,
            };
            assert!(checkpoint_codec::json_length(&applied).unwrap() <= MAX_APPLIED_WIRE_BYTES);
            apply(&mut state, &applied).unwrap();
        }
        assert_eq!(inner.publish_anchor_state(state, false).await.unwrap(), rows as u64);
    }

    #[tokio::test]
    #[ignore = "offline benchmark fixture; requires a fresh disposable collection"]
    async fn prepare_lexical_scale_fixture() {
        use super::super::*;
        let path = std::path::PathBuf::from(std::env::var_os("ANYSEEK_LEXICAL_FIXTURE_PARTITION").unwrap());
        let rows = std::env::var("ANYSEEK_LEXICAL_FIXTURE_ROWS").unwrap().parse::<usize>().unwrap();
        assert!((100..=100_000).contains(&rows));
        let partition = RecordPartition::open(&path, 256 * 1024 * 1024).await.unwrap();
        seed_lexical_fixture(&partition, rows, 100).await;
        partition.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn frozen_membership_and_bm25_survive_retirement_without_query_workspace() {
        use super::super::*;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("partition");
        let mut config = tests::config();
        config.max_journal_bytes = 16 * 1024 * 1024;
        config.schema.push(serde_json::from_value(serde_json::json!({
            "field":"priority", "field_type":"Int", "store":false, "facet":true, "index_lexical":false
        })).unwrap());
        let partition = RecordPartition::create(&path, config.clone()).await.unwrap();
        let rows = 1024;
        seed_lexical_fixture(&partition, rows, 8).await;
        partition.finish_service_shutdown().await.unwrap();
        drop(partition);
        let partition = RecordPartition::open(&path, config.max_journal_bytes).await.unwrap();
        partition.write(tests::write("edit", "r000000", "common rareterm repeated", ExpectedVersion::Match(1)))
            .await.unwrap();
        let mut delete = tests::write("delete", "r000001", "", ExpectedVersion::Match(1));
        delete.document = None;
        partition.write(delete).await.unwrap();

        let query = || RecordQuery {
            text: "rareterm".into(), vector: None, mode: SearchMode::Lexical,
            query_type: QueryType::Union, offset: 0, length: 10, result_type: ResultType::TopkCount,
            fields: vec![], filters: vec![crate::search::FacetFilter::Int {
                field: "priority".into(), filter: 0..5,
            }], sort: vec![], temporal: None,
        };
        let scope = std::sync::Arc::new(crate::resources::Governor::default()).admit(
            "frozen-view".into(), crate::resources::Lane::Foreground, 512 * 1024 * 1024 - 384 * 1024,
        ).unwrap();
        let fast = scope.clone().run(partition.query(query())).await.unwrap();
        assert_eq!(fast.count_total, 3);
        assert_eq!(fast.hits.len(), 3);
        assert_eq!(fast.statistics.physical_versions, rows + 1);

        let mut count = query();
        count.text = "common".into();
        count.length = 0;
        count.result_type = ResultType::Count;
        count.filters = vec![crate::search::FacetFilter::Int {
            field: "priority".into(), filter: 0..rows as i64,
        }];
        let counted = scope.run(partition.query(count)).await.unwrap();
        assert_eq!(counted.count_total, rows - 2);
        assert!(counted.hits.is_empty());
        {
            let inner = partition.inner.lock().await;
            let index = inner.index.test_index().read().await;
            for segment in &index.segments {
                segment.visibility.invalidate();
            }
        }
        let reference = partition.query(query()).await.unwrap();
        let signature = |result: RecordSearch| {
            (result.count_total, result.hits.into_iter().map(|hit| {
                (hit.record.record_id.clone(), hit.score.to_bits())
            }).collect::<Vec<_>>())
        };
        assert_eq!(signature(fast), signature(reference));
        partition.finish_service_shutdown().await.unwrap();
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
        assert_eq!(partition.inner.lock().await.index.component_count().await, 3);
        partition.write(tests::write("new", "new", "alpha", ExpectedVersion::Absent)).await.unwrap();
        let result = partition.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap();
        assert_eq!(result.count_total, seed_count + 1);
        partition.finish_service_shutdown().await.unwrap();
    }
}
