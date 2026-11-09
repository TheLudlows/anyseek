//! Retrieval pins only index topology while scoring. Record ownership is held by
//! the outer task; canonical records and retirement are checked after scoring.
use super::*;

pub(super) struct Handle {
    pub index: VectorIndexArc,
    // Keep package pins until the shallow index/shard handles are released.
    _pins: Vec<Arc<lexical::runtime::Runtime>>,
    _memory: crate::storage::build_budget::MemoryLease,
    _scope: Option<crate::resources::Lease>,
}
pub(super) struct Candidate {
    identity: lexical::Identity,
    fingerprint: [u8; 32],
    hit: crate::min_heap::Result,
}
impl Handle {
    pub async fn capture(index: &VectorIndexArc, length: usize) -> Result<Self, RecordError> {
        let root = index.read().await;
        let metadata = checkpoint_codec::json_length(&(&root.meta, &root.schema_map))? as u64;
        // Handle/candidate scaffolding only. Result documents are measured and
        // admitted separately before copying, with leases owned by QueryRecord.
        let buffers = (length as u64)
            .checked_mul(2048)
            .and_then(|n| n.checked_add(65536))
            .ok_or_else(|| storage("query handle budget overflow"))?;
        let quant = (root.turbo_quant.seed_mask.len() as u64)
            .checked_mul(4)
            .ok_or_else(|| storage("query handle budget overflow"))?;
        let bytes = metadata
            .checked_mul(8)
            .and_then(|n| n.checked_add(quant))
            .and_then(|n| n.checked_add(buffers))
            .ok_or_else(|| storage("query handle budget overflow"))?;
        let memory = crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
        let scope = crate::resources::Scope::current()
            .map(|scope| scope.bytes(bytes))
            .transpose()
            .map_err(|_| RecordError::Busy)?;
        Ok(Self {
            index: Arc::new(tokio::sync::RwLock::new(root.clone())),
            _pins: root.segments.iter().map(|segment| segment.lexical.clone()).collect(),
            _memory: memory,
            _scope: scope,
        })
    }
    async fn candidates(
        &self,
        hits: Vec<crate::min_heap::Result>,
    ) -> Result<Vec<Candidate>, RecordError> {
        let root = self.index.read().await;
        let shards =
            futures::future::join_all(root.segments.iter().map(|segment| segment.vector.read())).await;
        if shards.iter().any(|shard| shard.failed) {
            return Err(RecordError::RecoveryRequired);
        }
        let n = shards.len();
        let mut candidates = Vec::with_capacity(hits.len());
        for hit in hits {
            let slot = hit.doc_id % n;
            let local = hit.doc_id / n;
            let active = shards[slot]
                .active
                .as_ref()
                .ok_or_else(|| storage("query component statistics missing"))?;
            let key = active
                .keys
                .get(local)
                .ok_or_else(|| storage("candidate ordinal outside captured topology"))?;
            candidates.push(Candidate {
                identity: lexical::Identity {
                    id: key.clone(),
                    version: active.revisions.get(local).ok_or_else(|| storage("candidate revision missing"))?.0,
                    revision: active.revisions[local].1,
                },
                fingerprint: active.fingerprints[local],
                hit,
            });
        }
        Ok(candidates)
    }
}

impl RecordPartition {
    pub(super) async fn query_independent(
        &self,
        mut query: RecordQuery,
        output_fields: Option<&[String]>,
    ) -> Result<RecordSearch, RecordError> {
        let (handle, evaluation, has_ttl, barrier, root) = {
            let mut inner = self.inner.lock().await;
            inner.observe_time()?;
            if !inner.healthy || self.indexes.error().is_some()
            {
                return Err(RecordError::RecoveryRequired);
            }
            validate(&query, &inner.config)?;
            let handle = inner.index.capture_query(query.length).await?;
            (
                handle,
                inner.evaluation_time_ms,
                inner.state.records.live_ttl_count()>0,
                self.output_permit()?,
                inner.journal.root().to_owned(),
            )
        };
        #[cfg(test)]
        pause(&root, ".test-query-scoring-pause").await?;
        let _ = root;
        if has_ttl {query.filters.push(temporal::expiry_filter(evaluation));}
        if let Some(temporal) = query.temporal.take() {
            query.filters.extend(temporal::filters(temporal)?);
        }
        let channel = match &query.mode {
            SearchMode::Lexical => Some(if query.text.trim().is_empty() {
                "browse"
            } else {
                "lexical"
            }),
            SearchMode::Vector { .. } => Some("vector"),
            _ => None,
        };
        let rank_basis = if !query.sort.is_empty() {
            "requested_order"
        } else if channel == Some("browse") {
            "browse_order"
        } else {
            "score"
        };
        let offset = query.offset;
        // query_output already owns partition admission and the service resource
        // scope. Keep that admission until this one blocking worker joins; do
        // not impose a second process-wide limit across unrelated collections.
        let context = crate::resources::Scope::current();
        let runtime = tokio::runtime::Handle::current();
        let owner = self.clone();
        #[cfg(test)] let scoring_root = root.clone();
        let (handle, result) = crate::resources::spawn_blocking(move || {
            let _owner = owner;
            #[cfg(test)]
            blocking_scoring_pause(&scoring_root);
            let work = async move {
                let result = lexical::runtime_query::search(
                    &handle.index,
                    query.text,
                    query.vector,
                    query.mode,
                    query.query_type,
                    query.offset,
                    query.length,
                    query.result_type,
                    query.fields,
                    query.filters,
                    query.sort,
                    lexical::Cut {
                        sequence: u64::MAX,
                        evaluation_time_ms: evaluation,
                    },
                ).await;
                (handle, result)
            };
            runtime.block_on(async move {
                match context {
                    Some(context) => context.run(work).await,
                    None => work.await,
                }
            })
        }).await.map_err(storage)?;
        let mut result = result?;
        let candidates = handle
            .candidates(std::mem::take(&mut result.results))
            .await?;
        #[cfg(test)]
        pause(&root, ".test-query-candidates-pause").await?;
        let mut inner = self.inner.lock().await;
        inner.observe_time()?;
        if !inner.healthy || self.indexes.error().is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        let mut hits = Vec::with_capacity(candidates.len());
        for (rank, candidate) in candidates.into_iter().enumerate() {
            let Some(&ordinal) = inner.locations.get(&candidate.identity.id) else {
                continue;
            };
            if !inner
                .index
                .matches_identity(ordinal, &candidate.identity, candidate.fingerprint)
                .await?
            {
                continue;
            }
            let Some(record) = inner.state.records.get(&candidate.identity.id).filter(|r|temporal::visible(r,inner.evaluation_time_ms)) else {
                continue;
            };
            let channels = if let Some(channel) = channel {
                vec![crate::search::ChannelExplanation {
                    channel: channel.into(),
                    rank: offset + rank,
                    rank_basis: rank_basis.into(),
                    raw_score: if channel == "browse" {
                        None
                    } else {
                        Some(candidate.hit.score)
                    },
                    rrf_contribution: None,
                }]
            } else {
                result
                    .explanations
                    .remove(&candidate.hit.doc_id)
                    .ok_or_else(|| storage("hybrid explanation missing"))?
            };
            hits.push(RecordHit {
                channels,
                record: QueryRecord::copy(record, output_fields)?,
                score: candidate.hit.score,
            });
        }
        barrier.deliver(|| ())?;
        Ok(RecordSearch {
            evaluation_time_ms: inner.evaluation_time_ms,
            count_total: result.result_count_total,
            query_terms: result.query_terms,
            visible_seq: inner.state.seq,
            hits,
            statistics: inner.statistics(),
        })
    }
}
fn validate(query: &RecordQuery, config: &RecordConfig) -> Result<(), RecordError> {
    if query
        .offset
        .checked_add(query.length)
        .is_none_or(|n| n > config.max_candidates)
    {
        return Err(RecordError::Rejected("candidate budget exceeded".into()));
    }
    if let SearchMode::Hybrid { rrf, .. } = &query.mode {
        if rrf
            .keyword_candidates
            .is_some_and(|n| n > config.max_candidates)
            || rrf
                .vector_candidates
                .is_some_and(|n| n > config.max_candidates)
        {
            return Err(RecordError::Rejected(
                "fusion candidate budget exceeded".into(),
            ));
        }
    }
    Ok(())
}
#[cfg(test)]
pub(super) async fn pause(root: &Path, name: &str) -> Result<(), RecordError> {
    let gate = root.join(name);
    if gate.exists() {
        std::fs::write(root.join(format!("{name}-entered")), b"ready").map_err(storage)?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while gate.exists() {
            if std::time::Instant::now() > deadline {
                return Err(storage("query test pause timed out"));
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::tests::{config, write};
    use super::*;
    use std::collections::BTreeSet;
    async fn entered(root: &Path, name: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !root.join(format!("{name}-entered")).exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn euclidean_cached_rows_and_rebuild_inputs_require_safe_numbers() {
        use crate::component::{create_index, open_index, Commit};
        use crate::vector::{Inference, Quantization, VectorHeader};
        use crate::vector_similarity::VectorSimilarity;
        for quantization in [Quantization::None, Quantization::ScalarQuantizationI8, Quantization::TurboQuantI8] {
            let root = tempfile::tempdir().unwrap();
            let mut config = crate::record::tests::config();
            if let Inference::External { similarity, quantization: q, .. } = &mut config.index.inference {
                *similarity = VectorSimilarity::Euclidean; *q = quantization;
            }
            let schema = super::super::retrieval_schema(&config);
            let path = root.path().join("component");
            let index = create_index(&path, config.index.clone(), &schema, &config).await.unwrap();
            let component = index.read().await.segments[0].vector.clone();
            let bad = serde_json::from_value(serde_json::json!({"vector":[1e19,0.0]})).unwrap();
            assert!(component.write().await.append(bad).await.is_err());
            assert_eq!(component.read().await.indexed_doc_count, 0);
            component.write().await.append(serde_json::from_value(serde_json::json!({"vector":[1.0,0.0]})).unwrap()).await.unwrap();
            index.commit().await.unwrap();
            // Corrupt only numeric row content, retaining valid file structure.
            // The same lower-level check protects old, authenticated cache data.
            index.read().await.segments[0].lexical.seal().await.unwrap();
            drop(component); drop(index);
            let vector_path = path.join("vectors.bin");
            let original = std::fs::read(&vector_path).unwrap();
            let row = 8; // one level, one cluster, one row
            for value in [f32::INFINITY, 1e19] {
                let mut bytes = original.clone();
                if quantization == Quantization::None {
                    let start = row + size_of::<VectorHeader>();
                    bytes[start..start+4].copy_from_slice(&value.to_ne_bytes());
                } else {
                    let header = bytemuck::pod_read_unaligned::<VectorHeader>(&bytes[row..row+size_of::<VectorHeader>()]);
                    let header = VectorHeader { scale: value, ..header };
                    bytes[row..row+size_of::<VectorHeader>()].copy_from_slice(bytemuck::bytes_of(&header));
                }
                std::fs::write(&vector_path, bytes).unwrap();
                assert!(matches!(open_index(&path, &config).await, Err(RecordError::Rejected(_))));
            }
        }
    }

    #[tokio::test]
    async fn captured_query_fences_failed_components_and_bounds_output() {
        let temporary = tempfile::tempdir().unwrap();
        let mut settings = config();
        settings.max_record_bytes = 1024 * 1024;
        settings.max_journal_bytes = 64 * 1024 * 1024;
        let p = RecordPartition::create(&temporary.path().join("records"), settings).await.unwrap();
        let mut request = write("a", "a", "alpha", ExpectedVersion::Absent);
        request.document.as_mut().unwrap().insert("payload".into(), serde_json::json!("x".repeat(1024 * 1024 - 1024)));
        p.write(request).await.unwrap();
        let inner = p.inner.lock().await;
        let handle = inner.index.capture_query(10).await.unwrap();
        let components = inner.index.components().await;
        components[0].write().await.failed = true;
        let identity = lexical::Identity { id: "a".into(), version: 1, revision: 1 };
        assert!(matches!(inner.index.matches_identity(0, &identity, [0; 32]).await,
            Err(RecordError::RecoveryRequired)));
        assert!(matches!(handle.candidates(vec![]).await,
            Err(RecordError::RecoveryRequired)));
        let failed = lexical::runtime_query::search(&handle.index, String::new(),
            Some(Embedding::F32(vec![1.0, 0.0])), SearchMode::Vector {
                similarity_threshold: None, ann_mode: crate::vector_similarity::AnnMode::All },
            QueryType::Union, 0, 10, ResultType::TopkCount, vec![], vec![], vec![],
            lexical::Cut { sequence: u64::MAX, evaluation_time_ms: inner.evaluation_time_ms }).await;
        assert!(matches!(failed, Err(RecordError::RecoveryRequired)));
        // Exhaust only the caller's byte allowance; the same captured index is healthy.
        components[0].write().await.failed = false;
        let scope = Arc::new(crate::resources::Governor::default())
            .admit("query-bytes".into(), crate::resources::Lane::Foreground, 512 * 1024 * 1024).unwrap();
        assert!(matches!(scope.clone().run(Handle::capture(&handle.index, 10)).await, Err(RecordError::Busy)));
        drop(inner);
        drop(scope);
        let scope = Arc::new(crate::resources::Governor::default())
            .admit("output-bytes".into(), crate::resources::Lane::Foreground, 496 * 1024 * 1024).unwrap();
        let query = || RecordQuery { temporal:None, text:"alpha".into(), vector:None,
            mode:SearchMode::Lexical, query_type:QueryType::Union, offset:0, length:10,
            result_type:ResultType::TopkCount, fields:vec![], filters:vec![], sort:vec![] };
        let projected = scope.clone().run(p.query_output(query(), Some(vec!["body".into()]))).await.unwrap();
        assert_eq!(projected.count_total, 1);
        assert_eq!(projected.hits[0].record.document.as_ref().unwrap().len(), 1);
        assert_eq!(projected.hits[0].record.document.as_ref().unwrap()["body"], "alpha");
        assert!(matches!(scope.clone().run(p.query(query())).await, Err(RecordError::Busy)),
            "full body copy must be rejected by its real output size");
        drop(projected);
        let complete = p.query(query()).await.unwrap();
        assert_eq!(complete.hits[0].record.document.as_ref().unwrap()["payload"].as_str().unwrap().len(), 1024 * 1024 - 1024);


    }

    #[tokio::test]
    async fn independent_lexical_order_joins_by_revision() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        for id in ["a", "b"] {
            p.write(write(id, id, "alpha", ExpectedVersion::Absent)).await.unwrap();
        }
        let handle = p.inner.lock().await.index.capture_query(10).await.unwrap();
        let one = lexical::runtime::Runtime::create(&temporary.path().join("lexical-one"), &config()).unwrap();
        let doc = write("unused", "unused", "alpha", ExpectedVersion::Absent).document.unwrap();
        one.append(lexical::Identity { id: "b".into(), version: 1, revision: 2 }, &doc, true).await.unwrap();
        // Same body version, stale enrichment revision must not join the live row.
        one.append(lexical::Identity { id: "a".into(), version: 1, revision: 0 }, &doc, true).await.unwrap();
        one.append(lexical::Identity { id: "a".into(), version: 1, revision: 1 }, &doc, true).await.unwrap();
        one.seal().await.unwrap();
        // Open immutable packages again to exercise persisted identity addresses.
        let one = lexical::runtime::Runtime::open(&temporary.path().join("lexical-one"), &config()).unwrap();
        assert_eq!(one.snapshot().await.unwrap().identities.len(), 3,
            "the stale lexical revision remains present for join validation");
        handle.index.write().await.segments[0].lexical = one;
        let result = lexical::runtime_query::search(&handle.index, "alpha".into(),
            Some(Embedding::F32(vec![1.0, 0.0])), SearchMode::Hybrid {
                similarity_threshold: None, ann_mode: crate::vector_similarity::AnnMode::All, rrf: Default::default() },
            QueryType::Intersection, 0, 10, ResultType::TopkCount, vec![], vec![], vec![],
            lexical::Cut { sequence: u64::MAX, evaluation_time_ms: u64::MAX }).await.unwrap();
        assert_eq!(result.result_count_total, 2);
        let identities = handle.candidates(result.results).await.unwrap().into_iter()
            .map(|candidate| candidate.identity.id).collect::<BTreeSet<_>>();
        assert_eq!(identities, BTreeSet::from(["a".into(), "b".into()]));
    }

    #[tokio::test]
    async fn native_join_accepts_lag_but_rejects_unselected_vector_duplicates() {
        let temporary = tempfile::tempdir().unwrap();
        let p = RecordPartition::create(&temporary.path().join("records"), config()).await.unwrap();
        for id in ["a", "b"] {
            p.write(write(id, id, "alpha", ExpectedVersion::Absent)).await.unwrap();
        }
        let handle = p.inner.lock().await.index.capture_query(10).await.unwrap();
        let native = lexical::runtime::Runtime::create(&temporary.path().join("lagging"), &config()).unwrap();
        let doc = write("unused", "unused", "alpha", ExpectedVersion::Absent).document.unwrap();
        native.append(lexical::Identity {id:"a".into(), version:1, revision:1}, &doc, true).await.unwrap();
        native.seal().await.unwrap();
        handle.index.write().await.segments[0].lexical = native;
        for (keys, revisions, duplicate) in [
            (["a", "b"], [(1,1), (1,2)], false),
            (["a", "a"], [(1,1), (1,2)], false),
            (["z", "z"], [(1,7), (1,7)], true),
        ] {
            {
                let root = handle.index.read().await;
                let mut shard = root.segments[0].vector.write().await;
                let active = shard.active.as_mut().unwrap();
                active.keys = keys.into_iter().map(str::to_owned).collect();
                active.revisions = revisions.to_vec();
            }
            let result = lexical::runtime_query::search(&handle.index, "alpha".into(), None,
                SearchMode::Lexical, QueryType::Union, 0, 10, ResultType::Count,
                vec![], vec![], vec![], lexical::Cut {sequence:u64::MAX, evaluation_time_ms:u64::MAX}).await;
            if duplicate {
                assert!(matches!(result, Err(RecordError::Storage(message))
                    if message == "duplicate live vector revision identity"));
            } else { assert_eq!(result.unwrap().result_count_total, 1); }
        }
        // Restore the test-mutated metadata before ordinary shutdown.
        {
            let root = handle.index.read().await;
            let mut shard = root.segments[0].vector.write().await;
            let active = shard.active.as_mut().unwrap();
            active.keys = vec!["a".into(), "b".into()];
            active.revisions = vec![(1,1), (1,2)];
        }
        drop(handle);
        p.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn candidate_deadline_is_rechecked_after_scoring_without_persistence() {
        struct Clock(std::sync::atomic::AtomicU64);
        impl temporal::Clock for Clock {fn now_ms(&self)->Result<u64,RecordError>{Ok(self.0.load(std::sync::atomic::Ordering::SeqCst))}}
        let temporary=tempfile::tempdir().unwrap();let root=temporary.path().join("records");
        let p=RecordPartition::create(&root,config()).await.unwrap();
        let clock=Arc::new(Clock(std::sync::atomic::AtomicU64::new(1000)));
        p.inner.lock().await.clock=clock.clone();
        let mut request=write("a","a","alpha",ExpectedVersion::Absent);
        request.temporal=Some(TemporalMetadata{expire_at_ms:Some(1100),..Default::default()});
        p.write(request).await.unwrap();
        {
            let inner = p.inner.lock().await;
            assert_eq!(inner.state.records.live_ttl_count(), 1);
            assert!(!inner.state.records.may_have_due_ttl(inner.evaluation_time_ms));
            assert_eq!(inner.statistics().active_records, 1, "future TTL uses the cheap count");
        }
        let before = p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap();
        assert_eq!(before.statistics.active_records, 1);
        assert_eq!(before.hits.len(), 1);
        let gate=".test-query-candidates-pause";std::fs::write(root.join(gate),b"pause").unwrap();
        let pending={let p=p.clone();tokio::spawn(async move {p.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await})};
        entered(&root,gate).await;
        // Model encoding/upload: writer is held, but Inner remains available.
        let writer = p.writer.lock().await;
        clock.0.store(1100,std::sync::atomic::Ordering::SeqCst);
        std::fs::remove_file(root.join(gate)).unwrap();
        let result=tokio::time::timeout(std::time::Duration::from_secs(3),pending)
            .await.expect("final TTL revalidation must not wait for writer").unwrap().unwrap();
        assert!(result.hits.is_empty());
        assert_eq!(result.statistics.active_records,0);
        let inner = p.inner.lock().await;
        assert_eq!(inner.state.seq,1,"deadline revalidation is read-only");
        assert_eq!(inner.state.time_watermark_ms,1000);
        assert!(inner.state.records["a"].document.is_some());
        assert!(inner.state.records["a"].expired_at_ms.is_none());
        drop(inner);
        drop(writer);
    }

    #[tokio::test]
    async fn captured_candidates_survive_cache_rebuild_with_new_physical_rows() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(write("a", "a", "alpha", ExpectedVersion::Absent)).await.unwrap();
        p.write(write("b", "b", "beta", ExpectedVersion::Absent)).await.unwrap();
        let gate = ".test-query-candidates-pause";
        std::fs::write(root.join(gate), b"pause").unwrap();
        let pending = { let p = p.clone(); tokio::spawn(async move {
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await
        }) };
        entered(&root, gate).await;
        {
            let mut inner = p.inner.lock().await;
            // Rebuild uses sequence 2, while a's captured indexing revision is 1.
            let (index, ordinals) = build(&root, &inner.config, &inner.state).await.unwrap();
            inner.index = index;
            inner.locations = ordinals.iter().enumerate().map(|(i, id)| (id.clone(), i)).collect();
            inner.ordinals = ordinals;
        }
        std::fs::remove_file(root.join(gate)).unwrap();
        let result = pending.await.unwrap().unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].record.record_id, "a");
    }

    #[tokio::test]
    async fn captured_candidate_rejects_same_body_version_vector_patch() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        let mut initial = write("initial", "a", "alpha", ExpectedVersion::Absent);
        initial.document.as_mut().unwrap().shift_remove("vector");
        p.write(initial).await.unwrap();
        let gate = ".test-query-candidates-pause";
        std::fs::write(root.join(gate), b"pause").unwrap();
        let pending = { let p = p.clone(); tokio::spawn(async move {
            p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await
        }) };
        entered(&root, gate).await;
        let mut patch = write("patch", "a", "", ExpectedVersion::Match(1));
        patch.document = None;
        patch.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector": [1.0, 0.0]})).unwrap());
        let receipt = p.write(patch).await.unwrap();
        assert_eq!(receipt.version, 1);
        std::fs::remove_file(root.join(gate)).unwrap();
        assert!(pending.await.unwrap().unwrap().hits.is_empty());
        assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 1);
    }

    #[tokio::test]
    async fn scoring_releases_record_guard_and_accepts_newer_cache_publication() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();

        p
            .commit_writes(vec![write("first", "a", "alpha", ExpectedVersion::Absent)])
            .await
            .unwrap();
        p.inner.lock().await.index.drain_indexes().await.unwrap();
        let gate = ".test-query-scoring-pause";
        std::fs::write(root.join(gate), b"pause").unwrap();
        let pending = {
            let p = p.clone();
            tokio::spawn(async move {
                p.search(
                    "alpha".into(),
                    Some(Embedding::F32(vec![1.0, 0.0])),
                    SearchMode::Hybrid {
                        similarity_threshold: None,
                        ann_mode: crate::vector_similarity::AnnMode::All,
                        rrf: Default::default(),
                    },
                    vec![],
                    10,
                )
                .await
            })
        };
        entered(&root, gate).await;
        // Check the mutex directly: durable commit/native flush time is not a
        // mutex latency assertion. Initial consumers have already been drained.
        drop(p.inner.try_lock().expect("paused scoring must not own the Record mutex"));
        // Publication must still complete within the existing 20-second query
        // pause window, before releasing the query or checking its newer view.
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            p
                .commit_writes(vec![write("second", "b", "alpha", ExpectedVersion::Absent)])
                .await
                .unwrap();
            p.inner.lock().await.index.drain_indexes().await.unwrap();
        })
        .await
        .expect("write and index publication must finish while scoring is paused");
        std::fs::remove_file(root.join(gate)).unwrap();
        let result = pending.await.unwrap().unwrap();
        assert_eq!(result.visible_seq, 2);
        assert_eq!(result.hits.len(), 2);
    }
    #[tokio::test]
    async fn candidate_revalidation_drops_updated_and_deleted_rows_without_full_state_clone() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();

        p
            .commit_writes(vec![
                write("a", "a", "alpha", ExpectedVersion::Absent),
                write("b", "b", "alpha", ExpectedVersion::Absent),
            ])
            .await
            .unwrap();
        p.inner.lock().await.index.drain_indexes().await.unwrap();
        let gate = ".test-query-candidates-pause";
        std::fs::write(root.join(gate), b"pause").unwrap();
        let pending = {
            let p = p.clone();
            tokio::spawn(async move {
                p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10)
                    .await
            })
        };
        entered(&root, gate).await;
        let mut delete = write("delete", "b", "", ExpectedVersion::Match(1));
        delete.document = None;
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            p
                .commit_writes(vec![
                    write("update", "a", "beta", ExpectedVersion::Match(1)),
                    delete,
                ])
                .await
                .unwrap();
            p.inner.lock().await.index.drain_indexes().await.unwrap();
        })
        .await
        .unwrap();
        std::fs::remove_file(root.join(gate)).unwrap();
        let result = pending.await.unwrap().unwrap();
        assert!(result.hits.is_empty());
        assert_eq!(
            result.count_total, 2,
            "count describes the observed index candidates"
        );
        assert_eq!(result.visible_seq, 4);
    }

}

#[cfg(test)]
fn blocking_scoring_pause(root: &Path) {
    let marker = root.join(".test-blocking-scoring-pause");
    if !marker.exists() { return; }
    std::fs::write(marker.with_extension("started"), b"ready").unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while marker.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

#[cfg(test)]
#[tokio::test(flavor = "current_thread")]
async fn blocking_scoring_keeps_single_thread_timer_and_small_task_responsive() {
    use super::tests::{config, write};
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    p.write(write("timer", "row", "alpha", ExpectedVersion::Absent)).await.unwrap();
    let marker = root.join(".test-blocking-scoring-pause");
    std::fs::write(&marker, b"pause").unwrap();
    let task = { let p = p.clone(); tokio::spawn(async move {
        p.query_independent(RecordQuery {
            text: "alpha".into(), vector: None, query_type: QueryType::Intersection,
            mode: SearchMode::Lexical, offset: 0, length: 10, result_type: ResultType::TopkCount,
            fields: vec![], filters: vec![], sort: vec![], temporal: None,
        }, None).await
    }) };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !marker.with_extension("started").exists() { tokio::task::yield_now().await; }
        let start = std::time::Instant::now();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(tokio::spawn(async { 42 }).await.unwrap(), 42);
        assert!(!task.is_finished());
    }).await.unwrap();
    std::fs::remove_file(marker).unwrap();
    assert_eq!(task.await.unwrap().unwrap().hits.len(), 1);
}

#[cfg(test)]
#[tokio::test(flavor = "current_thread")]
async fn cancelled_scoring_retains_request_slot_until_worker_finishes() {
    use super::tests::{config, write};
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("records");
    let p = RecordPartition::create(&root, config()).await.unwrap();
    p.write(write("cancel", "row", "alpha", ExpectedVersion::Absent)).await.unwrap();
    let available = p.admission.available_permits();
    let marker = root.join(".test-blocking-scoring-pause");
    std::fs::write(&marker, b"pause").unwrap();
    let caller = { let p = p.clone(); tokio::spawn(async move {
        p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await
    }) };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while !marker.with_extension("started").exists() { tokio::task::yield_now().await; }
    }).await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(p.admission.available_permits(), available - 1);
    std::fs::remove_file(marker).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while p.admission.available_permits() != available { tokio::task::yield_now().await; }
    }).await.unwrap();
    assert_eq!(p.search("alpha".into(), None, SearchMode::Lexical, vec![], 10).await.unwrap().hits.len(), 1);
}
