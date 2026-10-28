//! Retrieval pins only index topology while scoring. Record ownership is held by
//! the outer task; canonical records and retirement are checked after scoring.
use super::*;

pub(super) struct Handle {
    pub index: IndexArc,
    // Keep package pins until the shallow index/shard handles are released.
    _pins: Vec<Arc<lexical::runtime::Runtime>>,
    _memory: crate::storage::build_budget::MemoryLease,
    _scope: Option<crate::resources::Lease>,
}
pub(super) struct Candidate {
    identity: lexical::Identity,
    fingerprint: Option<[u8; 32]>,
    hit: crate::min_heap::Result,
}
pub(super) struct Selection {
    rows: BTreeMap<String, ((u64, u64), Option<[u8; 32]>)>,
    _pins: Vec<Arc<lexical::runtime::Runtime>>,
    _memory: crate::storage::build_budget::MemoryLease,
    _scope: Option<crate::resources::Lease>,
}
impl Selection {
    pub async fn revalidate(
        &mut self,
        locations: &BTreeMap<String, usize>,
        components: &[crate::component::ShardArc],
    ) -> Result<(), RecordError> {
        // One read guard per component, not one lock acquisition per record.
        let shards = futures::future::join_all(components.iter().map(|shard| shard.read())).await;
        if shards.iter().any(|shard| shard.failed) { return Err(RecordError::RecoveryRequired); }
        self.rows.retain(|key, (revision, fingerprint)| {
            let Some(&ordinal) = locations.get(key) else { return false; };
            let local = ordinal / shards.len();
            shards[ordinal % shards.len()].active.as_ref().is_some_and(|active| {
                active.is_live(local) && active.keys.get(local) == Some(key)
                    && active.revisions.get(local).is_some_and(|&current| current.0 == revision.0
                        && (current.1 == revision.1 || fingerprint.is_some()
                            && active.fingerprints.get(local).copied().flatten() == *fingerprint))
            })
        });
        Ok(())
    }
    pub fn allows(&self, key: &str) -> bool { self.rows.contains_key(key) }

}
impl Handle {
    pub async fn filter(&self, filters: &[FacetFilter]) -> Result<Selection, RecordError> {
        let root = self.index.read().await;
        let shards =
            futures::future::join_all(root.shard_vec.iter().map(|shard| shard.read())).await;
        if shards.iter().any(|shard| shard.failed) {
            return Err(RecordError::RecoveryRequired);
        }
        let mut bytes = 65536u64;
        for shard in &shards {
            let active = shard
                .active
                .as_ref()
                .ok_or_else(|| storage("filter statistics missing"))?;
            for key in &active.keys {
                bytes = bytes
                    .checked_add(key.len() as u64 * 4 + 256)
                    .ok_or_else(|| storage("filter selection budget overflow"))?;
            }
        }
        let memory = crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
        let scope = crate::resources::Scope::current()
            .map(|scope| scope.bytes(bytes))
            .transpose()
            .map_err(storage)?;
        let mut rows = BTreeMap::new();
        for shard in &shards {
            let active = shard.active.as_ref().unwrap();
            let filters = crate::metadata_filter::compile_filters(shard, filters)
                .map_err(RecordError::Rejected)?;
            for (local, key) in active.keys.iter().enumerate() {
                if active.is_live(local)
                    && !crate::metadata_filter::is_facet_filter(shard, &filters, local)
                {
                    let revision = *active.revisions.get(local).ok_or_else(|| storage("filter revision missing"))?;
                    rows.insert(key.clone(), (revision, active.fingerprints.get(local).copied().flatten()));
                }
            }
        }
        Ok(Selection {
            rows,
            _pins: root.record_lexical.clone(),
            _memory: memory,
            _scope: scope,
        })
    }
    pub async fn capture(index: &IndexArc, length: usize) -> Result<Self, RecordError> {
        let root = index.read().await;
        let metadata = checkpoint_codec::json_length(&(&root.meta, &root.schema_map))? as u64;
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
            .map_err(storage)?;
        Ok(Self {
            index: Arc::new(tokio::sync::RwLock::new(root.clone())),
            _pins: root.record_lexical.clone(),
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
            futures::future::join_all(root.shard_vec.iter().map(|shard| shard.read())).await;
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
                fingerprint: active.fingerprints.get(local).copied().flatten(),
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
    ) -> Result<RecordSearch, RecordError> {
        let (handle, evaluation, barrier, root) = {
            let mut inner = self.inner.lock().await;
            inner.advance_time().await?;
            if !inner.healthy || inner.state.sealed_for.is_some() || self.indexes.error().is_some()
            {
                return Err(RecordError::RecoveryRequired);
            }
            validate(&query, &inner.config)?;
            let handle = inner.index.capture_query(query.length).await?;
            (
                handle,
                inner.evaluation_time_ms,
                self.output_permit()?,
                inner.journal.root().to_owned(),
            )
        };
        #[cfg(test)]
        pause(&root, ".test-query-scoring-pause").await?;
        let _ = root;
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
        let mut result = lexical::runtime_query::search(
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
        )
        .await?;
        if let Some(error) = result.error.take() {
            return Err(storage(error));
        }
        let candidates = handle
            .candidates(std::mem::take(&mut result.results))
            .await?;
        #[cfg(test)]
        pause(&root, ".test-query-candidates-pause").await?;
        let mut inner = self.inner.lock().await;
        inner.advance_time().await?;
        if !inner.healthy || inner.state.sealed_for.is_some() || self.indexes.error().is_some() {
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
            let Some(record) = inner.state.records.get(&candidate.identity.id) else {
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
                record: record.clone(),
                score: candidate.hit.score,
            });
        }
        barrier.deliver(|| ())?;
        Ok(RecordSearch {
            fusion: None,
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
    if let SearchMode::WeightedHybrid { rrf, .. } = &query.mode {
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
async fn pause(root: &Path, name: &str) -> Result<(), RecordError> {
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
    use super::super::tests::{config, source_write, write};
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
    async fn candidate_and_filter_revalidation_fence_failed_component_after_capture() {
        let temporary = tempfile::tempdir().unwrap();
        let p = RecordPartition::create(&temporary.path().join("records"), config()).await.unwrap();
        p.write(write("a", "a", "alpha", ExpectedVersion::Absent)).await.unwrap();
        let inner = p.inner.lock().await;
        let handle = inner.index.capture_query(10).await.unwrap();
        let mut selection = handle.filter(&[]).await.unwrap();
        let components = inner.index.components().await;
        components[0].write().await.failed = true;
        let identity = lexical::Identity { id: "a".into(), version: 1, revision: 1 };
        assert!(matches!(inner.index.matches_identity(0, &identity, None).await,
            Err(RecordError::RecoveryRequired)));
        assert!(matches!(selection.revalidate(&inner.locations, &components).await,
            Err(RecordError::RecoveryRequired)));
    }

    #[tokio::test]
    async fn independent_lexical_order_and_topology_join_by_revision() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        for id in ["a", "b"] {
            p.write(write(id, id, "alpha", ExpectedVersion::Absent)).await.unwrap();
        }
        let handle = p.inner.lock().await.index.capture_query(10).await.unwrap();
        let one = lexical::runtime::Runtime::create(&temporary.path().join("lexical-one"), &config()).unwrap();
        let two = lexical::runtime::Runtime::create(&temporary.path().join("lexical-two"), &config()).unwrap();
        let doc = write("unused", "unused", "alpha", ExpectedVersion::Absent).document.unwrap();
        one.append(lexical::Identity { id: "b".into(), version: 1, revision: 2 }, &doc, true).await.unwrap();
        // Same body version, stale enrichment revision must not join the live row.
        one.append(lexical::Identity { id: "a".into(), version: 1, revision: 0 }, &doc, true).await.unwrap();
        two.append(lexical::Identity { id: "a".into(), version: 1, revision: 1 }, &doc, true).await.unwrap();
        one.seal().await.unwrap(); two.seal().await.unwrap();
        // Open immutable packages again to exercise persisted identity addresses.
        let one = lexical::runtime::Runtime::open(&temporary.path().join("lexical-one"), &config()).unwrap();
        let two = lexical::runtime::Runtime::open(&temporary.path().join("lexical-two"), &config()).unwrap();
        handle.index.write().await.record_lexical = vec![one, two];
        let result = lexical::runtime_query::search(&handle.index, "alpha".into(),
            Some(Embedding::F32(vec![1.0, 0.0])), SearchMode::Hybrid {
                similarity_threshold: None, ann_mode: crate::vector_similarity::AnnMode::All },
            QueryType::Intersection, 0, 10, ResultType::TopkCount, vec![], vec![], vec![],
            lexical::Cut { sequence: u64::MAX, evaluation_time_ms: u64::MAX }).await.unwrap();
        assert_eq!(result.result_count_total, 2);
        let identities = handle.candidates(result.results).await.unwrap().into_iter()
            .map(|candidate| candidate.identity.id).collect::<BTreeSet<_>>();
        assert_eq!(identities, BTreeSet::from(["a".into(), "b".into()]));
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
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        pipeline
            .submit(vec![write("first", "a", "alpha", ExpectedVersion::Absent)])
            .await
            .unwrap();
        pipeline.finish().await.unwrap();
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
                    },
                    vec![],
                    10,
                )
                .await
            })
        };
        entered(&root, gate).await;
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            pipeline
                .submit(vec![write("second", "b", "alpha", ExpectedVersion::Absent)])
                .await
                .unwrap();
            pipeline.finish().await.unwrap();
        })
        .await
        .expect("scoring must not own the Record mutex");
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
        let pipeline = IngestPipeline::open(p.clone()).await.unwrap();
        pipeline
            .submit(vec![
                write("a", "a", "alpha", ExpectedVersion::Absent),
                write("b", "b", "alpha", ExpectedVersion::Absent),
            ])
            .await
            .unwrap();
        pipeline.finish().await.unwrap();
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
            pipeline
                .submit(vec![
                    write("update", "a", "beta", ExpectedVersion::Match(1)),
                    delete,
                ])
                .await
                .unwrap();
            pipeline.finish().await.unwrap();
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
    #[tokio::test]
    async fn source_deletion_still_fences_a_query_scored_outside_the_record_guard() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("records");
        let p = RecordPartition::create(&root, config()).await.unwrap();
        p.write(source_write("s", "s", &[])).await.unwrap();
        p.write(source_write("d", "d", &[("s", 1)])).await.unwrap();
        let gate = ".test-query-candidates-pause";
        std::fs::write(root.join(gate), b"pause").unwrap();
        let pending = {
            let p = p.clone();
            tokio::spawn(async move {
                p.search("evidence".into(), None, SearchMode::Lexical, vec![], 10)
                    .await
            })
        };
        entered(&root, gate).await;
        let mut delete = write("delete", "s", "", ExpectedVersion::Match(1));
        delete.document = None;
        tokio::time::timeout(std::time::Duration::from_secs(3), p.write(delete))
            .await
            .unwrap()
            .unwrap();
        std::fs::remove_file(root.join(gate)).unwrap();
        assert!(matches!(
            pending.await.unwrap(),
            Err(RecordError::RecoveryRequired)
        ));
    }
}
