//! Record-only query execution: native lexical postings, existing vector/metadata
//! component, possibly lagging native prefixes, and one global rank fusion.
use super::*;
use crate::{
    index::VectorIndexArc,
    search::{
        ChannelExplanation, FacetFilter, QueryType, ResultObject, ResultSort, ResultSortIndex,
        ResultType, RrfOptions, SearchMode, SortOrder, rrf_contribution,
    },
    vector::Embedding,
};

pub(crate) async fn search(
    index: &VectorIndexArc,
    text: String,
    vector: Option<Embedding>,
    mode: SearchMode,
    query_type: QueryType,
    offset: usize,
    length: usize,
    result_type: ResultType,
    fields: Vec<String>,
    filters: Vec<FacetFilter>,
    sort: Vec<ResultSort>,
    cut: Cut,
    captured: &mut Option<Vec<super::super::query::Candidate>>,
) -> Result<ResultObject, RecordError> {
    *captured = None;
    {
        let root = index.read().await;
        let config = &root.segments.first().ok_or_else(|| storage("empty retrieval view"))?.lexical.config;
        validate_selection(&config.schema, &mode, &fields, &sort)?;
        crate::metadata_filter::validate_filters(&root.schema_map, &filters)
            .map_err(RecordError::Rejected)?;
    }
    if matches!(mode, SearchMode::Vector { .. }) {
        return crate::search::search_with_counts(
                index,
                text,
                vector,
                mode,
                offset,
                length,
                result_type,
                true,
                fields,
                filters,
                sort,
                &mut Vec::new(),
            )
            .await;
    }
    let rrf = if let SearchMode::Hybrid { rrf, .. } = &mode {
        *rrf
    } else {
        RrfOptions::default()
    };
    rrf.validate().map_err(RecordError::Rejected)?;
    let hybrid = !matches!(mode, SearchMode::Lexical);
    let detached = (!hybrid && result_type == ResultType::Count)
        || sort.iter().all(|s| matches!(s.field.as_str(), "_id" | "_score"));
    let mut vector_result = ResultObject::default();
    let mut vector_counts = Vec::new();
    // Weights control rank contribution, not channel counts or validation.
    if hybrid {
        let vector_mode = match &mode {
            SearchMode::Hybrid {
                similarity_threshold,
                ann_mode,
                ..
            } => SearchMode::Vector {
                similarity_threshold: *similarity_threshold,
                ann_mode: ann_mode.clone(),
            },
            _ => unreachable!(),
        };
        vector_result = crate::search::search_with_counts(
                index,
                String::new(),
                vector,
                vector_mode,
                0,
                rrf.vector_candidates.unwrap_or(offset + length),
                result_type.clone(),
                true,
                fields.clone(),
                filters.clone(),
                vec![],
                &mut vector_counts,
            )
            .await?;
    }
    #[cfg(test)]
    if hybrid {
        let root = index.read().await;
        if let Some(path) = Path::new(&root.index_path_string).parent() {
            super::super::query::pause(path, ".test-hybrid-vector-phase-pause").await?;
        }
    }
    let root = index.read().await;
    let shards = futures::future::join_all(root.segments.iter().map(|segment| segment.vector.read())).await;
    if shards.iter().any(|shard|shard.failed) {return Err(RecordError::RecoveryRequired);}
    if shards.is_empty() {
        return Err(storage("empty retrieval view"));
    }
    let config = &root.segments[0].lexical.config;
    // Admission precedes active identity maps, term/field scorers and Top-K buffers.
    // The input bound is the selected live population, not retired physical history.
    // Native clauses and their shared memory predicate both retain analyzed
    // terms. Admit expansion from actual input bytes before compiling either.
    let mut query_bytes = (text.len() as u64).checked_mul(64)
        .and_then(|n|n.checked_mul(config.schema.len() as u64+1))
        .and_then(|n|n.checked_add(65536)).ok_or_else(||storage("query predicate memory overflow"))?;
    for shard in &shards {
        let active = shard.active.as_ref().ok_or_else(|| storage("native metadata statistics absent"))?;
        for (_, id) in active.keys.iter().enumerate().filter(|(ordinal, _)| active.is_live(*ordinal)) {
            query_bytes = query_bytes.checked_add(1024 + id.len() as u64 * 8)
                .ok_or_else(|| storage("native query memory overflow"))?;
        }
    }
    if detached {
        for hit in &vector_result.results {
            let active = shards[hit.doc_id % shards.len()].active.as_ref()
                .ok_or_else(|| storage("vector candidate metadata absent"))?;
            let key = active.keys.get(hit.doc_id / shards.len())
                .ok_or_else(|| storage("vector candidate outside captured topology"))?;
            query_bytes = query_bytes.checked_add(1024 + key.len() as u64 * 8)
                .ok_or_else(|| storage("vector candidate memory overflow"))?;
        }
    }
    query_bytes = query_bytes.checked_add(config.schema.len() as u64 * 100 * 512)
        .ok_or_else(|| storage("native query memory overflow"))?;
    let _memory = crate::storage::build_budget::reserve_memory(query_bytes).map_err(storage)?;
    let _buffer = crate::resources::Scope::current().map(|scope| scope.bytes(query_bytes))
        .transpose().map_err(|_| RecordError::Busy)?;

    let mut snapshots = Vec::new();
    for segment in &root.segments {
        snapshots.push(segment.lexical.snapshot().await?);
    }
    let mut locations = BTreeMap::new();
    let n = shards.len();
    // Components expose stable logical identities. Native and vector rows can
    // be reordered or merged independently; local ordinals are only addresses
    // inside the vector engine, never the cross-index join key.
    let mut vectors = BTreeMap::new();
    for (slot, shard) in shards.iter().enumerate() {
        let active = shard.active.as_ref().ok_or_else(|| storage("native metadata statistics absent"))?;
        if active.revisions.len() != active.keys.len() || active.fingerprints.len() != active.keys.len() {
            return Err(storage("vector revision identities missing"));
        }
        let compiled = crate::metadata_filter::compile_filters(shard, &filters)
            .map_err(RecordError::Rejected)?;
        for (local, key) in active.keys.iter().enumerate().filter(|(local, _)| active.is_live(*local)) {
            let (version, revision) = active.revisions[local];
            let identity = Identity { id: key.clone(), version, revision };
            if vectors.insert(identity, (local * n + slot,
                !crate::metadata_filter::is_facet_filter(shard, &compiled, local), active.fingerprints[local])).is_some() {
                return Err(storage("duplicate live vector revision identity"));
            }
        }
    }
    let sorts = sort
        .iter()
        .filter_map(|s| {
            let idx = match s.field.as_str() {
                "_id" => usize::MAX,
                "_score" => usize::MAX - 1,
                _ => *shards[0].facets_map.get(&s.field)?,
            };
            Some(ResultSortIndex { idx, order: s.order.clone() })
        })
        .collect::<Vec<_>>();
    // The captured root fixes physical addresses: append adds rows, retirement
    // changes membership only, and merge/rotation install different components.
    // A vector hit may already be retired and absent from `vectors`. Preserve
    // its OLD physical identity, not the newer live row with the same ID.
    let mut vector_identities = Vec::new();
    if detached && hybrid {
        for hit in &vector_result.results {
            let active = shards[hit.doc_id % n].active.as_ref().unwrap();
            let local = hit.doc_id / n;
            let (version, revision) = *active.revisions.get(local)
                .ok_or_else(|| storage("vector candidate revision absent"))?;
            let fingerprint = *active.fingerprints.get(local)
                .ok_or_else(|| storage("vector candidate fingerprint absent"))?;
            vector_identities.push((hit.doc_id, Identity {
                id: active.keys[local].clone(), version, revision,
            }, fingerprint));
        }
    }
    // Query-owned identities and small address entries only. Population and
    // vector-candidate storage was admitted before allocation. Native snapshots,
    // Handle/file pins and retirement tokens retain their existing ownership.
    let mut keys = if detached && (hybrid || result_type != ResultType::Count) {
        vectors.iter().map(|(identity, (address, _, fingerprint))|
            (*address, (identity, *fingerprint))).collect::<BTreeMap<_, _>>()
    } else { BTreeMap::new() };
    for (address, identity, fingerprint) in &vector_identities {
        keys.insert(*address, (identity, *fingerprint));
    }
    let shards = if detached { drop(shards); Vec::new() } else { shards };
    // root/Handle and the fixed native snapshots still retain topology and file
    // pins. Only component guards end here, before analysis/statistics/scoring.
    #[cfg(test)]
    if detached {
        if let Some(path) = Path::new(&root.index_path_string).parent() {
            super::super::query::pause(path, ".test-lexical-capture-pause").await?;
        }
    }
    #[cfg(test)]
    if hybrid {
        if let Some(path) = Path::new(&root.index_path_string).parent() {
            super::super::query::pause(path, ".test-hybrid-capture-pause").await?;
        }
    }
    let corpus = super::scoring::CorpusView::select_matching(
        &snapshots.iter().map(Arc::as_ref).collect::<Vec<_>>(),
        cut,
        |identity| {
            if let Some(&(address, accepts, _)) = vectors.get(identity) {
                // Filters gate hits only; every joined row remains in BM25.
                // Snapshot ownership keeps these borrowed map keys alive.
                if accepts { locations.insert(identity, address); }
                true
            } else { false }
        },
    )?;
    let names = config
        .schema
        .iter()
        .filter(|f| f.index_lexical)
        .collect::<Vec<_>>();
    let selected_fields = fields
        .iter()
        .filter_map(|name| {
            names
                .iter()
                .position(|f| &f.field == name)
                .map(|i| snapshots[0].fields[i])
        })
        .collect::<Vec<_>>();
    let analyzer = &root.segments[0].lexical.analyzer;
    let plan = analyzer
        .query_fields(
            &text,
            query_type,
            &snapshots[0].fields,
            &selected_fields,
            true,
        )
        .await;
    let indexed_fields = config
        .schema
        .iter()
        .filter(|f| f.index_lexical || f.index_vector)
        .count();
    let boosts = names
        .iter()
        .map(|f| if indexed_fields == 1 { 1.0 } else { f.boost })
        .collect::<Vec<_>>();
    // Lexical (including browse) has no score threshold. Hybrid/ANN retains
    // its channel counts, threshold evaluation and existing fusion contract.
    if !hybrid && result_type == ResultType::Count {
        super::scoring::validate_bounds(&plan.scores, &boosts, snapshots[0].fields.len(), MAX_POPULATION)?;
        let count = corpus.count(plan.predicate.as_ref(), |id| locations.contains_key(id))?;
        return Ok(ResultObject {
            original_query: text.clone(),
            query: text,
            query_terms: plan.query_terms,
            result_count_total: count,
            ..Default::default()
        });
    }
    let order = |a: crate::min_heap::Result, b: crate::min_heap::Result| {
        if detached { captured_ordering(&keys, &sorts, b, a) }
        else { crate::min_heap::result_ordering_root(&shards, text.is_empty(), &sorts, b, a) }
    };
    // The same user ordering selects candidates and orders the final result.
    // Hybrid reranks this bounded lexical set by score only before global RRF.
    let limit = if hybrid { rrf.keyword_candidates.unwrap_or(offset + length) }
        else { offset + length }.min(MAX_POPULATION);
    let mut candidates = std::collections::BinaryHeap::new();
    let lexical_counts = std::cell::RefCell::new(vec![0usize; n]);
    let lex_count = corpus.collect_scores(
        plan.predicate.as_ref(), &plan.scores, &boosts,
        config.index.field_length_normalization.clone(),
        |id| {
            let Some(&address) = locations.get(id) else { return false; };
            lexical_counts.borrow_mut()[address % n] += 1;
            true
        },
        |identity, score| {
            let hit = crate::min_heap::Result {
                doc_id: locations[identity], score,
                #[cfg(feature = "vb")]
                lexical_score: score,
                #[cfg(feature = "vb")]
                source: crate::vector::ResultSource::Lexical,
                ..Default::default()
            };
            Candidate { hit, order: &order }.retain(&mut candidates, limit);
        },
    )?;
    let mut lexical = candidates.into_sorted_vec().into_iter()
        .map(|candidate| candidate.hit).collect::<Vec<_>>();
    let mut result = ResultObject {
        original_query: text.clone(),
        query: text.clone(),
        query_terms: plan.query_terms,
        result_count_total: lex_count,
        ..Default::default()
    };
    if hybrid {
        let mut fused = BTreeMap::<usize, crate::min_heap::Result>::new();
        lexical.sort_by(|a, b| {
            if detached { captured_ordering(&keys, &[], *b, *a) }
            else { crate::min_heap::rank_with_segments(&shards, b.score, b.doc_id, a.score, a.doc_id) }
        });
        for (channel, weight, hits) in [
            ("lexical", rrf.keyword_weight, lexical),
            ("vector", rrf.vector_weight, vector_result.results),
        ] {
            if weight == 0.0 {
                continue;
            }
            for (rank, hit) in hits.into_iter().enumerate() {
                let contribution = rrf_contribution(weight, rrf.constant, rank);
                fused
                    .entry(hit.doc_id)
                    .and_modify(|h| {
                        let score = h.score + contribution;
                        #[cfg(feature = "vb")]
                        if channel == "vector" {
                            *h = crate::min_heap::Result {
                                lexical_score: h.lexical_score,
                                source: crate::vector::ResultSource::Hybrid,
                                ..hit
                            };
                        }
                        h.score = score;
                    })
                    .or_insert(crate::min_heap::Result { score: contribution, ..hit });
                result
                    .explanations
                    .entry(hit.doc_id)
                    .or_default()
                    .push(ChannelExplanation {
                        channel: channel.into(),
                        rank,
                        rank_basis: "score".into(),
                        raw_score: Some(hit.score),
                        rrf_contribution: Some(contribution),
                    });
            }
        }
        if vector_counts.len() != n { return Err(storage("vector count topology mismatch")); }
        result.result_count_total = lexical_counts.into_inner().into_iter()
            .zip(vector_counts).map(|(lexical, vector)| lexical.max(vector)).sum();
        result.observed_vector_count = vector_result.observed_vector_count;
        result.observed_cluster_count = vector_result.observed_cluster_count;
        result.results = fused.into_values().collect();
        result.results.sort_by(|a, b| order(*a, *b));
    } else {
        result.results = lexical;
    }
    result.results = if result_type == ResultType::Count {
        vec![]
    } else {
        result
            .results
            .into_iter()
            .skip(offset)
            .take(length)
            .collect()
    };
    let returned = result.results.iter().map(|hit| hit.doc_id).collect::<BTreeSet<_>>();
    result.explanations.retain(|id, _| returned.contains(id));
    result.result_count = result.results.len();
    if detached && hybrid {
        // Only final candidates escape the workspace. Handle's existing bounded
        // candidate lease retains these copies through authoritative revalidation.
        *captured = Some(result.results.iter().map(|hit| {
            let (identity, fingerprint) = keys[&hit.doc_id];
            super::super::query::Candidate { identity: identity.clone(), fingerprint, hit: *hit }
        }).collect());
    }
    Ok(result)
}

// Same greater-is-better contract as result_ordering_root, restricted to
// Record's stable ID and score sorts. Facet comparators keep their mmap guards.
fn captured_ordering(
    keys: &BTreeMap<usize, (&Identity, [u8; 32])>,
    sorts: &[ResultSortIndex],
    a: crate::min_heap::Result,
    b: crate::min_heap::Result,
) -> std::cmp::Ordering {
    let a_key = &keys.get(&a.doc_id).expect("selected query identity").0.id;
    let b_key = &keys.get(&b.doc_id).expect("selected query identity").0.id;
    for sort in sorts {
        if sort.idx == usize::MAX {
            return if sort.order == SortOrder::Descending { a_key.cmp(b_key) }
                else { b_key.cmp(a_key) };
        }
        debug_assert_eq!(sort.idx, usize::MAX - 1);
        let order = if sort.order == SortOrder::Descending {
            crate::min_heap::score_ordering(a.score, b.score)
        } else { crate::min_heap::score_ordering(b.score, a.score) };
        if !order.is_eq() { return order; }
    }
    crate::min_heap::score_ordering(a.score, b.score)
        .then_with(|| b_key.cmp(a_key))
        .then_with(|| b.doc_id.cmp(&a.doc_id))
}

// An empty selection preserves each channel's default fields. Hybrid selections
// may name either channel; a channel with no selected fields keeps its default.
fn validate_selection(
    schema: &[crate::index::SchemaField],
    mode: &SearchMode,
    fields: &[String],
    sorts: &[ResultSort],
) -> Result<(), RecordError> {
    for name in fields {
        let field = schema.iter().find(|field| &field.field == name)
            .ok_or_else(|| RecordError::Rejected(format!("unknown search field: {name}")))?;
        let applicable = match mode {
            SearchMode::Lexical => field.index_lexical,
            SearchMode::Vector { .. } => field.index_vector,
            SearchMode::Hybrid { .. } => field.index_lexical || field.index_vector,
        };
        if !applicable {
            return Err(RecordError::Rejected(format!("search field is not indexed for this mode: {name}")));
        }
    }
    for sort in sorts {
        if matches!(sort.field.as_str(), "_id" | "_score") { continue; }
        let field = schema.iter().find(|field| field.field == sort.field)
            .ok_or_else(|| RecordError::Rejected(format!("unknown sort field: {}", sort.field)))?;
        if !field.facet {
            return Err(RecordError::Rejected(format!("sort field is not a facet: {}", sort.field)));
        }
    }
    Ok(())
}

// BinaryHeap keeps the worst retained candidate at the root. The ordering is
// the existing final-result comparator (Less means better), including facets
// and stable business IDs; no per-hit identity allocation is needed.
struct Candidate<'a> {
    hit: crate::min_heap::Result,
    order: &'a dyn Fn(crate::min_heap::Result, crate::min_heap::Result) -> std::cmp::Ordering,
}
impl PartialEq for Candidate<'_> {
    fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() }
}
impl Eq for Candidate<'_> {}
impl PartialOrd for Candidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for Candidate<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { (self.order)(self.hit, other.hit) }
}

impl<'a> Candidate<'a> {
    fn retain(self, heap: &mut std::collections::BinaryHeap<Self>, limit: usize) {
        if heap.len() < limit {
            heap.push(self);
        } else if let Some(mut worst) = heap.peek_mut() {
            if self.cmp(&worst).is_lt() { *worst = self; }
        }
    }
}

#[cfg(test)]
mod candidate_tests {
    use super::*;

    #[tokio::test]
    async fn captured_order_matches_guarded_order_for_id_and_score() {
        use crate::record::{ExpectedVersion, RecordPartition, tests::{config, write}};
        let temporary = tempfile::tempdir().unwrap();
        let partition = RecordPartition::create(&temporary.path().join("records"), config()).await.unwrap();
        for id in ["z", "a", "m"] {
            partition.write(write(id, id, "alpha", ExpectedVersion::Absent)).await.unwrap();
        }
        let handle = partition.inner.lock().await.index.capture_query(10).await.unwrap();
        let root = handle.index.read().await;
        let shards = futures::future::join_all(root.segments.iter().map(|s| s.vector.read())).await;
        let mut identities = Vec::new();
        for (slot, shard) in shards.iter().enumerate() {
            for (local, key) in shard.active.as_ref().unwrap().keys.iter().enumerate() {
                identities.push((local * shards.len() + slot, Identity {id:key.clone(),version:1,revision:1}));
            }
        }
        let keys = identities.iter().map(|(address, id)| (*address,(id,[0;32]))).collect::<BTreeMap<_,_>>();
        let scores = [0.0, -0.0, 1.0, -1.0, f32::INFINITY, f32::NEG_INFINITY, f32::NAN];
        for ordering in [vec![], vec!["_id"], vec!["_score"], vec!["_score", "_id"], vec!["_id", "_score"]] {
            for descending in [false, true] {
                let sorts = ordering.iter().map(|field| ResultSortIndex {
                    idx: if *field == "_id" { usize::MAX } else { usize::MAX - 1 },
                    order: if descending { SortOrder::Descending } else { SortOrder::Ascending },
                }).collect::<Vec<_>>();
                for &a_id in keys.keys() {
                    for &b_id in keys.keys() {
                        for a_score in scores {
                            for b_score in scores {
                                let a = crate::min_heap::Result { doc_id: a_id, score: a_score, ..Default::default() };
                                let b = crate::min_heap::Result { doc_id: b_id, score: b_score, ..Default::default() };
                                for empty in [false, true] {
                                    assert_eq!(captured_ordering(&keys, &sorts, a, b),
                                        crate::min_heap::result_ordering_root(&shards, empty, &sorts, a, b),
                                        "{ordering:?} descending={descending} a={a:?} b={b:?}");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn bounded_candidates_match_full_sort() {
        let order = |a: crate::min_heap::Result, b: crate::min_heap::Result| {
            crate::min_heap::rank_ordering(b.score, b.doc_id, a.score, a.doc_id)
        };
        // Reverse identities force replacements at equal scores. Ascending scores
        // force replacements too; neither traversal may grow storage with hits.
        for tied in [false, true] {
            let full = (0..4096).rev().map(|doc_id| crate::min_heap::Result {
                doc_id, score: if tied { 1.0 } else { (4096 - doc_id) as f32 },
                ..Default::default()
            }).collect::<Vec<_>>();
            let mut expected = full.clone();
            expected.sort_by(|a, b| order(*a, *b));
            for limit in [0, 1, 37] {
                let mut heap = std::collections::BinaryHeap::new();
                for hit in &full {
                    Candidate { hit: *hit, order: &order }.retain(&mut heap, limit);
                    assert!(heap.len() <= limit);
                    assert!(heap.capacity() <= (2 * limit).max(4));
                }
                let actual = heap.into_sorted_vec();
                assert_eq!(actual.len(), limit);
                for (actual, expected) in actual.iter().zip(&expected) {
                    assert_eq!((actual.hit.doc_id, actual.hit.score), (expected.doc_id, expected.score));
                }
            }
        }
    }
}
