//! Record-only query execution: native lexical postings, existing vector/metadata
//! component, possibly lagging native prefixes, and one global rank fusion.
use super::*;
use std::cmp::Ordering;
use crate::{
    component::{FacetReader, RetrievalSegment},
    index::VectorIndexArc,
    min_heap::Result as SearchHit,
    search::{
        ChannelExplanation, FacetFilter, QueryType, QueryWork, ResultObject, ResultSort, ResultSortIndex,
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
) -> Result<ResultObject, RecordError> {
    let work = QueryWork::current();
    if let Some(work) = &work {
        if !matches!(mode, SearchMode::Lexical) || text.len() > 1024 || text.trim().is_empty() {
            return Err(RecordError::Rejected("bounded lexical recall requires a nonempty query of at most 1024 bytes".into()));
        }
        work.charge(text.len())?;
    }
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
                None,
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
                None,
            )
            .await?;
    }
    let root = index.read().await;
    let first = root.segments.first().ok_or_else(|| storage("empty retrieval view"))?;
    let config = &first.lexical.config;
    // Charge analyzed terms and the predicate before compiling them. Document
    // membership has its own admission, taken only when the query can match.
    let query_bytes = (text.len() as u64).checked_mul(64)
        .and_then(|n| n.checked_mul(config.schema.len() as u64 + 1))
        .and_then(|n| n.checked_add(65536 + config.schema.len() as u64 * 100 * 512))
        .ok_or_else(|| storage("query predicate memory overflow"))?;
    let scope = crate::resources::Scope::current();
    let _predicate_memory = crate::storage::build_budget::reserve_memory(query_bytes).map_err(storage)?;
    let _predicate_buffer = scope.as_ref().map(|scope| scope.bytes(query_bytes))
        .transpose().map_err(|_| RecordError::Busy)?;

    // The field layout is fixed for the captured topology. Compile the query
    // before taking component guards, then capture membership and native cuts.
    let native_fields = first.lexical.snapshot().await?.fields.clone();
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
                .map(|i| native_fields[i])
        })
        .collect::<Vec<_>>();
    let analyzer = &first.lexical.analyzer;
    let plan = analyzer
        .query_fields(
            &text,
            query_type,
            &native_fields,
            &selected_fields,
            true,
        )
        .await;
    if work.is_some()
        && (!plan.predicate.downcast_ref::<super::query::Compiled>()
            .is_some_and(super::query::Compiled::supports_bounded_scoring)
            || plan.scores.len() > 1
            || plan.scores.iter().any(|group| group.components.len() != 1))
    {
        return Err(RecordError::Rejected("bounded lexical recall supports a single positive analyzed term".into()));
    }
    let indexed_fields = config
        .schema
        .iter()
        .filter(|f| f.index_lexical || f.index_vector)
        .count();
    let boosts = names
        .iter()
        .map(|f| if indexed_fields == 1 { 1.0 } else { f.boost })
        .collect::<Vec<_>>();
    let browse = text.is_empty();
    let mut result = ResultObject {
        original_query: text.clone(),
        query: text,
        query_terms: plan.query_terms,
        ..Default::default()
    };

    let shards = futures::future::join_all(root.segments.iter().map(|segment| segment.vector.read())).await;
    if shards.iter().any(|shard| shard.failed) {
        return Err(RecordError::RecoveryRequired);
    }
    let mut active_shards = Vec::with_capacity(shards.len());
    for shard in &shards {
        let active = shard.active.as_ref().ok_or_else(|| storage("native metadata statistics absent"))?;
        if active.revisions.len() != active.keys.len() {
            return Err(storage("vector revision identities missing"));
        }
        active_shards.push(active);
    }
    let mut snapshots = Vec::with_capacity(root.segments.len());
    for segment in &root.segments {
        snapshots.push(segment.lexical.snapshot().await?);
    }
    let views = snapshots.iter().map(Arc::as_ref).collect::<Vec<_>>();
    scoring::validate_bounds(&plan.scores, &boosts, native_fields.len())?;
    let strict = if let Some(work) = &work {
        work.charge(snapshots.len())?;
        // An exact-cut consumer may use a published pair or an aligned bounded
        // tail, but cannot scan the whole corpus to rejoin mismatched identities.
        Some(capture_aligned(&root.segments, &snapshots, &active_shards, Some(work))?
            .ok_or(RecordError::Busy)?)
    } else {
        None
    };
    if !hybrid && !browse && !has_positive_terms(&views, &plan.scores, work.as_deref())? {
        scoring::CorpusView::validate_snapshots(&views, cut)?;
        for shard in &shards {
            crate::metadata_filter::validate_filters(&shard.schema_map, &filters)
                .map_err(RecordError::Rejected)?;
        }
        return Ok(result);
    }

    let selections = match strict {
        Some(selections) => Some(selections),
        None => capture_aligned(&root.segments, &snapshots, &active_shards, None)?,
    };
    let aligned = selections.is_some();
    let mut vectors = BTreeMap::new();
    let _identity_workspace = if aligned {
        None
    } else {
        let mut bytes = 0u64;
        for active in &active_shards {
            for (_, id) in active.keys.iter().enumerate().filter(|(ordinal, _)| active.is_live(*ordinal)) {
                bytes = bytes.checked_add(1024 + id.len() as u64 * 8)
                    .ok_or_else(|| storage("native query memory overflow"))?;
            }
        }
        crate::storage::build_budget::validate_single(query_bytes.checked_add(bytes)
            .ok_or_else(|| storage("native query memory overflow"))?).map_err(storage)?;
        let memory = crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
        let buffer = scope.as_ref().map(|scope| scope.bytes(bytes))
            .transpose().map_err(|_| RecordError::Busy)?;
        for (slot, (shard, active)) in shards.iter().zip(active_shards).enumerate() {
            let compiled = crate::metadata_filter::compile_filters(shard, &filters)
                .map_err(RecordError::Rejected)?;
            let mut facets = FacetReader::new(shard);
            for (local, key) in active.keys.iter().enumerate().filter(|(local, _)| active.is_live(*local)) {
                let (version, revision) = active.revisions[local];
                let identity = Identity { id: key.clone(), version, revision };
                let row = VectorRow {
                    address: local * shards.len() + slot,
                    passes_filter: !crate::metadata_filter::is_facet_filter(&mut facets, &compiled, local)?,
                };
                if vectors.insert(identity, row).is_some() {
                    return Err(storage("duplicate live vector revision identity"));
                }
            }
        }
        Some((memory, buffer))
    };
    let n = shards.len();
    let corpus = if let Some(selections) = selections {
        Some(scoring::CorpusView::from_selections(&views, selections, cut)?)
    } else {
        None
    };
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
    let count_only = !hybrid && result_type == ResultType::Count;
    // String-set filter expansion can depend on the metadata dictionary.
    // Reserve its workspace before compiling the predicates.
    let mut compiled = Vec::new();
    let _filter_workspace = if aligned && !filters.is_empty() {
        let mut bytes = (shards.len() as u64) * (size_of::<FacetReader<'_>>() as u64);
        for shard in &shards {
            let facet_bytes = match &work {
                Some(work) => crate::metadata_filter::metered_workspace_bytes(shard, &filters, work)?,
                None => crate::metadata_filter::workspace_bytes(shard, &filters).map_err(RecordError::Rejected)?,
            };
            bytes = bytes.checked_add(facet_bytes)
                .ok_or_else(|| storage("metadata predicate memory overflow"))?;
        }
        let workspace = crate::record::build_budget::admit_memory(bytes, scope.as_ref())?;
        for shard in &shards {
            if let Some(work) = &work { work.charge(0)?; }
            compiled.push(crate::metadata_filter::compile_filters(shard, &filters)
                .map_err(RecordError::Rejected)?);
        }
        Some(workspace)
    } else {
        None
    };
    // Count with a compiled filter can stream directly through the guarded
    // facets. It has no ranking pass that would need to retain match addresses.
    let detached = if count_only {
        compiled.is_empty()
    } else {
        !hybrid && sort.iter().all(|s| matches!(s.field.as_str(), "_id" | "_score"))
    };
    let filtered = if detached && !compiled.is_empty() {
        // Inspect metadata only for posting matches while the component is guarded,
        // then release component guards before native scoring.
        let mut accepted = FilteredMatches::default();
        let mut facets = shards.iter().map(|shard| FacetReader::new(shard)).collect::<Vec<_>>();
        let checks = work.as_ref().map(|_| compiled.iter().map(|filters|
            crate::metadata_filter::filter_work(filters)).collect::<Vec<_>>());
        corpus.as_ref().expect("aligned corpus").visit(plan.predicate.as_ref(), |slot, ordinal, _| {
            if let (Some(work), Some(checks)) = (&work, &checks) {
                work.charge(checks[slot])?;
            }
            if !crate::metadata_filter::is_facet_filter(&mut facets[slot], &compiled[slot], ordinal)? {
                accepted.insert(ordinal * n + slot, scope.as_ref())?;
            }
            Ok(())
        })?;
        Some(accepted)
    } else {
        None
    };
    let shards = if detached { drop(shards); Vec::new() } else { shards };
    #[cfg(test)]
    if detached {
        if let Some(path) = Path::new(&root.index_path_string).parent() {
            super::super::query::pause(path, ".test-lexical-capture-pause").await?;
        }
    }
    let corpus = match corpus {
        Some(corpus) => corpus,
        // BM25 uses every joined revision. Check the cached facet predicate
        // only when collecting hits.
        None => scoring::CorpusView::select_matching(&views, cut, |identity| vectors.contains_key(identity))?,
    };
    let mut facets = if compiled.is_empty() || detached {
        Vec::new()
    } else {
        shards.iter().map(|shard| FacetReader::new(shard)).collect()
    };
    let accepts = |slot: usize, ordinal: usize, identity: &Identity| {
        Ok(if !aligned {
            vectors[identity].passes_filter
        } else if let Some(filtered) = &filtered {
            filtered.addresses.contains(&(ordinal * n + slot))
        } else {
            compiled.is_empty()
                || !crate::metadata_filter::is_facet_filter(&mut facets[slot], &compiled[slot], ordinal)?
        })
    };
    let address = |slot: usize, ordinal: usize, identity: &Identity| {
        if aligned { ordinal * n + slot } else { vectors[identity].address }
    };
    // Lexical (including browse) has no score threshold. Hybrid/ANN retains
    // its channel counts, threshold evaluation and existing fusion contract.
    if count_only {
        result.result_count_total = corpus.count_where(plan.predicate.as_ref(), accepts)?;
        return Ok(result);
    }
    let order = |a: &Candidate<'_>, b: &Candidate<'_>| {
        if detached {
            captured_ordering(&sorts, b.hit, b.key, a.hit, a.key)
        } else {
            crate::min_heap::result_ordering_root(&shards, browse, &sorts, b.hit, &b.metadata, a.hit, &a.metadata)
        }
    };
    // The same user ordering selects candidates and orders the final result.
    // Hybrid reranks this bounded lexical set by score only before global RRF.
    let window = offset.saturating_add(length);
    let limit = if hybrid { rrf.keyword_candidates.unwrap_or(window) }
        else { window }.min(config.max_candidates);
    let _candidate_workspace = if aligned {
        let vector_candidates = if hybrid { vector_result.results.len() } else { 0 };
        let bytes = (limit as u64).checked_add(vector_candidates as u64)
            .and_then(|candidates| candidates.checked_mul(512))
            .ok_or_else(|| storage("candidate workspace overflow"))?;
        Some(crate::record::build_budget::admit_memory(bytes, scope.as_ref())?)
    } else {
        None
    };
    let mut candidates = std::collections::BinaryHeap::new();
    let mut lexical_counts = if hybrid { vec![0usize; n] } else { Vec::new() };
    result.result_count_total = corpus.collect_scores(
        plan.predicate.as_ref(), &plan.scores, &boosts,
        config.index.field_length_normalization.clone(),
        accepts,
        |slot, ordinal, identity, score| {
            let doc_id = address(slot, ordinal, identity);
            if hybrid {
                lexical_counts[doc_id % n] += 1;
            }
            let hit = SearchHit {
                doc_id,
                score,
                #[cfg(feature = "vb")]
                lexical_score: score,
                #[cfg(feature = "vb")]
                source: crate::vector::ResultSource::Lexical,
                ..Default::default()
            };
            let key = views[slot].identities[ordinal].id.as_str();
            let metadata = if detached { Default::default() }
                else { crate::min_heap::prepare_sort_key(&shards, &sorts, doc_id)? };
            Candidate { hit, key, metadata, order: &order }.retain(&mut candidates, limit);
            Ok(())
        },
    )?;
    let mut lexical = candidates.into_sorted_vec().into_iter()
        .map(|candidate| candidate.hit).collect::<Vec<_>>();
    if hybrid {
        let mut fused = BTreeMap::<usize, crate::min_heap::Result>::new();
        lexical.sort_by(|a, b| {
            crate::min_heap::rank_with_segments(&shards, b.score, b.doc_id, a.score, a.doc_id)
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
        result.result_count_total = lexical_counts.into_iter()
            .zip(vector_counts).map(|(lexical, vector)| lexical.max(vector)).sum();
        result.observed_vector_count = vector_result.observed_vector_count;
        result.observed_cluster_count = vector_result.observed_cluster_count;
        result.results = crate::min_heap::sort_results(&shards, browse, &sorts, fused.into_values().collect())?;
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
    if !result.explanations.is_empty() {
        let returned = result.results.iter().map(|hit| hit.doc_id).collect::<BTreeSet<_>>();
        result.explanations.retain(|id, _| returned.contains(id));
    }
    result.result_count = result.results.len();
    Ok(result)
}

struct VectorRow {
    address: usize,
    passes_filter: bool,
}

// Each frozen pair was verified at publication. Check each bounded
// mutable prefix, where either consumer may have published further.
fn capture_aligned(
    segments: &[RetrievalSegment],
    snapshots: &[Arc<Snapshot>],
    active_shards: &[&crate::active::ActiveStatistics],
    work: Option<&QueryWork>,
) -> Result<Option<Vec<LiveSelection>>, RecordError> {
    let mut remaining = runtime::MUTABLE_TAIL_UPDATES;
    let mut selections = Vec::with_capacity(snapshots.len());
    for ((segment, snapshot), active) in segments.iter().zip(snapshots).zip(active_shards) {
        if work.is_some() && snapshot.identities.len() != active.keys.len() {
            return Ok(None);
        }
        if let Some(live) = segment.visibility.capture(snapshot, active.keys.len()) {
            selections.push(LiveSelection::Published(live));
            continue;
        }
        let rows = snapshot.identities.len().max(active.keys.len());
        let Some(next) = remaining.checked_sub(rows) else { return Ok(None); };
        remaining = next;
        if let Some(work) = work {
            // Pair physical identities, then select live metadata ordinals.
            work.charge(rows * 2)?;
        }
        if !visibility::aligned_prefix(snapshot, active) {
            return Ok(None);
        }
        let common = snapshot.identities.len().min(active.keys.len());
        let mut keys = BTreeSet::new();
        let mut selected = BTreeSet::new();
        for (ordinal, key) in active.keys.iter().enumerate().filter(|(ordinal, _)| active.is_live(*ordinal)) {
            if !keys.insert(key) {
                return Ok(None);
            }
            if ordinal < common {
                selected.insert(ordinal);
            }
        }
        selections.push(LiveSelection::Ordinals(selected));
    }
    Ok(Some(selections))
}

#[derive(Default)]
struct FilteredMatches {
    addresses: BTreeSet<usize>,
    _memory: Vec<crate::resources::MemoryReservation>,
}
impl FilteredMatches {
    fn insert(&mut self, address: usize, scope: Option<&crate::resources::Scope>) -> Result<(), RecordError> {
        const CHUNK: usize = 64;
        if self.addresses.len().is_multiple_of(CHUNK) {
            self._memory.push(crate::record::build_budget::admit_memory((CHUNK * 128) as u64, scope)?);
        }
        self.addresses.insert(address);
        Ok(())
    }
}

// Each match of a non-browse predicate needs at least one positive
// term in a selected field. A dictionary hit is only evidence to keep
// searching: it may be retired or fail the full Boolean/phrase predicate.
fn has_positive_terms(
    snapshots: &[&Snapshot],
    groups: &[query::ScoreGroup],
    work: Option<&QueryWork>,
) -> Result<bool, RecordError> {
    for snapshot in snapshots {
        for group in groups {
            for field in &group.match_fields {
                let term = Term::from_field_text(*field, &group.matched_term);
                for segment in snapshot.searcher.segment_readers() {
                    if let Some(work) = work {
                        work.charge(1)?;
                    }
                    if segment.inverted_index(*field).map_err(super::native_search_error)?
                        .get_term_info(&term).map_err(super::native_io_error)?.is_some() {
                        return Ok(true);
                    }
                }
                if let Some(work) = work {
                    work.charge(snapshot.overlay.len())?;
                }
                if snapshot.overlay.iter().any(|(_, row)| {
                    !row.positions(&snapshot.fields, *field, &group.matched_term).is_empty()
                }) {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

// Same greater-is-better contract as result_ordering_root, restricted to
// Record's stable ID and score sorts. Prepared facet keys require component guards.
fn captured_ordering(
    sorts: &[ResultSortIndex],
    a: SearchHit,
    a_key: &str,
    b: SearchHit,
    b_key: &str,
) -> Ordering {
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
type CandidateOrder<'a> = dyn Fn(&Candidate<'a>, &Candidate<'a>) -> Ordering + 'a;

struct Candidate<'a> {
    hit: SearchHit,
    key: &'a str,
    metadata: crate::min_heap::SortKey<'a>,
    order: &'a CandidateOrder<'a>,
}
impl PartialEq for Candidate<'_> {
    fn eq(&self, other: &Self) -> bool { self.cmp(other).is_eq() }
}
impl Eq for Candidate<'_> {}
impl PartialOrd for Candidate<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
impl Ord for Candidate<'_> {
    fn cmp(&self, other: &Self) -> Ordering { (self.order)(self, other) }
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
    async fn aligned_lexical_misses_and_hits_skip_corpus_admission() {
        use crate::record::{ExpectedVersion, RecordPartition, tests::{config, write}};
        use crate::resources::{Governor, Lane};

        let temporary = tempfile::tempdir().unwrap();
        let config = config();
        let partition = RecordPartition::create(&temporary.path().join("records"), config.clone())
            .await
            .unwrap();
        for i in 0..32 {
            let id = format!("record-{i}");
            partition.write(write(&id, &id, "alpha", ExpectedVersion::Absent)).await.unwrap();
        }
        let handle = partition.inner.lock().await.index.capture_query(10).await.unwrap();
        let scope = Arc::new(Governor::default())
            .admit("lexical-lookup".into(), Lane::Foreground, 0)
            .unwrap();
        let predicate_bytes = 65536 + config.schema.len() as u64 * 100 * 512;
        let _occupied = scope.bytes(512 * 1024 * 1024 - predicate_bytes - 16 * 1024).unwrap();
        let lookup = |text: &str, result_type| {
            let length = if result_type == ResultType::Count { 0 } else { 10 };
            search(
                &handle.index, text.into(), None, SearchMode::Lexical, QueryType::Union,
                0, length, result_type, vec![], vec![], vec![],
                Cut { sequence: u64::MAX, evaluation_time_ms: u64::MAX },
            )
        };

        for result_type in [ResultType::Count, ResultType::TopkCount] {
            let result = scope.clone().run(lookup("zzqneverlinked", result_type)).await.unwrap();
            assert_eq!(result.result_count_total, 0);
            assert_eq!(result.query_terms, ["zzqneverlinked"]);
            assert!(result.results.is_empty());
        }
        let hit = scope.run(lookup("alpha", ResultType::TopkCount)).await.unwrap();
        assert_eq!(hit.result_count_total, 32);
        assert_eq!(hit.results.len(), 10);
    }

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
        let mut keys = BTreeMap::new();
        for (slot, shard) in shards.iter().enumerate() {
            for (local, key) in shard.active.as_ref().unwrap().keys.iter().enumerate() {
                keys.insert(local * shards.len() + slot, key.as_str());
            }
        }
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
                                    assert_eq!(captured_ordering(&sorts, a, keys[&a_id], b, keys[&b_id]),
                                        crate::min_heap::result_ordering_root(&shards, empty, &sorts, a, &Default::default(), b, &Default::default()),
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
        let rank = |a: SearchHit, b: SearchHit| {
            crate::min_heap::rank_ordering(b.score, b.doc_id, a.score, a.doc_id)
        };
        let order = |a: &Candidate<'_>, b: &Candidate<'_>| rank(a.hit, b.hit);
        // Reverse identities force replacements at equal scores. Ascending scores
        // force replacements too; neither traversal may grow storage with hits.
        for tied in [false, true] {
            let full = (0..4096).rev().map(|doc_id| crate::min_heap::Result {
                doc_id, score: if tied { 1.0 } else { (4096 - doc_id) as f32 },
                ..Default::default()
            }).collect::<Vec<_>>();
            let mut expected = full.clone();
            expected.sort_by(|a, b| rank(*a, *b));
            for limit in [0, 1, 37] {
                let mut heap = std::collections::BinaryHeap::new();
                for hit in &full {
                    Candidate { hit: *hit, key: "", metadata: Default::default(), order: &order }.retain(&mut heap, limit);
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
