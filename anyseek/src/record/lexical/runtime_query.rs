//! Record-only query execution: native lexical postings, existing vector/metadata
//! component, possibly lagging native prefixes, and one global rank fusion.
use super::*;
use crate::{
    index::IndexArc,
    search::{
        ChannelExplanation, FacetFilter, QueryType, ResultObject, ResultSort, ResultSortIndex,
        ResultType, RrfOptions, SearchMode, rrf_contribution,
    },
    vector::Embedding,
};

pub(crate) async fn search(
    index: &IndexArc,
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
    if matches!(mode, SearchMode::Vector { .. }) {
        return Ok(crate::search::search_with_counts(
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
            .await);
    }
    let rrf = if let SearchMode::WeightedHybrid { rrf, .. } = &mode {
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
            }
            | SearchMode::WeightedHybrid {
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
            .await;
        if let Some(error) = &vector_result.error {
            return Err(storage(error));
        }
    }
    let root = index.read().await;
    let shards = futures::future::join_all(root.shard_vec.iter().map(|s| s.read())).await;
    if root.record_lexical.len() != shards.len() {
        return Err(storage("native/vector view topology mismatch"));
    }
    let config = &root.record_lexical[0].config;
    // Admission precedes active identity maps, term/field scorers and Top-K buffers.
    // The input bound is the selected live population, not retired physical history.
    let mut query_bytes = 65536u64;
    for shard in &shards {
        let active = shard.active.as_ref().ok_or_else(|| storage("native metadata statistics absent"))?;
        for (_, id) in active.keys.iter().enumerate().filter(|(ordinal, _)| active.is_live(*ordinal)) {
            query_bytes = query_bytes.checked_add(1024 + id.len() as u64 * 8)
                .ok_or_else(|| storage("native query memory overflow"))?;
        }
    }
    query_bytes = query_bytes.checked_add(config.schema.len() as u64 * 100 * 512)
        .ok_or_else(|| storage("native query memory overflow"))?;
    let _memory = crate::storage::build_budget::reserve_memory(query_bytes).map_err(storage)?;
    let _buffer = crate::resources::Scope::current().map(|scope| scope.bytes(query_bytes))
        .transpose().map_err(storage)?;

    let mut snapshots = Vec::new();
    for runtime in &root.record_lexical {
        snapshots.push(runtime.snapshot().await?);
    }
    let mut selected = BTreeSet::new();
    let mut locations = BTreeMap::new();
    let mut allowed = BTreeSet::new();
    let n = shards.len();
    for (slot, (shard, snapshot)) in shards.iter().zip(&snapshots).enumerate() {
        let active = shard
            .active
            .as_ref()
            .ok_or_else(|| storage("native metadata statistics absent"))?;
        // Native intake consumption may lag vector/metadata. Its published
        // prefix contributes lexical hits; retired revisions stay masked.
        if active.keys.len() < snapshot.identities.len()
            || (active.keys.len() != snapshot.identities.len() && !root.record_lexical[slot].allows_lag()) {
            return Err(storage("native/vector identity count mismatch"));
        }
        let compiled = crate::metadata_filter::compile_filters(shard, &filters)
            .map_err(RecordError::Rejected)?;
        for (local, identity) in snapshot.identities.iter().enumerate() {
            if active.keys[local] != identity.id {
                return Err(storage("native/vector identity mismatch"));
            }
            if active.is_live(local) {
                selected.insert(identity.clone());
                locations.insert(identity.clone(), local * n + slot);
                if !crate::metadata_filter::is_facet_filter(shard, &compiled, local) {
                    allowed.insert(identity.clone());
                }
            }
        }
    }
    let corpus = super::scoring::CorpusView::select(
        &snapshots.iter().map(Arc::as_ref).collect::<Vec<_>>(),
        cut,
        &selected,
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
    let analyzer = &root.record_lexical[0].analyzer;
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
    let lexical_counts = std::cell::RefCell::new(vec![0usize; n]);
    let (lex_count, hits) = corpus.score_groups(
        plan.predicate.as_ref(),
        &plan.scores,
        &boosts,
        config.index.field_length_normalization.clone(),
        MAX_POPULATION,
        |id| {
            if !allowed.contains(id) { return false; }
            lexical_counts.borrow_mut()[locations[id] % n] += 1;
            true
        },
    )?;
    let mut lexical = hits
        .into_iter()
        .map(|hit| crate::min_heap::Result {
            doc_id: locations[&hit.identity],
            score: hit.score,
            #[cfg(feature = "vb")]
            lexical_score: hit.score,
            #[cfg(feature = "vb")]
            source: crate::vector::ResultSource::Lexical,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    let sorts = sort
        .iter()
        .filter_map(|s| {
            let idx = match s.field.as_str() {
                "_id" => usize::MAX,
                "_score" => usize::MAX - 1,
                _ => *shards[0].facets_map.get(&s.field)?,
            };
            Some(ResultSortIndex {
                idx,
                order: s.order.clone(),
            })
        })
        .collect::<Vec<_>>();
    let order = |a: crate::min_heap::Result, b: crate::min_heap::Result| {
        crate::min_heap::result_ordering_root(&shards, n, text.is_empty(), &sorts, b, a)
    };
    lexical.sort_by(|a, b| order(*a, *b));
    let mut result = ResultObject {
        original_query: text.clone(),
        query: text.clone(),
        query_terms: plan.query_terms,
        result_count_total: lex_count,
        ..Default::default()
    };
    if hybrid {
        let mut fused = BTreeMap::<usize, crate::min_heap::Result>::new();
        lexical.truncate(rrf.keyword_candidates.unwrap_or(offset + length));
        lexical.sort_by(|a, b| {
            crate::min_heap::rank_with_shards(&shards, b.score, b.doc_id, a.score, a.doc_id)
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
    Ok(result)
}
