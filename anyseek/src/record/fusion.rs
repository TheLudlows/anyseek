//! Opt-in three-channel fusion over independently observed retrieval caches.
//! Logical adjacency and final records use short Record guards; scoring does not.
use super::*;
use crate::search::{RrfOptions, rrf_contribution};
use std::collections::BTreeSet;

/// Explicit relation channel plus existing zero-based RRF settings.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationFusion {
    /// Bounded one-hop expansion; seed order then neighbor/edge ID defines ranks.
    pub neighbors: NeighborRequest,
    /// Existing keyword/vector weights and smoothing constant. Candidate limits
    /// must be explicit for enabled channels; absent channels contribute zero.
    pub rrf: RrfOptions,
    /// Positive finite contribution of unique neighbors, not edge weight.
    pub relation_weight: f32,
}
/// Counts refer to the union of retrieved candidates, never the whole corpus.
#[derive(Clone, Debug, Serialize)]
pub struct FusionWindow {
    /// Always candidate_window: ANN and bounded rankings are not exact recall.
    pub count_relation: String,
    /// Unique candidates before pagination.
    pub candidates: usize,
    /// Whether the requested adjacency was fully examined.
    pub relation_complete: bool,
    /// Reason for explicit partial adjacency, if allowed.
    pub relation_reason: Option<String>,
}
impl RecordPartition {
    /// Fuse lexical/vector/one-hop observations and revalidate current records.
    /// Cancellation retains owned retrieval tasks and their file/owner pins.
    pub async fn query_with_relations(
        &self,
        query: RecordQuery,
        options: RelationFusion,
    ) -> Result<RecordSearch, RecordError> {
        let permit = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let partition = self.clone();
        crate::resources::spawn(async move {
            let _permit = permit;
            let result = partition.fuse_independent(query, options).await;
            partition.kick_maintenance().await;
            result
        })
        .await
        .map_err(storage)?
    }
}
impl RecordPartition {
    async fn fuse_independent(
        &self,
        query: RecordQuery,
        options: RelationFusion,
    ) -> Result<RecordSearch, RecordError> {
        let (limit, barrier) = {
            let mut inner = self.inner.lock().await;
            inner.advance_time().await?;
            if !inner.healthy || inner.state.sealed_for.is_some() || self.indexes.error().is_some()
            {
                return Err(RecordError::RecoveryRequired);
            }
            (inner.config.max_candidates, self.output_permit()?)
        };
        let reject = |s: &str| RecordError::Rejected(s.into());
        let rrf = options.rrf;
        rrf.validate().map_err(RecordError::Rejected)?;
        if !options.relation_weight.is_finite()
            || options.relation_weight <= 0.0
            || !(rrf.keyword_weight / rrf.constant
                + rrf.vector_weight / rrf.constant
                + options.relation_weight / rrf.constant)
                .is_finite()
        {
            return Err(reject("invalid relation RRF weight or maximum score"));
        }
        if !query.sort.is_empty()
            || query.length == 0
            || matches!(query.result_type, ResultType::Count)
            || query
                .offset
                .checked_add(query.length)
                .is_none_or(|n| n > limit)
        {
            return Err(reject(
                "relation fusion requires positive bounded relevance window without field sort",
            ));
        }
        if matches!(query.mode, SearchMode::WeightedHybrid { .. }) {
            return Err(reject("specify RRF once in relation_fusion"));
        }
        if query.vector.is_some() && matches!(query.mode, SearchMode::Lexical) {
            return Err(reject("vector fusion requires vector search mode"));
        }
        let lexical = !query.text.trim().is_empty() && rrf.keyword_weight > 0.0;
        let vector = query.vector.is_some() && rrf.vector_weight > 0.0;
        if !lexical && !vector {
            return Err(reject(
                "relation fusion requires an enabled text or vector channel",
            ));
        }
        let budget = |enabled: bool, value: Option<usize>| -> Result<usize, RecordError> {
            if !enabled {
                return Ok(0);
            }
            value.filter(|v| *v > 0 && *v <= limit).ok_or_else(|| {
                reject("enabled fusion channels require explicit bounded candidate limits")
            })
        };
        let lex_limit = budget(lexical, rrf.keyword_candidates)?;
        let vec_limit = budget(vector, rrf.vector_candidates)?;
        if lex_limit
            .checked_add(vec_limit)
            .and_then(|v| v.checked_add(options.neighbors.max_candidates))
            .is_none_or(|v| v > limit)
        {
            return Err(reject("combined fusion candidate budget exceeded"));
        }
        let mut filters = query.filters.clone();
        if let Some(t) = query.temporal.clone() {
            filters.extend(temporal::filters(t)?);
        }
        let mut selection = if filters.is_empty() {
            None
        } else {
            let handle = {
                let inner = self.inner.lock().await;
                if !inner.healthy
                    || inner.state.sealed_for.is_some()
                    || self.indexes.error().is_some()
                {
                    return Err(RecordError::RecoveryRequired);
                }
                inner.index.capture_query(query.length).await?
            };
            Some(handle.filter(&filters).await?)
        };
        let neighbors = {
            let mut inner = self.inner.lock().await;
            inner.advance_time().await?;
            if !inner.healthy || inner.state.sealed_for.is_some() || self.indexes.error().is_some()
            {
                return Err(RecordError::RecoveryRequired);
            }
            let components = inner.index.components().await;
            if let Some(selection) = &mut selection {
                selection.revalidate(&inner.locations, &components).await?;
            }
            graph::neighbors_view(&inner, options.neighbors, |id| {
                selection.as_ref().is_none_or(|selection| selection.allows(id))
            })?
        };
        let mut scores = BTreeMap::<String, f32>::new();
        let mut versions = BTreeMap::<String, u64>::new();
        let mut explanations = BTreeMap::<String, Vec<crate::search::ChannelExplanation>>::new();
        let mut terms = Vec::new();
        for (enabled, limit, weight, mode) in [
            (lexical, lex_limit, rrf.keyword_weight, SearchMode::Lexical),
            (
                vector,
                vec_limit,
                rrf.vector_weight,
                match &query.mode {
                    SearchMode::Vector {
                        similarity_threshold,
                        ann_mode,
                    }
                    | SearchMode::Hybrid {
                        similarity_threshold,
                        ann_mode,
                    } => SearchMode::Vector {
                        similarity_threshold: *similarity_threshold,
                        ann_mode: ann_mode.clone(),
                    },
                    _ => SearchMode::Lexical,
                },
            ),
        ] {
            if !enabled {
                continue;
            }
            let is_lexical = matches!(mode, SearchMode::Lexical);
            let result = self
                .query_independent(RecordQuery {
                    temporal: None,
                    text: if is_lexical {
                        query.text.clone()
                    } else {
                        String::new()
                    },
                    vector: if is_lexical {
                        None
                    } else {
                        query.vector.clone()
                    },
                    mode,
                    query_type: query.query_type.clone(),
                    offset: 0,
                    length: limit,
                    result_type: ResultType::Topk,
                    fields: query.fields.clone(),
                    filters: filters.clone(),
                    sort: vec![],
                })
                .await?;
            if is_lexical {
                terms = result.query_terms;
            }
            for (rank, hit) in result.hits.into_iter().enumerate() {
                versions
                    .entry(hit.record.record_id.clone())
                    .and_modify(|version| *version = (*version).max(hit.record.version))
                    .or_insert(hit.record.version);
                let contribution = rrf_contribution(weight, rrf.constant, rank);
                explanations
                    .entry(hit.record.record_id.clone())
                    .or_default()
                    .push(crate::search::ChannelExplanation {
                        channel: if is_lexical { "lexical" } else { "vector" }.into(),
                        rank,
                        rank_basis: "score".into(),
                        raw_score: Some(hit.score),
                        rrf_contribution: Some(contribution),
                    });
                *scores.entry(hit.record.record_id).or_default() += contribution;
            }
        }
        let mut seen = BTreeSet::new();
        if options.relation_weight > 0.0 {
            for hit in &neighbors.hits {
                if seen.insert(hit.neighbor.record_id.clone()) {
                    versions
                        .entry(hit.neighbor.record_id.clone())
                        .and_modify(|version| *version = (*version).max(hit.neighbor.version))
                        .or_insert(hit.neighbor.version);
                    let rank = seen.len() - 1;
                    let contribution =
                        rrf_contribution(options.relation_weight, rrf.constant, rank);
                    explanations
                        .entry(hit.neighbor.record_id.clone())
                        .or_default()
                        .push(crate::search::ChannelExplanation {
                            channel: "relation".into(),
                            rank,
                            rank_basis: "stable_adjacency".into(),
                            raw_score: None,
                            rrf_contribution: Some(contribution),
                        });
                    *scores.entry(hit.neighbor.record_id.clone()).or_default() += contribution;
                }
            }
        }
        let count = scores.len();
        let mut ranks: Vec<_> = scores.into_iter().collect();
        ranks.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let mut inner = self.inner.lock().await;
        inner.advance_time().await?;
        if !inner.healthy || inner.state.sealed_for.is_some() || self.indexes.error().is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        let hits = ranks
            .into_iter()
            .filter(|(id, _)| {
                inner.locations.contains_key(id)
                    && inner
                        .state
                        .records
                        .get(id)
                        .is_some_and(|record| versions.get(id) == Some(&record.version))
            })
            .skip(query.offset)
            .take(query.length)
            .map(|(id, score)| RecordHit {
                channels: explanations.remove(&id).unwrap_or_default(),
                record: inner.state.records[&id].clone(),
                score,
            })
            .collect();
        let result = RecordSearch {
            fusion: Some(FusionWindow {
                count_relation: "candidate_window".into(),
                candidates: count,
                relation_complete: neighbors.complete,
                relation_reason: neighbors.reason,
            }),
            evaluation_time_ms: inner.evaluation_time_ms,
            count_total: count,
            query_terms: terms,
            visible_seq: inner.state.seq,
            hits,
            statistics: inner.statistics(),
        };
        if serde_json::to_vec(&result).map_err(storage)?.len() > 4 * 1024 * 1024 {
            return Err(reject("fusion response exceeds byte budget"));
        }
        barrier.deliver(|| ())?;
        Ok(result)
    }
}
