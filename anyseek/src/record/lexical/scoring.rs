//! Existing unigram BM25F arithmetic over native Tantivy postings. This proves
//! the statistics provider alone is insufficient, without changing public APIs.
use super::*;
use crate::index::{DOCUMENT_LENGTH_COMPRESSION, FieldLengthNormalization, int_to_byte4};
use tantivy::postings::Postings;

fn decoded(length: u64) -> f32 {
    DOCUMENT_LENGTH_COMPRESSION[int_to_byte4(length as u32) as usize] as f32
}

impl ActiveView<'_> {
    pub(super) fn bm25f_unigram_probe(
        &self,
        query: &dyn Query,
        terms: &[String],
        boosts: &[f32],
        normalization: FieldLengthNormalization,
        limit: usize,
        accepts: impl Fn(&Identity) -> bool,
    ) -> Result<(usize, Vec<Hit>), RecordError> {
        let groups = terms
            .iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|term| super::query::ScoreGroup {
                matched_term: term.clone(),
                components: vec![term.clone()],
            })
            .collect::<Vec<_>>();
        self.score_groups(query, &groups, boosts, normalization, limit, accepts)
    }

    pub(super) fn score_groups(
        &self,
        query: &dyn Query,
        groups: &[super::query::ScoreGroup],
        boosts: &[f32],
        normalization: FieldLengthNormalization,
        limit: usize,
        accepts: impl Fn(&Identity) -> bool,
    ) -> Result<(usize, Vec<Hit>), RecordError> {
        self.score_groups_in(
            &[self],
            query,
            groups,
            boosts,
            normalization,
            limit,
            accepts,
        )
    }

    fn score_groups_in(
        &self,
        corpus: &[&ActiveView<'_>],
        query: &dyn Query,
        groups: &[super::query::ScoreGroup],
        boosts: &[f32],
        normalization: FieldLengthNormalization,
        limit: usize,
        accepts: impl Fn(&Identity) -> bool,
    ) -> Result<(usize, Vec<Hit>), RecordError> {
        if groups.len() > 100
            || limit > MAX_DOCUMENTS
            || boosts.len() != self.snapshot.fields.len()
            || boosts.iter().any(|b| !b.is_finite() || *b < 0.0)
        {
            return Err(storage("invalid BM25F probe bounds"));
        }
        if self.live.is_empty() {
            return Ok((0, Vec::new()));
        }
        // Preserve accumulation order independently of Tantivy's execution order.
        let terms = groups
            .iter()
            .flat_map(|group| std::iter::once(&group.matched_term).chain(group.components.iter()))
            .collect::<BTreeSet<_>>();
        let slots: BTreeMap<_, _> = terms
            .iter()
            .enumerate()
            .map(|(slot, term)| (term.as_str(), slot))
            .collect();
        let mut sums = vec![0u64; boosts.len()];
        let mut nonempty = vec![0u64; boosts.len()];
        let population: usize = corpus.iter().map(|v| v.live.len()).sum();
        for view in corpus {
            for (segment, addresses) in view.snapshot.addresses.iter().enumerate() {
                for (doc, ordinal) in addresses.iter().enumerate() {
                    if !view.live.contains(ordinal) {
                        continue;
                    }
                    for (field, length) in view.snapshot.lengths[segment][doc].iter().enumerate() {
                        sums[field] += decoded(*length) as u64;
                        nonempty[field] += u64::from(*length > 0);
                    }
                }
            }
        }
        let mut averages = vec![1.0f32; boosts.len()];
        if normalization == FieldLengthNormalization::Shared {
            let sum = sums.iter().sum::<u64>();
            let mean = if sum == 0 {
                1.0
            } else {
                sum as f32 / population as f32
            };
            averages.fill(mean);
        } else {
            for ((average, sum), count) in averages.iter_mut().zip(sums).zip(nonempty) {
                *average = if count == 0 || sum == 0 {
                    1.0
                } else {
                    sum as f32 / count as f32
                };
            }
        }
        let mut idfs = Vec::new();
        for word in &terms {
            // DF counts each active stable revision once across all fields.
            let mut df = 0usize;
            for view in corpus {
                let mut present = BTreeSet::new();
                for (segment_id, segment) in
                    view.snapshot.searcher.segment_readers().iter().enumerate()
                {
                    for field in &view.snapshot.fields {
                        if let Some(mut postings) = segment
                            .inverted_index(*field)
                            .map_err(storage)?
                            .read_postings(
                                &Term::from_field_text(*field, word),
                                IndexRecordOption::Basic,
                            )
                            .map_err(storage)?
                        {
                            while postings.doc() != TERMINATED {
                                let ordinal =
                                    view.snapshot.addresses[segment_id][postings.doc() as usize];
                                if view.live.contains(&ordinal) {
                                    present.insert(ordinal);
                                }
                                postings.advance();
                            }
                        }
                    }
                }
                df += present.len();
            }
            let n = population as f32;
            let df = df as f32;
            idfs.push((((n - df + 0.5) / (df + 0.5)) + 1.0).ln());
        }
        // Tantivy executes the complete Boolean/phrase predicate, but cannot
        // prune with its own BM25 score bounds for this different score.
        let weight = query
            .weight(EnableScoring::disabled_from_searcher(
                &self.snapshot.searcher,
            ))
            .map_err(storage)?;
        let mut hits: Vec<Hit> = Vec::new();
        let mut count = 0;
        for (segment_id, segment) in self.snapshot.searcher.segment_readers().iter().enumerate() {
            let mut postings = Vec::new();
            for word in &terms {
                let fields = self
                    .snapshot
                    .fields
                    .iter()
                    .map(|field| {
                        Ok(segment.inverted_index(*field)?.read_postings(
                            &Term::from_field_text(*field, word),
                            IndexRecordOption::WithFreqs,
                        )?)
                    })
                    .collect::<tantivy::Result<Vec<_>>>()
                    .map_err(storage)?;
                postings.push(fields);
            }
            let mut scorer = weight.scorer(segment, 1.0).map_err(storage)?;
            while scorer.doc() != TERMINATED {
                let doc = scorer.doc();
                let ordinal = self.snapshot.addresses[segment_id][doc as usize];
                let identity = &self.snapshot.identities[ordinal];
                if self.live.contains(&ordinal) && accepts(identity) {
                    count += 1;
                    let mut score = 0.0f32;
                    for fields in &mut postings {
                        for posting in fields.iter_mut().flatten() {
                            if posting.doc() < doc {
                                posting.seek(doc);
                            }
                        }
                    }
                    for group in groups {
                        let gate = &postings[slots[group.matched_term.as_str()]];
                        if !gate.iter().flatten().any(|posting| posting.doc() == doc) {
                            continue;
                        }
                        for component in &group.components {
                            let slot = slots[component.as_str()];
                            for (field, posting) in postings[slot].iter().enumerate() {
                                let Some(posting) = posting else {
                                    continue;
                                };
                                if posting.doc() != doc {
                                    continue;
                                }
                                let tf = posting.term_freq() as f32;
                                let length =
                                    decoded(self.snapshot.lengths[segment_id][doc as usize][field]);
                                let denominator =
                                    1.2 * (1.0 - 0.75 + 0.75 * (length / averages[field]));
                                score += boosts[field]
                                    * idfs[slot]
                                    * (tf * (1.2 + 1.0) / (tf + denominator));
                            }
                        }
                    }
                    if !score.is_finite() {
                        return Err(storage("nonfinite BM25F score"));
                    }
                    let hit = Hit {
                        identity: identity.clone(),
                        score,
                    };
                    let position = hits.partition_point(|old| {
                        old.score > hit.score
                            || (old.score == hit.score && old.identity < hit.identity)
                    });
                    if position < limit {
                        hits.insert(position, hit);
                        hits.truncate(limit);
                    }
                }
                scorer.advance();
            }
        }
        Ok((count, hits))
    }
}

/// One selected cut across frozen native packages and a native tail. The caller
/// supplies the authority-selected revision set; package commits confer no authority.
pub(super) struct CorpusView<'a> {
    views: Vec<ActiveView<'a>>,
}
impl<'a> CorpusView<'a> {
    pub(super) fn select(
        snapshots: &[&'a Snapshot],
        cut: Cut,
        identities: &BTreeSet<Identity>,
    ) -> Result<Self, RecordError> {
        if snapshots.is_empty() || snapshots.len() > 64 || identities.len() > MAX_DOCUMENTS {
            return Err(storage("native corpus topology budget exceeded"));
        }
        if identities
            .iter()
            .map(|i| &i.id)
            .collect::<BTreeSet<_>>()
            .len()
            != identities.len()
        {
            return Err(storage("multiple active revisions across packages"));
        }
        let mut found = BTreeSet::new();
        let mut views = Vec::new();
        for snapshot in snapshots {
            if snapshot.cut.sequence > cut.sequence
                || snapshot.cut.evaluation_time_ms > cut.evaluation_time_ms
                || snapshot.names != snapshots[0].names
            {
                return Err(storage("native corpus cut/schema mismatch"));
            }
            let mut selected = BTreeSet::new();
            for identity in snapshot
                .identities
                .iter()
                .filter(|i| identities.contains(*i))
            {
                if !found.insert(identity.clone()) {
                    return Err(storage("duplicate selected native revision"));
                }
                selected.insert(identity.clone());
            }
            views.push(snapshot.active(snapshot.cut, &selected)?);
        }
        if &found != identities {
            return Err(storage("selected revision absent from native corpus"));
        }
        Ok(Self { views })
    }
    pub(super) fn score_groups(
        &self,
        query: &dyn Query,
        groups: &[super::query::ScoreGroup],
        boosts: &[f32],
        normalization: FieldLengthNormalization,
        limit: usize,
        accepts: impl Fn(&Identity) -> bool,
    ) -> Result<(usize, Vec<Hit>), RecordError> {
        if limit > MAX_DOCUMENTS {
            return Err(storage("native corpus candidate budget exceeded"));
        }
        let references: Vec<_> = self.views.iter().collect();
        let mut count = 0usize;
        let mut hits: Vec<Hit> = Vec::new();
        for view in &self.views {
            let (matched, candidates) = view.score_groups_in(
                &references,
                query,
                groups,
                boosts,
                normalization.clone(),
                limit,
                &accepts,
            )?;
            count = count
                .checked_add(matched)
                .ok_or_else(|| storage("native corpus count overflow"))?;
            for hit in candidates {
                let at = hits.partition_point(|old| {
                    old.score > hit.score || (old.score == hit.score && old.identity < hit.identity)
                });
                if at < limit {
                    hits.insert(at, hit);
                    hits.truncate(limit);
                }
            }
        }
        Ok((count, hits))
    }
}
