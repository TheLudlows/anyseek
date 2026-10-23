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
        for (segment, addresses) in self.snapshot.addresses.iter().enumerate() {
            for (doc, ordinal) in addresses.iter().enumerate() {
                if !self.live.contains(ordinal) {
                    continue;
                }
                for (field, length) in self.snapshot.lengths[segment][doc].iter().enumerate() {
                    sums[field] += decoded(*length) as u64;
                    nonempty[field] += u64::from(*length > 0);
                }
            }
        }
        let mut averages = vec![1.0f32; boosts.len()];
        if normalization == FieldLengthNormalization::Shared {
            let sum = sums.iter().sum::<u64>();
            let mean = if sum == 0 {
                1.0
            } else {
                sum as f32 / self.live.len() as f32
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
            let mut present = BTreeSet::new();
            for (segment_id, segment) in self.snapshot.searcher.segment_readers().iter().enumerate()
            {
                for field in &self.snapshot.fields {
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
                                self.snapshot.addresses[segment_id][postings.doc() as usize];
                            if self.live.contains(&ordinal) {
                                present.insert(ordinal);
                            }
                            postings.advance();
                        }
                    }
                }
            }
            let n = self.live.len() as f32;
            let df = present.len() as f32;
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
