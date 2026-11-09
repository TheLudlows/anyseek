//! BM25F/Proximity arithmetic over native postings and the selected active corpus.
use super::*;
use crate::index::{DOCUMENT_LENGTH_COMPRESSION, FieldLengthNormalization, int_to_byte4};
use tantivy::postings::Postings;

fn decoded(length: u64) -> f32 {
    DOCUMENT_LENGTH_COMPRESSION[int_to_byte4(length as u32) as usize] as f32
}

// Owned by one query over one selected corpus; never cached across views.
struct QueryStatistics<'a> {
    terms: BTreeSet<&'a String>,
    slots: BTreeMap<&'a str, usize>,
    averages: Vec<f32>,
    idfs: Vec<f32>,
}

impl<'a> QueryStatistics<'a> {
    fn build(
        corpus: &[&ActiveView<'_>],
        groups: &'a [super::query::ScoreGroup],
        field_count: usize,
        normalization: FieldLengthNormalization,
    ) -> Result<Self, RecordError> {
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
        let mut sums = vec![0u64; field_count];
        let mut nonempty = vec![0u64; field_count];
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
        for view in corpus {
            for (ordinal,row) in &view.snapshot.overlay {if view.live.contains(ordinal){
                for (field,length) in row.lengths.iter().enumerate(){sums[field]+=decoded(*length) as u64;nonempty[field]+=u64::from(*length>0);}
            }}
        }
        let mut averages = vec![1.0f32; field_count];
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
                    for field in view.snapshot.fields.iter() {
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
                for (ordinal,row) in &view.snapshot.overlay {if view.live.contains(ordinal) && row.postings.iter().any(|field|field.contains_key(word.as_str())){present.insert(*ordinal);}}
                df += present.len();
            }
            let n = population as f32;
            let df = df as f32;
            idfs.push((((n - df + 0.5) / (df + 0.5)) + 1.0).ln());
        }
        Ok(Self { terms, slots, averages, idfs })
    }
}

pub(super) fn validate_bounds(
    groups: &[super::query::ScoreGroup],
    boosts: &[f32],
    field_count: usize,
    limit: usize,
) -> Result<(), RecordError> {
    if groups.len() > 100
        || limit > MAX_POPULATION
        || boosts.len() != field_count
        || boosts.iter().any(|b| !b.is_finite() || *b < 0.0)
    {
        return Err(storage("invalid BM25F probe bounds"));
    }
    Ok(())
}

impl ActiveView<'_> {
    #[cfg(test)]
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
                match_fields: vec![],
            })
            .collect::<Vec<_>>();
        self.score_groups(query, &groups, boosts, normalization, limit, accepts)
    }

    #[cfg(test)]
    pub(super) fn score_groups(
        &self,
        query: &dyn Query,
        groups: &[super::query::ScoreGroup],
        boosts: &[f32],
        normalization: FieldLengthNormalization,
        limit: usize,
        accepts: impl Fn(&Identity) -> bool,
    ) -> Result<(usize, Vec<Hit>), RecordError> {
        validate_bounds(groups, boosts, self.snapshot.fields.len(), limit)?;
        let statistics = QueryStatistics::build(&[self], groups, boosts.len(), normalization)?;
        let mut hits = Vec::new();
        let count = self.score_groups_in(
            &statistics, query, groups, boosts, accepts,
            &mut |identity, score| hits.push(Hit { identity: identity.clone(), score }),
        )?;
        sort_probe_hits(&mut hits, limit);
        Ok((count, hits))
    }

    fn score_groups_in(
        &self,
        statistics: &QueryStatistics<'_>,
        query: &dyn Query,
        groups: &[super::query::ScoreGroup],
        boosts: &[f32],
        accepts: impl Fn(&Identity) -> bool,
        collect: &mut impl FnMut(&Identity, f32),
    ) -> Result<usize, RecordError> {
        if self.live.is_empty() {
            return Ok(0);
        }
        let QueryStatistics { terms, slots, averages, idfs } = statistics;
        // Tantivy executes the complete Boolean/phrase predicate, but cannot
        // prune with its own BM25 score bounds for this different score.
        let weight = query
            .weight(EnableScoring::disabled_from_searcher(
                &self.snapshot.searcher,
            ))
            .map_err(storage)?;
        let mut count = 0;
        for (segment_id, segment) in self.snapshot.searcher.segment_readers().iter().enumerate() {
            let mut postings = Vec::new();
            for word in terms {
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
                        if !gate.iter().enumerate().any(|(field, posting)| {
                            (group.match_fields.is_empty()
                                || group.match_fields.contains(&self.snapshot.fields[field]))
                                && posting.as_ref().is_some_and(|posting| posting.doc() == doc)
                        }) {
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
                    collect(identity, score);
                }
                scorer.advance();
            }
        }
        if !self.snapshot.overlay.is_empty() {
            let predicate=query.downcast_ref::<super::query::Compiled>().ok_or_else(||storage("visible delta requires shared query compiler"))?;
            for (ordinal,row) in &self.snapshot.overlay {
                // Match before admission callbacks, just like the native scorer.
                // Hybrid channel accounting happens in accepts and must not see nonmatches.
                if !self.live.contains(ordinal) || !predicate.matches(row,&self.snapshot.fields) || !accepts(&row.identity){continue;}
                count+=1;let mut score=0.0f32;
                for group in groups {
                    if !row.postings.iter().enumerate().any(|(field,terms)|
                        (group.match_fields.is_empty() || group.match_fields.contains(&self.snapshot.fields[field])) && terms.contains_key(&group.matched_term)){continue;}
                    for component in &group.components {let slot=slots[component.as_str()];
                        for (field,terms) in row.postings.iter().enumerate(){let Some(positions)=terms.get(component) else{continue};
                            let tf=positions.len() as f32;let length=decoded(row.lengths[field]);
                            let denominator=1.2*(1.0-0.75+0.75*(length/averages[field]));
                            score+=boosts[field]*idfs[slot]*(tf*(1.2+1.0)/(tf+denominator));
                        }
                    }
                }
                if !score.is_finite(){return Err(storage("nonfinite delta BM25F score"));}
                collect(&row.identity,score);
            }
        }
        Ok(count)
    }
}

/// One selected cut across frozen native packages and a native tail. The caller
/// supplies the authority-selected revision set; package commits confer no authority.
pub(super) struct CorpusView<'a> {
    views: Vec<ActiveView<'a>>,
}
impl<'a> CorpusView<'a> {
    #[cfg(test)]
    pub(super) fn select(
        snapshots: &[&'a Snapshot],
        cut: Cut,
        identities: &BTreeSet<Identity>,
    ) -> Result<Self, RecordError> {
        if identities.len() > MAX_POPULATION {
            return Err(storage("native corpus topology budget exceeded"));
        }
        if identities.iter().map(|i| &i.id).collect::<BTreeSet<_>>().len() != identities.len() {
            return Err(storage("multiple active revisions across packages"));
        }
        let corpus = Self::select_matching(snapshots, cut, |identity| identities.contains(identity))?;
        if corpus.views.iter().map(|view| view.live.len()).sum::<usize>() != identities.len() {
            return Err(storage("selected revision absent from native corpus"));
        }
        Ok(corpus)
    }

    /// Join only matching native identities: asynchronous consumers may expose
    /// different prefixes. Unselected rows do not affect the selected corpus.
    /// Borrow snapshot identities and construct ordinal views in this one pass.
    pub(super) fn select_matching(
        snapshots: &[&'a Snapshot],
        cut: Cut,
        mut matches: impl FnMut(&'a Identity) -> bool,
    ) -> Result<Self, RecordError> {
        if snapshots.is_empty() || snapshots.len() > 64 {
            return Err(storage("native corpus topology budget exceeded"));
        }
        let mut found = BTreeMap::new();
        let mut views = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            // Validate even when no identities match this package.
            if snapshot.cut.sequence > cut.sequence
                || snapshot.cut.evaluation_time_ms > cut.evaluation_time_ms
                || snapshot.names != snapshots[0].names
            {
                return Err(storage("native corpus cut/schema mismatch"));
            }
            let mut live = BTreeSet::new();
            for (ordinal, identity) in snapshot.identities.iter().enumerate() {
                if !matches(identity) { continue; }
                if let Some(previous) = found.insert(identity.id.as_str(), identity) {
                    return Err(storage(if previous == identity {
                        "duplicate selected native revision"
                    } else { "multiple active revisions across packages" }));
                }
                if found.len() > MAX_POPULATION {
                    return Err(storage("native corpus topology budget exceeded"));
                }
                live.insert(ordinal);
            }
            views.push(snapshot.with_live(live));
        }
        Ok(Self { views })
    }
    /// Execute the same predicate and active identity gate without score postings,
    /// corpus statistics, candidate buffers, or ordering.
    pub(super) fn count(
        &self,
        query: &dyn Query,
        accepts: impl Fn(&Identity) -> bool,
    ) -> Result<usize, RecordError> {
        let mut count = 0;
        for view in &self.views {
            if view.live.is_empty() {
                continue;
            }
            let weight = query
                .weight(EnableScoring::disabled_from_searcher(&view.snapshot.searcher))
                .map_err(storage)?;
            for (segment_id, segment) in view.snapshot.searcher.segment_readers().iter().enumerate() {
                let mut scorer = weight.scorer(segment, 1.0).map_err(storage)?;
                while scorer.doc() != TERMINATED {
                    let ordinal = view.snapshot.addresses[segment_id][scorer.doc() as usize];
                    if view.live.contains(&ordinal) && accepts(&view.snapshot.identities[ordinal]) {
                        count += 1;
                    }
                    scorer.advance();
                }
            }
        }
        for view in &self.views {if !view.snapshot.overlay.is_empty(){
            let predicate=query.downcast_ref::<super::query::Compiled>().ok_or_else(||storage("visible delta requires shared query compiler"))?;
            for (ordinal,row) in &view.snapshot.overlay {if view.live.contains(ordinal) && predicate.matches(row,&view.snapshot.fields) && accepts(&row.identity){count+=1;}}
        }}
        // select() bounds the unique active population by MAX_POPULATION.
        Ok(count)
    }

    /// Stream every accepted score into one query-owned collector across all views.
    pub(super) fn collect_scores(
        &self,
        query: &dyn Query,
        groups: &[super::query::ScoreGroup],
        boosts: &[f32],
        normalization: FieldLengthNormalization,
        accepts: impl Fn(&Identity) -> bool,
        mut collect: impl FnMut(&Identity, f32),
    ) -> Result<usize, RecordError> {
        validate_bounds(groups, boosts, self.views[0].snapshot.fields.len(), 0)?;
        let references: Vec<_> = self.views.iter().collect();
        let statistics = QueryStatistics::build(&references, groups, boosts.len(), normalization)?;
        let mut count = 0usize;
        for view in &self.views {
            let matched = view.score_groups_in(
                &statistics, query, groups, boosts, &accepts, &mut collect,
            )?;
            count = count.checked_add(matched)
                .ok_or_else(|| storage("native corpus count overflow"))?;
        }
        Ok(count)
    }

    #[cfg(test)]
    pub(super) fn score_groups(
        &self,
        query: &dyn Query,
        groups: &[super::query::ScoreGroup],
        boosts: &[f32],
        normalization: FieldLengthNormalization,
        limit: usize,
        accepts: impl Fn(&Identity) -> bool,
    ) -> Result<(usize, Vec<Hit>), RecordError> {
        validate_bounds(groups, boosts, self.views[0].snapshot.fields.len(), limit)?;
        let mut hits = Vec::new();
        let count = self.collect_scores(query, groups, boosts, normalization, accepts,
            |identity, score| hits.push(Hit { identity: identity.clone(), score }))?;
        sort_probe_hits(&mut hits, limit);
        Ok((count, hits))
    }
}

// Full-candidate oracle for the native scoring probes, independent of the heap.
#[cfg(test)]
fn sort_probe_hits(hits: &mut Vec<Hit>, limit: usize) {
    hits.sort_by(|a, b| crate::min_heap::score_ordering(b.score, a.score)
        .then_with(|| a.identity.cmp(&b.identity)));
    hits.truncate(limit);
}
