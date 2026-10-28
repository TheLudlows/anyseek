//! Native Tantivy postings and fixed-searcher execution for Record.
//!
//! Runtime owns production build/recovery; isolated Candidate fixtures exercise
//! the scoring contract without the service. A native commit is a private
//! candidate, never a Record commit. No original body or vector is stored here.
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use tantivy::{
    DocSet, Index, ReloadPolicy, Searcher, TERMINATED, TantivyDocument, Term,
    query::{EnableScoring, Query},
    schema::{FAST, Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions},
    tokenizer::{PreTokenizedString, Token},
};

#[cfg(test)]
use tantivy::{DocAddress, merge_policy::NoMergePolicy, query::Bm25StatisticsProvider};

use super::{RecordError, segment, storage};

const ENGINE: &str = "tantivy-0.27.0";
const FORMAT: u32 = 1;
const WRITER_BYTES: usize = 20_000_000;
// A package stays small; a selected corpus may span multiple packages.
#[cfg(test)]
const MAX_DOCUMENTS: usize = 10_000;
const MAX_POPULATION: usize = super::capacity::MAX_RECORDS;
const MAX_FIELDS: usize = 64;
const MAX_TOKENS: usize = 65_536;

mod directory;
#[cfg(test)]
pub(super) mod package;
pub(crate) mod runtime;
pub(crate) mod runtime_query;
mod query;
mod scoring;

/// Includes vector enrichment revisions independently of the document version.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Identity {
    pub id: String,
    pub version: u64,
    pub revision: u64,
}

/// The caller supplies the same marker to vector and relation execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Cut {
    pub sequence: u64,
    pub evaluation_time_ms: u64,
}

/// Already analyzed positional tokens. Tokenization is outside the native codec;
/// no Tantivy default analyzer can silently change the application contract.
#[cfg(test)]
pub(super) struct Input {
    pub identity: Identity,
    pub fields: Vec<Vec<String>>,
}

pub(super) struct AnalyzedField {
    pub text: String,
    pub tokens: Vec<Token>,
    pub length: u32,
}
#[cfg(test)]
pub(super) struct AnalyzedInput {
    pub identity: Identity,
    pub fields: Vec<AnalyzedField>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u32,
    engine: String,
    cut: Cut,
    fields: Vec<String>,
    identities: Vec<Identity>,
}

/// The outer authority must authenticate this entire closure before selecting
/// it. In particular, discovering a newer meta.json does not select a view.
#[cfg(test)]
pub(super) struct Candidate {
    pub files: segment::Inventory,
}

fn schema(names: &[String]) -> Result<(Schema, Vec<Field>), RecordError> {
    if names.len() > MAX_FIELDS
        || names.iter().any(|n| n.is_empty())
        || names.iter().collect::<BTreeSet<_>>().len() != names.len()
    {
        return Err(storage("invalid lexical fields"));
    }
    let mut builder = Schema::builder();
    // Internal names are independent of untrusted user field names.
    builder.add_u64_field("identity", FAST);
    let mut fields = Vec::new();
    for i in 0..names.len() {
        fields.push(
            builder.add_text_field(
                &format!("text_{i}"),
                TextOptions::default().set_indexing_options(
                    TextFieldIndexing::default()
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions),
                ),
            ),
        );
        builder.add_u64_field(&format!("length_{i}"), FAST);
    }
    Ok((builder.build(), fields))
}

#[cfg(test)]
fn sync_tree(path: &Path) -> Result<(), RecordError> {
    for entry in std::fs::read_dir(path).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        if !entry.file_type().map_err(storage)?.is_file() {
            return Err(storage("unexpected native index entry"));
        }
        std::fs::OpenOptions::new()
            .write(true)
            .open(entry.path())
            .map_err(storage)?
            .sync_all()
            .map_err(storage)?;
    }
    #[cfg(unix)]
    std::fs::File::open(path)
        .map_err(storage)?
        .sync_all()
        .map_err(storage)?;
    Ok(())
}

#[cfg(test)]
impl Candidate {
    /// Build only inside a new private directory. The caller owns removal of a
    /// failed candidate. Writer and merge workers finish before leases release.
    pub(super) fn build(
        path: &Path,
        cut: Cut,
        names: Vec<String>,
        inputs: Vec<Input>,
        byte_limit: u64,
    ) -> Result<Self, RecordError> {
        let inputs = inputs
            .into_iter()
            .map(|input| AnalyzedInput {
                identity: input.identity,
                fields: input
                    .fields
                    .into_iter()
                    .map(|words| {
                        let mut text = String::new();
                        let tokens = words
                            .iter()
                            .enumerate()
                            .map(|(position, word)| {
                                let offset_from = text.len();
                                text.push_str(word);
                                let token = Token {
                                    offset_from,
                                    offset_to: text.len(),
                                    position,
                                    text: word.clone(),
                                    position_length: 1,
                                };
                                text.push(' ');
                                token
                            })
                            .collect();
                        AnalyzedField {
                            text,
                            tokens,
                            length: words.len() as u32,
                        }
                    })
                    .collect(),
            })
            .collect();
        Self::build_analyzed(path, cut, names, inputs, byte_limit)
    }

    pub(super) fn build_analyzed(
        path: &Path,
        cut: Cut,
        names: Vec<String>,
        inputs: Vec<AnalyzedInput>,
        byte_limit: u64,
    ) -> Result<Self, RecordError> {
        if inputs.len() > MAX_DOCUMENTS {
            return Err(storage("lexical document budget exceeded"));
        }
        let (schema, fields) = schema(&names)?;
        let mut seen = BTreeSet::new();
        let mut input_bytes = 0u64;
        for input in &inputs {
            if input.identity.id.is_empty()
                || input.identity.id.len() > 1024
                || input.fields.len() != fields.len()
                || !seen.insert(input.identity.clone())
                || input.fields.iter().any(|f| {
                    f.length as usize > MAX_TOKENS
                        || f.tokens.len() > MAX_TOKENS * 3
                        || f.tokens.iter().any(|t| {
                            t.text.is_empty()
                                || t.text.len() > 4096
                                || t.position >= f.length as usize
                                || t.offset_to > f.text.len()
                                || t.offset_from > t.offset_to
                        })
                        || f.tokens.windows(2).any(|w| w[0].position > w[1].position)
                })
            {
                return Err(storage("invalid lexical input"));
            }
            input_bytes =
                input
                    .fields
                    .iter()
                    .flat_map(|f| &f.tokens)
                    .try_fold(input_bytes, |n, t| {
                        n.checked_add(t.text.len() as u64 + 128)
                            .ok_or_else(|| storage("lexical input budget overflow"))
                    })?;
        }
        let memory_bytes = input_bytes
            .checked_add(WRITER_BYTES as u64)
            .ok_or_else(|| storage("writer budget overflow"))?;
        std::fs::create_dir(path).map_err(storage)?;
        let native = path.join("lexical");
        std::fs::create_dir(&native).map_err(storage)?;
        let directory = directory::BudgetDirectory::new(&native, byte_limit, 1024, memory_bytes)?;
        let index =
            Index::create(directory, schema.clone(), Default::default()).map_err(storage)?;
        let mut writer = index
            .writer_with_options::<TantivyDocument>(
                tantivy::indexer::IndexWriterOptions::builder()
                    .num_worker_threads(1)
                    .num_merge_threads(1)
                    .memory_budget_per_thread(WRITER_BYTES)
                    .build(),
            )
            .map_err(storage)?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        let work = (|| {
            for (ordinal, input) in inputs.iter().enumerate() {
                let mut document = TantivyDocument::default();
                document.add_u64(
                    schema.get_field("identity").map_err(storage)?,
                    ordinal as u64,
                );
                for (i, field) in input.fields.iter().enumerate() {
                    document.add_pre_tokenized_text(
                        fields[i],
                        PreTokenizedString {
                            text: field.text.clone(),
                            tokens: field.tokens.clone(),
                        },
                    );
                    document.add_u64(
                        schema.get_field(&format!("length_{i}")).map_err(storage)?,
                        field.length as u64,
                    );
                }
                writer.add_document(document).map_err(storage)?;
            }
            writer.commit().map_err(storage)?;
            Ok::<_, RecordError>(())
        })();
        // Even a failed add/commit must not release budget while workers run.
        let drained = writer.wait_merging_threads().map_err(storage);
        work?;
        drained?;
        drop(index);
        segment::write_json(
            &path.join("package.json"),
            &Manifest {
                format: FORMAT,
                engine: ENGINE.into(),
                cut,
                fields: names,
                identities: inputs.into_iter().map(|input| input.identity).collect(),
            },
        )?;
        sync_tree(&native)?;
        #[cfg(unix)]
        std::fs::File::open(path)
            .map_err(storage)?
            .sync_all()
            .map_err(storage)?;
        // Also verify retained bytes and the complete package (including metadata).
        Ok(Self {
            files: segment::files(path, byte_limit)?,
        })
    }

    pub(super) fn open(
        &self,
        path: &Path,
        cut: Cut,
        byte_limit: u64,
    ) -> Result<Snapshot, RecordError> {
        // Reserve before hashing, decoding metadata, mapping native files or
        // allocating ordinal/length arrays. Retain the lease with the Searcher.
        if self.files.len() > 1024 {
            return Err(storage("lexical closure file budget exceeded"));
        }
        let declared = self
            .files
            .iter()
            .try_fold(0u64, |n, file| n.checked_add(file.bytes))
            .filter(|n| *n <= byte_limit)
            .ok_or_else(|| storage("lexical closure byte budget exceeded"))?;
        let estimate = declared
            .checked_mul(8)
            .and_then(|n| n.checked_add(64 * 1024))
            .ok_or_else(|| storage("lexical reader estimate overflow"))?;
        let memory = crate::storage::build_budget::reserve_memory(estimate).map_err(storage)?;
        let scope = crate::resources::Scope::current();
        let buffer = scope
            .as_ref()
            .map(|s| s.bytes(estimate))
            .transpose()
            .map_err(storage)?;
        let _io = scope
            .as_ref()
            .map(|s| s.io())
            .transpose()
            .map_err(storage)?;
        let actual = segment::files(path, byte_limit)?;
        if actual.as_slice() != self.files.as_slice()
        {
            return Err(storage("lexical package closure mismatch"));
        }
        let manifest: Manifest = serde_json::from_slice(&segment::read_bounded(
            &path.join("package.json"),
            byte_limit,
        )?)
        .map_err(storage)?;
        if manifest.format != FORMAT
            || manifest.engine != ENGINE
            || manifest.cut != cut
            || manifest.identities.len() > MAX_DOCUMENTS
            || manifest.identities.iter().collect::<BTreeSet<_>>().len()
                != manifest.identities.len()
        {
            return Err(storage("unsupported or inconsistent lexical package"));
        }
        let (expected, fields) = schema(&manifest.fields)?;
        let index = Index::open_in_dir(path.join("lexical")).map_err(storage)?;
        if index.schema() != expected {
            return Err(storage("lexical schema mismatch"));
        }
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(storage)?;
        let searcher = reader.searcher();
        let mut addresses = Vec::new();
        let mut lengths = Vec::new();
        let mut seen = BTreeSet::new();
        for segment in searcher.segment_readers() {
            if segment.num_docs() != segment.max_doc() {
                return Err(storage("unexpected native deletion"));
            }
            let identity = segment.fast_fields().u64("identity").map_err(storage)?;
            let columns = (0..fields.len())
                .map(|i| segment.fast_fields().u64(&format!("length_{i}")))
                .collect::<tantivy::Result<Vec<_>>>()
                .map_err(storage)?;
            let mut mapping = Vec::new();
            let mut segment_lengths = Vec::new();
            for doc in 0..segment.max_doc() {
                let values = identity.values_for_doc(doc).collect::<Vec<_>>();
                if values.len() != 1
                    || values[0] >= manifest.identities.len() as u64
                    || !seen.insert(values[0])
                {
                    return Err(storage("lexical address mapping mismatch"));
                }
                mapping.push(values[0] as usize);
                let mut row = Vec::new();
                for column in &columns {
                    let values = column.values_for_doc(doc).collect::<Vec<_>>();
                    if values.len() != 1 || values[0] > MAX_TOKENS as u64 {
                        return Err(storage("invalid exact field length"));
                    }
                    row.push(values[0]);
                }
                segment_lengths.push(row);
            }
            addresses.push(mapping);
            lengths.push(segment_lengths);
        }
        if seen.len() != manifest.identities.len() {
            return Err(storage("incomplete lexical address mapping"));
        }
        Ok(Snapshot {
            searcher,
            fields,
            cut,
            names: manifest.fields,
            identities: Arc::new(manifest.identities),
            addresses,
            lengths,
            _memory: memory,
            _buffer: buffer,
            _pin: None,
        })
    }
}

/// This struct holds a Searcher, not a reader with automatic reload. The native
/// DocAddress mapping is authenticated again each time a package is opened.
pub(super) struct Snapshot {
    searcher: Searcher,
    pub fields: Vec<Field>,
    names: Vec<String>,
    pub cut: Cut,
    pub(super) identities: Arc<Vec<Identity>>,
    addresses: Vec<Vec<usize>>,
    lengths: Vec<Vec<Vec<u64>>>,
    // Declared after native handles so the charge outlives their destruction.
    _memory: crate::storage::build_budget::MemoryLease,
    _buffer: Option<crate::resources::Lease>,
    _pin: Option<Arc<super::package_pins::Pin>>,
}

pub(super) struct ActiveView<'a> {
    snapshot: &'a Snapshot,
    live: BTreeSet<usize>,
    #[cfg(test)]
    field_tokens: BTreeMap<Field, u64>,
}

impl Snapshot {
    /// Exact revision selection, before both statistics and Top-K. Metadata
    /// filters are supplied separately and do not change corpus statistics.
    pub(super) fn active(
        &self,
        cut: Cut,
        identities: &BTreeSet<Identity>,
    ) -> Result<ActiveView<'_>, RecordError> {
        if cut != self.cut {
            return Err(storage("cross-channel cut mismatch"));
        }
        let mut live = BTreeSet::new();
        let mut record_ids = BTreeSet::new();
        for (ordinal, identity) in self.identities.iter().enumerate() {
            if identities.contains(identity) {
                if !record_ids.insert(&identity.id) {
                    return Err(storage("multiple active revisions"));
                }
                live.insert(ordinal);
            }
        }
        if live.len() != identities.len() {
            return Err(storage("active identity absent from lexical package"));
        }
        #[cfg(test)]
        let field_tokens = {
        let mut field_tokens = BTreeMap::new();
        for (segment, addresses) in self.addresses.iter().enumerate() {
            for (doc, ordinal) in addresses.iter().enumerate() {
                if live.contains(ordinal) {
                    for (field, length) in self.fields.iter().zip(&self.lengths[segment][doc]) {
                        *field_tokens.entry(*field).or_default() += length;
                    }
                }
            }
        }
            field_tokens
        };
        Ok(ActiveView {
            snapshot: self,
            live,
            #[cfg(test)]
            field_tokens,
        })
    }
}

#[cfg(test)]
impl Bm25StatisticsProvider for ActiveView<'_> {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        Ok(self.field_tokens.get(&field).copied().unwrap_or(0).max(1))
    }
    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.live.len() as u64)
    }
    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        let mut frequency = 0;
        for (segment_id, segment) in self.snapshot.searcher.segment_readers().iter().enumerate() {
            let inverted = segment.inverted_index(term.field())?;
            if let Some(mut postings) = inverted.read_postings(term, IndexRecordOption::Basic)? {
                while postings.doc() != TERMINATED {
                    if self
                        .live
                        .contains(&self.snapshot.addresses[segment_id][postings.doc() as usize])
                    {
                        frequency += 1;
                    }
                    postings.advance();
                }
            }
        }
        Ok(frequency)
    }
}

#[derive(Debug, PartialEq)]
pub(super) struct Hit {
    pub identity: Identity,
    pub score: f32,
}

impl ActiveView<'_> {
    /// Explicitly a BM25 *probe*, not an implementation of the legacy BM25F
    /// contract. Exhaustive scoring avoids unproven physical-statistics pruning
    /// bounds. Memory remains bounded by K, independently of invalid matches.
    #[cfg(test)]
    pub(super) fn bm25_probe(
        &self,
        query: &dyn Query,
        limit: usize,
        accepts: impl Fn(&Identity) -> bool,
    ) -> Result<(usize, Vec<Hit>), RecordError> {
        if limit > MAX_DOCUMENTS {
            return Err(storage("candidate budget exceeded"));
        }
        if self.live.is_empty() {
            return Ok((0, Vec::new()));
        }
        let weight = query
            .weight(EnableScoring::enabled_from_statistics_provider(
                self,
                &self.snapshot.searcher,
            ))
            .map_err(storage)?;
        let mut count = 0;
        let mut hits: Vec<Hit> = Vec::new();
        for (segment_id, segment) in self.snapshot.searcher.segment_readers().iter().enumerate() {
            let mut scorer = weight.scorer(segment, 1.0).map_err(storage)?;
            while scorer.doc() != TERMINATED {
                let address = DocAddress::new(segment_id as u32, scorer.doc());
                let ordinal =
                    self.snapshot.addresses[address.segment_ord as usize][address.doc_id as usize];
                let identity = &self.snapshot.identities[ordinal];
                if self.live.contains(&ordinal) && accepts(identity) {
                    count += 1;
                    let score = scorer.score();
                    if !score.is_finite() {
                        return Err(storage("nonfinite lexical score"));
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

#[cfg(test)]
mod tests;
