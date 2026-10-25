//! Compile the existing literal language directly into native queries. Never
//! invoke Tantivy QueryParser: it would introduce syntax and retired features.
use super::*;
use crate::{
    index::{IndexMetaObject, LexicalSimilarity, NgramType, TokenizerType},
    search::QueryType,
    tokenizer::{AnalysisContext, analyze},
};
use ahash::{AHashMap, AHashSet};
use tantivy::query::{AllQuery, BooleanQuery, EmptyQuery, Occur, PhraseQuery, TermQuery};

pub(super) struct Analyzer {
    meta: IndexMetaObject,
    _memory: Option<crate::storage::build_budget::MemoryLease>,
    _buffer: Option<crate::resources::Lease>,
    stop_words: AHashSet<String>,
    frequent: AHashSet<u64>,
    standard: crate::tokenizer::standard::StandardAnalyzer,
}
pub(super) struct ScoreGroup {
    /// A component-scored ngram contributes only when this whole gram matches.
    pub matched_term: String,
    pub components: Vec<String>,
    pub match_fields: Vec<Field>,
}
pub(super) struct Plan {
    pub predicate: Box<dyn Query>,
    pub scores: Vec<ScoreGroup>,
    pub query_terms: Vec<String>,
}
fn width(kind: &NgramType) -> usize {
    match kind {
        NgramType::SingleTerm => 1,
        NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => 2,
        _ => 3,
    }
}
fn across(fields: &[Field], word: &str) -> Box<dyn Query> {
    Box::new(BooleanQuery::new(
        fields
            .iter()
            .map(|field| {
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(*field, word),
                        IndexRecordOption::Basic,
                    )) as Box<dyn Query>,
                )
            })
            .collect(),
    ))
}
impl Analyzer {
    pub(super) fn new(meta: IndexMetaObject) -> Result<Self, RecordError> {
        let (stop_words, _, frequent) = crate::index::analysis_dictionaries(&meta);
        let standard = crate::tokenizer::standard::StandardAnalyzer::new(meta.tokenizer).map_err(storage)?;
        Ok(Self {
            meta,
            _memory: None,
            _buffer: None,
            stop_words,
            frequent,
            standard,
        })
    }
    /// Reserve dictionary storage before construction and retain it with its owner.
    pub(super) fn admitted(meta: IndexMetaObject) -> Result<Self, RecordError> {
        let estimate = 64 * 1024u64;
        let memory = crate::storage::build_budget::reserve_memory(estimate).map_err(storage)?;
        let buffer = crate::resources::Scope::current().map(|scope| scope.retained_io().bytes(estimate))
            .transpose().map_err(storage)?;
        let mut analyzer = Self::new(meta)?;
        analyzer._memory = Some(memory);
        analyzer._buffer = buffer;
        Ok(analyzer)
    }
    fn context(&self) -> AnalysisContext<'_> {
        AnalysisContext {
            text_normalization: self.meta.text_normalization,
            stop_words: &self.stop_words,
            frequent_hashset: &self.frequent,
            standard: Some(&self.standard),
        }
    }
    pub(super) async fn field(&self, input: &str) -> AnalyzedField {
        let mut unique = AHashMap::new();
        let mut length = 0;
        analyze(
            &self.context(),
            input,
            &mut unique,
            &mut Vec::new(),
            self.meta.tokenizer,
            0,
            &mut length,
            65536,
            65536,
            false,
            &mut QueryType::Union,
            self.meta.ngram_indexing,
            0,
            1,
        )
        .await;
        let mut positioned = Vec::new();
        for term in unique.values() {
            for position in &term.field_positions_vec[0] {
                positioned.push((
                    *position as usize,
                    term.term.clone(),
                    width(&term.ngram_type),
                ));
            }
        }
        positioned.sort();
        let mut text = String::new();
        let tokens = positioned
            .into_iter()
            .map(|(position, word, position_length)| {
                let offset_from = text.len();
                text.push_str(&word);
                let token = Token {
                    offset_from,
                    offset_to: text.len(),
                    position,
                    text: word,
                    position_length,
                };
                text.push(' ');
                token
            })
            .collect();
        AnalyzedField {
            text,
            tokens,
            length,
        }
    }
    pub(super) async fn query(
        &self,
        text: &str,
        mode: QueryType,
        fields: &[Field],
        browse: bool,
    ) -> Plan {
        self.query_fields(text, mode, fields, fields, browse).await
    }
    pub(super) async fn query_fields(
        &self,
        text: &str,
        mut mode: QueryType,
        fields: &[Field],
        selected: &[Field],
        browse: bool,
    ) -> Plan {
        let selected = if selected.is_empty() {
            fields
        } else {
            selected
        };
        if browse && text.is_empty() {
            return Plan {
                predicate: Box::new(AllQuery),
                scores: vec![],
                query_terms: vec![],
            };
        }
        let mut tokens = Vec::new();
        analyze(
            &self.context(),
            text,
            &mut AHashMap::new(),
            &mut tokens,
            self.meta.tokenizer,
            0,
            &mut 0,
            65536,
            65536,
            true,
            &mut mode,
            self.meta.ngram_indexing,
            0,
            1,
        )
        .await;
        let mut positive = Vec::new();
        let mut negative = Vec::new();
        let mut phrase = Vec::new();
        let mut phrase_position = 0;
        let mut score_groups = BTreeMap::new();
        let mut query_terms = Vec::new();
        for token in tokens {
            let parts = match width(&token.ngram_type) {
                1 => vec![token.term.clone()],
                2 => vec![token.term_ngram_1.clone(), token.term_ngram_0.clone()],
                _ => vec![
                    token.term_ngram_2.clone(),
                    token.term_ngram_1.clone(),
                    token.term_ngram_0.clone(),
                ],
            };
            if parts.len() > 1 {
                query_terms.extend(parts.iter().cloned());
            }
            query_terms.push(token.term.clone());
            if token.op == QueryType::Not {
                negative.push((Occur::MustNot, across(fields, &token.term)));
                continue;
            }
            if token.op == QueryType::Phrase {
                phrase.push((phrase_position, token.term.clone()));
                phrase_position += parts.len();
            }
            let components = if self.meta.lexical_similarity == LexicalSimilarity::Bm25f {
                parts
            } else {
                vec![token.term.clone()]
            };
            score_groups
                .entry((token.term.clone(), token.ngram_type as u8))
                .or_insert(ScoreGroup {
                    matched_term: token.term.clone(),
                    components,
                    match_fields: selected.to_vec(),
                });
            positive.push((
                if mode == QueryType::Union {
                    Occur::Should
                } else {
                    Occur::Must
                },
                across(selected, &token.term),
            ));
        }
        if positive.is_empty() || fields.is_empty() {
            return Plan {
                predicate: Box::new(EmptyQuery),
                scores: vec![],
                query_terms,
            };
        }
        let mut clauses = vec![(
            Occur::Must,
            Box::new(BooleanQuery::new(positive)) as Box<dyn Query>,
        )];
        clauses.extend(negative);
        // The existing one-distinct-term path collapses repeated terms even
        // inside quotes. Preserve that established behavior in this replacement;
        // fixing its membership is a separate query-contract change.
        if mode == QueryType::Phrase && phrase.len() >= 2 && score_groups.len() > 1 {
            let phrases = selected
                .iter()
                .map(|field| {
                    let terms = phrase
                        .iter()
                        .map(|(offset, term)| (*offset, Term::from_field_text(*field, term)))
                        .collect();
                    (
                        Occur::Should,
                        Box::new(PhraseQuery::new_with_offset(terms)) as Box<dyn Query>,
                    )
                })
                .collect();
            clauses.push((Occur::Must, Box::new(BooleanQuery::new(phrases))));
        }
        Plan {
            predicate: Box::new(BooleanQuery::new(clauses)),
            scores: score_groups.into_values().collect(),
            query_terms,
        }
    }
}
