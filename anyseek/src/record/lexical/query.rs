//! Compile the existing literal language directly into native queries. Never
//! invoke Tantivy QueryParser: it would introduce syntax and retired features.
use super::*;
use crate::{
    index::{IndexMetaObject, LexicalSimilarity, NgramType},
    search::QueryType,
    tokenizer::{AnalysisContext, analyze},
};
use ahash::{AHashMap, AHashSet};
use tantivy::query::{AllQuery, BooleanQuery as NativeBooleanQuery, EmptyQuery, Occur, PhraseQuery as NativePhraseQuery, TermQuery as NativeTermQuery};

pub(super) struct Analyzer {
    #[cfg(test)]
    pub(super) field_calls: std::sync::atomic::AtomicUsize,
    meta: IndexMetaObject,
    _memory: Option<crate::resources::MemoryReservation>,
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
            #[cfg(test)]
            field_calls: Default::default(),
            meta,
            _memory: None,
            stop_words,
            frequent,
            standard,
        })
    }
    /// Reserve dictionary storage before construction and retain it with its owner.
    pub(super) fn admitted(meta: IndexMetaObject) -> Result<Self, RecordError> {
        let estimate = 64 * 1024u64;
        let memory = crate::record::build_budget::retained_memory(estimate)?;
        let mut analyzer = Self::new(meta)?;
        analyzer._memory = Some(memory);
        Ok(analyzer)
    }
    fn context(&self) -> AnalysisContext<'_> {
        AnalysisContext {
            text_normalization: self.meta.text_normalization,
            stop_words: &self.stop_words,
            frequent_hashset: &self.frequent,
            standard: &self.standard,
        }
    }
    pub(super) async fn field(&self, input: &str) -> AnalyzedField {
        #[cfg(test)]
        self.field_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut unique = AHashMap::new();
        let mut length = 0;
        analyze(
            &self.context(),
            input,
            &mut unique,
            &mut Vec::new(),
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
                predicate: Box::new(Compiled::all()),
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
                predicate: Box::new(Compiled::empty()),
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

// One compiler owns both native execution and the exact memory predicate.
// In particular phrase offsets survive ngram expansion and stopword analysis.
#[derive(Clone, Debug)]
enum Expr {All,Empty,Term(Field,String),Boolean(Vec<(Occur,Expr)>),Phrase(Field,Vec<(usize,String)>)}
#[derive(Debug)]
pub(super) struct Compiled { native:Box<dyn Query>, expr:Expr }
impl Clone for Compiled {fn clone(&self)->Self{Self{native:self.native.box_clone(),expr:self.expr.clone()}}}
impl Query for Compiled {
    fn weight(&self,e:EnableScoring<'_>)->tantivy::Result<Box<dyn tantivy::query::Weight>>{self.native.weight(e)}
    fn query_terms<'a>(&'a self,v:&mut dyn FnMut(&'a Term,bool)){self.native.query_terms(v)}
}
impl Compiled {
    fn all()->Self{Self{native:Box::new(AllQuery),expr:Expr::All}}
    fn empty()->Self{Self{native:Box::new(EmptyQuery),expr:Expr::Empty}}
    pub(super) fn matches(&self,row:&super::memory::Row,fields:&[Field])->bool{self.expr.matches(row,fields)}
}
impl Expr {
    fn matches(&self,row:&super::memory::Row,fields:&[Field])->bool{match self {
        Self::All=>true,Self::Empty=>false,Self::Term(f,t)=>!row.positions(fields,*f,t).is_empty(),
        Self::Boolean(clauses)=>{let mut must=false;let mut should=false;let mut hit=false;
            for (o,e) in clauses{let yes=e.matches(row,fields);match o{Occur::Must=>{must=true;if !yes{return false}},Occur::MustNot=>if yes{return false},Occur::Should=>{should=true;hit|=yes}}}
            must || (should&&hit)
        },
        Self::Phrase(field,terms)=>{let Some((first_offset,first))=terms.first() else{return false};
            row.positions(fields,*field,first).iter().any(|start|terms.iter().all(|(offset,term)|{
                let target=i64::from(*start)+*offset as i64-*first_offset as i64;
                target>=0 && row.positions(fields,*field,term).binary_search(&(target as u32)).is_ok()
            }))
        }
    }}
}
struct BooleanQuery;
impl BooleanQuery {fn new(clauses:Vec<(Occur,Box<dyn Query>)>)->Compiled{
    let expr=Expr::Boolean(clauses.iter().map(|(o,q)|(*o,q.downcast_ref::<Compiled>().expect("compiler owns every clause").expr.clone())).collect());
    Compiled{native:Box::new(NativeBooleanQuery::new(clauses)),expr}
}}
struct TermQuery;
impl TermQuery {fn new(term:Term,option:IndexRecordOption)->Compiled{let expr=Expr::Term(term.field(),term.value().as_str().expect("text term").to_owned());Compiled{native:Box::new(NativeTermQuery::new(term,option)),expr}}}
struct PhraseQuery;
impl PhraseQuery {fn new_with_offset(terms:Vec<(usize,Term)>)->Compiled{let expr=Expr::Phrase(terms[0].1.field(),terms.iter().map(|(o,t)|(*o,t.value().as_str().expect("text term").to_owned())).collect());Compiled{native:Box::new(NativePhraseQuery::new_with_offset(terms)),expr}}}
