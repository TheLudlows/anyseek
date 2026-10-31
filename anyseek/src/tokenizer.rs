use std::cmp;

use ahash::{AHashMap, AHashSet};

use crate::{
    index::{
        MAX_QUERY_TERM_NUMBER, NgramSet, NgramType, NonUniqueTermObject, TermObject,
        hash32, hash64,
    },
    search::QueryType,
};

pub(crate) mod standard;

/// Analysis configuration independent of physical vector/metadata storage.
/// Native Tantivy indexing and query compilation can reuse the exact same
/// normalization, segmentation and syntax without creating a legacy index.
pub(crate) struct AnalysisContext<'a> {
    pub text_normalization: crate::index::TextNormalization,
    pub stop_words: &'a AHashSet<String>,
    pub frequent_hashset: &'a AHashSet<u64>,
    pub standard: &'a standard::StandardAnalyzer,
}
#[allow(clippy::too_many_arguments)]
#[allow(clippy::assigning_clones)]
pub(crate) async fn analyze(
    index: &AnalysisContext<'_>,
    text: &str,
    unique_terms: &mut AHashMap<String, TermObject>,
    non_unique_terms: &mut Vec<NonUniqueTermObject>,
    segment_number_mask1: u32,
    nonunique_terms_count: &mut u32,
    token_per_field_max: u32,
    position_per_term_max: usize,
    is_query: bool,
    query_type: &mut QueryType,
    ngram_indexing: u8,
    indexed_field_id: usize,
    indexed_field_number: usize,
) {
    // Normalize only the transient analysis view, never the stored document.
    // Both index and query enter this same path with the persisted shard rule.
    let normalized = match index.text_normalization {
        crate::index::TextNormalization::None => std::borrow::Cow::Borrowed(text),
        crate::index::TextNormalization::OpenCcT2s1_1_9 => crate::opencc_t2s::convert(text),
    };
    let text = normalized.as_ref();
    let token_per_field_max_capped = cmp::max(token_per_field_max, 65_536);

    let standard_terms = index.standard.terms(text, is_query);
    let mut non_unique_terms_line: Vec<&str> = standard_terms.iter().map(String::as_str).collect();

    if is_query && non_unique_terms_line.len() > MAX_QUERY_TERM_NUMBER {
        non_unique_terms_line.truncate(MAX_QUERY_TERM_NUMBER);
    }

    let mut position: u32 = 0;
    let mut is_phrase = query_type == &QueryType::Phrase;
    let mut term_string_1 = "".to_string();
    let mut term_frequent_1 = false;
    let mut term_string_2 = "".to_string();
    let mut term_frequent_2 = false;

    let mut non_unique_terms_raw = Vec::new();

    for term_string in non_unique_terms_line.iter_mut() {
        if is_query {
            let mut query_type_term = if is_phrase {
                QueryType::Phrase
            } else {
                query_type.clone()
            };
            if term_string.starts_with('+') {
                if query_type != &QueryType::Phrase {
                    *query_type = QueryType::Intersection;
                }
                query_type_term = QueryType::Intersection;
                *term_string = &term_string[1..];
            } else if term_string.starts_with('-') {
                query_type_term = QueryType::Not;
                *term_string = &term_string[1..];
            }
            if term_string.starts_with('\"') {
                is_phrase = true;
                *query_type = QueryType::Phrase;
                query_type_term = QueryType::Phrase;
                *term_string = &term_string[1..];
            }
            if term_string.ends_with('\"') {
                *query_type = QueryType::Phrase;
                *term_string = &term_string[0..term_string.len() - 1];
                is_phrase = false;
            }

            if term_string.is_empty() {
                continue;
            }

            if !index.stop_words.is_empty() && index.stop_words.contains(*term_string) {
                continue;
            }

            let term_string = term_string.to_string();

            non_unique_terms_raw.push((term_string, query_type_term));
        } else {
            if !index.stop_words.is_empty() && index.stop_words.contains(*term_string) {
                continue;
            }

            let term_string_0 = term_string.to_string();

            let mut term_positions_len;
            let term_hash_0 = hash64(term_string_0.as_bytes());
            let term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);

            let term_object = unique_terms
                .entry(term_string_0.clone())
                .or_insert_with(|| {
                    let term_bytes = term_string_0.as_bytes();
                    TermObject {
                        term: term_string_0.clone(),

                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes),

                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::SingleTerm,

                        ..Default::default()
                    }
                });

            term_object.field_positions_vec[indexed_field_id].push(position as u16);
            term_positions_len = term_object.field_positions_vec[indexed_field_id].len();

            if !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramFF as u8 != 0
                    && term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [term_string_1.as_str(), term_string_0.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFF,
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 1);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramRF as u8 != 0
                    && !term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [term_string_1.as_str(), term_string_0.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramRF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramRF,
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 1);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramFR as u8 != 0
                    && term_frequent_1
                    && !term_frequent_0)
            {
                let term_string = [term_string_1.as_str(), term_string_0.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFR as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFR,
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });

                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 1);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_2.is_empty()
                && !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramFFF as u8 != 0
                    && term_frequent_2
                    && term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [
                    term_string_2.as_str(),
                    term_string_1.as_str(),
                    term_string_0.as_str(),
                ]
                .join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFFF,
                        term_ngram_2: term_string_2.clone(),
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 2);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_2.is_empty()
                && !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramRFF as u8 != 0
                    && !term_frequent_2
                    && term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [
                    term_string_2.as_str(),
                    term_string_1.as_str(),
                    term_string_0.as_str(),
                ]
                .join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramRFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramRFF,
                        term_ngram_2: term_string_2.clone(),
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 2);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_2.is_empty()
                && !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramFFR as u8 != 0
                    && term_frequent_2
                    && term_frequent_1
                    && !term_frequent_0)
            {
                let term_string = [
                    term_string_2.as_str(),
                    term_string_1.as_str(),
                    term_string_0.as_str(),
                ]
                .join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFFR as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFFR,
                        term_ngram_2: term_string_2.clone(),
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 2);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            if !term_string_2.is_empty()
                && !term_string_1.is_empty()
                && (ngram_indexing & NgramSet::NgramFRF as u8 != 0
                    && term_frequent_2
                    && !term_frequent_1
                    && term_frequent_0)
            {
                let term_string = [
                    term_string_2.as_str(),
                    term_string_1.as_str(),
                    term_string_0.as_str(),
                ]
                .join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFRF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFRF,
                        term_ngram_2: term_string_2,
                        term_ngram_1: term_string_1.clone(),
                        term_ngram_0: term_string_0.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16 - 2);
                term_positions_len = term_object.field_positions_vec[indexed_field_id].len();
            }

            term_string_2 = term_string_1;
            term_string_1 = term_string_0;

            term_frequent_2 = term_frequent_1;
            term_frequent_1 = term_frequent_0;

            position += 1;

            if position >= token_per_field_max_capped {
                break;
            }
            if term_positions_len >= position_per_term_max {
                continue;
            }
        };
    }

    if is_query {
        let len = non_unique_terms_raw.len();

        let mut term_0;
        let mut term_frequent_0;
        let mut term_phrase_0;

        if len > 0 {
            let item = &non_unique_terms_raw[0];
            term_0 = item.0.clone();
            let term_hash_0 = hash64(term_0.as_bytes());
            term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
            term_phrase_0 = item.1 == QueryType::Phrase;
        } else {
            term_0 = "".to_string();
            term_frequent_0 = false;
            term_phrase_0 = false;
        }

        let mut term_1;
        let mut term_frequent_1;
        let mut term_phrase_1;
        if len > 1 {
            let item = &non_unique_terms_raw[1];
            term_1 = item.0.clone();
            let term_hash_1 = hash64(term_1.as_bytes());
            term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
            term_phrase_1 = item.1 == QueryType::Phrase;
        } else {
            term_1 = "".to_string();
            term_frequent_1 = false;
            term_phrase_1 = false;
        }

        let len = non_unique_terms_raw.len();
        let mut i = 0;
        while i < len {
            let term_2;
            let term_frequent_2;
            let term_phrase_2;
            if len > i + 2 {
                let item = &non_unique_terms_raw[i + 2];
                term_2 = item.0.clone();
                let term_hash_2 = hash64(term_2.as_bytes());
                term_frequent_2 = index.frequent_hashset.contains(&term_hash_2);
                term_phrase_2 = item.1 == QueryType::Phrase;
            } else {
                term_2 = "".to_string();
                term_frequent_2 = false;
                term_phrase_2 = false;
            }
            if i + 2 < len
                && (ngram_indexing & NgramSet::NgramFFF as u8 != 0
                    && term_frequent_0
                    && term_frequent_1
                    && term_frequent_2
                    && term_phrase_0
                    && term_phrase_1
                    && term_phrase_2)
            {
                let term_string = [term_0.as_str(), term_1.as_str(), term_2.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFFF,
                        term_ngram_2: term_0.clone(),
                        term_ngram_1: term_1.clone(),
                        term_ngram_0: term_2.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFFF,
                    op: QueryType::Phrase,
                    term_ngram_2: term_0.clone(),
                    term_ngram_1: term_1.clone(),
                    term_ngram_0: term_2.clone(),
                });

                i += 3;

                if len > i {
                    let item = &non_unique_terms_raw[i];
                    term_0 = item.0.clone();
                    let term_hash_0 = hash64(term_0.as_bytes());
                    term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
                    term_phrase_0 = item.1 == QueryType::Phrase;
                } else {
                    term_0 = "".to_string();
                    term_frequent_0 = false;
                    term_phrase_0 = false;
                }

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 2 < len
                && (ngram_indexing & NgramSet::NgramRFF as u8 != 0
                    && !term_frequent_0
                    && term_frequent_1
                    && term_frequent_2
                    && term_phrase_0
                    && term_phrase_1
                    && term_phrase_2)
            {
                let term_string = [term_0.as_str(), term_1.as_str(), term_2.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramRFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramRFF,
                        term_ngram_2: term_0.clone(),
                        term_ngram_1: term_1.clone(),
                        term_ngram_0: term_2.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramRFF,
                    op: QueryType::Phrase,
                    term_ngram_2: term_0.clone(),
                    term_ngram_1: term_1.clone(),
                    term_ngram_0: term_2.clone(),
                });

                i += 3;

                if len > i {
                    let item = &non_unique_terms_raw[i];
                    term_0 = item.0.clone();
                    let term_hash_0 = hash64(term_0.as_bytes());
                    term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
                    term_phrase_0 = item.1 == QueryType::Phrase;
                } else {
                    term_0 = "".to_string();
                    term_frequent_0 = false;
                    term_phrase_0 = false;
                }

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 2 < len
                && (ngram_indexing & NgramSet::NgramFFR as u8 != 0
                    && term_frequent_0
                    && term_frequent_1
                    && !term_frequent_2
                    && term_phrase_0
                    && term_phrase_1
                    && term_phrase_2)
            {
                let term_string = [term_0.as_str(), term_1.as_str(), term_2.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFFR as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFFR,
                        term_ngram_2: term_0.clone(),
                        term_ngram_1: term_1.clone(),
                        term_ngram_0: term_2.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFFR,
                    op: QueryType::Phrase,
                    term_ngram_2: term_0.clone(),
                    term_ngram_1: term_1.clone(),
                    term_ngram_0: term_2.clone(),
                });

                i += 3;

                if len > i {
                    let item = &non_unique_terms_raw[i];
                    term_0 = item.0.clone();
                    let term_hash_0 = hash64(term_0.as_bytes());
                    term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
                    term_phrase_0 = item.1 == QueryType::Phrase;
                } else {
                    term_0 = "".to_string();
                    term_frequent_0 = false;
                    term_phrase_0 = false;
                }

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 2 < len
                && (ngram_indexing & NgramSet::NgramFRF as u8 != 0
                    && term_frequent_0
                    && !term_frequent_1
                    && term_frequent_2
                    && term_phrase_0
                    && term_phrase_1
                    && term_phrase_2)
            {
                let term_string = [term_0.as_str(), term_1.as_str(), term_2.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFRF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFRF,
                        term_ngram_2: term_0.clone(),
                        term_ngram_1: term_1.clone(),
                        term_ngram_0: term_2.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFRF,
                    op: QueryType::Phrase,
                    term_ngram_2: term_0.clone(),
                    term_ngram_1: term_1.clone(),
                    term_ngram_0: term_2.clone(),
                });

                i += 3;

                if len > i {
                    let item = &non_unique_terms_raw[i];
                    term_0 = item.0.clone();
                    let term_hash_0 = hash64(term_0.as_bytes());
                    term_frequent_0 = index.frequent_hashset.contains(&term_hash_0);
                    term_phrase_0 = item.1 == QueryType::Phrase;
                } else {
                    term_0 = "".to_string();
                    term_frequent_0 = false;
                    term_phrase_0 = false;
                }

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 1 < len
                && (ngram_indexing & NgramSet::NgramFF as u8 != 0
                    && term_frequent_0
                    && term_frequent_1
                    && term_phrase_0
                    && term_phrase_1)
            {
                let term_string = [term_0.as_str(), term_1.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFF,
                        term_ngram_1: term_0.clone(),
                        term_ngram_0: term_1.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFF,
                    op: QueryType::Phrase,
                    term_ngram_1: term_0.clone(),
                    term_ngram_0: term_1.clone(),

                    ..Default::default()
                });

                i += 2;

                term_0 = term_2.clone();
                term_frequent_0 = term_frequent_2;

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 1 < len
                && (ngram_indexing & NgramSet::NgramRF as u8 != 0
                    && !term_frequent_0
                    && term_frequent_1
                    && term_phrase_0
                    && term_phrase_1)
            {
                let term_string = [term_0.as_str(), term_1.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramRF as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramRF,
                        term_ngram_1: term_0.clone(),
                        term_ngram_0: term_1.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramRF,
                    op: QueryType::Phrase,
                    term_ngram_1: term_0.clone(),
                    term_ngram_0: term_1.clone(),

                    ..Default::default()
                });

                i += 2;

                term_0 = term_2.clone();
                term_frequent_0 = term_frequent_2;

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else if i + 1 < len
                && (ngram_indexing & NgramSet::NgramFR as u8 != 0
                    && term_frequent_0
                    && !term_frequent_1
                    && term_phrase_0
                    && term_phrase_1)
            {
                let term_string = [term_0.as_str(), term_1.as_str()].join(" ");

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.clone(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes) | NgramType::NgramFR as u64,
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::NgramFR,
                        term_ngram_1: term_0.clone(),
                        term_ngram_0: term_1.clone(),

                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::NgramFR,
                    op: QueryType::Phrase,
                    term_ngram_1: term_0.clone(),
                    term_ngram_0: term_1.clone(),

                    ..Default::default()
                });

                i += 2;

                term_0 = term_2.clone();
                term_frequent_0 = term_frequent_2;

                if len > i + 1 {
                    let item = &non_unique_terms_raw[i + 1];
                    term_1 = item.0.clone();
                    let term_hash_1 = hash64(term_1.as_bytes());
                    term_frequent_1 = index.frequent_hashset.contains(&term_hash_1);
                    term_phrase_1 = item.1 == QueryType::Phrase;
                } else {
                    term_1 = "".to_string();
                    term_frequent_1 = false;
                    term_phrase_1 = false;
                }
            } else {
                let term_string = term_0.clone();

                let term_object = unique_terms.entry(term_string.clone()).or_insert_with(|| {
                    let term_bytes = term_string.as_bytes();
                    TermObject {
                        term: term_string.to_string(),
                        key0: hash32(term_bytes) & segment_number_mask1,
                        key_hash: hash64(term_bytes),
                        field_positions_vec: vec![Vec::new(); indexed_field_number],
                        ngram_type: NgramType::SingleTerm,
                        ..Default::default()
                    }
                });
                term_object.field_positions_vec[indexed_field_id].push(position as u16);

                non_unique_terms.push(NonUniqueTermObject {
                    term: term_string,
                    ngram_type: NgramType::SingleTerm,
                    op: non_unique_terms_raw[i].1.clone(),
                    ..Default::default()
                });

                i += 1;

                term_0.clone_from(&term_1);
                term_1.clone_from(&term_2);

                term_frequent_0 = term_frequent_1;
                term_frequent_1 = term_frequent_2;

                term_phrase_0 = term_phrase_1;
                term_phrase_1 = term_phrase_2;
            };

            position += 1;
        }
    }

    *nonunique_terms_count = position;
}

#[cfg(all(test, feature = "zh"))]
mod tests {
    use super::*;
    use crate::index::TokenizerType;
    use crate::{
        index::{
            Clustering, FrequentwordType,
            IndexMetaObject, LexicalSimilarity, StopwordType,
        },
        search::{ResultType, SearchMode},
        vector::Inference,
    };
    use serde_json::json;
    use std::{collections::BTreeSet, fs};

    #[tokio::test]
    async fn zh_tail_preserves_terms_positions_and_phrase_matches() {
        let path = std::env::temp_dir().join(format!("anyseek-zh-tail-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        let meta = IndexMetaObject {
            name: "zh-tail".into(),
            lexical_similarity: LexicalSimilarity::Bm25fProximity,
            tokenizer: TokenizerType::JiebaMixedV1,
        text_normalization: crate::index::TextNormalization::None,
        field_length_normalization: crate::index::FieldLengthNormalization::Shared,
            stop_words: StopwordType::None,
            frequent_words: FrequentwordType::English,
            ngram_indexing: 9,
            clustering: Clustering::None,
            inference: Inference::None,
        };
        let schema = serde_json::from_value(json!([
            {"field":"body", "field_type":"Text", "store":true, "index_lexical":true}
        ]))
        .unwrap();
        let (stop_words,_,frequent_hashset)=crate::index::analysis_dictionaries(&meta);
        let dictionary=standard::StandardAnalyzer::new(TokenizerType::JiebaMixedV1).unwrap();
        let context=AnalysisContext { text_normalization:meta.text_normalization,stop_words:&stop_words,
            frequent_hashset:&frequent_hashset,standard:&dictionary };
        let config=crate::record::RecordConfig {index:meta,schema,history:Default::default(),embedding_space_id:None,
            max_records:32,max_record_bytes:4096,max_operations:100,max_journal_bytes:1024*1024,max_candidates:100};
        let index=crate::record::RecordPartition::create(&path,config).await.unwrap();
        {
            for (text, expected) in [
                ("", vec![]),
                ("中国", vec!["中国"]),
                ("rust", vec!["rust"]),
                ("中国 rust", vec!["中国", "rust"]),
                ("rust 中国", vec!["rust", "中国"]),
                ("江苏省会是哪里", vec!["江苏省", "会", "是", "哪里"]),
            ] {
                for suffix in ["", "。", " "] {
                    for is_query in [false, true] {
                        let input = format!("{text}{suffix}");
                        let mut terms = AHashMap::new();
                        let mut query_terms = Vec::new();
                        let mut count = 0;
                        analyze(
                            &context,
                            &input,
                            &mut terms,
                            &mut query_terms,
                            0,
                            &mut count,
                            65_536,
                            65_536,
                            is_query,
                            &mut QueryType::Union,
                            9,
                            0,
                            1,
                        )
                        .await;
                        assert_eq!(
                            count as usize,
                            expected.len(),
                            "{input:?}, query={is_query}"
                        );
                        assert_eq!(terms.len(), expected.len(), "{input:?}, query={is_query}");
                        for (position, term) in expected.iter().enumerate() {
                            assert_eq!(
                                terms[*term].field_positions_vec,
                                vec![vec![position as u16]]
                            );
                        }
                        if is_query {
                            assert_eq!(
                                query_terms
                                    .iter()
                                    .map(|t| t.term.as_str())
                                    .collect::<Vec<_>>(),
                                expected
                            );
                        }
                    }
                }
            }
        }
        for (id,body) in ["中国 rust","中国 rust。","中国 rust search","中国 search rust","rust 中国"].into_iter().enumerate() {
            let request=serde_json::from_value(json!({"operation_id":crate::record::OperationId::new(format!("zh-{id}")),
                "record_id":id.to_string(),"expected":"Absent","document":{"body":body}})).unwrap();
            index.write(request).await.unwrap();
        }
        index.compact_journal().await.unwrap();drop(index);
        let index=crate::record::RecordPartition::open(&path,1024*1024).await.unwrap();
        for query in ["中国 rust", "中国 rust。", "中国 rust "] {
            let result=index.query(crate::record::RecordQuery {temporal:None,text:query.into(),vector:None,
                query_type:QueryType::Phrase,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,
                fields:vec![],filters:vec![],sort:vec![]}).await.unwrap();
            assert_eq!(result.count_total, 3, "{query}");
            assert_eq!(
                result
                    .hits
                    .iter()
                    .map(|r| r.record.record_id.parse::<usize>().unwrap())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([0, 1, 2])
            );
            let score = |id: usize| {
                result
                    .hits
                    .iter()
                    .find(|r| r.record.record_id == id.to_string())
                    .unwrap()
                    .score
            };
            assert_eq!(
                score(0),
                score(1),
                "punctuation must not change term frequency or document length"
            );
        }
        drop(index);
        fs::remove_dir_all(path).unwrap();
    }
}
