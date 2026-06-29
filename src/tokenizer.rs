//! Tokenizer module for English and Chinese text.
//!
//! Provides `tokenize_index` for indexing mode and `tokenize_query` for query parsing.
//!
//! TODO: integrate `snowball_stemmers_rs` for real stemming.
//! TODO: integrate `jieba-rs` for proper Chinese word segmentation (currently uses
//! character-based fallback).

use ahash::{AHashMap, AHashSet, AHasher};
use std::hash::Hasher;

use crate::posting::{NgramType, NonUniqueTermObject, QueryTypeTerm, TermObject};
use crate::schema::{IndexConfig, TokenizerType};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Check if a character belongs to a CJK Unicode block.
fn is_cjk(ch: char) -> bool {
    (ch >= '\u{4E00}' && ch <= '\u{9FFF}')
        || (ch >= '\u{3400}' && ch <= '\u{4DBF}')
        || (ch >= '\u{F900}' && ch <= '\u{FAFF}')
}

/// Common English stopword set.
fn is_stopword(word: &str) -> bool {
    matches!(
        word,
        "the" | "a" | "an" | "is" | "are" | "was" | "were" | "be" | "been" | "being"
            | "have" | "has" | "had" | "do" | "does" | "did" | "will" | "would" | "could"
            | "should" | "may" | "might" | "can" | "shall" | "to" | "of" | "in" | "for"
            | "on" | "with" | "at" | "by" | "from" | "as" | "into" | "through" | "during"
            | "before" | "after" | "above" | "below" | "between" | "out" | "off" | "over"
            | "under" | "again" | "further" | "then" | "once" | "and" | "but" | "or" | "nor"
            | "not" | "so" | "yet" | "both" | "either" | "neither" | "each" | "every" | "all"
            | "any" | "few" | "more" | "most" | "other" | "some" | "such" | "no" | "only"
            | "own" | "same" | "than" | "too" | "very" | "it" | "its" | "this" | "that"
            | "these" | "those" | "i" | "me" | "my" | "we" | "our" | "you" | "your"
            | "he" | "him" | "his" | "she" | "her" | "they" | "them" | "their" | "what"
            | "which" | "who" | "whom" | "if"
    )
}

/// Apply stemming to a term.
///
/// TODO: integrate `snowball_stemmers_rs` when available. Currently an identity
/// function — returns the input unchanged.
fn stem_term(term: &str, _language: &str) -> String {
    term.to_string()
}

/// Extract tokens from text according to the given `TokenizerType`.
/// All tokens are lowercased.
fn extract_tokens(text: &str, tokenizer_type: TokenizerType) -> Vec<String> {
    let lower = text.to_lowercase();

    match tokenizer_type {
        TokenizerType::AsciiAlphabetic => {
            let mut tokens = Vec::new();
            let mut cur = String::new();
            for ch in lower.chars() {
                if ch >= 'a' && ch <= 'z' {
                    cur.push(ch);
                } else if !cur.is_empty() {
                    tokens.push(std::mem::take(&mut cur));
                }
            }
            if !cur.is_empty() {
                tokens.push(cur);
            }
            tokens
        }

        TokenizerType::UnicodeAlphanumeric | TokenizerType::UnicodeAlphanumericFolded => {
            let mut tokens = Vec::new();
            let mut cur = String::new();
            for ch in lower.chars() {
                if ch.is_alphanumeric() {
                    cur.push(ch);
                } else if !cur.is_empty() {
                    tokens.push(std::mem::take(&mut cur));
                }
            }
            if !cur.is_empty() {
                tokens.push(cur);
            }
            tokens
        }

        // Chinese character-based segmentation: each CJK character becomes its own
        // term; non-CJK runs are handled like UnicodeAlphanumeric.
        TokenizerType::UnicodeAlphanumericZH => {
            let mut tokens = Vec::new();
            let mut cur = String::new();
            for ch in lower.chars() {
                if is_cjk(ch) {
                    if !cur.is_empty() {
                        tokens.push(std::mem::take(&mut cur));
                    }
                    tokens.push(ch.to_string());
                } else if ch.is_alphanumeric() {
                    cur.push(ch);
                } else if !cur.is_empty() {
                    tokens.push(std::mem::take(&mut cur));
                }
            }
            if !cur.is_empty() {
                tokens.push(cur);
            }
            tokens
        }
    }
}

/// Compute key_hash and key0 from a term string.
fn compute_key(term: &str, segment_mask: u32) -> (u64, u32) {
    let mut hasher = AHasher::default();
    hasher.write(term.as_bytes());
    let key_hash = hasher.finish();
    let key0 = (key_hash as u32) & segment_mask;
    (key_hash, key0)
}

/// Build a `TermObject` from a processed term string.
fn make_term(
    term: &str,
    segment_mask: u32,
    ngram_type: NgramType,
    position: u16,
) -> TermObject {
    let (key_hash, key0) = compute_key(term, segment_mask);
    let mut field_positions_vec = Vec::new();
    field_positions_vec.push(vec![position]);
    TermObject {
        term: term.to_string(),
        key_hash,
        key0,
        field_positions_vec,
        ngram_type,
    }
}

/// Filter stopwords and apply stemming. Returns (processed_term, position) pairs.
fn process_tokens(tokens: &[String], config: &IndexConfig) -> Vec<(String, u16)> {
    let mut result = Vec::new();
    let mut position: u16 = 0;
    for token in tokens {
        if token.is_empty() {
            continue;
        }
        if is_stopword(token) {
            continue;
        }
        let stemmed = match &config.stemmer_language {
            Some(lang) => stem_term(token, lang),
            None => token.clone(),
        };
        result.push((stemmed, position));
        position += 1;
    }
    result
}

// ---------------------------------------------------------------------------
// Indexing
// ---------------------------------------------------------------------------

/// Tokenize text for indexing.
///
/// Returns a deduplicated `Vec<TermObject>` with term positions.
pub fn tokenize_index(
    text: &str,
    _field_id: u16,
    config: &IndexConfig,
    segment_mask: u32,
) -> Vec<TermObject> {
    let tokens = extract_tokens(text, config.tokenizer_type);
    let processed = process_tokens(&tokens, config);

    let mut seen: AHashSet<String> = AHashSet::new();
    let mut result = Vec::new();
    for (term, pos) in processed {
        if seen.insert(term.clone()) {
            result.push(make_term(&term, segment_mask, NgramType::None, pos));
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Query parsing
// ---------------------------------------------------------------------------

/// Parse a raw query string into `(text, query_type)` segments.
///
/// Recognised operators:
/// - `+term` → Intersection
/// - `-term` → Not
/// - `"phrase text"` → each word gets Phrase type
/// - bare term → `default_query_type`
fn parse_query_segments(
    query: &str,
    default_query_type: QueryTypeTerm,
) -> Vec<(String, QueryTypeTerm)> {
    let mut segments = Vec::new();
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // skip whitespace
        while i < len && chars[i].is_whitespace() {
            i += 1;
        }
        if i >= len {
            break;
        }

        // operator prefix
        let mut prefix_type: Option<QueryTypeTerm> = None;
        if chars[i] == '+' {
            i += 1;
            prefix_type = Some(QueryTypeTerm::Intersection);
        } else if chars[i] == '-' {
            i += 1;
            prefix_type = Some(QueryTypeTerm::Not);
        }

        // quoted phrase (takes precedence over prefix)
        if i < len && chars[i] == '"' {
            i += 1; // skip opening quote
            let mut phrase = String::new();
            while i < len && chars[i] != '"' {
                phrase.push(chars[i]);
                i += 1;
            }
            if i < len {
                i += 1; // skip closing quote
            }
            segments.push((phrase, QueryTypeTerm::Phrase));
            continue;
        }

        // bare term
        let qt = prefix_type.unwrap_or(default_query_type);
        let mut term_text = String::new();
        while i < len && !chars[i].is_whitespace() {
            term_text.push(chars[i]);
            i += 1;
        }
        if !term_text.is_empty() {
            segments.push((term_text, qt));
        }
    }

    segments
}

/// Tokenize a search query.
///
/// Returns:
/// - a unique-term map (`AHashMap<String, TermObject>`)
/// - an ordered list of `NonUniqueTermObject` preserving query structure
pub fn tokenize_query(
    query: &str,
    config: &IndexConfig,
    segment_mask: u32,
    default_query_type: QueryTypeTerm,
) -> (AHashMap<String, TermObject>, Vec<NonUniqueTermObject>) {
    let segments = parse_query_segments(query, default_query_type);

    let mut unique_map: AHashMap<String, TermObject> = AHashMap::new();
    let mut ordered: Vec<NonUniqueTermObject> = Vec::new();

    for (text, qt) in segments {
        let tokens = extract_tokens(&text, config.tokenizer_type);
        let processed = process_tokens(&tokens, config);

        for (term, _pos) in processed {
            let (key_hash, key0) = compute_key(&term, segment_mask);
            let term_obj = make_term(&term, segment_mask, NgramType::None, 0);
            unique_map.entry(term.clone()).or_insert(term_obj);

            ordered.push(NonUniqueTermObject {
                term,
                query_type_term: qt,
                key_hash,
                key0,
            });
        }
    }

    (unique_map, ordered)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> IndexConfig {
        IndexConfig::default()
    }

    #[test]
    fn test_is_cjk() {
        assert!(is_cjk('搜'));
        assert!(is_cjk('中'));
        assert!(is_cjk('文'));
        assert!(!is_cjk('a'));
        assert!(!is_cjk('1'));
        assert!(!is_cjk(' '));
    }

    #[test]
    fn test_is_stopword() {
        assert!(is_stopword("the"));
        assert!(is_stopword("a"));
        assert!(is_stopword("is"));
        assert!(is_stopword("if"));
        assert!(!is_stopword("hello"));
        assert!(!is_stopword("world"));
        assert!(!is_stopword("rust"));
    }

    #[test]
    fn test_english_tokenization() {
        let config = default_config();
        let terms = tokenize_index("Hello World", 0, &config, 0xF);
        let mut t: Vec<String> = terms.iter().map(|t| t.term.clone()).collect();
        t.sort();
        assert_eq!(t, vec!["hello", "world"]);
    }

    #[test]
    fn test_stopword_filtering() {
        let config = default_config();
        let terms = tokenize_index("the quick brown fox", 0, &config, 0xF);
        let mut t: Vec<String> = terms.iter().map(|t| t.term.clone()).collect();
        t.sort();
        assert_eq!(t, vec!["brown", "fox", "quick"]);
    }

    #[test]
    fn test_term_deduplication() {
        let config = default_config();
        let terms = tokenize_index("hello world hello", 0, &config, 0xF);
        assert_eq!(terms.len(), 2);
    }

    #[test]
    fn test_chinese_segmentation() {
        let mut config = default_config();
        config.tokenizer_type = TokenizerType::UnicodeAlphanumericZH;
        let terms = tokenize_index("搜索引擎", 0, &config, 0xF);
        let mut t: Vec<String> = terms.iter().map(|t| t.term.clone()).collect();
        t.sort();
        assert_eq!(t, vec!["引", "搜", "擎", "索"]);
    }

    #[test]
    fn test_mixed_chinese_english() {
        let mut config = default_config();
        config.tokenizer_type = TokenizerType::UnicodeAlphanumericZH;
        let terms = tokenize_index("搜索引擎search", 0, &config, 0xF);
        let mut t: Vec<String> = terms.iter().map(|t| t.term.clone()).collect();
        t.sort();
        assert_eq!(t, vec!["search", "引", "搜", "擎", "索"]);
    }

    #[test]
    fn test_ascii_alphabetic() {
        let mut config = default_config();
        config.tokenizer_type = TokenizerType::AsciiAlphabetic;
        let terms = tokenize_index("Hello123 World!", 0, &config, 0xF);
        let mut t: Vec<String> = terms.iter().map(|t| t.term.clone()).collect();
        t.sort();
        assert_eq!(t, vec!["hello", "world"]);
    }

    #[test]
    fn test_hash_computation() {
        let config = default_config();
        let terms = tokenize_index("hello", 0, &config, 0xF);
        assert_eq!(terms.len(), 1);
        assert!(terms[0].key_hash > 0);
        assert!(terms[0].key0 <= 0xF);
    }

    #[test]
    fn test_query_operator_parsing() {
        let config = default_config();
        let (_unique, ordered) =
            tokenize_query("+rust -slow \"exact match\"", &config, 0xF, QueryTypeTerm::Union);
        assert_eq!(ordered.len(), 4);

        assert_eq!(ordered[0].term, "rust");
        assert_eq!(ordered[0].query_type_term, QueryTypeTerm::Intersection);

        assert_eq!(ordered[1].term, "slow");
        assert_eq!(ordered[1].query_type_term, QueryTypeTerm::Not);

        assert_eq!(ordered[2].term, "exact");
        assert_eq!(ordered[2].query_type_term, QueryTypeTerm::Phrase);

        assert_eq!(ordered[3].term, "match");
        assert_eq!(ordered[3].query_type_term, QueryTypeTerm::Phrase);
    }

    #[test]
    fn test_query_unique_map() {
        let config = default_config();
        let (unique, _ordered) =
            tokenize_query("hello world hello", &config, 0xF, QueryTypeTerm::Union);
        assert_eq!(unique.len(), 2);
        assert!(unique.contains_key("hello"));
        assert!(unique.contains_key("world"));
    }

    #[test]
    fn test_query_default_type() {
        let config = default_config();
        let (_unique, ordered) =
            tokenize_query("hello world", &config, 0xF, QueryTypeTerm::Intersection);
        assert_eq!(ordered.len(), 2);
        assert_eq!(ordered[0].query_type_term, QueryTypeTerm::Intersection);
        assert_eq!(ordered[1].query_type_term, QueryTypeTerm::Intersection);
    }

    #[test]
    fn test_stemming_noop() {
        // TODO: replace with real stemming test once snowball_stemmers_rs is added.
        let mut config = default_config();
        config.stemmer_language = Some("English".to_string());
        let terms = tokenize_index("running", 0, &config, 0xF);
        assert_eq!(terms.len(), 1);
        // Without a real stemmer this is an identity function.
        assert_eq!(terms[0].term, "running");
    }

    #[test]
    fn test_empty_input() {
        let config = default_config();
        assert!(tokenize_index("", 0, &config, 0xF).is_empty());
        let (unique, ordered) = tokenize_query("", &config, 0xF, QueryTypeTerm::Union);
        assert!(unique.is_empty());
        assert!(ordered.is_empty());
    }

    #[test]
    fn test_all_stopwords() {
        let config = default_config();
        assert!(tokenize_index("the a is", 0, &config, 0xF).is_empty());
        let (unique, ordered) =
            tokenize_query("the a is", &config, 0xF, QueryTypeTerm::Union);
        assert!(unique.is_empty());
        assert!(ordered.is_empty());
    }
}
