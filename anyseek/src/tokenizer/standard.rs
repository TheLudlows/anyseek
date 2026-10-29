//! Standard token streams; application operators and word N-grams stay outside.
use crate::index::TokenizerType;
use std::sync::Mutex;
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};

pub(crate) fn identity(kind: TokenizerType) -> &'static str {
    match kind {
        #[cfg(feature = "zh")]
        TokenizerType::JiebaMixedV1 => {
            "jieba-mixed-v1;tantivy-jieba=0.21.0;jieba-rs=0.11.0;dict-sha256=139519822fe8ab9e10d9d07e68ea0451045380aedaf54ecc51e2a28c6b42a13f;accurate;hmm=false;lowercase;ordinal;no-stopwords;no-length-filter"
        }
        TokenizerType::TantivySimpleV1 => {
            "tantivy-simple-v1;tantivy=0.27.0;lowercase;no-stopwords;no-length-filter"
        }
    }
}

/// The dependency keeps both its shared Jieba and decompressed dictionary for process life.
/// Its global charge must likewise outlive every individual analyzer; no request
/// or collection lease is retained by this process-owned cache.
#[cfg(feature = "zh")]
fn admit_dictionary_cache() -> std::io::Result<()> {
    static BLOB: Mutex<Option<crate::storage::build_budget::MemoryLease>> = Mutex::new(None);
    let mut blob = BLOB
        .lock()
        .map_err(|_| std::io::Error::other("dictionary accounting poisoned"))?;
    if blob.is_none() {
        *blob = Some(crate::storage::build_budget::reserve_memory(
            80 * 1024 * 1024,
        )?);
    }
    Ok(())
}

/// The adapter uses the dependency's one shared dictionary, never per-query copies.
/// The lock covers synchronous
/// token collection only, never an await, I/O or publication.
pub(crate) struct StandardAnalyzer(Mutex<TextAnalyzer>);
impl StandardAnalyzer {
    pub(crate) fn new(kind: TokenizerType) -> std::io::Result<Self> {
        let analyzer = match kind {
            #[cfg(feature = "zh")]
            TokenizerType::JiebaMixedV1 => {
                admit_dictionary_cache()?;
                let mut tokenizer = tantivy_jieba::JiebaTokenizer::new();
                tokenizer.set_search_mode(false);
                tokenizer.set_ordinal_position_mode(true);
                tokenizer.set_hmm(false);
                TextAnalyzer::builder(tokenizer).filter(LowerCaser).build()
            }
            TokenizerType::TantivySimpleV1 => TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .build(),
        };
        Ok(Self(Mutex::new(analyzer)))
    }

    fn words(&self, text: &str) -> Vec<String> {
        let mut analyzer = self.0.lock().expect("analysis lock poisoned");
        let mut stream = analyzer.token_stream(text);
        let mut result = Vec::new();
        while stream.advance() {
            let token = stream.token();
            // Jieba emits punctuation/whitespace. These are separators, not
            // stopwords. The application assigns compact word positions.
            if token.text.chars().any(char::is_alphanumeric) {
                result.push(token.text.clone());
            }
        }
        result
    }

    pub(crate) fn terms(&self, text: &str, query: bool) -> Vec<String> {
        if !query {
            return self.words(text);
        }
        let mut result: Vec<String> = Vec::new();
        let mut in_phrase = false;
        let mut pending_open = false;
        // Split only at syntax boundaries, not every punctuation character:
        // Jieba can retain ordinary compounds such as AT&T as a single token.
        for span in query_spans(text) {
            let (sign, body) = if span.starts_with(['+', '-']) {
                (&span[..1], &span[1..])
            } else {
                ("", span)
            };
            if body == "\"" {
                if in_phrase {
                    if let Some(last) = result.last_mut() {
                        last.push('"');
                    }
                    in_phrase = false;
                    pending_open = false;
                } else {
                    in_phrase = true;
                    pending_open = true;
                }
                continue;
            }
            let opening_here = body.starts_with('"');
            let opening = opening_here || pending_open;
            let closing = body.ends_with('"') && body.len() > usize::from(opening_here);
            let body = body.strip_prefix('"').unwrap_or(body);
            let body = if closing {
                body.strip_suffix('"').unwrap_or(body)
            } else {
                body
            };
            let mut words = self.words(body);
            if words.is_empty() && !sign.is_empty() {
                // A standalone + changes the existing query mode even though
                // it contributes no term; keep that operator in the adapter.
                result.push(sign.into());
            }
            if let Some(first) = words.first_mut() {
                *first = format!("{sign}{}{first}", if opening { "\"" } else { "" });
                pending_open = false;
            }
            if opening {
                in_phrase = true;
            }
            if closing {
                if let Some(last) = words.last_mut() {
                    last.push('"');
                }
                in_phrase = false;
            }
            result.extend(words);
        }
        result
    }
}

/// Preserve legacy operator placement without changing the analyzer's ordinary
/// word boundaries. Punctuation is retained unless it borders actual syntax.
fn query_spans(text: &str) -> Vec<&str> {
    let separator = |c: char| {
        !regex_syntax::is_word_character(c) && !matches!(c, '"' | '+' | '-' | '#')
    };
    let mut spans = Vec::new();
    let mut start = 0;
    let mut previous = None;
    let mut chars = text.char_indices().peekable();
    while let Some((offset, c)) = chars.next() {
        let end = offset + c.len_utf8();
        if c.is_whitespace() {
            if start < offset { spans.push(&text[start..offset]); }
            start = end;
        } else {
            if matches!(c, '+' | '-' | '"') && previous.is_some_and(separator) && start < offset {
                spans.push(&text[start..offset]);
                start = offset;
            }
            if c == '"' && chars.peek().is_some_and(|(_, next)| separator(*next)) {
                if start < end { spans.push(&text[start..end]); }
                start = end;
            }
        }
        previous = Some(c);
    }
    if start < text.len() { spans.push(&text[start..]); }
    spans
}
