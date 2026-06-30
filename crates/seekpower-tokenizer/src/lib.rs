use seekpower_schema::TokenizerType;
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// A single token produced by a [`Tokenizer`].
#[derive(Debug, Clone)]
pub struct Token {
    pub text: String,
    pub position: u32,
    pub start_offset: usize,
    pub end_offset: usize,
}

/// The core tokenizer trait.
///
/// All built-in and custom tokenizers must implement this trait.  It is
/// `Send + Sync` so that tokenizers can be shared across threads via the
/// global registry.
pub trait Tokenizer: Send + Sync {
    /// Tokenize `text` into a list of [`Token`]s.
    fn tokenize(&self, text: &str) -> Vec<Token>;

    /// Human-readable name of this tokenizer (used for registry look-ups).
    fn name(&self) -> &str;
}

/// Output of the index-time tokenization pipeline.
///
/// Each `TermObject` represents one term that will be written into the
/// inverted index for a particular field.
#[derive(Debug, Clone)]
pub struct TermObject {
    pub term: String,
    pub key_hash: u64,
    pub field_id: u16,
    pub positions: Vec<u16>,
}

// ---------------------------------------------------------------------------
// SimpleTokenizer
// ---------------------------------------------------------------------------

/// Splits text on ASCII whitespace / punctuation boundaries.
///
/// Only contiguous runs of ASCII alphabetic characters (`a-z`, `A-Z`) and
/// digits (`0-9`) are kept.  The returned token text is **not** lowercased
/// by the tokenizer itself – lowercasing is applied by the pipeline
/// functions ([`tokenize_index`] / [`tokenize_query`]).
pub struct SimpleTokenizer;

impl Tokenizer for SimpleTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position: u32 = 0;
        let mut current_start: Option<usize> = None;

        for (i, c) in text.char_indices() {
            if c.is_ascii_alphanumeric() {
                if current_start.is_none() {
                    current_start = Some(i);
                }
            } else if let Some(start) = current_start {
                tokens.push(Token {
                    text: text[start..i].to_string(),
                    position,
                    start_offset: start,
                    end_offset: i,
                });
                position += 1;
                current_start = None;
            }
        }

        // Flush the last token if the string ends mid-word.
        if let Some(start) = current_start {
            tokens.push(Token {
                text: text[start..].to_string(),
                position,
                start_offset: start,
                end_offset: text.len(),
            });
        }

        tokens
    }

    fn name(&self) -> &str {
        "simple"
    }
}

// ---------------------------------------------------------------------------
// UnicodeTokenizer
// ---------------------------------------------------------------------------

/// Splits text on Unicode word boundaries.
///
/// Contiguous runs of Unicode alphanumeric characters (including CJK
/// ideographs) are kept.  Like [`SimpleTokenizer`], no lowercasing is
/// performed here – that is the pipeline's responsibility.
pub struct UnicodeTokenizer;

impl Tokenizer for UnicodeTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position: u32 = 0;
        let mut current_start: Option<usize> = None;

        for (i, c) in text.char_indices() {
            if c.is_alphanumeric() {
                if current_start.is_none() {
                    current_start = Some(i);
                }
            } else if let Some(start) = current_start {
                tokens.push(Token {
                    text: text[start..i].to_string(),
                    position,
                    start_offset: start,
                    end_offset: i,
                });
                position += 1;
                current_start = None;
            }
        }

        if let Some(start) = current_start {
            tokens.push(Token {
                text: text[start..].to_string(),
                position,
                start_offset: start,
                end_offset: text.len(),
            });
        }

        tokens
    }

    fn name(&self) -> &str {
        "unicode"
    }
}

// ---------------------------------------------------------------------------
// JiebaTokenizer (feature-gated)
// ---------------------------------------------------------------------------

/// Chinese-aware tokenizer backed by [jieba-rs](https://crates.io/crates/jieba-rs).
///
/// Uses jieba's **search mode** cutting, which produces the finest-grained
/// segmentation suitable for full-text search.
#[cfg(feature = "jieba")]
pub struct JiebaTokenizer;

#[cfg(feature = "jieba")]
impl Tokenizer for JiebaTokenizer {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        let jieba = jieba_rs::Jieba::new();
        let tokens_raw = jieba.cut_for_search(text, true);

        let mut tokens = Vec::new();
        let mut position: u32 = 0;
        let mut byte_offset: usize = 0;

        for word in tokens_raw {
            if let Some(pos) = text[byte_offset..].find(word) {
                let abs_start = byte_offset + pos;
                let abs_end = abs_start + word.len();
                tokens.push(Token {
                    text: word.to_string(),
                    position,
                    start_offset: abs_start,
                    end_offset: abs_end,
                });
                position += 1;
                byte_offset = abs_end;
            }
        }

        tokens
    }

    fn name(&self) -> &str {
        "jieba"
    }
}

// ---------------------------------------------------------------------------
// TokenizerRegistry
// ---------------------------------------------------------------------------

/// A name → tokenizer mapping that supports both built-in and custom
/// tokenizers.
pub struct TokenizerRegistry {
    tokenizers: HashMap<String, Box<dyn Tokenizer>>,
}

impl TokenizerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            tokenizers: HashMap::new(),
        }
    }

    /// Register a tokenizer under the given `name`.
    pub fn register(&mut self, name: &str, tokenizer: Box<dyn Tokenizer>) {
        self.tokenizers.insert(name.to_string(), tokenizer);
    }

    /// Look up a tokenizer by name.
    pub fn get(&self, name: &str) -> Option<&dyn Tokenizer> {
        self.tokenizers.get(name).map(|b| b.as_ref())
    }

    // -- global singleton ---------------------------------------------------

    /// Access the process-wide global registry.
    ///
    /// On first access the registry is populated with all built-in
    /// tokenizers ([`SimpleTokenizer`], [`UnicodeTokenizer`], and – when the
    /// `jieba` feature is enabled – [`JiebaTokenizer`]).
    pub fn global() -> &'static RwLock<TokenizerRegistry> {
        static INSTANCE: OnceLock<RwLock<TokenizerRegistry>> = OnceLock::new();
        INSTANCE.get_or_init(|| {
            let mut registry = TokenizerRegistry::new();
            registry.register("simple", Box::new(SimpleTokenizer));
            registry.register("unicode", Box::new(UnicodeTokenizer));
            #[cfg(feature = "jieba")]
            registry.register("jieba", Box::new(JiebaTokenizer));
            RwLock::new(registry)
        })
    }
}

impl Default for TokenizerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map a [`TokenizerType`] (from the schema crate) to the string name used
/// in the [`TokenizerRegistry`].
fn tokenizer_type_name(tt: &TokenizerType) -> String {
    match tt {
        TokenizerType::Simple => "simple".to_string(),
        TokenizerType::Unicode => "unicode".to_string(),
        TokenizerType::Jieba => "jieba".to_string(),
        TokenizerType::Custom(name) => name.clone(),
    }
}

/// Compute the per-term key hash:
///
/// ```text
/// key_hash = ahash( field_id.to_le_bytes() ++ term.as_bytes() )
/// ```
fn compute_key_hash(field_id: u16, term: &str) -> u64 {
    let mut hasher = ahash::AHasher::default();
    use std::hash::Hasher;
    hasher.write(&field_id.to_le_bytes());
    hasher.write(term.as_bytes());
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Pipeline functions
// ---------------------------------------------------------------------------

/// Tokenize `text` for **indexing**.
///
/// 1. Look up the tokenizer specified by `config.tokenizer` in `registry`.
/// 2. Tokenize the text.
/// 3. Optionally lowercase token text (when `config.lowercase` is `true`).
/// 4. Filter stop words when `config.stop_words` is `Some(...)`.
/// 5. Compute `key_hash` for every surviving token.
///
/// Returns one [`TermObject`] per surviving token (duplicates are kept –
/// use [`tokenize_query`] for deduplicated output).
pub fn tokenize_index(
    text: &str,
    field_id: u16,
    config: &seekpower_schema::IndexConfig,
    registry: &TokenizerRegistry,
) -> Vec<TermObject> {
    let tokenizer_name = tokenizer_type_name(&config.tokenizer);
    let tokenizer = match registry.get(&tokenizer_name) {
        Some(t) => t,
        None => return Vec::new(),
    };

    let raw_tokens = tokenizer.tokenize(text);

    let stop_words: Option<std::collections::HashSet<&str>> =
        config.stop_words.as_ref().map(|sw| {
            sw.iter().map(|s| s.as_str()).collect()
        });

    let mut result = Vec::new();
    for token in raw_tokens {
        let mut term = token.text;
        if config.lowercase {
            term = term.to_lowercase();
        }

        if let Some(ref sw) = stop_words {
            if sw.contains(term.as_str()) {
                continue;
            }
        }

        let key_hash = compute_key_hash(field_id, &term);
        result.push(TermObject {
            term,
            key_hash,
            field_id,
            positions: vec![token.position as u16],
        });
    }

    result
}

/// Tokenize a **query** string.
///
/// Identical to [`tokenize_index`] except that terms with the same
/// `key_hash` are deduplicated and their positions merged.
pub fn tokenize_query(
    query: &str,
    config: &seekpower_schema::IndexConfig,
    registry: &TokenizerRegistry,
) -> Vec<TermObject> {
    let tokenizer_name = tokenizer_type_name(&config.tokenizer);
    let tokenizer = match registry.get(&tokenizer_name) {
        Some(t) => t,
        None => return Vec::new(),
    };

    let raw_tokens = tokenizer.tokenize(query);

    let stop_words: Option<std::collections::HashSet<&str>> =
        config.stop_words.as_ref().map(|sw| {
            sw.iter().map(|s| s.as_str()).collect()
        });

    let mut seen: HashMap<u64, TermObject> = HashMap::new();
    let mut order: Vec<u64> = Vec::new();

    for token in raw_tokens {
        let mut term = token.text;
        if config.lowercase {
            term = term.to_lowercase();
        }

        if let Some(ref sw) = stop_words {
            if sw.contains(term.as_str()) {
                continue;
            }
        }

        // Use field_id = 0 for query-side tokenization (no specific field).
        let key_hash = compute_key_hash(0, &term);

        if let Some(existing) = seen.get_mut(&key_hash) {
            existing.positions.push(token.position as u16);
        } else {
            order.push(key_hash);
            seen.insert(
                key_hash,
                TermObject {
                    term,
                    key_hash,
                    field_id: 0,
                    positions: vec![token.position as u16],
                },
            );
        }
    }

    order.into_iter().map(|h| seen.remove(&h).unwrap()).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- SimpleTokenizer ----------------------------------------------------

    #[test]
    fn test_simple_tokenizer_basic() {
        let tokenizer = SimpleTokenizer;
        let tokens = tokenizer.tokenize("hello world");
        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[0].position, 0);
        assert_eq!(tokens[0].start_offset, 0);
        assert_eq!(tokens[0].end_offset, 5);
        assert_eq!(tokens[1].text, "world");
        assert_eq!(tokens[1].position, 1);
        assert_eq!(tokens[1].start_offset, 6);
        assert_eq!(tokens[1].end_offset, 11);
    }

    #[test]
    fn test_simple_tokenizer_mixed_separators() {
        let tokenizer = SimpleTokenizer;
        let tokens = tokenizer.tokenize("hello, world! foo-bar");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "world");
        assert_eq!(tokens[2].text, "foo");
        assert_eq!(tokens[3].text, "bar");
        assert_eq!(tokens[0].position, 0);
        assert_eq!(tokens[3].position, 3);
    }

    #[test]
    fn test_simple_tokenizer_with_numbers() {
        let tokenizer = SimpleTokenizer;
        let tokens = tokenizer.tokenize("abc 123 def456");
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0].text, "abc");
        assert_eq!(tokens[1].text, "123");
        assert_eq!(tokens[2].text, "def456");
    }

    #[test]
    fn test_simple_tokenizer_empty() {
        let tokenizer = SimpleTokenizer;
        let tokens = tokenizer.tokenize("");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_simple_tokenizer_punctuation_only() {
        let tokenizer = SimpleTokenizer;
        let tokens = tokenizer.tokenize("!@#$%");
        assert_eq!(tokens.len(), 0);
    }

    #[test]
    fn test_simple_tokenizer_name() {
        assert_eq!(SimpleTokenizer.name(), "simple");
    }

    // -- UnicodeTokenizer ---------------------------------------------------

    #[test]
    fn test_unicode_tokenizer_chinese() {
        let tokenizer = UnicodeTokenizer;
        let tokens = tokenizer.tokenize("你好世界");
        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].text, "你好世界");
        assert_eq!(tokens[0].position, 0);
    }

    #[test]
    fn test_unicode_tokenizer_mixed() {
        let tokenizer = UnicodeTokenizer;
        let tokens = tokenizer.tokenize("hello 你好 world 世界");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].text, "hello");
        assert_eq!(tokens[1].text, "你好");
        assert_eq!(tokens[2].text, "world");
        assert_eq!(tokens[3].text, "世界");
    }

    #[test]
    fn test_unicode_tokenizer_name() {
        assert_eq!(UnicodeTokenizer.name(), "unicode");
    }

    // -- JiebaTokenizer -----------------------------------------------------

    #[cfg(feature = "jieba")]
    #[test]
    fn test_jieba_tokenizer_chinese() {
        let tokenizer = JiebaTokenizer;
        let tokens = tokenizer.tokenize("中华人民共和国");
        assert!(!tokens.is_empty());
        // In search mode jieba produces multiple sub-words.
        // The exact segmentation may vary, but we should get some tokens.
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        // At minimum, we should get the full phrase or some meaningful parts.
        assert!(texts.len() >= 1);
        // Verify positions are sequential.
        for (i, token) in tokens.iter().enumerate() {
            assert_eq!(token.position, i as u32);
        }
    }

    #[cfg(feature = "jieba")]
    #[test]
    fn test_jieba_tokenizer_name() {
        assert_eq!(JiebaTokenizer.name(), "jieba");
    }

    // -- TokenizerRegistry --------------------------------------------------

    #[test]
    fn test_registry_register_and_get() {
        let mut registry = TokenizerRegistry::new();
        registry.register("simple", Box::new(SimpleTokenizer));
        assert!(registry.get("simple").is_some());
        assert_eq!(registry.get("simple").unwrap().name(), "simple");
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn test_registry_custom_tokenizer() {
        struct UpperTokenizer;
        impl Tokenizer for UpperTokenizer {
            fn tokenize(&self, text: &str) -> Vec<Token> {
                vec![Token {
                    text: text.to_uppercase(),
                    position: 0,
                    start_offset: 0,
                    end_offset: text.len(),
                }]
            }
            fn name(&self) -> &str {
                "upper"
            }
        }

        let mut registry = TokenizerRegistry::new();
        registry.register("upper", Box::new(UpperTokenizer));
        let t = registry.get("upper").unwrap();
        let tokens = t.tokenize("hello");
        assert_eq!(tokens[0].text, "HELLO");
    }

    #[test]
    fn test_global_registry_has_builtins() {
        let registry = TokenizerRegistry::global().read().unwrap();
        assert!(registry.get("simple").is_some());
        assert!(registry.get("unicode").is_some());
    }

    // -- tokenize_index -----------------------------------------------------

    #[test]
    fn test_tokenize_index_basic() {
        let mut config = seekpower_schema::IndexConfig::default();
        config.tokenizer = TokenizerType::Simple;
        config.lowercase = false;

        let registry = TokenizerRegistry::global().read().unwrap();
        let terms = tokenize_index("hello world", 1, &config, &registry);

        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].term, "hello");
        assert_eq!(terms[0].field_id, 1);
        assert_eq!(terms[0].positions, vec![0]);
        assert_eq!(terms[1].term, "world");
        assert_eq!(terms[1].field_id, 1);
        assert_eq!(terms[1].positions, vec![1]);

        // key_hash must be deterministic.
        let expected_hash = compute_key_hash(1, "hello");
        assert_eq!(terms[0].key_hash, expected_hash);
    }

    #[test]
    fn test_tokenize_index_lowercase() {
        let mut config = seekpower_schema::IndexConfig::default();
        config.tokenizer = TokenizerType::Simple;
        config.lowercase = true;

        let registry = TokenizerRegistry::global().read().unwrap();
        let terms = tokenize_index("Hello WORLD", 0, &config, &registry);

        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].term, "hello");
        assert_eq!(terms[1].term, "world");
    }

    #[test]
    fn test_tokenize_index_stop_words() {
        let mut config = seekpower_schema::IndexConfig::default();
        config.tokenizer = TokenizerType::Simple;
        config.lowercase = true;
        config.stop_words = Some(vec!["the".to_string(), "a".to_string()]);

        let registry = TokenizerRegistry::global().read().unwrap();
        let terms = tokenize_index("the quick a brown", 0, &config, &registry);

        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].term, "quick");
        assert_eq!(terms[1].term, "brown");
    }

    #[test]
    fn test_tokenize_index_unknown_tokenizer() {
        let mut config = seekpower_schema::IndexConfig::default();
        config.tokenizer = TokenizerType::Custom("nonexistent".to_string());

        let registry = TokenizerRegistry::global().read().unwrap();
        let terms = tokenize_index("hello", 0, &config, &registry);
        assert!(terms.is_empty());
    }

    // -- tokenize_query -----------------------------------------------------

    #[test]
    fn test_tokenize_query_dedup() {
        let mut config = seekpower_schema::IndexConfig::default();
        config.tokenizer = TokenizerType::Simple;
        config.lowercase = true;

        let registry = TokenizerRegistry::global().read().unwrap();
        let terms = tokenize_query("hello world hello", &config, &registry);

        // "hello" appears twice but should be deduplicated.
        assert_eq!(terms.len(), 2);
        assert_eq!(terms[0].term, "hello");
        assert_eq!(terms[0].positions, vec![0, 2]);
        assert_eq!(terms[1].term, "world");
        assert_eq!(terms[1].positions, vec![1]);
    }

    #[test]
    fn test_tokenize_query_stop_words() {
        let mut config = seekpower_schema::IndexConfig::default();
        config.tokenizer = TokenizerType::Simple;
        config.lowercase = true;
        config.stop_words = Some(vec!["the".to_string()]);

        let registry = TokenizerRegistry::global().read().unwrap();
        let terms = tokenize_query("the quick the", &config, &registry);

        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].term, "quick");
    }

    // -- key_hash -----------------------------------------------------------

    #[test]
    fn test_key_hash_deterministic() {
        let h1 = compute_key_hash(1, "hello");
        let h2 = compute_key_hash(1, "hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_key_hash_differs_by_field() {
        let h1 = compute_key_hash(0, "hello");
        let h2 = compute_key_hash(1, "hello");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_key_hash_differs_by_term() {
        let h1 = compute_key_hash(0, "hello");
        let h2 = compute_key_hash(0, "world");
        assert_ne!(h1, h2);
    }
}
