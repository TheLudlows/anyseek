# seekpower-tokenizer

Pluggable tokenization for both indexing and query-time text analysis. Defines
the `Tokenizer` trait, a small set of built-in implementations, a process-wide
registry, and the index/query pipeline functions that turn raw text into
`TermObject`s ready to be written to / looked up in the inverted index.

Full context lives in `docs/superpowers/specs/2026-06-30-hybrid-search-design.md`
(section 7 covers tokenization; section 3.1 explains why `key_hash` mixes
`field_id` into the term hash).

## Public surface at a glance

| Item | Kind | Purpose |
|---|---|---|
| `Token` | struct | One token with text, position, byte offsets |
| `TermObject` | struct | Index-ready term: term text, `key_hash`, `field_id`, positions |
| `Tokenizer` | trait | `tokenize` + `name`; `Send + Sync` |
| `SimpleTokenizer` | struct | ASCII alphanum runs |
| `UnicodeTokenizer` | struct | Unicode alphanum runs (handles CJK) |
| `JiebaTokenizer` | struct | Chinese via `jieba-rs` (feature `jieba`) |
| `TokenizerRegistry` | struct | Name-keyed collection + global singleton |
| `tokenize_index` | fn | Pipeline that keeps duplicates and applies filters |
| `tokenize_query` | fn | Pipeline that dedupes by `key_hash` and merges positions |

## `Tokenizer` trait

```rust
pub trait Tokenizer: Send + Sync {
    fn tokenize(&self, text: &str) -> Vec<Token>;
    fn name(&self) -> &str;
}
```

`Send + Sync` is required so tokenizers can be shared across threads via the
global registry. Implementations are typically zero-sized unit structs.

## `Token`

```rust
pub struct Token {
    pub text: String,
    pub position: u32,
    pub start_offset: usize, // byte offset in the input
    pub end_offset: usize,   // byte offset in the input (exclusive)
}
```

Offsets are byte offsets, not character offsets, so they line up directly with
`str::char_indices` / `str::find` and with the span stored alongside the
token in posting lists.

## `TermObject`

```rust
pub struct TermObject {
    pub term: String,
    pub key_hash: u64,
    pub field_id: u16,
    pub positions: Vec<u16>,
}
```

This is the unit that gets written into the inverted index: a term, the
hash that picks the posting list, the originating field, and the position(s)
in the document.

## Built-in tokenizers

| Type | Name | What it does |
|---|---|---|
| `SimpleTokenizer` | `"simple"` | Contiguous runs of ASCII `[A-Za-z0-9]` |
| `UnicodeTokenizer` | `"unicode"` | Contiguous runs of Unicode alphanumerics (handles CJK) |
| `JiebaTokenizer` | `"jieba"` | `jieba_rs::Jieba::cut_for_search` for fine-grained Chinese segmentation |

`SimpleTokenizer` and `UnicodeTokenizer` perform **no lowercasing** -- the
pipeline functions own that. `JiebaTokenizer` is compiled in only when the
`jieba` feature is enabled.

## `TokenizerRegistry`

Name -> `Box<dyn Tokenizer>` map.

```rust
pub fn new() -> Self
pub fn register(&mut self, name: &str, tokenizer: Box<dyn Tokenizer>)
pub fn get(&self, name: &str) -> Option<&dyn Tokenizer>
```

`register` is infallible: a second registration under the same name replaces
the previous entry.

### Global singleton

```rust
pub fn global() -> &'static RwLock<TokenizerRegistry>
```

Lazily populated on first access with every built-in that is compiled in:

- `simple` -> `SimpleTokenizer`
- `unicode` -> `UnicodeTokenizer`
- `jieba` -> `JiebaTokenizer` (only if the `jieba` feature is on)

The handle is a `RwLock` so custom tokenizers can be added at startup.

## Pipeline functions

Both pipelines look up the tokenizer by name (translating the
`seekpower_schema::TokenizerType` enum into a string), tokenize, then apply
filters from `seekpower_schema::IndexConfig`.

```rust
pub fn tokenize_index(
    text: &str,
    field_id: u16,
    config: &seekpower_schema::IndexConfig,
    registry: &TokenizerRegistry,
) -> Vec<TermObject>

pub fn tokenize_query(
    query: &str,
    config: &seekpower_schema::IndexConfig,
    registry: &TokenizerRegistry,
) -> Vec<TermObject>
```

Shared steps (controlled by `IndexConfig`):

1. Tokenize.
2. Lowercase if `config.lowercase` is `true`.
3. Drop tokens that appear in `config.stop_words` (when set).

Differences:

| Function | Duplicates | `key_hash` | `field_id` |
|---|---|---|---|
| `tokenize_index` | Kept (one `TermObject` per token occurrence) | `ahash(field_id ++ term)` | passed in |
| `tokenize_query` | Merged into a single `TermObject` whose `positions` lists every occurrence | `ahash(0 ++ term)` | hard-coded to `0` (queries are not field-scoped at this layer) |

If the registry does not know the requested tokenizer, both functions return
an empty `Vec`.

## `key_hash`

```text
key_hash = ahash( field_id.to_le_bytes() ++ term.as_bytes() )
```

Mixing `field_id` into the hash means the same term in two different fields
lands in two different posting lists -- exactly what BM25F needs. The hashing
helper is private; use the pipeline functions and inspect `TermObject.key_hash`
when you need it.

## Custom tokenizers

```rust
use seekpower_tokenizer::{Tokenizer, Token, TokenizerRegistry};
use std::sync::RwLock;

struct StemmedAscii;
impl Tokenizer for StemmedAscii {
    fn tokenize(&self, text: &str) -> Vec<Token> {
        // ...your logic...
        vec![]
    }
    fn name(&self) -> &str { "stemmed-ascii" }
}

let mut reg = TokenizerRegistry::global().write().unwrap();
reg.register("stemmed-ascii", Box::new(StemmedAscii));
```

Then reference it from the schema:

```rust
config.tokenizer = seekpower_schema::TokenizerType::Custom("stemmed-ascii".into());
```

## Feature flag

| Feature | Default | Effect |
|---|---|---|
| `jieba` | off | Pulls in `jieba-rs` and registers `JiebaTokenizer` as `"jieba"` |

Leave it off in environments that do not need Chinese segmentation -- the
dictionary is large.

## Verifying against source

All of the above is the public API of
`crates/seekpower-tokenizer/src/lib.rs`. When the spec and the source
disagree, the source wins.
