# Versioned text normalization

New indexes can explicitly select `text_normalization: "OpenCcT2s1_1_9"`.
Omitting the setting selects `"None"`, preserving existing behavior. Existing
metadata without the field also means None. Product default activation and
performance budgets remain undecided in #35; this implementation does not change
them. No per-query override, update endpoint, or index migration is provided.

The rule is persisted in index/shard metadata and used by the same tokenizer
entry point for document and query text, before word segmentation and n-grams.
Stored documents are not changed. There is one lexical term stream, not parallel
traditional/simplified fields or a second stored simplified document. Custom
stop/frequent words use the same conversion in their runtime dictionaries;
the supplied metadata remains unchanged. Reopening reconstructs the same rule.

The Rust adapter implements only the pinned OpenCC 1.1.9 t2s rule: longest phrase
matching, followed by the first character mapping. It is a compatibility
implementation, not an official Rust binding or the complete OpenCC API.
Official `TSPhrases` and `TSCharacters` data and Apache-2.0 license are embedded
from commit `556ed22496d650bd0b13b6c163be9814637970ae`. Source URLs, SHA-256
hashes and version are in `seekpower/data/opencc-1.1.9/provenance.json`.
No network access, external converter process, runtime data download or native
library installation is required. An immutable process-wide LazyLock dictionary
is initialized once and reused across indexes and queries.

This is t2s, not Taiwanese/Hong Kong/mainland vocabulary rewriting: e.g.
`軟體` becomes `软体`, not `软件`. ASCII bytes (including query quotes, plus and
minus operators) are neither consumed nor introduced by the pinned mappings.
NOT posting-rank and leading CJK query-operator repairs are inherited from
the independent stage A commits (#37; Chinese behavior was noted in #28),
not implemented by normalization. Independent NOT acceptance checks are
retained separately from ordinary-query equivalence checks.

One deliberate compatibility boundary is recorded in #38: embedded NUL and all
following text are preserved. OpenCC 1.1.9's C-string path truncates at NUL. The
adapter does not silently discard that suffix. Original stored UTF-8 text is
preserved in both cases. No highlight, prefix or edge-ngram feature is added.

HTTP creation uses the existing schema, for example:

```json
{
  "index_name": "normalized-zh",
  "tokenizer": "UnicodeAlphanumericZH",
  "text_normalization": "OpenCcT2s1_1_9",
  "schema": [{"field":"body","field_type":"Text","index_lexical":true,"store":true}]
}
```

Index metadata responses expose the selected rule. Send original text both when
indexing and querying; do not add client-side regional conversion. Existing Rust
`IndexMetaObject` literals must specify `text_normalization`, normally
`TextNormalization::None` or `TextNormalization::OpenCcT2s1_1_9`.
