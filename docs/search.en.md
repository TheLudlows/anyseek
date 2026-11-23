# Search configuration and ranking

**English** · [简体中文](search.md)

See the [service guide](../anyseek/src/server/README.en.md#queries-and-iteration) for HTTP queries. This page defines text analysis, scoring, candidate ordering, and fusion.

## Text normalization

When creating a collection, select `text_normalization: "OpenCcT2s1_1_9"` under `config`. The default is `"None"`, also used for stored metadata missing the field. Query-level overrides, online changes, and legacy index migrations are unavailable. The following is a configuration excerpt.

```json
{
  "index_name": "normalized-zh",
  "tokenizer": "JiebaMixedV1",
  "text_normalization": "OpenCcT2s1_1_9",
  "schema": [{"field":"body","field_type":"Text","index_lexical":true,"store":true}]
}
```

The rule is stored in index metadata. Documents and queries use the same tokenizer entry point. Conversion happens before tokenization and N-grams and produces one token stream. It neither stores a duplicate simplified document nor mutates the original. Runtime custom stop-word and frequent-word dictionaries use the same conversion. User metadata stays intact. Reopening restores the same rule. Clients send the original text without regional vocabulary conversion. Metadata responses report the selected rule. The internal Rust setting is `IndexMetaObject::text_normalization`.

The Rust adapter implements only the pinned OpenCC 1.1.9 t2s rule. It prefers the longest phrase, then the first character mapping. It is a subset of the OpenCC API. The upstream TSPhrases, TSCharacters, and Apache-2.0 license are pinned to commit `556ed22496d650bd0b13b6c163be9814637970ae`. See [provenance.json](../anyseek/data/opencc-1.1.9/provenance.json) for the origin and SHA-256. The immutable process-wide LazyLock dictionary initializes once. Runtime requires no data downloads, converter process, or native library installation.

Normalization converts traditional to simplified characters without rewriting Taiwan, Hong Kong, or mainland regional vocabulary. `軟體` becomes `软体` and never `软件`. Mappings neither consume nor introduce ASCII bytes, including query quotes and plus/minus operators. Embedded NUL and trailing text are preserved, unlike the NUL truncation in OpenCC 1.1.9's C-string path. Original UTF-8 text is always kept. Query NOT and leading CJK operators have independent parsing and regressions outside normalization. The option adds no highlighting, prefix, or edge-ngram feature.

## N-gram indexing

An N-gram indexes two or three adjacent terms for lexical phrase searches. It does not perform character N-grams, spelling correction, or autocomplete.

Internal settings are `frequent_words` and `ngram_indexing`. HTTP does not accept them. Frequent-word sets are English, German, French, Spanish, Custom, or None. F denotes a frequent term and R denotes another term. New HTTP collections use English with FF | FFF (value 9). Valid combinations require the corresponding frequent-word set.

| Enum | Value | Combination |
| --- | --- | --- |
| SingleTerm | 0 | No additional N-grams |
| NgramFF | 1 | Frequent + frequent |
| NgramFR | 2 | Frequent + other |
| NgramRF | 4 | Other + frequent |
| NgramFFF | 8 | Three frequent words |
| NgramRFF | 16 | Other + frequent + frequent |
| NgramFFR | 32 | Frequent + frequent + other |
| NgramFRF | 64 | Frequent + other + frequent |

Combine options with bitwise OR. Single-term indexing remains enabled. FFR and FRF check their own flags independently of RFF. 5 means FF | RF, and the default 9 means FF | FFF. More combinations cost more space and build work. Measure with real data.

Ordinary phrase search works with N-grams disabled. Enabling them does not make `rust` match `r`, `ru`, `us`, or `r*`. Existing tests cover independent bits, default and combined flags, phrase membership and counts, and BM25F/Proximity before and after commit and reopening. The correction for historically missing FFR/FRF postings does not backfill old indexes. Rebuild affected data from original records. The default 9 enables neither bit.

See [NgramSet and FrequentwordType](../anyseek/src/index.rs) and [CreateIndexRequest](../anyseek/src/server/api_types.rs) for definitions, and [tokenizer.rs](../anyseek/src/tokenizer.rs) and [search.rs](../anyseek/src/search.rs) for execution.

## Field length normalization

`field_length_normalization` accepts `"Shared"` or `"PerField"`. Shared is the default, and the setting is immutable after creation. The internal Rust field is `IndexMetaObject::field_length_normalization`.

Production Tantivy scoring retains BM25F/Bm25fProximity, field boosts, k1/b, and decoded length quantization. Shared uses a common average length. PerField changes each field's denominator to the average of that field's nonempty decoded lengths. An empty population uses 1. Term DF counts the active Record population at the same read cut across fields, tails, and frozen components, excluding retired records.

TTL excludes expired candidates before Top-K. Their index rows can still affect corpus statistics until background reclamation. Metadata filtering restricts candidates without redefining the statistical population. Synchronous and asynchronous visibility follow the [Record contract](record-model.en.md). Checkpoints and native commits do not create a second business visibility switch.

The independent formula oracle in `anyseek/tests/common/ngram_oracle.rs` and `anyseek/tests/lexical_correctness.rs` covers boosts, unequal DF, Shared/PerField, and persistence states. It does not depend on the removed legacy posting encoding.

## Stable ordering

For fixed logical data, scoring settings, and candidates, relevance sorts by descending numeric score and then ascending stable `record_id` string. Candidate admission and final ordering use the same rule. A physical component ordinal is an address. Segmentation and merging must not redefine tie order by insertion order. An ordinary `docid` field cannot substitute for Record identity.

Explicit sorting respects declared field precedence and directions. For ties on `_score`, compare subsequent fields. When all explicit keys tie, fall back to descending score and ascending stable ID. `_id` sorts by stable identity. Empty queries default to ascending ID, as does explicit `_score` when all scores are zero. See [field types and numeric values](../anyseek/src/server/README.en.md#field-types-and-numeric-values) for null, absent, and StringSet sorting rules.

Signed zero values compare equal and infinities retain numeric ordering. The internal comparator puts NaNs after numeric values and ties them with each other. Nonfinite external vectors are still rejected.

The pure full-text collector uses a bounded heap of size `offset + length`. The Hybrid lexical lane uses its own candidate budget. Candidate selection and output honor the same request order. Pure full-text Count skips scoring and candidate collection. See `record/lexical/runtime_query.rs`, `min_heap.rs`, and the vector candidate path. Existing stable_ranking, weighted_rrf, collector, and comparator regressions cover ties, explicit ordering, pagination, numeric boundaries, and component layout.

ANN and bounded fusion can omit candidates even with stable tie ordering. Sorting fields within Vector/Hybrid candidates does not produce full-corpus field Top-K. Concurrent writes or TTL can shorten a page at final revalidation. Deterministic ordering does not provide a durable pagination snapshot across requests.

## Weighted RRF

Hybrid applies weighted RRF once after building a global ranking across components for each lane. Without `rrf` (internal `RrfOptions::default()`), the zero-based rank constant is 0.6, both weights are 1, and each candidate window defaults to `offset + length`. These defaults have not been retuned and do not guarantee quality or latency.

HTTP accepts `rrf` only when a nonempty textual query also supplies query_vector.

```json
{
  "query": "example",
  "query_vector": [1, 0, 0],
  "length": 10,
  "rrf": {
    "constant": 5,
    "keyword_weight": 0.5,
    "vector_weight": 1,
    "keyword_candidates": 100,
    "vector_candidates": 200
  }
}
```

Vector dimensions must match the collection. The example illustrates format and is not a measured recommendation. Rust validates default and custom settings through the same `SearchMode::Hybrid { similarity_threshold, ann_mode, rrf }`.

A hit's fused score is the sum of `weight / (constant + rank)` across participating lanes, with rank **starting at 0**. Each lane orders by descending raw score and ascending stable Record ID. An absent lane contributes zero, and final ties also sort by ascending stable ID. The zero-based constant=5 is equivalent to one-based constant=4. To compare to the common one-based constant=60, use 61 here. The UI displays ranks starting at 1 without changing the computation.

| Parameter | Constraints |
| --- | --- |
| constant | Finite and positive |
| keyword_weight / vector_weight | Finite and nonnegative, with at least one positive lane. Combinations that can overflow the maximum fused f32 score are rejected. |
| keyword_candidates / vector_candidates | Explicit values are positive integers. Omission or null uses each lane's global `offset + length`. |
| Other numeric fields and the rrf object | Reject null and unknown RRF fields |

A zero-weight lane contributes no hits or score, but both lanes still run. It does not reduce query work or relax vector validation. Diagnostic scores and source labels report the actual contributing lanes.

Each lane's budget bounds the **globally retained candidates before fusion**. Local component prefixes can join the global merge, but the number of components does not multiply the final budget. The budget does not bound scanned documents, elapsed time, or vector probes and is independent of the output window and `nprobe`/`all`. A budget smaller than the output window may yield fewer results. More vector candidates cannot compensate for too few cluster probes. More components and larger budgets still raise intermediate Top-K, merge memory, and sorting costs.

An explicit fixed budget holds candidate window size across pages. If omitted, the window changes with `offset + length`. Vector/Hybrid over HTTP requires a positive output length. Hybrid count_total remains the sum of each component's maximum count across the two lanes. It is neither an exact union nor a count of fusion candidates. RecordSearch has no general completeness flag. Explicit field ordering preserves lexical candidate selection semantics, while fusion ranks selected candidates by relevance.

Neither ANN nor bounded windows guarantee full-corpus hybrid recall. The application handles graph traversal and relationship fusion. Tune with fixed corpus, vectors, query/qrel splits, and original rankings, and measure resource use on an independent holdout. Previously seen dev queries do not establish blind quality. Cite exact report revisions for performance and regression results. [Evidence index](roadmap.md#验收证据)
