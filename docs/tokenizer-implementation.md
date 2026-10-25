# Tantivy standard analysis: implementation and acceptance

Base: master `6d28798a300bb947b2eb1d3e48e928d193103e17`. Independent branch
`feat/tantivy-standard-tokenizers`; no budget-branch changes or report PR edits.

## Decisions before implementation

- Default with `zh`: a newly named, versioned Jieba mixed Chinese/Latin analyzer,
  using Tantivy's `Tokenizer` / `TextAnalyzer` API. No language-detection model.
- Without `zh`: an explicitly named/versioned Tantivy Unicode simple analyzer.
  English applications can select that same path even with Chinese enabled.
- Never use Tantivy's stock `default` pipeline with its 40-byte removal filter.
  Preserve supported long tokens; retain existing explicit resource limits.
- Keep OpenCC and original payload independent. Keep query operators and AND/OR
  unchanged; no implicit stopword, prefix expansion, or QueryParser features.
- Retain only the thin application adapter for positions, word N-grams,
  field-length/statistical inputs and existing BM25F/proximity semantics.
  Tantivy character NgramTokenizer is not equivalent to our word N-grams.
- Do not reinterpret `UnicodeAlphanumericZH`: remove its selectable old identity
  when the replacement is validated. Name and persist the replacement algorithm,
  component/dictionary versions; reject incompatible packages, no migration path.
- First verify exact `tantivy-jieba=0.21.0` against Tantivy `0.27.0` and shared
  tokenizer-api `0.7`, lock resolved dependencies, and reject a second Tantivy.
  Compatibility probe passed: one Tantivy 0.27.0 and one tokenizer-api 0.7.0; jieba-rs is pinned exactly to 0.11.0. Both adapters are MIT licensed. Quality remains unverified.
- Jieba uses accurate mode, ordinal positions and HMM disabled. No overlapping search-mode token graph is introduced into phrase/word-N-gram scoring.
- Existing explicitly selected English profiles retain their punctuation/folding contracts; the new English path uses SimpleTokenizer + LowerCaser. They share the existing application query/scoring adapter, not a second engine.
- Reuse the component's existing process-shared Jieba dictionary and decompressed default-dictionary blob, with one process-lifetime 80MiB shared-build reservation acquired before use. Analyzer-local working state remains admitted separately; do not duplicate the old per-collection dictionary charge. No collection lease is retained by this process cache and no budget limit is raised.

## Deliberate behavior changes

With `zh` enabled (the normal build), omitted HTTP tokenizer configuration now
selects `JiebaMixedV1`; without `zh` it selects `TantivySimpleV1`. The latter is
also the explicit Unicode/English choice. Existing explicitly selected legacy
English profiles remain available for their punctuation/folding contracts.
SimpleTokenizer splits punctuation such as `C++`, `C#` and internal hyphens;
it is not an alias for the legacy Unicode scanner. Jieba accurate segmentation
can split or combine Chinese terms differently from WordSegmentationTM.
Neither profile removes stopwords or changes the existing query operator mode.
Unary signs still apply to the first term of their lexical span, as before.

The mature adapter emits byte offsets against its input. After OpenCC and the
existing word-N-gram expansion, native `PreTokenizedString` offsets address the
generated analysis buffer, not the original Payload. Word positions are compact;
unigram position length is one and word-N-gram lengths retain their width.
These offsets must not be exposed as original-text highlighting coordinates.
Payload originals remain unchanged and are read through Record authority.

The process-shared dictionary reservation is a conservative logical 80 MiB
charge, not measured RSS or a new allocator limit. It intentionally lives for the
process lifetime alongside the dependency cache. Per-collection working state
still has its own leases; a 20-owner test verifies local admission is released.

## Acceptance

- [x] Default-feature dependency/API probe passed; no duplicate engine/API.
- [x] No-default and complete feature regression.
- [x] Index/query use the same basic analysis; mixed Chinese/English, punctuation,
  long tokens, OpenCC, phrases, offsets/positions, N-grams and fields are covered.
- [x] New defaults and deliberate segmentation changes documented; no AND→OR
  change, no silent term truncation, no altered ranking formula or active corpus.
- [x] Immediate visibility, reopen/recovery, authenticated configuration identity,
  package pins and budget ownership remain covered.
- [x] Removed unused WordSegmentationTM and its exclusive 349,045-line dictionary after the native Record Chinese write/reopen/phrase regression passed.
- [x] Reuse the fixed 40 query/qrels diagnostic with original questions and fixed
  keyword queries reported separately. Disclose corpus coverage and AND/OR versus
  Meili all/last. Use equal smaller corpora if master budgets block 1000 documents;
  do not merge/increase budgets or rank empty-result QPS.
- [x] Run applicable full regressions and document exact evidence and limits.

Tokenizer work remains local-only. Separate budget PR #83 is authorized and contains none of this work. No tokenizer push/PR, merge or issue edit.

## Local validation

Production implementation: `253df6e6e8e10f6294af67a2158d55d12bfe2877` (not published).
`cargo test --locked -p anyseek --all-targets --features server,fault-injection
--no-fail-fast -- --test-threads=1` completed with exit 0: **345 passed, 0 failed,
13 ignored, 35 harnesses**. The 13 ignored entries are 11 subprocess workers
exercised by passing parents, dedicated real S3, and manual opposite-AES testing.
The three new production/ownership tests passed in this complete run.

No-default core: **200 passed, 0 failed, 10 subprocess workers ignored**.
Default/no-default doctests: **9 passed each**. Ordinary HTTP/restart, S3 fixture
object contract, seven cold-cache semantics groups and storage/backup/GC matrix
all completed with exit 0. The fixed 1,000-document quality diagnostic and
separate 256-document old/new Chinese comparison completed with no query errors.
See [results and limitations](../benchmarks/tokenizer/RESULTS.md). Windows, real S3,
system power loss and manual opposite-AES are not validated by this local run.

Maintenance cost: the old 191-line Chinese algorithm and exclusive 349,045-line
frequency dictionary are removed; the Chinese dictionary is now supplied by the
pinned MIT component, not eliminated. Cargo.lock adds 22 resolved packages, with
one Tantivy 0.27.0 and one tokenizer-api 0.7.0. The shared dictionary keeps its
80 MiB logical reservation for process life; no capacity or lane cap is raised.
The 256-document paired diagnostic has a small keyword-OR quality regression,
explicitly retained in the results, so this is not a universal quality-upgrade claim.

## Publication base

The publication branch is rebased onto master `702c385` (merged budget PR #83).
README conflict resolution retains both budget and tokenizer sections. The
multi-collection ownership test discovers the actual default admission boundary
instead of hardcoding the old 384 MiB value; its release assertions are unchanged.
Historical 345/200 tests and quality measurements above were run before this base
update, on `253df6e`; they are not relabeled as tests of the rebased HEAD. No
experiment was rerun with the higher master budgets. Publication CI validates the
combined current HEAD on Linux and Windows.

After rebase, the full native lexical suite passed: 37 tests, 0 failures, exit 0
(`server,fault-injection`, serial). Broader current-base checks run in publication CI.
