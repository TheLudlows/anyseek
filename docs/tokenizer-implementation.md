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
  Compatibility probe passed: one Tantivy 0.27.0 and one tokenizer-api 0.7.0; jieba-rs is pinned exactly to 0.11.0. Quality remains unverified.
- Jieba uses accurate mode, ordinal positions and HMM disabled. No overlapping search-mode token graph is introduced into phrase/word-N-gram scoring.
- Existing explicitly selected English profiles retain their punctuation/folding contracts; the new English path uses SimpleTokenizer + LowerCaser. They share the existing application query/scoring adapter, not a second engine.
- Reuse the component's existing process-shared Jieba dictionary and decompressed default-dictionary blob, with one process-lifetime 80MiB shared-build reservation acquired before use. Analyzer-local working state remains admitted separately; do not duplicate the old per-collection dictionary charge. No collection lease is retained by this process cache and no budget limit is raised.

## Acceptance

- [x] Default-feature dependency/API probe passed; no duplicate engine/API.
- [ ] No-default and complete feature regression.
- [ ] Index/query use the same basic analysis; mixed Chinese/English, punctuation,
  long tokens, OpenCC, phrases, offsets/positions, N-grams and fields are covered.
- [ ] New defaults and deliberate segmentation changes documented; no AND→OR
  change, no silent term truncation, no altered ranking formula or active corpus.
- [ ] Immediate visibility, reopen/recovery, authenticated configuration identity,
  package pins and budget ownership remain covered.
- [x] Removed unused WordSegmentationTM and its exclusive 349,045-line dictionary after the native Record Chinese write/reopen/phrase regression passed.
- [ ] Reuse the fixed 40 query/qrels diagnostic with original questions and fixed
  keyword queries reported separately. Disclose corpus coverage and AND/OR versus
  Meili all/last. Use equal smaller corpora if master budgets block 1000 documents;
  do not merge/increase budgets or rank empty-result QPS.
- [ ] Run applicable full regressions and document exact evidence and limits.

Tokenizer work remains local-only. Separate budget PR #83 is authorized and contains none of this work. No tokenizer push/PR, merge or issue edit.
