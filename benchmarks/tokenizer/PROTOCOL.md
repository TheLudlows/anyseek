# Bounded tokenizer quality diagnostic

Use the immutable MIRACL corpus/query/qrels artifacts from report PR #82, retaining
their original hashes and all raw HTTP responses. Do not overwrite that report.
The fixed `keywords.json` was written from the 40 question texts before evaluating
the new tokenizer; no result or relevance document was used to choose keywords.
Original spelling (including 新加披) is retained. Original MIRACL qrels are gold for
the questions, but only a proxy for manually shortened keyword queries, not new
human judgments.

First check the 1000-document ingestion against unchanged master budgets. If
refused, retain exact status, accepted count and operation outcomes, and use one
fresh 256-document collection per product. The reduced pool must contain all 91
known positive documents for these 40 queries, plus deterministic distractors
from the original 1000. Record IDs/content hashes, selection seed and exact query
coverage. This relevance-enriched closed pool is not full-corpus performance or
quality evidence, and must not be extrapolated to 1000/5000/100000.

- anyseek: explicit JiebaMixedV1, normalization None; question and fixed-keyword
  inputs, each with explicit AND and OR, fields title/text, top 10.
- Meilisearch: the already verified fixed Community v1.55.0 binary, same documents
  and searchable fields, typo tolerance and prefix search disabled; inputs with
  `matchingStrategy=all` and `last` separately. `last` is NOT global OR, and even
  `all`/AND do not guarantee identical token boundaries.
- Report empty-query-result rates, returned counts, Recall@10, binary nDCG@10 and
  MRR@10 by product/config/input group, preserving all 40 per-query responses.
  Never infer a quality winner from empty-result QPS; this run is a functional
  quality diagnostic, not a new performance-tuning project.
- Services run separately; no concurrent compilation during measurements. No
  model, capacity increase, budget-branch merge or multi-collection workaround.

Execution completed locally; see [results](RESULTS.md). The additional old-default
1,000-document rerun and old/new Chinese 256-document paired run are separately
labeled; neither overwrites or substitutes for the successful 1,000-document run.


The current and Meili comparison used all 1,000 documents. For direct comparison
with the retired Chinese algorithm, a separately named 256-document paired run
kept all 91 positives and used the predeclared fallback seed for distractors.
Both legacy/current Chinese profiles used the same new isolated roots and data;
these numbers are never substituted into the 1,000-document result table.

For the verified pre-change default, run `compare_previous.py --data ...
--binary /path/to/verified-old-anyseek --out /new/path`. Adding `--new-binary
/path/to/current-anyseek` runs the paired Chinese diagnostic instead. The script
requires the recorded old release binary hash and refuses inherited ANYSEEK
configuration overrides. [RESULTS.json](RESULTS.json) retains the numeric summaries;
full request/response and source hashes remain in the separately named raw directories.
