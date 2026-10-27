# Same-size capacity quality follow-up

Run only after the fixed `af45faf` phase24 service has finished its writes and all
uncertain outcomes have been resolved. Never substitute the earlier1k results.
No simultaneous service or compilation. Preserve phase24 data, rejected requests,
selected SHA/binary,3GiB single/6GiB shared budget and all raw responses.

Use exactly phase24's accepted document IDs and corresponding original rows from
the frozen100001-document corpus. Verify order/uniqueness and SHA256, not just a
matching count. Both searchable fields are title/text with raw spelling retained;
phase24 reports `text_normalization=None`. No extra T2S normalization, dictionary,
stopword, semantic model or relevance-informed keyword transformation is added.
393 original questions and original qrels remain frozen:40 previously used and353
held-out questions are separate. Report all judged/positive-document coverage;
Recall denominators are all known positive qrels per question, not returned hits.
IDCG uses original graded qrels. This relevance-enriched subset is not the full
4.93million-document MIRACL candidate universe.

Anyseek: original question AND and OR, length100, same title/text domain and fixed
capacity candidate limit. Meili1.55.0: strict `all` with typo disabled and prefix
`disabled`; separately report `last` under those strict settings and `last` with
its default typo/prefix settings restored and verified by GET. None of these
implies identical Chinese token boundaries; `last` is not global OR. Return
Recall@10/100, graded nDCG@10, MRR@10, empty results and errors by query group, plus
raw per-query responses and regression examples. Keep search truncation and any
Meili query-word limit explicit. Do not use these unequal result workloads for a
QPS ranking. Existing40 fixed keywords may be a separately named diagnostic, not
new gold labels for393 questions.

Same host cgroup16GiB and service CPUs0/1; client CPUs2/3. Meili uses two indexing
threads and explicit `MEILI_MAX_INDEXING_MEMORY=6442450944` to avoid host-memory
autodetection outside the cgroup. This is an indexing budget, not an RSS limit,
and is not identical to Anyseek's scoped memory ledger; disclose both. Retain the
2GiB disk safety margin. Record all asynchronous task results, accepted counts,
reopen counts and resource samples; ingestion batch sizes/durability differ and
are not a like-for-like write-throughput result. Do not delete prior databases to
make room: stop dependent execution and report insufficient safe space.

Official setting references (runtime GET remains authoritative for pinned1.55.0):
- [Prefix modes and reindexing](https://www.meilisearch.com/docs/capabilities/full_text_search/how_to/configure_prefix_search)
- [Indexing memory and thread configuration](https://www.meilisearch.com/docs/resources/self_hosting/configuration/reference)
