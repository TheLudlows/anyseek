# Single-collection capacity boundary probes

This staged repair measures finite capacity refusals; it is not yet a 100k
benchmark or a Meilisearch ranking. See [baseline](BASELINE.md),
[first repair](REPAIR-01.md), [both repairs](REPAIR-02.md), [ownership audit](../../docs/capacity-build-admission.md)
and [remaining design](../../docs/capacity-100k-plan.md).

Inputs: the repository's pinned `miracl-corpus-v1.0-zh/manifest.json` and all ten
shards; official MIRACL Chinese dev topics/qrels from
[revision 5be20db](https://huggingface.co/datasets/miracl/miracl/tree/5be20db9509754dadad47689368639fcec739c00).
Source topic SHA256: `5b284a9aabf08bb2d1c88ed7ea276025c9d23846457c5350dc8d391b5e0d0a13`;
qrels SHA256: `5546474d3dc8139014e6571e9f4041848bb0a314ca6d2d4bac708174d71c59eb`.
Frozen `queries-393.json` SHA256:
`ebb564fd7b99fb02cdad3d9e9809bd62f71ed4237edcb10cc473c979b42a6447`.
It contains all 393 original dev questions/3,928 judgments: 40 previously used
questions are explicitly nonblind; 353 remain separate. No keyword rewriting.

The sample first includes all 3,786 judged document IDs (966 distinct positives),
then deterministic document-ID hash distractors, to 10,001 documents. It is
relevance-enriched, not a random/full-corpus recall estimate. All 994 positive
query/document pairs remain in denominators; partial-load coverage is recorded.
The HTTP harness retains raw text and uses actual default tokenizer settings,
raw question OR, title/text lexical fields, top10 and count. It does not claim
semantic equivalence to Meili all/last or an exhaustive recall measurement.

```bash
python3 benchmarks/capacity/prepare.py \
  --corpus miracl-corpus-v1.0-zh \
  --out artifacts/benchmark/capacity/data
cargo build --locked -p anyseek --release --features server --bin anyseek_server
python3 benchmarks/capacity/baseline.py \
  --revision "$(git rev-parse HEAD)" \
  --binary target/release/anyseek_server \
  --data artifacts/benchmark/capacity/data/corpus-10001.json \
  --out artifacts/benchmark/capacity/new-run
```

Use a new output directory for each run; preserve old DBs/results. The harness
requires Linux CPUs 0–3: server 0–1, client 2–3. No simultaneous build/measurement,
no ANYSEEK budget overrides, no system cache dropping. It stops on the first batch
failure, probes queries, checkpoints, restarts and checks original successful and
failed operation IDs. A batch HTTP 200 is not per-item write success. Startup
polling has a 60s diagnostic limit and requests have 35s socket timeout; these are
not changes to production timeouts or proofs of the service's 15s recovery target.

The only embedded credentials are explicitly synthetic localhost test values.
Raw service logs/DBs/binaries/corpus bodies are not committed; logs may contain
those local test credentials. No deployment data is read, modified or removed.
