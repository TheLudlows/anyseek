# Final-build asynchronous comparison

Run `async_compare.py` only after the final production implementation is fixed and
its checks pass. Record the actual release binary hash and source revision. Do not
run a previous anyseek revision. Raw data, databases, logs and downloaded binaries
belong in a newly created directory outside the repository; preserve existing runs.

The bounded experiment uses the same first 2,305 records of the verified
`prepare.py` 10,001-document MIRACL sample and all 393 frozen query strings for both
services. It is a relevance-enriched small corpus, not a full-corpus quality or
capacity result. Both services run sequentially on CPUs 0–1, the client on CPUs
2–3, under the same existing cgroup memory/CPU limits. No compilation or other
benchmark may overlap measurement. Keep at least 4 GiB of free disk space.
There are 145 batches (144 of 16, one of 1). This exceeds sixteen 128-update
mutable-tail boundaries and is intended to exercise automatic derived-cache
merge. Tail/checkpoint policy can change, so document count alone is not evidence:
require observed intake compaction and cache-merge capture/build/install/cleanup
phase events before claiming maintenance coverage. Missing events are a validation
gap, not a zero-cost maintenance result. This remains a small finite workload.

Submit identical documents in batches of 16. For anyseek, measure HTTP 202
acceptance separately from waiting for terminal-prefix completion and both index
watermarks. Check every operation has a successful business receipt, then query
representative titles and verify visible document count. For Meilisearch, likewise
measure HTTP 202 acceptance separately from successful task completion and check
visible count and representative searches. A 202 response is never counted as
business completion or search visibility. Polling granularity differs (5 ms for
anyseek and 10 ms for Meilisearch) and is included in completion latency.

Report batch acceptance/completion p50, p95 and p99, total load wall time, summed
batch completion time, observed peak RSS, query latency and empty-result count,
checkpoint duration where available, reopened count, and database disk size.
Sequential-batch document rates are not saturated throughput. Per-operation
receipt validation is excluded from individual completion intervals but included
in total load wall time. Peak RSS uses 100 ms sampling and can miss short peaks.

Query fields are title/text, explicit JiebaMixedV1 and AND for anyseek, and
Meilisearch all with typo tolerance/prefix disabled. Token boundaries and scoring
remain different. Query latency is not a quality ranking, especially when many
queries return empty results. The script retains all raw result IDs locally.
Meilisearch has no equivalent per-index checkpoint in this harness; its successful
task and process-reopen checks are reported separately.

The approved local download is Meilisearch Community v1.56.0 from the project's
[official release](https://github.com/meilisearch/meilisearch/releases/tag/v1.56.0),
asset `meilisearch-linux-amd64`, 357,300,520 bytes, SHA256
`5a32e1d7a6a69e0ee5634a7456c421f59273d73f078b6f0d8f01384eafe79729`.
The experiment allocates at most 2 GiB for downloaded binary, sample, logs and
new databases, independent of preserved compilation/test binaries. The first
2,305 sample records encode to 1,277,744 JSON bytes before request envelopes,
with a largest record of 2,986 bytes. The 341 MiB Meili binary and 4.3 MiB frozen
sample leave more than 1.5 GiB for the new run within the 2 GiB planning budget.
The harness checks allocated run-directory size between batches and stops above
1.5 GiB, preserving data; this is a guard, not a hard quota against an individual
batch's temporary peak. Previously retained test executables and logs are outside
this incremental performance budget and must not be deleted.

Maintenance instrumentation must be run separately on the same final source.
`ANYSEEK_TEST_PHASE_TIMINGS` records named intervals; only explicitly locked
intervals may be described as lock measurements, and nested intervals must not be
summed. A checkpoint HTTP duration is not a Record mutex hold time. Simulated S3,
real cloud S3 and physical power-loss evidence remain distinct.

```bash
python3 benchmarks/capacity/async_compare.py \
  --anyseek /path/to/final/anyseek_server \
  --meili /path/to/meilisearch-v1.56.0 \
  --data /path/to/data/corpus-10001.json \
  --out /path/outside/repository/new-run \
  --revision FINAL_REVISION \
  --expected-anyseek-sha256 FINAL_BUILD_SHA256 \
  --take 2305
```

Only explicit HTTP 429 `code=busy` is retried, with the identical request and
operation IDs, a 50 ms backoff and 180 s deadline. Every attempt is retained.
Transport failures, unknown outcomes and other rejection codes stop the run.
Acceptance wall latency and batch completion latency include Busy attempts and
backoff; successful HTTP-only latency is reported separately. Checkpoint wall
time likewise includes all Busy waits, with attempts/rejections counted. Automatic
background merge remains enabled throughout measurement.

The harness verifies the final-build owner's expected anyseek binary SHA256 before
starting a service and rejects raw-output paths inside the repository. Successful
completion alone does not prove maintenance coverage: inspect
`required_maintenance_observed` and each `maintenance_coverage` count in the anyseek
summary. Server info logs supply `intake_compaction_locked`,
`cache_merge_capture_locked`, `cache_merge_install_locked`,
`cache_merge_cleanup_locked` and `cache_merge_build_unlocked`; the last interval is
explicitly outside the Record lock. Debug test phase timings are supplementary
regression evidence and are not release benchmark measurements.
