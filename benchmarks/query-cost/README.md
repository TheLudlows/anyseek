# Remaining query cost: measured, scoped changes

Baseline: master `768a18eb0ccb25d23d1f2b5bf9a09ec071afc857` (2026-11-03).
No repository `AGENTS.md` exists at that revision. Development and CI constraints
are in `docs/development.md` and `docs/ci-validation.md`.

## State verified before implementation

- #155 remains open. #178 is merged (`a047b55`): bounded lexical candidates,
  requested ordering/window, independent Hybrid channel budgets, global channel
  ranking followed by one RRF. This work does not repeat it.
- #188 is merged (`edb3ade`): pure lexical Count/default/ID/score paths release
  component guards before analysis/statistics/scoring. Hybrid and facet ordering
  still hold their guards. Fixed Searchers and runtime/file pins remain owned.
- #170 already builds query statistics once and skips scoring for pure Count.
- #179 already measures/adopts output admission before copying selected fields,
  and shares immutable query output ownership. #187 removed unrelated engine
  responsibilities; #191 reduced write planning/rotation copies; #192 cleaned
  current documentation. All are in this baseline.
- #190 is an open performance-evidence PR for #153, not another unmerged query
  implementation. This comparison is against current master, not pre-#178 code.

## Cost map and changes

| Phase | Current work | Decision |
| --- | --- | --- |
| Active identity capture | Scan live vector/metadata identities for admission, compile filters, join fixed native identities, construct query-owned sets/maps and comparator IDs | Remove the separate accepted-identity set. Keep **only accepted identities** in the address map and use it for both membership and lookup. Keep the complete selected corpus separately, so rejected rows still contribute to corpus statistics. No increase to lookup population for selective filters |
| Corpus selection | Check cut/schema/unique revisions, scan snapshots again, build per-view live ordinal sets | Measured; unchanged. A future change must preserve duplicate/missing identity rejection and fixed-reader semantics |
| BM25 statistics | Walk selected lengths; read query-term postings and deduplicate per-term DF across fields | Measured; unchanged. No per-commit global DF maintenance or score arithmetic change |
| Result statistics | `Inner::statistics` counts current records and physical versions under Inner | Reuse the existing conservative earliest TTL deadline bound. If no TTL exists or the bound is strictly after evaluation, `locations.len()` is exact. Due or stale-early bounds retain the original visibility scan. Physical-version counting still scans ordinal slots |
| Body output | Score/candidate window → capture identity/fingerprint → lock Inner → check current identity and TTL → measure projected output → copy selected fields | No unnecessary full body copy before scoring found. Measurement/serialization and the final admitted copy are separate necessary passes; this PR does not change them |

The existing query memory admission is unchanged and remains conservative. No new
cached counter, publication field, durable format, shared-tail snapshot, or full
per-query deep copy is introduced. Mapping keys remain query-owned. Lock release,
Searcher/file pins, retirement propagation, final health/version/fingerprint/TTL
checks, stable ID ties, field sorting, full counts, pagination, candidate budgets,
cross-component scores and global-rank RRF are unchanged.

## Baseline diagnostic profile

The optional `instrument.py` adds local phase timers in a **disposable checkout**.
It is not runtime telemetry and is not applied to the final production sources.
The first 2,000-future-TTL-record diagnostic (release/default features, Top-10,
10 warmups + 60 samples) produced these phase medians, in milliseconds:

| Mode | Capture (including lock acquisition) | Corpus selection | BM25 statistics | Score + collect | Revalidate + copy | Return statistics |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Lexical | 1.656 | 1.172 | 0.309 | 0.798 | 0.024 | 0.192 |
| Count | 2.065 | 1.293 | skipped | skipped | <0.001 | 0.331 |
| Facet sort | 1.736 | 1.244 | 0.313 | 0.768 | 0.026 | 0.217 |
| Hybrid | 1.410 | 1.224 | 0.321 | 0.778 | 0.023 | 0.241 |

These are diagnostic wall times with timer/logging overhead, not an end-to-end
acceptance comparison. Medians cannot be summed into a query percentile. The
capture timer includes waits; it does not separately measure lock wait. Hybrid's
preceding vector search is excluded from these lexical-phase timers.

## Lock and I/O boundaries

Hybrid still executes vector search first. Each vector task holds its component
read guard while accessing vector storage. Its lexical stage then acquires all
component guards, captures identities and fixed native snapshots, and retains
guards through analysis, statistics, lexical scoring and fusion. Facet ordering
needs guarded metadata mmap; its pure lexical path also retains those guards.
These guarded stages can delay vector/metadata consumption or tail changes that
require a component write guard. Record capture/final revalidation additionally
use Inner; a durable commit and subsequent indexing are separate measurements.

The pure lexical detached paths still release guards after capture. The shared
tail can change after capture, so simply copying a `VectorIndex` or moving a
`drop` is not an immutable read view. Extending detachment requires a separately
reviewed ownership design for needed identity/facet/vector data, preserving old
addresses and retirement. This PR does not make that dependency a commit-path
refactor. Mixed measurements below include all write/index work and are **not**
an isolated attribution of every microsecond to component lock waits.

Scoring reads native postings/lengths and vector/metadata data, not canonical
bodies. The final window alone is reconciled and projected from resident Record
state. There is no per-candidate object-store body GET in this path to combine.
Remote cold cache reads, exact-vector reranking, large bodies and transport
serialization need separate workloads before changing their order or storage.

## Reproduction and scope

`anyseek/examples/query_cost_probe.rs` is a manual executable, not a new default
test fixture. It uses disposable local roots and the public Record API. Build the
same probe source on baseline and candidate in separate worktrees, using identical
flags, and copy each executable before switching sources:

```sh
CARGO_PROFILE_RELEASE_LTO=false cargo build --offline --locked --release \
  --example query_cost_probe -j 2
# Save target/release/examples/query_cost_probe as /tmp/query-before or query-after.
python3 benchmarks/query-cost/compare.py --before /tmp/query-before \
  --after /tmp/query-after --output /tmp/query-cost-pairs --cpus 6,7
python3 benchmarks/query-cost/compare.py --before /tmp/query-before \
  --after /tmp/query-after --output /tmp/query-cost-hybrid --cpus 6,7 \
  --workloads 2000:true --mixed hybrid
# Repeat the mixed command with --mixed lexical and --mixed facet, distinct outputs.
```

Copy the probe from this PR into the baseline worktree; it is not present on old
master. Use fresh output directories. The runner refuses to overwrite run roots,
SHA-256 verifies fixture copies, checks exact result IDs/order/score bits/channel
ranks/counts, and retains raw samples plus executable hashes. Each workload uses
ABBA (two processes/version), 10 warmups then 60 measurements per query mode.
Statistics calls are also timed separately. Results exclude fixture construction
and reopen from request timing; whole-process CPU/wall fields include reopen and
all modes, so they must not be presented as request CPU.

The fixture uses short two-token ASCII bodies, a small integer facet, three-dimensional
F32 exact vectors, a 10-hit window, and 32 candidates/channel for Hybrid. Future
TTL is fixed at 4,000,000,000,000 ms. Initial writes use batches of 32, check every
receipt, and wait for both consumers; Busy retries retain operation identities.
Mixed runs use one continuous reader and 30 sequential durable updates, waiting
for both consumers after each update. The publication sample includes commit,
polling (1 ms interval), and consumer work. It is not a fine-grained lock timer.

An attempted 8,000-all-TTL fixture was rejected after 2,048 live TTL records by
the existing 8,192-byte-per-TTL expiration reserve and 16 MiB frame bound. It is
excluded from performance data. The valid workloads are 1,000/2,000 future-TTL
records and 8,000 records without TTL; no budget was relaxed for a benchmark.

## Final paired results

Release/default zh+vb, Rust 1.99.1, `opt-level=3`, `LTO=false`, native CPU flags;
Linux container, 8-CPU quota/8 GiB memory limit, local overlay filesystem (physical
backing unconfirmed). Each measurement process uses CPUs 6,7 and two Tokio
workers; fixtures and process ownership are separate. The final sequence started
**after this task's builds and correctness tests ended**. The host is shared,
not an exclusive physical CPU reservation. Preliminary overlapping-test runs
were exploratory and are not the acceptance tables below.

Raw results: [paired](paired.json), [lexical mixed](mixed-lexical.json),
[facet mixed](mixed-facet.json), [hybrid mixed](mixed-hybrid.json);
[diagnostic phase samples](phase-profile.json). Each table cell pools 120 calls
from **two processes per version**, not 120 independent experiments. P95 uses
sorted sample index `floor(0.95*(n-1))`. Units are milliseconds; A=baseline,
B=this candidate. Percentages compare pooled medians.

| Records / TTL | Mode | P50 A → B | Change | P95 A → B |
| --- | --- | ---: | ---: | ---: |
| 1000 / future | lexical | 2.0261 → 1.6358 | -19.3% | 2.6936 → 1.8934 |
| 1000 / future | count | 1.5234 → 1.2851 | -15.6% | 2.0900 → 1.6384 |
| 1000 / future | facet | 2.0746 → 1.6590 | -20.0% | 2.9774 → 2.2663 |
| 1000 / future | hybrid | 2.1741 → 1.8184 | -16.4% | 2.8490 → 2.7609 |
| 1000 / future | vector | 0.1909 → 0.1269 | -33.5% | 0.2508 → 0.1688 |
| 1000 / future | statistics | 0.0379 → 0.0006 | -98.5% | 0.0632 → 0.0006 |
| 2000 / future | lexical | 4.4487 → 3.2865 | -26.1% | 5.6362 → 4.8455 |
| 2000 / future | count | 3.4048 → 2.6235 | -22.9% | 5.0086 → 3.4169 |
| 2000 / future | facet | 4.2742 → 3.3605 | -21.4% | 5.8871 → 4.9882 |
| 2000 / future | hybrid | 4.7082 → 3.6219 | -23.1% | 5.9278 → 4.4235 |
| 2000 / future | vector | 0.4746 → 0.2419 | -49.0% | 0.6561 → 0.4210 |
| 2000 / future | statistics | 0.0858 → 0.0010 | -98.9% | 0.1386 → 0.0010 |
| 8000 / none | lexical | 17.0940 → 15.9664 | -6.6% | 22.3735 → 23.1724 |
| 8000 / none | count | 13.9488 → 12.2795 | -12.0% | 19.1858 → 19.7462 |
| 8000 / none | facet | 18.3613 → 15.2629 | -16.9% | 24.3250 → 22.3430 |
| 8000 / none | hybrid | 19.4456 → 17.9069 | -7.9% | 27.9885 → 22.7995 |
| 8000 / none | vector | 0.8231 → 0.7901 | -4.0% | 0.9968 → 1.0002 |
| 8000 / none | statistics | 0.0036 → 0.0035 | -1.1% | 0.0046 → 0.0036 |

Future-TTL statistics improve because the existing bound proves no visibility
scan is needed, not because vector scoring changed. The no-TTL vector/statistics
controls do not exercise either optimization; their small differences show the
remaining measurement noise. Whole-process mean CPU A→B (includes reopen and
all query modes, not fixture creation): 1k TTL 0.634→0.535 s, 2k TTL
1.360→1.071 s, 8k no TTL 5.546→4.967 s. These are not per-request CPU or
production service-throughput measurements. No allocation/RSS improvement is
claimed.

**Adverse observations:** at 8k without TTL, Lexical P95 rises 3.6% and Count
P95 rises 2.9%; candidate process 2's Lexical median is 17.503 ms versus the two
baseline medians 16.927/17.378 ms. Facet candidate process 1 is 18.587 ms versus
baseline 18.036/18.504 ms. Do not infer uniform tail-latency improvement from the
pooled median or discard these processes.

### Continuous reads and writes

2k future TTL, one continuous reader, 30 updates per process; two processes per
version, so write columns pool 60 updates/version. Query sample populations are
variable because readers run until writes/indexing finish. All 12 mixed
processes completed both consumers after every write, with zero query Busy and
zero write retries. This is short-run progress evidence, not a long-duration
starvation or shutdown test.

| Read mode | Commit P50 A → B | Commit + both consumers P50 A → B | Publication P95 A → B | Query P50 A → B |
| --- | ---: | ---: | ---: | ---: |
| lexical | 1.238 → 1.209 | 22.954 → 25.484 | 47.785 → 46.628 | 5.035 → 4.171 |
| facet | 1.330 → 1.083 | 30.026 → 23.887 | 51.989 → 42.412 | 5.285 → 4.074 |
| hybrid | 1.076 → 1.000 | 25.812 → 27.091 | 41.616 → 45.260 | 5.234 → 4.245 |

Publication median regresses **11.0% for lexical** and **5.0% for Hybrid**;
Hybrid publication P95 rises **8.8%**. Facet improves in this run, but earlier
exploratory runs varied. Faster readers also execute more queries during the
same write sequence (lexical 281→358, facet 341→360, Hybrid 292→363 queries),
so this is not a fixed-QPS isolated writer experiment. The evidence supports
lower query work, **not** a general write-throughput/lock-wait improvement.
Hybrid/facet component-lock ownership remains an independent bottleneck.

## Correctness validation and review

- Linux default features: all **194 exact core cases** in the existing manifest
  passed. This includes update/delete reconciliation, same-version vector patch,
  TTL cutoff, early/extended/deleted/restored TTL bounds, tail rotation, native
  merge, old-reader retirement/file pins and cancellation/failure guards.
- Extended the existing final-TTL revalidation test with a future-TTL count/query
  assertion; no new named correctness fixture or CI matrix. Existing bound tests
  cover stale-early bounds and reject late/missing bounds during validation.
- **21 existing integration tests** passed across `stable_ranking`, `weighted_rrf`,
  `server_shared_filters`, `lexical_correctness`, `bitmap_delete_consistency`,
  `union_count_cross_block`, `frequent_topk_consistency`, `union_topk_fill_ranking`,
  and `union_topk_many_terms` (small scale). These preserve field sorts, filters,
  both field-length policies, full score oracles, pagination and global RRF ranks.
- Production release probe built with `cfg(test)=false`; 24 final comparison
  processes passed exact fixed-fixture signatures. The final uninstrumented
  library-test binary was checked current without recompilation. Static source
  partition, console boundary, Python syntax and diff whitespace checks passed.
- Source review checked that corpus identities remain unfiltered; map lookups
  occur only for accepted hits; failed/duplicate native selections still error;
  query-owned strings outlive all reads; the existing TTL bound is conservative;
  no pin/retirement/final revalidation or lock scope changed. Review was performed
  by the implementing agent; no independent review is claimed.

Local Windows, full-size/manual acceptance, real HTTP/S3 performance, high-
selectivity filter performance, large document/vector workloads, cold caches,
allocator peaks and long-duration capacity/starvation were not run. PR CI is
reported separately for its exact head; it does not replace those measurements.
The initial all-TTL capacity rejection and benchmark Busy handling were fixed in
the manual probe, not hidden by loosening engine admission.

Remaining work: reduce duplicate corpus selection/identity scans with exact
cut/duplicate/missing checks; measure length/DF statistics and physical ordinal
scans independently; design safe Hybrid/facet detached ownership before moving
locks; measure fixed-QPS mixed traffic and real large-payload I/O. No shared-tail,
commit-protocol or vector-algorithm refactor is included.
