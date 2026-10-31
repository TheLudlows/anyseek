# CI feedback and complete acceptance

`PR feedback` runs on pull requests, master pushes and manual dispatch. It checks
dependency/core boundaries and the lexical partition;
on code changes it builds and runs 171 exact bounded core regressions plus the
small query integration matrix on Linux and Windows, then builds the service with the same features and generates its
OpenAPI schema. The case list is `scripts/ci_core_cases.json`; missing or renamed
cases fail rather than silently matching zero tests. Integration scales are listed
below. This is fast feedback, **not full acceptance**.
Documentation-only changes keep static checks but skip the compiled core matrix.
Markdown file links and anchors need a separate check; the retired brand checker
is not part of the workflow. Console build/unit checks run for code changes, and
the Linux core lane runs the standalone browser suite against a disposable API.
Both core lanes also run local/S3-fixture deleted-generation reclamation checks.
Generic x86-64 CPU flags permit target caching within each OS; full acceptance
retains the existing native-CPU configuration and source-only cache policy.

## Run full acceptance

For complete acceptance, use the `PR feedback` (`ci.yml`) workflow entry with
the candidate ref and `full=true` (replace `YOUR_CANDIDATE_REF`):

```sh
gh workflow run ci.yml --ref YOUR_CANDIDATE_REF -F full=true
```

This explicit command runs the reusable `.github/workflows/full-acceptance.yml`
from the selected commit, as well as quick checks. Do not run it merely to
inspect configuration. **Full acceptance (manual)** also has its own Run workflow entry. Verify the run's exact head SHA before
using its result for review. A later commit requires a new explicit run.

The nine-job matrix is retained, with full-size fixtures, assertions and
timeouts. The target ownership table below records deliberate deduplication: workspace all-targets; Linux/Windows persistence,
standalone features and doctests; the four independent full-size Windows lexical
cases; browser/HTTP process recovery; and S3 fixture semantics and contracts.
These long checks no longer run automatically on every PR or documentation push.
Manual runs do not cancel one another or get cancelled by ordinary PR feedback.
No scheduled workflow is added. No branch-protection setting is changed; an
existing required old job name may need repository-owner policy review.

Before treating storage/format/recovery changes as fully validated, run this
complete matrix on the candidate SHA. A green quick matrix does not resolve an
unrun or previously failing full check. Real S3, hard power loss and manual
cross-AES acceptance remain separate from fixture/process tests.

## Why split the workflows

Measured run `40301847907` at `a08456b`:

| Job / step | Actual duration |
| --- | ---: |
| Linux Persistence total | 94m 39s |
| Its retained integration contracts | 56m 42s |
| Its no-default-feature core | 11m 45s |
| Its HTTP process fault matrix | 14m 13s |
| build-and-smoke total | 71m 25s |
| Its workspace all-targets | 61m 50s |
| Linux S3 semantic fixture step | 16m 36s |

All-targets and persistence integrations overlap; several filtered lib suites are
also rerun under different feature combinations. Four Windows lexical jobs are
already excluded from the Windows remainder, so they are **not** four duplicate
runs there, but each runner separately builds dependencies. Most cost in this
run was execution, not compilation. The split reduces default work rather than
claiming target caching alone solves the delay. The subsequent P0 target ownership split below removes the duplicate Linux
integration run while explicitly retaining feature-specific library/bin coverage.

That run ended with **2 successful / 7 failed jobs**: Windows owner-lock errors
and a browser restore request exceeding its unchanged 30-second limit. The WAL
fix has 19 combined bounded Linux regressions passing locally together with the
encoding change, but needs Windows execution. The browser limit failure remains
unresolved; moving the matrix to manual execution is not a fix or a green result.
Original capacity writes remain stopped at 63,325 confirmed records; CI changes
do not authorize resuming that experiment or claim 100k success.

## Test-target ownership (P0)

The full manual workflow sets `ANYSEEK_TEST_SCALE=full`; an absent variable means
`small`, and any other value fails. Full-size inputs and repeated-query counts
remain available without a second copy of the test logic.

| Coverage | Owner in full acceptance |
| --- | --- |
| Linux all 33 integration targets, default zh/vb + server (no fault hooks) | build-and-smoke: workspace all-targets |
| Linux library + service unit tests with server/fault-injection | Persistence: explicit --lib --bins |
| Windows integration with server/fault-injection | Persistence remainder + four lexical matrix jobs |
| Both OS no-default library and default/no-default doctests | Persistence, unchanged |
| Linux normal HTTP + browser / Windows normal HTTP | build-and-smoke / Persistence |
| Both OS fault HTTP and S3 fixtures | Persistence and S3 jobs, unchanged |

The removed Linux integration rerun had no integration test conditional on
`fault-injection`; server and zh feature-specific tests still run in the Linux
all-targets owner. Fault-feature library and binary tests remain on Linux, and
fault-feature integrations remain on Windows. This is a deliberate coverage
assignment, not a claim that different feature combinations are identical.

The two dense deletion count tests now share one 2,048-record setup and one
delete/checkpoint sequence, while retaining all four AND/OR Count/TopkCount
assertions. The 760-record disjoint-union fixture keeps two repeated reads in
small mode and twenty in full mode; its obsolete 65,536-boundary comments are
corrected. Actual 65,536, 10,001, >16MiB, 64→65 and 128→129 boundary tests are
unchanged, as are HTTP repeated operation replay and separate Indexer/auth tests.

## Small and full query fixtures (P1)

Default PR core jobs explicitly select `small` and run six integration targets
in addition to the 171 exact core cases. The manual matrix explicitly selects
`full`, including its four independent Windows lexical jobs. No production
capacity, scoring, timeout or fault boundary changes.

| Fixture | Small | Full | Retained invariant |
| --- | ---: | ---: | --- |
| mixed and five-term lexical, both field-length policies | 514 each | 8194 each | literal member sets and score oracle, >100 candidates, Top1/10/100; mixed immediate/checkpoint/reopen and order variants |
| frequent terms | 256 | 3000 | frequencies 100%/50%, intersection >10, both Top10 modes and exact count |
| high/low-tier union ranking | 160 | 1000 | 40 high-score records, 12 low-score groups, full oracle versus Top100 |
| sparse uniform union | 200 | 2000 | 20 distinct groups, >100 candidates, full heap and score boundary |
| forty-term union | 120 | 1000 | all 40 terms, exact total, Top100 filled |
| repeated disjoint union | 760 / 2 repeats | 760 / 20 repeats | same members, first/repeated Count vs TopkCount |
| dense deletion AND/OR | 2048, one shared setup | same | all four independent count assertions |

The lexical 576/522 append/winner fixtures and ranking 400/160/250 score-gap,
mode-agreement and pagination fixtures remain unchanged. Full scale restores
original population values; small scale does not claim to exercise their larger
layout boundaries. Dedicated 8192-package/10001-identity and other physical,
byte-budget and publication boundaries remain in full acceptance.

Local P0 paired measurement on the same debug profile (excluding compilation):
bitmap 203.19s → 102.89s (all four assertions retained; about 49.4% shorter);
union 34.41s → 34.10s (the data build dominates, so fewer repetitions have little
measured benefit). These are one-run local observations, not a throughput claim.
P1 historical full-size CI timings use a different runner and are not a paired
speedup baseline for the local small-fixture run.

P1 local small runs passed 13/13: frequent 6.65s, lexical six cases 134.11s,
union ranking five cases 55.07s, forty-term union 4.75s. Selected full branches
were also executed with the same binary/profile: frequent (3000) 108.58s and
forty-term (1000) 47.10s, both passed. These compare fixture work at different
scales, not engine throughput or identical large-layout coverage. Full 8194
lexical and full union-ranking populations were preserved by parameter review
but not rerun locally in this bounded validation.

## Default long-recovery exclusions and full-suite deduplication

The default list omits these two 2,305-update tests; their source and assertions
remain unchanged and neither is ignored:

- `record::cache_merge::tests::repeated_updates_beyond_sixteen_tails_recover_from_original_wal`
- `record::cache_merge::remote_tests::remote_long_suffix_merges_caches_without_replacing_anchor_and_cold_recovers`

Manual `build-and-smoke` runs both with workspace/server all-targets. Persistence
also includes both in Linux server/fault-injection lib tests and Windows
server/fault-injection `--tests` (library unit tests are included). They retain the
long-tail boundary evidence; the default suite retains bounded cache recovery,
batch/crash, idempotency, owner-lock, TTL and queue coverage. Other historical
cases remain because their age alone does not establish duplicate assertions.

In successful CI run `40997772763`, these two cases consumed about 17m21s of
Windows core execution. Removing that work is an estimate of saved execution
on a comparable runner, not a measured new workflow duration or an equal
reduction in total CI wall time; compilation and other jobs remain.

Full acceptance avoids only these established repeats:

- S3 job's filtered `remote_checkpoint::tests` rerun: the persistence jobs run
  the same tests with the same `server,fault-injection` features on both OSes.
  S3 HTTP fixture contract and semantic suites remain.
- Linux persistence normal HTTP smoke and its standalone service build:
  `build-and-smoke` runs normal HTTP acceptance; standalone console browser checks
  run against their own disposable API in PR feedback and full acceptance;
  persistence builds its own fault-enabled service before all later consumers.
  Windows normal HTTP, both OS fault HTTP, Indexer and authorization remain.
- Linux filtered server `record_service::tests` and `resources::tests` reruns:
  workspace/server all-targets already owns these same-feature tests. Windows
  filtered tests and distinct s3-object-store `resource_` coverage remain.

No test timeouts, fixtures, sharding or cache policy change in this trimming.

## Shared test bodies without removing scenarios

The four `unlocked_encoding_*` / `unlocked_upload_*` pairs in
`record/remote_checkpoint.rs` retain all eight individually selected test names.
Each pair now calls one assertion helper with its original `encode` or `upload`
pause point; normalized bodies were identical before extraction. Both run under
the same s3-object-store test module (included by `server`).

| Shared scenario | Inputs and assertions retained at both pause points |
| --- | --- |
| reads, writer ordering, checkpoint and cancellation | seed receipt; two ordered writes; blocked writer/checkpoint; cancelled waiter; cold recovery and exact receipt replay |
| candidate TTL | deadline +100ms, advance +101ms; Busy; absent receipt; unchanged local/authority sequence; healthy writer; expired retry rejected |
| source expiry | expired original plus version-matched replacement; Busy; no replacement receipt; source absent before/after cold recovery; retry rejected/conflicted |

The common gate wait retains its five-second timeout and polling interval.
Owner/sequence fencing before persistence and owner/head changes after upload
remain separate: their error classes and attempted-write scopes differ.
Executor prefix-stop tests and committed-ingest continue-after-conflict tests
also remain separate contracts. This extraction reduces duplicate source, not
the number of executed scenarios or measured test runtime.

## Lexical behavior contracts

`lexical_correctness` names describe observable retrieval behavior rather than
retired posting codecs, OR-subset enumeration or score-pruning internals:

| Test family | Retained inputs and independent assertions |
| --- | --- |
| `mixed_term_membership_scores_and_query_order_{shared,per_field}` | Three literal term sets; exact OR membership, AND result 103, per-document BM25 sum, query-term permutations; immediate/checkpoint/reopen stages; implicit and explicit score order |
| `five_term_union_scores_and_topk_prefix_{shared,per_field}` | Five overlapping sets; exact union, independent additive BM25, Top1/10/100 prefixes; checkpoint; both score-order paths |
| `appended_documents_preserve_scores_and_top1` | 512 then 64 records with a checkpoint between and after; distinct Shared/PerField length formulas, membership 0/1/512, winner 1 and Top1; reopen; both policies |
| `late_matching_document_remains_in_top2` | 522 records, eleven hits, independent scores, late ID 521 remains second to ID 0; Top2 agrees with complete ranking; both policies |

The mixed and five-term populations remain 514 in small mode and 8194 in full
mode. Their different term sets change document lengths and expected scores;
they deliberately keep separate fixtures. Append checkpoints and mixed-stage
reopen checks remain because they exercise different observable states.
`common/scale.rs` retains its strict small/full selection and invalid-value error.

Only identical input construction is shared between the term-set fixtures.
The unused search `realtime` argument and an infallible Result wrapper were
removed; they never selected a different query or persistence path. The common
`append` helper now delegates to `insert`, retaining sequential IDs, operation
identity, synchronous visibility, Busy-only retry and panic-on-error behavior.
Windows matrix names and remainder exclusions are updated together and guarded
by `check_lexical_partition.py`. No production algorithm or data scale changes.

Local before/after observation for this cleanup (one run each, not a benchmark):
Linux x86-64, rustc 1.99.1, native CPU debug/test profile, `--features server`,
`ANYSEEK_TEST_SCALE=small`, `--test-threads=1 --nocapture`. Both runs passed all
six tests; compilation is excluded. Existing progress logs give these summed
phase times (rounded to milliseconds):

| Phase | Before | After |
| --- | ---: | ---: |
| Fixture writes | 127.545s | 127.496s |
| Checkpoint | 9.206s | 9.077s |
| Reopen | 1.560s | 1.556s |
| Queries | 1.302s | 1.279s |
| Whole test target | 139.70s | 139.50s |

Writes account for about 91% of execution. The 0.20s total difference is not
credible evidence of a performance improvement; renaming also changes test
execution order. The verified benefit is removing obsolete terminology and
identical helper code without reducing cases or data. Full-scale and Windows
execution were not repeated locally; their selection and unchanged fixtures
were verified, not presented as executed acceptance.

测试等价收敛：NOT oracle 的 Topk/TopkCount 共用初始化、compact 和 reopen 夹具，保留全部断言组合；固定状态计数查询保留首查和后续查两遍；退役字段按字段名各保留一个拒绝代表值。手工 S3 background_index 子集跳过同 feature 下已运行的 indexer::tests；不同平台、feature 和 HTTP 故障边界保持。
