# CI feedback and complete acceptance

`PR feedback` runs on pull requests, master pushes and manual dispatch. It checks
repository documentation, dependency/core boundaries and the lexical partition;
on code changes it builds and runs 27 exact bounded core regressions plus the
small query integration matrix on Linux and Windows, then builds the service with the same features and generates its
OpenAPI schema. The case list is `scripts/ci_core_cases.json`; missing or renamed
cases fail rather than silently matching zero tests. The largest rotation fixture
in the 27-case core set contains 129 records; integration scales are listed below. This is fast feedback, **not full acceptance**.
Documentation-only changes keep static checks but skip the compiled core matrix.
Generic x86-64 CPU flags permit target caching within each OS; full acceptance
retains the existing native-CPU configuration and source-only cache policy.

## Run full acceptance

Before merge, use the existing `ci.yml` workflow entry (the default-branch UI
may still label it **Retained workspace**) with the PR ref and `full=true`:

```sh
gh workflow run ci.yml --ref fix/record-capacity-100k -F full=true
```

This explicit command runs the reusable `.github/workflows/full-acceptance.yml`
from the selected commit, as well as quick checks. Do not run it merely to
inspect configuration. After the new file exists on master, **Full acceptance
(manual)** also has its own Run workflow entry. Verify the run's exact head SHA before
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
| Browser normal service / HTTP fault service / S3 fixtures | Original jobs, unchanged |

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
in addition to the 27 exact core cases. The manual matrix explicitly selects
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
