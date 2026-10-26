# CI feedback and complete acceptance

`PR feedback` runs on pull requests, master pushes and manual dispatch. It checks
repository documentation, dependency/core boundaries and the lexical partition;
on code changes it builds and runs 19 exact bounded core regressions on Linux
and Windows, then builds the service with the same features and generates its
OpenAPI schema. The case list is `scripts/ci_core_cases.json`; missing or renamed
cases fail rather than silently matching zero tests. The largest rotation fixture
in this set contains 129 records. This is fast feedback, **not full acceptance**.
Documentation-only changes keep static checks but skip the compiled core matrix.
Generic x86-64 CPU flags permit target caching within each OS; full acceptance
retains the existing native-CPU configuration and source-only cache policy.

## Run full acceptance

In GitHub Actions select **Full acceptance (manual)**, select the PR branch (or
master), and choose **Run workflow**. Its file is
`.github/workflows/full-acceptance.yml`. Verify the run's exact head SHA before
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
