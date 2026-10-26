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

The complete original nine-job matrix is retained, with unchanged tests, fixture
sizes, assertions and timeouts: workspace all-targets; Linux/Windows persistence,
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
claiming target caching alone solves the delay. Full-matrix deduplication is
intentionally deferred to avoid accidentally losing feature-specific coverage.

That run ended with **2 successful / 7 failed jobs**: Windows owner-lock errors
and a browser restore request exceeding its unchanged 30-second limit. The WAL
fix has 19 combined bounded Linux regressions passing locally together with the
encoding change, but needs Windows execution. The browser limit failure remains
unresolved; moving the matrix to manual execution is not a fix or a green result.
Original capacity writes remain stopped at 63,325 confirmed records; CI changes
do not authorize resuming that experiment or claim 100k success.
