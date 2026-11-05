# CI feedback and complete acceptance

`PR feedback` runs on pull requests, master pushes and manual dispatch. It checks
retired dependencies, console asset separation and the lexical partition;
on code changes it builds and runs the exact bounded core regression list plus the
small query integration matrix on Linux and Windows, then builds the service with the same features and generates its
OpenAPI schema. The case list is `scripts/ci_core_cases.json`; missing or renamed
cases fail rather than silently matching zero tests. Integration scales are listed
below. This is fast feedback, **not full acceptance**.
Documentation-only changes keep static checks but skip the compiled core matrix.
Markdown file links and anchors need a separate check; the retired brand checker
is not part of the workflow. Console build/unit checks run for code changes, and
the Linux core lane runs the standalone browser suite against a disposable API.
Both core lanes also run local/S3-fixture deleted-generation reclamation checks.
The service persistence-error/fence and shutdown ownership cases run explicitly
with `fault-injection` on both OSes. Linux also runs the real SIGTERM smoke; the
normal service build and OpenAPI smoke keep the default feature set.
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

Historical workflow timings, failed runs and cleanup experiments remain in Git
history and their PRs. They are not current validation results; moving a check to
manual execution does not resolve a previous failure. Validation results and
remaining acceptance gaps are tracked in [路线与验收证据](roadmap.md).

## Test-target ownership

The full manual workflow sets `ANYSEEK_TEST_SCALE=full`; an absent variable means
`small`, and any other value fails. Full-size inputs and repeated-query counts
remain available without a second copy of the test logic.

| Coverage | Owner in full acceptance |
| --- | --- |
| Linux all integration targets, default zh/vb (no fault hooks) | build-and-smoke: workspace all-targets |
| Linux module + service unit tests with fault-injection | Persistence: explicit --lib --bins |
| Windows integration with fault-injection | Persistence remainder + four lexical matrix jobs |
| Both OS no-default service unit tests and default/no-default doctests | Persistence |
| Linux normal HTTP + browser / Windows normal HTTP | build-and-smoke / Persistence |
| Both OS fault HTTP and S3 fixtures | Persistence and S3 jobs, unchanged |

The removed Linux integration rerun had no integration test conditional on
`fault-injection`; HTTP and zh-specific tests still run in the Linux
all-targets owner. Fault-feature library and binary tests remain on Linux, and
fault-feature integrations remain on Windows. This is a deliberate coverage
assignment, not a claim that different feature combinations are identical.

The two dense deletion count tests now share one 2,048-record setup and one
delete/checkpoint sequence, while retaining all four AND/OR Count/TopkCount
assertions. The 760-record disjoint-union fixture keeps two repeated reads in
small mode and twenty in full mode; its obsolete 65,536-boundary comments are
corrected. Actual 65,536, 10,001, >16MiB, 64→65 and 128→129 boundary tests are
unchanged, as are HTTP repeated operation replay and separate Indexer/auth tests.

## Small and full query fixtures

Default PR core jobs explicitly select `small` and run six integration targets
in addition to the exact core case list. The manual matrix explicitly selects
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

The lexical 576/522 append/winner fixtures and ranking 400/160 score-gap and mode-agreement fixtures retain their populations.
Long-query pagination now shares the 400-record score-gap fixture and checks
record IDs as well as scores; the duplicate 250-record fixture is removed. Full scale restores
original population values; small scale does not claim to exercise their larger
layout boundaries. Dedicated 8192-package/10001-identity and other physical,
byte-budget and publication boundaries remain in full acceptance.

## Default long-recovery exclusions and full-suite deduplication

The default list omits these two 2,305-update tests; their source and assertions
remain unchanged and neither is ignored:

- `record::cache_merge::tests::repeated_updates_beyond_sixteen_tails_recover_from_original_wal`
- `record::cache_merge::remote_tests::remote_long_suffix_merges_caches_without_replacing_anchor_and_cold_recovers`

Manual `build-and-smoke` runs both with workspace all-targets. Persistence
also includes both in Linux fault-injection lib tests and Windows
fault-injection `--tests` (library unit tests are included). They retain the
long-tail boundary evidence; the default suite retains bounded cache recovery,
batch/crash, idempotency, owner-lock, TTL and queue coverage. Other historical
cases remain because their age alone does not establish duplicate assertions.

Full acceptance avoids only these established repeats:

- S3 job's filtered `remote_checkpoint::tests` rerun: the persistence jobs run
  the same tests with the same `fault-injection` features on both OSes.
  S3 HTTP fixture contract and semantic suites remain.
- Linux persistence normal HTTP smoke and its standalone service build:
  `build-and-smoke` runs normal HTTP acceptance; standalone console browser checks
  run against their own disposable API in PR feedback and full acceptance;
  persistence builds its own fault-enabled service before all later consumers.
  Windows normal HTTP, both OS fault HTTP, Indexer and authorization remain.
- Linux filtered server `record_service::tests` and `resources::tests` reruns:
  workspace all-targets already owns these same-feature tests. Windows
  filtered tests and object-buffer `resource_` coverage remain.

No test timeouts, fixtures, sharding or cache policy change in this trimming.

## Shared test bodies without removing scenarios

The three `unlocked_encoding_*` / `unlocked_upload_*` pairs in
`record/remote_checkpoint.rs` retain six individually selected test names.
Each pair now calls one assertion helper with its original `encode` or `upload`
pause point; normalized bodies were identical before extraction. Both run under
the same object-store test module (always included in service builds).

| Shared scenario | Inputs and assertions retained at both pause points |
| --- | --- |
| reads, writer ordering, checkpoint and cancellation | seed receipt; two ordered writes; blocked writer/checkpoint; cancelled waiter; cold recovery and exact receipt replay |
| candidate TTL | deadline +100ms, advance +101ms; Busy; absent receipt; unchanged local/authority sequence; healthy writer; expired retry rejected |
| TTL during planning/upload | inherited deadline crossing returns Busy with no new receipt; reads filter without persistence or writer wait; a fresh full replacement may reuse the unchanged technical version after expiry |

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

测试等价收敛：NOT oracle 的 Topk/TopkCount 共用初始化、compact 和 reopen 夹具，保留全部断言组合；固定状态计数查询保留首查和后续查两遍；退役字段按字段名各保留一个拒绝代表值。手工 S3 background_index 子集跳过同 feature 下已运行的 indexer::tests；不同平台、feature 和 HTTP 故障边界保持。
