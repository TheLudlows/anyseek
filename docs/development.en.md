# Development and verification

**English** · [简体中文](development.md)

The AnySeek repository contains one `anyseek` Cargo package, which provides a standalone HTTP service and an in-process Rust SDK. See the [repository README](../README.en.md#embedded-rust-sdk) for the public SDK entry point and lifecycle. Storage and search live in `anyseek/src/`, HTTP in `anyseek/src/server/`, and the standalone React/TypeScript/Vite console in `console/`. The repository is `TheLudlows/anyseek`.

## Run locally

The API and UI run in separate processes. Rust is required. The frontend also needs Node.js 24 and npm. See the [quick start](../README.en.md#quick-start), [service configuration](../anyseek/src/server/README.en.md#build-and-start), and [console guide](../console/README.en.md#run-the-ui-locally).

## Build and test

Run the following from the repository root.

```sh
cargo check -p anyseek
cargo check -p anyseek --no-default-features
cargo build --workspace
cargo test --workspace --all-targets
cargo test -p anyseek --no-default-features --lib --bins
cargo test --workspace --doc
cargo doc --workspace --no-deps
python3 scripts/check_console_boundary.py
python3 scripts/check_retired_dependencies.py
python3 scripts/check_lexical_partition.py --source-only
python3 scripts/test_s3_acceptance.py
npm ci --prefix console
npm test --prefix console
npm run build --prefix console
```

Defaults enable `zh` (Chinese segmentation), `vb` (result diagnostics), and `ordered-json` (preserving JSON object field order). `--no-default-features` disables these optional features and still builds full HTTP and local/S3 service support. `fault-injection` is only for explicit fault tests. `.cargo/config.toml` selects native CPU instructions, so these artifacts cannot be reused across runners with different CPUs.

`ANYSEEK_TEST_SCALE` defaults to `small` and accepts only `small` or `full`. Full acceptance explicitly selects `full`. Local default `cargo test` cannot substitute for a full run. See [query fixture scale](#query-fixture-scale) for sizes and coverage.

## HTTP, browser, and fault tests

Smoke tests use temporary data and do not connect to user services. On Windows, the server path is `target/debug/anyseek_server.exe`. Browser tests require Playwright and Chromium. See [console verification](../console/README.en.md#verification) for installation and bootstrap.

```sh
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server
cargo build -p anyseek --features fault-injection
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server --faults
python3 scripts/s3_storage_smoke.py --server target/debug/anyseek_server --faults
python3 scripts/ci_write_path_cases.py
```

The targeted write runner compiles fault-injection tests from `scripts/ci_write_path_cases.json` and runs each in an isolated process. It covers queryable deltas, native materialization, reservation before WAL, transfer, cancellation, and cold recovery. Missing or renamed cases fail. Zero matches are rejected. The runner configures the process environment for small shared-budget cases to prevent a previous test contaminating LazyLock.

Verify production panic behavior in a real release subprocess. `cargo test --release` cannot substitute for it.

```sh
cargo build -p anyseek --locked --release --features fault-injection --example runtime_panic_probe
python3 scripts/release_panic_smoke.py --probe target/release/examples/runtime_panic_probe
```

The probe covers a panic after a partial vector consumer modification, before queue publication, and after publication but before receipt delivery. It reopens and looks up or retries the original Operation ID. These local process fault tests do not prove S3, all publication boundaries, or hard-power-loss recovery. Linux real signal checks use `scripts/record_shutdown_smoke.py`. [Shutdown contract](architecture.en.md#service-shutdown)

Test variables use the `ANYSEEK_` prefix, such as `ANYSEEK_TEST_ROOT`. The normal master secret is still `MASTER_KEY_SECRET`. After fault tests, rebuild with `cargo build --workspace` for the default binary.

## Cache-free S3 and contract acceptance

A common entry point reuses existing tests. `semantic` isolates base, temporal, history, and ingest into four service roots. `--faults` adds indexer and authorization. Each restart removes `collections/*/*/*` and `*.object-cache` caches and retains the SQLite coordination metadata and owner lock. `storage` covers checkpoint, GC, joint backups, and interrupted restores. `contract` runs explicitly ignored immutable-object contract tests. `all` runs all three in sequence.

```sh
cargo build -p anyseek --features fault-injection
python3 scripts/s3_acceptance.py --backend fixture --suite all \
  --server target/debug/anyseek_server --faults --report target/s3-acceptance.json
```

A single suite can be selected with `--suite semantic|storage|contract`. Contracts require cargo but no server. Before testing, the runner copies the pinned binary so compiling the contract cannot replace the tested program. Reports store binary SHA-256, exit status, and elapsed time, without credentials. When operations such as backup exceed the HTTP completion deadline, they may return outcome_unknown. Retry within bounds with the same request and operation_id.

Real mode requires a dedicated disposable test bucket and its current process environment configuration. It never falls back to production S3 settings. Full service restore also requires `ANYSEEK_S3_TEST_RESTORE_BUCKET`. The fixture supplies a second bucket automatically.

```sh
# 当前进程设置 ANYSEEK_S3_TEST_ENDPOINT、ANYSEEK_S3_TEST_BUCKET、
# ANYSEEK_S3_TEST_REGION、ANYSEEK_S3_TEST_ACCESS_KEY_ID、
# ANYSEEK_S3_TEST_SECRET_ACCESS_KEY；HTTP 另需 ANYSEEK_S3_TEST_ALLOW_HTTP=1。
python3 scripts/s3_acceptance.py --backend real --suite all \
  --server target/debug/anyseek_server --faults --report target/s3-real-acceptance.json
```

Tests create and delete objects. The end-to-end matrix leaves that run's objects for the test bucket administrator to destroy after acceptance. Low-level contracts delete only explicitly recorded keys, with no LIST or bulk prefix deletion. Contracts cover conditional creation races, immutable retry, readback and HEAD, hash and length, missing and oversized objects, old ownership, original-cut backup, and deletion.

Check `backend=real`, `real_endpoint_exercised=true`, `all_requested_checks_passed=true`, and the suite in the report. A passing fixture, empty-config refusal, or a contract-only run does not certify a complete real backend. See the [roadmap](roadmap.en.md) for gaps.

## Fast CI feedback

These lanes reflect [.github/workflows/ci.yml](../.github/workflows/ci.yml). `PR feedback` runs for PRs, pushes to master, and on manual triggers.

| Lane | Current checks |
| --- | --- |
| static (all changes) | S3 acceptance runner unit checks and lexical test source partition |
| static (code changes) | Console build and unit tests, frontend/backend boundary, and retired dependencies |
| core (Linux/Windows) | Exact core manifest, five deletion/ranking integration targets, synchronous and asynchronous HTTP, failure fencing and shutdown, service config and OpenAPI, and old generation reclamation on local and S3 fixture backends |
| lexical (Linux/Windows) | Standalone lexical_correctness with small membership and scoring oracles |
| write-path (Linux/Windows) | Exact fault-injection write manifest, one isolated process per case |

Linux core also runs real SIGTERM and a separate browser suite including bootstrap. The core manifest is `scripts/ci_core_cases.json` and write manifest is `scripts/ci_write_path_cases.json`. Runners verify exact cases exist and pass.

Documentation changes skip the compile matrix, console build, boundary, and dependency graph checks, and keep the two static source checks. Check Markdown links and anchors separately. Fast lanes use generic x86-64 CPU flags to share artifacts on the same OS and cannot substitute for full native acceptance.

## Full acceptance

Run manually against a pinned candidate ref.

```sh
gh workflow run ci.yml --ref YOUR_CANDIDATE_REF -F full=true
```

You can also trigger `Full acceptance (manual)` directly. The command executes the selected commit's [.github/workflows/full-acceptance.yml](../.github/workflows/full-acceptance.yml). You can read the configuration without triggering a run. Verify the exact head SHA before relying on results and reassess later commits. Manual runs neither cancel each other nor get cancelled by ordinary PR feedback.

The complete workflow uses `ANYSEEK_TEST_SCALE=full`, native CPU configuration, and source-only caching. It expands to 11 jobs.

| Coverage | Job |
| --- | --- |
| All default-feature Linux modules and integration tests, normal HTTP/browser, release panic, and OpenAPI | build-and-smoke |
| Linux fault-injection persistence library/server, targeted no-default-feature checks and default/no-default doctests on both platforms, and Windows native default-feature unit tests | persistence-native × 2 |
| Windows fault-injection library, binary, and integration tests | windows-fault-tests × 2 (library and binary, remaining integration cases) and four independent windows-lexical cases |
| Windows normal HTTP and both platforms' fault HTTP, indexer/authorization, storage faults, and recovery | persistence-native |
| S3 fixture object contracts and six cache-free semantic suites on both platforms | s3-native-acceptance × 2 |

S3 remote-checkpoint unit tests run with matching features in `persistence-native` on Linux and `windows-fault-tests` on Windows. The S3 job covers fixture object contracts and HTTP semantics. `build-and-smoke` uses `--all-targets` for default-feature Linux unit and integration tests. Windows runs the native default-feature unit tests in one filtered invocation. The Windows fault-injection library and binary tests, integration tests, and HTTP crash recovery in `persistence-native` run in parallel.

The no-default-feature step checks the binaries, embedded SDK, tokenizer and OpenAPI contracts, text normalization, snapshot encoding, and request diagnostics.

The fast manifest skips the following two long recovery cases with 2,305 updates. Full all-targets and fault library tests continue to cover them.

- `record::cache_merge::tests::repeated_updates_beyond_sixteen_tails_recover_from_original_wal`
- `record::cache_merge::remote_tests::remote_long_suffix_merges_caches_without_replacing_anchor_and_cold_recovers`

### Query fixture scale

This table describes retrieval fixtures only. Production capacity, timeouts, failures, and publication boundaries do not change between small and full.

| Fixture | small | full | Primary assertions |
| --- | ---: | ---: | --- |
| mixed / five-term, two field length policies | 514 each | 8194 each | Exact membership, independent BM25 oracle, Top1/10/100. Mixed covers immediate/checkpoint/reopen and query-term ordering. |
| frequent terms | 256 | 3000 | 100%/50% frequency, intersection, two Top10 modes, exact counts |
| high/low score union | 160 | 1000 | 40 high-score records, 12 low-score groups, complete oracle and Top100 |
| sparse uniform union | 200 | 2000 | 20 groups, more than 100 candidates, heap and score boundaries |
| forty-term union | 120 | 1000 | 40 terms, exact total, full Top100 |
| disjoint union | 760, repeat 2 times | 760, repeat 20 times | Agreement between initial/repeated Count and TopkCount |
| dense deletion AND/OR | 2048 | 2048 | Four AND/OR Count/TopkCount groups |

The 576/522 append/late-hit and 400/160 ranking fixtures retain fixed sizes. Independent boundaries such as 65,536, 10,001, >16 MiB, 64→65, 128→129, and 8192-package are not replaced by small fixtures. `scripts/check_lexical_partition.py` verifies the Windows lexical split. Update the manifest, matrix, and remainder exclusions when renaming or repartitioning cases.

The lexical fixtures explicitly build a checkpoint after writing. If background maintenance holds the build slot, the fixture retries `Busy` within a bounded window and checks that the completed checkpoint covers the full sequence for that phase. Reopen, membership, scoring, and ranking assertions still run.

## API and internal modules

```sh
target/debug/anyseek_server generate_openapi=true
```

The outputs are next to the executable, typically `target/debug/openapi.json` and `openapi.yml`. HTTP DTOs and schemas are in `anyseek::server`. The [service guide](../anyseek/src/server/README.en.md) defines the public contract.

### Service internals

[RecordPartition](../anyseek/src/record.rs) handles record persistence and search. The [asynchronous index implementation](../anyseek/src/record/committed.rs) manages durable business outcomes and consumer progress. The library exposes `anyseek::sdk::LocalEngine`. View internal types and methods with `cargo doc -p anyseek --no-deps`. See the [Record contract](record-model.en.md) for semantics and retry, and the [architecture](architecture.en.md) for ownership and failures.

## Verification boundaries

Linux and Windows both require native reliability acceptance. Cross-compilation cannot replace native execution on Windows. Storage, format, and recovery changes need a full matrix at a pinned candidate SHA. A passing fast CI run cannot cover a missing or failed full check.

Collect separate evidence for a real S3 endpoint, OS/filesystem failure, hard power loss, cross-AES operation, and sustained long-running operation. Process kill/exit, fixtures, fsync calls, and compilation do not substitute. Power-loss verification requires an isolated environment, durable hardware, and external acknowledged write evidence. Do not reboot shared environments.

Record commands for the final source, results, failures and ignored cases, and checks not run. Historical counts, measurements on other versions, and PR records are not current retest results. The [roadmap](roadmap.en.md) tracks pending acceptance and evidence.
