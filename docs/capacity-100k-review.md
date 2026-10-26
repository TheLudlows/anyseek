# Local 100k capacity-path review

Review code: `b03ba586fd2619faa9d218ef1e7585c6c94c3243`, relative to
`b561b65d971f2b7e51fcefbd0382e39ce1dfe0b2`. No new implementation was pushed.
Local exact core diff: `/tmp/anyseek-capacity-b03-core.patch`.
[Full evidence and experiment state](capacity-100k-plan.md).

## Scope and invariants

| Area | Change | Retained limit/behavior |
|---|---|---|
| HTTP creation | Explicit `capacity_profile: "bounded_100k"` sets records/operations to100000; missing/default profile stays10000 |256MiB authority,1MiB document,10000 candidates, existing memory hierarchy; unknown profile rejected |
| Core population | Record validation, evidence traversal and selected native corpus share100000 maximum |Depth16,32 evidence references, per-native-package10000, existing score/query formulas |
| Certified packages | Prepare at most8192 identities at a time; release writer before next package; at most16 packages for larger configurations |<=10000 configurations keep8-package selection bound; identity batch admitted before allocation; no independent package authority |
| Foreground State | Borrow live State under partition guard instead of deep-cloning history; release explicit input candidate before recovery |Old State remains authoritative through preparation/certificate failure; recovered State installed only after successful certificate |
| Query | Internal selected-corpus bound covers the larger population before scoring/sorting |Configured external candidate window, TopK filtering, active statistics, stable identities and one global RRF unchanged |

Complete-state residency remains conservatively charged at32 times canonical JSON;
no estimator coefficient or default quota was lowered. Background captures,
independent snapshots and speculative generation/TTL/lease candidates still own
separate admitted deep copies. Recovery keeps old and decoded State concurrently
until publication; this can remain the first finite-memory boundary.

Configuration is persisted in the authenticated State descriptor. The larger
profile is an upper admission bound, not a claim that arbitrary100k records fit.
At100k first-write operations the profile has no extra operation-history allowance;
no idempotency history is evicted to create room. WAL frames and future event
reserves remain16MiB. Migration/old-format readers were not added.

## Regressions and actual evidence

- Core population boundary test accepts100000 configuration, rejects100001,
  preserves candidate configuration and verifies bounded package arithmetic.
- A10001-identity canonical Applied fixture is certified into2 packages plus tail;
  full count10001, last-document match and original receipt survive reopen.
  This is structural coverage, not an HTTP load claim.
- HTTP profile test verifies unchanged defaults/other limits and rejects unknown
  profile names.
- Final code Record suite:174 passed,0failed,5 existing worker-entry ignores,
  exit0; parent tests invoke those cancellation/process-exit workers.
- Same code, unoptimized binary, default HTTP profile:10000 writes,393 query
  probes with zero HTTP errors, checkpoint200, reopen count10000 and identical
  final operation retry. The debug100k staircase stopped at a server deadline;
  corrected read-only observation resolved all25 original operations at11925.
  Release validation is separate; see the optimized-build section in the plan.

## Review focus / still unverified

1. Check authority rollback when explicit candidate is dropped before reload;
   the old live State must remain intact until certificate publication.
2. Check package chunk boundaries and aggregate selection limits, especially
   incremental freeze followed by full compaction and explicit field sorting.
3. Confirm population admission does not silently broaden the external candidate
   window or relation completeness claims.
4. Observe finite-memory refusals, checkpoint deadline and reopen behavior on the
   real staircase. The original harness terminated its service on error; the
   corrected observer preserves unresolved owned work and never resubmits writes.
   That original harness exit was not a spontaneous service crash. Process/RSS/I/O
   observations are retained in `/tmp/anyseek-capacity-phase20-observations.jsonl`.
   OS permission denied I/O-counter reads; CPU/RSS observations remain available,
   and the denied I/O read is not retried.
5. New all-targets/no-default/Windows/complete HTTP-S3 matrices remain outstanding
   for this cumulative capacity implementation. Real S3/system power-loss/manual
   cross-AES validation is not claimed. Process-exit fixtures are not power loss.

Do not repeat writes or replace the measured binary while the staircase runs.
Publication remains pending the specific approval following the prior automated
review rejection; do not retry PR creation or push new local commits yet.


## Latest measured limit (same code, optimized build)

Same explicit3GiB/6GiB budgets as both debug staircases; defaults remain1GiB/4GiB
and10k HTTP capacity. Release passed10k/20k/50k checkpoint/reopen/receipt checks,
then stopped at56272 accepted: one shared-budget storage error and2 not-attempted
items. A separate checkpoint failed complete-State clone admission. Recovery
exceeded60s but the same process later recovered every accepted identity and
sampled receipts/query results.393 OR probes completed with0 errors;100k and a
new Meilisearch comparison remain unfinished. Full numbers and raw evidence are
in the plan. No new production code or budget change was made during this run.

The component8MiB error is not sufficient to distinguish frozen loading from
empty-tail allocation; both call the same admitted base. The checkpoint's
2766117632-byte clone and128-decision recovery tail provide concrete next-stage
investigation targets. Retain complete history, certificate validation and owned
leases when removing copies; do not simply suppress charges.

## Phase 23: bounded verification and ordinary-tail replay (local, under validation)

The phase22 failure is a shared build-memory rejection, not an observed RSS/OOM failure. Its 8MiB request equals the four-field base reservation; both frozen-component loading and empty-tail construction use that reservation. New `stage=frozen_component_base package=...`, `stage=anchor_empty_tail`, and `stage=index_snapshot_clone` context distinguishes those callers on a new failure. The historical log alone does not uniquely identify the caller.

Changes being validated:

- Ordinary durable tail replay reuses the exact per-record/checkpoint size arithmetic used by write preparation, instead of cloning and serializing the complete retained State for each decision. Projection is admitted before copying incoming/prior bodies; receipt, generation, sequence and publication rules stay in force. Generation/expiration replay still retains its separately charged copy path.
- Background index planning removes the old additional 4x snapshot reservation, because the independently cloned State already owns a 32x residency lease and that plan no longer creates a full JSON buffer. Raw/decoded tail leases and the independent Scope quota remain.
- Foreground anchor publication verifies every persisted metadata/Payload frame against deterministic pieces of the guard-pinned State. The existing file closure, config/sequence, State digest, complete identity checks, frame bounds, ordered piece digest and visible native identity checks remain mandatory before certification. Each decoded/expected piece has a lease; no second complete decoded State is allocated. Explicit replacement candidates remain owned until the certificate succeeds. Normal recovery still decodes an owned State.
- Verification also checks the *complete decoded recovery footprint* against the single-reservation limit without pretending to allocate it. This prevents publication from bypassing a real recovery capacity limit. No memory coefficient, default budget, hard capacity profile or timeout changed.

The new `benchmarks/capacity/recovery_probe.py` copies the preserved phase22 database, performs no writes, and checks all 26 sampled receipts plus five exact query result/count comparisons before one explicit checkpoint and reopen. Unknown outcomes retain their process. The previous 60-second recovery failure remains historical evidence; a successful new probe is a repair result, not a rewrite of that failure.

Completed before the streaming-publication change: Record tests 157 passed / 0 failed / 5 ignored (`/tmp/anyseek-capacity-phase23-final-record.log`). Existing chunk tests after initial streaming change: 5 passed / 0 failed. Final combined regression, release probe and fresh capacity staircase are pending; these are not 100k acceptance results.

Final combined default Record suite for the phase23 code: **158 passed, 0 failed, 5 ignored**, exit0,354.68s (`/tmp/anyseek-capacity-phase23-stream-record.log`). This includes the new ordered-frame/duplicate-identity/expected-state/recovery-budget checks and the existing loaded-candidate rejection, certificate rollback, GC/pins, independent snapshots and durable-tail recovery tests. Python observer unit tests:2 passed; recovery probe syntax checked. Full all-targets/no-default/platform matrix remains pending. Release recovery/capacity measurements are not yet included in this result.

Phase23 release repair probe on production commit `af45fafb213848cdc8cb90aab2bfc5235a7c3b29` completed (exit0). Binary SHA256 `72bf6da6828bd6a73661b60584d5639711685f69c7c4cfb0aaaf2e9f09502ab1`; identical single3GiB/shared6GiB overrides, copied original56,272-record authority plus128Applied tail. Initial recovery3.544s (original version exceeded unchanged60s check);26original receipts and five top100 ID/score/count probes matched. One explicit checkpoint returned HTTP200 in19.344s; subsequent reopen2.078s, count/operation history and26receipts still matched. Peak sampled RSS753,856KiB. Original data/binary/error evidence preserved. This verifies recovery/checkpoint repair at the old boundary, not100k or same-scale Meili acceptance. Evidence: `/tmp/anyseek-capacity-phase23-recovery-probe/result.json` and associated receipts/queries/resources.
