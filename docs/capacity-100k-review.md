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
