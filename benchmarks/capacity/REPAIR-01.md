# First repair: measured next capacity boundary

Production revision: `37c935bec7bd4c988bd04f6a4c2da109cc1d1fe5`.
Binary SHA256: `e5a7e62b08783f85dc03566bce2600ad8229d65136bf6c545563364a042424d5`.
Same 10,001-document sample and defaults as BASELINE.md; no budget overrides,
compilation or second engine during measurement. New isolated data root.

| Observation | Original 34dc46f | Repair 37c935b |
| --- | ---: | ---: |
| Accepted documents | 5,248 | 8,320 |
| First refused ordinal | 5,249 | 8,321 |
| Failure | single build reservation >1GiB | payload build object budget exceeded |
| Load time to boundary | 148.25s | 314.13s |
| Peak sampled RSS | 388,724 KiB | 496,252 KiB |
| Reopen | 28.77s | 45.94s |
| Query errors /393 raw OR probes | 0 | 0 |
| Checkpoint | 503 build reservation | 503 payload object budget |

Different accepted populations make the timing/RSS columns boundary diagnostics,
not same-workload performance gains or regressions. All 994 positive pairs remain
included (91 prior40, 903 heldout353). The original successful operation replays
with the exact original result/receipt; the first failed operation still rejects.
Visible count is 8,320 before restart, after restart and after both retries.

The original 5,249 refusal is removed without coefficient or budget increases.
The next failure is independently located in `PoolBuilder::intern`: every reused
anchor may add an immutable Payload object; at 64 selected objects it cannot add
another. Retained historical operations keep older objects reachable, so ordinary
reuse cannot reclaim a slot. This is not a leak. The follow-up repair must compact
the complete referenced payload set into a private replacement at the fixed
ceiling, preserve all operation receipts, and publish through the same anchor
certificate; no deletion before selection and no increase to the object limit.

Neither checkpoint success nor 10k/100k support is claimed by this run. No Meili
large-scale comparison has run yet. Raw responses, resource samples, retained DB
and query probes: `artifacts/benchmark/capacity/repair-default-01/` (local evidence,
not an externally downloadable attachment). The original evidence is untouched.

Follow-up targeted validation: all 8 anchor tests pass (0 failed/ignored), including
the new 64→65-object boundary regression. Injected prepared-candidate failure
keeps the old anchor/64-object closure selected; retry compacts to one object,
reopen retains all 65 records and all 65 original operation receipts, and lexical
count remains 65. Log: `/tmp/anyseek-capacity-payload-tests.log`. This small
regression does not replace rerunning the real 8,321-record boundary.
