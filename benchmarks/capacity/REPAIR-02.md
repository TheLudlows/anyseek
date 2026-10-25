# Both repairs: existing 10k boundary reached

Measured production commit: `4451c9c139de5b7c471b77f4dc2a9ebb6cd1da31`.
Release binary SHA256: `a9029de0868d17ae554adb2280f61b2bf6603e90d03e82e820ef2d2c2b65ba3d`.
Same pinned 10,001-document input, schema, default tokenizer/budgets and host/CPU
allocation as [BASELINE.md](BASELINE.md); fresh isolated root. No simultaneous
compilation or other benchmark. No production capacity/configuration increase.

- Attempted 10,001; **10,000 applied** in 424.41 seconds to the boundary.
- First refusal ordinal 10,001: envelope HTTP 200, per-item HTTP 400/code `rejected`,
  `Rejected("record/operation budget exceeded")`. This is the retained hard limit.
- 393 original-question OR probes: **0 errors**. All 994 positive pairs are in the
  accepted corpus (prior40 91/91, heldout353 903/903). No Meili comparison yet.
- Checkpoint **HTTP 200**, visible/indexed sequence both 10,000, **7.94 seconds**.
- Restart after this successful checkpoint: **0.661 seconds**, visible count
  **10,000** before/after restart and after retries.
- The last successful operation returns exactly its original result/receipt;
  the first refused operation retries as the same budget rejection.
- Sampled peak RSS **656,196 KiB (~640.8 MiB)**. This is an observed process peak,
  not a quota guarantee or a same-workload comparison with the smaller runs.

Independent checks reconstruct accepted IDs from the frozen input, confirm exact
order/uniqueness/count, all 393 query IDs/statuses, unchanged visible counts,
receipt equality and rejected retry classification. Successful checkpoint allows
restart from a selected native package; its restart latency is not directly
comparable with the older runs whose checkpoint failed and required tail rebuild.

Raw evidence and DB: `artifacts/benchmark/capacity/repair-default-02/` (local only).
Driver: `baseline.py --revision 4451c9c139de5b7c471b77f4dc2a9ebb6cd1da31 ...`.
Execution log: `/tmp/anyseek-capacity-repair02-default.log`; build log:
`/tmp/anyseek-capacity-repair02-build.log`. The run completed; no server remains.
Later test/document-only commits do not change this measured production code.

This demonstrates these two defects are removed at the existing 10k ceiling.
It does **not** implement 100k support. Remaining work includes bounded snapshot
encoding, State/operation ownership, finite population/component/package admission,
full-target/platform regressions and larger same-corpus Meili quality comparison.
Real S3, system power loss and manual cross-AES checks are not claimed here.

Publication checkpoint validation: strengthened anchor regression now also holds
an old Payload reader and all old object pins across successful compaction and GC;
old bytes remain readable, and all retired paths disappear only after release.
All 8 anchor tests pass again, 0 failed/ignored, 46.81s
(`/tmp/anyseek-capacity-pin-tests.log`). The earlier 40 lexical tests passed at
`37c935b`; native ownership code is unchanged since that commit. Full all-target
and exact-head Linux/Windows CI results remain pending, not inferred from these
focused checks. Python syntax, Markdown relative links and `git diff --check` pass.
