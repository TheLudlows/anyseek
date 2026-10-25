# Bounded 100k capacity: implementation checkpoint

Base: merged master `34dc46fd6b6603c96beead765915e4b4fa5c2919`.
Branch: `fix/record-capacity-100k`. This is a plan, not a capacity claim.

1. Freeze the 393 official dev questions/qrels (prior 40 and held-out 353 separately).
   Verify the original corpus manifest, retain all 3,786 judged documents (966
   positive), then deterministic hash-selected distractors. Probe current master
   to 10,001 attempted identities through HTTP, recording every response and
   first rejection, logical/OS resources, query visibility, checkpoint and reopen.
   No limit changes in this baseline. Failed loads are not quality comparisons.
2. Reproduce the first actual refusal in a small regression before changing it.
   Audit Record/lexical/evidence limits together; distinguish population, request,
   graph traversal, candidates and physical package limits. Preserve explicit
   finite limits and pre-WAL rejection. Do not replace all constants with 100k.
3. Remove the complete-State-in-one-16MiB-frame requirement using bounded chunks
   selected by a single authenticated checkpoint certificate. Recovery reads a
   bounded stream; partial chunks never select a new authority. Retain local and
   SQLite/S3 publication, backup closure, pins and GC rules. Body/vector ownership
   remains canonical Payload; SQLite does not become a duplicated document store.
4. Account for state, receipts and builder/query transient copies explicitly.
   Preserve full operation-id retry identity/history; no expiry or deduplication
   policy changes. Keep query scoring/AND/OR, active statistics, filtering and
   global candidate windows unchanged. Avoid whole-state clones where a pinned
   immutable view or bounded stream suffices. Reject before exceeding admission.
5. Only after those invariants pass, expose a finite capacity profile and test
   10k boundary, 20k, 50k, 100k single-collection recovery/query/GC. Run Meili on
   exactly the same corpus, preprocessing and resources; report errors and recall
   separately by matching policy and query split. One million is conditional on
   observed 100k disk/RSS/latency headroom, not an automatic numeric increase.

Expected change boundary: checkpoint/journal framing and codec; Record state and
operation retention ownership; native population/admission; service capacity
configuration; targeted persistence/HTTP regressions; reproducible benchmark
scripts and evidence. No multi-engine framework, federation, model, score tuning,
new default matching policy, unbounded buffers, or broad performance project.

Decisions requiring escalation: shortening idempotency history, changing query
semantics/scoring, removing unique evidence/deployed data, adding resources/cost.
A format change needs explicit versioning/rejection and review; no silent reader
reinterpretation or migration chain. Implementation remains staged and reviewable.

Measured repair checkpoint: `37c935b` removes duplicate component lexical charge
and owns/drains queued native analysis. The unchanged sample now passes 5,249 and
reaches 8,320; next refusal is the fixed 64-Payload-object closure, not yet the
16 MiB frame. See `../benchmarks/capacity/REPAIR-01.md`. The immediate follow-up
compacts the complete referenced Payload set at that ceiling, retaining historical
operation bodies/receipts and the existing atomic anchor selection. Native private
batch draining also increases segment count: the authenticated package's 1,024-file
cap must be included in the later 100k component/segment design, not blindly raised.

Both fixes at `4451c9c` reach the existing 10,000-record ceiling: checkpoint and
reopen succeed, all 393 probes and original operation retries pass. Ordinal 10,001
rejects at the unchanged record/operation hard limit. See
`../benchmarks/capacity/REPAIR-02.md`. Steps 3–5 above remain unimplemented.

## Streaming preflight checkpoint

The first bounded-memory preparation replaces count-only checkpoint encoding and
recovery digest buffers with borrowed streaming serialization. It preserves the
existing RecordCheckpointV14 bytes, field ordering, skipped lookup cache and SHA256
identity. Catch-up compares canonical streamed SHA256 digests rather than retaining
two complete JSON buffers. No persistent format, capacity limit, history retention
or publication authority changes in this step. Record regressions: 162 passed,
0 failed, 5 existing ignored entries; log `/tmp/anyseek-capacity-stream-tests.log`.
This is not a bounded persisted snapshot or a 100k-capacity result.

The next persisted-codec change must remove the full `serde_json::Value` body
expansion in ReferencedState on both write and hydrate, not merely split the final
JSON bytes. Metadata entries must carry only authenticated Payload references;
user JSON remains opaque. Generation staging can span many batches, so one entire
generation cannot be assumed to fit a single operation-size chunk. Recovery must
admit each framed entry before decoding, retain state ownership charges, and
select all chunks only through the existing single publication certificate.
Only then can the frame/population limits be revised together with graph traversal,
physical segment counts and full-history state admission.
