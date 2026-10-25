# Current capacity baseline: first measured boundary

Measured production code: merged master `34dc46fd6b6603c96beead765915e4b4fa5c2919`.
Binary SHA256: `d7ee87787902ce4fab07d925927651dc4d60b2eb89f197a7595d33dc205fbf40`.
No production/configuration limit changes, no environment budget overrides.
Host cgroup: 4 CPU / 16 GiB. Server CPUs 0–1; client 2–3. No compilation during run.

Verified all ten pinned MIRACL shards (4,934,368 documents), selected 10,001
identities including all 3,786 judged documents for 393 official dev queries.
Corpus SHA256: `c88284131f1f0daa217b7f81aef6424a69ebe34ec071ec2aec4cd640988f3ee8`.
40 prior queries and 353 held-out queries remain separately labeled. This is a
relevance-enriched boundary sample, not full-corpus quality or a Meili comparison.

- Attempted 5,250 writes in batches of 25; 5,248 applied in 148.25 seconds.
- First refused identity: ordinal 5,249 (operation nonce benchmark-00005250;
  collection creation consumes the first operation number).
- Batch envelope HTTP 200; per-item status 503, code storage_error:
  `record build resource budget exceeded: class=single requested=1173257472 limit=1073741824`.
- The 393 original-question OR diagnostic requests had zero errors. All 994
  positive query/document judgments are present (prior40: 91/91, heldout353:903/903).
- Checkpoint returns HTTP 503 with exactly the same reserve error (2.60 seconds).
- Stop/reopen takes 28.77 seconds. All 5,248 records remain visible. Repeating the
  last accepted operation returns exactly its original receipt/result; repeating
  the first rejected operation remains rejected. Visible count stays 5,248.
- Sampled peak RSS: 388,724 KiB (~379.6 MiB). Logical admission is not RSS.

The reservation is independently reproduced exactly from `record/build_budget.rs`:
4 schema fields × 65536 × 32 = 8,388,608 bytes;
2,835,920 serialized document bytes × 32 = 90,749,440 bytes;
799,196 estimated text terms × 3 × (256 + 2 lexical fields × 96) = 1,074,119,424 bytes.
Sum = **1,173,257,472 bytes**. The dominant charge is the conservative unigram/
bigram/trigram term estimate. This proves the source of the refusal, not a leak
or permission to remove protection. The first investigation must map this charge
to actual owned component/native buffers and avoid double charging retired work.

No record-hard-limit or 16 MiB snapshot refusal was reached in this run. No claim
of 10k or 100k support is made. Recovery latency and checkpoint availability are
explicit follow-up issues; the measurement did not increase any acceptance timeout.

Raw evidence (local, not a download link):
`artifacts/benchmark/capacity/baseline-default-01/` contains summary, batch events,
query responses, sampled resources, settings, logs and accepted IDs. Input manifest
and exact sample are in `artifacts/benchmark/capacity/data/`. The failed DB is
retained intact; subsequent experiments must use a new data root. Scripts are
`prepare.py` and `baseline.py`; the staged design is `docs/capacity-100k-plan.md`.
