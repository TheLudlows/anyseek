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
`../benchmarks/capacity/REPAIR-02.md`. At that historical checkpoint, steps 3–5 remained unimplemented; later implementation follows below.

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

## Chunked state implementation

The next anchor format is explicitly **4** (format3 is rejected, with no migration
reader). Its state descriptor selects numbered metadata chunks through the same
outer authenticated file inventory and business publication certificate. Frames
carry encoded/expanded lengths; target chunk size is4MiB, individual frame bound
16MiB and maximum chunk count256. A larger legal frame occupies its own chunk.
Record bodies, original vectors, historical request bodies and private generation
bodies remain exclusively in Payload objects. Generation metadata and each staged
member are separate entries; no whole-State JSON Value is built or hydrated.

Recovery reserves each frame's logical expanded residency before buffer allocation
and decoding, bounds each Payload read by the remaining admitted expansion, verifies
exact frame length/count and final canonical state SHA256, then runs existing full
state validation. The reservation is transferred into the resulting State. Production
State no longer implements infallible Clone: each deep copy acquires its own lease
before copying maps/history. These use the existing finite shared build governor and
32x logical JSON expansion estimate; neither is an allocator/RSS guarantee. Normal
write state growth is admitted before WAL append. Retained history is not truncated.

The initial chunked commit retained the16MiB complete-checkpoint preflight. The
following snapshot stage removes that restriction only for logical whole-state
size; actual WAL frames and reserved future business events remain bounded. The
10k record ceiling remains. Native population, graph traversal and package counts
must be coordinated before a100k profile is enabled. The351-pass all-target result
remains specific to43d789c.

Workspace plan: reserve at least2GiB free on the separate `/tmp` filesystem and
admit no more than3GiB additional temporary artifacts in this focused stage. Large
builds and fixture roots remain in `/tmp`; no corpus run, cache purge, source/data
removal or resource purchase is part of this stage. Stop admitting new work below
that free-space margin. `/tmp` is tmpfs and counts toward the16GiB cgroup limit;
reclaimable disk cache is not confused with available persistent disk capacity.

The production chunked path passed the complete local Record regression group:
166 passed,0 failed,5 existing ignored entries, exit0, log
`/tmp/anyseek-capacity-chunks-final-record.log`. This includes the existing
certificate-before/after failure, pin/GC, SQLite/S3 fixture, and cancellation
regressions, plus four new codec/ownership tests. A subsequent test-only addition
checks actual finite-budget rejection retains the old owner and contents. Full
all-targets and Windows have not yet been rerun for this new format.

Final codec tests:5 passed,0 failed, exit0,
`/tmp/anyseek-capacity-chunks-final-codec2.log`. The first version of the ownership
rejection test held a Weak handle while requesting growth: Rust's Arc::get_mut
correctly took the shared-owner reservation branch, so the expected error class
was wrong. The test now checks pointer identity without changing the ownership
branch, then creates a Weak handle to verify final release. Production code and
budget assertions were unchanged; the first failed test log is retained.

## Independent snapshot and takeover publication

Independent snapshots and takeover targets now build a complete format4 root in
a fresh private sibling directory, close all writers/mappings, remove only their
own derived caches, then publish the root directory. The owned task retains the
admission permit after a waiting caller is cancelled. Pre-publication failure
cleans only that uniquely owned private root; a rename-attempt/unknown outcome
never deletes the target. Existing destinations remain untouched. Source records,
versions, receipts, quarantine and takeover rules are unchanged.

Complete-state size is checked against the configured journal authority budget
without requiring one giant frame. Each actual WAL frame keeps its16MiB bound;
future expiration/generation event reserves must still pass the existing frame
check before a business write. No population, operation or governor limit is raised.

A real300-write fixture produces **19,884,967 canonical checkpoint bytes**, exceeding
16MiB, then publishes/reopens a snapshot and replays every original operation id
without changing either sequence. It preserves the original32KiB opaque field.
Tests also cover cancelled waiters, failure before publication, explicit simulated
unknown return after successful durable publication, and existing-target protection.
This is synthetic byte-boundary coverage, not a100k or MIRACL quality result.

Validation: default Record171 passed/0failed/5existing ignored; Catalog21/0/1;
final no-default Record151/0/5, all exit0. Logs:
`/tmp/anyseek-capacity-snapshot-record.log`,
`/tmp/anyseek-capacity-snapshot-catalog.log`,
`/tmp/anyseek-capacity-snapshot-no-default.log`. Catalog and final no-default runs
include private-cache cleanup; the default Record run preceded that small cleanup
addition. Full all-targets, HTTP, Windows and realS3/power-loss validation remain
separate outstanding items for the final implementation.

## Component residency versus transient input

The vector/metadata component now retains admission for the selected vector and
facet projection plus row/address metadata, rather than charging the entire
canonical body for its lifetime. Full input JSON still has a separate transient
lease across append awaits; State/Payload and native lexical ownership remain
separate. Variable Record identities are explicitly charged (three component
copies), without assuming a nonexistent 256-byte identity bound. No coefficient
or configured limit was reduced. This addresses allocation lifetime, not an RSS
limit or a claim of 100k support.

Targeted validation: build budget 1, view 4, native runtime 14 passed; external
vector and service metadata integration passed. Logs are
`/tmp/anyseek-capacity-component-budget2.log`,
`/tmp/anyseek-capacity-component-view.log`,
`/tmp/anyseek-capacity-component-runtime.log`, and
`/tmp/anyseek-capacity-component-integration.log`. These are focused results;
the earlier 351-test all-targets result remains evidence for 43d789c only.
