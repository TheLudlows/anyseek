# Independent review and deadline follow-up

An independent agent (`/root/independent_review`) reviewed `bba809f` without editing
the implementation or running performance loads. It checked fence/revision checks,
publication failure, cancellation, slow clients, old responses, lifecycle ownership,
lock order and starvation. It independently reran the existing persistence-failure
and slow-client service tests. Both passed.

## Finding and resolution

**P1: header and body publication waits were not bounded by the original request
deadline.** The header middleware was outside `request_guard`; its new await was
therefore outside `timeout_at`. The body could repeatedly wait, lose the race to a
subsequent publication and wait again without a total limit. FIFO mutex fairness
does not guarantee a response-frame delivery opportunity under continuous writes.
This was a code-review finding; neither original short performance matrix observed
a timeout, so their zero-timeout count did not establish bounded waiting.

The follow-up moves the header gate inside the existing request budget. A query
response captures the same absolute deadline at construction; every body waiter
and the entire retry loop reuse it. Expiry discards undelivered bytes and produces
`io::ErrorKind::TimedOut`. A header deadline uses the existing HTTP 503
`outcome_unknown` envelope; after headers, expiry is a transport-body error and
cannot change the HTTP status. No timeout cancels an owned durable writer.

The independent reviewer rechecked this diff and found the P1 covered, with no
additional confirmed blocking defect. It also caught the test's assumption about
one layer of `axum::Error`; the test now follows the source chain to the I/O cause.

The new deterministic service test uses a 50ms request budget and paused index
publication in three cases: a completed handler blocked at header handoff, an
already-created body awaiting publication, and a body's first poll after its original
deadline. It checks timeout classification, no data delivery, continued writer
completion and successful shutdown. The late-poll case must fail immediately,
preventing a new budget from starting when the client first polls.

This bounds publication waiting, **not all network-transfer lifetime**. A normal
unconsumed response still retains its bytes, quota and shutdown activity until
consumed or dropped, as before. A waiter can finish independently while its body is
unpolled; it does not hold the publication mutex waiting for the client.

## Verification

After the fix, all **33 service tests passed** in release mode with the explicit
fault-injection feature, including the three deadline cases, real persistence/index
failure checks, cancellation, slow clients, generation handling and shutdown. The
27 unchanged engine regressions had already passed. The independent reviewer
checked the final diff and the 33-test log and reported no remaining blocking finding.
`git diff --check` also passed. No PR was opened and no branch was pushed.

Latency tables in [README.md](README.md) report successful requests separately from
all requests. The summarizer also counts **identified** timeouts and transport
errors separately: a body-stage timeout after headers can look like IncompleteRead
or a connection close, whose root cause is not identifiable from the client alone.
An unclassified transport failure is never counted as a successful request or
silently described as a proven non-timeout.

The original `paired-*` / `phased-*` evidence and first candidate's separate
backpressure timeout remain preserved. Final deadline-fixed measurements use
`deadline-paired-*` / `deadline-phased-*`; do not pool the versions or workload modes.
