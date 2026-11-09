# Benchmark code and experiment evidence

Experiment directories keep their own workloads, correctness checks and historical
results. Common execution code lives here:

- `shared_query_compare.py`: seed-copy verification and ABBA process scheduling.
  `query-cost/compare.py` retains one ABBA group by default;
  `query-alignment/allocation_compare.py` retains three groups with isolated child
  resource sampling. Both accept `--rounds` and `--[no-]resources`. Existing result
  directories are refused; failed-run databases and prior results are retained.
- `resource_run.py`: fresh-process CPU/RSS wrapper for diagnostic probes.
- `shared_record_http.py`: disposable Record server lifecycle, transport, bounded
  explicit Busy retries, receipt consistency and file hashes. Query alignment and
  write-cost share it; workload generation, public-cursor checks and sampling
  remain in their experiment entry points. Allocation preload paths remain local
  to the caller's experiment. Query alignment retains thread-local HTTP status.
- `shared_http_engine.py`: the existing capacity/tokenizer engine abstraction,
  including cross-engine configuration. It has a different API and workload
  contract; it is not folded into the Record transport.

Run helper regressions with `python3 benchmarks/test_shared_harness.py` (Linux).
These exercise ABBA defaults, resource sidecars, failed-run retention and retry
safety; they do not measure server performance. Clean timing and allocator
instrumentation remain distinct. Refactoring runners does not regenerate or
rewrite historical results; keep each experiment's recorded source/binary hashes.
