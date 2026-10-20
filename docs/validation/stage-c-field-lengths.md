# Stage C validation

Same corrected C binary for Shared and PerField; original text and native normalization in both arms. No dev tuning; dev was previously exposed. Shared remains the default. #35 budget unresolved; #42 realtime proximity limitation remains.

| Normalization | Policy | Dev nDCG@10 | MRR@10 | Recall@100 |
|---|---|---:|---:|---:|
| raw | Shared | 0.366805 | 0.412700 | 0.727478 |
| raw | PerField | 0.399525 | 0.441130 | 0.745156 |
| t2s | Shared | 0.409087 | 0.473766 | 0.781776 |
| t2s | PerField | 0.456273 | 0.515689 | 0.801687 |

Four paired rounds, fixed train validation256, C1/C4. Median across rounds; all ratios/intervals retained in JSON.

| Normalization | C | Policy | QPS | p50 ms | p95 ms | p99 ms | server CPU ms/query |
|---|---:|---|---:|---:|---:|---:|---:|
| raw | 1 | Shared | 203.31 | 4.569 | 8.827 | 12.355 | 3.965 |
| raw | 1 | PerField | 201.82 | 4.521 | 9.570 | 13.223 | 4.062 |
| raw | 4 | Shared | 505.27 | 7.076 | 15.395 | 20.266 | 4.531 |
| raw | 4 | PerField | 484.82 | 7.298 | 16.475 | 22.990 | 4.746 |
| t2s | 1 | Shared | 189.86 | 4.777 | 10.751 | 14.616 | 4.238 |
| t2s | 1 | PerField | 197.53 | 4.787 | 10.010 | 14.348 | 4.238 |
| t2s | 4 | Shared | 485.15 | 7.280 | 16.305 | 21.689 | 4.805 |
| t2s | 4 | PerField | 483.42 | 7.508 | 15.767 | 21.072 | 4.824 |

Construction cost is a separate 1000-document raw fixture, three alternating pairs using the same binary; it is a small-sample estimate with coarse CPU accounting, not a 300k paired build benchmark. Full PerField build timing and resource peaks are retained separately.

- Shared: median wall 0.1606s, server CPU 0.1500s, index bytes 1328186.
- PerField: median wall 0.1769s, server CPU 0.1700s, index bytes 1328222.

All 19 selected regressions passed (14 library, four lexical integration tests each exercising Shared/PerField, one realtime boost test); no ignored assertions. Fixed dev checks: 1,572 default/explicit score comparisons and 64 full-candidate comparisons. Performance: 8,192 timed requests plus 1,024 warmups, zero errors or anchor changes. The 21 reused Shared index files retained their original hashes.

Performance has substantial round-to-round variation; the four-pair bootstrap intervals in JSON are descriptive and wide. The medians do not establish a general speed improvement or a production acceptance decision. #35 remains unresolved.

The initial serial build completed raw300k, then the 14GiB guard stopped t2s at a persisted 24,700 documents (sampled peak14.0026GiB). All 24,700 stored original documents and unique IDs were verified before resuming in a fresh process, batch100 with each batch drained before the next. The remaining275,300 completed with peak13.9332GiB and no guard. No historical files were deleted and no limits were raised. Full PerField index bytes:361,641,340. Query quality peak13.7521GiB; performance peak13.2502GiB. These are shared-container cgroup measurements, not isolated engine allocations.

Measured code commit: `db03d2364672b0e3af678f40d75c490fd4b0361c`. Frozen server SHA-256: `5ecda0e329665ef03725812d97f0ca21bc3d6e3e1c24c08f1a80bf66547a1d36`. Later commits contain validation evidence only. The statistical definition matches the earlier experiment, but all reported C measurements were rerun with the maintained implementation and necessary correctness fixes.
