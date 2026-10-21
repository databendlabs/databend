# Python UDF isolation: approximate performance

The following results compare the shared-interpreter implementation on `main`
with process-isolated Python scalar UDFs. Both versions used matching release
builds (LTO disabled), CPython 3.12, and `max_threads=4` on a shared 24-logical-CPU host.
These are indicative measurements, not production performance guarantees.

The workload computes actual greatest common divisors with two varying integer
arguments. The Python UDF uses the Euclidean algorithm; the native variant calls
`math.gcd`. Results were independently checked against standard-library
`math.gcd`. Measurements combine two passes in opposite server order.

| Workload | main latency | Isolated latency | Approximate effect |
|---|---:|---:|---|
| Python GCD, one row | 32 ms | 198 ms | About 165 ms additional startup cost |
| Python GCD, 1 million rows | 971 ms | 843 ms | About 13% lower latency |
| Python GCD, 8 concurrent queries × 1 million rows | 8.84 s | 1.12 s | About 7.8× total throughput |
| Native math.gcd, 1 million rows | 354 ms | 417 ms | About 18% higher latency |
| Native math.gcd, 8 concurrent queries × 100,000 rows | 410 ms | 250 ms | About 1.7× total throughput |

Latencies are medians per query. Throughput uses total rows divided by elapsed
round time, rather than the inverse of median latency.

## Takeaways

- Fresh-worker startup is expensive for short queries. A lightweight worker or
  bounded clean-worker prewarming could reduce this cost.
- Independent interpreters avoid shared-GIL serialization for concurrent
  CPU-bound Python. The throughput gain also uses more CPU cores; it is not a
  claim of equivalent CPU efficiency or a universal speedup.
- Fast native functions can regress when startup and IPC outweigh isolation's
  concurrency benefits.
- Processes require additional memory. In the concurrent million-row GCD test,
  peak summed RSS was roughly 391 MiB on `main` versus 1.3 GiB with isolation.
  Summed RSS double-counts shared pages and is not unique physical memory.
- Eight concurrent queries used at most eight observed workers plus the query
  process. Process-count limits do not replace OS-enforced memory limits.

For reproducible workloads and independent result checks, run
`scripts/benchmark/query/python_udf_isolation.py --workload gcd` against each
version with identical settings. See [README.md](README.md#performance-comparison)
for the comparison workflow.
