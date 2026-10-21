# Python UDF isolation: approximate performance

This comparison uses a single SQL query computing Euclidean GCD for **5 million
rows**, with both arguments varying per row. Results were independently checked
with standard-library `math.gcd`; there is no artificial busy loop or sleep.

Both versions used matching release builds (LTO disabled), CPython 3.12, query
result caching disabled, and identical configuration on a shared 24-logical-CPU
host. The isolated node capacity was 16 workers. Medians combine two passes in
opposite server order, with ten samples per case. These are indicative local
measurements, not production guarantees.

| `max_threads` | Shared interpreter (`main`) | Isolated worker pool | Relative to main | Observed workers |
|---:|---:|---:|---:|---:|
| 1 | 5.05 s | 6.43 s | About 27% slower | 1 |
| 4 | 5.96 s | 1.90 s | About 3.1× faster | 4 |
| 8 | 6.79 s | 1.75 s | About 3.9× faster | 8 |

## Takeaways

- This is **single-query parallelism**, not eight independent SQL queries. The
  isolated implementation improves by about 3.7× from one to eight threads.
  Separate interpreters remove shared-GIL serialization while using more cores;
  this is not a claim of equal CPU efficiency or universal linear scaling.
  Shared-host variability is substantial (eight-thread p95 was about 8.22 s on
  main and 2.15 s with isolation); do not treat these ratios as stable guarantees.
- A scalar query still starts only one worker, including at `max_threads=8`.
  Fresh-process startup and IPC remain significant costs for short queries.
- Worker processes consume additional memory outside the query allocator. CPU
  sizing and process-count quotas do not replace OS-enforced memory limits.
- Auto-sizing and budget accounting for 64/128 CPUs have unit coverage. This
  host does not have 128 CPUs; no 64/128-core performance claim is made here.

To repeat the single-query comparison, run
`scripts/benchmark/query/python_udf_isolation.py --workload gcd-scaling --threads N`
for `N=1,4,8` against each version with identical settings. `--gcd-rows` controls
the input size. See [README.md](README.md#performance-comparison) for the workflow
and worker-capacity configuration.
