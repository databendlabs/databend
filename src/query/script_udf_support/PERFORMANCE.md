# Python scalar UDF isolation: real GCD benchmark

Measured on 2026-10-21 UTC against the same recent `main` base used by this PR.
The workload computes actual greatest common divisors, not sleeps or artificial
busy loops. Process isolation improves CPU-bound parallel execution here, but
fresh worker startup remains expensive for short queries and faster native UDFs.

## Implementation and correctness oracle

The primary UDF is the Euclidean algorithm, with two `INT64` arguments:

```python
def handler(a, b):
    a, b = abs(a), abs(b)
    while b:
        a, b = b, a % b
    return a
```

A second UDF calls the native standard-library implementation:

```python
import math

def handler(a, b):
    return math.gcd(a, b)
```

Both inputs vary for every row; SQL aggregates every result:

```sql
SELECT sum(bench_py_gcd(
    to_int64(1000000007 + 104729 * number),
    to_int64(1000000009 + 130363 * number)
)) FROM numbers(1000000);
```

The driver calculates expected sums independently with standard-library
`math.gcd`, outside the timed SQL region. Every query from both executables
returned the expected value:

| Rows/query | Expected GCD sum |
|---|---:|
| 1 | 1 |
| 1,000 | 6,002 |
| 100,000 | 600,002 |
| 1,000,000 | 6,000,002 |

Additional SQL checks passed for `(48, 18) → 6`, `(0, 27) → 27`, `(0, 0) → 0`,
`(-81, 153) → 9`, and consecutive Fibonacci numbers `(46368, 28657) → 1`.

## Setup and provenance

- Baseline: clean `main` worktree at
  `38d782fa135715b0a12034e0d6fa7dfd94b7646f`.
- Isolated executable: `26bace25c95c3f8924b49ddb0690c92136a028cb`, based directly
  on that `main`. Subsequent changes only refine the benchmark and documentation;
  they do not change the measured Rust execution path.
- Intel Core i9-12900KF, 24 logical CPUs, approximately 125 GiB RAM, Linux x86_64.
  This was a shared development host, not a dedicated benchmark machine.
- Rust `1.100.0-nightly (e457a7b0d 2026-08-27)`, repository toolchain
  `nightly-2026-08-28`; CPython 3.12.13 from the same installation for both builds.
- Both built with default binary features plus `python-udf`, release profile,
  `profile.release.lto=false`, and `profile.release.debug=0`. All other profile
  settings were unchanged. This is **not** the production ThinLTO configuration.
- Same standalone metadata server and filesystem backend, separate query-server
  lifetimes, identical configuration, `num_cpus=4`, `max_running_queries=64`,
  session `max_threads=4`, query-result caching disabled.
- Isolation limits: node 16, tenant 8, query 4; no clean-worker prewarming.
- Two passes: `main → isolated`, then `isolated → main`. Each server was restarted
  before its first Python execution; OS page caches were not flushed.
- Ten repetitions per workload per pass. Combined sequential workloads have 20
  query samples; 8-concurrent workloads have 160. First-execution results have
  only two samples and are descriptive, not meaningful percentile estimates.

Exact tested executable SHA-256 digests:

```text
main      333e5889af8528cfac35d6fd0f746bb8e7db6a04791bb8f0a00beb44ce1a532f
isolated  d3ba0e4450c7759b9ebbf802e0cf8a659adffe8fe15b51cf4f4ebad7d4862621
```

The shared build cache reused version/build-script metadata: both executables
reported the isolated revision in `version()`. Do not identify the baseline by
that string. The clean source worktree, build-log paths, and executable hashes
identify it; the baseline executable has no internal Python worker entrypoint.

## End-to-end SQL results

Timings include HTTP request/result consumption, planning, execution, Arrow IPC,
and fresh worker startup where applicable. Throughput is total rows divided by
round wall time, not the inverse of median query latency.

| UDF / workload | Rows/query | Concurrent SQL | main median ms | isolated median ms | Latency change | Throughput change |
|---|---:|---:|---:|---:|---:|---:|
| Euclidean GCD: first execution | 1 | 1 | 37.20 | 202.90 | +445.4% | -81.5% |
| Euclidean GCD: scalar | 1 | 1 | 32.33 | 197.56 | +511.1% | -83.6% |
| Euclidean GCD: small batch | 1,000 | 1 | 40.24 | 196.47 | +388.3% | -79.6% |
| Euclidean GCD: large batch | 1,000,000 | 1 | 970.54 | 842.55 | -13.2% | +17.0% |
| Euclidean GCD: concurrent batch | 100,000 | 8 | 878.13 | 283.16 | -67.8% | +205.1% |
| Euclidean GCD: concurrent large batch | 1,000,000 | 8 | 8838.02 | 1116.92 | -87.4% | +681.6% |
| Native math.gcd: large batch | 1,000,000 | 1 | 353.87 | 416.77 | +17.8% | -18.8% |
| Native math.gcd: concurrent batch | 100,000 | 8 | 409.51 | 250.14 | -38.9% | +65.6% |

For 8 concurrent million-row Euclidean queries, throughput increased from
approximately **0.878 million to 6.860 million rows/s**, or **7.82×**. This is
consistent with independent interpreters removing shared-GIL serialization, but
is not a profiler attribution: it also uses more CPU cores than the baseline.
The two individual passes measured 8.02× and 7.60× throughput, respectively.

The sequential million-row Euclidean throughput gains were 17.7% and 16.2% by
pass. Do not extrapolate them to all UDFs: native `math.gcd` was slower sequentially,
and short queries still pay roughly 160–170 ms of additional startup latency.

| Workload | main p95 ms | isolated p95 ms |
|---|---:|---:|
| Euclidean GCD: scalar | 41.89 | 203.03 |
| Euclidean GCD: large batch | 1266.54 | 847.14 |
| Euclidean GCD: concurrent large batch | 11455.65 | 1209.08 |
| Native math.gcd: large batch | 443.14 | 510.88 |
| Native math.gcd: concurrent batch | 525.11 | 283.66 |

## Memory, process limits, and boundaries

Sampled every 20 ms via `/proc`, including the query process and descendants:

| Workload | main peak summed RSS MiB | isolated peak summed RSS MiB | Peak processes: main / isolated |
|---|---:|---:|---:|
| Euclidean GCD: scalar | 218.1 | 300.0 | 1 / 2 |
| Euclidean GCD: large batch | 250.0 | 352.8 | 1 / 2 |
| Euclidean GCD: concurrent batch | 311.0 | 1139.7 | 1 / 9 |
| Euclidean GCD: concurrent large batch | 390.7 | 1305.9 | 1 / 9 |

Summed RSS double-counts shared pages; it is not PSS or unique physical memory.
The server stays alive across workloads, so allocator retention also affects
later peaks. Sampling can miss short-lived workers. These are observations, not
OS-enforced memory limits.

The worker limit controls manager-created processes, not arbitrary user-created
subprocesses. Python UDAFs remain in-process. Neither security sandboxing nor
cgroup memory accounting is provided by this change; see [README.md](README.md).

## Startup cost and next optimizations

The worker execs the full query executable. Although the hidden entrypoint skips
service initialization, it cannot skip constructors before `main`, including the
eager `BUILTIN_FUNCTIONS` registry in `src/query/functions/src/lib.rs`.
A prior diagnostic invocation that exited before Python initialization or Arrow
IPC already spent roughly 190–200 ms starting this executable. That was not an
independent profile of each constructor, but demonstrates why the entire scalar
latency penalty must not be called “IPC overhead.”

Potential follow-ups are a lightweight worker executable with a smaller runtime
dependency boundary, or a bounded inventory of clean workers that have never run
user code. Prewarming must integrate with the same physical-process quota, not
create an unbounded second pool or acquire quota twice. Do not reuse a dirty
interpreter across SQL executions merely to hide startup cost. Measure batch
transfer costs separately before introducing shared-memory IPC.

## Reproduction and evidence

See [README.md](README.md#performance-comparison) and run both servers with
identical settings, adding `--workload gcd --repetitions 10 --threads 4` to each
benchmark invocation. The standard-library-only driver records exact SQL, UDF
source, expected sums, all timing samples, medians, p95, throughput, and optional
process/RSS observations.

A compact per-pass summary is retained in
[`benchmarks/gcd-summary.csv`](benchmarks/gcd-summary.csv). Raw JSON and tested
executables remain locally under `/workspace/udf-isolation-run/`:

- `gcd-pass{1,2}-{main,isolated}.json`: all original timing samples.
- `gcd-combined-{main,isolated}.json`: combined samples used above.
- `gcd-comparison-run.log`: correctness checks and SQL regression results.
- `databend-query-gcd-{main,isolated}`: the measured executables.

Build logs are `/workspace/udf-gcd-{main,isolated}-build.log`.

Measurements of integer GCD do not characterize package-heavy imports, native
libraries that release the GIL, strings/variants, or very large Arrow values.
Full production rollout also needs the independent worker memory controls
identified in the design document.
