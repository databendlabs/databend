# Python scalar UDF process-isolation measurements

Measured locally on 2026-10-21 UTC. This is a correctness-first implementation,
**not a claim of negligible performance overhead**. Fresh query-executable workers
are expensive for short SQL queries; that tradeoff remains visible in this branch.

## Setup and provenance

- Baseline source: clean `main` worktree at
  `1a90c4ae16109ca632dc99229538fd17f5ba417a`.
- Isolated source: working tree on `feat/python-udf-process-isolation`, based on
  `65137248e172e74396a74548f25895eef4e1d2d1`. The base differs from `main` only in
  two version/building files; the pre-change UDF implementation is identical.
- Intel Core i9-12900KF, 24 logical CPUs, approximately 125 GiB RAM, Linux x86_64.
  This was a shared development host, not a dedicated benchmark machine.
- Rust `1.100.0-nightly (e457a7b0d 2026-08-27)`, repository toolchain
  `nightly-2026-08-28`; CPython 3.12.13 from the same installation for both builds.
- Both built with default binary features plus `python-udf`, release profile,
  `profile.release.lto=false`, and `profile.release.debug=0`. All other profile
  settings were unchanged. This is **not** the default production ThinLTO build.
- Same standalone metadata server and filesystem backend, separate query-server
  lifetimes, identical configuration, `num_cpus=4`, `max_running_queries=64`,
  session `max_threads=4`, and query-result caching disabled.
- Isolation limits: node 16, tenant 8, query 4; no clean-worker prewarming.
- Two passes: `main → isolated`, then `isolated → main`. Each pass restarted the
  query server before its first Python execution. Each warm workload has 15
  repetitions per pass; concurrent workloads have 8 queries per repetition.
- Reported medians/p95 combine both passes: 30 samples for sequential workloads,
  240 for concurrent workloads. `cold_first` has only two samples, so it is
  descriptive, not a statistically meaningful percentile estimate.

Exact tested executable SHA-256 digests:

```text
main      33e4b6c9f839ba881c4c36bcacdf9ea7323daaa594c508e30c560965c713d50a
isolated  41fd97a317043c4f88ac2a4cfbf4eab2b5becf8fba17ecdeb6855d241e1a7143
```

The shared build cache reused version/build-script metadata: both executables
reported the same `version()` string. Do not identify the baseline by that string.
The clean worktree revision, build log source paths, and executable hashes identify
it. The baseline executable has no `--internal-python-udf-worker` entrypoint; the
isolated executable does.

## End-to-end SQL results

Measured by `scripts/benchmark/query/python_udf_isolation.py`, including HTTP
request/result consumption, planning, execution, and fresh worker startup where
applicable. Throughput is total rows divided by total round wall time, not the
inverse of median query latency.

| Workload | Rows/query | Concurrent SQL | main median ms | isolated median ms | Latency change | Throughput change |
|---|---:|---:|---:|---:|---:|---:|
| First Python execution | 1 | 1 | 40.56 | 236.79 | +483.8% | -82.7% |
| Scalar | 1 | 1 | 10.48 | 194.05 | +1752.2% | -91.4% |
| Small batch | 1,000 | 1 | 18.87 | 193.97 | +928.1% | -90.8% |
| Large batch | 1,000,000 | 1 | 194.08 | 289.91 | +49.4% | -34.9% |
| Python CPU loop | 100,000 | 1 | 208.59 | 379.10 | +81.7% | -42.6% |
| Concurrent scalar | 1 | 8 | 50.48 | 205.34 | +306.8% | -78.4% |
| Concurrent batch | 100,000 | 8 | 241.13 | 231.24 | -4.1% | +2.6% |

| Workload | main p95 ms | isolated p95 ms |
|---|---:|---:|
| Scalar | 33.25 | 202.70 |
| Small batch | 30.98 | 200.39 |
| Large batch | 261.95 | 310.57 |
| Python CPU loop | 258.28 | 396.65 |
| Concurrent scalar | 75.52 | 265.30 |
| Concurrent batch | 326.98 | 264.52 |

The batch function is `return x + 1`, aggregated with SQL `sum`. The CPU function
performs 32 iterations of a 31-bit integer recurrence per row. Both definitions
and all SQL are in the benchmark driver; both executables returned identical
results.

There is noticeable host variability: scalar medians on `main` were 25.40 ms and
8.44 ms in the two passes, while isolated medians were 193.15 ms and 195.27 ms.
Large-batch latency increases were 32.4% and 77.2% in the individual passes. Treat
small concurrent-batch gains as approximately flat performance, **not** a proven
throughput improvement; its throughput changes were +5.5% and -0.4% by pass.

## Memory and process limits

Sampled every 20 ms via `/proc`, including the query process and its descendants:

| Workload | main peak summed RSS MiB | isolated peak summed RSS MiB | Peak processes: main / isolated |
|---|---:|---:|---:|
| Scalar | 233.9 | 317.3 | 1 / 2 |
| Large batch | 265.1 | 359.5 | 1 / 2 |
| Concurrent scalar | 295.7 | 973.8 | 1 / 9 |
| Concurrent batch | 332.0 | 1080.6 | 1 / 9 |

Summed RSS double-counts shared executable/library pages. These are not PSS or
unique physical-memory measurements, nor OS-enforced limits. Short-lived workers
can be missed between samples.

A separate stress test submitted **24 simultaneous SQL queries**, each modifying
its own `sys.path` marker and sleeping inside Python. All returned the correct
marker. Peak observed process count was **9: one query server plus 8 workers**,
matching the tenant cap despite `max_running_queries=64`. Peak sampled summed RSS
was approximately 1045 MiB. Unit tests additionally exercise the node-level cap,
whole-query reservation, bounded queue, timeout, and permit retention until reaping.

## Validation alongside the benchmark

- 20 Python-enabled support-crate unit tests and the real-process test passed.
  The latter includes worker crash, cancellation, timeout, and parent SIGKILL.
- 13 support-crate tests without Python and 17 configuration tests passed.
- Clippy passed for the support crate's complete target set and the Python-enabled
  query binary; the OSS query binary checked successfully without Python.
- All three Python SQL suites passed on a standalone server and on a two-node
  cluster. The new isolation regression was also run on `main`, where it failed
  with `AssertionError` from cross-query Python state pollution, as intended.
  The distributed nested-UDF query returned the expected integer sum on three
  executions; EXPLAIN contained a Merge Exchange with two input partitions.
- Python UDAFs and OS-enforced worker memory budgets remain outside this change.

## Why the short-query overhead is large

This implementation execs the full query executable. The hidden worker entrypoint
skips service initialization, but it cannot skip executable constructors that run
**before `main`**. For example, `src/query/functions/src/lib.rs` eagerly constructs
`BUILTIN_FUNCTIONS` using `#[ctor]` in every worker.

A diagnostic invocation of worker mode with a nonexistent socket, which exits
before Python initialization or any Arrow IPC, already took roughly 190–200 ms
on this host. Therefore the roughly 180 ms scalar-query increase must **not** be
labelled “IPC overhead.” Executable startup is a major contributor. This diagnostic
was not a controlled profiler attribution of individual constructors; startup,
Python initialization, and IPC costs have not been independently decomposed.

Next optimization candidates, without sacrificing cross-SQL isolation:

1. Use a lightweight worker executable/runtime dependency boundary that avoids
   query-only eager constructors. Account for distribution and version matching.
2. Prewarm a bounded inventory of clean, never-user-executed workers. Integrate
   their physical process count with admission; do not create a second unbounded
   pool or deadlock by reserving quota twice.
3. Only consider shared-memory IPC after separately measuring batch transfer cost.

Do not reuse a user-executed interpreter across SQL queries to hide startup cost.

## Evidence and reproduction

Raw artifacts remain outside the branch at `/workspace/udf-isolation-run/`:

- `main.json`, `isolated.json`: first pass and all raw timing samples.
- `repeat-main.json`, `repeat-isolated.json`: reverse-order pass.
- `combined-main.json`, `combined-isolated.json`: combined samples used above.
- `comparison-run.log`, `repeat-comparison-run.log`, `quota-stress.json`.
- `final-standalone.log`, `cluster-test-final.log`, `cluster-explain.txt`.
- `query.toml`, `meta.toml`, and the two tested executables.

Use the commands in [README.md](README.md#performance-comparison) to repeat the
measurement. Cold startup, package-heavy UDFs, large strings/variants, and native
libraries can have different costs; this report does not generalize these simple
integer UDF measurements to all Python workloads.
