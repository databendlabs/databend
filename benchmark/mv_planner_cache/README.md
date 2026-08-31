# Materialized View Planner Cache Benchmark

This benchmark measures the **planner stage** of MV-rewritten queries with a debug `databend-query` binary. It does not use client wall-clock time as the primary metric and does not include execution time in the reported planner timings.

The benchmark compares:

- `cache_disabled`: every query binds, performs MV rewrite, and optimizes;
- `cache_miss`: the first query with planner cache enabled;
- `cache_hit`: repeated queries that reuse the optimized plan.

The query log records these stages in microseconds:

- cache miss/disabled: `cache_context_us`, `bind_us`, `optimize_us`, `total_us`;
- cache hit: `cache_context_us`, `cache_lookup_us`, `total_us`.

## Run

Build a debug binary and run the benchmark from the repository root:

```bash
cargo build -p databend-query
BUILD_PROFILE=debug ./benchmark/mv_planner_cache/run.sh
```

By default the script starts a fresh standalone deployment, which stops local `databend-query` and `databend-meta` processes and removes `.databend`. To use an already running debug standalone deployment:

```bash
START_SERVER=0 ITERATIONS=50 ./benchmark/mv_planner_cache/run.sh
```

The deployment config writes DEBUG query logs to `.databend/logs_1`. Override the location with `LOG_DIR=/path/to/query-logs` when needed.

## Interpreting results

The useful comparison is the planner `total_us` median/p95:

```text
cache_disabled vs cache_hit
```

`cache_hit` still includes MV dependency fingerprint collection and cache lookup. The net planner benefit is therefore the cost of the full cache hit path, not just the in-memory lookup. Compare `avg_cache_context_us` with the bind and optimize costs to see whether metadata validation dominates the benefit.

Run at least 20 warm iterations and repeat the benchmark after restarting the debug service. Debug timings are useful for relative comparisons and stage attribution; they are not representative of release-mode latency.
