# Materialized View Planner Cache Benchmark

This benchmark measures the **planner stage** of MV-rewritten queries with a debug `databend-query` binary. It does not use client wall-clock time as the primary metric and does not include execution time in the reported planner timings.

The benchmark compares:

- `cache_disabled`: every query binds, performs MV rewrite, and optimizes;
- `cache_miss`: the first query with planner cache enabled;
- `cache_hit`: repeated queries that reuse the optimized plan.

The benchmark parses the existing `INFO` planner records:

- `Logical plan construction completed, elapsed: ...` for cache-disabled and cache-miss phases;
- `Logical plan retrieved from cache, elapsed: ...` for cache-hit phases.

It reports the total planner time in microseconds after normalizing the log duration units. No benchmark-specific logging or planner instrumentation is required.

## Run

Build a debug binary and run the benchmark from the repository root:

```bash
cargo build -p databend-binaries --bin databend-query
BUILD_PROFILE=debug ./benchmark/mv_planner_cache/run.sh
```

By default the script starts a fresh standalone deployment, which stops local `databend-query` and `databend-meta` processes and removes `.databend`. To use an already running debug standalone deployment:

```bash
START_SERVER=0 ITERATIONS=50 ./benchmark/mv_planner_cache/run.sh
```

The deployment config writes DEBUG query logs to `.databend/logs_1`. Override the location with `LOG_DIR=/path/to/query-logs` when needed.

## Interpreting results

The useful comparison is the planner `avg_us`, `p50_us`, and `p95_us` reported for:

```text
cache_disabled vs cache_miss vs cache_hit
```

`cache_hit` still includes MV dependency validation and cache lookup. The reported hit time is therefore the full planner cost of validating and retrieving a cached plan, not just the in-memory lookup.

Run at least 20 warm iterations and repeat the benchmark after restarting the debug service. Debug timings are useful for relative comparisons; they are not representative of release-mode latency.
