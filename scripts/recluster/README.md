# Recluster Quality Benchmark

This directory contains local tools for measuring recluster quality. They are
intended for development and investigation, not for CI.

The benchmark focuses on data order quality rather than runtime. Use prune
statistics as a secondary signal because they also depend on block count,
segment count, predicate shape, and block boundary alignment.

## Tools

- `recluster_bench.py`: creates synthetic clustered tables, runs recluster
  rounds, and records clustering quality after each round.
- `run_recluster_bench_with_bin.py`: starts `databend-meta` plus a selected
  `databend-query` binary, runs `recluster_bench.py`, and shuts services down.

Both scripts require `bendsql` in `PATH` unless `--bendsql` is provided.

## Primary Metrics

Use these metrics to compare recluster algorithms:

- `average_depth`: Databend's `clustering_information` average depth.
- `average_overlaps`: Databend's `clustering_information` average overlaps.
- `avg_depth_by_span`: benchmark-computed interval depth weighted by key span.
- `p90_depth_by_span`, `p99_depth_by_span`, `max_depth_by_span`: tail overlap
  quality.
- `p50_block_width`, `p90_block_width`: resulting block key range width.

Optional `EXPLAIN` prune stats can be collected with `--explain-prune`, but
they should not be treated as the only recluster quality metric.

## Example

Build or copy a `databend-query` binary, then run:

```bash
python3 scripts/recluster/run_recluster_bench_with_bin.py \
  --query-bin tmp/main/databend-query \
  --output tmp/main-tiered-prune-run1.json \
  --bench-arg '--case tiered_overlap' \
  --bench-arg '--rows-per-batch 50000' \
  --bench-arg '--batches 240' \
  --bench-arg '--waves 6' \
  --bench-arg '--recluster-per-wave 10' \
  --bench-arg '--recluster-rounds 12' \
  --bench-arg '--stop-at-depth 0' \
  --bench-arg '--row-per-block 10000' \
  --bench-arg '--block-per-segment 10' \
  --bench-arg '--max-threads 1' \
  --bench-arg '--explain-prune' \
  --bench-arg '--prune-samples 20' \
  --bench-arg '--prune-window 10000'
```

The output JSON contains per-round states and, when requested, sampled prune
statistics from `EXPLAIN`.

## Notes

- Keep outputs under `tmp/`; do not commit generated JSON or copied binaries.
- Prefer comparing final depth metrics and per-round convergence curves.
- Use fixed benchmark arguments when comparing binaries.
