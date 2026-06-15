# Concurrent TPC-H Workload

This suite is generated at CI runtime by:

```shell
python3 tests/sqllogictests/scripts/generate_concurrent_tpch_workload.py
```

The generated `.test` files are written to `generated/` and intentionally ignored
by git. By default the workload creates 64 worker files to match CI
`parallel=64`; each worker file runs 4 sequential rounds. This keeps the number
of HTTP sessions bounded while making every worker stay busy for several
minutes:

- 256 TPC-H Q21 executor-heavy cases.
- 256 TPC-H Q9 planner-only `EXPLAIN` cases.
- 256 actual `MERGE INTO` mutation case bundles.
- 256 actual `INSERT ALL` / `REPLACE INTO` / `INSERT OVERWRITE` case bundles.

Set `CONCURRENT_TPCH_SEED` to reproduce a failed randomized workload from CI
logs. The worker count and per-worker loop count can be overridden with
`CONCURRENT_TPCH_WORKERS` and `CONCURRENT_TPCH_ROUNDS`.
