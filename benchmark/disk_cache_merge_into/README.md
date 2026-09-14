# Disk Cache MERGE INTO Benchmark

This benchmark reproduces the workload shape where disk cache can slow down
`MERGE INTO`:

- a wide target table with 16 non-join string columns;
- sparse source keys distributed across the complete target range;
- a matched condition that forces the full mutation RowFetch path;
- only two small columns updated, leaving the wide target payload to RowFetch;
- a separate insert-only control query without target RowFetch.

The default target contains 5 million rows. Each payload column contains 128
mostly incompressible hexadecimal characters, for approximately 10 GiB of raw
payload before Parquet compression and encoding.

## Setup

Create and select a benchmark database:

```sql
CREATE DATABASE IF NOT EXISTS disk_cache_merge_into;
USE disk_cache_merge_into;
```

Generate the data:

```bash
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/create.sql
```

For a quicker test, edit `create.sql` and use 1 million target rows plus 50,000
source rows. Keep the source modulus equal to the target row count.

## Test Matrix

Run each case after restarting `databend-query` with the corresponding cache
configuration. Use the same `max_threads` in every case.

### A. Cache Disabled

```toml
[cache]
data_cache_storage = "none"
```

```bash
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/reset.sql
time bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/merge.sql
```

### B. Disk Cache, Cold

```toml
[cache]
data_cache_storage = "disk"
data_cache_in_memory_bytes = 0

[cache.disk]
path = "/path/to/ssd/cache"
max_bytes = 21474836480
sync_data = false
```

Restart with an empty disk-cache directory, then run:

```bash
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/reset.sql
time bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/merge.sql
```

### C. Disk Cache, Warm

Restart with disk cache enabled, reset the target, warm every payload column,
and then run the MERGE:

```bash
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/reset.sql
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/warm.sql
time bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/merge.sql
```

### D. Disk Plus Memory Cache

Repeat case C with a non-zero memory tier, for example:

```toml
[cache]
data_cache_storage = "disk"
data_cache_in_memory_bytes = 4294967296
```

## RowFetch Control

After resetting the target, run the insert-only MERGE. This query has no
matched branch and should not require target RowFetch:

```bash
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/reset.sql
time bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/control_insert_only.sql
```

If matched MERGE slows down with disk cache but insert-only MERGE does not, the
result strongly points to target RowFetch and disk-cache column reads.

## Profiling

First confirm that the physical plan contains `RowFetch` and lists the 16
payload columns under `columns to fetch`:

```bash
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/plan.sql
```

Reset the target before running `EXPLAIN ANALYZE`, because a completed MERGE
changes `version` from 1 to 2:

```bash
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/reset.sql
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/explain.sql
```

Compare these profile values between cases:

- RowFetch wait time;
- bytes read from local disk;
- bytes read from remote storage;
- total MERGE elapsed time;
- `disk_cache_column_data` access, hit, and miss counts;
- SSD IOPS, average latency, queue depth, and utilization.

On Linux, collect device statistics in another terminal:

```bash
iostat -x 1
pidstat -d -p "$(pidof databend-query)" 1
```

## Cleanup

```bash
bendsql --database disk_cache_merge_into \
  < benchmark/disk_cache_merge_into/clear.sql
```
