# Security Policy Benchmark

Local benchmark for measuring Row Access Policy (RAP) and Data Mask Policy
performance overhead in Databend.

## What It Measures

1. **RAP vs WHERE**: Overhead of row access policy (auto prewhere pushdown) compared
   to an equivalent hand-written WHERE clause.
2. **Mask vs Function**: Overhead of masking policy (auto function replacement) compared
   to an equivalent hand-written function call in SELECT.
3. **Planner Cache**: Cold vs warm plan cache latency with and without security policies.

## Usage

### Against a running Databend instance

```bash
# Default: 5M rows, all complexity levels, 5 bench rounds
python3 scripts/security_policy/security_policy_bench.py \
    --dsn "databend://root@127.0.0.1:8000/default?sslmode=disable" \
    --output tmp/security_policy_bench_result.json

# Quick run: single complexity, fewer rounds
python3 scripts/security_policy/security_policy_bench.py \
    --complexity simple --bench-rounds 3 --output tmp/quick.json

# Dry run: print generated SQL without executing
python3 scripts/security_policy/security_policy_bench.py --dry-run
```

### With a specific binary (auto start/stop services)

```bash
python3 scripts/security_policy/run_security_policy_bench_with_bin.py \
    --query-bin target/release/databend-query \
    --output tmp/result.json
```

### Cross-version A/B comparison

```bash
python3 scripts/security_policy/run_security_policy_bench_with_bin.py \
    --query-bin target/release/databend-query \
    --compare-bin /path/to/other/databend-query \
    --output tmp/result.json
```

This runs the benchmark on both binaries and outputs a diff report. Regressions
(>5% slower) are flagged.

## CLI Arguments

### security_policy_bench.py

| Argument | Default | Description |
|----------|---------|-------------|
| `--dsn` | env `BENDSQL_DSN` | bendsql connection string |
| `--rows` | 5,000,000 | Total rows to insert |
| `--rows-per-batch` | 1,000,000 | Rows per INSERT batch |
| `--rows-per-block` | 10,000 | ROW_PER_BLOCK table option |
| `--blocks-per-segment` | 10 | BLOCK_PER_SEGMENT table option |
| `--complexity` | all | RAP/mask complexity: simple, range, complex, all |
| `--pattern` | all | Query pattern filter |
| `--warmup-rounds` | 2 | Warmup iterations before measurement |
| `--bench-rounds` | 5 | Measurement iterations |
| `--output` | none | JSON output path |
| `--keep` | false | Don't drop database before run |
| `--drop-after` | false | Drop database after run |
| `--dry-run` | false | Print SQL without executing |

### run_security_policy_bench_with_bin.py

| Argument | Default | Description |
|----------|---------|-------------|
| `--query-bin` | required | Path to databend-query binary |
| `--meta-bin` | auto-detect | Path to databend-meta binary |
| `--compare-bin` | none | Second binary for A/B comparison |
| `--bench-arg` | none | Extra args passed to bench script (repeatable) |
| `--no-stop` | false | Leave services running after bench |

## Metrics

### Primary metrics (per query)

- `duration_ms`: Best-of-N wall time (ms)
- `duration_avg_ms`: Average wall time across rounds
- `all_ms`: All individual round timings

### Pruning stats (RAP queries)

- `segments_before/after`: Segment range pruning
- `blocks_before/after`: Block range pruning
- `*_pruned_pct`: Percentage pruned

### Summary output

- `rap_vs_where.overhead_pct`: Policy overhead relative to manual WHERE
- `mask_vs_func.overhead_pct`: Policy overhead relative to manual function

## Test Table

```sql
CREATE TABLE bench_security (
    id         INT NOT NULL,
    tenant_id  INT NOT NULL,
    email      VARCHAR NOT NULL,
    amount     DECIMAL(18,2) NOT NULL,
    status     VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
) CLUSTER BY (created_at);
```

- Clustered by time (realistic production pattern)
- 100 distinct tenant_id values scattered across all blocks
- RAP predicates on tenant_id force prewhere filtering (no cluster pruning shortcut)

## Policy Complexity Levels

| Level | RAP predicate | Mask expression |
|-------|--------------|-----------------|
| simple | `tenant_id = 1` | `'***'` |
| range | `tenant_id BETWEEN 10 AND 30` | `CONCAT(LEFT(val,3), '***@***.com')` |
| complex | `tenant_id % 10 = 0 AND status = 'active'` | `CASE WHEN ... END` |
