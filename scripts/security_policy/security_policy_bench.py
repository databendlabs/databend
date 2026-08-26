#!/usr/bin/env python3
"""
Security policy (RAP / Data Mask) benchmark.

Measures performance overhead of row access policies and data masking policies
compared to equivalent manual WHERE clauses and function calls.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.bench_common.bendsql import BendSQL, QueryResult, csv_records, kv_map


# ---------------------------------------------------------------------------
# Policy and query definitions
# ---------------------------------------------------------------------------

RAP_POLICIES: dict[str, dict[str, str]] = {
    "simple": {
        "body": "(tenant_id INT, status VARCHAR) RETURNS BOOLEAN -> tenant_id = 1",
        "columns": "(tenant_id, status)",
    },
    "range": {
        "body": "(tenant_id INT, status VARCHAR) RETURNS BOOLEAN -> tenant_id BETWEEN 10 AND 30",
        "columns": "(tenant_id, status)",
    },
    "complex": {
        "body": "(tenant_id INT, status VARCHAR) RETURNS BOOLEAN -> tenant_id % 10 = 0 AND status = 'active'",
        "columns": "(tenant_id, status)",
    },
}

MASK_POLICIES: dict[str, dict[str, str]] = {
    "simple": {
        "body": "(val STRING) RETURNS STRING -> '***'",
        "column": "email",
    },
    "range": {
        "body": "(val STRING) RETURNS STRING -> CONCAT(LEFT(val, 3), '***@***.com')",
        "column": "email",
    },
    "complex": {
        "body": "(val STRING) RETURNS STRING -> CASE WHEN LENGTH(val) > 5 THEN CONCAT(LEFT(val, 2), REPEAT('*', LENGTH(val) - 4), RIGHT(val, 2)) ELSE '***' END",
        "column": "email",
    },
}

def rap_where_equivalent(complexity: str) -> str:
    mapping = {
        "simple": "tenant_id = 1",
        "range": "tenant_id BETWEEN 10 AND 30",
        "complex": "tenant_id % 10 = 0 AND status = 'active'",
    }
    return mapping[complexity]


def mask_func_equivalent(complexity: str) -> str:
    mapping = {
        "simple": "'***' AS email",
        "range": "CONCAT(LEFT(email, 3), '***@***.com') AS email",
        "complex": "CASE WHEN LENGTH(email) > 5 THEN CONCAT(LEFT(email, 2), REPEAT('*', LENGTH(email) - 4), RIGHT(email, 2)) ELSE '***' END AS email",
    }
    return mapping[complexity]


def query_patterns(table: str, complexity: str) -> list[dict[str, str]]:
    where_clause = rap_where_equivalent(complexity)
    return [
        {
            "name": "point",
            "rap": f"SELECT id, email, amount FROM {table} LIMIT 50000",
            "where": f"SELECT id, email, amount FROM {table} WHERE {where_clause} LIMIT 50000",
        },
        {
            "name": "range_scan",
            "rap": f"SELECT id, email FROM {table}",
            "where": f"SELECT id, email FROM {table} WHERE {where_clause}",
        },
        {
            "name": "order_limit",
            "rap": f"SELECT id, email, amount FROM {table} ORDER BY created_at DESC LIMIT 100",
            "where": f"SELECT id, email, amount FROM {table} WHERE {where_clause} ORDER BY created_at DESC LIMIT 100",
        },
        {
            "name": "agg",
            "rap": f"SELECT COUNT(*), SUM(amount) FROM {table}",
            "where": f"SELECT COUNT(*), SUM(amount) FROM {table} WHERE {where_clause}",
        },
    ]


def mask_query_patterns(table: str, complexity: str) -> list[dict[str, str]]:
    func_expr = mask_func_equivalent(complexity)
    return [
        {
            "name": "full_scan",
            "mask": f"SELECT email, id FROM {table} LIMIT 50000",
            "func": f"SELECT {func_expr}, id FROM {table} LIMIT 50000",
        },
        {
            "name": "full_scan_large",
            "mask": f"SELECT email FROM {table}",
            "func": f"SELECT {func_expr} FROM {table}",
        },
    ]


# ---------------------------------------------------------------------------
# Metrics collection
# ---------------------------------------------------------------------------

def collect_query_metrics(bs: BendSQL, query_id: str) -> dict[str, Any]:
    sql = f"""
SELECT query_duration_ms, scan_rows, scan_bytes, scan_partitions,
       total_partitions, result_rows, result_bytes
FROM system.query_log
WHERE query_id = '{query_id}' AND exception_code = 0
ORDER BY event_time DESC LIMIT 1
"""
    res = bs.try_run(sql, f"metrics-{query_id[:8]}", echo_stdout=False)
    rows = csv_records(res.stdout)
    if not rows:
        return {}
    r = rows[0]
    if len(r) < 7:
        return {}
    return {
        "duration_ms": float(r[0]),
        "scan_rows": int(r[1]),
        "scan_bytes": int(r[2]),
        "scan_partitions": int(r[3]),
        "total_partitions": int(r[4]),
        "result_rows": int(r[5]),
        "result_bytes": int(r[6]),
    }


def parse_pruning_stats(explain_output: str) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    seg_match = __import__("re").search(
        r"segments:\s*<range pruning:\s*(\d+)\s*to\s*(\d+)", explain_output
    )
    if seg_match:
        before, after = int(seg_match.group(1)), int(seg_match.group(2))
        stats["segments_before"] = before
        stats["segments_after"] = after
        stats["segments_pruned_pct"] = round((before - after) / before * 100, 2) if before > 0 else 0

    block_match = __import__("re").search(
        r"blocks:\s*<\s*range pruning:\s*(\d+)\s*to\s*(\d+)", explain_output
    )
    if block_match:
        before, after = int(block_match.group(1)), int(block_match.group(2))
        stats["blocks_before"] = before
        stats["blocks_after"] = after
        stats["blocks_pruned_pct"] = round((before - after) / before * 100, 2) if before > 0 else 0

    return stats


def collect_cache_stats(bs: BendSQL) -> dict[str, dict[str, int]]:
    res = bs.try_run(
        "SELECT name, num_items, size, access, hit, miss FROM system.caches",
        "caches", echo_stdout=False,
    )
    out: dict[str, dict[str, int]] = {}
    for row in csv_records(res.stdout):
        if len(row) >= 6:
            out[row[0]] = {
                "num_items": int(row[1]),
                "size": int(row[2]),
                "access": int(row[3]),
                "hit": int(row[4]),
                "miss": int(row[5]),
            }
    return out


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------

def create_table_sql(database: str, table: str, row_per_block: int, block_per_segment: int) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {database}.{table} (
    id         INT NOT NULL,
    tenant_id  INT NOT NULL,
    email      VARCHAR NOT NULL,
    amount     DECIMAL(18,2) NOT NULL,
    status     VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL
)
CLUSTER BY (created_at)
ROW_PER_BLOCK = {row_per_block}
BLOCK_PER_SEGMENT = {block_per_segment}
"""


def insert_sql(database: str, table: str, batch: int, rows_per_batch: int) -> str:
    offset = batch * rows_per_batch
    return f"""
INSERT INTO {database}.{table}
SELECT
    (number + {offset})::INT AS id,
    (number % 100)::INT AS tenant_id,
    CONCAT('user', to_string(number + {offset}), '@example.com') AS email,
    ((number % 10000) * 1.5)::DECIMAL(18,2) AS amount,
    CASE WHEN number % 3 = 0 THEN 'active' WHEN number % 3 = 1 THEN 'inactive' ELSE 'pending' END AS status,
    to_timestamp({offset} + number) AS created_at
FROM numbers({rows_per_batch})
"""


# ---------------------------------------------------------------------------
# Benchmark execution
# ---------------------------------------------------------------------------

def run_query_timed(bs: BendSQL, sql: str, label: str, rounds: int) -> tuple[list[float], list[float]]:
    """Returns (client_times_sec, server_times_sec)."""
    client_times = []
    server_times = []
    for i in range(rounds):
        res = bs.run(sql, f"{label}-r{i}", output="null", echo_stdout=False)
        client_times.append(res.elapsed_sec)
        # Get server-side time via --time=server
        if not bs.dry_run:
            srv = bs.run_time_server(sql, f"{label}-srv-r{i}")
            if srv is not None:
                server_times.append(srv)
    return client_times, server_times


def bench_rap(
    bs: BendSQL,
    database: str,
    table: str,
    complexity: str,
    warmup: int,
    rounds: int,
) -> list[dict[str, Any]]:
    full_table = f"{database}.{table}"
    policy_name = f"bench_rap_{complexity}"
    policy_def = RAP_POLICIES[complexity]
    results = []

    patterns = query_patterns(full_table, complexity)
    for pat in patterns:
        # Mode: where (baseline)
        run_query_timed(bs, pat["where"], f"warmup-where-{complexity}-{pat['name']}", warmup)
        client_times, server_times = run_query_timed(bs, pat["where"], f"where-{complexity}-{pat['name']}", rounds)
        metrics: dict[str, Any] = {
            "duration_ms": round(min(client_times) * 1000, 2),
            "duration_avg_ms": round(sum(client_times) / len(client_times) * 1000, 2),
            "all_ms": [round(t * 1000, 2) for t in client_times],
        }
        if server_times:
            metrics["server_min_ms"] = round(min(server_times) * 1000, 2)
            metrics["server_avg_ms"] = round(sum(server_times) / len(server_times) * 1000, 2)
        results.append({
            "complexity": complexity,
            "pattern": pat["name"],
            "mode": "where",
            "cache": "warm",
            "metrics": metrics,
        })

        # Mode: rap
        bs.try_run(
            f"DROP ROW ACCESS POLICY IF EXISTS {policy_name}",
            f"drop-rap-pre-{complexity}", output="null",
        )
        bs.run(
            f"CREATE ROW ACCESS POLICY {policy_name} AS {policy_def['body']}",
            f"create-rap-{complexity}", output="null",
        )
        bs.run(
            f"ALTER TABLE {full_table} ADD ROW ACCESS POLICY {policy_name} ON {policy_def['columns']}",
            f"attach-rap-{complexity}", output="null",
        )

        run_query_timed(bs, pat["rap"], f"warmup-rap-{complexity}-{pat['name']}", warmup)
        client_times, server_times = run_query_timed(bs, pat["rap"], f"rap-{complexity}-{pat['name']}", rounds)
        metrics = {
            "duration_ms": round(min(client_times) * 1000, 2),
            "duration_avg_ms": round(sum(client_times) / len(client_times) * 1000, 2),
            "all_ms": [round(t * 1000, 2) for t in client_times],
        }
        if server_times:
            metrics["server_min_ms"] = round(min(server_times) * 1000, 2)
            metrics["server_avg_ms"] = round(sum(server_times) / len(server_times) * 1000, 2)
        results.append({
            "complexity": complexity,
            "pattern": pat["name"],
            "mode": "rap",
            "cache": "warm",
            "metrics": metrics,
        })

        # Collect EXPLAIN pruning stats for the rap query
        explain_res = bs.try_run(
            f"EXPLAIN ANALYZE {pat['rap']}", f"explain-rap-{complexity}-{pat['name']}",
            echo_stdout=False,
        )
        pruning = parse_pruning_stats(explain_res.stdout)
        if pruning:
            results[-1]["pruning"] = pruning

        bs.run(
            f"ALTER TABLE {full_table} DROP ROW ACCESS POLICY {policy_name}",
            f"drop-rap-{complexity}-{pat['name']}", output="null",
        )

    return results


def bench_mask(
    bs: BendSQL,
    database: str,
    table: str,
    complexity: str,
    warmup: int,
    rounds: int,
) -> list[dict[str, Any]]:
    full_table = f"{database}.{table}"
    policy_name = f"bench_mask_{complexity}"
    mask_def = MASK_POLICIES[complexity]
    results = []

    patterns = mask_query_patterns(full_table, complexity)
    for pat in patterns:
        # Mode: func (baseline)
        run_query_timed(bs, pat["func"], f"warmup-func-{complexity}-{pat['name']}", warmup)
        client_times, server_times = run_query_timed(bs, pat["func"], f"func-{complexity}-{pat['name']}", rounds)
        metrics: dict[str, Any] = {
            "duration_ms": round(min(client_times) * 1000, 2),
            "duration_avg_ms": round(sum(client_times) / len(client_times) * 1000, 2),
            "all_ms": [round(t * 1000, 2) for t in client_times],
        }
        if server_times:
            metrics["server_min_ms"] = round(min(server_times) * 1000, 2)
            metrics["server_avg_ms"] = round(sum(server_times) / len(server_times) * 1000, 2)
        results.append({
            "complexity": complexity,
            "pattern": pat["name"],
            "mode": "func",
            "cache": "warm",
            "metrics": metrics,
        })

        # Mode: mask
        bs.try_run(
            f"DROP MASKING POLICY IF EXISTS {policy_name}",
            f"drop-mask-pre-{complexity}", output="null",
        )
        bs.run(
            f"CREATE MASKING POLICY {policy_name} AS {mask_def['body']}",
            f"create-mask-{complexity}", output="null",
        )
        bs.run(
            f"ALTER TABLE {full_table} MODIFY COLUMN {mask_def['column']} SET MASKING POLICY {policy_name}",
            f"attach-mask-{complexity}", output="null",
        )

        run_query_timed(bs, pat["mask"], f"warmup-mask-{complexity}-{pat['name']}", warmup)
        client_times, server_times = run_query_timed(bs, pat["mask"], f"mask-{complexity}-{pat['name']}", rounds)
        metrics = {
            "duration_ms": round(min(client_times) * 1000, 2),
            "duration_avg_ms": round(sum(client_times) / len(client_times) * 1000, 2),
            "all_ms": [round(t * 1000, 2) for t in client_times],
        }
        if server_times:
            metrics["server_min_ms"] = round(min(server_times) * 1000, 2)
            metrics["server_avg_ms"] = round(sum(server_times) / len(server_times) * 1000, 2)
        results.append({
            "complexity": complexity,
            "pattern": pat["name"],
            "mode": "mask",
            "cache": "warm",
            "metrics": metrics,
        })

        bs.run(
            f"ALTER TABLE {full_table} MODIFY COLUMN {mask_def['column']} UNSET MASKING POLICY",
            f"drop-mask-{complexity}-{pat['name']}", output="null",
        )

    return results


def bench_planner_cache(
    bs: BendSQL,
    database: str,
    table: str,
    complexity: str,
    rounds: int,
) -> list[dict[str, Any]]:
    full_table = f"{database}.{table}"
    policy_name = f"bench_rap_cache_{complexity}"
    policy_def = RAP_POLICIES[complexity]
    results = []

    test_sql = f"SELECT COUNT(*) FROM {full_table}"

    # Without policy: cold vs warm (baseline)
    bs.run("SET enable_planner_cache = 1", "cache-enable", output="null")

    cold = bs.run(test_sql, "cache-cold-no-policy", output="null", echo_stdout=False)
    warm_times, _ = run_query_timed(bs, test_sql, "cache-warm-no-policy", rounds)
    results.append({
        "complexity": complexity,
        "pattern": "planner_cache",
        "mode": "where",
        "cache": "cold",
        "metrics": {"duration_ms": round(cold.elapsed_sec * 1000, 2)},
    })
    results.append({
        "complexity": complexity,
        "pattern": "planner_cache",
        "mode": "where",
        "cache": "warm",
        "metrics": {
            "duration_ms": round(min(warm_times) * 1000, 2),
            "duration_avg_ms": round(sum(warm_times) / len(warm_times) * 1000, 2),
        },
    })

    # With RAP policy: 3-layer cache isolation
    # Layer 1: both_cold - first query after fresh policy attach (policy cache miss + planner cache miss)
    # Layer 2: policy_warm_planner_cold - different SQL text forces planner miss, policy cache hit
    # Layer 3: both_warm - same SQL repeated (both caches hit)

    # Fresh policy: drop and recreate to ensure policy cache miss
    bs.try_run(
        f"DROP ROW ACCESS POLICY IF EXISTS {policy_name}",
        "cache-drop-rap-pre", output="null",
    )
    bs.run(
        f"CREATE ROW ACCESS POLICY {policy_name} AS {policy_def['body']}",
        "cache-create-rap", output="null",
    )
    bs.run(
        f"ALTER TABLE {full_table} ADD ROW ACCESS POLICY {policy_name} ON {policy_def['columns']}",
        "cache-attach-rap", output="null",
    )

    # Layer 1: both_cold (first query - policy cache miss + planner cache miss)
    # Use SQL variant A
    sql_a = f"SELECT COUNT(*) FROM {full_table}"
    both_cold = bs.run(sql_a, "cache-both-cold", output="null", echo_stdout=False)
    results.append({
        "complexity": complexity,
        "pattern": "planner_cache",
        "mode": "rap",
        "cache": "both_cold",
        "metrics": {"duration_ms": round(both_cold.elapsed_sec * 1000, 2)},
    })

    # Layer 2: policy_warm_planner_cold
    # Different SQL text → planner cache miss; same policy_id → policy cache hit
    sql_b = f"SELECT COUNT(1) FROM {full_table}"
    policy_warm = bs.run(sql_b, "cache-policy-warm-planner-cold", output="null", echo_stdout=False)
    results.append({
        "complexity": complexity,
        "pattern": "planner_cache",
        "mode": "rap",
        "cache": "policy_warm",
        "metrics": {"duration_ms": round(policy_warm.elapsed_sec * 1000, 2)},
    })

    # Layer 3: both_warm (repeat sql_a - both caches hit from Layer 1)
    warm_times, _ = run_query_timed(bs, sql_a, "cache-both-warm", rounds)
    results.append({
        "complexity": complexity,
        "pattern": "planner_cache",
        "mode": "rap",
        "cache": "both_warm",
        "metrics": {
            "duration_ms": round(min(warm_times) * 1000, 2),
            "duration_avg_ms": round(sum(warm_times) / len(warm_times) * 1000, 2),
        },
    })

    bs.run(
        f"ALTER TABLE {full_table} DROP ROW ACCESS POLICY {policy_name}",
        "cache-drop-rap", output="null",
    )

    return results


# ---------------------------------------------------------------------------
# Summary computation
# ---------------------------------------------------------------------------

def compute_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    rap_vs_where: list[dict[str, Any]] = []
    mask_vs_func: list[dict[str, Any]] = []
    cache_results: list[dict[str, Any]] = []

    by_key: dict[tuple, dict] = {}
    for r in results:
        key = (r["complexity"], r["pattern"], r["mode"], r["cache"])
        by_key[key] = r

    complexities = {r["complexity"] for r in results}
    patterns = {r["pattern"] for r in results}

    for c in sorted(complexities):
        for p in sorted(patterns):
            if p == "planner_cache":
                # Planner cache: output 3-layer comparison
                for mode in ("where",):
                    cold_r = by_key.get((c, p, mode, "cold"))
                    warm_r = by_key.get((c, p, mode, "warm"))
                    if cold_r and warm_r:
                        cold_ms = cold_r["metrics"]["duration_ms"]
                        warm_ms = warm_r["metrics"]["duration_ms"]
                        speedup = ((cold_ms - warm_ms) / cold_ms * 100) if cold_ms > 0 else 0
                        cache_results.append({
                            "complexity": c, "mode": mode, "layer": "planner",
                            "cold_ms": cold_ms, "warm_ms": warm_ms,
                            "speedup_pct": round(speedup, 2),
                        })

                both_cold_r = by_key.get((c, p, "rap", "both_cold"))
                policy_warm_r = by_key.get((c, p, "rap", "policy_warm"))
                both_warm_r = by_key.get((c, p, "rap", "both_warm"))
                if both_cold_r and policy_warm_r and both_warm_r:
                    both_cold_ms = both_cold_r["metrics"]["duration_ms"]
                    policy_warm_ms = policy_warm_r["metrics"]["duration_ms"]
                    both_warm_ms = both_warm_r["metrics"]["duration_ms"]
                    meta_cost = round(both_cold_ms - policy_warm_ms, 2)
                    planning_cost = round(policy_warm_ms - both_warm_ms, 2)
                    cache_results.append({
                        "complexity": c, "mode": "rap", "layer": "3-layer",
                        "both_cold_ms": both_cold_ms,
                        "policy_warm_ms": policy_warm_ms,
                        "both_warm_ms": both_warm_ms,
                        "meta_rpc_cost_ms": meta_cost,
                        "planning_cost_ms": planning_cost,
                    })
                continue

            rap_r = by_key.get((c, p, "rap", "warm"))
            where_r = by_key.get((c, p, "where", "warm"))
            if rap_r and where_r:
                rap_ms = rap_r["metrics"].get("server_min_ms", rap_r["metrics"]["duration_ms"])
                where_ms = where_r["metrics"].get("server_min_ms", where_r["metrics"]["duration_ms"])
                overhead = ((rap_ms - where_ms) / where_ms * 100) if where_ms > 0 else 0
                rap_vs_where.append({
                    "complexity": c, "pattern": p,
                    "rap_ms": rap_ms, "where_ms": where_ms,
                    "overhead_pct": round(overhead, 2),
                    "source": "server" if "server_min_ms" in rap_r["metrics"] else "client",
                })

            mask_r = by_key.get((c, p, "mask", "warm"))
            func_r = by_key.get((c, p, "func", "warm"))
            if mask_r and func_r:
                mask_ms = mask_r["metrics"].get("server_min_ms", mask_r["metrics"]["duration_ms"])
                func_ms = func_r["metrics"].get("server_min_ms", func_r["metrics"]["duration_ms"])
                overhead = ((mask_ms - func_ms) / func_ms * 100) if func_ms > 0 else 0
                mask_vs_func.append({
                    "complexity": c, "pattern": p,
                    "mask_ms": mask_ms, "func_ms": func_ms,
                    "overhead_pct": round(overhead, 2),
                    "source": "server" if "server_min_ms" in mask_r["metrics"] else "client",
                })

    return {
        "rap_vs_where": rap_vs_where,
        "mask_vs_func": mask_vs_func,
        "planner_cache": cache_results,
    }


# ---------------------------------------------------------------------------
# CLI and main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark RAP and Data Mask policy overhead."
    )
    parser.add_argument("--dsn", default=os.getenv("BENDSQL_DSN"), help="bendsql DSN")
    parser.add_argument("--bendsql", default=os.getenv("BENDSQL", "bendsql"))
    parser.add_argument("--database", default="tmp_security_policy_bench")
    parser.add_argument("--table", default="bench_security")
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--rows-per-batch", type=int, default=1_000_000)
    parser.add_argument("--rows-per-block", type=int, default=10_000)
    parser.add_argument("--blocks-per-segment", type=int, default=10)
    parser.add_argument(
        "--complexity",
        choices=["simple", "range", "complex", "all"],
        default="all",
    )
    parser.add_argument(
        "--pattern",
        choices=["point", "range_scan", "order_limit", "agg", "full_scan", "all"],
        default="all",
    )
    parser.add_argument("--warmup-rounds", type=int, default=2)
    parser.add_argument("--bench-rounds", type=int, default=5)
    parser.add_argument("--output", default=None, help="Write JSON result to this file")
    parser.add_argument("--keep", action="store_true", help="Do not drop database first")
    parser.add_argument("--drop-after", action="store_true", help="Drop database after run")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def print_summary(summary: dict[str, Any]) -> None:
    print("\n" + "=" * 70)
    print("SUMMARY: RAP vs WHERE (overhead %)")
    print("-" * 70)
    for item in summary.get("rap_vs_where", []):
        flag = "!!!" if item["overhead_pct"] > 5 else "   "
        print(f"  {flag} {item['complexity']:8s} | {item['pattern']:12s} | "
              f"rap={item['rap_ms']:8.1f}ms  where={item['where_ms']:8.1f}ms  "
              f"overhead={item['overhead_pct']:+.1f}%")

    print("\nSUMMARY: MASK vs FUNC (overhead %)")
    print("-" * 70)
    for item in summary.get("mask_vs_func", []):
        flag = "!!!" if item["overhead_pct"] > 5 else "   "
        print(f"  {flag} {item['complexity']:8s} | {item['pattern']:12s} | "
              f"mask={item['mask_ms']:8.1f}ms  func={item['func_ms']:8.1f}ms  "
              f"overhead={item['overhead_pct']:+.1f}%")

    print("\nSUMMARY: CACHE LAYERS (cost isolation)")
    print("-" * 70)
    for item in summary.get("planner_cache", []):
        if item.get("layer") == "planner":
            print(f"       {item['complexity']:8s} | {item['mode']:5s} | "
                  f"cold={item['cold_ms']:8.1f}ms  warm={item['warm_ms']:8.1f}ms  "
                  f"speedup={item['speedup_pct']:+.1f}%")
        elif item.get("layer") == "3-layer":
            print(f"       {item['complexity']:8s} | rap   | "
                  f"both_cold={item['both_cold_ms']:.1f}ms  "
                  f"policy_warm={item['policy_warm_ms']:.1f}ms  "
                  f"both_warm={item['both_warm_ms']:.1f}ms")
            print(f"       {'':8s} |       | "
                  f"meta_rpc={item['meta_rpc_cost_ms']:+.1f}ms  "
                  f"planning={item['planning_cost_ms']:+.1f}ms")
    print("=" * 70)


def main() -> int:
    args = parse_args()
    bs = BendSQL(args.bendsql, args.dsn, args.dry_run)

    complexities = ["simple", "range", "complex"] if args.complexity == "all" else [args.complexity]

    # Setup
    if not args.keep:
        bs.try_run(f"DROP DATABASE IF EXISTS {args.database}", "drop-db", output="null")
    bs.run(f"CREATE DATABASE IF NOT EXISTS {args.database}", "create-db", output="null")
    bs.run(
        create_table_sql(args.database, args.table, args.rows_per_block, args.blocks_per_segment),
        "create-table", output="null",
    )

    # Insert data (skip if --keep and table already has data)
    if not args.keep:
        batches = (args.rows + args.rows_per_batch - 1) // args.rows_per_batch
        print(f"\nInserting {args.rows} rows in {batches} batches...", flush=True)
        for i in range(batches):
            batch_rows = min(args.rows_per_batch, args.rows - i * args.rows_per_batch)
            bs.run(
                insert_sql(args.database, args.table, i, batch_rows),
                f"insert-batch-{i}", output="null",
            )

    # Verify data setup
    full_table = f"{args.database}.{args.table}"
    count_res = bs.try_run(f"SELECT COUNT(*) FROM {full_table}", "count-rows", echo_stdout=False)
    snapshot_res = bs.try_run(
        f"SELECT segment_count, block_count, row_count FROM fuse_snapshot('{args.database}', '{args.table}') LIMIT 1",
        "snapshot-info", echo_stdout=False,
    )
    data_setup: dict[str, Any] = {"rows": args.rows}
    snap_rows = csv_records(snapshot_res.stdout)
    if snap_rows and len(snap_rows[0]) >= 3:
        data_setup["segments"] = int(snap_rows[0][0])
        data_setup["blocks"] = int(snap_rows[0][1])
        data_setup["actual_rows"] = int(snap_rows[0][2])
    print(f"Data setup: {data_setup}", flush=True)

    # Switch to the benchmark database for policy DDL (policies don't support db.name syntax)
    bs.run(f"USE {args.database}", "use-db", output="null")
    all_results: list[dict[str, Any]] = []

    for complexity in complexities:
        print(f"\n{'='*40} RAP complexity={complexity} {'='*40}", flush=True)
        all_results.extend(bench_rap(bs, args.database, args.table, complexity, args.warmup_rounds, args.bench_rounds))

        print(f"\n{'='*40} MASK complexity={complexity} {'='*40}", flush=True)
        all_results.extend(bench_mask(bs, args.database, args.table, complexity, args.warmup_rounds, args.bench_rounds))

        print(f"\n{'='*40} CACHE complexity={complexity} {'='*40}", flush=True)
        all_results.extend(bench_planner_cache(bs, args.database, args.table, complexity, args.bench_rounds))

    # Filter by pattern if specified
    if args.pattern != "all":
        all_results = [r for r in all_results if r["pattern"] == args.pattern]

    # Compute summary
    summary = compute_summary(all_results)
    print_summary(summary)

    # Output
    output = {
        "args": vars(args),
        "data_setup": data_setup,
        "results": all_results,
        "summary": summary,
    }

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)
        print(f"\nResults written to: {args.output}")

    if args.drop_after:
        bs.try_run(f"DROP DATABASE IF EXISTS {args.database}", "drop-db-after", output="null")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
