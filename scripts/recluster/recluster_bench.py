#!/usr/bin/env python3
"""
Temporary recluster benchmark helper.

Runs data generation and recluster measurement through bendsql. This file is
intended for local experiments and is not part of the repository test suite.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


RECLUSTER_METRICS = (
    "fuse_recluster_build_task_milliseconds",
    "fuse_recluster_segment_nums_scheduled",
    "fuse_recluster_block_nums_to_read",
    "fuse_recluster_block_bytes_to_read",
    "fuse_recluster_row_nums_to_read",
    "fuse_recluster_write_block_nums",
)


@dataclass
class QueryResult:
    label: str
    sql: str
    elapsed_sec: float
    stdout: str
    stderr: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate clustered test data and measure recluster effect."
    )
    parser.add_argument("--dsn", default=os.getenv("BENDSQL_DSN"), help="bendsql DSN")
    parser.add_argument("--bendsql", default=os.getenv("BENDSQL", "bendsql"))
    parser.add_argument("--database", default="tmp_recluster_bench")
    parser.add_argument("--table", default="t")
    parser.add_argument("--rows-per-batch", type=int, default=1_000_000)
    parser.add_argument("--batches", type=int, default=4)
    parser.add_argument("--row-per-block", type=int, default=10_000)
    parser.add_argument("--block-per-segment", type=int, default=10)
    parser.add_argument("--domain", type=int, default=100_000)
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument("--recluster-rounds", type=int, default=3)
    parser.add_argument(
        "--waves",
        type=int,
        default=1,
        help="Split inserts into waves. Each wave inserts batches then optionally reclusters before the next wave.",
    )
    parser.add_argument(
        "--recluster-per-wave",
        type=int,
        default=0,
        help="Run this many recluster rounds after each insert wave before the final measurement rounds.",
    )
    parser.add_argument(
        "--stop-at-depth",
        type=float,
        default=1.0,
        help="Stop early when average_depth is <= this value. Use 0 to disable.",
    )
    parser.add_argument("--max-threads", type=int, default=8)
    parser.add_argument("--recluster-block-size", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--limit", type=int, default=None, help="ALTER TABLE ... LIMIT")
    parser.add_argument("--final", action="store_true", help="Use RECLUSTER FINAL")
    parser.add_argument("--keep", action="store_true", help="Do not drop database first")
    parser.add_argument("--drop-after", action="store_true", help="Drop database after run")
    parser.add_argument("--output", default=None, help="Write JSON result to this file")
    parser.add_argument(
        "--case",
        choices=(
            "overlap",
            "wide",
            "disjoint",
            "window",
            "backfill",
            "multi_overlap",
            "tiered_overlap",
        ),
        default="overlap",
        help="Data shape. overlap repeats the same key range; disjoint uses non-overlapping ranges; window uses partially-overlapping ranges; backfill mostly appends with periodic historical ranges; multi_overlap creates mixed 1/2/3-way overlap; tiered_overlap keeps rewriting a few hot key bands so recluster can build higher levels; wide is overlap plus payload column.",
    )
    parser.add_argument(
        "--window-step",
        type=int,
        default=None,
        help="Key-range step for --case window. Defaults to domain / 2.",
    )
    parser.add_argument(
        "--backfill-every",
        type=int,
        default=5,
        help="For --case backfill, every Nth batch writes a historical range.",
    )
    parser.add_argument(
        "--backfill-span",
        type=int,
        default=None,
        help="For --case backfill, number of historical batches to cycle through. Defaults to backfill_every.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--explain-prune",
        action="store_true",
        help="Collect EXPLAIN output for representative cluster-key range predicates after recluster.",
    )
    parser.add_argument(
        "--keep-block-stats",
        action="store_true",
        help="Store raw clustering_statistics rows in JSON for deterministic-layout debugging.",
    )
    parser.add_argument(
        "--prune-samples",
        type=int,
        default=0,
        help="Add this many deterministic range predicates to --explain-prune.",
    )
    parser.add_argument(
        "--prune-window",
        type=int,
        default=10_000,
        help="Width of sampled range predicates for --prune-samples.",
    )
    return parser.parse_args()


class BendSQL:
    def __init__(self, binary: str, dsn: str | None, dry_run: bool) -> None:
        self.binary = binary
        self.dsn = dsn
        self.dry_run = dry_run

    def run(
        self, sql: str, label: str, output: str = "csv", echo_stdout: bool = True
    ) -> QueryResult:
        cmd = [self.binary, "--output", output, f"--query={sql}"]
        if self.dsn:
            cmd[1:1] = ["--dsn", self.dsn]

        printable = " ".join(shlex.quote(part) for part in cmd)
        print(f"\n[{label}] {printable}", flush=True)
        start = time.monotonic()
        if self.dry_run:
            elapsed = time.monotonic() - start
            return QueryResult(label, sql, elapsed, "", "")

        proc = subprocess.run(cmd, text=True, capture_output=True)
        elapsed = time.monotonic() - start
        if proc.returncode != 0:
            print(proc.stdout, end="", file=sys.stdout)
            print(proc.stderr, end="", file=sys.stderr)
            raise RuntimeError(f"bendsql failed for {label}, exit={proc.returncode}")
        if echo_stdout and proc.stdout.strip():
            print(proc.stdout.strip(), flush=True)
        if proc.stderr.strip():
            print(proc.stderr.strip(), file=sys.stderr, flush=True)
        return QueryResult(label, sql, elapsed, proc.stdout, proc.stderr)

    def try_run(
        self, sql: str, label: str, output: str = "csv", echo_stdout: bool = True
    ) -> QueryResult:
        try:
            return self.run(sql, label, output, echo_stdout)
        except RuntimeError as err:
            print(f"[{label}] skipped: {err}", file=sys.stderr, flush=True)
            return QueryResult(label, sql, 0.0, "", str(err))


def csv_records(stdout: str) -> list[list[str]]:
    text = stdout.strip()
    if not text:
        return []
    return list(csv.reader(text.splitlines()))


def kv_map(stdout: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in csv_records(stdout):
        if len(row) >= 2:
            out[row[0]] = row[1]
    return out


def parse_value(value: str | None) -> Any:
    if value is None or value == "":
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def first_json_value(row: dict[str, Any], key: str) -> Any:
    return row.get(key)


def metric_sql() -> str:
    names = ", ".join(f"'{m}'" for m in RECLUSTER_METRICS)
    return f"""
select metric, value
from system.metrics
where metric in ({names}) or metric in ({", ".join(f"'{m}_total'" for m in RECLUSTER_METRICS)})
order by metric
"""


def metric_map(rows: list[list[str]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in rows:
        metric = row[0] if len(row) > 0 else None
        value = row[1] if len(row) > 1 else None
        if metric and value not in (None, ""):
            try:
                out[metric] = float(value)
            except ValueError:
                out[metric] = 0.0
    return out


def metric_delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    keys = sorted(set(before) | set(after))
    return {key: after.get(key, 0.0) - before.get(key, 0.0) for key in keys}


def level_hist_sql(args: argparse.Namespace) -> str:
    return f"""
select ifnull(to_string(level), 'null') as level, count() as blocks
from clustering_statistics('{args.database}', '{args.table}')
group by level
order by level
"""


def block_stats_sql(args: argparse.Namespace) -> str:
    return f"""
select segment_name, block_name, min, max, level
from clustering_statistics('{args.database}', '{args.table}')
order by min, max, block_name
"""


def table_name(args: argparse.Namespace) -> str:
    return f"{args.database}.{args.table}"


def create_table_sql(args: argparse.Namespace) -> str:
    payload = ", payload string" if args.case == "wide" or args.payload_bytes > 0 else ""
    return f"""
create table {table_name(args)} (
  a int not null,
  b int not null{payload}
)
cluster by(a)
row_per_block={args.row_per_block}
block_per_segment={args.block_per_segment}
"""


def insert_sql(args: argparse.Namespace, batch: int) -> str:
    n = args.rows_per_batch
    domain = args.domain
    if args.case == "disjoint":
        a_expr = f"(number + {batch * n})"
    elif args.case == "backfill":
        every = max(args.backfill_every, 1)
        span = max(args.backfill_span if args.backfill_span is not None else every, 1)
        if batch > 0 and batch % every == 0:
            target_batch = (batch // every - 1) % span
            if batch % 2 == 0:
                a_expr = f"(number + {target_batch * n})"
            else:
                a_expr = f"({target_batch * n} + {n} - 1 - number)"
        else:
            a_expr = f"(number + {batch * n})"
    elif args.case == "multi_overlap":
        group = batch % 6
        cycle = batch // 6
        base = cycle * 4 * n
        if group == 0:
            offset = base
        elif group == 1:
            offset = base + n
        elif group == 2:
            offset = base + 3 * n
        elif group == 3:
            offset = base + n // 2
        elif group == 4:
            offset = base + n
        else:
            offset = base + n + n // 2
        if batch % 2 == 0:
            a_expr = f"(number + {offset})"
        else:
            a_expr = f"({offset} + {n} - 1 - number)"
    elif args.case == "tiered_overlap":
        # Keep a bounded set of hot ranges active across waves. This lets repeated
        # hook/manual recluster rounds create level 2/3 blocks while new inserts
        # continue to add level 0 overlap.
        hot_bands = 4
        band = batch % hot_bands
        cycle = batch // hot_bands
        offset = band * n
        if cycle % 3 == 0:
            a_expr = f"(number + {offset})"
        elif cycle % 3 == 1:
            a_expr = f"({offset} + {n} - 1 - number)"
        else:
            shift = n // 2
            a_expr = f"((number % {n}) + {offset} + {shift})"
    elif args.case == "window":
        step = args.window_step if args.window_step is not None else max(domain // 2, 1)
        if batch % 2 == 0:
            a_expr = f"((number % {domain}) + {batch * step})"
        else:
            a_expr = f"({batch * step} + {domain} - 1 - (number % {domain}))"
    elif batch % 2 == 0:
        a_expr = f"(number % {domain})"
    else:
        a_expr = f"({domain} - 1 - (number % {domain}))"

    # Offset b so every batch is unique while a deliberately overlaps.
    b_expr = f"(number + {batch * n})"
    if args.case == "wide" or args.payload_bytes > 0:
        payload_len = max(args.payload_bytes, 200)
        return f"""
insert into {table_name(args)}
select {a_expr}::int, {b_expr}::int, repeat('x', {payload_len})
from numbers({n})
"""
    return f"""
insert into {table_name(args)}
select {a_expr}::int, {b_expr}::int
from numbers({n})
"""


def recluster_sql(args: argparse.Namespace) -> str:
    final = " final" if args.final else ""
    limit = f" limit {args.limit}" if args.limit is not None else ""
    return f"alter table {table_name(args)} recluster{final}{limit}"


def parse_cluster_bound(value: str | None) -> float | None:
    if value is None or value == "" or value.lower() == "null":
        return None
    text = value.strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1].split(",", 1)[0].strip()
    try:
        return float(text)
    except ValueError:
        return None


def percentile(sorted_values: list[float], p: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * p
    low = int(pos)
    high = min(low + 1, len(sorted_values) - 1)
    weight = pos - low
    return sorted_values[low] * (1.0 - weight) + sorted_values[high] * weight


def interval_quality(block_rows: list[list[str]]) -> dict[str, Any]:
    intervals = []
    levels: dict[str, int] = {}
    for row in block_rows:
        if len(row) < 5:
            continue
        min_key = parse_cluster_bound(row[2])
        max_key = parse_cluster_bound(row[3])
        level = row[4] if row[4] not in ("", None) else "null"
        levels[level] = levels.get(level, 0) + 1
        if min_key is None or max_key is None:
            continue
        if max_key < min_key:
            min_key, max_key = max_key, min_key
        intervals.append(
            {
                "segment": row[0],
                "block": row[1],
                "min": min_key,
                "max": max_key,
                "level": parse_value(row[4]),
            }
        )

    events = []
    for item in intervals:
        events.append((item["min"], 1))
        events.append((item["max"], -1))
    events.sort(key=lambda item: (item[0], -item[1]))

    active = 0
    weighted_depth = 0.0
    total_span = 0.0
    max_depth = 0
    span_depths = []
    prev = None
    for point, delta in events:
        if prev is not None and point > prev:
            span = point - prev
            weighted_depth += span * active
            total_span += span
            if active > 0:
                span_depths.append(active)
            max_depth = max(max_depth, active)
        active += delta
        prev = point

    span_depths.sort()
    widths = sorted(item["max"] - item["min"] for item in intervals)
    return {
        "block_count": len(block_rows),
        "clustered_block_count": len(intervals),
        "level_hist": levels,
        "interval": {
            "key_min": min((item["min"] for item in intervals), default=None),
            "key_max": max((item["max"] for item in intervals), default=None),
            "avg_depth_by_span": weighted_depth / total_span if total_span > 0 else None,
            "max_depth_by_span": max_depth,
            "p50_depth_by_span": percentile(span_depths, 0.50),
            "p90_depth_by_span": percentile(span_depths, 0.90),
            "p99_depth_by_span": percentile(span_depths, 0.99),
            "p50_block_width": percentile(widths, 0.50),
            "p90_block_width": percentile(widths, 0.90),
        },
    }


def state_delta(before: dict[str, Any] | None, after: dict[str, Any]) -> dict[str, Any]:
    if not before:
        return {}
    before_clustering = before.get("clustering", {})
    after_clustering = after.get("clustering", {})
    before_quality = before.get("quality", {}).get("interval", {})
    after_quality = after.get("quality", {}).get("interval", {})

    def diff_num(left: Any, right: Any) -> float | None:
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return right - left
        return None

    return {
        "average_depth": diff_num(
            before_clustering.get("average_depth"),
            after_clustering.get("average_depth"),
        ),
        "average_overlaps": diff_num(
            before_clustering.get("average_overlaps"),
            after_clustering.get("average_overlaps"),
        ),
        "avg_depth_by_span": diff_num(
            before_quality.get("avg_depth_by_span"),
            after_quality.get("avg_depth_by_span"),
        ),
        "max_depth_by_span": diff_num(
            before_quality.get("max_depth_by_span"),
            after_quality.get("max_depth_by_span"),
        ),
        "p90_depth_by_span": diff_num(
            before_quality.get("p90_depth_by_span"),
            after_quality.get("p90_depth_by_span"),
        ),
        "clustered_blocks": diff_num(
            before.get("quality", {}).get("clustered_block_count"),
            after.get("quality", {}).get("clustered_block_count"),
        ),
    }


def collect_state(client: BendSQL, args: argparse.Namespace, label: str) -> dict[str, Any]:
    clustering = client.run(
        f"""
select
  cluster_key,
  type,
  info
from clustering_information('{args.database}', '{args.table}')
""",
        f"{label}:clustering",
    )
    snapshot = client.run(
        f"""
select segment_count, block_count, row_count
from fuse_snapshot('{args.database}', '{args.table}')
order by timestamp desc
limit 1
""",
        f"{label}:snapshot",
    )
    metrics = client.run(metric_sql(), f"{label}:metrics")
    levels = client.try_run(level_hist_sql(args), f"{label}:levels")
    block_stats = client.try_run(
        block_stats_sql(args), f"{label}:block-stats", echo_stdout=False
    )
    clustering_records = csv_records(clustering.stdout)
    clustering_info = {}
    if clustering_records:
        row = clustering_records[0]
        info = parse_value(row[2] if len(row) > 2 else None)
        if isinstance(info, dict):
            clustering_info.update(info)
        clustering_info["cluster_key"] = row[0] if len(row) > 0 else None
        clustering_info["type"] = row[1] if len(row) > 1 else None

    snapshot_records = csv_records(snapshot.stdout)
    snapshot_info = {}
    if snapshot_records:
        row = snapshot_records[0]
        for idx, key in enumerate(("segment_count", "block_count", "row_count")):
            snapshot_info[key] = parse_value(row[idx] if len(row) > idx else None)

    block_records = csv_records(block_stats.stdout)
    state = {
        "label": label,
        "clustering": clustering_info,
        "snapshot": snapshot_info,
        "metrics": metric_map(csv_records(metrics.stdout)),
        "levels": {
            row[0]: parse_value(row[1])
            for row in csv_records(levels.stdout)
            if len(row) >= 2
        },
        "elapsed": {
            "clustering_sec": clustering.elapsed_sec,
            "snapshot_sec": snapshot.elapsed_sec,
            "metrics_sec": metrics.elapsed_sec,
            "levels_sec": levels.elapsed_sec,
            "block_stats_sec": block_stats.elapsed_sec,
        },
        "quality": interval_quality(block_records),
    }
    if args.keep_block_stats:
        state["block_stats"] = block_records
    return state


def explain_prune_sql(args: argparse.Namespace, low: int, high: int) -> str:
    return f"""
explain select count()
from {table_name(args)}
where a >= {low} and a < {high}
"""


def sampled_prune_ranges(args: argparse.Namespace) -> list[tuple[int, int]]:
    ranges = [
        (0, 10_000),
        (25_000, 35_000),
        (50_000, 60_000),
        (100_000, 110_000),
        (175_000, 185_000),
        (0, 50_000),
        (75_000, 125_000),
    ]
    if args.prune_samples <= 0:
        return ranges

    # Deterministic low-discrepancy-ish scan across the generated key domain.
    # This keeps runs comparable without depending on random seeds.
    max_key = max(args.domain + args.window_step if args.window_step else args.domain, 1)
    if args.case == "tiered_overlap":
        max_key = 225_000
    max_low = max(max_key - args.prune_window, 0)
    seen = set(ranges)
    for idx in range(args.prune_samples):
        if args.prune_samples == 1:
            low = max_low // 2
        else:
            low = round(max_low * idx / (args.prune_samples - 1))
        high = low + args.prune_window
        item = (low, high)
        if item not in seen:
            ranges.append(item)
            seen.add(item)
    return ranges


def parse_explain_prune(stdout: str) -> dict[str, Any]:
    def int_match(pattern: str) -> int | None:
        match = re.search(pattern, stdout)
        return int(match.group(1)) if match else None

    def pair_match(pattern: str) -> dict[str, int] | None:
        match = re.search(pattern, stdout)
        if not match:
            return None
        return {"before": int(match.group(1)), "after": int(match.group(2))}

    return {
        "read_rows": int_match(r"read rows: ([0-9]+)"),
        "partitions_total": int_match(r"partitions total: ([0-9]+)"),
        "partitions_scanned": int_match(r"partitions scanned: ([0-9]+)"),
        "segments": pair_match(r"segments: <range pruning: ([0-9]+) to ([0-9]+)"),
        "blocks": pair_match(r"blocks: <range pruning: ([0-9]+) to ([0-9]+)"),
    }


def collect_explain_prune(client: BendSQL, args: argparse.Namespace) -> list[dict[str, Any]]:
    if not args.explain_prune:
        return []

    out = []
    for low, high in sampled_prune_ranges(args):
        result = client.run(
            explain_prune_sql(args, low, high),
            f"explain-prune-{low}-{high}",
            "csv",
            echo_stdout=False,
        )
        out.append(
            {
                "range": [low, high],
                "sql": explain_prune_sql(args, low, high).strip(),
                "explain": result.stdout.strip(),
                "stats": parse_explain_prune(result.stdout),
            }
        )
    return out


def main() -> int:
    args = parse_args()
    client = BendSQL(args.bendsql, args.dsn, args.dry_run)

    run: dict[str, Any] = {
        "args": vars(args),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "queries": [],
        "states": [],
        "recluster_rounds": [],
    }

    def q(sql: str, label: str, output: str = "csv") -> QueryResult:
        result = client.run(sql, label, output)
        run["queries"].append(asdict(result))
        return result

    if not args.keep:
        q(f"drop database if exists {args.database}", "drop-db", "null")
    q(f"create database if not exists {args.database}", "create-db", "null")
    q(create_table_sql(args), "create-table", "null")
    q(f"set max_threads = {args.max_threads}", "set-max-threads", "null")
    q(
        f"set recluster_block_size = {args.recluster_block_size}",
        "set-recluster-block-size",
        "null",
    )

    prev_metrics = None
    waves = max(args.waves, 1)
    base_batches_per_wave = args.batches // waves
    extra_batches = args.batches % waves
    batch = 0
    for wave in range(waves):
        wave_batches = base_batches_per_wave + (1 if wave < extra_batches else 0)
        for _ in range(wave_batches):
            q(insert_sql(args, batch), f"insert-batch-{batch}", "null")
            batch += 1

        wave_state = collect_state(client, args, f"after-wave-{wave + 1}-insert")
        run["states"].append(wave_state)
        if prev_metrics is None:
            prev_metrics = wave_state["metrics"]

        if wave + 1 < waves and args.recluster_per_wave > 0:
            for round_no in range(1, args.recluster_per_wave + 1):
                label = f"wave-{wave + 1}-recluster-{round_no}"
                start = time.monotonic()
                q(recluster_sql(args), label, "null")
                recluster_elapsed = time.monotonic() - start
                before_state = run["states"][-1]
                state = collect_state(client, args, f"after-{label}")
                delta = metric_delta(prev_metrics, state["metrics"])
                run["states"].append(state)
                run["recluster_rounds"].append(
                    {
                        "round": label,
                        "elapsed_sec": recluster_elapsed,
                        "metric_delta": delta,
                        "quality_delta": state_delta(before_state, state),
                        "clustering": state["clustering"],
                        "snapshot": state["snapshot"],
                        "levels": state["levels"],
                        "quality": state["quality"],
                    }
                )
                prev_metrics = state["metrics"]

    before = collect_state(client, args, "before-final")
    run["states"].append(before)

    prev_metrics = before["metrics"]
    for round_no in range(1, args.recluster_rounds + 1):
        start = time.monotonic()
        q(recluster_sql(args), f"recluster-{round_no}", "null")
        recluster_elapsed = time.monotonic() - start
        before_state = run["states"][-1]
        state = collect_state(client, args, f"after-{round_no}")
        delta = metric_delta(prev_metrics, state["metrics"])
        run["states"].append(state)
        run["recluster_rounds"].append(
            {
                "round": round_no,
                "elapsed_sec": recluster_elapsed,
                "metric_delta": delta,
                "quality_delta": state_delta(before_state, state),
                "clustering": state["clustering"],
                "snapshot": state["snapshot"],
                "levels": state["levels"],
                "quality": state["quality"],
            }
        )
        prev_metrics = state["metrics"]

        avg_depth = first_json_value(state["clustering"], "average_depth")
        if (
            args.stop_at_depth > 0
            and isinstance(avg_depth, (int, float))
            and avg_depth <= args.stop_at_depth
        ):
            print(f"average_depth reached {avg_depth}; stopping early", flush=True)
            break

    if args.drop_after:
        q(f"drop database if exists {args.database}", "drop-after", "null")

    run["explain_prune"] = collect_explain_prune(client, args)
    run["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    summary = {
        "database": args.database,
        "table": args.table,
        "rows": args.rows_per_batch * args.batches,
        "rounds": [
            {
                "round": item["round"],
                "elapsed_sec": round(item["elapsed_sec"], 3),
                "metric_delta": item["metric_delta"],
                "clustering": item["clustering"],
                "snapshot": item["snapshot"],
                "levels": item.get("levels", {}),
                "quality": item.get("quality", {}),
                "quality_delta": item.get("quality_delta", {}),
            }
            for item in run["recluster_rounds"]
        ],
        "explain_prune": run["explain_prune"],
    }
    print("\nSUMMARY")
    print(json.dumps(summary, indent=2, sort_keys=True))

    if args.output:
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(run, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\nwrote {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
