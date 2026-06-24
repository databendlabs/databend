#!/usr/bin/env python3
"""
Local benchmark for ANALYZE TABLE histogram algorithms.

The script creates a synthetic FUSE table, runs ANALYZE TABLE WITH HISTOGRAM
with the window, KLL fast, and KLL full algorithms, samples databend-query RSS
while ANALYZE is running, and compares histogram-based cardinality estimates
with exact counts.

It is intended for local experiments and is not part of the CI test suite.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.bench_common.bendsql import BendSQL, QueryResult, csv_records

DEFAULT_LOCAL_DSN = "databend://root:@127.0.0.1:38000?sslmode=disable"

HISTOGRAM_RE = re.compile(
    r'\[bucket id: (?P<id>\d+), min: "(?P<min>.*?)", max: "(?P<max>.*?)", '
    r"ndv: (?P<ndv>[-+0-9.eE]+), count: (?P<count>[-+0-9.eE]+)\]"
)


@dataclass(frozen=True)
class Bucket:
    bucket_id: int
    lower: float
    upper: float
    ndv: float
    count: float


@dataclass(frozen=True)
class Probe:
    column: str
    kind: str
    lower: int
    upper: int | None = None

    @property
    def label(self) -> str:
        if self.kind == "eq":
            return f"{self.column} = {self.lower}"
        return f"{self.column} BETWEEN {self.lower} AND {self.upper}"

    @property
    def sql_predicate(self) -> str:
        column = quote_ident(self.column)
        if self.kind == "eq":
            return f"{column} = {self.lower}"
        assert self.upper is not None
        return f"{column} BETWEEN {self.lower} AND {self.upper}"


@dataclass
class AnalyzeResult:
    algorithm: str
    elapsed_sec: float
    query_pids: list[int]
    rss_baseline_kb: int | None
    rss_peak_kb: int | None
    rss_delta_kb: int | None


@dataclass
class AccuracyResult:
    algorithm: str
    column: str
    distribution: str
    predicate_kind: str
    predicate: str
    exact_count: float
    estimated_count: float
    absolute_error: float
    q_error: float
    relative_error: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare ANALYZE histogram algorithm cost and accuracy."
    )
    parser.add_argument("--dsn", default=os.getenv("BENDSQL_DSN"), help="bendsql DSN")
    parser.add_argument("--bendsql", default=os.getenv("BENDSQL", "bendsql"))
    parser.add_argument("--database", default="tmp_analyze_histogram_bench")
    parser.add_argument("--table", default="t")
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--batch-rows", type=int, default=500_000)
    parser.add_argument("--domain", type=int, default=100_000)
    parser.add_argument(
        "--skew-percent",
        type=int,
        default=80,
        help="Percent of rows forced to skew_key = 0.",
    )
    parser.add_argument("--max-threads", type=int, default=8)
    parser.add_argument("--kll-error-rate", type=float, default=0.01)
    parser.add_argument(
        "--algorithms",
        default="window,kll_fast,kll_full",
        help="Comma-separated analyze algorithms to run: window,kll_fast,kll_full.",
    )
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--sample-interval-sec", type=float, default=0.05)
    parser.add_argument(
        "--run-mode",
        choices=("sequential", "isolated-query"),
        default="sequential",
        help=(
            "sequential reuses the current query process; isolated-query starts a fresh "
            "databend-query process for setup and for each measured ANALYZE run."
        ),
    )
    parser.add_argument(
        "--query-bin",
        default="target/debug/databend-query",
        help="databend-query binary used by --run-mode=isolated-query.",
    )
    parser.add_argument(
        "--query-config",
        default="_data/local/databend-query.toml",
        help="databend-query config used by --run-mode=isolated-query.",
    )
    parser.add_argument(
        "--query-ready-timeout-sec",
        type=float,
        default=60.0,
        help="Timeout while waiting for a managed databend-query process to accept SQL.",
    )
    parser.add_argument(
        "--query-shutdown-timeout-sec",
        type=float,
        default=10.0,
        help="Grace period before killing a managed databend-query process.",
    )
    parser.add_argument(
        "--query-log-dir",
        default="target/bench-results",
        help="Directory for managed databend-query stdout/stderr logs.",
    )
    parser.add_argument(
        "--query-pid",
        action="append",
        type=int,
        default=[],
        help="databend-query PID to sample. Can be passed multiple times.",
    )
    parser.add_argument(
        "--query-process-substring",
        default="databend-query",
        help="Used to auto-discover query PIDs when --query-pid is omitted.",
    )
    parser.add_argument("--reuse-table", action="store_true")
    parser.add_argument("--keep", action="store_true", help="Do not drop database after run.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output", default=None, help="Write JSON result to this path.")
    return parser.parse_args()


def quote_ident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def fq_table(database: str, table: str) -> str:
    return f"{quote_ident(database)}.{quote_ident(table)}"


def build_client(args: argparse.Namespace) -> BendSQL:
    if args.run_mode == "isolated-query" and not args.dsn:
        args.dsn = DEFAULT_LOCAL_DSN
    return BendSQL(args.bendsql, args.dsn, args.dry_run)


def run_sql(client: BendSQL, sql: str, label: str, echo_stdout: bool = True) -> QueryResult:
    return client.run(sql, label, output="csv", echo_stdout=echo_stdout)


def session_sql(args: argparse.Namespace, sql: str) -> str:
    return "\n".join(
        [
            "SET enable_table_snapshot_stats = 1;",
            "SET enable_analyze_histogram = 1;",
            f"SET max_threads = {args.max_threads};",
            sql,
        ]
    )


def prepare_table(client: BendSQL, args: argparse.Namespace) -> None:
    table = fq_table(args.database, args.table)
    if not args.reuse_table:
        run_sql(client, f"DROP DATABASE IF EXISTS {quote_ident(args.database)}", "drop database")
        run_sql(client, f"CREATE DATABASE {quote_ident(args.database)}", "create database")
        run_sql(
            client,
            f"""
            CREATE TABLE {table} (
                id UInt64,
                uniform_key UInt64,
                skew_key UInt64,
                payload String
            )
            """,
            "create table",
        )

        inserted = 0
        while inserted < args.rows:
            batch_rows = min(args.batch_rows, args.rows - inserted)
            insert_sql = f"""
                INSERT INTO {table}
                SELECT
                    number + {inserted} AS id,
                    (number + {inserted}) % {args.domain} AS uniform_key,
                    IF(((number + {inserted}) % 100) < {args.skew_percent},
                        0,
                        (number + {inserted}) % {args.domain}) AS skew_key,
                    concat('payload_', to_string((number + {inserted}) % {args.domain})) AS payload
                FROM numbers({batch_rows})
            """
            run_sql(client, session_sql(args, insert_sql), f"insert batch offset={inserted}", False)
            inserted += batch_rows
    else:
        run_sql(client, f"CREATE DATABASE IF NOT EXISTS {quote_ident(args.database)}", "ensure database")

    run_sql(
        client,
        f"SELECT count(), count(DISTINCT uniform_key), count(DISTINCT skew_key) FROM {table}",
        "table cardinality",
    )


def discover_query_pids(process_substring: str) -> list[int]:
    pids: list[int] = []
    self_pid = os.getpid()
    proc_dir = Path("/proc")
    if not proc_dir.exists():
        return pids

    for entry in proc_dir.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == self_pid:
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes().replace(b"\x00", b" ").decode()
        except (OSError, UnicodeDecodeError):
            continue
        if process_substring in cmdline:
            pids.append(pid)
    return sorted(pids)


def rss_kb(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except OSError:
        return None
    return None


def total_rss_kb(pids: list[int]) -> int | None:
    values = [value for pid in pids if (value := rss_kb(pid)) is not None]
    if not values:
        return None
    return sum(values)


def safe_label(label: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", label).strip("_")


def tail_file(path: Path, max_bytes: int = 8192) -> str:
    try:
        with path.open("rb") as file:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(max(0, size - max_bytes))
            return file.read().decode(errors="replace")
    except OSError:
        return ""


def start_query_process(args: argparse.Namespace, label: str) -> tuple[subprocess.Popen[Any], Path]:
    if args.dry_run:
        raise RuntimeError("managed query process is not available in dry-run mode")

    log_dir = Path(args.query_log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"analyze_histogram_{safe_label(label)}_{int(time.time() * 1000)}.log"
    cmd = [args.query_bin, "-c", args.query_config]
    printable = " ".join(shlex.quote(part) for part in cmd)
    print(f"\n[start query {label}] {printable} > {log_path}", flush=True)

    env = os.environ.copy()
    env.setdefault("RUST_BACKTRACE", "1")
    with log_path.open("ab") as log_file:
        process = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, env=env)
    return process, log_path


def wait_for_query(
    client: BendSQL,
    args: argparse.Namespace,
    process: subprocess.Popen[Any],
    log_path: Path,
    label: str,
) -> None:
    deadline = time.monotonic() + args.query_ready_timeout_sec
    cmd = build_bendsql_cmd(client, "SELECT 1")
    last_error = ""

    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            log_tail = tail_file(log_path)
            raise RuntimeError(
                f"databend-query exited while starting {label}, exit={exit_code}\n{log_tail}"
            )

        try:
            probe = subprocess.run(cmd, text=True, capture_output=True, timeout=5)
        except subprocess.SubprocessError as err:
            last_error = str(err)
        else:
            if probe.returncode == 0:
                print(f"[query ready {label}] pid={process.pid}", flush=True)
                return
            last_error = (probe.stderr or probe.stdout).strip()
        time.sleep(0.5)

    log_tail = tail_file(log_path)
    raise RuntimeError(
        f"timed out waiting for databend-query {label}: {last_error}\n{log_tail}"
    )


def stop_query_process(
    process: subprocess.Popen[Any],
    args: argparse.Namespace,
    label: str,
) -> None:
    if process.poll() is not None:
        return

    print(f"[stop query {label}] pid={process.pid}", flush=True)
    process.terminate()
    try:
        process.wait(timeout=args.query_shutdown_timeout_sec)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def with_managed_query(
    client: BendSQL,
    args: argparse.Namespace,
    label: str,
    callback: Callable[[list[int]], Any],
) -> Any:
    if args.dry_run:
        return callback([])

    process, log_path = start_query_process(args, label)
    try:
        wait_for_query(client, args, process, log_path, label)
        return callback([process.pid])
    finally:
        stop_query_process(process, args, label)


class RssMonitor:
    def __init__(self, pids: list[int], interval_sec: float) -> None:
        self.pids = pids
        self.interval_sec = interval_sec
        self.baseline_kb: int | None = total_rss_kb(pids)
        self.peak_kb: int | None = self.baseline_kb
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "RssMonitor":
        self._thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while not self._stop.is_set():
            current = total_rss_kb(self.pids)
            if current is not None:
                if self.baseline_kb is None:
                    self.baseline_kb = current
                if self.peak_kb is None or current > self.peak_kb:
                    self.peak_kb = current
            self._stop.wait(self.interval_sec)

    @property
    def delta_kb(self) -> int | None:
        if self.baseline_kb is None or self.peak_kb is None:
            return None
        return max(0, self.peak_kb - self.baseline_kb)


def build_bendsql_cmd(client: BendSQL, sql: str, output: str = "csv") -> list[str]:
    cmd = [client.binary, "--output", output, f"--query={sql}"]
    if client.dsn:
        cmd[1:1] = ["--dsn", client.dsn]
    return cmd


def run_measured_sql(
    client: BendSQL,
    sql: str,
    label: str,
    pids: list[int],
    sample_interval_sec: float,
) -> tuple[QueryResult, int | None, int | None, int | None]:
    cmd = build_bendsql_cmd(client, sql)
    printable = " ".join(shlex.quote(part) for part in cmd)
    print(f"\n[{label}] {printable}", flush=True)
    if client.dry_run:
        return QueryResult(label, sql, 0.0, "", ""), None, None, None

    start = time.monotonic()
    with RssMonitor(pids, sample_interval_sec) as monitor:
        proc = subprocess.run(cmd, text=True, capture_output=True)
    elapsed = time.monotonic() - start

    if proc.returncode != 0:
        print(proc.stdout, end="", file=sys.stdout)
        print(proc.stderr, end="", file=sys.stderr)
        raise RuntimeError(f"bendsql failed for {label}, exit={proc.returncode}")
    if proc.stdout.strip():
        print(proc.stdout.strip(), flush=True)
    if proc.stderr.strip():
        print(proc.stderr.strip(), file=sys.stderr, flush=True)

    return (
        QueryResult(label, sql, elapsed, proc.stdout, proc.stderr),
        monitor.baseline_kb,
        monitor.peak_kb,
        monitor.delta_kb,
    )


def analyze_sql(args: argparse.Namespace, algorithm: str) -> str:
    options = f"ALGORITHM = '{algorithm}'"
    if algorithm in {"kll_fast", "kll_full"}:
        options += f", ERROR_RATE = {args.kll_error_rate}"
    return session_sql(args, f"ANALYZE TABLE {fq_table(args.database, args.table)} WITH HISTOGRAM {options}")


def run_analyze(
    client: BendSQL,
    args: argparse.Namespace,
    algorithm: str,
    iteration: int,
    pids: list[int],
) -> AnalyzeResult:
    sql = analyze_sql(args, algorithm)
    result, baseline_kb, peak_kb, delta_kb = run_measured_sql(
        client,
        sql,
        f"analyze {algorithm} iter={iteration}",
        pids,
        args.sample_interval_sec,
    )
    return AnalyzeResult(
        algorithm=algorithm,
        elapsed_sec=result.elapsed_sec,
        query_pids=pids,
        rss_baseline_kb=baseline_kb,
        rss_peak_kb=peak_kb,
        rss_delta_kb=delta_kb,
    )


def parse_histogram(histogram: str) -> list[Bucket]:
    buckets: list[Bucket] = []
    for match in HISTOGRAM_RE.finditer(histogram):
        try:
            buckets.append(
                Bucket(
                    bucket_id=int(match.group("id")),
                    lower=float(match.group("min")),
                    upper=float(match.group("max")),
                    ndv=float(match.group("ndv")),
                    count=float(match.group("count")),
                )
            )
        except ValueError:
            continue
    return buckets


def load_histograms(client: BendSQL, args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    result = run_sql(
        client,
        f"""
        SELECT column_name, distinct_count, histogram
        FROM fuse_statistic('{args.database}', '{args.table}')
        WHERE column_name IN ('uniform_key', 'skew_key')
        ORDER BY column_name
        """,
        "load histograms",
        False,
    )
    histograms: dict[str, dict[str, Any]] = {}
    for row in csv_records(result.stdout):
        if len(row) < 3:
            continue
        column_name, distinct_count, histogram = row[0], row[1], row[2]
        histograms[column_name] = {
            "distinct_count": int(distinct_count) if distinct_count else None,
            "histogram": histogram,
            "buckets": parse_histogram(histogram),
        }
    return histograms


def build_probes(domain: int) -> list[Probe]:
    domain = max(domain, 10)
    small_width = max(1, domain // 100)
    medium_width = max(1, domain // 20)
    return [
        Probe("uniform_key", "eq", 0),
        Probe("uniform_key", "eq", domain // 4),
        Probe("uniform_key", "eq", domain // 2),
        Probe("uniform_key", "eq", domain - 1),
        Probe("uniform_key", "range", 0, small_width),
        Probe("uniform_key", "range", domain // 3, domain // 3 + small_width),
        Probe("uniform_key", "range", domain // 2, domain // 2 + medium_width),
        Probe("skew_key", "eq", 0),
        Probe("skew_key", "eq", domain // 4),
        Probe("skew_key", "range", 0, 0),
        Probe("skew_key", "range", 0, small_width),
        Probe("skew_key", "range", domain // 2, domain // 2 + medium_width),
    ]


def probe_distribution(probe: Probe) -> str:
    if probe.column == "uniform_key":
        return "uniform"
    if probe.column == "skew_key":
        return "skew"
    return probe.column


def exact_counts(client: BendSQL, args: argparse.Namespace, probes: list[Probe]) -> dict[str, float]:
    counts: dict[str, float] = {}
    table = fq_table(args.database, args.table)
    for probe in probes:
        result = run_sql(
            client,
            f"SELECT count() FROM {table} WHERE {probe.sql_predicate}",
            f"exact {probe.label}",
            False,
        )
        rows = csv_records(result.stdout)
        counts[probe.label] = float(rows[0][0]) if rows and rows[0] else 0.0
    return counts


def estimate_probe(buckets: list[Bucket], probe: Probe) -> float:
    if probe.kind == "eq":
        estimate = 0.0
        value = float(probe.lower)
        for bucket in buckets:
            if bucket.lower <= value <= bucket.upper:
                estimate += bucket.count / max(bucket.ndv, 1.0)
        return estimate

    assert probe.upper is not None
    lower = float(probe.lower)
    upper = float(probe.upper)
    estimate = 0.0
    for bucket in buckets:
        overlap_lower = max(lower, bucket.lower)
        overlap_upper = min(upper, bucket.upper)
        if overlap_lower > overlap_upper:
            continue
        if bucket.lower == bucket.upper:
            estimate += bucket.count
            continue
        bucket_width = bucket.upper - bucket.lower + 1.0
        overlap_width = overlap_upper - overlap_lower + 1.0
        estimate += bucket.count * max(0.0, min(1.0, overlap_width / bucket_width))
    return estimate


def evaluate_accuracy(
    algorithm: str,
    histograms: dict[str, dict[str, Any]],
    probes: list[Probe],
    counts: dict[str, float],
) -> list[AccuracyResult]:
    results: list[AccuracyResult] = []
    for probe in probes:
        buckets = histograms.get(probe.column, {}).get("buckets", [])
        estimate = estimate_probe(buckets, probe)
        exact = counts[probe.label]
        absolute = abs(estimate - exact)
        q_error = cardinality_q_error(estimate, exact)
        relative = None if exact == 0 else absolute / exact
        results.append(
            AccuracyResult(
                algorithm=algorithm,
                column=probe.column,
                distribution=probe_distribution(probe),
                predicate_kind=probe.kind,
                predicate=probe.label,
                exact_count=exact,
                estimated_count=estimate,
                absolute_error=absolute,
                q_error=q_error,
                relative_error=relative,
            )
        )
    return results


def cardinality_q_error(estimated: float, exact: float) -> float:
    estimated = max(estimated, 1.0)
    exact = max(exact, 1.0)
    return max(estimated / exact, exact / estimated)


def summarize_histograms(histograms: dict[str, dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for column, info in histograms.items():
        buckets: list[Bucket] = info["buckets"]
        summary[column] = {
            "distinct_count": info["distinct_count"],
            "num_buckets": len(buckets),
            "bucket_count_sum": sum(bucket.count for bucket in buckets),
            "bucket_ndv_sum": sum(bucket.ndv for bucket in buckets),
            "histogram_bytes": len(info["histogram"]),
        }
    return summary


def summarize_accuracy_values(values: list[AccuracyResult]) -> dict[str, Any]:
    q_errors = sorted(result.q_error for result in values)
    relatives = sorted(
        result.relative_error for result in values if result.relative_error is not None
    )
    absolutes = sorted(result.absolute_error for result in values)
    return {
        "probe_count": len(values),
        "q_error_count": len(q_errors),
        "max_q_error": max(q_errors) if q_errors else None,
        "mean_q_error": sum(q_errors) / len(q_errors) if q_errors else None,
        "geomean_q_error": geometric_mean(q_errors),
        "p50_q_error": percentile(q_errors, 0.50),
        "p90_q_error": percentile(q_errors, 0.90),
        "p95_q_error": percentile(q_errors, 0.95),
        "relative_probe_count": len(relatives),
        "max_absolute_error": max(absolutes) if absolutes else None,
        "mean_absolute_error": sum(absolutes) / len(absolutes) if absolutes else None,
        "max_relative_error": max(relatives) if relatives else None,
        "mean_relative_error": sum(relatives) / len(relatives) if relatives else None,
    }


def geometric_mean(values: list[float]) -> float | None:
    if not values:
        return None
    return math.exp(sum(math.log(value) for value in values) / len(values))


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    index = (len(values) - 1) * quantile
    lower_index = int(index)
    upper_index = min(lower_index + 1, len(values) - 1)
    fraction = index - lower_index
    return values[lower_index] * (1.0 - fraction) + values[upper_index] * fraction


def summarize_accuracy(results: list[AccuracyResult]) -> dict[str, Any]:
    by_algorithm: dict[str, list[AccuracyResult]] = {}
    for result in results:
        by_algorithm.setdefault(result.algorithm, []).append(result)

    summary: dict[str, Any] = {}
    for algorithm, values in by_algorithm.items():
        summary[algorithm] = summarize_accuracy_values(values)
    return summary


def summarize_accuracy_by_distribution(results: list[AccuracyResult]) -> dict[str, Any]:
    by_group: dict[str, dict[str, list[AccuracyResult]]] = {}
    for result in results:
        by_group.setdefault(result.algorithm, {}).setdefault(result.distribution, []).append(result)

    return {
        algorithm: {
            distribution: summarize_accuracy_values(values)
            for distribution, values in sorted(distributions.items())
        }
        for algorithm, distributions in sorted(by_group.items())
    }


def summarize_accuracy_by_kind(results: list[AccuracyResult]) -> dict[str, Any]:
    by_group: dict[str, dict[str, list[AccuracyResult]]] = {}
    for result in results:
        by_group.setdefault(result.algorithm, {}).setdefault(result.predicate_kind, []).append(
            result
        )

    return {
        algorithm: {
            predicate_kind: summarize_accuracy_values(values)
            for predicate_kind, values in sorted(kinds.items())
        }
        for algorithm, kinds in sorted(by_group.items())
    }


def summarize_accuracy_by_distribution_and_kind(
    results: list[AccuracyResult],
) -> dict[str, Any]:
    by_group: dict[str, dict[str, dict[str, list[AccuracyResult]]]] = {}
    for result in results:
        by_group.setdefault(result.algorithm, {}).setdefault(result.distribution, {}).setdefault(
            result.predicate_kind, []
        ).append(result)

    return {
        algorithm: {
            distribution: {
                predicate_kind: summarize_accuracy_values(values)
                for predicate_kind, values in sorted(kinds.items())
            }
            for distribution, kinds in sorted(distributions.items())
        }
        for algorithm, distributions in sorted(by_group.items())
    }


def main() -> None:
    args = parse_args()
    if args.rows <= 0 or args.batch_rows <= 0 or args.domain <= 0:
        raise ValueError("--rows, --batch-rows, and --domain must be greater than zero")
    if not 0 <= args.skew_percent <= 100:
        raise ValueError("--skew-percent must be between 0 and 100")

    algorithms = [item.strip() for item in args.algorithms.split(",") if item.strip()]
    for algorithm in algorithms:
        if algorithm not in {"window", "kll_fast", "kll_full"}:
            raise ValueError(f"unsupported algorithm: {algorithm}")

    probes = build_probes(args.domain)
    client = build_client(args)

    analyze_results: list[AnalyzeResult] = []
    accuracy_results: list[AccuracyResult] = []
    histogram_summaries: dict[str, Any] = {}
    sampled_pids: list[int] = []

    if args.run_mode == "sequential":
        sampled_pids = args.query_pid or discover_query_pids(args.query_process_substring)
        if sampled_pids:
            print(f"Sampling databend-query RSS from pids: {sampled_pids}", flush=True)
        else:
            print(
                "No databend-query PID discovered; RSS fields will be null. "
                "Pass --query-pid to enable memory sampling.",
                file=sys.stderr,
                flush=True,
            )

        prepare_table(client, args)
        counts = exact_counts(client, args, probes)

        for iteration in range(1, args.repeat + 1):
            for algorithm in algorithms:
                analyze_result = run_analyze(client, args, algorithm, iteration, sampled_pids)
                analyze_results.append(analyze_result)
                histograms = load_histograms(client, args)
                key = f"{algorithm}_iter_{iteration}"
                histogram_summaries[key] = summarize_histograms(histograms)
                accuracy_results.extend(evaluate_accuracy(algorithm, histograms, probes, counts))
    else:
        if args.query_pid:
            print(
                "--query-pid is ignored in --run-mode=isolated-query; managed query "
                "processes are sampled by their own PIDs.",
                file=sys.stderr,
                flush=True,
            )

        def setup(_: list[int]) -> dict[str, float]:
            prepare_table(client, args)
            return exact_counts(client, args, probes)

        counts = with_managed_query(client, args, "setup", setup)

        for iteration in range(1, args.repeat + 1):
            for algorithm in algorithms:

                def run_once(pids: list[int]) -> None:
                    analyze_result = run_analyze(client, args, algorithm, iteration, pids)
                    analyze_results.append(analyze_result)
                    histograms = load_histograms(client, args)
                    key = f"{algorithm}_iter_{iteration}"
                    histogram_summaries[key] = summarize_histograms(histograms)
                    accuracy_results.extend(
                        evaluate_accuracy(algorithm, histograms, probes, counts)
                    )

                with_managed_query(client, args, f"{algorithm}_iter_{iteration}", run_once)

    result = {
        "config": {
            "database": args.database,
            "table": args.table,
            "rows": args.rows,
            "batch_rows": args.batch_rows,
            "domain": args.domain,
            "skew_percent": args.skew_percent,
            "max_threads": args.max_threads,
            "kll_error_rate": args.kll_error_rate,
            "algorithms": algorithms,
            "repeat": args.repeat,
            "run_mode": args.run_mode,
            "query_bin": args.query_bin if args.run_mode == "isolated-query" else None,
            "query_config": args.query_config if args.run_mode == "isolated-query" else None,
            "query_pids": sampled_pids,
        },
        "analyze_results": [asdict(result) for result in analyze_results],
        "histogram_summaries": histogram_summaries,
        "accuracy_results": [asdict(result) for result in accuracy_results],
        "accuracy_summary": summarize_accuracy(accuracy_results),
        "accuracy_summary_by_distribution": summarize_accuracy_by_distribution(accuracy_results),
        "accuracy_summary_by_kind": summarize_accuracy_by_kind(accuracy_results),
        "accuracy_summary_by_distribution_and_kind": summarize_accuracy_by_distribution_and_kind(
            accuracy_results
        ),
    }

    print(json.dumps(result, indent=2, sort_keys=True), flush=True)
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(f"Wrote {args.output}", flush=True)

    if not args.keep:
        if args.run_mode == "sequential":
            run_sql(client, f"DROP DATABASE IF EXISTS {quote_ident(args.database)}", "drop database")
        else:

            def cleanup(_: list[int]) -> None:
                run_sql(
                    client,
                    f"DROP DATABASE IF EXISTS {quote_ident(args.database)}",
                    "drop database",
                )

            with_managed_query(client, args, "cleanup", cleanup)


if __name__ == "__main__":
    main()
