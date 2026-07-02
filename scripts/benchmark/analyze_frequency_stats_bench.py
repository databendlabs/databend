#!/usr/bin/env python3
"""
Local benchmark for ANALYZE TABLE frequency statistics.

The script creates a synthetic FUSE table with uniform, single-hot skewed, and
wide-hot columns, runs ANALYZE with TopN and count-min sketch disabled/enabled
in isolation and together, samples databend-query RSS while ANALYZE is running,
and compares optimizer EXPLAIN cardinality estimates with exact counts by
q-error.

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
import time
from collections.abc import Callable
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.bench_common.bendsql import BendSQL
from scripts.benchmark.analyze_histogram_bench import AccuracyResult
from scripts.benchmark.analyze_histogram_bench import AnalyzeResult
from scripts.benchmark.analyze_histogram_bench import DEFAULT_LOCAL_DSN
from scripts.benchmark.analyze_histogram_bench import Probe
from scripts.benchmark.analyze_histogram_bench import build_bendsql_cmd
from scripts.benchmark.analyze_histogram_bench import cardinality_q_error
from scripts.benchmark.analyze_histogram_bench import discover_query_pids
from scripts.benchmark.analyze_histogram_bench import exact_counts
from scripts.benchmark.analyze_histogram_bench import fq_table
from scripts.benchmark.analyze_histogram_bench import probe_distribution
from scripts.benchmark.analyze_histogram_bench import quote_ident
from scripts.benchmark.analyze_histogram_bench import run_measured_sql
from scripts.benchmark.analyze_histogram_bench import run_sql
from scripts.benchmark.analyze_histogram_bench import safe_label
from scripts.benchmark.analyze_histogram_bench import summarize_accuracy
from scripts.benchmark.analyze_histogram_bench import summarize_accuracy_by_distribution
from scripts.benchmark.analyze_histogram_bench import summarize_accuracy_by_distribution_and_kind
from scripts.benchmark.analyze_histogram_bench import summarize_accuracy_by_kind
from scripts.benchmark.analyze_histogram_bench import tail_file

ESTIMATED_ROWS_RE = re.compile(r"estimated rows:\s*(?P<rows>[-+0-9.eE]+)")


@dataclass(frozen=True)
class VariantSpec:
    label: str
    collect_top_n: bool
    collect_count_min_sketch: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare ANALYZE frequency-stat cost and optimizer estimate accuracy."
    )
    parser.add_argument("--dsn", default=os.getenv("BENDSQL_DSN"), help="bendsql DSN")
    parser.add_argument("--bendsql", default=os.getenv("BENDSQL", "bendsql"))
    parser.add_argument("--database", default="tmp_analyze_frequency_stats_bench")
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
    parser.add_argument(
        "--wide-hot-values",
        type=int,
        default=200,
        help=(
            "Number of hot values in wide_hot_key. Half of the rows are spread "
            "across these values, so TopN may not retain every hot value."
        ),
    )
    parser.add_argument("--max-threads", type=int, default=8)
    parser.add_argument(
        "--stat-columns",
        default="uniform_key,skew_key,wide_hot_key",
        help="Columns listed in analyze_frequency_columns for every benchmark variant.",
    )
    parser.add_argument("--top-n-size", type=int, default=50)
    parser.add_argument(
        "--cms-error-rate",
        type=float,
        default=0.001,
        help=(
            "Value for analyze_count_min_sketch_error_rate when CMS is enabled. "
            "The option is set to 0 for variants without CMS."
        ),
    )
    parser.add_argument(
        "--variants",
        default="none,topn,cms,topn_cms",
        help="Comma-separated variants to run: none,topn,cms,topn_cms.",
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
    parser.add_argument("--query-ready-timeout-sec", type=float, default=60.0)
    parser.add_argument("--query-shutdown-timeout-sec", type=float, default=10.0)
    parser.add_argument("--query-log-dir", default="target/bench-results")
    parser.add_argument("--query-pid", action="append", type=int, default=[])
    parser.add_argument("--query-process-substring", default="databend-query")
    parser.add_argument("--reuse-table", action="store_true")
    parser.add_argument("--keep", action="store_true", help="Do not drop database after run.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output", default=None, help="Write JSON result to this path.")
    return parser.parse_args()


def build_client(args: argparse.Namespace) -> BendSQL:
    if args.run_mode == "isolated-query" and not args.dsn:
        args.dsn = DEFAULT_LOCAL_DSN
    return BendSQL(args.bendsql, args.dsn, args.dry_run)


def session_sql(args: argparse.Namespace, sql: str) -> str:
    return "\n".join(
        [
            "SET enable_table_snapshot_stats = 1;",
            "SET enable_analyze_histogram = 0;",
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
                wide_hot_key UInt64,
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
                    IF(((number + {inserted}) % 2) = 0,
                        ((number + {inserted}) DIV 2) % {args.wide_hot_values},
                        {args.wide_hot_values}
                            + (((number + {inserted}) DIV 2)
                                % {args.domain - args.wide_hot_values})) AS wide_hot_key,
                    concat('payload_', to_string((number + {inserted}) % {args.domain})) AS payload
                FROM numbers({batch_rows})
            """
            run_sql(
                client,
                session_sql(args, insert_sql),
                f"insert batch offset={inserted}",
                False,
            )
            inserted += batch_rows
    else:
        run_sql(
            client,
            f"CREATE DATABASE IF NOT EXISTS {quote_ident(args.database)}",
            "ensure database",
        )

    run_sql(
        client,
        f"""
        SELECT
            count(),
            count(DISTINCT uniform_key),
            count(DISTINCT skew_key),
            count(DISTINCT wide_hot_key)
        FROM {table}
        """,
        "table cardinality",
    )


def parse_variants(variants: str) -> list[VariantSpec]:
    specs = []
    for item in variants.split(","):
        variant = item.strip()
        if not variant:
            continue
        if variant == "none":
            specs.append(
                VariantSpec(
                    label="none",
                    collect_top_n=False,
                    collect_count_min_sketch=False,
                )
            )
        elif variant == "topn":
            specs.append(
                VariantSpec(
                    label="topn",
                    collect_top_n=True,
                    collect_count_min_sketch=False,
                )
            )
        elif variant == "cms":
            specs.append(
                VariantSpec(
                    label="cms",
                    collect_top_n=False,
                    collect_count_min_sketch=True,
                )
            )
        elif variant == "topn_cms":
            specs.append(
                VariantSpec(
                    label="topn_cms",
                    collect_top_n=True,
                    collect_count_min_sketch=True,
                )
            )
        else:
            raise ValueError(f"unsupported variant: {variant}")
    return specs


def build_frequency_stat_probes(
    domain: int,
    skew_percent: int,
    wide_hot_values: int,
) -> list[Probe]:
    domain = max(domain, 10)
    return [
        Probe("skew_key", "eq", 0),
        Probe("skew_key", "eq", absent_skew_value(domain, skew_percent)),
        Probe("skew_key", "eq", existing_skew_value(domain, skew_percent)),
        Probe("uniform_key", "eq", 0),
        Probe("uniform_key", "eq", domain // 2),
        Probe("uniform_key", "eq", domain - 1),
        Probe("wide_hot_key", "eq", 0),
        Probe("wide_hot_key", "eq", wide_hot_values // 2),
        Probe("wide_hot_key", "eq", wide_hot_values - 1),
        Probe("wide_hot_key", "eq", wide_hot_values),
    ]


def absent_skew_value(domain: int, skew_percent: int) -> int:
    if skew_percent <= 0:
        return max(1, domain // 4)
    for value in [domain // 4, domain // 2, 1, 2, 3]:
        if 0 < value < domain and value % 100 < skew_percent:
            return value
    return 1


def existing_skew_value(domain: int, skew_percent: int) -> int:
    for value in [domain // 2, domain // 4, domain - 1, 1]:
        if 0 < value < domain and value % 100 >= skew_percent:
            return value
    for value in range(1, min(domain, 200)):
        if value % 100 >= skew_percent:
            return value
    return max(1, domain - 1)


def configure_variant(client: BendSQL, args: argparse.Namespace, spec: VariantSpec) -> None:
    top_n_size = args.top_n_size if spec.collect_top_n else 0
    cms_error_rate = args.cms_error_rate if spec.collect_count_min_sketch else 0
    run_sql(
        client,
        f"""
        ALTER TABLE {fq_table(args.database, args.table)} SET OPTIONS (
            approx_distinct_columns = '',
            analyze_frequency_columns = '{args.stat_columns}',
            analyze_top_n_size = {top_n_size},
            analyze_count_min_sketch_error_rate = '{cms_error_rate}'
        )
        """,
        f"configure {spec.label}",
        False,
    )


def analyze_sql(args: argparse.Namespace) -> str:
    return session_sql(args, f"ANALYZE TABLE {fq_table(args.database, args.table)}")


def run_analyze(
    client: BendSQL,
    args: argparse.Namespace,
    spec: VariantSpec,
    iteration: int,
    pids: list[int],
) -> AnalyzeResult:
    result, baseline_kb, peak_kb, delta_kb = run_measured_sql(
        client,
        analyze_sql(args),
        f"analyze {spec.label} iter={iteration}",
        pids,
        args.sample_interval_sec,
    )
    return AnalyzeResult(
        algorithm=spec.label,
        elapsed_sec=result.elapsed_sec,
        query_pids=pids,
        rss_baseline_kb=baseline_kb,
        rss_peak_kb=peak_kb,
        rss_delta_kb=delta_kb,
    )


def start_query_process(
    args: argparse.Namespace, label: str
) -> tuple[subprocess.Popen[Any], Path]:
    if args.dry_run:
        raise RuntimeError("managed query process is not available in dry-run mode")

    log_dir = Path(args.query_log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = (
        log_dir / f"analyze_frequency_stats_{safe_label(label)}_{int(time.time() * 1000)}.log"
    )
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


def explain_estimated_rows(client: BendSQL, args: argparse.Namespace, probe: Probe) -> float:
    result = run_sql(
        client,
        (
            f"EXPLAIN SELECT * FROM {fq_table(args.database, args.table)} "
            f"WHERE {probe.sql_predicate}"
        ),
        f"explain {probe.label}",
        False,
    )
    if client.dry_run:
        return 0.0
    matches = ESTIMATED_ROWS_RE.findall(result.stdout)
    if not matches:
        raise RuntimeError(f"cannot find estimated rows in EXPLAIN output for {probe.label}")
    return float(matches[-1])


def evaluate_variant(
    client: BendSQL,
    args: argparse.Namespace,
    spec: VariantSpec,
    probes: list[Probe],
    counts: dict[str, float],
) -> list[AccuracyResult]:
    results = []
    for probe in probes:
        estimated = explain_estimated_rows(client, args, probe)
        exact = counts[probe.label]
        absolute = abs(estimated - exact)
        relative = None if exact == 0 else absolute / exact
        results.append(
            AccuracyResult(
                algorithm=spec.label,
                column=probe.column,
                distribution=probe_distribution(probe),
                predicate_kind=probe.kind,
                predicate=probe.label,
                exact_count=exact,
                estimated_count=estimated,
                absolute_error=absolute,
                q_error=cardinality_q_error(estimated, exact),
                relative_error=relative,
            )
        )
    return results


def validate_args(args: argparse.Namespace, variants: list[VariantSpec]) -> None:
    if not variants:
        raise ValueError("--variants must list at least one variant")
    if args.rows <= 0 or args.batch_rows <= 0 or args.domain <= 0:
        raise ValueError("--rows, --batch-rows, and --domain must be greater than zero")
    if not 0 <= args.skew_percent <= 100:
        raise ValueError("--skew-percent must be between 0 and 100")
    if args.wide_hot_values <= 0:
        raise ValueError("--wide-hot-values must be greater than zero")
    if args.wide_hot_values >= args.domain:
        raise ValueError("--wide-hot-values must be smaller than --domain")
    if args.top_n_size < 0:
        raise ValueError("--top-n-size must be non-negative")
    if any(variant.collect_top_n for variant in variants) and args.top_n_size == 0:
        raise ValueError("--top-n-size must be greater than zero when a TopN variant is used")
    if (
        not math.isfinite(args.cms_error_rate)
        or args.cms_error_rate < 0
        or args.cms_error_rate > 1.0
    ):
        raise ValueError("--cms-error-rate must be finite and between 0 and 1")
    if any(variant.collect_count_min_sketch for variant in variants) and args.cms_error_rate == 0:
        raise ValueError("--cms-error-rate must be greater than zero when a CMS variant is used")
    if not args.stat_columns.strip():
        raise ValueError("--stat-columns must not be empty")


def main() -> None:
    args = parse_args()
    variants = parse_variants(args.variants)
    validate_args(args, variants)

    probes = build_frequency_stat_probes(
        args.domain,
        args.skew_percent,
        args.wide_hot_values,
    )
    client = build_client(args)
    analyze_results: list[AnalyzeResult] = []
    accuracy_results: list[AccuracyResult] = []
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
            for spec in variants:
                configure_variant(client, args, spec)
                analyze_results.append(
                    run_analyze(client, args, spec, iteration, sampled_pids)
                )
                accuracy_results.extend(evaluate_variant(client, args, spec, probes, counts))
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
            for spec in variants:

                def run_once(pids: list[int]) -> None:
                    configure_variant(client, args, spec)
                    analyze_results.append(run_analyze(client, args, spec, iteration, pids))
                    accuracy_results.extend(evaluate_variant(client, args, spec, probes, counts))

                with_managed_query(client, args, f"{spec.label}_iter_{iteration}", run_once)

    result: dict[str, Any] = {
        "config": {
            "database": args.database,
            "table": args.table,
            "rows": args.rows,
            "batch_rows": args.batch_rows,
            "domain": args.domain,
            "skew_percent": args.skew_percent,
            "wide_hot_values": args.wide_hot_values,
            "max_threads": args.max_threads,
            "stat_columns": args.stat_columns,
            "top_n_size": args.top_n_size,
            "cms_error_rate": args.cms_error_rate,
            "variants": [spec.label for spec in variants],
            "variant_specs": [asdict(spec) for spec in variants],
            "repeat": args.repeat,
            "run_mode": args.run_mode,
            "query_bin": args.query_bin if args.run_mode == "isolated-query" else None,
            "query_config": args.query_config if args.run_mode == "isolated-query" else None,
            "query_pids": sampled_pids,
        },
        "analyze_results": [asdict(result) for result in analyze_results],
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
            run_sql(
                client,
                f"DROP DATABASE IF EXISTS {quote_ident(args.database)}",
                "drop database",
            )
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
