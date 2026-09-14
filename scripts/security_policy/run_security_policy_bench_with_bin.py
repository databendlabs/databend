#!/usr/bin/env python3
"""
Runner for security policy benchmarks.

Starts databend-meta and databend-query from specified binaries, runs
security_policy_bench.py, and shuts services down. Supports --compare-bin
for cross-version A/B comparison.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.bench_common.runner import (
    ROOT,
    find_meta_bin,
    start_services,
    stop_existing,
    terminate,
)

BENCH_SCRIPT = ROOT / "scripts/security_policy/security_policy_bench.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-bin", required=True, help="Path to databend-query binary")
    parser.add_argument("--meta-bin", default=None, help="Path to databend-meta binary")
    parser.add_argument("--compare-bin", default=None, help="Second query binary for A/B comparison")
    parser.add_argument("--work-dir", default=str(ROOT / ".databend/security_policy_bench"))
    parser.add_argument("--output", default=str(ROOT / "tmp/security_policy_bench_result.json"))
    parser.add_argument(
        "--dsn",
        default=os.getenv("BENDSQL_DSN", "databend://root@127.0.0.1:8000/default?sslmode=disable"),
    )
    parser.add_argument("--bench-arg", action="append", default=[])
    parser.add_argument("--no-stop", action="store_true", help="Leave services running after bench")
    return parser.parse_args()


def run_bench(dsn: str, output: str, extra_args: list[str]) -> dict:
    cmd = [
        sys.executable,
        str(BENCH_SCRIPT),
        "--dsn", dsn,
        "--output", output,
    ]
    for item in extra_args:
        cmd.extend(item.split())
    print(f"running: {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)
    with open(output, encoding="utf-8") as f:
        return json.load(f)


def compare_results(result_a: dict, result_b: dict) -> dict:
    comparison = []
    results_a = {(r["complexity"], r["pattern"], r["mode"], r["cache"]): r for r in result_a.get("results", [])}
    results_b = {(r["complexity"], r["pattern"], r["mode"], r["cache"]): r for r in result_b.get("results", [])}

    for key in results_a:
        if key not in results_b:
            continue
        ma = results_a[key]["metrics"]
        mb = results_b[key]["metrics"]
        dur_a = ma["duration_ms"]
        dur_b = mb["duration_ms"]
        diff_pct = ((dur_b - dur_a) / dur_a * 100) if dur_a > 0 else 0
        comparison.append({
            "complexity": key[0],
            "pattern": key[1],
            "mode": key[2],
            "cache": key[3],
            "binary_a_ms": dur_a,
            "binary_b_ms": dur_b,
            "diff_pct": round(diff_pct, 2),
            "regression": diff_pct > 5.0,
        })

    return {"comparison": comparison}


def main() -> int:
    args = parse_args()
    query_bin = Path(args.query_bin).resolve()
    meta_bin = Path(args.meta_bin).resolve() if args.meta_bin else find_meta_bin()
    work_dir = Path(args.work_dir).resolve()

    if not query_bin.exists():
        raise FileNotFoundError(query_bin)
    if not meta_bin.exists():
        raise FileNotFoundError(meta_bin)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    meta_proc = None
    query_proc = None
    try:
        meta_proc, query_proc = start_services(query_bin, meta_bin, work_dir)
        result_a = run_bench(args.dsn, args.output, args.bench_arg)
    finally:
        if not args.no_stop:
            terminate(query_proc)
            terminate(meta_proc)

    if args.compare_bin:
        compare_bin = Path(args.compare_bin).resolve()
        if not compare_bin.exists():
            raise FileNotFoundError(compare_bin)

        output_b = str(output_path.with_stem(output_path.stem + "_b"))
        try:
            meta_proc, query_proc = start_services(compare_bin, meta_bin, work_dir)
            result_b = run_bench(args.dsn, output_b, args.bench_arg)
        finally:
            if not args.no_stop:
                terminate(query_proc)
                terminate(meta_proc)

        diff = compare_results(result_a, result_b)
        diff_path = str(output_path.with_stem(output_path.stem + "_diff"))
        with open(diff_path, "w", encoding="utf-8") as f:
            json.dump(diff, f, indent=2)
        print(f"\nComparison written to: {diff_path}")

        regressions = [c for c in diff["comparison"] if c["regression"]]
        if regressions:
            print(f"\n*** {len(regressions)} REGRESSIONS DETECTED (>5% slower) ***")
            for r in regressions:
                print(f"  {r['complexity']}/{r['pattern']}/{r['mode']}: "
                      f"{r['binary_a_ms']:.1f}ms -> {r['binary_b_ms']:.1f}ms ({r['diff_pct']:+.1f}%)")
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
