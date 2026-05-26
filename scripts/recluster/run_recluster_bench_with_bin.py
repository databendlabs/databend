#!/usr/bin/env python3
"""
Runner for comparing databend-query binaries with recluster_bench.py.

It starts databend-meta from target/debug or target/release, starts the provided
query binary, runs the benchmark, and shuts services down.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.bench_common.runner import (
    ROOT,
    find_meta_bin,
    start_services,
    terminate,
)

BENCH_SCRIPT = ROOT / "scripts/recluster/recluster_bench.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-bin", required=True)
    parser.add_argument("--meta-bin", default=None)
    parser.add_argument("--work-dir", default=str(ROOT / ".databend/recluster_bench"))
    parser.add_argument("--output", default=str(ROOT / "tmp/recluster_bench_result.json"))
    parser.add_argument(
        "--dsn",
        default=os.getenv("BENDSQL_DSN", "databend://root@127.0.0.1:8000/default?sslmode=disable"),
    )
    parser.add_argument("--bench-arg", action="append", default=[])
    parser.add_argument("--no-stop", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    query_bin = Path(args.query_bin).resolve()
    meta_bin = Path(args.meta_bin).resolve() if args.meta_bin else find_meta_bin()
    work_dir = Path(args.work_dir).resolve()

    if not query_bin.exists():
        raise FileNotFoundError(query_bin)
    if not meta_bin.exists():
        raise FileNotFoundError(meta_bin)

    meta_proc = None
    query_proc = None
    try:
        meta_proc, query_proc = start_services(query_bin, meta_bin, work_dir)

        cmd = [
            sys.executable,
            str(BENCH_SCRIPT),
            "--dsn",
            args.dsn,
            "--output",
            args.output,
        ]
        for item in args.bench_arg:
            cmd.extend(item.split())
        print("running:", " ".join(cmd), flush=True)
        subprocess.run(cmd, cwd=ROOT, check=True)
    finally:
        if not args.no_stop:
            terminate(query_proc)
            terminate(meta_proc)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
