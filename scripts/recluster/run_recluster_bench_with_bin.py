#!/usr/bin/env python3
"""
Runner for comparing databend-query binaries with recluster_bench.py.

It starts databend-meta from target/debug or target/release, starts the provided
query binary, runs the benchmark, and shuts services down.
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "scripts/ci/deploy/config"
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


def find_meta_bin() -> Path:
    for path in (ROOT / "target/release/databend-meta", ROOT / "target/debug/databend-meta"):
        if path.exists():
            return path
    raise FileNotFoundError("databend-meta not found in target/release or target/debug")


def wait_tcp(port: int, timeout: int = 60) -> None:
    subprocess.run(
        [sys.executable, str(ROOT / "scripts/ci/wait_tcp.py"), "--timeout", str(timeout), "--port", str(port)],
        check=True,
    )


def stop_existing() -> None:
    subprocess.run(["killall", "databend-query"], check=False)
    subprocess.run(["killall", "databend-meta"], check=False)
    time.sleep(1)
    subprocess.run(["killall", "-9", "databend-query"], check=False)
    subprocess.run(["killall", "-9", "databend-meta"], check=False)
    time.sleep(1)


def start_process(cmd: list[str], log: Path) -> subprocess.Popen[str]:
    log.parent.mkdir(parents=True, exist_ok=True)
    fd = log.open("w", encoding="utf-8")
    return subprocess.Popen(cmd, cwd=ROOT, stdout=fd, stderr=subprocess.STDOUT, text=True)


def terminate(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def main() -> int:
    args = parse_args()
    query_bin = Path(args.query_bin).resolve()
    meta_bin = Path(args.meta_bin).resolve() if args.meta_bin else find_meta_bin()
    work_dir = Path(args.work_dir).resolve()
    logs = work_dir / "logs"
    data = work_dir / "data"

    if not query_bin.exists():
        raise FileNotFoundError(query_bin)
    if not meta_bin.exists():
        raise FileNotFoundError(meta_bin)

    stop_existing()
    if work_dir.exists():
        shutil.rmtree(work_dir)
    logs.mkdir(parents=True, exist_ok=True)
    data.mkdir(parents=True, exist_ok=True)

    meta_proc = None
    query_proc = None
    try:
        meta_proc = start_process(
            [
                str(meta_bin),
                "-c",
                str(CONFIG_DIR / "databend-meta-node-1.toml"),
                "--raft-dir",
                str(data / "meta"),
                "--log-dir",
                str(logs / "meta"),
            ],
            logs / "meta.out",
        )
        wait_tcp(9191)

        query_proc = start_process(
            [
                str(query_bin),
                "-c",
                str(CONFIG_DIR / "databend-query-node-1.toml"),
                "--internal-enable-sandbox-tenant",
            ],
            logs / "query.out",
        )
        wait_tcp(8000)

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
