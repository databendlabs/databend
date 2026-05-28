#!/usr/bin/env python3
"""Common runner utilities for starting/stopping Databend services."""

from __future__ import annotations

import signal
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "scripts/ci/deploy/config"


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


def start_services(
    query_bin: Path,
    meta_bin: Path,
    work_dir: Path,
) -> tuple[subprocess.Popen[str], subprocess.Popen[str]]:
    """Start meta + query services, return (meta_proc, query_proc)."""
    import shutil

    logs = work_dir / "logs"
    data = work_dir / "data"

    stop_existing()
    if work_dir.exists():
        shutil.rmtree(work_dir)
    logs.mkdir(parents=True, exist_ok=True)
    data.mkdir(parents=True, exist_ok=True)

    meta_proc = start_process(
        [
            str(meta_bin),
            "-c", str(CONFIG_DIR / "databend-meta-node-1.toml"),
            "--raft-dir", str(data / "meta"),
            "--log-dir", str(logs / "meta"),
        ],
        logs / "meta.out",
    )
    wait_tcp(9191)

    query_proc = start_process(
        [
            str(query_bin),
            "-c", str(CONFIG_DIR / "databend-query-node-1.toml"),
            "--internal-enable-sandbox-tenant",
        ],
        logs / "query.out",
    )
    wait_tcp(8000)

    return meta_proc, query_proc
