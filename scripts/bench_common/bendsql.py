"""Common benchmark utilities shared across benchmark scripts."""

from __future__ import annotations

import csv
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class QueryResult:
    label: str
    sql: str
    elapsed_sec: float
    stdout: str
    stderr: str


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
            return QueryResult(label, sql, 0.0, "", "")

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

    def run_time_server(self, sql: str, label: str) -> float | None:
        """Run query with --time=server, return server-side duration in seconds."""
        cmd = [self.binary, "--time=server", f"--query={sql}"]
        if self.dsn:
            cmd[1:1] = ["--dsn", self.dsn]
        if self.dry_run:
            return None
        proc = subprocess.run(cmd, text=True, capture_output=True)
        if proc.returncode != 0:
            return None
        try:
            return float(proc.stdout.strip())
        except ValueError:
            return None


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
