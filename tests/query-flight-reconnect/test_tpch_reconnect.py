#!/usr/bin/env python3

"""Run the complete TPC-H SF1 suite across three nodes while partitioning a
randomly selected live Flight TCP link. Both the injection time and partition
duration vary; packet counters and reconnect logs keep the assertions stable.
"""

import argparse
import random
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import mysql.connector


MYSQL_PORT = 3307
FLIGHT_PORTS = frozenset({9091, 9092, 9093})
IPTABLES_COMMENT_PREFIX = "databend-tpch-flight-reconnect"
RESET_IPTABLES_COMMENT = "databend-tpch-flight-reconnect-reset"
PARTITION_IPTABLES_COMMENT = "databend-tpch-flight-reconnect-partition"
POLL_INTERVAL_SECONDS = 0.1
QUERY_DISCOVERY_TIMEOUT_SECONDS = 30
FAULT_MATCH_TIMEOUT_SECONDS = 5
RECONNECT_TIMEOUT_SECONDS = 30
SUITE_TIMEOUT_SECONDS = 300
MAX_WORKLOAD_ROUNDS = 5
MAX_FAULT_ATTEMPTS = 60
FAULT_DELAY_RANGE_SECONDS = (0.2, 3.0)
PARTITION_DURATION_RANGE_SECONDS = (0.5, 2.5)
RECONNECT_LOG_MARKERS = (
    "do_exchange connection attempt failed",
    "do_exchange sender reconnected",
)
RECONNECT_CONFIRMED_MARKER = "do_exchange sender reconnected"


def chaos_log(message: str) -> None:
    timestamp = datetime.now(UTC).isoformat(timespec="milliseconds")
    print(f"{timestamp} TPCH_FLIGHT_CHAOS {message}", flush=True)


@dataclass(frozen=True, order=True)
class FlightLink:
    source_host: str
    source_port: int
    destination_host: str
    destination_port: int

    def __str__(self) -> str:
        return (
            f"{self.source_host}:{self.source_port}->"
            f"{self.destination_host}:{self.destination_port}"
        )


@dataclass(frozen=True)
class ActiveQuery:
    query_id: str
    sql: str


class OperationLog:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("w", encoding="utf-8", buffering=1)
        self._lock = threading.Lock()

    def record(self, message: str) -> None:
        timestamp = datetime.now(UTC).isoformat(timespec="milliseconds")
        line = f"{timestamp} {message}"
        with self._lock:
            chaos_log(message)
            self._file.write(f"{line}\n")

    def close(self) -> None:
        self._file.close()


class QueryLogWatcher:
    def __init__(self, repo_dir: Path):
        self._repo_dir = repo_dir
        self._offsets: dict[Path, int] = {}
        self._partials: dict[Path, str] = {}
        self._appended: list[tuple[Path, str]] = []
        self.checkpoint()

    def _log_paths(self) -> set[Path]:
        paths = set((self._repo_dir / ".databend").glob("query-*.out"))
        for node in range(1, 4):
            log_dir = self._repo_dir / f".databend/logs_{node}"
            if log_dir.exists():
                paths.update(path for path in log_dir.rglob("*") if path.is_file())
        return paths

    def _read_appended(self) -> None:
        for path in self._log_paths():
            try:
                size = path.stat().st_size
                offset = self._offsets.get(path, size)
                if size < offset:
                    offset = 0
                    self._partials.pop(path, None)
                if size > offset:
                    with path.open("rb") as log_file:
                        log_file.seek(offset)
                        chunk = log_file.read().decode(errors="replace")
                    text = self._partials.pop(path, "") + chunk
                    lines = text.splitlines(keepends=True)
                    for line in lines:
                        if line.endswith(("\n", "\r")):
                            self._appended.append(
                                (path, line.rstrip("\r\n").replace("\0", ""))
                            )
                        else:
                            self._partials[path] = line
                self._offsets[path] = size
            except FileNotFoundError:
                continue

    def checkpoint(self) -> None:
        self._read_appended()
        self._appended = []

    def _print_raw_log(self, path: Path, line: str) -> None:
        try:
            source = path.relative_to(self._repo_dir)
        except ValueError:
            source = path
        print(f"TPCH_FLIGHT_RAW_LOG source={source}", flush=True)
        print(line, flush=True)

    def wait_for_reconnect(self, stop: threading.Event) -> bool:
        deadline = time.monotonic() + RECONNECT_TIMEOUT_SECONDS
        inspected = 0
        while time.monotonic() < deadline and not stop.is_set():
            self._read_appended()
            reconnect_confirmed = False
            for path, line in self._appended[inspected:]:
                if any(marker in line for marker in RECONNECT_LOG_MARKERS):
                    self._print_raw_log(path, line)
                if RECONNECT_CONFIRMED_MARKER in line:
                    reconnect_confirmed = True
            inspected = len(self._appended)
            if reconnect_confirmed:
                return True
            stop.wait(POLL_INTERVAL_SECONDS)
        return False


class FlightPartitionFault:
    def __init__(self) -> None:
        self._link: FlightLink | None = None

    @staticmethod
    def _iptables(
        *args: str,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["sudo", "-n", "iptables", "-w", "5", *args],
            check=check,
            capture_output=True,
            text=True,
        )

    @staticmethod
    def _reset_rule(operation: str, link: FlightLink) -> list[str]:
        return [
            operation,
            "OUTPUT",
            "-p",
            "tcp",
            "-s",
            link.source_host,
            "--sport",
            str(link.source_port),
            "-d",
            link.destination_host,
            "--dport",
            str(link.destination_port),
            "-m",
            "comment",
            "--comment",
            RESET_IPTABLES_COMMENT,
            "-j",
            "REJECT",
            "--reject-with",
            "tcp-reset",
        ]

    @staticmethod
    def _partition_rule(operation: str, link: FlightLink) -> list[str]:
        return [
            operation,
            "OUTPUT",
            "-p",
            "tcp",
            "-s",
            link.source_host,
            "-d",
            link.destination_host,
            "--dport",
            str(link.destination_port),
            "-m",
            "comment",
            "--comment",
            PARTITION_IPTABLES_COMMENT,
            "-j",
            "DROP",
        ]

    def apply(self, link: FlightLink) -> None:
        self.clear()
        self._link = link
        try:
            # Install the broad partition first, then put the exact reset rule
            # ahead of it. The live connection receives RST while replacement
            # connections remain blackholed until the partition is removed.
            self._iptables(*self._partition_rule("-I", link))
            self._iptables(*self._reset_rule("-I", link))
        except BaseException:
            self.clear()
            raise

    def _clear_rule(self, rule: list[str]) -> None:
        while self._iptables(*rule, check=False).returncode == 0:
            pass

    def clear_reset(self) -> None:
        if self._link is not None:
            self._clear_rule(self._reset_rule("-D", self._link))

    def clear_partition(self) -> None:
        if self._link is not None:
            self._clear_rule(self._partition_rule("-D", self._link))

    def clear(self) -> None:
        if self._link is not None:
            self.clear_reset()
            self.clear_partition()
            self._link = None

        # Remove stale rules left by an interrupted previous run, but only when
        # they carry this test's unique comment prefix.
        while True:
            result = self._iptables("-L", "OUTPUT", "--line-numbers", "-n", check=False)
            matching_line = next(
                (
                    line.split()[0]
                    for line in result.stdout.splitlines()
                    if IPTABLES_COMMENT_PREFIX in line
                ),
                None,
            )
            if matching_line is None:
                return
            self._iptables("-D", "OUTPUT", matching_line, check=False)

    def matched_reset_packets(self) -> int:
        result = self._iptables("-L", "OUTPUT", "-v", "-n", "-x")
        return sum(
            int(line.split()[0])
            for line in result.stdout.splitlines()
            if RESET_IPTABLES_COMMENT in line
        )


def split_endpoint(endpoint: str) -> tuple[str, int]:
    host, port = endpoint.rsplit(":", 1)
    return host.strip("[]"), int(port)


def flight_links() -> set[FlightLink]:
    output = subprocess.run(
        ["ss", "-Hnt", "state", "established"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    links = set()
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        try:
            source_host, source_port = split_endpoint(fields[-2])
            destination_host, destination_port = split_endpoint(fields[-1])
        except ValueError:
            continue
        if destination_port not in FLIGHT_PORTS or source_port in FLIGHT_PORTS:
            continue
        links.add(
            FlightLink(
                source_host,
                source_port,
                destination_host,
                destination_port,
            )
        )
    return links


def connect_mysql() -> Any:
    deadline = time.monotonic() + QUERY_DISCOVERY_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return mysql.connector.connect(
                host="127.0.0.1",
                user="root",
                passwd="root",
                port=MYSQL_PORT,
                connection_timeout=5,
                autocommit=True,
            )
        except Exception as error:
            last_error = error
            time.sleep(0.5)
    raise AssertionError(f"could not connect to Databend: {last_error}") from last_error


def active_workload_query(cursor: Any) -> ActiveQuery | None:
    cursor.execute(
        "SELECT current_query_id, extra_info FROM system.processes "
        "WHERE current_query_id != '' AND extra_info != '' "
        "AND extra_info NOT LIKE '%system.processes%' LIMIT 1"
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return ActiveQuery(str(row[0]), " ".join(str(row[1]).split())[:240])


def wait_for_fault_match(
    fault: FlightPartitionFault,
    stop: threading.Event,
) -> int:
    deadline = time.monotonic() + FAULT_MATCH_TIMEOUT_SECONDS
    while time.monotonic() < deadline and not stop.is_set():
        matched = fault.matched_reset_packets()
        if matched > 0:
            return matched
        stop.wait(POLL_INTERVAL_SECONDS)
    return 0


class RandomLinkFaultInjector:
    def __init__(
        self,
        cursor: Any,
        fault: FlightPartitionFault,
        log_watcher: QueryLogWatcher,
        operations: OperationLog,
        stop: threading.Event,
    ):
        self._cursor = cursor
        self._fault = fault
        self._log_watcher = log_watcher
        self._operations = operations
        self._stop = stop
        self._random = random.SystemRandom()
        self.attempted = 0
        self.confirmed_reconnects = 0
        self.error: Exception | None = None

    def _wait_for_candidate(self) -> bool:
        while not self._stop.is_set():
            query = active_workload_query(self._cursor)
            if query is not None and flight_links():
                return True
            self._stop.wait(POLL_INTERVAL_SECONDS)
        return False

    def run(self) -> None:
        try:
            while not self._stop.is_set() and self.attempted < MAX_FAULT_ATTEMPTS:
                if not self._wait_for_candidate():
                    return

                delay = self._random.uniform(*FAULT_DELAY_RANGE_SECONDS)
                if self._stop.wait(delay):
                    return

                query = active_workload_query(self._cursor)
                before = flight_links()
                current_links = sorted(before)
                if query is None or not current_links:
                    continue

                selected = self._random.choice(current_links)
                partition_duration = self._random.uniform(
                    *PARTITION_DURATION_RANGE_SECONDS
                )
                self.attempted += 1
                self._operations.record(
                    f"attempt={self.attempted} random_delay={delay:.3f}s "
                    f"partition_duration={partition_duration:.3f}s "
                    f"query_id={query.query_id} sql={query.sql!r} "
                    f"selected_link={selected} candidates={len(current_links)}"
                )
                self._log_watcher.checkpoint()

                matched = 0
                partition_interrupted = False
                try:
                    self._fault.apply(selected)
                    matched = wait_for_fault_match(self._fault, self._stop)
                    if matched == 0:
                        self._operations.record(
                            f"attempt={self.attempted} result=no_packet_matched "
                            f"selected_link={selected}"
                        )
                        continue

                    # Once the selected live connection has received RST, keep
                    # the broader DROP rule active so reconnect attempts observe
                    # a real, randomly timed one-way network partition.
                    self._fault.clear_reset()
                    self._operations.record(
                        f"attempt={self.attempted} fault=tcp_reset_and_partition "
                        f"matched_packets={matched} selected_link={selected} "
                        f"partition_active=1"
                    )
                    partition_started = time.monotonic()
                    partition_interrupted = self._stop.wait(partition_duration)
                    actual_duration = time.monotonic() - partition_started
                    self._fault.clear_partition()
                    self._operations.record(
                        f"attempt={self.attempted} partition=healed "
                        f"planned_duration={partition_duration:.3f}s "
                        f"actual_duration={actual_duration:.3f}s"
                    )
                finally:
                    self._fault.clear()

                if partition_interrupted:
                    return

                reconnect_logged = self._log_watcher.wait_for_reconnect(self._stop)
                if not reconnect_logged:
                    self._operations.record(
                        f"attempt={self.attempted} result=reconnect_not_confirmed "
                        "reconnect_log=0"
                    )
                    continue

                current = flight_links()
                replacement = next(
                    (
                        link
                        for link in current - before
                        if link.destination_host == selected.destination_host
                        and link.destination_port == selected.destination_port
                    ),
                    None,
                )
                self.confirmed_reconnects += 1
                self._operations.record(
                    f"attempt={self.attempted} result=reconnected "
                    f"old_link={selected} new_link={replacement or 'not_observed'} "
                    f"reconnect_log=1 confirmed_reconnects={self.confirmed_reconnects}"
                )
        except Exception as error:
            self.error = error
            self._operations.record(
                f"injector=failed error={type(error).__name__}: {error}"
            )
            self._stop.set()
        finally:
            self._fault.clear()


def write_new_flight_suite(source: Path, destination: Path) -> None:
    prefix = """statement ok
set enable_experiment_new_flight = 1;

statement ok
set flight_connection_max_retry_times = 10;

statement ok
set flight_connection_retry_interval = 1;

statement ok
set group_by_shuffle_mode = 'before_partial';

"""
    destination.write_text(
        prefix + source.read_text(encoding="utf-8"), encoding="utf-8"
    )


def forward_output(process: subprocess.Popen[str]) -> None:
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)


def run_tpch_round(command: list[str], round_number: int) -> None:
    chaos_log(f"workload_round={round_number} starting command={' '.join(command)}")
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    output_thread = threading.Thread(
        target=forward_output,
        args=(process,),
        name="tpch-output",
        daemon=True,
    )
    output_thread.start()
    try:
        return_code = process.wait(timeout=SUITE_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        raise AssertionError(
            f"TPC-H round {round_number} exceeded {SUITE_TIMEOUT_SECONDS} seconds"
        )
    finally:
        output_thread.join(timeout=10)

    if return_code != 0:
        raise AssertionError(
            f"TPC-H sqllogictest round {round_number} exited with status {return_code}"
        )
    chaos_log(f"workload_round={round_number} passed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqllogictests", required=True, type=Path)
    parser.add_argument("--tpch-suite", required=True, type=Path)
    parser.add_argument("--operation-log", required=True, type=Path)
    parser.add_argument("--repo-dir", required=True, type=Path)
    args = parser.parse_args()

    operations = OperationLog(args.operation_log)
    fault = FlightPartitionFault()
    fault.clear()
    stop = threading.Event()
    connection = connect_mysql()
    cursor = connection.cursor()
    injector = RandomLinkFaultInjector(
        cursor,
        fault,
        QueryLogWatcher(args.repo_dir),
        operations,
        stop,
    )
    injector_thread = threading.Thread(
        target=injector.run,
        name="tpch-flight-fault-injector",
        daemon=True,
    )
    injector_started = False

    try:
        operations.record(
            "suite=starting randomness=system max_fault_attempts={} max_rounds={} "
            "fault_delay_range={} partition_duration_range={}".format(
                MAX_FAULT_ATTEMPTS,
                MAX_WORKLOAD_ROUNDS,
                FAULT_DELAY_RANGE_SECONDS,
                PARTITION_DURATION_RANGE_SECONDS,
            )
        )
        with tempfile.TemporaryDirectory(prefix="databend-tpch-reconnect-") as temp_dir:
            suite = Path(temp_dir) / "queries.test"
            write_new_flight_suite(args.tpch_suite, suite)
            command = [
                str(args.sqllogictests),
                "--handlers",
                "mysql",
                "--run",
                str(suite),
                "--parallel",
                "1",
            ]
            injector_thread.start()
            injector_started = True
            completed_rounds = 0
            for round_number in range(1, MAX_WORKLOAD_ROUNDS + 1):
                run_tpch_round(command, round_number)
                completed_rounds = round_number
                if injector.error is not None:
                    message = "fault injector failed during round {}: {}".format(
                        round_number, injector.error
                    )
                    raise AssertionError(message) from injector.error

        stop.set()
        injector_thread.join(timeout=RECONNECT_TIMEOUT_SECONDS)
        if injector_thread.is_alive():
            raise AssertionError("fault injector did not stop")
        if injector.error is not None:
            raise AssertionError(
                f"fault injector failed: {injector.error}"
            ) from injector.error
        if injector.confirmed_reconnects == 0:
            raise AssertionError(
                f"no reconnect was confirmed after {completed_rounds} complete TPC-H "
                f"rounds and {injector.attempted} random link selections"
            )
        operations.record(
            f"suite=passed rounds={completed_rounds} attempts={injector.attempted} "
            f"confirmed_reconnects={injector.confirmed_reconnects}"
        )
    except BaseException as error:
        operations.record(f"suite=failed error={type(error).__name__}: {error}")
        raise
    finally:
        stop.set()
        fault.clear()
        if injector_started:
            injector_thread.join(timeout=5)
        cursor.close()
        connection.close()
        operations.close()


if __name__ == "__main__":
    main()
