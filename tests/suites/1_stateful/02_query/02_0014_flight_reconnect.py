#!/usr/bin/env python3

import queue
import socket
import subprocess
import threading
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any

import mysql.connector


QUERY_START_TIMEOUT_SECONDS = 30
QUERY_RESULT_TIMEOUT_SECONDS = 180
POLL_INTERVAL_SECONDS = 0.1
FLIGHT_PORTS = (9091, 9092, 9093)
IPTABLES_COMMENT = "databend-flight-reconnect-test"
MARKER = "flight_stateful_reconnect"
REPO_DIR = Path(__file__).resolve().parents[4]


class QueryTask:
    def __init__(self, connection: Any, sql: str):
        self._connection = connection
        self._sql = sql
        self._result: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)
        self._thread = threading.Thread(target=self._execute, daemon=True)

    def _execute(self) -> None:
        cursor = self._connection.cursor()
        try:
            cursor.execute(self._sql)
            self._result.put((True, cursor.fetchall()))
        except Exception as error:  # The caller reports the concrete query failure.
            self._result.put((False, error))
        finally:
            cursor.close()

    def start(self) -> None:
        self._thread.start()

    def is_running(self) -> bool:
        return self._thread.is_alive()

    def rows(self) -> list[tuple[Any, ...]]:
        self._thread.join(QUERY_RESULT_TIMEOUT_SECONDS)
        if self._thread.is_alive():
            raise AssertionError(
                f"query did not finish within {QUERY_RESULT_TIMEOUT_SECONDS} seconds"
            )
        succeeded, result = self._result.get_nowait()
        if not succeeded:
            raise AssertionError(
                f"query failed after Flight reconnect: {result}"
            ) from result
        return result


class QueryLogWatcher:
    def __init__(self) -> None:
        self._offsets = {
            path: path.stat().st_size for path in self._log_paths() if path.is_file()
        }
        self._appended = ""

    @staticmethod
    def _log_paths() -> set[Path]:
        paths = set(REPO_DIR.glob(".databend/query-*.out"))
        for node in range(1, 4):
            log_dir = REPO_DIR / f".databend/logs_{node}"
            if log_dir.exists():
                paths.update(path for path in log_dir.rglob("*") if path.is_file())
        return paths

    def _read_appended(self) -> None:
        appended = []
        for path in self._log_paths():
            try:
                size = path.stat().st_size
                offset = self._offsets.get(path, 0)
                if size < offset:
                    offset = 0
                if size > offset:
                    with path.open("rb") as log_file:
                        log_file.seek(offset)
                        appended.append(log_file.read().decode(errors="ignore"))
                self._offsets[path] = size
            except FileNotFoundError:
                continue
        self._appended += "".join(appended)

    def wait_for(
        self,
        message: str,
        *,
        query_id: str | None = None,
        task: QueryTask | None = None,
    ) -> None:
        deadline = time.monotonic() + QUERY_START_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            self._read_appended()
            for line in self._appended.replace("\0", "").splitlines():
                if message in line and (query_id is None or query_id in line):
                    return
            if task is not None and not task.is_running():
                raise AssertionError(f"query finished before logging {message!r}")
            time.sleep(POLL_INTERVAL_SECONDS)
        query_context = "" if query_id is None else f" for query {query_id}"
        raise AssertionError(
            "did not log {!r}{} within {} seconds".format(
                message,
                query_context,
                QUERY_START_TIMEOUT_SECONDS,
            )
        )

    def checkpoint(self) -> None:
        self._read_appended()
        self._appended = ""


class FlightResetFault:
    @staticmethod
    def _iptables(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["sudo", "-n", "iptables", "-w", "5", *args],
            check=check,
            capture_output=True,
            text=True,
        )

    @classmethod
    def _rule(cls, operation: str, port: int) -> list[str]:
        # A distributed exchange may connect in either direction, so reset
        # outbound traffic to every query node instead of assuming a sender.
        return [
            operation,
            "OUTPUT",
            "-p",
            "tcp",
            "--dport",
            str(port),
            "-m",
            "comment",
            "--comment",
            IPTABLES_COMMENT,
            "-j",
            "REJECT",
            "--reject-with",
            "tcp-reset",
        ]

    def apply(self) -> None:
        self.clear()
        try:
            for port in FLIGHT_PORTS:
                self._iptables(*self._rule("-I", port))
        except BaseException:
            self.clear()
            raise

    def clear(self) -> None:
        for port in FLIGHT_PORTS:
            rule = self._rule("-D", port)
            while self._iptables(*rule, check=False).returncode == 0:
                pass

    def matched_packets(self) -> int:
        output = self._iptables("-L", "OUTPUT", "-v", "-n", "-x").stdout
        return sum(
            int(line.split()[0])
            for line in output.splitlines()
            if IPTABLES_COMMENT in line
        )


def connect_mysql() -> Any:
    return mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        passwd="root",
        port=3307,
        connection_timeout=5,
        autocommit=True,
    )


def has_third_cluster_node() -> bool:
    with socket.socket() as probe:
        probe.settimeout(0.2)
        return probe.connect_ex(("127.0.0.1", FLIGHT_PORTS[-1])) == 0


def wait_for_three_nodes(cursor: Any) -> None:
    deadline = time.monotonic() + QUERY_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        cursor.execute("SELECT count() FROM system.clusters")
        if cursor.fetchone()[0] == 3:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError("three query nodes did not register in system.clusters")


def wait_for_query(cursor: Any, task: QueryTask) -> str:
    deadline = time.monotonic() + QUERY_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        cursor.execute(
            "SELECT current_query_id FROM system.processes "
            "WHERE extra_info LIKE '%"
            + MARKER
            + "%' AND extra_info NOT LIKE '%system.processes%' LIMIT 1"
        )
        row = cursor.fetchone()
        if row is not None:
            return str(row[0])
        if not task.is_running():
            raise AssertionError("distributed query finished before fault injection")
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError("distributed query did not appear in system.processes")


def wait_for_fault_match(fault: FlightResetFault, task: QueryTask) -> None:
    deadline = time.monotonic() + QUERY_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if fault.matched_packets() > 0:
            return
        if not task.is_running():
            raise AssertionError(
                "distributed query finished before the TCP reset matched"
            )
        time.sleep(POLL_INTERVAL_SECONDS)
    raise AssertionError("TCP reset rules did not match Flight traffic")


def main() -> None:
    if not has_third_cluster_node():
        print("skipped: requires three-node cluster")
        return

    with ExitStack() as cleanup:
        control_connection = connect_mysql()
        cleanup.callback(control_connection.close)
        query_connection = connect_mysql()
        cleanup.callback(query_connection.close)
        control_cursor = control_connection.cursor()
        cleanup.callback(control_cursor.close)
        fault = FlightResetFault()
        cleanup.callback(fault.clear)

        wait_for_three_nodes(control_cursor)
        log_watcher = QueryLogWatcher()

        rows_count = 1_000_000_000
        modulus = 1_000_000
        sql = f"""
            SETTINGS (
                enable_experiment_new_flight = 1,
                flight_connection_max_retry_times = 10,
                flight_connection_retry_interval = 1
            )
            SELECT count(), sum(number % {modulus})
            FROM (
                SELECT lhs.number
                FROM numbers_mt({rows_count}) AS lhs
                FULL OUTER JOIN numbers_mt(100003) AS rhs
                    ON lhs.number % 100003 = rhs.number
            ) AS {MARKER}
        """
        task = QueryTask(query_connection, sql)
        task.start()
        query_id = wait_for_query(control_cursor, task)
        log_watcher.wait_for("handle_do_exchange:", query_id=query_id, task=task)
        # Only accept retry/reconnect messages emitted after this fault.
        log_watcher.checkpoint()

        fault.apply()
        wait_for_fault_match(fault, task)
        log_watcher.wait_for("do_exchange connection attempt failed", task=task)
        fault.clear()

        log_watcher.wait_for("do_exchange sender reconnected", task=task)
        rows = task.rows()
        expected_sum = (rows_count // modulus) * modulus * (modulus - 1) // 2
        assert rows == [(rows_count, expected_sum)], rows
        print("reconnect succeeded")


if __name__ == "__main__":
    main()
