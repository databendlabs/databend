#!/usr/bin/env python3

import argparse
import queue
import random
import subprocess
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import mysql.connector


MYSQL_PORT = 3307
QUERY_START_TIMEOUT = 30
QUERY_RESULT_TIMEOUT = 180
KEEPALIVE_DETECTION_TIMEOUT = 10
TPCH_REPLACEMENT_TIMEOUT = 15
MIN_TPCH_CHAOS_OPERATIONS = 3
MAX_TPCH_CHAOS_OPERATIONS = 12
POLL_INTERVAL = 0.1
WORKER_PODS = ("databend-query-1", "databend-query-2")


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(command), flush=True)
    return subprocess.run(command, check=check, text=True)


@dataclass(frozen=True)
class QueryIdentity:
    query_id: str
    connection_id: int


class ChaosOperationLog:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._file = path.open("w", encoding="utf-8", buffering=1)
        self._lock = threading.Lock()

    def record(self, message: str) -> None:
        line = f"{datetime.now(UTC).isoformat()} {message}"
        with self._lock:
            print(f"[cluster-chaos] {line}", flush=True)
            self._file.write(f"{line}\n")

    def close(self) -> None:
        self._file.close()


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
        except Exception as error:  # The caller asserts the concrete server outcome.
            self._result.put((False, error))
        finally:
            cursor.close()

    def start(self) -> None:
        self._thread.start()

    def is_running(self) -> bool:
        return self._thread.is_alive()

    def outcome(self, timeout: float = QUERY_RESULT_TIMEOUT) -> tuple[bool, Any]:
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise AssertionError(f"query did not finish within {timeout} seconds")
        return self._result.get_nowait()

    def rows(self, timeout: float = QUERY_RESULT_TIMEOUT) -> list[tuple[Any, ...]]:
        succeeded, result = self.outcome(timeout)
        if not succeeded:
            raise AssertionError(f"query failed unexpectedly: {result}") from result
        return result

    def error(self, timeout: float = QUERY_RESULT_TIMEOUT) -> Exception:
        succeeded, result = self.outcome(timeout)
        if succeeded:
            raise AssertionError(f"killed query completed unexpectedly: {result}")
        return result


class NetworkFault:
    def __init__(self, namespace: str, manifest: Path, coordinator_ip: str):
        self._namespace = namespace
        self._manifest = manifest
        self._coordinator_ip = coordinator_ip
        self._active = False

    def _exec_worker(
        self,
        pod: str,
        command: str,
        *,
        container: str = "net-admin",
        check: bool = True,
    ) -> None:
        run(
            [
                "kubectl",
                "-n",
                self._namespace,
                "exec",
                pod,
                "-c",
                container,
                "--",
                "sh",
                "-c",
                command,
            ],
            check=check,
        )

    def apply_partition(self) -> None:
        if self._active:
            raise AssertionError("network fault is already active")
        run(["kubectl", "apply", "-f", str(self._manifest)])
        self._active = True
        try:
            run(
                [
                    "kubectl",
                    "-n",
                    self._namespace,
                    "wait",
                    "--for=condition=AllInjected",
                    "networkchaos/worker-to-coordinator",
                    "--timeout=60s",
                ]
            )
        except Exception:
            self.recover()
            raise

    def reset_connections(self) -> None:
        # A partition blocks retries but does not reset established TCP streams. Put
        # the RST rule first so the active Flight transports fail immediately.
        for pod in WORKER_PODS:
            self._exec_worker(
                pod,
                "iptables -I OUTPUT 1 "
                f"-p tcp -d {self._coordinator_ip} --dport 9090 "
                "-j REJECT --reject-with tcp-reset",
            )

    def clear_resets(self) -> None:
        rule = (
            "iptables -D OUTPUT "
            f"-p tcp -d {self._coordinator_ip} --dport 9090 "
            "-j REJECT --reject-with tcp-reset"
        )
        for pod in WORKER_PODS:
            self._exec_worker(pod, f"while {rule} 2>/dev/null; do :; done", check=False)

    def recover(self) -> None:
        self.clear_resets()
        if self._active:
            run(
                [
                    "kubectl",
                    "delete",
                    "-f",
                    str(self._manifest),
                    "--ignore-not-found",
                    "--wait=true",
                    "--timeout=60s",
                ]
            )
            self._active = False

    @contextmanager
    def reset_then_partition(self) -> Iterator[None]:
        self.apply_partition()
        try:
            self.reset_connections()
            time.sleep(1)
            self.clear_resets()
            yield
        finally:
            self.recover()

    @contextmanager
    def reset_for_retry(self) -> Iterator[None]:
        try:
            self.reset_connections()
            time.sleep(1)
            self.clear_resets()
            yield
        finally:
            self.clear_resets()

    @contextmanager
    def keepalive_partition(self) -> Iterator[None]:
        paused = []
        try:
            # Freeze userspace after the Flight transports exist. Kernel TCP timers
            # continue running, so a silent partition can only be detected by keepalive.
            for pod in WORKER_PODS:
                self._exec_worker(pod, "kill -STOP 1", container="query")
                paused.append(pod)
            self.apply_partition()
            yield
        finally:
            try:
                self.recover()
            finally:
                for pod in paused:
                    self._exec_worker(pod, "kill -CONT 1", container="query")


def connect_mysql() -> Any:
    deadline = time.monotonic() + 60
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return mysql.connector.connect(
                host="127.0.0.1",
                user="root",
                password="",
                port=MYSQL_PORT,
                connection_timeout=5,
                autocommit=True,
            )
        except Exception as error:
            last_error = error
            time.sleep(0.5)
    raise AssertionError(f"could not connect to Databend: {last_error}") from last_error


def configure_session(
    connection: Any, retry_interval: int, *, keep_alive: bool = False
) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute("SET flight_connection_max_retry_times = 10")
        cursor.execute(f"SET flight_connection_retry_interval = {retry_interval}")
        cursor.execute("SET group_by_shuffle_mode = 'before_partial'")
        keep_alive_value = 1 if keep_alive else 0
        cursor.execute(f"SET flight_client_keep_alive_time_secs = {keep_alive_value}")
        cursor.execute(
            f"SET flight_client_keep_alive_interval_secs = {keep_alive_value}"
        )
        cursor.execute(
            f"SET flight_client_keep_alive_retries = {2 if keep_alive else 0}"
        )
    finally:
        cursor.close()


def wait_for_cluster(cursor: Any) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        cursor.execute("SELECT count() FROM system.clusters")
        if cursor.fetchone()[0] == 3:
            return
        time.sleep(0.5)
    raise AssertionError("three query nodes did not register in system.clusters")


def wait_for_query(cursor: Any, marker: str, task: QueryTask) -> QueryIdentity:
    deadline = time.monotonic() + QUERY_START_TIMEOUT
    while time.monotonic() < deadline:
        cursor.execute(
            "SELECT current_query_id, mysql_connection_id FROM system.processes "
            f"WHERE extra_info LIKE '%{marker}%' "
            "AND extra_info NOT LIKE '%system.processes%' "
            "AND mysql_connection_id IS NOT NULL LIMIT 1"
        )
        row = cursor.fetchone()
        if row is not None:
            return QueryIdentity(str(row[0]), int(row[1]))
        if not task.is_running():
            raise AssertionError(f"query {marker} finished before fault injection")
        time.sleep(POLL_INTERVAL)
    raise AssertionError(f"query {marker} did not appear in system.processes")


def is_query_running(cursor: Any, query_id: str) -> bool:
    cursor.execute(
        "SELECT count() FROM system.processes "
        f"WHERE current_query_id = '{query_id}' "
        "AND extra_info NOT LIKE '%system.processes%'"
    )
    return cursor.fetchone()[0] != 0


def wait_for_query_to_stop(cursor: Any, query_id: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not is_query_running(cursor, query_id):
            return
        time.sleep(POLL_INTERVAL)
    raise AssertionError(f"query {query_id} remained in system.processes")


def flight_endpoints(
    namespace: str, coordinator_ip: str, *, require_keepalive: bool = False
) -> dict[str, set[str]]:
    remote = f"{coordinator_ip}:9090"
    result: dict[str, set[str]] = {}
    for pod in WORKER_PODS:
        output = subprocess.run(
            [
                "kubectl",
                "-n",
                namespace,
                "exec",
                pod,
                "-c",
                "net-admin",
                "--",
                "ss",
                "-Hton",
                "state",
                "established",
                "dst",
                remote,
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        endpoints = set()
        for line in output.splitlines():
            if require_keepalive and "timer:(keepalive" not in line:
                continue
            fields = line.split()
            if remote in fields:
                endpoints.add(fields[fields.index(remote) - 1])
        result[pod] = endpoints
    return result


def wait_for_flight_connections(
    namespace: str,
    coordinator_ip: str,
    task: QueryTask,
    *,
    require_keepalive: bool = False,
    exclude: dict[str, set[str]] | None = None,
) -> dict[str, set[str]]:
    deadline = time.monotonic() + QUERY_START_TIMEOUT
    pending = set(WORKER_PODS)
    while time.monotonic() < deadline:
        current = flight_endpoints(
            namespace, coordinator_ip, require_keepalive=require_keepalive
        )
        selected = {
            pod: current[pod] - (exclude[pod] if exclude is not None else set())
            for pod in WORKER_PODS
        }
        for pod in tuple(pending):
            if selected[pod]:
                pending.remove(pod)
        if not pending:
            return selected
        if not task.is_running():
            raise AssertionError(
                "query finished before Flight connections were established"
            )
        time.sleep(POLL_INTERVAL)
    raise AssertionError(
        f"workers did not establish Flight connections: {sorted(pending)}"
    )


def wait_for_keepalive_disconnect(
    namespace: str,
    coordinator_ip: str,
    original: dict[str, set[str]],
    task: QueryTask,
) -> None:
    deadline = time.monotonic() + KEEPALIVE_DETECTION_TIMEOUT
    while time.monotonic() < deadline:
        current = flight_endpoints(namespace, coordinator_ip)
        # Some exchange streams may already have queued data and use TCP's persist
        # timer. One newly created idle stream per worker is enough to exercise keepalive.
        if all(original[pod] - current[pod] for pod in WORKER_PODS):
            return
        if not task.is_running():
            raise AssertionError(
                "query finished before TCP keepalive detected the partition"
            )
        time.sleep(POLL_INTERVAL)
    raise AssertionError("TCP keepalive did not close the original Flight connections")


def wait_for_replacement_connections(
    namespace: str,
    coordinator_ip: str,
    previous: dict[str, set[str]],
    task: QueryTask,
) -> None:
    deadline = time.monotonic() + QUERY_START_TIMEOUT
    pending = set(WORKER_PODS)
    while time.monotonic() < deadline:
        current = flight_endpoints(namespace, coordinator_ip, require_keepalive=True)
        for pod in tuple(pending):
            if current[pod] - previous[pod]:
                pending.remove(pod)
        if not pending:
            return
        if not task.is_running():
            raise AssertionError(
                "query finished before keepalive connections were replaced"
            )
        time.sleep(POLL_INTERVAL)
    raise AssertionError(
        f"workers did not replace keepalive connections: {sorted(pending)}"
    )


def active_workload_query(cursor: Any) -> tuple[str, str] | None:
    cursor.execute(
        "SELECT current_query_id, extra_info FROM system.processes "
        "WHERE extra_info NOT LIKE '%system.processes%' LIMIT 1"
    )
    row = cursor.fetchone()
    if row is None:
        return None
    sql = " ".join(str(row[1]).split())[:240]
    return str(row[0]), sql


def format_endpoints(endpoints: dict[str, set[str]]) -> str:
    return " ".join(
        f"{pod}=[{','.join(sorted(endpoints[pod]))}]" for pod in WORKER_PODS
    )


class TpchChaosInjector:
    def __init__(
        self,
        namespace: str,
        coordinator_ip: str,
        fault: NetworkFault,
        stop: threading.Event,
        operations: ChaosOperationLog,
    ):
        self._namespace = namespace
        self._coordinator_ip = coordinator_ip
        self._fault = fault
        self._stop = stop
        self._operations = operations
        self._random = random.SystemRandom()
        self._modes: list[str] = []
        self.attempted = 0
        self.confirmed = 0
        self.error: Exception | None = None

    def _next_mode(self) -> str:
        if not self._modes:
            self._modes = ["rst", "rst+partition"]
            self._random.shuffle(self._modes)
        return self._modes.pop()

    def _wait_for_workload_transport(self, cursor: Any) -> bool:
        while not self._stop.is_set():
            if active_workload_query(cursor) is not None:
                endpoints = flight_endpoints(self._namespace, self._coordinator_ip)
                if all(endpoints[pod] for pod in WORKER_PODS):
                    return True
            self._stop.wait(POLL_INTERVAL)
        return False

    def _wait_for_disconnect(
        self, before: dict[str, set[str]], timeout: float
    ) -> dict[str, set[str]] | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and not self._stop.is_set():
            current = flight_endpoints(self._namespace, self._coordinator_ip)
            removed = {pod: before[pod] - current[pod] for pod in WORKER_PODS}
            if all(removed[pod] for pod in WORKER_PODS):
                return removed
            self._stop.wait(POLL_INTERVAL)
        return None

    def _wait_for_replacement(
        self, before: dict[str, set[str]]
    ) -> dict[str, set[str]] | None:
        deadline = time.monotonic() + TPCH_REPLACEMENT_TIMEOUT
        while time.monotonic() < deadline and not self._stop.is_set():
            current = flight_endpoints(self._namespace, self._coordinator_ip)
            replacements = {pod: current[pod] - before[pod] for pod in WORKER_PODS}
            if all(replacements[pod] for pod in WORKER_PODS):
                return replacements
            self._stop.wait(POLL_INTERVAL)
        return None

    def _inject(
        self,
        operation: int,
        mode: str,
        before: dict[str, set[str]],
        rst_timeout: float,
        partition_duration: float,
    ) -> None:
        partition_applied = False
        removed: dict[str, set[str]] | None = None
        try:
            if mode == "rst+partition":
                self._fault.apply_partition()
                partition_applied = True
                self._operations.record(f"operation={operation} partition=applied")

            self._fault.reset_connections()
            self._operations.record(f"operation={operation} rst=applied")
            removed = self._wait_for_disconnect(before, rst_timeout)
        finally:
            self._fault.clear_resets()
            self._operations.record(f"operation={operation} rst=removed")
            if partition_applied:
                if removed is not None and not self._stop.is_set():
                    self._operations.record(
                        f"operation={operation} "
                        f"partition_hold={partition_duration:.3f}s"
                    )
                    self._stop.wait(partition_duration)
                self._fault.recover()
                self._operations.record(f"operation={operation} partition=recovered")

        if removed is None:
            self._operations.record(
                f"operation={operation} result=no_old_connection_closed"
            )
            return

        self._operations.record(
            f"operation={operation} old_connections_closed "
            f"{format_endpoints(removed)}"
        )
        replacements = self._wait_for_replacement(before)
        if replacements is None:
            self._operations.record(
                f"operation={operation} result=no_replacement_observed"
            )
            return

        self.confirmed += 1
        self._operations.record(
            f"operation={operation} result=reconnected "
            f"{format_endpoints(replacements)}"
        )

    def run(self, workload_started: threading.Event) -> None:
        connection = None
        cursor = None
        try:
            connection = connect_mysql()
            cursor = connection.cursor()
            while not workload_started.wait(POLL_INTERVAL):
                if self._stop.is_set():
                    return
            self._operations.record("injector=started")

            while (
                not self._stop.is_set()
                and self.attempted < MAX_TPCH_CHAOS_OPERATIONS
            ):
                if not self._wait_for_workload_transport(cursor):
                    return

                delay = self._random.uniform(0.5, 3.0)
                if self._stop.wait(delay):
                    return
                active_query = active_workload_query(cursor)
                if active_query is None:
                    continue

                before = flight_endpoints(self._namespace, self._coordinator_ip)
                if not all(before[pod] for pod in WORKER_PODS):
                    continue

                self.attempted += 1
                mode = self._next_mode()
                rst_timeout = self._random.uniform(0.8, 1.5)
                partition_duration = (
                    self._random.uniform(0.5, 2.5) if mode == "rst+partition" else 0
                )
                query_id, sql = active_query
                self._operations.record(
                    f"operation={self.attempted} mode={mode} delay={delay:.3f}s "
                    f"rst_timeout={rst_timeout:.3f}s "
                    f"partition_duration={partition_duration:.3f}s "
                    f"query_id={query_id} sql={sql!r} "
                    f"before={format_endpoints(before)}"
                )
                self._inject(
                    self.attempted,
                    mode,
                    before,
                    rst_timeout,
                    partition_duration,
                )
            if self.attempted == MAX_TPCH_CHAOS_OPERATIONS:
                self._operations.record(
                    f"injector=completed max_operations={self.attempted}"
                )
        except Exception as error:
            self.error = error
            self._operations.record(
                f"injector=failed error={type(error).__name__}: {error}"
            )
            self._stop.set()
        finally:
            self._fault.recover()
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


class ReconnectHarness:
    def __init__(
        self,
        query_connection: Any,
        control_cursor: Any,
        kill_connection: Any,
        fault: NetworkFault,
        namespace: str,
        coordinator_ip: str,
    ):
        self._query_connection = query_connection
        self._control_cursor = control_cursor
        self._kill_connection = kill_connection
        self._fault = fault
        self._namespace = namespace
        self._coordinator_ip = coordinator_ip

    def _start_query(
        self,
        sql: str,
        marker: str,
        retry_interval: int,
        *,
        keep_alive: bool = False,
    ) -> tuple[QueryTask, QueryIdentity]:
        configure_session(self._query_connection, retry_interval, keep_alive=keep_alive)
        task = QueryTask(self._query_connection, sql)
        task.start()
        identity = wait_for_query(self._control_cursor, marker, task)
        wait_for_flight_connections(self._namespace, self._coordinator_ip, task)
        return task, identity

    def reconnect(
        self, sql: str, marker: str, retry_interval: int = 1
    ) -> tuple[list[tuple[Any, ...]], QueryIdentity]:
        task, identity = self._start_query(sql, marker, retry_interval)
        with self._fault.reset_then_partition():
            time.sleep(3)
            if not task.is_running():
                raise AssertionError(
                    "query finished while worker traffic was still partitioned"
                )
        return task.rows(), identity

    def reconnect_after_keepalive(
        self, sql: str, marker: str
    ) -> tuple[list[tuple[Any, ...]], QueryIdentity]:
        baseline = flight_endpoints(self._namespace, self._coordinator_ip)
        task, identity = self._start_query(
            sql, marker, retry_interval=1, keep_alive=True
        )
        original = wait_for_flight_connections(
            self._namespace,
            self._coordinator_ip,
            task,
            require_keepalive=True,
            exclude=baseline,
        )
        with self._fault.keepalive_partition():
            wait_for_keepalive_disconnect(
                self._namespace, self._coordinator_ip, original, task
            )
            if not task.is_running():
                raise AssertionError("query finished before keepalive recovery")
        previous = {pod: baseline[pod] | original[pod] for pod in WORKER_PODS}
        wait_for_replacement_connections(
            self._namespace, self._coordinator_ip, previous, task
        )
        return task.rows(), identity

    def kill_during_reconnect(
        self, sql: str, marker: str
    ) -> tuple[Exception, QueryIdentity]:
        task, identity = self._start_query(sql, marker, retry_interval=5)
        kill_task = QueryTask(
            self._kill_connection, f"KILL QUERY {identity.connection_id}"
        )
        with self._fault.reset_for_retry():
            kill_task.start()
            error = task.error(timeout=30)
        kill_task.rows(timeout=30)
        return error, identity

    def assert_not_running(self, query_id: str, retry_delay: float) -> None:
        wait_for_query_to_stop(self._control_cursor, query_id)
        time.sleep(retry_delay)
        assert not is_query_running(self._control_cursor, query_id)

    def assert_healthy(self) -> None:
        cursor = self._query_connection.cursor()
        try:
            cursor.execute("SELECT count(), sum(number) FROM numbers_mt(1000000)")
            assert cursor.fetchall() == [(1000000, 499999500000)]
        finally:
            cursor.close()


def test_exact_aggregate(harness: ReconnectHarness) -> None:
    # 1. Start a distributed exact aggregate and establish its Flight transports.
    # 2. Partition worker traffic and reset the active TCP connections.
    # 3. Keep the partition while retries fail, then restore the network.
    # 4. Verify the exact aggregate to detect lost or replayed blocks.
    print("=== exact aggregate reconnect ===", flush=True)
    marker = "flight_reconnect_aggregate_ci"
    rows, _ = harness.reconnect(
        f"""
        SELECT count(), sum(k), sum(c)
        FROM (
            SELECT number % 100003 AS k, count(*) AS c
            FROM numbers_mt(2000000000)
            GROUP BY k
        ) AS {marker}
        """,
        marker,
    )
    assert rows == [(100003, 5000250003, 2000000000)], rows


def test_limit_early_stop(harness: ReconnectHarness) -> None:
    # 1. Start a distributed LIMIT query and reset its active connections.
    # 2. Keep retries partitioned briefly, then reconnect while scanning continues.
    # 3. Let LIMIT close its inputs through the normal early-stop path.
    # 4. Verify the query exits and a delayed retry cannot revive it.
    print("=== LIMIT early stop reconnect ===", flush=True)
    marker = "flight_reconnect_limit_ci"
    rows, identity = harness.reconnect(
        f"""
        SELECT number
        FROM numbers_mt(1000000000000) AS {marker}
        WHERE number % 500000000 = 0
        LIMIT 10
        """,
        marker,
    )
    values = [int(row[0]) for row in rows]
    assert len(values) == 10, values
    assert len(set(values)) == 10, values
    assert all(value % 500000000 == 0 for value in values), values
    harness.assert_not_running(identity.query_id, retry_delay=3)


def test_kill_during_reconnect(harness: ReconnectHarness) -> None:
    # 1. Start a long query with a five-second reconnect interval.
    # 2. Hold RST until the first reconnect fails, then restore the network.
    # 3. Execute KILL while the connector is still in retry backoff.
    # 4. Verify AbortedQuery and that the delayed retry cannot admit it again.
    print("=== KILL QUERY during reconnect ===", flush=True)
    marker = "flight_reconnect_kill_ci"
    error, identity = harness.kill_during_reconnect(
        f"SELECT sum(number % 1000000) FROM numbers_mt(1000000000000) AS {marker}",
        marker,
    )
    assert "AbortedQuery" in str(error), error
    harness.assert_not_running(identity.query_id, retry_delay=7)
    harness.assert_healthy()


def test_keepalive_reconnect(harness: ReconnectHarness) -> None:
    # 1. Enable TCP keepalive and capture this query's new Flight sockets.
    # 2. Pause worker userspace and silently partition the sockets without RST.
    # 3. Wait for kernel keepalive to remove an original socket on each worker.
    # 4. Restore workers and verify new endpoints and an exact query result.
    print("=== TCP keepalive reconnect ===", flush=True)
    marker = "flight_keepalive_reconnect_ci"
    rows, _ = harness.reconnect_after_keepalive(
        f"""
        SELECT count(), sum(c)
        FROM (
            SELECT number % 100003 AS k, count(*) AS c
            FROM numbers_mt(1000000000)
            GROUP BY k
        ) AS {marker}
        """,
        marker,
    )
    assert rows == [(100003, 1000000000)], rows


def test_tpch_queries(
    sqllogictests: Path,
    suite: Path,
    operation_log: Path,
    namespace: str,
    coordinator_ip: str,
    fault: NetworkFault,
) -> None:
    # 1. Run the complete TPC-H result suite through one MySQL session.
    # 2. Wait for an active query and its worker Flight sockets before each fault.
    # 3. Randomly reset the sockets, sometimes holding reconnects behind a partition.
    # 4. Record every action and require several observed physical replacements.
    # 5. Let sqllogictest verify every query result for loss or replay.
    print("=== complete TPC-H queries under random network faults ===", flush=True)
    operations = ChaosOperationLog(operation_log)
    stop = threading.Event()
    workload_started = threading.Event()
    injector = TpchChaosInjector(
        namespace, coordinator_ip, fault, stop, operations
    )
    command = [
        str(sqllogictests),
        "--handlers",
        "mysql",
        "--run",
        str(suite),
        "--parallel",
        "1",
    ]
    operations.record(f"workload=starting command={' '.join(command)}")
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    injector_thread = threading.Thread(
        target=injector.run,
        args=(workload_started,),
        name="tpch-chaos-injector",
        daemon=True,
    )
    injector_thread.start()

    try:
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            if "Running MySQL test for file:" in line:
                workload_started.set()
        return_code = process.wait()
    except BaseException:
        process.terminate()
        process.wait(timeout=10)
        raise
    finally:
        stop.set()
        workload_started.set()
        injector_thread.join(timeout=30)
        fault.recover()

    if injector_thread.is_alive():
        operations.record("injector=failed error=did not stop within 30 seconds")
        raise AssertionError("TPC-H chaos injector did not stop")

    operations.record(
        f"workload=finished exit_code={return_code} "
        f"attempted={injector.attempted} confirmed={injector.confirmed}"
    )
    operations.close()

    if return_code != 0:
        raise AssertionError(f"TPC-H sqllogictest exited with status {return_code}")
    if injector.error is not None:
        raise AssertionError(
            f"TPC-H chaos injector failed: {injector.error}"
        ) from injector.error
    if injector.confirmed < MIN_TPCH_CHAOS_OPERATIONS:
        raise AssertionError(
            "TPC-H workload completed without enough confirmed reconnects: "
            f"{injector.confirmed} < {MIN_TPCH_CHAOS_OPERATIONS}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--network-chaos", required=True, type=Path)
    parser.add_argument("--sqllogictests", required=True, type=Path)
    parser.add_argument("--tpch-suite", required=True, type=Path)
    parser.add_argument("--operation-log", required=True, type=Path)
    args = parser.parse_args()

    coordinator_ip = subprocess.check_output(
        [
            "kubectl",
            "-n",
            args.namespace,
            "get",
            "pod",
            "databend-query-0",
            "-o",
            "jsonpath={.status.podIP}",
        ],
        text=True,
    ).strip()

    # All sessions must exist before a partition. Opening a control session later can
    # itself block on cluster checks and would test admission rather than KILL semantics.
    query_connection = connect_mysql()
    control_connection = connect_mysql()
    kill_connection = connect_mysql()
    control_cursor = control_connection.cursor()
    fault = NetworkFault(args.namespace, args.network_chaos, coordinator_ip)
    harness = ReconnectHarness(
        query_connection,
        control_cursor,
        kill_connection,
        fault,
        args.namespace,
        coordinator_ip,
    )

    try:
        wait_for_cluster(control_cursor)
        test_exact_aggregate(harness)
        test_limit_early_stop(harness)
        test_keepalive_reconnect(harness)
        test_kill_during_reconnect(harness)
        test_tpch_queries(
            args.sqllogictests,
            args.tpch_suite,
            args.operation_log,
            args.namespace,
            coordinator_ip,
            fault,
        )
        harness.assert_healthy()
    finally:
        fault.recover()
        control_cursor.close()
        kill_connection.close()
        control_connection.close()
        query_connection.close()


if __name__ == "__main__":
    main()
