#!/usr/bin/env python3

import argparse
import queue
import subprocess
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import mysql.connector


MYSQL_PORT = 3307
QUERY_START_TIMEOUT = 30
QUERY_RESULT_TIMEOUT = 180
CONNECTION_FAILURE_TIMEOUT = 30
NODE_FAILURE_TIMEOUT = 90
NO_REPLACEMENT_WINDOW = 3
PAUSE_DURATION = 3
POLL_INTERVAL = 0.1
COORDINATOR = "databend-query-0"
FAILED_WORKER = "databend-query-1"
WORKER_PODS = ("databend-query-1", "databend-query-2")
QUERY_PODS = (COORDINATOR, *WORKER_PODS)


def chaos_log(message: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    print(f"{timestamp} CHAOS {message}", flush=True)


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    chaos_log(f"command={' '.join(command)}")
    return subprocess.run(command, check=check, text=True)


@dataclass(frozen=True)
class QueryIdentity:
    query_id: str
    connection_id: int


@dataclass
class RunningQuery:
    connection: Any
    task: "QueryTask"
    identity: QueryIdentity
    baseline_endpoints: dict[str, set[str]]
    query_endpoints: dict[str, set[str]]

    def close(self) -> None:
        self.connection.close()


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
        except Exception as error:  # Tests below assert the concrete outcome.
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
            raise AssertionError(f"query completed unexpectedly: {result}")
        return result


class ClusterFault:
    def __init__(self, namespace: str, coordinator_ip: str):
        self._namespace = namespace
        self._coordinator_ip = coordinator_ip

    def _exec_pod(self, pod: str, command: str, *, check: bool = True) -> None:
        run(
            [
                "kubectl",
                "-n",
                self._namespace,
                "exec",
                pod,
                "-c",
                "net-admin",
                "--",
                "sh",
                "-c",
                command,
            ],
            check=check,
        )

    def reset_connections(self) -> None:
        for pod in WORKER_PODS:
            self._exec_pod(
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
            self._exec_pod(pod, f"while {rule} 2>/dev/null; do :; done", check=False)

    def signal_query(self, pod: str, signal: str, *, check: bool = True) -> None:
        if pod not in QUERY_PODS:
            raise AssertionError(f"cannot signal unknown query pod {pod}")
        if signal not in {"STOP", "CONT", "KILL"}:
            raise AssertionError(f"unsupported query signal {signal}")
        self._exec_pod(
            pod,
            "query_pid=; "
            "for executable in /proc/[0-9]*/exe; do "
            '[ "$(readlink "$executable" 2>/dev/null)" = /databend-query ] || continue; '
            'pid="${executable#/proc/}"; pid="${pid%/exe}"; '
            '[ -z "$query_pid" ] || { echo "multiple databend-query processes" >&2; exit 1; }; '
            'query_pid="$pid"; done; '
            '[ -n "$query_pid" ] || { echo "databend-query process not found" >&2; exit 1; }; '
            '[ "$query_pid" -ne 1 ] || { echo "databend-query unexpectedly has PID 1" >&2; exit 1; }; '
            f'kill -{signal} "$query_pid"',
            check=check,
        )

    def query_restart_count(self, pod: str) -> int:
        output = subprocess.check_output(
            [
                "kubectl",
                "-n",
                self._namespace,
                "get",
                "pod",
                pod,
                "-o",
                'jsonpath={.status.containerStatuses[?(@.name=="query")].restartCount}',
            ],
            text=True,
        ).strip()
        return int(output)

    def crash_worker(self, pod: str) -> int:
        restart_count = self.query_restart_count(pod)
        self.signal_query(pod, "KILL")
        return restart_count

    def wait_for_worker_restart(
        self, pod: str, previous_restart_count: int, timeout: float = 120
    ) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                restarted = self.query_restart_count(pod) > previous_restart_count
            except (subprocess.CalledProcessError, ValueError):
                restarted = False
            if restarted:
                run(
                    [
                        "kubectl",
                        "-n",
                        self._namespace,
                        "wait",
                        f"pod/{pod}",
                        "--for=condition=Ready",
                        f"--timeout={int(timeout)}s",
                    ]
                )
                return
            time.sleep(POLL_INTERVAL)
        raise AssertionError(f"query container in {pod} did not restart")

    @contextmanager
    def pause_query(self, pod: str):
        self.signal_query(pod, "STOP")
        try:
            yield
        finally:
            self.signal_query(pod, "CONT")

    def recover(self) -> None:
        self.clear_resets()
        for pod in QUERY_PODS:
            self.signal_query(pod, "CONT", check=False)


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


def assert_default_no_retry(connection: Any) -> None:
    expected_zero = {
        "flight_connection_max_retry_times",
        "flight_client_keep_alive_time_secs",
        "flight_client_keep_alive_interval_secs",
        "flight_client_keep_alive_retries",
    }
    names = ", ".join(f"'{name}'" for name in sorted(expected_zero))
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT name, value, `default` FROM system.settings "
            f"WHERE name IN ({names})"
        )
        settings = {str(name): (str(value), str(default)) for name, value, default in cursor}
        assert settings.keys() == expected_zero, settings
        for name, (value, default) in settings.items():
            assert value == "0" and default == "0", (name, value, default)
        chaos_log(
            "settings_verified "
            + " ".join(
                f"{name}=value:{settings[name][0]},default:{settings[name][1]}"
                for name in sorted(settings)
            )
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


def flight_endpoints(namespace: str, coordinator_ip: str) -> dict[str, set[str]]:
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
            fields = line.split()
            if remote in fields:
                endpoints.add(fields[fields.index(remote) - 1])
        result[pod] = endpoints
    return result


def wait_for_flight_connections(
    namespace: str,
    coordinator_ip: str,
    task: QueryTask,
    baseline: dict[str, set[str]],
) -> dict[str, set[str]]:
    deadline = time.monotonic() + QUERY_START_TIMEOUT
    while time.monotonic() < deadline:
        current = flight_endpoints(namespace, coordinator_ip)
        selected = {pod: current[pod] - baseline[pod] for pod in WORKER_PODS}
        if all(selected[pod] for pod in WORKER_PODS):
            return selected
        if not task.is_running():
            raise AssertionError(
                "JOIN finished before worker-to-listener Flight connections were established"
            )
        time.sleep(POLL_INTERVAL)
    raise AssertionError("JOIN did not establish Flight connections from both workers")


def wait_for_disconnection(
    namespace: str,
    coordinator_ip: str,
    original: dict[str, set[str]],
    timeout: float = 10,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = flight_endpoints(namespace, coordinator_ip)
        if all(original[pod] - current[pod] for pod in WORKER_PODS):
            return
        time.sleep(POLL_INTERVAL)
    raise AssertionError("TCP reset did not close the JOIN's Flight connections")


class NoRetryHarness:
    def __init__(
        self,
        control_cursor: Any,
        fault: ClusterFault,
        namespace: str,
        coordinator_ip: str,
    ):
        self._control_cursor = control_cursor
        self._fault = fault
        self._namespace = namespace
        self._coordinator_ip = coordinator_ip

    def start_query(self, sql: str, marker: str) -> RunningQuery:
        connection = connect_mysql()
        try:
            assert_default_no_retry(connection)
            baseline = flight_endpoints(self._namespace, self._coordinator_ip)
            task = QueryTask(connection, sql)
            task.start()
            identity = wait_for_query(self._control_cursor, marker, task)
            endpoints = wait_for_flight_connections(
                self._namespace, self._coordinator_ip, task, baseline
            )
            chaos_log(
                f"query_ready marker={marker} query_id={identity.query_id} "
                f"connection_id={identity.connection_id} endpoints={endpoints}"
            )
            return RunningQuery(connection, task, identity, baseline, endpoints)
        except BaseException:
            connection.close()
            raise

    def assert_stopped(self, identity: QueryIdentity, delay: float = 0) -> None:
        wait_for_query_to_stop(self._control_cursor, identity.query_id)
        if delay:
            time.sleep(delay)
            assert not is_query_running(self._control_cursor, identity.query_id)
        chaos_log(f"query_stopped query_id={identity.query_id}")

    def assert_no_replacement(self, running: RunningQuery) -> None:
        known = {
            pod: running.baseline_endpoints[pod] | running.query_endpoints[pod]
            for pod in WORKER_PODS
        }
        deadline = time.monotonic() + NO_REPLACEMENT_WINDOW
        while time.monotonic() < deadline:
            current = flight_endpoints(self._namespace, self._coordinator_ip)
            replacements = {pod: current[pod] - known[pod] for pod in WORKER_PODS}
            assert not any(replacements.values()), replacements
            time.sleep(POLL_INTERVAL)
        chaos_log(
            f"flight_replacement=none observation_seconds={NO_REPLACEMENT_WINDOW}"
        )

    def wait_for_disconnection(self, original: dict[str, set[str]]) -> None:
        wait_for_disconnection(
            self._namespace,
            self._coordinator_ip,
            original,
        )

    def assert_healthy(self) -> None:
        connection = connect_mysql()
        cursor = None
        try:
            assert_default_no_retry(connection)
            cursor = connection.cursor()
            wait_for_cluster(cursor)
            cursor.execute("SELECT count(), sum(number) FROM numbers_mt(1000000)")
            assert cursor.fetchall() == [(1000000, 499999500000)]
            chaos_log("cluster_health=ok")
        finally:
            if cursor is not None:
                cursor.close()
            connection.close()

    @property
    def fault(self) -> ClusterFault:
        return self._fault


def exact_join(marker: str, rows: int = 1000000000) -> str:
    return f"""
        SELECT count(), sum(number)
        FROM (
            SELECT lhs.number
            FROM numbers_mt({rows}) AS lhs
            INNER JOIN numbers_mt(100003) AS rhs
                ON lhs.number % 100003 = rhs.number
        ) AS {marker}
    """


def test_limit_early_stop(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=join_limit_early_stop")
    marker = "flight_no_retry_limit_ci"
    running = harness.start_query(
        f"""
        SELECT number
        FROM (
            SELECT lhs.number
            FROM numbers_mt(1000000000000) AS lhs
            INNER JOIN numbers_mt(100003) AS rhs
                ON lhs.number % 100003 = rhs.number
            WHERE lhs.number % 500000000 = 0
        ) AS {marker}
        LIMIT 10
        """,
        marker,
    )
    try:
        rows = running.task.rows()
        values = [int(row[0]) for row in rows]
        assert len(values) == 10, values
        assert len(set(values)) == 10, values
        assert all(value % 500000000 == 0 for value in values), values
        harness.assert_stopped(running.identity)
        chaos_log("scenario_pass name=join_limit_early_stop rows=10")
    finally:
        running.close()


def test_kill_query(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=kill_query_during_join")
    marker = "flight_no_retry_kill_ci"
    running = harness.start_query(exact_join(marker), marker)
    kill_connection = None
    try:
        kill_connection = connect_mysql()
        chaos_log(
            "fault_inject type=kill_query "
            f"query_id={running.identity.query_id} "
            f"connection_id={running.identity.connection_id}"
        )
        kill_task = QueryTask(
            kill_connection, f"KILL QUERY {running.identity.connection_id}"
        )
        kill_task.start()
        error = running.task.error(timeout=CONNECTION_FAILURE_TIMEOUT)
        kill_task.rows(timeout=CONNECTION_FAILURE_TIMEOUT)
        assert "AbortedQuery" in str(error), error
        harness.assert_stopped(running.identity, delay=NO_REPLACEMENT_WINDOW)
        harness.assert_healthy()
        chaos_log(f"scenario_pass name=kill_query_during_join error={error}")
    finally:
        if kill_connection is not None:
            kill_connection.close()
        running.close()


def test_tcp_reset_failure(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=tcp_reset_during_join")
    marker = "flight_no_retry_rst_ci"
    running = harness.start_query(exact_join(marker), marker)
    original = running.query_endpoints
    try:
        try:
            injected_at = time.monotonic()
            chaos_log(
                f"fault_inject type=tcp_reset endpoints={original}"
            )
            harness.fault.reset_connections()
            harness.wait_for_disconnection(original)
            chaos_log("fault_observed type=tcp_reset state=flight_disconnected")
            error = running.task.error(timeout=CONNECTION_FAILURE_TIMEOUT)
            elapsed = time.monotonic() - injected_at
            chaos_log(
                f"query_failed type=tcp_reset elapsed_seconds={elapsed:.3f} "
                f"error={error}"
            )
        finally:
            harness.fault.clear_resets()
            chaos_log("fault_recovered type=tcp_reset")
        harness.assert_stopped(running.identity)
        harness.assert_no_replacement(running)
    finally:
        running.close()
    harness.assert_healthy()
    chaos_log("scenario_pass name=tcp_reset_during_join")


def test_worker_crash_failure(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=worker_oom_style_failure_during_join")
    marker = "flight_no_retry_worker_failure_ci"
    running = harness.start_query(exact_join(marker), marker)
    restart_count = None
    try:
        failed_at = time.monotonic()
        chaos_log(f"fault_inject type=sigkill pod={FAILED_WORKER}")
        restart_count = harness.fault.crash_worker(FAILED_WORKER)
        remaining = NODE_FAILURE_TIMEOUT - (time.monotonic() - failed_at)
        if remaining <= 0:
            raise AssertionError("worker crash consumed the query failure deadline")
        error = running.task.error(timeout=remaining)
        elapsed = time.monotonic() - failed_at
        chaos_log(
            f"query_failed type=sigkill elapsed_seconds={elapsed:.3f} error={error}"
        )
        harness.assert_stopped(running.identity, delay=NO_REPLACEMENT_WINDOW)
    finally:
        running.close()
    if restart_count is None:
        raise AssertionError("worker crash was not injected")
    harness.fault.wait_for_worker_restart(FAILED_WORKER, restart_count)
    chaos_log(
        f"fault_recovered type=sigkill pod={FAILED_WORKER} "
        f"previous_restart_count={restart_count}"
    )
    harness.assert_healthy()
    chaos_log("scenario_pass name=worker_oom_style_failure_during_join")


def test_short_receiver_pause(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=short_flight_listener_pause_during_join")
    marker = "flight_no_retry_pause_ci"
    rows_count = 200000000
    running = harness.start_query(exact_join(marker, rows_count), marker)
    try:
        chaos_log(
            f"fault_inject type=sigstop pod={COORDINATOR} "
            f"duration_seconds={PAUSE_DURATION}"
        )
        with harness.fault.pause_query(COORDINATOR):
            time.sleep(PAUSE_DURATION)
        chaos_log(f"fault_recovered type=sigcont pod={COORDINATOR}")
        rows = running.task.rows()
        assert len(rows) == 1, rows
        assert int(rows[0][0]) == rows_count, rows
        assert int(rows[0][1]) == rows_count * (rows_count - 1) // 2, rows
        harness.assert_stopped(running.identity)
        harness.assert_healthy()
        chaos_log("scenario_pass name=short_flight_listener_pause_during_join")
    finally:
        running.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()

    coordinator_ip = subprocess.check_output(
        [
            "kubectl",
            "-n",
            args.namespace,
            "get",
            "pod",
            COORDINATOR,
            "-o",
            "jsonpath={.status.podIP}",
        ],
        text=True,
    ).strip()

    control_connection = connect_mysql()
    control_cursor = control_connection.cursor()
    fault = ClusterFault(args.namespace, coordinator_ip)
    harness = NoRetryHarness(control_cursor, fault, args.namespace, coordinator_ip)

    try:
        chaos_log(
            f"suite_start namespace={args.namespace} coordinator_ip={coordinator_ip}"
        )
        assert_default_no_retry(control_connection)
        wait_for_cluster(control_cursor)
        test_limit_early_stop(harness)
        test_kill_query(harness)
        test_tcp_reset_failure(harness)
        test_worker_crash_failure(harness)
        test_short_receiver_pause(harness)
        harness.assert_healthy()
        chaos_log("suite_pass")
    except BaseException as error:
        chaos_log(f"suite_fail error={error}")
        raise
    finally:
        chaos_log("suite_cleanup_start")
        fault.recover()
        control_cursor.close()
        control_connection.close()
        chaos_log("suite_cleanup_complete")


if __name__ == "__main__":
    main()
