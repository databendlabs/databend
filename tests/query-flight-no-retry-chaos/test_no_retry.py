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
QUERY_STOP_STABILITY_WINDOW = 3
PAUSE_DURATION = 3
FLIGHT_RETRY_TIMES = 10
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

    def reset_match_counts(self) -> dict[str, int]:
        result = {}
        for pod in WORKER_PODS:
            output = subprocess.check_output(
                [
                    "kubectl",
                    "-n",
                    self._namespace,
                    "exec",
                    pod,
                    "-c",
                    "net-admin",
                    "--",
                    "iptables",
                    "-L",
                    "OUTPUT",
                    "-v",
                    "-n",
                    "-x",
                ],
                text=True,
            )
            matched = 0
            for line in output.splitlines():
                fields = line.split()
                if (
                    len(fields) >= 9
                    and fields[2:4] == ["REJECT", "tcp"]
                    and fields[8].split("/")[0] == self._coordinator_ip
                    and "dpt:9090" in fields
                    and "tcp-reset" in fields
                ):
                    matched += int(fields[0])
            result[pod] = matched
        return result

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
    expected_defaults = {
        "enable_experiment_new_flight": "0",
        "flight_connection_max_retry_times": "0",
        "flight_connection_retry_interval": "1",
        "flight_client_keep_alive_time_secs": "0",
        "flight_client_keep_alive_interval_secs": "0",
        "flight_client_keep_alive_retries": "0",
    }
    names = ", ".join(f"'{name}'" for name in sorted(expected_defaults))
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT name, value, `default` FROM system.settings "
            f"WHERE name IN ({names})"
        )
        settings = {
            str(name): (str(value), str(default)) for name, value, default in cursor
        }
        assert settings.keys() == expected_defaults.keys(), settings
        for name, (value, default) in settings.items():
            expected = expected_defaults[name]
            assert value == expected and default == expected, (
                name,
                value,
                default,
            )
        chaos_log(
            "settings_verified "
            + " ".join(
                f"{name}=value:{settings[name][0]},default:{settings[name][1]}"
                for name in sorted(settings)
            )
        )
    finally:
        cursor.close()


def configure_new_flight(
    connection: Any, *, retry_times: int = 0, retry_interval: int = 1
) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute("SET enable_experiment_new_flight = 1")
        cursor.execute(f"SET flight_connection_max_retry_times = {retry_times}")
        cursor.execute(f"SET flight_connection_retry_interval = {retry_interval}")
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


# gRPC may multiplex a new logical do_exchange stream over an existing TCP
# connection, so a new socket is not a valid readiness signal for this test.
def wait_for_do_exchange(
    namespace: str,
    query_id: str,
    task: QueryTask,
    required_pods: int = len(QUERY_PODS),
) -> None:
    deadline = time.monotonic() + QUERY_START_TIMEOUT
    pending = set(QUERY_PODS)
    while time.monotonic() < deadline:
        for pod in tuple(pending):
            try:
                output = subprocess.run(
                    [
                        "kubectl",
                        "-n",
                        namespace,
                        "logs",
                        pod,
                        "-c",
                        "query",
                        "--since=2m",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    errors="replace",
                ).stdout
            except subprocess.CalledProcessError:
                continue
            if any(
                query_id in line
                and ("do_exchange:" in line or "handle_do_exchange:" in line)
                for line in output.replace("\0", "").splitlines()
            ):
                pending.remove(pod)
        observed = len(QUERY_PODS) - len(pending)
        if observed >= required_pods:
            chaos_log(
                f"logical_exchange_ready query_id={query_id} "
                f"observed_pods={observed} required_pods={required_pods}"
            )
            return
        if not task.is_running():
            raise AssertionError(
                f"JOIN {query_id} finished before all nodes entered do_exchange; "
                f"missing={sorted(pending)}"
            )
        time.sleep(POLL_INTERVAL)
    raise AssertionError(
        f"JOIN {query_id} did not enter do_exchange on nodes {sorted(pending)}"
    )


def wait_for_reset_match(
    fault: ClusterFault, task: QueryTask, timeout: float = 10
) -> dict[str, int]:
    deadline = time.monotonic() + timeout
    last_counts = {pod: 0 for pod in WORKER_PODS}
    while time.monotonic() < deadline:
        last_counts = fault.reset_match_counts()
        if any(last_counts.values()):
            chaos_log(f"fault_observed type=tcp_reset packet_counts={last_counts}")
            return last_counts
        if not task.is_running():
            raise AssertionError(
                "JOIN finished before the TCP reset rule matched a Flight packet"
            )
        time.sleep(POLL_INTERVAL)
    raise AssertionError(
        f"TCP reset rules did not match a Flight packet within {timeout} seconds; "
        f"packet_counts={last_counts}"
    )


class NoRetryHarness:
    def __init__(
        self,
        control_cursor: Any,
        fault: ClusterFault,
        namespace: str,
    ):
        self._control_cursor = control_cursor
        self._fault = fault
        self._namespace = namespace

    def start_query(
        self,
        sql: str,
        marker: str,
        *,
        require_exchange: bool = True,
        required_exchange_pods: int = len(QUERY_PODS),
        retry_times: int = 0,
        retry_interval: int = 1,
    ) -> RunningQuery:
        connection = connect_mysql()
        try:
            assert_default_no_retry(connection)
            configure_new_flight(
                connection,
                retry_times=retry_times,
                retry_interval=retry_interval,
            )
            task = QueryTask(connection, sql)
            task.start()
            identity = wait_for_query(self._control_cursor, marker, task)
            if require_exchange:
                wait_for_do_exchange(
                    self._namespace,
                    identity.query_id,
                    task,
                    required_exchange_pods,
                )
            chaos_log(
                f"query_ready marker={marker} query_id={identity.query_id} "
                f"connection_id={identity.connection_id}"
            )
            return RunningQuery(connection, task, identity)
        except BaseException:
            connection.close()
            raise

    def assert_stopped(self, identity: QueryIdentity, delay: float = 0) -> None:
        wait_for_query_to_stop(self._control_cursor, identity.query_id)
        if delay:
            time.sleep(delay)
            assert not is_query_running(self._control_cursor, identity.query_id)
        chaos_log(f"query_stopped query_id={identity.query_id}")

    def assert_healthy(self) -> None:
        connection = connect_mysql()
        cursor = None
        try:
            assert_default_no_retry(connection)
            configure_new_flight(connection)
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

    def reset_then_recover(self, running: RunningQuery) -> None:
        try:
            self._fault.reset_connections()
            wait_for_reset_match(self._fault, running.task)
            # Keep the RST rule long enough for the immediate reconnect attempt to
            # fail, so the query must exercise retry backoff and replacement.
            time.sleep(1)
            if not running.task.is_running():
                raise AssertionError(
                    "query finished before the reconnect fault was recovered"
                )
        finally:
            self._fault.clear_resets()
        chaos_log(
            f"fault_recovered type=tcp_reset query_id={running.identity.query_id}"
        )


# Full joins cannot use the broadcast strategy, so the small build side stays
# cheap while the large probe side continuously crosses the shuffle exchange.
def exact_join(marker: str, rows: int = 1000000000000) -> str:
    return f"""
        SELECT count(), sum(number % 1000000)
        FROM (
            SELECT lhs.number
            FROM numbers_mt({rows}) AS lhs
            FULL OUTER JOIN numbers_mt(100003) AS rhs
                ON lhs.number % 100003 = rhs.number
        ) AS {marker}
    """


def test_exact_join_reconnect(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=exact_join_reconnect")
    marker = "flight_exact_join_reconnect_ci"
    rows_count = 1000000000
    modulus = 1000000
    running = harness.start_query(
        exact_join(marker, rows_count),
        marker,
        retry_times=FLIGHT_RETRY_TIMES,
    )
    try:
        harness.reset_then_recover(running)
        rows = running.task.rows()
        assert len(rows) == 1, rows
        assert int(rows[0][0]) == rows_count, rows
        expected_sum = (rows_count // modulus) * modulus * (modulus - 1) // 2
        assert int(rows[0][1]) == expected_sum, rows
        harness.assert_stopped(running.identity)
        chaos_log(f"scenario_pass name=exact_join_reconnect rows={rows_count}")
    finally:
        running.close()


def test_limit_early_stop_after_reconnect(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=join_limit_early_stop_after_reconnect")
    marker = "flight_limit_reconnect_ci"
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
        required_exchange_pods=1,
        retry_times=FLIGHT_RETRY_TIMES,
    )
    try:
        harness.reset_then_recover(running)
        rows = running.task.rows()
        values = [int(row[0]) for row in rows]
        assert len(values) == 10, values
        assert len(set(values)) == 10, values
        assert all(value % 500000000 == 0 for value in values), values
        harness.assert_stopped(running.identity, delay=QUERY_STOP_STABILITY_WINDOW)
        chaos_log("scenario_pass name=join_limit_early_stop_after_reconnect rows=10")
    finally:
        running.close()


def test_kill_query_during_reconnect(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=kill_query_during_reconnect")
    marker = "flight_kill_during_reconnect_ci"
    retry_interval = 5
    running = harness.start_query(
        exact_join(marker),
        marker,
        retry_times=FLIGHT_RETRY_TIMES,
        retry_interval=retry_interval,
    )
    kill_connection = None
    try:
        kill_connection = connect_mysql()
        harness.reset_then_recover(running)
        chaos_log(
            "fault_inject type=kill_query_during_retry_backoff "
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
        harness.assert_stopped(running.identity, delay=retry_interval + 2)
        harness.assert_healthy()
        chaos_log(f"scenario_pass name=kill_query_during_reconnect error={error}")
    finally:
        if kill_connection is not None:
            kill_connection.close()
        running.close()


def test_tcp_reset_failure(harness: NoRetryHarness) -> None:
    chaos_log("scenario_start name=tcp_reset_during_join")
    marker = "flight_no_retry_rst_ci"
    running = harness.start_query(exact_join(marker), marker)
    try:
        try:
            chaos_log(
                f"fault_inject type=tcp_reset query_id={running.identity.query_id}"
            )
            harness.fault.reset_connections()
            wait_for_reset_match(harness.fault, running.task)
            failed_at = time.monotonic()
            error = running.task.error(timeout=CONNECTION_FAILURE_TIMEOUT)
            elapsed = time.monotonic() - failed_at
            chaos_log(
                f"query_failed type=tcp_reset elapsed_seconds={elapsed:.3f} "
                f"error={error}"
            )
        finally:
            harness.fault.clear_resets()
            chaos_log("fault_recovered type=tcp_reset")
        harness.assert_stopped(running.identity, delay=QUERY_STOP_STABILITY_WINDOW)
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
        harness.assert_stopped(running.identity, delay=QUERY_STOP_STABILITY_WINDOW)
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
    rows_count = 1000000000
    modulus = 1000000
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
        expected_sum = (rows_count // modulus) * modulus * (modulus - 1) // 2
        assert int(rows[0][1]) == expected_sum, rows
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
    harness = NoRetryHarness(control_cursor, fault, args.namespace)

    try:
        chaos_log(
            f"suite_start namespace={args.namespace} coordinator_ip={coordinator_ip}"
        )
        assert_default_no_retry(control_connection)
        wait_for_cluster(control_cursor)
        test_exact_join_reconnect(harness)
        test_limit_early_stop_after_reconnect(harness)
        test_kill_query_during_reconnect(harness)
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
