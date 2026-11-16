#!/usr/bin/env python3
"""Query, metabench, and metaverifier against a strict-auth gRPC TLS node.

One databend-meta node requires a credential over gRPC TLS. Each consumer
first runs with the matching credential, CA, and domain name and must finish
real work: Query creates a database and reads it back, metabench and
metaverifier complete their smallest workload. Then a wrong password and a CA
that did not sign the server certificate must make each consumer fail.
"""

import contextlib
import dataclasses
import re
import subprocess
import time
from pathlib import Path

import requests
from metactl_utils import client_for
from utils import (
    CURRENT_CREDENTIAL,
    METABENCH_BINARY,
    METAVERIFIER_BINARY,
    QUERY_BINARY,
    STRICT_AUTH_TLS,
    UNRELATED_CA_CERT,
    LocalMetaCluster,
    MetaClientProfile,
    MetaNodePorts,
    MetaSecurityProfile,
    build_meta_node,
    meta_cluster,
    print_step,
    print_title,
    write_password_file,
)

WORK_DIR = Path(".databend/metactl-secured-consumers")
PORTS = MetaNodePorts(admin=28801, grpc=28802, raft=28803)
QUERY_PORTS = {
    "admin": 28811,
    "metric": 28812,
    "flight": 28813,
    "http": 28814,
    "mysql": 28815,
    "flight_sql": 28816,
}
WRONG_PASSWORD = "wrong-password"
PASSWORDS = (CURRENT_CREDENTIAL.password, WRONG_PASSWORD)
# databend-meta answers a rejected credential with this gRPC status.
AUTH_ERROR = "does not have valid authentication credentials"
# A failed TLS handshake surfaces as a generic transport error.
TLS_ERROR = "transport error"
OPERATIONS = 5
SUMMARY_PREFIX = "benchmark summary:"
VERIFIER_TIMEOUT_SEC = 60
# metaverifier reports its terminal state only through this fixed file.
VERIFIER_RESULT_FILE = Path("/tmp/meta-verifier")
QUERY_START_TIMEOUT_SEC = 60
QUERY_STOP_TIMEOUT_SEC = 20
HTTP_TIMEOUT_SEC = 1
SQL_WAIT_SEC = 10
POLL_SEC = 0.5
DATABASE = "secured_db"

# Query's own listeners and its meta client settings. Paths are absolute, so
# the config does not depend on Query's cwd.
QUERY_TOML = """\
[query]
tenant_id = "secured_tenant"
cluster_id = "secured_cluster"
admin_api_address = "127.0.0.1:{admin}"
metric_api_address = "127.0.0.1:{metric}"
flight_api_address = "127.0.0.1:{flight}"
http_handler_host = "127.0.0.1"
http_handler_port = {http}
mysql_handler_host = "127.0.0.1"
mysql_handler_port = {mysql}
flight_sql_handler_host = "127.0.0.1"
flight_sql_handler_port = {flight_sql}
table_engine_memory_enabled = true

[[query.users]]
name = "root"
auth_type = "no_password"

[log.stderr]
on = false
[log.file]
on = true
level = "INFO"
format = "text"
dir = "{log_dir}"

[meta]
endpoints = ["{meta_address}"]
username = "{username}"
password = "{password}"
client_timeout_in_second = 10
auto_sync_interval = 60
rpc_tls_meta_server_root_ca_cert = "{ca_cert}"
rpc_tls_meta_service_domain_name = "{domain_name}"

[storage]
type = "fs"
[storage.fs]
data_path = "{data_dir}"
"""


@dataclasses.dataclass(frozen=True)
class Clients:
    """The profiles every consumer runs with; the wrong ones derive from `valid`."""

    valid: MetaClientProfile
    wrong_password: MetaClientProfile
    wrong_ca: MetaClientProfile


def clients_for(server: MetaSecurityProfile, secrets: Path) -> Clients:
    valid = client_for(server, CURRENT_CREDENTIAL, secrets)
    wrong_file = write_password_file(secrets / "wrong.password", WRONG_PASSWORD)
    wrong_password = dataclasses.replace(valid, password_file=wrong_file)
    wrong_ca = dataclasses.replace(valid, grpc_tls_ca_cert=UNRELATED_CA_CERT)
    return Clients(valid, wrong_password, wrong_ca)


def assert_no_password(text: str, where: str) -> None:
    for password in PASSWORDS:
        assert password not in text, f"a password leaked into {where}"


def run_tool(
    binary: Path, args: list[str], client: MetaClientProfile
) -> subprocess.CompletedProcess:
    """Run metabench or metaverifier to completion; no password may appear in its output."""
    cmd = [str(binary), *args, *client.cli_args()]
    print(f"Running: {cmd}", flush=True)
    result = subprocess.run(
        cmd,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert_no_password(result.stdout + result.stderr, f"the output of {binary.name}")
    return result


def benchmark_summary(stdout: str) -> dict[str, str]:
    """The fields of the one `benchmark summary:` line, e.g. `error` -> "0"."""
    lines = [line for line in stdout.splitlines() if line.startswith(SUMMARY_PREFIX)]
    assert len(lines) == 1, stdout
    return dict(re.findall(r"(\w+)=(\S+)", lines[0]))


def check_metabench(address: str, clients: Clients) -> None:
    """metabench exits 0 even when every operation fails; the summary line decides."""
    args = ["--grpc-api-address", address, "--client", "1"]
    args += ["--number", str(OPERATIONS), "--rpc", "upsert_kv"]

    print_step("metabench with the valid client completes every upsert")
    result = run_tool(METABENCH_BINARY, args, clients.valid)
    assert result.returncode == 0, result.stderr
    summary = benchmark_summary(result.stdout)
    assert summary["success"] == str(OPERATIONS), summary
    assert summary["error"] == "0", summary

    print_step("metabench with the wrong password fails every upsert")
    result = run_tool(METABENCH_BINARY, args, clients.wrong_password)
    summary = benchmark_summary(result.stdout)
    assert summary["error"] == str(OPERATIONS), summary
    assert AUTH_ERROR in result.stderr, result.stderr

    print_step("metabench with a CA that did not sign the server certificate fails")
    result = run_tool(METABENCH_BINARY, args, clients.wrong_ca)
    summary = benchmark_summary(result.stdout)
    assert summary["error"] == str(OPERATIONS), summary
    assert TLS_ERROR in result.stderr, result.stderr


def run_metaverifier(
    address: str, client: MetaClientProfile
) -> subprocess.CompletedProcess:
    """Write and read back OPERATIONS keys; VERIFIER_RESULT_FILE then holds END or ERROR."""
    args = ["--grpc-api-address", address, "--client", "1"]
    args += ["--number", str(OPERATIONS), "--time", str(VERIFIER_TIMEOUT_SEC)]
    VERIFIER_RESULT_FILE.unlink(missing_ok=True)
    return run_tool(METAVERIFIER_BINARY, args, client)


def check_metaverifier(address: str, clients: Clients) -> None:
    print_step("metaverifier with the valid client reaches END")
    result = run_metaverifier(address, clients.valid)
    assert result.returncode == 0, result.stderr
    assert VERIFIER_RESULT_FILE.read_text() == "END"

    print_step("metaverifier with the wrong password reaches ERROR")
    result = run_metaverifier(address, clients.wrong_password)
    assert result.returncode != 0, result.stdout
    assert VERIFIER_RESULT_FILE.read_text() == "ERROR"
    assert AUTH_ERROR in result.stderr, result.stderr

    print_step(
        "metaverifier with a CA that did not sign the server certificate reaches ERROR"
    )
    result = run_metaverifier(address, clients.wrong_ca)
    assert result.returncode != 0, result.stdout
    assert VERIFIER_RESULT_FILE.read_text() == "ERROR"
    assert TLS_ERROR in result.stderr, result.stderr


@dataclasses.dataclass(frozen=True)
class QueryNode:
    """One databend-query process and the files it writes."""

    process: subprocess.Popen
    stdout_path: Path
    log_dir: Path


def query_config(query_dir: Path, meta_address: str, client: MetaClientProfile) -> str:
    """Query's config for `client`; the meta password comes from the profile's file."""
    password = client.password_file.read_text().strip()
    return QUERY_TOML.format(
        **QUERY_PORTS,
        log_dir=query_dir / "logs",
        meta_address=meta_address,
        username=client.username,
        password=password,
        ca_cert=client.grpc_tls_ca_cert,
        domain_name=client.grpc_tls_domain_name,
        data_dir=query_dir / "data",
    )


@contextlib.contextmanager
def query_node(cluster: LocalMetaCluster, label: str, client: MetaClientProfile):
    """Start one Query against `cluster` under `query-{label}/`; stop it after the block."""
    query_dir = cluster.work_dir / f"query-{label}"
    query_dir.mkdir()
    config_path = query_dir / "databend-query.toml"
    config_path.write_text(query_config(query_dir, cluster.grpc_address(1), client))
    stdout_path = query_dir / "stdout.log"
    cmd = [str(QUERY_BINARY), "--config-file", str(config_path)]
    print(f"Starting: {cmd}", flush=True)
    with stdout_path.open("w") as stdout_file:
        process = subprocess.Popen(
            cmd, cwd=query_dir, stdout=stdout_file, stderr=subprocess.STDOUT
        )
    try:
        yield QueryNode(process, stdout_path, query_dir / "logs")
    finally:
        stop_query(process)


def stop_query(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=QUERY_STOP_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def http_ok(url: str) -> bool:
    try:
        response = requests.get(url, timeout=HTTP_TIMEOUT_SEC)
    except requests.RequestException:
        return False
    return response.ok


def wait_for_startup(node: QueryNode) -> int | None:
    """Poll until Query answers its admin health endpoint (None) or exits (its code)."""
    health_url = f"http://127.0.0.1:{QUERY_PORTS['admin']}/v1/health"
    deadline = time.monotonic() + QUERY_START_TIMEOUT_SEC
    while time.monotonic() < deadline:
        exit_code = node.process.poll()
        if exit_code is not None:
            return exit_code
        if http_ok(health_url):
            return None
        time.sleep(POLL_SEC)
    raise TimeoutError(f"Query neither served nor exited; see {node.stdout_path}")


def run_sql(statement: str) -> list:
    """Run one statement through Query's HTTP handler as root; return its rows."""
    url = f"http://127.0.0.1:{QUERY_PORTS['http']}/v1/query"
    body = {"sql": statement, "pagination": {"wait_time_secs": SQL_WAIT_SEC}}
    response = requests.post(
        url, auth=("root", ""), json=body, timeout=SQL_WAIT_SEC + 5
    )
    result = response.json()
    assert result["state"] == "Succeeded", result
    return result["data"]


def check_no_password_in_logs(node: QueryNode) -> None:
    """Query's stdout and log files must not hold the meta password."""
    log_files = [path for path in node.log_dir.rglob("*") if path.is_file()]
    for path in [node.stdout_path, *log_files]:
        assert_no_password(path.read_text(errors="replace"), str(path))


def check_query_fails(
    cluster: LocalMetaCluster, label: str, client: MetaClientProfile, error: str
) -> None:
    """Query with `client` must exit before serving, naming `error`."""
    with query_node(cluster, label, client) as node:
        exit_code = wait_for_startup(node)
        assert exit_code not in (None, 0), f"Query started with {client}"
        output = node.stdout_path.read_text(errors="replace")
        assert error in output, output


def check_query(cluster: LocalMetaCluster, clients: Clients) -> None:
    print_step("Query with the valid settings creates a database and reads it back")
    with query_node(cluster, "valid", clients.valid) as node:
        exit_code = wait_for_startup(node)
        assert exit_code is None, (
            f"Query exited with {exit_code}; see {node.stdout_path}"
        )
        run_sql(f"CREATE DATABASE {DATABASE}")
        rows = run_sql(f"SELECT name FROM system.databases WHERE name = '{DATABASE}'")
        assert rows == [[DATABASE]], rows
        check_no_password_in_logs(node)

    print_step("Query with the wrong password fails to start")
    check_query_fails(cluster, "wrong-password", clients.wrong_password, AUTH_ERROR)

    print_step(
        "Query with a CA that did not sign the server certificate fails to start"
    )
    check_query_fails(cluster, "wrong-ca", clients.wrong_ca, TLS_ERROR)


def main():
    title = (
        "Query, metabench, and metaverifier with strict authentication over gRPC TLS"
    )
    print_title(f"Test {title}")
    node = build_meta_node(1, PORTS, security=STRICT_AUTH_TLS)
    with meta_cluster(WORK_DIR, [node]) as cluster:
        address = cluster.grpc_address(1)
        clients = clients_for(STRICT_AUTH_TLS, cluster.work_dir / "secrets")
        check_metabench(address, clients)
        check_metaverifier(address, clients)
        check_query(cluster, clients)
    print(f"✓ {title} passed")


if __name__ == "__main__":
    main()
