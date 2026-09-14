#!/usr/bin/env python3

import os
from contextlib import contextmanager
import shutil
import socket
import subprocess
import textwrap
import time
from pathlib import Path


CURDIR = Path(__file__).resolve().parent
ROOT = CURDIR.parents[3]
BUILD_PROFILE = os.getenv("BUILD_PROFILE", "debug")

MYSQL_PORT = 13318
HTTP_PORT = 18018
FLIGHT_PORT = 19018
FLIGHT_SQL_PORT = 18918
ADMIN_PORT = 18088
METRIC_PORT = 17088
META_GRPC_PORT = 19192
META_ADMIN_PORT = 19212
META_RAFT_PORT = 19214

TEST_NAME = "sleep_config_blackbox"
WORK_DIR = ROOT / ".databend" / TEST_NAME
CONFIG_PATH = WORK_DIR / "databend-query.toml"
META_LOG = WORK_DIR / "meta.out"


def wait_tcp(port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def write_config(limit):
    sleep_config = "" if limit is None else f"max_sleep_seconds = {limit}"
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(
        textwrap.dedent(
            f"""
            [query]
            max_active_sessions = 256
            shutdown_wait_timeout_ms = 5000

            flight_api_address = "0.0.0.0:{FLIGHT_PORT}"
            admin_api_address = "0.0.0.0:{ADMIN_PORT}"
            metric_api_address = "0.0.0.0:{METRIC_PORT}"

            mysql_handler_host = "0.0.0.0"
            mysql_handler_port = {MYSQL_PORT}

            http_handler_host = "0.0.0.0"
            http_handler_port = {HTTP_PORT}

            flight_sql_handler_host = "0.0.0.0"
            flight_sql_handler_port = {FLIGHT_SQL_PORT}

            tenant_id = "{TEST_NAME}"
            cluster_id = "{TEST_NAME}"
            warehouse_id = "{TEST_NAME}"

            table_engine_memory_enabled = true
            default_storage_format = "parquet"
            default_compression = "zstd"

            {sleep_config}

            [[query.users]]
            name = "root"
            auth_type = "no_password"

            [[query.users]]
            name = "default"
            auth_type = "no_password"

            [log]

            [log.file]
            level = "DEBUG"
            format = "text"
            dir = "{WORK_DIR / "logs"}"
            limit = 12

            [meta]
            endpoints = ["127.0.0.1:{META_GRPC_PORT}"]
            username = "root"
            password = "root"
            client_timeout_in_second = 60
            auto_sync_interval = 60

            [storage]
            type = "fs"

            [storage.fs]
            data_path = "{WORK_DIR / "data"}"

            [cache]
            data_cache_storage = "none"

            [cache.disk]
            path = "{WORK_DIR / "_cache"}"
            max_bytes = 21474836480

            [spill]
            spill_local_disk_path = "{WORK_DIR / "spill"}"
            """
        ).strip()
        + "\n"
    )


@contextmanager
def running_service(args, log_path, port):
    # Own each child even if startup or a test assertion fails.
    # CI exports S3 settings for its shared cluster; this pair uses its own TOML.
    env = {
        key: value
        for key, value in os.environ.items()
        if key != "CONFIG_FILE"
        and not key.startswith(("QUERY_", "META_", "STORAGE_", "CACHE_", "SPILL_"))
    }
    with log_path.open("w") as log_file:
        proc = subprocess.Popen(
            args, cwd=ROOT, env=env, stdout=log_file, stderr=log_file
        )
        try:
            if not wait_tcp(port) or proc.poll() is not None:
                raise RuntimeError(f"service failed to start: {args[0]}")
            yield
        except Exception:
            print(f"---- {log_path.name} tail ----")
            print("\n".join(log_path.read_text(errors="replace").splitlines()[-80:]))
            raise
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)


def execute(sql, error=None):
    result = subprocess.run(
        [
            "bendsql",
            "--host",
            "127.0.0.1",
            "--port",
            str(HTTP_PORT),
            "-uroot",
            "--output",
            "tsv",
            "--quote-style",
            "never",
            "--log-level",
            "error",
            f"--query={sql}",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=30,
    )
    if error is not None:
        assert result.returncode != 0, f"unexpected success: {sql}"
        assert error in result.stderr, result.stderr
    else:
        assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def run_case():
    query_bin = ROOT / "target" / BUILD_PROFILE / "databend-query"
    meta_bin = ROOT / "target" / BUILD_PROFILE / "databend-meta"
    for binary in (query_bin, meta_bin):
        if not binary.is_file():
            raise RuntimeError(f"missing binary: {binary}")

    # This test owns a private meta/query pair in both standalone and cluster CI.
    # Never connect to or restart the suite's existing services.
    for port in (
        MYSQL_PORT,
        HTTP_PORT,
        FLIGHT_PORT,
        FLIGHT_SQL_PORT,
        ADMIN_PORT,
        METRIC_PORT,
        META_GRPC_PORT,
        META_ADMIN_PORT,
        META_RAFT_PORT,
    ):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))
    if WORK_DIR.exists():
        shutil.rmtree(WORK_DIR)
    WORK_DIR.mkdir(parents=True)

    with running_service(
        [
            str(meta_bin),
            "--single",
            "--grpc-api-address",
            f"127.0.0.1:{META_GRPC_PORT}",
            "--admin-api-address",
            f"127.0.0.1:{META_ADMIN_PORT}",
            "--raft-api-port",
            str(META_RAFT_PORT),
            "--raft-dir",
            str(WORK_DIR / "meta"),
            "--log-file-dir",
            str(WORK_DIR / "meta_logs"),
        ],
        META_LOG,
        META_GRPC_PORT,
    ):
        # Omit the field first to test the server default, then restart query
        # with a raised limit and finally no limit. Meta persists across cases.
        for limit in (None, 4, 0):
            write_config(limit)
            label = "default" if limit is None else str(limit)
            with running_service(
                [str(query_bin), "-c", str(CONFIG_PATH)],
                WORK_DIR / f"query-{label}.out",
                HTTP_PORT,
            ):
                expected_limit = 3 if limit is None else limit
                value = execute(
                    "select value from system.configs where name = 'max_sleep_seconds'"
                )
                assert value == str(expected_limit), value
                assert execute("select sleep(0)") == "0"
                if limit is None:
                    execute("select sleep(3.01)", "The maximum sleep time is 3 seconds")
                else:
                    assert execute("select sleep(3.01)") == "0"
                    if limit == 4:
                        execute(
                            "select sleep(4.01)", "The maximum sleep time is 4 seconds"
                        )
                if limit == 0:
                    execute("select sleep(-1)", "value is negative")
                    execute(
                        "select sleep('inf'::float64)", "value is either too big or NaN"
                    )
                    execute(
                        "select sleep('nan'::float64)", "value is either too big or NaN"
                    )
                print(f"sleep max_sleep_seconds={label}: ok")


if __name__ == "__main__":
    run_case()
