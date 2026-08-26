#!/usr/bin/env python3

import os
import shutil
import socket
import subprocess
import textwrap
import time
from pathlib import Path


CURDIR = Path(__file__).resolve().parent
ROOT = CURDIR.parents[3]
BUILD_PROFILE = os.getenv("BUILD_PROFILE", "debug")

MYSQL_PORT = 13317
HTTP_PORT = 18017
FLIGHT_PORT = 19017
FLIGHT_SQL_PORT = 18917
ADMIN_PORT = 18087
METRIC_PORT = 17087
META_GRPC_PORT = 19191
META_ADMIN_PORT = 19202
META_RAFT_PORT = 19204

TEST_NAME = "async_table_hook_blackbox"
WORK_DIR = ROOT / ".databend" / TEST_NAME
CONFIG_PATH = WORK_DIR / "databend-query.toml"
QUERY_LOG = WORK_DIR / "query.out"
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


def write_config():
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

            table_hook_mode = "async"
            table_hook_async_max_concurrency = 1

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

            [log.structlog]
            on = true
            dir = "{WORK_DIR / "structlog"}"

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


def start_meta():
    meta_bin = ROOT / "target" / BUILD_PROFILE / "databend-meta"
    if not meta_bin.exists():
        raise RuntimeError(f"missing databend-meta binary: {meta_bin}")

    WORK_DIR.mkdir(parents=True, exist_ok=True)
    log_file = META_LOG.open("w")
    proc = subprocess.Popen(
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
            "--log-level",
            "DEBUG",
        ],
        cwd=str(ROOT),
        stdout=log_file,
        stderr=log_file,
    )
    if not wait_tcp(META_GRPC_PORT, timeout=30):
        proc.terminate()
        proc.wait(timeout=5)
        raise RuntimeError("private meta did not start")
    return proc, log_file


def start_query():
    query_bin = ROOT / "target" / BUILD_PROFILE / "databend-query"
    if not query_bin.exists():
        raise RuntimeError(f"missing databend-query binary: {query_bin}")

    write_config()
    log_file = QUERY_LOG.open("w")
    env = os.environ.copy()
    env["RUST_BACKTRACE"] = "1"
    proc = subprocess.Popen(
        [str(query_bin), "-c", str(CONFIG_PATH)],
        cwd=str(ROOT),
        env=env,
        stdout=log_file,
        stderr=log_file,
    )
    if not wait_tcp(MYSQL_PORT, timeout=30) or not wait_tcp(HTTP_PORT, timeout=30):
        proc.terminate()
        proc.wait(timeout=5)
        raise RuntimeError("async query node did not start")
    return proc, log_file


def execute(sql):
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
        cwd=str(ROOT),
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"bendsql failed with code {result.returncode}\n"
            f"sql: {sql}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return [line.split("\t") for line in result.stdout.splitlines() if line]


def wait_for_sql(sql, predicate, timeout=30):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = execute(sql)
        if predicate(last):
            return last
        time.sleep(0.5)
    raise RuntimeError(f"condition not met for sql: {sql}; last result: {last}")


def wait_for_log(pattern, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if QUERY_LOG.exists() and pattern in QUERY_LOG.read_text(errors="ignore"):
            return
        for log_path in (WORK_DIR / "logs").glob("**/*"):
            if log_path.is_file() and pattern in log_path.read_text(errors="ignore"):
                return
        time.sleep(0.5)
    raise RuntimeError(f"log pattern not found: {pattern}")


def print_log_tail():
    if QUERY_LOG.exists():
        print("---- query log tail ----")
        lines = QUERY_LOG.read_text(errors="ignore").splitlines()
        print("\n".join(lines[-80:]))
    for log_path in sorted((WORK_DIR / "logs").glob("**/*")):
        if log_path.is_file():
            print(f"---- {log_path} tail ----")
            lines = log_path.read_text(errors="ignore").splitlines()
            print("\n".join(lines[-80:]))
    if META_LOG.exists():
        print("---- meta log tail ----")
        lines = META_LOG.read_text(errors="ignore").splitlines()
        print("\n".join(lines[-80:]))


def run_case():
    meta_proc = None
    meta_log_file = None
    proc = None
    log_file = None
    try:
        if WORK_DIR.exists():
            shutil.rmtree(WORK_DIR)

        meta_proc, meta_log_file = start_meta()
        proc, log_file = start_query()

        mode = execute(
            "select value from system.configs where name like '%table_hook_mode'",
        )
        concurrency = execute(
            "select value from system.configs "
            "where name like '%table_hook_async_max_concurrency'",
        )
        assert mode == [["async"]], mode
        assert concurrency == [["1"]], concurrency
        print("async table hook config: ok")

        execute(
            f"drop database if exists {TEST_NAME}",
        )
        execute(f"create database {TEST_NAME}")
        execute(
            f"create table {TEST_NAME}.t(a int) row_per_block = 5 "
            "auto_compaction_imperfect_blocks_threshold = 3",
        )

        # These writes reproduce the auto-compaction case from 09_0041_auto_compaction:
        # several small/imperfect blocks are committed first, then the async table hook
        # should compact them in the background into a single segment with 3 blocks.
        execute(f"insert into {TEST_NAME}.t values(0)")
        execute(f"insert into {TEST_NAME}.t select number from numbers(12)")
        execute(f"insert into {TEST_NAME}.t select number from numbers(2)")
        execute(f"insert into {TEST_NAME}.t select number from numbers(2)")

        wait_for_log("Start async table hook job")
        print("async table hook dispatched: ok")

        wait_for_sql(
            f"select block_count, row_count from fuse_segment('{TEST_NAME}', 't')",
            lambda rows: rows == [["3", "17"]],
            timeout=60,
        )
        print("async table hook compact completed: ok")

        execute(f"drop database if exists {TEST_NAME}")
    except Exception:
        print_log_tail()
        raise
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        if log_file is not None:
            log_file.close()
        if meta_proc is not None:
            meta_proc.terminate()
            try:
                meta_proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                meta_proc.kill()
                meta_proc.wait(timeout=10)
        if meta_log_file is not None:
            meta_log_file.close()


if __name__ == "__main__":
    run_case()
