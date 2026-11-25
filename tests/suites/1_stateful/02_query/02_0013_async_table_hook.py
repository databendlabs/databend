#!/usr/bin/env python3

import os
import shutil
import socket
import subprocess
import textwrap
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.request import urlopen

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
PEER_DIR = WORK_DIR / "peer"
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


def write_config(node_dir, port_offset):
    node_dir.mkdir(parents=True, exist_ok=True)
    config_path = node_dir / "databend-query.toml"
    config_path.write_text(
        textwrap.dedent(
            f"""
            [query]
            max_active_sessions = 256
            shutdown_wait_timeout_ms = 5000

            flight_api_address = "0.0.0.0:{FLIGHT_PORT + port_offset}"
            admin_api_address = "0.0.0.0:{ADMIN_PORT + port_offset}"
            metric_api_address = "0.0.0.0:{METRIC_PORT + port_offset}"

            mysql_handler_host = "0.0.0.0"
            mysql_handler_port = {MYSQL_PORT + port_offset}

            http_handler_host = "0.0.0.0"
            http_handler_port = {HTTP_PORT + port_offset}

            flight_sql_handler_host = "0.0.0.0"
            flight_sql_handler_port = {FLIGHT_SQL_PORT + port_offset}

            tenant_id = "{TEST_NAME}"
            cluster_id = "{TEST_NAME}"
            warehouse_id = "{TEST_NAME}"

            table_engine_memory_enabled = true
            default_storage_format = "parquet"
            default_compression = "zstd"

            table_hook_async_max_concurrency = 1

            # Keep block layout deterministic without disabling async hooks.
            [query.settings]
            max_threads = 1

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
            dir = "{node_dir / "logs"}"
            limit = 12

            [log.structlog]
            on = true
            dir = "{node_dir / "structlog"}"

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
            spill_local_disk_path = "{node_dir / "spill"}"
            """
        ).strip()
        + "\n"
    )
    return config_path


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


def start_query(node_dir=WORK_DIR, port_offset=0):
    query_bin = ROOT / "target" / BUILD_PROFILE / "databend-query"
    if not query_bin.exists():
        raise RuntimeError(f"missing databend-query binary: {query_bin}")

    config_path = write_config(node_dir, port_offset)
    with (node_dir / "query.out").open("w") as log_file:
        env = os.environ.copy()
        env["RUST_BACKTRACE"] = "1"
        proc = subprocess.Popen(
            [str(query_bin), "-c", str(config_path)],
            cwd=str(ROOT),
            env=env,
            stdout=log_file,
            stderr=log_file,
        )
    if not wait_tcp(MYSQL_PORT + port_offset) or not wait_tcp(HTTP_PORT + port_offset):
        proc.terminate()
        proc.wait(timeout=5)
        raise RuntimeError(f"async query node did not start: {node_dir}")
    return proc


def execute(sql, port=HTTP_PORT, timeout=60):
    result = subprocess.run(
        [
            "bendsql",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
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
        check=False,
        timeout=timeout,
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
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = execute(sql)
        if predicate(last):
            return last
        time.sleep(0.1)
    raise RuntimeError(f"condition not met for sql: {sql}; last result: {last}")


def maintenance_gauges():
    with urlopen(f"http://127.0.0.1:{METRIC_PORT}/metrics", timeout=5) as response:
        text = response.read().decode()
    prefix = "databend_table_maintenance_"
    gauges = {}
    for line in text.splitlines():
        if line.startswith(prefix):
            name, value, *_ = line[len(prefix):].split()
            if name in ("mailbox_tables", "reserved_memory_bytes"):
                gauges[name] = float(value)
    return gauges["mailbox_tables"], gauges["reserved_memory_bytes"]


def wait_for_maintenance_idle(timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        tables, reserved = maintenance_gauges()
        if tables == 0 and reserved == 0:
            return
        time.sleep(0.1)
    raise RuntimeError(f"maintenance failed to release resources: tables={tables}, reserved={reserved}")


def run_refresh_lifecycle_case():
    # Any Recluster request that leaks through the disabled setting will stay queued.
    execute("SET GLOBAL recluster_accumulation_secs = 600")
    disable_compact = "SET enable_compact_after_write = 0; "
    table = f"{TEST_NAME}.coverage"
    execute(
        f"CREATE TABLE {table}(content STRING) CLUSTER BY(content) "
        "ROW_PER_BLOCK=1 BLOCK_PER_SEGMENT=1000 AUTO_COMPACTION_IMPERFECT_BLOCKS_THRESHOLD=0"
    )
    execute(disable_compact + f"INSERT INTO {table} VALUES ('alpha')")
    wait_for_maintenance_idle()
    execute(f"CREATE ASYNC INVERTED INDEX inv ON {table}(content)")
    execute(f"CREATE ASYNC NGRAM INDEX ng ON {table}(content)")
    coverage = (
        "SELECT count_if(inverted_index_size > 0), count_if(ngram_index_size > 0), count(*) "
        f"FROM fuse_block('{TEST_NAME}', 'coverage')"
    )
    execute(disable_compact + f"BEGIN; INSERT INTO {table} VALUES ('discard'); ROLLBACK;")
    wait_for_maintenance_idle()
    assert execute(coverage) == [["0", "0", "1"]]

    execute(
        disable_compact + f"BEGIN; INSERT INTO {table} VALUES ('gamma'); "
        f"INSERT INTO {table} VALUES ('delta'); COMMIT;"
    )
    wait_for_sql(coverage, lambda rows: rows == [["2", "2", "3"]], timeout=60)
    wait_for_maintenance_idle()
    execute("UNSET GLOBAL recluster_accumulation_secs")
    execute(f"REFRESH INVERTED INDEX inv ON {table}")
    assert execute(f"SELECT count(*) FROM {table} WHERE match(content, 'alpha')") == [["1"]]

    print("async refresh: only committed increments; manual refresh backfills history: ok")


def run_recluster_idle_case():
    # This table produces no Compact hint even with Compact enabled. A small
    # committed batch must still be pending with a zero accumulation window.
    execute("SET GLOBAL recluster_accumulation_secs = 0")
    table = f"{TEST_NAME}.clustered"
    execute(
        f"CREATE TABLE {table}(a INT, content STRING, embedding VECTOR(4)) "
        "CLUSTER BY(a) ROW_PER_BLOCK=5 AUTO_COMPACTION_IMPERFECT_BLOCKS_THRESHOLD=0"
    )
    execute(f"CREATE ASYNC INVERTED INDEX inv ON {table}(content)")
    execute(f"CREATE ASYNC VECTOR INDEX vec ON {table}(embedding) distance='l2'")
    for batch in [(1, 2, 7), (3, 5, 8), (4, 6, 10, 11), (0, 9, 12)]:
        values = ",".join(f"({a}, '{'alpha' if a % 2 else 'beta'}', [{a},0,0,0])" for a in batch)
        execute(f"INSERT INTO {table} VALUES {values}")

    blocks = f"fuse_block('{TEST_NAME}', 'clustered')"
    missing_indexes = (
        f"SELECT count(*) FROM {blocks} "
        "WHERE coalesce(inverted_index_size, 0) = 0 OR coalesce(vector_index_size, 0) = 0"
    )
    wait_for_sql(missing_indexes, lambda rows: rows == [["0"]], timeout=60)
    locations = f"SELECT block_location FROM {blocks} ORDER BY block_location"
    before = execute(locations)
    match_query = f"SELECT a FROM {table} WHERE match(content, 'alpha') ORDER BY a"
    vector_query = (
        f"SELECT a FROM {table} ORDER BY l2_distance(embedding, [5,0,0,0]::VECTOR(4)) LIMIT 1"
    )
    expected_matches = [[str(a)] for a in range(1, 13, 2)]
    assert maintenance_gauges()[0] == 1, "small clustered write was not retained in the mailbox"

    # Keep the peer busy past the writer's 60s idle interval and the 5s recheck.
    # Observe passively: any SQL on the writer would reset its idle interval.
    with ThreadPoolExecutor(max_workers=1) as pool:
        busy_peer = pool.submit(execute, "SELECT sleep(72) FROM numbers(1)", HTTP_PORT + 1, 90)
        until = time.monotonic() + 66
        while time.monotonic() < until:
            assert not busy_peer.done(), "peer query finished before the idle check"
            assert maintenance_gauges()[0] == 1, "Recluster drained while the peer was busy"
            time.sleep(0.5)
        assert busy_peer.result() == [["0"]]
    peer_finished = time.monotonic()

    wait_for_maintenance_idle(timeout=90)
    # Allow slack for the client to receive the result after the server finishes.
    assert time.monotonic() - peer_finished >= 55, "Recluster did not wait for the peer to become idle"
    assert execute(locations) != before, "Recluster did not replace any blocks after warehouse idle"
    assert execute(missing_indexes) == [["0"]], "Recluster lost index coverage"
    assert execute(match_query) == expected_matches
    # ANN ranking can change after a rebuild; only require a successful search.
    assert len(execute(vector_query)) == 1
    execute("UNSET GLOBAL recluster_accumulation_secs")
    print("async recluster: zero Compact hint retained until warehouse idle; indexes preserved: ok")


def print_log_tail():
    for node_dir in (WORK_DIR, PEER_DIR):
        for log_path in [node_dir / "query.out", *sorted((node_dir / "logs").glob("**/*"))]:
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
    query_procs = []
    try:
        if WORK_DIR.exists():
            shutil.rmtree(WORK_DIR)

        meta_proc, meta_log_file = start_meta()
        query_procs.append(start_query())

        execute(
            f"drop database if exists {TEST_NAME}",
        )
        execute(f"create database {TEST_NAME}")
        run_refresh_lifecycle_case()
        query_procs.append(start_query(PEER_DIR, port_offset=1))
        wait_for_sql("SELECT count() FROM system.clusters", lambda rows: rows == [["2"]])
        run_recluster_idle_case()

        execute(f"drop database if exists {TEST_NAME}")
    except Exception:
        print_log_tail()
        raise
    finally:
        for proc in reversed(query_procs):
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
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
