"""Environment hooks used by databend-sqllogictests.

The Rust runner invokes these through a generic nox session. Hook manifests are
plain argv lists, so the runner does not depend on nox.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
HOOK_DIR = Path(__file__).resolve().parent
CACHE_DIR = HOOK_DIR.parent / "cache"
DATA_DIR = ROOT / "tests" / "data"

TPCH_TABLES = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "partsupp",
    "part",
    "region",
    "supplier",
)
TPCDS_TABLES = (
    "call_center",
    "catalog_returns",
    "customer_address",
    "customer_demographics",
    "household_demographics",
    "inventory",
    "promotion",
    "ship_mode",
    "store_returns",
    "time_dim",
    "web_page",
    "web_sales",
    "catalog_page",
    "catalog_sales",
    "customer",
    "date_dim",
    "income_band",
    "item",
    "reason",
    "store",
    "store_sales",
    "warehouse",
    "web_returns",
    "web_site",
)


def databend_dsn() -> str:
    host = os.environ.get("QUERY_MYSQL_HANDLER_HOST", "127.0.0.1")
    port = os.environ.get("QUERY_HTTP_HANDLER_PORT", "8000")
    return f"databend://root:@{host}:{port}/?sslmode=disable"


class DatabendConnection:
    def __init__(self) -> None:
        from databend_driver import BlockingDatabendClient

        self.connection = BlockingDatabendClient(databend_dsn()).get_conn()

    def execute(self, sql: str) -> None:
        self.connection.exec(sql)

    def execute_script(self, sql: str) -> None:
        for statement in sql.split(";"):
            if statement := statement.strip():
                self.execute(statement)

    def count(self, sql: str) -> int:
        return int(self.connection.query_row(sql).values()[0])

    def close(self) -> None:
        self.connection.close()


def run(command: list[str]) -> None:
    subprocess.run(command, cwd=ROOT, check=True)


def data_is_loaded(connection: DatabendConnection, database: str, table: str) -> bool:
    if os.environ.get("DATABEND_SQLLOGICTEST_FORCE_LOAD", "0") != "0":
        return False

    table_exists = connection.count(
        f"SELECT COUNT() FROM system.tables WHERE database = '{database}' "
        f"AND name = '{table}'"
    )
    if not table_exists:
        return False

    row_count = connection.count(f"SELECT COUNT() FROM {database}.{table}")
    if not row_count:
        return False

    print(
        f"Table {database}.{table} already exists and is not empty, "
        f"size: {row_count}. Use --force_load to override it."
    )
    return True


def duckdb_cache_is_valid(output: Path, kind: str) -> bool:
    tables = TPCH_TABLES if kind == "tpch" else TPCDS_TABLES
    return all(
        (csv := output / f"{table}.csv").is_file() and csv.stat().st_size > 0
        for table in tables
    )


def generate_duckdb_data(kind: str, scale_factor: int) -> Path:
    import duckdb

    if kind not in {"tpch", "tpcds"}:
        raise ValueError(f"unsupported DuckDB data set: {kind}")

    output = CACHE_DIR / f"{kind}_{scale_factor}"
    if duckdb_cache_is_valid(output, kind):
        print(f"Reusing DuckDB cache: {output}")
        return output

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(output, ignore_errors=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.install_extension(kind)
        connection.load_extension(kind)
        generator = "dbgen" if kind == "tpch" else "dsdgen"
        connection.execute(f"CALL {generator}(sf={scale_factor})")
        connection.execute(f"EXPORT DATABASE '{output}' (FORMAT CSV, DELIMITER '|')")
    finally:
        connection.close()

    if not duckdb_cache_is_valid(output, kind):
        raise RuntimeError(f"generated DuckDB cache is incomplete: {output}")
    return output


def prepare_tpch(connection: DatabendConnection) -> None:
    database = "tpch_test"
    if data_is_loaded(connection, database, "nation"):
        return

    connection.execute(f"DROP DATABASE IF EXISTS {database}")
    connection.execute(f"CREATE DATABASE {database}")
    connection.execute(f"USE {database}")
    connection.execute_script((HOOK_DIR / "tpch.sql").read_text())

    data_dir = generate_duckdb_data("tpch", 1)
    connection.execute("DROP STAGE IF EXISTS s1")
    connection.execute(f"CREATE STAGE s1 URL='fs://{data_dir}/'")
    for table in TPCH_TABLES:
        print(table)
        connection.execute(
            f"COPY INTO {database}.{table} FROM @s1/{table}.csv FORCE = true "
            "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = '|' "
            "RECORD_DELIMITER = '\\n')"
        )
        connection.execute(f"ANALYZE TABLE {database}.{table}")


def prepare_tpcds(connection: DatabendConnection) -> None:
    database = "tpcds"
    if data_is_loaded(connection, database, "call_center"):
        return

    connection.execute(f"CREATE OR REPLACE DATABASE {database}")
    connection.execute_script((HOOK_DIR / "tpcds.sql").read_text())

    data_dir = generate_duckdb_data("tpcds", 1)
    connection.execute("DROP STAGE IF EXISTS s1")
    connection.execute(f"CREATE STAGE s1 URL='fs://{data_dir}/'")
    for table in TPCDS_TABLES:
        print(table)
        connection.execute(
            f"COPY INTO {database}.{table} FROM @s1/{table}.csv "
            "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = '|' "
            "RECORD_DELIMITER = '\\n')"
        )
        connection.execute(f"ANALYZE TABLE {database}.{table}")


def prepare_stage(connection: DatabendConnection) -> None:
    storage = os.environ.get("TEST_STAGE_STORAGE", "fs")
    if storage not in {"fs", "s3"}:
        raise ValueError(f"unknown TEST_STAGE_STORAGE value: {storage}")
    dedup = os.environ.get("TEST_STAGE_DEDUP", "full_path")
    if dedup not in {"full_path", "sub_path"}:
        raise ValueError("TEST_STAGE_DEDUP must be 'full_path' or 'sub_path'")
    size = os.environ.get("TEST_STAGE_SIZE", "small")
    if size not in {"small", "large"}:
        raise ValueError("TEST_STAGE_SIZE must be 'small' or 'large'")

    data_url = f"fs://{DATA_DIR.resolve()}/"
    connection.execute("DROP STAGE IF EXISTS data")
    connection.execute(
        "CREATE OR REPLACE CONNECTION my_conn_s3 STORAGE_TYPE = 's3' "
        "ACCESS_KEY_ID = 'minioadmin' SECRET_ACCESS_KEY = 'minioadmin' "
        "ENDPOINT_URL = 'http://127.0.0.1:9900'"
    )
    connection.execute(
        "CREATE OR REPLACE STAGE data_s3 URL = 's3://testbucket/data/' "
        "CONNECTION = (CONNECTION_NAME = 'my_conn_s3') "
        "FILE_FORMAT = (TYPE = PARQUET)"
    )
    connection.execute(
        f"CREATE OR REPLACE STAGE data_fs URL = '{data_url}' "
        "FILE_FORMAT = (TYPE = PARQUET)"
    )
    if storage == "fs":
        connection.execute(
            f"CREATE OR REPLACE STAGE data URL = '{data_url}' "
            "FILE_FORMAT = (TYPE = PARQUET)"
        )
    else:
        connection.execute(
            "CREATE OR REPLACE STAGE data URL = 's3://testbucket/data/' "
            "CONNECTION = (CONNECTION_NAME = 'my_conn_s3') "
            "FILE_FORMAT = (TYPE = PARQUET)"
        )

    connection.execute("DROP TABLE IF EXISTS ontime")
    connection.execute_script((DATA_DIR / "ddl" / "ontime.sql").read_text())
    connection.execute(
        "SET GLOBAL copy_dedup_full_path_by_default = "
        f"{1 if dedup == 'full_path' else 0}"
    )
    connection.execute(
        "SET GLOBAL parquet_fast_read_bytes = "
        f"{1048576 if size == 'small' else 0}"
    )


def dictionary_prepare() -> None:
    dictionary_cleanup()
    try:
        run(
            [
                "docker",
                "run",
                "--rm",
                "--detach",
                "--name",
                "databend-sqllogic-redis",
                "--network",
                "host",
                "redis:5.0",
            ]
        )
        run(
            [
                "docker",
                "run",
                "--rm",
                "--detach",
                "--name",
                "databend-sqllogic-mysql",
                "--network",
                "host",
                "-e",
                "MYSQL_DATABASE=test",
                "-e",
                "MYSQL_ALLOW_EMPTY_PASSWORD=yes",
                "mysql:8.1",
            ]
        )
        run(
            [
                "docker",
                "exec",
                "databend-sqllogic-mysql",
                "sh",
                "-c",
                "for _ in $(seq 1 60); do "
                "mysqladmin ping -h 127.0.0.1 --silent && break; sleep 1; done; "
                "mysqladmin ping -h 127.0.0.1 --silent || exit 1; "
                "mysql -uroot test <<'SQL'\n"
                "CREATE TABLE user(id INT, name VARCHAR(100), age SMALLINT UNSIGNED, "
                "salary DOUBLE, active BOOL);\n"
                "INSERT INTO user VALUES (1, 'Alice', 24, 100, true), "
                "(2, 'Bob', 35, 200.1, false), (3, 'Lily', 41, 1000.2, true), "
                "(4, 'Tom', 55, 3000.55, false), (5, NULL, NULL, NULL, NULL);\n"
                "SQL",
            ]
        )
        run(
            [
                "docker",
                "exec",
                "databend-sqllogic-redis",
                "sh",
                "-c",
                "for _ in $(seq 1 60); do redis-cli ping >/dev/null && break; "
                "sleep 1; done; redis-cli ping >/dev/null || exit 1; "
                "redis-cli MSET a a_value b b_value c c_value 1 1_value 2 2_value",
            ]
        )
    except Exception:
        dictionary_cleanup()
        raise


def dictionary_cleanup() -> None:
    for name in ("databend-sqllogic-mysql", "databend-sqllogic-redis"):
        subprocess.run(
            ["docker", "rm", "--force", name],
            cwd=ROOT,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


def run_hook(phase: str, name: str) -> None:
    if phase == "cleanup":
        if name != "dictionaries":
            raise ValueError(f"hook does not define cleanup: {name}")
        dictionary_cleanup()
        return
    if phase != "prepare":
        raise ValueError(f"unknown hook phase: {phase}")
    if name == "dictionaries":
        dictionary_prepare()
        return

    connection = DatabendConnection()
    try:
        if name == "tpch":
            prepare_tpch(connection)
        elif name == "tpcds":
            prepare_tpcds(connection)
        elif name == "stage":
            prepare_stage(connection)
        else:
            raise ValueError(f"unknown sqllogictest hook: {name}")
    finally:
        connection.close()


if __name__ == "__main__":
    run_hook(sys.argv[1], sys.argv[2])
