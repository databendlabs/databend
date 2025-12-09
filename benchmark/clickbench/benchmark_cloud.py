#!/usr/bin/env python3
"""Databend Cloud benchmark runner backed by bendsql."""

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


@dataclass
class BenchmarkConfig:
    benchmark_id: str
    dataset: str
    size: str
    cache_size: str
    version: str
    database: str
    tries: int
    user: str
    password: str
    gateway: str
    warehouse: str
    source: str
    source_id: str
    sha: str


@dataclass
class ResultRecord:
    date: str
    dataset: str
    database: str
    version: str
    warehouse: str
    machine: str
    cluster_size: str
    tags: List[str]
    result: List[List[float]]
    values: Dict[str, List[float]]
    run_id: str
    size: str
    tries: int
    storage: str
    cache_size: str
    load_time: Optional[float] = None
    data_size: Optional[int] = None
    system: Optional[str] = None
    comment: Optional[str] = None


class BendSQLRunner:
    def __init__(self) -> None:
        self._env = os.environ.copy()

    def set_dsn(self, dsn: str) -> None:
        self._env["BENDSQL_DSN"] = dsn
        logger.debug("Using DSN: %s", dsn)

    def run(
        self,
        args: Optional[List[str]] = None,
        *,
        sql: Optional[str] = None,
        capture_output: bool = False,
    ) -> subprocess.CompletedProcess:
        command = ["bendsql"] + (args or [])
        logger.debug("Running command: %s", " ".join(command))
        try:
            result = subprocess.run(
                command,
                input=sql,
                text=True,
                env=self._env,
                capture_output=True,  # Always capture output
                check=True,
            )
            # If not capturing output, print it to stdout/stderr
            if not capture_output:
                if result.stdout:
                    print(result.stdout, end="", file=sys.stdout, flush=True)
                if result.stderr:
                    print(result.stderr, end="", file=sys.stderr, flush=True)
            return result
        except subprocess.CalledProcessError as exc:  # pragma: no cover - passthrough
            stdout = exc.stdout.strip() if exc.stdout else ""
            stderr = exc.stderr.strip() if exc.stderr else ""
            logger.error("bendsql failed: %s", stderr or stdout)
            raise


def load_config() -> BenchmarkConfig:
    benchmark_id = os.environ.get("BENCHMARK_ID", str(int(time.time())))
    dataset = os.environ.get("BENCHMARK_DATASET", "hits")
    size = os.environ.get("BENCHMARK_SIZE", "Small")
    cache_size = os.environ.get("BENCHMARK_CACHE_SIZE", "0")
    version = os.environ.get("BENCHMARK_VERSION", "")
    database = os.environ.get("BENCHMARK_DATABASE", "default")
    tries_raw = os.environ.get("BENCHMARK_TRIES", "3")
    source = os.environ.get("BENCHMARK_SOURCE", "")
    source_id = os.environ.get("BENCHMARK_SOURCE_ID", "")
    sha = os.environ.get("BENCHMARK_SHA", "")

    if not version:
        logger.error("Please set BENCHMARK_VERSION to run the benchmark.")
        sys.exit(1)

    try:
        tries = int(tries_raw)
    except ValueError:
        logger.error("BENCHMARK_TRIES must be an integer, got %s", tries_raw)
        sys.exit(1)

    if tries < 1:
        logger.error("BENCHMARK_TRIES must be positive, got %s", tries)
        sys.exit(1)

    user = os.environ.get("CLOUD_USER", "")
    password = os.environ.get("CLOUD_PASSWORD", "")
    gateway = os.environ.get("CLOUD_GATEWAY", "")
    warehouse = os.environ.get("CLOUD_WAREHOUSE", f"benchmark-{benchmark_id}")

    if not user or not password or not gateway:
        logger.error(
            "Please set CLOUD_USER, CLOUD_PASSWORD and CLOUD_GATEWAY to run the benchmark.",
        )
        sys.exit(1)

    return BenchmarkConfig(
        benchmark_id=benchmark_id,
        dataset=dataset,
        size=size,
        cache_size=cache_size,
        version=version,
        database=database,
        tries=tries,
        user=user,
        password=password,
        gateway=gateway,
        warehouse=warehouse,
        source=source,
        source_id=source_id,
        sha=sha,
    )


def ensure_dependencies() -> None:
    if not shutil.which("bendsql"):
        logger.error("bendsql is required but was not found in PATH.")
        sys.exit(1)
    logger.info("Checking script dependencies...")
    logger.info("bendsql version: %s", subprocess.check_output(["bendsql", "--version"]).decode().strip())


SIZE_MAPPING: Dict[str, Dict[str, str]] = {
    "Small": {"cluster_size": "16", "machine": "Small"},
    "Large": {"cluster_size": "64", "machine": "Large"},
}


def build_dsn(
    config: BenchmarkConfig,
    *,
    database: Optional[str] = None,
    warehouse: Optional[str] = None,
    login_disable: bool = False,
) -> str:
    params = []
    if login_disable:
        params.append("login=disable")
    if warehouse:
        params.append(f"warehouse={warehouse}")
    query = f"?{'&'.join(params)}" if params else ""
    db_path = f"/{database}" if database else ""
    return f"databend://{config.user}:{config.password}@{config.gateway}:443{db_path}{query}"


def quote_literal(value: str) -> str:
    return value.replace("'", "''")


def resolve_sql_dataset(dataset: str) -> str:
    trimmed = re.sub(r"\d+$", "", dataset)
    return trimmed if trimmed else dataset


def wait_for_warehouse(runner: BendSQLRunner, warehouse: str, retries: int = 20, delay: int = 10) -> None:
    logger.info("Waiting for warehouse %s to be ready...", warehouse)
    for attempt in range(retries + 1):
        completed = runner.run(
            ["--output", "csv"],
            sql=f"SHOW WAREHOUSES LIKE '{quote_literal(warehouse)}'",
            capture_output=True,
        )
        output = completed.stdout.strip()
        if output and "Running" in output:
            logger.info("Warehouse %s is running.", warehouse)
            return
        logger.info("Warehouse not ready yet. Sleeping %s seconds...", delay)
        time.sleep(delay)
    logger.error("Failed to start warehouse %s in time.", warehouse)
    sys.exit(1)


def execute_sql_file(runner: BendSQLRunner, path: Path) -> None:
    sql = path.read_text()
    if sql.strip():
        runner.run(sql=sql)


def parse_time_output(raw_value: str) -> Optional[float]:
    # --time=server outputs only the time value (e.g., "0.014")
    value = raw_value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def run_timed_query(runner: BendSQLRunner, sql: str) -> Optional[float]:
    completed = runner.run(["--time=server"], sql=sql, capture_output=True)
    output = completed.stdout.strip()
    return parse_time_output(output)


def write_result_files(script_dir: Path, record: ResultRecord) -> None:
    result_path = script_dir / "result.json"
    cache_suffix = f"-cache-{record.cache_size}" if record.cache_size else ""
    final_result_path = script_dir / f"result-{record.dataset}-cloud-{record.size}{cache_suffix}.json"
    ndjson_name = (
        f"result-{record.dataset}-cloud-{record.size}{cache_suffix}"
        f"-{record.run_id}.ndjson"
    )
    ndjson_path = script_dir / ndjson_name
    payload = {key: value for key, value in asdict(record).items() if value is not None}

    result_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    final_result_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    ndjson_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    logger.info("Wrote JSON results to %s and %s", result_path, final_result_path)
    logger.info("Wrote NDJSON results to %s", ndjson_path)


def main() -> None:
    config = load_config()
    ensure_dependencies()
    logger.info("#######################################################")
    logger.info("Running benchmark for Databend Cloud with %s storage...", "S3")

    script_dir = Path(__file__).resolve().parent
    sql_dataset = resolve_sql_dataset(config.dataset)
    dataset_dir = script_dir / sql_dataset
    if not dataset_dir.exists():
        logger.error("Dataset directory %s does not exist", dataset_dir)
        sys.exit(1)
    if sql_dataset != config.dataset:
        logger.info("Dataset %s uses SQL directory %s", config.dataset, sql_dataset)

    if config.size not in SIZE_MAPPING:
        logger.error("Unsupported benchmark size: %s", config.size)
        sys.exit(1)

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    tags = ["s3", f"cache-{config.cache_size}"]
    cluster_size = SIZE_MAPPING[config.size]["cluster_size"]
    machine = SIZE_MAPPING[config.size]["machine"]
    system: Optional[str] = None
    comment: Optional[str] = None
    if config.source and config.source_id:
        if config.source == "pr":
            system = f"Databend(PR#{config.source_id})"
        elif config.source == "release":
            system = f"Databend(Release@{config.source_id})"
        else:
            logger.error("Unsupported benchmark source: %s", config.source)
            sys.exit(1)
    elif config.source or config.source_id:
        logger.error("Both BENCHMARK_SOURCE and BENCHMARK_SOURCE_ID must be provided together.")
        sys.exit(1)
    if config.sha:
        comment = f"commit: {config.sha}"

    record = ResultRecord(
        date=run_date,
        dataset=config.dataset,
        database=config.database,
        version=config.version,
        warehouse=config.warehouse,
        machine=machine,
        cluster_size=cluster_size,
        tags=tags,
        result=[],
        values={},
        run_id=config.benchmark_id,
        size=config.size,
        tries=config.tries,
        storage="s3",
        cache_size=config.cache_size,
        system=system,
        comment=comment,
    )

    runner = BendSQLRunner()
    runner.set_dsn(build_dsn(config, warehouse="default", login_disable=True))

    warehouse_literal = quote_literal(config.warehouse)
    logger.info("Creating warehouse %s...", config.warehouse)
    runner.run(sql=f"DROP WAREHOUSE IF EXISTS '{warehouse_literal}';")
    runner.run(
        sql=(
            f"CREATE WAREHOUSE '{warehouse_literal}' "
            f"WITH version='{config.version}' "
            f"warehouse_size='{config.size}' "
            f"cache_size={config.cache_size};"
        ),
    )
    runner.run(["--output", "table"], sql="SHOW WAREHOUSES;")
    wait_for_warehouse(runner, config.warehouse)

    runner.set_dsn(build_dsn(config, warehouse=config.warehouse))

    if config.dataset == "load":
        logger.info("Creating database %s for load dataset...", config.database)
        runner.run(["--database", "default"], sql=f"CREATE DATABASE {config.database};")

    runner.set_dsn(build_dsn(config, database=config.database, warehouse=config.warehouse))

    logger.info("Checking session settings...")
    runner.run(
        ["--output", "table"],
        sql="select * from system.settings where value != default;",
    )

    analyze_sql = dataset_dir / "analyze.sql"
    if analyze_sql.exists():
        logger.info("Analyze tables...")
        execute_sql_file(runner, analyze_sql)

    logger.info("Running queries...")
    queries = sorted(dataset_dir.glob("queries/*.sql"))
    if not queries:
        logger.error("No queries found under %s", dataset_dir)
        sys.exit(1)

    for query_num, query_file in enumerate(queries):
        logger.info("==> Running Q%s: %s", query_num, query_file)
        query_sql = query_file.read_text()
        record.result.append([])
        record.values[f"Q{query_num}"] = []
        for attempt in range(1, config.tries + 1):
            try:
                q_time = run_timed_query(runner, query_sql)
            except subprocess.CalledProcessError:
                logger.error("Q%s[%s] failed", query_num, attempt)
                continue
            if q_time is None:
                logger.error("Q%s[%s] returned no timing info", query_num, attempt)
                continue
            logger.info("Q%s[%s] succeeded in %.3f seconds", query_num, attempt, q_time)
            record.result[query_num].append(round(q_time, 3))
            record.values[f"Q{query_num}"].append(round(q_time, 3))

    cleanup_runner = BendSQLRunner()
    cleanup_runner.set_dsn(build_dsn(config, warehouse="default", login_disable=True))

    try:
        if config.dataset == "load":
            logger.info("Dropping database %s...", config.database)
            cleanup_runner.run(sql=f"DROP DATABASE IF EXISTS {config.database};")
        logger.info("Dropping warehouse %s...", config.warehouse)
        cleanup_runner.run(sql=f"DROP WAREHOUSE IF EXISTS '{warehouse_literal}';")
    finally:
        write_result_files(script_dir, record)


if __name__ == "__main__":
    main()
