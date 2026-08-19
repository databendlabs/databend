#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyspark==3.5.3"]
# ///
"""Prepare filesystem or S3 Paimon tables for stateful regression."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from pyspark.sql import SparkSession

PAIMON_VERSION = "1.4.1"
PAIMON_SPARK_COORD = f"org.apache.paimon:paimon-spark-3.5_2.12:{PAIMON_VERSION}"
PAIMON_S3_COORD = f"org.apache.paimon:paimon-s3:{PAIMON_VERSION}"
MAVEN_MIRRORS = (
    "https://repo1.maven.org/maven2",
    "https://maven-central.storage-download.googleapis.com/maven2",
    "https://repo.maven.apache.org/maven2",
)


def maven_artifact_url(mirror: str, coordinate: str) -> str:
    group, artifact, version = coordinate.split(":")
    return (
        f"{mirror.rstrip('/')}/{group.replace('.', '/')}/"
        f"{artifact}/{version}/{artifact}-{version}.jar"
    )


def download_maven_jar(coordinate: str, dest: Path) -> Path:
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    last_error: Exception | None = None
    for attempt in range(5):
        for mirror in MAVEN_MIRRORS:
            url = maven_artifact_url(mirror, coordinate)
            try:
                request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "databend-paimon-ci"},
                )
                with urllib.request.urlopen(request, timeout=60) as response:
                    with tempfile.NamedTemporaryFile(
                        dir=dest.parent, delete=False
                    ) as tmp:
                        shutil.copyfileobj(response, tmp)
                        tmp_path = Path(tmp.name)
                if tmp_path.stat().st_size == 0:
                    tmp_path.unlink(missing_ok=True)
                    raise RuntimeError(f"empty download from {url}")
                tmp_path.replace(dest)
                print(f"Downloaded {coordinate} from {mirror}", file=sys.stderr)
                return dest
            except (OSError, urllib.error.URLError, RuntimeError) as exc:
                last_error = exc
                print(f"WARN: download {url} failed: {exc}", file=sys.stderr)
        time.sleep(2 * (attempt + 1))

    raise RuntimeError(f"failed to download {coordinate}: {last_error}")


warehouse = os.environ.get(
    "PAIMON_WAREHOUSE",
    str(Path(__file__).resolve().parents[2] / "data" / "paimon_warehouse"),
)
if warehouse.startswith("s3://"):
    warehouse_uri = f"{warehouse.rstrip('/')}/"
elif "://" in warehouse:
    warehouse_uri = warehouse
else:
    Path(warehouse).mkdir(parents=True, exist_ok=True)
    warehouse_uri = f"file://{warehouse}"

jars_dir = Path(
    os.environ.get(
        "PAIMON_JARS_DIR",
        str(Path.home() / ".cache" / "databend-paimon-jars"),
    )
)
jars = [
    download_maven_jar(
        PAIMON_SPARK_COORD,
        jars_dir / f"paimon-spark-3.5_2.12-{PAIMON_VERSION}.jar",
    )
]
if warehouse.startswith("s3://"):
    jars.append(
        download_maven_jar(
            PAIMON_S3_COORD,
            jars_dir / f"paimon-s3-{PAIMON_VERSION}.jar",
        )
    )

builder = (
    SparkSession.builder.appName("prepare-paimon-fs-data")
    .master("local[4]")
    .config("spark.jars", ",".join(str(path) for path in jars))
    .config(
        "spark.sql.extensions",
        "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
    )
    .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
    .config("spark.sql.catalog.paimon.warehouse", warehouse_uri)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
)

if warehouse.startswith("s3://"):
    builder = (
        builder.config(
            "spark.sql.catalog.paimon.s3.endpoint",
            os.environ["PAIMON_S3_ENDPOINT"],
        )
        .config(
            "spark.sql.catalog.paimon.s3.access-key",
            os.environ["PAIMON_S3_ACCESS_KEY"],
        )
        .config(
            "spark.sql.catalog.paimon.s3.secret-key",
            os.environ["PAIMON_S3_SECRET_KEY"],
        )
        .config("spark.sql.catalog.paimon.s3.path.style.access", "true")
        .config("spark.sql.catalog.paimon.s3.region", "us-east-1")
    )

spark = builder.getOrCreate()


def prepare_tables() -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS paimon.regression")

    spark.sql("DROP TABLE IF EXISTS paimon.regression.append_t")
    spark.sql(
        """
CREATE TABLE paimon.regression.append_t (
  part INT,
  id INT,
  name STRING
) USING paimon
PARTITIONED BY (part)
"""
    )

    for part, name in [(0, "a0"), (1, "a1"), (2, "b0"), (3, "b1")]:
        spark.sql(
            f"""
INSERT INTO paimon.regression.append_t PARTITION (part = {part})
SELECT {part}, '{name}'
"""
        )

    spark.sql("DROP TABLE IF EXISTS paimon.regression.pk_t")
    spark.sql(
        """
CREATE TABLE paimon.regression.pk_t (
  id INT,
  name STRING
) USING paimon
TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1')
"""
    )
    spark.sql("INSERT INTO paimon.regression.pk_t VALUES (1, 'old')")
    spark.sql("INSERT INTO paimon.regression.pk_t VALUES (1, 'new')")

    # Empty write targets for Databend e2e (DROP then CREATE for idempotency).
    spark.sql("DROP TABLE IF EXISTS paimon.regression.write_append")
    spark.sql(
        """
CREATE TABLE paimon.regression.write_append (id INT, value STRING)
USING paimon TBLPROPERTIES ('bucket'='-1')
"""
    )

    spark.sql("DROP TABLE IF EXISTS paimon.regression.write_append_part")
    spark.sql(
        """
CREATE TABLE paimon.regression.write_append_part (id INT, value STRING, part INT)
USING paimon PARTITIONED BY (part) TBLPROPERTIES ('bucket'='-1')
"""
    )

    spark.sql("DROP TABLE IF EXISTS paimon.regression.write_pk")
    spark.sql(
        """
CREATE TABLE paimon.regression.write_pk (id INT, value STRING)
USING paimon TBLPROPERTIES ('primary-key'='id', 'bucket'='4')
"""
    )

    spark.sql("DROP TABLE IF EXISTS paimon.regression.write_pk_part")
    spark.sql(
        """
CREATE TABLE paimon.regression.write_pk_part (id INT, value STRING, part INT)
USING paimon PARTITIONED BY (part)
TBLPROPERTIES ('primary-key'='part,id', 'bucket'='4')
"""
    )

    # Cluster write regression: bucket counts below / near / above typical writer lanes.
    for buckets in (2, 8, 64):
        table = f"write_pk_part_b{buckets}"
        spark.sql(f"DROP TABLE IF EXISTS paimon.regression.{table}")
        spark.sql(
            f"""
CREATE TABLE paimon.regression.{table} (id INT, value STRING, part INT)
USING paimon PARTITIONED BY (part)
TBLPROPERTIES ('primary-key'='part,id', 'bucket'='{buckets}')
"""
        )

    print("Prepared Paimon warehouse at", warehouse)


if __name__ == "__main__":
    try:
        prepare_tables()
    finally:
        spark.stop()
    sys.exit(0)
