#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyspark==3.5.3"]
# ///
"""Prepare filesystem or S3 Paimon tables for stateful regression."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

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

packages = "org.apache.paimon:paimon-spark-3.5_2.12:1.4.1"
if warehouse.startswith("s3://"):
    packages += ",org.apache.paimon:paimon-s3:1.4.1"

builder = (
    SparkSession.builder.appName("prepare-paimon-fs-data")
    .master("local[4]")
    .config("spark.jars.packages", packages)
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
