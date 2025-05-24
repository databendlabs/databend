from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("CSV to Iceberg REST Catalog")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://127.0.0.1:8181")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-tpch/")
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin")
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://127.0.0.1:9000")
    .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    )
    # for delete file
    .config("spark.sql.shuffle.partitions", "1")
    # for delete file
    .config("spark.default.parallelism", "1")
    .getOrCreate()
)

spark.sql(f"DROP TABLE IF EXISTS iceberg.test.t1;")
spark.sql(f"DROP TABLE IF EXISTS iceberg.test.t1_orc;")

# Parquet
spark.sql(f"""
CREATE TABLE iceberg.test.t1 (
  c1 INT,
  c2 INT,
  c3 STRING
)
USING iceberg;
""")

spark.sql(
    f"""INSERT INTO iceberg.test.t1 VALUES (0, 0, 'a'), (1, 1, 'b'), (2, 2, 'c'), (3, 3, 'd'), (4, null, null);"""
)

# ORC
spark.sql(f"""
CREATE TABLE iceberg.test.t1_orc (
  c1 INT,
  c2 INT,
  c3 STRING
)
USING iceberg
TBLPROPERTIES (
  'write.format.default'='orc'
);
""")

spark.sql(
    f"""INSERT INTO iceberg.test.t1_orc VALUES (0, 0, 'a'), (1, 1, 'b'), (2, 2, 'c'), (3, 3, 'd'), (4, null, null);"""
)

spark.sql(
    f"""
CREATE OR REPLACE TABLE iceberg.test.test_positional_merge_on_read_deletes (
    dt     date,
    number integer,
    letter string
)
USING iceberg
TBLPROPERTIES (
    'write.delete.mode'='merge-on-read',
    'write.update.mode'='merge-on-read',
    'write.merge.mode'='merge-on-read',
    'format-version'='2'
);
"""
)

spark.sql(
    f"""
INSERT INTO iceberg.test.test_positional_merge_on_read_deletes
VALUES
    (CAST('2023-03-01' AS date), 1, 'a'),
    (CAST('2023-03-02' AS date), 2, 'b'),
    (CAST('2023-03-03' AS date), 3, 'c'),
    (CAST('2023-03-04' AS date), 4, 'd'),
    (CAST('2023-03-05' AS date), 5, 'e'),
    (CAST('2023-03-06' AS date), 6, 'f'),
    (CAST('2023-03-07' AS date), 7, 'g'),
    (CAST('2023-03-08' AS date), 8, 'h'),
    (CAST('2023-03-09' AS date), 9, 'i'),
    (CAST('2023-03-10' AS date), 10, 'j'),
    (CAST('2023-03-11' AS date), 11, 'k'),
    (CAST('2023-03-12' AS date), 12, 'l');
"""
)

spark.sql(f"DELETE FROM iceberg.test.test_positional_merge_on_read_deletes WHERE number > 9")

spark.stop()
