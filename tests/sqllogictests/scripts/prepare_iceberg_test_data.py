from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (
    SparkSession.builder.appName("CSV to Iceberg REST Catalog")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "rest")
    .config("spark.sql.catalog.iceberg.uri", "http://127.0.0.1:8181")
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-tpch/")
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin")
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://127.0.0.1:9002")
    .config("spark.sql.catalog.iceberg.client.region", "us-east-1")
    .config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-aws-bundle:1.10.0,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0",
    )
    # for delete file
    .config("spark.sql.shuffle.partitions", "1")
    # for delete file
    .config("spark.default.parallelism", "1")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.test")

spark.sql(f"DROP TABLE IF EXISTS iceberg.test.t1;")
spark.sql(f"DROP TABLE IF EXISTS iceberg.test.t1_orc;")
spark.sql("DROP TABLE IF EXISTS iceberg.test.t_variant_metadata;")

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

# create nested table
spark.sql("DROP TABLE IF EXISTS iceberg.test.t_nested")
data = [
    (1, ("Alice", 30), (("A1", 1), 10)),
    (2, ("Bob", 25), (("B1", 2), 20)),
    (3, ("Charlie", 35), (("C1", 3), 30)),
    (4, None, None),
]

# Create DataFrame and write
schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField(
            "item",
            StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "item_2",
            StructType(
                [
                    StructField(
                        "item",
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("level", IntegerType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("level", IntegerType(), True),
                ]
            ),
            True,
        ),
    ]
)

df = spark.createDataFrame(data, schema)
df.writeTo("iceberg.test.t_nested").using("iceberg").createOrReplace()

print("Table iceberg.test.t_nested created with sample data")


# Create an Iceberg v3 table with a physical variant column. Databend should be
# able to load and refresh the metadata and read projected non-variant columns.
spark.sql("""
CREATE TABLE iceberg.test.t_variant_metadata (
  id INT,
  name STRING,
  payload VARIANT
)
USING iceberg
TBLPROPERTIES (
  'format-version'='3',
  'write.parquet.shred-variants.enabled'='false'
);
""")

spark.sql("""
INSERT INTO iceberg.test.t_variant_metadata
SELECT
  1,
  'alice',
  parse_json('{"device":"ios","event":{"name":"open","count":1}}')
UNION ALL
SELECT
  2,
  'bob',
  parse_json('{"device":"android","event":{"name":"click","count":2}}');
""")

print("Table iceberg.test.t_variant_metadata created with Iceberg v3 variant data")

spark.stop()
