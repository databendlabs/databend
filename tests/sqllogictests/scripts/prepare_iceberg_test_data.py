from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

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

spark.stop()
