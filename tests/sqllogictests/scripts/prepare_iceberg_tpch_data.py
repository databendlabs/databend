from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType

spark = SparkSession.builder \
    .appName("CSV to Iceberg REST Catalog") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://127.0.0.1:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://iceberg-tpch/") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin") \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password") \
    .config("spark.sql.catalog.iceberg.client.region", "us-east-1") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://127.0.0.1:9000") \
    .config("spark.jars.packages",
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1") \
    .getOrCreate()

tables = {
    "lineitem": (
        StructType([
            StructField("l_orderkey", IntegerType(), True),
            StructField("l_partkey", IntegerType(), True),
            StructField("l_suppkey", IntegerType(), True),
            StructField("l_linenumber", IntegerType(), True),
            StructField("l_quantity", DoubleType(), True),
            StructField("l_extendedprice", DoubleType(), True),
            StructField("l_discount", DoubleType(), True),
            StructField("l_tax", DoubleType(), True),
            StructField("l_returnflag", StringType(), True),
            StructField("l_linestatus", StringType(), True),
            StructField("l_shipdate", DateType(), True),
            StructField("l_commitdate", DateType(), True),
            StructField("l_receiptdate", DateType(), True),
            StructField("l_shipinstruct", StringType(), True),
            StructField("l_shipmode", StringType(), True),
            StructField("l_comment", StringType(), True)
        ]),
        "tests/sqllogictests/data/lineitem.tbl"
    ),
    "orders": (
        StructType([
            StructField("o_orderkey", IntegerType(), True),
            StructField("o_custkey", IntegerType(), True),
            StructField("o_orderstatus", StringType(), True),
            StructField("o_totalprice", DoubleType(), True),
            StructField("o_orderdate", DateType(), True),
            StructField("o_orderpriority", StringType(), True),
            StructField("o_clerk", StringType(), True),
            StructField("o_shippriority", IntegerType(), True),
            StructField("o_comment", StringType(), True)
        ]),
        "tests/sqllogictests/data/orders.tbl"
    ),
    "customer": (
        StructType([
            StructField("c_custkey", IntegerType(), True),
            StructField("c_name", StringType(), True),
            StructField("c_address", StringType(), True),
            StructField("c_nationkey", IntegerType(), True),
            StructField("c_phone", StringType(), True),
            StructField("c_acctbal", DoubleType(), True),
            StructField("c_mktsegment", StringType(), True),
            StructField("c_comment", StringType(), True)
        ]),
        "tests/sqllogictests/data/customer.tbl"
    ),
    "nation": (
        StructType([
            StructField("n_nationkey", IntegerType(), True),
            StructField("n_name", StringType(), True),
            StructField("n_regionkey", IntegerType(), True),
            StructField("n_comment", StringType(), True)
        ]),
        "tests/sqllogictests/data/nation.tbl"
    ),
    "region": (
        StructType([
            StructField("r_regionkey", IntegerType(), True),
            StructField("r_name", StringType(), True),
            StructField("r_comment", StringType(), True)
        ]),
        "tests/sqllogictests/data/region.tbl"
    ),
    "part": (
        StructType([
            StructField("p_partkey", IntegerType(), True),
            StructField("p_name", StringType(), True),
            StructField("p_mfgr", StringType(), True),
            StructField("p_brand", StringType(), True),
            StructField("p_type", StringType(), True),
            StructField("p_size", IntegerType(), True),
            StructField("p_container", StringType(), True),
            StructField("p_retailprice", DoubleType(), True),
            StructField("p_comment", StringType(), True)
        ]),
        "tests/sqllogictests/data/part.tbl"
    ),
    "supplier": (
        StructType([
            StructField("s_suppkey", IntegerType(), True),
            StructField("s_name", StringType(), True),
            StructField("s_address", StringType(), True),
            StructField("s_nationkey", IntegerType(), True),
            StructField("s_phone", StringType(), True),
            StructField("s_acctbal", DoubleType(), True),
            StructField("s_comment", StringType(), True)
        ]),
        "tests/sqllogictests/data/supplier.tbl"
    ),
    "partsupp": (
        StructType([
            StructField("ps_partkey", IntegerType(), True),
            StructField("ps_suppkey", IntegerType(), True),
            StructField("ps_availqty", IntegerType(), True),
            StructField("ps_supplycost", DoubleType(), True),
            StructField("ps_comment", StringType(), True)
        ]),
        "tests/sqllogictests/data/partsupp.tbl"
    )
}

for table_name, (schema, file_path) in tables.items():
    full_table_name = f"iceberg.tpch.{table_name}"
    spark.sql(f"""
    CREATE OR REPLACE TABLE {full_table_name} (
        {', '.join([f'{field.name} {field.dataType.simpleString()}' for field in schema.fields])}
    ) USING iceberg;
    """)

    df = spark.read.csv(file_path, header=False, sep="|", schema=schema)
    df.write.format("iceberg").mode("overwrite").save(full_table_name)
    print(f"table {full_table_name} has been created")

spark.stop()
