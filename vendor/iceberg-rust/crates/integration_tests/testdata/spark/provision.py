# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_add, expr

# The configuration is important, otherwise we get many small
# parquet files with a single row. When a positional delete
# hits the Parquet file with one row, the parquet file gets
# dropped instead of having a merge-on-read delete file.
spark = (
    SparkSession
        .builder
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
)

spark.sql(f"""CREATE NAMESPACE IF NOT EXISTS rest.default""")

spark.sql(
    f"""
CREATE OR REPLACE TABLE rest.default.test_positional_merge_on_read_deletes (
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
INSERT INTO rest.default.test_positional_merge_on_read_deletes
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

spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_deletes WHERE number = 9")

spark.sql(
    f"""
  CREATE OR REPLACE TABLE rest.default.test_positional_merge_on_read_double_deletes (
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
INSERT INTO rest.default.test_positional_merge_on_read_double_deletes
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

#  Creates two positional deletes that should be merged
spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE number = 9")
spark.sql(f"DELETE FROM rest.default.test_positional_merge_on_read_double_deletes WHERE letter == 'f'")

#  Create a table, and do some renaming
spark.sql("CREATE OR REPLACE TABLE rest.default.test_rename_column (lang string) USING iceberg")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Python')")
spark.sql("ALTER TABLE rest.default.test_rename_column RENAME COLUMN lang TO language")
spark.sql("INSERT INTO rest.default.test_rename_column VALUES ('Java')")

#  Create a table, and do some evolution
spark.sql("CREATE OR REPLACE TABLE rest.default.test_promote_column (foo int) USING iceberg")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (19)")
spark.sql("ALTER TABLE rest.default.test_promote_column ALTER COLUMN foo TYPE bigint")
spark.sql("INSERT INTO rest.default.test_promote_column VALUES (25)")

#  Create a table, and do some evolution on a partition column
spark.sql("CREATE OR REPLACE TABLE rest.default.test_promote_partition_column (foo int, bar float, baz decimal(4, 2)) USING iceberg PARTITIONED BY (foo)")
spark.sql("INSERT INTO rest.default.test_promote_partition_column VALUES (19, 19.25, 19.25)")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN foo TYPE bigint")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN bar TYPE double")
spark.sql("ALTER TABLE rest.default.test_promote_partition_column ALTER COLUMN baz TYPE decimal(6, 2)")
spark.sql("INSERT INTO rest.default.test_promote_partition_column VALUES (25, 22.25, 22.25)")

#  Create a table with various types
spark.sql("""
CREATE OR REPLACE TABLE rest.default.types_test USING ICEBERG AS 
SELECT
    CAST(s % 2 = 1 AS BOOLEAN) AS cboolean,
    CAST(s % 256 - 128 AS TINYINT) AS ctinyint,
    CAST(s AS SMALLINT) AS csmallint,
    CAST(s AS INT) AS cint,
    CAST(s AS BIGINT) AS cbigint,
    CAST(s AS FLOAT) AS cfloat,
    CAST(s AS DOUBLE) AS cdouble,
    CAST(s / 100.0 AS DECIMAL(8, 2)) AS cdecimal,
    CAST(DATE('1970-01-01') + s AS DATE) AS cdate,
    CAST(from_unixtime(s) AS TIMESTAMP_NTZ) AS ctimestamp_ntz,
    CAST(from_unixtime(s) AS TIMESTAMP) AS ctimestamp,
    CAST(s AS STRING) AS cstring,
    CAST(s AS BINARY) AS cbinary
FROM (
    SELECT EXPLODE(SEQUENCE(0, 1000)) AS s
);
""")
