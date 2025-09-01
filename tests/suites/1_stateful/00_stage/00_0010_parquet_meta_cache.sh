#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh
stmt "DROP STAGE IF EXISTS s1;"
stmt "CREATE STAGE s1;"

aws --endpoint-url  ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/data/parquet/tuple.parquet s3://testbucket/admin/stage/internal/s1/parquet_cache.parquet >/dev/null 2>&1
query "select /*+ set_var(parquet_fast_read_bytes=0) */ * from @s1/parquet_cache.parquet"

aws --endpoint-url  ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/data/parquet/variant.parquet  s3://testbucket/admin/stage/internal/s1/parquet_cache.parquet >/dev/null 2>&1
query "select /*+ set_var(parquet_fast_read_bytes=0) */ * from @s1/parquet_cache.parquet"

stmt "DROP STAGE IF EXISTS s1;"
