#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists ontime200;" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists s1" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists named_external_stage" | $MYSQL_CLIENT_CONNECT

## Create table
cat $CURDIR/../ddl/ontime.sql | sed 's/ontime/ontime200/g' | $MYSQL_CLIENT_CONNECT

aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/stage/s1/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/stage/s1/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/stage/s1/ontime_200.csv.zst >/dev/null 2>&1
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv.bz2 s3://testbucket/admin/stage/s1/ontime_200.csv.bz2 >/dev/null 2>&1
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv.xz s3://testbucket/admin/stage/s1/ontime_200.csv.xz >/dev/null 2>&1
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.parquet s3://testbucket/admin/stage/s1/ontime_200.parquet >/dev/null 2>&1
aws --endpoint-url ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.ndjson s3://testbucket/admin/stage/s1/ontime_200.ndjson >/dev/null 2>&1

## Copy from internal stage
echo "CREATE STAGE s1;" | $MYSQL_CLIENT_CONNECT
echo "list @s1 PATTERN = 'ontime.*'" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort

copy_from_stage_cases=(
  # copy parquet
  "copy into ontime200 from @s1 PATTERN = 'ontime.*parquet$' FILE_FORMAT = (type = 'PARQUET');"
  # copy gzip csv
  "copy into ontime200 from @s1 FILES = ('ontime_200.csv.gz') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'gzip'  record_delimiter = '\n' skip_header = 1);"
  # copy zstd csv
  "copy into ontime200 from @s1 FILES = ('ontime_200.csv.zst') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'zstd'  record_delimiter = '\n' skip_header = 1);"
  # copy bz2 csv
  "copy into ontime200 from @s1 FILES = ('ontime_200.csv.bz2') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'bz2'  record_delimiter = '\n' skip_header = 1);"
  # copy xz csv
  "copy into ontime200 from @s1 FILES = ('ontime_200.csv.xz') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'xz'  record_delimiter = '\n' skip_header = 1);"
  # copy auto csv
  "copy into ontime200 from @s1 FILES = ('ontime_200.csv.gz', 'ontime_200.csv.zst', 'ontime_200.csv.bz2', 'ontime_200.csv.xz') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = AUTO  record_delimiter = '\n' skip_header = 1);"
   # copy ndjson
  "copy into ontime200 from @s1 PATTERN = 'ontime.*ndjson$' FILE_FORMAT = (type = 'ndjson');"
)

for i in "${copy_from_stage_cases[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime200" | $MYSQL_CLIENT_CONNECT
  echo "truncate table ontime200" | $MYSQL_CLIENT_CONNECT
done

## Copy from named external stage
echo "CREATE STAGE named_external_stage url = 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT
echo "list @named_external_stage PATTERN = 'ontime.*parquet$'" | $MYSQL_CLIENT_CONNECT | awk '{print $1}' | sort


copy_from_named_external_stage_cases=(
  # copy parquet
  "copy into ontime200 from @named_external_stage  PATTERN = 'ontime.*parquet$' FILE_FORMAT = (type = 'PARQUET')"
  # copy gzip csv
  "copy into ontime200 from @named_external_stage FILES = ('ontime_200.csv.gz') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'gzip'  record_delimiter = '\n' skip_header = 1);"
  # copy zstd csv
  "copy into ontime200 from @named_external_stage FILES = ('ontime_200.csv.zst') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'zstd'  record_delimiter = '\n' skip_header = 1);"
  # copy bz2 csv
  "copy into ontime200 from @named_external_stage FILES = ('ontime_200.csv.bz2') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'bz2'  record_delimiter = '\n' skip_header = 1);"
  # copy auto csv
  "copy into ontime200 from @named_external_stage FILES = ('ontime_200.csv.gz','ontime_200.csv.bz2','ontime_200.csv.zst') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'auto'  record_delimiter = '\n' skip_header = 1);"
)

for i in "${copy_from_named_external_stage_cases[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime200" | $MYSQL_CLIENT_CONNECT
  echo "truncate table ontime200" | $MYSQL_CLIENT_CONNECT
done


## List stage use http API

curl -s -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" --header 'Content-Type: application/json' -d '{"sql": "list @s1;"}'  | grep -o 'ontime_200.csv'


## Drop table.
echo "drop table ontime200" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists named_external_stage" | $MYSQL_CLIENT_CONNECT
echo "drop stage if exists s1" | $MYSQL_CLIENT_CONNECT
