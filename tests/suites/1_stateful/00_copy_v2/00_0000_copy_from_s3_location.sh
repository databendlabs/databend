#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists ontime200;" | $MYSQL_CLIENT_CONNECT

## Create table
cat $CURDIR/../ddl/ontime.sql | sed 's/ontime/ontime200/g' | $MYSQL_CLIENT_CONNECT

copy_from_location_cases=(
  # copy csv
  "copy into ontime200 from 's3://testbucket/admin/data/ontime_200.csv' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy gzip csv
  "copy into ontime200 from 's3://testbucket/admin/data/ontime_200.csv.gz' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'gzip'  record_delimiter = '\n' skip_header = 1)"
  # copy zstd csv
  "copy into ontime200 from 's3://testbucket/admin/data/ontime_200.csv.zst' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'zstd'  record_delimiter = '\n' skip_header = 1)"
  # copy bz2 csv
  "copy into ontime200 from 's3://testbucket/admin/data/ontime_200.csv.bz2' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'bz2'  record_delimiter = '\n' skip_header = 1)"
  # copy xz csv
  "copy into ontime200 from 's3://testbucket/admin/data/ontime_200.csv.xz' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'xz'  record_delimiter = '\n' skip_header = 1)"
  # copy files
  "copy into ontime200 from 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') FILES = ('ontime_200.csv', 'ontime_200_v1.csv') FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy dir with pattern
  "copy into ontime200 from 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') PATTERN = 'ontime.*csv$' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy parquet
  "copy into ontime200 from 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') PATTERN = 'ontime.*parquet' FILE_FORMAT = (type = 'PARQUET')"
)

for i in "${copy_from_location_cases[@]}"; do
  echo "set enable_planner_v2 = 1; $i" | $MYSQL_CLIENT_CONNECT
  echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime200" | $MYSQL_CLIENT_CONNECT
  echo "truncate table ontime200" | $MYSQL_CLIENT_CONNECT
done

## Drop table
echo "drop table if exists ontime200;" | $MYSQL_CLIENT_CONNECT


