#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists test_tsv;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table test_tsv(a string, b int);" | $MYSQL_CLIENT_CONNECT

copy_from_tsv_cases=(
  "copy into test_tsv from 's3://testbucket/admin/data/escape.tsv' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') FILE_FORMAT = (type = 'TSV')"
)

counter=1
for i in "${copy_from_tsv_cases[@]}"; do
  echo "---$counter"
  echo "set enable_planner_v2 = 1; $i" | $MYSQL_CLIENT_CONNECT
  echo "select a, b, length(a) from test_tsv" | $MYSQL_CLIENT_CONNECT
  echo "truncate table test_tsv" | $MYSQL_CLIENT_CONNECT
  _=$((counter++))
done

## Drop table
echo "drop table if exists test_tsv;" | $MYSQL_CLIENT_CONNECT
