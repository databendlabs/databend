#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# Should be <root>/tests/data/
DATADIR=$(realpath $CURDIR/../../../data/)

echo "drop table if exists test_tsv;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists test_tsv_field_whitespace;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table test_tsv(a string, b int);" | $MYSQL_CLIENT_CONNECT
echo "create table test_tsv_field_whitespace(a int, b string);" | $MYSQL_CLIENT_CONNECT

copy_from_tsv_cases=(
  "set enable_distributed_copy_into = 1;copy into test_tsv from 's3://testbucket/admin/data/escape.tsv' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') FILE_FORMAT = (type = 'TSV')"
)

counter=1
for i in "${copy_from_tsv_cases[@]}"; do
  echo "---$counter"
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select a, b, length(a) from test_tsv" | $MYSQL_CLIENT_CONNECT
  echo "truncate table test_tsv" | $MYSQL_CLIENT_CONNECT
  _=$((counter++))
done

copy_from_tsv_whitespace_cases=(
  "set enable_distributed_copy_into = 1;copy into test_tsv_field_whitespace from 'fs://${DATADIR}/field_white_space.tsv' FILE_FORMAT = (type = TSV)"
)
echo "---test tsv field with whitespace"
for i in "${copy_from_tsv_whitespace_cases[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select a, b, length(b) from test_tsv_field_whitespace" | $MYSQL_CLIENT_CONNECT
  echo "truncate table test_tsv_field_whitespace" | $MYSQL_CLIENT_CONNECT
done


## Drop table
echo "drop table if exists test_tsv;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists test_tsv_field_whitespace;" | $MYSQL_CLIENT_CONNECT