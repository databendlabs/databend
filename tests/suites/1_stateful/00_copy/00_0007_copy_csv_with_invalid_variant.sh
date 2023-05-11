#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# Should be <root>/tests/data/
DATADIR=$(realpath $CURDIR/../../../data/)

echo "drop table if exists test_csv_variant;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table test_csv_variant(a int, b variant);" | $MYSQL_CLIENT_CONNECT

copy_from_test_csv_variant=(
  "copy into test_csv_variant from 'fs://${DATADIR}/invalid_variant.csv' FILE_FORMAT = (field_delimiter = '\t' record_delimiter = '\n' type = CSV) disable_variant_check = false ON_ERROR = CONTINUE"
)
echo "---test csv field check invalid variant"
for i in "${copy_from_test_csv_variant[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select a, b from test_csv_variant" | $MYSQL_CLIENT_CONNECT
  echo "truncate table test_csv_variant" | $MYSQL_CLIENT_CONNECT
done

copy_from_test_csv_disable_variant_check=(
  "copy into test_csv_variant from 'fs://${DATADIR}/invalid_variant.csv' FILE_FORMAT = (field_delimiter = '\t' record_delimiter = '\n' type = CSV) disable_variant_check = true ON_ERROR = CONTINUE"
)
echo "---test csv field disable check invalid variant"
for i in "${copy_from_test_csv_disable_variant_check[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select a, b from test_csv_variant" | $MYSQL_CLIENT_CONNECT
  echo "truncate table test_csv_variant" | $MYSQL_CLIENT_CONNECT
done

## Drop table
echo "drop table if exists test_csv_variant;" | $MYSQL_CLIENT_CONNECT
