#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists wrong_csv;" | $MYSQL_CLIENT_CONNECT

## Create table
cat $CURDIR/../ddl/on_error_test.sql | $MYSQL_CLIENT_CONNECT

# Should be <root>/tests/data/
DATADIR=$(realpath $CURDIR/../../../data/)

# This line is kept for debuging.
# echo "Current data dir: ${DATADIR}"

copy_from_location_cases=(
  # copy csv
  "copy into ontime200 from 'fs://${DATADIR}/wrong_sample.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 0)"
  # copy csv on_error=continue
  "copy into ontime200 from 'fs://${DATADIR}/wrong_sample.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 0) ON_ERROR=continue"
)

for i in "${copy_from_location_cases[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select count(1) from wrong_csv" | $MYSQL_CLIENT_CONNECT
  echo "truncate table wrong_csv" | $MYSQL_CLIENT_CONNECT
done

## Drop table
echo "drop table if exists wrong_csv;" | $MYSQL_CLIENT_CONNECT
