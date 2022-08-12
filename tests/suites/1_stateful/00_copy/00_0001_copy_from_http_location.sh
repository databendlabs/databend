#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists ontime200;" | $MYSQL_CLIENT_CONNECT

## Create table
cat $CURDIR/../ddl/ontime.sql | sed 's/ontime/ontime200/g' | $MYSQL_CLIENT_CONNECT

copy_from_location_cases=(
  # copy csv
  "copy into ontime200 from 'https://repo.databend.rs/dataset/stateful/ontime_2006_200.csv'
    FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy csv from set pattern
  "copy into ontime200 from 'https://repo.databend.rs/dataset/stateful/ontime_200{6,7,8}_200.csv'
    FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy csv from set pattern
  "copy into ontime200 from 'https://repo.databend.rs/dataset/stateful/ontime_200[6-8]_200.csv'
    FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
)

for i in "${copy_from_location_cases[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select count(1), avg(Year), sum(DayOfWeek) from ontime200" | $MYSQL_CLIENT_CONNECT
  echo "truncate table ontime200" | $MYSQL_CLIENT_CONNECT
done

## Drop table
echo "drop table if exists ontime200;" | $MYSQL_CLIENT_CONNECT
