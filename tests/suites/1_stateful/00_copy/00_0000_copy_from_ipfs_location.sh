#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh
QMHASH=QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ

echo "drop table if exists ontime_200;" | $MYSQL_CLIENT_CONNECT

## Create table
cat $CURDIR/../ddl/ontime.sql | sed "s/ontime/ontime_200/g" | $MYSQL_CLIENT_CONNECT

copy_from_location_cases=(
  # copy csv
  "copy into ontime_200 from 'ipfs://$QMHASH/ontime.csv' CONNECTION = (ENDPOINT_URL='https://ipfs.filebase.io') FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy gzip csv
  "copy into ontime_200 from 'ipfs://$QMHASH/ontime.csv.gz' CONNECTION = (ENDPOINT_URL='https://ipfs.filebase.io') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'gzip'  record_delimiter = '\n' skip_header = 1)"
  # copy zstd csv
  "copy into ontime_200 from 'ipfs://$QMHASH/ontime.csv.zst' CONNECTION = (ENDPOINT_URL='https://ipfs.filebase.io') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'zstd'  record_delimiter = '\n' skip_header = 1)"
  # copy bz2 csv
  "copy into ontime_200 from 'ipfs://$QMHASH/ontime.csv.bz2' CONNECTION = (ENDPOINT_URL='https://ipfs.filebase.io') FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'bz2'  record_delimiter = '\n' skip_header = 1)"
)

for i in "${copy_from_location_cases[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime_200" | $MYSQL_CLIENT_CONNECT
  echo "truncate table ontime_200" | $MYSQL_CLIENT_CONNECT
done

## Drop table
echo "drop table if exists ontime_200;" | $MYSQL_CLIENT_CONNECT