#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh
TABLE=ontime200
FILE=ontime_200
QMHASH=Qmei4dyyPazUy24cfCAtmumC1Ff1VLLiqjzF1zCXXVtvSk

echo "drop table if exists ${TABLE};" | $MYSQL_CLIENT_CONNECT

## Create table
cat $CURDIR/../ddl/ontime.sql | sed "s/ontime/$TABLE/g" | $MYSQL_CLIENT_CONNECT

copy_from_location_cases=(
  # copy csv
  "copy into $TABLE from 'ipfs://$QMHASH/$FILE.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy gzip csv
  "copy into $TABLE from 'ipfs://$QMHASH/$FILE.csv.gz' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'gzip'  record_delimiter = '\n' skip_header = 1)"
  # copy zstd csv
  "copy into $TABLE from 'ipfs://$QMHASH/$FILE.csv.zstd' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'zstd'  record_delimiter = '\n' skip_header = 1)"
  # copy bz2 csv
  "copy into $TABLE from 'ipfs://$QMHASH/$FILE.csv.bz2' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'bz2'  record_delimiter = '\n' skip_header = 1)"
  # copy xz csv
  "copy into $TABLE from 'ipfs://$QMHASH/$FILE.csv.xz' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'xz'  record_delimiter = '\n' skip_header = 1)"
  # copy file
  "copy into $TABLE from 'ipfs://$QMHASH' FILES = ('$FILE.csv', '${FILE}_v1.csv') FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy dir with pattern
  "copy into $TABLE from 'ipfs://$QMHASH' PATTERN = 'ontime.*csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' record_delimiter = '\n' skip_header = 1)"
  # copy parquet
  "copy into $TABLE from 'ipfs://$QMHASH'  PATTERN = 'ontime.*parquet' FILE_FORMAT = (type = 'PARQUET')"
)

for i in "${copy_from_location_cases[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select count(1), avg(Year), sum(DayOfWeek)  from $TABLE" | $MYSQL_CLIENT_CONNECT
  echo "truncate table $TABLE" | $MYSQL_CLIENT_CONNECT
done

## Drop table
echo "drop table if exists $TABLE;" | $MYSQL_CLIENT_CONNECT