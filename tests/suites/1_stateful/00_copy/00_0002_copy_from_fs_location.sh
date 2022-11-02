#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists ontime200;" | $MYSQL_CLIENT_CONNECT

## Create table
cat $CURDIR/../ddl/ontime.sql | sed 's/ontime/ontime200/g' | $MYSQL_CLIENT_CONNECT

# Should be <root>/tests/data/
DATADIR=$(realpath $CURDIR/../../../data/)

# This line is kept for debuging.
# echo "Current data dir: ${DATADIR}"

copy_from_location_cases=(
  # copy csv
  "copy into ontime200 from 'fs://${DATADIR}/ontime_200.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy gzip csv
  "copy into ontime200 from 'fs://${DATADIR}/ontime_200.csv.gz' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'gzip'  record_delimiter = '\n' skip_header = 1)"
  # copy zstd csv
  "copy into ontime200 from 'fs://${DATADIR}/ontime_200.csv.zst' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'zstd'  record_delimiter = '\n' skip_header = 1)"
  # copy bz2 csv
  "copy into ontime200 from 'fs://${DATADIR}/ontime_200.csv.bz2' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'bz2'  record_delimiter = '\n' skip_header = 1)"
  # copy xz csv
  "copy into ontime200 from 'fs://${DATADIR}/ontime_200.csv.xz' FILE_FORMAT = (type = 'CSV' field_delimiter = ',' compression = 'xz'  record_delimiter = '\n' skip_header = 1)"
  # copy dir with pattern
  "copy into ontime200 from 'fs://${DATADIR}/' PATTERN = 'ontime.*csv$' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)"
  # copy parquet
  "copy into ontime200 from 'fs://${DATADIR}/' PATTERN = 'ontime.*parquet' FILE_FORMAT = (type = 'PARQUET')"
  # copy ndjson with split size
  "copy into ontime200 from 'fs://${DATADIR}/ontime_200.ndjson' FILE_FORMAT = (type = 'ndjson') split_size = 10240"
)

for i in "${copy_from_location_cases[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select count(1), avg(Year), sum(DayOfWeek)  from ontime200" | $MYSQL_CLIENT_CONNECT
  echo "truncate table ontime200" | $MYSQL_CLIENT_CONNECT
done

## Drop table
echo "drop table if exists ontime200;" | $MYSQL_CLIENT_CONNECT
