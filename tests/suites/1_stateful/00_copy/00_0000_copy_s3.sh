#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

## Create table
cat $CURDIR/../ontime/create_table.sql | sed 's/ontime/ontime_s3_copy/g' | $MYSQL_CLIENT_CONNECT

## Copy from s3.
echo "copy into ontime_s3_copy from 's3://repo.databend.rs/dataset/stateful/ontime.csv' FILE_FORMAT = (type = 'CSV' field_delimiter = ','  record_delimiter = '\n' skip_header = 1)" | $MYSQL_CLIENT_CONNECT

## Result.
echo "select count(1) ,avg(Year), sum(DayOfWeek)  from ontime_s3_copy" | $MYSQL_CLIENT_CONNECT

## Drop table.
echo "drop table ontime_s3_copy" | $MYSQL_CLIENT_CONNECT
