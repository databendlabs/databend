#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


## create ontime table
cat $CURDIR/../ontime/create_table.sql | sed 's/ontime/ontime_streaming_load/g' | $MYSQL_CLIENT_CONNECT


if [ ! -f /tmp/ontime.csv ]; then
    wget -P /tmp "https://repo.databend.rs/dataset/stateful/ontime.csv" > /dev/null 2>&1
fi


curl -H "insert_sql:insert into ontime_streaming_load format CSV" -H "csv_header:1" -F  "upload=@/tmp/ontime.csv"  -XPUT http://localhost:8001/v1/streaming_load > /dev/null 2>&1


echo "select count(1) ,avg(Year), sum(DayOfWeek)  from ontime_streaming_load;" | $MYSQL_CLIENT_CONNECT
echo "drop table ontime_streaming_load;" | $MYSQL_CLIENT_CONNECT
