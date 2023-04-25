#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


DATADIR_PATH="/tmp/small_parquets"
rm -rf ${DATADIR_PATH}
mkdir ${DATADIR_PATH}
DATADIR="fs://$DATADIR_PATH/"

cp "$CURDIR"/../../../data/tuple.parquet ${DATADIR_PATH}/

echo "drop stage if exists small_parquets;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists small_parquets;" | $MYSQL_CLIENT_CONNECT

echo "create stage small_parquets url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | $MYSQL_CLIENT_CONNECT
echo "create table small_parquets (id int, t tuple(a int, b string));" | $MYSQL_CLIENT_CONNECT

echo "--- copy"
echo "set parquet_read_whole_file_threshold=4096; copy into small_parquets from @small_parquets;" | $MYSQL_CLIENT_CONNECT
echo "select * from small_parquets" | $MYSQL_CLIENT_CONNECT
echo "truncate table small_parquets" | $MYSQL_CLIENT_CONNECT

echo "--- copy from select"
echo "set parquet_read_whole_file_threshold=4096; copy into small_parquets from (select * from @small_parquets t) force = true;" | $MYSQL_CLIENT_CONNECT
echo "select * from small_parquets" | $MYSQL_CLIENT_CONNECT

echo "drop stage small_parquets;"  | $MYSQL_CLIENT_CONNECT
echo "drop table small_parquets;" | $MYSQL_CLIENT_CONNECT
