#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh
DATADIR_PATH="$CURDIR/../../../../data/"

echo "drop stage if exists data;" | $MYSQL_CLIENT_CONNECT
echo "create stage data url = 'fs://$DATADIR_PATH' " | $MYSQL_CLIENT_CONNECT

for USE_PARQUET2 in 0 1; do

echo "USE_PARQUET2=${USE_PARQUET2}"

echo "set use_parquet2 = ${USE_PARQUET2} ; select t_timestamp from @data/mytime.parquet" | $MYSQL_CLIENT_CONNECT

echo "set use_parquet2 = ${USE_PARQUET2} ; select t_timestamp from @data/mytime.parquet where id < '100051135'" | $MYSQL_CLIENT_CONNECT

done
