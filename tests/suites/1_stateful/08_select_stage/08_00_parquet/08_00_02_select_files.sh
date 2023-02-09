#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh


DATADIR_PATH="/tmp/08_00_02"
rm -rf ${DATADIR_PATH}
mkdir ${DATADIR_PATH}
DATADIR="fs://$DATADIR_PATH/"

echo "drop stage if exists s2;" | $MYSQL_CLIENT_CONNECT
echo "create stage s2 url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | $MYSQL_CLIENT_CONNECT


cp "$CURDIR"/../../../../data/tuple.parquet ${DATADIR_PATH}/
cp "$CURDIR"/../../../../data/sample.csv ${DATADIR_PATH}/

echo "select * from @s2 (files => ('tuple.parquet'));" | $MYSQL_CLIENT_CONNECT

echo "---"

cp "$CURDIR"/../../../../data/tuple.parquet ${DATADIR_PATH}/tuple2.parquet
echo "select * from @s2 (pattern => '.*parquet');" | $MYSQL_CLIENT_CONNECT
