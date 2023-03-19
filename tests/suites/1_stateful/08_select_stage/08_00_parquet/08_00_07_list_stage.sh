#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh


DATADIR_PATH="/tmp/08_00_07/"
rm -rf ${DATADIR_PATH}
mkdir ${DATADIR_PATH}
mkdir ${DATADIR_PATH}/data
DATADIR="fs://$DATADIR_PATH/"

cp "$CURDIR"/../../../../data/tuple.parquet ${DATADIR_PATH}/data

# should omit this file
cp "$CURDIR"/../../../../data/sample.csv ${DATADIR_PATH}/00.csv

echo "drop stage if exists s7;" | $MYSQL_CLIENT_CONNECT
echo "create stage s7 url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | $MYSQL_CLIENT_CONNECT

echo "--- dir:"
echo "select * from list_stage(location => '@s7/data/');" | $MYSQL_CLIENT_CONNECT

echo "--- file:"
echo "select * from list_stage(location => '@s7/data/tuple.parquet');" | $MYSQL_CLIENT_CONNECT

echo "--- pattern:"
# should omit this file
cp "$CURDIR"/../../../../data/sample.csv ${DATADIR_PATH}/data/00.csv
echo "select * from list_stage(location => '@s7', pattern => '.*parquet');" | $MYSQL_CLIENT_CONNECT

echo "--- complex:"
cp "$CURDIR"/../../../../data/complex.parquet ${DATADIR_PATH}/data/complex.parquet
echo "select * from list_stage(location => '@s7/data/', pattern => 'complex.*');" | $MYSQL_CLIENT_CONNECT

echo "--- limit"
echo "select * from list_stage(location => '@s7/data/', pattern => '.*parquet') limit 1;" | $MYSQL_CLIENT_CONNECT

echo "--- where"
echo "select * from list_stage(location => '@s7/data/', pattern => '.*parquet') where name = 'data/tuple.parquet';" | $MYSQL_CLIENT_CONNECT

echo "--- list files so far"
echo "select * from list_stage(location => '@s7');" | $MYSQL_CLIENT_CONNECT
