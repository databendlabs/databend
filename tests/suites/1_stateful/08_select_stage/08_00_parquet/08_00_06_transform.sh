#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists t1;" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE t1 (id INT, age INT);" | $MYSQL_CLIENT_CONNECT
echo "insert into t1 (id, age) values(1,3), (4, 6);" | $MYSQL_CLIENT_CONNECT

DATADIR_PATH="/tmp/08_00_06"
rm -rf ${DATADIR_PATH}
DATADIR="fs://$DATADIR_PATH/"
echo "copy into '${DATADIR}' from t1 FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT
touch ${DATADIR_PATH}/transform.csv

echo "drop stage if exists s2;" | $MYSQL_CLIENT_CONNECT
echo "create stage s2 url = '${DATADIR}' FILE_FORMAT = (type = TSV);"  | $MYSQL_CLIENT_CONNECT

echo '--- copy 1'
echo "copy into t1 from (select (t.id+1), age from @s2 t)  FILE_FORMAT = (type = parquet) PATTERN='.*parquet';" | $MYSQL_CLIENT_CONNECT
echo "select * from t1 order by id;" | $MYSQL_CLIENT_CONNECT

echo '--- copy 2'
echo "copy into t1 from (select (t.id+1), age from @s2 t)  FILE_FORMAT = (type = parquet)  PATTERN='.*parquet';" | $MYSQL_CLIENT_CONNECT
echo "select * from t1 order by id;" | $MYSQL_CLIENT_CONNECT

echo '--- copy 3'
echo "copy into t1 from (select (t.id+1), age from @s2 t)  FILE_FORMAT = (type = parquet)  PATTERN='.*parquet' force=true;" | $MYSQL_CLIENT_CONNECT
echo "select * from t1 order by id;" | $MYSQL_CLIENT_CONNECT

echo '--- copy csv'
echo "copy into t1 from (select (t.id+1), age from @s2 t)  FILE_FORMAT = (type = csv)  PATTERN='.*csv' force=true;" | $MYSQL_CLIENT_CONNECT
echo "select * from t1 order by id;" | $MYSQL_CLIENT_CONNECT

#rm -rf ${DATADIR_PATH}
