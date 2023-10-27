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


echo '--- copy from uri with transform'
echo "drop table if exists t2;" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE t2 (a INT32);" | $MYSQL_CLIENT_CONNECT

echo "copy into t2 from (select (t.id+1) from '${DATADIR}' t)  PATTERN='.*parquet';" | $MYSQL_CLIENT_CONNECT > /dev/null
echo "select * from t2 order by a;" | $MYSQL_CLIENT_CONNECT

rm -rf ${DATADIR_PATH}
