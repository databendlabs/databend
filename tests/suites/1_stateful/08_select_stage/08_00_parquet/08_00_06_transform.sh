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

echo "drop stage if exists s1;" | $MYSQL_CLIENT_CONNECT
echo "create stage s1 url = '${DATADIR}' FILE_FORMAT = (type = TSV);"  | $MYSQL_CLIENT_CONNECT

echo '--- copy 1'
echo "copy into t1 from (select (t.id+1), age from @s1 t)  FILE_FORMAT = (type = parquet) PATTERN='.*parquet';" | $MYSQL_CLIENT_CONNECT
echo "select * from t1 order by id;" | $MYSQL_CLIENT_CONNECT

echo '--- copy 2'
echo "copy into t1 from (select (t.id+1), age from @s1 t)  FILE_FORMAT = (type = parquet)  PATTERN='.*parquet';" | $MYSQL_CLIENT_CONNECT
echo "select * from t1 order by id;" | $MYSQL_CLIENT_CONNECT

echo '--- copy 3'
echo "copy into t1 from (select (t.id+1), age from @s1 t)  FILE_FORMAT = (type = parquet)  PATTERN='.*parquet' force=true;" | $MYSQL_CLIENT_CONNECT
echo "select * from t1 order by id;" | $MYSQL_CLIENT_CONNECT

echo '--- copy csv'
echo "copy into t1 from (select (t.id+1), age from @s1 t)  FILE_FORMAT = (type = csv)  PATTERN='.*csv' force=true;" | $MYSQL_CLIENT_CONNECT
echo "select * from t1 order by id;" | $MYSQL_CLIENT_CONNECT

echo '--- copy from s3'
echo "drop table if exists t2;" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE t2 (a INT32);" | $MYSQL_CLIENT_CONNECT

echo "drop stage if exists s2;" | $MYSQL_CLIENT_CONNECT
echo "create stage s2 url='s3://testbucket/admin/data/' connection=(aws_key_id = 'minioadmin' aws_secret_key = 'minioadmin' endpoint_url = 'http://127.0.0.1:9900/') file_format = (type=parquet)" |  $MYSQL_CLIENT_CONNECT
echo "copy into t2 from (select (t.id+1) from @s2 t)  files=('tuple.parquet');" | $MYSQL_CLIENT_CONNECT
echo "select * from t2 order by a;" | $MYSQL_CLIENT_CONNECT

echo '--- copy from uri with transform'
echo "truncate table t2;" | $MYSQL_CLIENT_CONNECT
echo "copy into t2 from (select (t.id+1) from '${DATADIR}' t)  PATTERN='.*parquet';" | $MYSQL_CLIENT_CONNECT
echo "select * from t2 order by a;" | $MYSQL_CLIENT_CONNECT

rm -rf ${DATADIR_PATH}
