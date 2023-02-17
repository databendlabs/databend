#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists t1;" | $MYSQL_CLIENT_CONNECT
echo "CREATE TABLE t1 (id INT, name VARCHAR, age INT);" | $MYSQL_CLIENT_CONNECT
echo "insert into t1 (id,name,age) values(1,'2',3), (4, '5', 6);" | $MYSQL_CLIENT_CONNECT

echo '--- named internal stage'
echo "drop stage if exists s1;" | $MYSQL_CLIENT_CONNECT
echo "create stage s1 FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT
echo "copy into @s1 from t1;" | $MYSQL_CLIENT_CONNECT
echo "select * from @s1;" | $MYSQL_CLIENT_CONNECT

DATADIR_PATH="/tmp/08_00_00"
rm -rf ${DATADIR_PATH}
DATADIR="fs://$DATADIR_PATH/"
echo "copy into '${DATADIR}' from t1 FILE_FORMAT = (type = PARQUET);" | $MYSQL_CLIENT_CONNECT

#echo '--- uri'
#echo "select * from '${DATADIR}';" | $MYSQL_CLIENT_CONNECT

echo '--- external stage'
echo "drop stage if exists s2;" | $MYSQL_CLIENT_CONNECT
echo "create stage s2 url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | $MYSQL_CLIENT_CONNECT
echo "select * from @s2;" | $MYSQL_CLIENT_CONNECT

echo '--- file_format'
echo "drop stage if exists s3;" | $MYSQL_CLIENT_CONNECT
echo "create stage s3 url = '${DATADIR}' FILE_FORMAT = (type = CSV);"  | $MYSQL_CLIENT_CONNECT
echo "select * from @s3 (FILE_FORMAT => 'PARQUET');" | $MYSQL_CLIENT_CONNECT

rm -rf ${DATADIR_PATH}
