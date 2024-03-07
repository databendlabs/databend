#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists t1;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t1 (id INT, name VARCHAR, age INT);" | $BENDSQL_CLIENT_CONNECT
echo "insert into t1 (id,name,age) values(1,'2',3), (4, '5', 6);" | $BENDSQL_CLIENT_CONNECT

echo '--- named internal stage'
echo "drop stage if exists s1;" | $BENDSQL_CLIENT_CONNECT
echo "create stage s1 FILE_FORMAT = (type = PARQUET);" | $BENDSQL_CLIENT_CONNECT
echo "copy into @s1 from t1;" | $BENDSQL_CLIENT_CONNECT
echo "select * from @s1;" | $BENDSQL_CLIENT_CONNECT

DATADIR_PATH="/tmp/08_00_00"
rm -rf ${DATADIR_PATH}
DATADIR="fs://$DATADIR_PATH/"
echo "copy into '${DATADIR}' from t1 FILE_FORMAT = (type = PARQUET);" | $BENDSQL_CLIENT_CONNECT

#echo '--- uri'
#echo "select * from '${DATADIR}';" | $BENDSQL_CLIENT_CONNECT

echo '--- external stage'
echo "drop stage if exists s2;" | $BENDSQL_CLIENT_CONNECT
echo "create stage s2 url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | $BENDSQL_CLIENT_CONNECT
echo "select * from @s2;" | $BENDSQL_CLIENT_CONNECT

echo '--- file_format'
echo "drop stage if exists s3;" | $BENDSQL_CLIENT_CONNECT
echo "create stage s3 url = '${DATADIR}' FILE_FORMAT = (type = CSV);"  | $BENDSQL_CLIENT_CONNECT
echo "select * from @s3 (FILE_FORMAT => 'PARQUET');" | $BENDSQL_CLIENT_CONNECT

echo "drop table if exists t2;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE t2 (id INT, data VARIANT);" | $BENDSQL_CLIENT_CONNECT
echo "insert into t2 (id,data) values(1,'[1,2,3]'),(2,'{\"k\":\"v\"}');" | $BENDSQL_CLIENT_CONNECT

echo '--- variant named internal stage'
echo "drop stage if exists s4;" | $BENDSQL_CLIENT_CONNECT
echo "create stage s4 FILE_FORMAT = (type = PARQUET);" | $BENDSQL_CLIENT_CONNECT
echo "copy into @s4 from t2;" | $BENDSQL_CLIENT_CONNECT
echo "select * from @s4;" | $BENDSQL_CLIENT_CONNECT

rm -rf ${DATADIR_PATH}
