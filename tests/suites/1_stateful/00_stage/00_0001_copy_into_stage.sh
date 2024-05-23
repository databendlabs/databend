#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists test_table;" | $BENDSQL_CLIENT_CONNECT
echo "drop STAGE if exists s2;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE STAGE s2;" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE test_table (
    id INTEGER,
    name VARCHAR,
    age INT
);" | $BENDSQL_CLIENT_CONNECT

for i in `seq 1 10`;do
    echo "insert into test_table (id,name,age) values(1,'2',3), (4, '5', 6);" | $BENDSQL_CLIENT_CONNECT
done


echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV);" | $BENDSQL_CLIENT_CONNECT
echo "copy into @s2 from (select name, age, id from test_table limit 100) FILE_FORMAT = (type = 'PARQUET');" | $BENDSQL_CLIENT_CONNECT
echo "list @s2;" | $BENDSQL_CLIENT_CONNECT | wc -l | sed 's/ //g'


echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV) MAX_FILE_SIZE = 10;" | $BENDSQL_CLIENT_CONNECT

lines=`echo "list @s2;" | $BENDSQL_CLIENT_CONNECT | wc -l`

if [ $lines -eq 1 ];then
    echo "More than one line"
fi

echo "drop STAGE s2;" | $BENDSQL_CLIENT_CONNECT
echo "drop table test_table;" | $BENDSQL_CLIENT_CONNECT
