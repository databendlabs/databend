#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists test_table;" | bendsql_connect_root
echo "drop STAGE if exists s2;" | bendsql_connect_root
echo "CREATE STAGE s2;" | bendsql_connect_root

echo "CREATE TABLE test_table (
    id INTEGER,
    name VARCHAR,
    age INT
);" | bendsql_connect_root

for i in `seq 1 10`;do
    echo "insert into test_table (id,name,age) values(1,'2',3), (4, '5', 6);" | bendsql_connect_root
done

# The last column `output_bytes` is excluded to avoid flakiness
echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV);" | bendsql_connect_root | cut -d$'\t' -f1,2
echo "copy into @s2 from (select name, age, id from test_table limit 100) FILE_FORMAT = (type = 'PARQUET');" | bendsql_connect_root | cut -d$'\t' -f1,2
echo "list @s2;" | bendsql_connect_root | wc -l | sed 's/ //g'


# The last column `output_bytes` is excluded to avoid flakiness
echo "copy into @s2 from test_table FILE_FORMAT = (type = CSV) MAX_FILE_SIZE = 10;" | bendsql_connect_root | cut -d$'\t' -f1,2

lines=`echo "list @s2;" | bendsql_connect_root | wc -l`

if [ $lines -eq 1 ];then
    echo "More than one line"
fi

echo "drop STAGE s2;" | bendsql_connect_root
echo "drop table test_table;" | bendsql_connect_root
