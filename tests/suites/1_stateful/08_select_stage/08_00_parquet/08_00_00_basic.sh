#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../../shell_env.sh

echo "drop table if exists t1;" | bendsql_connect_root
echo "CREATE TABLE t1 (id INT, name VARCHAR, age INT);" | bendsql_connect_root
echo "insert into t1 (id,name,age) values(1,'2',3), (4, '5', 6);" | bendsql_connect_root

echo '--- named internal stage'
echo "drop stage if exists s1;" | bendsql_connect_root
echo "create stage s1 FILE_FORMAT = (type = PARQUET);" | bendsql_connect_root
echo "copy into @s1 from t1;" | bendsql_connect_root | cut -d$'\t' -f1,2
echo "select * from @s1;" | bendsql_connect_root

DATADIR_PATH="/tmp/08_00_00"
rm -rf ${DATADIR_PATH}
DATADIR="fs://$DATADIR_PATH/"
echo "copy into '${DATADIR}' from t1 FILE_FORMAT = (type = PARQUET);" | bendsql_connect_root | cut -d$'\t' -f1,2

#echo '--- uri'
#echo "select * from '${DATADIR}';" | bendsql_connect_root

echo '--- external stage'
echo "drop stage if exists s2;" | bendsql_connect_root
echo "create stage s2 url = '${DATADIR}' FILE_FORMAT = (type = PARQUET);"  | bendsql_connect_root
echo "select * from @s2;" | bendsql_connect_root

echo '--- file_format'
echo "drop stage if exists s3;" | bendsql_connect_root
echo "create stage s3 url = '${DATADIR}' FILE_FORMAT = (type = CSV);"  | bendsql_connect_root
echo "select * from @s3 (FILE_FORMAT => 'PARQUET');" | bendsql_connect_root

echo "drop table if exists t2;" | bendsql_connect_root
echo "CREATE TABLE t2 (id INT, data VARIANT);" | bendsql_connect_root
echo "insert into t2 (id,data) values(1,'[1,2,3]'),(2,'{\"k\":\"v\"}');" | bendsql_connect_root

echo '--- variant named internal stage'
echo "drop stage if exists s4;" | bendsql_connect_root
echo "create stage s4 FILE_FORMAT = (type = PARQUET);" | bendsql_connect_root
echo "copy into @s4 from t2;" | bendsql_connect_root | cut -d$'\t' -f1,2
echo "select * from @s4;" | bendsql_connect_root

## generate large parquet files will cause timeout, we comment it now
echo '--- large parquet file should be worked on parallel by rowgroups'
echo 'remove @s4;' | bendsql_connect_root
echo 'remove @s1;' | bendsql_connect_root
echo "copy into @s4 from (select number a, number::string b, number::decimal(15,2) c from numbers(5000000)) file_format=(type=parquet) single=true;" | bendsql_connect_root | cut -d$'\t' -f1

## A 2 MiB hint should split the large file into multiple scan partitions.
PARTITIONS=$(echo """
set parquet_rowgroup_hint_bytes = 2 * 1024 * 1024;
explain select * from @s4;
""" | bendsql_connect_root | grep 'partitions ')
PARTITIONS_TOTAL=$(echo "$PARTITIONS" | awk -F': ' '/partitions total/ {print $2}')
PARTITIONS_SCANNED=$(echo "$PARTITIONS" | awk -F': ' '/partitions scanned/ {print $2}')
if [[ $PARTITIONS_TOTAL -le 1 || $PARTITIONS_SCANNED -ne $PARTITIONS_TOTAL ]]; then
  echo "expected all of multiple partitions to be scanned, got total=$PARTITIONS_TOTAL scanned=$PARTITIONS_SCANNED" >&2
  exit 1
fi
echo 'parallel scan partitions: true'

echo "copy into @s1 from (select * from @s4) file_format=(type=parquet)" | bendsql_connect_root | cut -d$'\t' -f1
echo "select * from @s1 order by a except (select * from @s4 order by a)" | bendsql_connect_root

echo 'remove @s4;' | bendsql_connect_root
echo 'remove @s1;' | bendsql_connect_root

rm -rf ${DATADIR_PATH}
