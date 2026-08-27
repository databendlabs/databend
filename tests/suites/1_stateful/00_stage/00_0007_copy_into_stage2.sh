#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists test_table;" | bendsql_connect_root
echo "drop STAGE if exists s2;" | bendsql_connect_root
echo "CREATE STAGE s2;" | bendsql_connect_root

STAGE_DIR=/tmp/copy_into_stage2

rm -rf "$STAGE_DIR"

echo "drop stage if exists s1;" | bendsql_connect_root
echo "create stage s1 url = 'fs:///$STAGE_DIR/' FILE_FORMAT = (type = PARQUET)" | bendsql_connect_root

echo "CREATE TABLE test_table (
    id INTEGER,
    name VARCHAR,
    age INT
);" | bendsql_connect_root

# each insert create a block
for i in `seq 1 10`;do
    echo "insert into test_table (id,name,age) values(1,'2',3), (4, '5', 6);" | bendsql_connect_root
done

check_csv() {
	echo "---${1}"
	ls "$STAGE_DIR"/${1} | wc -l | sed 's/ //g'
  cat "$STAGE_DIR"/${1}/* | wc -l | sed 's/ //g'
}

# each block create a CSV chunk of size 16
echo "copy into @s1/csv from test_table FILE_FORMAT = (type = CSV);" | bendsql_connect_root
check_csv "csv"
echo "copy into @s1/csv_single from test_table FILE_FORMAT = (type = CSV) single=true;" | bendsql_connect_root
check_csv "csv_single"
echo "copy into @s1/csv_10 from test_table FILE_FORMAT = (type = CSV) MAX_FILE_SIZE = 10;" | bendsql_connect_root
check_csv "csv_10"
echo "copy into @s1/csv_20 from test_table FILE_FORMAT = (type = CSV) MAX_FILE_SIZE = 20;" | bendsql_connect_root
check_csv "csv_20"

echo "drop table if exists t1;" | bendsql_connect_root
echo "create table t1 (a int);" | bendsql_connect_root

## CSV slice block by 1024, so we should get 2 CSV files but one parquet file.
for i in $(seq 1 2000);do
  echo "$i" >> "$STAGE_DIR"/big.csv
done
echo "copy into t1 from @s1/big.csv FILE_FORMAT = (type = CSV);" | bendsql_connect_root
echo "copy into @s1/csv_big_20 from t1 FILE_FORMAT = (type = CSV) MAX_FILE_SIZE = 20;" | bendsql_connect_root
check_csv "csv_big_20"