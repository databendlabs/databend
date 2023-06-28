#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# Should be <root>/tests/data/

CSV_PATH=/tmp/test_copy_p.csv
PARQUET_PATH=/tmp/test_copy_p.parquet

rm -rf $PARQUET_PATH

cat << EOF > $CSV_PATH
1,2
3,4
EOF

prepare=(
  "drop table if exists test_copy_p2"
  "drop table if exists test_copy_p3"
  "drop stage if exists s_copy_p"

  "create table test_copy_p2(b int, c int);"
  "create table test_copy_p3(a int, b int, c int);"

  # gen parquet
  "copy into test_copy_p2 from 'fs://${CSV_PATH}' FILE_FORMAT = (type = CSV)"
  "copy into 'fs://${PARQUET_PATH}' from test_copy_p2 FILE_FORMAT = (type = PARQUET)"
  "create stage s_copy_p url='fs://${PARQUET_PATH}/'"
)

for i in "${prepare[@]}"; do
	echo "$i" | $MYSQL_CLIENT_CONNECT
done

tests=(
  "copy into test_copy_p3(b, c) from 'fs://${CSV_PATH}' FILE_FORMAT = (type = CSV)"
  "copy into test_copy_p3(b, c) from @s_copy_p FILE_FORMAT = (type = PARQUET)"
  "copy into test_copy_p3(b, c) from (select t.b+1, t.c+1 from @s_copy_p t) FILE_FORMAT = (type = PARQUET)"
)

for i in "${tests[@]}"; do
  echo "truncate table test_copy_p3" | $MYSQL_CLIENT_CONNECT
  echo "$i"
  echo "$i" | $MYSQL_CLIENT_CONNECT
  echo "select * from test_copy_p3" | $MYSQL_CLIENT_CONNECT
done

clean_up=(
  "drop table test_copy_p2"
  "drop table test_copy_p3"
  "drop stage s_copy_p"
)
for i in "${clean_up[@]}"; do
	echo "$i" | $MYSQL_CLIENT_CONNECT
done
