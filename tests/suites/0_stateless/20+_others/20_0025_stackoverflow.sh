#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP STAGE IF EXISTS test_stackoverflow_stage" | $BENDSQL_CLIENT_CONNECT
echo "CREATE STAGE test_stackoverflow_stage" | $BENDSQL_CLIENT_CONNECT
cp $CURDIR/books.parquet $CURDIR/test.parquet

FILE_NAME="$CURDIR/test.parquet"

for i in `seq 1 50`;do
  mv $FILE_NAME "$CURDIR/test_$i.parquet"
  FILE_NAME="$CURDIR/test_$i.parquet"
  echo "PUT fs://$FILE_NAME @test_stackoverflow_stage"|$BENDSQL_CLIENT_CONNECT >/dev/null
done

rm $FILE_NAME

echo "CREATE OR REPLACE TABLE test_files_multiprocessing(file_name varchar, title varchar)"|$BENDSQL_CLIENT_CONNECT

SQL="SELECT 'working/demo/test_1.parquet', \$1 FROM @test_stackoverflow_stage/test_1.parquet"

for i in `seq 2 50`;do
  SQL="$SQL UNION ALL SELECT 'working/demo/test_$i.parquet', \$1 FROM @test_stackoverflow_stage/test_$i.parquet"
done

echo "insert into test_files_multiprocessing(file_name, title) $SQL"|$BENDSQL_CLIENT_CONNECT
