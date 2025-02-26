#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_test_02_0010;" | $BENDSQL_CLIENT_CONNECT

echo "create table table_test_02_0010(TEXT String);" | $BENDSQL_CLIENT_CONNECT


table_inserts=(
  "INSERT INTO table_test_02_0010 VALUES('test_text1');"
  "INSERT INTO table_test_02_0010 VALUES('test_text2');"
  "INSERT INTO table_test_02_0010 VALUES('test_text3');"
  "INSERT INTO table_test_02_0010 VALUES('test_text4');"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $BENDSQL_CLIENT_CONNECT
done


echo "select count(*) from table_test_02_0010;" | $BENDSQL_CLIENT_CONNECT

# kill a node
sudo lsof -i :9093 -t | xargs -r sudo kill -9

echo "select count(*) from system.clusters;" | $BENDSQL_CLIENT_CONNECT

echo "select count(*) from table_test_02_0010;" | $BENDSQL_CLIENT_CONNECT

# restart the node
"$CURDIR"/../../../../scripts/ci/deploy/databend-query-cluster-3-nodes.sh > /dev/null 2>&1

echo "select count(*) from system.clusters;" | $BENDSQL_CLIENT_CONNECT
