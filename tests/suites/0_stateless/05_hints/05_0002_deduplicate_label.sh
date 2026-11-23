#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists t5;" | bendsql_connect_root
echo "drop stage if exists s5;" | bendsql_connect_root

echo "CREATE TABLE t5(a Int, b bool) Engine = Fuse;" | bendsql_connect_root

echo "INSERT /*+ SET_VAR(deduplicate_label='insert-test') */ INTO t5 (a, b) VALUES(1, false)" | bendsql_connect_root_null
echo "INSERT /*+ SET_VAR(deduplicate_label='insert-test') */ INTO t5 (a, b) VALUES(1, false)" | bendsql_connect_root_null
echo "select * from t5" | bendsql_connect_root

echo "CREATE STAGE s5;" | bendsql_connect_root
if bendsql_connect_root_null --query="COPY /*+ SET_VAR(deduplicate_label='copy-test') */ INTO @s5 FROM (SELECT * FROM t5)" 2>/dev/null; then
    echo "Expected labeled stage export to be rejected" >&2
    exit 1
fi
bendsql_connect_root_null --query="COPY INTO @s5 FROM (SELECT * FROM t5)" || exit 1

echo "UPDATE /*+ SET_VAR(deduplicate_label='update-test') */ t5 SET a = 20 WHERE b = false;" | bendsql_connect_root_null
echo "UPDATE /*+ SET_VAR(deduplicate_label='update-test') */ t5 SET a = 30 WHERE b = false;" | bendsql_connect_root_null
echo "select * from t5" | bendsql_connect_root

echo "replace /*+ SET_VAR(deduplicate_label='replace-test') */ into t5 on(a,b) values(40,false);" | bendsql_connect_root_null
echo "replace /*+ SET_VAR(deduplicate_label='replace-test') */ into t5 on(a,b) values(50,false);" | bendsql_connect_root_null
echo "select * from t5 order by a" | bendsql_connect_root

echo "drop table if exists t5;" | bendsql_connect_root
echo "drop stage if exists s5;" | bendsql_connect_root
