#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "DROP DATABASE IF EXISTS test_modify_column_type_no_rewrite" | $BENDSQL_CLIENT_CONNECT
echo "CREATE DATABASE test_modify_column_type_no_rewrite" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE test_modify_column_type_no_rewrite.t_int(a INT NULL)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type_no_rewrite.t_int VALUES (1), (2), (3)" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE test_modify_column_type_no_rewrite.loc_int(block_location STRING)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type_no_rewrite.loc_int SELECT block_location FROM fuse_block('test_modify_column_type_no_rewrite','t_int')" | $BENDSQL_CLIENT_CONNECT
echo "ALTER TABLE test_modify_column_type_no_rewrite.t_int MODIFY COLUMN a BIGINT NULL" | $BENDSQL_CLIENT_CONNECT
echo "SELECT (SELECT count(*) FROM fuse_block('test_modify_column_type_no_rewrite','t_int')) AS after_cnt, (SELECT count(*) FROM test_modify_column_type_no_rewrite.loc_int) AS before_cnt, (SELECT count(*) FROM (SELECT block_location FROM test_modify_column_type_no_rewrite.loc_int EXCEPT SELECT block_location FROM fuse_block('test_modify_column_type_no_rewrite','t_int'))) AS missing_from_after, (SELECT count(*) FROM (SELECT block_location FROM fuse_block('test_modify_column_type_no_rewrite','t_int') EXCEPT SELECT block_location FROM test_modify_column_type_no_rewrite.loc_int)) AS new_in_after" | $BENDSQL_CLIENT_CONNECT
echo "SELECT a FROM test_modify_column_type_no_rewrite.t_int ORDER BY a" | $BENDSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type_no_rewrite.t_int" | $BENDSQL_CLIENT_CONNECT

echo "CREATE TABLE test_modify_column_type_no_rewrite.t_dec(a DECIMAL(10, 2) NULL)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type_no_rewrite.t_dec VALUES (1.23), (4.56)" | $BENDSQL_CLIENT_CONNECT
echo "CREATE TABLE test_modify_column_type_no_rewrite.loc_dec(block_location STRING)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_modify_column_type_no_rewrite.loc_dec SELECT block_location FROM fuse_block('test_modify_column_type_no_rewrite','t_dec')" | $BENDSQL_CLIENT_CONNECT
echo "ALTER TABLE test_modify_column_type_no_rewrite.t_dec MODIFY COLUMN a DECIMAL(20, 2) NULL" | $BENDSQL_CLIENT_CONNECT
echo "SELECT (SELECT count(*) FROM fuse_block('test_modify_column_type_no_rewrite','t_dec')) AS after_cnt, (SELECT count(*) FROM test_modify_column_type_no_rewrite.loc_dec) AS before_cnt, (SELECT count(*) FROM (SELECT block_location FROM test_modify_column_type_no_rewrite.loc_dec EXCEPT SELECT block_location FROM fuse_block('test_modify_column_type_no_rewrite','t_dec'))) AS missing_from_after, (SELECT count(*) FROM (SELECT block_location FROM fuse_block('test_modify_column_type_no_rewrite','t_dec') EXCEPT SELECT block_location FROM test_modify_column_type_no_rewrite.loc_dec)) AS new_in_after" | $BENDSQL_CLIENT_CONNECT
echo "SELECT a FROM test_modify_column_type_no_rewrite.t_dec ORDER BY a" | $BENDSQL_CLIENT_CONNECT
echo "DESC test_modify_column_type_no_rewrite.t_dec" | $BENDSQL_CLIENT_CONNECT

echo "DROP DATABASE IF EXISTS test_modify_column_type_no_rewrite" | $BENDSQL_CLIENT_CONNECT
