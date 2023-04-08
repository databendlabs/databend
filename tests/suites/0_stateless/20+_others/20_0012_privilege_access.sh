#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="mysql --defaults-extra-file=password.out --port ${QUERY_MYSQL_HANDLER_PORT} -s"
echo -e "[mysql]\nhost=${QUERY_MYSQL_HANDLER_HOST}\nuser=test-user\npassword=${TEST_USER_PASSWORD}" >> password.out

## create user
echo "create user 'test-user'@'$QUERY_MYSQL_HANDLER_HOST' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $MYSQL_CLIENT_CONNECT
## create table
echo "create table t20_0012(c int)" | $MYSQL_CLIENT_CONNECT

## show tables
echo "show databases" | $TEST_USER_CONNECT

## insert data
echo "insert into t20_0012 values(1),(2)" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT SELECT, INSERT ON * TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## insert data
echo "insert into t20_0012 values(1),(2)" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT

## update data
echo "update t20_0012 set c=3 where c=1" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT UPDATE ON * TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## update data
echo "update t20_0012 set c=3 where c=1" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT

## delete data
echo "delete from t20_0012 where c=2" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT DELETE ON * TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## delete data
echo "delete from t20_0012 where c=2" | $TEST_USER_CONNECT
## verify
echo "select count(*) = 0 from t20_0012 where c=2" | $TEST_USER_CONNECT

## optimize table
echo "optimize table t20_0012 all" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT Super ON *.* TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "GRANT SELECT ON system.fuse_snapshot TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## optimize table
echo "set retention_period=0; optimize table t20_0012 all" | $TEST_USER_CONNECT
## verify
echo "select count(*)=1  from fuse_snapshot('default', 't20_0012')" | $TEST_USER_CONNECT
## revoke privilege
echo "REVOKE SELECT ON system.fuse_snapshot FROM 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT

## select data
## Init tables
echo "CREATE TABLE default.t20_0012_a(c int) CLUSTER BY(c)" | $MYSQL_CLIENT_CONNECT
echo "GRANT INSERT ON default.t20_0012_a TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO default.t20_0012_a values(1)" | $TEST_USER_CONNECT
echo "CREATE TABLE default.t20_0012_b(c int)" | $MYSQL_CLIENT_CONNECT
echo "GRANT INSERT ON default.t20_0012_b TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO default.t20_0012_b values(1)" | $TEST_USER_CONNECT
## Init privilege
echo "REVOKE SELECT ON * FROM 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
## Verify table privilege separately
echo "select * from default.t20_0012_a order by c" | $TEST_USER_CONNECT
echo "GRANT SELECT ON default.t20_0012_a TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "select * from default.t20_0012_a order by c" | $TEST_USER_CONNECT
echo "select * from default.t20_0012_b order by c" | $TEST_USER_CONNECT
echo "GRANT SELECT ON default.t20_0012_b TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "select * from default.t20_0012_b order by c" | $TEST_USER_CONNECT
## Create view table
echo "create database default2" | $MYSQL_CLIENT_CONNECT
echo "create view default2.v_t20_0012 as select * from default.t20_0012_a" | $MYSQL_CLIENT_CONNECT
## Verify view table privilege
echo "select * from default2.v_t20_0012" | $TEST_USER_CONNECT
## Only grant privilege for view table
echo "GRANT SELECT ON default2.v_t20_0012 TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "REVOKE SELECT ON default.t20_0012_a FROM 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "REVOKE SELECT ON default.t20_0012_b FROM 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "select * from default2.v_t20_0012" | $TEST_USER_CONNECT

## select procedure
## clustering_information
echo "select count(*)=1 from clustering_information('default', 't20_0012_a')" | $TEST_USER_CONNECT
echo "GRANT SELECT ON system.clustering_information TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "select count(*)=1 from clustering_information('default', 't20_0012_a')" | $TEST_USER_CONNECT
## fuse_snapshot
echo "select count(*)=1 from fuse_snapshot('default', 't20_0012_a')" | $TEST_USER_CONNECT
echo "GRANT SELECT ON system.fuse_snapshot TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "select count(*)=1 from fuse_snapshot('default', 't20_0012_a')" | $TEST_USER_CONNECT
## fuse_segment
echo "select count(*)=0 from fuse_segment('default', 't20_0012_a', '')" | $TEST_USER_CONNECT
echo "GRANT SELECT ON system.fuse_segment TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "select count(*)=0 from fuse_segment('default', 't20_0012_a', '')" | $TEST_USER_CONNECT
## fuse_block
echo "select count(*)=1 from fuse_block('default', 't20_0012_a')" | $TEST_USER_CONNECT
echo "GRANT SELECT ON system.fuse_block TO 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
echo "select count(*)=1 from fuse_block('default', 't20_0012_a')" | $TEST_USER_CONNECT

## Drop table.
echo "drop table default.t20_0012 all" | $MYSQL_CLIENT_CONNECT
echo "drop table default.t20_0012_a all" | $MYSQL_CLIENT_CONNECT
echo "drop table default.t20_0012_b all" | $MYSQL_CLIENT_CONNECT
echo "drop view default2.v_t20_0012" | $MYSQL_CLIENT_CONNECT

## Drop database.
echo "drop database default2" | $MYSQL_CLIENT_CONNECT

## Drop user
echo "drop user 'test-user'@'$QUERY_MYSQL_HANDLER_HOST'" | $MYSQL_CLIENT_CONNECT
rm -rf password.out
