#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_NAME="owner"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=owner --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

stmt "drop user if exists 'owner'"
stmt "drop role if exists role1"
stmt "create user 'owner' IDENTIFIED BY '$TEST_USER_PASSWORD' with DEFAULT_ROLE='role1'"
stmt 'create role role1'

stmt 'grant role role1 to owner'
stmt 'grant create on default.* to role role1'
stmt 'drop table if exists t'
stmt 'drop view if exists v_t'
stmt 'drop table if exists t_owner'
stmt 'drop view if exists v_t_owner'
stmt 'drop view if exists v_t_union'
stmt 'drop view if exists v_t1'
stmt 'create table t(id int)'
stmt 'insert into t values(1)'
echo 'create table t_owner(c1 int)' | $TEST_USER_CONNECT
echo 'insert into t_owner values(2)' | $TEST_USER_CONNECT
stmt 'revoke create on default.* from role role1'

echo 'need failed: with 1063'
echo 'create view v_t as select * from t' | $TEST_USER_CONNECT
echo 'need failed: with 1063'
echo 'create view v_t_union as select * from t union all select * from t_owner' | $TEST_USER_CONNECT
echo 'create view v_t_owner as select * from t_owner' | $TEST_USER_CONNECT
stmt 'grant ownership on default.v_t_owner to role role1'

stmt 'create view v_t as select * from t'
stmt 'create view v_t_union as select * from t union all select * from t_owner'

echo "'select * from v_t order by id' failed."
echo "select * from v_t order by id" | $TEST_USER_CONNECT
stmt 'grant select on default.v_t to owner'
stmt 'grant select on default.v_t_union to owner'
echo "select * from v_t order by id" | $TEST_USER_CONNECT
echo "select * from v_t_owner order by c1" | $TEST_USER_CONNECT
echo "select * from v_t_union order by id" | $TEST_USER_CONNECT

query "select * from v_t order by id"
query "select * from v_t_owner order by c1"
query "select * from v_t_union order by id"

echo "=== create view as select view ==="

stmt 'revoke select on default.v_t from owner'
stmt 'grant select on default.t to owner'
echo 'create view v_t1 as select * from t union select * from v_t' | $TEST_USER_CONNECT
stmt 'grant select on default.v_t to owner'
stmt 'grant select on default.t to owner'
echo 'create view v_t1 as select * from t union select * from v_t' | $TEST_USER_CONNECT
stmt 'grant select on default.v_t1 to owner'
echo "select * from v_t1 order by id" | $TEST_USER_CONNECT

stmt 'drop table if exists t'
stmt 'drop view if exists v_t'
stmt 'drop table if exists t_owner'
stmt 'drop view if exists v_t_owner'
stmt 'drop view if exists v_t_union'
stmt 'drop view if exists v_t1'
stmt 'drop user if exists owner'
stmt 'drop role if exists role1'
