#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "=== test UDF priv"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=test-user --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "drop user if exists 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "DROP FUNCTION IF EXISTS f1;" |  $BENDSQL_CLIENT_CONNECT
echo "DROP FUNCTION IF EXISTS f2;" |  $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.t;" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.t2;" | $BENDSQL_CLIENT_CONNECT

echo "CREATE FUNCTION f1 AS (p) -> (p)" | $BENDSQL_CLIENT_CONNECT
echo "CREATE FUNCTION f2 AS (p) -> (p)" | $BENDSQL_CLIENT_CONNECT
echo "create table default.t(i UInt8 not null);" | $BENDSQL_CLIENT_CONNECT
echo "create table default.t2(i UInt8 not null);" | $BENDSQL_CLIENT_CONNECT

## create user
echo "create user 'test-user' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
echo "grant insert, delete, update, select on default.t to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
echo "grant select on default.t to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
echo "grant super on *.* to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
sleep 2;

# error test need privielge f1
echo "=== Only Has Privilege on f2 ==="
echo "grant usage on udf f2 to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
sleep 1;
echo "select f2(f1(1));" | $TEST_USER_CONNECT
# bug need error
echo "select add(1, f2(f1(1)));" | $TEST_USER_CONNECT
echo "select max(f2(f1(1)));" | $TEST_USER_CONNECT
echo "select 1 from (select f2(f1(10)));"
echo "select 1 from (select f2(f1(10)));" | $TEST_USER_CONNECT
echo "select * from system.one where f2(f1(1));"
echo "select * from system.one where f2(f1(1));" | $TEST_USER_CONNECT
# bug need error
echo "insert into t values (f2(f1(1)));" | $TEST_USER_CONNECT
echo "update t set i=f2(f1(2)) where i=f2(f1(1));" | $TEST_USER_CONNECT
# bug need error
echo "SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t" | $TEST_USER_CONNECT
echo "delete from t where f2(f1(1));" | $TEST_USER_CONNECT
echo "select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on false;" | $TEST_USER_CONNECT
echo "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on false;" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" | $TEST_USER_CONNECT
echo "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" | $TEST_USER_CONNECT
echo "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" | $TEST_USER_CONNECT
# Same issue: https://github.com/datafuselabs/databend/issues/13727, better to fix it in other issue.
#echo "REPLACE INTO t ON(i) values (f2(f1(2))), (3), (4)" | $TEST_USER_CONNECT
#echo "insert into t select f2(f1(33))" | $TEST_USER_CONNECT
echo "delete from t;" | $TEST_USER_CONNECT
echo "insert into t values (99),(199);" | $TEST_USER_CONNECT
echo "select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;" | $TEST_USER_CONNECT
echo "select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;" | $TEST_USER_CONNECT
echo "select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;" | $TEST_USER_CONNECT
echo "delete from t;" | $TEST_USER_CONNECT

# error test need privielge f1
echo "=== Only Has Privilege on f1 ==="
echo "grant usage on udf f1 to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
echo "revoke usage on udf f2 from 'test-user';" |  $BENDSQL_CLIENT_CONNECT
sleep 1;
echo "select f2(f1(1))" | $TEST_USER_CONNECT
echo "select add(1, f1(1))" | $TEST_USER_CONNECT
echo "select max(f2(f1(1)));" | $TEST_USER_CONNECT
echo "select 1 from (select f2(f1(10)));" | $TEST_USER_CONNECT
echo "select * from system.one where f2(f1(1));" | $TEST_USER_CONNECT
# bug need error
echo "insert into t values (f2(f1(1)));" | $TEST_USER_CONNECT
echo "update t set i=f2(f1(2)) where i=f2(f1(1));" | $TEST_USER_CONNECT
# bug need error
echo "SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t" | $TEST_USER_CONNECT
echo "delete from t where f2(f1(1));" | $TEST_USER_CONNECT
echo "select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on false;" | $TEST_USER_CONNECT
echo "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on false;" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" | $TEST_USER_CONNECT
echo "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" | $TEST_USER_CONNECT
echo "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" | $TEST_USER_CONNECT
# Same issue: https://github.com/datafuselabs/databend/issues/13727, better to fix it in other issue.
#echo "REPLACE INTO t ON(i) values (f2(f1(2))), (3), (4)" | $TEST_USER_CONNECT
#echo "insert into t select f2(f1(33))" | $TEST_USER_CONNECT
echo "delete from t;" | $TEST_USER_CONNECT
echo "insert into t values (99),(199);" | $TEST_USER_CONNECT
echo "select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;" | $TEST_USER_CONNECT
echo "select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;" | $TEST_USER_CONNECT
echo "select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;" | $TEST_USER_CONNECT
echo "delete from t;" | $TEST_USER_CONNECT

echo "=== Has Privilege on f1, f2 ==="
echo "grant usage on udf f1 to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
echo "grant all on udf f2 to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
echo "select f2(f1(1))" | $TEST_USER_CONNECT
echo "select add(1, f1(1))" | $TEST_USER_CONNECT
echo "select max(f2(f1(1)));" | $TEST_USER_CONNECT
echo "select 1 from (select f2(f1(10)));" | $TEST_USER_CONNECT
echo "select * from system.one where f2(f1(1));" | $TEST_USER_CONNECT
# bug need error
echo "insert into t values (f2(f1(1)));" | $TEST_USER_CONNECT
echo "update t set i=f2(f1(2)) where i=f2(f1(1));" | $TEST_USER_CONNECT
# bug need error
echo "SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t" | $TEST_USER_CONNECT
echo "delete from t where f2(f1(1));" | $TEST_USER_CONNECT
echo "select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on false;" | $TEST_USER_CONNECT
echo "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on false;" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" | $TEST_USER_CONNECT
echo "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" | $TEST_USER_CONNECT
echo "set enable_experimental_merge_into = 1; merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" | $TEST_USER_CONNECT
# Same issue: https://github.com/datafuselabs/databend/issues/13727, better to fix it in other issue.
#echo "REPLACE INTO t ON(i) values (f2(f1(2))), (3), (4)" | $TEST_USER_CONNECT
#echo "insert into t select f2(f1(33))" | $TEST_USER_CONNECT
echo "delete from t;" | $TEST_USER_CONNECT
echo "insert into t values (99),(199);" | $TEST_USER_CONNECT
echo "select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;" | $TEST_USER_CONNECT
echo "select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;" | $TEST_USER_CONNECT
echo "select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;" | $TEST_USER_CONNECT
echo "delete from t;" | $TEST_USER_CONNECT

#udf server test
echo "drop function if exists a;" | $BENDSQL_CLIENT_CONNECT
echo "drop function if exists b;" | $BENDSQL_CLIENT_CONNECT
echo "CREATE FUNCTION a (TINYINT, SMALLINT, INT, BIGINT) RETURNS BIGINT LANGUAGE python HANDLER = 'add_signed' ADDRESS = 'http://0.0.0.0:8815';" | $BENDSQL_CLIENT_CONNECT
echo "CREATE FUNCTION b (TINYINT, SMALLINT, INT, BIGINT) RETURNS BIGINT LANGUAGE python HANDLER = 'add_signed' ADDRESS = 'http://0.0.0.0:8815';" | $BENDSQL_CLIENT_CONNECT

echo "grant usage on udf a to 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "select a(1,1,1,1)" | $TEST_USER_CONNECT
echo "select b(1,1,1,1)" | $TEST_USER_CONNECT


echo "drop user if exists 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "DROP FUNCTION IF EXISTS f1;" |  $BENDSQL_CLIENT_CONNECT
echo "DROP FUNCTION IF EXISTS f2;" |  $BENDSQL_CLIENT_CONNECT
echo "drop function if exists a;" | $BENDSQL_CLIENT_CONNECT
echo "drop function if exists b;" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.t;" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.t2;" | $BENDSQL_CLIENT_CONNECT
