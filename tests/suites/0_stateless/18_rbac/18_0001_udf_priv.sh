#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "=== test UDF priv"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql -A --user=test-user --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_root_sql "
drop user if exists 'test-user';
DROP FUNCTION IF EXISTS f1;
DROP FUNCTION IF EXISTS f2;
drop table if exists default.t;
drop table if exists default.t2;
CREATE FUNCTION f1 AS (p) -> (p);
CREATE FUNCTION f2 AS (p) -> (p);
create or replace table default.t(i UInt8 not null);
create or replace table default.t2(i UInt8 not null);
create user 'test-user' IDENTIFIED BY '$TEST_USER_PASSWORD';
grant insert, delete, update, select on default.t to 'test-user';
grant select on default.t to 'test-user';
grant super on *.* to 'test-user';
SYSTEM FLUSH PRIVILEGES;
"

# error test need privielge f1
echo "=== Only Has Privilege on f2 ==="
echo "grant usage on udf f2 to 'test-user';" |  $BENDSQL_CLIENT_CONNECT
echo "SYSTEM FLUSH PRIVILEGES" | $BENDSQL_CLIENT_CONNECT
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
echo "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on 1 = 1 ignore_result;" | $TEST_USER_CONNECT
echo "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on 1 = 1 ignore_result;" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" | $TEST_USER_CONNECT
echo "merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" | $TEST_USER_CONNECT
echo "merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" | $TEST_USER_CONNECT
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
echo "SYSTEM FLUSH PRIVILEGES" | $BENDSQL_CLIENT_CONNECT
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
echo "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on 1 = 1 ignore_result;" | $TEST_USER_CONNECT
echo "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on 1 = 1 ignore_result;" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" | $TEST_USER_CONNECT
echo "merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" | $TEST_USER_CONNECT
echo "merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" | $TEST_USER_CONNECT
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
echo "select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on 1 = 1 ignore_result;" | $TEST_USER_CONNECT
echo "select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on 1 = 1 ignore_result;" | $TEST_USER_CONNECT
echo "select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));" | $TEST_USER_CONNECT
echo "merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));" | $TEST_USER_CONNECT
echo "merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));" | $TEST_USER_CONNECT
# Same issue: https://github.com/datafuselabs/databend/issues/13727, better to fix it in other issue.
#echo "REPLACE INTO t ON(i) values (f2(f1(2))), (3), (4)" | $TEST_USER_CONNECT
#echo "insert into t select f2(f1(33))" | $TEST_USER_CONNECT
echo "delete from t;" | $TEST_USER_CONNECT
echo "insert into t values (99),(199);" | $TEST_USER_CONNECT
echo "select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;" | $TEST_USER_CONNECT
echo "select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;" | $TEST_USER_CONNECT
echo "select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;" | $TEST_USER_CONNECT
echo "delete from t;" | $TEST_USER_CONNECT

run_root_sql "
drop function if exists a;
drop function if exists b;
CREATE FUNCTION a (TINYINT, SMALLINT, INT, BIGINT) RETURNS BIGINT LANGUAGE python HANDLER = 'add_signed' ADDRESS = 'http://0.0.0.0:8815';
CREATE FUNCTION b (TINYINT, SMALLINT, INT, BIGINT) RETURNS BIGINT LANGUAGE python HANDLER = 'add_signed' ADDRESS = 'http://0.0.0.0:8815';
grant usage on udf a to 'test-user';
"
echo "select a(1,1,1,1)" | $TEST_USER_CONNECT
echo "select b(1,1,1,1)" | $TEST_USER_CONNECT


run_root_sql "
drop user if exists 'test-user';
DROP FUNCTION IF EXISTS f1;
DROP FUNCTION IF EXISTS f2;
drop function if exists a;
drop function if exists b;
drop table if exists default.t;
drop table if exists default.t2;
"
