#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_bendsql() {
  cat <<SQL | $BENDSQL_CLIENT_CONNECT
$1
SQL
}

run_test_user_sql() {
  cat <<SQL | $TEST_USER_CONNECT
$1
SQL
}

run_test_user_sql_with_print() {
  cat <<SQL | tee >(cat >&1) | $TEST_USER_CONNECT
$1
SQL
}

echo "=== test UDF priv"
export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=test-user --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_bendsql "
DROP USER IF EXISTS 'test-user';
DROP FUNCTION IF EXISTS f1;
DROP FUNCTION IF EXISTS f2;
DROP TABLE IF EXISTS default.t;
DROP TABLE IF EXISTS default.t2;
CREATE FUNCTION f1 AS (p) -> (p);
CREATE FUNCTION f2 AS (p) -> (p);
CREATE OR REPLACE TABLE default.t(i UInt8 NOT NULL);
CREATE OR REPLACE TABLE default.t2(i UInt8 NOT NULL);
"

## create user
run_bendsql "
CREATE USER 'test-user' IDENTIFIED BY '$TEST_USER_PASSWORD';
GRANT INSERT, DELETE, UPDATE, SELECT ON default.t TO 'test-user';
GRANT SELECT ON default.t TO 'test-user';
GRANT SUPER ON *.* TO 'test-user';
"
sleep 2;

# error test need privielge f1
echo "=== Only Has Privilege on f2 ==="
run_bendsql "
GRANT USAGE ON UDF f2 TO 'test-user';
"
sleep 1;
run_test_user_sql "
select f2(f1(1));
select add(1, f2(f1(1)));
select max(f2(f1(1)));
"
run_test_user_sql_with_print "
select 1 from (select f2(f1(10)));
select * from system.one where f2(f1(1));
"
# bug need error
run_test_user_sql "
insert into t values (f2(f1(1)));
update t set i=f2(f1(2)) where i=f2(f1(1));
"
# bug need error
run_test_user_sql "
SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t;
delete from t where f2(f1(1));
select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`;
select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on 1 = 1 ignore_result;
select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on 1 = 1 ignore_result;
select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));
merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));
merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));
delete from t;
insert into t values (99),(199);
select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;
select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;
select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;
delete from t;
"

# error test need privielge f1
echo "=== Only Has Privilege on f1 ==="
run_bendsql "
GRANT USAGE ON UDF f1 TO 'test-user';
REVOKE USAGE ON UDF f2 FROM 'test-user';
"
sleep 1;
run_test_user_sql "
select f2(f1(1));
select add(1, f1(1));
select max(f2(f1(1)));
select 1 from (select f2(f1(10)));
select * from system.one where f2(f1(1));
"
# bug need error
run_test_user_sql "
insert into t values (f2(f1(1)));
update t set i=f2(f1(2)) where i=f2(f1(1));
"
# bug need error
run_test_user_sql "
SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t;
delete from t where f2(f1(1));
select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`;
select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on 1 = 1 ignore_result;
select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on 1 = 1 ignore_result;
select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));
merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));
merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));
delete from t;
insert into t values (99),(199);
select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;
select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;
select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;
delete from t;
"

echo "=== Has Privilege on f1, f2 ==="
run_bendsql "
GRANT USAGE ON UDF f1 TO 'test-user';
GRANT ALL ON UDF f2 TO 'test-user';
"
run_test_user_sql "
select f2(f1(1));
select add(1, f1(1));
select max(f2(f1(1)));
select 1 from (select f2(f1(10)));
select * from system.one where f2(f1(1));
"
# bug need error
run_test_user_sql "
insert into t values (f2(f1(1)));
update t set i=f2(f1(2)) where i=f2(f1(1));
"
# bug need error
run_test_user_sql "
SELECT i,	nth_value(i, f2(f1(2))) OVER (PARTITION BY i) fv FROM t;
delete from t where f2(f1(1));
select f2(f1(1)) union all select 2 order by \`f2(f1(1))\`;
select 22, t.i as ti from t join (select f2(f1(1)), i from t) as t1 on 1 = 1 ignore_result;
select f2(f1(1)), t.i as ti from t join (select 22, i from t) as t1 on 1 = 1 ignore_result;
select 22, t.i as ti from t join (select 22, i from t) as t1 on f2(f1(false));
merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when matched then update set t3.i=f2(f1(13));
merge into t as t3 using (select * from t2) as t2 on t3.i = t2.i  when not matched  then insert (i) values(f2(f1(100)));
delete from t;
insert into t values (99),(199);
select case when i > f2(f1(100)) then 200 else 100 end as c1 from t;
select case when i > 100 then 200 else f2(f1(100)) end as c1 from t;
select case when i > 100 then f2(f1(200)) else 100 end as c1 from t;
delete from t;
"

#udf server test
run_bendsql "
DROP FUNCTION IF EXISTS a;
DROP FUNCTION IF EXISTS b;
CREATE FUNCTION a (TINYINT, SMALLINT, INT, BIGINT) RETURNS BIGINT LANGUAGE python HANDLER = 'add_signed' ADDRESS = 'http://0.0.0.0:8815';
CREATE FUNCTION b (TINYINT, SMALLINT, INT, BIGINT) RETURNS BIGINT LANGUAGE python HANDLER = 'add_signed' ADDRESS = 'http://0.0.0.0:8815';
"

run_bendsql "
GRANT USAGE ON UDF a TO 'test-user';
"
run_test_user_sql "
select a(1,1,1,1);
select b(1,1,1,1);
"


run_bendsql "
DROP USER IF EXISTS 'test-user';
DROP FUNCTION IF EXISTS f1;
DROP FUNCTION IF EXISTS f2;
DROP FUNCTION IF EXISTS a;
DROP FUNCTION IF EXISTS b;
DROP TABLE IF EXISTS default.t;
DROP TABLE IF EXISTS default.t2;
"
