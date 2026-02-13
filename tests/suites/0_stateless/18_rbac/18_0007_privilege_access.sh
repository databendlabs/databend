#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_root_sql() {
  cat <<SQL | $BENDSQL_CLIENT_CONNECT
$1
SQL
}

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql -A --user=test-user --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export RM_UUID="sed -E ""s/[-a-z0-9]{32,36}/UUID/g"""

run_test_user() {
  cat <<SQL | $TEST_USER_CONNECT
$1
SQL
}

run_root_sql "
drop database if exists db01;
create database db01;
drop database if exists dbnotexists;
"

# These need separate execution to show individual errors
echo "GRANT SELECT ON db01.tbnotexists TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT SELECT ON dbnotexists.* TO 'test-user'" | $BENDSQL_CLIENT_CONNECT

run_root_sql "
drop user if exists 'test-user';
drop role if exists 'test-role1';
drop role if exists 'test-role2';
create user 'test-user' IDENTIFIED BY '$TEST_USER_PASSWORD';
create role \`test-role1\`;
create role \`test-role2\`;
drop table if exists t20_0012;
create table t20_0012(c int not null);
"

echo "=== check list user's local stage ==="
echo "list @~" | $TEST_USER_CONNECT
echo "=== no err ==="

## show tables
echo "show databases" | $TEST_USER_CONNECT

## insert data - test permission denied first
echo "select 'test -- insert'" | $TEST_USER_CONNECT
echo "insert into t20_0012 values(1),(2)" | $TEST_USER_CONNECT

## grant user privileges via role
run_root_sql "
GRANT INSERT ON * TO ROLE \`test-role1\`;
GRANT SELECT ON * TO ROLE \`test-role2\`;
GRANT ROLE \`test-role1\` TO 'test-user';
GRANT ROLE \`test-role2\` TO 'test-user';
"

## insert data and verify
run_test_user "
insert into t20_0012 values(1),(2);
select * from t20_0012 order by c;
"

## insert overwrite, but no delete privilege - expect error
echo "insert overwrite t20_0012 values(3),(4)" | $TEST_USER_CONNECT

## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT

## update data - test permission denied
echo "select 'test -- update'" | $TEST_USER_CONNECT
echo "update t20_0012 set c=3 where c=1" | $TEST_USER_CONNECT

## grant user privilege and test update
run_root_sql "GRANT UPDATE ON * TO 'test-user';"
run_test_user "
update t20_0012 set c=3 where c=1;
select * from t20_0012 order by c;
"

## delete data - test permission denied
echo "select 'test -- delete'" | $TEST_USER_CONNECT
echo "delete from t20_0012 where c=2" | $TEST_USER_CONNECT

## grant user privilege and test delete
run_root_sql "GRANT DELETE ON * TO 'test-user';"
run_test_user "
delete from t20_0012 where c=2;
select count(*) = 0 from t20_0012 where c=2;
"

## insert overwrite
echo "select 'test -- insert overwrite'" | $TEST_USER_CONNECT
run_root_sql "GRANT DELETE ON default.t20_0012 TO 'test-user';"
run_test_user "
insert overwrite t20_0012 values(2);
select * from t20_0012 order by c;
insert overwrite t20_0012 values(3);
select * from t20_0012 order by c;
"

## optimize table - test permission denied
echo "select 'test -- optimize table'" | $TEST_USER_CONNECT
echo "optimize table t20_0012 all" | $TEST_USER_CONNECT

## grant user privilege and test optimize
run_root_sql "GRANT Super ON *.* TO 'test-user';"
run_test_user "
set data_retention_time_in_days=0; optimize table t20_0012 all;
select count(*)>=1 from fuse_snapshot('default', 't20_0012');
"

echo "=== NETWORK_POLICY SETTING ==="
# These need separate execution to show errors
echo "drop network policy if exists test_user_without_account_admin"  | $TEST_USER_CONNECT
echo "create network policy test_user_without_account_admin allowed_ip_list=('127.0.0.0/24')"  | $TEST_USER_CONNECT
echo "set global network_policy='test_user_without_account_admin'"  | $TEST_USER_CONNECT
echo "unset global network_policy"  | $TEST_USER_CONNECT
echo "drop network policy if exists test_user_without_account_admin"  | $TEST_USER_CONNECT

## select data
echo "select 'test -- select'" | $TEST_USER_CONNECT

## Init tables
run_root_sql "
CREATE TABLE default.t20_0012_a(c int not null) CLUSTER BY(c);
GRANT INSERT ON default.t20_0012_a TO 'test-user';
CREATE TABLE default.t20_0012_b(c int not null);
GRANT INSERT ON default.t20_0012_b TO 'test-user';
REVOKE SELECT ON * FROM 'test-user';
"

run_test_user "
INSERT INTO default.t20_0012_a values(1);
INSERT INTO default.t20_0012_b values(1);
"

## Verify table privilege separately - expect error first
echo "select * from default.t20_0012_a order by c" | $TEST_USER_CONNECT
run_root_sql "GRANT SELECT ON default.t20_0012_a TO 'test-user';"
echo "select * from default.t20_0012_a order by c" | $TEST_USER_CONNECT
echo "select * from default.t20_0012_b order by c" | $TEST_USER_CONNECT
run_root_sql "GRANT SELECT ON default.t20_0012_b TO 'test-user';"
echo "select * from default.t20_0012_b order by c" | $TEST_USER_CONNECT

## Create view table
echo "select 'test -- select view'" | $TEST_USER_CONNECT
run_root_sql "
create database default2;
create view default2.v_t20_0012 as select * from default.t20_0012_a;
"

echo "select * from default2.v_t20_0012" | $TEST_USER_CONNECT

run_root_sql "
GRANT SELECT ON default2.v_t20_0012 TO 'test-user';
REVOKE SELECT ON default.t20_0012_a FROM 'test-user';
REVOKE SELECT ON default.t20_0012_b FROM 'test-user';
"
echo "select * from default2.v_t20_0012" | $TEST_USER_CONNECT

## select procedure
run_root_sql "select 'test -- clustering_information';"
run_test_user "
select count(*)>=1 from clustering_information('default', 't20_0012_a');
select count(*)>=1 from fuse_snapshot('default', 't20_0012_a');
select count(*)=0 from fuse_segment('default', 't20_0012_a', '');
select count(*)>=1 from fuse_block('default', 't20_0012_a');
"

## Drop table.
run_root_sql "
drop table default.t20_0012 all;
drop table default.t20_0012_a all;
drop table default.t20_0012_b all;
drop view default2.v_t20_0012;
drop database default2;
drop user 'test-user';
"
rm -rf password.out

## Show grants test
export TEST_USER_PASSWORD="password"
export USER_A_CONNECT="bendsql -A --user=a --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_user_a() {
  cat <<SQL | $USER_A_CONNECT
$1
SQL
}

run_root_sql "
drop user if exists a;
create user a identified by '$TEST_USER_PASSWORD';
drop database if exists nogrant;
drop database if exists grant_db;
create database grant_db;
create table grant_db.t(c1 int not null);
create database nogrant;
create table nogrant.t(id int not null);
grant select on default.* to a;
grant select on grant_db.t to a;
drop table if exists default.test_t;
create table default.test_t(id int not null);
"

echo "show grants for a" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "show databases" | $USER_A_CONNECT

echo "select 'test -- show tables from system'" | $BENDSQL_CLIENT_CONNECT
echo "show tables from system" | $USER_A_CONNECT
echo "select 'test -- show tables from grant_db'" | $BENDSQL_CLIENT_CONNECT
echo "show tables from grant_db" | $USER_A_CONNECT
echo "use system" | $USER_A_CONNECT
echo "use grant_db" | $USER_A_CONNECT
echo "select 'test -- show columns from one from system'" | $BENDSQL_CLIENT_CONNECT
run_user_a "
show columns from one from system;
show columns from t from grant_db;
"

run_root_sql "grant select on system.clusters to a;"
echo "show columns from clusters from system" | $USER_A_CONNECT | echo $?
echo "show columns from tasks from system" | $USER_A_CONNECT

### will return err
echo "show columns from tables from system" | $USER_A_CONNECT
echo "show tables from nogrant" | $USER_A_CONNECT

run_user_a "
select count(1) from information_schema.columns where table_schema in ('grant_db');
select count(1) from information_schema.columns where table_schema in ('nogrant');
"

echo "show columns from clusters from system" | $BENDSQL_CLIENT_CONNECT | echo $?
echo "show columns from tasks from system" | $BENDSQL_CLIENT_CONNECT | echo $?

#DML privilege check
export USER_B_CONNECT="bendsql -A --user=b --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_user_b() {
  cat <<SQL | $USER_B_CONNECT
$1
SQL
}

rm -rf /tmp/00_0020
mkdir -p /tmp/00_0020
cat <<EOF >/tmp/00_0020/i0.csv
1
2
EOF

run_root_sql "
drop user if exists b;
create user b identified by '$TEST_USER_PASSWORD';
"

echo "=== Test: show grants on privilege check ==="
run_root_sql "
drop database if exists root_db;
drop table if exists default.root_table;
drop stage if exists root_stage;
drop function if exists root_func;
create database root_db;
create table default.root_table(id int);
create stage root_stage;
create function root_func as (a) -> (a+1);
"

# These need separate execution to show errors
echo "show grants on udf root_func" | $USER_B_CONNECT
echo "show grants on stage root_stage" | $USER_B_CONNECT
echo "show grants on database root_db" | $USER_B_CONNECT
echo "show grants on table default.root_table" | $USER_B_CONNECT
echo "show grants on table root_table" | $USER_B_CONNECT

run_root_sql "
drop database if exists root_db;
drop table if exists default.root_table;
drop stage if exists root_stage;
drop function if exists root_func;
drop table if exists t;
drop table if exists t1;
drop table if exists t2;
drop stage if exists s3;
create table t(id int);
create table t1(id int);
drop role if exists role_test_b;
create role role_test_b;
grant role role_test_b to b;
grant create on default.* to role role_test_b;
alter user b with default_role=role_test_b;
grant insert, delete on default.t to b;
grant select on system.* to b;
create stage s3;
"

echo "copy into '@s3/a b' from (select 2);" | $BENDSQL_CLIENT_CONNECT | $RM_UUID | cut -d$'\t' -f1,2

# need err - these must be separate to show individual errors
echo "insert into t select * from t1" | $USER_B_CONNECT
echo "insert into t select * from @s3" | $USER_B_CONNECT
echo "create table t2 as select * from t" | $USER_B_CONNECT
echo "create table t2 as select * from @s3" | $USER_B_CONNECT
echo "copy into t from (select * from @s3);" | $USER_B_CONNECT | $RM_UUID
echo "replace into t on(id) select * from t1;" | $USER_B_CONNECT

run_root_sql "
grant select on default.t to b;
grant select on default.t1 to b;
grant read on stage s3 to b;
"

run_user_b "
insert into t select * from t1;
insert into t select * from @s3;
create table t2 as select * from t;
"
run_root_sql "drop table t2;"
run_user_b "
create table t2 as select * from @s3;
"
echo "copy into t from (select * from @s3);" | $USER_B_CONNECT | $RM_UUID
echo "replace into t on(id) select * from t1;" | $USER_B_CONNECT

## check after alter table/db name, table id and db id is normal.
echo "=== check db/table_id ==="
run_root_sql "
drop database if exists c;
drop database if exists d;
create database c;
create table c.t (id int);
grant insert, select on c.t to b;
"

echo "show grants for b" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
run_user_b "
insert into c.t values(1);
select * from c.t;
"

run_root_sql "alter table c.t rename to t1;"
echo "show grants for b" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
run_user_b "
insert into c.t1 values(2);
select * from c.t1 order by id;
"

run_root_sql "alter database c rename to d;"
echo "show grants for b" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
run_user_b "
insert into d.t1 values(3);
select * from d.t1 order by id;
"

## Drop user
run_root_sql "
drop database if exists no_grant;
grant drop on d.* to b;
grant drop on *.* to a;
"
echo "drop database grant_db" | $USER_A_CONNECT
echo "drop database d" | $USER_B_CONNECT
run_root_sql "
drop user a;
drop user b;
drop user if exists c;
create user c identified by '123';
grant drop on default.t to c;
"

export USER_C_CONNECT="bendsql -A --user=c --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_user_c() {
  cat <<SQL | $USER_C_CONNECT
$1
SQL
}

# These need separate execution to show errors
echo "drop table if exists t" | $USER_C_CONNECT
echo "drop table if exists unknown_t" | $USER_C_CONNECT
echo "drop database if exists unknown_db" | $USER_C_CONNECT

run_root_sql "
drop table if exists t1;
drop table if exists t2;
drop stage if exists s3;
drop database if exists db01;
"

echo "=== set privilege check ==="
run_root_sql "
drop user if exists c;
create user c identified by '123';
"
export USER_C_CONNECT="bendsql -A --user=c --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_root_sql "
set session max_threads=1000;
unset session max_threads;
"
echo "settings (ddl_column_type_nullable=0) select 100" | $BENDSQL_CLIENT_CONNECT
run_root_sql "
SET variable a = 'a';
set global max_threads=1000;
unset global max_threads;
"

run_user_c "
set session max_threads=1000;
unset session max_threads;
"
echo "settings (ddl_column_type_nullable=0) select 100" | $USER_C_CONNECT
echo "SET variable a = 'a';" | $USER_C_CONNECT
echo "set global max_threads=1000;" | $USER_C_CONNECT 2>&1  | grep "Super" | wc -l
echo "unset global max_threads;" | $USER_C_CONNECT 2>&1  | grep "Super" | wc -l

run_root_sql "
drop user if exists c;
drop role if exists role_test_b;
"
echo "=== set privilege check succ ==="
