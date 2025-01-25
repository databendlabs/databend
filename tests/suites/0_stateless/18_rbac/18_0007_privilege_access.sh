#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export TEST_USER_CONNECT="bendsql --user=test-user --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export RM_UUID="sed -E ""s/[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}/UUID/g"""

stmt "drop database if exists db01;"
stmt "create database db01;"
stmt "drop database if exists dbnotexists;"
stmt "GRANT SELECT ON db01.tbnotexists TO 'test-user'"
stmt "GRANT SELECT ON dbnotexists.* TO 'test-user'"

echo "drop user if exists 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists 'test-role1'" | $BENDSQL_CLIENT_CONNECT
echo "drop role if exists 'test-role2'" | $BENDSQL_CLIENT_CONNECT

## create user
echo "create user 'test-user' IDENTIFIED BY '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
## create role
echo 'create role `test-role1`' | $BENDSQL_CLIENT_CONNECT
echo 'create role `test-role2`' | $BENDSQL_CLIENT_CONNECT
## create table
echo "drop table if exists t20_0012" | $BENDSQL_CLIENT_CONNECT
echo "create table t20_0012(c int not null)" | $BENDSQL_CLIENT_CONNECT

echo "=== check list user's local stage ==="
echo "list @~" | $TEST_USER_CONNECT
echo "=== no err ==="

## show tables
echo "show databases" | $TEST_USER_CONNECT

## insert data
echo "select 'test -- insert'" | $TEST_USER_CONNECT
echo "insert into t20_0012 values(1),(2)" | $TEST_USER_CONNECT
## grant user privileges via role
echo 'GRANT INSERT ON * TO ROLE `test-role1`' | $BENDSQL_CLIENT_CONNECT
echo 'GRANT SELECT ON * TO ROLE `test-role2`' | $BENDSQL_CLIENT_CONNECT
echo "GRANT ROLE \`test-role1\` TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "GRANT ROLE \`test-role2\` TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
## insert data
echo "insert into t20_0012 values(1),(2)" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT
## insert overwrite, but no delete privilege
echo "insert overwrite t20_0012 values(3),(4)" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT

## update data
echo "select 'test -- update'" | $TEST_USER_CONNECT
echo "update t20_0012 set c=3 where c=1" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT UPDATE ON * TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
## update data
echo "update t20_0012 set c=3 where c=1" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT

## delete data
echo "select 'test -- delete'" | $TEST_USER_CONNECT
echo "delete from t20_0012 where c=2" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT DELETE ON * TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
## delete data
echo "delete from t20_0012 where c=2" | $TEST_USER_CONNECT
## verify
echo "select count(*) = 0 from t20_0012 where c=2" | $TEST_USER_CONNECT

## insert overwrite
echo "select 'test -- insert overwrite'" | $TEST_USER_CONNECT
# now, user test-user has delete on default.t20_0012, role test-role1 has insert on default
# insert overwrite should pass privilege check.
echo "GRANT DELETE ON default.t20_0012 TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "insert overwrite t20_0012 values(2)" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT
echo "insert overwrite t20_0012 values(3)" | $TEST_USER_CONNECT
## verify
echo "select * from t20_0012 order by c" | $TEST_USER_CONNECT

## optimize table
echo "select 'test -- optimize table'" | $TEST_USER_CONNECT
echo "optimize table t20_0012 all" | $TEST_USER_CONNECT
## grant user privilege
echo "GRANT Super ON *.* TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
## optimize table
echo "set data_retention_time_in_days=0; optimize table t20_0012 all" | $TEST_USER_CONNECT
## verify
echo "select count(*)>=1 from fuse_snapshot('default', 't20_0012')" | $TEST_USER_CONNECT

## select data
echo "select 'test -- select'" | $TEST_USER_CONNECT
## Init tables
echo "CREATE TABLE default.t20_0012_a(c int not null) CLUSTER BY(c)" | $BENDSQL_CLIENT_CONNECT
echo "GRANT INSERT ON default.t20_0012_a TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO default.t20_0012_a values(1)" | $TEST_USER_CONNECT
echo "CREATE TABLE default.t20_0012_b(c int not null)" | $BENDSQL_CLIENT_CONNECT
echo "GRANT INSERT ON default.t20_0012_b TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO default.t20_0012_b values(1)" | $TEST_USER_CONNECT
## Init privilege
echo "REVOKE SELECT ON * FROM 'test-user'" | $BENDSQL_CLIENT_CONNECT
## Verify table privilege separately
echo "select * from default.t20_0012_a order by c" | $TEST_USER_CONNECT
echo "GRANT SELECT ON default.t20_0012_a TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "select * from default.t20_0012_a order by c" | $TEST_USER_CONNECT
echo "select * from default.t20_0012_b order by c" | $TEST_USER_CONNECT
echo "GRANT SELECT ON default.t20_0012_b TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "select * from default.t20_0012_b order by c" | $TEST_USER_CONNECT

## Create view table
## View is not covered with ownership yet, the privilge checks is bound to the database
## the view table is created in.
echo "select 'test -- select view'" | $TEST_USER_CONNECT
echo "create database default2" | $BENDSQL_CLIENT_CONNECT

echo "create view default2.v_t20_0012 as select * from default.t20_0012_a" | $BENDSQL_CLIENT_CONNECT
echo "select * from default2.v_t20_0012" | $TEST_USER_CONNECT
## Only grant privilege for view table, now this user can access the view under default2 db,
## but can not access the tables under the `default` database, stil raises permission error
## on SELECT default2.v_t20_0012
echo "GRANT SELECT ON default2.v_t20_0012 TO 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "REVOKE SELECT ON default.t20_0012_a FROM 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "REVOKE SELECT ON default.t20_0012_b FROM 'test-user'" | $BENDSQL_CLIENT_CONNECT
echo "select * from default2.v_t20_0012" | $TEST_USER_CONNECT

## select procedure
## clustering_information
echo "select 'test -- clustering_information'" | $BENDSQL_CLIENT_CONNECT
echo "select count(*)>=1 from clustering_information('default', 't20_0012_a')" | $TEST_USER_CONNECT
## fuse_snapshot
echo "select count(*)>=1 from fuse_snapshot('default', 't20_0012_a')" | $TEST_USER_CONNECT
## fuse_segment
echo "select count(*)=0 from fuse_segment('default', 't20_0012_a', '')" | $TEST_USER_CONNECT
## fuse_block
echo "select count(*)>=1 from fuse_block('default', 't20_0012_a')" | $TEST_USER_CONNECT

## Drop table.
echo "drop table default.t20_0012 all" | $BENDSQL_CLIENT_CONNECT
echo "drop table default.t20_0012_a all" | $BENDSQL_CLIENT_CONNECT
echo "drop table default.t20_0012_b all" | $BENDSQL_CLIENT_CONNECT
echo "drop view default2.v_t20_0012" | $BENDSQL_CLIENT_CONNECT

## Drop database.
echo "drop database default2" | $BENDSQL_CLIENT_CONNECT

## Drop user
echo "drop user 'test-user'" | $BENDSQL_CLIENT_CONNECT
rm -rf password.out

## Show grants test
export TEST_USER_PASSWORD="password"
export USER_A_CONNECT="bendsql --user=a --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "drop user if exists a" | $BENDSQL_CLIENT_CONNECT
echo "create user a identified by '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists nogrant" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists grant_db" | $BENDSQL_CLIENT_CONNECT
echo "create database grant_db" | $BENDSQL_CLIENT_CONNECT
echo "create table grant_db.t(c1 int not null)" | $BENDSQL_CLIENT_CONNECT
echo "create database nogrant" | $BENDSQL_CLIENT_CONNECT
echo "create table nogrant.t(id int not null)" | $BENDSQL_CLIENT_CONNECT
echo "grant select on default.* to a" | $BENDSQL_CLIENT_CONNECT
echo "grant select on grant_db.t to a" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.test_t" | $BENDSQL_CLIENT_CONNECT
echo "create table default.test_t(id int not null)" | $BENDSQL_CLIENT_CONNECT
echo "show grants for a" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "show databases" | $USER_A_CONNECT
echo "select 'test -- show tables from system'" | $BENDSQL_CLIENT_CONNECT
echo "show tables from system" | $USER_A_CONNECT
echo "select 'test -- show tables from grant_db'" | $BENDSQL_CLIENT_CONNECT
echo "show tables from grant_db" | $USER_A_CONNECT
echo "use system" | $USER_A_CONNECT
echo "use grant_db" | $USER_A_CONNECT
echo "select 'test -- show columns from one from system'" | $BENDSQL_CLIENT_CONNECT
echo "show columns from one from system" | $USER_A_CONNECT
echo "show columns from t from grant_db" | $USER_A_CONNECT
echo "grant select on system.clusters to a" | $BENDSQL_CLIENT_CONNECT
echo "show columns from clusters from system" | $USER_A_CONNECT | echo $?
echo "show columns from tasks from system" | $USER_A_CONNECT

### will return err
echo "show columns from tables from system" | $USER_A_CONNECT
echo "show tables from nogrant" | $USER_A_CONNECT

echo "select count(1) from information_schema.columns where table_schema in ('grant_db');" | $USER_A_CONNECT
echo "select count(1) from information_schema.columns where table_schema in ('nogrant');" | $USER_A_CONNECT

echo "show columns from clusters from system" | $BENDSQL_CLIENT_CONNECT | echo $?
echo "show columns from tasks from system" | $BENDSQL_CLIENT_CONNECT | echo $?

#DML privilege check
export USER_B_CONNECT="bendsql --user=b --password=password --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

rm -rf /tmp/00_0020
mkdir -p /tmp/00_0020
cat <<EOF >/tmp/00_0020/i0.csv
1
2
EOF

echo "drop user if exists b" | $BENDSQL_CLIENT_CONNECT
echo "create user b identified by '$TEST_USER_PASSWORD'" | $BENDSQL_CLIENT_CONNECT

echo "=== Test: show grants on privilege check ==="
echo "drop database if exists root_db" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.root_table" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists root_stage" | $BENDSQL_CLIENT_CONNECT
echo "drop function if exists root_func" | $BENDSQL_CLIENT_CONNECT
echo "create database root_db" | $BENDSQL_CLIENT_CONNECT
echo "create table default.root_table(id int)" | $BENDSQL_CLIENT_CONNECT
echo "create stage root_stage" | $BENDSQL_CLIENT_CONNECT
echo "create function root_func as (a) -> (a+1);" | $BENDSQL_CLIENT_CONNECT
echo "show grants on udf root_func" | $USER_B_CONNECT
echo "show grants on stage root_stage" | $USER_B_CONNECT
echo "show grants on database root_db" | $USER_B_CONNECT
echo "show grants on table default.root_table" | $USER_B_CONNECT
echo "show grants on table root_table" | $USER_B_CONNECT
echo "drop database if exists root_db" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists default.root_table" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists root_stage" | $BENDSQL_CLIENT_CONNECT
echo "drop function if exists root_func" | $BENDSQL_CLIENT_CONNECT

echo "drop table if exists t" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists t1" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists t2" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s3;" | $BENDSQL_CLIENT_CONNECT

echo "create table t(id int)" | $BENDSQL_CLIENT_CONNECT
echo "create table t1(id int)" | $BENDSQL_CLIENT_CONNECT
echo "grant create on default.* to b" | $BENDSQL_CLIENT_CONNECT
echo "grant insert, delete on default.t to b" | $BENDSQL_CLIENT_CONNECT
echo "grant select on system.* to b" | $BENDSQL_CLIENT_CONNECT

echo "create stage s3;" | $BENDSQL_CLIENT_CONNECT
echo "copy into '@s3/a b' from (select 2);" | $BENDSQL_CLIENT_CONNECT | $RM_UUID

# need err
echo "insert into t select * from t1" | $USER_B_CONNECT
echo "insert into t select * from @s3" | $USER_B_CONNECT
echo "create table t2 as select * from t" | $USER_B_CONNECT
echo "create table t2 as select * from @s3" | $USER_B_CONNECT
echo "copy into t from (select * from @s3);" | $USER_B_CONNECT | $RM_UUID
echo "replace into t on(id) select * from t1;" | $USER_B_CONNECT

echo "grant select on default.t to b" | $BENDSQL_CLIENT_CONNECT
echo "grant select on default.t1 to b" | $BENDSQL_CLIENT_CONNECT
echo "grant read on stage s3 to b" | $BENDSQL_CLIENT_CONNECT

echo "insert into t select * from t1" | $USER_B_CONNECT
echo "insert into t select * from @s3" | $USER_B_CONNECT
echo "create table t2 as select * from t" | $USER_B_CONNECT
echo "drop table t2" | $BENDSQL_CLIENT_CONNECT
echo "create table t2 as select * from @s3" | $USER_B_CONNECT
echo "copy into t from (select * from @s3);" | $USER_B_CONNECT | $RM_UUID
echo "replace into t on(id) select * from t1;" | $USER_B_CONNECT

## check after alter table/db name, table id and db id is normal.
echo "=== check db/table_id ==="
echo "drop database if exists c;" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists d;" | $BENDSQL_CLIENT_CONNECT

echo "create database c;" | $BENDSQL_CLIENT_CONNECT
echo "create table c.t (id int);" | $BENDSQL_CLIENT_CONNECT
echo "grant insert, select on c.t to b" | $BENDSQL_CLIENT_CONNECT
echo "show grants for b" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "insert into c.t values(1)" | $USER_B_CONNECT
echo "select * from c.t" | $USER_B_CONNECT

echo "alter table c.t rename to t1" | $BENDSQL_CLIENT_CONNECT
echo "show grants for b" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "insert into c.t1 values(2)" | $USER_B_CONNECT
echo "select * from c.t1 order by id" | $USER_B_CONNECT

echo "alter database c rename to d" | $BENDSQL_CLIENT_CONNECT
echo "show grants for b" | $BENDSQL_CLIENT_CONNECT | awk -F ' ' '{$3=""; print $0}'
echo "insert into d.t1 values(3)" | $USER_B_CONNECT
echo "select * from d.t1 order by id" | $USER_B_CONNECT

## Drop user
echo "drop database if exists no_grant" | $BENDSQL_CLIENT_CONNECT
echo "grant drop on d.* to b" | $BENDSQL_CLIENT_CONNECT
echo "grant drop on *.* to a" | $BENDSQL_CLIENT_CONNECT
echo "drop database grant_db" | $USER_A_CONNECT
echo "drop database d" | $USER_B_CONNECT
echo "drop user a" | $BENDSQL_CLIENT_CONNECT
echo "drop user b" | $BENDSQL_CLIENT_CONNECT

echo "drop user if exists c" | $BENDSQL_CLIENT_CONNECT
echo "create user c identified by '123'" | $BENDSQL_CLIENT_CONNECT
echo "grant drop on default.t to c" | $BENDSQL_CLIENT_CONNECT
export USER_C_CONNECT="bendsql --user=c --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

echo "drop table if exists t" | $USER_C_CONNECT
echo "drop table if exists unknown_t" | $USER_C_CONNECT
echo "drop database if exists unknown_db" | $USER_C_CONNECT

echo "drop table if exists t1" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists t2" | $BENDSQL_CLIENT_CONNECT
echo "drop stage if exists s3;" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db01" | $BENDSQL_CLIENT_CONNECT

echo "=== set privilege check ==="
echo "drop user if exists c" | $BENDSQL_CLIENT_CONNECT
echo "create user c identified by '123'" | $BENDSQL_CLIENT_CONNECT
export USER_C_CONNECT="bendsql --user=c --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
echo "set session max_threads=1000" | $BENDSQL_CLIENT_CONNECT
echo "unset session max_threads" | $BENDSQL_CLIENT_CONNECT
echo "settings (ddl_column_type_nullable=0) select 100" | $BENDSQL_CLIENT_CONNECT
echo "SET variable a = 'a';" | $BENDSQL_CLIENT_CONNECT
echo "set global max_threads=1000" | $BENDSQL_CLIENT_CONNECT
echo "unset global max_threads" | $BENDSQL_CLIENT_CONNECT

echo "set session max_threads=1000" | $USER_C_CONNECT
echo "unset session max_threads" | $USER_C_CONNECT
echo "settings (ddl_column_type_nullable=0) select 100" | $USER_C_CONNECT
echo "SET variable a = 'a';" | $USER_C_CONNECT
echo "set global max_threads=1000;" | $USER_C_CONNECT 2>&1  | grep "Super" | wc -l
echo "unset global max_threads;" | $USER_C_CONNECT 2>&1  | grep "Super" | wc -l
echo "drop user if exists c" | $BENDSQL_CLIENT_CONNECT
echo "=== set privilege check succ ==="
