#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_root_sql() {
  cat <<SQL | $BENDSQL_CLIENT_CONNECT
$1
SQL
}

export USER_A_CONNECT="bendsql -A --user=a --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_B_CONNECT="bendsql -A --user=b --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_C_CONNECT="bendsql -A --user=c --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

run_user_sql() {
  local client="$1"
  shift
  cat <<SQL | eval "$client"
$@
SQL
}

for seq in $(echo "select name from show_sequences();" | $BENDSQL_CLIENT_CONNECT); do
  echo "drop sequence if exists `$seq`;" | $BENDSQL_CLIENT_CONNECT
done

echo "=== OLD LOGIC: user has super privileges can operator all sequences with enable_experimental_sequence_privilege_check=0 ==="
echo "=== TEST USER A WITH SUPER PRIVILEGES ==="
run_root_sql "
set global enable_experimental_sequence_privilege_check=0;
drop sequence if exists seq1;
drop sequence if exists seq2;
drop sequence if exists seq3;
drop role if exists role1;
drop role if exists role2;
drop user if exists a;
drop user if exists b;
drop user if exists c;
create user a identified by '123' with default_role='role_a';
create user b identified by '123' with default_role='role_b';
create user c identified by '123' with default_role='role_c';
drop role if exists role_a;
drop role if exists role_b;
drop role if exists role_c;
create role if not exists role_a;
create role if not exists role_b;
create role if not exists role_c;
grant role role_a to a;
grant role role_b to b;
grant role role_c to c;
grant super on *.* to role role_a;
grant select, insert, create on *.* to role role_b;
grant select, insert, create on *.* to role role_c;
drop table if exists tmp_b;
drop table if exists tmp_b1;
drop table if exists tmp_b2;
drop table if exists tmp_b3;
"

run_user_sql "$USER_A_CONNECT" "
CREATE sequence seq1;
create sequence seq2;
create sequence seq3;
"
echo "DESC sequence seq1;" | $USER_A_CONNECT | grep seq1 | wc -l
echo "DESC sequence seq2;" | $USER_A_CONNECT | grep seq2 | wc -l
echo "DESC sequence seq3;" | $USER_A_CONNECT | grep seq3 | wc -l
echo "show sequences;" | $USER_A_CONNECT | wc -l
run_user_sql "$USER_A_CONNECT" "
drop sequence if exists seq1;
drop sequence if exists seq2;
drop sequence if exists seq3;
"


echo "=== NEW LOGIC: user has super privileges can operator all sequences with enable_experimental_sequence_privilege_check=1 ==="
echo "=== TEST USER A WITH SUPER PRIVILEGES ==="
echo "set global enable_experimental_sequence_privilege_check=1;" | $USER_A_CONNECT
echo "--- CREATE 3 sequences WILL SUCCESS ---"
run_user_sql "$USER_A_CONNECT" "
CREATE sequence seq1;
create sequence seq2;
create sequence seq3;
"
echo "DESC sequence seq1;" | $USER_A_CONNECT | grep seq1 | wc -l
echo "DESC sequence seq2;" | $USER_A_CONNECT | grep seq2 | wc -l
echo "DESC sequence seq3;" | $USER_A_CONNECT | grep seq3 | wc -l
echo "show sequences;" | $USER_A_CONNECT | wc -l
run_user_sql "$USER_A_CONNECT" "
drop sequence if exists seq1;
drop sequence if exists seq2;
drop sequence if exists seq3;
"

echo "=== TEST USER B, C WITH OWNERSHIP OR CREATE/ACCESS SEQUENCES PRIVILEGES ==="

run_root_sql "
drop role if exists role1;
drop role if exists role2;
drop role if exists role3;
create role role1;
create role role2;
create role role3;
grant create sequence, create on *.* to role role1;
grant role role1 to b;
"
echo "--- USER b failed to create conn seq1 because current role is public, can not create ---"
echo "alter user b with default_role='public';" | $BENDSQL_CLIENT_CONNECT
echo "CREATE sequence seq1" | $USER_B_CONNECT

echo "alter user b with default_role='role1';" | $BENDSQL_CLIENT_CONNECT

echo "--- success, seq1,2,3 owner role is role1 ---";
run_user_sql "$USER_B_CONNECT" "
CREATE sequence seq1;
create sequence seq2;
create sequence seq3;
"
echo "DESC sequence seq1;" | $USER_B_CONNECT | grep seq1 | wc -l
echo "DESC sequence seq2;" | $USER_B_CONNECT | grep seq2 | wc -l
echo "DESC sequence seq3;" | $USER_B_CONNECT | grep seq3 | wc -l
echo "show sequences;" | $USER_B_CONNECT | wc -l

echo "--- transform seq2'ownership from role1 to role2 ---"
echo "grant ownership on sequence seq2 to role role2;" | $BENDSQL_CLIENT_CONNECT
echo "--- USER failed to desc conn seq2, seq2 role is role2 ---"
echo "DESC sequence seq2;" | $USER_B_CONNECT
echo "show sequences;" | $USER_B_CONNECT | wc -l

echo "grant role role2 to c;" | $BENDSQL_CLIENT_CONNECT
echo "--- only return one row seq2 ---"
echo "DESC sequence seq2;" | $USER_C_CONNECT | grep seq2 | wc -l
echo "show sequences;" | $USER_C_CONNECT | wc -l
echo "--- grant access sequence seq1 to role3 ---"
run_root_sql "
grant access sequence on sequence seq1 to role role3;
grant role role3 to c;
"
echo "DESC sequence seq1;" | $USER_C_CONNECT | grep seq1 | wc -l
echo "--- grant access sequence seq3 to role3 ---"
echo "grant access sequence on sequence seq3 to role role3;" | $BENDSQL_CLIENT_CONNECT
echo "DESC sequence seq3;" | $USER_C_CONNECT | grep seq3 | wc -l
echo "--- return three rows seq1,2,3 ---"
echo "show sequences;" | $USER_C_CONNECT | wc -l

echo "--- user b can not drop sequence seq2 ---"
echo "drop sequence if exists seq2;" | $USER_B_CONNECT
run_root_sql "
CREATE TABLE tmp_b(a int);
"
run_user_sql "$USER_B_CONNECT" "
INSERT INTO tmp_b values(nextval(seq2));
INSERT INTO tmp_b select nextval(seq2) from numbers(2);
"
echo "CREATE TABLE tmp_b1(a int default nextval(seq2));" | $USER_B_CONNECT
echo "show grants on sequence seq2;" | $USER_B_CONNECT

echo "--- revoke access sequence from role3 , thne user c can not drop/use sequence seq1,3 ---"
run_root_sql "
revoke access sequence on sequence seq1 from role role3;
revoke access sequence on sequence seq3 from role role3;
grant select, insert, create on *.* to role role_c;
"
echo "INSERT INTO tmp_b values(nextval(seq1));" | $USER_C_CONNECT
echo "INSERT INTO tmp_b values(nextval(seq3));" | $USER_C_CONNECT
echo "INSERT INTO tmp_b select nextval(seq1) from numbers(2);" | $USER_C_CONNECT
echo "INSERT INTO tmp_b select nextval(seq3) from numbers(2);" | $USER_C_CONNECT
echo "CREATE TABLE tmp_b1(a int default nextval(seq1));" | $USER_C_CONNECT
echo "CREATE TABLE tmp_b1(a int default nextval(seq3));" | $USER_C_CONNECT
echo "show grants on sequence seq1;" | $USER_C_CONNECT
echo "show grants on sequence seq3;" | $USER_C_CONNECT
echo "drop sequence if exists seq1;" | $USER_C_CONNECT
echo "drop sequence if exists seq3;" | $USER_C_CONNECT

echo "--- user b can drop/use sequence seq1,3 ---"
run_user_sql "$USER_B_CONNECT" "
INSERT INTO tmp_b values(nextval(seq1));
INSERT INTO tmp_b values(nextval(seq3));
INSERT INTO tmp_b select nextval(seq1) from numbers(2);
INSERT INTO tmp_b select nextval(seq3) from numbers(2);
"
echo "select * from tmp_b order by a;" | $USER_B_CONNECT
run_user_sql "$USER_B_CONNECT" "
CREATE TABLE tmp_b1(a int default nextval(seq1),b int);
CREATE TABLE tmp_b2(a int default nextval(seq3),b int);
"
run_user_sql "$USER_B_CONNECT" "
insert into tmp_b1(b) values(1);
insert into tmp_b2(b) values(1);
"
echo "select * from tmp_b1 order by a" | $USER_B_CONNECT
echo "select * from tmp_b2 order by a" | $USER_B_CONNECT
echo "show grants on sequence seq1;" | $USER_B_CONNECT
echo "show grants on sequence seq3;" | $USER_B_CONNECT
run_user_sql "$USER_B_CONNECT" "
drop sequence if exists seq1;
DROP TABLE tmp_b1;
DROP TABLE tmp_b2;
"
echo "show grants for role role1;" | $USER_B_CONNECT
echo "drop sequence if exists seq3;" | $USER_B_CONNECT

echo "--- user c can drop/use sequence seq2 ---"
echo "truncate table tmp_b" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO tmp_b values(nextval(seq2));" | $USER_C_CONNECT
echo "INSERT INTO tmp_b select nextval(seq2) from numbers(2);" | $USER_C_CONNECT
echo "select * from tmp_b order by a;" | $USER_C_CONNECT
echo "CREATE TABLE tmp_b3(a int default nextval(seq2),b int);" | $USER_C_CONNECT
echo "insert into tmp_b3(b) values(1)" | $USER_C_CONNECT
echo "select * from tmp_b3 order by a" | $USER_C_CONNECT
echo "drop table tmp_b3" | $USER_C_CONNECT
echo "show grants for role role2;" | $USER_C_CONNECT
echo "show grants on sequence seq2;" | $USER_C_CONNECT
echo "drop sequence if exists seq2;" | $USER_C_CONNECT
echo "show grants for role role2;" | $USER_C_CONNECT

run_root_sql "
drop user if exists a;
drop user if exists b;
drop user if exists c;
drop sequence if exists seq1;
drop sequence if exists seq2;
drop sequence if exists seq3;
drop role if exists role1;
drop role if exists role2;
drop role if exists role3;
drop table if exists tmp_b;
unset global enable_experimental_sequence_privilege_check;
"
