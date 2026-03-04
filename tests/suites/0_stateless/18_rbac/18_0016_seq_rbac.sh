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

run_user_a() {
  cat <<SQL | $USER_A_CONNECT
$1
SQL
}

run_user_b() {
  cat <<SQL | $USER_B_CONNECT
$1
SQL
}

run_user_c() {
  cat <<SQL | $USER_C_CONNECT
$1
SQL
}

for seq in $(echo "select name from show_sequences();" | $BENDSQL_CLIENT_CONNECT); do
  echo "drop sequence if exists \`$seq\`;" | $BENDSQL_CLIENT_CONNECT
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

run_user_a "
CREATE sequence seq1;
create sequence seq2;
create sequence seq3;
"
run_user_a "DESC sequence seq1;" | grep seq1 | wc -l
run_user_a "DESC sequence seq2;" | grep seq2 | wc -l
run_user_a "DESC sequence seq3;" | grep seq3 | wc -l
run_user_a "show sequences;" | wc -l
run_user_a "
drop sequence if exists seq1;
drop sequence if exists seq2;
drop sequence if exists seq3;
"

echo "=== NEW LOGIC: user has super privileges can operator all sequences with enable_experimental_sequence_privilege_check=1 ==="
echo "=== TEST USER A WITH SUPER PRIVILEGES ==="
run_user_a "set global enable_experimental_sequence_privilege_check=1;"
echo "--- CREATE 3 sequences WILL SUCCESS ---"
run_user_a "
CREATE sequence seq1;
create sequence seq2;
create sequence seq3;
"
run_user_a "DESC sequence seq1;" | grep seq1 | wc -l
run_user_a "DESC sequence seq2;" | grep seq2 | wc -l
run_user_a "DESC sequence seq3;" | grep seq3 | wc -l
run_user_a "show sequences;" | wc -l
run_user_a "
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
run_root_sql "alter user b with default_role='public';"
echo "CREATE sequence seq1" | $USER_B_CONNECT

run_root_sql "alter user b with default_role='role1';"

echo "--- success, seq1,2,3 owner role is role1 ---"
run_user_b "
CREATE sequence seq1;
create sequence seq2;
create sequence seq3;
"
run_user_b "DESC sequence seq1;" | grep seq1 | wc -l
run_user_b "DESC sequence seq2;" | grep seq2 | wc -l
run_user_b "DESC sequence seq3;" | grep seq3 | wc -l
run_user_b "show sequences;" | wc -l

echo "--- transform seq2'ownership from role1 to role2 ---"
run_root_sql "grant ownership on sequence seq2 to role role2;"
echo "--- USER failed to desc conn seq2, seq2 role is role2 ---"
echo "DESC sequence seq2;" | $USER_B_CONNECT
run_user_b "show sequences;" | wc -l

run_root_sql "grant role role2 to c;"
echo "--- only return one row seq2 ---"
run_user_c "DESC sequence seq2;" | grep seq2 | wc -l
run_user_c "show sequences;" | wc -l
echo "--- grant access sequence seq1 to role3 ---"
run_root_sql "grant access sequence on sequence seq1 to role role3;"
run_root_sql "grant role role3 to c;"
run_user_c "DESC sequence seq1;" | grep seq1 | wc -l
echo "--- grant access sequence seq3 to role3 ---"
run_root_sql "grant access sequence on sequence seq3 to role role3;"
run_user_c "DESC sequence seq3;" | grep seq3 | wc -l
echo "--- return three rows seq1,2,3 ---"
run_user_c "show sequences;" | wc -l

echo "--- user b can not drop sequence seq2 ---"
# These need separate execution to show individual errors
echo "drop sequence if exists seq2;" | $USER_B_CONNECT
run_root_sql "CREATE TABLE tmp_b(a int);"
echo "INSERT INTO tmp_b values(nextval(seq2));" | $USER_B_CONNECT
echo "INSERT INTO tmp_b select nextval(seq2) from numbers(2);" | $USER_B_CONNECT
echo "CREATE TABLE tmp_b1(a int default nextval(seq2));" | $USER_B_CONNECT
echo "show grants on sequence seq2;" | $USER_B_CONNECT

echo "--- revoke access sequence from role3 , thne user c can not drop/use sequence seq1,3 ---"
run_root_sql "
revoke access sequence on sequence seq1 from role role3;
revoke access sequence on sequence seq3 from role role3;
grant select, insert, create on *.* to role role_c;
"
# These need separate execution to show individual errors
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
run_user_b "
INSERT INTO tmp_b values(nextval(seq1));
INSERT INTO tmp_b values(nextval(seq3));
INSERT INTO tmp_b select nextval(seq1) from numbers(2);
INSERT INTO tmp_b select nextval(seq3) from numbers(2);
select * from tmp_b order by a;
CREATE TABLE tmp_b1(a int default nextval(seq1),b int);
CREATE TABLE tmp_b2(a int default nextval(seq3),b int);
insert into tmp_b1(b) values(1);
insert into tmp_b2(b) values(1);
select * from tmp_b1 order by a;
select * from tmp_b2 order by a;
"
run_user_b "show grants on sequence seq1;"
run_user_b "show grants on sequence seq3;"
run_user_b "
drop sequence if exists seq1;
DROP TABLE tmp_b1;
DROP TABLE tmp_b2;
"
run_user_b "show grants for role role1;"
run_user_b "drop sequence if exists seq3;"

echo "--- user c can drop/use sequence seq2 ---"
run_root_sql "truncate table tmp_b;"
run_user_c "
INSERT INTO tmp_b values(nextval(seq2));
INSERT INTO tmp_b select nextval(seq2) from numbers(2);
select * from tmp_b order by a;
CREATE TABLE tmp_b3(a int default nextval(seq2),b int);
insert into tmp_b3(b) values(1);
select * from tmp_b3 order by a;
drop table tmp_b3;
"
run_user_c "show grants for role role2;"
run_user_c "show grants on sequence seq2;"
run_user_c "drop sequence if exists seq2;"
run_user_c "show grants for role role2;"

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
