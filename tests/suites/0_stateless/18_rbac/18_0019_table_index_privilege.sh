#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TEST_USER_PASSWORD="password"
export ALTER_USER_CONNECT="bendsql_connect_user idx_alter password -A"
export TABLE_OWNER_CONNECT="bendsql_connect_user idx_table_owner password -A"
export DB_OWNER_CONNECT="bendsql_connect_user idx_db_owner password -A"
export SUPER_USER_CONNECT="bendsql_connect_user idx_super password -A"
export DROP_USER_CONNECT="bendsql_connect_user idx_drop password -A"
export DENIED_USER_CONNECT="bendsql_connect_user idx_denied password -A"

run_user_alter() {
	cat <<SQL | $ALTER_USER_CONNECT
$1
SQL
}

run_table_owner() {
	cat <<SQL | $TABLE_OWNER_CONNECT
$1
SQL
}

run_db_owner() {
	cat <<SQL | $DB_OWNER_CONNECT
$1
SQL
}

run_user_super() {
	cat <<SQL | $SUPER_USER_CONNECT
$1
SQL
}

run_user_drop() {
	cat <<SQL | $DROP_USER_CONNECT
$1
SQL
}

run_root_sql "
drop user if exists idx_alter;
drop user if exists idx_table_owner;
drop user if exists idx_db_owner;
drop user if exists idx_super;
drop user if exists idx_drop;
drop user if exists idx_denied;
drop role if exists idx_table_owner_role;
drop role if exists idx_db_owner_role;
drop database if exists idx_priv;

create user idx_alter identified by '$TEST_USER_PASSWORD';
create user idx_table_owner identified by '$TEST_USER_PASSWORD';
create user idx_db_owner identified by '$TEST_USER_PASSWORD';
create user idx_super identified by '$TEST_USER_PASSWORD';
create user idx_drop identified by '$TEST_USER_PASSWORD';
create user idx_denied identified by '$TEST_USER_PASSWORD';
create role idx_table_owner_role;
create role idx_db_owner_role;

create database idx_priv;
create table idx_priv.t_alter(id int, content string);
create table idx_priv.t_table_owner(id int, content string);
create table idx_priv.t_db_owner_vec(id int, embedding vector(4));
create table idx_priv.t_super(id int, content string);
create table idx_priv.t_global_drop(id int, content string);
create table idx_priv.t_denied(id int, content string);
create table idx_priv.numbers(id int, content string);
create ngram index idx_ngram_global_drop on idx_priv.t_global_drop(content) gram_size = 2 bloom_size = 1048576;

grant alter on idx_priv.t_alter to idx_alter;
grant ownership on idx_priv.t_table_owner to role idx_table_owner_role;
grant role idx_table_owner_role to idx_table_owner;
alter user idx_table_owner with default_role = 'idx_table_owner_role';
grant super on *.* to idx_super;
grant drop on *.* to idx_drop;
grant ownership on idx_priv.* to role idx_db_owner_role;
grant role idx_db_owner_role to idx_db_owner;
alter user idx_db_owner with default_role = 'idx_db_owner_role';
"

echo "=== denied table index ==="
echo "CREATE NGRAM INDEX idx_denied ON idx_priv.t_denied(content) gram_size = 2 bloom_size = 1048576" |
	$DENIED_USER_CONNECT 2>&1 |
	grep 'privilege \[Alter\]' |
	wc -l

echo "=== table function name collision ==="
echo "CREATE NGRAM INDEX idx_numbers ON idx_priv.numbers(content) gram_size = 2 bloom_size = 1048576" |
	$DENIED_USER_CONNECT 2>&1 |
	grep 'privilege \[Alter\]' |
	wc -l

echo "=== alter ngram index ==="
run_user_alter "
CREATE NGRAM INDEX idx_ngram_alter ON idx_priv.t_alter(content) gram_size = 2 bloom_size = 1048576;
DROP NGRAM INDEX idx_ngram_alter ON idx_priv.t_alter;
"
echo "alter ok"

echo "=== table owner inverted index ==="
run_table_owner "
CREATE INVERTED INDEX idx_inv_table_owner ON idx_priv.t_table_owner(content);
REFRESH INVERTED INDEX idx_inv_table_owner ON idx_priv.t_table_owner;
DROP INVERTED INDEX idx_inv_table_owner ON idx_priv.t_table_owner;
"
echo "table owner ok"

echo "=== db owner vector index ==="
run_db_owner "
CREATE VECTOR INDEX idx_vec_db_owner ON idx_priv.t_db_owner_vec(embedding) m=10 ef_construct=40 distance='cosine';
DROP VECTOR INDEX idx_vec_db_owner ON idx_priv.t_db_owner_vec;
"
echo "db owner ok"

echo "=== super table index ==="
run_user_super "
CREATE NGRAM INDEX idx_ngram_super ON idx_priv.t_super(content) gram_size = 2 bloom_size = 1048576;
DROP NGRAM INDEX idx_ngram_super ON idx_priv.t_super;
"
echo "super ok"

echo "=== global drop table index ==="
run_user_drop "
DROP NGRAM INDEX idx_ngram_global_drop ON idx_priv.t_global_drop;
"
echo "global drop ok"

echo "=== system_history hard deny for super ==="
echo "REFRESH NGRAM INDEX idx_priv_sys ON system_history.idx_priv_sys" |
	$SUPER_USER_CONNECT 2>&1 |
	grep 'sensitive system resource' |
	wc -l

run_root_sql "
drop user if exists idx_alter;
drop user if exists idx_table_owner;
drop user if exists idx_db_owner;
drop user if exists idx_super;
drop user if exists idx_drop;
drop user if exists idx_denied;
drop role if exists idx_table_owner_role;
drop role if exists idx_db_owner_role;
drop database if exists idx_priv;
"
