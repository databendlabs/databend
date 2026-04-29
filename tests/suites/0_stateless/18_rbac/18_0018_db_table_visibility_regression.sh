#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

API_BASE="http://${QUERY_MYSQL_HANDLER_HOST}:${QUERY_HTTP_HANDLER_PORT}/v1/catalog"

export USER_TABLE_CONNECT="bendsql -A --user=u_table_0018 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_DB_CONNECT="bendsql -A --user=u_db_0018 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_GRANT_CONNECT="bendsql -A --user=u_grant_0018 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_DIRECT_CONNECT="bendsql -A --user=u_direct_0018 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"
export USER_INHERITED_CONNECT="bendsql -A --user=u_inherited_0018 --password=123 --host=${QUERY_MYSQL_HANDLER_HOST} --port ${QUERY_HTTP_HANDLER_PORT}"

api_get_databases() {
	local user=$1
	curl -s -u "${user}:123" "${API_BASE}/databases" |
		jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
		sort
}

api_get_tables() {
	local user=$1
	local db=$2
	curl -s -u "${user}:123" "${API_BASE}/databases/${db}/tables" |
		jq -r '.tables[].name' |
		sort
}

api_search_databases() {
	local user=$1
	curl -s -u "${user}:123" -XPOST "${API_BASE}/search/databases" \
		-H 'Content-Type: application/json' \
		-d '{"keyword":"db_"}' |
		jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
		sort
}

api_search_tables() {
	local user=$1
	curl -s -u "${user}:123" -XPOST "${API_BASE}/search/tables" \
		-H 'Content-Type: application/json' \
		-d '{"keyword":"case_0018"}' |
		jq -r '.tables[] | select(.database | contains("_0018")) | "\(.database).\(.name)"' |
		sort
}

api_tables_status() {
	local user=$1
	local db=$2
	curl -s -u "${user}:123" -o /dev/null -w "%{http_code}\n" \
		"${API_BASE}/databases/${db}/tables"
}

api_visible_table_count() {
	local user=$1
	curl -s -u "${user}:123" "${API_BASE}/stats" | jq -r '.tables'
}

sql_get_databases() {
	local user=$1
	bendsql -A --user="${user}" --password=123 --host="${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_HTTP_HANDLER_PORT}" -q \
		"select name from system.databases where name like 'db_%_0018' order by name"
}

sql_get_tables() {
	local user=$1
	local db=$2
	bendsql -A --user="${user}" --password=123 --host="${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_HTTP_HANDLER_PORT}" -q \
		"select name from system.tables where database = '${db}' order by name"
}

sql_get_exact_table() {
	local user=$1
	local db=$2
	local table=$3
	bendsql -A --user="${user}" --password=123 --host="${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_HTTP_HANDLER_PORT}" -q \
		"select name from system.tables where database = '${db}' and name = '${table}'"
}

sql_count_databases() {
	local user=$1
	local db=$2
	bendsql -A --user="${user}" --password=123 --host="${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_HTTP_HANDLER_PORT}" -q \
		"select count() from system.databases where name = '${db}'"
}

sql_count_tables() {
	local user=$1
	local db=$2
	bendsql -A --user="${user}" --password=123 --host="${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_HTTP_HANDLER_PORT}" -q \
		"select count() from system.tables where database = '${db}'"
}

sql_count_exact_table() {
	local user=$1
	local db=$2
	local table=$3
	bendsql -A --user="${user}" --password=123 --host="${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_HTTP_HANDLER_PORT}" -q \
		"select count() from system.tables where database = '${db}' and name = '${table}'"
}

run_root_sql "
drop user if exists u_table_0018;
drop user if exists u_db_0018;
drop user if exists u_grant_0018;
drop user if exists u_direct_0018;
drop user if exists u_inherited_0018;
drop user if exists u_dropped_db_0018;
drop role if exists role_table_only_0018;
drop role if exists role_db_owner_0018;
drop role if exists role_grant_0018;
drop role if exists role_parent_0018;
drop role if exists role_child_0018;
drop role if exists role_dropped_db_owner_0018;
drop database if exists db_table_only_0018;
drop database if exists db_db_owner_0018;
drop database if exists db_grant_0018;
drop database if exists db_direct_0018;
drop database if exists db_inherited_0018;
drop database if exists db_dropped_db_0018;

create database db_table_only_0018;
create table db_table_only_0018.owned_case_0018(id int);
create table db_table_only_0018.hidden_case_0018(id int);
insert into db_table_only_0018.owned_case_0018 values (1);
insert into db_table_only_0018.hidden_case_0018 values (2);

create database db_db_owner_0018;
create table db_db_owner_0018.all_case_a_0018(id int);
create table db_db_owner_0018.all_case_b_0018(id int);
insert into db_db_owner_0018.all_case_a_0018 values (10);
insert into db_db_owner_0018.all_case_b_0018 values (20);

create database db_grant_0018;
create table db_grant_0018.grant_case_0018(id int);
create table db_grant_0018.hidden_grant_0018(id int);
insert into db_grant_0018.grant_case_0018 values (100);
insert into db_grant_0018.hidden_grant_0018 values (200);

create database db_direct_0018;
create table db_direct_0018.direct_case_0018(id int);
create table db_direct_0018.hidden_direct_0018(id int);
insert into db_direct_0018.direct_case_0018 values (1000);
insert into db_direct_0018.hidden_direct_0018 values (2000);

create database db_inherited_0018;
create table db_inherited_0018.inherited_case_0018(id int);
create table db_inherited_0018.hidden_inherited_0018(id int);
insert into db_inherited_0018.inherited_case_0018 values (10000);
insert into db_inherited_0018.hidden_inherited_0018 values (20000);

create database db_dropped_db_0018;
create table db_dropped_db_0018.dropped_case_0018(id int);
create table db_dropped_db_0018.hidden_dropped_0018(id int);
insert into db_dropped_db_0018.dropped_case_0018 values (100000);
insert into db_dropped_db_0018.hidden_dropped_0018 values (200000);

create role role_table_only_0018;
create role role_db_owner_0018;
create role role_grant_0018;
create role role_parent_0018;
create role role_child_0018;
create role role_dropped_db_owner_0018;

grant ownership on db_table_only_0018.owned_case_0018 to role role_table_only_0018;
grant ownership on db_db_owner_0018.* to role role_db_owner_0018;
grant select on db_grant_0018.grant_case_0018 to role role_grant_0018;
grant select on db_inherited_0018.inherited_case_0018 to role role_child_0018;
grant ownership on db_dropped_db_0018.* to role role_dropped_db_owner_0018;
grant role role_child_0018 to role role_parent_0018;

create user u_table_0018 identified by '123' with default_role='role_table_only_0018';
create user u_db_0018 identified by '123' with default_role='role_db_owner_0018';
create user u_grant_0018 identified by '123' with default_role='role_grant_0018';
create user u_direct_0018 identified by '123';
create user u_inherited_0018 identified by '123' with default_role='role_parent_0018';
create user u_dropped_db_0018 identified by '123' with default_role='role_dropped_db_owner_0018';
grant select on db_direct_0018.direct_case_0018 to u_direct_0018;

grant role role_table_only_0018 to u_table_0018;
grant role role_db_owner_0018 to u_db_0018;
grant role role_grant_0018 to u_grant_0018;
grant role role_parent_0018 to u_inherited_0018;
grant role role_dropped_db_owner_0018 to u_dropped_db_0018;
"

echo "=== http table owner ==="
api_get_databases "u_table_0018"
api_get_tables "u_table_0018" "db_table_only_0018"
api_tables_status "u_table_0018" "db_grant_0018"
api_search_databases "u_table_0018"
api_search_tables "u_table_0018"
api_visible_table_count "u_table_0018"

echo "=== http db owner ==="
api_get_databases "u_db_0018"
api_get_tables "u_db_0018" "db_db_owner_0018"
api_search_databases "u_db_0018"
api_search_tables "u_db_0018"
api_visible_table_count "u_db_0018"

echo "=== http grant user ==="
api_get_databases "u_grant_0018"
api_get_tables "u_grant_0018" "db_grant_0018"
api_search_databases "u_grant_0018"
api_search_tables "u_grant_0018"
api_visible_table_count "u_grant_0018"

echo "=== http direct user grant ==="
api_get_databases "u_direct_0018"
api_get_tables "u_direct_0018" "db_direct_0018"
api_search_databases "u_direct_0018"
api_search_tables "u_direct_0018"
api_visible_table_count "u_direct_0018"

echo "=== http inherited role grant ==="
api_get_databases "u_inherited_0018"
api_get_tables "u_inherited_0018" "db_inherited_0018"
api_search_databases "u_inherited_0018"
api_search_tables "u_inherited_0018"
api_visible_table_count "u_inherited_0018"

echo "=== dropped db owner before role drop ==="
api_get_databases "u_dropped_db_0018"
api_get_tables "u_dropped_db_0018" "db_dropped_db_0018"
api_search_databases "u_dropped_db_0018"
api_search_tables "u_dropped_db_0018"
api_visible_table_count "u_dropped_db_0018"
sql_get_databases "u_dropped_db_0018"
sql_get_tables "u_dropped_db_0018" "db_dropped_db_0018"
sql_get_exact_table "u_dropped_db_0018" "db_dropped_db_0018" "dropped_case_0018"

echo "drop role role_dropped_db_owner_0018;" | $BENDSQL_CLIENT_CONNECT

echo "=== dropped db owner after role drop ==="
api_get_databases "u_dropped_db_0018"
api_tables_status "u_dropped_db_0018" "db_dropped_db_0018"
api_search_databases "u_dropped_db_0018"
api_search_tables "u_dropped_db_0018"
api_visible_table_count "u_dropped_db_0018"
sql_count_databases "u_dropped_db_0018" "db_dropped_db_0018"
sql_count_tables "u_dropped_db_0018" "db_dropped_db_0018"
sql_count_exact_table "u_dropped_db_0018" "db_dropped_db_0018" "dropped_case_0018"

run_root_sql "
drop user if exists u_table_0018;
drop user if exists u_db_0018;
drop user if exists u_grant_0018;
drop user if exists u_direct_0018;
drop user if exists u_inherited_0018;
drop user if exists u_dropped_db_0018;
drop role if exists role_table_only_0018;
drop role if exists role_db_owner_0018;
drop role if exists role_grant_0018;
drop role if exists role_parent_0018;
drop role if exists role_child_0018;
drop role if exists role_dropped_db_owner_0018;
drop database if exists db_table_only_0018;
drop database if exists db_db_owner_0018;
drop database if exists db_grant_0018;
drop database if exists db_direct_0018;
drop database if exists db_inherited_0018;
drop database if exists db_dropped_db_0018;
"
