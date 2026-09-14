#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

API_BASE="http://${QUERY_MYSQL_HANDLER_HOST}:${QUERY_HTTP_HANDLER_PORT}/v1/catalog"

export USER_DROPPED_DB_CONNECT="bendsql_connect_user u_dropped_db_0018 123 -A --quote-style=never"

# Prepare data for table ownership, database ownership, role grants, direct user
# grants, inherited roles, and ownership cleanup after a role is dropped.
bendsql_connect_root_null <<SQL
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
SQL

echo "=== http table owner ==="
echo "-- catalog databases"
curl -s -u "u_table_0018:123" "${API_BASE}/databases" |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- catalog tables"
curl -s -u "u_table_0018:123" "${API_BASE}/databases/db_table_only_0018/tables" |
	jq -r '.tables[].name' |
	sort
echo "-- hidden database tables status"
curl -s -u "u_table_0018:123" -o /dev/null -w "%{http_code}\n" \
	"${API_BASE}/databases/db_grant_0018/tables"
echo "-- search databases"
curl -s -u "u_table_0018:123" -XPOST "${API_BASE}/search/databases" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"db_"}' |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- search tables"
curl -s -u "u_table_0018:123" -XPOST "${API_BASE}/search/tables" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"case"}' |
	jq -r '.tables[] | select(.database | contains("_0018")) | "\(.database).\(.name)"' |
	sort
echo "-- catalog stats tables"
curl -s -u "u_table_0018:123" "${API_BASE}/stats" | jq -r '.tables'

echo "=== http db owner ==="
echo "-- catalog databases"
curl -s -u "u_db_0018:123" "${API_BASE}/databases" |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- catalog tables"
curl -s -u "u_db_0018:123" "${API_BASE}/databases/db_db_owner_0018/tables" |
	jq -r '.tables[].name' |
	sort
echo "-- search databases"
curl -s -u "u_db_0018:123" -XPOST "${API_BASE}/search/databases" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"db_"}' |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- search tables"
curl -s -u "u_db_0018:123" -XPOST "${API_BASE}/search/tables" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"case"}' |
	jq -r '.tables[] | select(.database | contains("_0018")) | "\(.database).\(.name)"' |
	sort
echo "-- catalog stats tables"
curl -s -u "u_db_0018:123" "${API_BASE}/stats" | jq -r '.tables'

echo "=== http grant user ==="
echo "-- catalog databases"
curl -s -u "u_grant_0018:123" "${API_BASE}/databases" |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- catalog tables"
curl -s -u "u_grant_0018:123" "${API_BASE}/databases/db_grant_0018/tables" |
	jq -r '.tables[].name' |
	sort
echo "-- search databases"
curl -s -u "u_grant_0018:123" -XPOST "${API_BASE}/search/databases" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"db_"}' |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- search tables"
curl -s -u "u_grant_0018:123" -XPOST "${API_BASE}/search/tables" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"case"}' |
	jq -r '.tables[] | select(.database | contains("_0018")) | "\(.database).\(.name)"' |
	sort
echo "-- catalog stats tables"
curl -s -u "u_grant_0018:123" "${API_BASE}/stats" | jq -r '.tables'

echo "=== http direct user grant ==="
echo "-- catalog databases"
curl -s -u "u_direct_0018:123" "${API_BASE}/databases" |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- catalog tables"
curl -s -u "u_direct_0018:123" "${API_BASE}/databases/db_direct_0018/tables" |
	jq -r '.tables[].name' |
	sort
echo "-- search databases"
curl -s -u "u_direct_0018:123" -XPOST "${API_BASE}/search/databases" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"db_"}' |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- search tables"
curl -s -u "u_direct_0018:123" -XPOST "${API_BASE}/search/tables" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"case"}' |
	jq -r '.tables[] | select(.database | contains("_0018")) | "\(.database).\(.name)"' |
	sort
echo "-- catalog stats tables"
curl -s -u "u_direct_0018:123" "${API_BASE}/stats" | jq -r '.tables'

echo "=== http inherited role grant ==="
echo "-- catalog databases"
curl -s -u "u_inherited_0018:123" "${API_BASE}/databases" |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- catalog tables"
curl -s -u "u_inherited_0018:123" "${API_BASE}/databases/db_inherited_0018/tables" |
	jq -r '.tables[].name' |
	sort
echo "-- search databases"
curl -s -u "u_inherited_0018:123" -XPOST "${API_BASE}/search/databases" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"db_"}' |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- search tables"
curl -s -u "u_inherited_0018:123" -XPOST "${API_BASE}/search/tables" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"case"}' |
	jq -r '.tables[] | select(.database | contains("_0018")) | "\(.database).\(.name)"' |
	sort
echo "-- catalog stats tables"
curl -s -u "u_inherited_0018:123" "${API_BASE}/stats" | jq -r '.tables'

echo "=== dropped db owner before role drop ==="
echo "-- catalog databases"
curl -s -u "u_dropped_db_0018:123" "${API_BASE}/databases" |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- catalog tables"
curl -s -u "u_dropped_db_0018:123" "${API_BASE}/databases/db_dropped_db_0018/tables" |
	jq -r '.tables[].name' |
	sort
echo "-- search databases"
curl -s -u "u_dropped_db_0018:123" -XPOST "${API_BASE}/search/databases" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"db_"}' |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- search tables"
curl -s -u "u_dropped_db_0018:123" -XPOST "${API_BASE}/search/tables" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"case"}' |
	jq -r '.tables[] | select(.database | contains("_0018")) | "\(.database).\(.name)"' |
	sort
echo "-- catalog stats tables"
curl -s -u "u_dropped_db_0018:123" "${API_BASE}/stats" | jq -r '.tables'
echo "-- system.databases"
echo "select name from system.databases where name like 'db_%_0018' order by name" | $USER_DROPPED_DB_CONNECT
echo "-- system.tables"
echo "select name from system.tables where database = 'db_dropped_db_0018' order by name" | $USER_DROPPED_DB_CONNECT
echo "-- system.tables exact table"
echo "select name from system.tables where database = 'db_dropped_db_0018' and name = 'dropped_case_0018'" | $USER_DROPPED_DB_CONNECT

echo "drop role role_dropped_db_owner_0018;" | bendsql_connect_root_null

echo "=== dropped db owner after role drop ==="
echo "-- catalog databases"
curl -s -u "u_dropped_db_0018:123" "${API_BASE}/databases" |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- hidden database tables status"
curl -s -u "u_dropped_db_0018:123" -o /dev/null -w "%{http_code}\n" \
	"${API_BASE}/databases/db_dropped_db_0018/tables"
echo "-- search databases"
curl -s -u "u_dropped_db_0018:123" -XPOST "${API_BASE}/search/databases" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"db_"}' |
	jq -r '.databases | map(select(.name | contains("_0018"))) | .[].name' |
	sort
echo "-- search tables"
curl -s -u "u_dropped_db_0018:123" -XPOST "${API_BASE}/search/tables" \
	-H 'Content-Type: application/json' \
	-d '{"keyword":"case"}' |
	jq -r '.tables[] | select(.database | contains("_0018")) | "\(.database).\(.name)"' |
	sort
echo "-- catalog stats tables"
curl -s -u "u_dropped_db_0018:123" "${API_BASE}/stats" | jq -r '.tables'
echo "-- system.databases count"
echo "select count() from system.databases where name = 'db_dropped_db_0018'" | $USER_DROPPED_DB_CONNECT
echo "-- system.tables count"
echo "select count() from system.tables where database = 'db_dropped_db_0018'" | $USER_DROPPED_DB_CONNECT

bendsql_connect_root_null <<SQL
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
SQL
