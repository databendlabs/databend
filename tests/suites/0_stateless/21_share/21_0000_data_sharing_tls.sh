#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

export TLS_SHARE_USER_CONNECT="bendsql_connect_user tls_share_user 123 -A --quote-style=never"

bendsql_connect_root_null <<SQL
set global enable_experimental_connection_privilege_check=1;
drop user if exists tls_share_user;
drop role if exists tls_share_role;
drop share if exists tls_share;
drop database if exists tls_share_db;
drop connection if exists tls_share_conn;

create connection tls_share_conn storage_type = 'fs';
create share tls_share connection = tls_share_conn;
create database tls_share_db;
create table tls_share_db.orders(id int);
grant usage on database tls_share_db to share tls_share;
grant select on table tls_share_db.orders to share tls_share;

create role tls_share_role;
create user tls_share_user identified by '123' with default_role='tls_share_role';
grant role tls_share_role to tls_share_user;
SQL

if echo "create share unauthorized_tls_share" | $TLS_SHARE_USER_CONNECT >/dev/null 2>&1; then
	echo "share management without GRANT unexpectedly succeeded"
	exit 1
fi
echo "share management without GRANT denied"

bendsql_connect_root_null <<SQL
grant grant on *.* to role tls_share_role;
SQL

echo "create share if not exists tls_share connection = missing_tls_connection" | $TLS_SHARE_USER_CONNECT >/dev/null
echo "CREATE SHARE IF NOT EXISTS skipped unused Connection authorization"

echo "alter share if exists missing_tls_share set connection = missing_tls_connection" | $TLS_SHARE_USER_CONNECT >/dev/null
echo "ALTER SHARE IF EXISTS skipped unused Connection authorization"

if echo "alter share tls_share add accounts = unauthorized_tls_consumer" | $TLS_SHARE_USER_CONNECT >/dev/null 2>&1; then
	echo "account authorization without ACCESS CONNECTION unexpectedly succeeded"
	exit 1
fi
echo "account authorization without ACCESS CONNECTION denied"

bendsql_connect_root_null <<SQL
grant access connection on connection tls_share_conn to role tls_share_role;
SQL

echo "alter share tls_share add accounts = authorized_tls_consumer" | $TLS_SHARE_USER_CONNECT >/dev/null
echo "account authorization with ACCESS CONNECTION succeeded"

bendsql_connect_root_null <<SQL
revoke access connection on connection tls_share_conn from role tls_share_role;
SQL

echo "alter share tls_share remove accounts = authorized_tls_consumer" | $TLS_SHARE_USER_CONNECT >/dev/null
echo "account removal without ACCESS CONNECTION succeeded"

bendsql_connect_root_null <<SQL
drop share if exists tls_rename_share;
drop database if exists tls_rename_db;
drop database if exists tls_rename_db_renamed;
create share tls_rename_share connection = tls_share_conn;
create database tls_rename_db;
create table tls_rename_db.t(id int);
grant usage on database tls_rename_db to share tls_rename_share;
grant select on table tls_rename_db.t to share tls_rename_share;
alter table tls_rename_db.t rename to renamed_t;
SQL

if echo "revoke select on table tls_rename_db.t from share tls_rename_share" | $TLS_SHARE_USER_CONNECT >/dev/null 2>&1; then
	echo "old-name table revoke bypassed stable object permissions"
	exit 1
fi
echo "old-name table revoke required stable object permissions"

bendsql_connect_root_null <<SQL
revoke select on table tls_rename_db.renamed_t from share tls_rename_share;
alter database tls_rename_db rename to tls_rename_db_renamed;
SQL

if echo "revoke usage on database tls_rename_db from share tls_rename_share" | $TLS_SHARE_USER_CONNECT >/dev/null 2>&1; then
	echo "old-name database revoke bypassed stable object permissions"
	exit 1
fi
echo "old-name database revoke required stable object permissions"

bendsql_connect_root_null <<SQL
revoke usage on database tls_rename_db_renamed from share tls_rename_share;
drop database tls_rename_db_renamed;
drop share tls_rename_share;
drop share if exists tls_stale_share;
drop database if exists tls_stale_db;
create share tls_stale_share connection = tls_share_conn;
create database tls_stale_db;
create table tls_stale_db.t(id int);
grant usage on database tls_stale_db to share tls_stale_share;
grant select on table tls_stale_db.t to share tls_stale_share;
drop table tls_stale_db.t;
create table tls_stale_db.t(id int);
SQL

echo "revoke select on table tls_stale_db.t from share tls_stale_share" | $TLS_SHARE_USER_CONNECT >/dev/null
echo "stale table grant cleanup ignored same-name replacement permissions"

bendsql_connect_root_null <<SQL
drop database tls_stale_db;
create database tls_stale_db;
SQL

echo "revoke usage on database tls_stale_db from share tls_stale_share" | $TLS_SHARE_USER_CONNECT >/dev/null
echo "stale database grant cleanup ignored same-name replacement permissions"

bendsql_connect_root_null <<SQL
drop database tls_stale_db;
drop share tls_stale_share;
drop user tls_share_user;
drop role tls_share_role;
revoke select on table tls_share_db.orders from share tls_share;
revoke usage on database tls_share_db from share tls_share;
drop share tls_share;
drop database tls_share_db;
drop connection tls_share_conn;
unset global enable_experimental_connection_privilege_check;
SQL
