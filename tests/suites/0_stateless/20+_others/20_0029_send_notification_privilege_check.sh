#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create user if not exists notification_sender identified by 'test'" | bendsql_connect_root
echo "create database if not exists notification_privilege_check" | bendsql_connect_root

# Function names must not override the grants of ordinary permanent tables.
for table in send_notification fuse_amend set_cache_capacity numbers; do
	echo "=== ordinary table ${table} ==="
	echo "create table notification_privilege_check.${table}(v int)" | bendsql_connect_root
	echo "insert into notification_privilege_check.${table} values (42)" | bendsql_connect_root_null

	echo "select v from notification_privilege_check.${table}" | bendsql_connect_user notification_sender test -A
	echo "alter table notification_privilege_check.${table} add column extra int" | bendsql_connect_user notification_sender test -A
	echo "drop table notification_privilege_check.${table}" | bendsql_connect_user notification_sender test -A

	echo "grant select, alter, drop on notification_privilege_check.${table} to notification_sender" | bendsql_connect_root
	echo "select v from notification_privilege_check.${table}" | bendsql_connect_user notification_sender test -A
	echo "alter table notification_privilege_check.${table} add column extra int" | bendsql_connect_user notification_sender test -A

	# Grants on an ordinary same-named table must not authorize the real function.
	if [ "$table" = "send_notification" ]; then
		echo "call system\$send_notification('oncall', 'alert')" | bendsql_connect_user notification_sender test -A
		echo "select * from send_notification('oncall', 'alert')" | bendsql_connect_user notification_sender test -A
	fi

	echo "drop table notification_privilege_check.${table}" | bendsql_connect_user notification_sender test -A
	dropped=$(echo "select count(*) from system.tables where database = 'notification_privilege_check' and name = '${table}'" | bendsql_connect_root -A)
	echo "remaining ${table} tables: ${dropped}"
done

# Other restricted functions must still require SUPER, while numbers stays public.
echo "=== actual table functions ==="
echo "select * from fuse_amend('notification_privilege_check', 'missing')" | bendsql_connect_user notification_sender test -A
echo "select * from set_cache_capacity('ALL', '0')" | bendsql_connect_user notification_sender test -A
echo "select count(*) from numbers(3)" | bendsql_connect_user notification_sender test -A

echo "drop database notification_privilege_check" | bendsql_connect_root
echo "drop user notification_sender" | bendsql_connect_root

exit 0
