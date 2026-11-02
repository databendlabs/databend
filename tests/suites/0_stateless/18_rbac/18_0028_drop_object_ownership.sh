#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

run_user() {
	printf '%s\n' "$2" | bendsql_connect_user "$1" drop_owner_test -A --output tsv --quote-style=never
}

owner_of() {
	bendsql_connect_root --output tsv --query="SHOW GRANTS ON $1" |
		awk -F '\t' '$1 == "OWNERSHIP" { print $5 }'
}

cleanup() {
	bendsql_connect_root_null --query="
DROP CONNECTION IF EXISTS drop_owner_conn;
DROP SEQUENCE IF EXISTS drop_owner_seq;
DROP STAGE IF EXISTS drop_owner_stage;
DROP FUNCTION IF EXISTS drop_owner_udf;
DROP USER IF EXISTS drop_owner_old;
DROP USER IF EXISTS drop_owner_new;
DROP ROLE IF EXISTS drop_owner_old_role;
DROP ROLE IF EXISTS drop_owner_new_role;
"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="
CREATE ROLE drop_owner_old_role;
CREATE ROLE drop_owner_new_role;
CREATE USER drop_owner_old IDENTIFIED BY 'drop_owner_test' WITH DEFAULT_ROLE = 'drop_owner_old_role';
CREATE USER drop_owner_new IDENTIFIED BY 'drop_owner_test' WITH DEFAULT_ROLE = 'drop_owner_new_role';
GRANT ROLE drop_owner_old_role TO drop_owner_old;
GRANT ROLE drop_owner_new_role TO drop_owner_new;
"

# Exercise ownership-only DROP, cleanup, same-name reuse, and administrative
# reassignment through SQL. This is not a concurrent CREATE/DROP stress test.
for kind in CONNECTION SEQUENCE STAGE UDF; do
	case "$kind" in
	CONNECTION)
		object='CONNECTION drop_owner_conn'
		create="CREATE CONNECTION drop_owner_conn STORAGE_TYPE = 's3'"
		drop='DROP CONNECTION drop_owner_conn'
		;;
	SEQUENCE)
		object='SEQUENCE drop_owner_seq'
		create='CREATE SEQUENCE drop_owner_seq'
		drop='DROP SEQUENCE drop_owner_seq'
		;;
	STAGE)
		object='STAGE drop_owner_stage'
		create='CREATE STAGE drop_owner_stage'
		drop='DROP STAGE drop_owner_stage'
		;;
	UDF)
		object='UDF drop_owner_udf'
		create='CREATE FUNCTION drop_owner_udf AS (x) -> x + 1'
		drop='DROP FUNCTION drop_owner_udf'
		;;
	esac

	echo "=== $kind ownership cleanup ==="
	bendsql_connect_root_null --query="$create; GRANT OWNERSHIP ON $object TO ROLE drop_owner_old_role;"
	run_user drop_owner_old "$drop" >/dev/null
	[[ -z "$(owner_of "$object")" ]]
	echo 'ownership removed'

	bendsql_connect_root_null --query="$create; GRANT OWNERSHIP ON $object TO ROLE drop_owner_new_role;"
	if output=$(run_user drop_owner_old "$drop" 2>&1); then
		echo "expected permission denial: $drop"
		exit 1
	fi
	grep -q 'Permission denied' <<<"$output"
	[[ "$(owner_of "$object")" == drop_owner_new_role ]]
	echo 'denied drop preserves replacement owner'

	# An administrator can use this same GRANT to recover ownership if revocation
	# succeeds but metadata deletion fails. Do not automatically restore an old owner.
	bendsql_connect_root_null --query="GRANT OWNERSHIP ON $object TO ROLE drop_owner_old_role;"
	[[ "$(owner_of "$object")" == drop_owner_old_role ]]
	run_user drop_owner_old "$drop" >/dev/null
	[[ -z "$(owner_of "$object")" ]]
	echo 'administrator can reassign ownership'
done
