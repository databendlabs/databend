#!/usr/bin/env bash

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

integrations=(
	'unquoted_access'
	'"QuotedAccess"'
	'"access with space"'
	'"access/r"'
	'"access""r"'
	'"select"'
)

cleanup() {
	for integration in "${integrations[@]}"; do
		bendsql_connect_root_null --query="DROP INTEGRATION IF EXISTS $integration"
	done
	bendsql_connect_root_null --query="DROP ROLE IF EXISTS integration_grants; DROP NETWORK RULE IF EXISTS integration_grant_rule"
}

cleanup
trap cleanup EXIT
bendsql_connect_root_null --query="CREATE ROLE integration_grants; CREATE NETWORK RULE integration_grant_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('example.com:443')"

for integration in "${integrations[@]}"; do
	bendsql_connect_root_null --query="CREATE EXTERNAL ACCESS INTEGRATION $integration ALLOWED_NETWORK_RULES = (integration_grant_rule) ENABLED = TRUE"
	bendsql_connect_root_null --query="GRANT USAGE ON INTEGRATION $integration TO ROLE integration_grants"
	grants=$(bendsql_connect_root --output tsv --query="SHOW GRANTS FOR ROLE integration_grants")
	printf '%s\n' "$grants"

	# SHOW GRANTS must emit SQL that can be executed without losing identifier quotes.
	grant_sql=$(printf '%s\n' "$grants" | cut -f6-)
	bendsql_connect_root_null --query="$grant_sql"
	bendsql_connect_root_null --query="REVOKE USAGE ON INTEGRATION $integration FROM ROLE integration_grants"
	bendsql_connect_root --output tsv --query="SHOW GRANTS FOR ROLE integration_grants"
	bendsql_connect_root_null --query="DROP INTEGRATION $integration"
done
