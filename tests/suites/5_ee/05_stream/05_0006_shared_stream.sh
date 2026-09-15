#!/usr/bin/env bash

# Requires an EE-enabled query with sandbox tenants, like the other stream/sharing suites.
set -euo pipefail
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# The consumer uses the deployment tenant so the local admin status endpoint can resolve it.
# TLS runs also need HTTPS QUERY_HTTP_URL, CURL_CA_BUNDLE, and a TLS-configured bendsql.
query_http_url=${QUERY_HTTP_URL:-http://${QUERY_MYSQL_HANDLER_HOST}:${QUERY_HTTP_HANDLER_PORT}}
consumer_tenant=$(curl -fsS "${QUERY_ADMIN_URL:-http://localhost:8080}/v1/config" | jq -r '.query.tenant_id')
[[ -n "$consumer_tenant" && "$consumer_tenant" != null ]]

# Match the provider storage: EE CI uses S3/MinIO, while local runs can use fs.
share_connection_options="storage_type = '${STORAGE_TYPE:-fs}'"
if [[ "${STORAGE_TYPE:-fs}" == s3 ]]; then
	share_connection_options+=" access_key_id = '${STORAGE_S3_ACCESS_KEY_ID:-minioadmin}'
secret_access_key = '${STORAGE_S3_SECRET_ACCESS_KEY:-minioadmin}'
endpoint_url = '${STORAGE_S3_ENDPOINT_URL:-http://127.0.0.1:9900}'"
fi

bendsql_connect_root_null <<SQL
set sandbox_tenant = 'shared_stream_api_provider';
drop share if exists stream_api_share;
drop database if exists stream_api_provider;
drop connection if exists stream_api_conn;
create database stream_api_provider;
create table stream_api_provider.t(a int) change_tracking = true;
create connection stream_api_conn ${share_connection_options};
create share stream_api_share connection = stream_api_conn;
grant usage on database stream_api_provider to share stream_api_share;
grant select on table stream_api_provider.t to share stream_api_share;
alter share stream_api_share add accounts = ${consumer_tenant};
set sandbox_tenant = '';
drop database if exists stream_api_shared;
create database stream_api_shared from share shared_stream_api_provider.stream_api_share;
create or replace database stream_api_local;
drop user if exists stream_api_user;
drop role if exists stream_api_role;
create role stream_api_role;
create user stream_api_user identified by '123' with default_role = 'stream_api_role';
grant role stream_api_role to stream_api_user;
SQL

expect_denied() {
	if printf '%s\n' "$1" | bendsql_connect_user stream_api_user 123 --output null >/dev/null 2>&1; then
		echo "Unexpected success: $1"
		exit 1
	fi
}

expect_denied "create stream stream_api_local.denied on table stream_api_shared.t"
echo "shared stream creation requires target CREATE"

bendsql_connect_root_null <<SQL
grant create on stream_api_local.* to role stream_api_role;
SQL
# Match ordinary stream creation: source SELECT is not required.
expect_denied "select a from stream_api_shared.t"
printf '%s\n' "create stream stream_api_local.s on table stream_api_shared.t" | bendsql_connect_user stream_api_user 123 --output null
echo "shared stream creation does not require source SELECT"
bendsql_connect_root_null <<SQL
grant select on stream_api_local.s to role stream_api_role;
SQL

curl -fsS "${QUERY_ADMIN_URL:-http://localhost:8080}/v1/stream_status?database=stream_api_local&stream_name=s" | jq -r '.has_data'
bendsql_connect_root_null <<SQL
set sandbox_tenant = 'shared_stream_api_provider';
insert into stream_api_provider.t values (1);
SQL
printf '%s\n' "select a from stream_api_local.s" | bendsql_connect_user stream_api_user 123 --quote-style=never
curl -fsS "${QUERY_ADMIN_URL:-http://localhost:8080}/v1/stream_status?database=stream_api_local&stream_name=s" | jq -r '.has_data'
curl -fsS -u root: "${query_http_url}/v1/catalog/databases/stream_api_local/streams" | jq -r '.streams[] | [.name, .table_name] | join(" ")'

bendsql_connect_root_null <<SQL
set sandbox_tenant = 'shared_stream_api_provider';
revoke select on table stream_api_provider.t from share stream_api_share;
SQL
expect_denied "select a from stream_api_local.s"
echo "provider revocation prevents stream reads"

bendsql_connect_root_null <<SQL
drop database stream_api_local;
drop database stream_api_shared;
drop user stream_api_user;
drop role stream_api_role;
set sandbox_tenant = 'shared_stream_api_provider';
drop share stream_api_share;
drop connection stream_api_conn;
drop database stream_api_provider;
SQL
