#!/usr/bin/env bash

export TESTS_DATA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/data

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_EC2_METADATA_DISABLED=true

export QUERY_DATABASE=${QUERY_DATABASE:="default"}
export QUERY_MYSQL_HANDLER_HOST=${QUERY_MYSQL_HANDLER_HOST:="127.0.0.1"}
export QUERY_MYSQL_HANDLER_PORT=${QUERY_MYSQL_HANDLER_PORT:="3307"}
export QUERY_HTTP_HANDLER_PORT=${QUERY_HTTP_HANDLER_PORT:="8000"}

bendsql_client() {
	bendsql "$@" 2> >(sed -E 's/ \[v[0-9][^]]*\]$//' >&2)
}

bendsql_query_http_connect() {
	bendsql_client \
		--host "${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_HTTP_HANDLER_PORT}" \
		"$@"
}

bendsql_query_http_user_connect() {
	local user="$1"
	local password="$2"
	shift 2

	bendsql_query_http_connect \
		--user "${user}" \
		--password "${password}" \
		"$@"
}

bendsql_client_connect() {
	bendsql_query_http_connect -uroot --quote-style=never "$@"
}

bendsql_client_output_null() {
	bendsql_client_connect --output null "$@"
}

export BENDSQL_CLIENT_CONNECT="bendsql_client_connect"
export BENDSQL_CLIENT_OUTPUT_NULL="bendsql_client_output_null"


# share client
export QUERY_MYSQL_HANDLER_SHARE_PROVIDER_PORT="18000"
export QUERY_MYSQL_HANDLER_SHARE_CONSUMER_PORT="28000"

bendsql_client_share_provider_connect() {
	bendsql_client \
		-uroot \
		--host "${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_MYSQL_HANDLER_SHARE_PROVIDER_PORT}" \
		--quote-style=never \
		"$@"
}

bendsql_client_share_consumer_connect() {
	bendsql_client \
		-uroot \
		--host "${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_MYSQL_HANDLER_SHARE_CONSUMER_PORT}" \
		--quote-style=never \
		"$@"
}

export BENDSQL_CLIENT_PROVIDER_CONNECT="bendsql_client_share_provider_connect"
export BENDSQL_CLIENT_CONSUMER_CONNECT="bendsql_client_share_consumer_connect"


query() {
	echo ">>>> $1"
	echo "$1" | $BENDSQL_CLIENT_CONNECT
	echo "<<<<"
}

stmt() {
	echo ">>>> $1"
	echo "$1" | $BENDSQL_CLIENT_CONNECT
	if [ $? -ne 0 ]; then
		echo "<<<<"
	fi
	return 0
}

stmt_fail() {
	echo ">>>> $1"
	echo "$1" | $BENDSQL_CLIENT_CONNECT > /dev/null 2>&1
	if [ $? -eq 0 ]; then
		return 1
	fi
    echo "<<<< expected failure happened"
	return 0
}

comment() {
	echo "#### $1"
}

# Execute SQL as root user, useful for setup/teardown blocks
run_root_sql() {
	cat <<SQL | $BENDSQL_CLIENT_CONNECT
$1
SQL
}
