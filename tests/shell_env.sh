#!/usr/bin/env bash

export TESTS_DATA_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/data

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_EC2_METADATA_DISABLED=true

export QUERY_DATABASE=${QUERY_DATABASE:="default"}
export QUERY_MYSQL_HANDLER_HOST=${QUERY_MYSQL_HANDLER_HOST:="127.0.0.1"}
export QUERY_MYSQL_HANDLER_PORT=${QUERY_MYSQL_HANDLER_PORT:="3307"}
export QUERY_HTTP_HANDLER_PORT=${QUERY_HTTP_HANDLER_PORT:="8000"}

export BENDSQL_ERROR_NO_VERSION=1

bendsql_connect() {
	bendsql \
		--host "${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_HTTP_HANDLER_PORT}" \
		"$@"
}

bendsql_connect_user() {
	local user="$1"
	local password="$2"
	shift 2

	bendsql_connect \
		--user "${user}" \
		--password "${password}" \
		"$@"
}

bendsql_connect_root() {
	bendsql_connect -uroot --quote-style=never "$@"
}

bendsql_connect_root_null() {
	bendsql_connect_root --output null "$@"
}

# share client
export QUERY_MYSQL_HANDLER_SHARE_PROVIDER_PORT="18000"
export QUERY_MYSQL_HANDLER_SHARE_CONSUMER_PORT="28000"

bendsql_connect_share_provider() {
	bendsql \
		-uroot \
		--host "${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_MYSQL_HANDLER_SHARE_PROVIDER_PORT}" \
		--quote-style=never \
		"$@"
}

bendsql_connect_share_consumer() {
	bendsql \
		-uroot \
		--host "${QUERY_MYSQL_HANDLER_HOST}" \
		--port "${QUERY_MYSQL_HANDLER_SHARE_CONSUMER_PORT}" \
		--quote-style=never \
		"$@"
}


query() {
	echo ">>>> $1"
	echo "$1" | bendsql_connect_root
	echo "<<<<"
}

stmt() {
	echo ">>>> $1"
	echo "$1" | bendsql_connect_root
	if [ $? -ne 0 ]; then
		echo "<<<<"
	fi
	return 0
}

stmt_fail() {
	echo ">>>> $1"
	echo "$1" | bendsql_connect_root > /dev/null 2>&1
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
	cat <<SQL | bendsql_connect_root
$1
SQL
}
