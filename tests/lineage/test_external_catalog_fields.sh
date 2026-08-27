#!/bin/bash

set -e

QUERY_HTTP_HANDLER_PORT="${QUERY_HTTP_HANDLER_PORT:-8000}"

execute_query() {
    local sql="$1"

    curl -fsS -u root: -XPOST "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/query" \
        -H 'Content-Type: application/json' \
        -d "$(jq -nc --arg sql "$sql" '{sql: $sql, pagination: {wait_time_secs: 10}}')"
}

execute_query_silent() {
    local sql="$1"
    local response

    response=$(execute_query "$sql")
    if ! echo "$response" | jq -e '.state == "Succeeded"' >/dev/null; then
        echo "Query failed: $sql"
        echo "$response"
        exit 1
    fi
}

user="external_catalog_fields_user"
password="external_catalog_fields_password"

execute_query_silent "drop user if exists ${user}"
execute_query_silent "create user ${user} identified by '${password}'"

response=$(curl -fsS \
    -u "${user}:${password}" \
    "http://localhost:${QUERY_HTTP_HANDLER_PORT}/v1/catalog/databases/lineage_db/tables/src/fields?catalog=lineage_history_iceberg_catalog")
if ! echo "$response" | jq -e '.fields | length == 1 and .[0].name == "a"' >/dev/null; then
    echo "External catalog fields API returned an unexpected response"
    echo "$response"
    exit 1
fi

execute_query_silent "drop user if exists ${user}"

echo "External catalog fields API test passed"
