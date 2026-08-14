#!/bin/bash

set -e

execute_query() {
  local sql="$1"
  local user_cred="$2"

  curl -s -u "${user_cred}" -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d "{\"sql\": \"$sql\"}"
}

execute_query_silent() {
  execute_query "$@" > /dev/null
}

execute_and_verify() {
    local cmd_description="$1"
    local user_cred="$2"
    local sql="$3"
    local expected_state="$4"
    local result

    echo "Executing: $cmd_description ... "

    result=$(execute_query "$sql" "$user_cred" | jq -r '.state')

    if [[ "$result" != "$expected_state" ]]; then
        echo "Failed! Expected result: $expected_state, actual result: $result."
        echo "$cmd_description failed."
        exit 1
    else
        echo "Description: $cmd_description completed successfully."
    fi
}

# Setup users and roles
execute_query_silent "drop user if exists a" "root:"
execute_query_silent "drop role if exists ra" "root:"
execute_query_silent "create user a identified by '123' with default_role='ra'" "root:"
execute_query_silent "create role ra" "root:"
execute_query_silent "grant select , drop on system_history.* to role ra" "root:"
execute_query_silent "grant alter on system_history.* to role ra" "root:"
execute_query_silent "grant role ra to a" "root:"
execute_query_silent "grant write, read on stage log_1f93b76af0bd4b1d8e018667865fbc65 to a" "root:"

external_catalog_user="external_catalog_fields_user"
external_catalog_password="external_catalog_fields_password"
execute_query_silent "drop user if exists ${external_catalog_user}" "root:"
execute_query_silent "create user ${external_catalog_user} identified by '${external_catalog_password}'" "root:"

check_external_catalog_fields_access() {
    local response

    response=$(curl -fsS \
        -u "${external_catalog_user}:${external_catalog_password}" \
        "http://localhost:8000/v1/catalog/databases/lineage_db/tables/src/fields?catalog=lineage_history_iceberg_catalog")
    if ! echo "$response" | jq -e '.fields | length == 1 and .[0].name == "a"' >/dev/null; then
        echo "External catalog fields API returned an unexpected response"
        echo "$response"
        exit 1
    fi
}

check_system_history_permissions() {
    # Test 1: User 'a:123' attempts to create a table in 'system_history' (expected to fail)
    execute_and_verify \
        "User 'a:123' attempts to create a table in 'system_history'" \
        "a:123" \
        "create table system_history.t (a INT)" \
        "Failed"

    # Test 2: User 'root:' attempts to modify 'system_history.log_history' table (expected to fail)
    execute_and_verify \
        "User 'root:' attempts to modify 'system_history.log_history' table" \
        "root:" \
        "alter table system_history.log_history add column id int" \
        "Failed"

    # Test 3: User 'a:123' drop 'system_history.access_history' table (expected to succeed)
    execute_and_verify \
        "User 'a:123' drop 'system_history.access_history' table" \
        "a:123" \
        "drop table system_history.access_history" \
        "Succeeded"

    # Test 4: User 'root:' grant ownership on 'system_history.*' (expected to fail)
    execute_and_verify \
        "User 'root:' grant ownership on 'system_history.*'" \
        "root:" \
        "grant ownership on system_history.* to role ra" \
        "Failed"

    # Test 5: User 'root:' grant ownership on 'system_history.query_history' (expected to fail)
    execute_and_verify \
        "User 'root:' grant ownership on 'system_history.query_history'" \
        "root:" \
        "grant ownership on system_history.query_history to role ra" \
        "Failed"

    # Test 6: User 'a:123' select 'system_history.query_history' table (expected to succeed)
    execute_and_verify \
        "User 'a:123' query 'system_history.query_history' table" \
        "a:123" \
        "select count() from system_history.query_history" \
        "Succeeded"

    # Test 7: User 'a:123' drop stage 'log_1f93b76af0bd4b1d8e018667865fbc65' table (expected to failed)
    execute_and_verify \
        "User 'a:123' drop stage log_1f93b76af0bd4b1d8e018667865fbc65" \
        "a:123" \
        "drop stage log_1f93b76af0bd4b1d8e018667865fbc65" \
        "Failed"

    # Test 8: User 'a:123' copy into @log_1f93b76af0bd4b1d8e018667865fbc65 from (select * from system.one) (expected to failed)
    execute_and_verify \
        "User 'a:123' copy into @log_1f93b76af0bd4b1d8e018667865fbc65 from (select * from system.one);" \
        "a:123" \
        "copy into @log_1f93b76af0bd4b1d8e018667865fbc65 from (select * from system.one);" \
        "Failed"

    # Test 9: User 'a:123' copy into t from (select * from @log_1f93b76af0bd4b1d8e018667865fbc65) (expected to failed)
    execute_and_verify \
        "User 'a:123' copy into t from (select * from @log_1f93b76af0bd4b1d8e018667865fbc65);" \
        "a:123" \
        "copy into t from (select * from @log_1f93b76af0bd4b1d8e018667865fbc65);" \
        "Failed"

    # Test 10: User 'root' grant stage 'log_1f93b76af0bd4b1d8e018667865fbc65' ownership (expected to failed)
    execute_and_verify \
        "User 'root' grant ownership on stage log_1f93b76af0bd4b1d8e018667865fbc65 to role ra" \
        "root:" \
        "grant ownership on stage log_1f93b76af0bd4b1d8e018667865fbc65 to role ra" \
        "Failed"
}

check_system_history_permissions
check_external_catalog_fields_access

# Cleanup
execute_query_silent "drop user if exists a" "root:"
execute_query_silent "drop role if exists ra" "root:"
execute_query_silent "drop user if exists ${external_catalog_user}" "root:"
