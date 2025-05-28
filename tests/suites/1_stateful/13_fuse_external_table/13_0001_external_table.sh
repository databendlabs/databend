#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists external_table_test;" | $BENDSQL_CLIENT_CONNECT

echo "drop connection if exists external_table_conn;" | $BENDSQL_CLIENT_CONNECT

stmt "create or replace connection external_table_conn storage_type='s3' access_key_id = 'minioadmin'  endpoint_url = 'http://127.0.0.1:9900' secret_access_key = 'minioadmin';"

stmt "CREATE OR REPLACE TABLE external_table_test (
    id INTEGER,
    name VARCHAR,
    age INT
) 's3://testbucket/13_fuse_external_table/' connection=(connection_name = 'external_table_conn');"

stmt "create or replace connection external_table_conn_wrong storage_type='s3' access_key_id = 'minioadmin'  endpoint_url = 'http://127.0.0.1:9900' secret_access_key = 'minio';"

# Alter with the wrong connection should fail
stmt_fail "ALTER TABLE external_table_test connection=(connection_name = 'external_table_conn_wrong');"

# Alter the connection back to the correct one
stmt "ALTER TABLE external_table_test connection=(connection_name = 'external_table_conn');"
