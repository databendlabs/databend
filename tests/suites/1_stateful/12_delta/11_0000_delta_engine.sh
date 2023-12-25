#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

ROOT=$(realpath "$CURDIR"/../../../data/delta/simple/)

stmt "drop table if exists test_delta;"

echo ">>>> create table test_delta engine = delta location = 'fs://\${ROOT}/';"
echo "create table test_delta engine = delta location = 'fs://${ROOT}/';" | $BENDSQL_CLIENT_CONNECT
# stmt "create table test_delta engine = delta location = 'fs://${ROOT}/';"
query "select * from test_delta order by id;"
stmt "drop table test_delta;"

stmt "drop connection if exists s3_conn;"
stmt "create connection s3_conn storage_type = 's3' access_key_id ='minioadmin' secret_access_key ='minioadmin' ENDPOINT_URL='http://127.0.0.1:9900';"

stmt "create table test_delta engine = delta location = 's3://testbucket/admin/data/delta/simple/' connection_name = 's3_conn';"
query "select * from test_delta order by id;"
query "show create table test_delta;"
stmt "drop table test_delta;"


