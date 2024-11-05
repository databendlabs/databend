#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

ROOT=$(realpath "$CURDIR"/../../../data/iceberg/iceberg_ctl/iceberg_db/iceberg_tbl/)

#stmt "drop table if exists test_iceberg;"

#echo ">>>> create table test_iceberg engine = iceberg location = 'fs://\${ROOT}/';"
#echo "create table test_iceberg engine = iceberg location = 'fs://${ROOT}/';" | $BENDSQL_CLIENT_CONNECT
#
#query "select * from test_iceberg order by id, data;"
#
#stmt "drop table test_iceberg;"
#
#stmt "drop connection if exists iceberg_conn;"
#stmt "create connection iceberg_conn storage_type = 's3' access_key_id ='minioadmin' secret_access_key ='minioadmin' ENDPOINT_URL='http://127.0.0.1:9900';"
#
#echo ">>>> create table test_iceberg engine = iceberg location = 's3://testbucket/iceberg_ctl/iceberg_db/iceberg_tbl/'  connection_name = 'iceberg_conn' ;"
#echo "create table test_iceberg engine = iceberg location = 's3://testbucket/iceberg_ctl/iceberg_db/iceberg_tbl/' connection_name = 'iceberg_conn';" | $BENDSQL_CLIENT_CONNECT
#query "select * from test_iceberg order by id, data;"
#query "show create table test_iceberg;"
#stmt "drop table test_iceberg;"
