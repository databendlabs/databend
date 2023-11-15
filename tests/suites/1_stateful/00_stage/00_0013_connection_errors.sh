#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "drop stage if exists my_stage;"
stmt "drop connection if exists my_conn;"

stmt "create connection my_conn storage_type = 's4';"
stmt "create connection my_conn storage_type = 's4' connection_name='my_conn'"
stmt "create connection my_conn storage_type = 's3' connection_name='my_conn'"
stmt "create connection my_conn storage_type = 's4' access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}'"
stmt "create connection my_conn storage_type = 's3' access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}'"
stmt "create stage my_stage url= 's3://testbucket/admin/tempdata/' connection = (connection_name='my_conn', endpoint_url='xx');"
stmt "create stage my_stage url= 'http://testbucket/admin/tempdata/' connection = (connection_name='my_conn');"
