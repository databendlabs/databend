#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


echo "drop stage if exists named_external_stage" | $BENDSQL_CLIENT_CONNECT

## tempdate/
aws --endpoint-url  ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/tempdata/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url  ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/tempdata/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url  ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/tempdata/ontime_200.csv.zst >/dev/null 2>&1

## tempdate/dir/
aws --endpoint-url  ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv s3://testbucket/admin/tempdata/dir/ontime_200.csv >/dev/null 2>&1
aws --endpoint-url  ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv.gz s3://testbucket/admin/tempdata/dir/ontime_200.csv.gz >/dev/null 2>&1
aws --endpoint-url  ${STORAGE_S3_ENDPOINT_URL} s3 cp s3://testbucket/admin/data/ontime_200.csv.zst s3://testbucket/admin/tempdata/dir/ontime_200.csv.zst >/dev/null 2>&1

## Copy from named external stage
echo "CREATE STAGE named_external_stage url = 's3://testbucket/admin/tempdata/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin'  endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT

## List files in internal stage
echo "=== List files in external stage ==="
echo "list @named_external_stage" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort

## Remove external stage file
echo "=== Test remove external stage file ==="
echo "remove @named_external_stage/ontime_200.csv.gz" | $BENDSQL_CLIENT_CONNECT
echo "list @named_external_stage" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort
echo "remove @named_external_stage/dir/ontime_200.csv.gz" | $BENDSQL_CLIENT_CONNECT
echo "list @named_external_stage/dir/" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort

## Remove external stage file with pattern
echo "=== Test remove external stage file with pattern ==="
echo "remove @named_external_stage/dir/ PATTERN = '.*zst'" | $BENDSQL_CLIENT_CONNECT
echo "list @named_external_stage" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort
echo "remove @named_external_stage PATTERN = '.*zst'" | $BENDSQL_CLIENT_CONNECT
echo "list @named_external_stage" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort

echo "drop stage named_external_stage" | $BENDSQL_CLIENT_CONNECT

echo "=== Test create or replace external stage ==="
echo "CREATE STAGE replace_stage url = 's3://testbucket/admin/tempdata/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin'  endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "=== List files in external stage ==="
echo "list @replace_stage" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort
echo "CREATE OR REPLACE STAGE replace_stage url = 's3://testbucket/admin/tempdata/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin'  endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT
echo "=== After create or replace List files in external stage again ==="
echo "list @replace_stage" | $BENDSQL_CLIENT_CONNECT | awk '{print $1}' | sort
