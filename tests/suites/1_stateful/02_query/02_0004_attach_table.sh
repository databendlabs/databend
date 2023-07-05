#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_from;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists table_to;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table table_from(a int) select * from numbers(3) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

echo "attach table_to 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

## Select table
echo "select * from table_to;" | $MYSQL_CLIENT_CONNECT
