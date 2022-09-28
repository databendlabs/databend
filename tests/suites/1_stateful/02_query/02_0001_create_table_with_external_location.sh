#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_external_location;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table table_external_location(a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

table_inserts=(
  "insert into table_external_location(a) values(888)"
  "insert into table_external_location(a) values(1024)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
done

## Select table
echo "select * from table_external_location order by a;" | $MYSQL_CLIENT_CONNECT

## Drop table
echo "drop table if exists table_external_location;" | $MYSQL_CLIENT_CONNECT


