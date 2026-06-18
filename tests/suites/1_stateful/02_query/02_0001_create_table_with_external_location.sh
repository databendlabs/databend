#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_external_location;" | bendsql_connect_root

## Create table
echo "create table table_external_location(a int) 's3://testbucket/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root

table_inserts=(
  "insert into table_external_location(a) values(888)"
  "insert into table_external_location(a) values(1024)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | bendsql_connect_root
done

## Select table
echo "select * from table_external_location order by a;" | bendsql_connect_root
echo "select is_external, storage_param from system.tables where name='table_external_location';" | bendsql_connect_root

## Drop table
echo "drop table if exists table_external_location;" | bendsql_connect_root

