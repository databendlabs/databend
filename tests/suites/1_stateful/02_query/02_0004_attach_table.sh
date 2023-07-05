#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop table if exists table_from;" | $MYSQL_CLIENT_CONNECT
echo "drop table if exists table_to;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table table_from(a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

table_inserts=(
  "insert into table_from(a) values(0)"
  "insert into table_from(a) values(1)"
  "insert into table_from(a) values(2)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
done

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table table_from" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

echo "attach table table_to 's3://testbucket/admin/data/$storage_prefix' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT


## Select table
echo "select * from table_to;" | $MYSQL_CLIENT_CONNECT
