#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create database if not exists test_attach_only ;" | $MYSQL_CLIENT_CONNECT

echo "drop table if exists test_attach_only.table_from;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table test_attach_only.table_from(a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

table_inserts=(
  "insert into test_attach_only.table_from(a) values(0)"
  "insert into test_attach_only.table_from(a) values(1)"
  "insert into test_attach_only.table_from(a) values(2)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
done

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table test_attach_only.table_from" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

echo "attach table test_attach_only.table_to 's3://testbucket/admin/data/$storage_prefix' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

# READ_ONLY attach

echo "Attach READ_ONLY"
# fetch table storage prefix
storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table test_attach_only.table_from" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

# READ_ONLY attach table
echo "attach table table_read_only 's3://testbucket/admin/data/$storage_prefix' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') READ_ONLY;" | $MYSQL_CLIENT_CONNECT


# READ_ONLY attach table is not allowed to be mutated

# mutation related enterprise features

echo "create table test_attach_only.test_json(id int, val json) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT
echo "insert into test_attach_only.test_json values(1, '{\"a\":33,\"b\":44}'),(2, '{\"a\":55,\"b\":66}')" | $MYSQL_CLIENT_CONNECT
storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table test_attach_only.test_json" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')
echo "attach table test_attach_only.test_json_read_only 's3://testbucket/admin/data/$storage_prefix' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}') READ_ONLY;" | $MYSQL_CLIENT_CONNECT

echo "create virtual column"
echo "CREATE VIRTUAL COLUMN (val['a'], val['b']) FOR test_attach_only.test_json" | $MYSQL_CLIENT_CONNECT

echo "alter virtual column"
echo "ALTER VIRTUAL COLUMN (val['k1']) FOR test_attach_only.test_json" | $MYSQL_CLIENT_CONNECT

echo "drop virtual column"
echo "DROP VIRTUAL COLUMN FOR test_attach_only.test_json" | $MYSQL_CLIENT_CONNECT

echo "refresh virtual column"
echo "REFRESH VIRTUAL COLUMN FOR test_attach_only.test_json" | $MYSQL_CLIENT_CONNECT

# vacuum
echo "vacuum dropped"
echo "VACUUM DROP TABLE test_attach_only.test_json RETAIN 0 HOURSN;" | $MYSQL_CLIENT_CONNECT

