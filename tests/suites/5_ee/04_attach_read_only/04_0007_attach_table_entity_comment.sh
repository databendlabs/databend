#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# base table
echo "drop table if exists comment_base" | $BENDSQL_CLIENT_CONNECT
echo "create table comment_base (c1 int comment 'c1 comment', c2 int comment 'c2 comment') comment = 'tbl comment'" | $BENDSQL_CLIENT_CONNECT

# empty table is not attachable currently, thus we need to insert some data
echo "init table"
echo "insert into comment_base values(1, 2)" | $BENDSQL_CLIENT_CONNECT

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table comment_base" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

# attach table
echo "drop table if exists att_comment" | $BENDSQL_CLIENT_CONNECT
echo "attach table att_comment 's3://testbucket/admin/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT


echo "check table comment"
echo "select comment from system.tables where name = 'att_comment'" | $BENDSQL_CLIENT_CONNECT

echo "check column comment"
echo "select name, comment from system.columns where table = 'att_comment' order by name" | $BENDSQL_CLIENT_CONNECT


echo "drop table if exists comment_base" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists att_comment" | $BENDSQL_CLIENT_CONNECT
