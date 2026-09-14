#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# base table
echo "drop table if exists comment_base" | bendsql_connect_root
echo "create table comment_base (c1 int comment 'c1 comment', c2 int comment 'c2 comment') comment = 'tbl comment'" | bendsql_connect_root

# empty table is not attachable currently, thus we need to insert some data
echo "init table"
echo "insert into comment_base values(1, 2)" | bendsql_connect_root

storage_prefix=$(mysql -uroot -h127.0.0.1 -P3307  -e "set global hide_options_in_show_create_table=0;show create table comment_base" | grep -i snapshot_location | awk -F'SNAPSHOT_LOCATION='"'"'|_ss' '{print $2}')

# attach table
echo "drop table if exists att_comment" | bendsql_connect_root
echo "attach table att_comment 's3://testbucket/admin/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root


echo "check table comment"
echo "select comment from system.tables where name = 'att_comment'" | bendsql_connect_root

echo "check column comment"
echo "select name, comment from system.columns where table = 'att_comment' order by name" | bendsql_connect_root


# alter table rename column should generate new hint file
stmt "alter table comment_base rename column c1 to c1_new"
echo "drop table if exists att_comment" | bendsql_connect_root
echo "attach table att_comment 's3://testbucket/admin/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root
echo "select name, comment from system.columns where table = 'att_comment' order by name" | bendsql_connect_root

# alter table rename comment should generate new hint file
stmt "alter table comment_base comment = 'new tbl comment'"
echo "drop table if exists att_comment" | bendsql_connect_root
echo "attach table att_comment 's3://testbucket/admin/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root
stmt "select comment from system.tables where name = 'att_comment'"

# alter table modify column comment should generate new hint file
stmt "ALTER TABLE comment_base MODIFY COLUMN c1_new comment 'new comment of c1_new'"
echo "drop table if exists att_comment" | bendsql_connect_root
echo "attach table att_comment 's3://testbucket/admin/$storage_prefix' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root
echo "select name, comment from system.columns where table = 'att_comment' order by name" | bendsql_connect_root

echo "drop table if exists comment_base" | bendsql_connect_root
echo "drop table if exists att_comment" | bendsql_connect_root
