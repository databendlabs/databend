#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

## Setup
echo "drop database if exists test_vacuum_drop" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_2" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_3" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_4" | $MYSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_vacuum_drop" | $MYSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop.a(c int)" | $MYSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop.a VALUES (1)" | $MYSQL_CLIENT_CONNECT

echo "select * from test_vacuum_drop.a" | $MYSQL_CLIENT_CONNECT

echo "drop table test_vacuum_drop.a" | $MYSQL_CLIENT_CONNECT

echo "set retention_period=0;vacuum drop table from test_vacuum_drop retain 0 hours" | $MYSQL_CLIENT_CONNECT

echo "create table test_vacuum_drop.b(c int)" | $MYSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop.b VALUES (2)" | $MYSQL_CLIENT_CONNECT

echo "drop table test_vacuum_drop.b" | $MYSQL_CLIENT_CONNECT

echo "set retention_period=0;vacuum drop table from test_vacuum_drop" | $MYSQL_CLIENT_CONNECT

echo "undrop table test_vacuum_drop.b" | $MYSQL_CLIENT_CONNECT

# test_vacuum_drop.b has not been vacuum, MUST return [2]
echo "select * from test_vacuum_drop.b" | $MYSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_vacuum_drop_2" | $MYSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_2.a(c int)" | $MYSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop_2.a VALUES (3)" | $MYSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_vacuum_drop_3" | $MYSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_3.a(c int)" | $MYSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop_3.a VALUES (4)" | $MYSQL_CLIENT_CONNECT

echo "select * from test_vacuum_drop_2.a" | $MYSQL_CLIENT_CONNECT
echo "select * from test_vacuum_drop_3.a" | $MYSQL_CLIENT_CONNECT

echo "drop database test_vacuum_drop_2" | $MYSQL_CLIENT_CONNECT
echo "drop table test_vacuum_drop_3.a" | $MYSQL_CLIENT_CONNECT

# vacuum without [from db] will vacuum all tables, including tables in drop db
echo "set retention_period=0;vacuum drop table retain 0 hours" | $MYSQL_CLIENT_CONNECT

echo "drop database if exists test_vacuum_drop" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_2" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_3" | $MYSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_4" | $MYSQL_CLIENT_CONNECT

# test external table
echo "drop table if exists table_drop_external_location;" | $MYSQL_CLIENT_CONNECT

## Create table
echo "create table table_drop_external_location(a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $MYSQL_CLIENT_CONNECT

table_inserts=(
  "insert into table_drop_external_location(a) values(888)"
  "insert into table_drop_external_location(a) values(1024)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $MYSQL_CLIENT_CONNECT
done

## Select table
echo "select * from table_drop_external_location order by a;" | $MYSQL_CLIENT_CONNECT

echo "drop table table_drop_external_location;" | $MYSQL_CLIENT_CONNECT

echo "set retention_period=0;vacuum drop table retain 0 hours" | $MYSQL_CLIENT_CONNECT

## dry run
echo "CREATE DATABASE test_vacuum_drop_4" | $MYSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_4.a(c int)" | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_vacuum_drop_4.a VALUES (1)" | $MYSQL_CLIENT_CONNECT
echo "select * from test_vacuum_drop_4.a"  | $MYSQL_CLIENT_CONNECT
echo "drop table test_vacuum_drop_4.a" | $MYSQL_CLIENT_CONNECT
echo "set retention_period=0;vacuum drop table retain 0 hours dry run" | $MYSQL_CLIENT_CONNECT > /dev/null 
echo "undrop table test_vacuum_drop_4.a" | $MYSQL_CLIENT_CONNECT
echo "select * from test_vacuum_drop_4.a"  | $MYSQL_CLIENT_CONNECT

# check vacuum drop table with the same name
echo "create table test_vacuum_drop_4.b(c int)" | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_vacuum_drop_4.b VALUES (1)" | $MYSQL_CLIENT_CONNECT
echo "drop table test_vacuum_drop_4.b" | $MYSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_4.b(c int)" | $MYSQL_CLIENT_CONNECT
echo "INSERT INTO test_vacuum_drop_4.b VALUES (2)" | $MYSQL_CLIENT_CONNECT
echo "select * from test_vacuum_drop_4.b"  | $MYSQL_CLIENT_CONNECT
echo "set retention_period=0; vacuum drop table retain 0 hours" | $MYSQL_CLIENT_CONNECT
echo "select * from test_vacuum_drop_4.b"  | $MYSQL_CLIENT_CONNECT

echo "drop database if exists test_vacuum_drop_4" | $MYSQL_CLIENT_CONNECT

## Drop table
echo "drop table if exists table_drop_external_location;" | $MYSQL_CLIENT_CONNECT
