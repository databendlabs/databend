#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

## test vacuum drop table dry run output
echo "drop database if exists test_vacuum_drop_dry_run" | $BENDSQL_CLIENT_CONNECT
echo "CREATE DATABASE test_vacuum_drop_dry_run" | $BENDSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_dry_run.a(c int)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_vacuum_drop_dry_run.a VALUES (1)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "drop table test_vacuum_drop_dry_run.a" | $BENDSQL_CLIENT_CONNECT
count=$(echo "set data_retention_time_in_days=0; vacuum drop table dry run" | $BENDSQL_CLIENT_CONNECT | wc -l)
if [[ ! "$count" ]]; then
  echo "vacuum drop table dry run, count:$count"
  exit 1
fi
count=$(echo "set data_retention_time_in_days=0; vacuum drop table dry run summary" | $BENDSQL_CLIENT_CONNECT | wc -l)
if [[ ! "$count" ]]; then
  echo "vacuum drop table dry run summary, count:$count"
  exit 1
fi
echo "drop database if exists test_vacuum_drop_dry_run" | $BENDSQL_CLIENT_CONNECT

## Setup
echo "drop database if exists test_vacuum_drop" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_2" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_3" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_4" | $BENDSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_vacuum_drop" | $BENDSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop.a(c int)" | $BENDSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop.a VALUES (1)" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "select * from test_vacuum_drop.a" | $BENDSQL_CLIENT_CONNECT

echo "drop table test_vacuum_drop.a" | $BENDSQL_CLIENT_CONNECT

echo "set data_retention_time_in_days=0;vacuum drop table from test_vacuum_drop" | $BENDSQL_CLIENT_CONNECT > /dev/null

echo "create table test_vacuum_drop.b(c int)" | $BENDSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop.b VALUES (2)" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "drop table test_vacuum_drop.b" | $BENDSQL_CLIENT_CONNECT

echo "vacuum drop table from test_vacuum_drop" | $BENDSQL_CLIENT_CONNECT > /dev/null

echo "undrop table test_vacuum_drop.b" | $BENDSQL_CLIENT_CONNECT

# test_vacuum_drop.b has not been vacuum, MUST return [2]
echo "select * from test_vacuum_drop.b" | $BENDSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_vacuum_drop_2" | $BENDSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_2.a(c int)" | $BENDSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop_2.a VALUES (3)" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "CREATE DATABASE test_vacuum_drop_3" | $BENDSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_3.a(c int)" | $BENDSQL_CLIENT_CONNECT

echo "INSERT INTO test_vacuum_drop_3.a VALUES (4)" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "select * from test_vacuum_drop_2.a" | $BENDSQL_CLIENT_CONNECT
echo "select * from test_vacuum_drop_3.a" | $BENDSQL_CLIENT_CONNECT

echo "drop database test_vacuum_drop_2" | $BENDSQL_CLIENT_CONNECT
echo "drop table test_vacuum_drop_3.a" | $BENDSQL_CLIENT_CONNECT

# vacuum without [from db] will vacuum all tables, including tables in drop db
echo "set data_retention_time_in_days=0;vacuum drop table" | $BENDSQL_CLIENT_CONNECT > /dev/null

echo "drop database if exists test_vacuum_drop" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_2" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_3" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists test_vacuum_drop_4" | $BENDSQL_CLIENT_CONNECT

# test external table
echo "drop table if exists table_drop_external_location;" | $BENDSQL_CLIENT_CONNECT

## Create table
echo "create table table_drop_external_location(a int) 's3://testbucket/admin/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | $BENDSQL_CLIENT_CONNECT

table_inserts=(
  "insert into table_drop_external_location(a) values(888)"
  "insert into table_drop_external_location(a) values(1024)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | $BENDSQL_CLIENT_OUTPUT_NULL
done

## Select table
echo "select * from table_drop_external_location order by a;" | $BENDSQL_CLIENT_CONNECT

echo "drop table table_drop_external_location;" | $BENDSQL_CLIENT_CONNECT

echo "set data_retention_time_in_days=0;vacuum drop table" | $BENDSQL_CLIENT_CONNECT > /dev/null

## dry run
echo "CREATE DATABASE test_vacuum_drop_4" | $BENDSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_4.a(c int)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_vacuum_drop_4.a VALUES (1)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "select * from test_vacuum_drop_4.a"  | $BENDSQL_CLIENT_CONNECT
echo "drop table test_vacuum_drop_4.a" | $BENDSQL_CLIENT_CONNECT
echo "set data_retention_time_in_days=0;vacuum drop table dry run" | $BENDSQL_CLIENT_CONNECT > /dev/null
echo "undrop table test_vacuum_drop_4.a" | $BENDSQL_CLIENT_CONNECT
echo "select * from test_vacuum_drop_4.a"  | $BENDSQL_CLIENT_CONNECT

# check vacuum drop table with the same name
echo "create table test_vacuum_drop_4.b(c int)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_vacuum_drop_4.b VALUES (1)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "drop table test_vacuum_drop_4.b" | $BENDSQL_CLIENT_CONNECT
echo "create table test_vacuum_drop_4.b(c int)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_vacuum_drop_4.b VALUES (2)" | $BENDSQL_CLIENT_OUTPUT_NULL
echo "select * from test_vacuum_drop_4.b"  | $BENDSQL_CLIENT_CONNECT
echo "set data_retention_time_in_days=0; vacuum drop table" | $BENDSQL_CLIENT_CONNECT > /dev/null
echo "select * from test_vacuum_drop_4.b"  | $BENDSQL_CLIENT_CONNECT

## test vacuum table output
echo "create table test_vacuum_drop_4.c(c int)" | $BENDSQL_CLIENT_CONNECT
echo "INSERT INTO test_vacuum_drop_4.c VALUES (1)" | $BENDSQL_CLIENT_OUTPUT_NULL
count=$(echo "set data_retention_time_in_days=0; vacuum table test_vacuum_drop_4.c" | $BENDSQL_CLIENT_CONNECT | awk '{print $9}')
if [[ "$count" != "4" ]]; then
  echo "vacuum table, count:$count"
  exit 1
fi
count=$(echo "set data_retention_time_in_days=0; vacuum table test_vacuum_drop_4.c dry run summary" | $BENDSQL_CLIENT_CONNECT | wc -l)
if [[ "$count" != "1" ]]; then
  echo "vacuum table dry run summary, count:$count"
  exit 1
fi

echo "drop database if exists test_vacuum_drop_4" | $BENDSQL_CLIENT_CONNECT

## Drop table
echo "drop table if exists table_drop_external_location;" | $BENDSQL_CLIENT_CONNECT


