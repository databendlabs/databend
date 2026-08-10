#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

## test vacuum drop table dry run output
echo "drop database if exists test_vacuum_drop_dry_run" | bendsql_connect_root
echo "CREATE DATABASE test_vacuum_drop_dry_run" | bendsql_connect_root
echo "create table test_vacuum_drop_dry_run.a(c int)" | bendsql_connect_root
echo "INSERT INTO test_vacuum_drop_dry_run.a VALUES (1)" | bendsql_connect_root_null
echo "drop table test_vacuum_drop_dry_run.a" | bendsql_connect_root
count=$(echo "set data_retention_time_in_days=0; vacuum drop table dry run" | bendsql_connect_root | wc -l)
if [[ ! "$count" ]]; then
  echo "vacuum drop table dry run, count:$count"
  exit 1
fi
count=$(echo "set data_retention_time_in_days=0; vacuum drop table dry run summary" | bendsql_connect_root | wc -l)
if [[ ! "$count" ]]; then
  echo "vacuum drop table dry run summary, count:$count"
  exit 1
fi
echo "drop database if exists test_vacuum_drop_dry_run" | bendsql_connect_root

## Setup
echo "drop database if exists test_vacuum_drop" | bendsql_connect_root
echo "drop database if exists test_vacuum_drop_2" | bendsql_connect_root
echo "drop database if exists test_vacuum_drop_3" | bendsql_connect_root
echo "drop database if exists test_vacuum_drop_4" | bendsql_connect_root

echo "CREATE DATABASE test_vacuum_drop" | bendsql_connect_root
echo "create table test_vacuum_drop.a(c int)" | bendsql_connect_root

echo "INSERT INTO test_vacuum_drop.a VALUES (1)" | bendsql_connect_root_null

echo "select * from test_vacuum_drop.a" | bendsql_connect_root

echo "drop table test_vacuum_drop.a" | bendsql_connect_root

echo "set data_retention_time_in_days=0;vacuum drop table from test_vacuum_drop" | bendsql_connect_root > /dev/null

echo "create table test_vacuum_drop.b(c int)" | bendsql_connect_root

echo "INSERT INTO test_vacuum_drop.b VALUES (2)" | bendsql_connect_root_null

echo "drop table test_vacuum_drop.b" | bendsql_connect_root

echo "vacuum drop table from test_vacuum_drop" | bendsql_connect_root > /dev/null

echo "undrop table test_vacuum_drop.b" | bendsql_connect_root

# test_vacuum_drop.b has not been vacuum, MUST return [2]
echo "select * from test_vacuum_drop.b" | bendsql_connect_root

echo "CREATE DATABASE test_vacuum_drop_2" | bendsql_connect_root
echo "create table test_vacuum_drop_2.a(c int)" | bendsql_connect_root

echo "INSERT INTO test_vacuum_drop_2.a VALUES (3)" | bendsql_connect_root_null

echo "CREATE DATABASE test_vacuum_drop_3" | bendsql_connect_root
echo "create table test_vacuum_drop_3.a(c int)" | bendsql_connect_root

echo "INSERT INTO test_vacuum_drop_3.a VALUES (4)" | bendsql_connect_root_null

echo "select * from test_vacuum_drop_2.a" | bendsql_connect_root
echo "select * from test_vacuum_drop_3.a" | bendsql_connect_root

echo "drop database test_vacuum_drop_2" | bendsql_connect_root
echo "drop table test_vacuum_drop_3.a" | bendsql_connect_root

# vacuum without [from db] will vacuum all tables, including tables in drop db
echo "set data_retention_time_in_days=0;vacuum drop table" | bendsql_connect_root > /dev/null

echo "drop database if exists test_vacuum_drop" | bendsql_connect_root
echo "drop database if exists test_vacuum_drop_2" | bendsql_connect_root
echo "drop database if exists test_vacuum_drop_3" | bendsql_connect_root
echo "drop database if exists test_vacuum_drop_4" | bendsql_connect_root

# test external table
echo "drop table if exists table_drop_external_location;" | bendsql_connect_root

## Create table
echo "create table table_drop_external_location(a int) 's3://testbucket/data/' connection=(access_key_id ='minioadmin' secret_access_key ='minioadmin' endpoint_url='${STORAGE_S3_ENDPOINT_URL}');" | bendsql_connect_root

table_inserts=(
  "insert into table_drop_external_location(a) values(888)"
  "insert into table_drop_external_location(a) values(1024)"
)

for i in "${table_inserts[@]}"; do
  echo "$i" | bendsql_connect_root_null
done

## Select table
echo "select * from table_drop_external_location order by a;" | bendsql_connect_root

echo "drop table table_drop_external_location;" | bendsql_connect_root

echo "set data_retention_time_in_days=0;vacuum drop table" | bendsql_connect_root > /dev/null

## dry run
echo "CREATE DATABASE test_vacuum_drop_4" | bendsql_connect_root
echo "create table test_vacuum_drop_4.a(c int)" | bendsql_connect_root
echo "INSERT INTO test_vacuum_drop_4.a VALUES (1)" | bendsql_connect_root_null
echo "select * from test_vacuum_drop_4.a"  | bendsql_connect_root
echo "drop table test_vacuum_drop_4.a" | bendsql_connect_root
echo "set data_retention_time_in_days=0;vacuum drop table dry run" | bendsql_connect_root > /dev/null
echo "undrop table test_vacuum_drop_4.a" | bendsql_connect_root
echo "select * from test_vacuum_drop_4.a"  | bendsql_connect_root

# check vacuum drop table with the same name
echo "create table test_vacuum_drop_4.b(c int)" | bendsql_connect_root
echo "INSERT INTO test_vacuum_drop_4.b VALUES (1)" | bendsql_connect_root_null
echo "drop table test_vacuum_drop_4.b" | bendsql_connect_root
echo "create table test_vacuum_drop_4.b(c int)" | bendsql_connect_root
echo "INSERT INTO test_vacuum_drop_4.b VALUES (2)" | bendsql_connect_root_null
echo "select * from test_vacuum_drop_4.b"  | bendsql_connect_root
echo "set data_retention_time_in_days=0; vacuum drop table" | bendsql_connect_root > /dev/null
echo "select * from test_vacuum_drop_4.b"  | bendsql_connect_root

## test vacuum table output
echo "create table test_vacuum_drop_4.c(c int)" | bendsql_connect_root
echo "INSERT INTO test_vacuum_drop_4.c VALUES (1),(2)" | bendsql_connect_root_null
count=$(echo "set data_retention_time_in_days=0; vacuum table test_vacuum_drop_4.c" | bendsql_connect_root | awk '{print $9}')
if [[ "$count" != "4" ]]; then
  echo "vacuum table, count:$count"
  exit 1
fi
count=$(echo "set data_retention_time_in_days=0; vacuum table test_vacuum_drop_4.c dry run summary" | bendsql_connect_root | wc -l)
if [[ "$count" != "1" ]]; then
  echo "vacuum table dry run summary, count:$count"
  exit 1
fi

echo "drop database if exists test_vacuum_drop_4" | bendsql_connect_root

## Drop table
echo "drop table if exists table_drop_external_location;" | bendsql_connect_root


