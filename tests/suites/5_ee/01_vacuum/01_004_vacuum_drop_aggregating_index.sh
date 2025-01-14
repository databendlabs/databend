#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


### vacuum from single table

stmt "create or replace database test_vacuum_drop_aggregating_index"

mkdir -p /tmp/test_vacuum_drop_aggregating_index/

stmt "create or replace table test_vacuum_drop_aggregating_index.agg(a int, b int,c int) 'fs:///tmp/test_vacuum_drop_aggregating_index/'"


stmt "insert into test_vacuum_drop_aggregating_index.agg values (1,1,4), (1,2,1), (1,2,4)"

stmt "CREATE OR REPLACE AGGREGATING INDEX index AS SELECT MIN(a), MAX(b) FROM test_vacuum_drop_aggregating_index.agg;"

stmt "insert into test_vacuum_drop_aggregating_index.agg values (2,2,5)"

stmt "REFRESH AGGREGATING INDEX index;"

SNAPSHOT_LOCATION=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_aggregating_index','agg') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX=$(echo "$SNAPSHOT_LOCATION" | cut -d'/' -f1-2)

echo "before vacuum, should be 1 index dir"

ls  /tmp/test_vacuum_drop_aggregating_index/"$PREFIX"/_i_a/ | wc -l

stmt "drop aggregating index index"

stmt "set data_retention_time_in_days=0; select * from fuse_vacuum_drop_aggregating_index('test_vacuum_drop_aggregating_index','agg')" > /dev/null

echo "after vacuum, should be 0 index dir"
find /tmp/test_vacuum_drop_aggregating_index/"$PREFIX"/_i_a/ -type f | wc -l


### vacuum from all tables

stmt "create or replace table test_vacuum_drop_aggregating_index.agg_1(a int, b int,c int) 'fs:///tmp/test_vacuum_drop_aggregating_index/'"


stmt "insert into test_vacuum_drop_aggregating_index.agg_1 values (1,1,4), (1,2,1), (1,2,4)"

stmt "CREATE OR REPLACE AGGREGATING INDEX index_1 AS SELECT MIN(a), MAX(b) FROM test_vacuum_drop_aggregating_index.agg_1;"
stmt "CREATE OR REPLACE AGGREGATING INDEX index_2 AS SELECT MIN(a), MAX(b) FROM test_vacuum_drop_aggregating_index.agg_1;"

stmt "insert into test_vacuum_drop_aggregating_index.agg_1 values (2,2,5)"

stmt "REFRESH AGGREGATING INDEX index_1;"
stmt "REFRESH AGGREGATING INDEX index_2;"

SNAPSHOT_LOCATION_1=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_aggregating_index','agg_1') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX_1=$(echo "$SNAPSHOT_LOCATION_1" | cut -d'/' -f1-2)

echo "before vacuum, should be 2 index dir"

find /tmp/test_vacuum_drop_aggregating_index/"$PREFIX_1"/_i_a/ -type f | wc -l

stmt "create or replace table test_vacuum_drop_aggregating_index.agg_2(a int, b int,c int) 'fs:///tmp/test_vacuum_drop_aggregating_index/'"


stmt "insert into test_vacuum_drop_aggregating_index.agg_2 values (1,1,4), (1,2,1), (1,2,4)"

stmt "CREATE OR REPLACE AGGREGATING INDEX index_3 AS SELECT MIN(a), MAX(b) FROM test_vacuum_drop_aggregating_index.agg_2;"

stmt "insert into test_vacuum_drop_aggregating_index.agg_2 values (2,2,5)"

stmt "REFRESH AGGREGATING INDEX index_3;"

SNAPSHOT_LOCATION_2=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_aggregating_index','agg_2') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX_2=$(echo "$SNAPSHOT_LOCATION_2" | cut -d'/' -f1-2)

stmt "drop aggregating index index_1"
stmt "drop aggregating index index_2"
stmt "drop aggregating index index_3"

echo "before vacuum, should be 1 index dir"

find /tmp/test_vacuum_drop_aggregating_index/"$PREFIX_2"/_i_a/ -type f | wc -l

stmt "set data_retention_time_in_days=0; select * from fuse_vacuum_drop_aggregating_index()" > /dev/null

echo "after vacuum, should be 0 index dir"

find /tmp/test_vacuum_drop_aggregating_index/"$PREFIX_1"/_i_a/ -type f | wc -l
find /tmp/test_vacuum_drop_aggregating_index/"$PREFIX_2"/_i_a/ -type f | wc -l


### create or replace index

stmt "create or replace database test_vacuum_drop_aggregating_index"

mkdir -p /tmp/test_vacuum_drop_aggregating_index/

stmt "create or replace table test_vacuum_drop_aggregating_index.agg(a int, b int,c int) 'fs:///tmp/test_vacuum_drop_aggregating_index/'"


stmt "insert into test_vacuum_drop_aggregating_index.agg values (1,1,4), (1,2,1), (1,2,4)"

stmt "CREATE OR REPLACE AGGREGATING INDEX index AS SELECT MIN(a), MAX(b) FROM test_vacuum_drop_aggregating_index.agg;"

stmt "insert into test_vacuum_drop_aggregating_index.agg values (2,2,5)"

stmt "REFRESH AGGREGATING INDEX index;"

SNAPSHOT_LOCATION=$(echo "select snapshot_location from fuse_snapshot('test_vacuum_drop_aggregating_index','agg') limit 1" | $BENDSQL_CLIENT_CONNECT)
PREFIX=$(echo "$SNAPSHOT_LOCATION" | cut -d'/' -f1-2)

echo "before vacuum, should be 1 index dir"

ls  /tmp/test_vacuum_drop_aggregating_index/"$PREFIX"/_i_a/ | wc -l

stmt "create or replace aggregating index index AS SELECT MIN(a), MAX(b) FROM test_vacuum_drop_aggregating_index.agg;"

stmt "set data_retention_time_in_days=0; select * from fuse_vacuum_drop_aggregating_index('test_vacuum_drop_aggregating_index','agg')" > /dev/null

echo "after vacuum, should be 0 index dir"
find /tmp/test_vacuum_drop_aggregating_index/"$PREFIX"/_i_a/ -type f | wc -l

stmt "drop aggregating index index"
