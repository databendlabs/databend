#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace database db_stream" | bendsql_connect_root
echo "create table db_stream.t(c int)" | bendsql_connect_root
echo "create stream db_stream.s on table db_stream.t" | bendsql_connect_root
echo "insert into db_stream.t values(1)" | bendsql_connect_root_null

echo "create or replace stage stage_05_0003" | bendsql_connect_root

echo "expects one row that outputs the file unloaded"
echo "copy into @stage_05_0003 from (select c from db_stream.s) DETAILED_OUTPUT = true" | bendsql_connect_root | wc -l
echo "expects that the stream s is empty"
echo "select count() from db_stream.s" | bendsql_connect_root

echo "drop stage if exists stage_05_0003" | bendsql_connect_root
echo "drop database if exists db_stream" | bendsql_connect_root
