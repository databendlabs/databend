#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace database db_stream" | $BENDSQL_CLIENT_CONNECT
echo "create table db_stream.t(c int)" | $BENDSQL_CLIENT_CONNECT
echo "create stream db_stream.s on table db_stream.t" | $BENDSQL_CLIENT_CONNECT
echo "insert into db_stream.t values(1)" | $BENDSQL_CLIENT_OUTPUT_NULL

echo "create or replace stage stage_05_0003" | $BENDSQL_CLIENT_CONNECT

echo "expects one row that outputs the file unloaded"
echo "copy into @stage_05_0003 from (select c from db_stream.s) DETAILED_OUTPUT = true" | $BENDSQL_CLIENT_CONNECT | wc -l
echo "expects that the stream s is empty"
echo "select count() from db_stream.s" | $BENDSQL_CLIENT_CONNECT

echo "drop stage if exists stage_05_0003" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db_stream" | $BENDSQL_CLIENT_CONNECT
