#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

$BENDSQL_CLIENT_CONNECT --query="set query_tag = 'test-query_tag'; select 1; create database if not exists c;"
$BENDSQL_CLIENT_CONNECT --query="set query_tag = 'test-query_tag-2'; select 2; create database if not exists b;"

# count % 5 = 0 because (set command + select start/finished + create start/finished) must be a positive multiple of 5
$BENDSQL_CLIENT_CONNECT --query="select count(query_text) % 5 = 0 from system.query_log group by query_tag having query_tag='test-query_tag'"
$BENDSQL_CLIENT_CONNECT --query="select count(query_text) % 5 = 0 from system.query_log group by query_tag having query_tag='test-query_tag-2'"

$BENDSQL_CLIENT_CONNECT --query="drop database if exists b;drop database if exists c;"
