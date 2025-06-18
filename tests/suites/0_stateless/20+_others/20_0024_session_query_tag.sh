#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

$BENDSQL_CLIENT_CONNECT --query="set query_tag = 'test-query_tag'; select 1; create database if not exists c;"
$BENDSQL_CLIENT_CONNECT --query="set query_tag = 'test-query_tag-2'; select 2; create database if not exists b;"

$BENDSQL_CLIENT_CONNECT --query="drop database if exists b;drop database if exists c;"
