#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace DATABASE test_faketime" |  $BENDSQL_CLIENT_CONNECT

echo "create table test_faketime.t(c timestamp)" |  $BENDSQL_CLIENT_CONNECT

echo "insert into table test_faketime.t values(now())" | $BENDSQL_CLIENT_CONNECT
