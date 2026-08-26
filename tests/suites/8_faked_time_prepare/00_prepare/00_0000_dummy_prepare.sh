#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "create or replace DATABASE test_faketime" |  bendsql_connect_root

echo "create table test_faketime.t(c timestamp)" |  bendsql_connect_root

echo "insert into table test_faketime.t values(now())" | bendsql_connect_root
