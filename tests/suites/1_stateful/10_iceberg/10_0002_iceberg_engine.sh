#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

ROOT=$(realpath "$CURDIR"/../../../data/iceberg/iceberg_ctl/iceberg_db/iceberg_tbl/)

stmt "drop table if exists test_iceberg;"

echo ">>>> create table test_iceberg engine = iceberg 'fs://\${ROOT}/';"
echo "create table test_iceberg engine = iceberg 'fs://${ROOT}/';" | $BENDSQL_CLIENT_CONNECT

query "select * from test_iceberg order by id, data;"

query "show create table test_iceberg;"

stmt "drop table test_iceberg;"
