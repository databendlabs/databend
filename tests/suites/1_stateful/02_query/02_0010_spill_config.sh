#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

bash ../scripts/ci/deploy/databend-query-standalone-config.sh >/dev/null 2>&1

echo "SELECT COUNT() FROM (SELECT number::string, count() FROM numbers_mt(100000) group by number::string);" | $BENDSQL_CLIENT_CONNECT

echo "select count(*)>0 from system.temp_files;" | $BENDSQL_CLIENT_CONNECT

bash ../scripts/ci/deploy/databend-query-standalone.sh >/dev/null 2>&1
