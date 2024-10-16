#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

ROOT=$(realpath "$CURDIR"/../../../data/delta/partitioned/)

stmt "drop table if exists test_delta;"

echo ">>>> create table test_delta engine = delta location = 'fs://\${ROOT}/';"
echo "create table test_delta engine = delta location = 'fs://${ROOT}/';" | $BENDSQL_CLIENT_CONNECT
# stmt "create table test_delta engine = delta location = 'fs://${ROOT}/';"
query "select * from test_delta order by c5;"
# p* is partition column, c* is normal column
query "select c1 from test_delta where c1 > 20 order by c1;"
query "select c1 from test_delta where c3 = 33 order by c1;"

query "select p4 from test_delta where p4 > 20 order by p4;"
query "select p4 from test_delta where p2 = 12 order by p4;"

query "select c1 from test_delta where p4 > 20 order by c1;"
query "select p4 from test_delta where c1 > 20 order by p4;"
## explain works
query "select count() from test_delta where p0 = 10 and p2 = 12;"

query "select c5, p4 from test_delta where c1 - p0 = 11 order by c5;"

stmt "drop table test_delta;"

