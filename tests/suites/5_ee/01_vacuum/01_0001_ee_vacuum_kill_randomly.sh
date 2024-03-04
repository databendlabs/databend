#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

## Setup
echo "drop database if exists test_vacuum" | $BENDSQL_CLIENT_CONNECT

echo "CREATE DATABASE test_vacuum" | $BENDSQL_CLIENT_CONNECT
echo "create table test_vacuum.a(c int)" | $BENDSQL_CLIENT_CONNECT

# insert some values
(
  for ((i = 1; i <= 1000; i++)); do
    echo "INSERT INTO test_vacuum.a VALUES ($i)" | $BENDSQL_CLIENT_CONNECT
  done
) >/dev/null 2>&1 &
pid=$!
# there is a new process introduced from background subshell, use ps -p $pid to checkout

# kill query randomly
sleep_time=$(expr $RANDOM % 5 + 5)
sleep $sleep_time
killall databend-query >/dev/null 2>&1
kill $pid

# restart query
bash ../scripts/ci/deploy/databend-query-standalone.sh >/dev/null 2>&1

# check if before and after vacuum table the table count matched
old_count=$(echo "select * from test_vacuum.a order by c" | $BENDSQL_CLIENT_CONNECT)

echo "set data_retention_time_in_days=0; vacuum table test_vacuum.a" | $BENDSQL_CLIENT_CONNECT >/dev/null
#echo "optimize table test_vacuum.a all" | $BENDSQL_CLIENT_CONNECT
count=$(echo "select * from test_vacuum.a order by c" | $BENDSQL_CLIENT_CONNECT)

if [[ "$old_count" != "$count" ]]; then
  echo "vacuum table, old count:$old_count,new count:$count"
  exit 1
fi

echo "vacuum table success"
echo "drop database if exists test_vacuum" | $BENDSQL_CLIENT_CONNECT
