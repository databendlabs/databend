#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "drop database if exists db_stream" | $BENDSQL_CLIENT_CONNECT
echo "create database db_stream" | $BENDSQL_CLIENT_CONNECT
echo "create table db_stream.base(a int) change_tracking = true" | $BENDSQL_CLIENT_CONNECT
echo "create table db_stream.rand like db_stream.base Engine = Random" | $BENDSQL_CLIENT_CONNECT
echo "create stream db_stream.s on table db_stream.base" | $BENDSQL_CLIENT_CONNECT
echo "create table db_stream.sink(a int)" | $BENDSQL_CLIENT_CONNECT

# Define function to write data into the base table.
write_to_base() {
  for i in {1..20}; do
    echo "insert into db_stream.base select * from db_stream.rand limit 10" | $BENDSQL_CLIENT_OUTPUT_NULL

    if (( i % 5 == 0 )); then
      echo "optimize table db_stream.base compact" | $BENDSQL_CLIENT_CONNECT
    fi
  done
}

# Define function to consume data from the stream into the sink table.
consume_from_stream() {
  for i in {1..10}; do
    echo "insert into db_stream.sink select a from db_stream.s" | $BENDSQL_CLIENT_OUTPUT_NULL
  done
}

# Start the write and consume operations in parallel
write_to_base &
write_pid=$!
consume_from_stream &
consume_pid=$!

# Wait for the data writing into the base table to complete
wait $write_pid
# Wait for the final consume operation to complete
wait $consume_pid

# Perform a final consume operation from the stream to ensure all data is consumed
echo "insert into db_stream.sink select a from db_stream.s" | $BENDSQL_CLIENT_OUTPUT_NULL

# Fetch the counts and sums from both base and sink tables
base_count=$(echo "SELECT COUNT(*) FROM db_stream.base;" | $BENDSQL_CLIENT_CONNECT)
sink_count=$(echo "SELECT COUNT(*) FROM db_stream.sink;" | $BENDSQL_CLIENT_CONNECT)
base_sum=$(echo "SELECT SUM(a) FROM db_stream.base;" | $BENDSQL_CLIENT_CONNECT)
sink_sum=$(echo "SELECT SUM(a) FROM db_stream.sink;" | $BENDSQL_CLIENT_CONNECT)

# Compare the counts and sums to verify consistency between the base and sink tables
if [ "$base_count" -eq "$sink_count" ] && [ "$base_sum" -eq "$sink_sum" ]; then
  echo "test stream success"
fi

echo "drop stream if exists db_stream.s" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists db_stream.base all" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists db_stream.rand all" | $BENDSQL_CLIENT_CONNECT
echo "drop table if exists db_stream.sink all" | $BENDSQL_CLIENT_CONNECT
echo "drop database if exists db_stream" | $BENDSQL_CLIENT_CONNECT
