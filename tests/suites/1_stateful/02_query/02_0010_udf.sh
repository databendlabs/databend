#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

echo "SET external_server_request_max_threads=1; SELECT ping(number) FROM numbers(100000) IGNORE_RESULT;" | $BENDSQL_CLIENT_CONNECT

echo "SET external_server_request_max_threads=2; SELECT ping(number) FROM numbers(100000) IGNORE_RESULT;" | $BENDSQL_CLIENT_CONNECT

echo "SET external_server_request_max_threads=4; SELECT ping(number) FROM numbers(100000) IGNORE_RESULT;" | $BENDSQL_CLIENT_CONNECT

echo "SET external_server_request_max_threads=8; SELECT ping(number) FROM numbers(100000) IGNORE_RESULT;" | $BENDSQL_CLIENT_CONNECT

echo "SET external_server_request_max_threads=16; SELECT ping(number) FROM numbers(100000) IGNORE_RESULT;" | $BENDSQL_CLIENT_CONNECT

echo "SET external_server_request_max_threads=256; SELECT ping(number) FROM numbers(100000) IGNORE_RESULT;" | $BENDSQL_CLIENT_CONNECT

echo "SET external_server_request_max_threads=1024; SELECT ping(number) FROM numbers(100000) IGNORE_RESULT;" | $BENDSQL_CLIENT_CONNECT

