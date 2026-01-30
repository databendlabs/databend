#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

SQL="SELECT COUNT(1), 'numbers(10)' FROM numbers(10)"

for i in $(seq 1 200); do
	SQL="$SQL UNION ALL SELECT COUNT(1), 'numbers(10)' FROM numbers(10)"
done

echo "SELECT * FROM ($SQL) LIMIT 200" | $BENDSQL_CLIENT_CONNECT | wc -l
