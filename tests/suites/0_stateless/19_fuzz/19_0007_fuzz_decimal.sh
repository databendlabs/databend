#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh


MAX_SCALE=74
TIMES=3

echo "SELECT 1 + 1" > /tmp/decimal.sql
for scale in `seq 0 ${MAX_SCALE}`;do
	precisions=$(shuf -i 1-$[$MAX_SCALE - scale] -n ${TIMES})
	for precision in ${precisions};do
		printf ", 1.1 + 2.%0${scale}d\n"
		printf ", 1.%0${scale}d + 2.%0${scale}d\n"
		printf ", 1%0${precision}d.%0${scale}d + 2.1\n"
		printf ", 1%0${precision}d.%0${scale}d + 2.%0${scale}d\n"
		printf ", 1%0${precision}d.%0${scale}d + 2%0${precision}d.%0${scale}d\n"
	done
done >> /tmp/decimal.sql

cat /tmp/decimal.sql | $BENDSQL_CLIENT_OUTPUT_NULL
echo "1"
