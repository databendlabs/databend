#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

$BENDSQL_CLIENT_CONNECT --query="""
CREATE OR REPLACE FUNCTION python_sleep ( INT64 ) RETURNS INT64 LANGUAGE python HANDLER = 'sleep' AS \$\$
import time

def sleep(n):
    time.sleep(n/100)
    return n
\$\$;
"""

$BENDSQL_CLIENT_CONNECT --query='select python_sleep(100)' &
job_pid=$!
$BENDSQL_CLIENT_CONNECT --query='select python_sleep(1)'
$BENDSQL_CLIENT_CONNECT --query='select python_sleep(2)'
wait $job_pid
