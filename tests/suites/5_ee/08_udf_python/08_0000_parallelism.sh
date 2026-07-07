#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

bendsql_connect_root --query="""
CREATE OR REPLACE FUNCTION python_sleep ( INT64 ) RETURNS INT64 LANGUAGE python HANDLER = 'sleep' AS \$\$
import time

def sleep(n):
    time.sleep(n/100)
    return n
\$\$;
"""

bendsql_connect_root --query='select python_sleep(100)' &
job_pid=$!
bendsql_connect_root --query='select python_sleep(1)'
bendsql_connect_root --query='select python_sleep(2)'
wait $job_pid
