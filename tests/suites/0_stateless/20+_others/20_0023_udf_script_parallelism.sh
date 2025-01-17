#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

$BENDSQL_CLIENT_CONNECT --query="""
CREATE OR REPLACE FUNCTION js_wait_test ( INT64 ) RETURNS INT64 LANGUAGE javascript HANDLER = 'wait' AS \$\$
export function wait(n) {
    let i = 0;
    while (i < n * 1000000) { i++ }
    return i;
}
\$\$;
"""

$BENDSQL_CLIENT_CONNECT --query='select js_wait_test(100)' &
job_pid=$!
$BENDSQL_CLIENT_CONNECT --query='select js_wait_test(1)'
$BENDSQL_CLIENT_CONNECT --query='select js_wait_test(2)'
wait $job_pid
