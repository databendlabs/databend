#!/usr/bin/env bash
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

bendsql_connect_root --query="""
CREATE OR REPLACE FUNCTION js_wait_test ( INT64 ) RETURNS INT64 LANGUAGE javascript HANDLER = 'wait' AS \$\$
export function wait(n) {
    let i = 0;
    while (i < n * 1000000) { i++ }
    return i;
}
\$\$;
"""

bendsql_connect_root --query='select js_wait_test(50)' &
job_pid=$!
bendsql_connect_root --query='select js_wait_test(1)'
bendsql_connect_root --query='select js_wait_test(2)'
wait $job_pid
