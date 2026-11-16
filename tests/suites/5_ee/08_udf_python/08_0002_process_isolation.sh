#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root --output null --query="DROP FUNCTION IF EXISTS python_process_isolation; DROP FUNCTION IF EXISTS python_process_check;"
}
trap cleanup EXIT

bendsql_connect_root --output null --query="
CREATE OR REPLACE FUNCTION python_process_isolation (INT64) RETURNS INT64
LANGUAGE python HANDLER = 'handler' AS \$\$
import builtins
import os
import sys
import time

def handler(x):
    marker = 'databend-process-isolation-' + str(x)
    assert not hasattr(builtins, '_databend_process_marker') or builtins._databend_process_marker == x
    builtins._databend_process_marker = x
    sys.path.append(marker)
    os.environ['DATABEND_PROCESS_MARKER'] = marker
    time.sleep(0.1)
    assert builtins._databend_process_marker == x
    assert sys.path[-1] == marker
    assert os.environ['DATABEND_PROCESS_MARKER'] == marker
    print('user output must not corrupt IPC')
    return x
\$\$;
CREATE OR REPLACE FUNCTION python_process_check (INT64) RETURNS INT64
LANGUAGE python HANDLER = 'handler' AS \$\$
import builtins
import os
import sys

def handler(x):
    assert not hasattr(builtins, '_databend_process_marker')
    assert 'DATABEND_PROCESS_MARKER' not in os.environ
    assert not any(p.startswith('databend-process-isolation-') for p in sys.path)
    return x
\$\$;
"

# Different SQL executions of the SAME UDF must not share Python state.
pids=()
for i in $(seq 1 8); do
	bendsql_connect_root --output null --query="SELECT python_process_isolation($i)" &
	pids+=("$!")
done
for pid in "${pids[@]}"; do
	wait "$pid"
done

# The next SQL starts clean, and different UDFs within one SQL are independent.
bendsql_connect_root --query='SELECT python_process_isolation(42)'
bendsql_connect_root --query='SELECT python_process_check(python_process_isolation(7))'
bendsql_connect_root --query='SELECT python_process_check(9)'
# Also exercise two separately built UDF operators below an aggregate/exchange.
# The same SQL works in both standalone and cluster regression runs.
bendsql_connect_root --query='SELECT sum(python_process_check(python_process_check(to_int64(number)))) FROM numbers(100000)'
