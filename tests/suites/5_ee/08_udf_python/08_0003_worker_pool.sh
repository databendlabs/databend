#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root --output null --query="DROP FUNCTION IF EXISTS python_worker_private; DROP STAGE IF EXISTS python_worker_private_stage;"
}
trap cleanup EXIT

bendsql_connect_root --output null --query="
CREATE OR REPLACE STAGE python_worker_private_stage;
COPY INTO @python_worker_private_stage/value.txt FROM (SELECT 42)
FILE_FORMAT=(TYPE=TEXT COMPRESSION=NONE)
SINGLE=TRUE INCLUDE_QUERY_ID=FALSE USE_RAW_PATH=TRUE;
CREATE OR REPLACE FUNCTION python_worker_private (INT64) RETURNS INT64
LANGUAGE python
IMPORTS = ('@python_worker_private_stage/value.txt')
HANDLER = 'handler'
AS \$\$
import os
import sys
import time

path = os.path.join(sys._xoptions['databend_import_directory'], 'value.txt')
with open(path) as f:
    assert int(f.read().strip()) == 42
pid = os.getpid()
with open(path, 'w') as f:
    f.write(str(pid))

def handler(x):
    if x % 256 == 0:
        time.sleep(0.01)
    with open(path) as f:
        assert int(f.read().strip()) == pid
    return x
\$\$;
"

# Many batches share the same runtime pool, but not writable dependencies.
# numbers_local keeps the regression valid in standalone and cluster runs.
bendsql_connect_root --query='SET max_threads=4; SET max_block_size=256; SELECT sum(python_worker_private(to_int64(number))) FROM numbers_local(4096)'
# A subsequent SQL must receive fresh copies, not files changed by old workers.
bendsql_connect_root --query='SET max_threads=4; SET max_block_size=256; SELECT sum(python_worker_private(to_int64(number))) FROM numbers_local(4096)'
# Multiple operators must still make progress with max_threads=1.
bendsql_connect_root --query='SET max_threads=1; SET max_block_size=256; SELECT sum(python_worker_private(python_worker_private(to_int64(number)))) FROM numbers_local(4096)'
