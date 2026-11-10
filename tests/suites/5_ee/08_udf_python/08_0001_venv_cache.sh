#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root --output null --query="DROP FUNCTION IF EXISTS python_venv_cache; DROP STAGE IF EXISTS python_venv_cache_stage;"
}
trap cleanup EXIT

# A stage import exercises the environment cache without pip/network access.
# Repeated queries drop the live directory and restore it from the cached zip.
bendsql_connect_root --output null --query="
CREATE OR REPLACE STAGE python_venv_cache_stage;
COPY INTO @python_venv_cache_stage/value.txt FROM (SELECT 42)
FILE_FORMAT=(TYPE=TEXT COMPRESSION=NONE)
SINGLE=TRUE INCLUDE_QUERY_ID=FALSE USE_RAW_PATH=TRUE;
CREATE OR REPLACE FUNCTION python_venv_cache () RETURNS INT64
LANGUAGE python
IMPORTS = ('@python_venv_cache_stage/value.txt')
HANDLER = 'handler'
AS \$\$
import os
import sys

def handler():
    with open(os.path.join(sys._xoptions['databend_import_directory'], 'value.txt')) as f:
        return int(f.read().strip())
\$\$;
"

bendsql_connect_root --query='SELECT python_venv_cache()'
# Concurrent hits must not recreate/delete a shared directory under one another.
pids=()
for i in $(seq 1 8); do
	bendsql_connect_root --output null --query='SELECT python_venv_cache()' &
	pids+=("$!")
done
bendsql_connect_root --query='SELECT 1'
for pid in "${pids[@]}"; do
	wait "$pid"
done
bendsql_connect_root --query='SELECT python_venv_cache()'
