#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root --output null --query="DROP FUNCTION IF EXISTS python_network_uncaught; DROP FUNCTION IF EXISTS python_network_caught; DROP FUNCTION IF EXISTS python_network_null; DROP TABLE IF EXISTS python_network_sink;"
}
trap cleanup EXIT

# Run with the default node policy, python_udf_allow_network=false. The opt-in
# policy is covered by the real-worker tests against a local HTTP server.
bendsql_connect_root --output null --query="
CREATE OR REPLACE FUNCTION python_network_uncaught (INT64) RETURNS INT64
LANGUAGE python HANDLER = 'handler' AS \$\$
import socket

def handler(x):
    socket.create_connection(('127.0.0.1', 1), timeout=1)
    return x
\$\$;
CREATE TABLE python_network_sink (v INT64 NULL);
"

# The runtime's per-row error column must fail INSERT, never become a NULL row.
if output=$(bendsql_connect_root --query='INSERT INTO python_network_sink SELECT python_network_uncaught(1)' 2>&1); then
	echo 'expected network-denied UDF to fail SQL' >&2
	exit 1
fi
echo "$output" | grep -c 'Permission denied'
bendsql_connect_root --query='SELECT count(*) FROM python_network_sink'

# A deliberate Python return is a value, not a runtime failure. Do not infer
# failure from a JSON object's business-level "error" field.
bendsql_connect_root --output null --query="
CREATE OR REPLACE FUNCTION python_network_caught () RETURNS STRING
LANGUAGE python HANDLER = 'handler' AS \$\$
import json, socket

def handler():
    try:
        socket.create_connection(('127.0.0.1', 1), timeout=1)
    except PermissionError:
        return json.dumps({'error': 'caught'})
    return json.dumps({'events': []})
\$\$;
CREATE OR REPLACE FUNCTION python_network_null () RETURNS INT64
LANGUAGE python HANDLER = 'handler' AS \$\$
def handler():
    return None
\$\$;
"
bendsql_connect_root --query="SELECT parse_json(python_network_caught()):error::STRING"
bendsql_connect_root --query='SELECT python_network_null() IS NULL'
# Administrator-only configuration must not appear as a mutable SQL setting.
bendsql_connect_root --query="SELECT count(*) FROM system.settings WHERE name = 'python_udf_allow_network'"
