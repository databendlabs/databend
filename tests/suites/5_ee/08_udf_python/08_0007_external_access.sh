#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

# Local Python worker route, as with the other tests in this suite. Bindings
# allow only approved proxy destinations; native sockets must remain denied.
# No external service/package is needed.
TMP=$(mktemp -d)
SERVER_PID=
cleanup() {
	bendsql_connect_root --output null --query="DROP FUNCTION IF EXISTS python_policy_get; DROP FUNCTION IF EXISTS python_policy_denied; DROP FUNCTION IF EXISTS python_policy_direct; DROP FUNCTION IF EXISTS python_policy_agg; DROP TABLE IF EXISTS python_policy_sink; DROP INTEGRATION IF EXISTS python_policy_access; DROP NETWORK RULE IF EXISTS python_policy_rule;" || true
	if [[ -n "$SERVER_PID" ]]; then
		kill "$SERVER_PID" 2>/dev/null || true
		wait "$SERVER_PID" 2>/dev/null || true
	fi
	rm -rf "$TMP"
}
trap cleanup EXIT
python3 - "$TMP" <<'PY' &
import http.server
import pathlib
import sys


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/redirect':
            self.send_response(302)
            self.send_header('Location', 'http://denied.invalid/events')
            self.end_headers()
        else:
            self.send_response(200)
            self.send_header('Content-Length', '1')
            self.end_headers()
            self.wfile.write(b'7')

    def log_message(self, *args):
        pass


server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
pathlib.Path(sys.argv[1], 'port').write_text(str(server.server_port))
server.serve_forever()
PY
SERVER_PID=$!
for _ in $(seq 1 100); do
	[[ -s "$TMP/port" ]] && break
	kill -0 "$SERVER_PID"
	sleep 0.05
done
PORT=$(<"$TMP/port")
bendsql_connect_root --output null --query="
CREATE NETWORK RULE python_policy_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('127.0.0.1:$PORT');
CREATE EXTERNAL ACCESS INTEGRATION python_policy_access ALLOWED_NETWORK_RULES = (python_policy_rule) ENABLED = TRUE;
CREATE FUNCTION python_policy_get (STRING) RETURNS INT64 LANGUAGE python HANDLER = 'handler' AS \$\$
import urllib.request

def handler(path):
    with urllib.request.urlopen('http://127.0.0.1:$PORT/' + path, timeout=5) as response:
        return int(response.read())
\$\$ EXTERNAL_ACCESS_INTEGRATIONS = (python_policy_access);
CREATE FUNCTION python_policy_denied () RETURNS INT64 LANGUAGE python HANDLER = 'handler' AS \$\$
import urllib.request

def handler():
    with urllib.request.urlopen('http://denied.invalid/events', timeout=5) as response:
        return int(response.read())
\$\$ EXTERNAL_ACCESS_INTEGRATIONS = (python_policy_access);
CREATE FUNCTION python_policy_direct () RETURNS INT64 LANGUAGE python HANDLER = 'handler' AS \$\$
import _socket, socket

def handler():
    _socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    return 1
\$\$ EXTERNAL_ACCESS_INTEGRATIONS = (python_policy_access);
CREATE FUNCTION python_policy_agg (INT64) STATE {s INT64} RETURNS INT64 LANGUAGE python AS \$\$
import urllib.request

class State:
    def __init__(self):
        self.s = 0

def create_state():
    return State()

def accumulate(state, value):
    with urllib.request.urlopen('http://127.0.0.1:$PORT/events', timeout=5) as response:
        state.s += int(response.read())
    return state

def merge(state, other):
    state.s += other.s
    return state

def finish(state):
    return state.s
\$\$ EXTERNAL_ACCESS_INTEGRATIONS = (python_policy_access);
CREATE TABLE python_policy_sink (v INT64 NULL);
"
bendsql_connect_root --query="SELECT python_policy_get('events')"
bendsql_connect_root --query="SELECT python_policy_agg(number::INT64) FROM numbers(2)"
expect_failure() {
	local sql=$1 pattern=$2
	if output=$(bendsql_connect_root --query="$sql" 2>&1); then
		echo 'expected policy-bound UDF to fail SQL' >&2
		exit 1
	fi
	echo "$output" | grep -c "$pattern"
}
expect_failure 'INSERT INTO python_policy_sink SELECT python_policy_denied()' '403'
expect_failure "SELECT python_policy_get('redirect')" '403'
expect_failure 'SELECT python_policy_direct()' 'Permission denied'
bendsql_connect_root --query='SELECT count(*) FROM python_policy_sink'
bendsql_connect_root --output null --query='ALTER EXTERNAL ACCESS INTEGRATION python_policy_access SET ENABLED = FALSE'
# Remote and Script policies share asynchronous meta-watch propagation.
sleep 1
expect_failure "SELECT python_policy_get('events')" '403'
expect_failure 'SELECT python_policy_agg(number::INT64) FROM numbers(2)' '403'
bendsql_connect_root --output null --query='ALTER EXTERNAL ACCESS INTEGRATION python_policy_access SET ENABLED = TRUE'
sleep 1
bendsql_connect_root --query="SELECT python_policy_get('events')"
bendsql_connect_root --output null --query="ALTER NETWORK RULE python_policy_rule SET VALUE_LIST = ('denied.invalid:443')"
sleep 1
expect_failure "SELECT python_policy_get('events')" '403'
