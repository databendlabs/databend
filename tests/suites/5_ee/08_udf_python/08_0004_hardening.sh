#!/usr/bin/env bash
set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

cleanup() {
	bendsql_connect_root --output null --query="DROP FUNCTION IF EXISTS python_env_leak; DROP FUNCTION IF EXISTS python_pip_injection; DROP FUNCTION IF EXISTS python_sandbox_escape;"
}
trap cleanup EXIT

# Workers start with a cleared environment: only the allowlist from
# script_udf_support/README.md may be visible to UDF code.
bendsql_connect_root --output null --query="
CREATE OR REPLACE FUNCTION python_env_leak () RETURNS STRING
LANGUAGE python HANDLER = 'handler' AS \$\$
import os

ALLOWED = {
    'PATH', 'HOME', 'LANG', 'LANGUAGE', 'TZ', 'TMPDIR', 'LD_LIBRARY_PATH',
    'DYLD_LIBRARY_PATH', 'PYTHONHOME', 'PYTHONPATH', 'SSL_CERT_FILE',
    'SSL_CERT_DIR', 'PYTHONIOENCODING', 'PYTHONUTF8', 'PYTHONHASHSEED',
    'OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
}

def handler():
    leaked = sorted(k for k in os.environ if k not in ALLOWED and not k.startswith('LC_'))
    return 'leaked: ' + ','.join(leaked) if leaked else 'clean'
\$\$;
"
bendsql_connect_root --query='SELECT python_env_leak()'

# A dependency must never be passed to pip as an option, URL or path. This is
# rejected before pip runs, so the test needs no network access.
bendsql_connect_root --output null --query="
CREATE OR REPLACE FUNCTION python_pip_injection () RETURNS INT64
LANGUAGE python HANDLER = 'handler' AS \$\$
# /// script
# dependencies = [\"--index-url=http://127.0.0.1:1/simple\"]
# ///
def handler():
    return 1
\$\$;
"
bendsql_connect_root --query='SELECT python_pip_injection()' 2>&1 | grep -c 'Invalid Python UDF dependency' || true

# User code runs in a Landlock + seccomp sandbox: no query node files, no
# network, no child programs, no signals to the query node.
bendsql_connect_root --output null --query="
CREATE OR REPLACE FUNCTION python_sandbox_escape () RETURNS STRING
LANGUAGE python HANDLER = 'handler' AS \$\$
import os, socket, subprocess, tempfile

def attempts():
    yield 'read /etc/passwd', lambda: open('/etc/passwd').read()
    yield 'read parent environ', lambda: open('/proc/%d/environ' % os.getppid()).read()
    yield 'list /', lambda: os.listdir('/')
    yield 'tcp connect', lambda: socket.create_connection(('127.0.0.1', 8000), timeout=1)
    yield 'spawn program', lambda: subprocess.run(['/bin/true'])
    yield 'signal parent', lambda: os.kill(os.getppid(), 0)

def handler():
    allowed = []
    for name, action in attempts():
        try:
            action()
            allowed.append(name)
        except PermissionError:
            pass
    with tempfile.TemporaryFile() as f:
        f.write(b'private temporary files still work')
    return 'allowed: ' + ', '.join(allowed) if allowed else 'sandboxed'
\$\$;
"
bendsql_connect_root --query='SELECT python_sandbox_escape()'
